package app

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/pandops/flowai-model-registry/internal/litellm"
	"github.com/pandops/flowai-model-registry/internal/store"
)

// edgeErr is a key-edge error. The edge carries no request_id, so the body
// has no request_id field (D12).
func edgeErr(status int, code string) (int, any) {
	body := map[string]any{"code": code, "message": code}
	if code == "configuration_error" {
		body["failure_reason"] = "llm_configuration_incomplete"
	}
	return status, body
}

var (
	edgeNotCurrent  = func() (int, any) { return edgeErr(404, "claim_not_current") }
	edgeUnavailable = func() (int, any) { return edgeErr(503, "service_unavailable") }
	edgeIncomplete  = func() (int, any) { return edgeErr(422, "configuration_error") }
)

// keyEdge serves GET /v1/tasks/{task_id}/litellm-key for the holder of a
// current claim token.
func (a *App) keyEdge(ctx context.Context, taskParam, authorization string) (int, any) {
	task, ok := parseID(taskParam)
	if !ok {
		return edgeErr(400, "invalid_request")
	}
	bearer, found := strings.CutPrefix(authorization, "Bearer ")
	if !found {
		return edgeNotCurrent()
	}
	tok, ok := a.verifyToken(strings.TrimSpace(bearer))
	if !ok || tok.TaskID != task {
		return edgeNotCurrent()
	}
	k := tok.ClaimKey
	if _, err := a.claimFlow(ctx, k, tok.ExecutorID); errors.Is(err, errClaimNotCurrent) {
		return edgeNotCurrent()
	} else if err != nil {
		return edgeUnavailable()
	}

	var snap *store.Snapshot
	var model *store.Model
	var stored *store.TaskKey
	var ft *store.FlowTeam
	err := a.deps.Store.Tx(ctx, "key-edge-read", func(tx store.Tx) error {
		s, err := tx.Snapshot(k, false)
		if errors.Is(err, store.ErrNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		snap = s
		if model, err = tx.ModelByKey(s.ModelKey, false); err != nil {
			return err
		}
		if key, err := tx.TaskKey(k); err == nil {
			stored = key
		} else if !errors.Is(err, store.ErrNotFound) {
			return err
		}
		ft, err = tx.FlowTeam(k.TeamID, s.FlowID, false)
		return err
	})
	if err != nil {
		return edgeUnavailable()
	}
	if snap == nil || snap.ExecutorID != tok.ExecutorID {
		return edgeIncomplete()
	}

	baseURL, status, body := a.dataURL(ctx, k.TeamID, tok.ExecutorID)
	if status != 0 {
		return status, body
	}
	if _, err := a.fetch(ctx, *model); errors.Is(err, errNotFound) {
		return edgeIncomplete()
	} else if err != nil {
		return edgeUnavailable()
	}

	if stored != nil {
		if stored.RevokeRequestedAt != nil {
			return edgeNotCurrent()
		}
		key, err := a.openKey(k, stored.Ciphertext)
		if err != nil {
			a.log.Error("stored task key does not decrypt", "task_id", k.TaskID)
			return edgeUnavailable()
		}
		if err := a.extend(ctx, *stored); err != nil {
			return edgeUnavailable()
		}
		return 200, keyBody(baseURL, snap.ModelAlias, key)
	}
	if ft == nil {
		return edgeIncomplete()
	}
	key, err := a.issue(ctx, *snap, *ft)
	if err != nil {
		return edgeUnavailable()
	}
	return 200, keyBody(baseURL, snap.ModelAlias, key)
}

func keyBody(baseURL, alias, key string) map[string]any {
	return map[string]any{"OPENAI_BASE_URL": baseURL, "OPENAI_MODEL": alias, "OPENAI_API_KEY": key}
}

// dataURL reads the Executor's registered type and returns its data URL; a
// non-zero status is the edge error.
func (a *App) dataURL(ctx context.Context, team, executor int64) (string, int, any) {
	r, err := a.call(ctx, "executor-registry.service.executor-type.pub", team, map[string]any{"executor_id": executor})
	if err != nil || r.Status >= 500 {
		s, b := edgeUnavailable()
		return "", s, b
	}
	var out struct {
		TeamID int64  `json:"team_id"`
		Type   string `json:"executor_type"`
	}
	if r.Status != 200 || json.Unmarshal(r.Body, &out) != nil || out.TeamID != team {
		s, b := edgeIncomplete()
		return "", s, b
	}
	u := a.deps.Config.LiteLLM.DataURLs[out.Type]
	if u == "" {
		s, b := edgeIncomplete()
		return "", s, b
	}
	return u, 0, nil
}

// extend moves a stored key's expiry to now plus the backstop.
func (a *App) extend(ctx context.Context, key store.TaskKey) error {
	backstop := a.deps.Config.KeyBackstop
	if err := a.llm.ExtendKey(ctx, key.Token, backstop); err != nil {
		return err
	}
	now := a.now()
	return a.deps.Store.Tx(ctx, "task-key-extend", func(tx store.Tx) error {
		cur, err := tx.TaskKey(key.ClaimKey)
		if err != nil {
			return err
		}
		cur.ExpiresAt, cur.ExtendedAt = now.Add(backstop), now
		if err := tx.UpdateTaskKey(*cur); err != nil {
			return err
		}
		return a.emit(tx, key.TeamID, "task-key-extended", map[string]any{"task_id": key.TaskID, "attempt": key.Attempt,
			"claim_seq": key.ClaimSeq, "expires_at": ts(cur.ExpiresAt)})
	})
}

// issue creates, stores and returns the claim's virtual key. The key is
// recorded as an orphan before LiteLLM creates it, so a key that is never
// stored is revoked by reconciliation.
func (a *App) issue(ctx context.Context, s store.Snapshot, ft store.FlowTeam) (string, error) {
	key, token, err := newVirtualKey()
	if err != nil {
		return "", err
	}
	a.issuing.Store(token, true)
	defer a.issuing.Delete(token)
	var orphan int64
	if err := a.deps.Store.Tx(ctx, store.TxTaskKeyOrphan, func(tx store.Tx) (err error) {
		orphan, err = tx.InsertOrphan(store.Orphan{ClaimKey: s.ClaimKey, Token: token, CreatedAt: a.now()})
		return err
	}); err != nil {
		return "", err
	}
	spec := litellm.KeySpec{Key: key, Model: s.ModelAlias, TeamID: ft.LiteLLMTeamID, Duration: a.deps.Config.KeyBackstop,
		Metadata: map[string]any{"team_id": s.TeamID, "task_id": s.TaskID, "attempt": s.Attempt, "claim_seq": s.ClaimSeq, "flow_id": s.FlowID}}
	if l := s.PerTaskLimit; l != nil {
		if l.MaxBudgetUSD != nil {
			if n, ok := decimalNumber(*l.MaxBudgetUSD); ok {
				spec.MaxBudget = &n
			}
		}
		spec.RPMLimit, spec.TPMLimit = l.RPM, l.TPM
	}
	if err := a.llm.GenerateKey(ctx, spec); err != nil {
		return "", err
	}
	sealed, err := a.sealKey(s.ClaimKey, key)
	if err != nil {
		return "", err
	}
	now := a.now()
	expires := now.Add(a.deps.Config.KeyBackstop)
	conflict := false
	err = a.deps.Store.Tx(ctx, store.TxTaskKeyStore, func(tx store.Tx) error {
		err := tx.InsertTaskKey(store.TaskKey{ClaimKey: s.ClaimKey, Token: token, Ciphertext: sealed, ExpiresAt: expires, ExtendedAt: now})
		if errors.Is(err, store.ErrConflict) {
			conflict = true // a concurrent request stored its key first
			return nil
		}
		if err != nil {
			return err
		}
		if err := tx.DeleteOrphan(orphan); err != nil {
			return err
		}
		return a.emit(tx, s.TeamID, "task-key-issued", map[string]any{"task_id": s.TaskID, "attempt": s.Attempt,
			"claim_seq": s.ClaimSeq, "model_alias": s.ModelAlias, "expires_at": ts(expires)})
	})
	if err != nil {
		return "", err
	}
	if conflict {
		// Our key stays an orphan and is revoked; return the stored one.
		var stored *store.TaskKey
		if err := a.deps.Store.Tx(ctx, "key-edge-read", func(tx store.Tx) (err error) {
			stored, err = tx.TaskKey(s.ClaimKey)
			return err
		}); err != nil {
			return "", err
		}
		return a.openKey(s.ClaimKey, stored.Ciphertext)
	}
	return key, nil
}
