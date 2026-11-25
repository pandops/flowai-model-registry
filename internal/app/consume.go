package app

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/pandops/flowai-model-registry/internal/broker"
	"github.com/pandops/flowai-model-registry/internal/litellm"
	"github.com/pandops/flowai-model-registry/internal/store"
)

// event is the part of a consumed stream message model-registry reads.
type event struct {
	Publisher     string          `json:"publisher"`
	PublisherSeq  json.RawMessage `json:"publisher_seq"`
	TeamID        json.RawMessage `json:"team_id"`
	Kind          string          `json:"kind"`
	ExecutorID    json.RawMessage `json:"executor_id"`
	TaskID        json.RawMessage `json:"task_id"`
	Attempt       json.RawMessage `json:"attempt"`
	ClaimSeq      json.RawMessage `json:"claim_seq"`
	FailureReason string          `json:"failure_reason"`
}

// errExecutorUnknown marks an Executor event whose executor_id is not yet in
// the executor_registered projection.
var errExecutorUnknown = errors.New("executor not in the projection")

// endCauses are the claim-ending kinds and their revocation cause.
var endCauses = map[string]string{"released": "released", "failed": "failed", "finished": "finished", "cancelled": "cancelled"}

// consume processes one delivery of the durable consumer idempotently by
// (publisher, publisher_seq).
func (a *App) consume(ctx context.Context, d broker.Delivery) {
	m := d.Message()
	var ev event
	if json.Unmarshal(m.Data, &ev) != nil {
		_ = d.Term()
		return
	}
	seq, ok1 := jsonID(ev.PublisherSeq)
	team, ok2 := jsonID(ev.TeamID)
	if !ok1 || !ok2 || ev.Publisher == "" {
		_ = d.Term()
		return
	}
	parts := strings.Split(m.Subject, ".")
	var apply func(tx store.Tx) error
	var after func()
	dedupe := ev.Publisher
	switch {
	case strings.HasPrefix(m.Subject, "executor-registry.event.executor_registered."),
		strings.HasPrefix(m.Subject, "executor-registry.event.executor_removed."):
		executor, ok := jsonID(ev.ExecutorID)
		if !ok {
			_ = d.Term()
			return
		}
		removed := ev.Kind == "executor_removed" || strings.Contains(m.Subject, ".executor_removed.")
		apply = func(tx store.Tx) error {
			return tx.PutExecutor(store.Executor{ExecutorID: executor, TeamID: team, Removed: removed})
		}
	case len(parts) == 5 && parts[0] == "executor" && parts[1] == "event":
		executor, ok := jsonID(ev.ExecutorID)
		subjectExecutor, ok2 := parseID(parts[2])
		_, ok3 := claimOf(team, ev)
		if !ok || !ok2 || executor != subjectExecutor || !ok3 {
			_ = d.Term()
			return
		}
		dedupe = "executor-" + strconv.FormatInt(executor, 10)
		kind := parts[3]
		cause, ends := endCauses[kind]
		// The team is the Executor's own team from the projection; header and
		// body grant nothing. Only the claim's own Executor touches it.
		var k store.ClaimKey
		owned := false
		apply = func(tx store.Tx) error {
			owned = false
			e, err := tx.Executor(executor)
			if errors.Is(err, store.ErrNotFound) {
				return errExecutorUnknown
			}
			if err != nil {
				return err
			}
			if e.Removed {
				return nil
			}
			k, _ = claimOf(e.TeamID, ev)
			s, err := tx.Snapshot(k, ends)
			if errors.Is(err, store.ErrNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			if s.ExecutorID != executor {
				return nil
			}
			owned = true
			if ends {
				return a.endClaim(tx, k, cause)
			}
			return nil
		}
		after = func() {
			switch {
			case !owned:
			case !ends:
				a.extendOnActivity(ctx, k)
			case kind == "failed" && ev.FailureReason == "llm_budget_exceeded":
				a.refreshClaimSpend(ctx, k)
			}
		}
	case m.Subject == "task-manager.event.released.sub" || m.Subject == "task-manager.event.cancelled.sub":
		k, ok := claimOf(team, ev)
		if !ok {
			_ = d.Term()
			return
		}
		cause := endCauses[parts[2]]
		apply = func(tx store.Tx) error { return a.endClaim(tx, k, cause) }
	case m.Subject == "task-manager.event.archived.sub":
		task, ok := jsonID(ev.TaskID)
		if !ok {
			_ = d.Term()
			return
		}
		apply = func(tx store.Tx) error {
			snaps, err := tx.TaskSnapshots(team, task)
			if err != nil {
				return err
			}
			for _, s := range snaps {
				if err := a.endClaim(tx, s.ClaimKey, "archived"); err != nil {
					return err
				}
			}
			return nil
		}
	default:
		_ = d.Ack()
		return
	}
	fresh := false
	err := a.deps.Store.Tx(ctx, store.TxConsume, func(tx store.Tx) error {
		var err error
		if fresh, err = tx.MarkConsumed(dedupe, seq); err != nil || !fresh {
			return err
		}
		return apply(tx)
	})
	if errors.Is(err, errExecutorUnknown) {
		// executor-registry's registration has not been projected yet.
		_ = d.Nak()
		return
	}
	if err != nil {
		a.log.Warn("stream message not processed", "subject", m.Subject, "error", err)
		_ = d.Nak()
		return
	}
	_ = d.Ack()
	if fresh && after != nil {
		after()
	}
}

func claimOf(team int64, ev event) (store.ClaimKey, bool) {
	task, ok1 := jsonID(ev.TaskID)
	attempt, ok2 := jsonID(ev.Attempt)
	seq, ok3 := jsonID(ev.ClaimSeq)
	return store.ClaimKey{TeamID: team, TaskID: task, Attempt: attempt, ClaimSeq: seq}, ok1 && ok2 && ok3
}

// endClaim ends a claim's snapshot and requests the revocation of its key.
func (a *App) endClaim(tx store.Tx, k store.ClaimKey, cause string) error {
	now := a.now()
	s, err := tx.Snapshot(k, true)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if s.EndedAt == nil {
		s.EndedAt = &now
		if err := tx.UpdateSnapshot(*s); err != nil {
			return err
		}
	}
	key, err := tx.TaskKey(k)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if key.RevokeRequestedAt != nil {
		return nil
	}
	key.RevokeCause, key.RevokeRequestedAt = &cause, &now
	return tx.UpdateTaskKey(*key)
}

// extendOnActivity extends a current claim's key at most once per quarter
// of the backstop period when its Executor reports activity.
func (a *App) extendOnActivity(ctx context.Context, k store.ClaimKey) {
	var key *store.TaskKey
	_ = a.deps.Store.Tx(ctx, "key-edge-read", func(tx store.Tx) error {
		cur, err := tx.TaskKey(k)
		if err == nil {
			key = cur
		}
		return nil
	})
	if key == nil || key.RevokeRequestedAt != nil || a.now().Sub(key.ExtendedAt) < a.deps.Config.KeyBackstop/4 {
		return
	}
	if err := a.extend(ctx, *key); err != nil {
		a.log.Warn("task key not extended", "task_id", k.TaskID, "error", err)
	}
}

// refreshClaimSpend refreshes the spend of the claim's Flow after an
// llm_budget_exceeded failure.
func (a *App) refreshClaimSpend(ctx context.Context, k store.ClaimKey) {
	var ft *store.FlowTeam
	_ = a.deps.Store.Tx(ctx, "flow-team-read", func(tx store.Tx) error {
		s, err := tx.Snapshot(k, false)
		if err != nil {
			return err
		}
		ft, err = tx.FlowTeam(k.TeamID, s.FlowID, false)
		return err
	})
	if ft != nil && ft.Limit != nil && ft.Limit.MaxBudgetUSD != nil {
		a.observeSpend(ctx, *ft)
	}
}

// ReconcileOnce runs compensations, orphan and pending revocations and
// pending archive and unarchive operations once. Items that LiteLLM or a
// peer cannot finish now stay pending; only store failures are errors.
func (a *App) ReconcileOnce(ctx context.Context) error {
	var comps []store.Compensation
	var orphans []store.Orphan
	var revocations []store.TaskKey
	var ops []store.Operation
	err := a.deps.Store.Tx(ctx, "reconcile-read", func(tx store.Tx) (err error) {
		if comps, err = tx.Compensations(); err != nil {
			return err
		}
		if orphans, err = tx.Orphans(); err != nil {
			return err
		}
		if revocations, err = tx.PendingRevocations(); err != nil {
			return err
		}
		ops, err = tx.Operations()
		return err
	})
	if err != nil {
		return err
	}
	for _, c := range comps {
		a.compensate(ctx, c)
	}
	for _, o := range orphans {
		a.revokeOrphan(ctx, o)
	}
	for _, k := range revocations {
		a.revoke(ctx, k)
	}
	for _, op := range ops {
		var m *store.Model
		if err := a.deps.Store.Tx(ctx, "reconcile-model", func(tx store.Tx) (err error) {
			m, err = tx.ModelByKey(op.ModelKey, false)
			return err
		}); err != nil {
			continue
		}
		if err := a.runOperation(ctx, *m, op, nil); err != nil {
			a.log.Info("model operation still pending", "model_alias", m.Alias, "operation", op.Kind, "reason", err.Error())
		}
	}
	return nil
}

func gone(err error) bool { return err == nil || errors.Is(err, litellm.ErrNotFound) }

func (a *App) compensate(ctx context.Context, c store.Compensation) {
	ids := []string{}
	if c.LiteLLMModelID != "" {
		ids = append(ids, c.LiteLLMModelID)
	} else {
		all, err := a.llm.ListModels(ctx)
		if err != nil {
			return
		}
		for _, r := range all {
			if id, ok := intOf(r.ModelInfo["flowai_model_id"]); ok && r.ModelName == c.Alias && id == c.ModelID {
				ids = append(ids, r.ID)
			}
		}
	}
	for _, id := range ids {
		if !gone(a.llm.DeleteModel(ctx, id)) {
			return
		}
	}
	if c.CredentialName != "" && !gone(a.llm.DeleteCredential(ctx, c.CredentialName)) {
		return
	}
	err := a.deps.Store.Tx(ctx, "compensation-done", func(tx store.Tx) error {
		if err := tx.DeleteCompensation(c.ID); err != nil {
			return err
		}
		return a.emit(tx, c.TeamID, "model-create-compensated", map[string]any{"model_id": c.ModelID, "model_alias": c.Alias})
	})
	if err != nil {
		a.log.Warn("compensation not recorded as done", "model_alias", c.Alias, "error", err)
	}
}

func (a *App) revokeOrphan(ctx context.Context, o store.Orphan) {
	if _, busy := a.issuing.Load(o.Token); busy {
		return
	}
	var stored *store.TaskKey
	_ = a.deps.Store.Tx(ctx, "key-edge-read", func(tx store.Tx) error {
		if k, err := tx.TaskKey(o.ClaimKey); err == nil {
			stored = k
		}
		return nil
	})
	if stored != nil && stored.Token == o.Token {
		_ = a.deps.Store.Tx(ctx, "orphan-done", func(tx store.Tx) error { return tx.DeleteOrphan(o.ID) })
		return
	}
	err := a.llm.DeleteKey(ctx, o.Token)
	if !gone(err) {
		return
	}
	existed := err == nil
	terr := a.deps.Store.Tx(ctx, "orphan-done", func(tx store.Tx) error {
		if err := tx.DeleteOrphan(o.ID); err != nil {
			return err
		}
		if !existed {
			return nil
		}
		return a.emit(tx, o.TeamID, "orphan-key-revoked", map[string]any{"task_id": o.TaskID, "attempt": o.Attempt, "claim_seq": o.ClaimSeq})
	})
	if terr != nil {
		a.log.Warn("orphan revocation not recorded", "task_id", o.TaskID, "error", terr)
	}
}

func (a *App) revoke(ctx context.Context, k store.TaskKey) {
	if !gone(a.llm.DeleteKey(ctx, k.Token)) {
		return
	}
	now := a.now()
	err := a.deps.Store.Tx(ctx, "task-key-revoked", func(tx store.Tx) error {
		cur, err := tx.TaskKey(k.ClaimKey)
		if err != nil {
			return err
		}
		if cur.RevokedAt != nil {
			return nil
		}
		cur.RevokedAt = &now
		if err := tx.UpdateTaskKey(*cur); err != nil {
			return err
		}
		cause := "released"
		if cur.RevokeCause != nil {
			cause = *cur.RevokeCause
		}
		return a.emit(tx, k.TeamID, "task-key-revoked", map[string]any{"task_id": k.TaskID, "attempt": k.Attempt, "claim_seq": k.ClaimSeq, "cause": cause})
	})
	if err != nil {
		a.log.Warn("revocation not recorded", "task_id", k.TaskID, "error", err)
	}
}

// RefreshAvailability refreshes the model availability cache from LiteLLM.
func (a *App) RefreshAvailability(ctx context.Context) error {
	// The cache rows are read before LiteLLM: a row a pause, resume or other
	// writer changed after this read keeps the newer value.
	var models []store.Model
	if err := a.deps.Store.Tx(ctx, "availability-read", func(tx store.Tx) (err error) {
		models, err = tx.Models()
		return err
	}); err != nil {
		return err
	}
	recs, err := a.llm.ListModels(ctx)
	if err != nil {
		return err
	}
	byID := map[string]litellm.Model{}
	for _, r := range recs {
		byID[r.ID] = r
	}
	for _, m := range models {
		r, ok := byID[m.LiteLLMModelID]
		next := availabilityOf(m, r, ok)
		if next == m.Availability {
			continue
		}
		err := a.deps.Store.Tx(ctx, "availability-refresh", func(tx store.Tx) error {
			cur, err := tx.ModelByKey(m.Key, true)
			if err != nil {
				return err
			}
			if cur.Availability != m.Availability || !cur.UpdatedAt.Equal(m.UpdatedAt) || !sameTime(cur.ArchivedAt, m.ArchivedAt) {
				return nil // changed after the LiteLLM read
			}
			if cur.Availability == next || cur.ArchivedAt != nil && next != store.AvailabilityArchived && next != store.AvailabilityUnavailable {
				return nil
			}
			prev := cur.Availability
			cur.Availability = next
			if err := tx.UpdateModel(cur); err != nil {
				return err
			}
			team := int64(0)
			if cur.CreatorTeamID != nil {
				team = *cur.CreatorTeamID
			} else if owners, err := tx.Owners(cur.Key); err == nil && len(owners) > 0 {
				team = owners[0]
			}
			if team == 0 {
				return nil
			}
			return a.emit(tx, team, "model-availability-changed", map[string]any{"model_id": cur.ModelID, "model_alias": cur.Alias,
				"availability": next, "previous_availability": prev})
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func sameTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(*b)
}
