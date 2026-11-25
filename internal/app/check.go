package app

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/pandops/flowai-model-registry/internal/broker"
	"github.com/pandops/flowai-model-registry/internal/store"
)

func temporary(reason string) broker.Reply {
	return jsonReply(200, map[string]any{"result": "temporary", "reason": reason})
}

func checkOK(alias string) broker.Reply {
	return jsonReply(200, map[string]any{"result": "ok", "model_alias": alias})
}

var errClaimNotCurrent = errors.New("claim not current")

// executorTeam is the team of a projected, not removed Executor.
func (a *App) executorTeam(ctx context.Context, executor int64) (int64, error) {
	var team int64
	err := a.deps.Store.Tx(ctx, "executor-read", func(tx store.Tx) error {
		e, err := tx.Executor(executor)
		if err != nil {
			return err
		}
		if e.Removed {
			return store.ErrNotFound
		}
		team = e.TeamID
		return nil
	})
	return team, err
}

// claimFlow asks task-manager whether the claim is current for executor and
// returns the task's Flow.
func (a *App) claimFlow(ctx context.Context, k store.ClaimKey, executor int64) (int64, error) {
	r, err := a.call(ctx, "task-manager.service.claim-current.pub", k.TeamID, map[string]any{
		"team_id": k.TeamID, "task_id": k.TaskID, "attempt": k.Attempt, "claim_seq": k.ClaimSeq, "executor_id": executor,
	})
	if err != nil || r.Status != 200 {
		return 0, errStore
	}
	var out struct {
		Current bool  `json:"current"`
		TeamID  int64 `json:"team_id"`
		FlowID  int64 `json:"flow_id"`
	}
	if json.Unmarshal(r.Body, &out) != nil {
		return 0, errStore
	}
	if !out.Current || out.TeamID != k.TeamID || out.FlowID < 1 {
		return 0, errClaimNotCurrent
	}
	return out.FlowID, nil
}

type llmSettings struct {
	Aliases   []string            `json:"model_aliases"`
	PerTask   *store.PerTaskLimit `json:"per_task_limit"`
	FlowLimit json.RawMessage     `json:"flow_limit"`
	Revision  int64               `json:"revision"`
}

func (a *App) check(ctx context.Context, m broker.Message) broker.Reply {
	parts := strings.Split(m.Subject, ".")
	if len(parts) != 5 {
		return errReply(400, "invalid_request", nil)
	}
	executor, ok := parseID(parts[2])
	req, err := decodeRequest(m.Data)
	if !ok || err != nil || len(req.Params) > 0 {
		return errReply(400, "invalid_request", nil)
	}
	f, err := strictBody(req.Body)
	if err != nil || !onlyKeys(f, "task_id", "attempt", "claim_seq") {
		return errReply(400, "invalid_request", nil)
	}
	var k store.ClaimKey
	var ok1, ok2, ok3 bool
	k.TaskID, ok1 = jsonID(f["task_id"])
	k.Attempt, ok2 = jsonID(f["attempt"])
	k.ClaimSeq, ok3 = jsonID(f["claim_seq"])
	if !ok1 || !ok2 || !ok3 {
		return errReply(400, "invalid_request", nil)
	}
	if k.TeamID, err = a.executorTeam(ctx, executor); err != nil {
		return errReply(503, "service_unavailable", nil)
	}
	flow, err := a.claimFlow(ctx, k, executor)
	if errors.Is(err, errClaimNotCurrent) {
		return errReply(404, "claim_not_current", nil)
	}
	if err != nil {
		return errReply(503, "service_unavailable", nil)
	}
	var existing *store.Snapshot
	if err := a.deps.Store.Tx(ctx, "snapshot-read", func(tx store.Tx) error {
		s, err := tx.Snapshot(k, false)
		if err == nil {
			existing = s
		} else if !errors.Is(err, store.ErrNotFound) {
			return err
		}
		return nil
	}); err != nil {
		return errReply(503, "service_unavailable", nil)
	}
	if existing != nil {
		return checkOK(existing.ModelAlias)
	}
	r, err := a.call(ctx, "flow-registry.service.llm-settings.pub", k.TeamID, map[string]any{"team_id": k.TeamID, "flow_id": flow})
	if err != nil || r.Status >= 500 {
		return errReply(503, "service_unavailable", nil)
	}
	var settings llmSettings
	if r.Status == 404 {
		return temporary("no_active_model")
	}
	if r.Status != 200 || json.Unmarshal(r.Body, &settings) != nil {
		return errReply(503, "service_unavailable", nil)
	}
	limit, ok := parseFlowLimit(settings.FlowLimit)
	if !ok {
		return errReply(503, "service_unavailable", nil)
	}

	if reason, err := a.pick(ctx, k.TeamID, settings.Aliases, nil); err != nil {
		return errReply(503, "service_unavailable", nil)
	} else if reason != "" {
		return temporary(reason)
	}

	var ft *store.FlowTeam
	if err := a.deps.Store.Tx(ctx, "flow-team-read", func(tx store.Tx) (err error) {
		ft, err = tx.FlowTeam(k.TeamID, flow, false)
		return err
	}); err != nil {
		return errReply(503, "service_unavailable", nil)
	}
	if ft == nil {
		// The Flow's LiteLLM team does not exist yet: create it from the
		// current settings, as apply-flow-team would.
		rev := settings.Revision
		if rev < 1 {
			rev = 1
		}
		created, err := a.ensureFlowTeam(ctx, k.TeamID, flow, rev, limit)
		if err != nil {
			return errReply(503, "service_unavailable", nil)
		}
		ft = &created
	}
	if ft.Limit != nil && ft.Limit.MaxBudgetUSD != nil {
		spend, ok := a.observeSpend(ctx, *ft)
		if !ok {
			return temporary("spend_observation_unavailable")
		}
		if atLeast(spend, *ft.Limit.MaxBudgetUSD) {
			return temporary("budget_exhausted")
		}
	}

	var pinned string
	var reason string
	err = a.deps.Store.Tx(ctx, store.TxSnapshot, func(tx store.Tx) error {
		if s, err := tx.Snapshot(k, true); err == nil {
			pinned = s.ModelAlias
			return nil
		} else if !errors.Is(err, store.ErrNotFound) {
			return err
		}
		var chosen *store.Model
		reason, chosen = pickIn(tx, k.TeamID, settings.Aliases, true)
		if chosen == nil {
			return nil
		}
		pinned = chosen.Alias
		return tx.InsertSnapshot(store.Snapshot{ClaimKey: k, ExecutorID: executor, FlowID: flow, ModelAlias: chosen.Alias,
			ModelKey: chosen.Key, PerTaskLimit: settings.PerTask, CreatedAt: a.now()})
	})
	if err != nil {
		return errReply(503, "service_unavailable", nil)
	}
	if pinned == "" {
		if reason == "" {
			reason = "no_active_model"
		}
		return temporary(reason)
	}
	return checkOK(pinned)
}

// pick evaluates aliases read-only; it returns the temporary reason, or ""
// when one alias is usable.
func (a *App) pick(ctx context.Context, team int64, aliases []string, _ *store.Model) (string, error) {
	var reason string
	err := a.deps.Store.Tx(ctx, "check-models", func(tx store.Tx) error {
		reason, _ = pickIn(tx, team, aliases, false)
		return nil
	})
	return reason, err
}

// pickIn returns the first usable alias in list order, or the temporary
// reason when none is usable. lock takes the chosen model's row lock.
func pickIn(tx store.Tx, team int64, aliases []string, lock bool) (string, *store.Model) {
	allPaused := len(aliases) > 0
	for _, alias := range aliases {
		st, err := aliasFor(tx, team, alias)
		if err != nil || !st.owned {
			allPaused = false
			continue
		}
		m := st.model
		if lock {
			locked, err := tx.ModelByKey(m.Key, true)
			if err != nil {
				allPaused = false
				continue
			}
			m = *locked
		}
		if m.Availability != store.AvailabilityPaused {
			allPaused = false
		}
		if m.Availability == store.AvailabilityActive && m.ArchivedAt == nil && st.op == nil {
			return "", &m
		}
	}
	if allPaused {
		return "all_selected_models_paused", nil
	}
	return "no_active_model", nil
}
