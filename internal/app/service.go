package app

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"time"

	"github.com/pandops/flowai-model-registry/internal/broker"
	"github.com/pandops/flowai-model-registry/internal/litellm"
	"github.com/pandops/flowai-model-registry/internal/store"
)

// aliasState is a model's standing for one team.
type aliasState struct {
	owned      bool
	model      store.Model
	op         *store.Operation
	selectable bool
}

func aliasFor(tx store.Tx, team int64, alias string) (aliasState, error) {
	m, err := tx.ModelByAlias(alias, false)
	if errors.Is(err, store.ErrNotFound) {
		return aliasState{}, nil
	}
	if err != nil {
		return aliasState{}, err
	}
	owners, err := tx.Owners(m.Key)
	if err != nil {
		return aliasState{}, err
	}
	if !slices.Contains(owners, team) {
		return aliasState{}, nil
	}
	op, err := tx.Operation(m.Key)
	if err != nil {
		return aliasState{}, err
	}
	return aliasState{owned: true, model: *m, op: op, selectable: m.ArchivedAt == nil && op == nil}, nil
}

func (a *App) validateModels(ctx context.Context, team int64, raw json.RawMessage, rid *int64) broker.Reply {
	var body struct {
		TeamID  json.RawMessage `json:"team_id"`
		Aliases []string        `json:"aliases"`
	}
	if f, err := strictBody(raw); err != nil || !onlyKeys(f, "team_id", "aliases") || json.Unmarshal(raw, &body) != nil || len(body.Aliases) > 20 {
		return errReply(400, "invalid_request", rid)
	}
	out := []any{}
	err := a.deps.Store.Tx(ctx, "validate-models", func(tx store.Tx) error {
		for _, alias := range body.Aliases {
			st, err := aliasFor(tx, team, alias)
			if err != nil {
				return err
			}
			entry := map[string]any{"alias": alias, "owned": st.owned, "availability": "no_access", "selectable": st.selectable}
			if st.owned {
				entry["availability"] = st.model.Availability
				if st.model.ArchivedAt != nil {
					entry["availability"] = store.AvailabilityArchived
				}
			}
			out = append(out, entry)
		}
		return nil
	})
	if err != nil {
		return errReply(503, "service_unavailable", rid)
	}
	return jsonReply(200, map[string]any{"aliases": out})
}

// limitFields maps a Flow limit to LiteLLM team fields; absent fields unset.
func limitFields(l *store.FlowLimit) map[string]any {
	f := map[string]any{"max_budget": nil, "budget_duration": nil, "rpm_limit": nil, "tpm_limit": nil}
	if l == nil {
		return f
	}
	if l.MaxBudgetUSD != nil {
		if n, ok := decimalNumber(*l.MaxBudgetUSD); ok {
			f["max_budget"] = n
		}
	}
	if l.BudgetPeriod != nil {
		f["budget_duration"] = map[string]string{"day": "1d", "week": "7d", "month": "30d"}[*l.BudgetPeriod]
	}
	if l.RPM != nil {
		f["rpm_limit"] = *l.RPM
	}
	if l.TPM != nil {
		f["tpm_limit"] = *l.TPM
	}
	return f
}

// nextReset is the start of the next UTC calendar period after now.
func nextReset(period string, now time.Time) time.Time {
	now = now.UTC()
	day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	switch period {
	case "week":
		days := (8 - int(now.Weekday())) % 7
		if days == 0 {
			days = 7
		}
		return day.AddDate(0, 0, days)
	case "month":
		return time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, time.UTC)
	}
	return day.AddDate(0, 0, 1)
}

func sameLimit(a, b *store.FlowLimit) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

func parseFlowLimit(raw json.RawMessage) (*store.FlowLimit, bool) {
	if len(raw) == 0 || isNull(raw) {
		return nil, true
	}
	f, err := strictBody(raw)
	if err != nil || len(f) == 0 || !onlyKeys(f, "max_budget_usd", "budget_period", "rpm", "tpm") {
		return nil, false
	}
	var l store.FlowLimit
	if json.Unmarshal(raw, &l) != nil {
		return nil, false
	}
	if (l.MaxBudgetUSD == nil) != (l.BudgetPeriod == nil) {
		return nil, false
	}
	if l.MaxBudgetUSD != nil {
		if _, ok := decimalNumber(*l.MaxBudgetUSD); !ok || !usdPattern.MatchString(*l.MaxBudgetUSD) {
			return nil, false
		}
	}
	if l.BudgetPeriod != nil && *l.BudgetPeriod != "day" && *l.BudgetPeriod != "week" && *l.BudgetPeriod != "month" {
		return nil, false
	}
	if l.RPM != nil && *l.RPM < 1 || l.TPM != nil && *l.TPM < 1 {
		return nil, false
	}
	return &l, true
}

func (a *App) applyFlowTeam(ctx context.Context, team int64, raw json.RawMessage, rid *int64) broker.Reply {
	f, err := strictBody(raw)
	if err != nil || !onlyKeys(f, "team_id", "flow_id", "flow_revision", "flow_limit") {
		return errReply(400, "invalid_request", rid)
	}
	flow, ok1 := jsonID(f["flow_id"])
	rev, ok2 := jsonID(f["flow_revision"])
	limit, ok3 := parseFlowLimit(f["flow_limit"])
	if _, present := f["flow_limit"]; !ok1 || !ok2 || !ok3 || !present {
		return errReply(400, "invalid_request", rid)
	}
	ft, err := a.ensureFlowTeam(ctx, team, flow, rev, limit)
	if err != nil {
		if errors.Is(err, litellm.ErrUnavailable) {
			return errReply(503, "model_gateway_unavailable", rid)
		}
		return errReply(503, "service_unavailable", rid)
	}
	return jsonReply(200, map[string]any{"team_id": team, "flow_id": flow, "flow_revision": ft.FlowRevision, "budget_reset_at": tsPtr(ft.BudgetResetAt)})
}

// ensureFlowTeam creates or updates the Flow's LiteLLM team so it holds
// limit; the same or an older revision with the same limit changes nothing.
func (a *App) ensureFlowTeam(ctx context.Context, team, flow, rev int64, limit *store.FlowLimit) (store.FlowTeam, error) {
	var cur *store.FlowTeam
	if err := a.deps.Store.Tx(ctx, "flow-team-read", func(tx store.Tx) (err error) {
		cur, err = tx.FlowTeam(team, flow, false)
		return err
	}); err != nil {
		return store.FlowTeam{}, err
	}
	if cur != nil && (rev < cur.FlowRevision || rev == cur.FlowRevision && sameLimit(cur.Limit, limit)) {
		return *cur, nil
	}
	fields := limitFields(limit)
	var reset *time.Time
	periodOf := func(l *store.FlowLimit) string {
		if l == nil || l.BudgetPeriod == nil {
			return ""
		}
		return *l.BudgetPeriod
	}
	newPeriod := periodOf(limit)
	switch {
	case newPeriod == "":
		fields["budget_reset_at"] = nil
	case cur == nil || periodOf(cur.Limit) != newPeriod || cur.BudgetResetAt == nil:
		at := nextReset(newPeriod, a.now())
		reset = &at
		fields["budget_reset_at"] = ts(at)
	default:
		reset = cur.BudgetResetAt
	}
	next := store.FlowTeam{TeamID: team, FlowID: flow, FlowRevision: rev, Limit: limit, BudgetResetAt: reset, UpdatedAt: a.now()}
	if cur == nil {
		fields["team_alias"] = "flowai-" + strconv.FormatInt(team, 10) + "-" + strconv.FormatInt(flow, 10)
		fields["metadata"] = map[string]any{"flowai_team_id": team, "flowai_flow_id": flow}
		id, err := a.llm.CreateTeam(ctx, fields)
		if err != nil {
			return store.FlowTeam{}, litellm.ErrUnavailable
		}
		next.LiteLLMTeamID = id
	} else {
		next.LiteLLMTeamID = cur.LiteLLMTeamID
		if err := a.llm.UpdateTeam(ctx, cur.LiteLLMTeamID, fields); err != nil {
			return store.FlowTeam{}, litellm.ErrUnavailable
		}
	}
	if err := a.deps.Store.Tx(ctx, "flow-team-apply", func(tx store.Tx) error { return tx.PutFlowTeam(next) }); err != nil {
		return store.FlowTeam{}, err
	}
	if limit != nil && limit.MaxBudgetUSD != nil {
		a.observeSpend(ctx, next)
	}
	return next, nil
}

// removeFlowTeam deletes the LiteLLM team of a Flow whose create failed in
// flow-registry (deviation D17). A Flow without a team, or whose team
// LiteLLM no longer has, is already removed.
func (a *App) removeFlowTeam(ctx context.Context, team int64, raw json.RawMessage, rid *int64) broker.Reply {
	f, err := strictBody(raw)
	if err != nil || !onlyKeys(f, "team_id", "flow_id") {
		return errReply(400, "invalid_request", rid)
	}
	flow, ok := jsonID(f["flow_id"])
	if !ok {
		return errReply(400, "invalid_request", rid)
	}
	var cur *store.FlowTeam
	if err := a.deps.Store.Tx(ctx, "flow-team-read", func(tx store.Tx) (err error) {
		cur, err = tx.FlowTeam(team, flow, false)
		return err
	}); err != nil {
		return errReply(503, "service_unavailable", rid)
	}
	if cur != nil {
		if err := a.llm.DeleteTeam(ctx, cur.LiteLLMTeamID); err != nil && !errors.Is(err, litellm.ErrNotFound) {
			return errReply(503, "model_gateway_unavailable", rid)
		}
		if err := a.deps.Store.Tx(ctx, "flow-team-remove", func(tx store.Tx) error { return tx.DeleteFlowTeam(team, flow) }); err != nil {
			return errReply(503, "service_unavailable", rid)
		}
	}
	return jsonReply(200, map[string]any{"team_id": team, "flow_id": flow})
}

// observeSpend reads the team spend from LiteLLM and stores the observation;
// it returns the spend, or ok=false when the read failed.
func (a *App) observeSpend(ctx context.Context, ft store.FlowTeam) (json.Number, bool) {
	t, err := a.llm.TeamInfo(ctx, ft.LiteLLMTeamID)
	now := a.now()
	obs := store.SpendObservation{TeamID: ft.TeamID, FlowID: ft.FlowID, ObservedAt: now}
	if err == nil {
		spend, ok := usdOf(t.Spend)
		if !ok {
			err = litellm.ErrUnavailable
		} else {
			obs.Succeeded, obs.SpendUSD, obs.BudgetResetAt = true, &spend, t.BudgetResetAt
			obs.LastSuccessSpendUSD, obs.LastSuccessResetAt, obs.LastSuccessAt = &spend, t.BudgetResetAt, &now
		}
	}
	terr := a.deps.Store.Tx(ctx, "spend-observation", func(tx store.Tx) error {
		prev, err := tx.SpendObservation(ft.TeamID, ft.FlowID)
		if err != nil {
			return err
		}
		if !obs.Succeeded && prev != nil {
			obs.LastSuccessSpendUSD, obs.LastSuccessResetAt, obs.LastSuccessAt = prev.LastSuccessSpendUSD, prev.LastSuccessResetAt, prev.LastSuccessAt
		}
		if err := tx.PutSpendObservation(obs); err != nil {
			return err
		}
		if prev != nil && prev.Succeeded == obs.Succeeded && strPtrEq(prev.SpendUSD, obs.SpendUSD) && timePtrEq(prev.BudgetResetAt, obs.BudgetResetAt) {
			return nil
		}
		var spend any
		if obs.SpendUSD != nil {
			spend = *obs.SpendUSD
		}
		return a.emit(tx, ft.TeamID, "spend-observed", map[string]any{"flow_id": ft.FlowID, "succeeded": obs.Succeeded,
			"spend_usd": spend, "budget_reset_at": tsPtr(obs.BudgetResetAt), "observed_at": ts(now)})
	})
	if terr != nil {
		a.log.Warn("spend observation not stored", "flow_id", ft.FlowID, "error", terr)
	}
	if err != nil {
		return "", false
	}
	return t.Spend, true
}

func strPtrEq(a, b *string) bool { return a == nil && b == nil || a != nil && b != nil && *a == *b }

func timePtrEq(a, b *time.Time) bool {
	return a == nil && b == nil || a != nil && b != nil && a.Equal(*b)
}

// RefreshSpend refreshes the spend observations of Flows with a budget.
func (a *App) RefreshSpend(ctx context.Context) error {
	var teams []store.FlowTeam
	if err := a.deps.Store.Tx(ctx, "spend-refresh", func(tx store.Tx) (err error) {
		teams, err = tx.FlowTeams()
		return err
	}); err != nil {
		return err
	}
	for _, ft := range teams {
		if ft.Limit != nil && ft.Limit.MaxBudgetUSD != nil {
			a.observeSpend(ctx, ft)
		}
	}
	return nil
}

func (a *App) flowsStatus(ctx context.Context, team int64, raw json.RawMessage, rid *int64) broker.Reply {
	var body struct {
		Flows []struct {
			FlowID  json.RawMessage `json:"flow_id"`
			Aliases []string        `json:"aliases"`
		} `json:"flows"`
	}
	if f, err := strictBody(raw); err != nil || !onlyKeys(f, "team_id", "flows") || json.Unmarshal(raw, &body) != nil || len(body.Flows) > 200 {
		return errReply(400, "invalid_request", rid)
	}
	out := []any{}
	err := a.deps.Store.Tx(ctx, "flows-status", func(tx store.Tx) error {
		for _, f := range body.Flows {
			flow, ok := jsonID(f.FlowID)
			if !ok {
				return errInvalid
			}
			models := []any{}
			for _, alias := range f.Aliases {
				st, err := aliasFor(tx, team, alias)
				if err != nil {
					return err
				}
				av := "no_access"
				if st.owned {
					av = st.model.Availability
					if av == store.AvailabilityArchived || st.model.ArchivedAt != nil {
						av = store.AvailabilityUnavailable
					}
				}
				models = append(models, map[string]any{"model_alias": alias, "availability": av})
			}
			var usage any
			ft, err := tx.FlowTeam(team, flow, false)
			if err != nil {
				return err
			}
			if ft != nil && ft.Limit != nil && ft.Limit.MaxBudgetUSD != nil {
				obs, err := tx.SpendObservation(team, flow)
				if err != nil {
					return err
				}
				if obs != nil && obs.Succeeded && obs.SpendUSD != nil {
					usage = map[string]any{"spend_usd": *obs.SpendUSD, "budget_reset_at": tsPtr(obs.BudgetResetAt),
						"max_budget_usd": *ft.Limit.MaxBudgetUSD, "budget_period": *ft.Limit.BudgetPeriod, "observed_at": ts(obs.ObservedAt)}
				}
			}
			out = append(out, map[string]any{"flow_id": flow, "models": models, "usage": usage})
		}
		return nil
	})
	if errors.Is(err, errInvalid) {
		return errReply(400, "invalid_request", rid)
	}
	if err != nil {
		return errReply(503, "service_unavailable", rid)
	}
	return jsonReply(200, map[string]any{"flows": out})
}
