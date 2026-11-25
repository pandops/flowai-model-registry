package test

import (
	"strings"
	"testing"
)

// Requirement: model-registry checks model availability and budget after a
// claim.
func TestV0048ModelRegistryChecksModelAvailabilityAndBudget(t *testing.T) {
	noSnapshot := func(h *harness) {
		h.t.Helper()
		if s := h.store.Snapshots(); len(s) != 0 {
			h.t.Fatalf("stored snapshots %+v", s)
		}
	}

	t.Run("Pending task has no configured model", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, nil, nil, nil)
		mustTemporary(t, h.check(7, k, nil), "no_active_model")
		noSnapshot(h)
		h.setFlow(3, 21, "Main", []string{"3/m1"}, nil, nil)
		h.mustCheckOK(7, k, "3/m1")
	})

	t.Run("Every listed model is paused", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		h.createModel(3, "m2", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/m1", "3/m2"}, nil, nil)
		h.setPaused(3, "m1", true)
		h.setPaused(3, "m2", true)
		mustTemporary(t, h.check(7, k, nil), "all_selected_models_paused")
		noSnapshot(h)
		h.setPaused(3, "m2", false)
		h.mustCheckOK(7, k, "3/m2")
	})

	t.Run("Budget is exhausted between refreshes", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/m1"}, nil, flowLimit("10", "week"))
		ft := h.flowTeam(3, 21)
		h.llm.SetTeamSpend(ft.ID, 5)
		_ = h.app.RefreshSpend(h.ctx)
		h.llm.SetTeamSpend(ft.ID, 10)
		mustTemporary(t, h.check(7, k, nil), "budget_exhausted")
		noSnapshot(h)
		rows := h.store.SpendRows()
		if len(rows) != 1 || !rows[0].Succeeded || rows[0].SpendUSD == nil || !strings.HasPrefix(*rows[0].SpendUSD, "10") {
			t.Fatalf("spend observation %+v", rows)
		}
	})

	t.Run("Settings change after the snapshot", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		h.createModel(3, "m2", nil)
		a, b := claim(3, 1010, 1, 1), claim(3, 1011, 1, 1)
		h.readyClaim(7, a, 21, []string{"3/m1"}, map[string]any{"max_budget_usd": "1.00"}, nil)
		h.mustCheckOK(7, a, "3/m1")
		h.setFlow(3, 21, "Main", []string{"3/m2"}, map[string]any{"max_budget_usd": "2.00"}, nil)
		h.mustCheckOK(7, a, "3/m1")
		h.setClaim(b, 7, 21)
		h.mustCheckOK(7, b, "3/m2")
		h.issueKey(7, a)
		h.issueKey(7, b)
		budgets := map[string]float64{}
		for _, k := range h.llm.Keys() {
			budgets[k.Models[0]] = *k.MaxBudget
		}
		if budgets["3/m1"] != 1 || budgets["3/m2"] != 2 {
			t.Fatalf("per-task budgets %v", budgets)
		}
	})

	t.Run("First model is paused", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		h.createModel(3, "m2", nil)
		early, later := claim(3, 1010, 1, 1), claim(3, 1011, 1, 1)
		h.readyClaim(7, early, 21, []string{"3/m1", "3/m2"}, nil, nil)
		h.mustCheckOK(7, early, "3/m1")
		key := h.issueKey(7, early)
		h.setPaused(3, "m1", true)
		h.setClaim(later, 7, 21)
		h.mustCheckOK(7, later, "3/m2")
		if err := h.llm.Use(key, "3/m1", 0.01); err != nil {
			t.Fatalf("earlier snapshot after pause: %v", err)
		}
		h.mustCheckOK(7, early, "3/m1")
	})

	t.Run("Released claim is checked again", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		h.createModel(3, "m2", nil)
		first, second := claim(3, 1010, 1, 1), claim(3, 1010, 1, 2)
		h.readyClaim(7, first, 21, []string{"3/m1", "3/m2"}, nil, nil)
		h.mustCheckOK(7, first, "3/m1")
		h.endClaim(first, "claim_ended")
		h.executorEvent(7, "released", first, map[string]any{"reason": "pending_timeout"})
		h.setFlow(3, 21, "Main", []string{"3/m2", "3/m1"}, nil, nil)
		h.setClaim(second, 7, 21)
		h.mustCheckOK(7, second, "3/m2")
		if n := len(h.store.Snapshots()); n != 2 {
			t.Fatalf("%d snapshots, want one per claim", n)
		}
	})

	t.Run("Stale claim token", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/m1"}, nil, nil)
		h.endClaim(k, "claim_ended")
		r := h.check(7, k, nil)
		if r.status != 404 || r.code(t) != "claim_not_current" {
			t.Fatalf("check: %d %s", r.status, r.body)
		}
		noSnapshot(h)
	})

	t.Run("Check carries no claim token", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/m1"}, nil, nil)
		h.broker.ResetRecords()
		h.mustCheckOK(7, k, "3/m1")
		reqs := h.broker.RequestsTo("task-manager.service.claim-current.pub")
		if len(reqs) != 1 {
			t.Fatalf("%d claim-current requests", len(reqs))
		}
		b := decodeReq(reqs[0]).Params
		if intField(b, "team_id") != 3 || intField(b, "task_id") != 1010 || intField(b, "executor_id") != 7 || reqs[0].Header["team"] != "3" {
			t.Fatalf("claim-current %s %v", reqs[0].Data, reqs[0].Header)
		}
		h.assertNoSecret(h.token(k, 7))
		h.assertNoSecret(strings.SplitN(h.token(k, 7), ".", 2)[1]) // the signature
	})
}
