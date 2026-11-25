package test

import (
	"errors"
	"testing"
	"time"

	"github.com/pandops/flowai-model-registry/internal/mocks"
)

// Requirement: model-registry enforces Flow limits through LiteLLM teams.
func TestV0048ModelRegistryEnforcesFlowLimits(t *testing.T) {
	t.Run("Weekly budget is exhausted and resets", func(t *testing.T) {
		h := newHarness(t) // Wednesday
		h.createModel(3, "gpt-4o", nil)
		a, b, c := claim(3, 1010, 1, 1), claim(3, 1011, 1, 1), claim(3, 1012, 1, 1)
		h.readyClaim(7, a, 21, []string{"3/gpt-4o"}, nil, flowLimit("10", "week"))
		h.setClaim(b, 7, 21)
		h.setClaim(c, 7, 21)
		h.mustCheckOK(7, a, "3/gpt-4o")
		h.mustCheckOK(7, b, "3/gpt-4o")
		ka, kb := h.issueKey(7, a), h.issueKey(7, b)
		ft := h.flowTeam(3, 21)
		monday := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
		if ft.BudgetDuration != "7d" || ft.BudgetResetAt == nil || !ft.BudgetResetAt.Equal(monday) || len(ft.Metadata) == 0 {
			t.Fatalf("LiteLLM team %+v", ft)
		}
		if err := h.llm.Use(ka, "3/gpt-4o", 10); err != nil {
			t.Fatal(err)
		}
		for _, k := range []string{ka, kb} {
			if err := h.llm.Use(k, "3/gpt-4o", 0.01); !errors.Is(err, mocks.ErrBudgetExceeded) {
				t.Fatalf("call after the Flow budget: %v", err)
			}
		}
		mustTemporary(t, h.check(7, c, nil), "budget_exhausted")
		h.setNow(monday.Add(time.Minute))
		h.llm.ResetTeamSpend(ft.ID, monday.Add(7*24*time.Hour))
		h.mustCheckOK(7, c, "3/gpt-4o")
	})

	t.Run("Operator raises an exhausted budget", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "gpt-4o", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/gpt-4o"}, nil, flowLimit("10", "week"))
		h.llm.SetTeamSpend(h.flowTeam(3, 21).ID, 10)
		mustTemporary(t, h.check(7, k, nil), "budget_exhausted")
		if r := h.applyFlowTeam(3, 21, 2, flowLimit("20", "week")); r.status != 200 {
			t.Fatalf("apply: %d %s", r.status, r.body)
		}
		if ft := h.flowTeam(3, 21); ft.MaxBudget == nil || *ft.MaxBudget != 20 || ft.Spend != 10 {
			t.Fatalf("team %+v", ft)
		}
		h.mustCheckOK(7, k, "3/gpt-4o")
	})

	t.Run("Flow limit apply while LiteLLM is down", func(t *testing.T) {
		h := newHarness(t)
		h.setFlow(3, 21, "Main", nil, nil, flowLimit("10", "week"))
		h.llm.SetDown(true)
		r := h.applyFlowTeam(3, 21, 1, flowLimit("10", "week"))
		if r.status != 503 || r.code(t) != "model_gateway_unavailable" {
			t.Fatalf("apply while down: %d %s", r.status, r.body)
		}
		if rows := h.store.FlowTeamRows(); len(rows) != 0 {
			t.Fatalf("recorded an applied limit: %+v", rows)
		}
		h.llm.SetDown(false)
		h.applyFlowTeam(3, 21, 1, flowLimit("10", "week"))
		writes := h.managementWrites()
		if r := h.applyFlowTeam(3, 21, 1, flowLimit("10", "week")); r.status != 200 || h.managementWrites() != writes {
			t.Fatalf("repeat of the same revision: %d, writes %d -> %d", r.status, writes, h.managementWrites())
		}
	})

	// Deviation D17, owner decision 2026-11-09: flow-registry removes the
	// LiteLLM team of a Flow create whose commit failed.
	t.Run("Orphan Flow team of a failed create is removed", func(t *testing.T) {
		h := newHarness(t)
		if r := h.applyFlowTeam(3, 22, 1, flowLimit("10", "week")); r.status != 200 {
			t.Fatalf("apply: %d %s", r.status, r.body)
		}
		h.applyFlowTeam(3, 23, 1, nil) // another Flow's team stays
		h.llm.SetDown(true)
		if r := h.removeFlowTeam(3, 22); r.status != 503 || r.code(t) != "model_gateway_unavailable" {
			t.Fatalf("remove while LiteLLM is down: %d %s", r.status, r.body)
		}
		if _, ok := h.llm.TeamFor(3, 22); !ok || len(h.store.FlowTeamRows()) != 2 {
			t.Fatalf("a failed remove changed the team or its row: %+v", h.store.FlowTeamRows())
		}
		h.llm.SetDown(false)
		before := len(h.llm.CallsTo("POST /team/delete"))
		if r := h.removeFlowTeam(3, 22); r.status != 200 {
			t.Fatalf("remove: %d %s", r.status, r.body)
		}
		if n := len(h.llm.CallsTo("POST /team/delete")) - before; n != 1 {
			t.Fatalf("LiteLLM team deletes = %d, want 1", n)
		}
		if _, ok := h.llm.TeamFor(3, 22); ok {
			t.Fatalf("LiteLLM team of the orphan Flow still exists")
		}
		if _, ok := h.llm.TeamFor(3, 23); !ok {
			t.Fatalf("remove deleted another Flow's LiteLLM team")
		}
		if rows := h.store.FlowTeamRows(); len(rows) != 1 || rows[0].FlowID != 23 {
			t.Fatalf("Flow team rows after remove = %+v, want only Flow 23", rows)
		}
		for _, o := range h.store.SpendRows() {
			if o.FlowID == 22 {
				t.Fatalf("spend observation of the removed team stays: %+v", o)
			}
		}
		// Already absent is success, without another LiteLLM write.
		writes := h.managementWrites()
		if r := h.removeFlowTeam(3, 22); r.status != 200 || h.managementWrites() != writes {
			t.Fatalf("repeat remove: %d, writes %d -> %d", r.status, writes, h.managementWrites())
		}
		// A row whose LiteLLM team is already gone is removed as success.
		ft := h.flowTeam(3, 23)
		h.llm.DeleteTeamDirectly(ft.ID)
		if r := h.removeFlowTeam(3, 23); r.status != 200 || len(h.store.FlowTeamRows()) != 0 {
			t.Fatalf("remove of a team LiteLLM no longer has: %d %s rows %+v", r.status, r.body, h.store.FlowTeamRows())
		}
	})
}
