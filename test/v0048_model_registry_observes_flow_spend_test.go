package test

import (
	"strconv"
	"testing"
)

// Requirement: model-registry observes Flow spend.
func TestV0048ModelRegistryObservesFlowSpend(t *testing.T) {
	status := func(h *harness) map[string]any {
		r := h.service("flows-status", "3", map[string]any{"team_id": 3, "flows": []any{map[string]any{"flow_id": 21, "aliases": []string{"3/gpt-4o"}, "flow_limit": flowLimit("50", "week")}}})
		return r.json(h.t)["flows"].([]any)[0].(map[string]any)
	}

	t.Run("Settings list shows cached usage", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "gpt-4o", nil)
		h.setFlow(3, 21, "Main", []string{"3/gpt-4o"}, nil, flowLimit("50", "week"))
		h.applyFlowTeam(3, 21, 1, flowLimit("50", "week"))
		h.llm.SetTeamSpend(h.flowTeam(3, 21).ID, 12.40)
		if err := h.app.RefreshSpend(h.ctx); err != nil {
			t.Fatal(err)
		}
		calls := len(h.llm.Calls())
		f := status(h)
		u, _ := f["usage"].(map[string]any)
		spend, _ := strconv.ParseFloat(u["spend_usd"].(string), 64)
		if spend != 12.40 || u["budget_reset_at"] != "2026-10-19T00:00:00.000Z" || u["budget_period"] != "week" || u["max_budget_usd"] != "50" && u["max_budget_usd"] != "50.00" {
			t.Fatalf("usage %v", u)
		}
		if len(h.llm.Calls()) != calls {
			t.Fatal("flows-status called LiteLLM")
		}
		if m := f["models"].([]any)[0].(map[string]any); m["model_alias"] != "3/gpt-4o" || m["availability"] != "active" {
			t.Fatalf("models %v", m)
		}
		if len(h.events("spend-observed")) == 0 {
			t.Fatal("no spend-observed event")
		}
	})
}
