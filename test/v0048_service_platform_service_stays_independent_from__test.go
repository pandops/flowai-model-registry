package test

import (
	"testing"
)

// service-platform: independent from execution and identity. "Image
// equality across teams" is not applicable (model-registry holds no images).
func TestV0048ServicePlatformStaysIndependentFromExecution(t *testing.T) {
	t.Run("Executor capacity is not decided centrally", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		h.registerExecutor(7, 3)
		h.setFlow(3, 21, "Main", []string{"3/m1"}, nil, nil)
		h.applyFlowTeam(3, 21, 1, nil)
		for task := int64(2000); task < 2030; task++ {
			k := claim(3, task, 1, 1)
			h.setClaim(k, 7, 21)
			h.mustCheckOK(7, k, "3/m1")
		}
	})

	t.Run("Flow cannot be created from a Source path", func(t *testing.T) {
		h := newHarness(t)
		v := h.service("validate-models", "3", map[string]any{"team_id": 3, "aliases": []string{"3/ghost"}}).json(t)
		if e := v["aliases"].([]any)[0].(map[string]any); e["owned"] != false || e["selectable"] != false {
			t.Fatalf("unknown alias %v", e)
		}
		h.service("flows-status", "3", map[string]any{"team_id": 3, "flows": []any{map[string]any{"flow_id": 99, "aliases": []string{"3/ghost"}}}})
		if len(h.store.Models()) != 0 || len(h.llm.Models()) != 0 || len(h.llm.Teams()) != 0 || len(h.store.FlowTeamRows()) != 0 {
			t.Fatal("a read created a model or Flow team")
		}
	})
}
