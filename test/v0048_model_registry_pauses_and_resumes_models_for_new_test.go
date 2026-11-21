package test

import "testing"

// Requirement: model-registry pauses and resumes models for new claims.
func TestV0048ModelRegistryPausesAndResumesModels(t *testing.T) {
	t.Run("Operator pauses a model in use", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		running, next := claim(3, 1010, 1, 1), claim(3, 1011, 1, 1)
		h.readyClaim(7, running, 21, []string{"3/m1"}, nil, nil)
		h.mustCheckOK(7, running, "3/m1")
		key := h.issueKey(7, running)
		keys := len(h.llm.Keys())
		if r := h.setPaused(3, "m1", true); r.status != 200 || r.json(t)["paused"] != true {
			t.Fatalf("pause: %d %s", r.status, r.body)
		}
		if m := h.llmModel("3/m1"); m.ModelInfo["flowai_paused"] != true {
			t.Fatalf("LiteLLM flag %v", m.ModelInfo)
		}
		if err := h.llm.Use(key, "3/m1", 0.01); err != nil || len(h.llm.Keys()) != keys {
			t.Fatalf("running task after pause: %v", err)
		}
		h.setClaim(next, 7, 21)
		mustTemporary(t, h.check(7, next, nil), "all_selected_models_paused")
		h.setPaused(3, "m1", false)
		h.mustCheckOK(7, next, "3/m1")
	})

	t.Run("Listed model disappears from LiteLLM", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		h.createModel(3, "m2", nil)
		a, b := claim(3, 1010, 1, 1), claim(3, 1011, 1, 1)
		h.readyClaim(7, a, 21, []string{"3/m1", "3/m2"}, nil, nil)
		h.setClaim(b, 7, 21)
		h.llm.DeleteModelDirectly("3/m1")
		h.mustRefreshAvailability()
		st := h.service("flows-status", "3", map[string]any{"team_id": 3, "flows": []any{map[string]any{"flow_id": 21, "aliases": []string{"3/m1"}}}}).json(t)
		if av := st["flows"].([]any)[0].(map[string]any)["models"].([]any)[0].(map[string]any)["availability"]; av != "unavailable" {
			t.Fatalf("3/m1 availability %v, want unavailable", av)
		}
		if len(h.events("model-availability-changed")) == 0 {
			t.Fatal("no model-availability-changed event")
		}
		h.mustCheckOK(7, a, "3/m2")
		h.llm.DeleteModelDirectly("3/m2")
		h.mustRefreshAvailability()
		mustTemporary(t, h.check(7, b, nil), "no_active_model")
	})

	t.Run("Operator resumes a model", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/m1"}, nil, nil)
		h.setPaused(3, "m1", true)
		mustTemporary(t, h.check(7, k, nil), "all_selected_models_paused")
		if r := h.setPaused(3, "m1", false); r.status != 200 || r.json(t)["paused"] != false {
			t.Fatalf("resume: %d %s", r.status, r.body)
		}
		h.mustCheckOK(7, k, "3/m1") // no refresh ran in between
	})
}
