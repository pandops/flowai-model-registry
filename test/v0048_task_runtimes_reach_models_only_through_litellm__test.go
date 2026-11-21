package test

import (
	"errors"
	"testing"

	"github.com/pandops/flowai-model-registry/internal/mocks"
)

// litellm-gateway requirement: Task runtimes reach models only through
// LiteLLM with per-task keys (model-registry part).
func TestV0048TaskRuntimesReachModelsThroughLiteLLM(t *testing.T) {
	t.Run("Assigned Executor opens a task", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "gpt-4o", nil)
		h.createModel(3, "other", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/gpt-4o"}, map[string]any{"rpm": 60, "tpm": 1000}, nil)
		h.mustCheckOK(7, k, "3/gpt-4o")
		r := h.keyEdge("1010", h.token(k, 7))
		if r.status != 200 || r.body["OPENAI_BASE_URL"] != "http://litellm-data:4000" || r.body["OPENAI_MODEL"] != "3/gpt-4o" {
			t.Fatalf("key edge: %d %s", r.status, r.raw)
		}
		key := r.body["OPENAI_API_KEY"].(string)
		if err := h.llm.Use(key, "3/gpt-4o", 0.01); err != nil {
			t.Fatalf("pinned alias call: %v", err)
		}
		if err := h.llm.Use(key, "3/other", 0.01); !errors.Is(err, mocks.ErrRejected) {
			t.Fatalf("other alias call: %v", err)
		}
		var lk *mocks.LiteLLMKey
		for _, kk := range h.llm.Keys() {
			if kk.Token == mocks.TokenOf(key) {
				lk = &kk
			}
		}
		if lk == nil || len(lk.Models) != 1 || lk.Models[0] != "3/gpt-4o" || lk.TeamID != h.flowTeam(3, 21).ID ||
			lk.RPMLimit == nil || *lk.RPMLimit != 60 || lk.TPMLimit == nil || *lk.TPMLimit != 1000 {
			t.Fatalf("LiteLLM key %+v", lk)
		}
		for field, want := range map[string]int64{"team_id": 3, "task_id": 1010, "attempt": 1, "claim_seq": 1} {
			if !jsonInt(lk.Metadata[field], want) {
				t.Fatalf("key metadata %s = %v", field, lk.Metadata[field])
			}
		}
		h.assertNoSecret(key)
	})

	t.Run("Per-task key is misused", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "gpt-4o", nil)
		h.createModel(3, "other", nil)
		a, b := claim(3, 1010, 1, 1), claim(3, 1011, 1, 1)
		h.readyClaim(7, a, 21, []string{"3/gpt-4o"}, nil, nil)
		h.setClaim(b, 7, 21)
		h.mustCheckOK(7, a, "3/gpt-4o")
		h.mustCheckOK(7, b, "3/gpt-4o")
		ka, kb := h.issueKey(7, a), h.issueKey(7, b)
		if err := h.llm.Use(ka, "3/other", 0.01); !errors.Is(err, mocks.ErrRejected) {
			t.Fatalf("other alias: %v", err)
		}
		h.endClaim(a, "task_terminal")
		h.executorEvent(7, "finished", a, nil)
		h.endClaim(b, "claim_ended")
		h.executorEvent(7, "released", b, map[string]any{"reason": "pending_timeout"})
		h.mustReconcile()
		for name, key := range map[string]string{"finished": ka, "released": kb} {
			if err := h.llm.Use(key, "3/gpt-4o", 0.01); !errors.Is(err, mocks.ErrRejected) {
				t.Fatalf("key after %s: %v", name, err)
			}
		}
	})

	t.Run("Pinned alias is gone at key request", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "gpt-4o", nil)
		a := claim(3, 1010, 1, 1)
		h.readyClaim(7, a, 21, []string{"3/gpt-4o"}, nil, nil)
		h.mustCheckOK(7, a, "3/gpt-4o")
		h.llm.DeleteModelDirectly("3/gpt-4o")
		gone := h.keyEdge("1010", h.token(a, 7))
		if gone.status != 422 || gone.body["code"] != "configuration_error" || gone.body["failure_reason"] != "llm_configuration_incomplete" {
			t.Fatalf("alias gone: %d %s", gone.status, gone.raw)
		}
		if _, ok := gone.body["OPENAI_API_KEY"]; ok {
			t.Fatal("422 returned a key")
		}
		if n := len(h.llm.CallsTo("POST /key/generate")); n != 0 {
			t.Fatalf("%d keys were generated", n)
		}
	})

	t.Run("Per-task budget is reached", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "gpt-4o", nil)
		a, b := claim(3, 1010, 1, 1), claim(3, 1011, 1, 1)
		h.readyClaim(7, a, 21, []string{"3/gpt-4o"}, map[string]any{"max_budget_usd": "0.50"}, nil)
		h.setClaim(b, 7, 21)
		h.mustCheckOK(7, a, "3/gpt-4o")
		h.mustCheckOK(7, b, "3/gpt-4o")
		ka, kb := h.issueKey(7, a), h.issueKey(7, b)
		for _, kk := range h.llm.Keys() {
			if kk.MaxBudget == nil || *kk.MaxBudget != 0.5 {
				t.Fatalf("key max_budget = %v, want 0.5", kk.MaxBudget)
			}
		}
		if err := h.llm.Use(ka, "3/gpt-4o", 0.50); err != nil {
			t.Fatal(err)
		}
		if err := h.llm.Use(ka, "3/gpt-4o", 0.01); !errors.Is(err, mocks.ErrBudgetExceeded) {
			t.Fatalf("call over the per-task budget: %v", err)
		}
		if err := h.llm.Use(kb, "3/gpt-4o", 0.01); err != nil {
			t.Fatalf("another task of the Flow: %v", err)
		}
	})
}
