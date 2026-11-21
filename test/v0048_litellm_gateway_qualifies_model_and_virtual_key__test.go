package test

import (
	"errors"
	"testing"
	"time"

	"github.com/pandops/flowai-model-registry/internal/mocks"
)

// litellm-gateway requirement: LiteLLM gateway qualifies model and
// virtual-key operations (FlowAI adapter part, against the fake management
// API; the deployed route itself is exercised by E2E v0041.6).
func TestV0048LiteLLMGatewayQualifiesModelAndVirtualKeyOperations(t *testing.T) {
	t.Run("Model configuration crosses the deployed management route", func(t *testing.T) {
		h := newHarness(t)
		r := h.createModel(3, "gpt-4o", map[string]any{"api_key": "sk-provider-secret-6"})
		if r.json(t)["credential_configured"] != true {
			t.Fatalf("write: %s", r.body)
		}
		id := h.llmModel("3/gpt-4o").ID
		body := modelUpdateBody(false)
		body["api_key"] = "sk-provider-secret-7"
		u := h.ui("model-update", 3, map[string]any{"name": "gpt-4o"}, body, h.etag(3, "gpt-4o"))
		cred, _ := h.llmModel("3/gpt-4o").LiteLLMParams["litellm_credential_name"].(string)
		if v, _ := h.llm.Credential(cred); u.status != 200 || u.json(t)["credential_configured"] != true || v != "sk-provider-secret-7" {
			t.Fatalf("replace: %d %s", u.status, u.body)
		}
		body = modelUpdateBody(false)
		body["clear_api_key"] = true
		c := h.ui("model-update", 3, map[string]any{"name": "gpt-4o"}, body, h.etag(3, "gpt-4o"))
		if _, ok := h.llm.Credential(cred); c.status != 200 || c.json(t)["credential_configured"] != false || ok {
			t.Fatalf("clear: %d %s (credential still held: %v)", c.status, c.body, ok)
		}
		m := h.llmModel("3/gpt-4o")
		if m.ID != id || !jsonInt(m.ModelInfo["flowai_team_id"], 3) || !jsonInt(m.ModelInfo["flowai_model_id"], 1) {
			t.Fatalf("identity or metadata changed: %s %v", m.ID, m.ModelInfo)
		}
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/gpt-4o"}, nil, nil)
		h.mustCheckOK(7, k, "3/gpt-4o")
		if err := h.llm.Use(h.issueKey(7, k), "3/gpt-4o", 0.01); err != nil {
			t.Fatalf("task data access with the selected alias: %v", err)
		}
		for _, s := range []string{"sk-provider-secret-6", "sk-provider-secret-7", "sk-master-test"} {
			h.assertNoSecret(s)
		}
	})

	t.Run("Claimed task consumes its key after model lifecycle changes", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		h.createModel(3, "m2", nil)
		a, b, c := claim(3, 1010, 1, 1), claim(3, 1011, 1, 1), claim(3, 1012, 1, 1)
		h.readyClaim(7, a, 21, []string{"3/m1", "3/m2"}, nil, nil)
		h.setClaim(b, 7, 21)
		h.mustCheckOK(7, a, "3/m1")
		h.mustCheckOK(7, b, "3/m1")
		ka := h.issueKey(7, a)
		if r := h.setPaused(3, "m1", true); r.status != 200 {
			t.Fatalf("pause: %d %s", r.status, r.body)
		}
		if err := h.llm.Use(ka, "3/m1", 0.01); err != nil {
			t.Fatalf("key after pause: %v", err)
		}
		if r := h.archive(3, "m1"); r.status != 204 {
			t.Fatalf("archive: %d %s", r.status, r.body)
		}
		if err := h.llm.Use(ka, "3/m1", 0.01); err != nil {
			t.Fatalf("key after archive: %v", err)
		}
		kb := h.issueKey(7, b) // first issued after archive, snapshot taken before
		if err := h.llm.Use(kb, "3/m1", 0.01); err != nil {
			t.Fatalf("key first issued after archive: %v", err)
		}
		h.setClaim(c, 7, 21)
		h.setFlow(3, 21, "Flow 21", []string{"3/m1", "3/m2"}, nil, nil) // a stale list still naming m1
		h.mustCheckOK(7, c, "3/m2")
		if err := h.llm.Use(ka, "3/m2", 0.01); !errors.Is(err, mocks.ErrRejected) {
			t.Fatalf("unauthorized alias: %v", err)
		}
		h.endClaim(a, "task_terminal")
		h.executorEvent(7, "finished", a, nil)
		h.mustReconcile()
		if err := h.llm.Use(ka, "3/m1", 0.01); !errors.Is(err, mocks.ErrRejected) {
			t.Fatalf("key after finished: %v", err)
		}
	})

	t.Run("Configured limit errors and reset behavior are integrated", func(t *testing.T) {
		h := newHarness(t) // Wednesday 2026-10-14 09:30 UTC
		h.createModel(3, "gpt-4o", nil)
		want := map[string]struct {
			duration string
			reset    time.Time
		}{
			"day":   {"1d", time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)},
			"week":  {"7d", time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)},
			"month": {"30d", time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)},
		}
		flow := int64(30)
		for period, w := range want {
			flow++
			h.setFlow(3, flow, period, []string{"3/gpt-4o"}, nil, flowLimit("10", period))
			if r := h.applyFlowTeam(3, flow, 1, flowLimit("10", period)); r.status != 200 {
				t.Fatalf("%s apply: %d %s", period, r.status, r.body)
			}
			ft := h.flowTeam(3, flow)
			if ft.BudgetDuration != w.duration || ft.BudgetResetAt == nil || !ft.BudgetResetAt.Equal(w.reset) || ft.MaxBudget == nil || *ft.MaxBudget != 10 {
				t.Fatalf("%s team: %+v", period, ft)
			}
		}
		// Changing only the amount keeps spend and reset; changing the period
		// moves the reset to the next period of the new kind and keeps spend.
		// Flow rpm/tpm reach the LiteLLM team and absent fields are unset.
		h.setFlow(3, 50, "rated", []string{"3/gpt-4o"}, nil, map[string]any{"rpm": 30, "tpm": 5000})
		h.applyFlowTeam(3, 50, 1, map[string]any{"rpm": 30, "tpm": 5000})
		if rt := h.flowTeam(3, 50); rt.RPMLimit == nil || *rt.RPMLimit != 30 || rt.TPMLimit == nil || *rt.TPMLimit != 5000 || rt.MaxBudget != nil {
			t.Fatalf("rate limits %+v", rt)
		}
		h.applyFlowTeam(3, 50, 2, map[string]any{"max_budget_usd": "5", "budget_period": "day"})
		if rt := h.flowTeam(3, 50); rt.RPMLimit != nil || rt.TPMLimit != nil || rt.MaxBudget == nil || *rt.MaxBudget != 5 {
			t.Fatalf("absent rpm/tpm not unset: %+v", rt)
		}
		h.applyFlowTeam(3, 50, 3, nil)
		if rt := h.flowTeam(3, 50); rt.MaxBudget != nil || rt.RPMLimit != nil || rt.BudgetDuration != "" {
			t.Fatalf("null flow limit not unset: %+v", rt)
		}
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 40, []string{"3/gpt-4o"}, nil, flowLimit("10", "week"))
		ft := h.flowTeam(3, 40)
		h.llm.SetTeamSpend(ft.ID, 4)
		h.applyFlowTeam(3, 40, 2, flowLimit("12", "week"))
		if ft2 := h.flowTeam(3, 40); ft2.Spend != 4 || !ft2.BudgetResetAt.Equal(*ft.BudgetResetAt) || *ft2.MaxBudget != 12 {
			t.Fatalf("amount change: %+v", ft2)
		}
		h.applyFlowTeam(3, 40, 3, flowLimit("12", "day"))
		if ft3 := h.flowTeam(3, 40); ft3.Spend != 4 || ft3.BudgetDuration != "1d" || !ft3.BudgetResetAt.Equal(want["day"].reset) {
			t.Fatalf("period change: %+v", ft3)
		}
		h.llm.SetTeamSpend(ft.ID, 12)
		mustTemporary(t, h.check(7, k, nil), "budget_exhausted")
		h.llm.ResetTeamSpend(ft.ID, want["day"].reset.Add(24*time.Hour))
		h.mustCheckOK(7, k, "3/gpt-4o")
		other := claim(3, 1011, 1, 1)
		h.setClaim(other, 7, 21)
		h.setFlow(3, 21, "free", []string{"3/gpt-4o"}, nil, nil)
		h.applyFlowTeam(3, 21, 1, nil)
		h.llm.SetTeamSpend(ft.ID, 100)
		h.mustCheckOK(7, other, "3/gpt-4o") // another Flow keeps access

		// Budget and rate rejections stay distinguishable. The data plane
		// answers a per-minute rate hit with 429 rate_limit_error and an
		// exhausted budget with the configured 422 budget_exceeded.
		rateK, budgetK := claim(3, 1012, 1, 1), claim(3, 1013, 1, 1)
		h.readyClaim(7, rateK, 60, []string{"3/gpt-4o"}, map[string]any{"rpm": 1}, flowLimit("1", "day"))
		h.setClaim(budgetK, 7, 60)
		h.mustCheckOK(7, rateK, "3/gpt-4o") // the check after claim stores the LLM snapshot
		h.mustCheckOK(7, budgetK, "3/gpt-4o")
		rateKey, budgetKey := h.issueKey(7, rateK), h.issueKey(7, budgetK)
		if status, typ := h.llm.Call(rateKey, "3/gpt-4o", 0.01); status != 200 || typ != "" {
			t.Fatalf("first call: %d %q", status, typ)
		}
		rateStatus, rateType := h.llm.Call(rateKey, "3/gpt-4o", 0.01)
		if rateStatus != 429 || rateType != "rate_limit_error" {
			t.Fatalf("rate-limited call: %d %q, want 429 rate_limit_error", rateStatus, rateType)
		}
		budgetTeam := h.flowTeam(3, 60)
		h.llm.SetTeamSpend(budgetTeam.ID, 1)
		budgetStatus, budgetType := h.llm.Call(budgetKey, "3/gpt-4o", 0.01)
		if budgetStatus != 422 || budgetType != "budget_exceeded" {
			t.Fatalf("budget-exhausted call: %d %q, want 422 budget_exceeded", budgetStatus, budgetType)
		}
		if budgetStatus == rateStatus || budgetType == rateType {
			t.Fatalf("budget and rate rejections share %d %q", budgetStatus, budgetType)
		}
		// The Executor reports them as different failures: only the budget
		// failure makes model-registry re-read the Flow's spend.
		reads := len(h.llm.CallsTo("GET /team/info"))
		observed := len(h.events("spend-observed"))
		if d := h.executorEvent(7, "failed", rateK, map[string]any{"failure_reason": "openhands_error", "retryable": true,
			"openhands_error_code": "LLMRateLimitError"}); d.Outcome() != "ack" {
			t.Fatalf("rate failure not acknowledged: %q", d.Outcome())
		}
		if n := len(h.llm.CallsTo("GET /team/info")); n != reads {
			t.Fatalf("a rate-limit failure re-read the Flow spend (%d reads, was %d)", n, reads)
		}
		if d := h.executorEvent(7, "failed", budgetK, map[string]any{"failure_reason": "llm_budget_exceeded", "retryable": true}); d.Outcome() != "ack" {
			t.Fatalf("budget failure not acknowledged: %q", d.Outcome())
		}
		if n := len(h.llm.CallsTo("GET /team/info")); n != reads+1 {
			t.Fatalf("a budget failure did not re-read the Flow spend once (%d reads, was %d)", n, reads)
		}
		if len(h.events("spend-observed")) != observed+1 {
			t.Fatalf("budget failure published no new spend observation")
		}
	})

	t.Run("Selected upstream fails", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		h.createModel(3, "m2", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/m1", "3/m2"}, nil, nil)
		h.mustCheckOK(7, k, "3/m1")
		h.llm.DeleteModelDirectly("3/m1")
		r := h.keyEdge("1010", h.token(k, 7))
		if r.status != 422 || r.body["failure_reason"] != "llm_configuration_incomplete" {
			t.Fatalf("key edge with the pinned alias gone: %d %s", r.status, r.raw)
		}
		h.mustCheckOK(7, k, "3/m1") // the snapshot is not re-pinned to m2
		if n := len(h.llm.CallsTo("POST /key/generate")); n != 0 {
			t.Fatalf("a key for another alias was generated (%d)", n)
		}
	})
}
