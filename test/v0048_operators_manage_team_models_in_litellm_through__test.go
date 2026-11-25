package test

import (
	"slices"
	"testing"
)

// litellm-gateway requirement: Operators manage team models in LiteLLM
// through Settings (model-registry part).
func TestV0048OperatorsManageTeamModelsInLiteLLM(t *testing.T) {
	t.Run("Operator adds a model", func(t *testing.T) {
		h := newHarness(t)
		r := h.createModel(3, "gpt-4o", map[string]any{"api_key": "sk-provider-secret-3"})
		if b := r.json(t); b["model_alias"] != "3/gpt-4o" || b["credential_configured"] != true {
			t.Fatalf("reply %s", r.body)
		}
		m := h.llmModel("3/gpt-4o")
		cred, _ := m.LiteLLMParams["litellm_credential_name"].(string)
		if v, ok := h.llm.Credential(cred); cred == "" || !ok || v != "sk-provider-secret-3" {
			t.Fatalf("LiteLLM does not hold the key under the model's credential (%q)", cred)
		}
		if _, ok := m.LiteLLMParams["api_key"]; ok {
			t.Fatal("the key sits in plain litellm_params instead of a credential")
		}
		if !jsonInt(m.ModelInfo["flowai_team_id"], 3) || !jsonInt(m.ModelInfo["flowai_model_id"], 1) {
			t.Fatalf("model_info %v", m.ModelInfo)
		}
		if len(h.store.Models()) != 1 || !slices.Equal(h.store.OwnersOf("3/gpt-4o"), []int64{3}) {
			t.Fatalf("model-registry rows: %+v", h.store.Models())
		}
		if bytesContain(r.body, "sk-provider-secret-3") {
			t.Fatal("reply contains the key")
		}
		h.assertNoSecret("sk-provider-secret-3")
	})

	t.Run("Operator reads another team's model", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(4, "private", map[string]any{"provider_model": "secret-upstream-model"})
		unknown := h.getModel(3, "missing")
		foreign := h.getModel(3, "private")
		if foreign.status != 404 || foreign.status != unknown.status || foreign.code(t) != unknown.code(t) {
			t.Fatalf("foreign %d %s, unknown %d %s", foreign.status, foreign.body, unknown.status, unknown.body)
		}
		if bytesContain(foreign.body, "4/") || bytesContain(foreign.body, "secret-upstream-model") {
			t.Fatalf("404 reveals the other team's model: %s", foreign.body)
		}
	})

	t.Run("LiteLLM is down", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "existing", nil)
		et := h.etag(3, "existing")
		h.llm.SetDown(true)
		c := h.ui("model-create", 3, nil, map[string]any{"name": "gpt-4o", "provider": "openai", "provider_model": "gpt-4o", "api_key": "sk-provider-secret-4"}, "")
		if c.status != 503 || c.code(t) != "model_gateway_unavailable" {
			t.Fatalf("create: %d %s", c.status, c.body)
		}
		u := h.ui("model-update", 3, map[string]any{"name": "existing"}, modelUpdateBody(true), et)
		if u.status != 503 || u.code(t) != "model_gateway_unavailable" {
			t.Fatalf("update: %d %s", u.status, u.body)
		}
		h.llm.SetDown(false)
		if len(h.store.Models()) != 1 || len(h.llm.ModelByAlias("3/gpt-4o")) != 0 || len(h.llm.Credentials()) != 0 {
			t.Fatal("a failed create stored something")
		}
		if g := h.getModel(3, "existing").json(t); g["paused"] != false {
			t.Fatal("a failed update changed the model")
		}
		h.assertNoSecret("sk-provider-secret-4")
	})

	t.Run("Unarchive clears archive and pause metadata", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "gpt-4o", map[string]any{"api_key": "sk-provider-secret-5", "api_base": "https://llm.example.com/v1",
			"pricing": map[string]any{"input_usd_per_million_tokens": "2.50", "output_usd_per_million_tokens": "10.00"}})
		before := h.llmModel("3/gpt-4o")
		if r := h.archive(3, "gpt-4o"); r.status != 204 {
			t.Fatalf("archive: %d %s", r.status, r.body)
		}
		if r := h.unarchive(3, "gpt-4o"); r.status != 204 {
			t.Fatalf("unarchive: %d %s", r.status, r.body)
		}
		after := h.llmModel("3/gpt-4o")
		if after.ID != before.ID || after.ModelInfo["flowai_archived"] != false || after.ModelInfo["flowai_paused"] != false {
			t.Fatalf("after unarchive: id %s->%s info %v", before.ID, after.ID, after.ModelInfo)
		}
		for _, k := range []string{"model", "api_base", "litellm_credential_name", "input_cost_per_token", "output_cost_per_token"} {
			if after.LiteLLMParams[k] != before.LiteLLMParams[k] {
				t.Fatalf("litellm_params[%s] %v -> %v", k, before.LiteLLMParams[k], after.LiteLLMParams[k])
			}
		}
		g := h.getModel(3, "gpt-4o").json(t)
		if g["credential_configured"] != true || g["model_alias"] != "3/gpt-4o" || g["archived_at"] != nil {
			t.Fatalf("read after unarchive %v", g)
		}
	})
}
