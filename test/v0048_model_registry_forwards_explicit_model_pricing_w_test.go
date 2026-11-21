package test

import (
	"strings"
	"testing"
)

// Requirement: model-registry forwards explicit model pricing with guarded
// confirmation.
func TestV0048ModelRegistryForwardsExplicitModelPricing(t *testing.T) {
	pair := func(in, out string) map[string]any {
		return map[string]any{"input_usd_per_million_tokens": in, "output_usd_per_million_tokens": out}
	}

	t.Run("Asymmetric prices reach LiteLLM", func(t *testing.T) {
		h := newHarness(t)
		r := h.createModel(3, "gpt-4o", map[string]any{"pricing": pair("2.50", "10.00")})
		p := h.llmModel("3/gpt-4o").LiteLLMParams
		if !numberIs(p["input_cost_per_token"], 0.0000025) || !numberIs(p["output_cost_per_token"], 0.00001) {
			t.Fatalf("litellm_params costs %v %v", p["input_cost_per_token"], p["output_cost_per_token"])
		}
		calls := h.llm.Calls()
		create := -1
		for i, c := range calls {
			if c.Method == "POST" && c.Path == "/model/new" {
				create = i
			}
		}
		readback := false
		for _, c := range calls[create+1:] {
			readback = readback || (c.Method == "GET" && strings.HasPrefix(c.Path, "/model/info"))
		}
		if !readback {
			t.Fatal("no confirmation readback after the pricing write")
		}
		b := r.json(t)
		pr, _ := b["pricing"].(map[string]any)
		if b["pricing_status"] != "configured" || pr["input_usd_per_million_tokens"] != "2.5" && pr["input_usd_per_million_tokens"] != "2.50" {
			t.Fatalf("reply pricing %v %v", b["pricing_status"], pr)
		}
		h.relay()
		for _, row := range h.store.Outbox() {
			if strings.Contains(string(row.Payload), "2.5") || strings.Contains(string(row.Payload), "cost_per_token") {
				t.Fatalf("model-registry stored a price: %s", row.Payload)
			}
		}
	})

	t.Run("Partial upstream pricing changes under a stale editor", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "gpt-4o", nil)
		h.llm.PatchModelDirectly("3/gpt-4o", map[string]any{"input_cost_per_token": 0.000001}, nil)
		g1 := h.getModel(3, "gpt-4o")
		h.llm.PatchModelDirectly("3/gpt-4o", map[string]any{"input_cost_per_token": 0.000002}, nil)
		g2 := h.getModel(3, "gpt-4o")
		for _, g := range []res{g1, g2} {
			if b := g.json(t); b["pricing"] != nil || b["pricing_status"] != "not_configured" {
				t.Fatalf("partial projection %v %v", b["pricing"], b["pricing_status"])
			}
		}
		if g1.header["ETag"] == g2.header["ETag"] {
			t.Fatal("ETag ignores a partial price change")
		}
		writes := h.managementWrites()
		if r := h.ui("model-update", 3, map[string]any{"name": "gpt-4o"}, modelUpdateBody(true), g1.header["ETag"]); r.status != 412 {
			t.Fatalf("stale update: %d %s", r.status, r.body)
		}
		if h.managementWrites() != writes {
			t.Fatal("stale update mutated LiteLLM")
		}
	})

	t.Run("Omitted pricing survives lifecycle operations", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "gpt-4o", map[string]any{"pricing": pair("2.50", "10.00")})
		h.llm.PatchModelDirectly("3/gpt-4o", map[string]any{"output_cost_per_token": nil, "cache_read_input_token_cost": 0.0000001}, nil)
		want := h.llmModel("3/gpt-4o").LiteLLMParams
		assertPrices := func(step string) {
			t.Helper()
			p := h.llmModel("3/gpt-4o").LiteLLMParams
			if _, ok := p["output_cost_per_token"]; ok {
				t.Fatalf("%s: missing rate became %v", step, p["output_cost_per_token"])
			}
			if p["input_cost_per_token"] != want["input_cost_per_token"] || p["cache_read_input_token_cost"] != want["cache_read_input_token_cost"] {
				t.Fatalf("%s: prices %v", step, p)
			}
		}
		edit := modelUpdateBody(false)
		edit["provider_model"] = "gpt-4o-2024"
		h.ui("model-update", 3, map[string]any{"name": "gpt-4o"}, edit, h.etag(3, "gpt-4o"))
		assertPrices("edit")
		h.ui("model-update", 3, map[string]any{"name": "gpt-4o"}, map[string]any{"provider": "openai", "provider_model": "gpt-4o-2024", "paused": true}, h.etag(3, "gpt-4o"))
		assertPrices("pause")
		if r := h.archive(3, "gpt-4o"); r.status != 204 {
			t.Fatalf("archive: %d %s", r.status, r.body)
		}
		assertPrices("archive")
		if r := h.unarchive(3, "gpt-4o"); r.status != 204 {
			t.Fatalf("unarchive: %d %s", r.status, r.body)
		}
		assertPrices("unarchive")
		h.mustRefreshAvailability()
		// Leave an archive pending (the metadata response is lost), so the
		// reconciliation pass really writes the LiteLLM model again.
		h.llm.FailNext("PATCH /model/", 1, true)
		if r := h.archive(3, "gpt-4o"); r.status != 503 || r.code(t) != "model_gateway_unavailable" {
			t.Fatalf("pending archive: %d %s", r.status, r.body)
		}
		if len(h.store.Operations()) != 1 {
			t.Fatalf("no pending operation before reconciliation: %v", h.store.Operations())
		}
		patches := len(h.llm.CallsTo("PATCH /model/"))
		h.mustReconcile()
		if len(h.llm.CallsTo("PATCH /model/")) <= patches {
			t.Fatal("reconciliation wrote nothing to LiteLLM")
		}
		if g := h.getModel(3, "gpt-4o").json(t); g["archived_at"] == nil || len(h.store.Operations()) != 0 {
			t.Fatalf("reconciliation did not complete the archive: %v", g)
		}
		assertPrices("reconciliation")
	})

	t.Run("Upstream commits but confirmation fails", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "gpt-4o", nil)
		for _, c := range []struct {
			name   string
			inject func()
		}{
			{"confirmation read fails", func() { h.llm.FailAfter("PATCH /model/", "GET /model/info") }},
			{"confirmation disagrees", func() {
				h.llm.PatchAfter("PATCH /model/", "3/gpt-4o", map[string]any{"input_cost_per_token": 0.000009})
			}},
			{"lost write response", func() { h.llm.FailNext("PATCH /model/", 1, true) }},
		} {
			name, inject := c.name, c.inject
			body := modelUpdateBody(false)
			body["pricing"] = pair("1.00", "2.00")
			et := h.etag(3, "gpt-4o")
			patches := len(h.llm.CallsTo("PATCH /model/"))
			inject()
			r := h.ui("model-update", 3, map[string]any{"name": "gpt-4o"}, body, et)
			if len(h.llm.CallsTo("PATCH /model/")) != patches+1 {
				t.Fatalf("%s: the pricing write was not dispatched", name)
			}
			if r.status != 503 || r.code(t) != "model_gateway_unavailable" || bytesContain(r.body, "pricing") {
				t.Fatalf("%s: %d %s", name, r.status, r.body)
			}
		}
	})
}
