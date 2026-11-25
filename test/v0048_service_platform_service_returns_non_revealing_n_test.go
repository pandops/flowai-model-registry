package test

import "testing"

// service-platform: non-revealing not-found for foreign resources (model
// form of the task and environment scenarios).
func TestV0048ServicePlatformReturnsNonRevealingNotFound(t *testing.T) {
	t.Run("Operator reads a foreign task", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(4, "m1010", nil)
		unknown, foreign := h.getModel(3, "nothing"), h.getModel(3, "m1010")
		if foreign.status != 404 || string(stripRequestID(t, foreign)) != string(stripRequestID(t, unknown)) {
			t.Fatalf("foreign %s, unknown %s", foreign.body, unknown.body)
		}
	})

	t.Run("Operator writes a foreign environment", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(4, "m5", map[string]any{"api_key": "sk-provider-secret-11"})
		et := h.etag(4, "m5")
		writes := h.managementWrites()
		body := modelUpdateBody(true)
		body["clear_api_key"] = true
		if r := h.ui("model-update", 3, map[string]any{"name": "m5"}, body, et); r.status != 404 {
			t.Fatalf("foreign update: %d", r.status)
		}
		if h.managementWrites() != writes || h.getModel(4, "m5").json(t)["credential_configured"] != true {
			t.Fatal("foreign update changed the model or its credential")
		}
	})
}

func stripRequestID(t testing.TB, r res) []byte {
	m := r.json(t)
	delete(m, "request_id")
	out, _ := jsonMarshal(m)
	return out
}
