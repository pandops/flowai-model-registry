package test

import (
	"strconv"
	"testing"
)

// service-platform: team context comes only from the ui request's team header.
func TestV0048ServicePlatformTrustsTeamContextOnlyFromUIRequests(t *testing.T) {
	t.Run("Gateway forwards verified operator context", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		with := uiHeaders(3, h.nextRequestID())
		without := uiHeaders(3, h.nextRequestID())
		delete(without, "team_name")
		renamed := uiHeaders(3, h.nextRequestID())
		renamed["team_name"] = "Team Four"
		for _, hdr := range []map[string]string{with, without, renamed} {
			r := h.mustSend("model-registry.ui.model-get.pub", hdr, map[string]any{"params": map[string]any{"name": "m1"}})
			if r.status != 200 || r.json(t)["model_alias"] != "3/m1" {
				t.Fatalf("get with %v: %d %s", hdr, r.status, r.body)
			}
		}
	})

	t.Run("Payload names another team", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		h.createModel(4, "m1", nil)
		h.createModel(4, "only4", nil)
		for _, params := range []map[string]any{{"name": "m1", "team_id": 4}, {"name": "only4", "team_id": 4}} {
			r := h.mustSend("model-registry.ui.model-get.pub", uiHeaders(3, h.nextRequestID()), map[string]any{"params": params})
			if r.status == 200 && r.json(t)["model_alias"] != "3/m1" || bytesContain(r.body, "4/") {
				t.Fatalf("team 3 request reached team 4: %d %s", r.status, r.body)
			}
		}
		r := h.mustSend("model-registry.ui.model-create.pub", uiHeaders(3, h.nextRequestID()), map[string]any{"params": map[string]any{},
			"body": map[string]any{"name": "x", "provider": "openai", "provider_model": "x", "team_id": 4}})
		if r.status == 201 && r.json(t)["model_alias"] != "3/x" {
			t.Fatalf("create %s", r.body)
		}
		for _, m := range h.llm.Models() {
			if m.ModelName == "4/x" {
				t.Fatal("created a team 4 model")
			}
		}
	})

	t.Run("Context is missing", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		calls := len(h.llm.Calls())
		hdr := map[string]string{"operator_id": "501", "request_id": strconv.FormatInt(h.nextRequestID(), 10)}
		r := h.mustSend("model-registry.ui.models-list.pub", hdr, map[string]any{"params": map[string]any{}})
		if r.status != 400 || r.code(t) != "invalid_request" || len(h.llm.Calls()) != calls {
			t.Fatalf("no team header: %d %s", r.status, r.body)
		}
		// D12 owner decision 2026-11-09: no valid request_id, no request_id
		// field; every other error keeps it.
		if rid := r.json(t)["request_id"]; rid != float64(mustAtoi(t, hdr["request_id"])) {
			t.Fatalf("no team header: %s, want request_id %s", r.body, hdr["request_id"])
		}
		for _, bad := range []string{"", "0", "-1", "09", "r-1"} {
			hdr := uiHeaders(3, 1)
			if bad == "" {
				delete(hdr, "request_id")
			} else {
				hdr["request_id"] = bad
			}
			r := h.mustSend("model-registry.ui.models-list.pub", hdr, map[string]any{"params": map[string]any{}})
			if r.status != 400 || r.code(t) != "invalid_request" {
				t.Fatalf("request_id %q: %d %s, want 400 invalid_request", bad, r.status, r.body)
			}
			if _, has := r.json(t)["request_id"]; has {
				t.Fatalf("request_id %q: %s, want no request_id field", bad, r.body)
			}
		}
		svc := map[string]string{"team": "3"}
		r = h.mustSend("model-registry.service.validate-models.pub", svc, map[string]any{"params": map[string]any{}, "body": map[string]any{}})
		if r.status != 400 {
			t.Fatalf("service request without request_id: %d %s, want 400", r.status, r.body)
		}
		if _, has := r.json(t)["request_id"]; has {
			t.Fatalf("service request without request_id: %s, want no request_id field", r.body)
		}
	})
}

func mustAtoi(t *testing.T, s string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return n
}
