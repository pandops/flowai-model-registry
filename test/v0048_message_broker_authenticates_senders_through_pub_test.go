package test

import (
	"strconv"
	"testing"
)

// message-broker: operator headers grant nothing on service subjects.
func TestV0048MessageBrokerAuthenticatesSendersThroughPublishPermissions(t *testing.T) {
	t.Run("Context headers on a service subject", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		h.createModel(4, "m1", nil)
		audits := len(h.audits())
		hdr := map[string]string{"team": "3", "operator_id": "501", "team_name": "Operations", "request_id": strconv.FormatInt(h.nextRequestID(), 10)}
		r := h.mustSend("model-registry.service.validate-models.pub", hdr, map[string]any{"body": map[string]any{"team_id": 3, "aliases": []string{"3/m1", "4/m1"}}})
		validateOwn(t, "service.validate-models.reply.schema.json", r.body)
		got := r.json(t)["aliases"].([]any)
		if got[0].(map[string]any)["owned"] != true || got[1].(map[string]any)["owned"] != false {
			t.Fatalf("validate-models for team 3: %v", got)
		}
		if len(h.audits()) != audits {
			t.Fatal("a service request was audited as an operator action")
		}
		// The body team must match the header team.
		bad := h.mustSend("model-registry.service.validate-models.pub", hdr, map[string]any{"body": map[string]any{"team_id": 4, "aliases": []string{"4/m1"}}})
		if bad.status != 400 {
			t.Fatalf("body team 4 under header team 3: %d %s", bad.status, bad.body)
		}
	})
}
