package test

import (
	"testing"
)

// Requirement: model-registry serves model operations only over its broker
// subjects. ("Service outside the caller list" is NATS permission
// enforcement: infrastructure, not covered here.)
func TestV0048ModelRegistryServesModelOperationsOverBrokerSubjects(t *testing.T) {
	t.Run("Operator request arrives on the ui subject", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "gpt-4o", nil)
		et := h.etag(3, "gpt-4o")
		r := h.ui("model-update", 3, map[string]any{"name": "gpt-4o"}, modelUpdateBody(true), et)
		if r.status != 200 || r.json(t)["model_alias"] != "3/gpt-4o" || r.json(t)["paused"] != true || r.header["ETag"] == "" || r.header["ETag"] == et {
			t.Fatalf("update: %d %v %s", r.status, r.header, r.body)
		}
		if r := h.ui("model-update", 4, map[string]any{"name": "gpt-4o"}, modelUpdateBody(false), r.header["ETag"]); r.status != 404 {
			t.Fatalf("team 4 update of team 3's model: %d", r.status)
		}
	})

	t.Run("Executor names another team in its check", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		h.createModel(4, "m1", nil)
		own, foreign := claim(3, 1010, 1, 1), claim(4, 1010, 1, 1)
		h.readyClaim(7, own, 21, []string{"3/m1"}, nil, nil)
		h.setFlow(4, 21, "Other", []string{"4/m1"}, nil, nil)
		h.setClaim(foreign, 7, 21)
		hdr := map[string]string{"team": "4"}
		mustOK(t, h.check(7, own, hdr), "3/m1") // the team-4 header is ignored
		h.endClaim(own, "claim_ended")
		r := h.check(7, claim(3, 1010, 1, 1), hdr)
		if r.status != 404 || r.code(t) != "claim_not_current" {
			t.Fatalf("team 3 has no current task 1010: %d %s", r.status, r.body)
		}
		for _, req := range h.broker.RequestsTo("task-manager.service.claim-current.pub") {
			if intField(decodeReq(req).Params, "team_id") != 3 || req.Header["team"] != "3" {
				t.Fatalf("claim-current asked for another team: %s %v", req.Data, req.Header)
			}
		}
	})
}
