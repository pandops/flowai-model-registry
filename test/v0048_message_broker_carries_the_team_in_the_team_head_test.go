package test

import "testing"

// message-broker: the team comes from the team header, or for Executor
// requests from model-registry's executor projection.
func TestV0048MessageBrokerCarriesTheTeamInTheTeamHeader(t *testing.T) {
	t.Run("Unknown Executor is not yet projected", func(t *testing.T) {
		h := newHarness(t)
		r := h.check(9, claim(3, 1010, 1, 1), nil)
		if r.status != 503 || r.code(t) != "service_unavailable" {
			t.Fatalf("check from an unprojected Executor: %d %s", r.status, r.body)
		}
		if n := len(h.broker.RequestsTo("task-manager.")); n != 0 {
			t.Fatalf("asked task-manager %d times without a team", n)
		}
		h.registerExecutor(9, 3)
		h.createModel(3, "m1", nil)
		k := claim(3, 1010, 1, 1)
		h.setFlow(3, 21, "Main", []string{"3/m1"}, nil, nil)
		h.setClaim(k, 9, 21)
		h.mustCheckOK(9, k, "3/m1")
	})
}
