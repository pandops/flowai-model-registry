package test

import "testing"

// service-platform: foreign data only from the broker. model-registry keeps
// no team projection (coordinator decision); its projection is executor
// ownership from executor-registry's events.
func TestV0048ServicePlatformGetsForeignDataOnlyFromTheBroker(t *testing.T) {
	t.Run("Team archive reaches a projection", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/m1"}, nil, nil)
		h.mustCheckOK(7, k, "3/m1")
		if d := h.removeExecutor(7, 3); d.Outcome() != "ack" {
			t.Fatalf("executor_removed %q", d.Outcome())
		}
		if r := h.check(7, k, nil); r.status != 503 || r.code(t) != "service_unavailable" {
			t.Fatalf("check from a removed Executor: %d %s", r.status, r.body)
		}
	})

	t.Run("Projection ignores a replayed event", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		reg := h.registerExecutor(7, 3)
		h.removeExecutor(7, 3)
		if again := h.redeliver(reg); again.Outcome() != "ack" {
			t.Fatalf("replay %q", again.Outcome())
		}
		k := claim(3, 1010, 1, 1)
		h.setFlow(3, 21, "Main", []string{"3/m1"}, nil, nil)
		h.setClaim(k, 7, 21)
		if r := h.check(7, k, nil); r.status != 503 {
			t.Fatalf("replayed registration revived the Executor: %d %s", r.status, r.body)
		}
	})
}
