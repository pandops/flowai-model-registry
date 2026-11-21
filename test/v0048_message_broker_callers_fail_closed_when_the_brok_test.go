package test

import (
	"testing"
)

// message-broker: callers fail closed when the broker or owner is
// unavailable. The broker-outage scenarios and the 503 of an owner that does
// not answer are not covered: infrastructure, exercised by the E2E full flow
// (owner, 2026-11-09). Only the operator-visible read below stays.
func TestV0048MessageBrokerCallersFailClosed(t *testing.T) {
	// What the operator sees: without flow-registry's alias usage the model
	// read offers no archive decision.
	t.Run("Model read without flow-registry's alias usage", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		h.broker.Unrespond("flow-registry.service.alias-usage.sub")
		g := h.getModel(3, "m1").json(t)
		if g["archive_blocking_flows"] != nil || g["can_archive"] != false {
			t.Fatalf("read without flow-registry %v", g)
		}
	})
}
