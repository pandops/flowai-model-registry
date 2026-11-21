package test

import (
	"testing"
	"time"

	"github.com/pandops/flowai-model-registry/internal/store"
)

// Requirement: model-registry takes the team of every Executor subject from
// its executor_id -> team_id projection, never from a header or body field
// (specs/model-registry/spec.md, specs/executor-registry/spec.md). An
// Executor event touches only claims held by the sending Executor.
func TestV0048ModelRegistryDerivesExecutorEventTeamFromProjection(t *testing.T) {
	// ready gives team 3 a current claim of Executor 7 with an issued key and
	// registers the other Executors the cases send from.
	ready := func(t *testing.T) (*harness, store.ClaimKey, string) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/m1"}, nil, nil)
		h.mustCheckOK(7, k, "3/m1")
		key := h.issueKey(7, k)
		if err := h.llm.Use(key, "3/m1", 0.01); err != nil {
			t.Fatalf("sentinel: key unusable before the event: %v", err)
		}
		for _, e := range []struct{ executor, team int64 }{{9, 4}, {8, 3}} {
			if d := h.registerExecutor(e.executor, e.team); d.Outcome() != "ack" {
				t.Fatalf("executor %d of team %d not registered: %q", e.executor, e.team, d.Outcome())
			}
		}
		return h, k, key
	}
	keyKept := func(t *testing.T, h *harness, k store.ClaimKey, key string) {
		t.Helper()
		h.mustReconcile()
		if err := h.llm.Use(key, "3/m1", 0.01); err != nil {
			t.Errorf("team 3's current claim lost its LiteLLM key: %v", err)
		}
		if r := h.keyEdge("1010", h.token(k, 7)); r.status != 200 {
			t.Errorf("litellm-key edge %d %v, want 200", r.status, r.body["code"])
		}
		if ev := h.events("task-key-revoked"); len(ev) != 0 {
			t.Errorf("task-key-revoked events %v, want none", ev)
		}
	}
	senders := []struct {
		name     string
		executor int64
	}{
		{"Executor of another team", 9},
		{"Another Executor of the same team", 8},
		{"Executor not in the projection", 5},
	}

	// The event names team 3 in body and header and carries the claim of
	// Executor 7; the sender holds no such claim.
	for _, s := range senders {
		t.Run(s.name+" cannot end the claim", func(t *testing.T) {
			h, k, key := ready(t)
			d := h.executorEvent(s.executor, "failed", k, nil)
			t.Logf("event outcome: %s", d.Outcome())
			keyKept(t, h, k, key)
		})
		t.Run(s.name+" cannot extend the key", func(t *testing.T) {
			h, k, _ := ready(t)
			before := h.llm.Keys()[0].ExpiresAt
			h.advance(7 * time.Hour) // past a quarter of the 24 h backstop
			h.executorEvent(s.executor, "running", k, nil)
			if after := h.llm.Keys()[0].ExpiresAt; !after.Equal(before) {
				t.Errorf("expiry moved from %v to %v on a foreign Executor's activity", before, after)
			}
		})
	}

	// Control: the claim's own Executor still ends and extends it.
	t.Run("Own Executor extends and ends the claim", func(t *testing.T) {
		h, k, key := ready(t)
		before := h.llm.Keys()[0].ExpiresAt
		h.advance(7 * time.Hour)
		if d := h.executorEvent(7, "running", k, nil); d.Outcome() != "ack" {
			t.Fatalf("running event %q", d.Outcome())
		}
		if after := h.llm.Keys()[0].ExpiresAt; !after.After(before) {
			t.Fatalf("own activity did not extend the key (%v -> %v)", before, after)
		}
		if d := h.executorEvent(7, "failed", k, nil); d.Outcome() != "ack" {
			t.Fatalf("failed event %q", d.Outcome())
		}
		h.mustReconcile()
		if err := h.llm.Use(key, "3/m1", 0.01); err == nil {
			t.Fatal("key still usable after its own Executor failed the claim")
		}
	})
}
