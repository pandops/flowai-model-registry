package test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/pandops/flowai-model-registry/internal/mocks"
	"github.com/pandops/flowai-model-registry/internal/store"
)

// Requirement: model-registry issues per-task LiteLLM keys over its HTTP edge.
func TestV0048ModelRegistryIssuesPerTaskLiteLLMKeys(t *testing.T) {
	ready := func(t *testing.T, opts ...harnessOption) (*harness, store.ClaimKey) {
		h := newHarness(t, opts...)
		h.createModel(3, "gpt-4o", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/gpt-4o"}, nil, nil)
		h.mustCheckOK(7, k, "3/gpt-4o")
		return h, k
	}
	rejected := func(t *testing.T, h *harness, key string) {
		t.Helper()
		if err := h.llm.Use(key, "3/gpt-4o", 0.01); !errors.Is(err, mocks.ErrRejected) {
			t.Fatalf("LiteLLM still accepts the key: %v", err)
		}
	}
	noKey := func(t *testing.T, h *harness, r edgeRes) {
		t.Helper()
		if _, ok := r.body["OPENAI_API_KEY"]; ok {
			t.Fatal("error reply carries a key")
		}
		for _, k := range h.llm.Keys() {
			if !k.Revoked {
				t.Fatalf("an unrevoked key exists: %s", k.Token)
			}
		}
	}

	t.Run("Key follows the claim lifecycle", func(t *testing.T) {
		h, k := ready(t)
		first := h.issueKey(7, k)
		exp1 := h.llm.Keys()[0].ExpiresAt
		if want := h.clock().Add(24 * time.Hour); !exp1.Equal(want) {
			t.Fatalf("backstop expiry %v, want %v", exp1, want)
		}
		h.advance(time.Hour)
		if second := h.issueKey(7, k); second != first {
			t.Fatal("second request returned another key")
		}
		if exp2 := h.llm.Keys()[0].ExpiresAt; !exp2.After(exp1) {
			t.Fatal("key request did not extend the expiry")
		}
		h.endClaim(k, "task_terminal")
		if d := h.executorEvent(7, "finished", k, nil); d.Outcome() != "ack" {
			t.Fatalf("finished event %q", d.Outcome())
		}
		h.mustReconcile()
		rejected(t, h, first)
		if ev := h.events("task-key-revoked"); len(ev) != 1 || ev[0].body["cause"] != "finished" {
			t.Fatalf("task-key-revoked events %v", ev)
		}
	})

	t.Run("Released claim loses its key", func(t *testing.T) {
		for _, c := range []struct {
			name    string
			release func(h *harness, k store.ClaimKey)
		}{
			{"Executor pending_timeout", func(h *harness, k store.ClaimKey) {
				h.executorEvent(7, "released", k, map[string]any{"reason": "pending_timeout"})
			}},
			{"task-manager executor_lost", func(h *harness, k store.ClaimKey) {
				h.taskManagerEvent("released", k, map[string]any{"reason": "executor_lost"})
			}},
			{"task-manager claim_missing", func(h *harness, k store.ClaimKey) {
				h.taskManagerEvent("released", k, map[string]any{"reason": "claim_missing"})
			}},
		} {
			t.Run(c.name, func(t *testing.T) {
				h, k := ready(t)
				key := h.issueKey(7, k)
				h.endClaim(k, "claim_ended")
				c.release(h, k)
				h.mustReconcile()
				rejected(t, h, key)
				next := claim(3, 1010, 1, 2)
				h.setClaim(next, 7, 21)
				if r := h.keyEdge("1010", h.token(next, 7)); r.status != 422 {
					t.Fatalf("key before the next claim's check: %d %s", r.status, r.raw)
				}
				h.mustCheckOK(7, next, "3/gpt-4o")
				if nk := h.issueKey(7, next); nk == key {
					t.Fatal("next claim reused the revoked key")
				}
			})
		}
	})

	t.Run("Lost Executor returns after the release", func(t *testing.T) {
		h, k := ready(t)
		key := h.issueKey(7, k)
		h.endClaim(k, "claim_ended")
		h.taskManagerEvent("released", k, map[string]any{"reason": "executor_lost"})
		h.mustReconcile()
		rejected(t, h, key)
		r := h.keyEdge("1010", h.token(k, 7))
		if r.status != 404 || r.body["code"] != "claim_not_current" {
			t.Fatalf("key edge after release: %d %s", r.status, r.raw)
		}
	})

	t.Run("Token holder is not the current claim", func(t *testing.T) {
		h, k := ready(t)
		tok := h.token(k, 7)
		parts := strings.SplitN(tok, ".", 2)
		sig := []byte(parts[1])
		if sig[0] == 'A' {
			sig[0] = 'B'
		} else {
			sig[0] = 'A'
		}
		other := claim(3, 1011, 1, 1)
		h.setClaim(other, 7, 21)
		for name, req := range map[string][2]string{
			"bad signature": {"1010", parts[0] + "." + string(sig)},
			"another task":  {"1011", tok},
			"garbage token": {"1010", "not-a-token"},
		} {
			r := h.keyEdge(req[0], req[1])
			if r.status != 404 || r.body["code"] != "claim_not_current" {
				t.Fatalf("%s: %d %s", name, r.status, r.raw)
			}
			// D12 owner decision 2026-11-09: the edge has no request_id,
			// so its error carries no request_id field.
			if _, has := r.body["request_id"]; has {
				t.Fatalf("%s: %s, want no request_id field", name, r.raw)
			}
		}
		h.endClaim(k, "claim_ended")
		if r := h.keyEdge("1010", tok); r.status != 404 || r.body["code"] != "claim_not_current" {
			t.Fatalf("not current: %d %s", r.status, r.raw)
		}
		if n := len(h.llm.CallsTo("POST /key/generate")); n != 0 {
			t.Fatalf("%d keys created", n)
		}
	})

	t.Run("Pinned model disappeared", func(t *testing.T) {
		h, k := ready(t)
		h.llm.DeleteModelDirectly("3/gpt-4o")
		r := h.keyEdge("1010", h.token(k, 7))
		if r.status != 422 || r.body["code"] != "configuration_error" || r.body["failure_reason"] != "llm_configuration_incomplete" {
			t.Fatalf("%d %s", r.status, r.raw)
		}
		noKey(t, h, r)
	})

	t.Run("Data route is not configured", func(t *testing.T) {
		h, k := ready(t, withConfig("litellm.data_urls", map[string]any{"executor_docker_openhands": "http://litellm-data:4000"}))
		h.setExecutorType(7, "executor_k8s_openhands")
		r := h.keyEdge("1010", h.token(k, 7))
		if r.status != 422 || r.body["code"] != "configuration_error" || r.body["failure_reason"] != "llm_configuration_incomplete" {
			t.Fatalf("%d %s", r.status, r.raw)
		}
		noKey(t, h, r)
	})

	t.Run("Key created but never stored", func(t *testing.T) {
		h, k := ready(t)
		h.store.FailCommit(store.TxTaskKeyStore, 1)
		if r := h.keyEdge("1010", h.token(k, 7)); r.status == 200 {
			t.Fatal("key returned although it was never stored")
		}
		if len(h.store.OrphanRows()) != 1 || len(h.llm.Keys()) != 1 {
			t.Fatalf("orphans %d, LiteLLM keys %d", len(h.store.OrphanRows()), len(h.llm.Keys()))
		}
		lost := h.llm.Keys()[0].Token
		h.boot() // restart
		h.mustReconcile()
		for _, kk := range h.llm.Keys() {
			if kk.Token == lost && !kk.Revoked {
				t.Fatal("orphan key not revoked")
			}
		}
		if len(h.store.OrphanRows()) != 0 || len(h.events("orphan-key-revoked")) != 1 {
			t.Fatal("orphan row kept or no orphan-key-revoked event")
		}
		if key := h.issueKey(7, k); mocks.TokenOf(key) == lost {
			t.Fatal("the orphan key was returned")
		}
	})
}
