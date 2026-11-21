package test

import (
	"testing"
)

// Requirement: model-registry routes human actions to audit and its own
// actions to events.
func TestV0048ModelRegistryRoutesHumanActionsToAuditAndEvents(t *testing.T) {
	t.Run("Model create is audited without the key", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "gpt-4o", map[string]any{"api_key": "sk-provider-secret-8"})
		a := h.audits()
		if len(a) != 1 || a[0].subject != "model-registry.audit.model-create.pub" {
			t.Fatalf("audits %v", a)
		}
		b := a[0].body
		changes, _ := b["changes"].(map[string]any)
		if b["publisher"] != "model-registry" || num(b["team_id"]) != 3 || num(b["actor_id"]) != 501 || b["actor_type"] != "operator" ||
			b["action"] != "model-create" || b["outcome"] != "succeeded" || changes["model_name"] != "gpt-4o" || num(b["request_id"]) <= 0 ||
			a[0].header["request_id"] == "" {
			t.Fatalf("audit %s %v", a[0].raw, a[0].header)
		}
		h.assertNoSecret("sk-provider-secret-8")
		h.assertNoSecret("sk-master-test")
	})

	t.Run("Revocation is a journal event", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "gpt-4o", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/gpt-4o"}, nil, nil)
		h.mustCheckOK(7, k, "3/gpt-4o")
		key := h.issueKey(7, k)
		audits := len(h.audits())
		h.endClaim(k, "task_terminal")
		h.executorEvent(7, "finished", k, nil)
		h.mustReconcile()
		ev := h.events("task-key-revoked")
		if len(ev) != 1 || num(ev[0].body["task_id"]) != 1010 || num(ev[0].body["attempt"]) != 1 || num(ev[0].body["claim_seq"]) != 1 || num(ev[0].body["team_id"]) != 3 {
			t.Fatalf("events %v", ev)
		}
		if len(h.audits()) != audits {
			t.Fatal("revocation published an audit message")
		}
		h.assertNoSecret(key)
	})

	t.Run("Lifecycle event is redelivered", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "gpt-4o", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/gpt-4o"}, nil, nil)
		h.mustCheckOK(7, k, "3/gpt-4o")
		h.issueKey(7, k)
		h.endClaim(k, "task_terminal")
		d := h.executorEvent(7, "finished", k, nil)
		h.mustReconcile()
		if again := h.redeliver(d); again.Outcome() != "ack" {
			t.Fatalf("redelivery %q", again.Outcome())
		}
		h.mustReconcile()
		if n := len(h.llm.CallsTo("POST /key/delete")); n != 1 {
			t.Fatalf("%d revocations", n)
		}
		if n := len(h.events("task-key-revoked")); n != 1 {
			t.Fatalf("%d task-key-revoked events", n)
		}
	})

	t.Run("Consumer contract test reads the Executor schema", func(t *testing.T) {
		for _, schema := range []string{schemaDockerEvent, schemaK8sEvent} {
			t.Run(schema, func(t *testing.T) {
				h := newHarness(t)
				h.createModel(3, "gpt-4o", nil)
				k := claim(3, 1010, 1, 1)
				h.readyClaim(7, k, 21, []string{"3/gpt-4o"}, nil, nil)
				h.mustCheckOK(7, k, "3/gpt-4o")
				key := h.issueKey(7, k)
				h.endClaim(k, "claim_ended")
				// executorEvent validates its input against the Docker schema; the
				// K8s Executor ships the same contract (rule 15).
				d := h.executorEvent(7, "released", k, map[string]any{"reason": "executor_shutdown"})
				validateForeign(t, schema, d.Message().Data)
				h.mustReconcile()
				if err := h.llm.Use(key, "3/gpt-4o", 0.01); err == nil {
					t.Fatal("key of the released claim still works")
				}
				assertSharedInput(t, schema, "model-registry")
			})
		}
	})
}
