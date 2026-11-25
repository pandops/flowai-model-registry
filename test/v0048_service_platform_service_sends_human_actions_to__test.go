package test

import "testing"

// service-platform: human actions to audit, others to events. "Forwarded
// administrator action" is not applicable (model-registry has no admin
// operation in this change).
func TestV0048ServicePlatformSendsHumanActionsToAudit(t *testing.T) {
	t.Run("Operator cancels a task", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		before := h.auditCount("model-update")
		h.setPaused(3, "m1", true)
		h.relay()
		a := h.publishedOn("model-registry.audit.model-update.pub")
		if len(a) != before+1 || a[len(a)-1].body["outcome"] != "succeeded" || a[len(a)-1].header["team"] != "3" {
			t.Fatalf("model-update audits %v", a)
		}
	})

	t.Run("Rejected human action is audited", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		writes := h.managementWrites()
		r := h.ui("model-update", 3, map[string]any{"name": "m1"}, modelUpdateBody(true), `"AAAAAAAAAAAAAAAAAAAAAAAA"`)
		if r.status != 412 {
			t.Fatalf("stale update: %d", r.status)
		}
		h.relay()
		a := h.publishedOn("model-registry.audit.model-update.pub")
		if len(a) != 1 || a[0].body["outcome"] != "rejected" || h.managementWrites() != writes {
			t.Fatalf("rejected audits %v", a)
		}
		if g := h.getModel(3, "m1").json(t); g["paused"] != false {
			t.Fatal("rejected update changed data")
		}
	})

	t.Run("Executor event goes to journal", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/m1"}, nil, nil)
		h.mustCheckOK(7, k, "3/m1")
		h.issueKey(7, k)
		audits := len(h.audits())
		h.executorEvent(7, "running", k, nil)
		h.endClaim(k, "task_terminal")
		h.executorEvent(7, "finished", k, nil)
		h.mustReconcile()
		if len(h.audits()) != audits || len(h.events("task-key-issued")) != 1 || len(h.events("task-key-revoked")) != 1 {
			t.Fatal("non-human actions must be events, never audit")
		}
	})
}
