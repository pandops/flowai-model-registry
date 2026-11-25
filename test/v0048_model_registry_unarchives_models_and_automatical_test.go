package test

import (
	"errors"
	"slices"
	"testing"

	"github.com/pandops/flowai-model-registry/internal/mocks"
	"github.com/pandops/flowai-model-registry/internal/store"
)

// Requirement: model-registry unarchives models and automatically unpauses
// them.
func TestV0048ModelRegistryUnarchivesModels(t *testing.T) {
	archived := func(t *testing.T) (*harness, string) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		h.createModel(3, "m2", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/m1", "3/m2"}, nil, nil)
		h.mustCheckOK(7, k, "3/m1")
		key := h.issueKey(7, k)
		h.endClaim(k, "task_terminal")
		h.executorEvent(7, "finished", k, nil)
		h.mustReconcile()
		if r := h.archive(3, "m1"); r.status != 204 {
			t.Fatalf("archive: %d %s", r.status, r.body)
		}
		return h, key
	}

	t.Run("Unarchive moves a model back as active", func(t *testing.T) {
		h, key := archived(t)
		if r := h.unarchive(3, "m1"); r.status != 204 {
			t.Fatalf("unarchive: %d %s", r.status, r.body)
		}
		m := h.llmModel("3/m1")
		if m.ModelInfo["flowai_archived"] != false || m.ModelInfo["flowai_paused"] != false {
			t.Fatalf("flags %v", m.ModelInfo)
		}
		g := h.getModel(3, "m1").json(t)
		if g["archived_at"] != nil || g["paused"] != false || !slices.Contains(listNames(t, h.ui("models-list", 3, nil, nil, "")), "m1") || !h.selectable(3, "3/m1") {
			t.Fatalf("read %v", g)
		}
		if len(h.broker.RequestsTo("flow-registry.service.restore-alias")) != 1 || !slices.Equal(h.flowAliases(3, 21), []string{"3/m2"}) {
			t.Fatal("restore-alias not sent once, or a Flow reference came back")
		}
		if err := h.llm.Use(key, "3/m1", 0.01); !errors.Is(err, mocks.ErrRejected) {
			t.Fatalf("revoked key revived: %v", err)
		}
		if h.auditCount("model-unarchive") != 1 {
			t.Fatal("unarchive not audited once")
		}
	})

	t.Run("Unarchive rejects stale or foreign callers", func(t *testing.T) {
		h, _ := archived(t)
		stale := `"AAAAAAAAAAAAAAAAAAAAAAAA"`
		writes := h.managementWrites()
		if r := h.ui("model-unarchive", 3, map[string]any{"name": "m1"}, nil, stale); r.status != 412 {
			t.Fatalf("stale: %d %s", r.status, r.body)
		}
		unknown := h.ui("model-unarchive", 4, map[string]any{"name": "nope"}, nil, stale)
		if r := h.ui("model-unarchive", 4, map[string]any{"name": "m1"}, nil, h.etag(3, "m1")); r.status != 404 || r.code(t) != unknown.code(t) {
			t.Fatalf("foreign: %d %s", r.status, r.body)
		}
		if h.managementWrites() != writes || len(h.broker.RequestsTo("flow-registry.service.restore-alias")) != 0 {
			t.Fatal("a rejected unarchive mutated something")
		}
	})

	t.Run("Unarchive reconciles before enabling selection", func(t *testing.T) {
		for _, c := range []struct {
			name   string
			inject func(h *harness)
		}{
			{"lost upstream response", func(h *harness) { h.llm.FailNext("PATCH /model/", 1, true) }},
			{"failed completion commit", func(h *harness) { h.store.FailCommit(store.TxUnarchiveComplete, 1) }},
		} {
			t.Run(c.name, func(t *testing.T) {
				h, _ := archived(t)
				c.inject(h)
				if r := h.unarchive(3, "m1"); r.status != 503 || r.code(t) != "model_gateway_unavailable" {
					t.Fatalf("unarchive: %d %s", r.status, r.body)
				}
				h.boot()
				if h.selectable(3, "3/m1") {
					t.Fatal("pending unarchive enabled selection")
				}
				h.mustReconcile()
				if !h.selectable(3, "3/m1") || h.getModel(3, "m1").json(t)["archived_at"] != nil || num(h.getModel(3, "m1").json(t)["model_id"]) != 1 {
					t.Fatal("reconciliation did not complete the original model")
				}
				if len(h.events("model-unarchive-reconciled")) != 1 {
					t.Fatal("no model-unarchive-reconciled event")
				}
				h.setPaused(3, "m1", true)
				audits, writes := h.auditCount("model-unarchive"), h.managementWrites()
				if r := h.unarchive(3, "m1"); r.status != 204 {
					t.Fatalf("repeat: %d", r.status)
				}
				if h.getModel(3, "m1").json(t)["paused"] != true || h.auditCount("model-unarchive") != audits || h.managementWrites() != writes {
					t.Fatal("repeated unarchive undid the pause or wrote again")
				}
			})
		}
	})
}
