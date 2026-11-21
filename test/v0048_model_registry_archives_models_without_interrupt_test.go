package test

import (
	"slices"
	"testing"

	"github.com/pandops/flowai-model-registry/internal/store"
)

// Requirement: model-registry archives models without interrupting claimed
// tasks.
func TestV0048ModelRegistryArchivesModels(t *testing.T) {
	setup := func(t *testing.T, aliases ...string) *harness {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		h.createModel(3, "m2", nil)
		h.registerExecutor(7, 3)
		h.setFlow(3, 21, "Main", aliases, nil, nil)
		h.applyFlowTeam(3, 21, 1, nil)
		return h
	}

	t.Run("Archive preserves running and unopened claimed tasks", func(t *testing.T) {
		h := setup(t, "3/m1", "3/m2")
		running, unopened := claim(3, 1010, 1, 1), claim(3, 1011, 1, 1)
		h.setClaim(running, 7, 21)
		h.setClaim(unopened, 7, 21)
		h.mustCheckOK(7, running, "3/m1")
		h.mustCheckOK(7, unopened, "3/m1")
		key := h.issueKey(7, running)
		if g := h.getModel(3, "m1").json(t); num(g["unfinished_task_count"]) != 2 || g["can_archive"] != true {
			t.Fatalf("read before archive %v", g)
		}
		if r := h.archive(3, "m1"); r.status != 204 {
			t.Fatalf("archive: %d %s", r.status, r.body)
		}
		if got := h.flowAliases(3, 21); !slices.Equal(got, []string{"3/m2"}) {
			t.Fatalf("Flow aliases %v", got)
		}
		if err := h.llm.Use(key, "3/m1", 0.01); err != nil {
			t.Fatalf("running task's key after archive: %v", err)
		}
		if err := h.llm.Use(h.issueKey(7, unopened), "3/m1", 0.01); err != nil {
			t.Fatalf("unopened claim's key: %v", err)
		}
		if len(h.llm.CallsTo("POST /key/delete")) != 0 {
			t.Fatal("archive revoked a key")
		}
	})

	t.Run("Sole selected model blocks Archive", func(t *testing.T) {
		h := setup(t, "3/m1")
		g := h.getModel(3, "m1").json(t)
		flows, _ := g["archive_blocking_flows"].([]any)
		if g["can_archive"] != false || len(flows) != 1 || num(flows[0].(map[string]any)["flow_id"]) != 21 || flows[0].(map[string]any)["name"] != "Main" {
			t.Fatalf("read %v", g)
		}
		writes := h.managementWrites()
		r := h.archive(3, "m1")
		if r.status != 409 || r.code(t) != "model_last_in_flow" {
			t.Fatalf("archive: %d %s", r.status, r.body)
		}
		if h.managementWrites() != writes || !slices.Equal(h.flowAliases(3, 21), []string{"3/m1"}) {
			t.Fatal("blocked archive changed LiteLLM or the Flow")
		}
		if g := h.getModel(3, "m1").json(t); g["archived_at"] != nil || g["paused"] != false || !h.selectable(3, "3/m1") {
			t.Fatalf("model after blocked archive %v", g)
		}
	})

	t.Run("Archive races with a Flow model replacement", func(t *testing.T) {
		// The Flow write commits first: retire-alias answers model_last_in_flow.
		h := setup(t, "3/m1", "3/m2")
		h.beforeRetire = func() { h.setFlow(3, 21, "Main", []string{"3/m1"}, nil, nil) }
		r := h.archive(3, "m1")
		if r.status != 409 || r.code(t) != "model_last_in_flow" {
			t.Fatalf("archive: %d %s", r.status, r.body)
		}
		if len(h.store.Operations()) != 0 || !h.selectable(3, "3/m1") || h.llmModel("3/m1").ModelInfo["flowai_archived"] == true {
			t.Fatal("cancelled archive left a pending operation or archive metadata")
		}
		if !slices.Equal(h.flowAliases(3, 21), []string{"3/m1"}) {
			t.Fatal("the Flow list was emptied")
		}
		// Retirement commits first: the model is unselectable for the write.
		h2 := setup(t, "3/m1", "3/m2")
		if r := h2.archive(3, "m1"); r.status != 204 {
			t.Fatalf("archive: %d %s", r.status, r.body)
		}
		if h2.selectable(3, "3/m1") {
			t.Fatal("archived model is selectable for a Flow write")
		}
	})

	t.Run("Stale archive", func(t *testing.T) {
		h := setup(t, "3/m1", "3/m2")
		for _, paused := range []bool{false, true} {
			stale := h.etag(3, "m1")
			body := modelUpdateBody(paused)
			body["provider_model"] = "gpt-4o-mini"
			h.ui("model-update", 3, map[string]any{"name": "m1"}, body, stale)
			writes := h.managementWrites()
			r := h.ui("model-archive", 3, map[string]any{"name": "m1"}, nil, stale)
			if r.status != 412 {
				t.Fatalf("paused=%v stale archive: %d %s", paused, r.status, r.body)
			}
			if h.managementWrites() != writes || len(h.broker.RequestsTo("flow-registry.service.retire-alias")) != 0 {
				t.Fatal("stale archive changed something")
			}
		}
	})

	t.Run("Archive races with resume", func(t *testing.T) {
		h := setup(t, "3/m1", "3/m2")
		h.setPaused(3, "m1", true)
		stale := h.etag(3, "m1")
		h.setPaused(3, "m1", false) // resume wins
		if r := h.ui("model-archive", 3, map[string]any{"name": "m1"}, nil, stale); r.status != 412 {
			t.Fatalf("archive after a winning resume: %d", r.status)
		}
		h.setPaused(3, "m1", true)
		et := h.etag(3, "m1")
		h.broker.Silence("flow-registry.service.retire-alias.sub", true) // archive stays pending
		if r := h.ui("model-archive", 3, map[string]any{"name": "m1"}, nil, et); r.status != 503 {
			t.Fatalf("pending archive: %d %s", r.status, r.body)
		}
		// A refused resume answers without touching LiteLLM (old
		// TestModelChangeRefusesArchivingModel oracle).
		noLiteLLMWrite := func(step string, run func() res) res {
			t.Helper()
			writes, patches := h.managementWrites(), len(h.llm.CallsTo("PATCH /model/"))
			r := run()
			if h.managementWrites() != writes || len(h.llm.CallsTo("PATCH /model/")) != patches {
				t.Fatalf("%s updated LiteLLM: writes %d -> %d", step, writes, h.managementWrites())
			}
			return r
		}
		r := noLiteLLMWrite("resume during pending archive", func() res {
			return h.ui("model-update", 3, map[string]any{"name": "m1"}, modelUpdateBody(false), h.etag(3, "m1"))
		})
		if r.status != 409 || r.code(t) != "model_operation_in_progress" {
			t.Fatalf("resume during pending archive: %d %s", r.status, r.body)
		}
		h.broker.Silence("flow-registry.service.retire-alias.sub", false)
		h.mustReconcile()
		r = noLiteLLMWrite("resume after archive", func() res {
			return h.ui("model-update", 3, map[string]any{"name": "m1"}, modelUpdateBody(false), h.etag(3, "m1"))
		})
		if r.status != 409 || r.code(t) != "model_archived" {
			t.Fatalf("resume after archive: %d %s", r.status, r.body)
		}
	})

	t.Run("Archive completes after uncertain upstream update", func(t *testing.T) {
		for _, c := range []struct {
			name   string
			inject func(h *harness)
		}{
			{"lost upstream response", func(h *harness) { h.llm.FailNext("PATCH /model/", 1, true) }},
			{"failed completion commit", func(h *harness) { h.store.FailCommit(store.TxArchiveComplete, 1) }},
		} {
			t.Run(c.name, func(t *testing.T) {
				h := setup(t, "3/m1", "3/m2")
				id := h.llmModel("3/m1").ID
				c.inject(h)
				if r := h.archive(3, "m1"); r.status != 503 || r.code(t) != "model_gateway_unavailable" {
					t.Fatalf("archive: %d %s", r.status, r.body)
				}
				h.boot() // restart
				if h.selectable(3, "3/m1") {
					t.Fatal("pending archive reopened selection")
				}
				if r := h.ui("model-create", 3, nil, map[string]any{"name": "m1", "provider": "openai", "provider_model": "x"}, ""); r.status != 409 || r.code(t) != "model_name_conflict" {
					t.Fatalf("name reuse: %d %s", r.status, r.body)
				}
				h.mustReconcile()
				g := h.ui("model-get", 3, map[string]any{"name": "m1"}, nil, "").json(t)
				if g["archived_at"] == nil || num(g["model_id"]) != 1 || h.llmModel("3/m1").ID != id || len(h.store.Operations()) != 0 {
					t.Fatalf("after reconcile %v", g)
				}
				if len(h.events("model-archive-reconciled")) != 1 {
					t.Fatal("no model-archive-reconciled event")
				}
			})
		}
	})

	t.Run("Archived model is retained and read-only", func(t *testing.T) {
		h := setup(t, "3/m1", "3/m2")
		h.archive(3, "m1")
		archived := h.ui("models-list", 3, map[string]any{"archived": true}, nil, "")
		if !slices.Equal(listNames(t, archived), []string{"m1"}) || slices.Contains(listNames(t, h.ui("models-list", 3, nil, nil, "")), "m1") {
			t.Fatal("archived partition is wrong")
		}
		g := h.getModel(3, "m1").json(t)
		if g["archived_at"] == nil || g["editable"] != false || g["can_archive"] != false || g["can_unarchive"] != true {
			t.Fatalf("read %v", g)
		}
		audits, writes := h.auditCount("model-archive"), h.managementWrites()
		if r := h.archive(3, "m1"); r.status != 204 {
			t.Fatalf("archive retry: %d", r.status)
		}
		if g2 := h.getModel(3, "m1").json(t); g2["archived_at"] != g["archived_at"] || h.auditCount("model-archive") != audits || h.managementWrites() != writes {
			t.Fatal("archive retry was not idempotent")
		}
		if r := h.setPaused(3, "m1", false); r.status != 409 || r.code(t) != "model_archived" {
			t.Fatalf("resume: %d %s", r.status, r.body)
		}
		if r := h.ui("model-create", 3, nil, map[string]any{"name": "m1", "provider": "openai", "provider_model": "x"}, ""); r.status != 409 || r.code(t) != "model_name_conflict" {
			t.Fatalf("create: %d %s", r.status, r.body)
		}
	})
}
