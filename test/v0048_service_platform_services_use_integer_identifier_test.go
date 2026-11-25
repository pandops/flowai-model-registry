package test

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/pandops/flowai-model-registry/internal/store"
)

// service-platform: integer identifiers.
func TestV0048ServicePlatformServicesUseIntegerIdentifiers(t *testing.T) {
	bad := []string{"abc", "0", "-1", "1.5", "9223372036854775808", "3f2c8a10-6b1e-4d2a-9c3e-5a7b8c9d0e1f"}

	t.Run("Invalid identifier is rejected", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/m1"}, nil, nil)
		h.mustCheckOK(7, k, "3/m1")
		calls := len(h.llm.Calls())
		for _, v := range bad {
			hdr := uiHeaders(3, h.nextRequestID())
			hdr["team"] = v
			r := h.mustSend("model-registry.ui.models-list.pub", hdr, map[string]any{"params": map[string]any{}})
			if r.status != 400 || r.code(t) != "invalid_request" {
				t.Fatalf("team %q: %d %s", v, r.status, r.body)
			}
			e := h.keyEdge(v, h.token(k, 7))
			if e.status != 400 || e.body["code"] != "invalid_request" {
				t.Fatalf("task_id %q: %d %s", v, e.status, e.raw)
			}
			raw := []byte(fmt.Sprintf(`{"params":{},"body":{"task_id":%q,"attempt":1,"claim_seq":1}}`, v))
			c := h.mustSend("model-registry.executor.7.check.pub", nil, raw)
			if c.status != 400 || c.code(t) != "invalid_request" {
				t.Fatalf("check task_id %q: %d %s", v, c.status, c.body)
			}
		}
		if len(h.llm.Calls()) != calls || len(h.llm.Keys()) != 0 {
			t.Fatal("an invalid identifier reached LiteLLM")
		}
	})

	t.Run("First record in each team is 1", func(t *testing.T) {
		h := newHarness(t)
		a := h.createModel(3, "a", nil).json(t)
		b := h.createModel(4, "a", nil).json(t)
		c := h.createModel(3, "b", nil).json(t)
		if num(a["model_id"]) != 1 || num(b["model_id"]) != 1 || num(c["model_id"]) != 2 {
			t.Fatalf("ids %v %v %v", a["model_id"], b["model_id"], c["model_id"])
		}
	})

	t.Run("Equal numbers in two teams grant nothing", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "mine", nil)
		h.createModel(4, "theirs", nil)
		if g := h.getModel(3, "theirs"); g.status != 404 {
			t.Fatalf("team 3 reads team 4's model 1: %d", g.status)
		}
		et := h.etag(4, "theirs")
		if r := h.ui("model-archive", 3, map[string]any{"name": "theirs"}, nil, et); r.status != 404 {
			t.Fatalf("team 3 archives team 4's model 1: %d", r.status)
		}
		if g := h.getModel(4, "theirs").json(t); g["archived_at"] != nil {
			t.Fatal("team 4's model changed")
		}
	})

	t.Run("Concurrent creates get unique increasing numbers", func(t *testing.T) {
		h := newHarness(t)
		h.store.FailCommit(store.TxModelCreate, 3)
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, _ = h.send("model-registry.ui.model-create.pub", uiHeaders(3, int64(9000+i)), map[string]any{"params": map[string]any{},
					"body": map[string]any{"name": fmt.Sprintf("m%02d", i), "provider": "openai", "provider_model": "x"}})
			}(i)
		}
		wg.Wait()
		var ids []int64
		for _, m := range h.store.Models() {
			ids = append(ids, m.ModelID)
		}
		if len(ids) != 17 {
			t.Fatalf("%d models committed, want 17", len(ids))
		}
		sorted := slices.Clone(ids)
		slices.Sort(sorted)
		if !slices.Equal(ids, sorted) || len(slices.Compact(sorted)) != 17 {
			t.Fatalf("ids in commit order %v", ids)
		}
	})

	t.Run("Rolled-back transaction neither publishes nor reuses its sequence", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		h.store.FailCommit(store.TxModelUpdate, 1)
		h.setPaused(3, "m1", true)
		h.setPaused(3, "m1", true)
		h.relay()
		var seqs []int64
		for _, m := range h.broker.Published() {
			var seq int64
			fmt.Sscanf(m.Header["Nats-Msg-Id"], "model-registry-%d", &seq)
			seqs = append(seqs, seq)
		}
		for i := 1; i < len(seqs); i++ {
			if seqs[i] <= seqs[i-1] {
				t.Fatalf("publisher_seq not increasing: %v", seqs)
			}
		}
		if len(seqs) < 2 || seqs[len(seqs)-1]-seqs[0] < int64(len(seqs)) {
			t.Fatalf("no gap from the rolled-back transaction: %v", seqs)
		}
	})

	t.Run("Redelivered message is deduplicated by publisher sequence", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/m1"}, nil, nil)
		h.mustCheckOK(7, k, "3/m1")
		h.issueKey(7, k)
		h.endClaim(k, "claim_ended")
		d := h.taskManagerEvent("released", k, nil)
		h.mustReconcile()
		if again := h.redeliver(d); again.Outcome() != "ack" {
			t.Fatalf("redelivery %q", again.Outcome())
		}
		h.mustReconcile()
		if n := len(h.llm.CallsTo("POST /key/delete")); n != 1 {
			t.Fatalf("%d revocations", n)
		}
	})
}
