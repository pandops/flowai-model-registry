//go:build integration

package test

// Survivors of deleted State Registry race tests whose oracle is a
// PostgreSQL row-lock behavior (decision ledger
// docs/testing/v0048-test-decisions.csv): the app runs on the real store.

import (
	"context"
	"encoding/json"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pandops/flowai-model-registry/internal/store"
)

// pgStore opens a migrated model-registry store on a fresh database.
func pgStore(t *testing.T) store.Store {
	t.Helper()
	admin := freshDB(t)
	role := serviceRole(t, admin, lastDBName, "model_registry_schema", true)
	return openStore(t, role, lastDBName, "model_registry_schema")
}

// lockGate holds the first armed snapshot transaction right after it locked
// a model row, as the old claim gate held a claim after its team lock.
type lockGate struct {
	inner   store.Store
	armed   atomic.Bool
	reached chan struct{}
	release chan struct{}
}

func (g *lockGate) Tx(ctx context.Context, op string, fn func(store.Tx) error) error {
	if op != store.TxSnapshot {
		return g.inner.Tx(ctx, op, fn)
	}
	return g.inner.Tx(ctx, op, func(tx store.Tx) error { return fn(gatedTx{Tx: tx, g: g}) })
}

func (g *lockGate) Ping(ctx context.Context) error { return g.inner.Ping(ctx) }

type gatedTx struct {
	store.Tx
	g *lockGate
}

func (t gatedTx) ModelByKey(key int64, lock bool) (*store.Model, error) {
	m, err := t.Tx.ModelByKey(key, lock)
	if lock && err == nil && t.g.armed.CompareAndSwap(true, false) {
		close(t.g.reached)
		<-t.g.release
	}
	return m, err
}

func (h *harness) checkAsync(executor int64, k store.ClaimKey) <-chan res {
	out := make(chan res, 1)
	raw, _ := json.Marshal(map[string]any{"params": map[string]any{}, "body": map[string]any{"task_id": k.TaskID, "attempt": k.Attempt, "claim_seq": k.ClaimSeq}})
	go func() {
		r, err := h.send("model-registry.executor."+strconv.FormatInt(executor, 10)+".check.pub", map[string]string{"team": strconv.FormatInt(k.TeamID, 10)}, raw)
		if err != nil {
			r = res{}
		}
		out <- r
	}()
	return out
}

// M176 (TestModelWritersDoNotDeadlockWithClaims): model writers and the
// check after claim, which locks model rows in one snapshot transaction,
// never deadlock, in either lock order.
func TestV0048ModelWritersDoNotDeadlockWithChecksIntegration(t *testing.T) {
	writers := map[string]struct {
		op   string
		body any
		want int
	}{
		"pause":   {"model-update", modelUpdateBody(true), 200},
		"archive": {"model-archive", nil, 204},
	}
	setup := func(t *testing.T, opts ...harnessOption) (*harness, store.ClaimKey) {
		h := newHarness(t, opts...)
		h.createModel(3, "m1", nil)
		h.createModel(3, "m2", nil)
		k := claim(3, 1010, 1, 1)
		// The check pins m2, the first model of the Flow.
		h.readyClaim(7, k, 21, []string{"3/m2", "3/m1"}, nil, nil)
		return h, k
	}
	for name, w := range writers {
		t.Run("check holds the model first: "+name, func(t *testing.T) {
			g := &lockGate{inner: pgStore(t), reached: make(chan struct{}), release: make(chan struct{})}
			h, k := setup(t, withStore(g))
			defer func() {
				select {
				case <-g.release:
				default:
					close(g.release)
				}
			}()
			etag := h.etag(3, "m2")
			g.armed.Store(true)
			check := h.checkAsync(7, k)
			waitClosed(t, g.reached, "the check to lock its model")
			write := h.uiAsync(w.op, 3, map[string]any{"name": "m2"}, w.body, etag)
			time.Sleep(500 * time.Millisecond)
			close(g.release)
			if r := waitRes(t, check, "check"); r.status != 200 {
				t.Fatalf("check: %d %s", r.status, r.body)
			} else if res, alias := checkResult(t, r); res != "ok" || alias != "3/m2" {
				t.Fatalf("check %s %s, want ok 3/m2", res, alias)
			}
			if r := waitRes(t, write, name); r.status != w.want {
				t.Fatalf("%s: %d %s", name, r.status, r.body)
			}
		})
		t.Run("writer holds the model first: "+name, func(t *testing.T) {
			h, k := setup(t, withStore(pgStore(t)))
			etag := h.etag(3, "m2")
			reached, release := h.llm.Hold("PATCH /model/"+h.llmModel("3/m2").ID+"/update", false)
			defer release()
			write := h.uiAsync(w.op, 3, map[string]any{"name": "m2"}, w.body, etag)
			waitClosed(t, reached, "the writer to update LiteLLM")
			check := h.checkAsync(7, k)
			time.Sleep(500 * time.Millisecond)
			release()
			if r := waitRes(t, write, name); r.status != w.want {
				t.Fatalf("%s: %d %s", name, r.status, r.body)
			}
			r := waitRes(t, check, "check")
			if r.status != 200 {
				t.Fatalf("check: %d %s", r.status, r.body)
			}
			// The writer committed before the check's snapshot: the check
			// must not pin the paused or archived model.
			if res, alias := checkResult(t, r); res != "ok" || alias != "3/m1" {
				t.Fatalf("check %s %s, want ok 3/m1", res, alias)
			}
		})
	}
}

// M177 (TestReviewConcurrentModelPUTRejectsStaleBeforeCredentialWrite): of
// two concurrent PUTs with the same ETag exactly one succeeds; the other
// returns 412 and writes neither the model nor a credential.
func TestV0048ConcurrentModelPUTRejectsStaleBeforeCredentialWriteIntegration(t *testing.T) {
	h := newHarness(t, withStore(pgStore(t)))
	h.createModel(3, "second", map[string]any{"api_key": "sk-initial-fixture"})
	etag := h.etag(3, "second")
	writes, credentials := len(h.llm.CallsTo("PATCH /model/")), len(h.llm.CallsTo("PATCH /credentials/"))+len(h.llm.CallsTo("POST /credentials"))
	start := make(chan struct{})
	results := make([]<-chan res, 0, 2)
	for _, key := range []string{"key-a", "key-b"} {
		ch := make(chan res, 1)
		results = append(results, ch)
		go func(key string) {
			<-start
			body := map[string]any{"provider": "openai", "provider_model": key, "api_key": "sk-" + key + "-fixture", "paused": false}
			ch <- <-h.uiAsync("model-update", 3, map[string]any{"name": "second"}, body, etag)
		}(key)
	}
	close(start)
	codes := map[int]int{}
	for _, ch := range results {
		select {
		case r := <-ch:
			codes[r.status]++
			if r.status != 200 && r.status != 412 {
				t.Fatalf("PUT = %d: %s", r.status, r.body)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("PUT deadlocked")
		}
	}
	if codes[200] != 1 || codes[412] != 1 {
		t.Fatalf("same-ETag PUT statuses = %v, want one 200 and one 412", codes)
	}
	gotWrites := len(h.llm.CallsTo("PATCH /model/")) - writes
	gotCreds := len(h.llm.CallsTo("PATCH /credentials/")) + len(h.llm.CallsTo("POST /credentials")) - credentials
	if gotWrites != 1 || gotCreds != 1 {
		t.Fatalf("upstream writes: models=%d credentials=%d, the rejected PUT must write neither", gotWrites, gotCreds)
	}
}
