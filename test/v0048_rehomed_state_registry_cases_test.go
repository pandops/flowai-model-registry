package test

// Survivors of deleted State Registry tests (decision ledger
// docs/testing/v0048-test-decisions.csv), re-homed to model-registry with
// the old inputs and oracles translated to v0048 wire shapes.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pandops/flowai-model-registry/internal/litellm"
	"github.com/pandops/flowai-model-registry/internal/mocks"
	"github.com/pandops/flowai-model-registry/internal/store"
)

// holdAfterTx is a store that, once armed, holds the first transaction named
// op after it committed, as a request delayed between two steps would be.
type holdAfterTx struct {
	inner   store.Store
	op      string
	armed   atomic.Bool
	reached chan struct{}
	release chan struct{}
}

func newHoldAfterTx(op string) *holdAfterTx {
	return &holdAfterTx{op: op, reached: make(chan struct{}), release: make(chan struct{})}
}

func (d *holdAfterTx) Tx(ctx context.Context, op string, fn func(store.Tx) error) error {
	err := d.inner.Tx(ctx, op, fn)
	if op == d.op && d.armed.CompareAndSwap(true, false) {
		close(d.reached)
		<-d.release
	}
	return err
}

func (d *holdAfterTx) Ping(ctx context.Context) error { return d.inner.Ping(ctx) }

// withHold runs the app on d wrapped around the harness memory store.
func withHold(d *holdAfterTx) harnessOption {
	return func(h *harness) {
		d.inner = h.store
		h.backing = d
	}
}

// uiAsync sends a ui request from another goroutine; status 0 means the
// request could not be sent.
func (h *harness) uiAsync(op string, team int64, params map[string]any, body any, ifMatch string) <-chan res {
	out := make(chan res, 1)
	hdr := uiHeaders(team, h.nextRequestID())
	if ifMatch != "" {
		hdr["If-Match"] = ifMatch
	}
	payload := map[string]any{"params": params}
	if body != nil {
		payload["body"] = body
	}
	go func() {
		r, err := h.send("model-registry.ui."+op+".pub", hdr, payload)
		if err != nil {
			r = res{}
		}
		out <- r
	}()
	return out
}

func waitRes(t *testing.T, ch <-chan res, what string) res {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not finish (deadlock)", what)
	}
	return res{}
}

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func (h *harness) localModel(alias string) store.Model {
	h.t.Helper()
	for _, m := range h.store.Models() {
		if m.Alias == alias {
			return m
		}
	}
	h.t.Fatalf("no local model %s", alias)
	return store.Model{}
}

func flag(m mocks.LiteLLMModel, name string) bool {
	v, _ := m.ModelInfo[name].(bool)
	return v
}

func (h *harness) succeededAudits(action string) int {
	n := 0
	h.relay()
	for _, p := range h.publishedOn("model-registry.audit." + action + ".pub") {
		if p.body["outcome"] == "succeeded" {
			n++
		}
	}
	return n
}

// M144 (TestGetModelRejectsTrailingReadback): a model readback with bytes
// after the JSON value is not a confirmed readback.
func TestV0048GetModelRejectsTrailingReadback(t *testing.T) {
	for _, suffix := range []string{" trailing", " {}"} {
		t.Run(suffix, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`{"data":[{"model_name":"3/model","litellm_params":{"model":"openai/custom","input_cost_per_token":1e-12,"output_cost_per_token":2e-12},"model_info":{"id":"owned-model","flowai_team_id":3,"flowai_model_id":1}}]}` + suffix))
			}))
			defer server.Close()
			_, err := litellm.New(server.URL, "synthetic-master", 2*time.Second).GetModel(t.Context(), "owned-model")
			if !errors.Is(err, litellm.ErrUnavailable) {
				t.Fatalf("malformed readback accepted: %v", err)
			}
		})
	}
}

// M173 (TestTaskKeyWithUnrecordableOrphanIsStillDisposed): a key whose
// orphan record or LiteLLM create fails never stays usable after recovery.
func TestV0048TaskKeyWithUnrecordableOrphanIsStillDisposed(t *testing.T) {
	ready := func(t *testing.T) (*harness, store.ClaimKey) {
		h := newHarness(t)
		h.createModel(3, "gpt-4o", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/gpt-4o"}, nil, nil)
		h.mustCheckOK(7, k, "3/gpt-4o")
		return h, k
	}
	onlyLive := func(t *testing.T, h *harness, key string) {
		t.Helper()
		for _, k := range h.llm.Keys() {
			if !k.Revoked && k.Key != key {
				t.Fatalf("key %s is still usable after recovery", k.Token)
			}
		}
	}

	// v0048 records the orphan before the LiteLLM create, so a failed
	// orphan record means LiteLLM is never asked; a failing create after
	// the record is the next subtest.
	t.Run("orphan record fails", func(t *testing.T) {
		h, k := ready(t)
		h.store.FailCommit(store.TxTaskKeyOrphan, 1)
		if r := h.keyEdge("1010", h.token(k, 7)); r.status != 503 {
			t.Fatalf("open with a failing orphan record: %d %s", r.status, r.raw)
		}
		if n := len(h.llm.CallsTo("POST /key/generate")); n != 0 {
			t.Fatalf("LiteLLM created %d keys without an orphan record", n)
		}
		h.mustReconcile()
		key := h.issueKey(7, k)
		h.mustReconcile()
		onlyLive(t, h, key)
	})

	t.Run("LiteLLM create fails after the orphan record", func(t *testing.T) {
		h, k := ready(t)
		h.llm.FailNext("POST /key/generate", 1, true) // created, response lost
		if r := h.keyEdge("1010", h.token(k, 7)); r.status != 503 {
			t.Fatalf("open with a failing LiteLLM create: %d %s", r.status, r.raw)
		}
		keys := h.llm.Keys()
		if len(keys) != 1 || len(h.store.OrphanRows()) != 1 {
			t.Fatalf("generated keys %d, orphans %d", len(keys), len(h.store.OrphanRows()))
		}
		lost := keys[0].Token
		h.mustReconcile()
		for _, kk := range h.llm.Keys() {
			if kk.Token == lost && !kk.Revoked {
				t.Fatalf("key %s not revoked after recovery", lost)
			}
		}
		if n := len(h.store.OrphanRows()); n != 0 {
			t.Fatalf("%d orphan rows left", n)
		}
	})
}

// M174 (TestOrphanThatFailsToRevokeDoesNotBlockOthers): one orphan whose
// revocation LiteLLM keeps refusing does not block revoking the others.
func TestV0048OrphanThatFailsToRevokeDoesNotBlockOthers(t *testing.T) {
	h := newHarness(t)
	h.createModel(3, "gpt-4o", nil)
	k := claim(3, 1010, 1, 1)
	h.readyClaim(7, k, 21, []string{"3/gpt-4o"}, nil, nil)
	h.mustCheckOK(7, k, "3/gpt-4o")
	h.store.FailCommit(store.TxTaskKeyStore, 3)
	for i := range 3 {
		if r := h.keyEdge("1010", h.token(k, 7)); r.status == 200 {
			t.Fatalf("open %d with a failing key store succeeded", i)
		}
	}
	orphans := h.store.OrphanRows()
	if len(orphans) != 3 || len(h.llm.Keys()) != 3 {
		t.Fatalf("orphans %d, generated keys %d", len(orphans), len(h.llm.Keys()))
	}
	refused := orphans[0].Token
	h.llm.RefuseKeyDelete(refused, true)
	h.mustReconcile()
	for _, kk := range h.llm.Keys() {
		if kk.Token == refused && kk.Revoked {
			t.Fatal("the refused key was revoked")
		}
		if kk.Token != refused && !kk.Revoked {
			t.Fatalf("key %s not revoked although only %s was refused", kk.Token, refused)
		}
	}
	if left := h.store.OrphanRows(); len(left) != 1 || left[0].Token != refused {
		t.Fatalf("orphans left %v, want only the refused one", left)
	}
	h.llm.RefuseKeyDelete(refused, false)
	h.mustReconcile()
	for _, kk := range h.llm.Keys() {
		if !kk.Revoked {
			t.Fatalf("key %s not revoked once LiteLLM accepts", kk.Token)
		}
	}
	if n := len(h.store.OrphanRows()); n != 0 {
		t.Fatalf("%d orphan rows left", n)
	}
}

// M175, model-registry half (TestFlowWriteWaitsForModelChangeInsteadOfFailing):
// validate-models sent while a pause of a listed model is in flight answers
// instead of failing.
func TestV0048ValidateModelsDuringInFlightPauseSucceeds(t *testing.T) {
	h := newHarness(t)
	h.createModel(3, "m1", nil)
	h.createModel(3, "m2", nil)
	etag := h.etag(3, "m2")
	reached, release := h.llm.Hold("PATCH /model/"+h.llmModel("3/m2").ID+"/update", false)
	defer release()
	pause := h.uiAsync("model-update", 3, map[string]any{"name": "m2"}, modelUpdateBody(true), etag)
	waitClosed(t, reached, "the pause to hold the model")
	validate := make(chan res, 1)
	go func() {
		r, err := h.send("model-registry.service.validate-models.pub", map[string]string{"team": "3", "request_id": "9101"},
			map[string]any{"body": map[string]any{"team_id": 3, "aliases": []string{"3/m2", "3/m1"}}})
		if err != nil {
			r = res{}
		}
		validate <- r
	}()
	time.Sleep(300 * time.Millisecond)
	release()
	if r := waitRes(t, pause, "pause"); r.status != 200 {
		t.Fatalf("pause: %d %s", r.status, r.body)
	}
	r := waitRes(t, validate, "validate-models")
	if r.status != 200 {
		t.Fatalf("validate-models during the pause: %d %s", r.status, r.body)
	}
	validateOwn(t, "service.validate-models.reply.schema.json", r.body)
	for _, a := range r.json(t)["aliases"].([]any) {
		e := a.(map[string]any)
		if e["owned"] != true || e["selectable"] != true {
			t.Fatalf("alias %v not selectable during the pause", e)
		}
		if e["alias"] == "3/m1" && e["availability"] != "active" {
			t.Fatalf("3/m1 %v", e)
		}
	}
}

// M179 (TestUnarchiveRetryCannotUndoLaterSuccessfulPause): an Unarchive
// request delayed after its operation began cannot undo a Pause that
// succeeded after another request completed the Unarchive.
func TestV0048UnarchiveRetryCannotUndoLaterSuccessfulPause(t *testing.T) {
	hold := newHoldAfterTx(store.TxUnarchiveBegin)
	h := newHarness(t, withHold(hold))
	defer func() {
		select {
		case <-hold.release:
		default:
			close(hold.release)
		}
	}()
	h.createModel(3, "m2", nil)
	if r := h.archive(3, "m2"); r.status != 204 {
		t.Fatalf("archive: %d %s", r.status, r.body)
	}
	etag := h.etag(3, "m2")
	hold.armed.Store(true)
	first := h.uiAsync("model-unarchive", 3, map[string]any{"name": "m2"}, nil, etag)
	waitClosed(t, hold.reached, "the first Unarchive to begin")
	if r := h.ui("model-unarchive", 3, map[string]any{"name": "m2"}, nil, etag); r.status != 204 {
		t.Fatalf("retried Unarchive: %d %s", r.status, r.body)
	}
	if r := h.setPaused(3, "m2", true); r.status != 200 {
		t.Fatalf("pause: %d %s", r.status, r.body)
	}
	if !flag(h.llmModel("3/m2"), "flowai_paused") {
		t.Fatal("the successful Pause did not pause")
	}
	close(hold.release)
	if r := waitRes(t, first, "first Unarchive"); r.status != 204 {
		t.Fatalf("first Unarchive: %d %s", r.status, r.body)
	}
	m := h.llmModel("3/m2")
	if !flag(m, "flowai_paused") || flag(m, "flowai_archived") {
		t.Fatalf("later Pause undone by the delayed Unarchive: LiteLLM model_info %v", m.ModelInfo)
	}
	if got := h.localModel("3/m2"); got.Availability != store.AvailabilityPaused || got.ArchivedAt != nil {
		t.Fatalf("local availability %s archived_at %v", got.Availability, got.ArchivedAt)
	}
	if n := h.succeededAudits("model-unarchive"); n != 1 {
		t.Fatalf("%d succeeded model-unarchive audits, want 1", n)
	}
}

// M180 (TestArchiveRetryCannotUndoLaterSuccessfulUnarchive): an Archive
// request delayed after its operation began cannot re-archive after another
// request completed the Archive and a later Unarchive succeeded.
func TestV0048ArchiveRetryCannotUndoLaterSuccessfulUnarchive(t *testing.T) {
	hold := newHoldAfterTx(store.TxArchiveBegin)
	h := newHarness(t, withHold(hold))
	defer func() {
		select {
		case <-hold.release:
		default:
			close(hold.release)
		}
	}()
	h.createModel(3, "m2", nil)
	etag := h.etag(3, "m2")
	hold.armed.Store(true)
	first := h.uiAsync("model-archive", 3, map[string]any{"name": "m2"}, nil, etag)
	waitClosed(t, hold.reached, "the first Archive to begin")
	if r := h.ui("model-archive", 3, map[string]any{"name": "m2"}, nil, etag); r.status != 204 {
		t.Fatalf("archive retry: %d %s", r.status, r.body)
	}
	if r := h.unarchive(3, "m2"); r.status != 204 {
		t.Fatalf("later Unarchive: %d %s", r.status, r.body)
	}
	if flag(h.llmModel("3/m2"), "flowai_archived") {
		t.Fatal("the successful Unarchive left LiteLLM archived")
	}
	retires := len(h.broker.RequestsTo("flow-registry.service.retire-alias."))
	close(hold.release)
	if r := waitRes(t, first, "first Archive"); r.status != 204 {
		t.Fatalf("first Archive: %d %s", r.status, r.body)
	}
	m := h.llmModel("3/m2")
	if flag(m, "flowai_archived") || flag(m, "flowai_paused") {
		t.Fatalf("late Archive undid the Unarchive: LiteLLM model_info %v", m.ModelInfo)
	}
	if got := h.localModel("3/m2"); got.Availability != store.AvailabilityActive || got.ArchivedAt != nil {
		t.Fatalf("local availability %s archived_at %v", got.Availability, got.ArchivedAt)
	}
	if n := len(h.broker.RequestsTo("flow-registry.service.retire-alias.")); n != retires {
		t.Fatalf("late Archive retired the alias again (%d -> %d retire-alias requests)", retires, n)
	}
	if !h.selectable(3, "3/m2") {
		t.Fatal("the unarchived model is not selectable")
	}
}

// M181 (TestDetachedCredentialCleanupCannotDeleteNewlyAttachedKey): removing
// a provider credential cannot later delete a credential attached by a
// newer update.
func TestV0048DetachedCredentialCleanupCannotDeleteNewlyAttachedKey(t *testing.T) {
	clear := map[string]any{"provider": "openai", "provider_model": "gpt-4o", "clear_api_key": true, "paused": false}
	attach := map[string]any{"provider": "openai", "provider_model": "gpt-4o", "api_key": "sk-new-fixture-key", "paused": false}
	attached := func(t *testing.T, h *harness) {
		t.Helper()
		name, _ := h.llmModel("3/m2").LiteLLMParams["litellm_credential_name"].(string)
		if name == "" {
			t.Fatal("no credential attached")
		}
		if key, ok := h.llm.Credential(name); !ok || key != "sk-new-fixture-key" {
			t.Fatalf("attached credential %s present=%v", name, ok)
		}
	}
	for _, c := range []struct {
		name  string
		lost  bool
		clear int
	}{{"clear confirmed", false, 200}, {"clear response lost", true, 503}} {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t)
			h.createModel(3, "m2", map[string]any{"api_key": "sk-old-fixture-key"})
			if c.lost {
				h.llm.FailNext("DELETE /credentials/", 1, true)
			}
			if r := h.ui("model-update", 3, map[string]any{"name": "m2"}, clear, h.etag(3, "m2")); r.status != c.clear {
				t.Fatalf("clear: %d %s", r.status, r.body)
			}
			if r := h.ui("model-update", 3, map[string]any{"name": "m2"}, attach, h.etag(3, "m2")); r.status != 200 {
				t.Fatalf("attach: %d %s", r.status, r.body)
			}
			attached(t, h)
			deletes := len(h.llm.CallsTo("DELETE /credentials/"))
			h.mustReconcile()
			h.boot() // restart
			h.mustReconcile()
			h.mustRefreshAvailability()
			attached(t, h)
			if n := len(h.llm.CallsTo("DELETE /credentials/")); n != deletes {
				t.Fatalf("a credential cleanup ran after the attach (%d -> %d deletes)", deletes, n)
			}
		})
	}
}

// M182 (TestModelAvailabilityObservationNeverOverwritesNewerChange): an
// availability refresh read taken before a Pause or Resume never
// overwrites it, a fresh read applies, and an archiving model stays
// excluded.
func TestV0048ModelAvailabilityObservationNeverOverwritesNewerChange(t *testing.T) {
	staleRefresh := func(t *testing.T, h *harness, change func()) {
		t.Helper()
		reached, release := h.llm.Hold("GET /model/info", true)
		defer release()
		done := make(chan error, 1)
		go func() { done <- h.app.RefreshAvailability(h.ctx) }()
		waitClosed(t, reached, "the refresh read")
		change()
		release()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("refresh: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("refresh did not finish")
		}
	}

	t.Run("Pause after the refresh read", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		staleRefresh(t, h, func() {
			if r := h.setPaused(3, "m1", true); r.status != 200 {
				t.Fatalf("pause: %d %s", r.status, r.body)
			}
		})
		if got := h.localModel("3/m1").Availability; got != store.AvailabilityPaused {
			t.Fatalf("availability after the stale refresh = %s, want paused", got)
		}
	})

	t.Run("Resume after the refresh read", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", map[string]any{"paused": true})
		staleRefresh(t, h, func() {
			if r := h.setPaused(3, "m1", false); r.status != 200 {
				t.Fatalf("resume: %d %s", r.status, r.body)
			}
		})
		if got := h.localModel("3/m1").Availability; got != store.AvailabilityActive {
			t.Fatalf("availability after the stale refresh = %s, want active", got)
		}
	})

	t.Run("A fresh read applies", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		h.llm.DeleteModelDirectly("3/m1")
		h.mustRefreshAvailability()
		if got := h.localModel("3/m1").Availability; got != store.AvailabilityUnavailable {
			t.Fatalf("availability after a fresh read = %s, want unavailable", got)
		}
	})

	t.Run("An archiving model stays excluded", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		h.createModel(3, "m2", nil)
		h.broker.Silence("flow-registry.service.retire-alias.sub", true)
		if r := h.archive(3, "m1"); r.status != 503 {
			t.Fatalf("archive with flow-registry silent: %d %s", r.status, r.body)
		}
		h.mustRefreshAvailability()
		if h.selectable(3, "3/m1") {
			t.Fatal("refresh made the archiving model selectable")
		}
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/m1", "3/m2"}, nil, nil)
		h.mustCheckOK(7, k, "3/m2")
	})

	t.Run("An archived model stays excluded whatever LiteLLM reports", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		if r := h.archive(3, "m1"); r.status != 204 {
			t.Fatalf("archive: %d %s", r.status, r.body)
		}
		h.llm.PatchModelDirectly("3/m1", nil, map[string]any{"flowai_archived": false, "flowai_paused": false})
		h.mustRefreshAvailability()
		if got := h.localModel("3/m1").Availability; got == store.AvailabilityActive || got == store.AvailabilityPaused {
			t.Fatalf("archived model refreshed to %s", got)
		}
		if h.selectable(3, "3/m1") {
			t.Fatal("archived model selectable after refresh")
		}
	})
}

// M232 (TestV0034_14_PriceChangeIssuesOnlyTheModelUpdate): a price change
// issues only the LiteLLM model update and leaves the Flow team, Flow
// references and usage untouched.
func TestV0048PriceChangeIssuesOnlyTheModelUpdate(t *testing.T) {
	h := newHarness(t)
	pair := func(in, out string) map[string]any {
		return map[string]any{"input_usd_per_million_tokens": in, "output_usd_per_million_tokens": out}
	}
	h.createModel(3, "priced", map[string]any{"provider_model": "custom", "pricing": pair("1", "2")})
	limit := flowLimit("1.00", "day")
	h.setFlow(3, 21, "tt", []string{"3/priced"}, nil, limit)
	if r := h.applyFlowTeam(3, 21, 1, limit); r.status != 200 {
		t.Fatalf("apply-flow-team: %d %s", r.status, r.body)
	}
	h.llm.SetTeamSpend(h.flowTeam(3, 21).ID, 0.75)
	if err := h.app.RefreshSpend(h.ctx); err != nil {
		t.Fatal(err)
	}
	status := func() string {
		r := h.service("flows-status", "3", map[string]any{"team_id": 3, "flows": []any{map[string]any{"flow_id": 21, "aliases": []string{"3/priced"}, "flow_limit": limit}}})
		u, _ := json.Marshal(r.json(t)["flows"].([]any)[0].(map[string]any)["usage"])
		return string(u)
	}
	usageBefore := status()
	if !strings.Contains(usageBefore, `"spend_usd":"0.75"`) {
		t.Fatalf("usage before %s", usageBefore)
	}
	teamBefore := fmt.Sprintf("%+v", h.flowTeam(3, 21))
	aliasesBefore := fmt.Sprint(h.flowAliases(3, 21))
	calls, requests := len(h.llm.Calls()), len(h.broker.Requests())

	body := map[string]any{"provider": "openai", "provider_model": "custom", "paused": false, "pricing": pair("4", "5")}
	r := h.ui("model-update", 3, map[string]any{"name": "priced"}, body, h.etag(3, "priced"))
	if r.status != 200 {
		t.Fatalf("price change: %d %s", r.status, r.body)
	}
	if got, _ := json.Marshal(r.json(t)["pricing"]); string(got) != `{"input_usd_per_million_tokens":"4","output_usd_per_million_tokens":"5"}` &&
		string(got) != `{"input_usd_per_million_tokens":"4.000000","output_usd_per_million_tokens":"5.000000"}` {
		t.Fatalf("pricing after the change %s", got)
	}
	var writes []string
	for _, c := range h.llm.Calls()[calls:] {
		line := c.Method + " " + c.Path
		if strings.HasPrefix(c.Path, "/team/") || strings.HasPrefix(c.Path, "/key/") || strings.HasPrefix(c.Path, "/credentials") {
			t.Fatalf("price change called %s", line)
		}
		if c.Method != http.MethodGet {
			writes = append(writes, line)
		}
	}
	if len(writes) != 1 || !strings.HasPrefix(writes[0], "PATCH /model/") {
		t.Fatalf("price change LiteLLM writes = %v, want only the model update", writes)
	}
	for _, m := range h.broker.Requests()[requests:] {
		if strings.HasPrefix(m.Subject, "flow-registry.") && m.Subject != "flow-registry.service.alias-usage.pub" {
			t.Fatalf("price change sent %s", m.Subject)
		}
	}
	if after := fmt.Sprintf("%+v", h.flowTeam(3, 21)); after != teamBefore {
		t.Fatalf("Flow team changed: %s -> %s", teamBefore, after)
	}
	if after := fmt.Sprint(h.flowAliases(3, 21)); after != aliasesBefore {
		t.Fatalf("Flow references changed: %s -> %s", aliasesBefore, after)
	}
	if after := status(); after != usageBefore {
		t.Fatalf("usage changed: %s -> %s", usageBefore, after)
	}
}
