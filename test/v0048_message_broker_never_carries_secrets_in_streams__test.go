package test

import (
	"strings"
	"testing"
)

// message-broker: no secret in model-registry's messages or logs.
func TestV0048MessageBrokerNeverCarriesSecrets(t *testing.T) {
	t.Run("Stream content inspection", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", map[string]any{"api_key": "sk-provider-secret-9"})
		h.createModel(3, "m2", nil)
		body := modelUpdateBody(false)
		body["api_key"] = "sk-provider-secret-10"
		h.ui("model-update", 3, map[string]any{"name": "m1"}, body, h.etag(3, "m1"))
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/m1", "3/m2"}, nil, flowLimit("10", "week"))
		h.mustCheckOK(7, k, "3/m1")
		key := h.issueKey(7, k)
		h.issueKey(7, k)
		h.archive(3, "m1")
		h.endClaim(k, "task_terminal")
		h.executorEvent(7, "finished", k, nil)
		h.mustReconcile()
		tok := h.token(k, 7)
		for _, s := range []string{"sk-provider-secret-9", "sk-provider-secret-10", "sk-master-test", key, tok, strings.SplitN(tok, ".", 2)[1],
			h.yaml["db"].(map[string]any)["password"].(string)} {
			h.assertNoSecret(s)
		}
		for _, row := range h.store.Outbox() {
			if strings.Contains(string(row.Payload), key) {
				t.Fatal("outbox row holds the virtual key")
			}
		}
		for _, tk := range h.store.TaskKeys() {
			if strings.Contains(string(tk.Ciphertext), key) {
				t.Fatal("task key stored in plaintext")
			}
		}
	})

	t.Run("Model check carries no claim token", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/m1"}, nil, nil)
		withToken := map[string]any{"params": map[string]any{}, "body": map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1, "claim_token": h.token(k, 7)}}
		r := h.mustSend("model-registry.executor.7.check.pub", map[string]string{"team": "3"}, withToken)
		if r.status != 400 || r.code(t) != "invalid_request" {
			t.Fatalf("check with a claim token: %d %s", r.status, r.body)
		}
		h.mustCheckOK(7, k, "3/m1")
		req := h.broker.RequestsTo("task-manager.service.claim-current.pub")
		if len(req) == 0 || intField(decodeReq(req[len(req)-1]).Params, "executor_id") != 7 {
			t.Fatal("Executor not taken from the subject")
		}
	})
}
