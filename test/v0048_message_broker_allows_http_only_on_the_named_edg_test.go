package test

import (
	"net/http"
	"strings"
	"testing"
)

// message-broker: model-registry's HTTP listener serves only the probes and
// the Executor key edge. ("Source system reads its settings" and "Journal
// stream edge" are not applicable to model-registry.)
func TestV0048MessageBrokerAllowsHTTPOnlyOnNamedEdges(t *testing.T) {
	t.Run("Service offers no internal HTTP API", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		writes := h.managementWrites()
		for _, p := range []string{"/", "/v1/models", "/ui/v1/models", "/v1/_test/decrypt-ops", "/v1/tasks/1010", "/v1/flows/21/status", "/healthz", "/metrics"} {
			for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut} {
				req, _ := http.NewRequest(method, h.edge.URL+p, strings.NewReader(`{}`))
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				resp.Body.Close()
				if resp.StatusCode != 404 {
					t.Fatalf("%s %s = %d, want 404", method, p, resp.StatusCode)
				}
			}
		}
		if h.managementWrites() != writes || len(h.llm.Keys()) != 0 {
			t.Fatal("a non-edge request changed data")
		}
	})

	t.Run("Executor edge errors are distinguishable", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		released, incomplete := claim(3, 1010, 1, 1), claim(3, 1012, 1, 1)
		h.readyClaim(7, released, 21, []string{"3/m1"}, nil, nil)
		h.setClaim(incomplete, 7, 21) // never checked: no snapshot
		h.endClaim(released, "claim_ended")
		a := h.keyEdge("1010", h.token(released, 7))
		c := h.keyEdge("1012", h.token(incomplete, 7))
		for _, x := range []struct {
			r      edgeRes
			status int
			code   string
		}{{a, 404, "claim_not_current"}, {c, 422, "configuration_error"}} {
			if x.r.status != x.status || x.r.body["code"] != x.code {
				t.Fatalf("got %d %s, want %d %s", x.r.status, x.r.raw, x.status, x.code)
			}
			if _, ok := x.r.body["OPENAI_API_KEY"]; ok {
				t.Fatal("error carries a key")
			}
		}
		if c.body["failure_reason"] != "llm_configuration_incomplete" {
			t.Fatalf("422 failure_reason %v", c.body["failure_reason"])
		}
	})
}
