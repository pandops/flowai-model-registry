package test

import (
	"errors"
	"slices"
	"testing"

	"github.com/pandops/flowai-model-registry/internal/broker"
)

// Requirement: model-registry reserves the model launch-parameter names.
// env-registry enforces the reservation; model-registry's part is that it
// validates no launch-parameter write and alone supplies the three values.
func TestV0048ModelRegistryReservesModelLaunchParameterNames(t *testing.T) {
	t.Run("Operator stores an OpenAI key as a launch parameter", func(t *testing.T) {
		h := newHarness(t)
		for _, op := range []string{"launch-parameters-create", "environment-update", "secret-create"} {
			_, err := h.send("model-registry.ui."+op+".pub", uiHeaders(3, 1), map[string]any{"params": map[string]any{}, "body": map[string]any{"name": "OPENAI_API_KEY"}})
			if !errors.Is(err, broker.ErrNoResponder) {
				t.Fatalf("model-registry answers %s: %v", op, err)
			}
		}
	})

	t.Run("Task receives model values only from model-registry", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "gpt-4o", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"3/gpt-4o"}, nil, nil)
		h.mustCheckOK(7, k, "3/gpt-4o")
		r := h.keyEdge("1010", h.token(k, 7))
		var names []string
		for n := range r.body {
			names = append(names, n)
		}
		slices.Sort(names)
		if r.status != 200 || !slices.Equal(names, []string{"OPENAI_API_KEY", "OPENAI_BASE_URL", "OPENAI_MODEL"}) || r.body["OPENAI_MODEL"] != "3/gpt-4o" {
			t.Fatalf("key edge: %d %s", r.status, r.raw)
		}
	})
}
