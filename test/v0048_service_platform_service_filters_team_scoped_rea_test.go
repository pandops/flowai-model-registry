package test

import (
	"slices"
	"testing"
)

// service-platform: team filter before shaping. "Concurrent team streams" is
// not applicable to model-registry (no streams).
func TestV0048ServicePlatformFiltersTeamScopedReads(t *testing.T) {
	t.Run("Teams share filter values", func(t *testing.T) {
		h := newHarness(t)
		for _, n := range []string{"a", "b", "c"} {
			h.createModel(3, n, nil)
			h.createModel(4, n, nil)
		}
		h.createModel(4, "d", nil)
		h.archive(3, "c")
		h.archive(4, "c")
		h.archive(4, "d")
		r := h.ui("models-list", 3, map[string]any{"limit": 1}, nil, "")
		var all []string
		cur := r.json(t)["next_cursor"]
		all = append(all, listNames(t, r)...)
		for cur != nil {
			r = h.ui("models-list", 3, map[string]any{"limit": 1, "cursor": cur}, nil, "")
			all = append(all, listNames(t, r)...)
			cur = r.json(t)["next_cursor"]
		}
		if !slices.Equal(all, []string{"a", "b"}) {
			t.Fatalf("team 3 current models %v", all)
		}
		for _, it := range items(t, h.ui("models-list", 3, map[string]any{"archived": true}, nil, "")) {
			if it["model_alias"] != "3/c" {
				t.Fatalf("team 3 archived list has %v", it["model_alias"])
			}
		}
		st := h.service("flows-status", "3", map[string]any{"team_id": 3, "flows": []any{map[string]any{"flow_id": 21, "aliases": []string{"3/a", "4/a"}}}}).json(t)
		models := st["flows"].([]any)[0].(map[string]any)["models"].([]any)
		if models[1].(map[string]any)["availability"] != "no_access" {
			t.Fatalf("team 4 alias for team 3: %v", models[1])
		}
	})
}
