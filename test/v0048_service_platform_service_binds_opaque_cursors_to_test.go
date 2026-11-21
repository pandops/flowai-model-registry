package test

import "testing"

// service-platform: opaque cursors bound to their read (models-list is
// model-registry's only paginated read; the partition is its filter).
//
// Error codes: the platform rule answers every rejection with
// invalid_pagination, but model-registry's own scenario "Operator pages
// through models" answers a tampered or foreign-team cursor with
// invalid_request. The capability spec wins (verification/agents.md rule 13),
// so those two cases expect invalid_request; a changed filter and a bad limit
// keep invalid_pagination.
func TestV0048ServicePlatformBindsOpaqueCursors(t *testing.T) {
	setup := func(t *testing.T) (*harness, string) {
		h := newHarness(t)
		for _, n := range []string{"a", "b", "c"} {
			h.createModel(3, n, nil)
			h.createModel(4, n, nil)
		}
		r := h.ui("models-list", 3, map[string]any{"limit": 1}, nil, "")
		return h, r.json(t)["next_cursor"].(string)
	}
	rejected := func(t *testing.T, h *harness, team int64, params map[string]any, code string) {
		t.Helper()
		h.store.SetDown(true) // any row read now fails with 503
		defer h.store.SetDown(false)
		r := h.ui("models-list", team, params, nil, "")
		if r.status != 400 || r.code(t) != code {
			t.Fatalf("%v: %d %s, want 400 %s", params, r.status, r.body, code)
		}
	}
	invalid := func(t *testing.T, h *harness, team int64, params map[string]any) {
		t.Helper()
		rejected(t, h, team, params, "invalid_pagination")
	}
	invalidRequest := func(t *testing.T, h *harness, team int64, params map[string]any) {
		t.Helper()
		rejected(t, h, team, params, "invalid_request")
	}

	t.Run("Forward page advances", func(t *testing.T) {
		h, cur := setup(t)
		r := h.ui("models-list", 3, map[string]any{"limit": 1, "cursor": cur}, nil, "")
		if names := listNames(t, r); len(names) != 1 || names[0] != "b" || r.json(t)["next_cursor"] == nil {
			t.Fatalf("second page %s", r.body)
		}
	})

	t.Run("Tampered cursor", func(t *testing.T) {
		h, cur := setup(t)
		for i := 0; i < len(cur); i += max(1, len(cur)/5) {
			b := []byte(cur)
			b[i] ^= 0x01
			invalidRequest(t, h, 3, map[string]any{"limit": 1, "cursor": string(b)})
		}
	})

	t.Run("Cursor reused for another operation or team", func(t *testing.T) {
		h, cur := setup(t)
		invalidRequest(t, h, 4, map[string]any{"limit": 1, "cursor": cur})
	})

	t.Run("Changed filter", func(t *testing.T) {
		h, cur := setup(t)
		invalid(t, h, 3, map[string]any{"limit": 1, "cursor": cur, "archived": true})
	})

	t.Run("Limit out of range", func(t *testing.T) {
		h, _ := setup(t)
		for _, l := range []any{0, 201, "abc", "1.5", -1} {
			invalid(t, h, 3, map[string]any{"limit": l})
		}
	})
}
