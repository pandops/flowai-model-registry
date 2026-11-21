package test

import (
	"slices"
	"testing"
)

// service-platform: team ownership in normalized form.
func TestV0048ServicePlatformStoresTeamOwnership(t *testing.T) {
	t.Run("Team-owned parent keeps its team", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		body := modelUpdateBody(true)
		body["team_id"] = 4
		h.ui("model-update", 3, map[string]any{"name": "m1"}, body, h.etag(3, "m1"))
		h.setPaused(3, "m1", true)
		h.archive(3, "m1")
		h.unarchive(3, "m1")
		m := h.store.Models()
		if len(m) != 1 || m[0].CreatorTeamID == nil || *m[0].CreatorTeamID != 3 || !slices.Equal(h.store.OwnersOf("3/m1"), []int64{3}) {
			t.Fatalf("ownership changed: %+v", m)
		}
		if !jsonInt(h.llmModel("3/m1").ModelInfo["flowai_team_id"], 3) {
			t.Fatal("LiteLLM team metadata changed")
		}
	})

	t.Run("Child references a foreign-team parent", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(4, "m1", nil)
		k := claim(3, 1010, 1, 1)
		h.readyClaim(7, k, 21, []string{"4/m1"}, nil, nil) // a team 3 Flow naming a team 4 model
		mustTemporary(t, h.check(7, k, nil), "no_active_model")
		if len(h.store.Snapshots()) != 0 {
			t.Fatal("snapshot pinned a foreign model")
		}
	})

	t.Run("Display metadata never authorizes", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		hdr := uiHeaders(4, h.nextRequestID())
		hdr["team_name"] = "Operations" // team 3's display name
		r := h.mustSend("model-registry.ui.model-get.pub", hdr, map[string]any{"params": map[string]any{"name": "m1"}})
		if r.status != 404 {
			t.Fatalf("team 4 with team 3's name: %d %s", r.status, r.body)
		}
	})
}
