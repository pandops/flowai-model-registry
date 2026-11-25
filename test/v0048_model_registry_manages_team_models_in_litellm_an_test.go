package test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/pandops/flowai-model-registry/internal/store"
)

// Requirement: model-registry manages team models in LiteLLM and stores only
// their ownership.
func TestV0048ModelRegistryManagesTeamModelsInLiteLLM(t *testing.T) {
	t.Run("Operator creates a model", func(t *testing.T) {
		h := newHarness(t)
		for i := 1; i <= 4; i++ {
			h.createModel(3, fmt.Sprintf("m%d", i), nil)
		}
		r := h.createModel(3, "gpt-4o", map[string]any{"api_key": "sk-provider-secret-1"})
		body := r.json(t)
		if body["model_alias"] != "3/gpt-4o" || num(body["model_id"]) != 5 || body["credential_configured"] != true || body["created_by"] != "team" {
			t.Fatalf("create reply = %s", r.body)
		}
		m := h.llmModel("3/gpt-4o")
		if !jsonInt(m.ModelInfo["flowai_team_id"], 3) || !jsonInt(m.ModelInfo["flowai_model_id"], 5) {
			t.Fatalf("LiteLLM model_info = %v", m.ModelInfo)
		}
		if _, ok := m.ModelInfo["team_id"]; ok {
			t.Fatalf("model_info carries LiteLLM's reserved team_id: %v", m.ModelInfo)
		}
		var row *store.Model
		for _, mm := range h.store.Models() {
			if mm.Alias == "3/gpt-4o" {
				row = &mm
			}
		}
		if row == nil || row.CreatorTeamID == nil || *row.CreatorTeamID != 3 || row.ModelID != 5 || row.CreatedBy != store.CreatedByTeam {
			t.Fatalf("models row = %+v", row)
		}
		if owners := h.store.OwnersOf("3/gpt-4o"); !slices.Equal(owners, []int64{3}) {
			t.Fatalf("owners = %v, want [3]", owners)
		}
		if bytesContain(r.body, "sk-provider-secret-1") {
			t.Fatal("reply returns the provider key")
		}
		h.assertNoSecret("sk-provider-secret-1")
	})

	t.Run("model-registry refuses mismatched ownership", func(t *testing.T) {
		h := newHarness(t)
		h.createModel(3, "m1", nil)
		h.createModel(3, "m2", nil)
		h.createModel(3, "m3", nil)
		unknown := h.getModel(3, "nope")
		if unknown.status != 404 {
			t.Fatalf("unknown name: %d", unknown.status)
		}
		et2, et3 := h.etag(3, "m2"), h.etag(3, "m3")
		h.llm.InsertModelDirectly("3/ghost", map[string]any{"model": "openai/gpt-4o"}, map[string]any{"flowai_team_id": 3, "flowai_model_id": 99})
		h.llm.PatchModelDirectly("3/m2", nil, map[string]any{"flowai_team_id": 4})
		h.llm.PatchModelDirectly("3/m3", nil, map[string]any{"flowai_model_id": 77})
		before := h.managementWrites()
		cases := []struct {
			team       int64
			name, etag string
		}{{3, "ghost", `"AAAAAAAAAAAAAAAAAAAAAAAA"`}, {3, "m2", et2}, {3, "m3", et3}, {4, "m1", `"AAAAAAAAAAAAAAAAAAAAAAAA"`}}
		for _, c := range cases {
			g := h.getModel(c.team, c.name)
			if g.status != 404 || g.code(t) != unknown.code(t) {
				t.Fatalf("team %d get %s = %d %s, want the unknown-name 404", c.team, c.name, g.status, g.body)
			}
			u := h.ui("model-update", c.team, map[string]any{"name": c.name}, modelUpdateBody(true), c.etag)
			if u.status != 404 || u.code(t) != unknown.code(t) {
				t.Fatalf("team %d update %s = %d %s, want the unknown-name 404", c.team, c.name, u.status, u.body)
			}
		}
		if got := listNames(t, h.ui("models-list", 3, nil, nil, "")); !slices.Equal(got, []string{"m1"}) {
			t.Fatalf("team 3 lists %v, want only m1", got)
		}
		if h.managementWrites() != before {
			t.Fatal("a mismatched model was changed in LiteLLM")
		}
	})

	t.Run("model-registry stores integer model metadata in LiteLLM", func(t *testing.T) {
		h := newHarness(t)
		a := h.createModel(3, "gpt-4o", nil).json(t)
		b := h.createModel(4, "gpt-4o", nil).json(t)
		if num(a["model_id"]) != 1 || num(b["model_id"]) != 1 {
			t.Fatalf("model ids %v %v, want 1 and 1", a["model_id"], b["model_id"])
		}
		for _, c := range h.llm.CallsTo("POST /model/new") {
			info, _ := c.Body["model_info"].(map[string]any)
			team := int64(3)
			if c.Body["model_name"] == "4/gpt-4o" {
				team = 4
			}
			if !jsonInt(info["flowai_team_id"], team) || !jsonInt(info["flowai_model_id"], 1) {
				t.Fatalf("model/new model_info = %v, want JSON numbers team %d model 1", info, team)
			}
		}
		if m := h.llmModel("4/gpt-4o"); !jsonInt(m.ModelInfo["flowai_team_id"], 4) {
			t.Fatalf("4/gpt-4o info %v", m.ModelInfo)
		}
		// The same name addresses each team's own model only.
		if r := h.setPaused(4, "gpt-4o", true); r.status != 200 {
			t.Fatalf("team 4 pauses its gpt-4o: %d %s", r.status, r.body)
		}
		if h.llmModel("3/gpt-4o").ModelInfo["flowai_paused"] == true || h.getModel(3, "gpt-4o").json(t)["paused"] != false {
			t.Fatal("team 4's pause changed team 3's model")
		}
		if g := h.getModel(4, "gpt-4o").json(t); g["model_alias"] != "4/gpt-4o" {
			t.Fatalf("team 4 reads %v", g["model_alias"])
		}
	})

	t.Run("Administrator model has several owner teams", func(t *testing.T) {
		h := newHarness(t)
		h.seedAdminModel("gpt-4o", 3, 4)
		for _, team := range []int64{3, 4} {
			its := items(t, h.ui("models-list", team, nil, nil, ""))
			if len(its) != 1 || its[0]["model_alias"] != "system/gpt-4o" || its[0]["editable"] != false || its[0]["created_by"] != "admin" {
				t.Fatalf("team %d list = %v", team, its)
			}
			v := h.service("validate-models", fmt.Sprint(team), map[string]any{"team_id": team, "aliases": []string{"system/gpt-4o"}}).json(t)
			entry := v["aliases"].([]any)[0].(map[string]any)
			if entry["owned"] != true || entry["selectable"] != true {
				t.Fatalf("team %d validate-models = %v", team, entry)
			}
		}
		if its := items(t, h.ui("models-list", 5, nil, nil, "")); len(its) != 0 {
			t.Fatalf("team 5 sees %v", its)
		}
		if g := h.getModel(5, "gpt-4o"); g.status != 404 {
			t.Fatalf("team 5 reads the administrator model: %d", g.status)
		}
		v := h.service("validate-models", "5", map[string]any{"team_id": 5, "aliases": []string{"system/gpt-4o"}}).json(t)
		if e := v["aliases"].([]any)[0].(map[string]any); e["owned"] != false || e["selectable"] != false {
			t.Fatalf("team 5 validate-models = %v", e)
		}
	})

	t.Run("Operator pages through models", func(t *testing.T) {
		h := newHarness(t)
		names := []string{"e5", "a1", "d4", "b2", "c3"}
		for _, n := range names {
			h.createModel(3, n, nil)
		}
		h.createModel(4, "z9", nil)
		var got []string
		var cursor any
		var first string
		for page := 0; page < 5; page++ {
			params := map[string]any{"limit": 2}
			if cursor != nil {
				params["cursor"] = cursor
			}
			r := h.ui("models-list", 3, params, nil, "")
			if r.status != 200 {
				t.Fatalf("page %d: %d %s", page, r.status, r.body)
			}
			got = append(got, listNames(t, r)...)
			cursor = r.json(t)["next_cursor"]
			if page == 0 {
				first = cursor.(string)
			}
			if cursor == nil {
				break
			}
		}
		if !slices.Equal(got, []string{"a1", "b2", "c3", "d4", "e5"}) {
			t.Fatalf("pages = %v", got)
		}
		tampered := []byte(first)
		tampered[len(tampered)/2] ^= 0x01
		h.store.SetDown(true) // a row read would now fail with 503
		calls := len(h.llm.Calls())
		for _, c := range []struct {
			team   int64
			cursor string
		}{{3, string(tampered)}, {4, first}} {
			r := h.ui("models-list", c.team, map[string]any{"cursor": c.cursor}, nil, "")
			if r.status != 400 || r.code(t) != "invalid_request" {
				t.Fatalf("team %d cursor: %d %s, want 400 invalid_request", c.team, r.status, r.body)
			}
		}
		if len(h.llm.Calls()) != calls {
			t.Fatal("an invalid cursor read models from LiteLLM")
		}
	})

	t.Run("Commit fails after the LiteLLM create", func(t *testing.T) {
		h := newHarness(t)
		h.store.FailCommit(store.TxModelCreate, 1)
		r := h.ui("model-create", 3, nil, map[string]any{"name": "gpt-4o", "provider": "openai", "provider_model": "gpt-4o", "api_key": "sk-provider-secret-2"}, "")
		if r.status != 503 {
			t.Fatalf("create with failed commit: %d %s", r.status, r.body)
		}
		h.llm.FailNext("POST /model/delete", 1, false)
		h.mustReconcile() // LiteLLM refuses the first delete
		h.mustReconcile()
		if ms := h.llm.ModelByAlias("3/gpt-4o"); len(ms) != 0 {
			t.Fatalf("compensation left %d deployments", len(ms))
		}
		if len(h.events("model-create-compensated")) != 1 {
			t.Fatal("no model-create-compensated event")
		}
		h.createModel(3, "gpt-4o", nil)
		h.assertNoSecret("sk-provider-secret-2")
	})
}
