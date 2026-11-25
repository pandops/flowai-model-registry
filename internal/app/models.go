package app

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/pandops/flowai-model-registry/internal/broker"
	"github.com/pandops/flowai-model-registry/internal/litellm"
	"github.com/pandops/flowai-model-registry/internal/store"
)

var (
	namePattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)
	providerPattern = regexp.MustCompile(`^[a-z0-9_]{1,64}$`)
)

// opError is an operator-visible failure.
type opError struct {
	status int
	code   string
	extra  map[string]any
}

func (e *opError) Error() string { return e.code }

var (
	errNotFound    = &opError{status: 404, code: "not_found"}
	errGateway     = &opError{status: 503, code: "model_gateway_unavailable"}
	errStore       = &opError{status: 503, code: "service_unavailable"}
	errStale       = &opError{status: 412, code: "precondition_failed"}
	errNoIfMatch   = &opError{status: 428, code: "precondition_required"}
	errArchived    = &opError{status: 409, code: "model_archived"}
	errInProgress  = &opError{status: 409, code: "model_operation_in_progress"}
	errNameTaken   = &opError{status: 409, code: "model_name_conflict"}
	errNotEditable = &opError{status: 409, code: "model_not_editable"}
	errInvalid     = &opError{status: 400, code: "invalid_request"}
	errPagination  = &opError{status: 400, code: "invalid_pagination"}
	errIdempotent  = errors.New("already in the requested state")
)

func asOpError(err error) *opError {
	var oe *opError
	if errors.As(err, &oe) {
		return oe
	}
	if errors.Is(err, litellm.ErrUnavailable) {
		return errGateway
	}
	return errStore
}

func (e *opError) reply(rid *int64) broker.Reply {
	return broker.Reply{Status: e.status, Body: errBody(e.code, e.code, rid, e.extra)}
}

// modelState is a verified owned model with its LiteLLM record.
type modelState struct {
	m          store.Model
	rec        litellm.Model
	op         *store.Operation
	unfinished int
}

func intOf(v any) (int64, bool) {
	switch n := v.(type) {
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	case float64:
		if n == float64(int64(n)) {
			return int64(n), true
		}
	case int64:
		return n, true
	case int:
		return int64(n), true
	}
	return 0, false
}

// matches reports whether a LiteLLM record agrees with the local row.
func matches(m store.Model, r litellm.Model) bool {
	if r.ModelName != m.Alias {
		return false
	}
	team, hasTeam := r.ModelInfo["flowai_team_id"]
	if m.CreatorTeamID != nil {
		t, ok := intOf(team)
		if !hasTeam || !ok || t != *m.CreatorTeamID || !strings.HasPrefix(m.Alias, strconv.FormatInt(t, 10)+"/") {
			return false
		}
	} else if hasTeam || !strings.HasPrefix(m.Alias, "system/") {
		return false
	}
	id, ok := intOf(r.ModelInfo["flowai_model_id"])
	return ok && id == m.ModelID
}

// fetch reads and verifies a model's LiteLLM record: a missing or disagreeing
// record is not found.
func (a *App) fetch(ctx context.Context, m store.Model) (litellm.Model, error) {
	r, err := a.llm.GetModel(ctx, m.LiteLLMModelID)
	switch {
	case errors.Is(err, litellm.ErrNotFound):
		return litellm.Model{}, errNotFound
	case err != nil:
		return litellm.Model{}, errGateway
	case !matches(m, r):
		return litellm.Model{}, errNotFound
	}
	return r, nil
}

// etagOf is a digest of the LiteLLM record.
func etagOf(r litellm.Model) string {
	raw, _ := json.Marshal(map[string]any{"n": r.ModelName, "p": r.LiteLLMParams, "i": r.ModelInfo})
	sum := sha256.Sum256(raw)
	return `"` + b64.EncodeToString(sum[:18]) + `"`
}

func paused(r litellm.Model) bool { v, _ := r.ModelInfo["flowai_paused"].(bool); return v }

func availabilityOf(m store.Model, r litellm.Model, ok bool) string {
	switch {
	case !ok || !matches(m, r):
		return store.AvailabilityUnavailable
	case m.ArchivedAt != nil:
		if v, _ := r.ModelInfo["flowai_archived"].(bool); v {
			return store.AvailabilityArchived
		}
		return store.AvailabilityUnavailable
	case paused(r):
		return store.AvailabilityPaused
	}
	return store.AvailabilityActive
}

// view is the public model object for team.
func (a *App) view(ctx context.Context, team int64, s modelState) map[string]any {
	m, r := s.m, s.rec
	provider, providerModel := "", ""
	if full, _ := r.LiteLLMParams["model"].(string); full != "" {
		if i := strings.Index(full, "/"); i >= 0 {
			provider, providerModel = full[:i], full[i+1:]
		} else {
			providerModel = full
		}
	}
	var apiBase any
	if v, ok := r.LiteLLMParams["api_base"].(string); ok && v != "" {
		apiBase = v
	}
	cred, _ := r.LiteLLMParams["litellm_credential_name"].(string)
	own := m.CreatorTeamID != nil && *m.CreatorTeamID == team
	editable := own && m.ArchivedAt == nil
	pricing, status := pricingOf(r.LiteLLMParams)
	var blocking any = []any{}
	canArchive := false
	if editable {
		blocking = nil
		if flows, ok := a.soleAliasFlows(ctx, team, m.Alias); ok {
			blocking = flows
			canArchive = s.op == nil && len(flows) == 0
		}
	}
	return map[string]any{
		"name": m.Name, "model_alias": m.Alias, "provider": provider, "provider_model": providerModel, "api_base": apiBase,
		"credential_configured": cred != "", "created_at": ts(m.CreatedAt), "updated_at": ts(m.UpdatedAt), "model_id": m.ModelID,
		"editable": editable, "unfinished_task_count": s.unfinished, "can_archive": canArchive, "paused": paused(r),
		"created_by": m.CreatedBy, "archived_at": tsPtr(m.ArchivedAt), "can_unarchive": own && m.ArchivedAt != nil && s.op == nil,
		"archive_blocking_flows": blocking, "pricing": pricing, "pricing_status": status,
	}
}

// soleAliasFlows asks flow-registry which Flows list only alias.
func (a *App) soleAliasFlows(ctx context.Context, team int64, alias string) ([]any, bool) {
	r, err := a.call(ctx, "flow-registry.service.alias-usage.pub", team, map[string]any{"team_id": team, "alias": alias})
	if err != nil || r.Status != 200 {
		return nil, false
	}
	var out struct {
		Sole []struct {
			FlowID int64  `json:"flow_id"`
			Name   string `json:"name"`
		} `json:"sole_alias_flows"`
	}
	if json.Unmarshal(r.Body, &out) != nil {
		return nil, false
	}
	flows := []any{}
	for _, f := range out.Sole {
		flows = append(flows, map[string]any{"flow_id": f.FlowID, "name": f.Name})
	}
	return flows, true
}

func (a *App) withETag(status int, s modelState, body map[string]any) broker.Reply {
	r := jsonReply(status, body)
	r.Header = map[string]string{"ETag": etagOf(s.rec)}
	return r
}

// loadLocal reads the local state of an owned model inside tx.
func loadLocal(tx store.Tx, m store.Model) (modelState, error) {
	op, err := tx.Operation(m.Key)
	if err != nil {
		return modelState{}, err
	}
	n, err := tx.UnfinishedCount(m.Key)
	if err != nil {
		return modelState{}, err
	}
	return modelState{m: m, op: op, unfinished: n}, nil
}

func ownedByName(tx store.Tx, team int64, name string, lock bool) (store.Model, error) {
	if !namePattern.MatchString(name) {
		return store.Model{}, errNotFound
	}
	m, err := tx.OwnedModelByName(team, name)
	if errors.Is(err, store.ErrNotFound) {
		return store.Model{}, errNotFound
	}
	if err != nil {
		return store.Model{}, err
	}
	if lock {
		if m, err = tx.ModelByKey(m.Key, true); err != nil {
			return store.Model{}, err
		}
	}
	return *m, nil
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

func (a *App) modelGet(ctx context.Context, c uiCtx, req request, _ map[string]string) broker.Reply {
	name, _ := rawString(req.Params["name"])
	var s modelState
	err := a.deps.Store.Tx(ctx, "model-get", func(tx store.Tx) error {
		m, err := ownedByName(tx, c.team, name, false)
		if err != nil {
			return err
		}
		s, err = loadLocal(tx, m)
		return err
	})
	if err == nil {
		s.rec, err = a.fetch(ctx, s.m)
	}
	if err != nil {
		e := asOpError(err)
		a.auditRejected(ctx, c, "model-get", name, e.code, nil)
		return e.reply(c.rid())
	}
	return a.withETag(200, s, a.view(ctx, c.team, s))
}

func (a *App) modelsList(ctx context.Context, c uiCtx, req request, _ map[string]string) broker.Reply {
	fail := func(e *opError) broker.Reply {
		a.auditRejected(ctx, c, "models-list", "", e.code, nil)
		return e.reply(c.rid())
	}
	archived := false
	if raw, ok := req.Params["archived"]; ok {
		if b, ok := rawBool(raw); ok {
			archived = b
		} else if s, _ := rawString(raw); s == "true" || s == "false" {
			archived = s == "true"
		} else {
			return fail(errInvalid)
		}
	}
	limit := 50
	if raw, ok := req.Params["limit"]; ok {
		n, err := strconv.Atoi(strings.TrimSpace(string(raw)))
		if err != nil || n < 1 || n > 200 {
			return fail(errPagination)
		}
		limit = n
	}
	after := ""
	if raw, ok := req.Params["cursor"]; ok {
		s, _ := rawString(raw)
		cur, cerr := a.readCursor(s, c.team, "models-list", archived)
		if cerr != nil {
			return fail(cerr)
		}
		after = cur.After
	}
	var states []modelState
	err := a.deps.Store.Tx(ctx, "models-list", func(tx store.Tx) error {
		rows, err := tx.OwnedModels(c.team, archived, after, limit+1)
		if err != nil {
			return err
		}
		for _, m := range rows {
			s, err := loadLocal(tx, m)
			if err != nil {
				return err
			}
			states = append(states, s)
		}
		return nil
	})
	if err != nil {
		return fail(errStore)
	}
	var next any
	if len(states) > limit {
		states = states[:limit]
		next = a.signCursor(cursor{Team: c.team, Endpoint: "models-list", Archived: archived, After: states[limit-1].m.Name})
	}
	items := []any{}
	if len(states) > 0 {
		recs, err := a.llm.ListModels(ctx)
		if err != nil {
			return fail(errGateway)
		}
		byID := map[string]litellm.Model{}
		for _, r := range recs {
			byID[r.ID] = r
		}
		for _, s := range states {
			r, ok := byID[s.m.LiteLLMModelID]
			if !ok || !matches(s.m, r) {
				continue
			}
			s.rec = r
			items = append(items, a.view(ctx, c.team, s))
		}
	}
	return jsonReply(200, map[string]any{"items": items, "next_cursor": next})
}

// ---------------------------------------------------------------------------
// Write bodies
// ---------------------------------------------------------------------------

type modelInput struct {
	name, provider, providerModel string
	apiBase                       *string // nil: absent
	apiBaseNull                   bool
	apiKey                        string
	clearKey                      bool
	paused                        *bool
	pricing                       *[2]*big.Rat
}

func (a *App) parseModelBody(raw json.RawMessage, create bool) (modelInput, error) {
	f, err := strictBody(raw)
	if err != nil {
		return modelInput{}, errInvalid
	}
	allowed := []string{"provider", "provider_model", "api_base", "api_key", "paused", "pricing"}
	if create {
		allowed = append(allowed, "name")
	} else {
		allowed = append(allowed, "clear_api_key")
	}
	if !onlyKeys(f, allowed...) {
		return modelInput{}, errInvalid
	}
	var in modelInput
	var ok bool
	if create {
		if in.name, ok = rawString(f["name"]); !ok || !namePattern.MatchString(in.name) {
			return modelInput{}, errInvalid
		}
	}
	if in.provider, ok = rawString(f["provider"]); !ok || !providerPattern.MatchString(in.provider) {
		return modelInput{}, errInvalid
	}
	if in.providerModel, ok = rawString(f["provider_model"]); !ok || in.providerModel == "" || len(in.providerModel) > 200 {
		return modelInput{}, errInvalid
	}
	if v, present := f["api_base"]; present {
		if isNull(v) {
			in.apiBaseNull = true
		} else {
			s, ok := rawString(v)
			if !ok || !a.allowedBase(s) {
				return modelInput{}, errInvalid
			}
			in.apiBase = &s
		}
	}
	if v, present := f["api_key"]; present {
		if in.apiKey, ok = rawString(v); !ok || in.apiKey == "" || len(in.apiKey) > 4096 {
			return modelInput{}, errInvalid
		}
	}
	if v, present := f["clear_api_key"]; present {
		if in.clearKey, ok = rawBool(v); !ok {
			return modelInput{}, errInvalid
		}
	}
	if in.clearKey && in.apiKey != "" {
		return modelInput{}, errInvalid
	}
	if v, present := f["paused"]; present {
		b, ok := rawBool(v)
		if !ok {
			return modelInput{}, errInvalid
		}
		in.paused = &b
	}
	if v, present := f["pricing"]; present {
		p, err := strictBody(v)
		if err != nil || len(p) != 2 || !onlyKeys(p, "input_usd_per_million_tokens", "output_usd_per_million_tokens") {
			return modelInput{}, errInvalid
		}
		var pair [2]*big.Rat
		for i, k := range []string{"input_usd_per_million_tokens", "output_usd_per_million_tokens"} {
			s, ok := rawString(p[k])
			if !ok {
				return modelInput{}, errInvalid
			}
			if pair[i], ok = parsePrice(s); !ok {
				return modelInput{}, errInvalid
			}
		}
		in.pricing = &pair
	}
	return in, nil
}

// allowedBase accepts an HTTPS api_base whose origin is allowlisted.
func (a *App) allowedBase(s string) bool {
	u, err := url.Parse(s)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
		return false
	}
	origin := u.Scheme + "://" + u.Host
	for _, o := range a.deps.Config.LiteLLM.APIBaseAllowedOrigins {
		if strings.TrimRight(o, "/") == origin {
			return true
		}
	}
	return false
}

func priceParams(params map[string]any, p *[2]*big.Rat) {
	if p == nil {
		return
	}
	params["input_cost_per_token"] = perToken(p[0])
	params["output_cost_per_token"] = perToken(p[1])
}

// confirmed reports whether a readback holds every written value; a nil
// written value must be absent.
func confirmed(r litellm.Model, params, info map[string]any) bool {
	check := func(got map[string]any, want map[string]any) bool {
		for k, v := range want {
			g, has := got[k]
			if v == nil {
				if has && g != nil {
					return false
				}
				continue
			}
			if !has || !sameValue(g, v) {
				return false
			}
		}
		return true
	}
	return check(r.LiteLLMParams, params) && check(r.ModelInfo, info)
}

func sameValue(a, b any) bool {
	_, aStr := a.(string)
	_, bStr := b.(string)
	if !aStr && !bStr {
		if _, ok := a.(bool); !ok {
			if x, ok1 := numberRat(a); ok1 {
				if y, ok2 := numberRat(b); ok2 {
					return x.Cmp(y) == 0
				}
			}
		}
	}
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

func numberRat(v any) (*big.Rat, bool) {
	switch n := v.(type) {
	case int64:
		return new(big.Rat).SetInt64(n), true
	case int:
		return new(big.Rat).SetInt64(int64(n)), true
	case json.Number, float64:
		return upstreamRat(n)
	}
	return nil, false
}

func credentialName(team, model int64) string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("flowai-%d-%d-%s", team, model, hex.EncodeToString(b))
}

// ---------------------------------------------------------------------------
// Create
// ---------------------------------------------------------------------------

func (a *App) modelCreate(ctx context.Context, c uiCtx, req request, _ map[string]string) broker.Reply {
	if len(req.Params) > 0 {
		a.auditRejected(ctx, c, "model-create", "", "invalid_request", nil)
		return errInvalid.reply(c.rid())
	}
	in, err := a.parseModelBody(req.Body, true)
	if err != nil {
		a.auditRejected(ctx, c, "model-create", "", "invalid_request", nil)
		return errInvalid.reply(c.rid())
	}
	alias := strconv.FormatInt(c.team, 10) + "/" + in.name
	team := c.team
	var (
		s                    modelState
		comp                 *store.Compensation
		availability, cred   string
		createdID, uncertain = "", false
	)
	err = a.deps.Store.Tx(ctx, store.TxModelCreate, func(tx store.Tx) error {
		comp, createdID, uncertain, cred = nil, "", false, ""
		if _, err := tx.ModelByAlias(alias, false); err == nil {
			return errNameTaken
		} else if !errors.Is(err, store.ErrNotFound) {
			return err
		}
		pending, err := tx.Compensations()
		if err != nil {
			return err
		}
		for _, p := range pending {
			if p.Alias == alias {
				return errNameTaken
			}
		}
		id, err := tx.NextTeamCounter(team, store.CounterModel)
		if err != nil {
			return err
		}
		comp = &store.Compensation{TeamID: team, ModelID: id, Alias: alias, CreatedAt: a.now()}
		params := map[string]any{"model": in.provider + "/" + in.providerModel}
		if in.apiBase != nil {
			params["api_base"] = *in.apiBase
		}
		if in.apiKey != "" {
			cred = credentialName(team, id)
			if err := a.llm.CreateCredential(ctx, cred, in.apiKey); err != nil {
				if errors.Is(err, litellm.ErrUnavailable) {
					comp.CredentialName = cred
				}
				return errGateway
			}
			comp.CredentialName = cred
			params["litellm_credential_name"] = cred
		}
		priceParams(params, in.pricing)
		isPaused := in.paused != nil && *in.paused
		info := map[string]any{"flowai_team_id": team, "flowai_model_id": id, "flowai_paused": isPaused, "flowai_archived": false}
		createdID, err = a.llm.CreateModel(ctx, alias, params, info)
		if err != nil {
			uncertain = errors.Is(err, litellm.ErrUnavailable)
			if errors.Is(err, litellm.ErrRejected) {
				return errInvalid
			}
			return errGateway
		}
		comp.LiteLLMModelID = createdID
		rec, err := a.llm.GetModel(ctx, createdID)
		if err != nil || !confirmed(rec, params, info) {
			return errGateway
		}
		now := a.now()
		availability = store.AvailabilityActive
		if isPaused {
			availability = store.AvailabilityPaused
		}
		m := &store.Model{CreatorTeamID: &team, ModelID: id, CreatedBy: store.CreatedByTeam, Name: in.name, Alias: alias,
			LiteLLMModelID: createdID, Availability: availability, CreatedAt: now, UpdatedAt: now}
		if !matches(*m, rec) {
			return errGateway
		}
		if err := tx.InsertModel(m); err != nil {
			if errors.Is(err, store.ErrConflict) {
				return errNameTaken
			}
			return err
		}
		if err := tx.AddOwner(m.Key, team); err != nil {
			return err
		}
		if err := a.emit(tx, team, "model-availability-changed", map[string]any{"model_id": id, "model_alias": alias,
			"availability": availability, "previous_availability": nil}); err != nil {
			return err
		}
		if err := a.writeAudit(tx, c, auditFields{action: "model-create", outcome: "succeeded", name: in.name, resourceID: &id, paused: in.paused}); err != nil {
			return err
		}
		s = modelState{m: *m, rec: rec}
		return nil
	})
	if err != nil {
		if comp != nil && (createdID != "" || uncertain || comp.CredentialName != "") {
			if cerr := a.deps.Store.Tx(ctx, "model-create-compensation", func(tx store.Tx) error { return tx.AddCompensation(*comp) }); cerr != nil {
				a.log.Error("model create compensation not recorded", "model_alias", alias, "error", cerr)
			}
		}
		e := asOpError(err)
		a.auditRejected(ctx, c, "model-create", in.name, e.code, nil)
		return e.reply(c.rid())
	}
	return a.withETag(201, s, a.view(ctx, c.team, s))
}

// ---------------------------------------------------------------------------
// Update, pause and resume
// ---------------------------------------------------------------------------

// lockedOwn loads and locks the team's own model, verifies its LiteLLM
// record and compares the ETag.
func (a *App) lockedOwn(ctx context.Context, tx store.Tx, team int64, name, ifMatch string) (modelState, error) {
	m, err := ownedByName(tx, team, name, true)
	if err != nil {
		return modelState{}, err
	}
	if m.CreatorTeamID == nil || *m.CreatorTeamID != team {
		return modelState{}, errNotEditable
	}
	s, err := loadLocal(tx, m)
	if err != nil {
		return modelState{}, err
	}
	if s.rec, err = a.fetch(ctx, m); err != nil {
		return modelState{}, err
	}
	if ifMatch == "" {
		return modelState{}, errNoIfMatch
	}
	if ifMatch != etagOf(s.rec) {
		return modelState{}, errStale
	}
	return s, nil
}

func (a *App) modelUpdate(ctx context.Context, c uiCtx, req request, hdr map[string]string) broker.Reply {
	name, _ := rawString(req.Params["name"])
	in, err := a.parseModelBody(req.Body, false)
	if err != nil {
		a.auditRejected(ctx, c, "model-update", name, "invalid_request", nil)
		return errInvalid.reply(c.rid())
	}
	var s modelState
	var changed bool
	err = a.deps.Store.Tx(ctx, store.TxModelUpdate, func(tx store.Tx) error {
		var err error
		if s, err = a.lockedOwn(ctx, tx, c.team, name, hdr["If-Match"]); err != nil {
			return err
		}
		if s.op != nil {
			return errInProgress
		}
		if s.m.ArchivedAt != nil {
			return errArchived
		}
		params := map[string]any{"model": in.provider + "/" + in.providerModel}
		if in.apiBase != nil {
			params["api_base"] = *in.apiBase
		} else if in.apiBaseNull {
			if _, has := s.rec.LiteLLMParams["api_base"]; has {
				params["api_base"] = nil
			}
		}
		cred, _ := s.rec.LiteLLMParams["litellm_credential_name"].(string)
		switch {
		case in.apiKey != "" && cred != "":
			err := a.llm.UpdateCredential(ctx, cred, in.apiKey)
			if errors.Is(err, litellm.ErrNotFound) {
				err = a.llm.CreateCredential(ctx, cred, in.apiKey)
			}
			if err != nil {
				return errGateway
			}
		case in.apiKey != "":
			cred = credentialName(c.team, s.m.ModelID)
			if err := a.llm.CreateCredential(ctx, cred, in.apiKey); err != nil {
				return errGateway
			}
			params["litellm_credential_name"] = cred
		case in.clearKey && cred != "":
			if err := a.llm.DeleteCredential(ctx, cred); err != nil && !errors.Is(err, litellm.ErrNotFound) {
				return errGateway
			}
			params["litellm_credential_name"] = nil
		}
		priceParams(params, in.pricing)
		info := map[string]any{}
		if in.paused != nil {
			info["flowai_paused"] = *in.paused
		}
		if err := a.llm.UpdateModel(ctx, s.m.LiteLLMModelID, params, info); err != nil {
			return errGateway
		}
		rec, err := a.llm.GetModel(ctx, s.m.LiteLLMModelID)
		if err != nil || !matches(s.m, rec) || !confirmed(rec, params, info) {
			return errGateway
		}
		s.rec = rec
		prev := s.m.Availability
		s.m.Availability = availabilityOf(s.m, rec, true)
		s.m.UpdatedAt = a.now()
		if err := tx.UpdateModel(&s.m); err != nil {
			return err
		}
		if changed = prev != s.m.Availability; changed {
			if err := a.emit(tx, c.team, "model-availability-changed", map[string]any{"model_id": s.m.ModelID, "model_alias": s.m.Alias,
				"availability": s.m.Availability, "previous_availability": prev}); err != nil {
				return err
			}
		}
		id := s.m.ModelID
		return a.writeAudit(tx, c, auditFields{action: "model-update", outcome: "succeeded", name: s.m.Name, resourceID: &id, paused: in.paused})
	})
	if err != nil {
		e := asOpError(err)
		var rid *int64
		if s.m.ModelID != 0 && e != errNotFound {
			id := s.m.ModelID
			rid = &id
		}
		a.auditRejected(ctx, c, "model-update", name, e.code, rid)
		return e.reply(c.rid())
	}
	return a.withETag(200, s, a.view(ctx, c.team, s))
}

// ---------------------------------------------------------------------------
// Archive and unarchive
// ---------------------------------------------------------------------------

func (a *App) modelArchive(ctx context.Context, c uiCtx, req request, hdr map[string]string) broker.Reply {
	return a.lifecycle(ctx, c, req, hdr, store.OperationArchive)
}

func (a *App) modelUnarchive(ctx context.Context, c uiCtx, req request, hdr map[string]string) broker.Reply {
	return a.lifecycle(ctx, c, req, hdr, store.OperationUnarchive)
}

func (a *App) lifecycle(ctx context.Context, c uiCtx, req request, hdr map[string]string, kind string) broker.Reply {
	action := "model-" + kind
	name, _ := rawString(req.Params["name"])
	begin := store.TxArchiveBegin
	if kind == store.OperationUnarchive {
		begin = store.TxUnarchiveBegin
	}
	var s modelState
	var op store.Operation
	err := a.deps.Store.Tx(ctx, begin, func(tx store.Tx) error {
		var err error
		if s, err = a.lockedOwn(ctx, tx, c.team, name, hdr["If-Match"]); err != nil {
			return err
		}
		if s.op != nil {
			if s.op.Kind != kind {
				return errInProgress
			}
			op = *s.op
			return nil
		}
		if (kind == store.OperationArchive) == (s.m.ArchivedAt != nil) {
			return errIdempotent
		}
		op = store.Operation{ModelKey: s.m.Key, Kind: kind, TeamID: c.team, OperatorID: c.operator, RequestID: c.requestID, CreatedAt: a.now()}
		return tx.PutOperation(op)
	})
	if errors.Is(err, errIdempotent) {
		return broker.Reply{Status: 204}
	}
	if err == nil {
		err = a.runOperation(ctx, s.m, op, &c)
	}
	if err != nil {
		e := asOpError(err)
		var rid *int64
		if s.m.ModelID != 0 && e != errNotFound {
			id := s.m.ModelID
			rid = &id
		}
		a.auditRejected(ctx, c, action, name, e.code, rid)
		return e.reply(c.rid())
	}
	return broker.Reply{Status: 204}
}

// runOperation drives a pending archive or unarchive to completion. human
// is the operator request, or nil for background reconciliation.
func (a *App) runOperation(ctx context.Context, m store.Model, op store.Operation, human *uiCtx) error {
	if op.Kind == store.OperationArchive {
		return a.runArchive(ctx, m, op, human)
	}
	return a.runUnarchive(ctx, m, op, human)
}

// errOperationDone means the operation a request or reconciliation pass is
// driving was completed by someone else; its upstream steps must not run.
var errOperationDone = errors.New("operation completed elsewhere")

func sameOperation(cur *store.Operation, op store.Operation) bool {
	return cur != nil && cur.Kind == op.Kind && cur.TeamID == op.TeamID && cur.OperatorID == op.OperatorID && cur.RequestID == op.RequestID
}

// stillPending reports whether op is still the model's pending operation.
func (a *App) stillPending(ctx context.Context, m store.Model, op store.Operation) error {
	err := a.deps.Store.Tx(ctx, "operation-pending", func(tx store.Tx) error {
		cur, err := tx.Operation(m.Key)
		if err != nil {
			return err
		}
		if !sameOperation(cur, op) {
			return errOperationDone
		}
		return nil
	})
	if err != nil && !errors.Is(err, errOperationDone) {
		return errGateway
	}
	return err
}

// setLifecycleFlags writes the LiteLLM archive flags under the model lock
// and only while op is still pending, so a delayed request or pass cannot
// undo a later pause, resume, archive or unarchive.
func (a *App) setLifecycleFlags(ctx context.Context, m store.Model, op store.Operation, archived bool) error {
	err := a.deps.Store.Tx(ctx, "lifecycle-flags", func(tx store.Tx) error {
		if _, err := tx.ModelByKey(m.Key, true); err != nil {
			return err
		}
		cur, err := tx.Operation(m.Key)
		if err != nil {
			return err
		}
		if !sameOperation(cur, op) {
			return errOperationDone
		}
		info := map[string]any{"flowai_archived": archived, "flowai_paused": archived}
		if err := a.llm.UpdateModel(ctx, m.LiteLLMModelID, nil, info); err != nil {
			return errGateway
		}
		rec, err := a.llm.GetModel(ctx, m.LiteLLMModelID)
		if err != nil || !matches(m, rec) || !confirmed(rec, nil, info) {
			return errGateway
		}
		return nil
	})
	if err != nil && !errors.Is(err, errOperationDone) && !errors.Is(err, errGateway) {
		return errGateway
	}
	return err
}

func (a *App) runArchive(ctx context.Context, m store.Model, op store.Operation, human *uiCtx) error {
	if err := a.stillPending(ctx, m, op); err != nil {
		return doneIsSuccess(err)
	}
	r, err := a.call(ctx, "flow-registry.service.retire-alias.pub", op.TeamID, map[string]any{"team_id": op.TeamID, "alias": m.Alias, "operator_id": op.OperatorID})
	if err != nil {
		return errGateway
	}
	if r.Status == 409 {
		var body struct {
			Code  string `json:"code"`
			Flows []any  `json:"flows"`
		}
		_ = json.Unmarshal(r.Body, &body)
		if body.Code == "" {
			body.Code = "model_last_in_flow"
		}
		err := a.deps.Store.Tx(ctx, "archive-cancel", func(tx store.Tx) error {
			if err := tx.DeleteOperation(m.Key); err != nil {
				return err
			}
			if human == nil {
				return a.emit(tx, op.TeamID, "model-archive-reconciled", map[string]any{"model_id": m.ModelID, "model_alias": m.Alias, "outcome": "cancelled_model_last_in_flow"})
			}
			return nil
		})
		if err != nil {
			return errStore
		}
		flows := body.Flows
		if flows == nil {
			flows = []any{}
		}
		return &opError{status: 409, code: body.Code, extra: map[string]any{"flows": flows}}
	}
	if r.Status != 204 && r.Status != 200 {
		return errGateway
	}
	if err := a.setLifecycleFlags(ctx, m, op, true); err != nil {
		return doneIsSuccess(err)
	}
	return a.complete(ctx, m, op, human, store.TxArchiveComplete)
}

func (a *App) runUnarchive(ctx context.Context, m store.Model, op store.Operation, human *uiCtx) error {
	if err := a.setLifecycleFlags(ctx, m, op, false); err != nil {
		return doneIsSuccess(err)
	}
	r, err := a.call(ctx, "flow-registry.service.restore-alias.pub", op.TeamID, map[string]any{"team_id": op.TeamID, "alias": m.Alias})
	if err != nil || r.Status >= 300 {
		return errGateway
	}
	return a.complete(ctx, m, op, human, store.TxUnarchiveComplete)
}

// doneIsSuccess turns "completed elsewhere" into success: the request
// joined an operation another request or pass already finished.
func doneIsSuccess(err error) error {
	if errors.Is(err, errOperationDone) {
		return nil
	}
	return err
}

// complete commits the local markers, completes the operation and records
// the audit (operator) or reconciliation event (background).
func (a *App) complete(ctx context.Context, m store.Model, op store.Operation, human *uiCtx, txName string) error {
	err := a.deps.Store.Tx(ctx, txName, func(tx store.Tx) error {
		cur, err := tx.ModelByKey(m.Key, true)
		if err != nil {
			return err
		}
		pending, err := tx.Operation(m.Key)
		if err != nil {
			return err
		}
		if pending == nil || pending.Kind != op.Kind {
			return nil // completed elsewhere
		}
		prev := cur.Availability
		now := a.now()
		if op.Kind == store.OperationArchive {
			cur.ArchivedAt = &now
			cur.Availability = store.AvailabilityArchived
		} else {
			cur.ArchivedAt = nil
			cur.Availability = store.AvailabilityActive
		}
		cur.UpdatedAt = now
		if err := tx.UpdateModel(cur); err != nil {
			return err
		}
		if err := tx.DeleteOperation(m.Key); err != nil {
			return err
		}
		if prev != cur.Availability {
			if err := a.emit(tx, op.TeamID, "model-availability-changed", map[string]any{"model_id": cur.ModelID, "model_alias": cur.Alias,
				"availability": cur.Availability, "previous_availability": prev}); err != nil {
				return err
			}
		}
		if human != nil {
			id := cur.ModelID
			return a.writeAudit(tx, *human, auditFields{action: "model-" + op.Kind, outcome: "succeeded", name: cur.Name, resourceID: &id})
		}
		fields := map[string]any{"model_id": cur.ModelID, "model_alias": cur.Alias}
		kind := "model-unarchive-reconciled"
		if op.Kind == store.OperationArchive {
			kind, fields["outcome"] = "model-archive-reconciled", "archived"
		}
		return a.emit(tx, op.TeamID, kind, fields)
	})
	if err != nil {
		return errGateway
	}
	return nil
}
