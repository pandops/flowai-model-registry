package mocks

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// LiteLLMModel is one deployment held by the fake.
type LiteLLMModel struct {
	ID            string
	ModelName     string
	LiteLLMParams map[string]any
	ModelInfo     map[string]any
}

// LiteLLMTeam is one team held by the fake.
type LiteLLMTeam struct {
	ID             string
	Alias          string
	Metadata       map[string]any
	MaxBudget      *float64
	BudgetDuration string
	BudgetResetAt  *time.Time
	RPMLimit       *int64
	TPMLimit       *int64
	Spend          float64
}

// LiteLLMKey is one virtual key held by the fake.
type LiteLLMKey struct {
	Key       string
	Token     string
	Models    []string
	TeamID    string
	MaxBudget *float64
	RPMLimit  *int64
	TPMLimit  *int64
	Metadata  map[string]any
	ExpiresAt time.Time
	Spend     float64
	Revoked   bool
}

// Call is one management request the fake received.
type Call struct {
	Method string
	Path   string
	Body   map[string]any
}

type failure struct {
	prefix  string
	n       int
	after   bool   // apply the change, then answer 503 (lost response)
	trigger string // when set, armed only after a call matching trigger
	armed   bool
}

// hook changes a model right after a call matching trigger was served.
type hook struct {
	trigger string
	alias   string
	params  map[string]any
	done    bool
}

// LiteLLM is a fake LiteLLM management API served over HTTP, plus a small
// data-plane emulation (Use) that enforces key restrictions and budgets.
type LiteLLM struct {
	Server    *httptest.Server
	MasterKey string

	mu          sync.Mutex
	now         func() time.Time
	seq         int
	models      map[string]*LiteLLMModel
	credentials map[string]string
	teams       map[string]*LiteLLMTeam
	keys        map[string]*LiteLLMKey // by token
	calls       []Call
	down        bool
	failures    []*failure
	hooks       []*hook
	gates       []*gate
	refused     map[string]bool // key tokens whose revocation is refused
	minute      map[string]time.Time
	perMinute   map[string]int64
}

// gate holds one request (see Hold).
type gate struct {
	line    string
	after   bool
	used    bool
	reached chan struct{}
	release chan struct{}
}

// NewLiteLLM starts the fake. Close it with t.Cleanup(f.Close).
func NewLiteLLM(now func() time.Time) *LiteLLM {
	if now == nil {
		now = time.Now
	}
	f := &LiteLLM{
		MasterKey: "sk-master-test", now: now,
		models: map[string]*LiteLLMModel{}, credentials: map[string]string{},
		teams: map[string]*LiteLLMTeam{}, keys: map[string]*LiteLLMKey{}, refused: map[string]bool{},
	}
	f.Server = httptest.NewServer(http.HandlerFunc(f.serve))
	return f
}

// URL is the management base URL.
func (f *LiteLLM) URL() string { return f.Server.URL }

// Close stops the server.
func (f *LiteLLM) Close() { f.Server.Close() }

// SetDown makes every request fail with 503 before any change.
func (f *LiteLLM) SetDown(down bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down = down
}

// FailNext makes the next n requests whose "METHOD path" starts with prefix
// fail with 503; after=true applies the change first (a lost response).
func (f *LiteLLM) FailNext(prefix string, n int, after bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures = append(f.failures, &failure{prefix: prefix, n: n, after: after})
}

// FailAfter makes the first call matching prefix that follows a call
// matching trigger fail with 503 before any change.
func (f *LiteLLM) FailAfter(trigger, prefix string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures = append(f.failures, &failure{prefix: prefix, n: 1, trigger: trigger})
}

// Hold blocks the first request whose "METHOD path?query" equals line,
// before the fake applies it or, with after, after it was applied but before
// the response is sent. reached is closed when the request is held; release
// lets it continue (call it exactly once).
func (f *LiteLLM) Hold(line string, after bool) (reached <-chan struct{}, release func()) {
	g := &gate{line: line, after: after, reached: make(chan struct{}), release: make(chan struct{})}
	f.mu.Lock()
	f.gates = append(f.gates, g)
	f.mu.Unlock()
	var once sync.Once
	return g.reached, func() { once.Do(func() { close(g.release) }) }
}

// RefuseKeyDelete makes every revocation naming token answer 503 without a
// change while refuse is true.
func (f *LiteLLM) RefuseKeyDelete(token string, refuse bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refused[token] = refuse
}

// PatchAfter merges params into alias's litellm_params right after the first
// call matching trigger was applied (another writer changed the record).
func (f *LiteLLM) PatchAfter(trigger, alias string, params map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hooks = append(f.hooks, &hook{trigger: trigger, alias: alias, params: params})
}

// Calls returns the management requests received, in order.
func (f *LiteLLM) Calls() []Call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

// CallsTo returns the calls whose "METHOD path" starts with prefix.
func (f *LiteLLM) CallsTo(prefix string) []Call {
	var out []Call
	for _, c := range f.Calls() {
		if strings.HasPrefix(c.Method+" "+c.Path, prefix) {
			out = append(out, c)
		}
	}
	return out
}

// Models returns a copy of every deployment.
func (f *LiteLLM) Models() []LiteLLMModel {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []LiteLLMModel
	for _, m := range f.models {
		out = append(out, LiteLLMModel{ID: m.ID, ModelName: m.ModelName, LiteLLMParams: maps.Clone(m.LiteLLMParams), ModelInfo: maps.Clone(m.ModelInfo)})
	}
	slices.SortFunc(out, func(a, b LiteLLMModel) int { return strings.Compare(a.ID, b.ID) })
	return out
}

// ModelByAlias returns the deployments with a model_name.
func (f *LiteLLM) ModelByAlias(alias string) []LiteLLMModel {
	var out []LiteLLMModel
	for _, m := range f.Models() {
		if m.ModelName == alias {
			out = append(out, m)
		}
	}
	return out
}

// DeleteModelDirectly removes a deployment behind FlowAI's back.
func (f *LiteLLM) DeleteModelDirectly(alias string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id, m := range f.models {
		if m.ModelName == alias {
			delete(f.models, id)
		}
	}
}

// PatchModelDirectly changes a deployment behind FlowAI's back.
func (f *LiteLLM) PatchModelDirectly(alias string, params, info map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, m := range f.models {
		if m.ModelName == alias {
			merge(m.LiteLLMParams, params)
			merge(m.ModelInfo, info)
		}
	}
}

// InsertModelDirectly creates a deployment behind FlowAI's back.
func (f *LiteLLM) InsertModelDirectly(alias string, params, info map[string]any) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	id := "dep-" + strconv.Itoa(f.seq)
	info = maps.Clone(info)
	if info == nil {
		info = map[string]any{}
	}
	info["id"] = id
	f.models[id] = &LiteLLMModel{ID: id, ModelName: alias, LiteLLMParams: maps.Clone(params), ModelInfo: info}
	return id
}

// Credential returns a stored provider key.
func (f *LiteLLM) Credential(name string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.credentials[name]
	return v, ok
}

// Credentials returns the stored credential names.
func (f *LiteLLM) Credentials() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Sorted(maps.Keys(f.credentials))
}

// Teams returns a copy of every team.
func (f *LiteLLM) Teams() []LiteLLMTeam {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []LiteLLMTeam
	for _, t := range f.teams {
		out = append(out, *t)
	}
	slices.SortFunc(out, func(a, b LiteLLMTeam) int { return strings.Compare(a.ID, b.ID) })
	return out
}

// TeamFor returns the team whose metadata names (team, flow).
func (f *LiteLLM) TeamFor(teamID, flowID int64) (LiteLLMTeam, bool) {
	for _, t := range f.Teams() {
		if numberEquals(t.Metadata["flowai_team_id"], teamID) && numberEquals(t.Metadata["flowai_flow_id"], flowID) {
			return t, true
		}
	}
	return LiteLLMTeam{}, false
}

// DeleteTeamDirectly removes a team as an out-of-band LiteLLM change.
func (f *LiteLLM) DeleteTeamDirectly(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.teams, id)
}

// SetTeamSpend sets the spend of a team.
func (f *LiteLLM) SetTeamSpend(teamID string, spend float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if t, ok := f.teams[teamID]; ok {
		t.Spend = spend
	}
}

// ResetTeamSpend resets a team's spend as LiteLLM's budget reset job does.
func (f *LiteLLM) ResetTeamSpend(teamID string, nextReset time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if t, ok := f.teams[teamID]; ok {
		t.Spend = 0
		t.BudgetResetAt = &nextReset
	}
}

// Keys returns a copy of every virtual key.
func (f *LiteLLM) Keys() []LiteLLMKey {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []LiteLLMKey
	for _, k := range f.keys {
		c := *k
		c.Models = slices.Clone(k.Models)
		out = append(out, c)
	}
	slices.SortFunc(out, func(a, b LiteLLMKey) int { return strings.Compare(a.Token, b.Token) })
	return out
}

// ErrRejected is returned by Use when LiteLLM's data plane rejects a call.
var ErrRejected = errors.New("litellm: call rejected")

// ErrBudgetExceeded is returned by Use when a budget is reached.
var ErrBudgetExceeded = errors.New("litellm: budget exceeded")

// Use emulates one data-plane call costing cost USD with a virtual key.
func (f *LiteLLM) Use(key, alias string, cost float64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	k, ok := f.keys[TokenOf(key)]
	if !ok || k.Revoked || !f.now().Before(k.ExpiresAt) || !slices.Contains(k.Models, alias) {
		return ErrRejected
	}
	found := false
	for _, m := range f.models {
		if m.ModelName == alias {
			found = true
		}
	}
	if !found {
		return ErrRejected
	}
	if k.MaxBudget != nil && k.Spend >= *k.MaxBudget {
		return ErrBudgetExceeded
	}
	if t, ok := f.teams[k.TeamID]; ok && t.MaxBudget != nil && t.Spend >= *t.MaxBudget {
		return ErrBudgetExceeded
	}
	k.Spend += cost
	if t, ok := f.teams[k.TeamID]; ok {
		t.Spend += cost
	}
	return nil
}

// Data-plane rejection statuses and error types of the fake, matching the
// deployed gateway: budget_exceeded_status_code is 422, so a budget
// rejection never shares the rate limit's 429.
const (
	BudgetExceededStatus = http.StatusUnprocessableEntity
	BudgetExceededType   = "budget_exceeded"
	RateLimitStatus      = http.StatusTooManyRequests
	RateLimitType        = "rate_limit_error"
	RejectedStatus       = http.StatusUnauthorized
	RejectedType         = "auth_error"
)

// Call emulates one data-plane chat call as the task runtime sees it: the
// HTTP status and the error type of a rejection ("" on success). A key's
// rpm_limit counts calls per clock minute.
func (f *LiteLLM) Call(key, alias string, cost float64) (int, string) {
	f.mu.Lock()
	k, ok := f.keys[TokenOf(key)]
	if ok && k.RPMLimit != nil {
		minute := f.now().UTC().Truncate(time.Minute)
		if f.minute == nil {
			f.minute = map[string]time.Time{}
			f.perMinute = map[string]int64{}
		}
		if !f.minute[TokenOf(key)].Equal(minute) {
			f.minute[TokenOf(key)], f.perMinute[TokenOf(key)] = minute, 0
		}
		if f.perMinute[TokenOf(key)] >= *k.RPMLimit {
			f.mu.Unlock()
			return RateLimitStatus, RateLimitType
		}
		f.perMinute[TokenOf(key)]++
	}
	f.mu.Unlock()
	switch err := f.Use(key, alias, cost); {
	case err == nil:
		return http.StatusOK, ""
	case errors.Is(err, ErrBudgetExceeded):
		return BudgetExceededStatus, BudgetExceededType
	default:
		return RejectedStatus, RejectedType
	}
}

// TokenOf is LiteLLM's token hash of a virtual key.
func TokenOf(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func numberEquals(v any, want int64) bool {
	switch n := v.(type) {
	case json.Number:
		i, err := n.Int64()
		return err == nil && i == want
	case float64:
		return n == float64(want)
	case int64:
		return n == want
	}
	return false
}

func merge(dst, src map[string]any) {
	for k, v := range src {
		if v == nil {
			delete(dst, k)
			continue
		}
		dst[k] = v
	}
}

func (f *LiteLLM) serve(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	var body map[string]any
	if len(raw) > 0 {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(&body); err != nil {
			http.Error(w, `{"error":"bad json"}`, http.StatusBadRequest)
			return
		}
	}
	line := r.Method + " " + r.URL.Path + queryOf(r)
	f.wait(line, false)
	f.mu.Lock()
	status, out := f.handle(r, body, line)
	f.mu.Unlock()
	f.wait(line, true)
	if s, ok := out.(string); ok {
		http.Error(w, s, status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(out)
}

// handle serves one request under f.mu; a string body is an error text.
func (f *LiteLLM) handle(r *http.Request, body map[string]any, line string) (int, any) {
	path := r.URL.Path
	f.calls = append(f.calls, Call{Method: r.Method, Path: path + queryOf(r), Body: body})
	if r.Header.Get("Authorization") != "Bearer "+f.MasterKey {
		return http.StatusUnauthorized, `{"error":"unauthorized"}`
	}
	if f.down {
		return http.StatusServiceUnavailable, `{"error":"down"}`
	}
	if r.Method == http.MethodPost && path == "/key/delete" {
		for _, v := range asSlice(body["keys"]) {
			if token, _ := v.(string); f.refused[token] {
				return http.StatusServiceUnavailable, `{"error":"refused"}`
			}
		}
	}
	lost := false
	for _, fl := range f.failures {
		if fl.trigger != "" && !fl.armed {
			continue
		}
		if fl.n > 0 && strings.HasPrefix(line, fl.prefix) {
			fl.n--
			if !fl.after {
				return http.StatusServiceUnavailable, `{"error":"injected"}`
			}
			lost = true
			break
		}
	}
	status, out := f.route(r.Method, path, r, body)
	for _, fl := range f.failures {
		if fl.trigger != "" && !fl.armed && strings.HasPrefix(line, fl.trigger) {
			fl.armed = true
		}
	}
	for _, hk := range f.hooks {
		if !hk.done && strings.HasPrefix(line, hk.trigger) {
			hk.done = true
			for _, m := range f.models {
				if m.ModelName == hk.alias {
					merge(m.LiteLLMParams, hk.params)
				}
			}
		}
	}
	if lost {
		return http.StatusServiceUnavailable, `{"error":"lost response"}`
	}
	return status, out
}

// wait blocks the first request equal to a held line, before (after=false)
// or after (after=true) the fake applied it.
func (f *LiteLLM) wait(line string, after bool) {
	f.mu.Lock()
	var g *gate
	for _, c := range f.gates {
		if !c.used && c.after == after && c.line == line {
			c.used, g = true, c
			break
		}
	}
	f.mu.Unlock()
	if g != nil {
		close(g.reached)
		<-g.release
	}
}

func queryOf(r *http.Request) string {
	if r.URL.RawQuery == "" {
		return ""
	}
	return "?" + r.URL.RawQuery
}

func toMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return maps.Clone(m)
}

func optFloat(v any) *float64 {
	if n, ok := v.(json.Number); ok {
		f, err := n.Float64()
		if err == nil {
			return &f
		}
	}
	return nil
}

func optInt(v any) *int64 {
	if n, ok := v.(json.Number); ok {
		i, err := n.Int64()
		if err == nil {
			return &i
		}
	}
	return nil
}

func (f *LiteLLM) modelJSON(m *LiteLLMModel) map[string]any {
	return map[string]any{"model_name": m.ModelName, "litellm_params": maps.Clone(m.LiteLLMParams), "model_info": maps.Clone(m.ModelInfo)}
}

func (f *LiteLLM) route(method, path string, r *http.Request, body map[string]any) (int, any) {
	notFound := map[string]any{"error": map[string]any{"message": "not found"}}
	switch {
	case method == http.MethodPost && path == "/model/new":
		f.seq++
		id := "dep-" + strconv.Itoa(f.seq)
		info := toMap(body["model_info"])
		info["id"] = id
		name, _ := body["model_name"].(string)
		params := toMap(body["litellm_params"])
		for k, v := range params {
			if v == nil {
				delete(params, k)
			}
		}
		f.models[id] = &LiteLLMModel{ID: id, ModelName: name, LiteLLMParams: params, ModelInfo: info}
		return http.StatusOK, map[string]any{"model_id": id}
	case method == http.MethodPatch && strings.HasPrefix(path, "/model/") && strings.HasSuffix(path, "/update"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/model/"), "/update")
		m, ok := f.models[id]
		if !ok {
			return http.StatusNotFound, notFound
		}
		merge(m.LiteLLMParams, toMap(body["litellm_params"]))
		info := toMap(body["model_info"])
		delete(info, "id")
		merge(m.ModelInfo, info)
		return http.StatusOK, map[string]any{"model_id": id}
	case method == http.MethodPost && path == "/model/delete":
		id, _ := body["id"].(string)
		if _, ok := f.models[id]; !ok {
			return http.StatusNotFound, notFound
		}
		delete(f.models, id)
		return http.StatusOK, map[string]any{"message": "deleted"}
	case method == http.MethodGet && path == "/model/info":
		id := r.URL.Query().Get("litellm_model_id")
		data := []any{}
		for _, key := range slices.Sorted(maps.Keys(f.models)) {
			if id == "" || id == key {
				data = append(data, f.modelJSON(f.models[key]))
			}
		}
		return http.StatusOK, map[string]any{"data": data}
	case method == http.MethodPost && path == "/credentials":
		name, _ := body["credential_name"].(string)
		values := toMap(body["credential_values"])
		key, _ := values["api_key"].(string)
		f.credentials[name] = key
		return http.StatusOK, map[string]any{"success": true}
	case method == http.MethodPatch && strings.HasPrefix(path, "/credentials/"):
		name := strings.TrimPrefix(path, "/credentials/")
		if _, ok := f.credentials[name]; !ok {
			return http.StatusNotFound, notFound
		}
		values := toMap(body["credential_values"])
		key, _ := values["api_key"].(string)
		f.credentials[name] = key
		return http.StatusOK, map[string]any{"success": true}
	case method == http.MethodDelete && strings.HasPrefix(path, "/credentials/"):
		name := strings.TrimPrefix(path, "/credentials/")
		if _, ok := f.credentials[name]; !ok {
			return http.StatusNotFound, notFound
		}
		delete(f.credentials, name)
		return http.StatusOK, map[string]any{"success": true}
	case method == http.MethodPost && path == "/team/new":
		f.seq++
		id := "team-" + strconv.Itoa(f.seq)
		t := &LiteLLMTeam{ID: id}
		t.Alias, _ = body["team_alias"].(string)
		f.applyTeam(t, body)
		f.teams[id] = t
		return http.StatusOK, map[string]any{"team_id": id}
	case method == http.MethodPost && path == "/team/update":
		id, _ := body["team_id"].(string)
		t, ok := f.teams[id]
		if !ok {
			return http.StatusNotFound, notFound
		}
		f.applyTeam(t, body)
		return http.StatusOK, map[string]any{"team_id": id}
	case method == http.MethodPost && path == "/team/delete":
		deleted := []any{}
		for _, v := range asSlice(body["team_ids"]) {
			id, _ := v.(string)
			if _, ok := f.teams[id]; ok {
				delete(f.teams, id)
				deleted = append(deleted, id)
			}
		}
		if len(deleted) == 0 {
			return http.StatusNotFound, notFound
		}
		return http.StatusOK, map[string]any{"deleted_teams": deleted}
	case method == http.MethodGet && path == "/team/info":
		t, ok := f.teams[r.URL.Query().Get("team_id")]
		if !ok {
			return http.StatusNotFound, notFound
		}
		info := map[string]any{"team_id": t.ID, "spend": t.Spend, "max_budget": t.MaxBudget, "budget_duration": t.BudgetDuration}
		if t.BudgetResetAt != nil {
			info["budget_reset_at"] = t.BudgetResetAt.UTC().Format(time.RFC3339Nano)
		} else {
			info["budget_reset_at"] = nil
		}
		return http.StatusOK, map[string]any{"team_id": t.ID, "team_info": info}
	case method == http.MethodPost && path == "/key/generate":
		key, _ := body["key"].(string)
		if key == "" {
			f.seq++
			key = fmt.Sprintf("sk-fake-%d", f.seq)
		}
		token := TokenOf(key)
		if _, ok := f.keys[token]; ok {
			return http.StatusBadRequest, map[string]any{"error": map[string]any{"message": "duplicate key"}}
		}
		k := &LiteLLMKey{Key: key, Token: token, Metadata: toMap(body["metadata"])}
		for _, m := range asSlice(body["models"]) {
			if s, ok := m.(string); ok {
				k.Models = append(k.Models, s)
			}
		}
		k.TeamID, _ = body["team_id"].(string)
		k.MaxBudget = optFloat(body["max_budget"])
		k.RPMLimit = optInt(body["rpm_limit"])
		k.TPMLimit = optInt(body["tpm_limit"])
		k.ExpiresAt = f.now().Add(parseDuration(body["duration"]))
		f.keys[token] = k
		return http.StatusOK, map[string]any{"key": key, "token": token, "expires": k.ExpiresAt.UTC().Format(time.RFC3339Nano)}
	case method == http.MethodPost && path == "/key/update":
		token, _ := body["key"].(string)
		k, ok := f.keys[token]
		if !ok || k.Revoked {
			return http.StatusNotFound, notFound
		}
		k.ExpiresAt = f.now().Add(parseDuration(body["duration"]))
		return http.StatusOK, map[string]any{"key": token}
	case method == http.MethodPost && path == "/key/delete":
		deleted := 0
		for _, v := range asSlice(body["keys"]) {
			token, _ := v.(string)
			if k, ok := f.keys[token]; ok && !k.Revoked {
				k.Revoked = true
				deleted++
			}
		}
		if deleted == 0 {
			return http.StatusNotFound, notFound
		}
		return http.StatusOK, map[string]any{"deleted_keys": body["keys"]}
	}
	return http.StatusNotFound, notFound
}

func asSlice(v any) []any {
	s, _ := v.([]any)
	return s
}

func parseDuration(v any) time.Duration {
	s, _ := v.(string)
	if strings.HasSuffix(s, "s") {
		n, err := strconv.ParseInt(strings.TrimSuffix(s, "s"), 10, 64)
		if err == nil {
			return time.Duration(n) * time.Second
		}
	}
	return 24 * time.Hour
}

func (f *LiteLLM) applyTeam(t *LiteLLMTeam, body map[string]any) {
	if md, ok := body["metadata"].(map[string]any); ok {
		t.Metadata = maps.Clone(md)
	}
	if v, present := body["max_budget"]; present {
		t.MaxBudget = optFloat(v)
	}
	if v, present := body["budget_duration"]; present {
		t.BudgetDuration, _ = v.(string)
	}
	if v, present := body["rpm_limit"]; present {
		t.RPMLimit = optInt(v)
	}
	if v, present := body["tpm_limit"]; present {
		t.TPMLimit = optInt(v)
	}
	if v, present := body["budget_reset_at"]; present {
		if s, ok := v.(string); ok {
			if at, err := time.Parse(time.RFC3339Nano, s); err == nil {
				t.BudgetResetAt = &at
			}
		} else {
			t.BudgetResetAt = nil
		}
	}
}
