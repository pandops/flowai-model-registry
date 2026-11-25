package test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"gopkg.in/yaml.v3"

	"github.com/pandops/flowai-model-registry/internal/app"
	"github.com/pandops/flowai-model-registry/internal/broker"
	"github.com/pandops/flowai-model-registry/internal/config"
	"github.com/pandops/flowai-model-registry/internal/mocks"
	"github.com/pandops/flowai-model-registry/internal/store"
)

// ---------------------------------------------------------------------------
// Paths and schemas
// ---------------------------------------------------------------------------

func serviceDir(t testing.TB) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(filepath.Join(wd, ".."))
}

func contractFixtureDir(t testing.TB) string {
	t.Helper()
	dir := os.Getenv("FLOWAI_CONTRACT_FIXTURE_DIR")
	if !filepath.IsAbs(dir) {
		t.Fatal("FLOWAI_CONTRACT_FIXTURE_DIR must be an absolute path to staged publisher fixtures")
	}
	return dir
}

var (
	schemaOnce     sync.Once
	schemaCompiler *jsonschema.Compiler
	schemaErr      error
	schemaCache    sync.Map
)

// ownSchemas registers every model-registry schema under its $id.
func ownSchemas(t testing.TB) *jsonschema.Compiler {
	t.Helper()
	schemaOnce.Do(func() {
		c := jsonschema.NewCompiler()
		dir := filepath.Join(serviceDir(t), "schemas")
		schemaErr = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".schema.json") {
				return err
			}
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()
			doc, err := jsonschema.UnmarshalJSON(f)
			if err != nil {
				return fmt.Errorf("%s: %w", path, err)
			}
			id, _ := doc.(map[string]any)["$id"].(string)
			if id == "" {
				return fmt.Errorf("%s: no $id", path)
			}
			return c.AddResource(id, doc)
		})
		schemaCompiler = c
	})
	if schemaErr != nil {
		t.Fatalf("load own schemas: %v", schemaErr)
	}
	return schemaCompiler
}

const ownSchemaBase = "https://flowai.local/schemas/model-registry/"

// validateOwn validates data against svc/model-registry/schemas/<rel>.
func validateOwn(t testing.TB, rel string, data []byte) {
	t.Helper()
	c := ownSchemas(t)
	key := "own:" + rel
	cached, ok := schemaCache.Load(key)
	if !ok {
		s, err := c.Compile(ownSchemaBase + rel)
		if err != nil {
			t.Fatalf("compile %s: %v", rel, err)
		}
		cached, _ = schemaCache.LoadOrStore(key, s)
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("%s: message is not JSON: %v: %s", rel, err, data)
	}
	if err := cached.(*jsonschema.Schema).Validate(inst); err != nil {
		t.Fatalf("message violates %s: %v\n%s", rel, err, data)
	}
}

// validateForeign validates data against a publisher's schema file
// (relative to the staged fixture root), a declared consumer test input.
func validateForeign(t testing.TB, rel string, data []byte) {
	t.Helper()
	path := filepath.Join(contractFixtureDir(t), rel)
	key := "foreign:" + rel
	cached, ok := schemaCache.Load(key)
	if !ok {
		c := jsonschema.NewCompiler()
		s, err := c.Compile(path)
		if err != nil {
			t.Fatalf("compile publisher schema %s: %v", rel, err)
		}
		cached, _ = schemaCache.LoadOrStore(key, s)
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("%s: not JSON: %v", rel, err)
	}
	if err := cached.(*jsonschema.Schema).Validate(inst); err != nil {
		t.Fatalf("fixture violates publisher schema %s: %v\n%s", rel, err, data)
	}
}

func foreignSchemaExists(t testing.TB, rel string) bool {
	_, err := os.Stat(filepath.Join(contractFixtureDir(t), rel))
	return err == nil
}

// Publisher schema files model-registry reads as consumer test inputs.
const (
	schemaExecutorRegistered = "svc/executor-registry/schemas/event.executor_registered.schema.json"
	schemaExecutorRemoved    = "svc/executor-registry/schemas/event.executor_removed.schema.json"
	schemaExecutorTypeReply  = "svc/executor-registry/schemas/service.executor-type.reply.schema.json"
	schemaExecutorTypeReq    = "svc/executor-registry/schemas/service.executor-type.request.schema.json"
	schemaDockerEvent        = "executor/docker_openhands/schemas/executor-event.schema.json"
	schemaDockerCheckReq     = "executor/docker_openhands/schemas/check-request.schema.json"
	schemaK8sEvent           = "executor/k8s-openhands/schemas/executor-event.schema.json"
	schemaK8sCheckReq        = "executor/k8s-openhands/schemas/check-request.schema.json"
	schemaTMClaimCurrentReq  = "svc/task-manager/schemas/service.claim-current.request.schema.json"
	schemaTMClaimCurrentRep  = "svc/task-manager/schemas/service.claim-current.reply.schema.json"
	schemaTMReleased         = "svc/task-manager/schemas/event.released.schema.json"
	schemaTMCancelled        = "svc/task-manager/schemas/event.cancelled.schema.json"
	schemaTMArchived         = "svc/task-manager/schemas/event.archived.schema.json"
	schemaFRLLMSettingsReq   = "svc/flow-registry/schemas/service.llm-settings.request.schema.json"
	schemaFRLLMSettingsRep   = "svc/flow-registry/schemas/service.llm-settings.reply.schema.json"
	schemaFRAliasUsageReq    = "svc/flow-registry/schemas/service.alias-usage.request.schema.json"
	schemaFRAliasUsageRep    = "svc/flow-registry/schemas/service.alias-usage.reply.schema.json"
	schemaFRRetireReq        = "svc/flow-registry/schemas/service.retire-alias.request.schema.json"
	schemaFRRestoreReq       = "svc/flow-registry/schemas/service.restore-alias.request.schema.json"
)

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

type fakeClaim struct {
	current    bool
	reason     string
	flowID     int64
	executorID int64
}

type fakeFlow struct {
	name      string
	aliases   []string
	perTask   any
	flowLimit any
	revision  int64
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

type harness struct {
	t     *testing.T
	ctx   context.Context
	mu    sync.Mutex
	now   time.Time
	store *mocks.MemStore
	// backing, when set, is the store the app runs on instead of store
	// (a PostgreSQL store or a test wrapper); store then stays unused.
	backing store.Store
	broker  *mocks.Broker
	llm     *mocks.LiteLLM
	logs    *syncBuffer
	cfg     config.Config
	yaml    map[string]any
	app     *app.App
	edge    *httptest.Server
	signer  ed25519.PrivateKey

	claims        map[store.ClaimKey]fakeClaim
	flows         map[[2]int64]*fakeFlow
	executorTypes map[int64]string
	retired       map[string]bool
	beforeRetire  func() // runs before the fake flow-registry evaluates retire-alias
	evSeq         int64
	reqSeq        int64
}

type harnessOption func(*harness)

// withConfig overrides YAML keys ("a.b" paths) before the app is built.
func withConfig(path string, value any) harnessOption {
	return func(h *harness) { setPath(h.yaml, path, value) }
}

func setPath(m map[string]any, path string, value any) {
	parts := strings.Split(path, ".")
	for _, p := range parts[:len(parts)-1] {
		next, ok := m[p].(map[string]any)
		if !ok {
			next = map[string]any{}
			m[p] = next
		}
		m = next
	}
	if value == nil {
		delete(m, parts[len(parts)-1])
		return
	}
	m[parts[len(parts)-1]] = value
}

func writeFile(t testing.TB, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func randomHex32() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// baseYAML is a complete production configuration with test files, using
// the key names of config.example.yaml.
func baseYAML(t testing.TB, llmURL string, verify ed25519.PublicKey) map[string]any {
	dir := t.TempDir()
	ca := writeFile(t, dir, "ca.pem", "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n")
	return map[string]any{
		"http": map[string]any{"listen": ":8080"},
		"db": map[string]any{
			"host": "postgres", "port": 5432, "user": "model_registry", "password": "db-password",
			"schema": "model_registry_schema", "sslmode": "verify-full", "ca_file": ca,
		},
		"nats": map[string]any{
			"url": "tls://nats:4222", "ca_file": ca,
			"credentials_file": writeFile(t, dir, "user.creds", "-----BEGIN NATS USER JWT-----\neyJ0eXAiOiJKV1QifQ.e30.sig\n------END NATS USER JWT------\n\n-----BEGIN USER NKEY SEED-----\nSUAEXAMPLE\n------END USER NKEY SEED------\n"),
			"inbox_prefix":     "_INBOX.model-registry",
			"request_timeout":  "200ms",
		},
		"litellm": map[string]any{
			"management_url": llmURL, "master_key": "sk-master-test", "timeout": "2s",
			"data_urls": map[string]any{
				"executor_docker_openhands": "http://litellm-data:4000",
				"executor_k8s_openhands":    "http://litellm-data.flowai.svc.cluster.local:4000",
			},
			"api_base_allowed_origins":     []any{"https://api.openai.com", "https://llm.example.com"},
			"availability_refresh_seconds": 60,
			"usage_refresh_seconds":        60,
		},
		"claim_token":        map[string]any{"verification_key": base64.StdEncoding.EncodeToString(verify)},
		"encryption_keys":    []any{map[string]any{"id": 1, "key_hex": randomHex32()}},
		"cursor_keys":        []any{map[string]any{"id": 1, "key_hex": randomHex32()}},
		"reconcile_interval": "5s",
		"outbox":             map[string]any{"relay_interval": "200ms"},
		"task_keys":          map[string]any{"backstop": "24h"},
	}
}

func marshalYAML(t testing.TB, m map[string]any) []byte {
	t.Helper()
	raw, err := yaml.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func newHarness(t *testing.T, opts ...harnessOption) *harness {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{
		t: t, ctx: context.Background(),
		now:   time.Date(2026, 10, 14, 9, 30, 0, 0, time.UTC), // a Wednesday
		store: mocks.NewMemStore(), broker: mocks.NewBroker(), logs: &syncBuffer{}, signer: priv,
		claims: map[store.ClaimKey]fakeClaim{}, flows: map[[2]int64]*fakeFlow{},
		executorTypes: map[int64]string{}, retired: map[string]bool{},
	}
	h.llm = mocks.NewLiteLLM(h.clock)
	t.Cleanup(h.llm.Close)
	h.yaml = baseYAML(t, h.llm.URL(), pub)
	for _, o := range opts {
		o(h)
	}
	cfg, err := config.Parse(marshalYAML(t, h.yaml))
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	h.cfg = cfg
	h.installOwners()
	h.boot()
	t.Cleanup(h.checkPublishedSchemas)
	return h
}

// boot builds and starts a fresh app on the same store and broker (a
// restart when called again).
func (h *harness) boot() {
	h.t.Helper()
	a, err := app.New(app.Deps{
		Config: h.cfg, Store: h.appStore(), Broker: h.broker, Clock: h.clock,
		Logger: slog.New(slog.NewJSONHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	if err != nil {
		h.t.Fatalf("app.New: %v", err)
	}
	if err := a.Start(h.ctx); err != nil {
		h.t.Fatalf("app.Start: %v", err)
	}
	if h.edge != nil {
		h.edge.Close()
	}
	h.app = a
	h.edge = httptest.NewServer(a.Handler())
	h.t.Cleanup(h.edge.Close)
}

// appStore is the store the app runs on.
func (h *harness) appStore() store.Store {
	if h.backing != nil {
		return h.backing
	}
	return h.store
}

// withStore runs the app on s instead of the in-memory store.
func withStore(s store.Store) harnessOption {
	return func(h *harness) { h.backing = s }
}

func (h *harness) clock() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.now
}

func (h *harness) advance(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.now = h.now.Add(d)
}

func (h *harness) setNow(at time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.now = at
}

// ---------------------------------------------------------------------------
// Fake owners: task-manager, flow-registry, executor-registry
// ---------------------------------------------------------------------------

func reply(status int, body any) broker.Reply {
	var raw []byte
	if body != nil {
		raw, _ = json.Marshal(body)
	}
	return broker.Reply{Status: status, Body: raw}
}

func errReply(status int, code string) broker.Reply {
	return reply(status, map[string]any{"code": code, "message": code, "request_id": nil})
}

type reqBody struct {
	Params map[string]json.RawMessage `json:"params"`
	Body   map[string]json.RawMessage `json:"body"`
}

func decodeReq(m broker.Message) reqBody {
	var r reqBody
	_ = json.Unmarshal(m.Data, &r)
	return r
}

func intField(raw map[string]json.RawMessage, key string) int64 {
	var v int64
	_ = json.Unmarshal(raw[key], &v)
	return v
}

func strField(raw map[string]json.RawMessage, key string) string {
	var v string
	_ = json.Unmarshal(raw[key], &v)
	return v
}

// installOwners answers the requests model-registry sends, as the owning
// services do. Each request and reply is checked against the owner's schema.
func (h *harness) installOwners() {
	h.broker.Respond("task-manager.service.claim-current.sub", func(_ context.Context, m broker.Message) broker.Reply {
		validateForeign(h.t, schemaTMClaimCurrentReq, m.Data)
		b := decodeReq(m).Params
		k := store.ClaimKey{TeamID: intField(b, "team_id"), TaskID: intField(b, "task_id"), Attempt: intField(b, "attempt"), ClaimSeq: intField(b, "claim_seq")}
		h.mu.Lock()
		c, ok := h.claims[k]
		h.mu.Unlock()
		var out map[string]any
		switch {
		case !ok:
			out = map[string]any{"current": false, "reason": "unknown_task"}
		case !c.current:
			out = map[string]any{"current": false, "reason": c.reason}
		case c.executorID != intField(b, "executor_id"):
			out = map[string]any{"current": false, "reason": "executor_mismatch"}
		default:
			out = map[string]any{"current": true, "team_id": k.TeamID, "flow_id": c.flowID, "execution_revision_id": 1}
		}
		return h.foreignReply(schemaTMClaimCurrentRep, 200, out)
	})
	h.broker.Respond("flow-registry.service.llm-settings.sub", func(_ context.Context, m broker.Message) broker.Reply {
		validateForeign(h.t, schemaFRLLMSettingsReq, m.Data)
		b := decodeReq(m).Params
		team, flow := intField(b, "team_id"), intField(b, "flow_id")
		h.mu.Lock()
		f, ok := h.flows[[2]int64{team, flow}]
		var out map[string]any
		if ok {
			out = map[string]any{"model_aliases": append([]string{}, f.aliases...), "per_task_limit": f.perTask, "flow_limit": f.flowLimit, "revision": f.revision}
		}
		h.mu.Unlock()
		if !ok {
			return errReply(404, "not_found")
		}
		return h.foreignReply(schemaFRLLMSettingsRep, 200, out)
	})
	h.broker.Respond("flow-registry.service.alias-usage.sub", func(_ context.Context, m broker.Message) broker.Reply {
		validateForeign(h.t, schemaFRAliasUsageReq, m.Data)
		b := decodeReq(m).Params
		team, alias := intField(b, "team_id"), strField(b, "alias")
		h.mu.Lock()
		flows, sole := []any{}, []any{}
		for _, id := range h.flowIDs(team) {
			f := h.flows[[2]int64{team, id}]
			if slices.Contains(f.aliases, alias) {
				flows = append(flows, map[string]any{"flow_id": id, "name": f.name})
				if len(f.aliases) == 1 {
					sole = append(sole, map[string]any{"flow_id": id, "name": f.name})
				}
			}
		}
		h.mu.Unlock()
		return h.foreignReply(schemaFRAliasUsageRep, 200, map[string]any{"flows": flows, "sole_alias_flows": sole})
	})
	h.broker.Respond("flow-registry.service.retire-alias.sub", func(_ context.Context, m broker.Message) broker.Reply {
		validateForeign(h.t, schemaFRRetireReq, m.Data)
		b := decodeReq(m).Params
		team, alias := intField(b, "team_id"), strField(b, "alias")
		if h.beforeRetire != nil {
			h.beforeRetire()
		}
		h.mu.Lock()
		var sole []any
		for _, id := range h.flowIDs(team) {
			f := h.flows[[2]int64{team, id}]
			if len(f.aliases) == 1 && f.aliases[0] == alias {
				sole = append(sole, map[string]any{"flow_id": id, "name": f.name})
			}
		}
		if len(sole) > 0 {
			h.mu.Unlock()
			return reply(409, map[string]any{"code": "model_last_in_flow", "message": "model_last_in_flow", "request_id": nil, "flows": sole})
		}
		for _, id := range h.flowIDs(team) {
			f := h.flows[[2]int64{team, id}]
			if i := slices.Index(f.aliases, alias); i >= 0 {
				f.aliases = slices.Delete(f.aliases, i, i+1)
				f.revision++
			}
		}
		h.retired[alias] = true
		h.mu.Unlock()
		return broker.Reply{Status: 204} // a repeated retirement is 204 too
	})
	h.broker.Respond("flow-registry.service.restore-alias.sub", func(_ context.Context, m broker.Message) broker.Reply {
		validateForeign(h.t, schemaFRRestoreReq, m.Data)
		b := decodeReq(m).Params
		h.mu.Lock()
		delete(h.retired, strField(b, "alias"))
		h.mu.Unlock()
		return broker.Reply{Status: 204}
	})
	h.broker.Respond("executor-registry.service.executor-type.sub", func(_ context.Context, m broker.Message) broker.Reply {
		validateForeign(h.t, schemaExecutorTypeReq, m.Data)
		id := intField(decodeReq(m).Params, "executor_id")
		team, _ := strconv.ParseInt(m.Header["team"], 10, 64)
		h.mu.Lock()
		typ, ok := h.executorTypes[id]
		h.mu.Unlock()
		if !ok {
			return errReply(404, "executor_unknown_or_unavailable")
		}
		return h.foreignReply(schemaExecutorTypeReply, 200, map[string]any{"executor_id": id, "team_id": team, "execution_tag": "gpu", "executor_type": typ})
	})
}

// foreignReply checks a fake owner's reply against the owner's schema.
func (h *harness) foreignReply(schema string, status int, body map[string]any) broker.Reply {
	raw, _ := json.Marshal(body)
	validateForeign(h.t, schema, raw)
	return broker.Reply{Status: status, Body: raw}
}

func (h *harness) flowIDs(team int64) []int64 {
	var ids []int64
	for k := range h.flows {
		if k[0] == team {
			ids = append(ids, k[1])
		}
	}
	slices.Sort(ids)
	return ids
}

// setFlow sets the Flow that the fake flow-registry serves.
func (h *harness) setFlow(team, flow int64, name string, aliases []string, perTask, flowLimit any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f, ok := h.flows[[2]int64{team, flow}]
	if !ok {
		f = &fakeFlow{revision: 0}
		h.flows[[2]int64{team, flow}] = f
	}
	f.name, f.aliases, f.perTask, f.flowLimit = name, slices.Clone(aliases), perTask, flowLimit
	f.revision++
}

func (h *harness) flowAliases(team, flow int64) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.flows[[2]int64{team, flow}].aliases)
}

// setClaim makes the fake task-manager report a claim.
func (h *harness) setClaim(k store.ClaimKey, executor, flow int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.claims[k] = fakeClaim{current: true, flowID: flow, executorID: executor}
}

func (h *harness) endClaim(k store.ClaimKey, reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c := h.claims[k]
	c.current, c.reason = false, reason
	h.claims[k] = c
}

func (h *harness) setExecutorType(executor int64, typ string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if typ == "" {
		delete(h.executorTypes, executor)
		return
	}
	h.executorTypes[executor] = typ
}

// ---------------------------------------------------------------------------
// Requests to model-registry
// ---------------------------------------------------------------------------

type res struct {
	status int
	header map[string]string
	body   []byte
}

func (r res) json(t testing.TB) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(r.body, &out); err != nil {
		t.Fatalf("reply is not a JSON object (status %d): %v: %q", r.status, err, r.body)
	}
	return out
}

func (r res) code(t testing.TB) string {
	t.Helper()
	if len(r.body) == 0 {
		return ""
	}
	c, _ := r.json(t)["code"].(string)
	return c
}

func (h *harness) nextRequestID() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reqSeq++
	return 7000 + h.reqSeq
}

func (h *harness) send(subject string, header map[string]string, payload any) (res, error) {
	h.t.Helper()
	var data []byte
	switch p := payload.(type) {
	case nil:
		data = []byte(`{}`)
	case []byte:
		data = p
	default:
		data, _ = json.Marshal(p)
	}
	r, err := h.broker.Request(h.ctx, broker.Message{Subject: subject, Header: header, Data: data}, time.Second)
	if err != nil {
		return res{}, err
	}
	return res{status: r.Status, header: r.Header, body: r.Body}, nil
}

func (h *harness) mustSend(subject string, header map[string]string, payload any) res {
	h.t.Helper()
	r, err := h.send(subject, header, payload)
	if err != nil {
		h.t.Fatalf("request %s: %v (model-registry does not serve it)", subject, err)
	}
	return r
}

func uiHeaders(team int64, requestID int64) map[string]string {
	return map[string]string{"team": strconv.FormatInt(team, 10), "operator_id": "501", "request_id": strconv.FormatInt(requestID, 10), "team_name": "Operations"}
}

// ui sends a model-registry.ui.<op> request as API Gateway does.
func (h *harness) ui(op string, team int64, params map[string]any, body any, ifMatch string) res {
	h.t.Helper()
	hdr := uiHeaders(team, h.nextRequestID())
	if ifMatch != "" {
		hdr["If-Match"] = ifMatch
	}
	payload := map[string]any{"params": params}
	if params == nil {
		payload["params"] = map[string]any{}
	}
	if body != nil {
		payload["body"] = body
	}
	r := h.mustSend("model-registry.ui."+op+".pub", hdr, payload)
	h.validateUIReply(op, r)
	return r
}

func (h *harness) validateUIReply(op string, r res) {
	h.t.Helper()
	if r.status == 204 {
		if len(r.body) != 0 {
			h.t.Fatalf("%s 204 with a body: %s", op, r.body)
		}
		return
	}
	switch {
	case r.status >= 400:
		validateOwn(h.t, "error.reply.schema.json", r.body)
	default:
		validateOwn(h.t, "ui."+op+".reply.schema.json", r.body)
	}
}

// service sends a model-registry.service.<op> request as flow-registry does.
func (h *harness) service(op string, team string, body any) res {
	h.t.Helper()
	hdr := map[string]string{"team": team, "request_id": strconv.FormatInt(h.nextRequestID(), 10)}
	r := h.mustSend("model-registry.service."+op+".pub", hdr, map[string]any{"body": body})
	if r.status >= 400 {
		validateOwn(h.t, "error.reply.schema.json", r.body)
	} else {
		validateOwn(h.t, "service."+op+".reply.schema.json", r.body)
	}
	return r
}

// check sends model-registry.executor.<executor>.check as an Executor does.
func (h *harness) check(executor int64, k store.ClaimKey, header map[string]string) res {
	h.t.Helper()
	if header == nil {
		header = map[string]string{"team": strconv.FormatInt(k.TeamID, 10)}
	}
	payload := map[string]any{"params": map[string]any{}, "body": map[string]any{"task_id": k.TaskID, "attempt": k.Attempt, "claim_seq": k.ClaimSeq}}
	raw, _ := json.Marshal(payload)
	validateForeign(h.t, schemaDockerCheckReq, raw)
	r := h.mustSend("model-registry.executor."+strconv.FormatInt(executor, 10)+".check.pub", header, raw)
	validateOwn(h.t, "executor.check.reply.schema.json", r.body)
	return r
}

func checkResult(t testing.TB, r res) (string, string) {
	t.Helper()
	if r.status != 200 {
		return r.code(t), ""
	}
	m := r.json(t)
	result, _ := m["result"].(string)
	if result == "ok" {
		alias, _ := m["model_alias"].(string)
		return "ok", alias
	}
	reason, _ := m["reason"].(string)
	return result, reason
}

// token signs a claim token as task-manager does (agents.md rule 12).
func (h *harness) token(k store.ClaimKey, executor int64) string {
	payload, _ := json.Marshal(map[string]any{"team_id": k.TeamID, "task_id": k.TaskID, "attempt": k.Attempt, "claim_seq": k.ClaimSeq, "executor_id": executor})
	sig := ed25519.Sign(h.signer, payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(sig)
}

type edgeRes struct {
	status int
	body   map[string]any
	raw    []byte
}

// keyEdge calls GET /v1/tasks/{task}/litellm-key as an Executor does.
func (h *harness) keyEdge(task string, bearer string) edgeRes {
	h.t.Helper()
	req, _ := http.NewRequest(http.MethodGet, h.edge.URL+"/v1/tasks/"+task+"/litellm-key", nil)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := edgeRes{status: resp.StatusCode, raw: raw}
	_ = json.Unmarshal(raw, &out.body)
	if resp.StatusCode != http.StatusNotImplemented {
		validateOwn(h.t, "edge.litellm-key.reply.schema.json", raw)
	}
	return out
}

// ---------------------------------------------------------------------------
// Stream deliveries to model-registry's durable consumer
// ---------------------------------------------------------------------------

const (
	consumerStream  = "ALL_EVENT"
	consumerDurable = "model-registry"
)

func (h *harness) nextSeq() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.evSeq++
	return h.evSeq
}

func ts(at time.Time) string { return at.UTC().Format("2006-01-02T15:04:05.000Z") }

// deliver pushes one message on a ".sub" subject and returns the delivery.
func (h *harness) deliver(subject string, header map[string]string, payload map[string]any) *mocks.Delivery {
	h.t.Helper()
	raw, _ := json.Marshal(payload)
	d, err := h.broker.Deliver(h.ctx, consumerStream, consumerDurable, broker.Message{Subject: subject, Header: header, Data: raw})
	if err != nil {
		h.t.Fatalf("deliver %s: %v (model-registry bound no durable consumer)", subject, err)
	}
	return d
}

func (h *harness) registerExecutor(executor, team int64) *mocks.Delivery {
	h.t.Helper()
	seq := h.nextSeq()
	payload := map[string]any{
		"publisher": "executor-registry", "publisher_seq": seq, "team_id": team, "occurred_at": ts(h.clock()),
		"kind": "executor_registered", "executor_id": executor, "execution_tag": "gpu", "executor_type": "executor_docker_openhands",
	}
	raw, _ := json.Marshal(payload)
	validateForeign(h.t, schemaExecutorRegistered, raw)
	h.setExecutorType(executor, "executor_docker_openhands")
	return h.deliver("executor-registry.event.executor_registered.sub",
		map[string]string{"team": strconv.FormatInt(team, 10), "Nats-Msg-Id": fmt.Sprintf("executor-registry-%d", seq)}, payload)
}

// executorEvent delivers an Executor task event of kind for claim k.
func (h *harness) executorEvent(executor int64, kind string, k store.ClaimKey, extra map[string]any) *mocks.Delivery {
	h.t.Helper()
	seq := h.nextSeq()
	payload := map[string]any{
		"publisher": "executor", "publisher_seq": seq, "team_id": k.TeamID, "executor_id": executor,
		"kind": kind, "occurred_at": ts(h.clock()), "task_id": k.TaskID, "attempt": k.Attempt, "claim_seq": k.ClaimSeq,
	}
	switch kind {
	case "released":
		payload["reason"] = "executor_shutdown"
	case "failed":
		payload["failure_reason"], payload["retryable"] = "agent_error", false
	case "cancelled":
		payload["control_id"] = 1
	}
	for key, v := range extra {
		payload[key] = v
	}
	raw, _ := json.Marshal(payload)
	validateForeign(h.t, schemaDockerEvent, raw)
	return h.deliver(fmt.Sprintf("executor.event.%d.%s.sub", executor, kind),
		map[string]string{"team": strconv.FormatInt(k.TeamID, 10), "Nats-Msg-Id": fmt.Sprintf("executor-%d-%d-%d", k.TeamID, executor, seq)}, payload)
}

// taskName is the fixture task name task-manager carries in its task events.
func taskName(team, task int64) string { return fmt.Sprintf("team%d-%d", team, task) }

// taskManagerEvent delivers a task-manager event (released, cancelled,
// archived) in the shape of task-manager's schema.
func (h *harness) taskManagerEvent(kind string, k store.ClaimKey, extra map[string]any) *mocks.Delivery {
	h.t.Helper()
	seq := h.nextSeq()
	h.mu.Lock()
	c := h.claims[k]
	h.mu.Unlock()
	executor := c.executorID
	if executor == 0 {
		executor = 1
	}
	payload := map[string]any{
		"publisher": "task-manager", "publisher_seq": seq, "team_id": k.TeamID, "kind": kind, "occurred_at": ts(h.clock()), "task_id": k.TaskID,
		"task_name": taskName(k.TeamID, k.TaskID),
	}
	schema := ""
	switch kind {
	case "released":
		schema = schemaTMReleased
		payload["attempt"], payload["claim_seq"], payload["executor_id"] = k.Attempt, k.ClaimSeq, executor
		payload["reason"], payload["republish_at"], payload["lost_publisher_seq"] = "executor_lost", nil, nil
	case "cancelled":
		schema = schemaTMCancelled
		payload["attempt"], payload["claim_seq"], payload["executor_id"], payload["control_id"] = k.Attempt, k.ClaimSeq, executor, 1
	case "archived":
		schema = schemaTMArchived
		flow := c.flowID
		if flow == 0 {
			flow = 1
		}
		payload["flow_id"], payload["source_id"], payload["final_state"], payload["final_reason"], payload["attempts"] = flow, 1, "finished", nil, k.Attempt
	default:
		h.t.Fatalf("unknown task-manager event kind %q", kind)
	}
	for key, v := range extra {
		payload[key] = v
	}
	raw, _ := json.Marshal(payload)
	validateForeign(h.t, schema, raw)
	return h.deliver("task-manager.event."+kind+".sub",
		map[string]string{"team": strconv.FormatInt(k.TeamID, 10), "Nats-Msg-Id": fmt.Sprintf("task-manager-%d", seq)}, payload)
}

// ---------------------------------------------------------------------------
// Outbox and published messages
// ---------------------------------------------------------------------------

func (h *harness) relay() {
	h.t.Helper()
	for i := 0; i < 20; i++ {
		n, err := h.app.RelayOnce(h.ctx)
		if err != nil {
			h.t.Fatalf("relay: %v", err)
		}
		if n == 0 {
			return
		}
	}
}

type published struct {
	subject string
	header  map[string]string
	body    map[string]any
	raw     []byte
}

func (h *harness) publishedOn(prefix string) []published {
	var out []published
	for _, m := range h.broker.PublishedOn(prefix) {
		var body map[string]any
		_ = json.Unmarshal(m.Data, &body)
		out = append(out, published{subject: m.Subject, header: m.Header, body: body, raw: m.Data})
	}
	return out
}

func (h *harness) audits() []published { h.relay(); return h.publishedOn("model-registry.audit.") }

func (h *harness) events(kind string) []published {
	h.relay()
	return h.publishedOn("model-registry.event." + kind + ".pub")
}

// checkPublishedSchemas validates every message model-registry published
// against its own schema (message-broker: payloads follow publisher-owned
// JSON Schemas).
func (h *harness) checkPublishedSchemas() {
	for _, m := range h.broker.Published() {
		parts := strings.Split(m.Subject, ".")
		if len(parts) != 4 || parts[0] != "model-registry" || parts[3] != "pub" {
			h.t.Errorf("model-registry published on a foreign or malformed subject %q", m.Subject)
			continue
		}
		switch parts[1] {
		case "event":
			validateOwn(h.t, "event."+parts[2]+".schema.json", m.Data)
		case "audit":
			validateOwn(h.t, "audit."+parts[2]+".schema.json", m.Data)
		default:
			h.t.Errorf("unexpected stream subject %q", m.Subject)
		}
		var env struct {
			Publisher    string `json:"publisher"`
			PublisherSeq int64  `json:"publisher_seq"`
			TeamID       *int64 `json:"team_id"`
		}
		_ = json.Unmarshal(m.Data, &env)
		if m.Header["Nats-Msg-Id"] != fmt.Sprintf("model-registry-%d", env.PublisherSeq) {
			h.t.Errorf("%s: Nats-Msg-Id %q does not match publisher_seq %d", m.Subject, m.Header["Nats-Msg-Id"], env.PublisherSeq)
		}
		if env.TeamID != nil && m.Header["team"] != strconv.FormatInt(*env.TeamID, 10) {
			h.t.Errorf("%s: team header %q, payload team_id %d", m.Subject, m.Header["team"], *env.TeamID)
		}
	}
}

// assertNoSecret fails when s appears in any published message, request,
// reply header or log line.
func (h *harness) assertNoSecret(s string) {
	h.t.Helper()
	h.relay()
	for _, m := range h.broker.Published() {
		if bytes.Contains(m.Data, []byte(s)) || strings.Contains(fmt.Sprint(m.Header), s) {
			h.t.Fatalf("secret in published %s", m.Subject)
		}
	}
	for _, m := range h.broker.Requests() {
		if strings.HasPrefix(m.Subject, "model-registry.") {
			continue // requests the test itself sent to model-registry
		}
		if bytes.Contains(m.Data, []byte(s)) || strings.Contains(fmt.Sprint(m.Header), s) {
			h.t.Fatalf("secret in request %s", m.Subject)
		}
	}
	if strings.Contains(h.logs.String(), s) {
		h.t.Fatalf("secret in logs")
	}
}

// ---------------------------------------------------------------------------
// Domain helpers
// ---------------------------------------------------------------------------

func (h *harness) createModel(team int64, name string, extra map[string]any) res {
	h.t.Helper()
	body := map[string]any{"name": name, "provider": "openai", "provider_model": "gpt-4o"}
	for k, v := range extra {
		body[k] = v
	}
	r := h.ui("model-create", team, nil, body, "")
	if r.status != 201 {
		h.t.Fatalf("create %s for team %d: status %d %s", name, team, r.status, r.body)
	}
	return r
}

func (h *harness) getModel(team int64, name string) res {
	h.t.Helper()
	return h.ui("model-get", team, map[string]any{"name": name}, nil, "")
}

func (h *harness) etag(team int64, name string) string {
	h.t.Helper()
	r := h.getModel(team, name)
	if r.status != 200 || r.header["ETag"] == "" {
		h.t.Fatalf("get %s: status %d etag %q %s", name, r.status, r.header["ETag"], r.body)
	}
	return r.header["ETag"]
}

func modelUpdateBody(paused bool) map[string]any {
	return map[string]any{"provider": "openai", "provider_model": "gpt-4o", "paused": paused}
}

func (h *harness) setPaused(team int64, name string, paused bool) res {
	h.t.Helper()
	return h.ui("model-update", team, map[string]any{"name": name}, modelUpdateBody(paused), h.etag(team, name))
}

func (h *harness) archive(team int64, name string) res {
	h.t.Helper()
	return h.ui("model-archive", team, map[string]any{"name": name}, nil, h.etag(team, name))
}

func (h *harness) unarchive(team int64, name string) res {
	h.t.Helper()
	return h.ui("model-unarchive", team, map[string]any{"name": name}, nil, h.etag(team, name))
}

func usd(v string) *string { return &v }

// applyFlowTeam sends apply-flow-team as flow-registry does on a Flow save.
func (h *harness) applyFlowTeam(team, flow, revision int64, limit any) res {
	h.t.Helper()
	return h.service("apply-flow-team", strconv.FormatInt(team, 10), map[string]any{
		"team_id": team, "flow_id": flow, "flow_revision": revision, "flow_limit": limit,
	})
}

// removeFlowTeam sends remove-flow-team as flow-registry does after a
// failed Flow create.
func (h *harness) removeFlowTeam(team, flow int64) res {
	h.t.Helper()
	return h.service("remove-flow-team", strconv.FormatInt(team, 10), map[string]any{"team_id": team, "flow_id": flow})
}

// readyClaim prepares everything a check after claim needs: executor
// projection, a Flow with its LiteLLM team, and a current claim.
func (h *harness) readyClaim(executor int64, k store.ClaimKey, flow int64, aliases []string, perTask, flowLimit any) {
	h.t.Helper()
	if d := h.registerExecutor(executor, k.TeamID); d.Outcome() != "ack" {
		h.t.Fatalf("executor_registered not acknowledged: %q", d.Outcome())
	}
	h.setFlow(k.TeamID, flow, fmt.Sprintf("Flow %d", flow), aliases, perTask, flowLimit)
	if r := h.applyFlowTeam(k.TeamID, flow, 1, flowLimit); r.status != 200 {
		h.t.Fatalf("apply-flow-team: %d %s", r.status, r.body)
	}
	h.setClaim(k, executor, flow)
}

func claim(team, task, attempt, seq int64) store.ClaimKey {
	return store.ClaimKey{TeamID: team, TaskID: task, Attempt: attempt, ClaimSeq: seq}
}

func mustOK(t testing.TB, r res, alias string) {
	t.Helper()
	result, got := checkResult(t, r)
	if result != "ok" || got != alias {
		t.Fatalf("check = %s %s (status %d %s), want ok %s", result, got, r.status, r.body, alias)
	}
}

func mustTemporary(t testing.TB, r res, reason string) {
	t.Helper()
	result, got := checkResult(t, r)
	if result != "temporary" || got != reason {
		t.Fatalf("check = %s %s (status %d %s), want temporary %s", result, got, r.status, r.body, reason)
	}
}

func items(t testing.TB, r res) []map[string]any {
	t.Helper()
	raw, _ := r.json(t)["items"].([]any)
	var out []map[string]any
	for _, it := range raw {
		out = append(out, it.(map[string]any))
	}
	return out
}

func num(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case json.Number:
		i, _ := n.Int64()
		return i
	}
	return -1
}

// llmModel returns the single LiteLLM deployment of alias.
func (h *harness) llmModel(alias string) mocks.LiteLLMModel {
	h.t.Helper()
	ms := h.llm.ModelByAlias(alias)
	if len(ms) != 1 {
		h.t.Fatalf("LiteLLM holds %d deployments of %s, want 1", len(ms), alias)
	}
	return ms[0]
}

// jsonInt reports whether v decoded from JSON is the integer want encoded
// as a JSON number (not a string).
func jsonInt(v any, want int64) bool {
	switch n := v.(type) {
	case float64:
		return n == float64(want)
	case json.Number:
		i, err := n.Int64()
		return err == nil && i == want
	}
	return false
}

// seedAdminModel stores an administrator-created model with owners, as a
// later change's administrator operation would.
func (h *harness) seedAdminModel(name string, owners ...int64) {
	h.t.Helper()
	alias := "system/" + name
	err := h.store.Tx(h.ctx, "seed-admin-model", func(tx store.Tx) error {
		id, err := tx.NextAdminModelID()
		if err != nil {
			return err
		}
		dep := h.llm.InsertModelDirectly(alias, map[string]any{"model": "openai/" + name}, map[string]any{"flowai_model_id": id})
		m := &store.Model{ModelID: id, CreatedBy: store.CreatedByAdmin, Name: name, Alias: alias, LiteLLMModelID: dep,
			Availability: store.AvailabilityActive, CreatedAt: h.clock(), UpdatedAt: h.clock()}
		if err := tx.InsertModel(m); err != nil {
			return err
		}
		for _, o := range owners {
			if err := tx.AddOwner(m.Key, o); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		h.t.Fatalf("seed admin model: %v", err)
	}
}

func listNames(t testing.TB, r res) []string {
	t.Helper()
	var out []string
	for _, it := range items(t, r) {
		out = append(out, it["name"].(string))
	}
	return out
}

func (h *harness) mustReconcile() {
	h.t.Helper()
	if err := h.app.ReconcileOnce(h.ctx); err != nil {
		h.t.Fatalf("reconcile: %v", err)
	}
}

// managementWrites counts LiteLLM calls that change state.
func (h *harness) managementWrites() int {
	n := 0
	for _, c := range h.llm.Calls() {
		if c.Method != http.MethodGet {
			n++
		}
	}
	return n
}

// numberIs reports whether v decoded from JSON (the fake LiteLLM keeps
// json.Number) is the JSON number want.
func numberIs(v any, want float64) bool {
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()
		return err == nil && f == want
	case float64:
		return n == want
	}
	return false
}

func bytesContain(b []byte, s string) bool { return strings.Contains(string(b), s) }

// flowTeam returns the LiteLLM team model-registry keeps for a Flow.
func (h *harness) flowTeam(team, flow int64) mocks.LiteLLMTeam {
	h.t.Helper()
	ft, ok := h.llm.TeamFor(team, flow)
	if !ok {
		h.t.Fatalf("no LiteLLM team for Flow (%d, %d)", team, flow)
	}
	return ft
}

// issueKey checks a ready claim and requests its key; it returns the key.
func (h *harness) issueKey(executor int64, k store.ClaimKey) string {
	h.t.Helper()
	r := h.keyEdge(strconv.FormatInt(k.TaskID, 10), h.token(k, executor))
	key, _ := r.body["OPENAI_API_KEY"].(string)
	if r.status != 200 || key == "" {
		h.t.Fatalf("key edge for %+v: %d %s", k, r.status, r.raw)
	}
	return key
}

func (h *harness) mustCheckOK(executor int64, k store.ClaimKey, alias string) {
	h.t.Helper()
	mustOK(h.t, h.check(executor, k, nil), alias)
}

func flowLimit(budget, period string) map[string]any {
	return map[string]any{"max_budget_usd": budget, "budget_period": period}
}

// selectable asks validate-models whether team may select alias.
func (h *harness) selectable(team int64, alias string) bool {
	h.t.Helper()
	r := h.service("validate-models", strconv.FormatInt(team, 10), map[string]any{"team_id": team, "aliases": []string{alias}})
	if r.status != 200 {
		h.t.Fatalf("validate-models: %d %s", r.status, r.body)
	}
	return r.json(h.t)["aliases"].([]any)[0].(map[string]any)["selectable"] == true
}

func (h *harness) mustRefreshAvailability() {
	h.t.Helper()
	if err := h.app.RefreshAvailability(h.ctx); err != nil {
		h.t.Fatalf("refresh availability: %v", err)
	}
}

func (h *harness) auditCount(action string) int {
	h.relay()
	return len(h.publishedOn("model-registry.audit." + action + ".pub"))
}

// redeliver delivers a message again, as JetStream does after a lost ack.
func (h *harness) redeliver(d *mocks.Delivery) *mocks.Delivery {
	h.t.Helper()
	again, err := h.broker.Deliver(h.ctx, consumerStream, consumerDurable, d.Message())
	if err != nil {
		h.t.Fatal(err)
	}
	return again
}

func (h *harness) removeExecutor(executor, team int64) *mocks.Delivery {
	h.t.Helper()
	seq := h.nextSeq()
	payload := map[string]any{
		"publisher": "executor-registry", "publisher_seq": seq, "team_id": team, "occurred_at": ts(h.clock()),
		"kind": "executor_removed", "executor_id": executor, "execution_tag": "gpu", "executor_type": "executor_docker_openhands",
	}
	raw, _ := json.Marshal(payload)
	validateForeign(h.t, schemaExecutorRemoved, raw)
	return h.deliver("executor-registry.event.executor_removed.sub",
		map[string]string{"team": strconv.FormatInt(team, 10), "Nats-Msg-Id": fmt.Sprintf("executor-registry-%d", seq)}, payload)
}

// assertSharedInput checks that qa-e2e/scripts/shared-test-inputs.json
// declares consumer for the publisher schema file.
func assertSharedInput(t testing.TB, schema, consumer string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(contractFixtureDir(t), "qa-e2e", "scripts", "shared-test-inputs.json"))
	if err != nil {
		t.Fatalf("shared-test-inputs.json: %v", err)
	}
	var doc struct {
		Schemas map[string][]string `json:"schemas"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("shared-test-inputs.json: %v", err)
	}
	if !slices.Contains(doc.Schemas[schema], consumer) {
		t.Fatalf("shared-test-inputs.json does not declare %s as a consumer of %s (has %v)", consumer, schema, doc.Schemas[schema])
	}
}

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }
