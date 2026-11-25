// Package app is model-registry: the model, Flow team, check and key-edge
// behavior on top of its store, broker and LiteLLM boundaries.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pandops/flowai-model-registry/internal/broker"
	"github.com/pandops/flowai-model-registry/internal/config"
	"github.com/pandops/flowai-model-registry/internal/litellm"
	"github.com/pandops/flowai-model-registry/internal/store"
)

// Deps are model-registry's dependencies. Clock defaults to time.Now.
type Deps struct {
	Config config.Config
	Store  store.Store
	Broker broker.Broker
	Clock  func() time.Time
	Logger *slog.Logger
}

// App is one model-registry instance.
type App struct {
	deps  Deps
	llm   *litellm.Client
	log   *slog.Logger
	bound atomic.Bool // the durable consumer is bound
	ctx   context.Context

	issuing sync.Map // tokens of keys being created right now
	reqSeq  atomic.Int64
}

const (
	consumerStream  = "ALL_EVENT"
	consumerDurable = "model-registry"
)

// New builds an instance.
func New(d Deps) (*App, error) {
	if d.Store == nil || d.Broker == nil {
		return nil, errors.New("app: store and broker are required")
	}
	if d.Clock == nil {
		d.Clock = time.Now
	}
	if d.Logger == nil {
		d.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	a := &App{deps: d, log: d.Logger.With("service", publisher), ctx: context.Background()}
	a.llm = litellm.New(d.Config.LiteLLM.ManagementURL, d.Config.LiteLLM.MasterKey, d.Config.LiteLLM.Timeout)
	a.reqSeq.Store(d.Clock().UnixMilli())
	return a, nil
}

func (a *App) now() time.Time { return a.deps.Clock().UTC() }

// Start registers request handlers and binds the durable consumer. A broker
// that is down does not fail Start: Run keeps binding until it succeeds.
func (a *App) Start(ctx context.Context) error {
	a.ctx = context.WithoutCancel(ctx)
	serve := map[string]broker.Handler{
		"model-registry.ui.models-list.sub":     a.ui(a.modelsList),
		"model-registry.ui.model-get.sub":       a.ui(a.modelGet),
		"model-registry.ui.model-create.sub":    a.ui(a.modelCreate),
		"model-registry.ui.model-update.sub":    a.ui(a.modelUpdate),
		"model-registry.ui.model-archive.sub":   a.ui(a.modelArchive),
		"model-registry.ui.model-unarchive.sub": a.ui(a.modelUnarchive),

		"model-registry.service.validate-models.sub":  a.svc(a.validateModels),
		"model-registry.service.apply-flow-team.sub":  a.svc(a.applyFlowTeam),
		"model-registry.service.flows-status.sub":     a.svc(a.flowsStatus),
		"model-registry.service.remove-flow-team.sub": a.svc(a.removeFlowTeam),

		"model-registry.executor.*.check.sub": a.check,
	}
	for subject, h := range serve {
		if err := a.deps.Broker.Serve(subject, h); err != nil {
			return err
		}
	}
	a.bind(ctx)
	return nil
}

func (a *App) bind(ctx context.Context) {
	if a.bound.Load() {
		return
	}
	if err := a.deps.Broker.Consume(ctx, consumerStream, consumerDurable, a.consume); err != nil {
		a.log.Warn("durable consumer not bound yet", "error", err)
		return
	}
	a.bound.Store(true)
}

// Handler serves the key edge and the probes; every other request is 404.
func (a *App) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case r.Method == http.MethodGet && path == "/v1/livez":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && path == "/v1/readyz":
			if a.ready(r.Context()) {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
		case r.Method == http.MethodGet && strings.HasPrefix(path, "/v1/tasks/") && strings.HasSuffix(path, "/litellm-key") &&
			strings.Count(path, "/") == 4:
			task := strings.TrimSuffix(strings.TrimPrefix(path, "/v1/tasks/"), "/litellm-key")
			status, body := a.keyEdge(r.Context(), task, r.Header.Get("Authorization"))
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(body)
		default:
			http.NotFound(w, r)
		}
	})
}

func (a *App) ready(ctx context.Context) bool {
	if !a.deps.Broker.Connected() || !a.bound.Load() {
		return false
	}
	return a.deps.Store.Ping(ctx) == nil
}

// Run runs the background loops until ctx ends.
func (a *App) Run(ctx context.Context) error {
	cfg := a.deps.Config
	relay := time.NewTicker(orDefault(cfg.RelayInterval, config.DefaultRelay))
	reconcile := time.NewTicker(orDefault(cfg.ReconcileInterval, config.DefaultReconcile))
	avail := time.NewTicker(orDefault(cfg.LiteLLM.AvailabilityRefresh, config.DefaultRefresh))
	usage := time.NewTicker(orDefault(cfg.LiteLLM.UsageRefresh, config.DefaultRefresh))
	defer relay.Stop()
	defer reconcile.Stop()
	defer avail.Stop()
	defer usage.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-relay.C:
			a.bind(ctx)
			if _, err := a.RelayOnce(ctx); err != nil {
				a.log.Debug("relay", "error", err)
			}
		case <-reconcile.C:
			if err := a.ReconcileOnce(ctx); err != nil {
				a.log.Warn("reconcile", "error", err)
			}
		case <-avail.C:
			if err := a.RefreshAvailability(ctx); err != nil {
				a.log.Warn("availability refresh", "error", err)
			}
		case <-usage.C:
			if err := a.RefreshSpend(ctx); err != nil {
				a.log.Warn("spend refresh", "error", err)
			}
		}
	}
}

func orDefault(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

// RelayOnce publishes unsent outbox rows in order and marks them sent.
func (a *App) RelayOnce(ctx context.Context) (int, error) {
	var rows []store.OutboxRow
	if err := a.deps.Store.Tx(ctx, "relay-read", func(tx store.Tx) (err error) {
		rows, err = tx.Unsent(100)
		return err
	}); err != nil {
		return 0, err
	}
	n := 0
	for _, row := range rows {
		hdr := map[string]string{"Nats-Msg-Id": publisher + "-" + strconv.FormatInt(row.Seq, 10)}
		if row.Team != "" {
			hdr["team"] = row.Team
		}
		if row.RequestID != nil {
			hdr["request_id"] = strconv.FormatInt(*row.RequestID, 10)
		}
		if err := a.deps.Broker.Publish(ctx, broker.Message{Subject: row.Subject, Header: hdr, Data: row.Payload}); err != nil {
			return n, err
		}
		if err := a.deps.Store.Tx(ctx, "relay-mark", func(tx store.Tx) error { return tx.MarkSent(row.Seq, a.now()) }); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// call sends a request to another service's ".pub" subject with fields in
// params (rule 9). A non-2xx reply is returned with its status.
func (a *App) call(ctx context.Context, subject string, team int64, params map[string]any) (broker.Reply, error) {
	data, _ := json.Marshal(map[string]any{"params": params})
	hdr := map[string]string{"team": strconv.FormatInt(team, 10), "request_id": strconv.FormatInt(a.reqSeq.Add(1), 10)}
	timeout := orDefault(a.deps.Config.NATS.RequestTimeout, config.DefaultRequestTimeout)
	return a.deps.Broker.Request(ctx, broker.Message{Subject: subject, Header: hdr, Data: data}, timeout)
}

// ui wraps an operator operation: it validates the context headers first.
func (a *App) ui(op func(context.Context, uiCtx, request, map[string]string) broker.Reply) broker.Handler {
	return func(ctx context.Context, m broker.Message) broker.Reply {
		var rid *int64
		if v, ok := parseID(m.Header["request_id"]); ok {
			rid = &v
		}
		team, ok1 := parseID(m.Header["team"])
		operator, ok2 := parseID(m.Header["operator_id"])
		if !ok1 || !ok2 || rid == nil {
			return errReply(400, "invalid_request", rid)
		}
		req, err := decodeRequest(m.Data)
		if err != nil {
			return errReply(400, "invalid_request", rid)
		}
		return op(ctx, uiCtx{team: team, operator: operator, requestID: *rid}, req, m.Header)
	}
}

// svc wraps a flow-registry service operation: header team is a decimal
// team_id or admin; a decimal header must equal the body team_id.
func (a *App) svc(op func(context.Context, int64, json.RawMessage, *int64) broker.Reply) broker.Handler {
	return func(ctx context.Context, m broker.Message) broker.Reply {
		var rid *int64
		if v, ok := parseID(m.Header["request_id"]); ok {
			rid = &v
		}
		req, err := decodeRequest(m.Data)
		if err != nil {
			return errReply(400, "invalid_request", rid)
		}
		var head struct {
			TeamID json.RawMessage `json:"team_id"`
		}
		_ = json.Unmarshal(req.Body, &head)
		team, ok := jsonID(head.TeamID)
		if !ok {
			return errReply(400, "invalid_request", rid)
		}
		if h := m.Header["team"]; h != "admin" {
			ht, ok := parseID(h)
			if !ok || ht != team {
				return errReply(400, "invalid_request", rid)
			}
		}
		return op(ctx, team, req.Body, rid)
	}
}
