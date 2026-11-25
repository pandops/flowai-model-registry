package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"time"

	"github.com/pandops/flowai-model-registry/internal/broker"
	"github.com/pandops/flowai-model-registry/internal/store"
)

const (
	publisher = "model-registry"
	tsLayout  = "2006-01-02T15:04:05.000Z"
)

var decimalID = regexp.MustCompile(`^[1-9][0-9]{0,18}$`)

// parseID parses a positive decimal int64 identifier.
func parseID(s string) (int64, bool) {
	if !decimalID.MatchString(s) {
		return 0, false
	}
	v, err := strconv.ParseInt(s, 10, 64)
	return v, err == nil && v > 0
}

// jsonID reads a positive integer JSON number (not a string).
func jsonID(raw json.RawMessage) (int64, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] == '"' {
		return 0, false
	}
	return parseID(string(raw))
}

func ts(t time.Time) string { return t.UTC().Format(tsLayout) }

func tsPtr(t *time.Time) any {
	if t == nil {
		return nil
	}
	return ts(*t)
}

// request is a decoded rule-9 request envelope.
type request struct {
	Params map[string]json.RawMessage `json:"params"`
	Body   json.RawMessage            `json:"body"`
}

func decodeRequest(data []byte) (request, error) {
	var r request
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return request{}, err
	}
	if r.Params == nil {
		r.Params = map[string]json.RawMessage{}
	}
	return r, nil
}

// strictBody decodes a body object and reports unknown or duplicate keys.
func strictBody(raw json.RawMessage) (map[string]json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 || string(bytes.TrimSpace(raw)) == "null" {
		return nil, errors.New("body is required")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return nil, errors.New("body is not an object")
	}
	out := map[string]json.RawMessage{}
	for dec.More() {
		k, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, _ := k.(string)
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return nil, err
		}
		if _, dup := out[key]; dup {
			return nil, errors.New("duplicate field " + key)
		}
		out[key] = v
	}
	return out, nil
}

func onlyKeys(m map[string]json.RawMessage, allowed ...string) bool {
	for k := range m {
		ok := false
		for _, a := range allowed {
			if k == a {
				ok = true
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

func isNull(raw json.RawMessage) bool { return string(bytes.TrimSpace(raw)) == "null" }

func rawString(raw json.RawMessage) (string, bool) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil || isNull(raw) {
		return "", false
	}
	return s, true
}

func rawBool(raw json.RawMessage) (bool, bool) {
	var b bool
	if err := json.Unmarshal(raw, &b); err != nil || isNull(raw) {
		return false, false
	}
	return b, true
}

// errBody is the rule-9 error object.
func errBody(code, message string, requestID *int64, extra map[string]any) []byte {
	m := map[string]any{"code": code, "message": message} // no request_id without a valid header (D12)
	if requestID != nil {
		m["request_id"] = *requestID
	}
	for k, v := range extra {
		m[k] = v
	}
	raw, _ := json.Marshal(m)
	return raw
}

func errReply(status int, code string, requestID *int64) broker.Reply {
	return broker.Reply{Status: status, Body: errBody(code, code, requestID, nil)}
}

func jsonReply(status int, body any) broker.Reply {
	raw, _ := json.Marshal(body)
	return broker.Reply{Status: status, Body: raw}
}

// emit stores one event in the outbox of tx.
func (a *App) emit(tx store.Tx, team int64, kind string, fields map[string]any) error {
	p := map[string]any{"team_id": team, "occurred_at": ts(a.now()), "kind": kind}
	for k, v := range fields {
		p[k] = v
	}
	_, err := tx.Enqueue("model-registry.event."+kind+".pub", strconv.FormatInt(team, 10), nil, p)
	return err
}

// uiCtx is the verified operator context of a ui request.
type uiCtx struct {
	team, operator, requestID int64
}

func (c uiCtx) rid() *int64 { return &c.requestID }

// auditFields is what one audit message records.
type auditFields struct {
	action, outcome, reason, name string
	resourceID                    *int64
	paused                        *bool
}

func (a *App) writeAudit(tx store.Tx, c uiCtx, f auditFields) error {
	changes := map[string]any{}
	if f.name != "" {
		changes["model_name"] = f.name
	}
	if f.paused != nil {
		changes["paused"] = *f.paused
	}
	p := map[string]any{
		"team_id": c.team, "occurred_at": ts(a.now()), "source_service": publisher, "actor_type": "operator",
		"actor_id": c.operator, "action": f.action, "resource_type": "model", "resource_id": nil,
		"request_id": c.requestID, "outcome": f.outcome, "changes": changes,
	}
	if f.resourceID != nil {
		p["resource_id"] = *f.resourceID
	}
	if f.reason != "" {
		p["reason"] = f.reason
	}
	rid := c.requestID
	_, err := tx.Enqueue("model-registry.audit."+f.action+".pub", strconv.FormatInt(c.team, 10), &rid, p)
	return err
}

// auditRejected records a rejected operator action in its own transaction.
// It is best effort: the reply to the operator does not depend on it.
func (a *App) auditRejected(ctx context.Context, c uiCtx, action, name, reason string, resourceID *int64) {
	err := a.deps.Store.Tx(ctx, "audit-rejected", func(tx store.Tx) error {
		return a.writeAudit(tx, c, auditFields{action: action, outcome: "rejected", reason: reason, name: name, resourceID: resourceID})
	})
	if err != nil {
		a.log.Warn("rejected-action audit not stored", "action", action, "error", err)
	}
}
