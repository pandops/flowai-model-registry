// Package litellm is model-registry's client for the LiteLLM management API.
// It never logs request bodies, so provider keys and virtual keys stay out of
// logs.
package litellm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var (
	// ErrUnavailable means LiteLLM did not answer or answered with a server
	// error; the outcome of a write is unknown.
	ErrUnavailable = errors.New("litellm: management API unavailable")
	// ErrNotFound means LiteLLM has no such object.
	ErrNotFound = errors.New("litellm: not found")
	// ErrRejected means LiteLLM refused the request (4xx other than 404).
	ErrRejected = errors.New("litellm: request rejected")
)

// Client calls the LiteLLM management API with the master key.
type Client struct {
	base      string
	masterKey string
	http      *http.Client
}

// New returns a management client.
func New(baseURL, masterKey string, timeout time.Duration) *Client {
	return &Client{base: strings.TrimRight(baseURL, "/"), masterKey: masterKey, http: &http.Client{Timeout: timeout}}
}

// Model is one LiteLLM deployment as FlowAI reads it. Numbers are
// json.Number so prices are read without rounding.
type Model struct {
	ID            string
	ModelName     string
	LiteLLMParams map[string]any
	ModelInfo     map[string]any
}

// Team is the part of a LiteLLM team FlowAI reads.
type Team struct {
	Spend         json.Number
	BudgetResetAt *time.Time
}

func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.masterKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return ErrUnavailable
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return ErrUnavailable
	}
	switch {
	case resp.StatusCode >= 500:
		return ErrUnavailable
	case resp.StatusCode == http.StatusNotFound:
		return ErrNotFound
	case resp.StatusCode >= 400:
		return fmt.Errorf("%w: status %d", ErrRejected, resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(out); err != nil {
		return ErrUnavailable
	}
	// Bytes after the JSON value make the readback unconfirmed.
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return ErrUnavailable
	}
	return nil
}

// CreateModel creates a deployment and returns its LiteLLM model id.
func (c *Client) CreateModel(ctx context.Context, name string, params, info map[string]any) (string, error) {
	var out struct {
		ModelID string `json:"model_id"`
	}
	err := c.do(ctx, http.MethodPost, "/model/new", map[string]any{"model_name": name, "litellm_params": params, "model_info": info}, &out)
	if err != nil {
		return "", err
	}
	if out.ModelID == "" {
		return "", ErrUnavailable
	}
	return out.ModelID, nil
}

// UpdateModel merges params and info into a deployment; a nil value removes
// the key. Keys absent from both maps stay as they are.
func (c *Client) UpdateModel(ctx context.Context, id string, params, info map[string]any) error {
	body := map[string]any{}
	if len(params) > 0 {
		body["litellm_params"] = params
	}
	if len(info) > 0 {
		body["model_info"] = info
	}
	return c.do(ctx, http.MethodPatch, "/model/"+url.PathEscape(id)+"/update", body, nil)
}

// DeleteModel deletes a deployment; a missing one is ErrNotFound.
func (c *Client) DeleteModel(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodPost, "/model/delete", map[string]any{"id": id}, nil)
}

type modelInfoReply struct {
	Data []struct {
		ModelName     string         `json:"model_name"`
		LiteLLMParams map[string]any `json:"litellm_params"`
		ModelInfo     map[string]any `json:"model_info"`
	} `json:"data"`
}

func (r modelInfoReply) models() []Model {
	out := make([]Model, 0, len(r.Data))
	for _, d := range r.Data {
		id, _ := d.ModelInfo["id"].(string)
		if d.LiteLLMParams == nil {
			d.LiteLLMParams = map[string]any{}
		}
		if d.ModelInfo == nil {
			d.ModelInfo = map[string]any{}
		}
		out = append(out, Model{ID: id, ModelName: d.ModelName, LiteLLMParams: d.LiteLLMParams, ModelInfo: d.ModelInfo})
	}
	return out
}

// GetModel reads one deployment by id; a missing one is ErrNotFound.
func (c *Client) GetModel(ctx context.Context, id string) (Model, error) {
	var out modelInfoReply
	if err := c.do(ctx, http.MethodGet, "/model/info?litellm_model_id="+url.QueryEscape(id), nil, &out); err != nil {
		return Model{}, err
	}
	for _, m := range out.models() {
		if m.ID == id {
			return m, nil
		}
	}
	return Model{}, ErrNotFound
}

// ListModels reads every deployment.
func (c *Client) ListModels(ctx context.Context) ([]Model, error) {
	var out modelInfoReply
	if err := c.do(ctx, http.MethodGet, "/model/info", nil, &out); err != nil {
		return nil, err
	}
	return out.models(), nil
}

// CreateCredential stores a provider key under name.
func (c *Client) CreateCredential(ctx context.Context, name, apiKey string) error {
	return c.do(ctx, http.MethodPost, "/credentials", map[string]any{
		"credential_name": name, "credential_values": map[string]any{"api_key": apiKey}, "credential_info": map[string]any{},
	}, nil)
}

// UpdateCredential replaces the provider key under name.
func (c *Client) UpdateCredential(ctx context.Context, name, apiKey string) error {
	return c.do(ctx, http.MethodPatch, "/credentials/"+url.PathEscape(name), map[string]any{
		"credential_name": name, "credential_values": map[string]any{"api_key": apiKey}, "credential_info": map[string]any{},
	}, nil)
}

// DeleteCredential deletes a credential; a missing one is ErrNotFound.
func (c *Client) DeleteCredential(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, "/credentials/"+url.PathEscape(name), nil, nil)
}

// CreateTeam creates a team from fields and returns its id.
func (c *Client) CreateTeam(ctx context.Context, fields map[string]any) (string, error) {
	var out struct {
		TeamID string `json:"team_id"`
	}
	if err := c.do(ctx, http.MethodPost, "/team/new", fields, &out); err != nil {
		return "", err
	}
	if out.TeamID == "" {
		return "", ErrUnavailable
	}
	return out.TeamID, nil
}

// UpdateTeam sets fields of a team; a field with a nil value is unset.
func (c *Client) UpdateTeam(ctx context.Context, id string, fields map[string]any) error {
	body := map[string]any{"team_id": id}
	for k, v := range fields {
		body[k] = v
	}
	return c.do(ctx, http.MethodPost, "/team/update", body, nil)
}

// DeleteTeam deletes a team; a missing one is ErrNotFound.
func (c *Client) DeleteTeam(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodPost, "/team/delete", map[string]any{"team_ids": []string{id}}, nil)
}

// TeamInfo reads a team's spend and next budget reset.
func (c *Client) TeamInfo(ctx context.Context, id string) (Team, error) {
	var out struct {
		TeamInfo struct {
			Spend         json.Number `json:"spend"`
			BudgetResetAt *string     `json:"budget_reset_at"`
		} `json:"team_info"`
	}
	if err := c.do(ctx, http.MethodGet, "/team/info?team_id="+url.QueryEscape(id), nil, &out); err != nil {
		return Team{}, err
	}
	t := Team{Spend: out.TeamInfo.Spend}
	if t.Spend == "" {
		t.Spend = "0"
	}
	if s := out.TeamInfo.BudgetResetAt; s != nil && *s != "" {
		if at, err := time.Parse(time.RFC3339Nano, *s); err == nil {
			at = at.UTC()
			t.BudgetResetAt = &at
		}
	}
	return t, nil
}

// KeySpec is one claim's virtual key.
type KeySpec struct {
	Key       string
	Model     string
	TeamID    string
	MaxBudget *json.Number
	RPMLimit  *int64
	TPMLimit  *int64
	Duration  time.Duration
	Metadata  map[string]any
}

// GenerateKey creates the virtual key spec.Key.
func (c *Client) GenerateKey(ctx context.Context, spec KeySpec) error {
	body := map[string]any{
		"key": spec.Key, "models": []string{spec.Model}, "team_id": spec.TeamID,
		"duration": durationString(spec.Duration), "metadata": spec.Metadata,
	}
	if spec.MaxBudget != nil {
		body["max_budget"] = *spec.MaxBudget
	}
	if spec.RPMLimit != nil {
		body["rpm_limit"] = *spec.RPMLimit
	}
	if spec.TPMLimit != nil {
		body["tpm_limit"] = *spec.TPMLimit
	}
	return c.do(ctx, http.MethodPost, "/key/generate", body, nil)
}

// ExtendKey sets the key's expiry to now plus d; token is its hash.
func (c *Client) ExtendKey(ctx context.Context, token string, d time.Duration) error {
	return c.do(ctx, http.MethodPost, "/key/update", map[string]any{"key": token, "duration": durationString(d)}, nil)
}

// DeleteKey revokes a key by its hash; a missing key is ErrNotFound.
func (c *Client) DeleteKey(ctx context.Context, token string) error {
	return c.do(ctx, http.MethodPost, "/key/delete", map[string]any{"keys": []string{token}}, nil)
}

func durationString(d time.Duration) string {
	return fmt.Sprintf("%ds", int64(d/time.Second))
}
