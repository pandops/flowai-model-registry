// Package config loads and validates model-registry's YAML configuration.
// Key names follow the deployment runtime contract (agents.md rule 16);
// config.example.yaml lists every key.
package config

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Defaults.
const (
	DefaultSchema         = "model_registry_schema"
	DefaultDatabase       = "flowai"
	DefaultListen         = ":8080"
	DefaultInboxPrefix    = "_INBOX.model-registry"
	DefaultRequestTimeout = 5 * time.Second
	DefaultLiteLLMTimeout = 10 * time.Second
	DefaultRefresh        = 60 * time.Second
	DefaultReconcile      = 5 * time.Second
	DefaultRelay          = 200 * time.Millisecond
	DefaultBackstop       = 24 * time.Hour
)

// DB is the PostgreSQL connection of the service's own schema and role.
type DB struct {
	Host     string
	Port     int
	User     string
	Password string
	Name     string
	Schema   string
	SSLMode  string
	CAFile   string
}

// NATS is the broker connection.
type NATS struct {
	URL             string
	CAFile          string
	CredentialsFile string
	InboxPrefix     string
	RequestTimeout  time.Duration
}

// LiteLLM is the LiteLLM gateway: management API and data-plane URLs.
type LiteLLM struct {
	ManagementURL         string
	MasterKey             string
	Timeout               time.Duration
	DataURLs              map[string]string // by executor_type
	APIBaseAllowedOrigins []string
	AvailabilityRefresh   time.Duration
	UsageRefresh          time.Duration
}

// Config is the validated configuration.
type Config struct {
	HTTPListen          string
	DB                  DB
	NATS                NATS
	LiteLLM             LiteLLM
	ClaimTokenKey       ed25519.PublicKey
	EncryptionKeys      map[int64][]byte // AES-256 keys of stored task keys
	EncryptionActiveKey int64            // highest id; encrypts new keys
	CursorKeys          map[int64][]byte
	CursorActiveKey     int64 // highest id; signs new cursors
	ReconcileInterval   time.Duration
	RelayInterval       time.Duration
	KeyBackstop         time.Duration
}

type rawKey struct {
	ID     int64  `yaml:"id"`
	KeyHex string `yaml:"key_hex"`
}

type raw struct {
	HTTP struct {
		Listen string `yaml:"listen"`
	} `yaml:"http"`
	DB struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
		Name     string `yaml:"name"`
		Schema   string `yaml:"schema"`
		SSLMode  string `yaml:"sslmode"`
		CAFile   string `yaml:"ca_file"`
	} `yaml:"db"`
	NATS struct {
		URL             string `yaml:"url"`
		CAFile          string `yaml:"ca_file"`
		CredentialsFile string `yaml:"credentials_file"`
		InboxPrefix     string `yaml:"inbox_prefix"`
		RequestTimeout  string `yaml:"request_timeout"`
	} `yaml:"nats"`
	LiteLLM struct {
		ManagementURL         string            `yaml:"management_url"`
		MasterKey             string            `yaml:"master_key"`
		Timeout               string            `yaml:"timeout"`
		DataURLs              map[string]string `yaml:"data_urls"`
		APIBaseAllowedOrigins []string          `yaml:"api_base_allowed_origins"`
		AvailabilityRefresh   int               `yaml:"availability_refresh_seconds"`
		UsageRefresh          int               `yaml:"usage_refresh_seconds"`
	} `yaml:"litellm"`
	ClaimToken struct {
		VerificationKey string `yaml:"verification_key"`
	} `yaml:"claim_token"`
	EncryptionKeys    []rawKey `yaml:"encryption_keys"`
	CursorKeys        []rawKey `yaml:"cursor_keys"`
	ReconcileInterval string   `yaml:"reconcile_interval"`
	Outbox            struct {
		RelayInterval string `yaml:"relay_interval"`
	} `yaml:"outbox"`
	TaskKeys struct {
		Backstop string `yaml:"backstop"`
	} `yaml:"task_keys"`
}

// Parse decodes and validates YAML configuration.
func Parse(data []byte) (Config, error) {
	var r raw
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&r); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	var c Config
	var err error

	c.HTTPListen = def(r.HTTP.Listen, DefaultListen)

	c.DB = DB{Host: r.DB.Host, Port: r.DB.Port, User: r.DB.User, Password: r.DB.Password,
		Name: def(r.DB.Name, DefaultDatabase), Schema: def(r.DB.Schema, DefaultSchema), SSLMode: r.DB.SSLMode, CAFile: r.DB.CAFile}
	if c.DB.Port == 0 {
		c.DB.Port = 5432
	}
	switch {
	case c.DB.Host == "":
		return Config{}, errors.New("db.host is required")
	case c.DB.User == "":
		return Config{}, errors.New("db.user is required")
	case c.DB.Password == "":
		return Config{}, errors.New("db.password is required")
	case c.DB.SSLMode != "verify-full":
		return Config{}, errors.New("db.sslmode must be verify-full")
	case !validIdent(c.DB.Schema):
		return Config{}, errors.New("db.schema is not a valid identifier")
	}
	if err := fileExists("db.ca_file", c.DB.CAFile); err != nil {
		return Config{}, err
	}

	c.NATS = NATS{URL: r.NATS.URL, CAFile: r.NATS.CAFile, CredentialsFile: r.NATS.CredentialsFile,
		InboxPrefix: def(r.NATS.InboxPrefix, DefaultInboxPrefix)}
	if !strings.HasPrefix(c.NATS.URL, "tls://") {
		return Config{}, errors.New("nats.url must use tls://")
	}
	if err := fileExists("nats.ca_file", c.NATS.CAFile); err != nil {
		return Config{}, err
	}
	if err := checkCreds(c.NATS.CredentialsFile); err != nil {
		return Config{}, err
	}
	if c.NATS.RequestTimeout, err = duration("nats.request_timeout", r.NATS.RequestTimeout, DefaultRequestTimeout); err != nil {
		return Config{}, err
	}

	c.LiteLLM = LiteLLM{ManagementURL: r.LiteLLM.ManagementURL, MasterKey: r.LiteLLM.MasterKey,
		DataURLs: map[string]string{}, APIBaseAllowedOrigins: r.LiteLLM.APIBaseAllowedOrigins}
	if u, err := url.Parse(c.LiteLLM.ManagementURL); err != nil || u.Host == "" {
		return Config{}, errors.New("litellm.management_url is required")
	}
	if c.LiteLLM.MasterKey == "" {
		return Config{}, errors.New("litellm.master_key is required")
	}
	for typ, u := range r.LiteLLM.DataURLs {
		if u != "" {
			c.LiteLLM.DataURLs[typ] = u
		}
	}
	for _, o := range c.LiteLLM.APIBaseAllowedOrigins {
		if u, err := url.Parse(o); err != nil || u.Scheme != "https" || u.Host == "" {
			return Config{}, fmt.Errorf("litellm.api_base_allowed_origins: %q is not an HTTPS origin", o)
		}
	}
	if c.LiteLLM.Timeout, err = duration("litellm.timeout", r.LiteLLM.Timeout, DefaultLiteLLMTimeout); err != nil {
		return Config{}, err
	}
	c.LiteLLM.AvailabilityRefresh = seconds(r.LiteLLM.AvailabilityRefresh)
	c.LiteLLM.UsageRefresh = seconds(r.LiteLLM.UsageRefresh)

	key, err := base64.StdEncoding.DecodeString(r.ClaimToken.VerificationKey)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return Config{}, errors.New("claim_token.verification_key must be a base64 Ed25519 public key")
	}
	c.ClaimTokenKey = ed25519.PublicKey(key)

	if c.EncryptionKeys, c.EncryptionActiveKey, err = keys("encryption_keys", r.EncryptionKeys); err != nil {
		return Config{}, err
	}
	if c.CursorKeys, c.CursorActiveKey, err = keys("cursor_keys", r.CursorKeys); err != nil {
		return Config{}, err
	}
	if c.ReconcileInterval, err = duration("reconcile_interval", r.ReconcileInterval, DefaultReconcile); err != nil {
		return Config{}, err
	}
	if c.RelayInterval, err = duration("outbox.relay_interval", r.Outbox.RelayInterval, DefaultRelay); err != nil {
		return Config{}, err
	}
	if c.KeyBackstop, err = duration("task_keys.backstop", r.TaskKeys.Backstop, DefaultBackstop); err != nil {
		return Config{}, err
	}
	return c, nil
}

// Load reads and parses a configuration file.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	return Parse(data)
}

// DSN is the pgx connection string of the configured database.
func (c Config) DSN() string {
	q := func(s string) string {
		if s != "" && !strings.ContainsAny(s, " '\\\t\n") {
			return s
		}
		return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
	}
	return strings.Join([]string{
		"host=" + q(c.DB.Host),
		"port=" + strconv.Itoa(c.DB.Port),
		"user=" + q(c.DB.User),
		"password=" + q(c.DB.Password),
		"dbname=" + q(c.DB.Name),
		"sslmode=" + c.DB.SSLMode,
		"sslrootcert=" + q(c.DB.CAFile),
		"search_path=" + c.DB.Schema,
	}, " ")
}

func def(v, d string) string {
	if v == "" {
		return d
	}
	return v
}

func seconds(n int) time.Duration {
	if n <= 0 {
		return DefaultRefresh
	}
	return time.Duration(n) * time.Second
}

func duration(key, v string, d time.Duration) (time.Duration, error) {
	if v == "" {
		return d, nil
	}
	out, err := time.ParseDuration(v)
	if err != nil || out <= 0 {
		return 0, fmt.Errorf("%s: invalid duration %q", key, v)
	}
	return out, nil
}

func fileExists(key, path string) error {
	if path == "" {
		return fmt.Errorf("%s is required", key)
	}
	if st, err := os.Stat(path); err != nil || st.IsDir() {
		return fmt.Errorf("%s: %s is not a readable file", key, path)
	}
	return nil
}

// checkCreds requires a NATS credentials file holding a user JWT and seed.
func checkCreds(path string) error {
	if err := fileExists("nats.credentials_file", path); err != nil {
		return err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return errors.New("nats.credentials_file is not readable")
	}
	s := string(raw)
	if !strings.Contains(s, "BEGIN NATS USER JWT") {
		return errors.New("nats.credentials_file holds no user JWT")
	}
	if !strings.Contains(s, "BEGIN USER NKEY SEED") {
		return errors.New("nats.credentials_file holds no user seed")
	}
	return nil
}

func keys(name string, in []rawKey) (map[int64][]byte, int64, error) {
	if len(in) == 0 {
		return nil, 0, fmt.Errorf("%s: at least one key is required", name)
	}
	out := map[int64][]byte{}
	var active int64
	for _, k := range in {
		b, err := hex.DecodeString(k.KeyHex)
		if k.ID < 1 || err != nil || len(b) != 32 {
			return nil, 0, fmt.Errorf("%s: key %d must have a positive id and 64 hex characters", name, k.ID)
		}
		if _, dup := out[k.ID]; dup {
			return nil, 0, fmt.Errorf("%s: duplicate id %d", name, k.ID)
		}
		out[k.ID] = b
		if k.ID > active {
			active = k.ID
		}
	}
	return out, active, nil
}

func validIdent(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for i, r := range s {
		if r != '_' && (r < 'a' || r > 'z') && (i == 0 || r < '0' || r > '9') {
			return false
		}
	}
	return true
}
