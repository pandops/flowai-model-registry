// Package postgres is model-registry's PostgreSQL store in its own schema.
package postgres

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pandops/flowai-model-registry/internal/store"
)

//go:embed schema.sql
var schemaSQL string

var identifier = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// Store is the PostgreSQL implementation of store.Store.
type Store struct {
	pool *pgxpool.Pool
	// counters runs counter increments outside the caller's transaction, so
	// a rolled-back create never returns its number to the pool.
	counters *pgxpool.Pool
	schema   string
	once     sync.Once
}

var _ store.Store = (*Store)(nil)

func newPool(ctx context.Context, dsn, schema string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("postgres: invalid DSN")
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}

// Open connects with dsn and uses schema for every object.
func Open(ctx context.Context, dsn, schema string) (*Store, error) {
	if !identifier.MatchString(schema) {
		return nil, fmt.Errorf("postgres: invalid schema name %q", schema)
	}
	pool, err := newPool(ctx, dsn, schema, 0)
	if err != nil {
		return nil, err
	}
	counters, err := newPool(ctx, dsn, schema, 2)
	if err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool, counters: counters, schema: schema}, nil
}

// Migrate creates or upgrades the service's objects in its own schema only.
// The schema is created only when missing, because the service role has no
// CREATE right on the database.
func (s *Store) Migrate(ctx context.Context) error {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)`, s.schema).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		if _, err := s.pool.Exec(ctx, `CREATE SCHEMA `+s.schema); err != nil {
			return fmt.Errorf("create schema %s: %w", s.schema, err)
		}
	}
	if _, err := s.pool.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("migrate %s: %w", s.schema, err)
	}
	return nil
}

// Close closes the pools; it is safe to call more than once.
func (s *Store) Close() {
	s.once.Do(func() {
		s.pool.Close()
		s.counters.Close()
	})
}

// Ping checks the database connection.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Tx runs fn in one transaction; any error rolls back.
func (s *Store) Tx(ctx context.Context, _ string, fn func(store.Tx) error) error {
	t, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = t.Rollback(context.WithoutCancel(ctx)) }()
	if err := fn(&tx{ctx: ctx, t: t, s: s}); err != nil {
		return err
	}
	return t.Commit(ctx)
}

type tx struct {
	ctx context.Context
	t   pgx.Tx
	s   *Store
}

func mapErr(err error) error {
	var pg *pgconn.PgError
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return store.ErrNotFound
	case errors.As(err, &pg) && pg.Code == "23505":
		return store.ErrConflict
	}
	return err
}

func (t *tx) exec(q string, args ...any) error {
	_, err := t.t.Exec(t.ctx, q, args...)
	return mapErr(err)
}

func forUpdate(lock bool) string {
	if lock {
		return " FOR UPDATE"
	}
	return ""
}

func utc(p *time.Time) *time.Time {
	if p == nil {
		return nil
	}
	v := p.UTC()
	return &v
}

func toJSON(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return json.Marshal(v)
}

// collect scans every row with scan.
func collect[T any](rows pgx.Rows, err error, scan func(pgx.Row) (T, error)) ([]T, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []T
	for rows.Next() {
		v, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (t *tx) NextTeamCounter(teamID int64, kind string) (int64, error) {
	var n int64
	err := t.s.counters.QueryRow(t.ctx, `INSERT INTO counters (team_id, kind, value) VALUES ($1, $2, 1)
		ON CONFLICT (team_id, kind) DO UPDATE SET value = counters.value + 1 RETURNING value`, teamID, kind).Scan(&n)
	return n, err
}

func (t *tx) NextAdminModelID() (int64, error) {
	var n int64
	err := t.t.QueryRow(t.ctx, `SELECT nextval('admin_model_seq')`).Scan(&n)
	return n, err
}

const modelCols = `key, creator_team_id, model_id, created_by, name, alias, litellm_model_id, availability, archived_at, created_at, updated_at`

func scanModel(r pgx.Row) (store.Model, error) {
	var m store.Model
	err := r.Scan(&m.Key, &m.CreatorTeamID, &m.ModelID, &m.CreatedBy, &m.Name, &m.Alias, &m.LiteLLMModelID,
		&m.Availability, &m.ArchivedAt, &m.CreatedAt, &m.UpdatedAt)
	m.ArchivedAt, m.CreatedAt, m.UpdatedAt = utc(m.ArchivedAt), m.CreatedAt.UTC(), m.UpdatedAt.UTC()
	return m, mapErr(err)
}

func (t *tx) oneModel(q string, args ...any) (*store.Model, error) {
	m, err := scanModel(t.t.QueryRow(t.ctx, q, args...))
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (t *tx) InsertModel(m *store.Model) error {
	return mapErr(t.t.QueryRow(t.ctx, `INSERT INTO models (creator_team_id, model_id, created_by, name, alias, litellm_model_id,
		availability, archived_at, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING key`,
		m.CreatorTeamID, m.ModelID, m.CreatedBy, m.Name, m.Alias, m.LiteLLMModelID, m.Availability, m.ArchivedAt,
		m.CreatedAt, m.UpdatedAt).Scan(&m.Key))
}

func (t *tx) ModelByKey(key int64, lock bool) (*store.Model, error) {
	return t.oneModel(`SELECT `+modelCols+` FROM models WHERE key = $1`+forUpdate(lock), key)
}

func (t *tx) ModelByAlias(alias string, lock bool) (*store.Model, error) {
	return t.oneModel(`SELECT `+modelCols+` FROM models WHERE alias = $1`+forUpdate(lock), alias)
}

func (t *tx) UpdateModel(m *store.Model) error {
	tag, err := t.t.Exec(t.ctx, `UPDATE models SET name = $2, litellm_model_id = $3, availability = $4, archived_at = $5,
		updated_at = $6 WHERE key = $1 AND alias = $7 AND model_id = $8`,
		m.Key, m.Name, m.LiteLLMModelID, m.Availability, m.ArchivedAt, m.UpdatedAt, m.Alias, m.ModelID)
	if err != nil {
		return mapErr(err)
	}
	if tag.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (t *tx) Models() ([]store.Model, error) {
	rows, err := t.t.Query(t.ctx, `SELECT `+modelCols+` FROM models ORDER BY key`)
	return collect(rows, err, scanModel)
}

func (t *tx) AddOwner(modelKey, teamID int64) error {
	err := t.exec(`INSERT INTO model_owners (model_key, team_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, modelKey, teamID)
	var pg *pgconn.PgError
	if errors.As(err, &pg) && pg.Code == "23503" {
		return store.ErrNotFound
	}
	return err
}

func (t *tx) Owners(modelKey int64) ([]int64, error) {
	rows, err := t.t.Query(t.ctx, `SELECT team_id FROM model_owners WHERE model_key = $1 ORDER BY team_id`, modelKey)
	return collect(rows, err, func(r pgx.Row) (v int64, err error) { err = r.Scan(&v); return })
}

const ownedFrom = ` FROM models m JOIN model_owners o ON o.model_key = m.key AND o.team_id = $1`

func (t *tx) OwnedModels(teamID int64, archived bool, afterName string, limit int) ([]store.Model, error) {
	rows, err := t.t.Query(t.ctx, `SELECT m.`+modelCols[len("key, "):]+`, m.key`+ownedFrom+`
		WHERE (m.archived_at IS NOT NULL) = $2 AND ($3 = '' OR m.name > $3)
		ORDER BY m.name COLLATE "C", m.alias COLLATE "C" LIMIT $4`, teamID, archived, afterName, limit)
	return collect(rows, err, scanOwned)
}

// scanOwned scans the owned-model column order, which puts key last.
func scanOwned(r pgx.Row) (store.Model, error) {
	var m store.Model
	err := r.Scan(&m.CreatorTeamID, &m.ModelID, &m.CreatedBy, &m.Name, &m.Alias, &m.LiteLLMModelID,
		&m.Availability, &m.ArchivedAt, &m.CreatedAt, &m.UpdatedAt, &m.Key)
	m.ArchivedAt, m.CreatedAt, m.UpdatedAt = utc(m.ArchivedAt), m.CreatedAt.UTC(), m.UpdatedAt.UTC()
	return m, mapErr(err)
}

func (t *tx) OwnedModelByName(teamID int64, name string) (*store.Model, error) {
	// The team's own model wins over an administrator model of the same name.
	m, err := scanOwned(t.t.QueryRow(t.ctx, `SELECT m.`+modelCols[len("key, "):]+`, m.key`+ownedFrom+`
		WHERE m.name = $2 ORDER BY (m.creator_team_id IS NOT DISTINCT FROM $1) DESC, m.alias COLLATE "C" LIMIT 1`, teamID, name))
	if err != nil {
		return nil, err
	}
	return &m, nil
}

const opCols = `model_key, kind, team_id, operator_id, request_id, created_at`

func scanOp(r pgx.Row) (store.Operation, error) {
	var o store.Operation
	err := r.Scan(&o.ModelKey, &o.Kind, &o.TeamID, &o.OperatorID, &o.RequestID, &o.CreatedAt)
	o.CreatedAt = o.CreatedAt.UTC()
	return o, mapErr(err)
}

func (t *tx) PutOperation(op store.Operation) error {
	return t.exec(`INSERT INTO operations (`+opCols+`) VALUES ($1,$2,$3,$4,$5,$6)`,
		op.ModelKey, op.Kind, op.TeamID, op.OperatorID, op.RequestID, op.CreatedAt)
}

func (t *tx) Operation(modelKey int64) (*store.Operation, error) {
	o, err := scanOp(t.t.QueryRow(t.ctx, `SELECT `+opCols+` FROM operations WHERE model_key = $1`, modelKey))
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &o, nil
}

func (t *tx) DeleteOperation(modelKey int64) error {
	return t.exec(`DELETE FROM operations WHERE model_key = $1`, modelKey)
}

func (t *tx) Operations() ([]store.Operation, error) {
	rows, err := t.t.Query(t.ctx, `SELECT `+opCols+` FROM operations ORDER BY model_key`)
	return collect(rows, err, scanOp)
}

func (t *tx) AddCompensation(c store.Compensation) error {
	return t.exec(`INSERT INTO compensations (team_id, model_id, alias, litellm_model_id, credential_name, created_at)
		VALUES ($1,$2,$3,$4,$5,$6)`, c.TeamID, c.ModelID, c.Alias, c.LiteLLMModelID, c.CredentialName, c.CreatedAt)
}

func (t *tx) Compensations() ([]store.Compensation, error) {
	rows, err := t.t.Query(t.ctx, `SELECT id, team_id, model_id, alias, litellm_model_id, credential_name, created_at
		FROM compensations ORDER BY id`)
	return collect(rows, err, func(r pgx.Row) (c store.Compensation, err error) {
		err = r.Scan(&c.ID, &c.TeamID, &c.ModelID, &c.Alias, &c.LiteLLMModelID, &c.CredentialName, &c.CreatedAt)
		c.CreatedAt = c.CreatedAt.UTC()
		return
	})
}

func (t *tx) DeleteCompensation(id int64) error {
	return t.exec(`DELETE FROM compensations WHERE id = $1`, id)
}

const ftCols = `team_id, flow_id, litellm_team_id, flow_revision, flow_limit, budget_reset_at, updated_at`

func scanFlowTeam(r pgx.Row) (store.FlowTeam, error) {
	var ft store.FlowTeam
	var limit []byte
	if err := r.Scan(&ft.TeamID, &ft.FlowID, &ft.LiteLLMTeamID, &ft.FlowRevision, &limit, &ft.BudgetResetAt, &ft.UpdatedAt); err != nil {
		return ft, mapErr(err)
	}
	ft.BudgetResetAt, ft.UpdatedAt = utc(ft.BudgetResetAt), ft.UpdatedAt.UTC()
	if limit != nil {
		ft.Limit = &store.FlowLimit{}
		if err := json.Unmarshal(limit, ft.Limit); err != nil {
			return ft, err
		}
	}
	return ft, nil
}

func (t *tx) FlowTeam(teamID, flowID int64, lock bool) (*store.FlowTeam, error) {
	ft, err := scanFlowTeam(t.t.QueryRow(t.ctx, `SELECT `+ftCols+` FROM flow_teams WHERE team_id = $1 AND flow_id = $2`+forUpdate(lock), teamID, flowID))
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &ft, nil
}

func (t *tx) PutFlowTeam(ft store.FlowTeam) error {
	var limit any
	if ft.Limit != nil {
		raw, err := toJSON(ft.Limit)
		if err != nil {
			return err
		}
		limit = raw
	}
	return t.exec(`INSERT INTO flow_teams (`+ftCols+`) VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (team_id, flow_id) DO UPDATE SET litellm_team_id = EXCLUDED.litellm_team_id,
		flow_revision = EXCLUDED.flow_revision, flow_limit = EXCLUDED.flow_limit,
		budget_reset_at = EXCLUDED.budget_reset_at, updated_at = EXCLUDED.updated_at`,
		ft.TeamID, ft.FlowID, ft.LiteLLMTeamID, ft.FlowRevision, limit, ft.BudgetResetAt, ft.UpdatedAt)
}

func (t *tx) FlowTeams() ([]store.FlowTeam, error) {
	rows, err := t.t.Query(t.ctx, `SELECT `+ftCols+` FROM flow_teams ORDER BY team_id, flow_id`)
	return collect(rows, err, scanFlowTeam)
}

func (t *tx) DeleteFlowTeam(teamID, flowID int64) error {
	if err := t.exec(`DELETE FROM spend_observations WHERE team_id = $1 AND flow_id = $2`, teamID, flowID); err != nil {
		return err
	}
	return t.exec(`DELETE FROM flow_teams WHERE team_id = $1 AND flow_id = $2`, teamID, flowID)
}

func (t *tx) SpendObservation(teamID, flowID int64) (*store.SpendObservation, error) {
	var o store.SpendObservation
	err := t.t.QueryRow(t.ctx, `SELECT team_id, flow_id, succeeded, spend_usd, budget_reset_at, observed_at,
		last_success_spend_usd, last_success_reset_at, last_success_at FROM spend_observations
		WHERE team_id = $1 AND flow_id = $2`, teamID, flowID).Scan(&o.TeamID, &o.FlowID, &o.Succeeded, &o.SpendUSD,
		&o.BudgetResetAt, &o.ObservedAt, &o.LastSuccessSpendUSD, &o.LastSuccessResetAt, &o.LastSuccessAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	o.BudgetResetAt, o.ObservedAt = utc(o.BudgetResetAt), o.ObservedAt.UTC()
	o.LastSuccessResetAt, o.LastSuccessAt = utc(o.LastSuccessResetAt), utc(o.LastSuccessAt)
	return &o, nil
}

func (t *tx) PutSpendObservation(o store.SpendObservation) error {
	return t.exec(`INSERT INTO spend_observations (team_id, flow_id, succeeded, spend_usd, budget_reset_at, observed_at,
		last_success_spend_usd, last_success_reset_at, last_success_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (team_id, flow_id) DO UPDATE SET succeeded = EXCLUDED.succeeded, spend_usd = EXCLUDED.spend_usd,
		budget_reset_at = EXCLUDED.budget_reset_at, observed_at = EXCLUDED.observed_at,
		last_success_spend_usd = EXCLUDED.last_success_spend_usd, last_success_reset_at = EXCLUDED.last_success_reset_at,
		last_success_at = EXCLUDED.last_success_at`,
		o.TeamID, o.FlowID, o.Succeeded, o.SpendUSD, o.BudgetResetAt, o.ObservedAt, o.LastSuccessSpendUSD,
		o.LastSuccessResetAt, o.LastSuccessAt)
}

func (t *tx) Executor(id int64) (*store.Executor, error) {
	var e store.Executor
	err := t.t.QueryRow(t.ctx, `SELECT executor_id, team_id, removed FROM executors WHERE executor_id = $1`, id).
		Scan(&e.ExecutorID, &e.TeamID, &e.Removed)
	if err != nil {
		return nil, mapErr(err)
	}
	return &e, nil
}

func (t *tx) PutExecutor(e store.Executor) error {
	return t.exec(`INSERT INTO executors (executor_id, team_id, removed) VALUES ($1,$2,$3)
		ON CONFLICT (executor_id) DO UPDATE SET team_id = EXCLUDED.team_id, removed = EXCLUDED.removed`,
		e.ExecutorID, e.TeamID, e.Removed)
}

const snapCols = `team_id, task_id, attempt, claim_seq, executor_id, flow_id, model_alias, model_key, per_task_limit, created_at, ended_at`
const claimWhere = ` WHERE team_id = $1 AND task_id = $2 AND attempt = $3 AND claim_seq = $4`

func scanSnapshot(r pgx.Row) (store.Snapshot, error) {
	var s store.Snapshot
	var limit []byte
	if err := r.Scan(&s.TeamID, &s.TaskID, &s.Attempt, &s.ClaimSeq, &s.ExecutorID, &s.FlowID, &s.ModelAlias, &s.ModelKey,
		&limit, &s.CreatedAt, &s.EndedAt); err != nil {
		return s, mapErr(err)
	}
	s.CreatedAt, s.EndedAt = s.CreatedAt.UTC(), utc(s.EndedAt)
	if limit != nil {
		s.PerTaskLimit = &store.PerTaskLimit{}
		if err := json.Unmarshal(limit, s.PerTaskLimit); err != nil {
			return s, err
		}
	}
	return s, nil
}

func (t *tx) Snapshot(k store.ClaimKey, lock bool) (*store.Snapshot, error) {
	s, err := scanSnapshot(t.t.QueryRow(t.ctx, `SELECT `+snapCols+` FROM snapshots`+claimWhere+forUpdate(lock),
		k.TeamID, k.TaskID, k.Attempt, k.ClaimSeq))
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func (t *tx) InsertSnapshot(s store.Snapshot) error {
	var limit any
	if s.PerTaskLimit != nil {
		raw, err := toJSON(s.PerTaskLimit)
		if err != nil {
			return err
		}
		limit = raw
	}
	return t.exec(`INSERT INTO snapshots (`+snapCols+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		s.TeamID, s.TaskID, s.Attempt, s.ClaimSeq, s.ExecutorID, s.FlowID, s.ModelAlias, s.ModelKey, limit, s.CreatedAt, s.EndedAt)
}

func (t *tx) UpdateSnapshot(s store.Snapshot) error {
	tag, err := t.t.Exec(t.ctx, `UPDATE snapshots SET ended_at = $5`+claimWhere, s.TeamID, s.TaskID, s.Attempt, s.ClaimSeq, s.EndedAt)
	if err != nil {
		return mapErr(err)
	}
	if tag.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (t *tx) TaskSnapshots(teamID, taskID int64) ([]store.Snapshot, error) {
	rows, err := t.t.Query(t.ctx, `SELECT `+snapCols+` FROM snapshots WHERE team_id = $1 AND task_id = $2
		ORDER BY attempt, claim_seq`, teamID, taskID)
	return collect(rows, err, scanSnapshot)
}

func (t *tx) UnfinishedCount(modelKey int64) (int, error) {
	var n int
	err := t.t.QueryRow(t.ctx, `SELECT count(*) FROM snapshots s LEFT JOIN task_keys k USING (team_id, task_id, attempt, claim_seq)
		WHERE s.model_key = $1 AND s.ended_at IS NULL AND k.revoke_requested_at IS NULL`, modelKey).Scan(&n)
	return n, err
}

const keyCols = `team_id, task_id, attempt, claim_seq, token, ciphertext, expires_at, extended_at, revoke_cause, revoke_requested_at, revoked_at`

func scanKey(r pgx.Row) (store.TaskKey, error) {
	var k store.TaskKey
	err := r.Scan(&k.TeamID, &k.TaskID, &k.Attempt, &k.ClaimSeq, &k.Token, &k.Ciphertext, &k.ExpiresAt, &k.ExtendedAt,
		&k.RevokeCause, &k.RevokeRequestedAt, &k.RevokedAt)
	k.ExpiresAt, k.ExtendedAt = k.ExpiresAt.UTC(), k.ExtendedAt.UTC()
	k.RevokeRequestedAt, k.RevokedAt = utc(k.RevokeRequestedAt), utc(k.RevokedAt)
	return k, mapErr(err)
}

func (t *tx) TaskKey(k store.ClaimKey) (*store.TaskKey, error) {
	key, err := scanKey(t.t.QueryRow(t.ctx, `SELECT `+keyCols+` FROM task_keys`+claimWhere, k.TeamID, k.TaskID, k.Attempt, k.ClaimSeq))
	if err != nil {
		return nil, err
	}
	return &key, nil
}

func (t *tx) InsertTaskKey(k store.TaskKey) error {
	return t.exec(`INSERT INTO task_keys (`+keyCols+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		k.TeamID, k.TaskID, k.Attempt, k.ClaimSeq, k.Token, k.Ciphertext, k.ExpiresAt, k.ExtendedAt,
		k.RevokeCause, k.RevokeRequestedAt, k.RevokedAt)
}

func (t *tx) UpdateTaskKey(k store.TaskKey) error {
	tag, err := t.t.Exec(t.ctx, `UPDATE task_keys SET token = $5, ciphertext = $6, expires_at = $7, extended_at = $8,
		revoke_cause = $9, revoke_requested_at = $10, revoked_at = $11`+claimWhere,
		k.TeamID, k.TaskID, k.Attempt, k.ClaimSeq, k.Token, k.Ciphertext, k.ExpiresAt, k.ExtendedAt,
		k.RevokeCause, k.RevokeRequestedAt, k.RevokedAt)
	if err != nil {
		return mapErr(err)
	}
	if tag.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (t *tx) PendingRevocations() ([]store.TaskKey, error) {
	rows, err := t.t.Query(t.ctx, `SELECT `+keyCols+` FROM task_keys
		WHERE revoke_requested_at IS NOT NULL AND revoked_at IS NULL ORDER BY revoke_requested_at`)
	return collect(rows, err, scanKey)
}

func (t *tx) InsertOrphan(o store.Orphan) (int64, error) {
	var id int64
	err := t.t.QueryRow(t.ctx, `INSERT INTO orphans (team_id, task_id, attempt, claim_seq, token, created_at)
		VALUES ($1,$2,$3,$4,$5,$6) RETURNING id`, o.TeamID, o.TaskID, o.Attempt, o.ClaimSeq, o.Token, o.CreatedAt).Scan(&id)
	return id, mapErr(err)
}

func (t *tx) DeleteOrphan(id int64) error {
	return t.exec(`DELETE FROM orphans WHERE id = $1`, id)
}

func (t *tx) Orphans() ([]store.Orphan, error) {
	rows, err := t.t.Query(t.ctx, `SELECT id, team_id, task_id, attempt, claim_seq, token, created_at FROM orphans ORDER BY id`)
	return collect(rows, err, func(r pgx.Row) (o store.Orphan, err error) {
		err = r.Scan(&o.ID, &o.TeamID, &o.TaskID, &o.Attempt, &o.ClaimSeq, &o.Token, &o.CreatedAt)
		o.CreatedAt = o.CreatedAt.UTC()
		return
	})
}

func (t *tx) MarkConsumed(publisher string, seq int64) (bool, error) {
	tag, err := t.t.Exec(t.ctx, `INSERT INTO consumed (publisher, seq) VALUES ($1, $2) ON CONFLICT DO NOTHING`, publisher, seq)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (t *tx) Enqueue(subject, team string, requestID *int64, payload map[string]any) (int64, error) {
	var seq int64
	if err := t.t.QueryRow(t.ctx, `SELECT nextval('publisher_seq')`).Scan(&seq); err != nil {
		return 0, err
	}
	payload = maps.Clone(payload)
	payload["publisher"] = "model-registry"
	payload["publisher_seq"] = seq
	raw, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	err = t.exec(`INSERT INTO outbox (seq, subject, team, request_id, payload, created_at) VALUES ($1,$2,$3,$4,$5,$6)`,
		seq, subject, team, requestID, raw, time.Now().UTC())
	return seq, err
}

func (t *tx) Unsent(limit int) ([]store.OutboxRow, error) {
	rows, err := t.t.Query(t.ctx, `SELECT seq, subject, team, request_id, payload, created_at, sent_at FROM outbox
		WHERE sent_at IS NULL ORDER BY seq LIMIT $1`, limit)
	return collect(rows, err, func(r pgx.Row) (o store.OutboxRow, err error) {
		err = r.Scan(&o.Seq, &o.Subject, &o.Team, &o.RequestID, &o.Payload, &o.CreatedAt, &o.SentAt)
		o.CreatedAt, o.SentAt = o.CreatedAt.UTC(), utc(o.SentAt)
		return
	})
}

func (t *tx) MarkSent(seq int64, at time.Time) error {
	tag, err := t.t.Exec(t.ctx, `UPDATE outbox SET sent_at = $2 WHERE seq = $1`, seq, at)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return nil
}
