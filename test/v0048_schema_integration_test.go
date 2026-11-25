//go:build integration

package test

// 1.7 row: model-registry writes only model_registry_schema with its own
// role; another schema is refused. Also service-platform "Access to a
// foreign schema is denied" and rule 16 (schema pre-created, no CREATE on
// the database).

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/pandops/flowai-model-registry/internal/store"
	"github.com/pandops/flowai-model-registry/internal/store/postgres"
)

type pgRole struct {
	name, password string
	db             *sql.DB
}

// serviceRole creates a login role, optionally owning schema, as the
// deployment does (no CREATE on the database).
func serviceRole(t *testing.T, admin *sql.DB, dbName, schema string, ownSchema bool) pgRole {
	t.Helper()
	suffix, _ := randomToken(4)
	r := pgRole{name: "mr_role_" + suffix}
	r.password, _ = randomToken(12)
	mustExec(t, admin, `REVOKE CREATE ON DATABASE "`+dbName+`" FROM PUBLIC`)
	mustExec(t, admin, `REVOKE ALL ON SCHEMA public FROM PUBLIC`)
	mustExec(t, admin, `CREATE ROLE `+r.name+` LOGIN PASSWORD '`+r.password+`'`)
	mustExec(t, admin, `GRANT CONNECT ON DATABASE "`+dbName+`" TO `+r.name)
	if ownSchema {
		mustExec(t, admin, `CREATE SCHEMA `+schema+` AUTHORIZATION `+r.name)
	}
	var err error
	r.db, err = sql.Open("pgx", shared.roleDSN(r.name, r.password, dbName))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = r.db.Close()
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if ownSchema {
			_, _ = admin.ExecContext(c, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
		}
		_, _ = admin.ExecContext(c, `DROP OWNED BY `+r.name)
		if _, err := admin.ExecContext(c, `DROP ROLE `+r.name); err != nil {
			t.Errorf("cleanup: drop role %s: %v", r.name, err)
		}
	})
	return r
}

// openStore opens model-registry's store as role and migrates it.
func openStore(t *testing.T, r pgRole, dbName, schema string) *postgres.Store {
	t.Helper()
	s, err := postgres.Open(context.Background(), shared.roleDSN(r.name, r.password, dbName), schema)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate %s: %v", schema, err)
	}
	return s
}

func mustExec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func sqlState(err error) string {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return pg.Code
	}
	return ""
}

func TestV0048SchemaIsolationIntegration(t *testing.T) {
	ctx := context.Background()
	admin := freshDB(t)
	dbName := lastDBName
	mustExec(t, admin, `CREATE SCHEMA flow_registry_schema`)
	mustExec(t, admin, `CREATE TABLE flow_registry_schema.flows (team_id bigint, flow_id bigint)`)
	mustExec(t, admin, `INSERT INTO flow_registry_schema.flows VALUES (3, 21)`)
	role := serviceRole(t, admin, dbName, "model_registry_schema", true)
	s := openStore(t, role, dbName, "model_registry_schema")
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("second migrate is not idempotent: %v", err)
	}

	t.Run("migrations create objects only in model_registry_schema", func(t *testing.T) {
		rows, err := admin.QueryContext(ctx, `
			SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE pg_get_userbyid(c.relowner) = $1
			  AND n.nspname <> 'pg_toast' -- PostgreSQL stores out-of-line values of the service tables here`, role.name)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			var ns, rel string
			_ = rows.Scan(&ns, &rel)
			count++
			if ns != "model_registry_schema" {
				t.Errorf("migration created %s.%s outside model_registry_schema", ns, rel)
			}
		}
		if count == 0 {
			t.Fatal("migration created no object")
		}
	})

	t.Run("Access to a foreign schema is denied", func(t *testing.T) {
		var n int
		if err := role.db.QueryRowContext(ctx, `SELECT count(*) FROM flow_registry_schema.flows`).Scan(&n); sqlState(err) != "42501" {
			t.Fatalf("read of flow_registry_schema = %v, want 42501", err)
		}
		if _, err := role.db.ExecContext(ctx, `INSERT INTO flow_registry_schema.flows VALUES (4, 1)`); sqlState(err) != "42501" {
			t.Fatalf("write to flow_registry_schema = %v, want 42501", err)
		}
		if _, err := role.db.ExecContext(ctx, `CREATE TABLE public.mr_leak (x int)`); sqlState(err) != "42501" {
			t.Fatalf("create in public = %v, want 42501", err)
		}
	})

	t.Run("store works as the model-registry role", func(t *testing.T) {
		team := int64(3)
		err := s.Tx(ctx, store.TxModelCreate, func(tx store.Tx) error {
			id, err := tx.NextTeamCounter(3, store.CounterModel)
			if err != nil {
				return err
			}
			m := &store.Model{CreatorTeamID: &team, ModelID: id, CreatedBy: store.CreatedByTeam, Name: "m1", Alias: "3/m1",
				LiteLLMModelID: "dep-1", Availability: store.AvailabilityActive, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
			if err := tx.InsertModel(m); err != nil {
				return err
			}
			if err := tx.AddOwner(m.Key, 3); err != nil {
				return err
			}
			_, err = tx.Enqueue("model-registry.audit.model-create.pub", "3", nil, map[string]any{"team_id": 3})
			return err
		})
		if err != nil {
			t.Fatalf("transaction as the role: %v", err)
		}
		if err := s.Ping(ctx); err != nil {
			t.Fatal(err)
		}
	})

	// Deviation D17: remove-flow-team deletes the orphan Flow's team row and
	// its spend observation, and nothing of another Flow.
	t.Run("Flow team row and its spend observation are removed", func(t *testing.T) {
		now := time.Now().UTC()
		err := s.Tx(ctx, "flow-team-apply", func(tx store.Tx) error {
			for _, flow := range []int64{31, 32} {
				if err := tx.PutFlowTeam(store.FlowTeam{TeamID: 3, FlowID: flow, LiteLLMTeamID: "team-x", FlowRevision: 1, UpdatedAt: now}); err != nil {
					return err
				}
				if err := tx.PutSpendObservation(store.SpendObservation{TeamID: 3, FlowID: flow, ObservedAt: now}); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("seed: %v", err)
		}
		for range 2 { // the second delete finds nothing and still succeeds
			if err := s.Tx(ctx, "flow-team-remove", func(tx store.Tx) error { return tx.DeleteFlowTeam(3, 31) }); err != nil {
				t.Fatalf("delete: %v", err)
			}
		}
		err = s.Tx(ctx, "flow-team-read", func(tx store.Tx) error {
			for flow, want := range map[int64]bool{31: false, 32: true} {
				ft, err := tx.FlowTeam(3, flow, false)
				if err != nil {
					return err
				}
				obs, err := tx.SpendObservation(3, flow)
				if err != nil {
					return err
				}
				if (ft != nil) != want || (obs != nil) != want {
					t.Errorf("Flow %d: team row %v, spend row %v, want present = %v", flow, ft != nil, obs != nil, want)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	})

	t.Run("Configured schema and host are used", func(t *testing.T) {
		other := serviceRole(t, admin, dbName, "mr_custom_schema", true)
		openStore(t, other, dbName, "mr_custom_schema")
		var n int
		if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema='mr_custom_schema'`).Scan(&n); err != nil || n == 0 {
			t.Fatalf("tables in mr_custom_schema = %d %v", n, err)
		}
		if _, err := other.db.ExecContext(ctx, `SELECT 1 FROM model_registry_schema.models`); sqlState(err) != "42501" {
			t.Fatalf("second role reads the first schema: %v", err)
		}
	})
}
