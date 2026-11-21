//go:build integration

package test

// 1.7 row: outbox publisher_seq from model-registry's own sequence in the
// committing transaction; rollback publishes nothing; restart continues.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pandops/flowai-model-registry/internal/store"
	"github.com/pandops/flowai-model-registry/internal/store/postgres"
)

func TestV0048PublisherSeqIntegration(t *testing.T) {
	ctx := context.Background()
	admin := freshDB(t)
	dbName := lastDBName
	role := serviceRole(t, admin, dbName, "model_registry_schema", true)
	s := openStore(t, role, dbName, "model_registry_schema")
	enqueue := func(s *postgres.Store, fail bool) (int64, error) {
		var seq int64
		err := s.Tx(ctx, "outbox-test", func(tx store.Tx) error {
			var err error
			seq, err = tx.Enqueue("model-registry.event.spend-observed.pub", "3", nil, map[string]any{"team_id": 3, "kind": "spend-observed"})
			if err == nil && fail {
				return errors.New("rollback")
			}
			return err
		})
		return seq, err
	}
	first, err := enqueue(s, false)
	if err != nil {
		t.Fatal(err)
	}
	lost, _ := enqueue(s, true)
	second, err := enqueue(s, false)
	if err != nil {
		t.Fatal(err)
	}
	if first >= lost || lost >= second {
		t.Fatalf("sequence %d, rolled back %d, next %d", first, lost, second)
	}
	var rows []store.OutboxRow
	_ = s.Tx(ctx, "relay", func(tx store.Tx) (err error) { rows, err = tx.Unsent(100); return })
	if len(rows) != 2 || rows[0].Seq != first || rows[1].Seq != second {
		t.Fatalf("unsent rows %+v", rows)
	}
	for _, r := range rows {
		if !bytesContain(r.Payload, `"publisher":"model-registry"`) {
			t.Fatalf("payload lacks publisher: %s", r.Payload)
		}
	}
	_ = s.Tx(ctx, "relay", func(tx store.Tx) error { return tx.MarkSent(first, time.Now().UTC()) })
	s.Close()

	restarted := openStore(t, role, dbName, "model_registry_schema")
	third, err := enqueue(restarted, false)
	if err != nil || third <= second {
		t.Fatalf("after restart seq %d (err %v), previous %d", third, err, second)
	}
	_ = restarted.Tx(ctx, "relay", func(tx store.Tx) (err error) { rows, err = tx.Unsent(100); return })
	if len(rows) != 2 || rows[0].Seq != second || rows[1].Seq != third {
		t.Fatalf("unsent after restart %+v", rows)
	}
}
