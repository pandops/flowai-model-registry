//go:build integration

package test

// 1.7 row: per-team model counters under concurrent creates.

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/pandops/flowai-model-registry/internal/store"
)

func TestV0048PerTeamCounterIntegration(t *testing.T) {
	ctx := context.Background()
	admin := freshDB(t)
	role := serviceRole(t, admin, lastDBName, "model_registry_schema", true)
	s := openStore(t, role, lastDBName, "model_registry_schema")
	rollback := errors.New("rollback")

	var mu sync.Mutex
	var committed, rolledBack []int64
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var got int64
			err := s.Tx(ctx, store.TxModelCreate, func(tx store.Tx) error {
				id, err := tx.NextTeamCounter(3, store.CounterModel)
				if err != nil {
					return err
				}
				got = id
				if i%5 == 0 {
					return rollback
				}
				return nil
			})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case errors.Is(err, rollback):
				rolledBack = append(rolledBack, got)
			case err != nil:
				t.Errorf("tx %d: %v", i, err)
			default:
				committed = append(committed, got)
			}
		}(i)
	}
	wg.Wait()
	if len(committed) != 16 {
		t.Fatalf("%d committed", len(committed))
	}
	sorted := slices.Clone(committed)
	slices.Sort(sorted)
	if len(slices.Compact(slices.Clone(sorted))) != 16 || sorted[0] < 1 {
		t.Fatalf("committed numbers %v", committed)
	}
	var next int64
	_ = s.Tx(ctx, store.TxModelCreate, func(tx store.Tx) (err error) { next, err = tx.NextTeamCounter(3, store.CounterModel); return })
	for _, v := range append(slices.Clone(committed), rolledBack...) {
		if next <= v {
			t.Fatalf("number %d reused or not increasing after %v / %v", next, committed, rolledBack)
		}
	}
	var first int64
	_ = s.Tx(ctx, store.TxModelCreate, func(tx store.Tx) (err error) { first, err = tx.NextTeamCounter(4, store.CounterModel); return })
	if first != 1 {
		t.Fatalf("first model of team 4 = %d, want 1", first)
	}
}
