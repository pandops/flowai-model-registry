// Package mocks holds in-process fakes of model-registry's external
// dependencies for unit tests: PostgreSQL (MemStore), NATS (Broker) and the
// LiteLLM management and data plane (LiteLLM). Production wiring never uses
// them.
package mocks

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pandops/flowai-model-registry/internal/store"
)

// ErrInjected is returned by a transaction whose commit a test made fail.
var ErrInjected = errors.New("memstore: injected commit failure")

// ErrDown is returned while the fake database is down.
var ErrDown = errors.New("memstore: database down")

type memState struct {
	counters      map[string]int64
	adminSeq      int64
	modelSeq      int64
	models        map[int64]store.Model
	owners        map[int64][]int64
	operations    map[int64]store.Operation
	compSeq       int64
	compensations map[int64]store.Compensation
	flowTeams     map[[2]int64]store.FlowTeam
	spend         map[[2]int64]store.SpendObservation
	executors     map[int64]store.Executor
	snapshots     map[store.ClaimKey]store.Snapshot
	keys          map[store.ClaimKey]store.TaskKey
	orphanSeq     int64
	orphans       map[int64]store.Orphan
	consumed      map[string]bool
	outbox        map[int64]store.OutboxRow
}

func newMemState() *memState {
	return &memState{
		counters: map[string]int64{}, models: map[int64]store.Model{}, owners: map[int64][]int64{},
		operations: map[int64]store.Operation{}, compensations: map[int64]store.Compensation{},
		flowTeams: map[[2]int64]store.FlowTeam{}, spend: map[[2]int64]store.SpendObservation{},
		executors: map[int64]store.Executor{}, snapshots: map[store.ClaimKey]store.Snapshot{},
		keys: map[store.ClaimKey]store.TaskKey{}, orphans: map[int64]store.Orphan{},
		consumed: map[string]bool{}, outbox: map[int64]store.OutboxRow{},
	}
}

func (s *memState) clone() *memState {
	c := *s
	c.counters = maps.Clone(s.counters)
	c.models = maps.Clone(s.models)
	c.owners = map[int64][]int64{}
	for k, v := range s.owners {
		c.owners[k] = slices.Clone(v)
	}
	c.operations = maps.Clone(s.operations)
	c.compensations = maps.Clone(s.compensations)
	c.flowTeams = maps.Clone(s.flowTeams)
	c.spend = maps.Clone(s.spend)
	c.executors = maps.Clone(s.executors)
	c.snapshots = maps.Clone(s.snapshots)
	c.keys = maps.Clone(s.keys)
	c.orphans = maps.Clone(s.orphans)
	c.consumed = maps.Clone(s.consumed)
	c.outbox = maps.Clone(s.outbox)
	return &c
}

// MemStore is a transactional in-memory fake of the PostgreSQL store.
// Transactions are serialized, so every row lock is trivially held.
type MemStore struct {
	mu        sync.Mutex // serializes transactions
	stateMu   sync.Mutex
	state     *memState
	seq       int64 // publisher_seq sequence: never rolled back, like a PostgreSQL sequence
	down      bool
	failOnce  map[string]int
	committed []string
}

// NewMemStore returns an empty fake database.
func NewMemStore() *MemStore {
	return &MemStore{state: newMemState(), failOnce: map[string]int{}}
}

// FailCommit makes the next n commits of transactions named op fail after
// their function ran; the transaction rolls back.
func (m *MemStore) FailCommit(op string, n int) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	m.failOnce[op] += n
}

// SetDown makes every transaction and ping fail while down.
func (m *MemStore) SetDown(down bool) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	m.down = down
}

// Committed lists the names of committed transactions in order.
func (m *MemStore) Committed() []string {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return slices.Clone(m.committed)
}

// Ping reports the fake database state.
func (m *MemStore) Ping(context.Context) error {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if m.down {
		return ErrDown
	}
	return nil
}

// Tx runs fn on a copy of the state and installs the copy on success.
func (m *MemStore) Tx(ctx context.Context, op string, fn func(store.Tx) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stateMu.Lock()
	if m.down {
		m.stateMu.Unlock()
		return ErrDown
	}
	work := m.state.clone()
	m.stateMu.Unlock()
	tx := &memTx{s: work, store: m}
	if err := fn(tx); err != nil {
		return err
	}
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if m.down {
		return ErrDown
	}
	if m.failOnce[op] > 0 {
		m.failOnce[op]--
		return ErrInjected
	}
	m.state = work
	m.committed = append(m.committed, op)
	return nil
}

// Snapshot helpers for tests (read without a transaction).

// Models returns every model row.
func (m *MemStore) Models() []store.Model {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	out := slices.Collect(maps.Values(m.state.models))
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// OwnersOf returns the owner teams of a model alias.
func (m *MemStore) OwnersOf(alias string) []int64 {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	for k, model := range m.state.models {
		if model.Alias == alias {
			return slices.Clone(m.state.owners[k])
		}
	}
	return nil
}

// Snapshots returns every LLM snapshot.
func (m *MemStore) Snapshots() []store.Snapshot {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return slices.Collect(maps.Values(m.state.snapshots))
}

// TaskKeys returns every stored task key row.
func (m *MemStore) TaskKeys() []store.TaskKey {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return slices.Collect(maps.Values(m.state.keys))
}

// OrphanRows returns every orphan row.
func (m *MemStore) OrphanRows() []store.Orphan {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return slices.Collect(maps.Values(m.state.orphans))
}

// Outbox returns every outbox row in publisher_seq order.
func (m *MemStore) Outbox() []store.OutboxRow {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	out := slices.Collect(maps.Values(m.state.outbox))
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// Operations returns every pending model operation.
func (m *MemStore) Operations() []store.Operation {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return slices.Collect(maps.Values(m.state.operations))
}

// FlowTeamRows returns every Flow team row.
func (m *MemStore) FlowTeamRows() []store.FlowTeam {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return slices.Collect(maps.Values(m.state.flowTeams))
}

// SpendRows returns every spend observation.
func (m *MemStore) SpendRows() []store.SpendObservation {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return slices.Collect(maps.Values(m.state.spend))
}

type memTx struct {
	s     *memState
	store *MemStore
}

func (t *memTx) NextTeamCounter(teamID int64, kind string) (int64, error) {
	key := kind + "/" + jsonInt(teamID)
	t.s.counters[key]++
	return t.s.counters[key], nil
}

func (t *memTx) NextAdminModelID() (int64, error) {
	t.s.adminSeq++
	return t.s.adminSeq, nil
}

func (t *memTx) InsertModel(m *store.Model) error {
	for _, existing := range t.s.models {
		if existing.Alias == m.Alias {
			return store.ErrConflict
		}
		if sameCreator(existing.CreatorTeamID, m.CreatorTeamID) && existing.ModelID == m.ModelID {
			return store.ErrConflict
		}
	}
	t.s.modelSeq++
	m.Key = t.s.modelSeq
	t.s.models[m.Key] = *m
	return nil
}

func sameCreator(a, b *int64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func (t *memTx) ModelByKey(key int64, _ bool) (*store.Model, error) {
	m, ok := t.s.models[key]
	if !ok {
		return nil, store.ErrNotFound
	}
	return &m, nil
}

func (t *memTx) ModelByAlias(alias string, _ bool) (*store.Model, error) {
	for _, m := range t.s.models {
		if m.Alias == alias {
			return &m, nil
		}
	}
	return nil, store.ErrNotFound
}

func (t *memTx) UpdateModel(m *store.Model) error {
	existing, ok := t.s.models[m.Key]
	if !ok {
		return store.ErrNotFound
	}
	if !sameCreator(existing.CreatorTeamID, m.CreatorTeamID) || existing.ModelID != m.ModelID || existing.Alias != m.Alias {
		return errors.New("memstore: model identity is immutable")
	}
	t.s.models[m.Key] = *m
	return nil
}

func (t *memTx) Models() ([]store.Model, error) {
	out := slices.Collect(maps.Values(t.s.models))
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (t *memTx) AddOwner(modelKey, teamID int64) error {
	if _, ok := t.s.models[modelKey]; !ok {
		return store.ErrNotFound
	}
	if slices.Contains(t.s.owners[modelKey], teamID) {
		return nil
	}
	t.s.owners[modelKey] = append(t.s.owners[modelKey], teamID)
	return nil
}

func (t *memTx) Owners(modelKey int64) ([]int64, error) {
	return slices.Clone(t.s.owners[modelKey]), nil
}

func (t *memTx) owned(teamID int64) []store.Model {
	var out []store.Model
	for key, m := range t.s.models {
		if slices.Contains(t.s.owners[key], teamID) {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].Alias < out[j].Alias
	})
	return out
}

func (t *memTx) OwnedModels(teamID int64, archived bool, afterName string, limit int) ([]store.Model, error) {
	var out []store.Model
	for _, m := range t.owned(teamID) {
		if (m.ArchivedAt != nil) != archived {
			continue
		}
		if afterName != "" && m.Name <= afterName {
			continue
		}
		out = append(out, m)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (t *memTx) OwnedModelByName(teamID int64, name string) (*store.Model, error) {
	var found *store.Model
	for _, m := range t.owned(teamID) {
		if m.Name != name {
			continue
		}
		m := m
		// The team's own model wins over an administrator model of the same name.
		if found == nil || (m.CreatorTeamID != nil && *m.CreatorTeamID == teamID) {
			found = &m
		}
	}
	if found == nil {
		return nil, store.ErrNotFound
	}
	return found, nil
}

func (t *memTx) PutOperation(op store.Operation) error {
	if _, ok := t.s.operations[op.ModelKey]; ok {
		return store.ErrConflict
	}
	t.s.operations[op.ModelKey] = op
	return nil
}

func (t *memTx) Operation(modelKey int64) (*store.Operation, error) {
	op, ok := t.s.operations[modelKey]
	if !ok {
		return nil, nil
	}
	return &op, nil
}

func (t *memTx) DeleteOperation(modelKey int64) error {
	delete(t.s.operations, modelKey)
	return nil
}

func (t *memTx) Operations() ([]store.Operation, error) {
	out := slices.Collect(maps.Values(t.s.operations))
	sort.Slice(out, func(i, j int) bool { return out[i].ModelKey < out[j].ModelKey })
	return out, nil
}

func (t *memTx) AddCompensation(c store.Compensation) error {
	t.s.compSeq++
	c.ID = t.s.compSeq
	t.s.compensations[c.ID] = c
	return nil
}

func (t *memTx) Compensations() ([]store.Compensation, error) {
	out := slices.Collect(maps.Values(t.s.compensations))
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (t *memTx) DeleteCompensation(id int64) error {
	delete(t.s.compensations, id)
	return nil
}

func (t *memTx) FlowTeam(teamID, flowID int64, _ bool) (*store.FlowTeam, error) {
	ft, ok := t.s.flowTeams[[2]int64{teamID, flowID}]
	if !ok {
		return nil, nil
	}
	return &ft, nil
}

func (t *memTx) PutFlowTeam(ft store.FlowTeam) error {
	t.s.flowTeams[[2]int64{ft.TeamID, ft.FlowID}] = ft
	return nil
}

func (t *memTx) FlowTeams() ([]store.FlowTeam, error) {
	out := slices.Collect(maps.Values(t.s.flowTeams))
	sort.Slice(out, func(i, j int) bool {
		if out[i].TeamID != out[j].TeamID {
			return out[i].TeamID < out[j].TeamID
		}
		return out[i].FlowID < out[j].FlowID
	})
	return out, nil
}

func (t *memTx) DeleteFlowTeam(teamID, flowID int64) error {
	delete(t.s.flowTeams, [2]int64{teamID, flowID})
	delete(t.s.spend, [2]int64{teamID, flowID})
	return nil
}

func (t *memTx) SpendObservation(teamID, flowID int64) (*store.SpendObservation, error) {
	o, ok := t.s.spend[[2]int64{teamID, flowID}]
	if !ok {
		return nil, nil
	}
	return &o, nil
}

func (t *memTx) PutSpendObservation(o store.SpendObservation) error {
	t.s.spend[[2]int64{o.TeamID, o.FlowID}] = o
	return nil
}

func (t *memTx) Executor(id int64) (*store.Executor, error) {
	e, ok := t.s.executors[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return &e, nil
}

func (t *memTx) PutExecutor(e store.Executor) error {
	t.s.executors[e.ExecutorID] = e
	return nil
}

func (t *memTx) Snapshot(k store.ClaimKey, _ bool) (*store.Snapshot, error) {
	s, ok := t.s.snapshots[k]
	if !ok {
		return nil, store.ErrNotFound
	}
	return &s, nil
}

func (t *memTx) InsertSnapshot(s store.Snapshot) error {
	if _, ok := t.s.snapshots[s.ClaimKey]; ok {
		return store.ErrConflict
	}
	t.s.snapshots[s.ClaimKey] = s
	return nil
}

func (t *memTx) UpdateSnapshot(s store.Snapshot) error {
	if _, ok := t.s.snapshots[s.ClaimKey]; !ok {
		return store.ErrNotFound
	}
	t.s.snapshots[s.ClaimKey] = s
	return nil
}

func (t *memTx) TaskSnapshots(teamID, taskID int64) ([]store.Snapshot, error) {
	var out []store.Snapshot
	for k, s := range t.s.snapshots {
		if k.TeamID == teamID && k.TaskID == taskID {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Attempt != out[j].Attempt {
			return out[i].Attempt < out[j].Attempt
		}
		return out[i].ClaimSeq < out[j].ClaimSeq
	})
	return out, nil
}

func (t *memTx) UnfinishedCount(modelKey int64) (int, error) {
	n := 0
	for k, s := range t.s.snapshots {
		if s.ModelKey != modelKey || s.EndedAt != nil {
			continue
		}
		if key, ok := t.s.keys[k]; ok && key.RevokeRequestedAt != nil {
			continue
		}
		n++
	}
	return n, nil
}

func (t *memTx) TaskKey(k store.ClaimKey) (*store.TaskKey, error) {
	key, ok := t.s.keys[k]
	if !ok {
		return nil, store.ErrNotFound
	}
	return &key, nil
}

func (t *memTx) InsertTaskKey(k store.TaskKey) error {
	if _, ok := t.s.snapshots[k.ClaimKey]; !ok {
		return errors.New("memstore: task key without snapshot")
	}
	if _, ok := t.s.keys[k.ClaimKey]; ok {
		return store.ErrConflict
	}
	t.s.keys[k.ClaimKey] = k
	return nil
}

func (t *memTx) UpdateTaskKey(k store.TaskKey) error {
	if _, ok := t.s.keys[k.ClaimKey]; !ok {
		return store.ErrNotFound
	}
	t.s.keys[k.ClaimKey] = k
	return nil
}

func (t *memTx) PendingRevocations() ([]store.TaskKey, error) {
	var out []store.TaskKey
	for _, k := range t.s.keys {
		if k.RevokeRequestedAt != nil && k.RevokedAt == nil {
			out = append(out, k)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RevokeRequestedAt.Before(*out[j].RevokeRequestedAt) })
	return out, nil
}

func (t *memTx) InsertOrphan(o store.Orphan) (int64, error) {
	t.s.orphanSeq++
	o.ID = t.s.orphanSeq
	t.s.orphans[o.ID] = o
	return o.ID, nil
}

func (t *memTx) DeleteOrphan(id int64) error {
	delete(t.s.orphans, id)
	return nil
}

func (t *memTx) Orphans() ([]store.Orphan, error) {
	out := slices.Collect(maps.Values(t.s.orphans))
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (t *memTx) MarkConsumed(publisher string, seq int64) (bool, error) {
	key := publisher + "\x00" + jsonInt(seq)
	if t.s.consumed[key] {
		return false, nil
	}
	t.s.consumed[key] = true
	return true, nil
}

func (t *memTx) Enqueue(subject, team string, requestID *int64, payload map[string]any) (int64, error) {
	t.store.stateMu.Lock()
	t.store.seq++
	seq := t.store.seq
	t.store.stateMu.Unlock()
	payload = maps.Clone(payload)
	payload["publisher"] = "model-registry"
	payload["publisher_seq"] = seq
	raw, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	t.s.outbox[seq] = store.OutboxRow{Seq: seq, Subject: subject, Team: team, RequestID: requestID, Payload: raw, CreatedAt: time.Now().UTC()}
	return seq, nil
}

func (t *memTx) Unsent(limit int) ([]store.OutboxRow, error) {
	var out []store.OutboxRow
	for _, row := range t.s.outbox {
		if row.SentAt == nil {
			out = append(out, row)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (t *memTx) MarkSent(seq int64, at time.Time) error {
	row, ok := t.s.outbox[seq]
	if !ok {
		return store.ErrNotFound
	}
	row.SentAt = &at
	t.s.outbox[seq] = row
	return nil
}

func jsonInt(v int64) string {
	raw, _ := json.Marshal(v)
	return strings.TrimSpace(string(raw))
}
