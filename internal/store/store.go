// Package store defines model-registry's persistence boundary: the rows it
// keeps in its own schema and the transactional operations on them. The
// PostgreSQL implementation lives in store/postgres; unit tests use the
// in-process fake in internal/mocks.
package store

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrNotFound means the row does not exist.
	ErrNotFound = errors.New("store: not found")
	// ErrConflict means a unique key already exists.
	ErrConflict = errors.New("store: conflict")
)

// Model kinds and owners.
const (
	CreatedByTeam  = "team"
	CreatedByAdmin = "admin"

	AvailabilityActive      = "active"
	AvailabilityPaused      = "paused"
	AvailabilityArchived    = "archived"
	AvailabilityUnavailable = "unavailable"

	OperationArchive   = "archive"
	OperationUnarchive = "unarchive"

	// CounterModel is the per-team counter kind of team-created models.
	CounterModel = "model"
)

// Transaction names passed to Store.Tx; tests inject commit failures by name.
const (
	TxModelCreate       = "model-create"
	TxModelUpdate       = "model-update"
	TxArchiveBegin      = "archive-begin"
	TxArchiveComplete   = "archive-complete"
	TxUnarchiveBegin    = "unarchive-begin"
	TxUnarchiveComplete = "unarchive-complete"
	TxSnapshot          = "snapshot"
	TxTaskKeyOrphan     = "task-key-orphan"
	TxTaskKeyStore      = "task-key-store"
	TxConsume           = "consume"
)

// Model is one ownership row. Model configuration and credentials live only
// in LiteLLM.
type Model struct {
	Key            int64  // internal surrogate key, never in a contract
	CreatorTeamID  *int64 // nil for an administrator-created model
	ModelID        int64
	CreatedBy      string
	Name           string
	Alias          string
	LiteLLMModelID string
	Availability   string
	ArchivedAt     *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Operation is a durable pending archive or unarchive of one model.
type Operation struct {
	ModelKey   int64
	Kind       string
	TeamID     int64
	OperatorID int64
	RequestID  int64
	CreatedAt  time.Time
}

// Compensation is a LiteLLM model (and/or credential) to delete in the
// background because its local create did not commit.
type Compensation struct {
	ID             int64
	TeamID         int64
	ModelID        int64
	Alias          string
	LiteLLMModelID string // empty: find by alias and flowai_model_id
	CredentialName string
	CreatedAt      time.Time
}

// FlowLimit is the shared limit of a Flow as flow-registry sends it.
type FlowLimit struct {
	MaxBudgetUSD *string `json:"max_budget_usd,omitempty"`
	BudgetPeriod *string `json:"budget_period,omitempty"`
	RPM          *int64  `json:"rpm,omitempty"`
	TPM          *int64  `json:"tpm,omitempty"`
}

// PerTaskLimit is the per-claim limit pinned in an LLM snapshot.
type PerTaskLimit struct {
	MaxBudgetUSD *string `json:"max_budget_usd,omitempty"`
	RPM          *int64  `json:"rpm,omitempty"`
	TPM          *int64  `json:"tpm,omitempty"`
}

// FlowTeam is the LiteLLM team of one Flow and the limit last applied to it.
type FlowTeam struct {
	TeamID        int64
	FlowID        int64
	LiteLLMTeamID string
	FlowRevision  int64
	Limit         *FlowLimit
	BudgetResetAt *time.Time
	UpdatedAt     time.Time
}

// SpendObservation is the cached spend of one Flow team.
type SpendObservation struct {
	TeamID        int64
	FlowID        int64
	Succeeded     bool
	SpendUSD      *string
	BudgetResetAt *time.Time
	ObservedAt    time.Time
	// The latest successful read, kept when a later read fails.
	LastSuccessSpendUSD *string
	LastSuccessResetAt  *time.Time
	LastSuccessAt       *time.Time
}

// Executor is the projection of executor-registry's executor_registered and
// executor_removed events.
type Executor struct {
	ExecutorID int64
	TeamID     int64
	Removed    bool
}

// ClaimKey names one claim of one task.
type ClaimKey struct {
	TeamID   int64
	TaskID   int64
	Attempt  int64
	ClaimSeq int64
}

// Snapshot is the immutable LLM snapshot of one claim.
type Snapshot struct {
	ClaimKey
	ExecutorID   int64
	FlowID       int64
	ModelAlias   string
	ModelKey     int64
	PerTaskLimit *PerTaskLimit
	CreatedAt    time.Time
	EndedAt      *time.Time
}

// TaskKey is the stored, encrypted LiteLLM virtual key of one claim.
type TaskKey struct {
	ClaimKey
	Token             string // LiteLLM token hash, addresses the key
	Ciphertext        []byte
	ExpiresAt         time.Time
	ExtendedAt        time.Time
	RevokeCause       *string
	RevokeRequestedAt *time.Time
	RevokedAt         *time.Time
}

// Orphan is a key recorded before the LiteLLM create, deleted once the key
// is stored; a remaining row is revoked by reconciliation.
type Orphan struct {
	ID int64
	ClaimKey
	Token     string
	CreatedAt time.Time
}

// OutboxRow is one stream message waiting for its JetStream acknowledgement.
type OutboxRow struct {
	Seq       int64
	Subject   string
	Team      string
	RequestID *int64
	Payload   []byte
	CreatedAt time.Time
	SentAt    *time.Time
}

// Store runs transactions on model-registry's own schema.
type Store interface {
	// Tx runs fn in one transaction named op; any error rolls back.
	Tx(ctx context.Context, op string, fn func(Tx) error) error
	Ping(ctx context.Context) error
}

// Tx is the set of operations available inside one transaction. A lock
// argument takes a row lock held until the transaction ends.
type Tx interface {
	NextTeamCounter(teamID int64, kind string) (int64, error)
	NextAdminModelID() (int64, error)

	InsertModel(m *Model) error
	ModelByKey(key int64, lock bool) (*Model, error)
	ModelByAlias(alias string, lock bool) (*Model, error)
	UpdateModel(m *Model) error
	Models() ([]Model, error)
	AddOwner(modelKey, teamID int64) error
	Owners(modelKey int64) ([]int64, error)
	OwnedModels(teamID int64, archived bool, afterName string, limit int) ([]Model, error)
	OwnedModelByName(teamID int64, name string) (*Model, error)

	PutOperation(op Operation) error
	Operation(modelKey int64) (*Operation, error)
	DeleteOperation(modelKey int64) error
	Operations() ([]Operation, error)

	AddCompensation(c Compensation) error
	Compensations() ([]Compensation, error)
	DeleteCompensation(id int64) error

	FlowTeam(teamID, flowID int64, lock bool) (*FlowTeam, error)
	PutFlowTeam(ft FlowTeam) error
	FlowTeams() ([]FlowTeam, error)
	// DeleteFlowTeam removes a Flow's team row and its spend observation.
	DeleteFlowTeam(teamID, flowID int64) error
	SpendObservation(teamID, flowID int64) (*SpendObservation, error)
	PutSpendObservation(o SpendObservation) error

	Executor(id int64) (*Executor, error)
	PutExecutor(e Executor) error

	Snapshot(k ClaimKey, lock bool) (*Snapshot, error)
	InsertSnapshot(s Snapshot) error
	UpdateSnapshot(s Snapshot) error
	TaskSnapshots(teamID, taskID int64) ([]Snapshot, error)
	UnfinishedCount(modelKey int64) (int, error)

	TaskKey(k ClaimKey) (*TaskKey, error)
	InsertTaskKey(k TaskKey) error
	UpdateTaskKey(k TaskKey) error
	PendingRevocations() ([]TaskKey, error)

	InsertOrphan(o Orphan) (int64, error)
	DeleteOrphan(id int64) error
	Orphans() ([]Orphan, error)

	// MarkConsumed records (publisher, seq); false means it was already
	// consumed.
	MarkConsumed(publisher string, seq int64) (bool, error)

	// Enqueue allocates the next publisher_seq from the service sequence,
	// sets publisher and publisher_seq in payload and stores the row.
	Enqueue(subject, team string, requestID *int64, payload map[string]any) (int64, error)
	Unsent(limit int) ([]OutboxRow, error)
	MarkSent(seq int64, at time.Time) error
}
