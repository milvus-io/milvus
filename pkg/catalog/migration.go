package catalog

import "context"

type MigrationPhase string

const (
	MigrationDisabled             MigrationPhase = "disabled"
	MigrationBackfilling          MigrationPhase = "backfilling"
	MigrationDualWriteSourceFirst MigrationPhase = "dual-write-source-first"
	MigrationDualWriteTargetFirst MigrationPhase = "dual-write-target-first"
	MigrationTargetOnly           MigrationPhase = "target-only"
	MigrationRollback             MigrationPhase = "rollback"
)

type MigrationState struct {
	Phase     MigrationPhase
	Source    Implementation
	Target    Implementation
	ClusterID string
	Version   Version
}

type BackfillRequest struct {
	ClusterID string
	Domains   []string
}

type BackfillResult struct {
	Copied int64
}

type CompareRequest struct {
	ClusterID string
	Domains   []string
}

type CompareResult struct {
	Matched     bool
	Differences []string
}

type CutoverRequest struct {
	ClusterID string
	Target    Implementation
}

type RollbackRequest struct {
	ClusterID string
	Target    Implementation
}

type MigrationCatalog interface {
	State(ctx context.Context) (*MigrationState, error)
	SetState(ctx context.Context, state MigrationState) error
	Backfill(ctx context.Context, req BackfillRequest) (*BackfillResult, error)
	Compare(ctx context.Context, req CompareRequest) (*CompareResult, error)
	Cutover(ctx context.Context, req CutoverRequest) error
	Rollback(ctx context.Context, req RollbackRequest) error
}
