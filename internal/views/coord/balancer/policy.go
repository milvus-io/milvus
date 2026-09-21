package balancer

import (
	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

// BalancePolicy consumes a cache Reader and the set of dirty shards
// flagged by the work queue, and returns an execution plan.
//
// The Policy retains immutable read objects to coordinate across shards
// (e.g., avoid two shards contending for the same target node via a shared
// predicted-load tracker maintained internally during Plan).
//
// Implementations may retain target layouts across calls, but must not mutate
// cache facts or apply views. The default policy serializes its planning calls.
type BalancePolicy interface {
	Plan(reader balancercache.Reader, dirty []qviews.ShardID) *BalancePlan
}

// BalancePlan is the complete set of actions to execute for one reconcile
// batch. The Balancer applies Prepares (via AddPreparing) and Releases (via
// RequestRelease) in an unspecified order; both are idempotent operations on
// the per-shard ShardViewManager.
//
// A shard listed in neither Prepares nor Releases is implicitly a no-op for
// this batch.
type BalancePlan struct {
	// Discovery is published before releasing retired/suspended views.
	Discovery []loadmgr.CollectionDiscoveryUpdate
	// Retries could not be allocated because desired inputs or eligible nodes were unavailable.
	Retries []qviews.ShardID
	// Prepares lists shards that should receive a new Preparing view.
	// The value is the builder the Balancer passes to AddPreparing.
	Prepares map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder

	// Releases lists shards whose existing views should be released
	// (desired state absent but current views still exist).
	Releases []qviews.ShardID
}
