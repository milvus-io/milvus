package coordview

import (
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/coordview/shardview"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

type QueryViewAtCoordBuilder = shardview.QueryViewAtCoordBuilder

// ApplyResult is the result of the apply operation.
type ApplyResult struct {
	Version *qviews.QueryViewVersion
	Err     error
}

type ReleaseShardsRequest struct {
	shards []qviews.ShardID
}

// ReleaseShardsResult is the result of the release shard operation.
type ReleaseShardsResult struct{}

// QueryViewManager is the underlying state machine for the query view sync between node and coordinator.
type QueryViewManager interface {
	// Apply applies the new incoming query view to the event loop.
	// The result of the apply operation will be returned by the future after it has been applied into the state machine.
	// So the qview may be lost if the coordinator is crashed before the qview is persisted.
	Apply(newIncomingQV *QueryViewAtCoordBuilder) *syncutil.Future[ApplyResult]

	// ReleaseShard releases the shard from the query view manager.
	ReleaseShard(req ReleaseShardsRequest) *syncutil.Future[ReleaseShardsResult]

	// Close stops the underlying event loop.
	Close()
}
