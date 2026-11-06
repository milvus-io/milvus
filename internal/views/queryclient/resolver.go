package queryclient

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/searchutil"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// QueryPlanClient provides Phase 1 operations against StreamingNode.
// Implementation handles StreamingNode resolution and gRPC connection management internally.
// ShardID is used as the routing key to locate the owning StreamingNode.
type QueryPlanClient interface {
	SubscribeReplicas(vchannel string, handler wal.ReplicaWatchHandler) (wal.ReplicaWatchSubscription, error)

	// GetQueryPlan generates a shard-level query plan from the StreamingNode owning the given shard.
	GetQueryPlan(ctx context.Context, shardID qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error)

	// GetMVCCTimestamp returns the MVCC frontiers from the primary replica's WAL.
	// Used for cross-replica strong consistency.
	GetMVCCTimestamp(ctx context.Context, shardID qviews.ShardID, req *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error)
}

// ViewQueryServiceClient provides Phase 2 operations against work nodes (StreamingNode or QueryNode).
// Implementation handles work node resolution and gRPC connection management internally.
// WorkNode is used as the routing key to locate the target node.
// QN implementations must return connection failures promptly, without hidden
// same-node retries. Missing sessions carry viewerror.NodeSessionUnavailable
// with the resolver change channel captured before dispatch. This lets shard
// orchestration retry other replicas and reconsider a racing session update.
type ViewQueryServiceClient interface {
	StatisticsOnView(ctx context.Context, node qviews.WorkNode, req *viewpb.StatisticsOnViewRequest) (*viewpb.StatisticsOnViewResponse, error)

	// SearchOnView executes a vector search on the given work node.
	SearchOnView(ctx context.Context, node qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error)
	SearchOnViewStream(ctx context.Context, node qviews.WorkNode, req *viewpb.SearchOnViewRequest) (searchutil.ReduceStream, error)

	// QueryOnView executes a query (retrieve by expression) on the given work node.
	QueryOnView(ctx context.Context, node qviews.WorkNode, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error)
	QueryOnViewStream(ctx context.Context, node qviews.WorkNode, req *viewpb.QueryOnViewRequest) (queryutil.ReduceStream, error)
}
