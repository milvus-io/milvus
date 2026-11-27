package viewquery

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// TaskProvider converts a QueryView version into ready concrete segment tasks.
type TaskProvider interface {
	// Prewarm borrows this exact View for one cache-filling request.
	Prewarm(context.Context, *viewpb.PrewarmOnViewRequest) error

	// Statistics waits for visibility and counts the resources retained by this View.
	Statistics(ctx context.Context, req *viewpb.StatisticsOnViewRequest) (int64, error)

	AcquireSearchSegmentTasks(
		ctx context.Context,
		shardID qviews.ShardID,
		version qviews.QueryViewVersion,
		mvcc *viewpb.QueryPlanMVCC,
		req *internalpb.SearchRequest,
		selection *viewpb.SegmentSelection,
	) (SearchSegmentTasks, error)

	AcquireQuerySegmentTasks(
		ctx context.Context,
		shardID qviews.ShardID,
		version qviews.QueryViewVersion,
		mvcc *viewpb.QueryPlanMVCC,
		req *internalpb.RetrieveRequest,
		selection *viewpb.SegmentSelection,
	) (QuerySegmentTasks, error)
}
