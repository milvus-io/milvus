package viewquery

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// TaskProvider converts a QueryView version into ready concrete segment tasks.
type TaskProvider interface {
	AcquireSearchSegmentTasks(
		ctx context.Context,
		shardID qviews.ShardID,
		version qviews.QueryViewVersion,
		mvcc *viewpb.QueryPlanMVCC,
		req *internalpb.SearchRequest,
	) (SearchSegmentTasks, error)

	AcquireQuerySegmentTasks(
		ctx context.Context,
		shardID qviews.ShardID,
		version qviews.QueryViewVersion,
		mvcc *viewpb.QueryPlanMVCC,
		req *internalpb.RetrieveRequest,
	) (QuerySegmentTasks, error)
}
