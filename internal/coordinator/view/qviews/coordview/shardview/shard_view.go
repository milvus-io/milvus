package shardview

import (
	"context"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
)

var _ ShardView = (*shardViewImpl)(nil)

// ShardView is the interface of one ShardView.
// All API of the ShardView is asynchronous, the caller should watch the event to get the finish status.
// All API of the ShardView is not current safe, so the caller should promise that the API is called in one thread.
type ShardView interface {
	// ApplyNewQueryViewView apply a new query view to the shard view.
	ApplyNewQueryView(ctx context.Context, b *QueryViewAtCoordBuilder) (*qviews.QueryViewVersion, error)

	// RequestRelease request to release the shard view.
	RequestRelease(ctx context.Context) error

	// ApplyViewFromWorkNode apply a query view from the work node.
	ApplyViewFromWorkNode(ctx context.Context, w qviews.QueryViewAtWorkNode) error
}
