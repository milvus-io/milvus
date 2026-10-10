package viewquery

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// Scheduler executes ready concrete segment tasks and performs node-local reduce.
type Scheduler interface {
	Search(ctx context.Context, tasks SearchSegmentTasks) (*internalpb.SearchResults, error)
	Query(ctx context.Context, tasks QuerySegmentTasks) (*internalpb.RetrieveResults, error)
}
