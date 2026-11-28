package optimizer

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// LocalSegmentScope contains the remaining sealed segments of the leased view,
// after applying partition and plan selections. SN has no sealed partitions.
type LocalSegmentScope struct {
	Key        qviews.QueryViewKey
	Partitions []*viewpb.QueryViewOfPartition
}

// LocalOptimization holds the execution decisions for one node-local request.
// Its zero value uses the task provider's defaults.
type LocalOptimization struct {
	// SegmentSelection further narrows the scoped sealed segments. Nil keeps all of them.
	SegmentSelection *viewpb.SegmentSelection
}

// LocalOptimizer plans node-local execution during Phase 2.
type LocalOptimizer interface {
	OptimizeSearch(ctx context.Context, req *internalpb.SearchRequest, scope LocalSegmentScope) (LocalOptimization, error)
	OptimizeRetrieve(ctx context.Context, req *internalpb.RetrieveRequest, scope LocalSegmentScope) (LocalOptimization, error)
}

// NewNoopLocalOptimizer returns a LocalOptimizer that performs no optimization.
func NewNoopLocalOptimizer() LocalOptimizer {
	return noopLocalOptimizer{}
}

type noopLocalOptimizer struct{}

func (noopLocalOptimizer) OptimizeSearch(context.Context, *internalpb.SearchRequest, LocalSegmentScope) (LocalOptimization, error) {
	return LocalOptimization{}, nil
}

func (noopLocalOptimizer) OptimizeRetrieve(context.Context, *internalpb.RetrieveRequest, LocalSegmentScope) (LocalOptimization, error) {
	return LocalOptimization{}, nil
}
