package optimizer

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// GlobalOptimizer optimizes a request using global query view information.
// Runs on StreamingNode during Phase 1 of the two-phase query process.
// Has access to all segments across all nodes via the query view.
//
// Implementations include BM25 IDF computation and search parameter tuning.
type GlobalOptimizer interface {
	OptimizeSearch(ctx context.Context, req *internalpb.SearchRequest) error
	OptimizeRetrieve(ctx context.Context, req *internalpb.RetrieveRequest) error
}

// NewNoopGlobalOptimizer returns a GlobalOptimizer that performs no optimization.
func NewNoopGlobalOptimizer() GlobalOptimizer {
	return noopGlobalOptimizer{}
}

type noopGlobalOptimizer struct{}

func (noopGlobalOptimizer) OptimizeSearch(context.Context, *internalpb.SearchRequest) error {
	return nil
}

func (noopGlobalOptimizer) OptimizeRetrieve(context.Context, *internalpb.RetrieveRequest) error {
	return nil
}
