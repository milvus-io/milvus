package optimizer

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// LocalOptimizer optimizes query execution using local segment information.
// Runs on each node (SN/QN) during Phase 2 of the two-phase query process.
// Has access to local segment information only.
//
// Implementations include segment pruning based on partition statistics.
type LocalOptimizer interface {
	OptimizeSearch(ctx context.Context, req *internalpb.SearchRequest) error
	OptimizeRetrieve(ctx context.Context, req *internalpb.RetrieveRequest) error
}

// NewNoopLocalOptimizer returns a LocalOptimizer that performs no optimization.
func NewNoopLocalOptimizer() LocalOptimizer {
	return noopLocalOptimizer{}
}

type noopLocalOptimizer struct{}

func (noopLocalOptimizer) OptimizeSearch(context.Context, *internalpb.SearchRequest) error {
	return nil
}

func (noopLocalOptimizer) OptimizeRetrieve(context.Context, *internalpb.RetrieveRequest) error {
	return nil
}
