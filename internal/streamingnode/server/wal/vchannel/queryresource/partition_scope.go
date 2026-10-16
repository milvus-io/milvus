package queryresource

import (
	"slices"

	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ResolvePartitions validates the leased view before empty-worker pruning and
// expands an unfiltered request to that view's immutable loaded partition set.
func (r *QueryRuntime) ResolvePartitions(version uint64, requested []int64) ([]int64, error) {
	if version == 0 {
		return requested, nil
	}
	if r == nil {
		return nil, viewerror.NewViewNotFound("query runtime is unavailable")
	}
	r.mu.Lock()
	loaded, ok := r.partitionScopes[version]
	r.mu.Unlock()
	if !ok {
		return nil, viewerror.NewViewNotFound("query view partition scope is not prepared")
	}
	for _, id := range requested {
		if !slices.Contains(loaded, id) {
			return nil, merr.WrapErrPartitionNotLoaded(id)
		}
	}
	if len(requested) != 0 {
		return requested, nil
	}
	// A released collection/last partition has no serving view. Never interpret
	// an empty loaded scope as the legacy all-partitions sentinel.
	if len(loaded) == 0 {
		return nil, merr.WrapErrCollectionNotLoaded(0)
	}
	return slices.Clone(loaded), nil
}
