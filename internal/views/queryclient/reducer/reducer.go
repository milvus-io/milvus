package reducer

import (
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// SearchResultReducer incrementally reduces search results as they arrive from work nodes.
// Thread-safe: Add may be called concurrently from multiple goroutines.
//
// Internally maintains a per-shard sub-reducer with eager reduce.
// ResetShard discards a single shard's accumulated results in O(1),
// enabling shard-level retry without losing other shards' data.
//
// Multiple implementations handle different semantics:
// - Standard top-k reduce (sorted by score)
// - GroupBy per-group top-k reduce (grouped by field value)
type SearchResultReducer interface {
	// Add feeds a single work node's search result into the reducer.
	Add(shardID qviews.ShardID, result *viewpb.SearchOnViewResponse) error

	// ResetShard discards all accumulated results for the given shard.
	ResetShard(shardID qviews.ShardID)

	// Finish merges results across all shards and returns the final reduced result.
	Finish() (*internalpb.SearchResults, error)
}

// RetrieveResultReducer incrementally reduces retrieve results as they arrive from work nodes.
// Thread-safe: Add may be called concurrently from multiple goroutines.
//
// Follows the same shard-aware pattern as SearchResultReducer.
type RetrieveResultReducer interface {
	// Add feeds a single work node's retrieve result into the reducer.
	Add(shardID qviews.ShardID, result *viewpb.QueryOnViewResponse) error

	// ResetShard discards all accumulated results for the given shard.
	ResetShard(shardID qviews.ShardID)

	// Finish merges results across all shards and returns the final reduced result.
	Finish() (*internalpb.RetrieveResults, error)
}
