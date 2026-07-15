package queryclient

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// ShardPlan holds the Phase 1 result for a shard, retained for use
// in Requery stages that dispatch RequeryOnView using the same view version and MVCC.
type ShardPlan struct {
	ShardID   qviews.ShardID
	Version   *viewpb.QueryViewVersion
	Mvcc      *viewpb.QueryPlanMVCC
	WorkNodes []qviews.WorkNode
}

// RequeryRunner handles the Requery stage of the query pipeline.
// It extracts PKs from results, dispatches RequeryOnView to the original
// query plan's work nodes, and merges the fetched fields back into results.
//
// The implementation holds a ViewQueryServiceClient internally for dispatch.
// PK extraction and result merging are Milvus-internal format operations
// encapsulated within the implementation.
type RequeryRunner interface {
	// RequerySearchResults fetches additional fields for search results.
	// It extracts PKs from results, dispatches RequeryOnView to all work nodes
	// in each shard plan, and merges the fetched fields into results in-place.
	RequerySearchResults(ctx context.Context, results *internalpb.SearchResults, fields []string, plans []ShardPlan) error

	// RequeryRetrieveResults fetches additional fields for retrieve results.
	// Same dispatch and merge pattern as RequerySearchResults.
	RequeryRetrieveResults(ctx context.Context, results *internalpb.RetrieveResults, fields []string, plans []ShardPlan) error
}
