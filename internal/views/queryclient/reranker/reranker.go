package reranker

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// Reranker reranks search results.
// For HybridSearch: merges and reranks results from multiple sub-searches.
// For single search with rerank: applies decay, model-based reranking, etc.
// For order-by: sorts results by specified field values.
type Reranker interface {
	// RequiredFields returns the field names needed by this reranker.
	// These fields must be available before Rerank is called.
	// Used by FieldFetchPlanner to decide field fetch strategy.
	RequiredFields() []string

	// Rerank processes search results and returns the final reranked result.
	Rerank(ctx context.Context, results []*internalpb.SearchResults) (*internalpb.SearchResults, error)
}

// Builder creates a Reranker from user request parameters.
// It encapsulates all dependencies needed for reranker construction
// (schema cache, model service clients, etc.) internally.
type Builder interface {
	// Build creates a Reranker for the given request.
	// Returns (nil, nil) if the request does not require reranking.
	Build(ctx context.Context, req *BuildRequest) (Reranker, error)
}

// OrderByField describes a field to sort results by.
type OrderByField struct {
	FieldName string
	Desc      bool // true for descending order.
}

// BuildRequest contains the user-facing parameters needed to construct a reranker.
type BuildRequest struct {
	CollectionID int64
	// Number of sub-searches in the request.
	NumSubSearches int
	// Legacy rank params from HybridSearchRequest.RankParams.
	// Used when FunctionScore is not set.
	RankParams []*commonpb.KeyValuePair
	// FunctionScore from the search request schema.
	FunctionScore *schemapb.FunctionScore
	// Order-by fields for sorting results. If non-empty, an order-by reranker
	// is created instead of a score-based reranker.
	OrderByFields []OrderByField
}
