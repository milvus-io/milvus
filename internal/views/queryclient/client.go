package queryclient

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/views/queryclient/reranker"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// ViewQueryClient executes queries using the two-phase query process.
//
// Execution stages: Plan → Search → [RerankQuery] → [Rerank] → [Requery] → Render
//
// It provides separate entry points for Search and Query, both orchestrating
// Plan + Search across all shards with streaming reduce and shard-level retry.
// Reranking is handled internally via the reranker.Builder injected at construction time.
// It applies to both single search (e.g., decay, model-based rerank) and HybridSearch
// (e.g., RRF/weighted across multiple sub-searches).
type ViewQueryClient interface {
	// Legacy returns the raw legacy execution domain used by Proxy's existing pipelines.
	Legacy() LegacyClient

	// Search executes a vector search with optional reranking.
	// Single Search: Req.IsAdvanced=false, may still have reranking (e.g., decay, model-based).
	// HybridSearch: Req.IsAdvanced=true with SubReqs, typically requires reranking (RRF/weighted).
	Search(ctx context.Context, req *SearchRequest) (*SearchResult, error)

	// Query executes a single expression-based retrieve.
	Query(ctx context.Context, req *QueryRequest) (*QueryResult, error)
}

// SearchRequest wraps an internal search request with orchestration metadata.
// For HybridSearch, Req.IsAdvanced=true and Req.SubReqs contains the sub-searches.
// For single search, Req.IsAdvanced=false.
//
// Metadata fields (OutputFieldNames, RankParams, FunctionScore, OrderByFields)
// are populated by the Proxy from the API-level request and collection schema.
type SearchRequest struct {
	Req *internalpb.SearchRequest

	// OutputFieldNames are the user-requested output field names,
	// resolved from Req.OutputFieldsId by the Proxy using the collection schema.
	OutputFieldNames []string

	// RankParams from HybridSearch API (legacy rank params).
	// Used by the reranker builder to construct the appropriate reranker.
	RankParams []*commonpb.KeyValuePair

	// FunctionScore from the search request schema.
	// Used for score-function-based reranking.
	FunctionScore *schemapb.FunctionScore

	// OrderByFields parsed from the search request.
	// When non-empty, results are sorted by these fields instead of by score.
	OrderByFields []reranker.OrderByField
}

// SearchResult contains the final search result after reduce and optional reranking.
type SearchResult struct {
	Results *internalpb.SearchResults
}

// QueryRequest wraps an internal retrieve request with orchestration metadata.
type QueryRequest struct {
	Req *internalpb.RetrieveRequest

	// OutputFieldNames are the user-requested output field names,
	// resolved from Req.OutputFieldsId by the Proxy using the collection schema.
	OutputFieldNames []string
}

// QueryResult contains the final retrieve result after reduce.
type QueryResult struct {
	Results *internalpb.RetrieveResults
}
