package queryclient

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/views/queryclient/reducer"
	"github.com/milvus-io/milvus/internal/views/queryclient/renderer"
	"github.com/milvus-io/milvus/internal/views/queryclient/reranker"
	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

const defaultMaxRetries = 3

// ViewQueryClientConfig holds configuration for the ViewQueryClient.
type ViewQueryClientConfig struct {
	MaxRetries int // Maximum per-shard retries on retryable errors. 0 uses default (3).
}

// viewQueryClientImpl implements ViewQueryClient.
//
// It orchestrates collection-level concerns (shard resolution, reranker/renderer
// construction, field fetch planning, cross-shard reduce, requery, rerank, render)
// and delegates shard-level Phase 1 + Phase 2 execution to shardViewQueryClient.
//
// Execution stages for Search: Plan → Search → [RerankQuery] → [Rerank] → [Requery] → Render
// Execution stages for Query:  Plan → Search → [Requery] → Render
type viewQueryClientImpl struct {
	shardClient            *shardViewQueryClient
	legacyClient           LegacyClient
	shardResolver          resolver.ShardResolver
	fieldFetchPlanner      FieldFetchPlanner
	rerankerBuilder        reranker.Builder
	rendererBuilder        renderer.Builder
	searchReducerBuilder   reducer.SearchResultReducerBuilder
	retrieveReducerBuilder reducer.RetrieveResultReducerBuilder
	requeryRunner          RequeryRunner
}

// NewViewQueryClient creates a new ViewQueryClient with the given dependencies.
func NewViewQueryClient(
	cfg ViewQueryClientConfig,
	queryPlanClient QueryPlanClient,
	queryServiceClient ViewQueryServiceClient,
	shardResolver resolver.ShardResolver,
	replicaPicker ReplicaPicker,
	fieldFetchPlanner FieldFetchPlanner,
	rerankerBuilder reranker.Builder,
	rendererBuilder renderer.Builder,
	searchReducerBuilder reducer.SearchResultReducerBuilder,
	retrieveReducerBuilder reducer.RetrieveResultReducerBuilder,
	requeryRunner RequeryRunner,
) ViewQueryClient {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = defaultMaxRetries
	}
	return &viewQueryClientImpl{
		shardClient:            newShardViewQueryClient(cfg.MaxRetries, queryPlanClient, queryServiceClient, shardResolver, replicaPicker),
		legacyClient:           newLegacyClient(cfg, queryPlanClient, queryServiceClient, shardResolver, replicaPicker),
		shardResolver:          shardResolver,
		fieldFetchPlanner:      fieldFetchPlanner,
		rerankerBuilder:        rerankerBuilder,
		rendererBuilder:        rendererBuilder,
		searchReducerBuilder:   searchReducerBuilder,
		retrieveReducerBuilder: retrieveReducerBuilder,
		requeryRunner:          requeryRunner,
	}
}

func (c *viewQueryClientImpl) Legacy() LegacyClient {
	return c.legacyClient
}

// Search implements ViewQueryClient.Search.
func (c *viewQueryClientImpl) Search(ctx context.Context, req *SearchRequest) (*SearchResult, error) {
	// === Stage: Plan ===

	// Resolve vchannels first — cheapest operation, fast-fails on invalid collection.
	vchannels, err := c.shardResolver.ResolveVChannels(ctx, req.Req.CollectionID)
	if err != nil {
		return nil, err
	}

	// Build reranker (nil if no reranking needed).
	rnk, err := c.rerankerBuilder.Build(ctx, &reranker.BuildRequest{
		CollectionID:   req.Req.CollectionID,
		NumSubSearches: len(req.Req.SubReqs),
		RankParams:     req.RankParams,
		FunctionScore:  req.FunctionScore,
		OrderByFields:  req.OrderByFields,
	})
	if err != nil {
		return nil, err
	}

	// Build renderer (noop if no special rendering needed).
	rnd, err := c.rendererBuilder.Build(ctx, &renderer.BuildRequest{
		CollectionID:  req.Req.CollectionID,
		SearchRequest: req.Req,
	})
	if err != nil {
		return nil, err
	}

	// Plan field fetch strategy.
	var rerankFields []string
	if rnk != nil {
		rerankFields = rnk.RequiredFields()
	}
	fieldPlan, err := c.fieldFetchPlanner.Plan(ctx, &FieldFetchPlanParams{
		RerankFields:   rerankFields,
		RenderFields:   rnd.RequiredFields(),
		OutputFields:   req.OutputFieldNames,
		NumSubSearches: len(req.Req.SubReqs),
		TopK:           req.Req.Topk,
	})
	if err != nil {
		return nil, err
	}

	// Create per-request reducer just before search.
	searchReducer, err := c.searchReducerBuilder.Build(req.Req)
	if err != nil {
		return nil, err
	}

	// === Stage: Search (per-shard pipelined) ===
	// TODO: set fieldPlan.SearchFields into req.Req.OutputFieldsId
	// (requires field name → ID resolution from collection schema).
	_ = fieldPlan
	shardPlans, err := c.searchAllShards(ctx, vchannels, req.Req, searchReducer)
	if err != nil {
		return nil, err
	}

	// Finish cross-shard reduce.
	searchResults, err := searchReducer.Finish()
	if err != nil {
		return nil, err
	}

	// === Stage: RerankQuery (optional) ===
	if len(fieldPlan.RerankQueryFields) > 0 {
		if err := c.requeryRunner.RequerySearchResults(ctx, searchResults, fieldPlan.RerankQueryFields, shardPlans); err != nil {
			return nil, err
		}
	}

	// === Stage: Rerank (optional) ===
	if rnk != nil {
		searchResults, err = rnk.Rerank(ctx, []*internalpb.SearchResults{searchResults})
		if err != nil {
			return nil, err
		}
	}

	// === Stage: Requery (optional) ===
	if len(fieldPlan.RequeryFields) > 0 {
		if err := c.requeryRunner.RequerySearchResults(ctx, searchResults, fieldPlan.RequeryFields, shardPlans); err != nil {
			return nil, err
		}
	}

	// === Stage: Render ===
	searchResults, err = rnd.RenderSearch(ctx, searchResults)
	if err != nil {
		return nil, err
	}

	return &SearchResult{Results: searchResults}, nil
}

// Query implements ViewQueryClient.Query.
func (c *viewQueryClientImpl) Query(ctx context.Context, req *QueryRequest) (*QueryResult, error) {
	// === Stage: Plan ===

	// Resolve vchannels first — cheapest operation, fast-fails on invalid collection.
	vchannels, err := c.shardResolver.ResolveVChannels(ctx, req.Req.CollectionID)
	if err != nil {
		return nil, err
	}

	// Build renderer (no reranker for Query).
	rnd, err := c.rendererBuilder.Build(ctx, &renderer.BuildRequest{
		CollectionID: req.Req.CollectionID,
	})
	if err != nil {
		return nil, err
	}

	// Plan field fetch strategy (no rerank fields for Query).
	fieldPlan, err := c.fieldFetchPlanner.Plan(ctx, &FieldFetchPlanParams{
		RenderFields: rnd.RequiredFields(),
		OutputFields: req.OutputFieldNames,
	})
	if err != nil {
		return nil, err
	}

	// Create per-request reducer just before search.
	retrieveReducer, err := c.retrieveReducerBuilder.Build(req.Req)
	if err != nil {
		return nil, err
	}

	// === Stage: Search/Query (per-shard pipelined) ===
	// TODO: set fieldPlan.SearchFields into req.Req.OutputFieldsId
	_ = fieldPlan
	shardPlans, err := c.queryAllShards(ctx, vchannels, req.Req, retrieveReducer)
	if err != nil {
		return nil, err
	}

	// Finish cross-shard reduce.
	retrieveResults, err := retrieveReducer.Finish()
	if err != nil {
		return nil, err
	}

	// === Stage: Requery (optional) ===
	if len(fieldPlan.RequeryFields) > 0 {
		if err := c.requeryRunner.RequeryRetrieveResults(ctx, retrieveResults, fieldPlan.RequeryFields, shardPlans); err != nil {
			return nil, err
		}
	}

	// === Stage: Render ===
	retrieveResults, err = rnd.RenderRetrieve(ctx, retrieveResults)
	if err != nil {
		return nil, err
	}

	return &QueryResult{Results: retrieveResults}, nil
}

// searchAllShards dispatches Search to all shards concurrently via shardViewQueryClient.
func (c *viewQueryClientImpl) searchAllShards(
	ctx context.Context,
	vchannels []string,
	req *internalpb.SearchRequest,
	searchReducer reducer.SearchResultReducer,
) ([]ShardPlan, error) {
	shardPlans := make([]ShardPlan, len(vchannels))
	g, gCtx := errgroup.WithContext(ctx)
	for i := range vchannels {
		i := i
		g.Go(func() error {
			plan, err := c.shardClient.Search(gCtx, &ShardSearchRequest{
				VChannel: vchannels[i],
				Req:      req,
				Reducer:  searchReducer,
			})
			if err != nil {
				return err
			}
			shardPlans[i] = *plan
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return shardPlans, nil
}

// queryAllShards dispatches Query to all shards concurrently via shardViewQueryClient.
func (c *viewQueryClientImpl) queryAllShards(
	ctx context.Context,
	vchannels []string,
	req *internalpb.RetrieveRequest,
	retrieveReducer reducer.RetrieveResultReducer,
) ([]ShardPlan, error) {
	shardPlans := make([]ShardPlan, len(vchannels))
	g, gCtx := errgroup.WithContext(ctx)
	for i := range vchannels {
		i := i
		g.Go(func() error {
			plan, err := c.shardClient.Query(gCtx, &ShardQueryRequest{
				VChannel: vchannels[i],
				Req:      req,
				Reducer:  retrieveReducer,
			})
			if err != nil {
				return err
			}
			shardPlans[i] = *plan
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return shardPlans, nil
}
