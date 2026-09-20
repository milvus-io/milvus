package queryclient

import (
	"context"
	"errors"
	"io"
	"sync"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/searchutil"
	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Client exposes QueryView query client domains.
type Client interface {
	Close()
	Legacy() LegacyClient
}

// LegacyClient returns batch results or the final ReduceStream.
// Errors are business merr errors; ViewError and StreamingError remain internal.
type LegacyClient interface {
	// Statistics uses the outer BarrierMVCC; zero selects ordinary Strong sampling.
	Statistics(ctx context.Context, collectionID int64, req *viewpb.StatisticsRequest, barrierMVCC uint64) (int64, error)

	Search(ctx context.Context, req *LegacySearchRequest) (*LegacySearchResult, error)
	Query(ctx context.Context, req *LegacyQueryRequest) (*LegacyQueryResult, error)
}

type LegacySearchRequest struct {
	Req *internalpb.SearchRequest
	// MVCC, when supplied, overrides consistency-level sampling across retries.
	MVCC *viewpb.QueryPlanMVCC
	// BarrierMVCC fixes a scalar snapshot and actively establishes WAL coverage.
	// Mutually exclusive with MVCC. Zero preserves ordinary consistency sampling.
	BarrierMVCC uint64
}

type LegacySearchResult struct {
	Results []*internalpb.SearchResults
	Stream  searchutil.ReduceStream
	Plans   []ShardPlan
}

type LegacyQueryRequest struct {
	Req *internalpb.RetrieveRequest
	// Schema is the immutable read schema used to build Req.
	Schema *schemapb.CollectionSchema
	// MVCC fixes the read boundary across replanning; nil samples by consistency.
	MVCC *viewpb.QueryPlanMVCC
	// BarrierMVCC fixes a scalar snapshot and actively establishes WAL coverage.
	// Mutually exclusive with MVCC. Zero preserves ordinary consistency sampling.
	BarrierMVCC uint64
	// Plans binds an internal requery to the original Search attempt. Non-nil
	// (including empty) never resolves a new schema, view or MVCC.
	Plans []ShardPlan
}

type LegacyQueryResult struct {
	Results []*internalpb.RetrieveResults
	Stream  queryutil.ReduceStream
	Plans   []ShardPlan
}

type legacyOnlyClient struct {
	legacy *legacyClient
}

func (c *legacyOnlyClient) Close() { c.legacy.shardClient.replicas.Close() }

func (c *legacyOnlyClient) Legacy() LegacyClient {
	return c.legacy
}

type legacyClient struct {
	shardClient            *shardViewQueryClient
	shardResolver          resolver.ShardResolver
	enableSearchStreaming  bool
	searchStreamChunkBytes int
	enableQueryStreaming   bool
	queryStreamChunkBytes  int
}

type prefetchedReduceStream struct {
	stream     searchutil.ReduceStream
	firstChunk *internalpb.SearchResults
}

type prefetchedQueryReduceStream struct {
	stream     queryutil.ReduceStream
	firstChunk *internalpb.RetrieveResults
}

func (s *prefetchedQueryReduceStream) Recv() (*internalpb.RetrieveResults, error) {
	if s.firstChunk != nil {
		chunk := s.firstChunk
		s.firstChunk = nil
		return chunk, nil
	}
	return s.stream.Recv()
}

func (s *prefetchedQueryReduceStream) Close() error {
	s.firstChunk = nil
	return s.stream.Close()
}

func (s *prefetchedQueryReduceStream) Interrupt() (*internalpb.RetrieveResults, error) {
	s.firstChunk = nil
	return s.stream.Interrupt()
}

func (s *prefetchedReduceStream) Recv() (*internalpb.SearchResults, error) {
	if s.firstChunk != nil {
		chunk := s.firstChunk
		s.firstChunk = nil
		return chunk, nil
	}
	return s.stream.Recv()
}

func (s *prefetchedReduceStream) Close() error {
	s.firstChunk = nil
	return s.stream.Close()
}

func (s *prefetchedReduceStream) Interrupt() (*internalpb.SearchResults, error) {
	s.firstChunk = nil
	return s.stream.Interrupt()
}

func NewLegacyViewQueryClient(
	cfg ViewQueryClientConfig,
	queryPlanClient QueryPlanClient,
	queryServiceClient ViewQueryServiceClient,
	shardResolver resolver.ShardResolver,
) Client {
	return &legacyOnlyClient{
		legacy: newLegacyClient(cfg, queryPlanClient, queryServiceClient, shardResolver),
	}
}

func newLegacyClient(
	cfg ViewQueryClientConfig,
	queryPlanClient QueryPlanClient,
	queryServiceClient ViewQueryServiceClient,
	shardResolver resolver.ShardResolver,
) *legacyClient {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = defaultMaxRetries
	}
	if cfg.SearchStreamChunkBytes <= 0 {
		cfg.SearchStreamChunkBytes = defaultStreamChunkBytes
	}
	if cfg.QueryStreamChunkBytes <= 0 {
		cfg.QueryStreamChunkBytes = defaultStreamChunkBytes
	}
	shardClient := newShardViewQueryClient(cfg.MaxRetries, queryPlanClient, queryServiceClient)
	shardClient.queryHook = cfg.QueryHook
	shardClient.loadChecks = newCollectionLoadChecks(cfg.LoadInfoClient)
	return &legacyClient{
		shardClient:            shardClient,
		shardResolver:          shardResolver,
		enableSearchStreaming:  cfg.EnableSearchStreaming,
		searchStreamChunkBytes: cfg.SearchStreamChunkBytes,
		enableQueryStreaming:   cfg.EnableQueryStreaming,
		queryStreamChunkBytes:  cfg.QueryStreamChunkBytes,
	}
}

func supportsSearchStream(req *internalpb.SearchRequest) bool {
	return req != nil &&
		req.GetIsIterator() &&
		!req.GetIsAdvanced() &&
		len(req.GetSubReqs()) == 0 &&
		req.GetGroupByFieldId() <= 0 &&
		len(req.GetGroupByFieldIds()) == 0
}

func (c *legacyClient) Search(ctx context.Context, req *LegacySearchRequest) (result *LegacySearchResult, err error) {
	defer func() { err = toMilvusError(err) }()
	if c.enableSearchStreaming && supportsSearchStream(req.Req) {
		return c.searchStream(ctx, req)
	}

	vchannels, err := c.shardResolver.ResolveVChannels(ctx, req.Req.CollectionID)
	if err != nil {
		return nil, err
	}

	collector := newLegacySearchCollector()
	shardPlans := make([]ShardPlan, len(vchannels))
	g, gCtx := errgroup.WithContext(ctx)
	for i := range vchannels {
		i := i
		g.Go(func() error {
			plan, err := c.shardClient.Search(gCtx, &ShardSearchRequest{
				TotalChannelNum: int32(len(vchannels)),
				VChannel:        vchannels[i],
				Req:             req.Req,
				MVCC:            req.MVCC,
				BarrierMVCC:     req.BarrierMVCC,
				Reducer:         collector,
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
	results, err := collector.Reduce(ctx, req.Req, shardPlans)
	if err != nil {
		return nil, err
	}
	return &LegacySearchResult{
		Results: results,
		Plans:   shardPlans,
	}, nil
}

func (c *legacyClient) searchStream(ctx context.Context, req *LegacySearchRequest) (*LegacySearchResult, error) {
	var lastErr error
	for attempt := 0; attempt < c.shardClient.maxRetries; attempt++ {
		vchannels, err := c.shardResolver.ResolveVChannels(ctx, req.Req.GetCollectionID())
		if err != nil {
			return nil, err
		}

		vchannelStreams := make([]searchutil.ReduceStream, len(vchannels))
		shardPlans := make([]ShardPlan, len(vchannels))
		var g errgroup.Group
		for i := range vchannels {
			i := i
			g.Go(func() error {
				stream, plan, err := c.shardClient.SearchStream(ctx, vchannels[i], req.Req, c.searchStreamChunkBytes)
				if err != nil {
					return err
				}
				vchannelStreams[i] = stream
				shardPlans[i] = *plan
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			for _, stream := range vchannelStreams {
				if stream != nil {
					err = errors.Join(err, stream.Close())
				}
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr = err
			continue
		}

		finalStream, err := searchutil.NewReduceStream(req.Req, vchannelStreams, c.searchStreamChunkBytes)
		if err != nil {
			for _, stream := range vchannelStreams {
				err = errors.Join(err, stream.Close())
			}
			return nil, err
		}

		firstChunk, recvErr := finalStream.Recv()
		if recvErr != nil && !errors.Is(recvErr, io.EOF) {
			err = errors.Join(recvErr, finalStream.Close())
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr = err
			continue
		}

		stream := finalStream
		if firstChunk != nil {
			stream = &prefetchedReduceStream{
				stream:     finalStream,
				firstChunk: firstChunk,
			}
		}

		return &LegacySearchResult{
			Stream: stream,
			Plans:  shardPlans,
		}, nil
	}
	return nil, lastErr
}

func (c *legacyClient) Query(ctx context.Context, req *LegacyQueryRequest) (result *LegacyQueryResult, err error) {
	defer func() { err = toMilvusError(err) }()
	if req.Plans != nil {
		return c.queryRetainedPlans(ctx, req)
	}
	if c.enableQueryStreaming && supportsQueryStream(req.Req) {
		return c.queryStream(ctx, req)
	}
	vchannels, err := c.shardResolver.ResolveVChannels(ctx, req.Req.CollectionID)
	if err != nil {
		return nil, err
	}

	collector := newLegacyQueryCollector()
	shardPlans := make([]ShardPlan, len(vchannels))
	g, gCtx := errgroup.WithContext(ctx)
	for i := range vchannels {
		i := i
		g.Go(func() error {
			plan, err := c.shardClient.Query(gCtx, &ShardQueryRequest{
				VChannel:    vchannels[i],
				Req:         req.Req,
				MVCC:        req.MVCC,
				BarrierMVCC: req.BarrierMVCC,
				Reducer:     collector,
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
	results, err := collector.Reduce(ctx, req, shardPlans)
	if err != nil {
		return nil, err
	}
	return &LegacyQueryResult{
		Results: results,
		Plans:   shardPlans,
	}, nil
}

func supportsQueryStream(req *internalpb.RetrieveRequest) bool {
	return req != nil &&
		req.GetLimit() > 0 &&
		!req.GetIsCount() &&
		len(req.GetGroupByFieldIds()) == 0 &&
		len(req.GetAggregates()) == 0 &&
		len(req.GetOrderByFields()) == 0
}

func (c *legacyClient) queryStream(ctx context.Context, req *LegacyQueryRequest) (*LegacyQueryResult, error) {
	var lastErr error
	for attempt := 0; attempt < c.shardClient.maxRetries; attempt++ {
		vchannels, err := c.shardResolver.ResolveVChannels(ctx, req.Req.GetCollectionID())
		if err != nil {
			return nil, err
		}

		vchannelStreams := make([]queryutil.ReduceStream, len(vchannels))
		shardPlans := make([]ShardPlan, len(vchannels))
		var group errgroup.Group
		for i := range vchannels {
			i := i
			group.Go(func() error {
				stream, plan, err := c.shardClient.QueryStream(ctx, vchannels[i], req.Req, c.queryStreamChunkBytes)
				if err != nil {
					return err
				}
				vchannelStreams[i] = stream
				shardPlans[i] = *plan
				return nil
			})
		}
		if err := group.Wait(); err != nil {
			for _, stream := range vchannelStreams {
				if stream != nil {
					err = errors.Join(err, stream.Close())
				}
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr = err
			continue
		}

		finalStream, err := queryutil.NewReduceStream(req.Req, vchannelStreams, c.queryStreamChunkBytes)
		if err != nil {
			for _, stream := range vchannelStreams {
				err = errors.Join(err, stream.Close())
			}
			return nil, err
		}
		firstChunk, recvErr := finalStream.Recv()
		if recvErr != nil && !errors.Is(recvErr, io.EOF) {
			err = errors.Join(recvErr, finalStream.Close())
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr = err
			continue
		}

		stream := finalStream
		if firstChunk != nil {
			stream = &prefetchedQueryReduceStream{stream: finalStream, firstChunk: firstChunk}
		}
		return &LegacyQueryResult{Stream: stream, Plans: shardPlans}, nil
	}
	return nil, lastErr
}

type legacySearchCollector struct {
	mu      sync.Mutex
	results map[string][]*internalpb.SearchResults
}

func newLegacySearchCollector() *legacySearchCollector {
	return &legacySearchCollector{
		results: make(map[string][]*internalpb.SearchResults),
	}
}

func (c *legacySearchCollector) Add(shardID qviews.ShardID, resp *viewpb.SearchOnViewResponse) error {
	result := resp.GetLegacyResults()
	if result == nil {
		return merr.WrapErrServiceInternalMsg("missing legacy search result for shard %s", shardID.String())
	}
	if !merr.Ok(result.GetStatus()) {
		return merr.Error(result.GetStatus())
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.results[shardID.String()] = append(c.results[shardID.String()], result)
	return nil
}

func (c *legacySearchCollector) ResetShard(shardID qviews.ShardID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.results, shardID.String())
}

func (c *legacySearchCollector) Finish() (*internalpb.SearchResults, error) {
	return nil, merr.WrapErrServiceInternalMsg("legacy search collector does not reduce results")
}

type legacyQueryCollector struct {
	mu      sync.Mutex
	results map[string][]*internalpb.RetrieveResults
}

func newLegacyQueryCollector() *legacyQueryCollector {
	return &legacyQueryCollector{
		results: make(map[string][]*internalpb.RetrieveResults),
	}
}

func (c *legacyQueryCollector) Add(shardID qviews.ShardID, resp *viewpb.QueryOnViewResponse) error {
	result := resp.GetLegacyResults()
	if result == nil {
		return merr.WrapErrServiceInternalMsg("missing legacy query result for shard %s", shardID.String())
	}
	if !merr.Ok(result.GetStatus()) {
		return merr.Error(result.GetStatus())
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.results[shardID.String()] = append(c.results[shardID.String()], result)
	return nil
}

func (c *legacyQueryCollector) ResetShard(shardID qviews.ShardID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.results, shardID.String())
}

func (c *legacyQueryCollector) Finish() (*internalpb.RetrieveResults, error) {
	return nil, merr.WrapErrServiceInternalMsg("legacy query collector does not reduce results")
}

// Requery failure aborts the whole parent operation. Retrying Phase 1 here would
// mix its old reduced search results with a new schema or data snapshot.
func (c *legacyClient) queryRetainedPlans(ctx context.Context, req *LegacyQueryRequest) (*LegacyQueryResult, error) {
	collector := newLegacyQueryCollector()
	group, groupCtx := errgroup.WithContext(ctx)
	for _, plan := range req.Plans {
		for _, node := range plan.WorkNodes {
			group.Go(func() error {
				request := proto.Clone(req.Req).(*internalpb.RetrieveRequest)
				request.MvccTimestamp = legacyMVCCForNode(plan.Mvcc, node)
				response, err := c.shardClient.queryServiceClient.QueryOnView(groupCtx, node, &viewpb.QueryOnViewRequest{
					SegmentSelection: qviews.WorkNodeSelection(&viewpb.QueryPlan{WorkNodes: plan.Selections}, node).GetSegmentSelection(),
					LegacyReq:        request, ShardId: plan.ShardID.IntoProto(), Version: plan.Version, Mvcc: plan.Mvcc,
				})
				if err != nil {
					return err
				}
				if isEmptySuccessfulQueryResponse(response) {
					return nil
				}
				return collector.Add(plan.ShardID, response)
			})
		}
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	results, err := collector.Reduce(ctx, req, req.Plans)
	if err != nil {
		return nil, err
	}
	return &LegacyQueryResult{Results: results, Plans: req.Plans}, nil
}
