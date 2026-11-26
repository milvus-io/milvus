package queryclient

import (
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	queryreducestream "github.com/milvus-io/milvus/internal/util/queryutil/reduce_stream"
	searchreducestream "github.com/milvus-io/milvus/internal/util/searchutil/reduce_stream"
	"github.com/milvus-io/milvus/internal/views/queryclient/reducer"
	"github.com/milvus-io/milvus/internal/views/queryclient/renderer"
	"github.com/milvus-io/milvus/internal/views/queryclient/reranker"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestWholeRequestRetryOwnsLegacyHints(t *testing.T) {
	for _, search := range []bool{false, true} {
		t.Run(map[bool]string{false: "query", true: "search"}[search], func(t *testing.T) {
			prior := &workNodeFailure{node: qviews.NewQueryNode(1), shardID: qviews.ShardID{VChannel: "a", ReplicaID: 1}, err: status.Error(codes.Unavailable, "prior")}
			failure := &workNodeFailure{node: qviews.NewQueryNode(2), shardID: qviews.ShardID{VChannel: "a", ReplicaID: 2}, err: status.Error(codes.Unavailable, "current")}
			backing := []error{prior, merr.ErrServiceInternal}
			client := newLegacyClient(ViewQueryClientConfig{MaxRetries: 2, LoadInfoClient: &servingCacheClient{}}, nil, nil, nil)
			attempts := 0
			check := func(hints []error) error {
				attempts++
				require.Same(t, prior, hints[0])
				if attempts == 1 {
					require.Len(t, hints, 1)
					return failure
				}
				require.Equal(t, []error{prior, failure}, hints)
				return nil
			}
			if search {
				req := &LegacySearchRequest{Req: &internalpb.SearchRequest{CollectionID: 100}, RetryErrors: backing[:1]}
				patch := mockey.Mock((*legacyClient).search).To(func(_ *legacyClient, _ context.Context, actual *LegacySearchRequest) (*LegacySearchResult, error) {
					require.NotSame(t, req, actual)
					return &LegacySearchResult{}, check(actual.RetryErrors)
				}).Build()
				defer patch.UnPatch()
				_, err := client.Search(t.Context(), req)
				require.NoError(t, err)
				require.Len(t, req.RetryErrors, 1)
			} else {
				req := &LegacyQueryRequest{Req: &internalpb.RetrieveRequest{CollectionID: 100}, RetryErrors: backing[:1]}
				patch := mockey.Mock((*legacyClient).query).To(func(_ *legacyClient, _ context.Context, actual *LegacyQueryRequest) (*LegacyQueryResult, error) {
					require.NotSame(t, req, actual)
					return &LegacyQueryResult{}, check(actual.RetryErrors)
				}).Build()
				defer patch.UnPatch()
				_, err := client.Query(t.Context(), req)
				require.NoError(t, err)
				require.Len(t, req.RetryErrors, 1)
			}
			require.Equal(t, 2, attempts)
			require.Equal(t, merr.ErrServiceInternal, backing[1], "append must not overwrite the caller's slice capacity")
		})
	}
}

// A competing request consumes the next router slot before a whole-request
// retry. Explicit failure hints must still prefer the healthy replica.
func TestWholeRequestRetryReplansEveryShard(t *testing.T) {
	for _, operation := range []string{"search", "query", "statistics", "generic_search", "generic_query", "search_setup", "query_setup", "search_recv", "query_recv"} {
		t.Run(operation, func(t *testing.T) {
			router, directory := newTestReplicaRouter(t)
			channels := []string{"p_100v0", "p_100v1"}
			version := qviews.QueryViewVersion{QueryVersion: 1}
			for _, channel := range channels {
				directory.Set(channel, 1, &version, false)
				directory.Set(channel, 2, &version, false)
				route, err := router.route(channel)
				require.NoError(t, err)
				require.Eventually(t, func() bool { route.mu.Lock(); defer route.mu.Unlock(); return len(route.snapshot.Replicas) == 2 }, time.Second, time.Millisecond)
			}
			var mu sync.Mutex
			plans := make(map[string][]*viewpb.GetQueryPlanRequest)
			var joined atomic.Bool
			started := make(chan struct{})
			planner := &legacyPlanClient{getQueryPlan: func(_ context.Context, shard qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
				mu.Lock()
				defer mu.Unlock()
				if len(plans[shard.VChannel]) > 0 {
					require.True(t, joined.Load(), "old workers must join before any shard replans")
				}
				plans[shard.VChannel] = append(plans[shard.VChannel], req)
				require.EqualValues(t, 150, req.GetBarrierMvcc())
				plan := legacySearchPlan(shard, qviews.NewQueryNode(shard.ReplicaID*10))
				plan.Mvcc = &viewpb.QueryPlanMVCC{GrowingTimetick: 150, TransformingTimetick: 150}
				response := &viewpb.GetQueryPlanResponse{Plan: plan}
				switch operation {
				case "query", "generic_query", "query_setup", "query_recv":
					plan.Request = &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: req.GetLegacyRetrieveRequest()}
				case "statistics":
					plan.Request = &viewpb.QueryPlan_StatisticsRequest{StatisticsRequest: req.GetStatisticsRequest()}
					response.GrowingInlineResponse = &viewpb.GrowingInlineResponse{Response: &viewpb.GrowingInlineResponse_StatisticsResponse{StatisticsResponse: &viewpb.StatisticsOnViewResponse{RowCount: 1}}}
				default:
					plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: req.GetLegacySearchRequest()}
				}
				return response, nil
			}}
			fail := func(ctx context.Context) error {
				select {
				case <-started:
				case <-ctx.Done():
					return ctx.Err()
				}
				route, err := router.route(channels[0])
				require.NoError(t, err)
				_, _, err = route.nextReplica(ctx, map[qviews.Replica]bool{}, nil, testCollectionLoaded)
				require.NoError(t, err)
				return status.Error(codes.Unavailable, "worker disconnected")
			}
			execute := func(ctx context.Context, shard *viewpb.ShardID) error {
				if shard.GetReplicaId() != 1 {
					return nil
				}
				if shard.GetVchannel() == channels[1] {
					close(started)
					<-ctx.Done()
					joined.Store(true)
					return ctx.Err()
				}
				return fail(ctx)
			}
			receiving := make(chan struct{})
			prepareStream := func(ctx context.Context, shard *viewpb.ShardID) (func() error, func(), error) {
				if shard.GetReplicaId() != 1 {
					return nil, nil, nil
				}
				if shard.GetVchannel() == channels[1] {
					close(started)
					return func() error { close(receiving); <-ctx.Done(); return ctx.Err() }, func() { joined.Store(true) }, nil
				}
				if strings.HasSuffix(operation, "_setup") {
					return nil, nil, fail(ctx)
				}
				return func() error {
					select {
					case <-receiving:
						return fail(ctx)
					case <-ctx.Done():
						return ctx.Err()
					}
				}, nil, nil
			}
			workers := &legacyServiceClient{
				searchOnView: func(ctx context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
					return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success()}}, execute(ctx, req.GetShardId())
				},
				queryOnView: func(ctx context.Context, _ qviews.WorkNode, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
					return &viewpb.QueryOnViewResponse{LegacyResults: &internalpb.RetrieveResults{Status: merr.Success()}}, execute(ctx, req.GetShardId())
				},
				statisticsOnView: func(ctx context.Context, _ qviews.WorkNode, req *viewpb.StatisticsOnViewRequest) (*viewpb.StatisticsOnViewResponse, error) {
					return &viewpb.StatisticsOnViewResponse{RowCount: 10}, execute(ctx, req.GetShardId())
				},
			}
			workers.searchOnViewStream = func(ctx context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (searchreducestream.ReduceStream, error) {
				recv, closed, err := prepareStream(ctx, req.GetShardId())
				if err != nil {
					return nil, err
				}
				id := req.GetShardId().GetReplicaId() * 10
				if req.GetShardId().GetVchannel() == channels[1] {
					id++
				}
				child := &fakeSearchStream{recv: []fakeSearchStreamRecv{{chunk: newTestSearchChunk(1, []int64{id}, []float32{1})}}}
				return &retryJoinStream[*internalpb.SearchResults]{receive: func() (*internalpb.SearchResults, error) {
					if recv != nil {
						return nil, recv()
					}
					return child.Recv()
				}, onClose: closed}, nil
			}
			workers.queryOnViewStream = func(ctx context.Context, _ qviews.WorkNode, req *viewpb.QueryOnViewRequest) (queryreducestream.ReduceStream, error) {
				recv, closed, err := prepareStream(ctx, req.GetShardId())
				if err != nil {
					return nil, err
				}
				id := req.GetShardId().GetReplicaId() * 10
				if req.GetShardId().GetVchannel() == channels[1] {
					id++
				}
				child := &fakeQueryStream{recv: []fakeQueryStreamRecv{{chunk: newTestQueryChunk([]int64{id})}}}
				return &retryJoinStream[*internalpb.RetrieveResults]{receive: func() (*internalpb.RetrieveResults, error) {
					if recv != nil {
						return nil, recv()
					}
					return child.Recv()
				}, onClose: closed}, nil
			}
			var loads atomic.Int32
			client := newLegacyClient(ViewQueryClientConfig{MaxRetries: 2, LoadInfoClient: &servingCacheClient{get: func(context.Context, int64) (*querypb.GetQueryViewLoadInfoResponse, error) {
				loads.Add(1)
				return &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success(), Version: 11, ShardSetEpoch: 3, ServingShards: []*viewpb.DataViewOfShard{{Vchannel: channels[0]}, {Vchannel: channels[1]}}}, nil
			}}}, planner, workers, nil)
			client.shardClient.replicas = router
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			switch operation {
			case "search_setup", "query_setup", "search_recv", "query_recv":
				attempts := 1
				if strings.HasSuffix(operation, "_recv") {
					attempts = 2
				}
				var retryErrors []error
				for attempt := 0; attempt < attempts; attempt++ {
					var recvErr error
					if strings.HasPrefix(operation, "search") {
						result, err := client.Search(ctx, &LegacySearchRequest{Req: &internalpb.SearchRequest{CollectionID: 100, Nq: 1, Topk: 1, MetricType: "IP"}, BarrierMVCC: 150, AllowStreaming: true, ReduceStreamChunkBytes: 64, RetryErrors: retryErrors})
						require.NoError(t, err)
						_, recvErr = result.Stream.Recv()
						require.NoError(t, result.Stream.Close())
					} else {
						result, err := client.Query(ctx, &LegacyQueryRequest{Req: &internalpb.RetrieveRequest{CollectionID: 100, Limit: 1}, BarrierMVCC: 150, AllowStreaming: true, ReduceStreamChunkBytes: 64, RetryErrors: retryErrors})
						require.NoError(t, err)
						_, recvErr = result.Stream.Recv()
						require.NoError(t, result.Stream.Close())
					}
					if attempt+1 < attempts {
						require.Equal(t, codes.Unavailable, status.Code(recvErr))
						require.True(t, joined.Load(), "Proxy must close and join old streams before retrying")
						// This is the existing Proxy receive retry boundary.
						retryErrors = append(retryErrors, recvErr)
					} else {
						require.NoError(t, recvErr)
					}
				}
			case "generic_search", "generic_query":
				generic := NewViewQueryClient(ViewQueryClientConfig{MaxRetries: 2}, planner, workers, client.shardResolver,
					NewDefaultFieldFetchPlanner(), retryRerankerBuilder{}, retryRendererBuilder{}, retrySearchReducerBuilder{}, retryRetrieveReducerBuilder{}, nil).(*viewQueryClientImpl)
				generic.shardClient.replicas = router
				if operation == "generic_search" {
					_, err := generic.Search(ctx, &SearchRequest{Req: &internalpb.SearchRequest{CollectionID: 100}, BarrierMVCC: 150})
					require.NoError(t, err)
				} else {
					_, err := generic.Query(ctx, &QueryRequest{Req: &internalpb.RetrieveRequest{CollectionID: 100}, BarrierMVCC: 150})
					require.NoError(t, err)
				}
			case "search":
				_, err := client.Search(ctx, &LegacySearchRequest{Req: &internalpb.SearchRequest{CollectionID: 100}, BarrierMVCC: 150})
				require.NoError(t, err)
			case "query":
				_, err := client.Query(ctx, &LegacyQueryRequest{Req: &internalpb.RetrieveRequest{CollectionID: 100}, BarrierMVCC: 150})
				require.NoError(t, err)
			case "statistics":
				rows, err := client.Statistics(ctx, 100, &viewpb.StatisticsRequest{}, 150)
				require.NoError(t, err)
				require.EqualValues(t, 22, rows, "failed attempt counts cannot leak")
			}
			for _, channel := range channels {
				require.Len(t, plans[channel], 2)
				require.EqualValues(t, 1, plans[channel][0].GetShardId().GetReplicaId())
				require.EqualValues(t, 2, plans[channel][1].GetShardId().GetReplicaId())
			}
			require.EqualValues(t, 1, loads.Load())
		})
	}
}

type retryRendererBuilder struct{}

func (retryRendererBuilder) Build(context.Context, *renderer.BuildRequest) (renderer.Renderer, error) {
	return renderer.NewNoopRenderer(), nil
}

type retryRerankerBuilder struct{}

func (retryRerankerBuilder) Build(context.Context, *reranker.BuildRequest) (reranker.Reranker, error) {
	return nil, nil
}

type retrySearchReducerBuilder struct{}

func (retrySearchReducerBuilder) Build(*internalpb.SearchRequest) (reducer.SearchResultReducer, error) {
	return fakeSearchResultReducer{}, nil
}

type retryRetrieveReducerBuilder struct{}

func (retryRetrieveReducerBuilder) Build(*internalpb.RetrieveRequest) (reducer.RetrieveResultReducer, error) {
	return retryRetrieveReducer{}, nil
}

type retryRetrieveReducer struct{}

func (retryRetrieveReducer) Add(qviews.ShardID, *viewpb.QueryOnViewResponse) error { return nil }
func (retryRetrieveReducer) ResetShard(qviews.ShardID)                             {}
func (retryRetrieveReducer) Finish() (*internalpb.RetrieveResults, error) {
	return &internalpb.RetrieveResults{}, nil
}

// Close joins an in-flight Recv. The real reducer/client must cancel its worker
// context first; this fake intentionally cannot make that happen on its own.
type retryJoinStream[T any] struct {
	mu      sync.Mutex
	pending sync.WaitGroup
	closed  bool
	receive func() (T, error)
	onClose func()
}

func (s *retryJoinStream[T]) Recv() (T, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		var zero T
		return zero, io.ErrClosedPipe
	}
	s.pending.Add(1)
	s.mu.Unlock()
	defer s.pending.Done()
	return s.receive()
}

func (s *retryJoinStream[T]) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.pending.Wait()
	if s.onClose != nil {
		s.onClose()
	}
	return nil
}
func (s *retryJoinStream[T]) Interrupt() (T, error) { var zero T; return zero, io.ErrClosedPipe }
