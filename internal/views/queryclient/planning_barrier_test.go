package queryclient

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	queryreducestream "github.com/milvus-io/milvus/internal/util/queryutil/reduce_stream"
	searchreducestream "github.com/milvus-io/milvus/internal/util/searchutil/reduce_stream"
	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLineageExecutionFailureDoesNotReplanOneShard(t *testing.T) {
	setupTestReplicaRouting(t)
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprintf("streaming_%t", streaming), func(t *testing.T) {
			sn := newLineageTestSN()
			sn.setPrefix(lineageT1, 120, 110)
			sn.setPrefix(lineageT2, 100, 110)
			failure := viewerror.NewViewInvalidated("plan retired after the request barrier")
			workers := &legacyServiceClient{
				queryOnView: func(_ context.Context, _ qviews.WorkNode, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
					if req.GetShardId().GetVchannel() == lineageT1 {
						return nil, failure
					}
					return &viewpb.QueryOnViewResponse{LegacyResults: &internalpb.RetrieveResults{Status: merr.Success()}}, nil
				},
				queryOnViewStream: func(ctx context.Context, _ qviews.WorkNode, req *viewpb.QueryOnViewRequest) (queryreducestream.ReduceStream, error) {
					if req.GetShardId().GetVchannel() == lineageT1 {
						return &fakeQueryStream{ctx: ctx, recv: []fakeQueryStreamRecv{{err: failure}}}, nil
					}
					return &fakeQueryStream{ctx: ctx, recv: []fakeQueryStreamRecv{{chunk: newTestQueryChunk([]int64{1})}}}, nil
				},
			}
			client := newLegacyClient(ViewQueryClientConfig{MaxRetries: 2}, sn, workers, &lineageTestResolver{
				legacyResolver: legacyResolver{vchannels: []string{lineageS, lineageT1, lineageT2}}, shards: lineageShards(),
			})
			defer client.shardClient.replicas.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			result, err := client.Query(ctx, &LegacyQueryRequest{Req: &internalpb.RetrieveRequest{CollectionID: 100, Limit: 1}, AllowStreaming: streaming, ReduceStreamChunkBytes: 64})
			if streaming {
				require.NoError(t, err)
				_, err = result.Stream.Recv()
				require.NoError(t, result.Stream.Close())
			}
			require.ErrorContains(t, err, "plan retired after the request barrier")
			for _, shard := range lineageShards() {
				require.Len(t, sn.planRequests(shard.VChannel), 1, "a failed execution must discard the whole request attempt")
			}
		})
	}
}

type blockedLineagePlanner struct {
	*lineageTestSN
	blockedChannel string
	twoStage       bool
	blocked        chan struct{}
	release        chan struct{}
	ready          chan struct{}
	once           sync.Once
}

func TestLineageStreamsKeepThePlannedReadPoint(t *testing.T) {
	setupTestReplicaRouting(t)
	for _, operation := range []string{"search", "query", "hybrid"} {
		t.Run(operation, func(t *testing.T) {
			sn := newLineageTestSN()
			sn.setPrefix(lineageT1, 120, 110)
			sn.setPrefix(lineageT2, 100, 110)
			var mu sync.Mutex
			var premature atomic.Int32
			executed := map[string]*viewpb.QueryPlanMVCC{}
			record := func(channel string, mvcc *viewpb.QueryPlanMVCC) {
				mu.Lock()
				defer mu.Unlock()
				executed[channel] = mvcc
				if len(sn.planRequests(lineageS)) != 1 {
					premature.Add(1)
				}
			}
			workers := &legacyServiceClient{
				searchOnViewStream: func(ctx context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (searchreducestream.ReduceStream, error) {
					record(req.GetShardId().GetVchannel(), req.GetMvcc())
					id := map[string]int64{lineageS: 1, lineageT1: 2, lineageT2: 3}[req.GetShardId().GetVchannel()]
					chunk := newTestSearchChunk(req.GetLegacyReq().GetTopk(), []int64{id}, []float32{1})
					chunk.MetricType = req.GetLegacyReq().GetMetricType()
					return &fakeSearchStream{ctx: ctx, recv: []fakeSearchStreamRecv{{chunk: chunk}}}, nil
				},
				queryOnViewStream: func(ctx context.Context, _ qviews.WorkNode, req *viewpb.QueryOnViewRequest) (queryreducestream.ReduceStream, error) {
					record(req.GetShardId().GetVchannel(), req.GetMvcc())
					id := map[string]int64{lineageS: 1, lineageT1: 2, lineageT2: 3}[req.GetShardId().GetVchannel()]
					return &fakeQueryStream{ctx: ctx, recv: []fakeQueryStreamRecv{{chunk: newTestQueryChunk([]int64{id})}}}, nil
				},
			}
			client := newLegacyClient(ViewQueryClientConfig{MaxRetries: 2}, sn, workers, &lineageTestResolver{
				legacyResolver: legacyResolver{vchannels: []string{lineageS, lineageT1, lineageT2}}, shards: lineageShards(),
			})
			defer client.shardClient.replicas.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if operation == "query" {
				result, err := client.Query(ctx, &LegacyQueryRequest{Req: &internalpb.RetrieveRequest{CollectionID: 100, Limit: 1, ConsistencyLevel: commonpb.ConsistencyLevel_Strong}, AllowStreaming: true, ReduceStreamChunkBytes: 64})
				require.NoError(t, err)
				_, err = result.Stream.Recv()
				require.NoError(t, err, "successful setup must not cancel worker streams")
				require.NoError(t, result.Stream.Close())
			} else {
				req := &internalpb.SearchRequest{CollectionID: 100, Nq: 1, Topk: 1, MetricType: "IP", ConsistencyLevel: commonpb.ConsistencyLevel_Strong}
				if operation == "hybrid" {
					req = hybridStreamRequest()
					req.CollectionID = 100
					req.ConsistencyLevel = commonpb.ConsistencyLevel_Strong
				}
				result, err := client.Search(ctx, &LegacySearchRequest{Req: req, AllowStreaming: true, ReduceStreamChunkBytes: 64})
				require.NoError(t, err)
				streams := result.SubStreams
				if result.Stream != nil {
					streams = []searchreducestream.ReduceStream{result.Stream}
				}
				for _, stream := range streams {
					_, err := stream.Recv()
					require.NoError(t, err)
					require.NoError(t, stream.Close())
				}
			}
			require.Zero(t, premature.Load(), "source must plan before any worker starts")
			require.True(t, proto.Equal(sourceMVCC(110, 110, 110), executed[lineageS]))
			for _, target := range []string{lineageT1, lineageT2} {
				require.Len(t, sn.planRequests(target), 1)
				require.EqualValues(t, 110, sn.planRequests(target)[0].GetBarrierMvcc())
				require.EqualValues(t, 110, executed[target].GetGrowingTimetick())
			}
		})
	}
}

func TestLineageStreamsPreservePlanningFailures(t *testing.T) {
	setupTestReplicaRouting(t)
	for _, invalidTopology := range []bool{false, true} {
		t.Run(fmt.Sprintf("invalid_topology_%t", invalidTopology), func(t *testing.T) {
			sn := newLineageTestSN()
			sn.setPrefix(lineageT1, 120, 110)
			sn.setPrefix(lineageT2, 100, 110)
			shards := lineageShards()
			want := "prefix sampling failed"
			if invalidTopology {
				shards = shards[:2]
				want = "is not in the serving list"
			} else {
				sn.prefixErrs[lineageT2] = []error{merr.WrapErrServiceInternalMsg("prefix sampling failed")}
			}
			var calls atomic.Int32
			service := &legacyServiceClient{queryOnViewStream: func(context.Context, qviews.WorkNode, *viewpb.QueryOnViewRequest) (queryreducestream.ReduceStream, error) {
				calls.Add(1)
				return nil, merr.WrapErrServiceInternalMsg("unexpected worker open")
			}}
			client := newLegacyClient(ViewQueryClientConfig{MaxRetries: 1}, sn, service, &lineageTestResolver{shards: shards})
			defer client.shardClient.replicas.Close()
			result, err := client.Query(t.Context(), &LegacyQueryRequest{Req: &internalpb.RetrieveRequest{CollectionID: 100}, AllowStreaming: true})
			require.Nil(t, result)
			require.ErrorContains(t, err, want)
			require.Zero(t, calls.Load())
		})
	}
}

func (p *blockedLineagePlanner) GetQueryPlan(ctx context.Context, shard qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	if shard.VChannel == p.blockedChannel {
		close(p.blocked)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.release:
			return nil, merr.WrapErrServiceInternalMsg("injected shard planning failure")
		}
	}
	response, err := p.lineageTestSN.GetQueryPlan(ctx, shard, req)
	if err == nil && p.twoStage {
		prepared := twoStageResponse(shard)
		prepared.Plan.Mvcc = response.Plan.Mvcc
		prepared.Plan.GetLegacySearchRequest().SchemaVersion = req.GetLegacySearchRequest().GetSchemaVersion()
		response = prepared
	}
	if shard.VChannel == lineageT1 {
		p.once.Do(func() { close(p.ready) })
	}
	return response, err
}

func TestLineageRequestPlansBeforeExecution(t *testing.T) {
	setupTestReplicaRouting(t)
	for _, operation := range []string{"search", "query", "statistics", "search_stream", "query_stream", "hybrid_stream", "published_query", "published_g0", "source_last", "two_stage"} {
		t.Run(operation, func(t *testing.T) {
			planner := &blockedLineagePlanner{lineageTestSN: newLineageTestSN(), blockedChannel: lineageT2, blocked: make(chan struct{}), release: make(chan struct{}), ready: make(chan struct{})}
			if operation == "source_last" {
				planner.blockedChannel = lineageS
			}
			if operation == "two_stage" {
				enableTwoStageSearch(t)
				planner.twoStage = true
			}
			planner.setPrefix(lineageT1, 120, 110)
			planner.setPrefix(lineageT2, 100, 110)
			var executions atomic.Int32
			unexpected := func() error {
				executions.Add(1)
				return merr.WrapErrServiceInternalMsg("execution started before every shard planned")
			}
			workers := &legacyServiceClient{
				searchOnView: func(context.Context, qviews.WorkNode, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
					return nil, unexpected()
				},
				queryOnView: func(context.Context, qviews.WorkNode, *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
					return nil, unexpected()
				},
				statisticsOnView: func(context.Context, qviews.WorkNode, *viewpb.StatisticsOnViewRequest) (*viewpb.StatisticsOnViewResponse, error) {
					return nil, unexpected()
				},
				searchOnViewStream: func(context.Context, qviews.WorkNode, *viewpb.SearchOnViewRequest) (searchreducestream.ReduceStream, error) {
					return nil, unexpected()
				},
				queryOnViewStream: func(context.Context, qviews.WorkNode, *viewpb.QueryOnViewRequest) (queryreducestream.ReduceStream, error) {
					return nil, unexpected()
				},
			}
			shards := append(lineageShards(), resolver.Shard{VChannel: plainA})
			if operation == "published_query" {
				shards = []resolver.Shard{{VChannel: lineageT1, ShardSetEpoch: 3}, {VChannel: lineageT2, ShardSetEpoch: 3}, {VChannel: plainA, ShardSetEpoch: 3}}
			}
			if operation == "published_g0" {
				for i := range shards {
					shards[i].Lineage = nil
					shards[i].LoadInfoVersion = 10
				}
			}
			client := newLegacyClient(ViewQueryClientConfig{MaxRetries: 1}, planner, workers, &lineageTestResolver{
				legacyResolver: legacyResolver{vchannels: []string{lineageS, lineageT1, lineageT2, plainA}}, shards: shards,
			})
			defer client.shardClient.replicas.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var err error
				switch operation {
				case "statistics":
					_, err = client.Statistics(ctx, 100, &viewpb.StatisticsRequest{}, 110)
				case "query", "query_stream", "published_query", "published_g0", "source_last":
					_, err = client.Query(ctx, &LegacyQueryRequest{Req: &internalpb.RetrieveRequest{CollectionID: 100}, AllowStreaming: operation == "query_stream"})
				default:
					req := &internalpb.SearchRequest{CollectionID: 100, Nq: 1, Topk: 1, MetricType: "IP"}
					if operation == "hybrid_stream" {
						req = hybridStreamRequest()
						req.CollectionID = 100
					}
					_, err = client.Search(ctx, &LegacySearchRequest{Req: req, AllowStreaming: operation != "search" && operation != "two_stage"})
				}
				done <- err
			}()
			select {
			case <-planner.blocked:
			case <-ctx.Done():
				t.Fatal("target plan did not start")
			}
			select {
			case <-planner.ready:
			case <-ctx.Done():
				t.Fatal("sibling plan did not complete")
			}
			// Let the completed sibling reach the execution boundary while the
			// other target cannot produce a plan. It must stay behind the barrier.
			require.Never(t, func() bool { return executions.Load() != 0 }, 50*time.Millisecond, time.Millisecond)
			close(planner.release)
			require.ErrorContains(t, <-done, "injected shard planning failure")
			require.Zero(t, executions.Load())
		})
	}
}
