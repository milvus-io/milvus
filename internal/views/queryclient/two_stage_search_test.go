package queryclient

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/internal/util/searchutil/optimizers"
	searchreducestream "github.com/milvus-io/milvus/internal/util/searchutil/reduce_stream"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func enableTwoStageSearch(t *testing.T) {
	paramtable.Init()
	cfg := &paramtable.Get().AutoIndexConfig
	for key, value := range map[string]string{cfg.TwoStageSearchEnabled.Key: "true", cfg.TwoStageSearchMinTopk.Key: "1", cfg.TwoStageSearchMinNumSegments.Key: "1"} {
		old := paramtable.Get().GetWithDefault(key, "")
		require.NoError(t, paramtable.Get().Save(key, value))
		t.Cleanup(func() { require.NoError(t, paramtable.Get().Save(key, old)) })
	}
}

func twoStageResponse(id qviews.ShardID) *viewpb.GetQueryPlanResponse {
	plan := newTestSearchQueryPlan(id, &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90})
	plan.GetLegacySearchRequest().Topk = 5
	plan.GetLegacySearchRequest().SearchType = internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER
	plan.GetLegacySearchRequest().SchemaVersion = 7
	plan.WorkNodes[0].SegmentSelection = &viewpb.SegmentSelection{Mode: viewpb.SegmentSelection_Include, SegmentIds: []int64{10, 20}}
	plan.WorkNodes = append(plan.WorkNodes, &viewpb.QueryPlanWorkNode{Node: &viewpb.QueryPlanWorkNode_StreamingNode{StreamingNode: &viewpb.StreamingWorkNode{Pchannel: "p"}}})
	return &viewpb.GetQueryPlanResponse{Plan: plan, ClientOptimizeInfo: &viewpb.QueryPlanClientOptimizeInfo{SearchOptimization: &viewpb.SearchOptimizationInfo{SealedSegmentCount: 2, VectorDim: 128, IndexType: "HNSW"}}}
}

func TestTwoStageSearchStatistics(t *testing.T) {
	enableTwoStageSearch(t)
	for _, tc := range []struct {
		name               string
		counts, ids        []int64
		completed, wantErr bool
	}{
		{"complete", []int64{2, 3}, []int64{10, 20}, true, false},
		{"missing", nil, []int64{10, 20}, false, false},
		{"partial", []int64{2}, []int64{10}, false, false},
		{"negative", []int64{-1, 3}, []int64{10, 20}, false, true},
		{"duplicate", []int64{2, 3}, []int64{10, 10}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := qviews.ShardID{VChannel: "p_1v0", ReplicaID: 1}
			planResponse := twoStageResponse(id)
			plan := planResponse.GetPlan()
			original := proto.Clone(plan.GetLegacySearchRequest())
			service := &fakeViewQueryServiceClient{}
			calls := 0
			patch := mockey.Mock((*fakeViewQueryServiceClient).SearchOnView).To(func(_ *fakeViewQueryServiceClient, _ context.Context, node qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
				calls++
				require.IsType(t, qviews.QueryNode{}, node)
				require.True(t, proto.Equal(plan.Mvcc, req.Mvcc))
				require.True(t, proto.Equal(plan.Version, req.Version))
				require.True(t, proto.Equal(plan.WorkNodes[0].SegmentSelection, req.SegmentSelection))
				require.EqualValues(t, 7, req.LegacyReq.SchemaVersion)
				require.True(t, req.ExecutionOptions.FilterOnly)
				require.True(t, req.ExecutionOptions.EnableExprCache)
				return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success(), SealedSegmentIDsSearched: tc.ids, FilterValidCounts: tc.counts, FeatureBits: 1}}, nil
			}).Build()
			defer patch.UnPatch()
			optimized := false
			tune := mockey.Mock(optimizers.OptimizeSearchParams).To(func(_ context.Context, req *querypb.SearchRequest, _ optimizers.QueryHook, segments int, second bool, dim func(int64) int64, index string) (*querypb.SearchRequest, error) {
				optimized = true
				require.Equal(t, 2, segments)
				require.True(t, second)
				require.EqualValues(t, 3, req.TotalChannelNum)
				require.EqualValues(t, 128, dim(0))
				require.Equal(t, "HNSW", index)
				req.Req.Topk = 3
				return req, nil
			}).Build()
			defer tune.UnPatch()
			client := newShardViewQueryClient(1, nil, service)
			var stage twoStageSearch
			err := client.prepareTwoStageSearch(t.Context(), planResponse, id, 3, &stage)
			require.Equal(t, tc.wantErr, err != nil)
			require.Equal(t, tc.completed, stage.completed)
			require.Equal(t, tc.completed, optimized)
			require.Equal(t, 1, calls)
			if tc.completed {
				require.EqualValues(t, 3, plan.GetLegacySearchRequest().Topk)
			}
			require.EqualValues(t, 5, original.(*internalpb.SearchRequest).Topk)
			response := &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{}}
			stage.mergeFeatures(qviews.NewQueryNode(11), response)
			bit, _ := featureusage.ExecBit(featureusage.FeatureTwoStageSearch)
			require.Equal(t, tc.completed, response.LegacyResults.FeatureBits&bit != 0)
			require.NotZero(t, response.LegacyResults.FeatureBits&1)
		})
	}
}

func TestTwoStageSearchRetriesEntireAttempt(t *testing.T) {
	enableTwoStageSearch(t)
	setupTestReplicaRouting(t)
	id := qviews.ShardID{VChannel: "p_1v0", ReplicaID: 1}
	source := &fakeQueryPlanClient{}
	service := &fakeViewQueryServiceClient{}
	var mu sync.Mutex
	plans, filters, anns := 0, 0, 0
	patchPlan := mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).To(func(_ *fakeQueryPlanClient, _ context.Context, _ qviews.ShardID, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
		plans++
		planResponse := twoStageResponse(id)
		plan := planResponse.GetPlan()
		plan.WorkNodes = plan.WorkNodes[:1]
		plan.Version.QueryVersion = int64(plans)
		return planResponse, nil
	}).Build()
	defer patchPlan.UnPatch()
	patchRPC := mockey.Mock((*fakeViewQueryServiceClient).SearchOnView).To(func(_ *fakeViewQueryServiceClient, _ context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		if req.GetExecutionOptions().GetFilterOnly() {
			filters++
			require.EqualValues(t, filters, req.Version.QueryVersion)
			return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success(), SealedSegmentIDsSearched: []int64{10, 20}, FilterValidCounts: []int64{2, 3}}}, nil
		}
		anns++
		require.True(t, req.GetExecutionOptions().GetEnableExprCache())
		if anns == 1 {
			return nil, viewerror.NewViewNotFound("handoff")
		}
		return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success()}}, nil
	}).Build()
	defer patchRPC.UnPatch()
	tune := mockey.Mock(optimizers.OptimizeSearchParams).To(func(_ context.Context, req *querypb.SearchRequest, _ optimizers.QueryHook, _ int, _ bool, _ func(int64) int64, _ string) (*querypb.SearchRequest, error) {
		return req, nil
	}).Build()
	defer tune.UnPatch()
	client := newShardViewQueryClient(3, source, service)
	defer client.replicas.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	plan, err := client.Search(ctx, &ShardSearchRequest{VChannel: id.VChannel, Req: twoStageResponse(id).GetPlan().GetLegacySearchRequest(), Reducer: fakeSearchResultReducer{}})
	require.NoError(t, err)
	require.Equal(t, 2, plans)
	require.Equal(t, 2, filters)
	require.Equal(t, 2, anns)
	require.EqualValues(t, 2, plan.Version.QueryVersion)
}

func TestTwoStageSearchSkipsIneligibleRequests(t *testing.T) {
	enableTwoStageSearch(t)
	for _, name := range []string{"no-sealed", "no-filter", "small-topk", "hybrid", "disabled"} {
		t.Run(name, func(t *testing.T) {
			id := qviews.ShardID{VChannel: "p_1v0", ReplicaID: 1}
			planResponse := twoStageResponse(id)
			plan := planResponse.GetPlan()
			switch name {
			case "no-sealed":
				planResponse.ClientOptimizeInfo.SearchOptimization = nil
			case "no-filter":
				plan.GetLegacySearchRequest().SearchType = internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER
			case "small-topk":
				plan.GetLegacySearchRequest().Topk = 0
			case "hybrid":
				plan.GetLegacySearchRequest().IsAdvanced = true
			case "disabled":
				require.NoError(t, paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key, "false"))
				defer paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key, "true")
			}
			// No service is installed: entering the filter round would panic.
			client := newShardViewQueryClient(1, nil, nil)
			var stage twoStageSearch
			require.NoError(t, client.prepareTwoStageSearch(t.Context(), planResponse, id, 1, &stage))
			require.False(t, stage.completed)
			require.Nil(t, stage.filters)
		})
	}
}

func TestTwoStageSearchCancellationJoinsFilterWorkers(t *testing.T) {
	enableTwoStageSearch(t)
	id := qviews.ShardID{VChannel: "p_1v0", ReplicaID: 1}
	planResponse := twoStageResponse(id)
	service := &fakeViewQueryServiceClient{}
	entered, exited := make(chan struct{}), make(chan struct{})
	patch := mockey.Mock((*fakeViewQueryServiceClient).SearchOnView).To(func(_ *fakeViewQueryServiceClient, ctx context.Context, _ qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
		close(entered)
		<-ctx.Done()
		close(exited)
		return nil, ctx.Err()
	}).Build()
	defer patch.UnPatch()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	client := newShardViewQueryClient(1, nil, service)
	var stage twoStageSearch
	go func() { done <- client.prepareTwoStageSearch(ctx, planResponse, id, 1, &stage) }()
	<-entered
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	select {
	case <-exited:
	default:
		t.Fatal("returned before filter worker exited")
	}
	require.False(t, stage.completed)
}

func TestTwoStageSearchMergesFilterCostsOnlyIntoItsWorker(t *testing.T) {
	stage := twoStageSearch{completed: true, filters: map[int64]*internalpb.SearchResults{
		11: {FeatureBits: 1, ScannedRemoteBytes: 7, ScannedTotalBytes: 9, CostAggregation: &internalpb.CostAggregation{ServiceTime: 4}, SearchReadUsage: &internalpb.QueryReadUsage{DataBytes: 11}},
	}}
	response := &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{ScannedRemoteBytes: 3, ScannedTotalBytes: 5, SearchReadUsage: &internalpb.QueryReadUsage{DataBytes: 13}}}
	stage.mergeFeatures(qviews.NewQueryNode(11), response)
	require.NoError(t, stage.mergeReadUsage(qviews.NewQueryNode(11), response.GetLegacyResults()))
	require.EqualValues(t, 24, response.GetLegacyResults().GetSearchReadUsage().GetDataBytes())
	require.Error(t, stage.mergeReadUsage(qviews.NewQueryNode(11), &internalpb.SearchResults{}))
	require.EqualValues(t, 10, response.LegacyResults.ScannedRemoteBytes)
	require.EqualValues(t, 14, response.LegacyResults.ScannedTotalBytes)
	require.EqualValues(t, 4, response.LegacyResults.CostAggregation.ServiceTime)
	other := &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{}}
	stage.mergeFeatures(qviews.NewQueryNode(12), other)
	require.Zero(t, other.LegacyResults.ScannedTotalBytes)
	stage.mergeFeatures(qviews.NewQueryNode(11), &viewpb.SearchOnViewResponse{})
}

func TestTwoStageSearchStreams(t *testing.T) {
	enableTwoStageSearch(t)
	setupTestReplicaRouting(t)
	for _, tc := range []struct {
		name      string
		counts    []int64
		empty     bool
		completed bool
	}{
		{name: "multiple chunks", counts: []int64{2, 3}, completed: true},
		{name: "empty result", counts: []int64{0, 0}, empty: true, completed: true},
		{name: "incomplete statistics", counts: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := qviews.ShardID{VChannel: "p_1v0", ReplicaID: 1}
			response := twoStageResponse(id)
			response.Plan.WorkNodes = response.Plan.WorkNodes[:1]
			request := response.Plan.GetLegacySearchRequest()
			request.Nq, request.MetricType = 1, "IP"
			request.SerializedExprPlan = []byte("original")
			planClient := &fakeQueryPlanClient{plan: response.Plan, clientOptimizeInfo: response.ClientOptimizeInfo}
			first := newTestSearchChunk(5, []int64{1}, []float32{0.9})
			second := newTestSearchChunk(5, []int64{2}, []float32{0.8})
			if tc.empty {
				first = newTestSearchChunk(5, nil, nil)
			}
			first.ScannedRemoteBytes, first.ScannedTotalBytes = 3, 5
			first.CostAggregation = &internalpb.CostAggregation{ServiceTime: 2}
			recv := []fakeSearchStreamRecv{{chunk: first}}
			if !tc.empty {
				recv = append(recv, fakeSearchStreamRecv{chunk: second})
			}
			service := &fakeViewQueryServiceClient{
				searchOnViewStream: func(_ context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (searchreducestream.ReduceStream, error) {
					if tc.completed {
						require.Equal(t, []byte("optimized"), req.GetLegacyReq().GetSerializedExprPlan())
						require.True(t, req.GetExecutionOptions().GetEnableExprCache())
					} else {
						require.Equal(t, []byte("original"), req.GetLegacyReq().GetSerializedExprPlan())
						require.Nil(t, req.GetExecutionOptions())
					}
					return &fakeSearchStream{recv: recv}, nil
				},
			}
			filter := mockey.Mock((*fakeViewQueryServiceClient).SearchOnView).To(func(_ *fakeViewQueryServiceClient, _ context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
				require.True(t, req.GetExecutionOptions().GetFilterOnly())
				return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{
					Status: merr.Success(), SealedSegmentIDsSearched: []int64{10, 20}, FilterValidCounts: tc.counts,
					FeatureBits: 1, ScannedRemoteBytes: 7, ScannedTotalBytes: 9,
					CostAggregation: &internalpb.CostAggregation{ServiceTime: 4},
				}}, nil
			}).Build()
			defer filter.UnPatch()
			tune := mockey.Mock(optimizers.OptimizeSearchParams).To(func(_ context.Context, req *querypb.SearchRequest, _ optimizers.QueryHook, _ int, _ bool, _ func(int64) int64, _ string) (*querypb.SearchRequest, error) {
				req.Req.SerializedExprPlan = []byte("optimized")
				return req, nil
			}).Build()
			defer tune.UnPatch()
			client := newShardViewQueryClient(1, planClient, service)
			defer client.replicas.Close()
			streams, _, err := client.SearchStreams(t.Context(), &ShardSearchRequest{VChannel: id.VChannel, Req: request}, 1)
			require.NoError(t, err)
			require.Len(t, streams, 1)
			defer streams[0].Close()
			var bits uint64
			var remote, total, serviceTime int64
			chunks := 0
			for {
				chunk, err := streams[0].Recv()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				chunks++
				bits |= chunk.GetFeatureBits()
				remote += chunk.GetScannedRemoteBytes()
				total += chunk.GetScannedTotalBytes()
				serviceTime += chunk.GetCostAggregation().GetServiceTime()
			}
			if tc.empty {
				require.Equal(t, 1, chunks)
			} else {
				require.Equal(t, 2, chunks)
			}
			bit, _ := featureusage.ExecBit(featureusage.FeatureTwoStageSearch)
			require.Equal(t, tc.completed, bits&bit != 0)
			require.NotZero(t, bits&1)
			require.EqualValues(t, 10, remote)
			require.EqualValues(t, 14, total)
			require.EqualValues(t, 6, serviceTime)
		})
	}
}

func TestTwoStageSearchStreamsRetryDiscardsFilterMetadata(t *testing.T) {
	enableTwoStageSearch(t)
	setupTestReplicaRouting(t)
	id := qviews.ShardID{VChannel: "p_1v0", ReplicaID: 1}
	var plans, filters, streams int
	plan := mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).To(func(_ *fakeQueryPlanClient, _ context.Context, _ qviews.ShardID, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
		plans++
		response := twoStageResponse(id)
		response.Plan.WorkNodes = response.Plan.WorkNodes[:1]
		response.Plan.GetLegacySearchRequest().Nq = 1
		response.Plan.GetLegacySearchRequest().MetricType = "IP"
		return response, nil
	}).Build()
	defer plan.UnPatch()
	filter := mockey.Mock((*fakeViewQueryServiceClient).SearchOnView).To(func(_ *fakeViewQueryServiceClient, _ context.Context, _ qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
		filters++
		return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{
			Status: merr.Success(), SealedSegmentIDsSearched: []int64{10, 20}, FilterValidCounts: []int64{2, 3}, ScannedRemoteBytes: int64(filters * 7),
		}}, nil
	}).Build()
	defer filter.UnPatch()
	tune := mockey.Mock(optimizers.OptimizeSearchParams).To(func(_ context.Context, req *querypb.SearchRequest, _ optimizers.QueryHook, _ int, _ bool, _ func(int64) int64, _ string) (*querypb.SearchRequest, error) {
		return req, nil
	}).Build()
	defer tune.UnPatch()
	service := &fakeViewQueryServiceClient{searchOnViewStream: func(_ context.Context, _ qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (searchreducestream.ReduceStream, error) {
		streams++
		if streams == 1 {
			return &fakeSearchStream{recv: []fakeSearchStreamRecv{{err: viewerror.NewViewNotFound("retry")}}}, nil
		}
		return &fakeSearchStream{recv: []fakeSearchStreamRecv{{chunk: newTestSearchChunk(5, []int64{1}, []float32{0.9})}}}, nil
	}}
	client := newShardViewQueryClient(3, &fakeQueryPlanClient{}, service)
	defer client.replicas.Close()
	request := &internalpb.SearchRequest{Nq: 1, Topk: 5, MetricType: "IP", SearchType: internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER}
	results, _, err := client.SearchStreams(t.Context(), &ShardSearchRequest{VChannel: id.VChannel, Req: request}, 1024)
	require.NoError(t, err)
	defer results[0].Close()
	chunk, err := results[0].Recv()
	require.NoError(t, err)
	require.EqualValues(t, 14, chunk.GetScannedRemoteBytes())
	require.Equal(t, 2, plans)
	require.Equal(t, 2, filters)
	require.Equal(t, 2, streams)
}

func TestTwoStageSearchStreamsMergeMultipleWorkerCosts(t *testing.T) {
	enableTwoStageSearch(t)
	setupTestReplicaRouting(t)
	id := qviews.ShardID{VChannel: "p_1v0", ReplicaID: 1}
	response := twoStageResponse(id)
	response.Plan.GetLegacySearchRequest().Nq = 1
	response.Plan.GetLegacySearchRequest().MetricType = "IP"
	response.Plan.WorkNodes[1] = &viewpb.QueryPlanWorkNode{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: 12}}}
	filter := mockey.Mock((*fakeViewQueryServiceClient).SearchOnView).To(func(_ *fakeViewQueryServiceClient, _ context.Context, node qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
		id, bytes := int64(10), int64(7)
		if node.(qviews.QueryNode).ID == 12 {
			id, bytes = 20, 11
		}
		return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{
			Status: merr.Success(), SealedSegmentIDsSearched: []int64{id}, FilterValidCounts: []int64{1}, ScannedRemoteBytes: bytes,
		}}, nil
	}).Build()
	defer filter.UnPatch()
	tune := mockey.Mock(optimizers.OptimizeSearchParams).To(func(_ context.Context, req *querypb.SearchRequest, _ optimizers.QueryHook, _ int, _ bool, _ func(int64) int64, _ string) (*querypb.SearchRequest, error) {
		return req, nil
	}).Build()
	defer tune.UnPatch()
	service := &fakeViewQueryServiceClient{searchOnViewStream: func(_ context.Context, node qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (searchreducestream.ReduceStream, error) {
		id, bytes := int64(1), int64(3)
		if node.(qviews.QueryNode).ID == 12 {
			id, bytes = 2, 5
		}
		chunk := newTestSearchChunk(5, []int64{id}, []float32{0.9})
		chunk.ScannedRemoteBytes = bytes
		return &fakeSearchStream{recv: []fakeSearchStreamRecv{{chunk: chunk}}}, nil
	}}
	client := newShardViewQueryClient(1, &fakeQueryPlanClient{plan: response.Plan, clientOptimizeInfo: response.ClientOptimizeInfo}, service)
	defer client.replicas.Close()
	streams, _, err := client.SearchStreams(t.Context(), &ShardSearchRequest{VChannel: id.VChannel, Req: response.Plan.GetLegacySearchRequest()}, 1024)
	require.NoError(t, err)
	defer streams[0].Close()
	chunk, err := streams[0].Recv()
	require.NoError(t, err)
	require.EqualValues(t, 26, chunk.GetScannedRemoteBytes())
	bit, _ := featureusage.ExecBit(featureusage.FeatureTwoStageSearch)
	require.NotZero(t, chunk.GetFeatureBits()&bit)
}

func TestLegacyTwoStageSearchChannelCountWithRetry(t *testing.T) {
	enableTwoStageSearch(t)
	setupTestReplicaRouting(t)
	channels := []string{"p_1v0", "p_1v1"}
	var mu sync.Mutex
	var counts []int32
	optimize := mockey.Mock(optimizers.OptimizeSearchParams).To(func(_ context.Context, req *querypb.SearchRequest, _ optimizers.QueryHook, _ int, _ bool, _ func(int64) int64, _ string) (*querypb.SearchRequest, error) {
		mu.Lock()
		counts = append(counts, req.GetTotalChannelNum())
		mu.Unlock()
		return req, nil
	}).Build()
	defer optimize.UnPatch()

	for _, streaming := range []bool{false, true} {
		name := "batch"
		if streaming {
			name = "stream"
		}
		t.Run(name, func(t *testing.T) {
			counts = nil
			plans := make(map[string]int)
			failed := false
			planClient := &legacyPlanClient{getQueryPlan: func(_ context.Context, id qviews.ShardID, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
				mu.Lock()
				plans[id.VChannel]++
				mu.Unlock()
				response := twoStageResponse(id)
				response.Plan.WorkNodes = response.Plan.WorkNodes[:1]
				response.Plan.GetLegacySearchRequest().Nq = 1
				response.Plan.GetLegacySearchRequest().MetricType = "IP"
				return response, nil
			}}
			failOnce := func(channel string) bool {
				mu.Lock()
				defer mu.Unlock()
				if channel == channels[0] && !failed {
					failed = true
					return true
				}
				return false
			}
			service := &legacyServiceClient{
				searchOnView: func(_ context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
					if req.GetExecutionOptions().GetFilterOnly() {
						return &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{Status: merr.Success(), SealedSegmentIDsSearched: []int64{10, 20}, FilterValidCounts: []int64{2, 3}}}, nil
					}
					if failOnce(req.GetShardId().GetVchannel()) {
						return nil, viewerror.NewViewNotFound("retry")
					}
					return &viewpb.SearchOnViewResponse{LegacyResults: newTestSearchChunk(5, []int64{1}, []float32{0.9})}, nil
				},
				searchOnViewStream: func(_ context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (searchreducestream.ReduceStream, error) {
					if failOnce(req.GetShardId().GetVchannel()) {
						return &fakeSearchStream{recv: []fakeSearchStreamRecv{{err: viewerror.NewViewNotFound("retry")}}}, nil
					}
					id := int64(1)
					if req.GetShardId().GetVchannel() == channels[1] {
						id = 2
					}
					return &fakeSearchStream{recv: []fakeSearchStreamRecv{{chunk: newTestSearchChunk(5, []int64{id}, []float32{0.9})}}}, nil
				},
			}
			client := newLegacyClient(ViewQueryClientConfig{MaxRetries: 3}, planClient, service, &legacyResolver{vchannels: channels})
			defer client.shardClient.replicas.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			result, err := client.Search(ctx, &LegacySearchRequest{Req: &internalpb.SearchRequest{Nq: 1, Topk: 5, MetricType: "IP", SearchType: internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER}, AllowStreaming: streaming, ReduceStreamChunkBytes: 1024})
			require.NoError(t, err)
			if streaming {
				defer result.Stream.Close()
				for {
					_, err := result.Stream.Recv()
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
				}
			}
			require.Equal(t, map[string]int{channels[0]: 2, channels[1]: 1}, plans)
			require.Equal(t, []int32{2, 2, 2}, counts)
		})
	}
}
