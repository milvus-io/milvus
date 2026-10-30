package queryclient

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/internal/util/searchutil/optimizers"
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
		11: {FeatureBits: 1, ScannedRemoteBytes: 7, ScannedTotalBytes: 9, CostAggregation: &internalpb.CostAggregation{ServiceTime: 4}},
	}}
	response := &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{ScannedRemoteBytes: 3, ScannedTotalBytes: 5}}
	stage.mergeFeatures(qviews.NewQueryNode(11), response)
	require.EqualValues(t, 10, response.LegacyResults.ScannedRemoteBytes)
	require.EqualValues(t, 14, response.LegacyResults.ScannedTotalBytes)
	require.EqualValues(t, 4, response.LegacyResults.CostAggregation.ServiceTime)
	other := &viewpb.SearchOnViewResponse{LegacyResults: &internalpb.SearchResults{}}
	stage.mergeFeatures(qviews.NewQueryNode(12), other)
	require.Zero(t, other.LegacyResults.ScannedTotalBytes)
	stage.mergeFeatures(qviews.NewQueryNode(11), &viewpb.SearchOnViewResponse{})
}
