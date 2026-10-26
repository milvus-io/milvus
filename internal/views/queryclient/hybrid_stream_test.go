package queryclient

import (
	"context"
	"io"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/searchutil"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func hybridStreamRequest() *internalpb.SearchRequest {
	return &internalpb.SearchRequest{
		CollectionID: 100, IsAdvanced: true, Nq: 1, Topk: 1,
		SubReqs: []*internalpb.SubSearchRequest{
			{Nq: 1, Topk: 3, Offset: 1, MetricType: "IP", FieldId: 101, SearchType: internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER},
			{Nq: 1, Topk: 2, MetricType: "COSINE", FieldId: 102, SearchType: internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER},
		},
	}
}

func TestHybridStreamsShareOptimizedShardPlan(t *testing.T) {
	for _, skipped := range []bool{false, true} {
		t.Run(map[bool]string{false: "both", true: "skipped"}[skipped], func(t *testing.T) {
			request := hybridStreamRequest()
			shard := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
			plan := legacySearchPlan(shard, qviews.NewQueryNode(11))
			plan.WorkNodes = append(plan.WorkNodes, &viewpb.QueryPlanWorkNode{
				Node: &viewpb.QueryPlanWorkNode_StreamingNode{StreamingNode: &viewpb.StreamingWorkNode{Pchannel: "by-dev-rootcoord-dml_0"}},
			})
			planned := proto.Clone(request).(*internalpb.SearchRequest)
			planned.SubReqs[0].SerializedExprPlan = []byte("optimized first plan")
			planned.SubReqs[1].Skip = skipped
			plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: planned}
			planClient := &fakeQueryPlanClient{plan: plan}
			var opened []*fakeSearchStream
			service := &fakeViewQueryServiceClient{searchOnViewStream: func(_ context.Context, node qviews.WorkNode, req *viewpb.SearchOnViewRequest) (searchutil.ReduceStream, error) {
				require.False(t, req.GetLegacyReq().GetIsAdvanced())
				require.Empty(t, req.GetLegacyReq().GetSubReqs())
				require.True(t, proto.Equal(plan.GetMvcc(), req.GetMvcc()))
				require.True(t, proto.Equal(plan.GetVersion(), req.GetVersion()))
				require.Equal(t, legacyMVCCForNode(plan.GetMvcc(), node), req.GetLegacyReq().GetMvccTimestamp())
				require.EqualValues(t, 20, req.GetStreamChunkBytes())
				if req.GetLegacyReq().GetFieldId() == 101 {
					require.Equal(t, []byte("optimized first plan"), req.GetLegacyReq().GetSerializedExprPlan())
					require.EqualValues(t, 3, req.GetLegacyReq().GetTopk())
				} else {
					require.False(t, skipped)
					require.EqualValues(t, 102, req.GetLegacyReq().GetFieldId())
				}
				stream := &fakeSearchStream{}
				opened = append(opened, stream)
				return stream, nil
			}}
			streams, retainedPlan, err := newShardViewQueryClient(1, planClient, service).SearchStreams(context.Background(), &ShardSearchRequest{Req: request, VChannel: shard.VChannel}, 20)
			require.NoError(t, err)
			require.Len(t, streams, 2)
			require.Equal(t, 1, planClient.planReqCount)
			require.True(t, planClient.planReq.GetLegacySearchRequest().GetIsAdvanced())
			require.Equal(t, shard, retainedPlan.ShardID)
			require.Len(t, opened, map[bool]int{false: 4, true: 2}[skipped])
			for _, stream := range streams {
				_, err := stream.Recv()
				require.ErrorIs(t, err, io.EOF)
				require.NoError(t, stream.Close())
			}
			for _, stream := range opened {
				require.Equal(t, 1, stream.closeCount())
			}
			require.Empty(t, request.SubReqs[0].GetSerializedExprPlan())
		})
	}
}

func TestHybridStreamSetupFailureClosesEarlierSubSearches(t *testing.T) {
	request := hybridStreamRequest()
	shard := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	plan := legacySearchPlan(shard, qviews.NewQueryNode(11))
	plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: request}
	child := &fakeSearchStream{}
	failure := errors.New("second sub-search failed to open")
	service := &fakeViewQueryServiceClient{searchOnViewStream: func(_ context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (searchutil.ReduceStream, error) {
		if req.GetLegacyReq().GetFieldId() == 102 {
			return nil, failure
		}
		return child, nil
	}}
	streams, _, err := newShardViewQueryClient(1, &fakeQueryPlanClient{plan: plan}, service).SearchStreams(context.Background(), &ShardSearchRequest{Req: request, VChannel: shard.VChannel}, 20)
	require.ErrorIs(t, err, failure)
	require.Nil(t, streams)
	require.Equal(t, 1, child.closeCount())
}

func TestSearchStreamsPreserveRangeAndIterativeFilterPlans(t *testing.T) {
	for _, hybrid := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "hybrid"}[hybrid], func(t *testing.T) {
			request := hybridStreamRequest()
			infos := []*planpb.QueryInfo{
				{SearchParams: `{"radius":10,"range_filter":1}`},
				{SearchParams: `{"ef":128}`, Hints: "iterative_filter"},
			}
			for i, sub := range request.SubReqs {
				plan := &planpb.PlanNode{Node: &planpb.PlanNode_VectorAnns{VectorAnns: &planpb.VectorANNS{QueryInfo: infos[i]}}}
				var err error
				sub.SerializedExprPlan, err = proto.Marshal(plan)
				require.NoError(t, err)
				sub.SearchType = internalpb.SearchType_DEFAULT
			}
			if !hybrid {
				request.IsAdvanced = false
				request.SerializedExprPlan = request.SubReqs[0].SerializedExprPlan
				request.SubReqs = nil
			}
			shard := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
			plan := legacySearchPlan(shard, qviews.NewQueryNode(11))
			plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: request}
			calls := 0
			service := &fakeViewQueryServiceClient{searchOnViewStream: func(_ context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (searchutil.ReduceStream, error) {
				actual := &planpb.PlanNode{}
				require.NoError(t, proto.Unmarshal(req.GetLegacyReq().GetSerializedExprPlan(), actual))
				require.True(t, proto.Equal(infos[calls], actual.GetVectorAnns().GetQueryInfo()))
				require.Equal(t, internalpb.SearchType_DEFAULT, req.GetLegacyReq().GetSearchType())
				calls++
				return &fakeSearchStream{}, nil
			}}
			streams, _, err := newShardViewQueryClient(1, &fakeQueryPlanClient{plan: plan}, service).SearchStreams(context.Background(), &ShardSearchRequest{Req: request, VChannel: shard.VChannel}, 20)
			require.NoError(t, err)
			require.Equal(t, map[bool]int{false: 1, true: 2}[hybrid], calls)
			for _, stream := range streams {
				require.NoError(t, stream.Close())
			}
		})
	}
}

func TestLegacyHybridStreamsReduceEachSubSearchAcrossVChannels(t *testing.T) {
	request := hybridStreamRequest()
	plans := make(map[string]*viewpb.QueryPlan)
	channels := []string{"by-dev-rootcoord-dml_0_100v0", "by-dev-rootcoord-dml_1_100v1"}
	for _, channel := range channels {
		shard := qviews.ShardID{ReplicaID: 1, VChannel: channel}
		plan := legacySearchPlan(shard, qviews.NewQueryNode(11))
		plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: proto.Clone(request).(*internalpb.SearchRequest)}
		plans[channel] = plan
	}
	var calls atomic.Int64
	service := &legacyServiceClient{searchOnViewStream: func(ctx context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (searchutil.ReduceStream, error) {
		calls.Add(1)
		ids, scores := []int64{1, 3, 5}, []float32{.9, .7, .5}
		if req.GetShardId().GetVchannel() == channels[1] {
			ids, scores = []int64{2, 4, 6}, []float32{.8, .6, .4}
		}
		if req.GetLegacyReq().GetFieldId() == 102 {
			for i := range ids {
				ids[i] += 10
			}
		}
		chunk := newTestSearchChunk(req.GetLegacyReq().GetTopk(), ids, scores)
		if limit := int(req.GetLegacyReq().GetTopk()); len(ids) > limit {
			chunk = newTestSearchChunk(int64(limit), ids[:limit], scores[:limit])
		}
		chunk.MetricType = req.GetLegacyReq().GetMetricType()
		chunk.FeatureBits = 8
		chunks, err := searchutil.SplitSearchResult(chunk, 20)
		if err != nil {
			return nil, err
		}
		stream := &fakeSearchStream{ctx: ctx}
		for _, chunk := range chunks {
			stream.recv = append(stream.recv, fakeSearchStreamRecv{chunk: chunk})
		}
		return stream, nil
	}}
	client := NewLegacyViewQueryClient(ViewQueryClientConfig{EnableReduceStream: true, ReduceStreamChunkBytes: 20}, &legacyPlanClient{plans: plans}, service, &legacyResolver{vchannels: channels})
	result, err := client.Legacy().Search(context.Background(), &LegacySearchRequest{Req: request, AllowStreaming: true})
	require.NoError(t, err)
	require.Nil(t, result.Stream)
	require.Empty(t, result.Results)
	require.Len(t, result.SubStreams, 2)
	require.Len(t, result.Plans, 2)
	require.EqualValues(t, 4, calls.Load())
	for i, stream := range result.SubStreams {
		var ids []int64
		var bits uint64
		for {
			chunk, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
			ids = append(ids, chunk.GetResultData().GetIds().GetIntId().GetData()...)
			bits |= chunk.GetFeatureBits()
		}
		require.Equal(t, [][]int64{{1, 2, 3}, {11, 12}}[i], ids)
		require.EqualValues(t, 8, bits)
		require.NoError(t, stream.Close())
	}
}
