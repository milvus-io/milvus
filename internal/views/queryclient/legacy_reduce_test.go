package queryclient

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func shardTestIDs(ids ...int64) *schemapb.IDs {
	return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}}
}

func shardTestField(id int64, values ...int64) *schemapb.FieldData {
	return &schemapb.FieldData{FieldId: id, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}}}}}
}

func TestLegacyShardQueryReduction(t *testing.T) {
	paramtable.Init()
	id := qviews.ShardID{VChannel: "p_1v0", ReplicaID: 1}
	plan := ShardPlan{ShardID: id, Mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90}}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}, {FieldID: 101, Name: "value", DataType: schemapb.DataType_Int64}}}
	req := &LegacyQueryRequest{Req: &internalpb.RetrieveRequest{Limit: 10, OutputFieldsId: []int64{101}}, Schema: schema}
	collector := newLegacyQueryCollector()
	for _, input := range []*internalpb.RetrieveResults{
		{Status: merr.Success(), Ids: shardTestIDs(1, 2), FieldsData: []*schemapb.FieldData{shardTestField(1, 10, 10), shardTestField(101, 11, 22)}, AllRetrieveCount: 2, ScannedTotalBytes: 4, CostAggregation: &internalpb.CostAggregation{ResponseTime: 10, TotalRelatedDataSize: 20}},
		{Status: merr.Success(), Ids: shardTestIDs(1, 3), FieldsData: []*schemapb.FieldData{shardTestField(1, 20, 20), shardTestField(101, 99, 33)}, AllRetrieveCount: 2, ScannedTotalBytes: 6, HasMoreResult: true, CostAggregation: &internalpb.CostAggregation{ResponseTime: 5, TotalRelatedDataSize: 30}},
	} {
		require.NoError(t, collector.Add(id, &viewpb.QueryOnViewResponse{LegacyResults: input}))
	}
	results, err := collector.Reduce(t.Context(), req, []ShardPlan{plan})
	require.NoError(t, err)
	require.Len(t, results, 1)
	result := results[0]
	require.Equal(t, []int64{1, 2, 3}, result.GetIds().GetIntId().GetData())
	require.Equal(t, []int64{99, 22, 33}, result.GetFieldsData()[1].GetScalars().GetLongData().GetData())
	require.EqualValues(t, 4, result.AllRetrieveCount)
	require.EqualValues(t, 10, result.ScannedTotalBytes)
	require.EqualValues(t, 50, result.CostAggregation.TotalRelatedDataSize)
	require.True(t, result.HasMoreResult)
	require.EqualValues(t, 100, result.MvccTimestamp)
	collector.ResetShard(id)
	results, err = collector.Reduce(t.Context(), req, []ShardPlan{plan})
	require.NoError(t, err)
	require.Empty(t, results)
}

func TestLegacyShardSearchReduction(t *testing.T) {
	paramtable.Init()
	for _, advanced := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "hybrid"}[advanced], func(t *testing.T) {
			req := &internalpb.SearchRequest{Nq: 1, Topk: 3, MetricType: "IP", IsAdvanced: advanced}
			if advanced {
				req.SubReqs = []*internalpb.SubSearchRequest{{Nq: 1, Topk: 3, MetricType: "IP"}, {Nq: 1, Topk: 3, MetricType: "IP"}}
			}
			var inputs []*internalpb.SearchResults
			for i, ids := range [][]int64{{1, 2}, {1, 3}} {
				data := &schemapb.SearchResultData{NumQueries: 1, TopK: 3, Topks: []int64{2}, Ids: shardTestIDs(ids...), Scores: []float32{1, 0.5}}
				r := &internalpb.SearchResults{Status: merr.Success(), NumQueries: 1, TopK: 3, MetricType: "IP", ResultData: data, ScannedTotalBytes: 10, IsRecallEvaluation: i == 1, CostAggregation: &internalpb.CostAggregation{ResponseTime: int64(i + 1), TotalRelatedDataSize: 10}}
				if advanced {
					r.ResultData = nil
					r.IsAdvanced = true
					for j := int64(0); j < 2; j++ {
						r.SubResults = append(r.SubResults, &internalpb.SubSearchResults{ReqIndex: j, NumQueries: 1, TopK: 3, MetricType: "IP", ResultData: data})
					}
				}
				inputs = append(inputs, r)
			}
			result, err := reduceLegacyShardSearch(t.Context(), req, inputs)
			require.NoError(t, err)
			var data []*schemapb.SearchResultData
			if advanced {
				require.Len(t, result.SubResults, 2)
				for i, sub := range result.SubResults {
					require.EqualValues(t, i, sub.ReqIndex)
					decoded := &schemapb.SearchResultData{}
					if sub.ResultData != nil {
						decoded = sub.ResultData
					} else {
						require.NoError(t, proto.Unmarshal(sub.SlicedBlob, decoded))
					}
					data = append(data, decoded)
				}
			} else {
				decoded := result.ResultData
				if decoded == nil {
					decoded = &schemapb.SearchResultData{}
					require.NoError(t, proto.Unmarshal(result.SlicedBlob, decoded))
				}
				data = append(data, decoded)
			}
			for _, d := range data {
				require.Equal(t, []int64{1, 2, 3}, d.Ids.GetIntId().Data)
			}
			require.EqualValues(t, 20, result.ScannedTotalBytes)
			require.EqualValues(t, 20, result.CostAggregation.TotalRelatedDataSize)
			require.True(t, result.IsRecallEvaluation)
		})
	}
}

func TestLegacyExplicitMVCCSurvivesShardRetry(t *testing.T) {
	mockey.PatchConvey("explicit read frontiers survive failed attempts on both worker types", t, func() {
		id := qviews.ShardID{VChannel: "p_1v0", ReplicaID: 1}
		fixed := &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90}
		var attempts atomic.Int32
		var fail atomic.Bool
		fail.Store(true)
		planClient := &fakeQueryPlanClient{}
		service := &fakeViewQueryServiceClient{}
		mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).To(func(_ *fakeQueryPlanClient, _ context.Context, _ qviews.ShardID, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
			attempts.Add(1)
			require.True(t, proto.Equal(fixed, req.GetQueryPlanMvcc()))
			plan := legacyQueryPlan(id, qviews.NewQueryNode(1))
			plan.Mvcc = proto.Clone(req.GetQueryPlanMvcc()).(*viewpb.QueryPlanMVCC)
			plan.WorkNodes = append(plan.WorkNodes, &viewpb.QueryPlanWorkNode{Node: &viewpb.QueryPlanWorkNode_StreamingNode{StreamingNode: &viewpb.StreamingWorkNode{Pchannel: "p"}}})
			return &viewpb.GetQueryPlanResponse{Plan: plan}, nil
		}).Build()
		mockey.Mock((*fakeViewQueryServiceClient).QueryOnView).To(func(_ *fakeViewQueryServiceClient, _ context.Context, node qviews.WorkNode, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
			require.True(t, proto.Equal(fixed, req.Mvcc))
			require.Equal(t, legacyMVCCForNode(fixed, node), req.LegacyReq.MvccTimestamp)
			if node.NodeType() == qviews.NodeTypeQueryNode && fail.CompareAndSwap(true, false) {
				return nil, viewerror.NewViewInvalidated("retry")
			}
			return &viewpb.QueryOnViewResponse{LegacyResults: &internalpb.RetrieveResults{Status: merr.Success()}}, nil
		}).Build()
		client := NewLegacyViewQueryClient(ViewQueryClientConfig{MaxRetries: 3}, planClient, service, &legacyResolver{vchannels: []string{id.VChannel}})
		req := &LegacyQueryRequest{Req: &internalpb.RetrieveRequest{CollectionID: 1, ConsistencyLevel: commonpb.ConsistencyLevel_Strong}, MVCC: fixed}
		result, err := client.Legacy().Query(t.Context(), req)
		require.NoError(t, err)
		require.EqualValues(t, 2, attempts.Load())
		require.True(t, proto.Equal(fixed, result.Plans[0].Mvcc))
		require.Zero(t, req.Req.MvccTimestamp)
	})
}

func TestLegacyCostsPreserveZeroLatencyAndDoNotMutateInputs(t *testing.T) {
	first := &internalpb.CostAggregation{ServiceTime: 7, TotalNQ: 3, TotalRelatedDataSize: 10}
	result := mergeLegacyCosts([]*internalpb.CostAggregation{nil, first, {TotalRelatedDataSize: 20}})
	require.EqualValues(t, 7, result.ServiceTime)
	require.EqualValues(t, 3, result.TotalNQ)
	require.EqualValues(t, 30, result.TotalRelatedDataSize)
	require.EqualValues(t, 10, first.TotalRelatedDataSize)
}
