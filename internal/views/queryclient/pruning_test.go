package queryclient

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestEmptyPlanPreservesFeatureAndSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name   string
		pruned bool
	}{{"unpruned", false}, {"pruned", true}} {
		t.Run(tc.name, func(t *testing.T) {
			plan := ShardPlan{ShardID: qviews.ShardID{VChannel: "p_1v0", ReplicaID: 1}, Mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90}}
			bit, _ := featureusage.ExecBit(featureusage.FeatureSegmentPrune)
			var expectedBit uint64
			if tc.pruned {
				expectedBit = bit
				plan.ExecutionSummary = &viewpb.ExecutionSummary{GlobalOptimizer: &viewpb.GlobalOptimizerExecutionSummary{Flags: 1 << viewpb.GlobalOptimizerExecutionSummary_PrunedByScalarFiltering}}
			}
			query, err := newLegacyQueryCollector().Reduce(t.Context(), &LegacyQueryRequest{Req: &internalpb.RetrieveRequest{}}, []ShardPlan{plan})
			require.NoError(t, err)
			require.Len(t, query, 1)
			require.Equal(t, expectedBit, query[0].FeatureBits&bit)
			require.Equal(t, uint64(100), query[0].MvccTimestamp)
			for _, req := range []*internalpb.SearchRequest{
				{Nq: 2, Topk: 3, MetricType: "L2"},
				{IsAdvanced: true, SubReqs: []*internalpb.SubSearchRequest{{Nq: 2, Topk: 3, MetricType: "L2"}, {Nq: 2, Topk: 4, MetricType: "IP"}}},
			} {
				search, err := newLegacySearchCollector().Reduce(t.Context(), req, []ShardPlan{plan})
				require.NoError(t, err)
				require.Len(t, search, 1)
				require.Equal(t, expectedBit, search[0].FeatureBits&bit)
				require.Equal(t, uint64(100), search[0].ChannelsMvcc["p_1v0"])
				if req.IsAdvanced {
					require.Len(t, search[0].SubResults, 2)
				}
			}
		})
	}
}

func TestEmptyCountResults(t *testing.T) {
	for _, tc := range []struct {
		name   string
		pruned bool
	}{{"unpruned", false}, {"pruned", true}} {
		t.Run(tc.name, func(t *testing.T) {
			paramtable.Init()
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64}}}
			req := &LegacyQueryRequest{Schema: schema, Req: &internalpb.RetrieveRequest{Aggregates: []*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 100}}}}
			plan := ShardPlan{ShardID: qviews.ShardID{VChannel: "p_1v0", ReplicaID: 1}}
			bit, _ := featureusage.ExecBit(featureusage.FeatureSegmentPrune)
			var expectedBit uint64
			if tc.pruned {
				expectedBit = bit
				plan.ExecutionSummary = &viewpb.ExecutionSummary{GlobalOptimizer: &viewpb.GlobalOptimizerExecutionSummary{Flags: 1 << viewpb.GlobalOptimizerExecutionSummary_PrunedByScalarFiltering}}
			}
			for _, input := range [][]*internalpb.RetrieveResults{
				nil,
				{{}},
				{{}, {FieldsData: []*schemapb.FieldData{{Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{5}}}}}}}}},
			} {
				collector := newLegacyQueryCollector()
				for _, r := range input {
					require.NoError(t, collector.Add(plan.ShardID, &viewpb.QueryOnViewResponse{LegacyResults: r}))
				}
				result, err := collector.Reduce(t.Context(), req, []ShardPlan{plan})
				require.NoError(t, err)
				require.Len(t, result, 1)
				require.Len(t, result[0].GetFieldsData(), 1)
				if len(input) == 2 {
					require.Equal(t, []int64{5}, result[0].FieldsData[0].GetScalars().GetLongData().GetData())
				} else {
					require.Equal(t, []int64{0}, result[0].FieldsData[0].GetScalars().GetLongData().GetData())
				}
				require.Equal(t, expectedBit, result[0].FeatureBits&bit)
			}
		})
	}
}

// Enum definitions are descriptors, not wire fields. Only set bitmap words and
// their message envelopes consume bytes.
func TestExecutionSummaryWireSize(t *testing.T) {
	for _, tc := range []struct {
		name  string
		flags uint32
		size  int
	}{
		{"absent", 0, 0},
		{"partition", 1 << viewpb.GlobalOptimizerExecutionSummary_PrunedByPartitionFiltering, 6},
		{"scalar", 1 << viewpb.GlobalOptimizerExecutionSummary_PrunedByScalarFiltering, 6},
		{"vector", 1 << viewpb.GlobalOptimizerExecutionSummary_PrunedByVectorFiltering, 6},
		{"combined", 7, 6},
		{"all_bits", ^uint32(0), 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := &viewpb.GetQueryPlanResponse{}
			if tc.flags != 0 {
				response.ExecutionSummary = &viewpb.ExecutionSummary{GlobalOptimizer: &viewpb.GlobalOptimizerExecutionSummary{Flags: tc.flags}}
			}
			data, err := proto.Marshal(response)
			require.NoError(t, err)
			require.Len(t, data, tc.size)
			decoded := &viewpb.GetQueryPlanResponse{}
			require.NoError(t, proto.Unmarshal(data, decoded))
			require.Equal(t, tc.flags, decoded.GetExecutionSummary().GetGlobalOptimizer().GetFlags())
			if tc.flags == 0 {
				require.Nil(t, decoded.GetExecutionSummary())
			}
		})
	}
}

func TestExecutionSummaryFeatureMapping(t *testing.T) {
	bit, ok := featureusage.ExecBit(featureusage.FeatureSegmentPrune)
	require.True(t, ok)
	require.Zero(t, executionSummaryFeatureBits(nil))
	for _, tc := range []struct {
		name     string
		flags    uint32
		expected uint64
	}{
		{"partition", 1 << viewpb.GlobalOptimizerExecutionSummary_PrunedByPartitionFiltering, 0},
		{"scalar", 1 << viewpb.GlobalOptimizerExecutionSummary_PrunedByScalarFiltering, bit},
		{"vector", 1 << viewpb.GlobalOptimizerExecutionSummary_PrunedByVectorFiltering, bit},
		{"combined", 7, bit},
		{"unknown", 1 << 31, 0},
		{"known_and_unknown", 1<<31 | 1<<viewpb.GlobalOptimizerExecutionSummary_PrunedByScalarFiltering, bit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			summary := &viewpb.ExecutionSummary{GlobalOptimizer: &viewpb.GlobalOptimizerExecutionSummary{Flags: tc.flags}}
			require.Equal(t, tc.expected, executionSummaryFeatureBits(summary))
		})
	}
}

func TestEmptyNonCountAggregateResults(t *testing.T) {
	paramtable.Init()
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
		{FieldID: 101, Name: "value", DataType: schemapb.DataType_Float},
	}}
	for _, tc := range []struct {
		name       string
		op         planpb.AggregateOp
		fieldID    int64
		resultType schemapb.DataType
	}{
		{"sum_integer", planpb.AggregateOp_sum, 100, schemapb.DataType_Int64},
		{"sum_float", planpb.AggregateOp_sum, 101, schemapb.DataType_Double},
		{"avg_integer", planpb.AggregateOp_avg, 100, schemapb.DataType_Int64},
		{"min_float", planpb.AggregateOp_min, 101, schemapb.DataType_Float},
		{"max_integer", planpb.AggregateOp_max, 100, schemapb.DataType_Int64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &LegacyQueryRequest{Schema: schema, Req: &internalpb.RetrieveRequest{Aggregates: []*planpb.Aggregate{{Op: tc.op, FieldId: tc.fieldID}}}}
			// Match Proxy's decomposition: execution receives sum/count, never avg.
			if tc.op == planpb.AggregateOp_avg {
				operators, err := agg.NewAggregate("avg", tc.fieldID, "avg(pk)", schemapb.DataType_Int64)
				require.NoError(t, err)
				req.Req.Aggregates = nil
				for _, operator := range operators {
					req.Req.Aggregates = append(req.Req.Aggregates, operator.ToPB())
				}
			}
			plan := ShardPlan{ShardID: qviews.ShardID{VChannel: "p_1v0", ReplicaID: 1}, Mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90}, ExecutionSummary: &viewpb.ExecutionSummary{GlobalOptimizer: &viewpb.GlobalOptimizerExecutionSummary{Flags: 1 << viewpb.GlobalOptimizerExecutionSummary_PrunedByScalarFiltering}}}
			// No workers and fieldless responses must agree, including the
			// multi-worker reduction path. The wire carries null aggregate states.
			for _, workerCount := range []int{0, 1, 2} {
				collector := newLegacyQueryCollector()
				inputs := make([]*internalpb.RetrieveResults, workerCount)
				for i := range inputs {
					inputs[i] = &internalpb.RetrieveResults{}
					require.NoError(t, collector.Add(plan.ShardID, &viewpb.QueryOnViewResponse{LegacyResults: inputs[i]}))
				}
				results, err := collector.Reduce(t.Context(), req, []ShardPlan{plan})
				require.NoError(t, err)
				require.Len(t, results, 1)
				require.Len(t, results[0].GetFieldsData(), len(req.Req.Aggregates))
				if tc.op == planpb.AggregateOp_avg {
					require.Equal(t, schemapb.DataType_Int64, results[0].FieldsData[1].Type)
					require.Equal(t, []int64{0}, results[0].FieldsData[1].GetScalars().GetLongData().GetData())
				}
				field := results[0].FieldsData[0]
				require.Equal(t, tc.resultType, field.GetType())
				accessor, err := agg.NewFieldAccessor(field.GetType())
				require.NoError(t, err)
				accessor.SetVals(field)
				require.Equal(t, 1, accessor.RowCount())
				require.True(t, accessor.IsNullAt(0))
				require.Equal(t, []bool{false}, typeutil.GetFieldDataValidData(field))
				require.Equal(t, uint64(100), results[0].GetMvccTimestamp())
				bit, _ := featureusage.ExecBit(featureusage.FeatureSegmentPrune)
				require.Equal(t, bit, results[0].FeatureBits&bit)
				for _, input := range inputs {
					require.Empty(t, input.FieldsData, "normalization must not mutate worker responses")
				}
			}
		})
	}
}
