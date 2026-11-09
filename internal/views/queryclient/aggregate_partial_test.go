package queryclient_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/internal/proxy/dql"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Exercise the actual QueryView worker -> shard -> Proxy reduction chain.
// Ordinary fieldless worker responses are filtered before collection; the
// unpopulated shard plan still must survive without fabricating an aggregate row.
func TestAggregatePartialAcrossEmptyShard(t *testing.T) {
	paramtable.Init()
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64}}}
	for _, tc := range []struct {
		name, op string
		workers  [][]int64
		want     float64
	}{
		{"positive_min", "min", [][]int64{{150}, {250}}, 150},
		{"negative_max", "max", [][]int64{{-20}, {-30}}, -20},
		{"real_zero_min", "min", [][]int64{{150}, {0}}, 0},
		{"real_zero_max", "max", [][]int64{{-20}, {0}}, 0},
		{"sum", "sum", [][]int64{{30}, {90}}, 120},
		{"count", "count", [][]int64{{2}, {4}}, 6},
		{"avg", "avg", [][]int64{{30, 2}, {90, 4}}, 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			operators, err := agg.NewAggregate(tc.op, 100, tc.op+"(pk)", schemapb.DataType_Int64)
			require.NoError(t, err)
			outputMap, err := agg.NewAggregationFieldMap([]string{tc.op + "(pk)"}, nil, operators)
			require.NoError(t, err)
			request := &queryclient.LegacyQueryRequest{Schema: schema, Req: &internalpb.RetrieveRequest{Aggregates: agg.AggregatesToPB(operators)}}
			pipeline, err := dql.NewQueryPipeline(schema, -1, 0, reduce.IReduceNoOrder, nil, nil, request.Req.Aggregates, outputMap, nil)
			require.NoError(t, err)
			plans := []queryclient.ShardPlan{
				{ShardID: qviews.ShardID{VChannel: "empty_1v0", ReplicaID: 1}, Mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 100}, ExecutionSummary: &viewpb.ExecutionSummary{GlobalOptimizer: &viewpb.GlobalOptimizerExecutionSummary{Flags: 1 << viewpb.GlobalOptimizerExecutionSummary_PrunedByScalarFiltering}}},
				{ShardID: qviews.ShardID{VChannel: "values_1v0", ReplicaID: 1}, Mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 110}},
			}
			for _, emptyFirst := range []bool{true, false} {
				collector := queryclient.NewLegacyQueryCollectorForTest()
				for _, values := range tc.workers {
					response := &internalpb.RetrieveResults{ScannedTotalBytes: 10, CostAggregation: &internalpb.CostAggregation{TotalRelatedDataSize: 20}}
					for _, value := range values {
						response.FieldsData = append(response.FieldsData, &schemapb.FieldData{Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{value}}}}}})
					}
					require.NoError(t, collector.Add(plans[1].ShardID, &viewpb.QueryOnViewResponse{LegacyResults: response}))
				}
				ordered := plans
				emptyIndex := 0
				if !emptyFirst {
					ordered = []queryclient.ShardPlan{plans[1], plans[0]}
					emptyIndex = 1
				}
				shards, err := collector.Reduce(t.Context(), request, ordered)
				require.NoError(t, err)
				require.Len(t, shards, 2)
				empty := shards[emptyIndex]
				require.Equal(t, uint64(100), empty.MvccTimestamp)
				bit, _ := featureusage.ExecBit(featureusage.FeatureSegmentPrune)
				require.Equal(t, bit, empty.FeatureBits&bit)
				for i, field := range empty.FieldsData {
					require.Len(t, field.GetScalars().GetLongData().GetData(), 1)
					require.Equal(t, []bool{request.Req.Aggregates[i].GetOp() == planpb.AggregateOp_count}, field.GetValidData())
				}
				values := shards[1-emptyIndex]
				require.Equal(t, int64(20), values.ScannedTotalBytes)
				require.Equal(t, int64(40), values.CostAggregation.TotalRelatedDataSize)
				// Protobuf roundtrip checks the existing RPC representation of Empty.
				for i, shard := range shards {
					encoded, err := proto.Marshal(shard)
					require.NoError(t, err)
					shards[i] = &internalpb.RetrieveResults{}
					require.NoError(t, proto.Unmarshal(encoded, shards[i]))
				}
				result, err := pipeline.Execute(t.Context(), shards)
				require.NoError(t, err)
				field := result.FieldsData[0]
				if tc.op == "avg" {
					require.Equal(t, []float64{tc.want}, field.GetScalars().GetDoubleData().GetData())
				} else {
					require.Equal(t, []int64{int64(tc.want)}, field.GetScalars().GetLongData().GetData())
				}
			}
		})
	}
}
