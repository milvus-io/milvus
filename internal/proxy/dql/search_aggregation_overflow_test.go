package dql

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSearchAggregationPipelineIntegerOverflow(t *testing.T) {
	for _, tc := range []struct {
		name     string
		values   []int64
		withSum  bool
		overflow bool
		wantAvg  float64
	}{
		{"avg only", []int64{math.MaxInt64, 1}, false, false, math.Exp2(62)},
		{"sum and avg overflow", []int64{math.MaxInt64, 1}, true, true, 0},
		{"sum and avg cancellation", []int64{math.MaxInt64, 1, -1}, true, false, float64(math.MaxInt64) / 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			pk := &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}
			task := &SearchTask{
				ctx:           ctx,
				SearchRequest: &internalpb.SearchRequest{Nq: 1, MetricType: "IP"},
				schema: &schemaInfo{
					PkField: pk,
					CollectionSchema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
						pk,
						{FieldID: 101, Name: "g", DataType: schemapb.DataType_VarChar},
						{FieldID: 102, Name: "v", DataType: schemapb.DataType_Int64, Nullable: true},
					}},
				},
				request: &milvuspb.SearchRequest{SearchAggregation: &commonpb.SearchAggregationSpec{
					Fields: []string{"g"}, Size: 1, SearchSize: 8,
					Metrics: map[string]*commonpb.MetricAggSpec{
						"avg":   {Op: "avg", FieldName: "v"},
						"count": {Op: "count", FieldName: "v"},
					},
				}},
			}
			if tc.withSum {
				task.request.SearchAggregation.Metrics["sum"] = &commonpb.MetricAggSpec{Op: "sum", FieldName: "v"}
			}
			require.NoError(t, task.initSearchAggregation())
			task.Topk, task.GroupSize = task.aggCtx.DerivedTopK, task.aggCtx.DerivedGroupSize
			task.queryInfos = []*planpb.QueryInfo{{Topk: task.Topk, GroupSize: task.GroupSize, GroupByFieldIds: task.GroupByFieldIds, RoundDecimal: -1}}
			pipe, err := newSearchPipeline(task)
			require.NoError(t, err)

			// Shards contribute one row each to the same group. Include a NULL
			// and both encoded and in-process responses before cross-shard reduce.
			shards := make([]*internalpb.SearchResults, len(tc.values)+1)
			for i := range shards {
				field := multiGroupByTestLongField(102, nil)
				field.ValidData = []bool{i < len(tc.values)}
				if i < len(tc.values) {
					field.GetScalars().GetLongData().Data = []int64{tc.values[i]}
				}
				data := &schemapb.SearchResultData{
					NumQueries: 1, TopK: task.Topk, Topks: []int64{1},
					Ids:                &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{int64(i + 1)}}}},
					Scores:             []float32{float32(len(shards) - i)},
					FieldsData:         []*schemapb.FieldData{field},
					GroupByFieldValues: []*schemapb.FieldData{multiGroupByTestStringField(101, []string{"a"})},
				}
				shards[i] = &internalpb.SearchResults{MetricType: "IP", ResultData: data}
				if i == 0 {
					shards[i].SlicedBlob, err = proto.Marshal(data)
					require.NoError(t, err)
					shards[i].ResultData = nil
				}
			}
			result, _, err := pipe.Run(ctx, trace.SpanFromContext(ctx), shards, segcore.StorageCost{})
			if tc.overflow {
				require.Nil(t, result)
				require.ErrorIs(t, err, merr.ErrAggregateOverflow)
				require.Contains(t, err.Error(), `metric "sum"`)
				status := merr.Status(err)
				require.EqualValues(t, 2203, status.GetCode())
				require.False(t, status.GetRetriable())
				return
			}
			require.NoError(t, err)
			blob, err := proto.Marshal(result)
			require.NoError(t, err)
			decoded := &milvuspb.SearchResults{}
			require.NoError(t, proto.Unmarshal(blob, decoded))
			require.Len(t, decoded.GetResults().GetAggBuckets(), 1)
			bucket := decoded.GetResults().GetAggBuckets()[0]
			require.Equal(t, int64(len(shards)), bucket.GetCount())
			metrics := bucket.GetMetrics()
			require.IsType(t, &schemapb.MetricValue_DoubleVal{}, metrics["avg"].GetValue())
			require.Equal(t, tc.wantAvg, metrics["avg"].GetDoubleVal())
			require.Equal(t, int64(len(tc.values)), metrics["count"].GetIntVal())
			if tc.withSum {
				require.IsType(t, &schemapb.MetricValue_IntVal{}, metrics["sum"].GetValue())
				require.Equal(t, int64(math.MaxInt64), metrics["sum"].GetIntVal())
			}
		})
	}
}
