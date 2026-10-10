package dql

import (
	"context"
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
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
)

func TestSearchAggregationPipelineMetricDirection(t *testing.T) {
	for _, metricType := range []string{metric.L2, metric.HAMMING, metric.IP, metric.COSINE} {
		t.Run(metricType, func(t *testing.T) {
			pk := &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}
			task := &SearchTask{
				ctx: context.Background(),
				// MetricType is intentionally absent: the resolved metric is returned
				// by querynode, and must be shared by reduction and aggregation.
				SearchRequest: &internalpb.SearchRequest{Nq: 1},
				schema: &schemaInfo{
					PkField: pk,
					CollectionSchema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
						pk,
						{FieldID: 101, Name: "brand", DataType: schemapb.DataType_VarChar},
						{FieldID: 102, Name: "color", DataType: schemapb.DataType_VarChar},
					}},
				},
				request: &milvuspb.SearchRequest{SearchAggregation: &commonpb.SearchAggregationSpec{
					Fields: []string{"brand"}, Size: 1,
					TopHits: &commonpb.TopHitsSpec{Size: 2},
					SubAggregation: &commonpb.SearchAggregationSpec{
						Fields: []string{"color"}, Size: 2,
						Order:   []*commonpb.OrderSpec{{Key: "_key", Direction: "asc"}},
						TopHits: &commonpb.TopHitsSpec{Size: 1},
					},
				}},
			}
			require.NoError(t, task.initSearchAggregation())
			task.Topk = task.aggCtx.DerivedTopK
			task.queryInfos = []*planpb.QueryInfo{{
				Topk: task.Topk, GroupSize: task.aggCtx.DerivedGroupSize,
				GroupByFieldIds: task.GroupByFieldIds, RoundDecimal: -1,
			}}
			pipe, err := newSearchPipeline(task)
			require.NoError(t, err)

			rawScores := []float32{0.1, 0.2, 0.3, 0.9}
			if metric.PositivelyRelated(metricType) {
				rawScores = []float32{0.9, 0.8, 0.3, 0.1}
			}
			shards := make([]*internalpb.SearchResults, 2)
			for i := range shards {
				scores := []float32{rawScores[i], rawScores[i+2]}
				if !metric.PositivelyRelated(metricType) {
					// segcore normalizes distances to larger-is-better before reduce.
					for j := range scores {
						scores[j] = -scores[j]
					}
				}
				shards[i] = &internalpb.SearchResults{
					MetricType: metricType,
					ResultData: &schemapb.SearchResultData{
						NumQueries: 1, TopK: task.Topk, Topks: []int64{2},
						Ids:    &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{int64(i + 1), int64(i + 3)}}}},
						Scores: scores,
						GroupByFieldValues: []*schemapb.FieldData{
							multiGroupByTestStringField(101, []string{"X", "X"}),
							multiGroupByTestStringField(102, []string{"red", "blue"}),
						},
					},
				}
			}
			// Exercise both the encoded wire path and the in-process result path.
			blob, err := proto.Marshal(shards[0].ResultData)
			require.NoError(t, err)
			shards[0].SlicedBlob = blob
			shards[0].ResultData = nil

			result, _, err := pipe.Run(context.Background(), trace.SpanFromContext(context.Background()), shards, segcore.StorageCost{})
			require.NoError(t, err)
			require.Equal(t, []int64{1}, result.GetResults().GetAggTopks())
			buckets := result.GetResults().GetAggBuckets()
			require.Len(t, buckets, 1)
			require.Equal(t, int64(4), buckets[0].GetCount())
			hits := buckets[0].GetHits()
			require.Len(t, hits, 2)
			for i, hit := range hits {
				require.Equal(t, int64(i+1), hit.GetIntPk())
				require.Equal(t, rawScores[i], hit.GetScore())
			}
			sub := buckets[0].GetSubGroups()
			require.Len(t, sub, 2)
			for i, wantPK := range []int64{3, 1} { // blue, red
				require.Len(t, sub[i].GetHits(), 1)
				require.Equal(t, wantPK, sub[i].GetHits()[0].GetIntPk())
				require.Equal(t, rawScores[wantPK-1], sub[i].GetHits()[0].GetScore())
			}
		})
	}
}
