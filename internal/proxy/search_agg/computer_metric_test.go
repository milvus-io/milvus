package search_agg

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
)

func TestSearchAggregationTopHitsMetricDirection(t *testing.T) {
	for _, metricType := range []string{
		metric.L2, metric.HAMMING, metric.JACCARD, metric.SUBSTRUCTURE, metric.SUPERSTRUCTURE,
		metric.MaxSimL2, metric.MaxSimHamming, metric.MaxSimJaccard,
		metric.IP, metric.COSINE, metric.BM25, metric.MHJACCARD, metric.MaxSim, metric.MaxSimIP, metric.MaxSimCosine,
	} {
		t.Run(metricType, func(t *testing.T) {
			for _, tc := range []struct {
				name string
				sort []*commonpb.SortSpec
				dir  string
			}{
				{name: "default"},
				{name: "score default", sort: []*commonpb.SortSpec{{FieldName: "_score"}}},
				{name: "score whitespace default", sort: []*commonpb.SortSpec{{FieldName: "_score", Direction: " "}}},
				{name: "scalar tie", sort: []*commonpb.SortSpec{{FieldName: "stock"}}},
				{name: "score asc", sort: []*commonpb.SortSpec{{FieldName: "_score", Direction: "asc"}}, dir: "asc"},
				{name: "score desc", sort: []*commonpb.SortSpec{{FieldName: "_score", Direction: "desc"}}, dir: "desc"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					for _, nested := range []bool{false, true} {
						name := "single level"
						if nested {
							name = "parent and leaf truncation"
						}
						t.Run(name, func(t *testing.T) {
							spec := &commonpb.SearchAggregationSpec{
								Fields: []string{"brand"}, Size: 1,
								TopHits: &commonpb.TopHitsSpec{Size: 4, Sort: tc.sort},
								Metrics: map[string]*commonpb.MetricAggSpec{"sum_score": {Op: "sum", FieldName: "_score"}},
							}
							if nested {
								spec.TopHits.Size = 2
								spec.SubAggregation = &commonpb.SearchAggregationSpec{
									Fields: []string{"category"}, Size: 2,
									Order:   []*commonpb.OrderSpec{{Key: "_key", Direction: "asc"}},
									TopHits: &commonpb.TopHitsSpec{Size: 1, Sort: tc.sort},
								}
							}
							aggCtx, err := BuildSearchAggregationContext(spec, testCollectionSchema(), 1)
							require.NoError(t, err)
							data := &schemapb.SearchResultData{
								NumQueries: 1, Topks: []int64{4},
								Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{4, 2, 1, 3}}}},
								// These are the metric's raw values after proxy reduce. Equal
								// scores deliberately arrive in reverse PK order.
								Scores:     []float32{0.5, 0.1, 0.1, 0.9},
								FieldsData: []*schemapb.FieldData{testLongFieldData(104, []int64{7, 7, 7, 7})},
								GroupByFieldValues: []*schemapb.FieldData{
									testStringFieldData(101, []string{"X", "X", "X", "X"}),
									testStringFieldData(102, []string{"red", "red", "blue", "blue"}),
								},
							}
							result, err := NewSearchAggregationComputer(data, aggCtx, metricType).Compute(context.Background())
							require.NoError(t, err)
							require.Len(t, result, 1)
							require.Len(t, result[0], 1)
							bucket := result[0][0]
							desc := tc.dir == "desc" || (tc.dir == "" && metric.PositivelyRelated(metricType))
							wantPKs := []int64{1, 2, 4, 3}
							wantScores := []float32{0.1, 0.1, 0.5, 0.9}
							if desc {
								wantPKs = []int64{3, 4, 1, 2}
								wantScores = []float32{0.9, 0.5, 0.1, 0.1}
							}
							if nested {
								wantPKs, wantScores = wantPKs[:2], wantScores[:2]
								require.Equal(t, int64(2), aggCtx.DerivedGroupSize)
								require.Len(t, bucket.SubAggBuckets, 2)
								leafPKs := []int64{1, 2} // blue, red
								if desc {
									leafPKs = []int64{3, 4}
								}
								for i, sub := range bucket.SubAggBuckets {
									require.Len(t, sub.Hits, 1)
									require.Equal(t, leafPKs[i], sub.Hits[0].PK)
								}
							}
							require.Len(t, bucket.Hits, len(wantPKs))
							for i, hit := range bucket.Hits {
								require.Equal(t, wantPKs[i], hit.PK)
								require.Equal(t, wantScores[i], hit.Score)
							}
							require.Equal(t, int64(4), bucket.Count)
							require.InDelta(t, 1.6, bucket.Metrics["sum_score"], 1e-6)
							require.Equal(t, []float32{0.5, 0.1, 0.1, 0.9}, data.GetScores(), "ranking must not mutate scores")
						})
					}
				})
			}
		})
	}
}
