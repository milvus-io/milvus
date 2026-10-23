package dql

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/search_agg"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func aggregationWireForTest(sources []*schemapb.SearchResultData) []*internalpb.SearchResults {
	wire := make([]*internalpb.SearchResults, len(sources))
	for i, source := range sources {
		wire[i] = &internalpb.SearchResults{ResultData: source}
	}
	return wire
}

func aggregationIntIDsForTest(rows *search_agg.ReducedRows) []int64 {
	ids := make([]int64, len(rows.Rows))
	for i, ref := range rows.Rows {
		ids[i] = rows.Sources[ref.ResultIdx].GetIds().GetIntId().GetData()[ref.RowIdx]
	}
	return ids
}

type aggregationRowSnapshot struct {
	Score  float32
	Fields map[int64]any
	Key    []any
}

func aggregationFieldValuesForTest(data *schemapb.SearchResultData, row int) map[int64]any {
	values := make(map[int64]any, len(data.GetFieldsData()))
	for _, field := range data.GetFieldsData() {
		values[field.GetFieldId()] = typeutil.GetDataIterator(field)(row)
	}
	return values
}

// The ordinary materializing group-by reducer remains an independent oracle
// for which rows and values survive reduction. Its N>=2 emit order differs
// from SearchAggregation, so compare records by PK rather than position.
func materializedAggregationRowsForTest(data *schemapb.SearchResultData, groupFieldIDs []int64) map[any]aggregationRowSnapshot {
	snapshots := make(map[any]aggregationRowSnapshot, len(data.GetScores()))
	readers := make([]func(int) any, len(groupFieldIDs))
	for i, id := range groupFieldIDs {
		readers[i] = typeutil.GetDataIterator(reduce.FindGroupByFieldData(data, id, len(groupFieldIDs) == 1))
	}
	for row, score := range data.GetScores() {
		key := make([]any, len(readers))
		for i, read := range readers {
			key[i] = reduce.NormalizeScalar(read(row))
		}
		snapshots[typeutil.GetPK(data.GetIds(), int64(row))] = aggregationRowSnapshot{
			Score: score, Fields: aggregationFieldValuesForTest(data, row), Key: key,
		}
	}
	return snapshots
}

func selectedAggregationRowsForTest(rows *search_agg.ReducedRows) map[any]aggregationRowSnapshot {
	snapshots := make(map[any]aggregationRowSnapshot, len(rows.Rows))
	for i, ref := range rows.Rows {
		data := rows.Sources[ref.ResultIdx]
		score := data.GetScores()[ref.RowIdx]
		if rows.NegateScores {
			score = -score
		}
		snapshots[typeutil.GetPK(data.GetIds(), ref.RowIdx)] = aggregationRowSnapshot{
			Score: score, Fields: aggregationFieldValuesForTest(data, int(ref.RowIdx)),
			Key: rows.GroupKeys[rows.GroupIDs[i]],
		}
	}
	return snapshots
}

// Build row references from the independent materialized reducer output.
// Each row gets a distinct ID so the test does not rely on the production
// GroupID cache when checking aggregation results.
func materializedRowsForTest(data *schemapb.SearchResultData, groupFieldIDs []int64) *search_agg.ReducedRows {
	rows := &search_agg.ReducedRows{
		Sources: []*schemapb.SearchResultData{data}, Topks: data.GetTopks(),
		GroupFieldIDs: groupFieldIDs,
	}
	readers := make([]func(int) any, len(groupFieldIDs))
	for i, id := range groupFieldIDs {
		readers[i] = typeutil.GetDataIterator(reduce.FindGroupByFieldData(data, id, len(groupFieldIDs) == 1))
	}
	for row := range data.GetScores() {
		rows.Rows = append(rows.Rows, reduce.RowRef{ResultIdx: 0, RowIdx: int64(row)})
		rows.GroupIDs = append(rows.GroupIDs, row)
		key := make([]any, len(readers))
		for i, read := range readers {
			key[i] = reduce.NormalizeScalar(read(row))
		}
		rows.GroupKeys = append(rows.GroupKeys, key)
	}
	return rows
}

func normalizedAggBucketsForTest(buckets []*search_agg.AggBucketResult) []*search_agg.AggBucketResult {
	normalized := make([]*search_agg.AggBucketResult, len(buckets))
	for i, bucket := range buckets {
		copy := *bucket
		copy.SubAggBuckets = normalizedAggBucketsForTest(bucket.SubAggBuckets)
		normalized[i] = &copy
	}
	sort.Slice(normalized, func(i, j int) bool {
		return fmt.Sprint(normalized[i].Key) < fmt.Sprint(normalized[j].Key)
	})
	return normalized
}

func TestSearchAggregationReduceQueryBoundariesAndQuotas(t *testing.T) {
	for _, fields := range [][]int64{{101}, {101, 102}} {
		for _, aggregation := range []bool{false, true} {
			shards := []*schemapb.SearchResultData{
				{
					NumQueries: 3, TopK: 2, Topks: []int64{0, 4, 0}, AllSearchCount: 7,
					Ids: testSearchResultIDs(5, 7, 9, 11), Scores: []float32{9, 8, 7, 6},
					GroupByFieldValues: []*schemapb.FieldData{
						multiGroupByTestStringField(101, []string{"a", "a", "b", "c"}),
						multiGroupByTestStringField(102, []string{"x", "x", "x", "x"}),
					},
				},
				{
					NumQueries: 3, TopK: 2, Topks: []int64{1, 3, 1}, AllSearchCount: 11,
					Ids: testSearchResultIDs(20, 1, 3, 10, 30), Scores: []float32{9, 9, 8.5, 7, 9},
					GroupByFieldValues: []*schemapb.FieldData{
						multiGroupByTestStringField(101, []string{"a", "b", "a", "b", "a"}),
						multiGroupByTestStringField(102, []string{"x", "x", "x", "x", "x"}),
					},
				},
				{
					NumQueries: 3, TopK: 2, Topks: []int64{0, 0, 0}, AllSearchCount: 13,
					Ids: testSearchResultIDs(),
					GroupByFieldValues: []*schemapb.FieldData{
						multiGroupByTestStringField(101, nil), multiGroupByTestStringField(102, nil),
					},
				},
			}
			before := make([]proto.Message, len(shards))
			for i, shard := range shards {
				before[i] = proto.Clone(shard)
			}
			var topks []int64
			var allSearchCount int64
			var ids []int64
			if aggregation {
				rows, err := reduceSearchAggregationRows(context.Background(), aggregationWireForTest(shards),
					3, 2, 0, 2, fields, metric.IP, schemapb.DataType_Int64, 0, nil)
				require.NoError(t, err)
				topks, allSearchCount, ids = rows.Topks, rows.AllSearchCount, aggregationIntIDsForTest(rows)
			} else {
				result, err := reduceSearchResultDataWithGroupBy(context.Background(), shards,
					3, 2, metric.IP, schemapb.DataType_Int64, 0, 2, fields)
				require.NoError(t, err)
				topks = result.GetResults().GetTopks()
				allSearchCount = result.GetResults().GetAllSearchCount()
				ids = result.GetResults().GetIds().GetIntId().GetData()
			}
			require.Equal(t, []int64{1, 4, 1}, topks)
			require.Equal(t, int64(31), allSearchCount)
			want := []int64{20, 1, 9, 5, 3, 30}
			if len(fields) > 1 && aggregation {
				want = []int64{20, 1, 5, 3, 9, 30}
			}
			require.Equal(t, want, ids,
				"same-score PK order, full-group rejection, query boundaries, and legacy regrouping must survive")
			for i, shard := range shards {
				require.True(t, proto.Equal(before[i], shard), "source shard %d changed", i)
			}
		}
	}
}

func TestSearchAggregationReduceNonFiniteScoreOrdering(t *testing.T) {
	shards := make([]*schemapb.SearchResultData, 3)
	for i, scores := range [][]float32{{1, float32(math.NaN())}, {2, float32(math.Inf(-1))}, {float32(math.Inf(1)), 0}} {
		ids := []int64{int64(2 * i), int64(2*i + 1)}
		shards[i] = &schemapb.SearchResultData{
			NumQueries: 1, TopK: 6, Topks: []int64{2}, Ids: testSearchResultIDs(ids...), Scores: scores,
			GroupByFieldValues: []*schemapb.FieldData{
				multiGroupByTestLongField(101, ids), multiGroupByTestStringField(102, []string{"x", "x"}),
			},
		}
	}
	rows, err := reduceSearchAggregationRows(context.Background(), aggregationWireForTest(shards),
		1, 6, 0, 1, []int64{101, 102}, metric.IP, schemapb.DataType_Int64, 0, nil)
	require.NoError(t, err)
	// A NaN at the first nonempty shard head wins the existing scan, while a
	// NaN at a later shard cannot displace a finite head; it is not a total order.
	require.Equal(t, []int64{4, 2, 0, 1, 5, 3}, aggregationIntIDsForTest(rows))
}

func TestGroupByReduceManySourcesPreservesOffsetAndQuota(t *testing.T) {
	// Skip a, retain two rows each for b and c, and reject further rows from
	// full groups while other shards still have candidates.
	groups := [][]string{{"a", "b", "c"}, {"b", "b", "d"}, {"a", "c", "c"}, {"c", "d", "b"}}
	shards := make([]*schemapb.SearchResultData, len(groups))
	for shard, keys := range groups {
		ids := []int64{int64(shard + 1), int64(shard + 5), int64(shard + 9)}
		shards[shard] = &schemapb.SearchResultData{
			NumQueries: 1, TopK: 3, Topks: []int64{3}, Ids: testSearchResultIDs(ids...),
			Scores: []float32{float32(12 - shard), float32(8 - shard), float32(4 - shard)},
			GroupByFieldValues: []*schemapb.FieldData{
				multiGroupByTestStringField(101, keys), multiGroupByTestStringField(102, []string{"x", "x", "x"}),
			},
		}
	}
	for _, fields := range [][]int64{{101}, {101, 102}} {
		result, err := reduceSearchResultDataWithGroupBy(context.Background(), shards,
			1, 3, metric.IP, schemapb.DataType_Int64, 1, 2, fields)
		require.NoError(t, err)
		require.Equal(t, []int64{4}, result.Results.Topks)
		require.Equal(t, []int64{2, 5, 4, 7}, result.Results.GetIds().GetIntId().GetData())
		require.Equal(t, []string{"b", "b", "c", "c"}, result.Results.GroupByFieldValues[0].GetScalars().GetStringData().GetData())
	}
}

func TestSearchAggregationReduceOutputSizeBoundary(t *testing.T) {
	for _, nullable := range []bool{false, true} {
		vector := &schemapb.FieldData{
			FieldId: 104, Type: schemapb.DataType_FloatVector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  2,
				Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}},
			}},
		}
		if nullable {
			vector.GetVectors().ValidData = []bool{false, true}
			vector.GetVectors().GetFloatVector().Data = []float32{3, 4}
		}
		data := &schemapb.SearchResultData{
			NumQueries: 1, TopK: 2, Topks: []int64{2}, Ids: testSearchResultIDs(1, 2), Scores: []float32{2, 1},
			FieldsData: []*schemapb.FieldData{multiGroupByTestLongField(103, []int64{10, 20}), vector},
			GroupByFieldValues: []*schemapb.FieldData{
				multiGroupByTestLongField(101, []int64{1, 2}), multiGroupByTestStringField(102, []string{"x", "x"}),
			},
		}
		// Preserve the current AppendFieldData accounting, including its vector
		// slice-header charge, rather than introducing a new payload-size policy.
		accounted := typeutil.PrepareResultFieldData(data.FieldsData, 2)
		indexer := typeutil.NewFieldDataIdxComputer(data.FieldsData)
		var size int64
		for row := int64(0); row < 2; row++ {
			size += typeutil.AppendFieldData(accounted, data.FieldsData, row, indexer.Compute(row)...)
		}
		for _, fields := range [][]int64{{101}, {101, 102}} {
			for _, bound := range []int64{size - 1, size} {
				ret := &milvuspb.SearchResults{Results: &schemapb.SearchResultData{
					Ids: testSearchResultIDs(), FieldsData: typeutil.PrepareResultFieldData(data.FieldsData, 2),
				}}
				var err error
				if len(fields) == 1 {
					_, err = runSingleFieldGroupByHotLoop(context.Background(), ret, []*schemapb.SearchResultData{data},
						[][]int64{{0}}, []*typeutil.FieldDataIdxComputer{indexer}, 1, 0, 1, 2, 2, fields[0], bound, nil)
				} else {
					_, err = runMultiFieldGroupByHotLoop(context.Background(), ret, []*schemapb.SearchResultData{data},
						[][]int64{{0}}, []*typeutil.FieldDataIdxComputer{indexer}, 1, 0, 1, 2, 2, fields, bound, nil)
				}
				if bound < size {
					require.ErrorContains(t, err, "search results exceed the maxOutputSize Limit")
				} else {
					require.NoError(t, err)
					require.Equal(t, []int64{1, 2}, ret.Results.GetIds().GetIntId().GetData())
					require.True(t, proto.Equal(accounted[1], ret.Results.FieldsData[1]))
				}

				selected := &aggregationSelection{
					rows:  &search_agg.ReducedRows{},
					sizes: []aggregationRowSizer{newAggregationRowSizer(data.FieldsData)},
				}
				rowRet := &milvuspb.SearchResults{Results: &schemapb.SearchResultData{Ids: testSearchResultIDs()}}
				var selectedRefs []reduce.RowRef
				var rowErr error
				if len(fields) == 1 {
					selectedRefs, rowErr = runSingleFieldGroupByHotLoop(context.Background(), rowRet, []*schemapb.SearchResultData{data},
						[][]int64{{0}}, nil, 1, 0, 1, 2, 2, fields[0], bound, selected)
				} else {
					selectedRefs, rowErr = runMultiFieldGroupByHotLoop(context.Background(), rowRet, []*schemapb.SearchResultData{data},
						[][]int64{{0}}, nil, 1, 0, 1, 2, 2, fields, bound, selected)
				}
				if err != nil {
					require.EqualError(t, rowErr, err.Error())
				} else {
					require.NoError(t, rowErr)
					require.Equal(t, []reduce.RowRef{{ResultIdx: 0, RowIdx: 0}, {ResultIdx: 0, RowIdx: 1}}, selectedRefs)
					require.Equal(t, []int{0, 1}, selected.rows.GroupIDs)
					require.Equal(t, ret.Results.Topks, rowRet.Results.Topks)
				}
				require.Empty(t, rowRet.Results.FieldsData)
				require.Empty(t, rowRet.Results.Scores)
				require.Empty(t, rowRet.Results.GetIds().GetIntId().GetData())
			}
		}
	}
}

func TestSearchAggregationReduceEqualScoreAndPKKeepsSourceOrder(t *testing.T) {
	shards := make([]*schemapb.SearchResultData, 3)
	for i := range shards {
		shards[i] = &schemapb.SearchResultData{
			NumQueries: 1, TopK: 1, Topks: []int64{1},
			Ids: testSearchResultStringIDs("same"), Scores: []float32{1},
			FieldsData: []*schemapb.FieldData{multiGroupByTestLongField(103, []int64{int64(i)})},
			GroupByFieldValues: []*schemapb.FieldData{
				multiGroupByTestStringField(101, []string{"a"}), multiGroupByTestStringField(102, []string{"x"}),
			},
		}
	}
	rows, err := reduceSearchAggregationRows(context.Background(), aggregationWireForTest(shards),
		1, 1, 0, 2, []int64{101, 102}, metric.IP, schemapb.DataType_VarChar, 0, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{2}, rows.Topks)
	values := make([]int64, len(rows.Rows))
	for i, ref := range rows.Rows {
		values[i] = rows.Sources[ref.ResultIdx].GetFieldsData()[0].GetScalars().GetLongData().GetData()[ref.RowIdx]
	}
	require.Equal(t, []int64{0, 1}, values)
}

func TestSearchAggregationSerializeSharedFieldsKeepsPerHitValues(t *testing.T) {
	hits := []*search_agg.HitResult{
		{PK: int64(1), Score: .9, Fields: map[int64]any{300: "first", 100: int64(10), 200: nil}},
		{PK: int64(2), Score: .8, Fields: map[int64]any{200: true, 300: "second", 100: nil}},
	}
	names := map[int64]string{100: "price", 200: "stock", 300: "label"}
	for run := 0; run < 2; run++ {
		result := serializeAggHits(hits, names)
		require.Len(t, result, 2)
		for i, hit := range result {
			require.Equal(t, int64(i+1), hit.GetIntPk())
			require.Len(t, hit.GetFields(), 3)
			for j, id := range []int64{100, 200, 300} {
				require.Equal(t, id, hit.Fields[j].GetFieldId())
				require.Equal(t, names[id], hit.Fields[j].GetFieldName())
			}
		}
		require.Equal(t, int64(10), result[0].Fields[0].GetIntVal())
		require.Nil(t, result[0].Fields[1].GetValue())
		require.Equal(t, "first", result[0].Fields[2].GetStringVal())
		require.Nil(t, result[1].Fields[0].GetValue())
		require.True(t, result[1].Fields[1].GetBoolVal())
		require.Equal(t, "second", result[1].Fields[2].GetStringVal())
	}
}

func TestSearchAggregationRowsMatchesMaterializedReduction(t *testing.T) {
	for _, pkType := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_VarChar} {
		for _, metricType := range []string{metric.IP, metric.L2} {
			for _, depth := range []int{1, 3} {
				metrics := map[string]search_agg.MetricSpec{
					"sum": {Op: "sum", FieldID: 104, FieldType: schemapb.DataType_Int64},
					"avg": {Op: "avg", FieldID: 104, FieldType: schemapb.DataType_Int64},
				}
				levels := []search_agg.LevelContext{
					{
						OwnFieldIDs: []int64{101}, Size: 3, Metrics: metrics,
						TopHits: &search_agg.TopHitsConfig{Size: 3, Sort: []search_agg.SortCriterion{{FieldID: 104, Dir: "desc"}}},
					},
					{OwnFieldIDs: []int64{102}, Size: 2, Metrics: metrics, TopHits: &search_agg.TopHitsConfig{Size: 2}},
					{OwnFieldIDs: []int64{103}, Size: 2, Metrics: metrics},
				}
				aggCtx, err := search_agg.NewContext(3, levels[:depth], []int64{101, 104, 105}, nil)
				require.NoError(t, err)
				sources := make([]*schemapb.SearchResultData, 4)
				wire := make([]*internalpb.SearchResults, len(sources))
				before := make([]proto.Message, len(sources))
				for shard := range sources {
					const rowsPerQuery = 20
					data := &schemapb.SearchResultData{NumQueries: 3, TopK: aggCtx.DerivedTopK, Topks: []int64{rowsPerQuery, 0, rowsPerQuery}, AllSearchCount: int64(shard + 1)}
					ids, prices := make([]int64, 0, rowsPerQuery*2), make([]int64, 0, rowsPerQuery*2)
					keys := [3][]int64{}
					var priceValid, vectorValid []bool
					var vectors []float32
					for qi := 0; qi < 3; qi += 2 {
						for row := 0; row < rowsPerQuery; row++ {
							ids = append(ids, int64(qi*1000+row*4+shard))
							prices = append(prices, int64(qi*100+row*7+shard))
							priceValid = append(priceValid, row%5 != 0)
							vectorValid = append(vectorValid, row%3 != 0)
							if row%3 != 0 {
								vectors = append(vectors, float32(qi), float32(row), float32(shard), 1)
							}
							keys[0] = append(keys[0], int64(row%3))
							keys[1] = append(keys[1], int64(row/3%2))
							keys[2] = append(keys[2], int64(row/6%2))
							score := float32(1000 - row*4 - shard)
							if metricType == metric.L2 {
								score = -float32(1 + row*4 + shard)
							}
							data.Scores = append(data.Scores, score)
						}
					}
					data.Ids = testSearchResultIDs(ids...)
					if pkType == schemapb.DataType_VarChar {
						values := make([]string, len(ids))
						for i, id := range ids {
							values[i] = strconv.FormatInt(id, 10)
						}
						data.Ids = testSearchResultStringIDs(values...)
					}
					price := multiGroupByTestLongField(104, prices)
					price.GetScalars().ValidData = priceValid
					data.FieldsData = []*schemapb.FieldData{price, {
						FieldId: 105, Type: schemapb.DataType_FloatVector,
						Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
							Dim: 4, ValidData: vectorValid,
							Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: vectors}},
						}},
					}}
					for i := range keys {
						field := multiGroupByTestLongField(int64(101+i), keys[i])
						if depth == 3 && i > 0 {
							valid := make([]bool, len(keys[i]))
							for row := range valid {
								valid[row] = row%(7+i) != 0
							}
							field.GetScalars().ValidData = valid
						}
						data.GroupByFieldValues = append(data.GroupByFieldValues, field)
					}
					if depth == 1 && shard == 2 {
						// Rolling-upgrade legacy wire: singular group column may have
						// no FieldId stamp, including when projected in top_hits.
						data.GroupByFieldValue = data.GroupByFieldValues[0]
						data.GroupByFieldValue.FieldId = 0
						data.GroupByFieldValues = nil
					}
					sources[shard] = data
					wire[shard] = &internalpb.SearchResults{ResultData: data, MetricType: metricType}
					before[shard] = proto.Clone(data)
				}
				materialized, err := reduceSearchResultDataWithGroupBy(context.Background(), sources, 3, aggCtx.DerivedTopK, metricType,
					pkType, 0, aggCtx.DerivedGroupSize, aggCtx.AllGroupByFieldIDs())
				require.NoError(t, err)
				materializedRows := materializedRowsForTest(materialized.Results, aggCtx.AllGroupByFieldIDs())
				want, err := search_agg.NewSearchAggregationComputerFromRows(materializedRows, aggCtx, metricType).Compute(context.Background())
				require.NoError(t, err)
				// Exercise both decode branches in the row-reference path.
				wire[0].SlicedBlob, err = proto.Marshal(wire[0].ResultData)
				require.NoError(t, err)
				wire[0].ResultData = nil
				selected, err := reduceSearchAggregationRows(context.Background(), wire, 3, aggCtx.DerivedTopK, 0,
					aggCtx.DerivedGroupSize, aggCtx.AllGroupByFieldIDs(), metricType, pkType, 0, nil)
				require.NoError(t, err)
				require.Equal(t, int64(10), selected.AllSearchCount)
				require.Equal(t, materialized.Results.Topks, selected.Topks)
				require.Len(t, selected.GroupIDs, len(selected.Rows))
				require.Same(t, sources[1], selected.Sources[1], "in-process source columns must not be copied")
				require.Equal(t, materializedAggregationRowsForTest(materialized.Results, aggCtx.AllGroupByFieldIDs()),
					selectedAggregationRowsForTest(selected),
					"materialized and row-reference reduction must retain the same records and values")
				oldTrace, newTrace := newPipelineTrace(true), newPipelineTrace(true)
				oldTrace.TraceMsg(searchReduceOp, opMsg{reducedMsgKey: []*milvuspb.SearchResults{materialized}})
				newTrace.TraceMsg(searchReduceOp, opMsg{reducedMsgKey: selected})
				require.Equal(t, oldTrace.String(), newTrace.String())
				computer := search_agg.NewSearchAggregationComputerFromRows(selected, aggCtx, metricType)
				var first [][]*search_agg.AggBucketResult
				for run := 0; run < 2; run++ {
					got, err := computer.Compute(context.Background())
					require.NoError(t, err)
					require.Len(t, got, 3)
					require.NotEmpty(t, got[0])
					require.Empty(t, got[1])
					require.NotEmpty(t, got[2])
					for qi := range got {
						require.Equal(t, normalizedAggBucketsForTest(want[qi]), normalizedAggBucketsForTest(got[qi]),
							"materialized and row-reference reductions must produce equivalent buckets")
					}
					if run == 0 {
						first = got
					} else {
						require.Equal(t, first, got, "reusing a computer must not retain bucket state")
					}
				}
				for i, source := range sources {
					require.True(t, proto.Equal(before[i], source), "source %d was mutated", i)
				}
			}
		}
	}
}

func TestSearchAggregationRowsPreservesReduceErrorsAndEmptyQueries(t *testing.T) {
	for _, scenario := range []string{"nq", "topk", "scores", "group", "decode", "no_sources", "empty_shard", "pk_type"} {
		t.Run(scenario, func(t *testing.T) {
			data := &schemapb.SearchResultData{
				NumQueries: 2, TopK: 2, Topks: []int64{1, 0},
				Ids: testSearchResultIDs(1), Scores: []float32{1}, AllSearchCount: 7,
				GroupByFieldValues: []*schemapb.FieldData{multiGroupByTestLongField(101, []int64{3})},
			}
			wire := []*internalpb.SearchResults{{ResultData: data}}
			pkType := schemapb.DataType_Int64
			switch scenario {
			case "nq":
				data.NumQueries = 1
			case "topk":
				data.TopK = 1
			case "scores":
				data.Scores = nil
			case "group":
				data.GroupByFieldValues = nil
			case "decode":
				wire = []*internalpb.SearchResults{{SlicedBlob: []byte{0xff}}}
			case "no_sources":
				wire = nil
			case "empty_shard":
				data.Ids, data.Scores, data.Topks = testSearchResultIDs(), nil, []int64{0, 0}
				data.GroupByFieldValues = []*schemapb.FieldData{multiGroupByTestLongField(101, nil)}
			case "pk_type":
				pkType = schemapb.DataType_Float
			}
			info := &planpb.QueryInfo{GroupByFieldIds: []int64{101}, GroupSize: 1}
			want, oldErr := reduceResults(context.Background(), wire, 2, 2, 0, metric.IP, pkType, info, false, 0, nil)
			got, newErr := reduceSearchAggregationRows(context.Background(), wire, 2, 2, 0, 1, []int64{101}, metric.IP, pkType, 0, nil)
			if oldErr != nil {
				require.EqualError(t, newErr, oldErr.Error())
				require.Equal(t, merr.Code(oldErr), merr.Code(newErr))
				return
			}
			require.NoError(t, newErr)
			require.Empty(t, got.Rows)
			require.Equal(t, want.Results.Topks, got.Topks)
			require.Equal(t, want.Results.AllSearchCount, got.AllSearchCount)
		})
	}
}

// The row-reference path must enforce the historical output-size limit even
// though it no longer calls AppendFieldData to materialize each selected row.
func TestAggregationRowSizerPreservesAppendFieldDataCharge(t *testing.T) {
	fixtures := []string{
		`{"type":"Bool","scalars":{"boolData":{"data":[true,false,true]}}}`,
		`{"type":"Int32","scalars":{"intData":{"data":[1,0,3]}}}`,
		`{"type":"Int64","scalars":{"longData":{"data":[1,0,3]}}}`,
		`{"type":"Float","scalars":{"floatData":{"data":[1,0,3]}}}`,
		`{"type":"Double","scalars":{"doubleData":{"data":[1,0,3]}}}`,
		`{"type":"VarChar","scalars":{"stringData":{"data":["short","","a longer string"]}}}`,
		`{"type":"Array","scalars":{"arrayData":{"elementType":"Int64","data":[{"longData":{"data":[1,2]}},{"longData":{}},{"longData":{"data":[3]}}]}}}`,
		`{"type":"JSON","scalars":{"jsonData":{"data":["e30=","","WzEsMl0="]}}}`,
		`{"type":"Timestamptz","scalars":{"timestamptzData":{"data":[1,0,3]}}}`,
		`{"type":"Geometry","scalars":{"geometryData":{"data":["AQID","","BA=="]}}}`,
		`{"type":"Geometry","scalars":{"geometryWktData":{"data":["POINT (1 2)","","POINT (3 4)"]}}}`,
		`{"type":"FloatVector","vectors":{"dim":2,"floatVector":{"data":[1,2,3,4,5,6]}}}`,
		`{"type":"BinaryVector","vectors":{"dim":8,"binaryVector":"AQID"}}`,
		`{"type":"Float16Vector","vectors":{"dim":1,"float16Vector":"AQIDBAUG"}}`,
		`{"type":"BFloat16Vector","vectors":{"dim":1,"bfloat16Vector":"AQIDBAUG"}}`,
		`{"type":"Int8Vector","vectors":{"dim":1,"int8Vector":"AQID"}}`,
		`{"type":"SparseFloatVector","vectors":{"dim":8,"sparseFloatVector":{"dim":8,"contents":["","AAAAAAAAAAA=","AQAAAAAAAAACAAAAAAAAAA=="]}}}`,
	}
	for _, fixture := range fixtures {
		original := &schemapb.FieldData{}
		require.NoError(t, protojson.Unmarshal([]byte(fixture), original))
		original.FieldId = 100
		for _, nullable := range []bool{false, true} {
			field := proto.Clone(original).(*schemapb.FieldData)
			if nullable {
				// Nullable scalar storage retains placeholders; nullable vector
				// storage is compact, and the final value below may be unused.
				typeutil.SetFieldDataValidData(field, []bool{true, false, true})
			}
			fields := []*schemapb.FieldData{field}
			sizer := newAggregationRowSizer(fields)
			indices := typeutil.NewFieldDataIdxComputer(fields)
			dst := typeutil.PrepareResultFieldData(fields, 5)
			for _, row := range []int64{2, 0, 1, 2, 0} {
				want := typeutil.AppendFieldData(dst, fields, row, indices.Compute(row)...)
				require.Equal(t, want, sizer.size(int(row)), "fixture=%s nullable=%t row=%d", fixture, nullable, row)
			}
		}
	}
}
