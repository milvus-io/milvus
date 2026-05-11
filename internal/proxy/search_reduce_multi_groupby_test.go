package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
)

// helper to build a single-field int64 FieldData with a FieldId set.
func multiGroupByTestLongField(fieldID int64, values []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId: fieldID,
		Type:    schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}},
		}},
	}
}

func multiGroupByTestStringField(fieldID int64, values []string) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId: fieldID,
		Type:    schemapb.DataType_VarChar,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}},
		}},
	}
}

func TestReduceMultiGroupBy_CrossShardSameComposite(t *testing.T) {
	// Two shards each return 2 rows for composite (brand=A, category=X).
	// groupSize=3 must keep only the top 3 by score across the union.
	shardA := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       5,
		Topks:      []int64{2},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		Scores:     []float32{0.9, 0.5},
		GroupByFieldValues: []*schemapb.FieldData{
			multiGroupByTestStringField(101, []string{"A", "A"}),
			multiGroupByTestStringField(102, []string{"X", "X"}),
		},
	}
	shardB := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       5,
		Topks:      []int64{2},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{3, 4}}}},
		Scores:     []float32{0.8, 0.6},
		GroupByFieldValues: []*schemapb.FieldData{
			multiGroupByTestStringField(101, []string{"A", "A"}),
			multiGroupByTestStringField(102, []string{"X", "X"}),
		},
	}

	ret, err := reduceSearchResultDataWithGroupBy(
		context.Background(),
		[]*schemapb.SearchResultData{shardA, shardB},
		1, 5, metric.IP, schemapb.DataType_Int64,
		0, 3, []int64{101, 102}, false,
	)
	require.NoError(t, err)
	require.NotNil(t, ret.GetResults())
	require.Equal(t, []int64{3}, ret.GetResults().GetTopks(), "groupSize=3 should produce 3 rows")
	require.ElementsMatch(t, []int64{1, 3, 4}, ret.GetResults().GetIds().GetIntId().GetData(),
		"top 3 by score: 0.9 (pk=1), 0.8 (pk=3), 0.6 (pk=4); drops 0.5 (pk=2)")
	require.Len(t, ret.GetResults().GetGroupByFieldValues(), 2)
}

func TestReduceMultiGroupBy_MultipleComposites(t *testing.T) {
	// Two distinct composites: (A, X) and (A, Y). topK=10 groupSize=2.
	shard := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       10,
		Topks:      []int64{4},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}}}},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6},
		GroupByFieldValues: []*schemapb.FieldData{
			multiGroupByTestStringField(101, []string{"A", "A", "A", "A"}),
			multiGroupByTestStringField(102, []string{"X", "Y", "X", "Y"}),
		},
	}

	ret, err := reduceSearchResultDataWithGroupBy(
		context.Background(),
		[]*schemapb.SearchResultData{shard},
		1, 10, metric.IP, schemapb.DataType_Int64,
		0, 2, []int64{101, 102}, false,
	)
	require.NoError(t, err)
	require.Equal(t, []int64{4}, ret.GetResults().GetTopks(), "two groups × 2 rows each = 4")
}

func TestReduceMultiGroupBy_TopKTruncation(t *testing.T) {
	// Three distinct composites but topK=2 → only top 2 groups kept.
	shard := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       2,
		Topks:      []int64{3},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
		Scores:     []float32{0.9, 0.8, 0.7},
		GroupByFieldValues: []*schemapb.FieldData{
			multiGroupByTestLongField(101, []int64{1, 2, 3}),
		},
	}

	ret, err := reduceSearchResultDataWithGroupBy(
		context.Background(),
		[]*schemapb.SearchResultData{shard},
		1, 2, metric.IP, schemapb.DataType_Int64,
		0, 1, []int64{101}, false,
	)
	require.NoError(t, err)
	require.Equal(t, []int64{2}, ret.GetResults().GetTopks(), "topK=2 → only top 2 groups kept (scores 0.9, 0.8)")
	require.ElementsMatch(t, []int64{1, 2}, ret.GetResults().GetIds().GetIntId().GetData())
}

// TestReduceMultiGroupBy_RegroupByBucketWhenNotAggregation pins the N>=2
// non-aggregation contract: emit order must be per-group-contiguous (matching
// the N=1 reducer shape). A single shard delivers rows in score-desc order:
//
//	pk=1 (A,X) score=0.9
//	pk=2 (B,Y) score=0.8
//	pk=3 (A,X) score=0.7
//	pk=4 (B,Y) score=0.6
//
// Pure score-desc walk order would interleave groups as [1,2,3,4]. The
// non-agg emit phase regroups so rows of (A,X) and (B,Y) each land
// contiguously — either [1,3,2,4] or [2,4,1,3] depending on group
// insertion order.
func TestReduceMultiGroupBy_RegroupByBucketWhenNotAggregation(t *testing.T) {
	shard := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       10,
		Topks:      []int64{4},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}}}},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6},
		GroupByFieldValues: []*schemapb.FieldData{
			multiGroupByTestStringField(101, []string{"A", "B", "A", "B"}),
			multiGroupByTestStringField(102, []string{"X", "Y", "X", "Y"}),
		},
	}

	ret, err := reduceSearchResultDataWithGroupBy(
		context.Background(),
		[]*schemapb.SearchResultData{shard},
		1, 10, metric.IP, schemapb.DataType_Int64,
		0, 2, []int64{101, 102}, false,
	)
	require.NoError(t, err)
	ids := ret.GetResults().GetIds().GetIntId().GetData()
	require.Len(t, ids, 4)
	// Walk-order insertion hits (A,X) first, then (B,Y) → emit order [1,3,2,4].
	require.Equal(t, []int64{1, 3, 2, 4}, ids,
		"non-agg N>=2 must regroup by bucket so same-group rows are contiguous")
}

// TestReduceMultiGroupBy_WalkOrderWhenAggregation pins the SearchAggregation
// contract: emit order is the pure score-desc walk order without regroup,
// because the downstream aggOp reorganizes by group itself. Uses the same
// shard as the non-agg test to make the ordering difference explicit.
func TestReduceMultiGroupBy_WalkOrderWhenAggregation(t *testing.T) {
	shard := &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       10,
		Topks:      []int64{4},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4}}}},
		Scores:     []float32{0.9, 0.8, 0.7, 0.6},
		GroupByFieldValues: []*schemapb.FieldData{
			multiGroupByTestStringField(101, []string{"A", "B", "A", "B"}),
			multiGroupByTestStringField(102, []string{"X", "Y", "X", "Y"}),
		},
	}

	ret, err := reduceSearchResultDataWithGroupBy(
		context.Background(),
		[]*schemapb.SearchResultData{shard},
		1, 10, metric.IP, schemapb.DataType_Int64,
		0, 2, []int64{101, 102}, true,
	)
	require.NoError(t, err)
	ids := ret.GetResults().GetIds().GetIntId().GetData()
	require.Equal(t, []int64{1, 2, 3, 4}, ids,
		"agg path must stream in score-desc walk order (no regroup at reduce stage)")
}
