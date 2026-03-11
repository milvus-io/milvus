package agg

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func init() {
	paramtable.Init()
}

func makeTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "category", DataType: schemapb.DataType_VarChar},
			{FieldID: 2, Name: "value", DataType: schemapb.DataType_Int64},
		},
	}
}

func makeGroupAggReducer() *GroupAggReducer {
	return NewGroupAggReducer(
		[]int64{1},
		[]*planpb.Aggregate{
			{Op: planpb.AggregateOp_sum, FieldId: 2},
		},
		-1,
		makeTestSchema(),
	)
}

// TestReduceNilResultReturnsError verifies that nil entries in the results slice
// return a proper error rather than panicking.
func TestReduceNilResultReturnsError(t *testing.T) {
	reducer := makeGroupAggReducer()

	validResult := NewAggregationResult([]*schemapb.FieldData{
		{
			Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{Data: []string{"a"}},
					},
				},
			},
		},
		{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{10}},
					},
				},
			},
		},
	}, 1)

	// A nil entry in the results slice should return an error, not panic.
	results := []*AggregationResult{validResult, nil}
	_, err := reducer.Reduce(context.Background(), results)
	assert.Error(t, err, "Reduce should return an error when a result is nil, not panic")
}

// TestBucketAccumulateErrorPropagated verifies that Accumulate returns an error
// when the column count of the incoming row does not match what is expected.
func TestBucketAccumulateErrorPropagated(t *testing.T) {
	bucket := NewBucket()

	row1 := NewRow([]*FieldValue{
		NewFieldValue("key1"),
		NewFieldValue(int64(10)),
	})
	bucket.AddRow(row1)

	// wrongRow has 3 columns but the bucket row has 2 and aggs has 1
	wrongRow := NewRow([]*FieldValue{
		NewFieldValue("key1"),
		NewFieldValue(int64(5)),
		NewFieldValue(int64(99)),
	})

	agg := &SumAggregate{fieldID: 2}
	err := bucket.Accumulate(wrongRow, 0, 1, []AggregateBase{agg})
	assert.Error(t, err, "Accumulate should return an error on column count mismatch")
}

// TestReduceWithValidGroupResults verifies that reduce correctly aggregates results
// from multiple shards.
func TestReduceWithValidGroupResults(t *testing.T) {
	reducer := makeGroupAggReducer()

	makeResult := func(key string, val int64) *AggregationResult {
		return NewAggregationResult([]*schemapb.FieldData{
			{
				Type: schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{Data: []string{key}},
						},
					},
				},
			},
			{
				Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{val}},
						},
					},
				},
			},
		}, 1)
	}

	results := []*AggregationResult{
		makeResult("a", 10),
		makeResult("a", 20),
		makeResult("b", 5),
	}

	out, err := reducer.Reduce(context.Background(), results)
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, int64(3), out.GetAllRetrieveCount())
}

// TestReduceEmptyResults verifies that reduce returns an empty result for empty input.
func TestReduceEmptyResults(t *testing.T) {
	reducer := makeGroupAggReducer()
	out, err := reducer.Reduce(context.Background(), []*AggregationResult{})
	require.NoError(t, err)
	require.NotNil(t, out)
}

// TestReduceSingleResult verifies that reduce returns the single input unchanged.
func TestReduceSingleResult(t *testing.T) {
	reducer := makeGroupAggReducer()
	singleResult := NewAggregationResult([]*schemapb.FieldData{
		{
			Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{Data: []string{"a"}},
					},
				},
			},
		},
		{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{42}},
					},
				},
			},
		},
	}, 1)

	out, err := reducer.Reduce(context.Background(), []*AggregationResult{singleResult})
	require.NoError(t, err)
	assert.Equal(t, singleResult, out)
}

// TestReduceSingleResultWithGroupLimit verifies that when Reduce receives
// a single result and groupLimit > 0, the result is truncated to groupLimit rows.
func TestReduceSingleResultWithGroupLimit(t *testing.T) {
	groupByFieldIds := []int64{101}
	aggregates := []*planpb.Aggregate{
		{Op: planpb.AggregateOp_count, FieldId: 102},
	}
	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "gk", DataType: schemapb.DataType_Int64},
			{FieldID: 102, Name: "cnt", DataType: schemapb.DataType_Int64},
		},
	}

	groupLimit := int64(3)
	reducer := NewGroupAggReducer(groupByFieldIds, aggregates, groupLimit, schema)

	// Build a single AggregationResult with 5 groups
	result := NewAggregationResult([]*schemapb.FieldData{
		{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5}},
					},
				},
			},
		},
		{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{10, 20, 30, 40, 50}},
					},
				},
			},
		},
	}, 5)

	reduced, err := reducer.Reduce(context.Background(), []*AggregationResult{result})
	assert.NoError(t, err)
	assert.NotNil(t, reduced)

	// Should be truncated to 3 groups
	keys := reduced.GetFieldDatas()[0].GetScalars().GetLongData().GetData()
	vals := reduced.GetFieldDatas()[1].GetScalars().GetLongData().GetData()
	assert.Equal(t, 3, len(keys))
	assert.Equal(t, 3, len(vals))
	assert.Equal(t, []int64{1, 2, 3}, keys)
	assert.Equal(t, []int64{10, 20, 30}, vals)
}

// TestReduceSingleResultWithoutGroupLimit verifies that when groupLimit <= 0,
// a single result is returned as-is without truncation.
func TestReduceSingleResultWithoutGroupLimit(t *testing.T) {
	groupByFieldIds := []int64{101}
	aggregates := []*planpb.Aggregate{
		{Op: planpb.AggregateOp_count, FieldId: 102},
	}
	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "gk", DataType: schemapb.DataType_Int64},
			{FieldID: 102, Name: "cnt", DataType: schemapb.DataType_Int64},
		},
	}

	// groupLimit = -1 means no limit (QN level)
	reducer := NewGroupAggReducer(groupByFieldIds, aggregates, -1, schema)

	result := NewAggregationResult([]*schemapb.FieldData{
		{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5}},
					},
				},
			},
		},
		{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{10, 20, 30, 40, 50}},
					},
				},
			},
		},
	}, 5)

	reduced, err := reducer.Reduce(context.Background(), []*AggregationResult{result})
	assert.NoError(t, err)
	assert.NotNil(t, reduced)

	// Should NOT be truncated
	keys := reduced.GetFieldDatas()[0].GetScalars().GetLongData().GetData()
	assert.Equal(t, 5, len(keys))
}

// TestReduceSingleResultGroupLimitLargerThanData verifies that when groupLimit
// exceeds the number of rows, the result is returned as-is.
func TestReduceSingleResultGroupLimitLargerThanData(t *testing.T) {
	groupByFieldIds := []int64{101}
	aggregates := []*planpb.Aggregate{
		{Op: planpb.AggregateOp_count, FieldId: 102},
	}
	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "gk", DataType: schemapb.DataType_Int64},
			{FieldID: 102, Name: "cnt", DataType: schemapb.DataType_Int64},
		},
	}

	reducer := NewGroupAggReducer(groupByFieldIds, aggregates, 100, schema)

	result := NewAggregationResult([]*schemapb.FieldData{
		{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}},
					},
				},
			},
		},
		{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}},
					},
				},
			},
		},
	}, 3)

	reduced, err := reducer.Reduce(context.Background(), []*AggregationResult{result})
	assert.NoError(t, err)
	keys := reduced.GetFieldDatas()[0].GetScalars().GetLongData().GetData()
	assert.Equal(t, 3, len(keys))
}

// TestReduceMultipleResultsAggValuesCorrect verifies that cross-shard merge
// produces correct aggregation values even with groupLimit truncation.
// The early-stop bug would have returned SUM=10 for key=1 instead of 110.
func TestReduceMultipleResultsAggValuesCorrect(t *testing.T) {
	groupByFieldIds := []int64{101}
	aggregates := []*planpb.Aggregate{
		{Op: planpb.AggregateOp_sum, FieldId: 102},
	}
	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "gk", DataType: schemapb.DataType_Int64},
			{FieldID: 102, Name: "agg", DataType: schemapb.DataType_Int64},
		},
	}

	// No limit — verify full merge correctness first
	reducer := NewGroupAggReducer(groupByFieldIds, aggregates, -1, schema)

	// Shard 1: groups {1:10, 2:20, 3:30}
	result1 := NewAggregationResult([]*schemapb.FieldData{
		{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}},
					},
				},
			},
		},
		{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}},
					},
				},
			},
		},
	}, 3)

	// Shard 2: groups {1:100, 4:40, 5:50}
	result2 := NewAggregationResult([]*schemapb.FieldData{
		{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1, 4, 5}},
					},
				},
			},
		},
		{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{100, 40, 50}},
					},
				},
			},
		},
	}, 3)

	reduced, err := reducer.Reduce(context.Background(), []*AggregationResult{result1, result2})
	assert.NoError(t, err)
	assert.NotNil(t, reduced)

	// Expected merged groups: {1:110, 2:20, 3:30, 4:40, 5:50}
	keys := reduced.GetFieldDatas()[0].GetScalars().GetLongData().GetData()
	vals := reduced.GetFieldDatas()[1].GetScalars().GetLongData().GetData()
	assert.Equal(t, 5, len(keys))
	assert.Equal(t, 5, len(vals))

	// Build map from key->val for order-independent comparison
	aggMap := make(map[int64]int64, len(keys))
	for i := range keys {
		aggMap[keys[i]] = vals[i]
	}
	assert.Equal(t, int64(110), aggMap[1], "key=1 should be SUM(10+100)=110")
	assert.Equal(t, int64(20), aggMap[2])
	assert.Equal(t, int64(30), aggMap[3])
	assert.Equal(t, int64(40), aggMap[4])
	assert.Equal(t, int64(50), aggMap[5])
}

// TestReduceMultipleResultsGroupLimitApplied verifies that groupLimit truncation
// happens AFTER full merge, preserving correct aggregation values.
func TestReduceMultipleResultsGroupLimitApplied(t *testing.T) {
	groupByFieldIds := []int64{101}
	aggregates := []*planpb.Aggregate{
		{Op: planpb.AggregateOp_sum, FieldId: 102},
	}
	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "gk", DataType: schemapb.DataType_Int64},
			{FieldID: 102, Name: "agg", DataType: schemapb.DataType_Int64},
		},
	}

	groupLimit := int64(2)
	reducer := NewGroupAggReducer(groupByFieldIds, aggregates, groupLimit, schema)

	// Shard 1: groups {1:10, 2:20, 3:30}
	result1 := NewAggregationResult([]*schemapb.FieldData{
		{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}},
					},
				},
			},
		},
		{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}},
					},
				},
			},
		},
	}, 3)

	// Shard 2: groups {1:100, 4:40, 5:50}
	result2 := NewAggregationResult([]*schemapb.FieldData{
		{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1, 4, 5}},
					},
				},
			},
		},
		{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{100, 40, 50}},
					},
				},
			},
		},
	}, 3)

	reduced, err := reducer.Reduce(context.Background(), []*AggregationResult{result1, result2})
	assert.NoError(t, err)
	assert.NotNil(t, reduced)

	// groupLimit=2 means at most 2 groups in output
	keys := reduced.GetFieldDatas()[0].GetScalars().GetLongData().GetData()
	vals := reduced.GetFieldDatas()[1].GetScalars().GetLongData().GetData()
	assert.Equal(t, 2, len(keys))
	assert.Equal(t, 2, len(vals))

	// Verify aggregation values are fully merged (not early-stopped).
	// If key=1 is in the output, its SUM must be 110 (10+100), not 10.
	aggMap := make(map[int64]int64, len(keys))
	for i := range keys {
		aggMap[keys[i]] = vals[i]
	}
	expectedFullMerge := map[int64]int64{1: 110, 2: 20, 3: 30, 4: 40, 5: 50}
	for k, v := range aggMap {
		assert.Equal(t, expectedFullMerge[k], v, "group key=%d should have correct merged value", k)
	}
}
