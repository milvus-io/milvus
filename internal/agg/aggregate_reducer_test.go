package agg

import (
	"context"
	"fmt"
	"strings"
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

	validResult := &AggregationResult{
		fieldDatas: []*schemapb.FieldData{
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
		},
		allRetrieveCount: 1,
	}

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
		return &AggregationResult{
			fieldDatas: []*schemapb.FieldData{
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
			},
			allRetrieveCount: 1,
		}
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
	singleResult := &AggregationResult{
		fieldDatas: []*schemapb.FieldData{
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
		},
		allRetrieveCount: 1,
	}

	out, err := reducer.Reduce(context.Background(), []*AggregationResult{singleResult})
	require.NoError(t, err)
	assert.Equal(t, singleResult, out)
}

// buildTestSchema creates a simple schema with an INT64 groupBy field and an INT64 agg field.
func buildTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "group_field", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "agg_field", DataType: schemapb.DataType_Int64},
		},
	}
}

// buildAggResult creates an AggregationResult with N distinct groups.
// Each group has group key = startKey+i and count = 1.
func buildAggResult(startKey int64, numGroups int) *AggregationResult {
	groupKeys := make([]int64, numGroups)
	counts := make([]int64, numGroups)
	for i := 0; i < numGroups; i++ {
		groupKeys[i] = startKey + int64(i)
		counts[i] = 1
	}
	return NewAggregationResult([]*schemapb.FieldData{
		{
			Type:      schemapb.DataType_Int64,
			FieldName: "group_field",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: groupKeys},
					},
				},
			},
		},
		{
			Type:      schemapb.DataType_Int64,
			FieldName: "agg_field",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: counts},
					},
				},
			},
		},
	}, int64(numGroups))
}

func TestGroupAggReducer_MaxGroupByGroupsExceeded(t *testing.T) {
	maxGroups := int64(10)
	paramtable.Get().Save(paramtable.Get().CommonCfg.GroupByMaxGroups.Key, fmt.Sprintf("%d", maxGroups))
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.GroupByMaxGroups.Key)

	schema := buildTestSchema()
	aggregates := []*planpb.Aggregate{
		{Op: planpb.AggregateOp_count, FieldId: 101},
	}
	reducer := NewGroupAggReducer([]int64{100}, aggregates, -1, schema)

	// Two results each with 10 distinct groups (20 total > 10 limit)
	results := []*AggregationResult{
		buildAggResult(0, 10),
		buildAggResult(10, 10),
	}

	_, err := reducer.Reduce(context.Background(), results)
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "too many groups"))
}

func TestGroupAggReducer_MaxGroupByGroupsExactlyAtLimit(t *testing.T) {
	maxGroups := int64(10)
	paramtable.Get().Save(paramtable.Get().CommonCfg.GroupByMaxGroups.Key, fmt.Sprintf("%d", maxGroups))
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.GroupByMaxGroups.Key)

	schema := buildTestSchema()
	aggregates := []*planpb.Aggregate{
		{Op: planpb.AggregateOp_count, FieldId: 101},
	}
	reducer := NewGroupAggReducer([]int64{100}, aggregates, -1, schema)

	// Exactly 10 groups = limit, should succeed
	// Use 2 results to force cross-segment merge path (single result fast-returns)
	results := []*AggregationResult{
		buildAggResult(0, 5),
		buildAggResult(5, 5),
	}

	result, err := reducer.Reduce(context.Background(), results)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestGroupAggReducer_MaxGroupByGroupsJustOverLimit(t *testing.T) {
	maxGroups := int64(10)
	paramtable.Get().Save(paramtable.Get().CommonCfg.GroupByMaxGroups.Key, fmt.Sprintf("%d", maxGroups))
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.GroupByMaxGroups.Key)

	schema := buildTestSchema()
	aggregates := []*planpb.Aggregate{
		{Op: planpb.AggregateOp_count, FieldId: 101},
	}
	reducer := NewGroupAggReducer([]int64{100}, aggregates, -1, schema)

	// 6 + 5 = 11 distinct groups > 10 limit, should fail
	// Need 2 results to trigger cross-segment merge path (single result fast-returns)
	results := []*AggregationResult{
		buildAggResult(0, 6),
		buildAggResult(6, 5),
	}

	_, err := reducer.Reduce(context.Background(), results)
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "too many groups"))
}
