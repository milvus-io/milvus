package agg

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

	validResult := &PartialAggregationResult{
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
	results := []*PartialAggregationResult{validResult, nil}
	_, err := reducer.Merge(context.Background(), results)
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

	makeResult := func(key string, val int64) *PartialAggregationResult {
		return &PartialAggregationResult{
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

	results := []*PartialAggregationResult{
		makeResult("a", 10),
		makeResult("a", 20),
		makeResult("b", 5),
	}

	out, err := reducer.Merge(context.Background(), results)
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, int64(3), out.GetAllRetrieveCount())
}

func TestGroupAggReducerMergesIncrementalResults(t *testing.T) {
	reducer := makeGroupAggReducer()
	buffer := reducer.NewBuffer()
	makeResult := func(keys []string, values []int64) *PartialAggregationResult {
		return newPartialAggregationResult([]*schemapb.FieldData{
			{
				Type: schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: keys}},
				}},
			},
			{
				Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}},
				}},
			},
		}, int64(len(keys)))
	}

	require.NoError(t, buffer.Merge([]*PartialAggregationResult{makeResult([]string{"a", "b"}, []int64{10, 5})}))
	require.NoError(t, buffer.Merge([]*PartialAggregationResult{makeResult([]string{"a", "c"}, []int64{20, 7})}))

	result, err := buffer.Result()
	require.NoError(t, err)
	require.Equal(t, int64(4), result.GetAllRetrieveCount())
	keys := result.GetFieldDatas()[0].GetScalars().GetStringData().GetData()
	values := result.GetFieldDatas()[1].GetScalars().GetLongData().GetData()
	actual := make(map[string]int64, len(keys))
	for i, key := range keys {
		actual[key] = values[i]
	}
	require.Equal(t, map[string]int64{"a": 30, "b": 5, "c": 7}, actual)
}

func TestGroupAggBufferOwnsAccumulation(t *testing.T) {
	reducer := NewGroupAggReducer([]int64{100}, []*planpb.Aggregate{{Op: planpb.AggregateOp_sum, FieldId: 101}}, -1, buildTestSchema())
	input := buildAggResult(1, 1)
	wire, err := reducer.EncodePartial(input)
	require.NoError(t, err)
	before := proto.Clone(wire)
	for range 2 {
		merged, err := reducer.Merge(t.Context(), []*PartialAggregationResult{input, input})
		require.NoError(t, err)
		require.Equal(t, []int64{2}, merged.GetFieldDatas()[1].GetScalars().GetLongData().GetData())
	}
	buffer := reducer.NewBuffer()
	require.NoError(t, buffer.Merge([]*PartialAggregationResult{{}, input}))
	first, err := buffer.Result()
	require.NoError(t, err)
	require.NoError(t, buffer.Merge([]*PartialAggregationResult{input}))
	second, err := buffer.Result()
	require.NoError(t, err)
	require.Equal(t, []int64{1}, first.GetFieldDatas()[1].GetScalars().GetLongData().GetData())
	require.Equal(t, []int64{2}, second.GetFieldDatas()[1].GetScalars().GetLongData().GetData())
	empty, err := reducer.NewBuffer().Result()
	require.NoError(t, err)
	require.True(t, empty.IsEmpty())
	after, err := reducer.EncodePartial(input)
	require.NoError(t, err)
	require.True(t, proto.Equal(before, after))
	require.Error(t, reducer.NewBuffer().Merge([]*PartialAggregationResult{nil}))
}

// TestReduceEmptyResults verifies that reduce returns an empty result for empty input.
func TestGroupAggBufferPreservesGroupLimit(t *testing.T) {
	for _, limit := range []int64{-1, 0, 1} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			reducer := NewGroupAggReducer([]int64{100}, []*planpb.Aggregate{{Op: planpb.AggregateOp_sum, FieldId: 101}}, limit, buildTestSchema())
			inputs := []*PartialAggregationResult{buildAggResult(1, 1), buildAggResult(2, 1), buildAggResult(1, 1)}
			buffer := reducer.NewBuffer()
			for _, input := range inputs {
				require.NoError(t, buffer.Merge([]*PartialAggregationResult{input}))
			}
			stream, err := buffer.Result()
			require.NoError(t, err)
			batch, err := reducer.Merge(t.Context(), inputs)
			require.NoError(t, err)
			for _, result := range []*PartialAggregationResult{stream, batch} {
				groups := result.GetFieldDatas()[0].GetScalars().GetLongData().GetData()
				values := result.GetFieldDatas()[1].GetScalars().GetLongData().GetData()
				actual := make(map[int64]int64, len(groups))
				for i, group := range groups {
					actual[group] = values[i]
				}
				if limit == -1 {
					require.Equal(t, map[int64]int64{1: 2, 2: 1}, actual)
				} else {
					require.Equal(t, map[int64]int64{1: 2}, actual)
				}
				require.Equal(t, int64(3), result.GetAllRetrieveCount())
			}
		})
	}
}

func TestReduceEmptyResults(t *testing.T) {
	reducer := makeGroupAggReducer()
	out, err := reducer.Merge(context.Background(), []*PartialAggregationResult{})
	require.NoError(t, err)
	require.NotNil(t, out)
}

func TestEmptyGroupedAggregationHasZeroRows(t *testing.T) {
	for _, op := range []planpb.AggregateOp{
		planpb.AggregateOp_count, planpb.AggregateOp_sum, planpb.AggregateOp_min,
		planpb.AggregateOp_max, planpb.AggregateOp_avg,
	} {
		t.Run(op.String(), func(t *testing.T) {
			reducer := NewGroupAggReducer([]int64{1}, []*planpb.Aggregate{{Op: op, FieldId: 2}}, -1, makeTestSchema())
			empty := &PartialAggregationResult{}
			batch, err := reducer.Merge(context.Background(), nil)
			require.NoError(t, err)
			stream, err := reducer.NewBuffer().Result()
			require.NoError(t, err)
			for _, result := range []*PartialAggregationResult{empty, batch, stream} {
				wire, err := reducer.EncodePartial(result)
				require.NoError(t, err)
				fields := wire.GetFieldsData()
				require.Len(t, fields, 2)
				require.Equal(t, int64(1), fields[0].GetFieldId())
				require.Equal(t, "category", fields[0].GetFieldName())
				require.Zero(t, result.GetAllRetrieveCount())
				for _, field := range fields {
					accessor, err := NewFieldAccessor(field.GetType())
					require.NoError(t, err)
					accessor.SetVals(field)
					require.Zero(t, accessor.RowCount())
				}
			}
		})
	}
}

func TestEmptyGlobalCountReturnsSingleZero(t *testing.T) {
	reducer := NewGroupAggReducer(nil, []*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 2}}, -1, makeTestSchema())
	empty := &PartialAggregationResult{}
	batch, err := reducer.Merge(context.Background(), nil)
	require.NoError(t, err)
	for _, result := range []*PartialAggregationResult{empty, batch} {
		wire, err := reducer.EncodePartial(result)
		require.NoError(t, err)
		require.Len(t, wire.GetFieldsData(), 1)
		require.Equal(t, []int64{0}, wire.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	}
}

// TestReduceSingleResult verifies that reduce returns the single input unchanged.
func TestReduceSingleResult(t *testing.T) {
	reducer := makeGroupAggReducer()
	singleResult := &PartialAggregationResult{
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

	out, err := reducer.Merge(context.Background(), []*PartialAggregationResult{singleResult})
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

// buildAggResult creates an PartialAggregationResult with N distinct groups.
// Each group has group key = startKey+i and count = 1.
func buildAggResult(startKey int64, numGroups int) *PartialAggregationResult {
	groupKeys := make([]int64, numGroups)
	counts := make([]int64, numGroups)
	for i := 0; i < numGroups; i++ {
		groupKeys[i] = startKey + int64(i)
		counts[i] = 1
	}
	return newPartialAggregationResult([]*schemapb.FieldData{
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
	results := []*PartialAggregationResult{
		buildAggResult(0, 10),
		buildAggResult(10, 10),
	}

	_, err := reducer.Merge(context.Background(), results)
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
	results := []*PartialAggregationResult{
		buildAggResult(0, 5),
		buildAggResult(5, 5),
	}

	result, err := reducer.Merge(context.Background(), results)
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
	results := []*PartialAggregationResult{
		buildAggResult(0, 6),
		buildAggResult(6, 5),
	}

	_, err := reducer.Merge(context.Background(), results)
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "too many groups"))
}
