// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package agg

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestNewAggregationFieldMap_GroupByInvalidField(t *testing.T) {
	// GROUP BY query with an output field that's neither group_by column nor aggregate
	countAggs, err := NewAggregate("count", 500, "count(*)", 0)
	require.NoError(t, err)
	aggs := make([]AggregateBase, len(countAggs))
	copy(aggs, countAggs)

	_, err = NewAggregationFieldMap(
		[]string{"category", "count(*)", "invalid_field"}, // "invalid_field" is not groupBy or agg
		[]string{"category"},                              // groupBy
		aggs,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid_field")
	assert.Contains(t, err.Error(), "GROUP BY")
	assert.Contains(t, err.Error(), "category") // should list valid targets
}

func TestNewAggregationFieldMap_GlobalAggInvalidField(t *testing.T) {
	// Global aggregation (no GROUP BY) with a regular column mixed in
	countAggs, err := NewAggregate("count", 500, "count(*)", 0)
	require.NoError(t, err)
	aggs := make([]AggregateBase, len(countAggs))
	copy(aggs, countAggs)

	_, err = NewAggregationFieldMap(
		[]string{"count(*)", "int64"}, // "int64" is not an aggregate
		[]string{},                    // no groupBy (global aggregation)
		aggs,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "int64")
	assert.Contains(t, err.Error(), "aggregation functions")
	assert.NotContains(t, err.Error(), "GROUP BY") // should NOT mention GROUP BY
}

func TestNewAggregationFieldMap_ValidGroupBy(t *testing.T) {
	countAggs, err := NewAggregate("count", 500, "count(*)", 0)
	require.NoError(t, err)
	aggs := make([]AggregateBase, len(countAggs))
	copy(aggs, countAggs)

	aggMap, err := NewAggregationFieldMap(
		[]string{"category", "count(*)"},
		[]string{"category"},
		aggs,
	)
	require.NoError(t, err)
	assert.Equal(t, 2, aggMap.Count())
	assert.Equal(t, "category", aggMap.NameAt(0))
	assert.Equal(t, "count(*)", aggMap.NameAt(1))
}

func TestNewAggregationFieldMap_ValidGlobalAgg(t *testing.T) {
	countAggs, err := NewAggregate("count", 500, "count(*)", 0)
	require.NoError(t, err)
	aggs := make([]AggregateBase, len(countAggs))
	copy(aggs, countAggs)

	aggMap, err := NewAggregationFieldMap(
		[]string{"count(*)"},
		[]string{},
		aggs,
	)
	require.NoError(t, err)
	assert.Equal(t, 1, aggMap.Count())
	assert.Equal(t, "count(*)", aggMap.NameAt(0))
}

func TestComputeAvgFromSumAndCount_Success(t *testing.T) {
	// Int64 sum and Int64 count
	sumFieldInt64 := &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}},
				},
			},
		},
	}
	countField := &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{2, 4, 5}},
				},
			},
		},
	}

	result, err := ComputeAvgFromSumAndCount(sumFieldInt64, countField)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, schemapb.DataType_Double, result.GetType())
	expected := []float64{5.0, 5.0, 6.0}
	assert.Equal(t, expected, result.GetScalars().GetDoubleData().GetData())
	assert.Empty(t, typeutil.GetFieldDataValidData(result), "result validData should be empty/nil when no nulls exist in Int64 sum")

	// Double sum and Int64 count
	sumFieldDouble := &schemapb.FieldData{
		Type: schemapb.DataType_Double,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{Data: []float64{10.5, 20.0, 30.25}},
				},
			},
		},
	}

	result, err = ComputeAvgFromSumAndCount(sumFieldDouble, countField)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, schemapb.DataType_Double, result.GetType())
	expectedDouble := []float64{5.25, 5.0, 6.05}
	assert.Equal(t, expectedDouble, result.GetScalars().GetDoubleData().GetData())
	assert.Empty(t, typeutil.GetFieldDataValidData(result), "result validData should be empty/nil when no nulls exist in Double sum")
}

func TestComputeAvgFromSumAndCount_ZeroCountTreatedAsNull(t *testing.T) {
	// Group 0: nonnull group (sum=10, count=2 -> avg=5.0)
	// Group 1: nullonly group where count is 0 (sum=0, count=0 -> avg=NULL)
	// Group 2: nonnull group (sum=30, count=5 -> avg=6.0)
	sumField := &schemapb.FieldData{
		Type: schemapb.DataType_Double,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{Data: []float64{10.0, 0.0, 30.0}},
				},
			},
		},
	}
	countField := &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{2, 0, 5}},
				},
			},
		},
	}

	result, err := ComputeAvgFromSumAndCount(sumField, countField)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, schemapb.DataType_Double, result.GetType())

	data := result.GetScalars().GetDoubleData().GetData()
	require.Len(t, data, 3)
	assert.Equal(t, 5.0, data[0])
	assert.Equal(t, 0.0, data[1])
	assert.Equal(t, 6.0, data[2])

	validData := typeutil.GetFieldDataValidData(result)
	require.Len(t, validData, 3)
	assert.True(t, validData[0], "row 0 should be valid non-null")
	assert.False(t, validData[1], "row 1 (zero count) should be treated as null aggregate")
	assert.True(t, validData[2], "row 2 should be valid non-null")

	// Same verification for Int64 sum
	sumFieldInt64 := &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{10, 0, 30}},
				},
			},
		},
	}

	resultInt64, err := ComputeAvgFromSumAndCount(sumFieldInt64, countField)
	require.NoError(t, err)
	require.NotNil(t, resultInt64)
	validDataInt64 := typeutil.GetFieldDataValidData(resultInt64)
	require.Len(t, validDataInt64, 3)
	assert.True(t, validDataInt64[0])
	assert.False(t, validDataInt64[1])
	assert.True(t, validDataInt64[2])
}

func TestComputeAvgFromSumAndCount_NullInputs(t *testing.T) {
	// Test when input sumFieldData or countFieldData has existing validData mask
	sumField := &schemapb.FieldData{
		Type: schemapb.DataType_Double,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{Data: []float64{10.0, 20.0}},
				},
			},
		},
	}
	typeutil.SetFieldDataValidData(sumField, []bool{true, false})

	countField := &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{2, 4}},
				},
			},
		},
	}

	result, err := ComputeAvgFromSumAndCount(sumField, countField)
	require.NoError(t, err)
	require.NotNil(t, result)
	validData := typeutil.GetFieldDataValidData(result)
	require.Len(t, validData, 2)
	assert.True(t, validData[0])
	assert.False(t, validData[1], "null in sumFieldData should propagate to result validData")

	// Test when countFieldData has existing validData mask (exercises countValidData branch)
	sumFieldCountMask := &schemapb.FieldData{
		Type: schemapb.DataType_Double,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{Data: []float64{10.0, 20.0}},
				},
			},
		},
	}
	countFieldCountMask := &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{2, 4}},
				},
			},
		},
	}
	typeutil.SetFieldDataValidData(countFieldCountMask, []bool{true, false})

	resultCountMask, err := ComputeAvgFromSumAndCount(sumFieldCountMask, countFieldCountMask)
	require.NoError(t, err)
	require.NotNil(t, resultCountMask)
	validDataCountMask := typeutil.GetFieldDataValidData(resultCountMask)
	require.Len(t, validDataCountMask, 2)
	assert.True(t, validDataCountMask[0])
	assert.False(t, validDataCountMask[1], "null in countFieldData should propagate to result validData")
	assert.Equal(t, 5.0, resultCountMask.GetScalars().GetDoubleData().GetData()[0])
	assert.Equal(t, 0.0, resultCountMask.GetScalars().GetDoubleData().GetData()[1])

	// Test when both sumFieldData and countFieldData have validity masks
	typeutil.SetFieldDataValidData(sumFieldCountMask, []bool{true, true})
	typeutil.SetFieldDataValidData(countFieldCountMask, []bool{false, true})
	resultBothMask, err := ComputeAvgFromSumAndCount(sumFieldCountMask, countFieldCountMask)
	require.NoError(t, err)
	require.NotNil(t, resultBothMask)
	validDataBothMask := typeutil.GetFieldDataValidData(resultBothMask)
	require.Len(t, validDataBothMask, 2)
	assert.False(t, validDataBothMask[0], "row 0 has invalid count mask")
	assert.True(t, validDataBothMask[1], "row 1 has valid sum and count")
	assert.Equal(t, 0.0, resultBothMask.GetScalars().GetDoubleData().GetData()[0])
	assert.Equal(t, 5.0, resultBothMask.GetScalars().GetDoubleData().GetData()[1])
}

func TestComputeAvgFromSumAndCount_Errors(t *testing.T) {
	// Nil inputs
	_, err := ComputeAvgFromSumAndCount(nil, nil)
	assert.Error(t, err)

	sumField := &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{10}},
				},
			},
		},
	}
	// Count field not Int64
	invalidCountField := &schemapb.FieldData{
		Type: schemapb.DataType_Double,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{Data: []float64{2.0}},
				},
			},
		},
	}
	_, err = ComputeAvgFromSumAndCount(sumField, invalidCountField)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "count field must be Int64 type")

	// Length mismatch
	countFieldMismatch := &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{2, 3}},
				},
			},
		},
	}
	_, err = ComputeAvgFromSumAndCount(sumField, countFieldMismatch)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must have the same length")

	// Unsupported sum field type
	unsupportedSumField := &schemapb.FieldData{
		Type: schemapb.DataType_VarChar,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{"abc"}},
				},
			},
		},
	}
	validCountField := &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{1}},
				},
			},
		},
	}
	_, err = ComputeAvgFromSumAndCount(unsupportedSumField, validCountField)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported sum field type")
}

// TestGroupAggReducer_AvgWithZeroCountAndNullGroup verifies end-to-end multi-segment reduction
// for AVG aggregation when a group contains only NULL values across segments (zero count / masked sum).
// It verifies that zero-count and masked groups survive the reducer without division-by-zero errors
// and that SQL-NULL aggregate semantics (0.0 fill value and validData=false) are preserved.
func TestGroupAggReducer_AvgWithZeroCountAndNullGroup(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "group_field", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "agg_field", DataType: schemapb.DataType_Int64},
		},
	}
	aggregates := []*planpb.Aggregate{
		{Op: planpb.AggregateOp_sum, FieldId: 101},
		{Op: planpb.AggregateOp_count, FieldId: 101},
	}
	reducer := NewGroupAggReducer([]int64{100}, aggregates, -1, schema)

	// Segment 1:
	// Group 1: sum=10, count=2 (valid)
	// Group 2: sum=null (masked), count=0 (zero count)
	groupField1 := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "group_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}},
			},
		},
	}
	sumField1 := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "agg_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{10, 0}}},
			},
		},
	}
	typeutil.SetFieldDataValidData(sumField1, []bool{true, false})
	countField1 := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "agg_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{2, 0}}},
			},
		},
	}

	// Segment 2:
	// Group 1: sum=20, count=3 (valid)
	// Group 2: sum=null (masked), count=0 (zero count)
	groupField2 := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "group_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}},
			},
		},
	}
	sumField2 := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "agg_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{20, 0}}},
			},
		},
	}
	typeutil.SetFieldDataValidData(sumField2, []bool{true, false})
	countField2 := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "agg_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{3, 0}}},
			},
		},
	}

	res1 := NewAggregationResult([]*schemapb.FieldData{groupField1, sumField1, countField1}, 2)
	res2 := NewAggregationResult([]*schemapb.FieldData{groupField2, sumField2, countField2}, 2)

	reduced, err := reducer.Reduce(context.Background(), []*AggregationResult{res1, res2})
	require.NoError(t, err)
	require.NotNil(t, reduced)

	reducedFields := reduced.GetFieldDatas()
	require.Len(t, reducedFields, 3)

	// Post-reduction: proxy computes AVG from reduced sum and count columns
	avgField, err := ComputeAvgFromSumAndCount(reducedFields[1], reducedFields[2])
	require.NoError(t, err)
	require.NotNil(t, avgField)

	groupKeys := reducedFields[0].GetScalars().GetLongData().GetData()
	avgValues := avgField.GetScalars().GetDoubleData().GetData()
	validData := typeutil.GetFieldDataValidData(avgField)

	require.Len(t, groupKeys, 2)
	require.Len(t, avgValues, 2)
	require.Len(t, validData, 2)

	for i, key := range groupKeys {
		if key == 1 {
			// Valid group: (10 + 20) / (2 + 3) = 6.0
			assert.InDelta(t, 6.0, avgValues[i], 0.0001)
			assert.True(t, validData[i], "group 1 should be marked valid non-null")
		} else if key == 2 {
			// All-NULL group: count is 0, sum is null -> emitted as SQL-NULL aggregate (0.0 fill, valid=false)
			assert.Equal(t, 0.0, avgValues[i])
			assert.False(t, validData[i], "group 2 (all-NULL group) should be marked invalid/null")
		}
	}
}
