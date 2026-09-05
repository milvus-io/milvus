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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
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
