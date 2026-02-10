package agg

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

func TestGroupAggReducerNullableGroupBy(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_VarChar, IsPrimaryKey: true},
			{FieldID: 101, Name: "c1", DataType: schemapb.DataType_VarChar, Nullable: true},
			{FieldID: 102, Name: "c3", DataType: schemapb.DataType_Int32},
		},
	}

	aggregates := []*planpb.Aggregate{
		{Op: planpb.AggregateOp_sum, FieldId: 102},
	}

	reducer := NewGroupAggReducer([]int64{101}, aggregates, -1, schema)

	// Segment 1: groups A, B, NULL
	result1 := NewAggregationResult([]*schemapb.FieldData{
		{
			Type:      schemapb.DataType_VarChar,
			FieldId:   101,
			FieldName: "c1",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"A", "B", ""},
						},
					},
				},
			},
			ValidData: []bool{true, true, false},
		},
		{
			Type:    schemapb.DataType_Int64,
			FieldId: 102,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{100, 200, 50},
						},
					},
				},
			},
		},
	}, 3)

	// Segment 2: groups A, NULL, C
	result2 := NewAggregationResult([]*schemapb.FieldData{
		{
			Type:      schemapb.DataType_VarChar,
			FieldId:   101,
			FieldName: "c1",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"A", "", "C"},
						},
					},
				},
			},
			ValidData: []bool{true, false, true},
		},
		{
			Type:    schemapb.DataType_Int64,
			FieldId: 102,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{150, 75, 300},
						},
					},
				},
			},
		},
	}, 3)

	reduced, err := reducer.Reduce(context.Background(), []*AggregationResult{result1, result2})
	require.NoError(t, err)

	fieldDatas := reduced.GetFieldDatas()
	require.Len(t, fieldDatas, 2)

	groupFieldData := fieldDatas[0]
	sumFieldData := fieldDatas[1]

	groupValues := groupFieldData.GetScalars().GetStringData().GetData()
	validData := groupFieldData.GetValidData()
	sumValues := sumFieldData.GetScalars().GetLongData().GetData()

	// 4 groups: A, B, NULL, C
	require.Len(t, groupValues, 4)
	require.Len(t, validData, 4)
	require.Len(t, sumValues, 4)

	groupSums := make(map[string]int64)
	var nullSum int64
	hasNullGroup := false
	for i := 0; i < len(groupValues); i++ {
		if validData[i] {
			groupSums[groupValues[i]] = sumValues[i]
		} else {
			hasNullGroup = true
			nullSum = sumValues[i]
		}
	}

	assert.True(t, hasNullGroup, "should have a NULL group")
	assert.Equal(t, int64(250), groupSums["A"]) // 100 + 150
	assert.Equal(t, int64(200), groupSums["B"]) // 200
	assert.Equal(t, int64(300), groupSums["C"]) // 300
	assert.Equal(t, int64(125), nullSum)        // 50 + 75
}

func TestGroupAggReducerNullableInt64GroupBy(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_VarChar, IsPrimaryKey: true},
			{FieldID: 101, Name: "c1", DataType: schemapb.DataType_Int64, Nullable: true},
			{FieldID: 102, Name: "c3", DataType: schemapb.DataType_Int32},
		},
	}

	aggregates := []*planpb.Aggregate{
		{Op: planpb.AggregateOp_count, FieldId: 102},
	}

	reducer := NewGroupAggReducer([]int64{101}, aggregates, -1, schema)

	result := NewAggregationResult([]*schemapb.FieldData{
		{
			Type:      schemapb.DataType_Int64,
			FieldId:   101,
			FieldName: "c1",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{10, 0, 20}},
					},
				},
			},
			ValidData: []bool{true, false, true},
		},
		{
			Type:    schemapb.DataType_Int64,
			FieldId: 102,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1, 1, 1}},
					},
				},
			},
		},
	}, 3)

	reduced, err := reducer.Reduce(context.Background(), []*AggregationResult{result})
	require.NoError(t, err)

	fieldDatas := reduced.GetFieldDatas()
	require.Len(t, fieldDatas, 2)

	validData := fieldDatas[0].GetValidData()
	require.Len(t, validData, 3)

	nullCount := 0
	for _, v := range validData {
		if !v {
			nullCount++
		}
	}
	assert.Equal(t, 1, nullCount)
}

func TestGroupAggReducerNullableInt32GroupBy(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_VarChar, IsPrimaryKey: true},
			{FieldID: 101, Name: "c1", DataType: schemapb.DataType_Int32, Nullable: true},
			{FieldID: 102, Name: "c3", DataType: schemapb.DataType_Int32},
		},
	}

	aggregates := []*planpb.Aggregate{
		{Op: planpb.AggregateOp_count, FieldId: 102},
	}

	reducer := NewGroupAggReducer([]int64{101}, aggregates, -1, schema)

	result := NewAggregationResult([]*schemapb.FieldData{
		{
			Type:      schemapb.DataType_Int32,
			FieldId:   101,
			FieldName: "c1",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{Data: []int32{10, 0}},
					},
				},
			},
			ValidData: []bool{true, false},
		},
		{
			Type:    schemapb.DataType_Int64,
			FieldId: 102,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1, 1}},
					},
				},
			},
		},
	}, 2)

	reduced, err := reducer.Reduce(context.Background(), []*AggregationResult{result})
	require.NoError(t, err)

	validData := reduced.GetFieldDatas()[0].GetValidData()
	require.Len(t, validData, 2)
}

func TestGroupAggReducerNullableFloatGroupBy(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_VarChar, IsPrimaryKey: true},
			{FieldID: 101, Name: "c1", DataType: schemapb.DataType_Float, Nullable: true},
			{FieldID: 102, Name: "c3", DataType: schemapb.DataType_Int32},
		},
	}

	aggregates := []*planpb.Aggregate{
		{Op: planpb.AggregateOp_count, FieldId: 102},
	}

	reducer := NewGroupAggReducer([]int64{101}, aggregates, -1, schema)

	result := NewAggregationResult([]*schemapb.FieldData{
		{
			Type:      schemapb.DataType_Float,
			FieldId:   101,
			FieldName: "c1",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{
						FloatData: &schemapb.FloatArray{Data: []float32{1.5, 0}},
					},
				},
			},
			ValidData: []bool{true, false},
		},
		{
			Type:    schemapb.DataType_Int64,
			FieldId: 102,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1, 1}},
					},
				},
			},
		},
	}, 2)

	reduced, err := reducer.Reduce(context.Background(), []*AggregationResult{result})
	require.NoError(t, err)

	validData := reduced.GetFieldDatas()[0].GetValidData()
	require.Len(t, validData, 2)
}

func TestGroupAggReducerNullableDoubleGroupBy(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_VarChar, IsPrimaryKey: true},
			{FieldID: 101, Name: "c1", DataType: schemapb.DataType_Double, Nullable: true},
			{FieldID: 102, Name: "c3", DataType: schemapb.DataType_Int32},
		},
	}

	aggregates := []*planpb.Aggregate{
		{Op: planpb.AggregateOp_count, FieldId: 102},
	}

	reducer := NewGroupAggReducer([]int64{101}, aggregates, -1, schema)

	result := NewAggregationResult([]*schemapb.FieldData{
		{
			Type:      schemapb.DataType_Double,
			FieldId:   101,
			FieldName: "c1",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{Data: []float64{1.5, 0}},
					},
				},
			},
			ValidData: []bool{true, false},
		},
		{
			Type:    schemapb.DataType_Int64,
			FieldId: 102,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1, 1}},
					},
				},
			},
		},
	}, 2)

	reduced, err := reducer.Reduce(context.Background(), []*AggregationResult{result})
	require.NoError(t, err)

	validData := reduced.GetFieldDatas()[0].GetValidData()
	require.Len(t, validData, 2)
}

func TestGroupAggReducerNullableBoolGroupBy(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_VarChar, IsPrimaryKey: true},
			{FieldID: 101, Name: "c1", DataType: schemapb.DataType_Bool, Nullable: true},
			{FieldID: 102, Name: "c3", DataType: schemapb.DataType_Int32},
		},
	}

	aggregates := []*planpb.Aggregate{
		{Op: planpb.AggregateOp_count, FieldId: 102},
	}

	reducer := NewGroupAggReducer([]int64{101}, aggregates, -1, schema)

	result := NewAggregationResult([]*schemapb.FieldData{
		{
			Type:      schemapb.DataType_Bool,
			FieldId:   101,
			FieldName: "c1",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_BoolData{
						BoolData: &schemapb.BoolArray{Data: []bool{true, false}},
					},
				},
			},
			ValidData: []bool{true, false},
		},
		{
			Type:    schemapb.DataType_Int64,
			FieldId: 102,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1, 1}},
					},
				},
			},
		},
	}, 2)

	reduced, err := reducer.Reduce(context.Background(), []*AggregationResult{result})
	require.NoError(t, err)

	validData := reduced.GetFieldDatas()[0].GetValidData()
	require.Len(t, validData, 2)
}

// TestAppendNullValue directly tests the appendNullValue function for all data types
func TestAppendNullValue(t *testing.T) {
	tests := []struct {
		name      string
		fieldData *schemapb.FieldData
	}{
		{
			name: "Bool",
			fieldData: &schemapb.FieldData{
				Type: schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{Data: []bool{}},
						},
					},
				},
			},
		},
		{
			name: "Int32",
			fieldData: &schemapb.FieldData{
				Type: schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{Data: []int32{}},
						},
					},
				},
			},
		},
		{
			name: "Int64",
			fieldData: &schemapb.FieldData{
				Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{}},
						},
					},
				},
			},
		},
		{
			name: "Timestamptz",
			fieldData: &schemapb.FieldData{
				Type: schemapb.DataType_Timestamptz,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_TimestamptzData{
							TimestamptzData: &schemapb.TimestamptzArray{Data: []int64{}},
						},
					},
				},
			},
		},
		{
			name: "Float",
			fieldData: &schemapb.FieldData{
				Type: schemapb.DataType_Float,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{Data: []float32{}},
						},
					},
				},
			},
		},
		{
			name: "Double",
			fieldData: &schemapb.FieldData{
				Type: schemapb.DataType_Double,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{Data: []float64{}},
						},
					},
				},
			},
		},
		{
			name: "VarChar",
			fieldData: &schemapb.FieldData{
				Type: schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{Data: []string{}},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := appendNullValue(tt.fieldData)
			assert.NoError(t, err)
		})
	}
}

// TestAppendNullValueUnsupportedType tests the error case for unsupported types
func TestAppendNullValueUnsupportedType(t *testing.T) {
	fieldData := &schemapb.FieldData{
		Type: schemapb.DataType_JSON,
	}
	err := appendNullValue(fieldData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported DataType")
}

// TestNullableFieldAccessor tests NullableFieldAccessor methods directly
func TestNullableFieldAccessor(t *testing.T) {
	inner := newStringFieldAccessor()
	nfa := &NullableFieldAccessor{inner: inner}

	fieldData := &schemapb.FieldData{
		Type: schemapb.DataType_VarChar,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{"a", "b", "c"}},
				},
			},
		},
		ValidData: []bool{true, false, true},
	}
	nfa.SetVals(fieldData)

	assert.Equal(t, 3, nfa.RowCount())

	// Valid value at index 0
	assert.NotEqual(t, nullHashSentinel, nfa.Hash(0))
	assert.Equal(t, "a", nfa.ValAt(0))

	// Null value at index 1
	assert.Equal(t, nullHashSentinel, nfa.Hash(1))
	assert.Nil(t, nfa.ValAt(1))

	// Valid value at index 2
	assert.NotEqual(t, nullHashSentinel, nfa.Hash(2))
	assert.Equal(t, "c", nfa.ValAt(2))
}

// TestNullableFieldAccessorNoValidData tests NullableFieldAccessor with non-nullable field
func TestNullableFieldAccessorNoValidData(t *testing.T) {
	inner := newStringFieldAccessor()
	nfa := &NullableFieldAccessor{inner: inner}

	fieldData := &schemapb.FieldData{
		Type: schemapb.DataType_VarChar,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{"a", "b"}},
				},
			},
		},
	}
	nfa.SetVals(fieldData)

	assert.NotEqual(t, nullHashSentinel, nfa.Hash(0))
	assert.Equal(t, "a", nfa.ValAt(0))
	assert.NotEqual(t, nullHashSentinel, nfa.Hash(1))
	assert.Equal(t, "b", nfa.ValAt(1))
}

// TestAssembleSingleValueNil tests AssembleSingleValue with nil value
func TestAssembleSingleValueNil(t *testing.T) {
	fieldData := &schemapb.FieldData{
		Type: schemapb.DataType_VarChar,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{}},
				},
			},
		},
		ValidData: []bool{},
	}

	err := AssembleSingleValue(nil, fieldData)
	require.NoError(t, err)

	assert.Len(t, fieldData.ValidData, 1)
	assert.False(t, fieldData.ValidData[0])
	assert.Len(t, fieldData.GetScalars().GetStringData().GetData(), 1)
}

// TestAssembleSingleValueNonNilWithValidData tests AssembleSingleValue with non-nil on nullable field
func TestAssembleSingleValueNonNilWithValidData(t *testing.T) {
	fieldData := &schemapb.FieldData{
		Type: schemapb.DataType_VarChar,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{}},
				},
			},
		},
		ValidData: []bool{},
	}

	err := AssembleSingleValue("test", fieldData)
	require.NoError(t, err)

	assert.Len(t, fieldData.ValidData, 1)
	assert.True(t, fieldData.ValidData[0])
	assert.Equal(t, []string{"test"}, fieldData.GetScalars().GetStringData().GetData())
}
