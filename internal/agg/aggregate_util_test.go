package agg

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestTruncateFieldData_NilInput(t *testing.T) {
	// Should not panic on nil
	truncateFieldData(nil, 5)
}

func TestTruncateFieldData_NilScalars(t *testing.T) {
	fd := &schemapb.FieldData{Type: schemapb.DataType_Int64}
	truncateFieldData(fd, 2)
	// No scalars set — nothing to truncate, should not panic
}

func TestTruncateFieldData_BoolData(t *testing.T) {
	fd := &schemapb.FieldData{
		Type: schemapb.DataType_Bool,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{Data: []bool{true, false, true, false, true}},
				},
			},
		},
	}
	truncateFieldData(fd, 3)
	assert.Equal(t, 3, len(fd.GetScalars().GetBoolData().GetData()))
}

func TestTruncateFieldData_IntData(t *testing.T) {
	fd := &schemapb.FieldData{
		Type: schemapb.DataType_Int32,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{Data: []int32{1, 2, 3, 4, 5}},
				},
			},
		},
	}
	truncateFieldData(fd, 2)
	assert.Equal(t, []int32{1, 2}, fd.GetScalars().GetIntData().GetData())
}

func TestTruncateFieldData_LongData(t *testing.T) {
	fd := &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{10, 20, 30, 40}},
				},
			},
		},
	}
	truncateFieldData(fd, 2)
	assert.Equal(t, []int64{10, 20}, fd.GetScalars().GetLongData().GetData())
}

func TestTruncateFieldData_FloatData(t *testing.T) {
	fd := &schemapb.FieldData{
		Type: schemapb.DataType_Float,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{Data: []float32{1.1, 2.2, 3.3}},
				},
			},
		},
	}
	truncateFieldData(fd, 2)
	assert.Equal(t, []float32{1.1, 2.2}, fd.GetScalars().GetFloatData().GetData())
}

func TestTruncateFieldData_DoubleData(t *testing.T) {
	fd := &schemapb.FieldData{
		Type: schemapb.DataType_Double,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{Data: []float64{1.1, 2.2, 3.3}},
				},
			},
		},
	}
	truncateFieldData(fd, 1)
	assert.Equal(t, []float64{1.1}, fd.GetScalars().GetDoubleData().GetData())
}

func TestTruncateFieldData_StringData(t *testing.T) {
	fd := &schemapb.FieldData{
		Type: schemapb.DataType_VarChar,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{"a", "b", "c", "d"}},
				},
			},
		},
	}
	truncateFieldData(fd, 2)
	assert.Equal(t, []string{"a", "b"}, fd.GetScalars().GetStringData().GetData())
}

func TestTruncateFieldData_ValidData(t *testing.T) {
	fd := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		ValidData: []bool{true, true, false, true},
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{10, 20, 30, 40}},
				},
			},
		},
	}
	truncateFieldData(fd, 2)
	assert.Equal(t, []bool{true, true}, fd.ValidData)
	assert.Equal(t, []int64{10, 20}, fd.GetScalars().GetLongData().GetData())
}

func TestTruncateFieldData_DataShorterThanLimit(t *testing.T) {
	fd := &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{10, 20}},
				},
			},
		},
	}
	truncateFieldData(fd, 100)
	assert.Equal(t, []int64{10, 20}, fd.GetScalars().GetLongData().GetData())
}

func TestTruncateFieldData_TimestamptzData(t *testing.T) {
	fd := &schemapb.FieldData{
		Type: schemapb.DataType_Timestamptz,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_TimestamptzData{
					TimestamptzData: &schemapb.TimestamptzArray{Data: []int64{1704067200, 1704153600, 1704240000}},
				},
			},
		},
	}
	truncateFieldData(fd, 1)
	assert.Equal(t, []int64{1704067200}, fd.GetScalars().GetTimestamptzData().GetData())
}

func TestTruncateAggResult_NilResult(t *testing.T) {
	result := truncateAggResult(nil, 10)
	assert.Nil(t, result)
}

func TestTruncateAggResult_ZeroLimit(t *testing.T) {
	aggResult := &AggregationResult{
		fieldDatas: []*schemapb.FieldData{
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
		},
	}
	result := truncateAggResult(aggResult, 0)
	// limit <= 0, should return as-is without truncation
	assert.Equal(t, 3, len(result.GetFieldDatas()[0].GetScalars().GetLongData().GetData()))
}

func TestTruncateAggResult_NegativeLimit(t *testing.T) {
	aggResult := &AggregationResult{
		fieldDatas: []*schemapb.FieldData{
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
		},
	}
	result := truncateAggResult(aggResult, -1)
	assert.Equal(t, 3, len(result.GetFieldDatas()[0].GetScalars().GetLongData().GetData()))
}

func TestTruncateAggResult_MultipleFields(t *testing.T) {
	aggResult := &AggregationResult{
		fieldDatas: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{Data: []int32{1, 2, 3, 4, 5}},
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
			{
				Type: schemapb.DataType_Double,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{Data: []float64{1.1, 2.2, 3.3, 4.4, 5.5}},
						},
					},
				},
			},
		},
	}
	result := truncateAggResult(aggResult, 3)
	assert.Equal(t, []int32{1, 2, 3}, result.GetFieldDatas()[0].GetScalars().GetIntData().GetData())
	assert.Equal(t, []int64{10, 20, 30}, result.GetFieldDatas()[1].GetScalars().GetLongData().GetData())
	assert.Equal(t, []float64{1.1, 2.2, 3.3}, result.GetFieldDatas()[2].GetScalars().GetDoubleData().GetData())
}
