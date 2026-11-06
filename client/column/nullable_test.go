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

package column

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/client/v3/entity"
)

type NullableScalarSuite struct {
	suite.Suite
}

func (s *NullableScalarSuite) TestBasic() {
	s.Run("nullable_bool", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		compactData := []bool{false}
		sparseData := []bool{false, false}
		validData := []bool{true, false}
		// compact mode
		column, err := NewNullableColumnBool(name, compactData, validData)
		s.NoError(err)
		s.Equal(entity.FieldTypeBool, column.Type())
		s.Equal(name, column.Name())
		s.Equal(compactData, column.Data())
		for i := 0; i < len(validData); i++ {
			r, err := column.IsNull(i)
			s.NoError(err)
			s.Equal(validData[i], !r)
		}
		s.NoError(column.AppendValue(true))
		s.NoError(column.AppendNull())

		// sparse mode
		column, err = NewNullableColumnBool(name, sparseData, validData, WithSparseNullableMode[bool](true))
		s.NoError(err)
		s.Equal(sparseData, column.Data())

		fd := column.FieldData()
		s.Equal(validData, getFieldDataValidData(fd))
		s.Equal(validData, fd.GetValidData())
		s.Equal(validData, fd.GetScalars().GetValidData())
		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnBool)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(sparseData, parsed.Data())
			s.Equal(entity.FieldTypeBool, column.Type())
		}

		_, err = NewNullableColumnBool(name, compactData, []bool{false, false})
		s.Error(err)
	})

	s.Run("nullable_int8", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		compactData := []int8{1, 3}
		sparseData := []int8{1, 0, 3}
		validData := []bool{true, false, true}
		// compact mode
		column, err := NewNullableColumnInt8(name, compactData, validData)
		s.NoError(err)
		s.Equal(entity.FieldTypeInt8, column.Type())
		s.Equal(name, column.Name())
		s.Equal(compactData, column.Data())
		for i := 0; i < len(validData); i++ {
			r, err := column.IsNull(i)
			s.NoError(err)
			s.Equal(validData[i], !r)
		}

		// sparse mode
		column, err = NewNullableColumnInt8(name, sparseData, validData, WithSparseNullableMode[int8](true))
		s.NoError(err)

		fd := column.FieldData()
		s.Equal(validData, getFieldDataValidData(fd))
		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnInt8)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(sparseData, parsed.Data())
			s.Equal(entity.FieldTypeInt8, column.Type())
		}

		_, err = NewNullableColumnInt8(name, compactData, []bool{false, false})
		s.Error(err)
	})

	s.Run("nullable_int16", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		compactData := []int16{1, 3}
		sparseData := []int16{1, 0, 3}
		validData := []bool{true, false, true}
		// compact mode
		column, err := NewNullableColumnInt16(name, compactData, validData)
		s.NoError(err)
		s.Equal(entity.FieldTypeInt16, column.Type())
		s.Equal(name, column.Name())
		s.Equal(compactData, column.Data())
		for i := 0; i < len(validData); i++ {
			r, err := column.IsNull(i)
			s.NoError(err)
			s.Equal(validData[i], !r)
		}

		// compact mode
		column, err = NewNullableColumnInt16(name, sparseData, validData, WithSparseNullableMode[int16](true))
		s.NoError(err)

		fd := column.FieldData()
		s.Equal(validData, getFieldDataValidData(fd))
		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnInt16)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(sparseData, parsed.Data())
			s.Equal(entity.FieldTypeInt16, column.Type())
		}

		_, err = NewNullableColumnInt16(name, compactData, []bool{false, false})
		s.Error(err)
	})

	s.Run("nullable_int32", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		compactData := []int32{1, 3}
		sparseData := []int32{1, 0, 3}
		validData := []bool{true, false, true}
		// compact mode
		column, err := NewNullableColumnInt32(name, compactData, validData)
		s.NoError(err)
		s.Equal(entity.FieldTypeInt32, column.Type())
		s.Equal(name, column.Name())
		s.Equal(compactData, column.Data())
		for i := 0; i < len(validData); i++ {
			r, err := column.IsNull(i)
			s.NoError(err)
			s.Equal(validData[i], !r)
		}

		// compact mode
		column, err = NewNullableColumnInt32(name, sparseData, validData, WithSparseNullableMode[int32](true))
		s.NoError(err)

		fd := column.FieldData()
		s.Equal(validData, getFieldDataValidData(fd))
		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnInt32)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(sparseData, parsed.Data())
			s.Equal(entity.FieldTypeInt32, column.Type())
		}

		_, err = NewNullableColumnInt32(name, compactData, []bool{false, false})
		s.Error(err)
	})

	s.Run("nullable_int64", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		compactData := []int64{1, 3}
		sparseData := []int64{1, 0, 3}
		validData := []bool{true, false, true}
		// compact mode
		column, err := NewNullableColumnInt64(name, compactData, validData)
		s.NoError(err)
		s.Equal(entity.FieldTypeInt64, column.Type())
		s.Equal(name, column.Name())
		s.Equal(compactData, column.Data())
		for i := 0; i < len(validData); i++ {
			r, err := column.IsNull(i)
			s.NoError(err)
			s.Equal(validData[i], !r)
		}

		// compact mode
		column, err = NewNullableColumnInt64(name, sparseData, validData, WithSparseNullableMode[int64](true))
		s.NoError(err)

		fd := column.FieldData()
		s.Equal(validData, getFieldDataValidData(fd))
		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnInt64)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(sparseData, parsed.Data())
			s.Equal(entity.FieldTypeInt64, column.Type())
		}

		_, err = NewNullableColumnInt64(name, compactData, []bool{false, false})
		s.Error(err)
	})

	s.Run("nullable_float", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		compactData := []float32{0.1, 0.3}
		sparseData := []float32{0.1, 0, 0.3}
		validData := []bool{true, false, true}
		// compact mode
		column, err := NewNullableColumnFloat(name, compactData, validData)
		s.NoError(err)
		s.Equal(entity.FieldTypeFloat, column.Type())
		s.Equal(name, column.Name())
		s.Equal(compactData, column.Data())
		for i := 0; i < len(validData); i++ {
			r, err := column.IsNull(i)
			s.NoError(err)
			s.Equal(validData[i], !r)
		}

		// sparse mode
		column, err = NewNullableColumnFloat(name, sparseData, validData, WithSparseNullableMode[float32](true))
		s.NoError(err)

		fd := column.FieldData()
		s.Equal(validData, getFieldDataValidData(fd))
		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnFloat)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(sparseData, parsed.Data())
			s.Equal(entity.FieldTypeFloat, column.Type())
		}

		_, err = NewNullableColumnFloat(name, compactData, []bool{false, false})
		s.Error(err)
	})

	s.Run("nullable_double", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		compactData := []float64{0.1, 0.3}
		sparseData := []float64{0.1, 0, 0.3}
		validData := []bool{true, false, true}
		// compact data
		column, err := NewNullableColumnDouble(name, compactData, validData)
		s.NoError(err)
		s.Equal(entity.FieldTypeDouble, column.Type())
		s.Equal(name, column.Name())
		s.Equal(compactData, column.Data())
		for i := 0; i < len(validData); i++ {
			r, err := column.IsNull(i)
			s.NoError(err)
			s.Equal(validData[i], !r)
		}

		// sparse data
		column, err = NewNullableColumnDouble(name, sparseData, validData, WithSparseNullableMode[float64](true))
		s.NoError(err)

		fd := column.FieldData()
		s.Equal(validData, getFieldDataValidData(fd))
		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnDouble)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(sparseData, parsed.Data())
			s.Equal(entity.FieldTypeDouble, column.Type())
		}

		_, err = NewNullableColumnDouble(name, compactData, []bool{false, false})
		s.Error(err)
	})

	s.Run("nullable_timestamptz", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		now := time.Now().UTC()
		compactData := []time.Time{now, now.Add(2 * time.Hour)}
		validData := []bool{true, false, true}
		// compact mode
		column, err := NewNullableColumnTimestamptz(name, compactData, validData)
		s.NoError(err)
		s.Equal(entity.FieldTypeTimestamptz, column.Type())
		s.Equal(name, column.Name())
		// verify time values are converted to normalized RFC3339Nano strings
		expectedStrings := []string{
			formatTimestamptz(compactData[0]),
			formatTimestamptz(compactData[1]),
		}
		s.Equal(expectedStrings, column.Data())
		for i := 0; i < len(validData); i++ {
			r, err := column.IsNull(i)
			s.NoError(err)
			s.Equal(validData[i], !r)
		}

		// sparse mode
		sparseData := []time.Time{now, {}, now.Add(2 * time.Hour)}
		sparseExpected := []string{
			formatTimestamptz(sparseData[0]),
			formatTimestamptz(sparseData[1]),
			formatTimestamptz(sparseData[2]),
		}
		column, err = NewNullableColumnTimestamptz(name, sparseData, validData, WithSparseNullableMode[string](true))
		s.NoError(err)

		fd := column.FieldData()
		s.Equal(validData, getFieldDataValidData(fd))
		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnTimestampTzIsoString)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(sparseExpected, parsed.Data())
			s.Equal(entity.FieldTypeTimestamptz, parsed.Type())
		}

		_, err = NewNullableColumnTimestamptz(name, compactData, []bool{false, false})
		s.Error(err)
	})

	s.Run("nullable_timestamptz_iso_string", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		compactData := []string{"2024-01-01T00:00:00Z", "2024-06-15T12:30:45Z"}
		sparseData := []string{"2024-01-01T00:00:00Z", "", "2024-06-15T12:30:45Z"}
		validData := []bool{true, false, true}
		// compact mode
		column, err := NewNullableColumnTimestamptzIsoString(name, compactData, validData)
		s.NoError(err)
		s.Equal(entity.FieldTypeTimestamptz, column.Type())
		s.Equal(name, column.Name())
		s.Equal(compactData, column.Data())
		for i := 0; i < len(validData); i++ {
			r, err := column.IsNull(i)
			s.NoError(err)
			s.Equal(validData[i], !r)
		}

		// sparse mode
		column, err = NewNullableColumnTimestamptzIsoString(name, sparseData, validData, WithSparseNullableMode[string](true))
		s.NoError(err)

		fd := column.FieldData()
		s.Equal(validData, getFieldDataValidData(fd))
		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnTimestampTzIsoString)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(sparseData, parsed.Data())
			s.Equal(entity.FieldTypeTimestamptz, parsed.Type())
		}

		_, err = NewNullableColumnTimestamptzIsoString(name, compactData, []bool{false, false})
		s.Error(err)
	})

	s.Run("nullable_text", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		compactData := []string{"short text", "长文本"}
		sparseData := []string{"short text", "", "长文本"}
		validData := []bool{true, false, true}

		column, err := NewNullableColumnText(name, compactData, validData)
		s.NoError(err)
		s.Equal(entity.FieldTypeText, column.Type())
		s.Equal(name, column.Name())
		s.Equal(compactData, column.Data())

		column, err = NewNullableColumnText(name, sparseData, validData, WithSparseNullableMode[string](true))
		s.NoError(err)
		fd := column.FieldData()
		s.EqualValues(entity.FieldTypeText, fd.GetType())
		s.Equal(validData, getFieldDataValidData(fd))
		s.Equal(sparseData, fd.GetScalars().GetStringData().GetData())

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnText)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(sparseData, parsed.Data())
			s.Equal(entity.FieldTypeText, parsed.Type())
		}

		_, err = NewNullableColumnText(name, compactData, []bool{false, false})
		s.Error(err)
	})
}

func (s *NullableScalarSuite) TestVectorSlice() {
	sparse, err := entity.NewSliceSparseEmbedding([]uint32{0}, []float32{1})
	s.Require().NoError(err)

	cases := []struct {
		name   string
		create func() (Column, error)
	}{
		{
			name: "float vector",
			create: func() (Column, error) {
				return NewNullableColumnFloatVector("vector", 2, [][]float32{{1, 2}}, []bool{false, true})
			},
		},
		{
			name: "binary vector",
			create: func() (Column, error) {
				return NewNullableColumnBinaryVector("vector", 8, [][]byte{{1}}, []bool{false, true})
			},
		},
		{
			name: "float16 vector",
			create: func() (Column, error) {
				return NewNullableColumnFloat16Vector("vector", 2, [][]byte{{1, 2, 3, 4}}, []bool{false, true})
			},
		},
		{
			name: "bfloat16 vector",
			create: func() (Column, error) {
				return NewNullableColumnBFloat16Vector("vector", 2, [][]byte{{1, 2, 3, 4}}, []bool{false, true})
			},
		},
		{
			name: "int8 vector",
			create: func() (Column, error) {
				return NewNullableColumnInt8Vector("vector", 2, [][]int8{{1, 2}}, []bool{false, true})
			},
		},
		{
			name: "sparse vector",
			create: func() (Column, error) {
				return NewNullableColumnSparseFloatVector("vector", []entity.SparseEmbedding{sparse}, []bool{false, true})
			},
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			vector, err := tc.create()
			s.Require().NoError(err)
			s.Require().NoError(vector.ValidateNullable())

			var sliced Column
			s.NotPanics(func() {
				sliced = vector.Slice(0, vector.Len())
			})
			s.Require().NotNil(sliced)
			s.Equal(2, sliced.Len())
			isNull, err := sliced.IsNull(0)
			s.Require().NoError(err)
			s.True(isNull)
			_, err = sliced.Get(1)
			s.NoError(err)
		})
	}
}

type appendValueCase struct {
	input    any
	expected any
}

type appendValueColumnCase struct {
	name   string
	create func() Column
	cases  []appendValueCase
}

func appendValueColumnCases() []appendValueColumnCase {
	type jsonStruct struct {
		A int `json:"a"`
	}
	mapJSON, _ := json.Marshal(map[string]any{"a": 1})
	structJSON, _ := json.Marshal(jsonStruct{A: 2})
	fp32 := []float32{1, 2}

	return []appendValueColumnCase{
		{
			name:   "json",
			create: func() Column { return NewColumnJSONBytes("json", nil) },
			cases: []appendValueCase{
				{input: []byte(`{"a":0}`), expected: []byte(`{"a":0}`)},
				{input: map[string]any{"a": 1}, expected: mapJSON},
				{input: jsonStruct{A: 2}, expected: structJSON},
				{input: &jsonStruct{A: 2}, expected: structJSON},
			},
		},
		{
			name:   "float_vector",
			create: func() Column { return NewColumnFloatVector("vector", 2, nil) },
			cases: []appendValueCase{
				{input: entity.FloatVector(fp32), expected: entity.FloatVector(fp32)},
				{input: fp32, expected: entity.FloatVector(fp32)},
			},
		},
		{
			name:   "binary_vector",
			create: func() Column { return NewColumnBinaryVector("vector", 8, nil) },
			cases: []appendValueCase{
				{input: entity.BinaryVector([]byte{1}), expected: entity.BinaryVector([]byte{1})},
				{input: []byte{2}, expected: entity.BinaryVector([]byte{2})},
			},
		},
		{
			name:   "float16_vector",
			create: func() Column { return NewColumnFloat16Vector("vector", 2, nil) },
			cases: []appendValueCase{
				{input: entity.Float16Vector([]byte{1, 2, 3, 4}), expected: entity.Float16Vector([]byte{1, 2, 3, 4})},
				{input: []byte{5, 6, 7, 8}, expected: entity.Float16Vector([]byte{5, 6, 7, 8})},
				{input: entity.FloatVector(fp32), expected: entity.FloatVector(fp32).ToFloat16Vector()},
				{input: fp32, expected: entity.FloatVector(fp32).ToFloat16Vector()},
			},
		},
		{
			name:   "bfloat16_vector",
			create: func() Column { return NewColumnBFloat16Vector("vector", 2, nil) },
			cases: []appendValueCase{
				{input: entity.BFloat16Vector([]byte{1, 2, 3, 4}), expected: entity.BFloat16Vector([]byte{1, 2, 3, 4})},
				{input: []byte{5, 6, 7, 8}, expected: entity.BFloat16Vector([]byte{5, 6, 7, 8})},
				{input: entity.FloatVector(fp32), expected: entity.FloatVector(fp32).ToBFloat16Vector()},
				{input: fp32, expected: entity.FloatVector(fp32).ToBFloat16Vector()},
			},
		},
		{
			name:   "int8_vector",
			create: func() Column { return NewColumnInt8Vector("vector", 2, nil) },
			cases: []appendValueCase{
				{input: entity.Int8Vector([]int8{1, 2}), expected: entity.Int8Vector([]int8{1, 2})},
				{input: []int8{3, 4}, expected: entity.Int8Vector([]int8{3, 4})},
			},
		},
	}
}

// checkNullableAppend appends every case value followed by a null and checks that
// the logical length, validity and payload stay consistent.
func (s *NullableScalarSuite) checkNullableAppend(appendNull func(Column) error) {
	for _, tc := range appendValueColumnCases() {
		s.Run(tc.name, func() {
			col := tc.create()
			col.SetNullable(true)

			// interleave a null between every value to exercise both bookkeeping paths
			for _, c := range tc.cases {
				s.Require().NoError(col.AppendValue(c.input))
				s.Require().NoError(appendNull(col))
			}

			s.Equal(len(tc.cases)*2, col.Len())
			s.NoError(col.ValidateNullable())
			s.Len(col.FieldData().GetValidData(), len(tc.cases)*2)

			for i, c := range tc.cases {
				isNull, err := col.IsNull(i * 2)
				s.NoError(err)
				s.False(isNull, "row %d should be valid", i*2)
				val, err := col.Get(i * 2)
				s.NoError(err)
				s.Equal(c.expected, val)

				isNull, err = col.IsNull(i*2 + 1)
				s.NoError(err)
				s.True(isNull, "row %d should be null", i*2+1)
			}

			sliced := col.Slice(0, col.Len())
			s.Equal(col.Len(), sliced.Len())
			s.NoError(sliced.ValidateNullable())
		})
	}
}

func (s *NullableScalarSuite) TestAppendValueNullableKeepsValidity() {
	// row-based insert path: values via AppendValue, nils via AppendNull
	s.Run("append_null", func() {
		s.checkNullableAppend(func(col Column) error { return col.AppendNull() })
	})
	// AppendValue(nil) shall be treated as a null for nullable columns
	s.Run("append_value_nil", func() {
		s.checkNullableAppend(func(col Column) error { return col.AppendValue(nil) })
	})
}

func (s *NullableScalarSuite) TestAppendValueNonNullable() {
	for _, tc := range appendValueColumnCases() {
		s.Run(tc.name, func() {
			col := tc.create()

			for _, c := range tc.cases {
				s.Require().NoError(col.AppendValue(c.input))
			}
			s.Equal(len(tc.cases), col.Len())

			var err error
			s.NotPanics(func() {
				err = col.AppendValue(nil)
			})
			s.Error(err)
			s.Equal(len(tc.cases), col.Len())

			s.Error(col.AppendValue(true))
			s.Equal(len(tc.cases), col.Len())

			s.Empty(col.FieldData().GetValidData())
			for i, c := range tc.cases {
				val, err := col.Get(i)
				s.NoError(err)
				s.Equal(c.expected, val)
			}
		})
	}
}

func TestNullableScalar(t *testing.T) {
	suite.Run(t, new(NullableScalarSuite))
}
