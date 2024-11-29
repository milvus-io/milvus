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
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/client/v2/entity"
)

type ScalarSuite struct {
	suite.Suite
}

func (s *ScalarSuite) TestBasic() {
	s.Run("column_bool", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []bool{true, false}
		column := NewColumnBool(name, data)
		s.Equal(entity.FieldTypeBool, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		s.Equal(data, fd.GetScalars().GetBoolData().GetData())

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnBool)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeBool, column.Type())
		}
	})

	s.Run("column_int8", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []int8{1, 2, 3}
		column := NewColumnInt8(name, data)
		s.Equal(entity.FieldTypeInt8, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		fdData := fd.GetScalars().GetIntData().GetData()
		for i, row := range data {
			s.EqualValues(row, fdData[i])
		}

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnInt8)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeInt8, column.Type())
		}
	})

	s.Run("column_int16", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []int16{1, 2, 3}
		column := NewColumnInt16(name, data)
		s.Equal(entity.FieldTypeInt16, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		fdData := fd.GetScalars().GetIntData().GetData()
		for i, row := range data {
			s.EqualValues(row, fdData[i])
		}

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnInt16)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeInt16, column.Type())
		}
	})

	s.Run("column_int32", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []int32{1, 2, 3}
		column := NewColumnInt32(name, data)
		s.Equal(entity.FieldTypeInt32, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		s.Equal(data, fd.GetScalars().GetIntData().GetData())

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnInt32)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeInt32, column.Type())
		}
	})

	s.Run("column_int64", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []int64{1, 2, 3}
		column := NewColumnInt64(name, data)
		s.Equal(entity.FieldTypeInt64, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		s.Equal(data, fd.GetScalars().GetLongData().GetData())

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnInt64)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeInt64, column.Type())
		}
	})

	s.Run("column_float", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []float32{1.1, 2.2, 3.3}
		column := NewColumnFloat(name, data)
		s.Equal(entity.FieldTypeFloat, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		s.Equal(data, fd.GetScalars().GetFloatData().GetData())

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnFloat)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeFloat, column.Type())
		}
	})

	s.Run("column_double", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []float64{1.1, 2.2, 3.3}
		column := NewColumnDouble(name, data)
		s.Equal(entity.FieldTypeDouble, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		s.Equal(data, fd.GetScalars().GetDoubleData().GetData())

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnDouble)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeDouble, column.Type())
		}
	})

	s.Run("column_varchar", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []string{"a", "b", "c"}
		column := NewColumnVarChar(name, data)
		s.Equal(entity.FieldTypeVarChar, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		s.Equal(data, fd.GetScalars().GetStringData().GetData())

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnVarChar)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeVarChar, column.Type())
		}
	})
}

func (s *ScalarSuite) TestSlice() {
	n := 100
	s.Run("column_bool", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := make([]bool, 0, n)
		for i := 0; i < 100; i++ {
			data = append(data, rand.Int()%2 == 0)
		}
		column := NewColumnBool(name, data)

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnBool)
		if s.True(ok) {
			s.Equal(column.Type(), slicedColumn.Type())
			s.Equal(data[:l], slicedColumn.Data())
		}
	})

	s.Run("column_int8", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := make([]int8, 0, n)
		for i := 0; i < 100; i++ {
			data = append(data, int8(rand.Intn(math.MaxInt8)))
		}
		column := NewColumnInt8(name, data)

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnInt8)
		if s.True(ok) {
			s.Equal(column.Type(), slicedColumn.Type())
			s.Equal(data[:l], slicedColumn.Data())
		}
	})

	s.Run("column_int16", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := make([]int16, 0, n)
		for i := 0; i < 100; i++ {
			data = append(data, int16(rand.Intn(math.MaxInt16)))
		}
		column := NewColumnInt16(name, data)

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnInt16)
		if s.True(ok) {
			s.Equal(column.Type(), slicedColumn.Type())
			s.Equal(data[:l], slicedColumn.Data())
		}
	})

	s.Run("column_int32", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := make([]int32, 0, n)
		for i := 0; i < 100; i++ {
			data = append(data, rand.Int31())
		}
		column := NewColumnInt32(name, data)

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnInt32)
		if s.True(ok) {
			s.Equal(column.Type(), slicedColumn.Type())
			s.Equal(data[:l], slicedColumn.Data())
		}
	})

	s.Run("column_int64", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := make([]int64, 0, n)
		for i := 0; i < 100; i++ {
			data = append(data, rand.Int63())
		}
		column := NewColumnInt64(name, data)

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnInt64)
		if s.True(ok) {
			s.Equal(column.Type(), slicedColumn.Type())
			s.Equal(data[:l], slicedColumn.Data())
		}
	})

	s.Run("column_float", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := make([]float32, 0, n)
		for i := 0; i < 100; i++ {
			data = append(data, rand.Float32())
		}
		column := NewColumnFloat(name, data)

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnFloat)
		if s.True(ok) {
			s.Equal(column.Type(), slicedColumn.Type())
			s.Equal(data[:l], slicedColumn.Data())
		}
	})

	s.Run("column_double", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := make([]float64, 0, n)
		for i := 0; i < 100; i++ {
			data = append(data, rand.Float64())
		}
		column := NewColumnDouble(name, data)

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnDouble)
		if s.True(ok) {
			s.Equal(column.Type(), slicedColumn.Type())
			s.Equal(data[:l], slicedColumn.Data())
		}
	})

	s.Run("column_varchar", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := make([]string, 0, n)
		for i := 0; i < 100; i++ {
			data = append(data, fmt.Sprintf("%d", rand.Int()))
		}
		column := NewColumnVarChar(name, data)

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnVarChar)
		if s.True(ok) {
			s.Equal(column.Type(), slicedColumn.Type())
			s.Equal(data[:l], slicedColumn.Data())
		}
	})
}

func TestScalarColumn(t *testing.T) {
	suite.Run(t, new(ScalarSuite))
}
