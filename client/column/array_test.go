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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

type ArraySuite struct {
	suite.Suite
}

func (s *ArraySuite) TestBasic() {
	s.Run("bool_array", func() {
		data := [][]bool{
			{true, false},
			{false, true},
		}
		name := fmt.Sprintf("field_%d", rand.Intn(100))

		column := NewColumnBoolArray(name, data)
		s.Equal(name, column.Name())
		s.Equal(entity.FieldTypeArray, column.Type())
		s.Equal(entity.FieldTypeBool, column.ElementType())

		fd := column.FieldData()
		arrayData := fd.GetScalars().GetArrayData()
		s.Equal(schemapb.DataType_Bool, arrayData.GetElementType())
		for i, row := range data {
			sf := arrayData.GetData()[i]
			s.Equal(row, sf.GetBoolData().GetData())
		}

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnBoolArray)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeArray, column.Type())
			s.Equal(entity.FieldTypeBool, column.ElementType())
			s.Equal(data, parsed.Data())
		}
	})

	s.Run("int8_array", func() {
		data := [][]int8{
			{1, 2},
			{3, 4},
		}
		name := fmt.Sprintf("field_%d", rand.Intn(100))

		column := NewColumnInt8Array(name, data)
		s.Equal(name, column.Name())
		s.Equal(entity.FieldTypeArray, column.Type())
		s.Equal(entity.FieldTypeInt8, column.ElementType())

		fd := column.FieldData()
		arrayData := fd.GetScalars().GetArrayData()
		s.Equal(schemapb.DataType_Int8, arrayData.GetElementType())
		for i, row := range data {
			sf := arrayData.GetData()[i]
			for j, item := range row {
				s.EqualValues(item, sf.GetIntData().GetData()[j])
			}
		}

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnInt8Array)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeArray, column.Type())
			s.Equal(entity.FieldTypeInt8, column.ElementType())
			s.Equal(data, parsed.Data())
		}
	})

	s.Run("int16_array", func() {
		data := [][]int16{
			{1, 2},
			{3, 4},
		}
		name := fmt.Sprintf("field_%d", rand.Intn(100))

		column := NewColumnInt16Array(name, data)
		s.Equal(name, column.Name())
		s.Equal(entity.FieldTypeArray, column.Type())
		s.Equal(entity.FieldTypeInt16, column.ElementType())

		fd := column.FieldData()
		arrayData := fd.GetScalars().GetArrayData()
		s.Equal(schemapb.DataType_Int16, arrayData.GetElementType())
		for i, row := range data {
			sf := arrayData.GetData()[i]
			for j, item := range row {
				s.EqualValues(item, sf.GetIntData().GetData()[j])
			}
		}

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnInt16Array)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeArray, column.Type())
			s.Equal(entity.FieldTypeInt16, column.ElementType())
			s.Equal(data, parsed.Data())
		}
	})

	s.Run("int32_array", func() {
		data := [][]int32{
			{1, 2},
			{3, 4},
		}
		name := fmt.Sprintf("field_%d", rand.Intn(100))

		column := NewColumnInt32Array(name, data)
		s.Equal(name, column.Name())
		s.Equal(entity.FieldTypeArray, column.Type())
		s.Equal(entity.FieldTypeInt32, column.ElementType())

		fd := column.FieldData()
		arrayData := fd.GetScalars().GetArrayData()
		s.Equal(schemapb.DataType_Int32, arrayData.GetElementType())
		for i, row := range data {
			sf := arrayData.GetData()[i]
			s.Equal(row, sf.GetIntData().GetData())
		}

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnInt32Array)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeArray, column.Type())
			s.Equal(entity.FieldTypeInt32, column.ElementType())
			s.Equal(data, parsed.Data())
		}
	})

	s.Run("int64_array", func() {
		data := [][]int64{
			{1, 2},
			{3, 4},
		}
		name := fmt.Sprintf("field_%d", rand.Intn(100))

		column := NewColumnInt64Array(name, data)
		s.Equal(name, column.Name())
		s.Equal(entity.FieldTypeArray, column.Type())
		s.Equal(entity.FieldTypeInt64, column.ElementType())

		fd := column.FieldData()
		arrayData := fd.GetScalars().GetArrayData()
		s.Equal(schemapb.DataType_Int64, arrayData.GetElementType())
		for i, row := range data {
			sf := arrayData.GetData()[i]
			s.Equal(row, sf.GetLongData().GetData())
		}

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnInt64Array)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeArray, column.Type())
			s.Equal(entity.FieldTypeInt64, column.ElementType())
			s.Equal(data, parsed.Data())
		}
	})

	s.Run("float_array", func() {
		data := [][]float32{
			{0.1, 0.2},
			{1.3, 1.4},
		}
		name := fmt.Sprintf("field_%d", rand.Intn(100))

		column := NewColumnFloatArray(name, data)
		s.Equal(name, column.Name())
		s.Equal(entity.FieldTypeArray, column.Type())
		s.Equal(entity.FieldTypeFloat, column.ElementType())

		fd := column.FieldData()
		arrayData := fd.GetScalars().GetArrayData()
		s.Equal(schemapb.DataType_Float, arrayData.GetElementType())
		for i, row := range data {
			sf := arrayData.GetData()[i]
			s.Equal(row, sf.GetFloatData().GetData())
		}

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnFloatArray)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeArray, column.Type())
			s.Equal(entity.FieldTypeFloat, column.ElementType())
			s.Equal(data, parsed.Data())
		}
	})

	s.Run("double_array", func() {
		data := [][]float64{
			{0.1, 0.2},
			{1.3, 1.4},
		}
		name := fmt.Sprintf("field_%d", rand.Intn(100))

		column := NewColumnDoubleArray(name, data)
		s.Equal(name, column.Name())
		s.Equal(entity.FieldTypeArray, column.Type())
		s.Equal(entity.FieldTypeDouble, column.ElementType())

		fd := column.FieldData()
		arrayData := fd.GetScalars().GetArrayData()
		s.Equal(schemapb.DataType_Double, arrayData.GetElementType())
		for i, row := range data {
			sf := arrayData.GetData()[i]
			s.Equal(row, sf.GetDoubleData().GetData())
		}

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnDoubleArray)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeArray, column.Type())
			s.Equal(entity.FieldTypeDouble, column.ElementType())
			s.Equal(data, parsed.Data())
		}
	})

	s.Run("varchar_array", func() {
		data := [][]string{
			{"abc", "def"},
			{"xyz"},
		}

		name := fmt.Sprintf("field_%d", rand.Intn(100))

		column := NewColumnVarCharArray(name, data)
		s.Equal(name, column.Name())
		s.Equal(entity.FieldTypeArray, column.Type())
		s.Equal(entity.FieldTypeVarChar, column.ElementType())

		fd := column.FieldData()
		arrayData := fd.GetScalars().GetArrayData()
		s.Equal(schemapb.DataType_VarChar, arrayData.GetElementType())
		for i, row := range data {
			sf := arrayData.GetData()[i]
			s.Equal(row, sf.GetStringData().GetData())
		}

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnVarCharArray)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeArray, column.Type())
			s.Equal(entity.FieldTypeVarChar, column.ElementType())
			s.Equal(data, parsed.Data())
		}
	})
}

func TestArrays(t *testing.T) {
	suite.Run(t, new(ArraySuite))
}
