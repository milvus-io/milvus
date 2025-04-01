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

	"github.com/milvus-io/milvus/client/v2/entity"
)

type NullableScalarSuite struct {
	suite.Suite
}

func (s *NullableScalarSuite) TestBasic() {
	s.Run("nullable_bool", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []bool{false}
		validData := []bool{true, false}
		column, err := NewNullableColumnBool(name, data, validData)
		s.NoError(err)
		s.Equal(entity.FieldTypeBool, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())
		for i := 0; i < len(validData); i++ {
			r, err := column.IsNull(i)
			s.NoError(err)
			s.Equal(validData[i], !r)
		}

		fd := column.FieldData()
		s.Equal(validData, fd.GetValidData())
		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnBool)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeBool, column.Type())
		}

		_, err = NewNullableColumnBool(name, data, []bool{false, false})
		s.Error(err)
	})

	s.Run("nullable_int8", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []int8{1, 3}
		validData := []bool{true, false, true}
		column, err := NewNullableColumnInt8(name, data, validData)
		s.NoError(err)
		s.Equal(entity.FieldTypeInt8, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())
		for i := 0; i < len(validData); i++ {
			r, err := column.IsNull(i)
			s.NoError(err)
			s.Equal(validData[i], !r)
		}

		fd := column.FieldData()
		s.Equal(validData, fd.GetValidData())
		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnInt8)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeInt8, column.Type())
		}

		_, err = NewNullableColumnInt8(name, data, []bool{false, false})
		s.Error(err)
	})

	s.Run("nullable_int16", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []int16{1, 3}
		validData := []bool{true, false, true}
		column, err := NewNullableColumnInt16(name, data, validData)
		s.NoError(err)
		s.Equal(entity.FieldTypeInt16, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())
		for i := 0; i < len(validData); i++ {
			r, err := column.IsNull(i)
			s.NoError(err)
			s.Equal(validData[i], !r)
		}

		fd := column.FieldData()
		s.Equal(validData, fd.GetValidData())
		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnInt16)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeInt16, column.Type())
		}

		_, err = NewNullableColumnInt16(name, data, []bool{false, false})
		s.Error(err)
	})

	s.Run("nullable_int32", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []int32{1, 3}
		validData := []bool{true, false, true}
		column, err := NewNullableColumnInt32(name, data, validData)
		s.NoError(err)
		s.Equal(entity.FieldTypeInt32, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())
		for i := 0; i < len(validData); i++ {
			r, err := column.IsNull(i)
			s.NoError(err)
			s.Equal(validData[i], !r)
		}

		fd := column.FieldData()
		s.Equal(validData, fd.GetValidData())
		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnInt32)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeInt32, column.Type())
		}

		_, err = NewNullableColumnInt32(name, data, []bool{false, false})
		s.Error(err)
	})

	s.Run("nullable_int64", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []int64{1, 3}
		validData := []bool{true, false, true}
		column, err := NewNullableColumnInt64(name, data, validData)
		s.NoError(err)
		s.Equal(entity.FieldTypeInt64, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())
		for i := 0; i < len(validData); i++ {
			r, err := column.IsNull(i)
			s.NoError(err)
			s.Equal(validData[i], !r)
		}

		fd := column.FieldData()
		s.Equal(validData, fd.GetValidData())
		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnInt64)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeInt64, column.Type())
		}

		_, err = NewNullableColumnInt64(name, data, []bool{false, false})
		s.Error(err)
	})

	s.Run("nullable_float", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []float32{0.1, 0.3}
		validData := []bool{true, false, true}
		column, err := NewNullableColumnFloat(name, data, validData)
		s.NoError(err)
		s.Equal(entity.FieldTypeFloat, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())
		for i := 0; i < len(validData); i++ {
			r, err := column.IsNull(i)
			s.NoError(err)
			s.Equal(validData[i], !r)
		}

		fd := column.FieldData()
		s.Equal(validData, fd.GetValidData())
		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnFloat)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeFloat, column.Type())
		}

		_, err = NewNullableColumnFloat(name, data, []bool{false, false})
		s.Error(err)
	})

	s.Run("nullable_double", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		data := []float64{0.1, 0.3}
		validData := []bool{true, false, true}
		column, err := NewNullableColumnDouble(name, data, validData)
		s.NoError(err)
		s.Equal(entity.FieldTypeDouble, column.Type())
		s.Equal(name, column.Name())
		s.Equal(data, column.Data())
		for i := 0; i < len(validData); i++ {
			r, err := column.IsNull(i)
			s.NoError(err)
			s.Equal(validData[i], !r)
		}

		fd := column.FieldData()
		s.Equal(validData, fd.GetValidData())
		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnDouble)
		if s.True(ok) {
			s.Equal(name, parsed.Name())
			s.Equal(data, parsed.Data())
			s.Equal(entity.FieldTypeDouble, column.Type())
		}

		_, err = NewNullableColumnDouble(name, data, []bool{false, false})
		s.Error(err)
	})
}

func TestNullableScalar(t *testing.T) {
	suite.Run(t, new(NullableScalarSuite))
}
