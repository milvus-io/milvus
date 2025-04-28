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
		s.Equal(validData, fd.GetValidData())
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
		s.Equal(validData, fd.GetValidData())
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
		s.Equal(validData, fd.GetValidData())
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
		s.Equal(validData, fd.GetValidData())
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
		s.Equal(validData, fd.GetValidData())
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
		s.Equal(validData, fd.GetValidData())
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
		s.Equal(validData, fd.GetValidData())
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
}

func TestNullableScalar(t *testing.T) {
	suite.Run(t, new(NullableScalarSuite))
}
