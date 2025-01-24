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

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type VectorSuite struct {
	suite.Suite
}

func (s *VectorSuite) TestBasic() {
	s.Run("float_vector", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		n := 3
		dim := rand.Intn(10) + 2
		data := make([][]float32, 0, n)
		for i := 0; i < n; i++ {
			row := lo.RepeatBy(dim, func(i int) float32 {
				return rand.Float32()
			})
			data = append(data, row)
		}
		column := NewColumnFloatVector(name, dim, data)
		s.Equal(entity.FieldTypeFloatVector, column.Type())
		s.Equal(name, column.Name())
		s.Equal(lo.Map(data, func(row []float32, _ int) entity.FloatVector { return entity.FloatVector(row) }), column.Data())
		s.Equal(dim, column.Dim())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		s.Equal(lo.Flatten(data), fd.GetVectors().GetFloatVector().GetData())

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnFloatVector)
		if s.True(ok) {
			s.Equal(entity.FieldTypeFloatVector, parsed.Type())
			s.Equal(name, parsed.Name())
			s.Equal(lo.Map(data, func(row []float32, _ int) entity.FloatVector { return entity.FloatVector(row) }), parsed.Data())
			s.Equal(dim, parsed.Dim())
		}
	})

	s.Run("binary_vector", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		n := 3
		dim := (rand.Intn(10) + 1) * 8
		data := make([][]byte, 0, n)
		for i := 0; i < n; i++ {
			row := lo.RepeatBy(dim/8, func(i int) byte {
				return byte(rand.Intn(math.MaxUint8))
			})
			data = append(data, row)
		}
		column := NewColumnBinaryVector(name, dim, data)
		s.Equal(entity.FieldTypeBinaryVector, column.Type())
		s.Equal(name, column.Name())
		s.Equal(lo.Map(data, func(row []byte, _ int) entity.BinaryVector { return entity.BinaryVector(row) }), column.Data())
		s.Equal(dim, column.Dim())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		s.Equal(lo.Flatten(data), fd.GetVectors().GetBinaryVector())

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnBinaryVector)
		if s.True(ok) {
			s.Equal(entity.FieldTypeBinaryVector, parsed.Type())
			s.Equal(name, parsed.Name())
			s.Equal(lo.Map(data, func(row []byte, _ int) entity.BinaryVector { return entity.BinaryVector(row) }), parsed.Data())
			s.Equal(dim, parsed.Dim())
		}
	})

	s.Run("fp16_vector", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		n := 3
		dim := rand.Intn(10) + 1
		data := make([][]byte, 0, n)
		for i := 0; i < n; i++ {
			row := lo.RepeatBy(dim*2, func(i int) byte {
				return byte(rand.Intn(math.MaxUint8))
			})
			data = append(data, row)
		}
		column := NewColumnFloat16Vector(name, dim, data)
		s.Equal(entity.FieldTypeFloat16Vector, column.Type())
		s.Equal(name, column.Name())
		s.Equal(lo.Map(data, func(row []byte, _ int) entity.Float16Vector { return entity.Float16Vector(row) }), column.Data())
		s.Equal(dim, column.Dim())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		s.Equal(lo.Flatten(data), fd.GetVectors().GetFloat16Vector())

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnFloat16Vector)
		if s.True(ok) {
			s.Equal(entity.FieldTypeFloat16Vector, parsed.Type())
			s.Equal(name, parsed.Name())
			s.Equal(lo.Map(data, func(row []byte, _ int) entity.Float16Vector { return entity.Float16Vector(row) }), parsed.Data())
			s.Equal(dim, parsed.Dim())
		}
	})

	s.Run("bf16_vector", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		n := 3
		dim := rand.Intn(10) + 1
		data := make([][]byte, 0, n)
		for i := 0; i < n; i++ {
			row := lo.RepeatBy(dim*2, func(i int) byte {
				return byte(rand.Intn(math.MaxUint8))
			})
			data = append(data, row)
		}
		column := NewColumnBFloat16Vector(name, dim, data)
		s.Equal(entity.FieldTypeBFloat16Vector, column.Type())
		s.Equal(name, column.Name())
		s.Equal(lo.Map(data, func(row []byte, _ int) entity.BFloat16Vector { return entity.BFloat16Vector(row) }), column.Data())
		s.Equal(dim, column.Dim())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		s.Equal(lo.Flatten(data), fd.GetVectors().GetBfloat16Vector())

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnBFloat16Vector)
		if s.True(ok) {
			s.Equal(entity.FieldTypeBFloat16Vector, parsed.Type())
			s.Equal(name, parsed.Name())
			s.Equal(lo.Map(data, func(row []byte, _ int) entity.BFloat16Vector { return entity.BFloat16Vector(row) }), parsed.Data())
			s.Equal(dim, parsed.Dim())
		}
	})

	s.Run("fp32 <-> fp16/bf16 vector conversion", func() {
		dim := 3
		data := [][]float32{{0.1, 0.2, 0.3}, {0.4, 0.5, 0.6}, {0.7, 0.8, 0.9}, {1.0, 1.1, 1.2}}

		fp16Vector := NewColumnFloat16VectorFromFp32Vector("fp16_vector", dim, data[:2])
		fp16Vector.AppendValue(data[2])
		fp16Vector.AppendValue(data[3])
		for i, vec := range fp16Vector.Data() {
			fp32Vector := vec.ToFloat32Vector()
			s.Equal(dim, len(fp32Vector))
			for j := 0; j < dim; j++ {
				s.InDelta(data[i][j], fp32Vector[j], 7e-3)
			}
		}

		bf16Vector := NewColumnBFloat16VectorFromFp32Vector("bf16_vector", dim, data[:2])
		bf16Vector.AppendValue(data[2])
		bf16Vector.AppendValue(data[3])
		for i, vec := range bf16Vector.Data() {
			fp32Vector := vec.ToFloat32Vector()
			s.Equal(dim, len(fp32Vector))
			for j := 0; j < dim; j++ {
				s.InDelta(data[i][j], fp32Vector[j], 7e-3)
			}
		}
	})

	s.Run("int8_vector", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		n := 3
		dim := rand.Intn(10) + 2
		data := make([][]int8, 0, n)
		for i := 0; i < n; i++ {
			row := lo.RepeatBy(dim, func(i int) int8 {
				return int8(rand.Intn(256) - 128)
			})
			data = append(data, row)
		}
		column := NewColumnInt8Vector(name, dim, data)
		s.Equal(entity.FieldTypeInt8Vector, column.Type())
		s.Equal(name, column.Name())
		s.Equal(lo.Map(data, func(row []int8, _ int) entity.Int8Vector { return entity.Int8Vector(row) }), column.Data())
		s.Equal(dim, column.Dim())

		fd := column.FieldData()
		s.Equal(name, fd.GetFieldName())
		s.Equal(typeutil.Int8ArrayToBytes(lo.Flatten(data)), fd.GetVectors().GetInt8Vector())

		result, err := FieldDataColumn(fd, 0, -1)
		s.NoError(err)
		parsed, ok := result.(*ColumnInt8Vector)
		if s.True(ok) {
			s.Equal(entity.FieldTypeInt8Vector, parsed.Type())
			s.Equal(name, parsed.Name())
			s.Equal(lo.Map(data, func(row []int8, _ int) entity.Int8Vector { return entity.Int8Vector(row) }), parsed.Data())
			s.Equal(dim, parsed.Dim())
		}
	})
}

func (s *VectorSuite) TestSlice() {
	s.Run("float_vector", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		n := 100
		dim := rand.Intn(10) + 2
		data := make([][]float32, 0, n)
		for i := 0; i < n; i++ {
			row := lo.RepeatBy(dim, func(i int) float32 {
				return rand.Float32()
			})
			data = append(data, row)
		}
		column := NewColumnFloatVector(name, dim, data)

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnFloatVector)
		if s.True(ok) {
			s.Equal(dim, slicedColumn.Dim())
			s.Equal(lo.Map(data[:l], func(row []float32, _ int) entity.FloatVector { return entity.FloatVector(row) }), slicedColumn.Data())
		}
	})

	s.Run("binary_vector", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		n := 100
		dim := (rand.Intn(10) + 1) * 8
		data := make([][]byte, 0, n)
		for i := 0; i < n; i++ {
			row := lo.RepeatBy(dim/8, func(i int) byte {
				return byte(rand.Intn(math.MaxUint8))
			})
			data = append(data, row)
		}
		column := NewColumnBinaryVector(name, dim, data)

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnBinaryVector)
		if s.True(ok) {
			s.Equal(dim, slicedColumn.Dim())
			s.Equal(lo.Map(data[:l], func(row []byte, _ int) entity.BinaryVector { return entity.BinaryVector(row) }), slicedColumn.Data())
		}
	})

	s.Run("fp16_vector", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		n := 3
		dim := rand.Intn(10) + 1
		data := make([][]byte, 0, n)
		for i := 0; i < n; i++ {
			row := lo.RepeatBy(dim*2, func(i int) byte {
				return byte(rand.Intn(math.MaxUint8))
			})
			data = append(data, row)
		}
		column := NewColumnFloat16Vector(name, dim, data)

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnFloat16Vector)
		if s.True(ok) {
			s.Equal(dim, slicedColumn.Dim())
			s.Equal(lo.Map(data[:l], func(row []byte, _ int) entity.Float16Vector { return entity.Float16Vector(row) }), slicedColumn.Data())
		}
	})

	s.Run("bf16_vector", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		n := 3
		dim := rand.Intn(10) + 1
		data := make([][]byte, 0, n)
		for i := 0; i < n; i++ {
			row := lo.RepeatBy(dim*2, func(i int) byte {
				return byte(rand.Intn(math.MaxUint8))
			})
			data = append(data, row)
		}
		column := NewColumnBFloat16Vector(name, dim, data)

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnBFloat16Vector)
		if s.True(ok) {
			s.Equal(dim, slicedColumn.Dim())
			s.Equal(lo.Map(data[:l], func(row []byte, _ int) entity.BFloat16Vector { return entity.BFloat16Vector(row) }), slicedColumn.Data())
		}
	})

	s.Run("int8_vector", func() {
		name := fmt.Sprintf("field_%d", rand.Intn(1000))
		n := 100
		dim := rand.Intn(10) + 2
		data := make([][]int8, 0, n)
		for i := 0; i < n; i++ {
			row := lo.RepeatBy(dim, func(i int) int8 {
				return int8(rand.Intn(256) - 128)
			})
			data = append(data, row)
		}
		column := NewColumnInt8Vector(name, dim, data)

		l := rand.Intn(n)
		sliced := column.Slice(0, l)
		slicedColumn, ok := sliced.(*ColumnInt8Vector)
		if s.True(ok) {
			s.Equal(dim, slicedColumn.Dim())
			s.Equal(lo.Map(data[:l], func(row []int8, _ int) entity.Int8Vector { return entity.Int8Vector(row) }), slicedColumn.Data())
		}
	})
}

func TestVectors(t *testing.T) {
	suite.Run(t, new(VectorSuite))
}
