/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package chain

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/suite"
)

type BaseOpTestSuite struct {
	suite.Suite
	pool *memory.CheckedAllocator
}

func (s *BaseOpTestSuite) SetupTest() {
	s.pool = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func (s *BaseOpTestSuite) TearDownTest() {
	s.pool.AssertSize(s.T(), 0)
}

func TestBaseOpTestSuite(t *testing.T) {
	suite.Run(t, new(BaseOpTestSuite))
}

func (s *BaseOpTestSuite) TestBaseOpInputsOutputs() {
	op := BaseOp{
		inputs:  []string{"a", "b"},
		outputs: []string{"c"},
	}
	s.Equal([]string{"a", "b"}, op.Inputs())
	s.Equal([]string{"c"}, op.Outputs())
}

func (s *BaseOpTestSuite) TestBaseOpNilInputsOutputs() {
	op := BaseOp{}
	s.Nil(op.Inputs())
	s.Nil(op.Outputs())
}

// =============================================================================
// dispatchPickByIndices tests
// =============================================================================

func (s *BaseOpTestSuite) TestDispatchPickByIndicesInt64() {
	b := array.NewInt64Builder(s.pool)
	b.AppendValues([]int64{10, 20, 30, 40, 50}, nil)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	result, err := dispatchPickByIndices(s.pool, arr, []int{2, 0, 4})
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(3, result.Len())
	r := result.(*array.Int64)
	s.Equal(int64(30), r.Value(0))
	s.Equal(int64(10), r.Value(1))
	s.Equal(int64(50), r.Value(2))
}

func (s *BaseOpTestSuite) TestDispatchPickByIndicesFloat32() {
	b := array.NewFloat32Builder(s.pool)
	b.AppendValues([]float32{1.1, 2.2, 3.3}, nil)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	result, err := dispatchPickByIndices(s.pool, arr, []int{1, 2})
	s.Require().NoError(err)
	defer result.Release()

	r := result.(*array.Float32)
	s.InDelta(2.2, float64(r.Value(0)), 1e-5)
	s.InDelta(3.3, float64(r.Value(1)), 1e-5)
}

func (s *BaseOpTestSuite) TestDispatchPickByIndicesFloat64() {
	b := array.NewFloat64Builder(s.pool)
	b.AppendValues([]float64{1.1, 2.2, 3.3}, nil)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	result, err := dispatchPickByIndices(s.pool, arr, []int{0})
	s.Require().NoError(err)
	defer result.Release()

	r := result.(*array.Float64)
	s.InDelta(1.1, r.Value(0), 1e-9)
}

func (s *BaseOpTestSuite) TestDispatchPickByIndicesString() {
	b := array.NewStringBuilder(s.pool)
	b.AppendValues([]string{"hello", "world", "test"}, nil)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	result, err := dispatchPickByIndices(s.pool, arr, []int{2, 0})
	s.Require().NoError(err)
	defer result.Release()

	r := result.(*array.String)
	s.Equal("test", r.Value(0))
	s.Equal("hello", r.Value(1))
}

func (s *BaseOpTestSuite) TestDispatchPickByIndicesBoolean() {
	b := array.NewBooleanBuilder(s.pool)
	b.AppendValues([]bool{true, false, true}, nil)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	result, err := dispatchPickByIndices(s.pool, arr, []int{1, 2})
	s.Require().NoError(err)
	defer result.Release()

	r := result.(*array.Boolean)
	s.False(r.Value(0))
	s.True(r.Value(1))
}

func (s *BaseOpTestSuite) TestDispatchPickByIndicesInt8() {
	b := array.NewInt8Builder(s.pool)
	b.AppendValues([]int8{1, 2, 3}, nil)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	result, err := dispatchPickByIndices(s.pool, arr, []int{2})
	s.Require().NoError(err)
	defer result.Release()

	r := result.(*array.Int8)
	s.Equal(int8(3), r.Value(0))
}

func (s *BaseOpTestSuite) TestDispatchPickByIndicesInt16() {
	b := array.NewInt16Builder(s.pool)
	b.AppendValues([]int16{100, 200}, nil)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	result, err := dispatchPickByIndices(s.pool, arr, []int{0, 1})
	s.Require().NoError(err)
	defer result.Release()

	r := result.(*array.Int16)
	s.Equal(int16(100), r.Value(0))
	s.Equal(int16(200), r.Value(1))
}

func (s *BaseOpTestSuite) TestDispatchPickByIndicesInt32() {
	b := array.NewInt32Builder(s.pool)
	b.AppendValues([]int32{10, 20, 30}, nil)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	result, err := dispatchPickByIndices(s.pool, arr, []int{1})
	s.Require().NoError(err)
	defer result.Release()

	r := result.(*array.Int32)
	s.Equal(int32(20), r.Value(0))
}

func (s *BaseOpTestSuite) TestDispatchPickByIndicesUint8() {
	b := array.NewUint8Builder(s.pool)
	b.AppendValues([]uint8{1, 2, 3}, nil)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	result, err := dispatchPickByIndices(s.pool, arr, []int{0, 2})
	s.Require().NoError(err)
	defer result.Release()

	r := result.(*array.Uint8)
	s.Equal(uint8(1), r.Value(0))
	s.Equal(uint8(3), r.Value(1))
}

func (s *BaseOpTestSuite) TestDispatchPickByIndicesUint16() {
	b := array.NewUint16Builder(s.pool)
	b.AppendValues([]uint16{10, 20}, nil)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	result, err := dispatchPickByIndices(s.pool, arr, []int{1})
	s.Require().NoError(err)
	defer result.Release()

	r := result.(*array.Uint16)
	s.Equal(uint16(20), r.Value(0))
}

func (s *BaseOpTestSuite) TestDispatchPickByIndicesUint32() {
	b := array.NewUint32Builder(s.pool)
	b.AppendValues([]uint32{100, 200, 300}, nil)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	result, err := dispatchPickByIndices(s.pool, arr, []int{2})
	s.Require().NoError(err)
	defer result.Release()

	r := result.(*array.Uint32)
	s.Equal(uint32(300), r.Value(0))
}

func (s *BaseOpTestSuite) TestDispatchPickByIndicesUint64() {
	b := array.NewUint64Builder(s.pool)
	b.AppendValues([]uint64{1000, 2000, 3000}, nil)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	result, err := dispatchPickByIndices(s.pool, arr, []int{0, 1})
	s.Require().NoError(err)
	defer result.Release()

	r := result.(*array.Uint64)
	s.Equal(uint64(1000), r.Value(0))
	s.Equal(uint64(2000), r.Value(1))
}

func (s *BaseOpTestSuite) TestDispatchPickByIndicesOutOfBounds() {
	b := array.NewInt64Builder(s.pool)
	b.AppendValues([]int64{1, 2, 3}, nil)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	_, err := dispatchPickByIndices(s.pool, arr, []int{5})
	s.Error(err)
	s.Contains(err.Error(), "index out of bounds")
}

func (s *BaseOpTestSuite) TestDispatchPickByIndicesNegativeIndex() {
	b := array.NewInt64Builder(s.pool)
	b.AppendValues([]int64{1, 2, 3}, nil)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	_, err := dispatchPickByIndices(s.pool, arr, []int{-1})
	s.Error(err)
	s.Contains(err.Error(), "index out of bounds")
}

func (s *BaseOpTestSuite) TestDispatchPickByIndicesWithNulls() {
	b := array.NewInt64Builder(s.pool)
	b.Append(10)
	b.AppendNull()
	b.Append(30)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	result, err := dispatchPickByIndices(s.pool, arr, []int{0, 1, 2})
	s.Require().NoError(err)
	defer result.Release()

	r := result.(*array.Int64)
	s.Equal(3, r.Len())
	s.False(r.IsNull(0))
	s.Equal(int64(10), r.Value(0))
	s.True(r.IsNull(1))
	s.False(r.IsNull(2))
	s.Equal(int64(30), r.Value(2))
}

func (s *BaseOpTestSuite) TestDispatchPickByIndicesEmptyIndices() {
	b := array.NewInt64Builder(s.pool)
	b.AppendValues([]int64{1, 2, 3}, nil)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	result, err := dispatchPickByIndices(s.pool, arr, []int{})
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(0, result.Len())
}

// =============================================================================
// sliceArray tests
// =============================================================================

func (s *BaseOpTestSuite) TestSliceArray() {
	b := array.NewInt64Builder(s.pool)
	b.AppendValues([]int64{10, 20, 30, 40, 50}, nil)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	result, err := sliceArray(arr, 1, 4)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(3, result.Len())
	r := result.(*array.Int64)
	s.Equal(int64(20), r.Value(0))
	s.Equal(int64(30), r.Value(1))
	s.Equal(int64(40), r.Value(2))
}

func (s *BaseOpTestSuite) TestSliceArrayFull() {
	b := array.NewInt64Builder(s.pool)
	b.AppendValues([]int64{1, 2, 3}, nil)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	result, err := sliceArray(arr, 0, 3)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(3, result.Len())
}

func (s *BaseOpTestSuite) TestSliceArrayEmpty() {
	b := array.NewInt64Builder(s.pool)
	b.AppendValues([]int64{1, 2, 3}, nil)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	result, err := sliceArray(arr, 2, 2)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(0, result.Len())
}

func (s *BaseOpTestSuite) TestSliceArrayEmptyInput() {
	b := array.NewInt64Builder(s.pool)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	result, err := sliceArray(arr, 0, 0)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(0, result.Len())
}
