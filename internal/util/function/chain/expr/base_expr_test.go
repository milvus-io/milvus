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

package expr

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/suite"
)

type BaseExprTestSuite struct {
	suite.Suite
	pool *memory.CheckedAllocator
}

func (s *BaseExprTestSuite) SetupTest() {
	s.pool = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func (s *BaseExprTestSuite) TearDownTest() {
	s.pool.AssertSize(s.T(), 0)
}

func TestBaseExprTestSuite(t *testing.T) {
	suite.Run(t, new(BaseExprTestSuite))
}

// =============================================================================
// BaseExpr Tests
// =============================================================================

func (s *BaseExprTestSuite) TestBaseExprName() {
	expr := NewBaseExpr("test_func", []string{"rerank"})
	s.Equal("test_func", expr.Name())
}

func (s *BaseExprTestSuite) TestBaseExprIsRunnableWithStages() {
	expr := NewBaseExpr("test_func", []string{"rerank", "ingestion"})
	s.True(expr.IsRunnable("rerank"))
	s.True(expr.IsRunnable("ingestion"))
	s.False(expr.IsRunnable("unknown_stage"))
}

func (s *BaseExprTestSuite) TestBaseExprIsRunnableNoStages() {
	expr := NewBaseExpr("test_func", nil)
	// No stage restrictions -> runnable in any stage
	s.True(expr.IsRunnable("anything"))
}

func (s *BaseExprTestSuite) TestBaseExprIsRunnableEmptyStages() {
	expr := NewBaseExpr("test_func", []string{})
	// Empty stage restrictions -> runnable in any stage
	s.True(expr.IsRunnable("anything"))
}

// =============================================================================
// GetStringParam Tests
// =============================================================================

func (s *BaseExprTestSuite) TestGetStringParamSuccess() {
	params := map[string]interface{}{"key": "value"}
	val, err := GetStringParam(params, "func", "key", true)
	s.Require().NoError(err)
	s.Equal("value", val)
}

func (s *BaseExprTestSuite) TestGetStringParamMissingRequired() {
	params := map[string]interface{}{}
	_, err := GetStringParam(params, "func", "key", true)
	s.Error(err)
	s.Contains(err.Error(), "missing required parameter")
}

func (s *BaseExprTestSuite) TestGetStringParamMissingOptional() {
	params := map[string]interface{}{}
	val, err := GetStringParam(params, "func", "key", false)
	s.Require().NoError(err)
	s.Equal("", val)
}

func (s *BaseExprTestSuite) TestGetStringParamWrongType() {
	params := map[string]interface{}{"key": 123}
	_, err := GetStringParam(params, "func", "key", true)
	s.Error(err)
	s.Contains(err.Error(), "must be a string")
}

// =============================================================================
// GetFloat64Param Tests
// =============================================================================

func (s *BaseExprTestSuite) TestGetFloat64ParamFloat64() {
	params := map[string]interface{}{"key": float64(3.14)}
	val, err := GetFloat64Param(params, "func", "key", true, 0)
	s.Require().NoError(err)
	s.InDelta(3.14, val, 1e-9)
}

func (s *BaseExprTestSuite) TestGetFloat64ParamFloat32() {
	params := map[string]interface{}{"key": float32(2.5)}
	val, err := GetFloat64Param(params, "func", "key", true, 0)
	s.Require().NoError(err)
	s.InDelta(2.5, val, 1e-5)
}

func (s *BaseExprTestSuite) TestGetFloat64ParamInt() {
	params := map[string]interface{}{"key": 42}
	val, err := GetFloat64Param(params, "func", "key", true, 0)
	s.Require().NoError(err)
	s.InDelta(42.0, val, 1e-9)
}

func (s *BaseExprTestSuite) TestGetFloat64ParamInt64() {
	params := map[string]interface{}{"key": int64(100)}
	val, err := GetFloat64Param(params, "func", "key", true, 0)
	s.Require().NoError(err)
	s.InDelta(100.0, val, 1e-9)
}

func (s *BaseExprTestSuite) TestGetFloat64ParamInt32() {
	params := map[string]interface{}{"key": int32(50)}
	val, err := GetFloat64Param(params, "func", "key", true, 0)
	s.Require().NoError(err)
	s.InDelta(50.0, val, 1e-9)
}

func (s *BaseExprTestSuite) TestGetFloat64ParamMissingRequired() {
	params := map[string]interface{}{}
	_, err := GetFloat64Param(params, "func", "key", true, 0)
	s.Error(err)
	s.Contains(err.Error(), "missing required parameter")
}

func (s *BaseExprTestSuite) TestGetFloat64ParamMissingOptionalDefault() {
	params := map[string]interface{}{}
	val, err := GetFloat64Param(params, "func", "key", false, 99.9)
	s.Require().NoError(err)
	s.InDelta(99.9, val, 1e-9)
}

func (s *BaseExprTestSuite) TestGetFloat64ParamWrongType() {
	params := map[string]interface{}{"key": "not_a_number"}
	_, err := GetFloat64Param(params, "func", "key", true, 0)
	s.Error(err)
	s.Contains(err.Error(), "must be a number")
}

// =============================================================================
// ParseStringSliceParam Tests
// =============================================================================

func (s *BaseExprTestSuite) TestParseStringSliceParamStringSlice() {
	params := map[string]interface{}{"key": []string{"a", "b", "c"}}
	val, err := ParseStringSliceParam(params, "func", "key")
	s.Require().NoError(err)
	s.Equal([]string{"a", "b", "c"}, val)
}

func (s *BaseExprTestSuite) TestParseStringSliceParamInterfaceSlice() {
	params := map[string]interface{}{"key": []interface{}{"x", "y"}}
	val, err := ParseStringSliceParam(params, "func", "key")
	s.Require().NoError(err)
	s.Equal([]string{"x", "y"}, val)
}

func (s *BaseExprTestSuite) TestParseStringSliceParamMissing() {
	params := map[string]interface{}{}
	_, err := ParseStringSliceParam(params, "func", "key")
	s.Error(err)
	s.Contains(err.Error(), "missing required parameter")
}

func (s *BaseExprTestSuite) TestParseStringSliceParamWrongType() {
	params := map[string]interface{}{"key": "not_an_array"}
	_, err := ParseStringSliceParam(params, "func", "key")
	s.Error(err)
	s.Contains(err.Error(), "must be a string array")
}

func (s *BaseExprTestSuite) TestParseStringSliceParamNonStringElement() {
	params := map[string]interface{}{"key": []interface{}{"ok", 123}}
	_, err := ParseStringSliceParam(params, "func", "key")
	s.Error(err)
	s.Contains(err.Error(), "must be a string")
}

// =============================================================================
// ParseFloat64SliceParam Tests
// =============================================================================

func (s *BaseExprTestSuite) TestParseFloat64SliceParamFloat64Slice() {
	params := map[string]interface{}{"key": []float64{1.1, 2.2, 3.3}}
	val, err := ParseFloat64SliceParam(params, "func", "key")
	s.Require().NoError(err)
	s.Equal([]float64{1.1, 2.2, 3.3}, val)
}

func (s *BaseExprTestSuite) TestParseFloat64SliceParamInterfaceSlice() {
	params := map[string]interface{}{"key": []interface{}{float64(1.0), float32(2.0), 3, int64(4), int32(5)}}
	val, err := ParseFloat64SliceParam(params, "func", "key")
	s.Require().NoError(err)
	s.Equal(5, len(val))
	s.InDelta(1.0, val[0], 1e-9)
	s.InDelta(2.0, val[1], 1e-5)
	s.InDelta(3.0, val[2], 1e-9)
	s.InDelta(4.0, val[3], 1e-9)
	s.InDelta(5.0, val[4], 1e-9)
}

func (s *BaseExprTestSuite) TestParseFloat64SliceParamMissingOptional() {
	params := map[string]interface{}{}
	val, err := ParseFloat64SliceParam(params, "func", "key")
	s.Require().NoError(err)
	s.Nil(val)
}

func (s *BaseExprTestSuite) TestParseFloat64SliceParamWrongType() {
	params := map[string]interface{}{"key": "not_an_array"}
	_, err := ParseFloat64SliceParam(params, "func", "key")
	s.Error(err)
	s.Contains(err.Error(), "must be a number array")
}

func (s *BaseExprTestSuite) TestParseFloat64SliceParamNonNumericElement() {
	params := map[string]interface{}{"key": []interface{}{1.0, "bad"}}
	_, err := ParseFloat64SliceParam(params, "func", "key")
	s.Error(err)
	s.Contains(err.Error(), "must be a number")
}

// =============================================================================
// GetNumericValue Tests
// =============================================================================

func (s *BaseExprTestSuite) TestGetNumericValueInt8() {
	b := array.NewInt8Builder(s.pool)
	b.Append(42)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	val, err := GetNumericValue(arr, 0)
	s.Require().NoError(err)
	s.InDelta(42.0, val, 1e-9)
}

func (s *BaseExprTestSuite) TestGetNumericValueInt16() {
	b := array.NewInt16Builder(s.pool)
	b.Append(1000)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	val, err := GetNumericValue(arr, 0)
	s.Require().NoError(err)
	s.InDelta(1000.0, val, 1e-9)
}

func (s *BaseExprTestSuite) TestGetNumericValueInt32() {
	b := array.NewInt32Builder(s.pool)
	b.Append(50000)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	val, err := GetNumericValue(arr, 0)
	s.Require().NoError(err)
	s.InDelta(50000.0, val, 1e-9)
}

func (s *BaseExprTestSuite) TestGetNumericValueInt64() {
	b := array.NewInt64Builder(s.pool)
	b.Append(999999)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	val, err := GetNumericValue(arr, 0)
	s.Require().NoError(err)
	s.InDelta(999999.0, val, 1e-9)
}

func (s *BaseExprTestSuite) TestGetNumericValueFloat32() {
	b := array.NewFloat32Builder(s.pool)
	b.Append(3.14)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	val, err := GetNumericValue(arr, 0)
	s.Require().NoError(err)
	s.InDelta(3.14, val, 1e-5)
}

func (s *BaseExprTestSuite) TestGetNumericValueFloat64() {
	b := array.NewFloat64Builder(s.pool)
	b.Append(2.71828)
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	val, err := GetNumericValue(arr, 0)
	s.Require().NoError(err)
	s.InDelta(2.71828, val, 1e-9)
}

func (s *BaseExprTestSuite) TestGetNumericValueUnsupportedType() {
	b := array.NewStringBuilder(s.pool)
	b.Append("hello")
	arr := b.NewArray()
	b.Release()
	defer arr.Release()

	_, err := GetNumericValue(arr, 0)
	s.Error(err)
	s.Contains(err.Error(), "unsupported input column type")
}
