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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
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
// ParamReader Tests
// =============================================================================

func (s *BaseExprTestSuite) TestParamReaderString() {
	r := types.NewParamReader("func", map[string]*schemapb.FunctionParamValue{"key": stringParam("value")})
	val, err := r.String("key", true)
	s.Require().NoError(err)
	s.Equal("value", val)

	_, err = r.String("missing", true)
	s.ErrorContains(err, "missing required parameter")

	val, err = r.String("missing", false)
	s.Require().NoError(err)
	s.Equal("", val)

	_, err = types.NewParamReader("func", map[string]*schemapb.FunctionParamValue{"key": intParam(1)}).String("key", true)
	s.ErrorContains(err, "must be a string")
}

func (s *BaseExprTestSuite) TestParamReaderFloat64() {
	r := types.NewParamReader("func", map[string]*schemapb.FunctionParamValue{
		"double": doubleParam(3.14),
		"int":    intParam(42),
	})
	val, err := r.Float64("double", true, 0)
	s.Require().NoError(err)
	s.InDelta(3.14, val, 1e-9)

	val, err = r.Float64("int", true, 0)
	s.Require().NoError(err)
	s.InDelta(42.0, val, 1e-9)

	val, err = r.Float64("missing", false, 99.9)
	s.Require().NoError(err)
	s.InDelta(99.9, val, 1e-9)

	_, err = types.NewParamReader("func", map[string]*schemapb.FunctionParamValue{"key": stringParam("bad")}).Float64("key", true, 0)
	s.ErrorContains(err, "must be a number")
}

func (s *BaseExprTestSuite) TestParamReaderInt64() {
	r := types.NewParamReader("func", map[string]*schemapb.FunctionParamValue{
		"int":    intParam(42),
		"double": doubleParam(42.0),
	})
	val, err := r.Int64("int", true, 0)
	s.Require().NoError(err)
	s.Equal(int64(42), val)

	val, err = r.Int64("double", true, 0)
	s.Require().NoError(err)
	s.Equal(int64(42), val)

	_, err = types.NewParamReader("func", map[string]*schemapb.FunctionParamValue{"key": doubleParam(1.5)}).Int64("key", true, 0)
	s.ErrorContains(err, "must be an integer")
}

func (s *BaseExprTestSuite) TestParamReaderBool() {
	r := types.NewParamReader("func", map[string]*schemapb.FunctionParamValue{"key": boolParam(true)})
	val, err := r.Bool("key", true, false)
	s.Require().NoError(err)
	s.True(val)

	val, err = r.Bool("missing", false, true)
	s.Require().NoError(err)
	s.True(val)

	_, err = types.NewParamReader("func", map[string]*schemapb.FunctionParamValue{"key": stringParam("bad")}).Bool("key", true, false)
	s.ErrorContains(err, "must be a bool")
}

func (s *BaseExprTestSuite) TestParamReaderStringSlice() {
	r := types.NewParamReader("func", map[string]*schemapb.FunctionParamValue{"key": arrayParam(stringParam("a"), stringParam("b"))})
	val, err := r.StringSlice("key", true)
	s.Require().NoError(err)
	s.Equal([]string{"a", "b"}, val)

	_, err = r.StringSlice("missing", true)
	s.ErrorContains(err, "missing required parameter")

	_, err = types.NewParamReader("func", map[string]*schemapb.FunctionParamValue{"key": stringParam("bad")}).StringSlice("key", true)
	s.ErrorContains(err, "must be a string array")

	_, err = types.NewParamReader("func", map[string]*schemapb.FunctionParamValue{"key": arrayParam(stringParam("ok"), intParam(1))}).StringSlice("key", true)
	s.ErrorContains(err, "must be a string")
}

func (s *BaseExprTestSuite) TestParamReaderFloat64Slice() {
	r := types.NewParamReader("func", map[string]*schemapb.FunctionParamValue{"key": arrayParam(doubleParam(1.1), intParam(2))})
	val, err := r.Float64Slice("key", true)
	s.Require().NoError(err)
	s.Equal([]float64{1.1, 2.0}, val)

	val, err = r.Float64Slice("missing", false)
	s.Require().NoError(err)
	s.Nil(val)

	_, err = types.NewParamReader("func", map[string]*schemapb.FunctionParamValue{"key": stringParam("bad")}).Float64Slice("key", true)
	s.ErrorContains(err, "must be a number array")

	_, err = types.NewParamReader("func", map[string]*schemapb.FunctionParamValue{"key": arrayParam(doubleParam(1.0), stringParam("bad"))}).Float64Slice("key", true)
	s.ErrorContains(err, "must be a number")
}

func (s *BaseExprTestSuite) TestParamReaderKeyValuePairs() {
	r := types.NewParamReader("func", map[string]*schemapb.FunctionParamValue{
		"s": stringParam("value"),
		"i": intParam(8),
		"f": doubleParam(0.5),
		"b": boolParam(true),
	})
	kvs, err := r.KeyValuePairs()
	s.Require().NoError(err)
	values := map[string]string{}
	for _, kv := range kvs {
		values[kv.Key] = kv.Value
	}
	s.Equal("value", values["s"])
	s.Equal("8", values["i"])
	s.Equal("0.5", values["f"])
	s.Equal("true", values["b"])

	_, err = types.NewParamReader("func", map[string]*schemapb.FunctionParamValue{"bad": arrayParam(stringParam("x"))}).KeyValuePairs()
	s.ErrorContains(err, "must be scalar")
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
