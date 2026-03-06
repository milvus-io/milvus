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

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
)

type RoundDecimalExprTestSuite struct {
	suite.Suite
	pool *memory.CheckedAllocator
}

func (s *RoundDecimalExprTestSuite) SetupTest() {
	s.pool = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func (s *RoundDecimalExprTestSuite) TearDownTest() {
	s.pool.AssertSize(s.T(), 0)
}

func TestRoundDecimalExprTestSuite(t *testing.T) {
	suite.Run(t, new(RoundDecimalExprTestSuite))
}

func (s *RoundDecimalExprTestSuite) buildScoreInput(scores []float32) *arrow.Chunked {
	builder := array.NewFloat32Builder(s.pool)
	defer builder.Release()
	builder.AppendValues(scores, nil)
	arr := builder.NewArray()
	chunked := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{arr})
	arr.Release()
	return chunked
}

func (s *RoundDecimalExprTestSuite) TestNewRoundDecimalExpr_ValidDecimals() {
	for _, d := range []int64{0, 1, 2, 3, 4, 5, 6} {
		expr, err := NewRoundDecimalExpr(d)
		s.Require().NoError(err)
		s.Equal(RoundDecimalFuncName, expr.Name())
		s.Equal([]arrow.DataType{arrow.PrimitiveTypes.Float32}, expr.OutputDataTypes())
		s.True(expr.IsRunnable(types.StageL2Rerank))
	}
}

func (s *RoundDecimalExprTestSuite) TestNewRoundDecimalExpr_InvalidDecimals() {
	_, err := NewRoundDecimalExpr(-1)
	s.Error(err)

	_, err = NewRoundDecimalExpr(7)
	s.Error(err)
}

func (s *RoundDecimalExprTestSuite) TestExecute_RoundTo2Decimals() {
	expr, err := NewRoundDecimalExpr(2)
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)
	input := s.buildScoreInput([]float32{0.956, 0.834, 0.715, 0.123, 0.999})
	defer input.Release()

	results, err := expr.Execute(ctx, []*arrow.Chunked{input})
	s.Require().NoError(err)
	s.Len(results, 1)
	defer results[0].Release()

	chunk := results[0].Chunk(0).(*array.Float32)
	s.Equal(5, chunk.Len())
	s.InDelta(float32(0.96), chunk.Value(0), 0.001)
	s.InDelta(float32(0.83), chunk.Value(1), 0.001)
	// 0.715 as float32 is ~0.71499997, so floor(71.499997+0.5)/100 = 0.71
	s.InDelta(float32(0.71), chunk.Value(2), 0.001)
	s.InDelta(float32(0.12), chunk.Value(3), 0.001)
	s.InDelta(float32(1.00), chunk.Value(4), 0.001)
}

func (s *RoundDecimalExprTestSuite) TestExecute_RoundTo0Decimals() {
	expr, err := NewRoundDecimalExpr(0)
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)
	input := s.buildScoreInput([]float32{0.5, 1.4, 2.5, 3.6})
	defer input.Release()

	results, err := expr.Execute(ctx, []*arrow.Chunked{input})
	s.Require().NoError(err)
	defer results[0].Release()

	chunk := results[0].Chunk(0).(*array.Float32)
	s.InDelta(float32(1.0), chunk.Value(0), 0.001) // 0.5 -> 1
	s.InDelta(float32(1.0), chunk.Value(1), 0.001) // 1.4 -> 1
	s.InDelta(float32(3.0), chunk.Value(2), 0.001) // 2.5 -> 3
	s.InDelta(float32(4.0), chunk.Value(3), 0.001) // 3.6 -> 4
}

func (s *RoundDecimalExprTestSuite) TestExecute_MultipleChunks() {
	expr, err := NewRoundDecimalExpr(1)
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)

	b1 := array.NewFloat32Builder(s.pool)
	b1.AppendValues([]float32{0.96, 0.84}, nil)
	arr1 := b1.NewArray()
	b1.Release()

	b2 := array.NewFloat32Builder(s.pool)
	b2.AppendValues([]float32{0.71}, nil)
	arr2 := b2.NewArray()
	b2.Release()

	input := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{arr1, arr2})
	arr1.Release()
	arr2.Release()
	defer input.Release()

	results, err := expr.Execute(ctx, []*arrow.Chunked{input})
	s.Require().NoError(err)
	defer results[0].Release()

	s.Equal(2, len(results[0].Chunks()))
	chunk0 := results[0].Chunk(0).(*array.Float32)
	s.InDelta(float32(1.0), chunk0.Value(0), 0.001)
	s.InDelta(float32(0.8), chunk0.Value(1), 0.001)

	chunk1 := results[0].Chunk(1).(*array.Float32)
	s.InDelta(float32(0.7), chunk1.Value(0), 0.001)
}

func (s *RoundDecimalExprTestSuite) TestExecute_WrongInputCount() {
	expr, err := NewRoundDecimalExpr(2)
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)
	input1 := s.buildScoreInput([]float32{0.5})
	defer input1.Release()
	input2 := s.buildScoreInput([]float32{0.5})
	defer input2.Release()

	_, err = expr.Execute(ctx, []*arrow.Chunked{input1, input2})
	s.Error(err)
	s.Contains(err.Error(), "expected 1 input")
}

func (s *RoundDecimalExprTestSuite) TestExecute_MatchesOldFormula() {
	// Verify the formula matches the old rerank implementation:
	// score = floor(score * 10^decimal + 0.5) / 10^decimal
	expr, err := NewRoundDecimalExpr(3)
	s.Require().NoError(err)

	ctx := types.NewFuncContext(s.pool)
	input := s.buildScoreInput([]float32{0.12345, 0.98765, 0.55555})
	defer input.Release()

	results, err := expr.Execute(ctx, []*arrow.Chunked{input})
	s.Require().NoError(err)
	defer results[0].Release()

	chunk := results[0].Chunk(0).(*array.Float32)
	s.InDelta(float32(0.123), chunk.Value(0), 0.0001)
	s.InDelta(float32(0.988), chunk.Value(1), 0.0001)
	s.InDelta(float32(0.556), chunk.Value(2), 0.0001)
}
