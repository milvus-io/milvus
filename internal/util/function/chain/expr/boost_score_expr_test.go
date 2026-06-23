/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * # or implied. See the License for the specific language governing
 * # permissions and limitations under the License.
 */

package expr

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
)

type BoostScoreExprTestSuite struct {
	suite.Suite
	pool *memory.CheckedAllocator
}

func (s *BoostScoreExprTestSuite) SetupTest() {
	s.pool = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func (s *BoostScoreExprTestSuite) TearDownTest() {
	s.pool.AssertSize(s.T(), 0)
}

func TestBoostScoreExprTestSuite(t *testing.T) {
	suite.Run(t, new(BoostScoreExprTestSuite))
}

func (s *BoostScoreExprTestSuite) createOffsetColumn(chunks ...[]int64) *arrow.Chunked {
	arrays := make([]arrow.Array, 0, len(chunks))
	for _, values := range chunks {
		builder := array.NewInt64Builder(s.pool)
		builder.AppendValues(values, nil)
		arr := builder.NewArray()
		builder.Release()
		arrays = append(arrays, arr)
	}
	chunked := arrow.NewChunked(arrow.PrimitiveTypes.Int64, arrays)
	for _, arr := range arrays {
		arr.Release()
	}
	return chunked
}

func (s *BoostScoreExprTestSuite) createScoreColumn(values [][]float32, valid [][]bool) *arrow.Chunked {
	arrays := make([]arrow.Array, 0, len(values))
	for chunkIdx, chunkValues := range values {
		builder := array.NewFloat32Builder(s.pool)
		for rowIdx, value := range chunkValues {
			if valid != nil && !valid[chunkIdx][rowIdx] {
				builder.AppendNull()
			} else {
				builder.Append(value)
			}
		}
		arr := builder.NewArray()
		builder.Release()
		arrays = append(arrays, arr)
	}
	chunked := arrow.NewChunked(arrow.PrimitiveTypes.Float32, arrays)
	for _, arr := range arrays {
		arr.Release()
	}
	return chunked
}

func (s *BoostScoreExprTestSuite) TestNewBoostScoreExpr() {
	runner := func(context.Context, *arrow.Chunked) (*arrow.Chunked, error) {
		return nil, nil
	}
	expr, err := NewBoostScoreExpr(runner)
	s.Require().NoError(err)
	s.Equal(BoostScoreFuncName, expr.Name())
	s.Equal([]arrow.DataType{arrow.PrimitiveTypes.Float32}, expr.OutputDataTypes())
	s.True(expr.IsRunnable(types.StageL0Rerank))
	s.False(expr.IsRunnable(types.StageL2Rerank))

	_, err = NewBoostScoreExpr(nil)
	s.Error(err)
	s.Contains(err.Error(), "runner is nil")
}

func (s *BoostScoreExprTestSuite) TestExecutePassesChunkedOffsets() {
	offsets := s.createOffsetColumn([]int64{10, 20}, []int64{30, 40, 50})
	defer offsets.Release()

	called := false
	expr, err := NewBoostScoreExpr(func(ctx context.Context, got *arrow.Chunked) (*arrow.Chunked, error) {
		called = true
		s.Equal(context.TODO(), ctx)
		s.Same(offsets, got)
		return s.createScoreColumn(
			[][]float32{{1.5, 0}, {2.5, 3.5, 0}},
			[][]bool{{true, false}, {true, true, false}},
		), nil
	})
	s.Require().NoError(err)

	ctx := types.NewFuncContextFull(context.TODO(), s.pool, types.StageL0Rerank)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{offsets})
	s.Require().NoError(err)
	defer outputs[0].Release()

	s.True(called)
	s.Len(outputs, 1)
	s.Equal(2, len(outputs[0].Chunks()))
	s.Equal(2, outputs[0].Chunk(0).Len())
	s.Equal(3, outputs[0].Chunk(1).Len())

	chunk0 := outputs[0].Chunk(0).(*array.Float32)
	chunk1 := outputs[0].Chunk(1).(*array.Float32)
	s.InDelta(1.5, float64(chunk0.Value(0)), 1e-6)
	s.True(chunk0.IsNull(1))
	s.InDelta(2.5, float64(chunk1.Value(0)), 1e-6)
	s.InDelta(3.5, float64(chunk1.Value(1)), 1e-6)
	s.True(chunk1.IsNull(2))
}

func (s *BoostScoreExprTestSuite) TestExecuteRejectsInvalidInputs() {
	expr, err := NewBoostScoreExpr(func(context.Context, *arrow.Chunked) (*arrow.Chunked, error) {
		return nil, nil
	})
	s.Require().NoError(err)
	ctx := types.NewFuncContextFull(context.Background(), s.pool, types.StageL0Rerank)

	_, err = expr.Execute(ctx, nil)
	s.Error(err)
	s.Contains(err.Error(), "expected 1 input")

	builder := array.NewInt32Builder(s.pool)
	builder.Append(1)
	arr := builder.NewArray()
	builder.Release()
	wrongType := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{arr})
	arr.Release()
	defer wrongType.Release()

	_, err = expr.Execute(ctx, []*arrow.Chunked{wrongType})
	s.Error(err)
	s.Contains(err.Error(), "must be Int64")

	_, err = expr.Execute(ctx, []*arrow.Chunked{nil})
	s.Error(err)
	s.Contains(err.Error(), "offset column is nil")

	expr.runner = nil
	_, err = expr.Execute(ctx, []*arrow.Chunked{wrongType})
	s.Error(err)
	s.Contains(err.Error(), "runner is nil")
}

func (s *BoostScoreExprTestSuite) TestExecuteRunnerError() {
	offsets := s.createOffsetColumn([]int64{10})
	defer offsets.Release()

	expr, err := NewBoostScoreExpr(func(context.Context, *arrow.Chunked) (*arrow.Chunked, error) {
		return nil, fmt.Errorf("runner failed")
	})
	s.Require().NoError(err)

	ctx := types.NewFuncContextFull(context.Background(), s.pool, types.StageL0Rerank)
	_, err = expr.Execute(ctx, []*arrow.Chunked{offsets})
	s.Error(err)
	s.Contains(err.Error(), "runner failed")
}

func (s *BoostScoreExprTestSuite) TestExecuteRejectsInvalidRunnerOutput() {
	offsets := s.createOffsetColumn([]int64{10, 20}, []int64{30})
	defer offsets.Release()
	ctx := types.NewFuncContextFull(context.Background(), s.pool, types.StageL0Rerank)

	cases := []struct {
		name    string
		scores  func() *arrow.Chunked
		message string
	}{
		{
			name: "nil scores",
			scores: func() *arrow.Chunked {
				return nil
			},
			message: "nil scores",
		},
		{
			name: "wrong type",
			scores: func() *arrow.Chunked {
				builder := array.NewFloat64Builder(s.pool)
				builder.Append(1)
				arr := builder.NewArray()
				builder.Release()
				chunked := arrow.NewChunked(arrow.PrimitiveTypes.Float64, []arrow.Array{arr})
				arr.Release()
				return chunked
			},
			message: "must be Float32",
		},
		{
			name: "chunk count mismatch",
			scores: func() *arrow.Chunked {
				return s.createScoreColumn([][]float32{{1, 2, 3}}, nil)
			},
			message: "score chunks",
		},
		{
			name: "chunk length mismatch",
			scores: func() *arrow.Chunked {
				return s.createScoreColumn([][]float32{{1}, {2}}, nil)
			},
			message: "length",
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			expr, err := NewBoostScoreExpr(func(context.Context, *arrow.Chunked) (*arrow.Chunked, error) {
				return tc.scores(), nil
			})
			s.Require().NoError(err)
			_, err = expr.Execute(ctx, []*arrow.Chunked{offsets})
			s.Error(err)
			s.Contains(err.Error(), tc.message)
		})
	}
}
