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
	"context"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
)

type FilterOpTestSuite struct {
	suite.Suite
	pool *memory.CheckedAllocator
}

func (s *FilterOpTestSuite) SetupTest() {
	s.pool = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func (s *FilterOpTestSuite) TearDownTest() {
	s.pool.AssertSize(s.T(), 0)
}

func TestFilterOpTestSuite(t *testing.T) {
	suite.Run(t, new(FilterOpTestSuite))
}

// alwaysFalseExpr is a FunctionExpr that returns all-false boolean column.
type alwaysFalseExpr struct{}

func (e *alwaysFalseExpr) Name() string { return "always_false" }
func (e *alwaysFalseExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	input := inputs[0]
	chunks := make([]arrow.Array, len(input.Chunks()))
	for i, chunk := range input.Chunks() {
		b := array.NewBooleanBuilder(ctx.Pool())
		for j := 0; j < chunk.Len(); j++ {
			b.Append(false)
		}
		chunks[i] = b.NewArray()
		b.Release()
	}
	result := arrow.NewChunked(arrow.FixedWidthTypes.Boolean, chunks)
	for _, c := range chunks {
		c.Release()
	}
	return []*arrow.Chunked{result}, nil
}

func (e *alwaysFalseExpr) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.FixedWidthTypes.Boolean}
}
func (e *alwaysFalseExpr) IsRunnable(stage string) bool { return true }
func (e *alwaysFalseExpr) Stages() []string             { return []string{"rerank"} }

// alwaysTrueExpr is a FunctionExpr that returns all-true boolean column.
type alwaysTrueExpr struct{}

func (e *alwaysTrueExpr) Name() string { return "always_true" }
func (e *alwaysTrueExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	input := inputs[0]
	chunks := make([]arrow.Array, len(input.Chunks()))
	for i, chunk := range input.Chunks() {
		b := array.NewBooleanBuilder(ctx.Pool())
		for j := 0; j < chunk.Len(); j++ {
			b.Append(true)
		}
		chunks[i] = b.NewArray()
		b.Release()
	}
	result := arrow.NewChunked(arrow.FixedWidthTypes.Boolean, chunks)
	for _, c := range chunks {
		c.Release()
	}
	return []*arrow.Chunked{result}, nil
}

func (e *alwaysTrueExpr) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.FixedWidthTypes.Boolean}
}
func (e *alwaysTrueExpr) IsRunnable(stage string) bool { return true }
func (e *alwaysTrueExpr) Stages() []string             { return []string{"rerank"} }

func (s *FilterOpTestSuite) createFilterTestDF(ids []int64, scores []float64) *DataFrame {
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{int64(len(ids))})

	idBuilder := array.NewInt64Builder(s.pool)
	idBuilder.AppendValues(ids, nil)
	idChunk := idBuilder.NewArray()
	idBuilder.Release()

	scoreBuilder := array.NewFloat64Builder(s.pool)
	scoreBuilder.AppendValues(scores, nil)
	scoreChunk := scoreBuilder.NewArray()
	scoreBuilder.Release()

	err := builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idChunk})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreChunk})
	s.Require().NoError(err)

	return builder.Build()
}

func (s *FilterOpTestSuite) TestFilterAllFalse() {
	df := s.createFilterTestDF([]int64{1, 2, 3}, []float64{0.9, 0.8, 0.7})
	defer df.Release()

	op, err := NewFilterOp(&alwaysFalseExpr{}, []string{types.IDFieldName})
	s.Require().NoError(err)

	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	// All filtered out
	s.Equal(int64(0), result.NumRows())
}

func (s *FilterOpTestSuite) TestFilterAllTrue() {
	df := s.createFilterTestDF([]int64{1, 2, 3}, []float64{0.9, 0.8, 0.7})
	defer df.Release()

	op, err := NewFilterOp(&alwaysTrueExpr{}, []string{types.IDFieldName})
	s.Require().NoError(err)

	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	// Nothing filtered
	s.Equal(int64(3), result.NumRows())
}

func (s *FilterOpTestSuite) TestFilterNilFunction() {
	_, err := NewFilterOp(nil, []string{types.IDFieldName})
	s.Error(err)
	s.Contains(err.Error(), "function is nil")
}

func (s *FilterOpTestSuite) TestFilterColumnNotFound() {
	df := s.createFilterTestDF([]int64{1}, []float64{0.5})
	defer df.Release()

	op, err := NewFilterOp(&alwaysTrueExpr{}, []string{"nonexistent"})
	s.Require().NoError(err)

	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	_, err = op.Execute(ctx, df)
	s.Error(err)
	s.Contains(err.Error(), "not found")
}
