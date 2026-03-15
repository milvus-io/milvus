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
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
)

// =============================================================================
// Mock FunctionExpr implementations for MapOp tests
// =============================================================================

// doubleScoreExpr doubles the float32 score column.
type doubleScoreExpr struct{}

func (e *doubleScoreExpr) Name() string { return "double_score" }
func (e *doubleScoreExpr) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.PrimitiveTypes.Float32}
}
func (e *doubleScoreExpr) IsRunnable(stage string) bool { return true }
func (e *doubleScoreExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	input := inputs[0]
	chunks := make([]arrow.Array, len(input.Chunks()))
	for i, chunk := range input.Chunks() {
		f32 := chunk.(*array.Float32)
		b := array.NewFloat32Builder(ctx.Pool())
		for j := 0; j < f32.Len(); j++ {
			if f32.IsNull(j) {
				b.AppendNull()
			} else {
				b.Append(f32.Value(j) * 2)
			}
		}
		chunks[i] = b.NewArray()
		b.Release()
	}
	result := arrow.NewChunked(arrow.PrimitiveTypes.Float32, chunks)
	for _, c := range chunks {
		c.Release()
	}
	return []*arrow.Chunked{result}, nil
}

// errorExpr always returns an error on Execute.
type errorExpr struct{}

func (e *errorExpr) Name() string { return "error_expr" }
func (e *errorExpr) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.PrimitiveTypes.Float32}
}
func (e *errorExpr) IsRunnable(stage string) bool { return true }
func (e *errorExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	return nil, fmt.Errorf("intentional error")
}

// wrongOutputCountExpr returns 2 outputs when only 1 is expected.
type wrongOutputCountExpr struct{}

func (e *wrongOutputCountExpr) Name() string { return "wrong_count" }
func (e *wrongOutputCountExpr) OutputDataTypes() []arrow.DataType {
	return nil // dynamic output types
}
func (e *wrongOutputCountExpr) IsRunnable(stage string) bool { return true }
func (e *wrongOutputCountExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	// Return 2 outputs
	b1 := array.NewFloat32Builder(ctx.Pool())
	b1.Append(1.0)
	arr1 := b1.NewArray()
	b1.Release()

	b2 := array.NewFloat32Builder(ctx.Pool())
	b2.Append(2.0)
	arr2 := b2.NewArray()
	b2.Release()

	c1 := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{arr1})
	c2 := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{arr2})
	arr1.Release()
	arr2.Release()

	return []*arrow.Chunked{c1, c2}, nil
}

// =============================================================================
// MapOp Test Suite
// =============================================================================

type MapOpTestSuite struct {
	suite.Suite
	pool *memory.CheckedAllocator
}

func (s *MapOpTestSuite) SetupTest() {
	s.pool = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func (s *MapOpTestSuite) TearDownTest() {
	s.pool.AssertSize(s.T(), 0)
}

func TestMapOpTestSuite(t *testing.T) {
	suite.Run(t, new(MapOpTestSuite))
}

func (s *MapOpTestSuite) createTestDF(ids []int64, scores []float32, chunkSizes []int64) *DataFrame {
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes(chunkSizes)

	offset := 0
	idChunks := make([]arrow.Array, len(chunkSizes))
	scoreChunks := make([]arrow.Array, len(chunkSizes))
	for i, size := range chunkSizes {
		idBuilder := array.NewInt64Builder(s.pool)
		scoreBuilder := array.NewFloat32Builder(s.pool)
		for j := 0; j < int(size); j++ {
			idBuilder.Append(ids[offset+j])
			scoreBuilder.Append(scores[offset+j])
		}
		idChunks[i] = idBuilder.NewArray()
		idBuilder.Release()
		scoreChunks[i] = scoreBuilder.NewArray()
		scoreBuilder.Release()
		offset += int(size)
	}

	err := builder.AddColumnFromChunks(types.IDFieldName, idChunks)
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(types.ScoreFieldName, scoreChunks)
	s.Require().NoError(err)

	return builder.Build()
}

func (s *MapOpTestSuite) TestNewMapOpNilFunction() {
	_, err := NewMapOp(nil, []string{"in"}, []string{"out"})
	s.Error(err)
	s.Contains(err.Error(), "function is nil")
}

func (s *MapOpTestSuite) TestNewMapOpOutputCountMismatch() {
	fn := &doubleScoreExpr{}
	// Function outputs 1 column but we specify 2 output names
	_, err := NewMapOp(fn, []string{types.ScoreFieldName}, []string{"out1", "out2"})
	s.Error(err)
	s.Contains(err.Error(), "output columns count")
}

func (s *MapOpTestSuite) TestNewMapOpSuccess() {
	fn := &doubleScoreExpr{}
	op, err := NewMapOp(fn, []string{types.ScoreFieldName}, []string{types.ScoreFieldName})
	s.Require().NoError(err)
	s.Equal("Map", op.Name())
	s.Equal([]string{types.ScoreFieldName}, op.Inputs())
	s.Equal([]string{types.ScoreFieldName}, op.Outputs())
}

func (s *MapOpTestSuite) TestMapOpExecuteBasic() {
	df := s.createTestDF(
		[]int64{1, 2, 3},
		[]float32{1.0, 2.0, 3.0},
		[]int64{3},
	)
	defer df.Release()

	fn := &doubleScoreExpr{}
	op, err := NewMapOp(fn, []string{types.ScoreFieldName}, []string{types.ScoreFieldName})
	s.Require().NoError(err)

	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	// Scores should be doubled
	scoreCol := result.Column(types.ScoreFieldName)
	scores := scoreCol.Chunk(0).(*array.Float32)
	s.InDelta(2.0, float64(scores.Value(0)), 1e-6)
	s.InDelta(4.0, float64(scores.Value(1)), 1e-6)
	s.InDelta(6.0, float64(scores.Value(2)), 1e-6)

	// ID column should be preserved
	idCol := result.Column(types.IDFieldName)
	s.NotNil(idCol)
	ids := idCol.Chunk(0).(*array.Int64)
	s.Equal(int64(1), ids.Value(0))
	s.Equal(int64(2), ids.Value(1))
	s.Equal(int64(3), ids.Value(2))
}

func (s *MapOpTestSuite) TestMapOpExecuteMultiChunk() {
	df := s.createTestDF(
		[]int64{1, 2, 3, 4},
		[]float32{1.0, 2.0, 3.0, 4.0},
		[]int64{2, 2},
	)
	defer df.Release()

	fn := &doubleScoreExpr{}
	op, err := NewMapOp(fn, []string{types.ScoreFieldName}, []string{types.ScoreFieldName})
	s.Require().NoError(err)

	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(4), result.NumRows())
	scores0 := result.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	scores1 := result.Column(types.ScoreFieldName).Chunk(1).(*array.Float32)
	s.InDelta(2.0, float64(scores0.Value(0)), 1e-6)
	s.InDelta(4.0, float64(scores0.Value(1)), 1e-6)
	s.InDelta(6.0, float64(scores1.Value(0)), 1e-6)
	s.InDelta(8.0, float64(scores1.Value(1)), 1e-6)
}

func (s *MapOpTestSuite) TestMapOpExecuteColumnNotFound() {
	df := s.createTestDF([]int64{1}, []float32{1.0}, []int64{1})
	defer df.Release()

	fn := &doubleScoreExpr{}
	op, err := NewMapOp(fn, []string{"nonexistent"}, []string{"out"})
	s.Require().NoError(err)

	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	_, err = op.Execute(ctx, df)
	s.Error(err)
	s.Contains(err.Error(), "not found")
}

func (s *MapOpTestSuite) TestMapOpExecuteFunctionError() {
	df := s.createTestDF([]int64{1}, []float32{1.0}, []int64{1})
	defer df.Release()

	fn := &errorExpr{}
	op, err := NewMapOp(fn, []string{types.ScoreFieldName}, []string{types.ScoreFieldName})
	s.Require().NoError(err)

	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	_, err = op.Execute(ctx, df)
	s.Error(err)
	s.Contains(err.Error(), "intentional error")
}

func (s *MapOpTestSuite) TestMapOpExecuteOutputCountMismatchAtRuntime() {
	df := s.createTestDF([]int64{1}, []float32{1.0}, []int64{1})
	defer df.Release()

	fn := &wrongOutputCountExpr{} // returns 2 outputs but we expect 1
	op, err := NewMapOp(fn, []string{types.ScoreFieldName}, []string{"out1"})
	s.Require().NoError(err) // passes creation (dynamic types)

	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	_, err = op.Execute(ctx, df)
	s.Error(err)
	s.Contains(err.Error(), "function returned 2 outputs, expected 1")
}

func (s *MapOpTestSuite) TestMapOpString() {
	fn := &doubleScoreExpr{}
	op, err := NewMapOp(fn, []string{"in"}, []string{"out"})
	s.Require().NoError(err)
	s.Equal("Map(double_score)", op.String())
}

func (s *MapOpTestSuite) TestMapOpStringNilFunction() {
	op := &MapOp{}
	s.Equal("Map(nil)", op.String())
}

func (s *MapOpTestSuite) TestMapOpExecuteNilFunction() {
	op := &MapOp{}
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	df := s.createTestDF([]int64{1}, []float32{1.0}, []int64{1})
	defer df.Release()

	_, err := op.Execute(ctx, df)
	s.Error(err)
	s.Contains(err.Error(), "function is nil")
}

func (s *MapOpTestSuite) TestNewMapOpFromReprNilFunction() {
	repr := &OperatorRepr{
		Type:    types.OpTypeMap,
		Inputs:  []string{"in"},
		Outputs: []string{"out"},
	}
	_, err := NewMapOpFromRepr(repr)
	s.Error(err)
	s.Contains(err.Error(), "requires function")
}

func (s *MapOpTestSuite) TestNewMapOpFromReprNoInputs() {
	repr := &OperatorRepr{
		Type:     types.OpTypeMap,
		Function: &FunctionRepr{Name: "score_combine", Params: map[string]interface{}{"input_count": float64(2)}},
		Outputs:  []string{"out"},
	}
	_, err := NewMapOpFromRepr(repr)
	s.Error(err)
	s.Contains(err.Error(), "requires inputs")
}

func (s *MapOpTestSuite) TestNewMapOpFromReprNoOutputs() {
	repr := &OperatorRepr{
		Type:     types.OpTypeMap,
		Function: &FunctionRepr{Name: "score_combine", Params: map[string]interface{}{"input_count": float64(2)}},
		Inputs:   []string{"in"},
	}
	_, err := NewMapOpFromRepr(repr)
	s.Error(err)
	s.Contains(err.Error(), "requires outputs")
}
