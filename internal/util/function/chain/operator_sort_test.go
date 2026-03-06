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

type SortOpTestSuite struct {
	suite.Suite
	pool *memory.CheckedAllocator
}

func (s *SortOpTestSuite) SetupTest() {
	s.pool = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func (s *SortOpTestSuite) TearDownTest() {
	s.pool.AssertSize(s.T(), 0)
}

func TestSortOpTestSuite(t *testing.T) {
	suite.Run(t, new(SortOpTestSuite))
}

// createSortTestDF creates a simple DataFrame with $id, $score columns and the given chunk sizes.
func (s *SortOpTestSuite) createSortTestDF(ids []int64, scores []float64, chunkSizes []int64) *DataFrame {
	builder := NewDataFrameBuilder()

	builder.SetChunkSizes(chunkSizes)

	offset := 0
	idChunks := make([]arrow.Array, len(chunkSizes))
	scoreChunks := make([]arrow.Array, len(chunkSizes))
	for i, size := range chunkSizes {
		idBuilder := array.NewInt64Builder(s.pool)
		scoreBuilder := array.NewFloat64Builder(s.pool)
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

func (s *SortOpTestSuite) TestSortDescending() {
	df := s.createSortTestDF(
		[]int64{1, 2, 3, 4},
		[]float64{0.1, 0.9, 0.5, 0.3},
		[]int64{4},
	)
	defer df.Release()

	op := NewSortOp(types.ScoreFieldName, true)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(4), result.NumRows())
	scoreCol := result.Column(types.ScoreFieldName)
	scores := scoreCol.Chunk(0).(*array.Float64)
	// Should be sorted descending: 0.9, 0.5, 0.3, 0.1
	s.InDelta(0.9, scores.Value(0), 1e-9)
	s.InDelta(0.5, scores.Value(1), 1e-9)
	s.InDelta(0.3, scores.Value(2), 1e-9)
	s.InDelta(0.1, scores.Value(3), 1e-9)
}

func (s *SortOpTestSuite) TestSortAscending() {
	df := s.createSortTestDF(
		[]int64{1, 2, 3, 4},
		[]float64{0.9, 0.1, 0.5, 0.3},
		[]int64{4},
	)
	defer df.Release()

	op := NewSortOp(types.ScoreFieldName, false)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	scoreCol := result.Column(types.ScoreFieldName)
	scores := scoreCol.Chunk(0).(*array.Float64)
	// Should be sorted ascending: 0.1, 0.3, 0.5, 0.9
	s.InDelta(0.1, scores.Value(0), 1e-9)
	s.InDelta(0.3, scores.Value(1), 1e-9)
	s.InDelta(0.5, scores.Value(2), 1e-9)
	s.InDelta(0.9, scores.Value(3), 1e-9)
}

func (s *SortOpTestSuite) TestSortWithAllNullScores() {
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{3})

	idBuilder := array.NewInt64Builder(s.pool)
	idBuilder.AppendValues([]int64{1, 2, 3}, nil)
	idChunk := idBuilder.NewArray()
	idBuilder.Release()

	scoreBuilder := array.NewFloat64Builder(s.pool)
	scoreBuilder.AppendNull()
	scoreBuilder.AppendNull()
	scoreBuilder.AppendNull()
	scoreChunk := scoreBuilder.NewArray()
	scoreBuilder.Release()

	err := builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idChunk})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreChunk})
	s.Require().NoError(err)

	df := builder.Build()
	defer df.Release()

	op := NewSortOp(types.ScoreFieldName, true)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	// All nulls - should still have 3 rows and not panic
	s.Equal(int64(3), result.NumRows())
	scoreCol := result.Column(types.ScoreFieldName)
	scores := scoreCol.Chunk(0)
	for i := 0; i < 3; i++ {
		s.True(scores.IsNull(i))
	}
}

func (s *SortOpTestSuite) TestSortMultiChunkIndependent() {
	// Two chunks (two queries), each sorted independently
	df := s.createSortTestDF(
		[]int64{1, 2, 3, 4, 5, 6},
		[]float64{0.1, 0.9, 0.5, 0.8, 0.2, 0.6},
		[]int64{3, 3},
	)
	defer df.Release()

	op := NewSortOp(types.ScoreFieldName, true)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	// Chunk 0 sorted desc: 0.9, 0.5, 0.1
	scores0 := result.Column(types.ScoreFieldName).Chunk(0).(*array.Float64)
	s.InDelta(0.9, scores0.Value(0), 1e-9)
	s.InDelta(0.5, scores0.Value(1), 1e-9)
	s.InDelta(0.1, scores0.Value(2), 1e-9)

	// Chunk 1 sorted desc: 0.8, 0.6, 0.2
	scores1 := result.Column(types.ScoreFieldName).Chunk(1).(*array.Float64)
	s.InDelta(0.8, scores1.Value(0), 1e-9)
	s.InDelta(0.6, scores1.Value(1), 1e-9)
	s.InDelta(0.2, scores1.Value(2), 1e-9)
}

func (s *SortOpTestSuite) TestSortColumnNotFound() {
	df := s.createSortTestDF([]int64{1}, []float64{0.5}, []int64{1})
	defer df.Release()

	op := NewSortOp("nonexistent", true)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	_, err := op.Execute(ctx, df)
	s.Error(err)
	s.Contains(err.Error(), "not found")
}

func (s *SortOpTestSuite) TestSortEmptyChunk() {
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{0})

	idBuilder := array.NewInt64Builder(s.pool)
	idChunk := idBuilder.NewArray()
	idBuilder.Release()

	scoreBuilder := array.NewFloat64Builder(s.pool)
	scoreChunk := scoreBuilder.NewArray()
	scoreBuilder.Release()

	err := builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idChunk})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreChunk})
	s.Require().NoError(err)

	df := builder.Build()
	defer df.Release()

	op := NewSortOp(types.ScoreFieldName, true)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(0), result.NumRows())
}

func (s *SortOpTestSuite) TestSortTieBreakByID() {
	// All scores are equal (0.5), IDs are 5, 3, 1, 4, 2
	// After sort descending by score with tie-break by $id ascending:
	// expected order by ID: 1, 2, 3, 4, 5
	df := s.createSortTestDF(
		[]int64{5, 3, 1, 4, 2},
		[]float64{0.5, 0.5, 0.5, 0.5, 0.5},
		[]int64{5},
	)
	defer df.Release()

	op := NewSortOpWithTieBreak(types.ScoreFieldName, true, types.IDFieldName)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	idCol := result.Column(types.IDFieldName)
	ids := idCol.Chunk(0).(*array.Int64)
	s.Equal(int64(1), ids.Value(0))
	s.Equal(int64(2), ids.Value(1))
	s.Equal(int64(3), ids.Value(2))
	s.Equal(int64(4), ids.Value(3))
	s.Equal(int64(5), ids.Value(4))
}

func (s *SortOpTestSuite) TestSortTieBreakPartialTies() {
	// Scores: 0.9, 0.5, 0.5, 0.5, 0.1 with IDs: 10, 30, 20, 40, 50
	// Expected: ID 10 (0.9), then 20, 30, 40 (0.5 sorted by ID asc), then 50 (0.1)
	df := s.createSortTestDF(
		[]int64{10, 30, 20, 40, 50},
		[]float64{0.9, 0.5, 0.5, 0.5, 0.1},
		[]int64{5},
	)
	defer df.Release()

	op := NewSortOpWithTieBreak(types.ScoreFieldName, true, types.IDFieldName)
	ctx := types.NewFuncContextFull(context.TODO(), s.pool, "rerank")
	result, err := op.Execute(ctx, df)
	s.Require().NoError(err)
	defer result.Release()

	idCol := result.Column(types.IDFieldName)
	ids := idCol.Chunk(0).(*array.Int64)
	s.Equal(int64(10), ids.Value(0)) // score 0.9
	s.Equal(int64(20), ids.Value(1)) // score 0.5, smallest ID
	s.Equal(int64(30), ids.Value(2)) // score 0.5
	s.Equal(int64(40), ids.Value(3)) // score 0.5
	s.Equal(int64(50), ids.Value(4)) // score 0.1
}
