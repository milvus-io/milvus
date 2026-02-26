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

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
)

// =============================================================================
// GroupByOp Test Suite
// =============================================================================

type GroupByOpTestSuite struct {
	suite.Suite
	pool *memory.CheckedAllocator
}

func (s *GroupByOpTestSuite) SetupTest() {
	s.pool = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func (s *GroupByOpTestSuite) TearDownTest() {
	s.pool.AssertSize(s.T(), 0)
}

func TestGroupByOpTestSuite(t *testing.T) {
	suite.Run(t, new(GroupByOpTestSuite))
}

// =============================================================================
// Helper Functions
// =============================================================================

// createGroupByTestDataFrame creates a test DataFrame for GroupBy tests.
// Chunk 0 (query 1): 8 rows
//
//	$id: [1, 2, 3, 4, 5, 6, 7, 8]
//	$score: [0.9, 0.8, 0.7, 0.6, 0.5, 0.85, 0.75, 0.65]
//	category: ["A", "A", "A", "B", "B", "B", "C", "C"]
//
// Group A: scores 0.9, 0.8, 0.7 (max=0.9)
// Group B: scores 0.6, 0.5, 0.85 (max=0.85)
// Group C: scores 0.75, 0.65 (max=0.75)
func (s *GroupByOpTestSuite) createGroupByTestDataFrame() *DataFrame {
	builder := NewDataFrameBuilder()

	// Set chunk sizes
	builder.SetChunkSizes([]int64{8})

	// Build $id column
	idBuilder := array.NewInt64Builder(s.pool)
	idBuilder.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7, 8}, nil)
	idChunk := idBuilder.NewArray()
	idBuilder.Release()

	err := builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idChunk})
	s.Require().NoError(err)

	// Build $score column
	scoreBuilder := array.NewFloat32Builder(s.pool)
	scoreBuilder.AppendValues([]float32{0.9, 0.8, 0.7, 0.6, 0.5, 0.85, 0.75, 0.65}, nil)
	scoreChunk := scoreBuilder.NewArray()
	scoreBuilder.Release()

	err = builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreChunk})
	s.Require().NoError(err)

	// Build category column
	catBuilder := array.NewStringBuilder(s.pool)
	catBuilder.AppendValues([]string{"A", "A", "A", "B", "B", "B", "C", "C"}, nil)
	catChunk := catBuilder.NewArray()
	catBuilder.Release()

	err = builder.AddColumnFromChunks("category", []arrow.Array{catChunk})
	s.Require().NoError(err)

	return builder.Build()
}

// createMultiChunkGroupByTestDataFrame creates a DataFrame with multiple chunks.
func (s *GroupByOpTestSuite) createMultiChunkGroupByTestDataFrame() *DataFrame {
	builder := NewDataFrameBuilder()

	// Set chunk sizes: 2 chunks
	builder.SetChunkSizes([]int64{5, 4})

	// Chunk 0
	idBuilder1 := array.NewInt64Builder(s.pool)
	idBuilder1.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	idChunk1 := idBuilder1.NewArray()
	idBuilder1.Release()

	scoreBuilder1 := array.NewFloat32Builder(s.pool)
	scoreBuilder1.AppendValues([]float32{0.9, 0.8, 0.7, 0.6, 0.5}, nil)
	scoreChunk1 := scoreBuilder1.NewArray()
	scoreBuilder1.Release()

	catBuilder1 := array.NewStringBuilder(s.pool)
	catBuilder1.AppendValues([]string{"A", "A", "B", "B", "A"}, nil)
	catChunk1 := catBuilder1.NewArray()
	catBuilder1.Release()

	// Chunk 1
	idBuilder2 := array.NewInt64Builder(s.pool)
	idBuilder2.AppendValues([]int64{6, 7, 8, 9}, nil)
	idChunk2 := idBuilder2.NewArray()
	idBuilder2.Release()

	scoreBuilder2 := array.NewFloat32Builder(s.pool)
	scoreBuilder2.AppendValues([]float32{0.85, 0.75, 0.65, 0.55}, nil)
	scoreChunk2 := scoreBuilder2.NewArray()
	scoreBuilder2.Release()

	catBuilder2 := array.NewStringBuilder(s.pool)
	catBuilder2.AppendValues([]string{"C", "C", "D", "C"}, nil)
	catChunk2 := catBuilder2.NewArray()
	catBuilder2.Release()

	err := builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idChunk1, idChunk2})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreChunk1, scoreChunk2})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks("category", []arrow.Array{catChunk1, catChunk2})
	s.Require().NoError(err)

	return builder.Build()
}

// getChunkInt64Values extracts int64 values from a specific chunk.
func (s *GroupByOpTestSuite) getChunkInt64Values(col *arrow.Chunked, chunkIdx int) []int64 {
	arr := col.Chunk(chunkIdx).(*array.Int64)
	result := make([]int64, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		result[i] = arr.Value(i)
	}
	return result
}

// getChunkFloat32Values extracts float32 values from a specific chunk.
func (s *GroupByOpTestSuite) getChunkFloat32Values(col *arrow.Chunked, chunkIdx int) []float32 {
	arr := col.Chunk(chunkIdx).(*array.Float32)
	result := make([]float32, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		result[i] = arr.Value(i)
	}
	return result
}

// getChunkStringValues extracts string values from a specific chunk.
func (s *GroupByOpTestSuite) getChunkStringValues(col *arrow.Chunked, chunkIdx int) []string {
	arr := col.Chunk(chunkIdx).(*array.String)
	result := make([]string, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		result[i] = arr.Value(i)
	}
	return result
}

// =============================================================================
// GroupByOp Basic Tests
// =============================================================================

func (s *GroupByOpTestSuite) TestGroupByOp_Name() {
	op := NewGroupByOp("category", 2, 10, 0)
	s.Equal("GroupBy", op.Name())
}

func (s *GroupByOpTestSuite) TestGroupByOp_String() {
	op := NewGroupByOp("category", 2, 10, 5)
	str := op.String()
	s.Contains(str, "GroupBy")
	s.Contains(str, "category")
	s.Contains(str, "groupSize=2")
	s.Contains(str, "limit=10")
	s.Contains(str, "offset=5")
}

// =============================================================================
// GroupByOp Execute Tests
// =============================================================================

func (s *GroupByOpTestSuite) TestGroupByOp_Basic() {
	df := s.createGroupByTestDataFrame()
	defer df.Release()

	// GroupBy category, groupSize=2, limit=3, offset=0
	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		GroupBy("category", 2, 3, 0).
		Execute(df)

	s.Require().NoError(err)
	defer result.Release()

	// Should have $group_score column
	s.True(result.HasColumn(GroupScoreFieldName))

	// Groups sorted by max score:
	// A: max=0.9, top 2 rows: ids 1(0.9), 2(0.8)
	// B: max=0.85, top 2 rows: ids 6(0.85), 4(0.6)
	// C: max=0.75, top 2 rows: ids 7(0.75), 8(0.65)
	// Output order: A, A, B, B, C, C (6 rows total)

	s.Equal(int64(6), result.NumRows())

	categories := s.getChunkStringValues(result.Column("category"), 0)
	groupScores := s.getChunkFloat32Values(result.Column(GroupScoreFieldName), 0)

	// First 2 rows should be group A
	s.Equal("A", categories[0])
	s.Equal("A", categories[1])
	s.InDelta(float32(0.9), groupScores[0], 0.001)
	s.InDelta(float32(0.9), groupScores[1], 0.001)

	// Next 2 rows should be group B
	s.Equal("B", categories[2])
	s.Equal("B", categories[3])
	s.InDelta(float32(0.85), groupScores[2], 0.001)
	s.InDelta(float32(0.85), groupScores[3], 0.001)

	// Last 2 rows should be group C
	s.Equal("C", categories[4])
	s.Equal("C", categories[5])
	s.InDelta(float32(0.75), groupScores[4], 0.001)
	s.InDelta(float32(0.75), groupScores[5], 0.001)
}

func (s *GroupByOpTestSuite) TestGroupByOp_GroupSizeLimit() {
	df := s.createGroupByTestDataFrame()
	defer df.Release()

	// GroupBy category, groupSize=1 (only top 1 per group)
	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		GroupBy("category", 1, 3, 0).
		Execute(df)

	s.Require().NoError(err)
	defer result.Release()

	// Only 3 rows (1 per group)
	s.Equal(int64(3), result.NumRows())

	categories := s.getChunkStringValues(result.Column("category"), 0)
	scores := s.getChunkFloat32Values(result.Column(types.ScoreFieldName), 0)

	// Each group's top scorer
	s.Equal("A", categories[0])
	s.InDelta(float32(0.9), scores[0], 0.001) // Top of A

	s.Equal("B", categories[1])
	s.InDelta(float32(0.85), scores[1], 0.001) // Top of B

	s.Equal("C", categories[2])
	s.InDelta(float32(0.75), scores[2], 0.001) // Top of C
}

func (s *GroupByOpTestSuite) TestGroupByOp_LimitGroups() {
	df := s.createGroupByTestDataFrame()
	defer df.Release()

	// GroupBy category, groupSize=2, limit=2 (only top 2 groups)
	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		GroupBy("category", 2, 2, 0).
		Execute(df)

	s.Require().NoError(err)
	defer result.Release()

	// Only 4 rows (2 groups * 2 rows)
	s.Equal(int64(4), result.NumRows())

	categories := s.getChunkStringValues(result.Column("category"), 0)

	// Should only have A and B (top 2 groups by score)
	distinctCats := make(map[string]bool)
	for _, cat := range categories {
		distinctCats[cat] = true
	}
	s.Equal(2, len(distinctCats))
	s.True(distinctCats["A"])
	s.True(distinctCats["B"])
	s.False(distinctCats["C"]) // C should be excluded
}

func (s *GroupByOpTestSuite) TestGroupByOp_Offset() {
	df := s.createGroupByTestDataFrame()
	defer df.Release()

	// GroupBy category, groupSize=2, limit=2, offset=1 (skip first group)
	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		GroupBy("category", 2, 2, 1).
		Execute(df)

	s.Require().NoError(err)
	defer result.Release()

	// Only 4 rows (2 groups * 2 rows), skipped group A
	s.Equal(int64(4), result.NumRows())

	categories := s.getChunkStringValues(result.Column("category"), 0)

	// Should have B and C (skipped A)
	distinctCats := make(map[string]bool)
	for _, cat := range categories {
		distinctCats[cat] = true
	}
	s.Equal(2, len(distinctCats))
	s.False(distinctCats["A"]) // A should be skipped
	s.True(distinctCats["B"])
	s.True(distinctCats["C"])
}

func (s *GroupByOpTestSuite) TestGroupByOp_OffsetExceedsGroups() {
	df := s.createGroupByTestDataFrame()
	defer df.Release()

	// offset=10 exceeds number of groups (3)
	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		GroupBy("category", 2, 10, 10).
		Execute(df)

	s.Require().NoError(err)
	defer result.Release()

	// Should return empty result
	s.Equal(int64(0), result.NumRows())
}

func (s *GroupByOpTestSuite) TestGroupByOp_MultipleChunks() {
	df := s.createMultiChunkGroupByTestDataFrame()
	defer df.Release()

	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		GroupBy("category", 2, 10, 0).
		Execute(df)

	s.Require().NoError(err)
	defer result.Release()

	// Each chunk is processed independently
	s.Equal(2, result.NumChunks())

	// Chunk 0: A(0.9,0.8), B(0.7,0.6) - A first (higher max)
	chunk0Cats := s.getChunkStringValues(result.Column("category"), 0)
	s.Equal(4, len(chunk0Cats)) // 2 groups * 2 rows

	// Chunk 1: C(0.85,0.75), D(0.65) - C first (higher max)
	chunk1Cats := s.getChunkStringValues(result.Column("category"), 1)
	s.True(len(chunk1Cats) <= 4)
}

// =============================================================================
// GroupByOp Error Tests
// =============================================================================

func (s *GroupByOpTestSuite) TestGroupByOp_NonExistentGroupField() {
	df := s.createGroupByTestDataFrame()
	defer df.Release()

	ctx := types.NewFuncContextWithStage(s.pool, types.StageL2Rerank)
	op := NewGroupByOp("nonexistent", 2, 10, 0)

	_, err := op.Execute(ctx, df)
	s.Error(err)
	s.Contains(err.Error(), "not found")
}

func (s *GroupByOpTestSuite) TestGroupByOp_NoScoreColumn() {
	// Create DataFrame without $score column
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{3})

	idBuilder := array.NewInt64Builder(s.pool)
	idBuilder.AppendValues([]int64{1, 2, 3}, nil)
	idChunk := idBuilder.NewArray()
	idBuilder.Release()

	catBuilder := array.NewStringBuilder(s.pool)
	catBuilder.AppendValues([]string{"A", "A", "B"}, nil)
	catChunk := catBuilder.NewArray()
	catBuilder.Release()

	err := builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idChunk})
	s.Require().NoError(err)
	err = builder.AddColumnFromChunks("category", []arrow.Array{catChunk})
	s.Require().NoError(err)

	df := builder.Build()
	defer df.Release()

	ctx := types.NewFuncContextWithStage(s.pool, types.StageL2Rerank)
	op := NewGroupByOp("category", 2, 10, 0)

	_, err = op.Execute(ctx, df)
	s.Error(err)
	s.Contains(err.Error(), types.ScoreFieldName)
}

// =============================================================================
// FuncChain GroupBy Method Tests
// =============================================================================

func (s *GroupByOpTestSuite) TestFuncChain_GroupBy() {
	df := s.createGroupByTestDataFrame()
	defer df.Release()

	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		GroupBy("category", 2, 3, 0).
		Execute(df)

	s.Require().NoError(err)
	defer result.Release()

	s.True(result.HasColumn(GroupScoreFieldName))
	s.Equal(int64(6), result.NumRows())
}

func (s *GroupByOpTestSuite) TestFuncChain_GroupBy_InvalidParams() {
	// Empty field
	fc := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		GroupBy("", 2, 10, 0)
	err := fc.Validate()
	s.Error(err)
	s.Contains(err.Error(), "empty")

	// Invalid groupSize
	fc = NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		GroupBy("category", 0, 10, 0)
	err = fc.Validate()
	s.Error(err)
	s.Contains(err.Error(), "groupSize")

	// Invalid limit
	fc = NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		GroupBy("category", 2, 0, 0)
	err = fc.Validate()
	s.Error(err)
	s.Contains(err.Error(), "limit")

	// Invalid offset
	fc = NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		GroupBy("category", 2, 10, -1)
	err = fc.Validate()
	s.Error(err)
	s.Contains(err.Error(), "offset")
}

// =============================================================================
// NewGroupByOpFromRepr Tests
// =============================================================================

func (s *GroupByOpTestSuite) TestNewGroupByOpFromRepr() {
	repr := &OperatorRepr{
		Type: types.OpTypeGroupBy,
		Params: map[string]interface{}{
			"field":      "category",
			"group_size": int64(3),
			"limit":      int64(10),
			"offset":     int64(5),
		},
	}

	op, err := NewGroupByOpFromRepr(repr)
	s.Require().NoError(err)
	s.NotNil(op)

	groupByOp := op.(*GroupByOp)
	s.Equal("category", groupByOp.groupByField)
	s.Equal(int64(3), groupByOp.groupSize)
	s.Equal(int64(10), groupByOp.limit)
	s.Equal(int64(5), groupByOp.offset)
}

func (s *GroupByOpTestSuite) TestNewGroupByOpFromRepr_DefaultOffset() {
	repr := &OperatorRepr{
		Type: types.OpTypeGroupBy,
		Params: map[string]interface{}{
			"field":      "category",
			"group_size": int64(3),
			"limit":      int64(10),
		},
	}

	op, err := NewGroupByOpFromRepr(repr)
	s.Require().NoError(err)

	groupByOp := op.(*GroupByOp)
	s.Equal(int64(0), groupByOp.offset) // Default offset is 0
}

// =============================================================================
// Memory Leak Tests
// =============================================================================

func (s *GroupByOpTestSuite) TestMemoryLeak_GroupByOp() {
	df := s.createGroupByTestDataFrame()
	defer df.Release()

	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		GroupBy("category", 2, 3, 0).
		Execute(df)

	s.Require().NoError(err)
	result.Release()

	// Memory check happens in TearDownTest
}

func (s *GroupByOpTestSuite) TestMemoryLeak_GroupByOp_Error() {
	df := s.createGroupByTestDataFrame()
	defer df.Release()

	ctx := types.NewFuncContextWithStage(s.pool, types.StageL2Rerank)
	op := NewGroupByOp("nonexistent", 2, 10, 0)

	_, err := op.Execute(ctx, df)
	s.Error(err)

	// Memory check happens in TearDownTest
}

// =============================================================================
// Integration Tests
// =============================================================================

func (s *GroupByOpTestSuite) TestGroupByOp_WithSort() {
	df := s.createGroupByTestDataFrame()
	defer df.Release()

	// GroupBy then Sort by $group_score (should already be sorted)
	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		GroupBy("category", 2, 3, 0).
		Sort(GroupScoreFieldName, true).
		Execute(df)

	s.Require().NoError(err)
	defer result.Release()

	// Verify still sorted by group score DESC
	groupScores := s.getChunkFloat32Values(result.Column(GroupScoreFieldName), 0)
	for i := 0; i < len(groupScores)-1; i++ {
		s.GreaterOrEqual(groupScores[i], groupScores[i+1])
	}
}

func (s *GroupByOpTestSuite) TestGroupByOp_WithLimit() {
	df := s.createGroupByTestDataFrame()
	defer df.Release()

	// GroupBy then Limit (total rows)
	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		GroupBy("category", 2, 3, 0).
		Limit(3).
		Execute(df)

	s.Require().NoError(err)
	defer result.Release()

	// Should have only 3 rows
	s.Equal(int64(3), result.NumRows())
}

// =============================================================================
// GroupScorer Tests
// =============================================================================

func (s *GroupByOpTestSuite) TestGroupByOp_ScorerMax() {
	df := s.createGroupByTestDataFrame()
	defer df.Release()

	// Use max scorer (default)
	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		GroupByWithScorer("category", 2, 3, 0, GroupScorerMax).
		Execute(df)

	s.Require().NoError(err)
	defer result.Release()

	// Group A: scores 0.9, 0.8 -> max = 0.9
	// Group B: scores 0.85, 0.6 -> max = 0.85
	// Group C: scores 0.75, 0.65 -> max = 0.75
	groupScores := s.getChunkFloat32Values(result.Column(GroupScoreFieldName), 0)

	// First 2 rows are group A with score 0.9
	s.InDelta(float32(0.9), groupScores[0], 0.001)
	s.InDelta(float32(0.9), groupScores[1], 0.001)

	// Next 2 rows are group B with score 0.85
	s.InDelta(float32(0.85), groupScores[2], 0.001)
	s.InDelta(float32(0.85), groupScores[3], 0.001)
}

func (s *GroupByOpTestSuite) TestGroupByOp_ScorerSum() {
	df := s.createGroupByTestDataFrame()
	defer df.Release()

	// Use sum scorer
	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		GroupByWithScorer("category", 2, 3, 0, GroupScorerSum).
		Execute(df)

	s.Require().NoError(err)
	defer result.Release()

	// Group A: top 2 scores 0.9, 0.8 -> sum = 1.7
	// Group B: top 2 scores 0.85, 0.6 -> sum = 1.45
	// Group C: top 2 scores 0.75, 0.65 -> sum = 1.4
	// Order by sum DESC: A, B, C

	groupScores := s.getChunkFloat32Values(result.Column(GroupScoreFieldName), 0)
	categories := s.getChunkStringValues(result.Column("category"), 0)

	// First 2 rows are group A with sum = 1.7
	s.Equal("A", categories[0])
	s.InDelta(float32(1.7), groupScores[0], 0.001)
	s.InDelta(float32(1.7), groupScores[1], 0.001)

	// Next 2 rows are group B with sum = 1.45
	s.Equal("B", categories[2])
	s.InDelta(float32(1.45), groupScores[2], 0.001)

	// Last 2 rows are group C with sum = 1.4
	s.Equal("C", categories[4])
	s.InDelta(float32(1.4), groupScores[4], 0.001)
}

func (s *GroupByOpTestSuite) TestGroupByOp_ScorerAvg() {
	df := s.createGroupByTestDataFrame()
	defer df.Release()

	// Use avg scorer
	result, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		GroupByWithScorer("category", 2, 3, 0, GroupScorerAvg).
		Execute(df)

	s.Require().NoError(err)
	defer result.Release()

	// Group A: top 2 scores 0.9, 0.8 -> avg = 0.85
	// Group B: top 2 scores 0.85, 0.6 -> avg = 0.725
	// Group C: top 2 scores 0.75, 0.65 -> avg = 0.7
	// Order by avg DESC: A, B, C

	groupScores := s.getChunkFloat32Values(result.Column(GroupScoreFieldName), 0)
	categories := s.getChunkStringValues(result.Column("category"), 0)

	// First 2 rows are group A with avg = 0.85
	s.Equal("A", categories[0])
	s.InDelta(float32(0.85), groupScores[0], 0.001)

	// Next 2 rows are group B with avg = 0.725
	s.Equal("B", categories[2])
	s.InDelta(float32(0.725), groupScores[2], 0.001)

	// Last 2 rows are group C with avg = 0.7
	s.Equal("C", categories[4])
	s.InDelta(float32(0.7), groupScores[4], 0.001)
}

func (s *GroupByOpTestSuite) TestGroupByOp_ScorerChangesGroupOrder() {
	// Create DataFrame where different scorers produce different group orders
	builder := NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{6})

	idBuilder := array.NewInt64Builder(s.pool)
	idBuilder.AppendValues([]int64{1, 2, 3, 4, 5, 6}, nil)
	idChunk := idBuilder.NewArray()
	idBuilder.Release()

	// Group A: 0.9, 0.1 -> max=0.9, sum=1.0, avg=0.5
	// Group B: 0.6, 0.5 -> max=0.6, sum=1.1, avg=0.55
	scoreBuilder := array.NewFloat32Builder(s.pool)
	scoreBuilder.AppendValues([]float32{0.9, 0.1, 0.6, 0.5, 0.3, 0.2}, nil)
	scoreChunk := scoreBuilder.NewArray()
	scoreBuilder.Release()

	catBuilder := array.NewStringBuilder(s.pool)
	catBuilder.AppendValues([]string{"A", "A", "B", "B", "C", "C"}, nil)
	catChunk := catBuilder.NewArray()
	catBuilder.Release()

	builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idChunk})
	builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreChunk})
	builder.AddColumnFromChunks("category", []arrow.Array{catChunk})
	df := builder.Build()
	defer df.Release()

	// With max scorer: A(0.9) > B(0.6) > C(0.3)
	resultMax, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		GroupByWithScorer("category", 2, 3, 0, GroupScorerMax).
		Execute(df)
	s.Require().NoError(err)
	defer resultMax.Release()

	catsMax := s.getChunkStringValues(resultMax.Column("category"), 0)
	s.Equal("A", catsMax[0]) // A is first with max scorer

	// With sum scorer: B(1.1) > A(1.0) > C(0.5)
	resultSum, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		GroupByWithScorer("category", 2, 3, 0, GroupScorerSum).
		Execute(df)
	s.Require().NoError(err)
	defer resultSum.Release()

	catsSum := s.getChunkStringValues(resultSum.Column("category"), 0)
	s.Equal("B", catsSum[0]) // B is first with sum scorer

	// With avg scorer: B(0.55) > A(0.5) > C(0.25)
	resultAvg, err := NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		GroupByWithScorer("category", 2, 3, 0, GroupScorerAvg).
		Execute(df)
	s.Require().NoError(err)
	defer resultAvg.Release()

	catsAvg := s.getChunkStringValues(resultAvg.Column("category"), 0)
	s.Equal("B", catsAvg[0]) // B is first with avg scorer
}

func (s *GroupByOpTestSuite) TestNewGroupByOpFromRepr_WithScorer() {
	// Test max scorer
	repr := &OperatorRepr{
		Type: types.OpTypeGroupBy,
		Params: map[string]interface{}{
			"field":      "category",
			"group_size": int64(3),
			"limit":      int64(10),
			"scorer":     "max",
		},
	}
	op, err := NewGroupByOpFromRepr(repr)
	s.Require().NoError(err)
	s.Equal(GroupScorerMax, op.(*GroupByOp).groupScorer)

	// Test sum scorer
	repr.Params["scorer"] = "sum"
	op, err = NewGroupByOpFromRepr(repr)
	s.Require().NoError(err)
	s.Equal(GroupScorerSum, op.(*GroupByOp).groupScorer)

	// Test avg scorer
	repr.Params["scorer"] = "avg"
	op, err = NewGroupByOpFromRepr(repr)
	s.Require().NoError(err)
	s.Equal(GroupScorerAvg, op.(*GroupByOp).groupScorer)

	// Test invalid scorer
	repr.Params["scorer"] = "invalid"
	_, err = NewGroupByOpFromRepr(repr)
	s.Error(err)
	s.Contains(err.Error(), "invalid group scorer")
}

func (s *GroupByOpTestSuite) TestGroupByOp_String_WithScorer() {
	op := NewGroupByOpWithScorer("category", 2, 10, 5, GroupScorerSum)
	str := op.String()
	s.Contains(str, "scorer=sum")
}
