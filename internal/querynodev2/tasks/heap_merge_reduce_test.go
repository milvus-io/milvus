/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tasks

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/function/chain"
)

// buildTestDF creates a simple test DataFrame with int64 PKs.
// ids and scores are per-chunk (NQ) arrays.
func buildTestDF(pool memory.Allocator, idsPerChunk [][]int64, scoresPerChunk [][]float32) *chain.DataFrame {
	numChunks := len(idsPerChunk)
	chunkSizes := make([]int64, numChunks)

	builder := chain.NewDataFrameBuilder()
	colNames := []string{idFieldName, scoreFieldName}
	collector := chain.NewChunkCollector(colNames, numChunks)

	for i := 0; i < numChunks; i++ {
		chunkSizes[i] = int64(len(idsPerChunk[i]))

		idBuilder := array.NewInt64Builder(pool)
		idBuilder.AppendValues(idsPerChunk[i], nil)
		collector.Set(idFieldName, i, idBuilder.NewArray())
		idBuilder.Release()

		scoreBuilder := array.NewFloat32Builder(pool)
		scoreBuilder.AppendValues(scoresPerChunk[i], nil)
		collector.Set(scoreFieldName, i, scoreBuilder.NewArray())
		scoreBuilder.Release()
	}

	builder.SetChunkSizes(chunkSizes)
	_ = builder.AddColumnFromChunks(idFieldName, collector.Consume(idFieldName))
	_ = builder.AddColumnFromChunks(scoreFieldName, collector.Consume(scoreFieldName))
	collector.Release()

	return builder.Build()
}

// buildTestDFWithGroupBy creates a test DataFrame with int64 PKs and int64 group-by values.
func buildTestDFWithGroupBy(pool memory.Allocator, idsPerChunk [][]int64, scoresPerChunk [][]float32, groupByPerChunk [][]int64, groupByFieldName string) *chain.DataFrame {
	numChunks := len(idsPerChunk)
	chunkSizes := make([]int64, numChunks)

	colNames := []string{idFieldName, scoreFieldName, groupByFieldName}
	collector := chain.NewChunkCollector(colNames, numChunks)

	for i := 0; i < numChunks; i++ {
		chunkSizes[i] = int64(len(idsPerChunk[i]))

		idBuilder := array.NewInt64Builder(pool)
		idBuilder.AppendValues(idsPerChunk[i], nil)
		collector.Set(idFieldName, i, idBuilder.NewArray())
		idBuilder.Release()

		scoreBuilder := array.NewFloat32Builder(pool)
		scoreBuilder.AppendValues(scoresPerChunk[i], nil)
		collector.Set(scoreFieldName, i, scoreBuilder.NewArray())
		scoreBuilder.Release()

		gbBuilder := array.NewInt64Builder(pool)
		gbBuilder.AppendValues(groupByPerChunk[i], nil)
		collector.Set(groupByFieldName, i, gbBuilder.NewArray())
		gbBuilder.Release()
	}

	builder := chain.NewDataFrameBuilder()
	builder.SetChunkSizes(chunkSizes)
	_ = builder.AddColumnFromChunks(idFieldName, collector.Consume(idFieldName))
	_ = builder.AddColumnFromChunks(scoreFieldName, collector.Consume(scoreFieldName))
	_ = builder.AddColumnFromChunks(groupByFieldName, collector.Consume(groupByFieldName))
	collector.Release()

	return builder.Build()
}

// buildTestDFWithElementIndices creates a test DataFrame with int64 PKs and
// int32 element indices (simulating element-level search results).
func buildTestDFWithElementIndices(pool memory.Allocator, idsPerChunk [][]int64, scoresPerChunk [][]float32, elemIdxPerChunk [][]int32) *chain.DataFrame {
	numChunks := len(idsPerChunk)
	chunkSizes := make([]int64, numChunks)

	colNames := []string{idFieldName, scoreFieldName, elementIndicesCol}
	collector := chain.NewChunkCollector(colNames, numChunks)

	for i := 0; i < numChunks; i++ {
		chunkSizes[i] = int64(len(idsPerChunk[i]))

		idBuilder := array.NewInt64Builder(pool)
		idBuilder.AppendValues(idsPerChunk[i], nil)
		collector.Set(idFieldName, i, idBuilder.NewArray())
		idBuilder.Release()

		scoreBuilder := array.NewFloat32Builder(pool)
		scoreBuilder.AppendValues(scoresPerChunk[i], nil)
		collector.Set(scoreFieldName, i, scoreBuilder.NewArray())
		scoreBuilder.Release()

		eiBuilder := array.NewInt32Builder(pool)
		eiBuilder.AppendValues(elemIdxPerChunk[i], nil)
		collector.Set(elementIndicesCol, i, eiBuilder.NewArray())
		eiBuilder.Release()
	}

	builder := chain.NewDataFrameBuilder()
	builder.SetChunkSizes(chunkSizes)
	_ = builder.AddColumnFromChunks(idFieldName, collector.Consume(idFieldName))
	_ = builder.AddColumnFromChunks(scoreFieldName, collector.Consume(scoreFieldName))
	_ = builder.AddColumnFromChunks(elementIndicesCol, collector.Consume(elementIndicesCol))
	collector.Release()

	return builder.Build()
}

func TestHeapMergeReduce_BasicMerge(t *testing.T) {
	pool := memory.NewGoAllocator()

	// Segment 0: NQ=1, results [id=1,score=0.9], [id=2,score=0.7], [id=3,score=0.5]
	df0 := buildTestDF(pool, [][]int64{{1, 2, 3}}, [][]float32{{0.9, 0.7, 0.5}})
	defer df0.Release()

	// Segment 1: NQ=1, results [id=4,score=0.8], [id=5,score=0.6], [id=6,score=0.4]
	df1 := buildTestDF(pool, [][]int64{{4, 5, 6}}, [][]float32{{0.8, 0.6, 0.4}})
	defer df1.Release()

	result, err := heapMergeReduce(pool, []*chain.DataFrame{df0, df1}, 4, nil)
	require.NoError(t, err)
	defer result.DF.Release()

	// Expected merged order: id=1(0.9), id=4(0.8), id=2(0.7), id=5(0.6)
	assert.Equal(t, 1, result.DF.NumChunks())
	assert.Equal(t, int64(4), result.DF.NumRows())

	idCol := result.DF.Column(idFieldName)
	scoreCol := result.DF.Column(scoreFieldName)

	ids := idCol.Chunk(0).(*array.Int64)
	scores := scoreCol.Chunk(0).(*array.Float32)

	assert.Equal(t, int64(1), ids.Value(0))
	assert.Equal(t, int64(4), ids.Value(1))
	assert.Equal(t, int64(2), ids.Value(2))
	assert.Equal(t, int64(5), ids.Value(3))

	assert.InDelta(t, float32(0.9), scores.Value(0), 0.001)
	assert.InDelta(t, float32(0.8), scores.Value(1), 0.001)
	assert.InDelta(t, float32(0.7), scores.Value(2), 0.001)
	assert.InDelta(t, float32(0.6), scores.Value(3), 0.001)
}

func TestHeapMergeReduce_PKDedup(t *testing.T) {
	pool := memory.NewGoAllocator()

	// Same PK=1 in both segments, different scores
	df0 := buildTestDF(pool, [][]int64{{1, 2}}, [][]float32{{0.9, 0.7}})
	defer df0.Release()

	df1 := buildTestDF(pool, [][]int64{{1, 3}}, [][]float32{{0.8, 0.6}})
	defer df1.Release()

	result, err := heapMergeReduce(pool, []*chain.DataFrame{df0, df1}, 3, nil)
	require.NoError(t, err)
	defer result.DF.Release()

	// id=1 from df0 (score=0.9) wins, id=1 from df1 (score=0.8) is deduped
	// Expected: id=1(0.9), id=2(0.7), id=3(0.6)
	assert.Equal(t, int64(3), result.DF.NumRows())

	ids := result.DF.Column(idFieldName).Chunk(0).(*array.Int64)
	assert.Equal(t, int64(1), ids.Value(0))
	assert.Equal(t, int64(2), ids.Value(1))
	assert.Equal(t, int64(3), ids.Value(2))
}

func TestHeapMergeReduce_EqualScoreDeterminism(t *testing.T) {
	pool := memory.NewGoAllocator()

	// heapMergeReduce expects inputs already normalized (score DESC, ties by
	// PK ASC) — the C++ exporter does this. Tests must mirror that contract.
	df0 := buildTestDF(pool, [][]int64{{3, 5}}, [][]float32{{0.9, 0.9}})
	defer df0.Release()

	df1 := buildTestDF(pool, [][]int64{{1, 7}}, [][]float32{{0.9, 0.9}})
	defer df1.Release()

	result, err := heapMergeReduce(pool, []*chain.DataFrame{df0, df1}, 4, nil)
	require.NoError(t, err)
	defer result.DF.Release()

	// Equal score → PK ASC: id=1, id=3, id=5, id=7
	ids := result.DF.Column(idFieldName).Chunk(0).(*array.Int64)
	assert.Equal(t, int64(1), ids.Value(0))
	assert.Equal(t, int64(3), ids.Value(1))
	assert.Equal(t, int64(5), ids.Value(2))
	assert.Equal(t, int64(7), ids.Value(3))
}

// TestHeapMergeReduce_SamePKEqualScore verifies that when the same PK appears
// in multiple segments with equal scores, dedup deterministically keeps one
// entry. With PK ASC tie-breaking, the "first seen" pop picks the sole row,
// and the duplicate from the later segment is dropped.
func TestHeapMergeReduce_SamePKEqualScore(t *testing.T) {
	pool := memory.NewGoAllocator()

	// Both segments have pk=1 at the same score (0.9). Dedup must drop one.
	df0 := buildTestDF(pool, [][]int64{{1, 2}}, [][]float32{{0.9, 0.7}})
	defer df0.Release()

	df1 := buildTestDF(pool, [][]int64{{1, 3}}, [][]float32{{0.9, 0.6}})
	defer df1.Release()

	result, err := heapMergeReduce(pool, []*chain.DataFrame{df0, df1}, 5, nil)
	require.NoError(t, err)
	defer result.DF.Release()

	// Expect 3 distinct rows: pk=1 (once), pk=2, pk=3. Not 4.
	assert.Equal(t, int64(3), result.DF.NumRows())
	ids := result.DF.Column(idFieldName).Chunk(0).(*array.Int64)
	assert.Equal(t, int64(1), ids.Value(0)) // pk=1 at 0.9 (from whichever segment)
	assert.Equal(t, int64(2), ids.Value(1)) // pk=2 at 0.7
	assert.Equal(t, int64(3), ids.Value(2)) // pk=3 at 0.6
}

func TestHeapMergeReduce_MultiNQ(t *testing.T) {
	pool := memory.NewGoAllocator()

	// NQ=2: chunk0=[id=1,0.9 | id=2,0.7], chunk1=[id=10,0.8 | id=11,0.6]
	df0 := buildTestDF(pool,
		[][]int64{{1, 2}, {10, 11}},
		[][]float32{{0.9, 0.7}, {0.8, 0.6}})
	defer df0.Release()

	df1 := buildTestDF(pool,
		[][]int64{{3, 4}, {12, 13}},
		[][]float32{{0.85, 0.65}, {0.75, 0.55}})
	defer df1.Release()

	result, err := heapMergeReduce(pool, []*chain.DataFrame{df0, df1}, 3, nil)
	require.NoError(t, err)
	defer result.DF.Release()

	assert.Equal(t, 2, result.DF.NumChunks())

	// Chunk 0: id=1(0.9), id=3(0.85), id=2(0.7)
	ids0 := result.DF.Column(idFieldName).Chunk(0).(*array.Int64)
	assert.Equal(t, int64(1), ids0.Value(0))
	assert.Equal(t, int64(3), ids0.Value(1))
	assert.Equal(t, int64(2), ids0.Value(2))

	// Chunk 1: id=10(0.8), id=12(0.75), id=11(0.6)
	ids1 := result.DF.Column(idFieldName).Chunk(1).(*array.Int64)
	assert.Equal(t, int64(10), ids1.Value(0))
	assert.Equal(t, int64(12), ids1.Value(1))
	assert.Equal(t, int64(11), ids1.Value(2))
}

// TestHeapMergeReduce_RaggedPerNQ verifies that heapMergeReduce handles
// inputs where per-NQ chunk sizes differ both across NQs within a segment
// AND across segments for the same NQ. This mirrors the post-Truncate
// (Global Refine) layout where survivors per (segment, NQ) are uneven, and
// the GroupBy path where SearchGroupByOperator emits variable-size chunks.
// The invariant heapMergeReduce relies on is only "same number of chunks",
// not "same chunk size".
func TestHeapMergeReduce_RaggedPerNQ(t *testing.T) {
	pool := memory.NewGoAllocator()

	// 3 NQs across 2 segments. Per-NQ row counts (shown as seg0 / seg1):
	//   NQ0: 3 / 1
	//   NQ1: 0 / 4   (seg0 empty for this NQ)
	//   NQ2: 2 / 2
	df0 := buildTestDF(pool,
		[][]int64{{1, 2, 3}, {}, {7, 8}},
		[][]float32{{0.9, 0.8, 0.5}, {}, {0.6, 0.4}})
	defer df0.Release()

	df1 := buildTestDF(pool,
		[][]int64{{10}, {20, 21, 22, 23}, {30, 31}},
		[][]float32{{0.85}, {0.95, 0.75, 0.55, 0.35}, {0.65, 0.45}})
	defer df1.Release()

	result, err := heapMergeReduce(pool, []*chain.DataFrame{df0, df1}, 5, nil)
	require.NoError(t, err)
	defer result.DF.Release()

	assert.Equal(t, 3, result.DF.NumChunks(), "must preserve NQ count")

	// NQ0: seg0 contributes 3, seg1 contributes 1, topK=5 → 4 rows.
	// Global score-sorted order: 0.9, 0.85, 0.8, 0.5
	ids0 := result.DF.Column(idFieldName).Chunk(0).(*array.Int64)
	assert.Equal(t, 4, ids0.Len())
	assert.Equal(t, int64(1), ids0.Value(0))
	assert.Equal(t, int64(10), ids0.Value(1))
	assert.Equal(t, int64(2), ids0.Value(2))
	assert.Equal(t, int64(3), ids0.Value(3))

	// NQ1: seg0 empty, seg1 contributes 4 → 4 rows (topK=5 isn't hit).
	ids1 := result.DF.Column(idFieldName).Chunk(1).(*array.Int64)
	assert.Equal(t, 4, ids1.Len())
	assert.Equal(t, int64(20), ids1.Value(0))
	assert.Equal(t, int64(21), ids1.Value(1))
	assert.Equal(t, int64(22), ids1.Value(2))
	assert.Equal(t, int64(23), ids1.Value(3))

	// NQ2: 2 + 2 = 4 rows. Order: 0.65, 0.6, 0.45, 0.4
	ids2 := result.DF.Column(idFieldName).Chunk(2).(*array.Int64)
	assert.Equal(t, 4, ids2.Len())
	assert.Equal(t, int64(30), ids2.Value(0))
	assert.Equal(t, int64(7), ids2.Value(1))
	assert.Equal(t, int64(31), ids2.Value(2))
	assert.Equal(t, int64(8), ids2.Value(3))
}

func TestHeapMergeReduce_EmptyInput(t *testing.T) {
	pool := memory.NewGoAllocator()

	// One segment has results, one is empty
	df0 := buildTestDF(pool, [][]int64{{1, 2}}, [][]float32{{0.9, 0.7}})
	defer df0.Release()

	df1 := buildTestDF(pool, [][]int64{{}}, [][]float32{{}})
	defer df1.Release()

	result, err := heapMergeReduce(pool, []*chain.DataFrame{df0, df1}, 3, nil)
	require.NoError(t, err)
	defer result.DF.Release()

	// Only results from df0
	assert.Equal(t, int64(2), result.DF.NumRows())
	ids := result.DF.Column(idFieldName).Chunk(0).(*array.Int64)
	assert.Equal(t, int64(1), ids.Value(0))
	assert.Equal(t, int64(2), ids.Value(1))
}

func TestHeapMergeReduce_SingleSegment(t *testing.T) {
	pool := memory.NewGoAllocator()

	// Inputs must be pre-sorted (score DESC, ties by PK ASC).
	df0 := buildTestDF(pool, [][]int64{{1, 2, 3}}, [][]float32{{0.9, 0.7, 0.5}})
	defer df0.Release()

	result, err := heapMergeReduce(pool, []*chain.DataFrame{df0}, 2, nil)
	require.NoError(t, err)
	defer result.DF.Release()

	// Should sort by score DESC and take top 2
	assert.Equal(t, int64(2), result.DF.NumRows())
	ids := result.DF.Column(idFieldName).Chunk(0).(*array.Int64)
	assert.Equal(t, int64(1), ids.Value(0)) // score=0.9
	assert.Equal(t, int64(2), ids.Value(1)) // score=0.7
}

func TestHeapMergeReduce_GroupByBasic(t *testing.T) {
	pool := memory.NewGoAllocator()

	// Segment 0: group A has 3 items, group B has 1
	df0 := buildTestDFWithGroupBy(pool,
		[][]int64{{1, 2, 3, 4}},
		[][]float32{{0.9, 0.8, 0.7, 0.6}},
		[][]int64{{100, 100, 100, 200}}, // group 100 and 200
		groupByCol)
	defer df0.Release()

	// Segment 1: group B has 2 items
	df1 := buildTestDFWithGroupBy(pool,
		[][]int64{{5, 6}},
		[][]float32{{0.85, 0.75}},
		[][]int64{{200, 200}},
		groupByCol)
	defer df1.Release()

	result, err := heapMergeReduce(pool, []*chain.DataFrame{df0, df1}, 2, &groupByOptions{
		GroupSize: 2,
	})
	require.NoError(t, err)
	defer result.DF.Release()

	// topK=2, groupSize=2 → max 4 results
	// group 100: id=1(0.9), id=2(0.8) → 2 items (full)
	// group 200: id=5(0.85), id=4(0.6) or id=6(0.75) → 2 items (full)
	// Expected order by score: 1(0.9), 5(0.85), 2(0.8), 6(0.75)
	assert.True(t, result.DF.NumRows() <= 4)

	ids := result.DF.Column(idFieldName).Chunk(0).(*array.Int64)
	assert.Equal(t, int64(1), ids.Value(0))
	assert.Equal(t, int64(5), ids.Value(1))
	assert.Equal(t, int64(2), ids.Value(2))
	assert.Equal(t, int64(6), ids.Value(3))

	// Verify group_by column exists
	assert.True(t, result.DF.HasColumn(groupByCol))
}

func TestHeapMergeReduce_GroupByMaxGroups(t *testing.T) {
	pool := memory.NewGoAllocator()

	// 4 groups, but topK=2 → only 2 groups allowed
	df0 := buildTestDFWithGroupBy(pool,
		[][]int64{{1, 2, 3, 4}},
		[][]float32{{0.9, 0.8, 0.7, 0.6}},
		[][]int64{{10, 20, 30, 40}},
		groupByCol)
	defer df0.Release()

	result, err := heapMergeReduce(pool, []*chain.DataFrame{df0}, 2, &groupByOptions{
		GroupSize: 1,
	})
	require.NoError(t, err)
	defer result.DF.Release()

	// topK=2, groupSize=1 → max 2 results, 2 groups
	assert.Equal(t, int64(2), result.DF.NumRows())
	ids := result.DF.Column(idFieldName).Chunk(0).(*array.Int64)
	assert.Equal(t, int64(1), ids.Value(0)) // group 10
	assert.Equal(t, int64(2), ids.Value(1)) // group 20
}

func TestHeapMergeReduce_GroupByCompositeKey(t *testing.T) {
	pool := memory.NewGoAllocator()
	groupByCol0 := groupByColumnName(105)
	groupByCol1 := groupByColumnName(106)

	df0 := buildTestDFWithTwoGroupByColumns(pool,
		[][]int64{{1, 2, 3, 4}},
		[][]float32{{0.95, 0.9, 0.85, 0.8}},
		[][]int64{{10, 10, 10, 20}},
		[][]string{{"a", "b", "a", "a"}},
		groupByCol0,
		groupByCol1)
	defer df0.Release()

	result, err := heapMergeReduce(pool, []*chain.DataFrame{df0}, 3, &groupByOptions{
		GroupSize: 1,
		Columns:   []string{groupByCol0, groupByCol1},
	})
	require.NoError(t, err)
	defer result.DF.Release()

	ids := result.DF.Column(idFieldName).Chunk(0).(*array.Int64)
	require.Equal(t, 3, ids.Len())
	assert.Equal(t, int64(1), ids.Value(0)) // (10, a)
	assert.Equal(t, int64(2), ids.Value(1)) // (10, b), distinct from (10, a)
	assert.Equal(t, int64(4), ids.Value(2)) // (20, a); id=3 is duplicate composite group

	require.True(t, result.DF.HasColumn(groupByCol0))
	require.True(t, result.DF.HasColumn(groupByCol1))
	gb0 := result.DF.Column(groupByCol0).Chunk(0).(*array.Int64)
	gb1 := result.DF.Column(groupByCol1).Chunk(0).(*array.String)
	assert.Equal(t, []int64{10, 10, 20}, []int64{gb0.Value(0), gb0.Value(1), gb0.Value(2)})
	assert.Equal(t, []string{"a", "b", "a"}, []string{gb1.Value(0), gb1.Value(1), gb1.Value(2)})
}

func buildTestDFWithTwoGroupByColumns(
	pool memory.Allocator,
	idsPerChunk [][]int64,
	scoresPerChunk [][]float32,
	groupBy0PerChunk [][]int64,
	groupBy1PerChunk [][]string,
	groupByCol0 string,
	groupByCol1 string,
) *chain.DataFrame {
	numChunks := len(idsPerChunk)
	chunkSizes := make([]int64, numChunks)

	colNames := []string{idFieldName, scoreFieldName, groupByCol0, groupByCol1}
	collector := chain.NewChunkCollector(colNames, numChunks)

	for i := 0; i < numChunks; i++ {
		chunkSizes[i] = int64(len(idsPerChunk[i]))

		idBuilder := array.NewInt64Builder(pool)
		idBuilder.AppendValues(idsPerChunk[i], nil)
		collector.Set(idFieldName, i, idBuilder.NewArray())
		idBuilder.Release()

		scoreBuilder := array.NewFloat32Builder(pool)
		scoreBuilder.AppendValues(scoresPerChunk[i], nil)
		collector.Set(scoreFieldName, i, scoreBuilder.NewArray())
		scoreBuilder.Release()

		gb0Builder := array.NewInt64Builder(pool)
		gb0Builder.AppendValues(groupBy0PerChunk[i], nil)
		collector.Set(groupByCol0, i, gb0Builder.NewArray())
		gb0Builder.Release()

		gb1Builder := array.NewStringBuilder(pool)
		gb1Builder.AppendValues(groupBy1PerChunk[i], nil)
		collector.Set(groupByCol1, i, gb1Builder.NewArray())
		gb1Builder.Release()
	}

	builder := chain.NewDataFrameBuilder()
	builder.SetChunkSizes(chunkSizes)
	_ = builder.AddColumnFromChunks(idFieldName, collector.Consume(idFieldName))
	_ = builder.AddColumnFromChunks(scoreFieldName, collector.Consume(scoreFieldName))
	_ = builder.AddColumnFromChunks(groupByCol0, collector.Consume(groupByCol0))
	_ = builder.AddColumnFromChunks(groupByCol1, collector.Consume(groupByCol1))
	collector.Release()

	return builder.Build()
}

// buildTestDFWithGroupByArr accepts a pre-built arrow.Array per chunk so tests
// can exercise group-by value types beyond the int64 covered by buildTestDFWithGroupBy.
func buildTestDFWithGroupByArr(pool memory.Allocator, idsPerChunk [][]int64, scoresPerChunk [][]float32, groupByArrPerChunk []arrow.Array) *chain.DataFrame {
	numChunks := len(idsPerChunk)
	chunkSizes := make([]int64, numChunks)

	colNames := []string{idFieldName, scoreFieldName, groupByCol}
	collector := chain.NewChunkCollector(colNames, numChunks)

	for i := 0; i < numChunks; i++ {
		chunkSizes[i] = int64(len(idsPerChunk[i]))

		idBuilder := array.NewInt64Builder(pool)
		idBuilder.AppendValues(idsPerChunk[i], nil)
		collector.Set(idFieldName, i, idBuilder.NewArray())
		idBuilder.Release()

		scoreBuilder := array.NewFloat32Builder(pool)
		scoreBuilder.AppendValues(scoresPerChunk[i], nil)
		collector.Set(scoreFieldName, i, scoreBuilder.NewArray())
		scoreBuilder.Release()

		collector.Set(groupByCol, i, groupByArrPerChunk[i])
	}

	builder := chain.NewDataFrameBuilder()
	builder.SetChunkSizes(chunkSizes)
	_ = builder.AddColumnFromChunks(idFieldName, collector.Consume(idFieldName))
	_ = builder.AddColumnFromChunks(scoreFieldName, collector.Consume(scoreFieldName))
	_ = builder.AddColumnFromChunks(groupByCol, collector.Consume(groupByCol))
	collector.Release()

	return builder.Build()
}

// TestHeapMergeReduce_GroupByValueTypes covers the dispatcher cases (Int8/16/32,
// String) not exercised by the int64/bool tests above.
func TestHeapMergeReduce_GroupByValueTypes(t *testing.T) {
	idsSeg0 := [][]int64{{1, 2, 3, 4}}
	scoresSeg0 := [][]float32{{0.9, 0.8, 0.7, 0.6}}
	idsSeg1 := [][]int64{{5, 6}}
	scoresSeg1 := [][]float32{{0.85, 0.75}}

	expectedIDs := []int64{1, 5, 2, 6}

	tests := []struct {
		name      string
		buildSeg0 func(pool memory.Allocator) arrow.Array
		buildSeg1 func(pool memory.Allocator) arrow.Array
		assertCol func(t *testing.T, arr arrow.Array)
	}{
		{
			name: "Int8",
			buildSeg0: func(pool memory.Allocator) arrow.Array {
				b := array.NewInt8Builder(pool)
				defer b.Release()
				b.AppendValues([]int8{10, 10, 10, 20}, nil)
				return b.NewArray()
			},
			buildSeg1: func(pool memory.Allocator) arrow.Array {
				b := array.NewInt8Builder(pool)
				defer b.Release()
				b.AppendValues([]int8{20, 20}, nil)
				return b.NewArray()
			},
			assertCol: func(t *testing.T, arr arrow.Array) {
				a, ok := arr.(*array.Int8)
				require.True(t, ok, "group-by column should be *array.Int8")
				// Group values follow same order as expectedIDs: A, B, A, B.
				assert.Equal(t, int8(10), a.Value(0))
				assert.Equal(t, int8(20), a.Value(1))
				assert.Equal(t, int8(10), a.Value(2))
				assert.Equal(t, int8(20), a.Value(3))
			},
		},
		{
			name: "Int16",
			buildSeg0: func(pool memory.Allocator) arrow.Array {
				b := array.NewInt16Builder(pool)
				defer b.Release()
				b.AppendValues([]int16{100, 100, 100, 200}, nil)
				return b.NewArray()
			},
			buildSeg1: func(pool memory.Allocator) arrow.Array {
				b := array.NewInt16Builder(pool)
				defer b.Release()
				b.AppendValues([]int16{200, 200}, nil)
				return b.NewArray()
			},
			assertCol: func(t *testing.T, arr arrow.Array) {
				a, ok := arr.(*array.Int16)
				require.True(t, ok, "group-by column should be *array.Int16")
				assert.Equal(t, int16(100), a.Value(0))
				assert.Equal(t, int16(200), a.Value(1))
				assert.Equal(t, int16(100), a.Value(2))
				assert.Equal(t, int16(200), a.Value(3))
			},
		},
		{
			name: "Int32",
			buildSeg0: func(pool memory.Allocator) arrow.Array {
				b := array.NewInt32Builder(pool)
				defer b.Release()
				b.AppendValues([]int32{1000, 1000, 1000, 2000}, nil)
				return b.NewArray()
			},
			buildSeg1: func(pool memory.Allocator) arrow.Array {
				b := array.NewInt32Builder(pool)
				defer b.Release()
				b.AppendValues([]int32{2000, 2000}, nil)
				return b.NewArray()
			},
			assertCol: func(t *testing.T, arr arrow.Array) {
				a, ok := arr.(*array.Int32)
				require.True(t, ok, "group-by column should be *array.Int32")
				assert.Equal(t, int32(1000), a.Value(0))
				assert.Equal(t, int32(2000), a.Value(1))
				assert.Equal(t, int32(1000), a.Value(2))
				assert.Equal(t, int32(2000), a.Value(3))
			},
		},
		{
			name: "String",
			buildSeg0: func(pool memory.Allocator) arrow.Array {
				b := array.NewStringBuilder(pool)
				defer b.Release()
				b.AppendValues([]string{"groupA", "groupA", "groupA", "groupB"}, nil)
				return b.NewArray()
			},
			buildSeg1: func(pool memory.Allocator) arrow.Array {
				b := array.NewStringBuilder(pool)
				defer b.Release()
				b.AppendValues([]string{"groupB", "groupB"}, nil)
				return b.NewArray()
			},
			assertCol: func(t *testing.T, arr arrow.Array) {
				a, ok := arr.(*array.String)
				require.True(t, ok, "group-by column should be *array.String")
				assert.Equal(t, "groupA", a.Value(0))
				assert.Equal(t, "groupB", a.Value(1))
				assert.Equal(t, "groupA", a.Value(2))
				assert.Equal(t, "groupB", a.Value(3))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool := memory.NewGoAllocator()

			df0 := buildTestDFWithGroupByArr(pool,
				idsSeg0, scoresSeg0,
				[]arrow.Array{tc.buildSeg0(pool)},
			)
			defer df0.Release()

			df1 := buildTestDFWithGroupByArr(pool,
				idsSeg1, scoresSeg1,
				[]arrow.Array{tc.buildSeg1(pool)},
			)
			defer df1.Release()

			result, err := heapMergeReduce(pool, []*chain.DataFrame{df0, df1}, 2, &groupByOptions{
				GroupSize: 2,
			})
			require.NoError(t, err)
			defer result.DF.Release()

			require.Equal(t, int64(4), result.DF.NumRows(),
				"topK=2 * groupSize=2 = 4 rows expected")

			ids := result.DF.Column(idFieldName).Chunk(0).(*array.Int64)
			for i, want := range expectedIDs {
				assert.Equal(t, want, ids.Value(i), "id mismatch at row %d", i)
			}

			require.True(t, result.DF.HasColumn(groupByCol),
				"group-by column should be propagated to output")
			tc.assertCol(t, result.DF.Column(groupByCol).Chunk(0))
		})
	}
}

func TestHeapMergeReduce_SourceTracking(t *testing.T) {
	pool := memory.NewGoAllocator()

	df0 := buildTestDF(pool, [][]int64{{1, 2}}, [][]float32{{0.9, 0.7}})
	defer df0.Release()

	df1 := buildTestDF(pool, [][]int64{{3}}, [][]float32{{0.8}})
	defer df1.Release()

	result, err := heapMergeReduce(pool, []*chain.DataFrame{df0, df1}, 3, nil)
	require.NoError(t, err)
	defer result.DF.Release()

	// Verify source tracking
	require.Len(t, result.Sources, 1) // 1 chunk
	sources := result.Sources[0]
	require.Len(t, sources, 3)

	// id=1(0.9) from input 0
	assert.Equal(t, 0, sources[0].InputIdx)
	// id=3(0.8) from input 1
	assert.Equal(t, 1, sources[1].InputIdx)
	// id=2(0.7) from input 0
	assert.Equal(t, 0, sources[2].InputIdx)
}

func TestHeapMergeReduce_LargeScale(t *testing.T) {
	pool := memory.NewGoAllocator()

	// 4 segments, each with 100 results
	numSegments := 4
	numResults := 100
	dfs := make([]*chain.DataFrame, numSegments)
	for seg := 0; seg < numSegments; seg++ {
		ids := make([]int64, numResults)
		scores := make([]float32, numResults)
		for i := 0; i < numResults; i++ {
			ids[i] = int64(seg*1000 + i)
			scores[i] = float32(numResults-i) / float32(numResults) // descending scores
		}
		dfs[seg] = buildTestDF(pool, [][]int64{ids}, [][]float32{scores})
		defer dfs[seg].Release()
	}

	result, err := heapMergeReduce(pool, dfs, 50, nil)
	require.NoError(t, err)
	defer result.DF.Release()

	assert.Equal(t, int64(50), result.DF.NumRows())

	// Verify scores are in descending order
	scoreCol := result.DF.Column(scoreFieldName).Chunk(0).(*array.Float32)
	for i := 1; i < 50; i++ {
		assert.GreaterOrEqual(t, scoreCol.Value(i-1), scoreCol.Value(i),
			"scores should be descending at index %d", i)
	}
}

func TestHeapMergeReduce_NoInputs(t *testing.T) {
	pool := memory.NewGoAllocator()
	_, err := heapMergeReduce(pool, nil, 10, nil)
	assert.Error(t, err)
}

func TestHeapMergeReduce_MismatchedChunks(t *testing.T) {
	pool := memory.NewGoAllocator()

	df0 := buildTestDF(pool, [][]int64{{1}, {2}}, [][]float32{{0.9}, {0.8}})
	defer df0.Release()

	df1 := buildTestDF(pool, [][]int64{{3}}, [][]float32{{0.7}})
	defer df1.Release()

	_, err := heapMergeReduce(pool, []*chain.DataFrame{df0, df1}, 10, nil)
	assert.Error(t, err)
}

// TestHeapMergeReduce_ElementIndicesBasic verifies that the $element_indices
// column is propagated through the heap merge. Each result row should carry
// the element index from its original segment.
func TestHeapMergeReduce_ElementIndicesBasic(t *testing.T) {
	pool := memory.NewGoAllocator()

	// Segment 0: doc 1 matched element 2, doc 2 matched element 0
	df0 := buildTestDFWithElementIndices(pool,
		[][]int64{{1, 2}},
		[][]float32{{0.9, 0.7}},
		[][]int32{{2, 0}})
	defer df0.Release()

	// Segment 1: doc 3 matched element 5, doc 4 matched element 1
	df1 := buildTestDFWithElementIndices(pool,
		[][]int64{{3, 4}},
		[][]float32{{0.8, 0.6}},
		[][]int32{{5, 1}})
	defer df1.Release()

	result, err := heapMergeReduce(pool, []*chain.DataFrame{df0, df1}, 4, nil)
	require.NoError(t, err)
	defer result.DF.Release()

	// Merged order by score: doc1(0.9,ei=2), doc3(0.8,ei=5), doc2(0.7,ei=0), doc4(0.6,ei=1)
	require.True(t, result.DF.HasColumn(elementIndicesCol),
		"merged DataFrame should have $element_indices column")

	eiCol := result.DF.Column(elementIndicesCol).Chunk(0).(*array.Int32)
	require.Equal(t, 4, eiCol.Len())

	assert.Equal(t, int32(2), eiCol.Value(0)) // doc1 from seg0
	assert.Equal(t, int32(5), eiCol.Value(1)) // doc3 from seg1
	assert.Equal(t, int32(0), eiCol.Value(2)) // doc2 from seg0
	assert.Equal(t, int32(1), eiCol.Value(3)) // doc4 from seg1
}

// TestHeapMergeReduce_ElementIndicesDedup verifies that when duplicate PKs are
// removed, the correct element index (from the winning row) is kept.
func TestHeapMergeReduce_ElementIndicesDedup(t *testing.T) {
	pool := memory.NewGoAllocator()

	// Both segments have pk=1. Seg0 has higher score, so its element index (3) should win.
	df0 := buildTestDFWithElementIndices(pool,
		[][]int64{{1, 2}},
		[][]float32{{0.9, 0.5}},
		[][]int32{{3, 7}})
	defer df0.Release()

	df1 := buildTestDFWithElementIndices(pool,
		[][]int64{{1, 3}},
		[][]float32{{0.8, 0.6}},
		[][]int32{{9, 4}})
	defer df1.Release()

	result, err := heapMergeReduce(pool, []*chain.DataFrame{df0, df1}, 3, nil)
	require.NoError(t, err)
	defer result.DF.Release()

	// pk=1(0.9,ei=3) wins over pk=1(0.8,ei=9)
	// Order: pk=1(0.9,ei=3), pk=3(0.6,ei=4), pk=2(0.5,ei=7)
	ids := result.DF.Column(idFieldName).Chunk(0).(*array.Int64)
	eiCol := result.DF.Column(elementIndicesCol).Chunk(0).(*array.Int32)
	require.Equal(t, 3, ids.Len())

	assert.Equal(t, int64(1), ids.Value(0))
	assert.Equal(t, int32(3), eiCol.Value(0)) // seg0's element index, not seg1's 9

	assert.Equal(t, int64(3), ids.Value(1))
	assert.Equal(t, int32(4), eiCol.Value(1))

	assert.Equal(t, int64(2), ids.Value(2))
	assert.Equal(t, int32(7), eiCol.Value(2))
}

// TestHeapMergeReduce_ElementIndicesMultiNQ verifies element indices propagation
// across multiple NQ chunks.
func TestHeapMergeReduce_ElementIndicesMultiNQ(t *testing.T) {
	pool := memory.NewGoAllocator()

	df0 := buildTestDFWithElementIndices(pool,
		[][]int64{{1, 2}, {10, 11}},
		[][]float32{{0.9, 0.7}, {0.8, 0.6}},
		[][]int32{{0, 1}, {2, 3}})
	defer df0.Release()

	df1 := buildTestDFWithElementIndices(pool,
		[][]int64{{3}, {12}},
		[][]float32{{0.85}, {0.75}},
		[][]int32{{4}, {5}})
	defer df1.Release()

	result, err := heapMergeReduce(pool, []*chain.DataFrame{df0, df1}, 3, nil)
	require.NoError(t, err)
	defer result.DF.Release()

	assert.Equal(t, 2, result.DF.NumChunks())

	// Chunk 0: doc1(0.9,ei=0), doc3(0.85,ei=4), doc2(0.7,ei=1)
	ei0 := result.DF.Column(elementIndicesCol).Chunk(0).(*array.Int32)
	assert.Equal(t, int32(0), ei0.Value(0))
	assert.Equal(t, int32(4), ei0.Value(1))
	assert.Equal(t, int32(1), ei0.Value(2))

	// Chunk 1: doc10(0.8,ei=2), doc12(0.75,ei=5), doc11(0.6,ei=3)
	ei1 := result.DF.Column(elementIndicesCol).Chunk(1).(*array.Int32)
	assert.Equal(t, int32(2), ei1.Value(0))
	assert.Equal(t, int32(5), ei1.Value(1))
	assert.Equal(t, int32(3), ei1.Value(2))
}

// TestHeapMergeReduce_NoElementIndices verifies that when inputs don't have
// $element_indices, the output also doesn't — no spurious column is created.
func TestHeapMergeReduce_NoElementIndices(t *testing.T) {
	pool := memory.NewGoAllocator()

	df0 := buildTestDF(pool, [][]int64{{1, 2}}, [][]float32{{0.9, 0.7}})
	defer df0.Release()

	result, err := heapMergeReduce(pool, []*chain.DataFrame{df0}, 2, nil)
	require.NoError(t, err)
	defer result.DF.Release()

	assert.False(t, result.DF.HasColumn(elementIndicesCol),
		"no $element_indices column should exist when inputs lack it")
}
