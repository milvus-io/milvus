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
	"container/heap"
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var defaultAllocator = memory.DefaultAllocator

const (
	idFieldName    = types.IDFieldName
	scoreFieldName = types.ScoreFieldName
	segOffsetCol   = types.SegOffsetFieldName

	// elementIndicesCol is a segment-specific column carrying per-row element
	// indices for element-level (Struct Array) search. The C++ Arrow exporter
	// populates it when SearchResult::element_level_ is true; the reduce
	// pipeline propagates it and marshal writes it to
	// SearchResultData.ElementIndices. It lives here (not in the generic chain
	// package) because its semantics are specific to segment search results.
	elementIndicesCol = "$element_indices"
)

// groupByOptions configures GroupBy mode for heapMergeReduce.
type groupByOptions struct {
	GroupSize int64    // max results per group
	Columns   []string // $group_by_<fieldID> columns in composite-key order
}

// segmentSource records the origin of each result row (for Late Materialization).
type segmentSource struct {
	InputIdx    int   // which input DataFrame
	SegOffset   int64 // original segment offset (-1 if not available)
	OriginalIdx int   // original row index in the source chunk array
}

// mergeResult contains the merged result and source tracking info.
type mergeResult struct {
	DF      *chain.DataFrame  // merged result with $id + $score [+ $group_by] [+ $element_indices]
	Sources [][]segmentSource // per-chunk (NQ) sources for Late Materialization
}

// heapMergeReduce merges per-segment DataFrames via k-way heap merge with PK
// deduplication (same PK across segments → keep the highest-scoring row).
//
// Each input DataFrame must have $id and $score columns with the same number
// of chunks (NQ). $seg_offset is optional (Late Materialization tracking).
//
// Input ordering contract: within each chunk, rows MUST be pre-sorted by score
// DESC with equal-score ties broken by PK ASC. The function does not re-sort
// internally — any producer that rewrites $score (e.g. L0 rerank) must restore
// this order before calling. Violating it yields wrong topK results and
// non-deterministic dedup among equal-score runs.
func heapMergeReduce(
	pool memory.Allocator,
	inputs []*chain.DataFrame,
	topK int64,
	groupByOpts *groupByOptions,
) (*mergeResult, error) {
	if len(inputs) == 0 {
		return nil, merr.WrapErrServiceInternal("heapMergeReduce: no inputs")
	}
	if groupByOpts != nil && len(groupByOpts.Columns) == 0 {
		columns := groupByColumnNames(inputs[0])
		if len(columns) == 0 {
			groupByOpts = nil
		} else {
			opts := *groupByOpts
			opts.Columns = columns
			groupByOpts = &opts
		}
	}

	numChunks := inputs[0].NumChunks()
	for i, df := range inputs {
		if df.NumChunks() != numChunks {
			return nil, merr.WrapErrServiceInternal(
				fmt.Sprintf("heapMergeReduce: input %d has %d chunks, expected %d", i, df.NumChunks(), numChunks))
		}
	}

	// Detect PK type from first input
	idCol := inputs[0].Column(idFieldName)
	if idCol == nil {
		return nil, merr.WrapErrServiceInternal("heapMergeReduce: $id column not found")
	}

	isStringPK := idCol.DataType().ID() == arrow.STRING
	return heapMergeReduceImpl(pool, inputs, topK, groupByOpts, numChunks, isStringPK)
}

// inputCols holds per-input column references resolved once before the per-NQ
// chunk loop. Resolving via df.Column() inside the loop costs a map lookup per
// input per chunk; for many-segment / multi-NQ requests this dominates.
type inputCols struct {
	id             *arrow.Chunked
	score          *arrow.Chunked
	segOffset      *arrow.Chunked // may be nil (tests)
	groupBys       []*arrow.Chunked
	elementIndices *arrow.Chunked // nil unless element-level search
}

func resolveInputCols(inputs []*chain.DataFrame, groupByOpts *groupByOptions, hasElementIndices bool) []inputCols {
	cols := make([]inputCols, len(inputs))
	for i, df := range inputs {
		cols[i] = inputCols{
			id:        df.Column(idFieldName),
			score:     df.Column(scoreFieldName),
			segOffset: df.Column(segOffsetCol),
		}
		if groupByOpts != nil {
			cols[i].groupBys = make([]*arrow.Chunked, len(groupByOpts.Columns))
			for j, name := range groupByOpts.Columns {
				cols[i].groupBys[j] = df.Column(name)
			}
		}
		if hasElementIndices {
			cols[i].elementIndices = df.Column(elementIndicesCol)
		}
	}
	return cols
}

func heapMergeReduceImpl(
	pool memory.Allocator,
	inputs []*chain.DataFrame,
	topK int64,
	groupByOpts *groupByOptions,
	numChunks int,
	isStringPK bool,
) (*mergeResult, error) {
	hasGroupBy := groupByOpts != nil
	hasElementIndices := len(inputs) > 0 && inputs[0].HasColumn(elementIndicesCol)
	chunkSizes := make([]int64, numChunks)
	allSources := make([][]segmentSource, numChunks)

	outCols := []string{idFieldName, scoreFieldName}
	if hasGroupBy {
		outCols = append(outCols, groupByOpts.Columns...)
	}
	if hasElementIndices {
		outCols = append(outCols, elementIndicesCol)
	}

	cols := resolveInputCols(inputs, groupByOpts, hasElementIndices)

	collector := chain.NewChunkCollector(outCols, numChunks)
	defer collector.Release()

	for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
		entries := buildMergeEntries(cols, chunkIdx, hasGroupBy, isStringPK)

		var resultSources []segmentSource
		if isStringPK {
			resultSources = mergeChunkStringPk(pool, collector, entries, chunkIdx, topK, groupByOpts, cols)
		} else {
			resultSources = mergeChunkInt64Pk(pool, collector, entries, chunkIdx, topK, groupByOpts, cols)
		}

		if hasElementIndices {
			eiArr := pickElementIndicesValues(pool, cols, chunkIdx, resultSources)
			collector.Set(elementIndicesCol, chunkIdx, eiArr)
		}

		chunkSizes[chunkIdx] = int64(len(resultSources))
		allSources[chunkIdx] = resultSources
	}

	builder := chain.NewDataFrameBuilder()
	defer builder.Release()
	builder.SetChunkSizes(chunkSizes)

	for _, colName := range outCols {
		if err := builder.AddColumnFromChunks(colName, collector.Consume(colName)); err != nil {
			return nil, err
		}
		builder.CopyFieldMetadata(inputs[0], colName)
	}
	builder.CopyAllMetadata(inputs[0])

	return &mergeResult{
		DF:      builder.Build(),
		Sources: allSources,
	}, nil
}

// buildMergeEntries creates a mergeEntry for each input's chunk.
// Row order is assumed pre-normalized per the heapMergeReduce contract.
func buildMergeEntries(
	cols []inputCols,
	chunkIdx int,
	hasGroupBy bool,
	isStringPK bool,
) []*mergeEntry {
	entries := make([]*mergeEntry, 0, len(cols))
	for inputIdx, c := range cols {
		if c.id == nil || c.score == nil {
			continue
		}
		idChunk := c.id.Chunk(chunkIdx)
		scoreChunk := c.score.Chunk(chunkIdx)
		if idChunk.Len() == 0 {
			continue
		}

		entry := &mergeEntry{
			inputIdx: inputIdx,
			scoreArr: scoreChunk.(*array.Float32),
		}

		if isStringPK {
			entry.idString = idChunk.(*array.String)
		} else {
			entry.idInt64 = idChunk.(*array.Int64)
		}

		// $seg_offset is expected on real search results but tests may omit it;
		// segOffsetVal handles a nil array by returning -1.
		if c.segOffset != nil {
			entry.segOffsetArr = c.segOffset.Chunk(chunkIdx).(*array.Int64)
		}
		if hasGroupBy {
			entry.groupByArrs = make([]arrow.Array, len(c.groupBys))
			for j, groupBy := range c.groupBys {
				if groupBy != nil {
					entry.groupByArrs[j] = groupBy.Chunk(chunkIdx)
				}
			}
		}

		entries = append(entries, entry)
	}
	return entries
}

// mergeChunkInt64Pk performs the k-way merge for one chunk with int64 PK.
func mergeChunkInt64Pk(
	pool memory.Allocator,
	collector *chain.ChunkCollector,
	entries []*mergeEntry,
	chunkIdx int,
	topK int64,
	groupByOpts *groupByOptions,
	cols []inputCols,
) []segmentSource {
	h := &mergeHeapInt64Pk{}
	heap.Init(h)
	for _, e := range entries {
		heap.Push(h, e)
	}

	var ids []int64
	var scores []float32
	var sources []segmentSource

	if groupByOpts != nil {
		ids, scores, sources = mergeGroupByInt64Pk(h, topK, groupByOpts.GroupSize, len(groupByOpts.Columns))
	} else {
		ids, scores, sources = mergeStandardInt64Pk(h, topK)
	}

	idBuilder := array.NewInt64Builder(pool)
	idBuilder.AppendValues(ids, nil)
	collector.Set(idFieldName, chunkIdx, idBuilder.NewArray())
	idBuilder.Release()

	scoreBuilder := array.NewFloat32Builder(pool)
	scoreBuilder.AppendValues(scores, nil)
	collector.Set(scoreFieldName, chunkIdx, scoreBuilder.NewArray())
	scoreBuilder.Release()

	if groupByOpts != nil {
		for i, name := range groupByOpts.Columns {
			gbArr := pickGroupByValues(pool, cols, chunkIdx, sources, i)
			collector.Set(name, chunkIdx, gbArr)
		}
	}

	return sources
}

// mergeChunkStringPk performs the k-way merge for one chunk with string PK.
func mergeChunkStringPk(
	pool memory.Allocator,
	collector *chain.ChunkCollector,
	entries []*mergeEntry,
	chunkIdx int,
	topK int64,
	groupByOpts *groupByOptions,
	cols []inputCols,
) []segmentSource {
	h := &mergeHeapStringPk{}
	heap.Init(h)
	for _, e := range entries {
		heap.Push(h, e)
	}

	var ids []string
	var scores []float32
	var sources []segmentSource

	if groupByOpts != nil {
		ids, scores, sources = mergeGroupByStringPk(h, topK, groupByOpts.GroupSize, len(groupByOpts.Columns))
	} else {
		ids, scores, sources = mergeStandardStringPk(h, topK)
	}

	idBuilder := array.NewStringBuilder(pool)
	idBuilder.AppendValues(ids, nil)
	collector.Set(idFieldName, chunkIdx, idBuilder.NewArray())
	idBuilder.Release()

	scoreBuilder := array.NewFloat32Builder(pool)
	scoreBuilder.AppendValues(scores, nil)
	collector.Set(scoreFieldName, chunkIdx, scoreBuilder.NewArray())
	scoreBuilder.Release()

	if groupByOpts != nil {
		for i, name := range groupByOpts.Columns {
			gbArr := pickGroupByValues(pool, cols, chunkIdx, sources, i)
			collector.Set(name, chunkIdx, gbArr)
		}
	}

	return sources
}

// mergeStandardInt64Pk performs standard k-way merge for one NQ (int64 PK).
func mergeStandardInt64Pk(h *mergeHeapInt64Pk, topK int64) ([]int64, []float32, []segmentSource) {
	pkSet := make(map[int64]struct{}, topK)
	ids := make([]int64, 0, topK)
	scores := make([]float32, 0, topK)
	sources := make([]segmentSource, 0, topK)

	for int64(len(ids)) < topK && h.Len() > 0 {
		e := heap.Pop(h).(*mergeEntry)
		pk := e.idInt64Val()

		if _, dup := pkSet[pk]; !dup {
			ids = append(ids, pk)
			scores = append(scores, e.scoreVal())
			pkSet[pk] = struct{}{}
			sources = append(sources, segmentSource{
				InputIdx:    e.inputIdx,
				SegOffset:   e.segOffsetVal(),
				OriginalIdx: e.cursor,
			})
		}

		if e.advance() {
			heap.Push(h, e)
		}
	}
	return ids, scores, sources
}

// mergeGroupByInt64Pk performs GroupBy-aware k-way merge for one NQ (int64 PK).
func mergeGroupByInt64Pk(
	h *mergeHeapInt64Pk,
	topK, groupSize int64,
	numGroupFields int,
) ([]int64, []float32, []segmentSource) {
	if numGroupFields == 0 {
		return mergeStandardInt64Pk(h, topK)
	}
	totalLimit := topK * groupSize
	pkSet := make(map[int64]struct{}, totalLimit)
	counter := newCompositeGroupCounter(topK, groupSize)

	ids := make([]int64, 0, totalLimit)
	scores := make([]float32, 0, totalLimit)
	sources := make([]segmentSource, 0, totalLimit)

	for int64(len(ids)) < totalLimit && h.Len() > 0 {
		e := heap.Pop(h).(*mergeEntry)
		pk := e.idInt64Val()

		if _, dup := pkSet[pk]; dup {
			if e.advance() {
				heap.Push(h, e)
			}
			continue
		}

		values := extractCompositeGroupValues(e, numGroupFields)
		if !counter.shouldAccept(values) {
			if e.advance() {
				heap.Push(h, e)
			}
			continue
		}

		ids = append(ids, pk)
		scores = append(scores, e.scoreVal())
		pkSet[pk] = struct{}{}
		sources = append(sources, segmentSource{
			InputIdx:    e.inputIdx,
			SegOffset:   e.segOffsetVal(),
			OriginalIdx: e.cursor,
		})

		if e.advance() {
			heap.Push(h, e)
		}
	}
	return ids, scores, sources
}

// mergeStandardStringPk performs standard k-way merge for one NQ (string PK).
func mergeStandardStringPk(h *mergeHeapStringPk, topK int64) ([]string, []float32, []segmentSource) {
	pkSet := make(map[string]struct{}, topK)
	ids := make([]string, 0, topK)
	scores := make([]float32, 0, topK)
	sources := make([]segmentSource, 0, topK)

	for int64(len(ids)) < topK && h.Len() > 0 {
		e := heap.Pop(h).(*mergeEntry)
		pk := e.idStringVal()

		if _, dup := pkSet[pk]; !dup {
			ids = append(ids, pk)
			scores = append(scores, e.scoreVal())
			pkSet[pk] = struct{}{}
			sources = append(sources, segmentSource{
				InputIdx:    e.inputIdx,
				SegOffset:   e.segOffsetVal(),
				OriginalIdx: e.cursor,
			})
		}

		if e.advance() {
			heap.Push(h, e)
		}
	}
	return ids, scores, sources
}

// mergeGroupByStringPk performs GroupBy-aware merge for one NQ (string PK).
func mergeGroupByStringPk(
	h *mergeHeapStringPk,
	topK, groupSize int64,
	numGroupFields int,
) ([]string, []float32, []segmentSource) {
	if numGroupFields == 0 {
		return mergeStandardStringPk(h, topK)
	}
	totalLimit := topK * groupSize
	pkSet := make(map[string]struct{}, totalLimit)
	counter := newCompositeGroupCounter(topK, groupSize)

	ids := make([]string, 0, totalLimit)
	scores := make([]float32, 0, totalLimit)
	sources := make([]segmentSource, 0, totalLimit)

	for int64(len(ids)) < totalLimit && h.Len() > 0 {
		e := heap.Pop(h).(*mergeEntry)
		pk := e.idStringVal()

		if _, dup := pkSet[pk]; dup {
			if e.advance() {
				heap.Push(h, e)
			}
			continue
		}

		values := extractCompositeGroupValues(e, numGroupFields)
		if !counter.shouldAccept(values) {
			if e.advance() {
				heap.Push(h, e)
			}
			continue
		}

		ids = append(ids, pk)
		scores = append(scores, e.scoreVal())
		pkSet[pk] = struct{}{}
		sources = append(sources, segmentSource{
			InputIdx:    e.inputIdx,
			SegOffset:   e.segOffsetVal(),
			OriginalIdx: e.cursor,
		})

		if e.advance() {
			heap.Push(h, e)
		}
	}
	return ids, scores, sources
}

type compositeGroup struct {
	values []any
	count  int64
}

// compositeGroupCounter tracks per-composite-group row counts for GroupBy reduce.
type compositeGroupCounter struct {
	groups    map[uint64][]*compositeGroup
	distinct  int64
	topK      int64
	groupSize int64
}

func newCompositeGroupCounter(topK, groupSize int64) *compositeGroupCounter {
	return &compositeGroupCounter{
		groups:    make(map[uint64][]*compositeGroup, topK),
		topK:      topK,
		groupSize: groupSize,
	}
}

// shouldAccept returns true if the row should be accepted into its group.
// If accepted, the counter is updated.
func (c *compositeGroupCounter) shouldAccept(values []any) bool {
	hash := reduce.HashGroupValues(values)
	for _, group := range c.groups[hash] {
		if !reduce.EqualGroupValues(group.values, values) {
			continue
		}
		if group.count >= c.groupSize {
			return false
		}
		group.count++
		return true
	}
	if c.distinct >= c.topK {
		return false
	}
	c.groups[hash] = append(c.groups[hash], &compositeGroup{
		values: append([]any(nil), values...),
		count:  1,
	})
	c.distinct++
	return true
}

func extractCompositeGroupValues(e *mergeEntry, numGroupFields int) []any {
	values := make([]any, numGroupFields)
	for i := 0; i < numGroupFields; i++ {
		if i >= len(e.groupByArrs) {
			continue
		}
		values[i] = extractArrowScalar(e.groupByArrs[i], e.cursor)
	}
	return values
}

func extractArrowScalar(arr arrow.Array, idx int) any {
	if arr == nil || arr.IsNull(idx) {
		return nil
	}
	switch typed := arr.(type) {
	case *array.Int8:
		return reduce.NormalizeScalar(typed.Value(idx))
	case *array.Int16:
		return reduce.NormalizeScalar(typed.Value(idx))
	case *array.Int32:
		return reduce.NormalizeScalar(typed.Value(idx))
	case *array.Int64:
		return reduce.NormalizeScalar(typed.Value(idx))
	case *array.Boolean:
		return reduce.NormalizeScalar(typed.Value(idx))
	case *array.String:
		return reduce.NormalizeScalar(typed.Value(idx))
	default:
		return nil
	}
}

// pickGroupByValues builds one group-by output array by picking values from source entries.
// Uses segmentSource.OriginalIdx to look up values from the original input chunk arrays.
func pickGroupByValues(
	pool memory.Allocator,
	cols []inputCols,
	chunkIdx int,
	sources []segmentSource,
	groupIdx int,
) arrow.Array {
	if len(sources) == 0 {
		return buildEmptyGroupByArray(pool, cols, groupIdx)
	}

	// All inputs share numChunks per the heapMergeReduce contract (enforced at
	// the top of heapMergeReduce), so chunkIdx is always in range when groupBy
	// is non-nil. Don't add a bound check.
	chunkArrays := make([]arrow.Array, len(cols))
	for i, c := range cols {
		if groupIdx < len(c.groupBys) && c.groupBys[groupIdx] != nil {
			chunkArrays[i] = c.groupBys[groupIdx].Chunk(chunkIdx)
		}
	}

	var firstArr arrow.Array
	for _, src := range sources {
		if chunkArrays[src.InputIdx] != nil {
			firstArr = chunkArrays[src.InputIdx]
			break
		}
	}
	if firstArr == nil {
		return buildEmptyGroupByArray(pool, cols, groupIdx)
	}

	switch firstArr.(type) {
	case *array.Int8:
		return pickTyped(pool, chunkArrays, sources, func(arr arrow.Array, idx int) int8 {
			return arr.(*array.Int8).Value(idx)
		}, array.NewInt8Builder)
	case *array.Int16:
		return pickTyped(pool, chunkArrays, sources, func(arr arrow.Array, idx int) int16 {
			return arr.(*array.Int16).Value(idx)
		}, array.NewInt16Builder)
	case *array.Int32:
		return pickTyped(pool, chunkArrays, sources, func(arr arrow.Array, idx int) int32 {
			return arr.(*array.Int32).Value(idx)
		}, array.NewInt32Builder)
	case *array.Int64:
		return pickTyped(pool, chunkArrays, sources, func(arr arrow.Array, idx int) int64 {
			return arr.(*array.Int64).Value(idx)
		}, array.NewInt64Builder)
	case *array.Boolean:
		return pickTyped(pool, chunkArrays, sources, func(arr arrow.Array, idx int) bool {
			return arr.(*array.Boolean).Value(idx)
		}, array.NewBooleanBuilder)
	case *array.String:
		return pickTyped(pool, chunkArrays, sources, func(arr arrow.Array, idx int) string {
			return arr.(*array.String).Value(idx)
		}, array.NewStringBuilder)
	default:
		return buildEmptyGroupByArray(pool, cols, groupIdx)
	}
}

type appendable[T any] interface {
	Append(T)
	AppendNull()
	NewArray() arrow.Array
	Release()
}

// pickTyped builds a typed Arrow array from sources. The captureless getValue
// closures at call sites compile to static singletons, so this is allocation-free.
func pickTyped[T any, B appendable[T]](
	pool memory.Allocator,
	chunkArrays []arrow.Array,
	sources []segmentSource,
	getValue func(arrow.Array, int) T,
	newBuilder func(memory.Allocator) B,
) arrow.Array {
	b := newBuilder(pool)
	defer b.Release()
	for _, src := range sources {
		arr := chunkArrays[src.InputIdx]
		if arr == nil || arr.IsNull(src.OriginalIdx) {
			b.AppendNull()
		} else {
			b.Append(getValue(arr, src.OriginalIdx))
		}
	}
	return b.NewArray()
}

// pickElementIndicesValues builds an int32 Arrow array of element indices by
// picking values from each source's original chunk. element_indices is always
// int32 (matching the C++ SearchResult::element_indices_ type).
func pickElementIndicesValues(
	pool memory.Allocator,
	cols []inputCols,
	chunkIdx int,
	sources []segmentSource,
) arrow.Array {
	chunkArrays := make([]arrow.Array, len(cols))
	for i, c := range cols {
		if c.elementIndices != nil {
			chunkArrays[i] = c.elementIndices.Chunk(chunkIdx)
		}
	}
	return pickTyped(pool, chunkArrays, sources, func(arr arrow.Array, idx int) int32 {
		return arr.(*array.Int32).Value(idx)
	}, array.NewInt32Builder)
}

func buildEmptyGroupByArray(pool memory.Allocator, cols []inputCols, groupIdx int) arrow.Array {
	dt := arrow.PrimitiveTypes.Int64 // fallback type
	for _, c := range cols {
		if groupIdx < len(c.groupBys) && c.groupBys[groupIdx] != nil {
			dt = c.groupBys[groupIdx].DataType()
			break
		}
	}
	b := array.NewBuilder(pool, dt)
	a := b.NewArray()
	b.Release()
	return a
}
