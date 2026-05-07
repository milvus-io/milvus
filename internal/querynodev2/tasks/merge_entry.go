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
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

// epsilon matches C++ common/Consts.h: const float EPSILON = 0.0000000119
const epsilon float32 = 0.0000000119

// mergeEntry represents one segment's search results for a single NQ chunk,
// with a cursor that advances row by row. The C++ exporter has already
// normalized the row order (score DESC, equal-score ties broken by PK ASC),
// so the cursor maps directly to the row index in the Arrow arrays.
type mergeEntry struct {
	inputIdx int // which input DataFrame this came from
	cursor   int // current row index in the Arrow arrays

	idInt64  *array.Int64   // int64 PK array (one of idInt64/idString is set)
	idString *array.String  // varchar PK array
	scoreArr *array.Float32 // $score array

	segOffsetArr *array.Int64  // $seg_offset array (for Late Materialization)
	groupByArrs  []arrow.Array // $group_by_<fieldID> arrays (optional, for GroupBy mode)
}

func (e *mergeEntry) scoreVal() float32 {
	return e.scoreArr.Value(e.cursor)
}

func (e *mergeEntry) idInt64Val() int64 {
	return e.idInt64.Value(e.cursor)
}

func (e *mergeEntry) idStringVal() string {
	return e.idString.Value(e.cursor)
}

func (e *mergeEntry) segOffsetVal() int64 {
	if e.segOffsetArr == nil {
		return -1
	}
	return e.segOffsetArr.Value(e.cursor)
}

func (e *mergeEntry) advance() bool {
	e.cursor++
	return e.cursor < e.scoreArr.Len()
}

// greaterInt64Pk: equal scores (within epsilon) → smaller PK is "greater" so it
// pops first; otherwise sort by score DESC.
func (e *mergeEntry) greaterInt64Pk(other *mergeEntry) bool {
	diff := e.scoreVal() - other.scoreVal()
	if diff > -epsilon && diff < epsilon {
		return e.idInt64Val() < other.idInt64Val() // equal score → PK ASC
	}
	return diff > 0 // score DESC
}

// greaterStringPk is the varchar PK variant of greater.
func (e *mergeEntry) greaterStringPk(other *mergeEntry) bool {
	diff := e.scoreVal() - other.scoreVal()
	if diff > -epsilon && diff < epsilon {
		return e.idStringVal() < other.idStringVal()
	}
	return diff > 0
}

// mergeHeapInt64Pk is a max-heap of mergeEntry by greaterInt64Pk.
type mergeHeapInt64Pk []*mergeEntry

func (h mergeHeapInt64Pk) Len() int           { return len(h) }
func (h mergeHeapInt64Pk) Less(i, j int) bool { return h[i].greaterInt64Pk(h[j]) }
func (h mergeHeapInt64Pk) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *mergeHeapInt64Pk) Push(x interface{}) {
	*h = append(*h, x.(*mergeEntry))
}

func (h *mergeHeapInt64Pk) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

// mergeHeapStringPk implements heap.Interface for max-heap with varchar PK.
type mergeHeapStringPk []*mergeEntry

func (h mergeHeapStringPk) Len() int           { return len(h) }
func (h mergeHeapStringPk) Less(i, j int) bool { return h[i].greaterStringPk(h[j]) }
func (h mergeHeapStringPk) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *mergeHeapStringPk) Push(x interface{}) {
	*h = append(*h, x.(*mergeEntry))
}

func (h *mergeHeapStringPk) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}
