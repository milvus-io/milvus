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
	elementIdx   *array.Int32  // $element_indices array (optional, for element-level search)
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
	return e.segOffsetArr.Value(e.cursor)
}

func (e *mergeEntry) elementIndexVal() int32 {
	return e.elementIdx.Value(e.cursor)
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

// advanceRoot consumes the current root and moves that entry to its next row.
// It deliberately preserves the legacy heap.Pop -> advance -> heap.Push
// ordering: first remove the current root and repair the remaining heap, then
// advance and reinsert the entry if it still has rows. This two-phase repair is
// required because the epsilon-based score comparator is not a strict weak
// ordering, so advancing the root in place and performing a single sift-down
// can produce a different result from the legacy merge path.
func (h *mergeHeapInt64Pk) advanceRoot() {
	entries := *h
	entry := entries[0]

	last := len(entries) - 1
	if last == 0 {
		entries[0] = nil
		entries = entries[:0]
	} else {
		entries[0] = entries[last]
		entries[last] = nil
		entries = entries[:last]
		siftDownInt64Pk(entries, 0)
	}

	if entry.advance() {
		entries = append(entries, entry)
		siftUpInt64Pk(entries, len(entries)-1)
	}
	*h = entries
}

func siftDownInt64Pk(h mergeHeapInt64Pk, root int) {
	for {
		left := root*2 + 1
		if left >= len(h) {
			return
		}
		best := left
		right := left + 1
		if right < len(h) && h[right].greaterInt64Pk(h[left]) {
			best = right
		}
		if !h[best].greaterInt64Pk(h[root]) {
			return
		}
		h[root], h[best] = h[best], h[root]
		root = best
	}
}

func siftUpInt64Pk(h mergeHeapInt64Pk, child int) {
	for child > 0 {
		parent := (child - 1) / 2
		if !h[child].greaterInt64Pk(h[parent]) {
			return
		}
		h[parent], h[child] = h[child], h[parent]
		child = parent
	}
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

// advanceRoot is the varchar counterpart of mergeHeapInt64Pk.advanceRoot.
func (h *mergeHeapStringPk) advanceRoot() {
	entries := *h
	entry := entries[0]

	last := len(entries) - 1
	if last == 0 {
		entries[0] = nil
		entries = entries[:0]
	} else {
		entries[0] = entries[last]
		entries[last] = nil
		entries = entries[:last]
		siftDownStringPk(entries, 0)
	}

	if entry.advance() {
		entries = append(entries, entry)
		siftUpStringPk(entries, len(entries)-1)
	}
	*h = entries
}

func siftDownStringPk(h mergeHeapStringPk, root int) {
	for {
		left := root*2 + 1
		if left >= len(h) {
			return
		}
		best := left
		right := left + 1
		if right < len(h) && h[right].greaterStringPk(h[left]) {
			best = right
		}
		if !h[best].greaterStringPk(h[root]) {
			return
		}
		h[root], h[best] = h[best], h[root]
		root = best
	}
}

func siftUpStringPk(h mergeHeapStringPk, child int) {
	for child > 0 {
		parent := (child - 1) / 2
		if !h[child].greaterStringPk(h[parent]) {
			return
		}
		h[parent], h[child] = h[child], h[parent]
		child = parent
	}
}
