// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"io"
	"sort"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// sort key kinds shared by Sort and MergeSort.
const (
	keyInt64 = iota
	keyString
)

// SortTimings holds phase-level timing information from the Sort function.
type SortTimings struct {
	ReadCost   time.Duration
	SortCost   time.Duration
	WriteCost  time.Duration
	NumBatches int
	NumRows    int
}

func Sort(batchSize uint64, schema *schemapb.CollectionSchema, rr []RecordReader,
	rw RecordWriter, predicate func(r Record, ri, i int) bool, sortByFieldIDs []int64,
) (int, *SortTimings, error) {
	records := make([]Record, 0)

	type index struct {
		ri int
		i  int
	}
	indices := make([]*index, 0)

	// release cgo records
	defer func() {
		for _, rec := range records {
			rec.Release()
		}
	}()

	phaseStart := time.Now()
	for _, r := range rr {
		for {
			rec, err := r.Next()
			if err == nil {
				rec.Retain()
				ri := len(records)
				records = append(records, rec)
				for i := 0; i < rec.Len(); i++ {
					if predicate(rec, ri, i) {
						indices = append(indices, &index{ri, i})
					}
				}
			} else if err == io.EOF {
				break
			} else {
				return 0, nil, err
			}
		}
	}
	readCost := time.Since(phaseStart)

	if len(records) == 0 {
		return 0, &SortTimings{ReadCost: readCost}, nil
	}

	phaseStart = time.Now()
	if len(sortByFieldIDs) > 0 {
		type keyCmp func(x, y *index) int
		comparators := make([]keyCmp, 0, len(sortByFieldIDs))
		for _, fid := range sortByFieldIDs {
			switch records[0].Column(fid).(type) {
			case *array.Int64:
				f := func(x, y *index) int {
					xVal := records[x.ri].Column(fid).(*array.Int64).Value(x.i)
					yVal := records[y.ri].Column(fid).(*array.Int64).Value(y.i)
					if xVal < yVal {
						return -1
					}
					if xVal > yVal {
						return 1
					}
					return 0
				}
				comparators = append(comparators, f)
			case *array.String:
				f := func(x, y *index) int {
					xVal := records[x.ri].Column(fid).(*array.String).Value(x.i)
					yVal := records[y.ri].Column(fid).(*array.String).Value(y.i)
					if xVal < yVal {
						return -1
					}
					if xVal > yVal {
						return 1
					}
					return 0
				}
				comparators = append(comparators, f)
			default:
				return 0, nil, merr.WrapErrStorageMsg("unsupported type for sorting key")
			}
		}

		sort.Slice(indices, func(i, j int) bool {
			x := indices[i]
			y := indices[j]
			for _, cmp := range comparators {
				c := cmp(x, y)
				if c < 0 {
					return true
				}
				if c > 0 {
					return false
				}
			}
			return false
		})
	}
	sortCost := time.Since(phaseStart)

	phaseStart = time.Now()
	rb := NewRecordBuilder(schema)
	writeRecord := func() error {
		rec := rb.Build()
		defer rec.Release()
		if rec.Len() > 0 {
			return rw.Write(rec)
		}
		return nil
	}

	for _, idx := range indices {
		if err := rb.Append(records[idx.ri], idx.i, idx.i+1); err != nil {
			return 0, nil, err
		}

		// Write when accumulated data size reaches batchSize
		if rb.GetSize() >= batchSize {
			if err := writeRecord(); err != nil {
				return 0, nil, err
			}
		}
	}

	// write the last batch
	if err := writeRecord(); err != nil {
		return 0, nil, err
	}
	writeCost := time.Since(phaseStart)

	timings := &SortTimings{
		ReadCost:   readCost,
		SortCost:   sortCost,
		WriteCost:  writeCost,
		NumBatches: len(records),
		NumRows:    len(indices),
	}
	return len(indices), timings, nil
}

// rowIndex addresses a single row as (record index, row-in-record index). It is
// stored by value to avoid a per-row heap allocation.
type rowIndex struct {
	ri int32
	i  int32
}

// rowHeap is a min-heap of rowIndex values. It exists instead of container/heap
// because heap.Push takes `any`, which boxes the value and costs one allocation
// per push; MergeSort pushes once per row.
type rowHeap struct {
	items []rowIndex
	less  func(x, y rowIndex) bool
}

func (h *rowHeap) len() int { return len(h.items) }

func (h *rowHeap) push(v rowIndex) {
	h.items = append(h.items, v)
	i := len(h.items) - 1
	for i > 0 {
		p := (i - 1) / 2
		if !h.less(h.items[i], h.items[p]) {
			break
		}
		h.items[i], h.items[p] = h.items[p], h.items[i]
		i = p
	}
}

func (h *rowHeap) pop() rowIndex {
	top := h.items[0]
	n := len(h.items) - 1
	h.items[0] = h.items[n]
	h.items = h.items[:n]
	i := 0
	for {
		l, r := 2*i+1, 2*i+2
		m := i
		if l < n && h.less(h.items[l], h.items[m]) {
			m = l
		}
		if r < n && h.less(h.items[r], h.items[m]) {
			m = r
		}
		if m == i {
			break
		}
		h.items[i], h.items[m] = h.items[m], h.items[i]
		i = m
	}
	return top
}

// sortKeyCol is a merge key column of the record a reader currently holds.
// int64 keys reference the arrow buffer directly; varchar keys keep the array
// pointer so Value(i) stays available without a per-comparison map lookup and
// type assert. Both are rebuilt when the reader advances to the next record.
type sortKeyCol struct {
	kind int
	i64  []int64
	str  *array.String
}

// MergeSort merges rows from rr, which each yield records already sorted by
// sortedByFieldIDs, into a single sorted stream written through rw in batches
// of roughly batchSize bytes. Rows for which predicate returns false are
// skipped; predicate is evaluated exactly once per row.
//
// Performance notes (vs. the earlier all-rows-in-the-queue approach):
//   - The heap holds one entry per reader rather than every in-flight row, so
//     comparisons per row drop from O(log totalRows) to O(log len(rr)) and the
//     heap stays small enough to be cache resident.
//   - Merge keys are resolved once per record in advanceRecord instead of once
//     per comparison, avoiding a Column() map lookup plus type assert per side.
//   - The heap stores rowIndex by value, removing the per-row heap allocation
//     that came from queueing *index through container/heap.
func MergeSort(batchSize uint64, schema *schemapb.CollectionSchema, rr []RecordReader,
	rw RecordWriter, predicate func(r Record, ri, i int) bool, sortedByFieldIDs []int64,
) (numRows int, err error) {
	// Fast path: no readers provided
	if len(rr) == 0 {
		return 0, nil
	}

	nk := len(sortedByFieldIDs)
	recs := make([]Record, len(rr))
	// keys[ri][fp] is the fp-th merge key column of the record reader ri holds.
	// Allocated once and overwritten in place on every advance; recs[ri] == nil
	// is the sole exhausted-reader sentinel. keys[ri] stays valid until
	// seedNext(ri) advances that reader again -- not merely while ri has a heap
	// entry: the main loop reads keys[ri] in compareWithLast and saveLast after
	// popping ri's only entry. Moving either of those after seedNext would be a
	// use-after-advance.
	keys := make([][]sortKeyCol, len(rr))
	for i := range keys {
		keys[i] = make([]sortKeyCol, nk)
	}
	// pos[ri] is the next row of that record to consider.
	pos := make([]int32, len(rr))
	// recNo[ri] counts the records that reader has produced. It turns an
	// out-of-order row into a (record, row) coordinate, since pos -- and so
	// idx.i -- restarts at zero on every record.
	recNo := make([]int32, len(rr))
	for i := range recNo {
		recNo[i] = -1
	}

	extractKeys := func(ri int) error {
		cols := keys[ri]
		for fp, fid := range sortedByFieldIDs {
			switch a := recs[ri].Column(fid).(type) {
			case *array.Int64:
				cols[fp] = sortKeyCol{kind: keyInt64, i64: a.Int64Values()}
			case *array.String:
				cols[fp] = sortKeyCol{kind: keyString, str: a}
			default:
				return merr.WrapErrStorageMsg("unsupported type for sorting key")
			}
		}
		return nil
	}

	advanceRecord := func(ri int) error {
		rec, err := rr[ri].Next()
		recs[ri] = rec // assign nil if err
		if err != nil {
			return err
		}
		pos[ri] = 0
		recNo[ri]++
		return extractKeys(ri)
	}

	// compareKeys orders two rows that are both currently live in the heap.
	// sortKeyCol is 40 bytes, so take it by pointer: this runs on both sides of
	// every comparison.
	compareKeys := func(x, y rowIndex) int {
		for fp := 0; fp < nk; fp++ {
			cx, cy := &keys[x.ri][fp], &keys[y.ri][fp]
			switch cx.kind {
			case keyInt64:
				xv, yv := cx.i64[x.i], cy.i64[y.i]
				if xv != yv {
					if xv < yv {
						return -1
					}
					return 1
				}
			case keyString:
				xv, yv := cx.str.Value(int(x.i)), cy.str.Value(int(y.i))
				if xv != yv {
					if xv < yv {
						return -1
					}
					return 1
				}
			}
		}
		return 0
	}

	h := &rowHeap{
		items: make([]rowIndex, 0, len(rr)),
		less: func(x, y rowIndex) bool {
			if c := compareKeys(x, y); c != 0 {
				return c < 0
			}
			// Equal keys break by reader index alone: a reader holds at most one
			// heap entry, since seedNext pushes a single row and is called again
			// only after that entry is popped. So x.ri != y.ri always holds here,
			// and there is no second row of the same reader to order against.
			// Stability is unaffected -- a reader's equal-key rows are re-seeded
			// in increasing pos, so they still leave the heap in input order.
			return x.ri < y.ri
		},
	}

	// seedNext pushes reader ri's next qualifying row, advancing across records
	// as needed. Every (record, row) position is evaluated by predicate exactly
	// once: pos only moves forward within a record, and is reset only when
	// advanceRecord installs a new one.
	seedNext := func(ri int) error {
		for recs[ri] != nil {
			r := recs[ri]
			for int(pos[ri]) < r.Len() {
				i := pos[ri]
				if predicate(r, ri, int(i)) {
					h.push(rowIndex{ri: int32(ri), i: i})
					return nil
				}
				pos[ri]++
			}
			if err := advanceRecord(ri); err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
		}
		return nil
	}

	for i := range rr {
		if err := advanceRecord(i); err != nil {
			if err == io.EOF {
				continue
			}
			return 0, err
		}
		if err := seedNext(i); err != nil {
			return 0, err
		}
	}

	rb := NewRecordBuilder(schema)
	writeRecord := func() error {
		rec := rb.Build()
		defer rec.Release()
		if rec.Len() > 0 {
			return rw.Write(rec)
		}
		return nil
	}

	// The emitted key must never decrease. It can only do so when an input
	// record is not sorted by the merge key, which this merge relies on. Detect
	// that explicitly instead of silently emitting rows out of order. The
	// previous key is kept by value because seedNext may already have advanced
	// the record it came from, and records are only borrowed from the reader.
	lastI64 := make([]int64, nk)
	// varchar keys are copied into reusable buffers rather than cloned per row:
	// the arrow buffer is only borrowed until the reader advances, but a fresh
	// string per row would reintroduce exactly the per-row allocation this
	// rewrite removes. Comparing via string(buf) does not allocate.
	lastStrBuf := make([][]byte, nk)
	hasLast := false

	compareWithLast := func(x rowIndex) int {
		for fp := 0; fp < nk; fp++ {
			cx := &keys[x.ri][fp]
			switch cx.kind {
			case keyInt64:
				xv := cx.i64[x.i]
				if xv != lastI64[fp] {
					if xv < lastI64[fp] {
						return -1
					}
					return 1
				}
			case keyString:
				xv := cx.str.Value(int(x.i))
				if xv != string(lastStrBuf[fp]) {
					if xv < string(lastStrBuf[fp]) {
						return -1
					}
					return 1
				}
			}
		}
		return 0
	}

	saveLast := func(x rowIndex) {
		for fp := 0; fp < nk; fp++ {
			cx := &keys[x.ri][fp]
			switch cx.kind {
			case keyInt64:
				lastI64[fp] = cx.i64[x.i]
			case keyString:
				lastStrBuf[fp] = append(lastStrBuf[fp][:0], cx.str.Value(int(x.i))...)
			}
		}
		hasLast = true
	}

	for h.len() > 0 {
		idx := h.pop()

		if hasLast && compareWithLast(idx) < 0 {
			return 0, merr.WrapErrDataIntegrityMsg(
				"input record is not sorted by the merge key: reader %d record %d row %d out of order, merge key fields %v",
				idx.ri, recNo[idx.ri], idx.i, sortedByFieldIDs)
		}
		saveLast(idx)

		if err := rb.Append(recs[idx.ri], int(idx.i), int(idx.i)+1); err != nil {
			return 0, err
		}
		numRows++

		// Due to current arrow impl (v12), the write performance is largely dependent on the batch size,
		//	small batch size will cause write performance degradation. To work around this issue, we accumulate
		//	records and write them in batches. This requires additional memory copy.
		if rb.GetSize() >= batchSize {
			if err := writeRecord(); err != nil {
				return 0, err
			}
		}

		pos[idx.ri]++
		if err := seedNext(int(idx.ri)); err != nil {
			return 0, err
		}
	}

	// write the last batch
	if rb.GetRowNum() > 0 {
		if err := writeRecord(); err != nil {
			return 0, err
		}
	}

	return numRows, nil
}
