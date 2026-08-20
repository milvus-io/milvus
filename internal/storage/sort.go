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
	"slices"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

// Sort materializes the records from rr, stable-selects the rows for which
// predicate returns true, sorts them by sortByFieldIDs, and writes them out
// through rw in batches of roughly batchSize bytes.
//
// Performance notes (vs. the naive row-at-a-time approach):
//   - The row selection is kept in a value slice ([]rowIndex) instead of a
//     []*rowIndex, avoiding one heap allocation per row.
//   - Sort keys are extracted into flat per-record slices once. A single int64
//     key (the common PK case) is then sorted with an O(N) stable LSD radix
//     sort; other keys use slices.SortFunc over the flat keys (plain slice
//     indexing, no Column() map lookup per comparison).
//   - When writing the output, each source column's array is resolved once per
//     input record rather than once per row (RecordBuilder.Append would do the
//     latter); rows are then emitted in order and flushed once the accumulated
//     batch reaches batchSize bytes.
func Sort(batchSize uint64, schema *schemapb.CollectionSchema, rr []RecordReader,
	rw RecordWriter, predicate func(r Record, ri, i int) bool, sortByFieldIDs []int64,
) (int, *SortTimings, error) {
	records := make([]Record, 0)
	indices := make([]rowIndex, 0)

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
						indices = append(indices, rowIndex{int32(ri), int32(i)})
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
		// Pre-extract the sort key columns into flat per-record slices so the
		// comparator avoids a Column() map lookup + type assert per comparison.
		kinds := make([]int, len(sortByFieldIDs))
		int64Keys := make([][][]int64, len(sortByFieldIDs))
		stringKeys := make([][][]string, len(sortByFieldIDs))
		for fp, fid := range sortByFieldIDs {
			switch records[0].Column(fid).(type) {
			case *array.Int64:
				kinds[fp] = keyInt64
				cols := make([][]int64, len(records))
				for ri, rec := range records {
					cols[ri] = rec.Column(fid).(*array.Int64).Int64Values()
				}
				int64Keys[fp] = cols
			case *array.String:
				kinds[fp] = keyString
				cols := make([][]string, len(records))
				for ri, rec := range records {
					a := rec.Column(fid).(*array.String)
					vals := make([]string, a.Len())
					for i := range vals {
						vals[i] = a.Value(i)
					}
					cols[ri] = vals
				}
				stringKeys[fp] = cols
			default:
				return 0, nil, merr.WrapErrStorageMsg("unsupported type for sorting key")
			}
		}

		// A single int64 sort key (the common PK case) is sorted with a stable
		// LSD radix sort: O(N) instead of O(N log N) and no comparator calls.
		// Multi-field or varchar keys fall back to comparison sort.
		if len(sortByFieldIDs) == 1 && kinds[0] == keyInt64 {
			radixSortByInt64(indices, int64Keys[0])
		} else {
			slices.SortFunc(indices, func(x, y rowIndex) int {
				for fp := range sortByFieldIDs {
					switch kinds[fp] {
					case keyInt64:
						xv, yv := int64Keys[fp][x.ri][x.i], int64Keys[fp][y.ri][y.i]
						if xv != yv {
							if xv < yv {
								return -1
							}
							return 1
						}
					case keyString:
						xv, yv := stringKeys[fp][x.ri][x.i], stringKeys[fp][y.ri][y.i]
						if xv != yv {
							if xv < yv {
								return -1
							}
							return 1
						}
					}
				}
				return 0
			})
		}
	}
	sortCost := time.Since(phaseStart)

	phaseStart = time.Now()
	rb := NewRecordBuilder(schema)
	if err := rb.prepareAppendDefaults(); err != nil {
		return 0, nil, err
	}

	// Resolve each output column's source array once per input record (instead
	// of once per row, as RecordBuilder.Append would).
	srcByField := make([][]arrow.Array, len(rb.builders))
	for fi := range rb.builders {
		fid := rb.fields[fi].FieldID
		cols := make([]arrow.Array, len(records))
		for ri := range records {
			cols[ri] = records[ri].Column(fid)
		}
		srcByField[fi] = cols
	}

	writeRecord := func() error {
		rec := rb.Build()
		defer rec.Release()
		if rec.Len() > 0 {
			return rw.Write(rec)
		}
		return nil
	}

	for _, idx := range indices {
		for fi, builder := range rb.builders {
			size, err := appendValueAt(builder, srcByField[fi][idx.ri], int(idx.i), rb.fields[fi], rb.defaults[fi])
			if err != nil {
				return 0, nil, merr.Wrapf(err, "failed to append value at row %d for field %s", idx.i, rb.fields[fi].GetName())
			}
			rb.size += size
		}
		rb.nRows++

		// Flush once the accumulated batch reaches batchSize bytes (exact, like
		// the original) so a single output record never exceeds the target.
		if rb.GetSize() >= batchSize {
			if err := writeRecord(); err != nil {
				return 0, nil, err
			}
		}
	}

	// write the last partial batch
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

// pushAfterRoot inserts v after the caller has already proved that v does not
// belong before the current root. It preserves normal heap insertion below
// the root while avoiding a duplicate comparison at the top of the heap.
func (h *rowHeap) pushAfterRoot(v rowIndex) {
	h.items = append(h.items, v)
	i := len(h.items) - 1
	for i > 0 {
		p := (i - 1) / 2
		if p == 0 || !h.less(h.items[i], h.items[p]) {
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

// mergeReaderState owns all mutable state associated with one input reader.
// Keeping it together avoids a collection of reader-count-sized allocations
// and makes record advancement reset the whole borrowed-record state in one
// place.
type mergeReaderState struct {
	rec                     Record
	keys                    []sortKeyCol
	pos                     int32
	recNo                   int32
	prepared                preparedRecordAppender
	preparedReady           bool
	cachedKeepStart         int32
	cachedKeepEnd           int32
	cachedDrop              int32
	allPredicatesKept       bool
	recordPredicateComplete bool
	nextKnownAfterHeapRoot  bool
}

// radixSortByInt64 sorts indices in place so that keys[indices[k].ri][indices[k].i]
// is non-decreasing, using a stable LSD radix sort over the 8 bytes of the int64
// key (O(N)). The sign bit is flipped so unsigned byte ordering matches signed
// int64 ordering.
func radixSortByInt64(indices []rowIndex, keys [][]int64) {
	n := len(indices)
	if n < 2 {
		return
	}
	srcKey := make([]uint64, n)
	for i, idx := range indices {
		srcKey[i] = uint64(keys[idx.ri][idx.i]) ^ (uint64(1) << 63)
	}
	dstKey := make([]uint64, n)
	srcIdx := indices
	dstIdx := make([]rowIndex, n)
	var counts [256]int
	for shift := uint(0); shift < 64; shift += 8 {
		counts = [256]int{}
		for i := 0; i < n; i++ {
			counts[(srcKey[i]>>shift)&0xff]++
		}
		sum := 0
		for b := 0; b < 256; b++ {
			c := counts[b]
			counts[b] = sum
			sum += c
		}
		for i := 0; i < n; i++ {
			b := (srcKey[i] >> shift) & 0xff
			p := counts[b]
			counts[b]++
			dstIdx[p] = srcIdx[i]
			dstKey[p] = srcKey[i]
		}
		srcIdx, dstIdx = dstIdx, srcIdx
		srcKey, dstKey = dstKey, srcKey
	}
	// 8 passes is even, so the sorted data ends up back in the original `indices`
	// backing array; copy defensively in case the pass count ever becomes odd.
	if &srcIdx[0] != &indices[0] {
		copy(indices, srcIdx)
	}
}

// MergeSort merges rows from rr, which each yield records already sorted by
// sortedByFieldIDs, into a single sorted stream written through rw in batches
// of roughly batchSize bytes. Rows for which predicate returns false are
// skipped; predicate is evaluated exactly once per row. Every input record
// must expose every field in schema; schema-evolution callers must materialize
// missing fields before calling MergeSort.
//
// Performance notes (vs. the earlier all-rows-in-the-queue approach):
//   - The heap holds one entry per reader rather than every in-flight row, so
//     comparisons per row drop from O(log totalRows) to O(log len(rr)) and the
//     heap stays small enough to be cache resident.
//   - Merge keys are resolved once per record in advanceRecord instead of once
//     per comparison, avoiding a Column() map lookup plus type assert per side.
//   - The heap stores rowIndex by value, removing the per-row heap allocation
//     that came from queueing *index through container/heap.
type recordBatchWriter interface {
	WriteBatch([]Record) error
}

func MergeSort(batchSize uint64, schema *schemapb.CollectionSchema, rr []RecordReader,
	rw RecordWriter, predicate func(r Record, ri, i int) bool, sortedByFieldIDs []int64,
) (numRows int, err error) {
	// Fast path: no readers provided
	if len(rr) == 0 {
		return 0, nil
	}

	nk := len(sortedByFieldIDs)
	rb := NewRecordBuilder(schema)
	defer rb.Release()
	// With a single string field, the generic append has only one type switch,
	// and run bookkeeping costs more than it saves on interleaved varchar PKs.
	// Preserve the compact row-at-a-time loop for this narrow schema shape.
	useGenericSingleString := false
	if len(rb.builders) == 1 {
		_, useGenericSingleString = rb.builders[0].(*array.StringBuilder)
	}
	var pendingRecords []Record
	var pendingSize uint64
	releasePending := func() {
		for _, record := range pendingRecords {
			record.Release()
		}
		pendingRecords = pendingRecords[:0]
		pendingSize = 0
	}
	defer releasePending()
	states := make([]mergeReaderState, len(rr))
	for i := range states {
		states[i].keys = make([]sortKeyCol, nk)
		states[i].recNo = -1
	}
	// states[ri].keys[fp] is the fp-th merge key column held by reader ri.
	// It is allocated once and overwritten in place on every record advance;
	// states[ri].rec == nil is the sole exhausted-reader sentinel. The keys stay
	// valid until seedNext(ri) advances that reader again, not merely while ri
	// has a heap entry: the main loop reads them after popping ri's only entry.

	extractKeys := func(ri int) error {
		state := &states[ri]
		for fp, fid := range sortedByFieldIDs {
			switch a := state.rec.Column(fid).(type) {
			case *array.Int64:
				state.keys[fp] = sortKeyCol{kind: keyInt64, i64: a.Int64Values()}
			case *array.String:
				state.keys[fp] = sortKeyCol{kind: keyString, str: a}
			default:
				return merr.WrapErrStorageMsg("unsupported type for sorting key")
			}
		}
		return nil
	}

	advanceRecord := func(ri int) error {
		state := &states[ri]
		rec, err := rr[ri].Next()
		state.rec = rec // assign nil if err
		state.cachedKeepStart = 0
		state.cachedKeepEnd = 0
		state.cachedDrop = -1
		state.allPredicatesKept = true
		state.recordPredicateComplete = false
		state.nextKnownAfterHeapRoot = false
		state.preparedReady = false
		if err != nil {
			return err
		}
		state.pos = 0
		state.recNo++
		return extractKeys(ri)
	}

	ensurePrepared := func(ri int) error {
		state := &states[ri]
		if state.preparedReady {
			return nil
		}
		if state.prepared.fields == nil {
			state.prepared.fields = make([]preparedValueAppender, len(rb.builders))
		}
		if err := rb.prepareRecord(state.rec, &state.prepared); err != nil {
			return err
		}
		state.preparedReady = true
		return nil
	}

	// Keep the post-rejection fallback global for the rest of the merge. This is
	// intentionally conservative: filters are rare after compactor selection,
	// and not re-entering run lookahead keeps its predicate cache state simple.
	predicateDropped := false
	rowKept := func(ri, i int) bool {
		state := &states[ri]
		if state.recordPredicateComplete && state.allPredicatesKept {
			return true
		}
		if int32(i) >= state.cachedKeepStart && int32(i) < state.cachedKeepEnd {
			return true
		}
		if int32(i) == state.cachedDrop {
			state.cachedDrop = -1
			return false
		}
		kept := predicate(state.rec, ri, i)
		if !kept {
			state.allPredicatesKept = false
			predicateDropped = true
		}
		return kept
	}

	// compareKeys orders two rows that are both currently live in the heap.
	// sortKeyCol is 40 bytes, so take it by pointer: this runs on both sides of
	// every comparison.
	compareKeys := func(x, y rowIndex) int {
		for fp := 0; fp < nk; fp++ {
			cx, cy := &states[x.ri].keys[fp], &states[y.ri].keys[fp]
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
	// as needed. It evaluates directly until a run starts a lazy result cache;
	// either way every original row is evaluated exactly once.
	seedNext := func(ri int) error {
		state := &states[ri]
		for state.rec != nil {
			for int(state.pos) < state.rec.Len() {
				i := state.pos
				kept := false
				if useGenericSingleString {
					kept = predicate(state.rec, ri, int(i))
				} else {
					kept = rowKept(ri, int(i))
				}
				if kept {
					idx := rowIndex{ri: int32(ri), i: i}
					if state.nextKnownAfterHeapRoot && h.len() > 0 {
						h.pushAfterRoot(idx)
						state.nextKnownAfterHeapRoot = false
					} else {
						h.push(idx)
					}
					return nil
				}
				state.pos++
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

	flushPending := func() error {
		if len(pendingRecords) == 0 {
			return nil
		}
		var err error
		if batchWriter, ok := rw.(recordBatchWriter); ok {
			err = batchWriter.WriteBatch(pendingRecords)
		} else {
			for _, record := range pendingRecords {
				if err = rw.Write(record); err != nil {
					break
				}
			}
		}
		releasePending()
		return err
	}

	writeRecord := func() error {
		if rb.GetRowNum() > 0 {
			size := rb.GetSize()
			rec := rb.Build()
			if len(pendingRecords) == 0 {
				err := rw.Write(rec)
				rec.Release()
				return err
			}
			pendingRecords = append(pendingRecords, rec)
			pendingSize += size
		}
		return flushPending()
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
			cx := &states[x.ri].keys[fp]
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
			cx := &states[x.ri].keys[fp]
			switch cx.kind {
			case keyInt64:
				lastI64[fp] = cx.i64[x.i]
			case keyString:
				lastStrBuf[fp] = append(lastStrBuf[fp][:0], cx.str.Value(int(x.i))...)
			}
		}
		hasLast = true
	}

	emitRow := func(idx rowIndex) error {
		if hasLast && compareWithLast(idx) < 0 {
			return merr.WrapErrDataIntegrityMsg(
				"input record is not sorted by the merge key: reader %d record %d row %d out of order, merge key fields %v",
				idx.ri, states[idx.ri].recNo, idx.i, sortedByFieldIDs)
		}
		saveLast(idx)
		if useGenericSingleString {
			if err := rb.Append(states[idx.ri].rec, int(idx.i), int(idx.i)+1); err != nil {
				return err
			}
		} else {
			if err := ensurePrepared(int(idx.ri)); err != nil {
				return err
			}
			if err := rb.appendPreparedRow(&states[idx.ri].prepared, int(idx.i)); err != nil {
				return err
			}
		}
		numRows++
		if pendingSize+rb.GetSize() >= batchSize {
			return writeRecord()
		}
		return nil
	}

	emitRun := func(ri int) error {
		state := &states[ri]
		if state.rec == nil {
			return nil
		}
		r := state.rec
		start := int(state.pos)
		if start >= r.Len() {
			return seedNext(ri)
		}
		end := start + 1
		for end < r.Len() {
			if !rowKept(ri, end) {
				state.cachedDrop = int32(end)
				break
			}
			candidate := rowIndex{ri: int32(ri), i: int32(end)}
			if h.len() > 0 && !h.less(candidate, h.items[0]) {
				state.cachedKeepStart = int32(end)
				state.cachedKeepEnd = int32(end + 1)
				state.nextKnownAfterHeapRoot = true
				break
			}
			end++
		}
		// Direct forwarding may bypass per-row reconstruction, never the
		// predicate contract. Only pay for the full-record keep-all proof when
		// this interval and record shape could actually use the fast path.
		forwardCandidate := end-start >= directForwardMinRows && rb.canDirectForwardRecord(r)
		if forwardCandidate && state.allPredicatesKept && !state.recordPredicateComplete {
			// Rows before start were already evaluated while seeding the heap and
			// are known kept because allPredicatesKept is still true. The run
			// lookahead has cached [start, end); continue only with rows not yet
			// inspected so predicate remains exactly-once.
			proofComplete := true
			for i := end; i < r.Len(); i++ {
				if !rowKept(ri, i) {
					state.cachedKeepStart = int32(end)
					state.cachedKeepEnd = int32(i)
					state.cachedDrop = int32(i)
					proofComplete = false
					break
				}
			}
			state.recordPredicateComplete = proofComplete
		}
		remaining := uint64(0)
		bufferedSize := pendingSize + rb.GetSize()
		if bufferedSize < batchSize {
			remaining = batchSize - bufferedSize
		}
		forwarded, forwardedSize, forwardedEnd, forwardedOK := (*simpleArrowRecord)(nil), uint64(0), start, false
		// A rebuilt record cannot be sliced together with a source Arrow record.
		// Keep rebuilding until that record is complete; previously forwarded
		// slices remain queued in the same logical output batch.
		if rb.GetRowNum() != 0 {
			forwardCandidate = false
		}
		if forwardCandidate && state.allPredicatesKept && state.recordPredicateComplete {
			forwarded, forwardedSize, forwardedEnd, forwardedOK = rb.directForwardRecord(r, start, end, remaining, forwardCandidate)
		}
		if forwardedOK {
			// nextKnownAfterHeapRoot proves only that row end belongs after the
			// current root. A batch-size-limited forward stops before end, so its
			// next row must go through the normal heap comparison.
			if forwardedEnd != end {
				state.nextKnownAfterHeapRoot = false
			}
			for i := start; i < forwardedEnd; i++ {
				candidate := rowIndex{ri: int32(ri), i: int32(i)}
				if hasLast && compareWithLast(candidate) < 0 {
					forwarded.Release()
					return merr.WrapErrDataIntegrityMsg(
						"input record is not sorted by the merge key: reader %d record %d row %d out of order, merge key fields %v",
						candidate.ri, state.recNo, candidate.i, sortedByFieldIDs)
				}
				saveLast(candidate)
			}
			pendingRecords = append(pendingRecords, forwarded)
			pendingSize += forwardedSize
			numRows += forwardedEnd - start
			state.pos = int32(forwardedEnd)
			if pendingSize >= batchSize {
				if err := flushPending(); err != nil {
					return err
				}
			}
		} else {
			if end-start > 1 {
				rb.reservePrepared(end - start)
			}
			for i := start; i < end; i++ {
				candidate := rowIndex{ri: int32(ri), i: int32(i)}
				if err := emitRow(candidate); err != nil {
					return err
				}
				state.pos++
			}
		}

		if int(state.pos) < r.Len() {
			return seedNext(ri)
		}
		if err := advanceRecord(ri); err != nil {
			if err == io.EOF {
				state.rec = nil
				return nil
			}
			return err
		}
		return seedNext(ri)
	}

	for h.len() > 0 {
		idx := h.pop()
		if useGenericSingleString {
			if hasLast && compareWithLast(idx) < 0 {
				return 0, merr.WrapErrDataIntegrityMsg(
					"input record is not sorted by the merge key: reader %d record %d row %d out of order, merge key fields %v",
					idx.ri, states[idx.ri].recNo, idx.i, sortedByFieldIDs)
			}
			saveLast(idx)
			if err := rb.Append(states[idx.ri].rec, int(idx.i), int(idx.i)+1); err != nil {
				return 0, err
			}
			numRows++
			if rb.GetSize() >= batchSize {
				if err := writeRecord(); err != nil {
					return 0, err
				}
			}
			states[idx.ri].pos++
			if err := seedNext(int(idx.ri)); err != nil {
				return 0, err
			}
			continue
		}
		if predicateDropped {
			if err := emitRow(idx); err != nil {
				return 0, err
			}
			states[idx.ri].pos++
			if err := seedNext(int(idx.ri)); err != nil {
				return 0, err
			}
			continue
		}
		if err := emitRun(int(idx.ri)); err != nil {
			return 0, err
		}
	}

	// Write the last logical batch. It may contain rebuilt records, forwarded
	// slices, or both; batch-aware writers rotate once for the whole group.
	if rb.GetRowNum() > 0 {
		if err := writeRecord(); err != nil {
			return 0, err
		}
	} else if err := flushPending(); err != nil {
		return 0, err
	}

	return numRows, nil
}
