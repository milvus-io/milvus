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
	"container/heap"
	"io"
	"sort"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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
				return 0, nil, merr.WrapErrParameterInvalidMsg("unsupported type for sorting key")
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

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue[T any] struct {
	items []*T
	less  func(x, y *T) bool
}

var _ heap.Interface = (*PriorityQueue[any])(nil)

func (pq PriorityQueue[T]) Len() int { return len(pq.items) }

func (pq PriorityQueue[T]) Less(i, j int) bool {
	return pq.less(pq.items[i], pq.items[j])
}

func (pq PriorityQueue[T]) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
}

func (pq *PriorityQueue[T]) Push(x any) {
	pq.items = append(pq.items, x.(*T))
}

func (pq *PriorityQueue[T]) Pop() any {
	old := pq.items
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	pq.items = old[0 : n-1]
	return x
}

func (pq *PriorityQueue[T]) Enqueue(x *T) {
	heap.Push(pq, x)
}

func (pq *PriorityQueue[T]) Dequeue() *T {
	return heap.Pop(pq).(*T)
}

func NewPriorityQueue[T any](less func(x, y *T) bool) *PriorityQueue[T] {
	pq := PriorityQueue[T]{
		items: make([]*T, 0),
		less:  less,
	}
	heap.Init(&pq)
	return &pq
}

func MergeSort(batchSize uint64, schema *schemapb.CollectionSchema, rr []RecordReader,
	rw RecordWriter, predicate func(r Record, ri, i int) bool, sortedByFieldIDs []int64,
) (numRows int, err error) {
	// Fast path: no readers provided
	if len(rr) == 0 {
		return 0, nil
	}

	type index struct {
		ri int
		i  int
	}

	recs := make([]Record, len(rr))
	advanceRecord := func(i int) error {
		rec, err := rr[i].Next()
		recs[i] = rec // assign nil if err
		return err
	}

	for i := range rr {
		err := advanceRecord(i)
		if err == io.EOF {
			continue
		}
		if err != nil {
			return 0, err
		}
	}

	comparators := make([]func(x, y *index) int, 0, len(sortedByFieldIDs))
	for _, fid := range sortedByFieldIDs {
		switch recs[0].Column(fid).(type) {
		case *array.Int64:
			comparators = append(comparators, func(x, y *index) int {
				xVal := recs[x.ri].Column(fid).(*array.Int64).Value(x.i)
				yVal := recs[y.ri].Column(fid).(*array.Int64).Value(y.i)
				if xVal < yVal {
					return -1
				}
				if xVal > yVal {
					return 1
				}
				return 0
			})
		case *array.String:
			comparators = append(comparators, func(x, y *index) int {
				xVal := recs[x.ri].Column(fid).(*array.String).Value(x.i)
				yVal := recs[y.ri].Column(fid).(*array.String).Value(y.i)
				if xVal < yVal {
					return -1
				}
				if xVal > yVal {
					return 1
				}
				return 0
			})
		default:
			return 0, merr.WrapErrParameterInvalidMsg("unsupported type for sorting key")
		}
	}

	pq := NewPriorityQueue(func(x, y *index) bool {
		for _, cmp := range comparators {
			c := cmp(x, y)
			if c < 0 {
				return true
			}
			if c > 0 {
				return false
			}
		}
		if x.ri != y.ri {
			return x.ri < y.ri
		}
		return x.i < y.i
	})

	endPositions := make([]int, len(recs))
	var enqueueAll func(ri int) error
	enqueueAll = func(ri int) error {
		r := recs[ri]
		hasValid := false
		endPosition := 0
		for j := 0; j < r.Len(); j++ {
			if predicate(r, ri, j) {
				pq.Enqueue(&index{
					ri: ri,
					i:  j,
				})
				numRows++
				hasValid = true
				endPosition = j
			}
		}
		if !hasValid {
			err := advanceRecord(ri)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			return enqueueAll(ri)
		}
		endPositions[ri] = endPosition
		return nil
	}

	for i, v := range recs {
		if v != nil {
			if err := enqueueAll(i); err != nil {
				return 0, err
			}
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

	for pq.Len() > 0 {
		idx := pq.Dequeue()
		rb.Append(recs[idx.ri], idx.i, idx.i+1)
		// Due to current arrow impl (v12), the write performance is largely dependent on the batch size,
		//	small batch size will cause write performance degradation. To work around this issue, we accumulate
		//	records and write them in batches. This requires additional memory copy.
		if rb.GetSize() >= batchSize {
			if err := writeRecord(); err != nil {
				return 0, err
			}
		}

		// If the popped idx reaches the last valid data of the segment, invalidate the cache and advance to the next record
		if idx.i == endPositions[idx.ri] {
			err := advanceRecord(idx.ri)
			if err == io.EOF {
				continue
			}
			if err != nil {
				return 0, err
			}
			if err := enqueueAll(idx.ri); err != nil {
				return 0, err
			}
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
