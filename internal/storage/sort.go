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

	"github.com/apache/arrow/go/v12/arrow/array"
)

func Sort(rr []RecordReader, pkField FieldID, rw RecordWriter, predicate func(r Record, ri, i int) bool) (numRows int, err error) {
	records := make([]Record, 0)

	type index struct {
		ri int
		i  int
	}
	indices := make([]*index, 0)

	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	for _, r := range rr {
		for {
			err := r.Next()
			if err == nil {
				rec := r.Record()
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
				return 0, err
			}
		}
	}

	if len(records) == 0 {
		return 0, nil
	}

	switch records[0].Column(pkField).(type) {
	case *array.Int64:
		sort.Slice(indices, func(i, j int) bool {
			pki := records[indices[i].ri].Column(pkField).(*array.Int64).Value(indices[i].i)
			pkj := records[indices[j].ri].Column(pkField).(*array.Int64).Value(indices[j].i)
			return pki < pkj
		})
	case *array.String:
		sort.Slice(indices, func(i, j int) bool {
			pki := records[indices[i].ri].Column(pkField).(*array.String).Value(indices[i].i)
			pkj := records[indices[j].ri].Column(pkField).(*array.String).Value(indices[j].i)
			return pki < pkj
		})
	}

	writeOne := func(i *index) error {
		rec := records[i.ri].Slice(i.i, i.i+1)
		defer rec.Release()
		return rw.Write(rec)
	}
	for _, i := range indices {
		numRows++
		writeOne(i)
	}

	return numRows, nil
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

func MergeSort(rr []RecordReader, pkField FieldID, rw RecordWriter, predicate func(r Record, ri, i int) bool) (numRows int, err error) {
	type index struct {
		ri int
		i  int
	}

	advanceRecord := func(r RecordReader) (Record, error) {
		err := r.Next()
		if err != nil {
			return nil, err
		}
		return r.Record(), nil
	}

	recs := make([]Record, len(rr))
	for i, r := range rr {
		rec, err := advanceRecord(r)
		if err == io.EOF {
			recs[i] = nil
			continue
		}
		if err != nil {
			return 0, err
		}
		recs[i] = rec
	}

	var pq *PriorityQueue[index]
	switch recs[0].Column(pkField).(type) {
	case *array.Int64:
		pq = NewPriorityQueue[index](func(x, y *index) bool {
			return rr[x.ri].Record().Column(pkField).(*array.Int64).Value(x.i) < rr[y.ri].Record().Column(pkField).(*array.Int64).Value(y.i)
		})
	case *array.String:
		pq = NewPriorityQueue[index](func(x, y *index) bool {
			return rr[x.ri].Record().Column(pkField).(*array.String).Value(x.i) < rr[y.ri].Record().Column(pkField).(*array.String).Value(y.i)
		})
	}

	enqueueAll := func(ri int, r Record) {
		for j := 0; j < r.Len(); j++ {
			if predicate(r, ri, j) {
				pq.Enqueue(&index{
					ri: ri,
					i:  j,
				})
				numRows++
			}
		}
	}

	for i, v := range recs {
		if v != nil {
			enqueueAll(i, v)
		}
	}

	ri, istart, iend := -1, -1, -1
	for pq.Len() > 0 {
		idx := pq.Dequeue()
		if ri == idx.ri {
			// record end of cache, do nothing
			iend = idx.i + 1
		} else {
			if ri != -1 {
				// record changed, write old one and reset
				sr := rr[ri].Record().Slice(istart, iend)
				err := rw.Write(sr)
				sr.Release()
				if err != nil {
					return 0, err
				}
			}
			ri = idx.ri
			istart = idx.i
			iend = idx.i + 1
		}

		// If poped idx reaches end of segment, invalidate cache and advance to next segment
		if idx.i == rr[idx.ri].Record().Len()-1 {
			sr := rr[ri].Record().Slice(istart, iend)
			err := rw.Write(sr)
			sr.Release()
			if err != nil {
				return 0, err
			}
			ri, istart, iend = -1, -1, -1
			rec, err := advanceRecord(rr[idx.ri])
			if err == io.EOF {
				continue
			}
			if err != nil {
				return 0, err
			}
			enqueueAll(idx.ri, rec)
		}
	}
	return numRows, nil
}
