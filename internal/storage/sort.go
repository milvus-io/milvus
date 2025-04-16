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

	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func Sort(schema *schemapb.CollectionSchema, rr []RecordReader,
	rw RecordWriter, predicate func(r Record, ri, i int) bool,
) (int, error) {
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
				return 0, err
			}
		}
	}

	if len(records) == 0 {
		return 0, nil
	}

	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return 0, err
	}
	pkFieldId := pkField.FieldID

	switch records[0].Column(pkFieldId).(type) {
	case *array.Int64:
		sort.Slice(indices, func(i, j int) bool {
			pki := records[indices[i].ri].Column(pkFieldId).(*array.Int64).Value(indices[i].i)
			pkj := records[indices[j].ri].Column(pkFieldId).(*array.Int64).Value(indices[j].i)
			return pki < pkj
		})
	case *array.String:
		sort.Slice(indices, func(i, j int) bool {
			pki := records[indices[i].ri].Column(pkFieldId).(*array.String).Value(indices[i].i)
			pkj := records[indices[j].ri].Column(pkFieldId).(*array.String).Value(indices[j].i)
			return pki < pkj
		})
	}

	rb := NewRecordBuilder(schema)
	batchSize := 100000
	writeRecord := func() error {
		rec := rb.Build()
		defer rec.Release()
		if rec.Len() > 0 {
			return rw.Write(rec)
		}
		return nil
	}

	for i, idx := range indices {
		rb.Append(records[idx.ri], idx.i, idx.i+1)
		if (i+1)%batchSize == 0 {
			if err := writeRecord(); err != nil {
				return 0, err
			}
		}
	}

	// write the last batch
	if err := writeRecord(); err != nil {
		return 0, err
	}

	return len(indices), nil
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

func MergeSort(schema *schemapb.CollectionSchema, rr []RecordReader,
	rw RecordWriter, predicate func(r Record, ri, i int) bool,
) (numRows int, err error) {
	type index struct {
		ri int
		i  int
	}

	recs := make([]Record, len(rr))
	advanceRecord := func(i int) error {
		rec, err := rr[i].Next()
		recs[i] = rec // assign nil if err
		if err != nil {
			return err
		}
		return nil
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

	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return 0, err
	}
	pkFieldId := pkField.FieldID

	var pq *PriorityQueue[index]
	switch recs[0].Column(pkFieldId).(type) {
	case *array.Int64:
		pq = NewPriorityQueue(func(x, y *index) bool {
			return recs[x.ri].Column(pkFieldId).(*array.Int64).Value(x.i) < recs[y.ri].Column(pkFieldId).(*array.Int64).Value(y.i)
		})
	case *array.String:
		pq = NewPriorityQueue(func(x, y *index) bool {
			return recs[x.ri].Column(pkFieldId).(*array.String).Value(x.i) < recs[y.ri].Column(pkFieldId).(*array.String).Value(y.i)
		})
	}

	enqueueAll := func(ri int) {
		r := recs[ri]
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
			enqueueAll(i)
		}
	}

	// Due to current arrow impl (v12), the write performance is largely dependent on the batch size,
	//	small batch size will cause write performance degradation. To work around this issue, we accumulate
	//	records and write them in batches. This requires additional memory copy.
	batchSize := 100000
	rb := NewRecordBuilder(schema)

	for pq.Len() > 0 {
		idx := pq.Dequeue()
		rb.Append(recs[idx.ri], idx.i, idx.i+1)
		if rb.GetRowNum()%batchSize == 0 {
			if err := rw.Write(rb.Build()); err != nil {
				return 0, err
			}
		}

		// If poped idx reaches end of segment, invalidate cache and advance to next segment
		if idx.i == recs[idx.ri].Len()-1 {
			err := advanceRecord(idx.ri)
			if err == io.EOF {
				continue
			}
			if err != nil {
				return 0, err
			}
			enqueueAll(idx.ri)
		}
	}

	// write the last batch
	if rb.GetRowNum() > 0 {
		if err := rw.Write(rb.Build()); err != nil {
			return 0, err
		}
	}

	return numRows, nil
}
