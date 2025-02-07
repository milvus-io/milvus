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
	"strconv"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func Sort(schema *schemapb.CollectionSchema, rr []RecordReader,
	pkField FieldID, rw RecordWriter, predicate func(r Record, ri, i int) bool,
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

	// Due to current arrow impl (v12), the write performance is largely dependent on the batch size,
	//	small batch size will cause write performance degradation. To work around this issue, we accumulate
	//	records and write them in batches. This requires additional memory copy.
	batchSize := 100000
	builders := make([]array.Builder, len(schema.Fields))
	for i, f := range schema.Fields {
		b := array.NewBuilder(memory.DefaultAllocator, records[0].Column(f.FieldID).DataType())
		b.Reserve(batchSize)
		builders[i] = b
	}

	writeRecord := func(rowNum int64) {
		arrays := make([]arrow.Array, len(builders))
		fields := make([]arrow.Field, len(builders))
		field2Col := make(map[FieldID]int, len(builders))

		for c, builder := range builders {
			arrays[c] = builder.NewArray()
			fid := schema.Fields[c].FieldID
			fields[c] = arrow.Field{
				Name:     strconv.Itoa(int(fid)),
				Type:     arrays[c].DataType(),
				Nullable: true, // No nullable check here.
			}
			field2Col[fid] = c
		}

		rec := newSimpleArrowRecord(array.NewRecord(arrow.NewSchema(fields, nil), arrays, rowNum), field2Col)
		rw.Write(rec)
		rec.Release()
	}

	for i, idx := range indices {
		for c, builder := range builders {
			fid := schema.Fields[c].FieldID
			appendValueAt(builder, records[idx.ri].Column(fid), idx.i)
		}
		if (i+1)%batchSize == 0 {
			writeRecord(int64(batchSize))
		}
	}

	// write the last batch
	if len(indices)%batchSize != 0 {
		writeRecord(int64(len(indices) % batchSize))
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
	pkField FieldID, rw RecordWriter, predicate func(r Record, ri, i int) bool,
) (numRows int, err error) {
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

	// Due to current arrow impl (v12), the write performance is largely dependent on the batch size,
	//	small batch size will cause write performance degradation. To work around this issue, we accumulate
	//	records and write them in batches. This requires additional memory copy.
	batchSize := 100000
	builders := make([]array.Builder, len(schema.Fields))
	for i, f := range schema.Fields {
		b := array.NewBuilder(memory.DefaultAllocator, recs[0].Column(f.FieldID).DataType())
		b.Reserve(batchSize)
		builders[i] = b
	}

	writeRecord := func(rowNum int64) {
		arrays := make([]arrow.Array, len(builders))
		fields := make([]arrow.Field, len(builders))
		field2Col := make(map[FieldID]int, len(builders))

		for c, builder := range builders {
			arrays[c] = builder.NewArray()
			fid := schema.Fields[c].FieldID
			fields[c] = arrow.Field{
				Name:     strconv.Itoa(int(fid)),
				Type:     arrays[c].DataType(),
				Nullable: true, // No nullable check here.
			}
			field2Col[fid] = c
		}

		rec := newSimpleArrowRecord(array.NewRecord(arrow.NewSchema(fields, nil), arrays, rowNum), field2Col)
		rw.Write(rec)
		rec.Release()
	}

	rc := 0
	for pq.Len() > 0 {
		idx := pq.Dequeue()

		for c, builder := range builders {
			fid := schema.Fields[c].FieldID
			appendValueAt(builder, rr[idx.ri].Record().Column(fid), idx.i)
		}
		if (rc+1)%batchSize == 0 {
			writeRecord(int64(batchSize))
			rc = 0
		} else {
			rc++
		}

		// If poped idx reaches end of segment, invalidate cache and advance to next segment
		if idx.i == rr[idx.ri].Record().Len()-1 {
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

	// write the last batch
	if rc > 0 {
		writeRecord(int64(rc))
	}

	return numRows, nil
}
