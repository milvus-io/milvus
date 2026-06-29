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
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// SortTimings holds phase-level timing information from the Sort function.
type SortTimings struct {
	ReadCost   time.Duration
	SortCost   time.Duration
	WriteCost  time.Duration
	NumBatches int
	NumRows    int
}

// how many rows per run we allow in memory (you requested 1,000,000)
var runRowLimit = 1000000

func Sort(batchSize uint64, schema *schemapb.CollectionSchema, rr []RecordReader,
	rw RecordWriter, predicate func(r Record, ri, i int) bool, sortByFieldIDs []int64,
) (int, *SortTimings, error) {
	records := make([]Record, 0)

	type index struct {
		ri int
		i  int
	}
	phaseStart := time.Now()
	rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	tmpDir, err := os.MkdirTemp(rootPath, "milvus-sort-*")
	if err != nil {
		return 0, nil, merr.WrapErrStorageMsg("failed to create temp dir")
	}
	// remove entire tmpDir at the end
	defer os.RemoveAll(tmpDir)

	indices := make([]*index, 0)

	runRows := 0
	// keep a list of temp file paths (each holds a sorted run)
	tmpFiles := make([]string, 0)

	// current run builder + pk values
	alloc := memory.NewGoAllocator()

	flushRun := func() error {
		if runRows == 0 {
			return nil
		}
		type keyCmp func(x, y *index) int
		comparators := make([]keyCmp, 0, len(sortByFieldIDs))
		if len(sortByFieldIDs) > 0 {
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
					return merr.WrapErrStorageMsg("unsupported type for sorting key")
				}
			}
		}

		if len(comparators) > 0 {
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
		// open a temp file to write the sorted run
		tmpFile, err := os.CreateTemp(tmpDir, "run-*.arrow")
		if err != nil {
			return err
		}
		defer tmpFile.Close()
		var ipcWriter *ipc.FileWriter
		// helper to write an Arrow record to the file
		writeRecordToFile := func(rec Record) error {
			sar, ok := rec.(*simpleArrowRecord)
			if !ok {
				return merr.WrapErrStorageMsg("unexpected Record type")
			}
			arrowRec := sar.r
			if ipcWriter == nil {
				ipcWriter, err = ipc.NewFileWriter(tmpFile, ipc.WithSchema(arrowRec.Schema()))
				if err != nil {
					return err
				}
			}
			if err := ipcWriter.Write(arrowRec); err != nil {
				ipcWriter.Close()
				return err
			}
			return nil
		}

		outRB := NewRecordBuilder(schema)

		// append rows in sorted order
		for _, idx := range indices {
			if err := outRB.Append(records[idx.ri], idx.i, idx.i+1); err != nil {
				return err
			}

			// flush to file if batchSize reached
			if outRB.GetSize() >= batchSize {
				tmpRec := outRB.Build()
				if err := writeRecordToFile(tmpRec); err != nil {
					tmpRec.Release()
					return err
				}
				tmpRec.Release()
				outRB = NewRecordBuilder(schema)
			}
		}

		// flush any remaining rows
		if outRB.GetRowNum() > 0 {
			tmpRec := outRB.Build()
			if err := writeRecordToFile(tmpRec); err != nil {
				tmpRec.Release()
				return err
			}
			tmpRec.Release()
		}
		if ipcWriter != nil {
			err = ipcWriter.Close()
			if err != nil {
				return err
			}
		}
		// save temp file path
		tmpFiles = append(tmpFiles, tmpFile.Name())
		// release cgo records
		for _, rec := range records {
			rec.Release()
		}

		// reset builder and pk arrays
		indices = make([]*index, 0)
		runRows = 0
		records = make([]Record, 0)
		return nil
	}

	totalRecords := 0
	for _, r := range rr {
		for {
			rec, err := r.Next()
			if err == nil {
				rec.Retain()
				ri := len(records)
				records = append(records, rec)
				for i := 0; i < rec.Len(); i++ {
					if !predicate(rec, ri, i) {
						continue
					}
					indices = append(indices, &index{ri, i})
					runRows++
				}
				totalRecords++
				// if run reached limit, flush it
				if runRows >= runRowLimit {
					if err := flushRun(); err != nil {
						return 0, nil, err
					}
				}
			} else if err == io.EOF {
				break
			} else {
				return 0, nil, err
			}
		}
	}
	if runRows > 0 {
		if err := flushRun(); err != nil {
			return 0, nil, err
		}
	}
	readCost := time.Since(phaseStart)

	if totalRecords == 0 {
		return 0, &SortTimings{ReadCost: readCost}, nil
	}

	rdrs := make([]RecordReader, 0, len(tmpFiles))
	for _, p := range tmpFiles {
		rdr, err := newIPCFileRecordReader(p, alloc, schema)
		if err != nil {
			// close already opened readers
			for _, rrdr := range rdrs {
				rrdr.Close()
			}
			return 0, nil, err
		}
		rdrs = append(rdrs, rdr)
	}
	defer func() {
		for _, rrdr := range rdrs {
			rrdr.Close()
		}
	}()

	phaseStart = time.Now()
	// MergeSort handles both 1 or many readers
	numRows, err := MergeSort(batchSize, schema, rdrs, rw, predicate, sortByFieldIDs)
	if err != nil {
		return 0, nil, err
	}
	sortCost := time.Since(phaseStart)
	writeCost := time.Since(phaseStart)

	timings := &SortTimings{
		ReadCost:   readCost,
		SortCost:   sortCost,
		WriteCost:  writeCost,
		NumBatches: totalRecords,
		NumRows:    numRows,
	}
	return numRows, timings, nil
}

type ipcFileRecordReader struct {
	f            *os.File
	fr           *ipc.FileReader
	schema       *schemapb.CollectionSchema
	batchIdx     int
	totalBatches int
}

// newIPCFileRecordReader opens the temp file and prepares for sequential batch reading
func newIPCFileRecordReader(path string, alloc memory.Allocator, schema *schemapb.CollectionSchema) (RecordReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fr, err := ipc.NewFileReader(f, ipc.WithAllocator(alloc))
	if err != nil {
		f.Close()
		return nil, err
	}
	return &ipcFileRecordReader{
		f:            f,
		fr:           fr,
		schema:       schema,
		batchIdx:     0,
		totalBatches: fr.NumRecords(),
	}, nil
}

// Next returns the next Arrow Record batch, or io.EOF if all batches are consumed
func (r *ipcFileRecordReader) Next() (Record, error) {
	if r.batchIdx >= r.totalBatches {
		return nil, io.EOF
	}

	arrowRec, err := r.fr.Record(r.batchIdx)
	if err != nil {
		return nil, err
	}
	r.batchIdx++

	// build FieldID -> column index mapping
	field2col := make(map[FieldID]int)
	nameToFieldID := map[string]FieldID{}
	if r.schema != nil {
		for _, f := range r.schema.GetFields() {
			nameToFieldID[f.GetName()] = f.GetFieldID()
		}
		for _, sf := range r.schema.GetStructArrayFields() {
			for _, f := range sf.GetFields() {
				nameToFieldID[f.GetName()] = f.GetFieldID()
			}
		}
	}

	for colIdx, f := range arrowRec.Schema().Fields() {
		if id, err := strconv.Atoi(f.Name); err == nil {
			field2col[FieldID(id)] = colIdx
			continue
		}
		if fid, ok := nameToFieldID[f.Name]; ok {
			field2col[fid] = colIdx
		}
	}

	return NewSimpleArrowRecord(arrowRec, field2col), nil
}

// SetNeededFields is a no-op for consistency
func (r *ipcFileRecordReader) SetNeededFields(_ typeutil.Set[int64]) {}

// Close closes both the file reader and the underlying file
func (r *ipcFileRecordReader) Close() error {
	if err := r.fr.Close(); err != nil {
		r.f.Close()
		return err
	}
	return r.f.Close()
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
			return 0, merr.WrapErrStorageMsg("unsupported type for sorting key")
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
