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

package compactor

import (
	"container/heap"
	"context"
	"encoding/binary"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const clusteringMergeFanIn = 16

// Source position breaks distance ties independently of spill boundaries.
// Offsets and group ordinals belong to this Analyze generation only.
type clusteringSortKey struct {
	group, cluster uint32
	distance       float32
	source, row    uint64
}

func (k clusteringSortKey) compare(other clusteringSortKey) int {
	if k.group != other.group {
		if k.group < other.group {
			return -1
		}
		return 1
	}
	if k.cluster != other.cluster {
		if k.cluster < other.cluster {
			return -1
		}
		return 1
	}
	if k.distance != other.distance {
		if k.distance < other.distance {
			return -1
		}
		return 1
	}
	if k.source != other.source {
		if k.source < other.source {
			return -1
		}
		return 1
	}
	if k.row < other.row {
		return -1
	}
	if k.row > other.row {
		return 1
	}
	return 0
}

func (k clusteringSortKey) bytes() []byte {
	b := make([]byte, 28)
	binary.LittleEndian.PutUint32(b, k.group)
	binary.LittleEndian.PutUint32(b[4:], k.cluster)
	binary.LittleEndian.PutUint32(b[8:], math.Float32bits(k.distance))
	binary.LittleEndian.PutUint64(b[12:], k.source)
	binary.LittleEndian.PutUint64(b[20:], k.row)
	return b
}

func clusteringKeyFromBytes(b []byte) clusteringSortKey {
	return clusteringSortKey{binary.LittleEndian.Uint32(b), binary.LittleEndian.Uint32(b[4:]), math.Float32frombits(binary.LittleEndian.Uint32(b[8:])), binary.LittleEndian.Uint64(b[12:]), binary.LittleEndian.Uint64(b[20:])}
}

type clusteringRowEmitter func(storage.Record, int, clusteringSortKey) error

// A task-private external sort. Runs contain Arrow columns, preserving nulls,
// half vectors, nested fields and TEXT/LOB representation without Value casts.
// Only the final merge writes real segments. Intermediate runs have no IDs or
// metadata visible to DataCoord and are deleted when this mapping returns.
type clusteringMappingSorter struct {
	ctx          context.Context
	schema       *schemapb.CollectionSchema
	fields       []*schemapb.FieldSchema
	fieldColumns map[int64]int
	dir          string
	budget       uint64
	builder      *storage.RecordBuilder
	keys         []clusteringSortKey
	levels       [][]string
	peakOpenRuns int
}

func newClusteringMappingSorter(ctx context.Context, schema *schemapb.CollectionSchema, root string, budget uint64) (*clusteringMappingSorter, error) {
	if budget == 0 {
		return nil, merr.WrapErrServiceInternalMsg("zero clustering sort budget")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, merr.WrapErrIoFailed("create clustering spill root", err)
	}
	dir, err := os.MkdirTemp(root, "mapping-")
	if err != nil {
		return nil, merr.WrapErrIoFailed("create clustering spill directory", err)
	}
	fields := slices.Clone(schema.GetFields())
	for _, field := range schema.GetStructArrayFields() {
		fields = append(fields, field.GetFields()...)
	}
	columns := make(map[int64]int, len(fields))
	for i, field := range fields {
		columns[field.GetFieldID()] = i
	}
	return &clusteringMappingSorter{ctx: ctx, schema: schema, fields: fields, fieldColumns: columns, dir: dir, budget: budget, builder: storage.NewRecordBuilderWithFieldID(schema)}, nil
}

func (s *clusteringMappingSorter) Close() error {
	s.builder.Release()
	if err := os.RemoveAll(s.dir); err != nil {
		return merr.WrapErrIoFailed("remove clustering spill directory", err)
	}
	return nil
}

func (s *clusteringMappingSorter) Append(record storage.Record, row int, key clusteringSortKey) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if math.IsNaN(float64(key.distance)) || math.IsInf(float64(key.distance), 0) || key.distance < 0 {
		return merr.WrapErrDataIntegrityMsg("invalid clustering sort distance")
	}
	if err := s.builder.Append(record, row, row+1); err != nil {
		return err
	}
	s.keys = append(s.keys, key)
	if s.builder.GetMemorySize()+uint64(cap(s.keys))*32 >= s.budget {
		return s.spill()
	}
	return nil
}

func (s *clusteringMappingSorter) spill() error {
	if len(s.keys) == 0 {
		return nil
	}
	record := s.builder.Build()
	defer record.Release()
	order := make([]int, len(s.keys))
	for i := range order {
		order[i] = i
	}
	slices.SortFunc(order, func(a, b int) int { return s.keys[a].compare(s.keys[b]) })
	run, err := s.writeRun(func(emit clusteringRowEmitter) error {
		for _, row := range order {
			if err := emit(record, row, s.keys[row]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.keys = nil
	return s.addRun(run, 0)
}

// Eager leveled merging bounds both open files and the in-memory run directory.
func (s *clusteringMappingSorter) addRun(run string, level int) error {
	for len(s.levels) <= level {
		s.levels = append(s.levels, nil)
	}
	s.levels[level] = append(s.levels[level], run)
	if len(s.levels[level]) < clusteringMergeFanIn {
		return nil
	}
	inputs := s.levels[level]
	merged, err := s.writeRun(func(emit clusteringRowEmitter) error { return s.merge(inputs, emit) })
	if err != nil {
		return err
	}
	for _, input := range inputs {
		if err := os.Remove(input); err != nil {
			return merr.WrapErrIoFailed("remove merged clustering run", err)
		}
	}
	s.levels[level] = nil
	return s.addRun(merged, level+1)
}

func (s *clusteringMappingSorter) Finish(emit clusteringRowEmitter) error {
	if err := s.spill(); err != nil {
		return err
	}
	var runs []string
	for _, level := range s.levels {
		runs = append(runs, level...)
	}
	for len(runs) > clusteringMergeFanIn {
		inputs := runs[:clusteringMergeFanIn]
		merged, err := s.writeRun(func(out clusteringRowEmitter) error { return s.merge(inputs, out) })
		if err != nil {
			return err
		}
		for _, input := range inputs {
			if err := os.Remove(input); err != nil {
				return merr.WrapErrIoFailed("remove merged clustering run", err)
			}
		}
		runs = append(runs[clusteringMergeFanIn:], merged)
	}
	return s.merge(runs, emit)
}

func (s *clusteringMappingSorter) writeRun(produce func(clusteringRowEmitter) error) (string, error) {
	file, err := os.CreateTemp(s.dir, "run-")
	if err != nil {
		return "", merr.WrapErrIoFailed("create clustering run", err)
	}
	defer file.Close()
	builder := storage.NewRecordBuilderWithFieldID(s.schema)
	defer builder.Release()
	var keys []clusteringSortKey
	var writer *ipc.Writer
	defer func() {
		if writer != nil {
			_ = writer.Close()
		}
	}()
	flush := func() error {
		if len(keys) == 0 {
			return nil
		}
		r := builder.Build()
		defer r.Release()
		columns := make([]arrow.Array, 0, len(s.fields)+1)
		fields := make([]arrow.Field, 0, len(s.fields)+1)
		for _, field := range s.fields {
			col := r.Column(field.GetFieldID())
			// Materialized missing TEXT may arrive as binary nulls. A spill stream
			// needs one schema even if its first batch has no decoded text values.
			if field.GetDataType() == schemapb.DataType_Text && col.DataType().ID() == arrow.BINARY && col.NullN() == col.Len() {
				b := array.NewStringBuilder(memory.DefaultAllocator)
				b.AppendNulls(col.Len())
				normalized := b.NewStringArray()
				b.Release()
				defer normalized.Release()
				col = normalized
			}
			columns = append(columns, col)
			fields = append(fields, arrow.Field{Name: strconv.FormatInt(field.GetFieldID(), 10), Type: col.DataType(), Nullable: true})
		}
		kb := array.NewFixedSizeBinaryBuilder(memory.DefaultAllocator, &arrow.FixedSizeBinaryType{ByteWidth: 28})
		for _, key := range keys {
			kb.Append(key.bytes())
		}
		ka := kb.NewArray()
		kb.Release()
		defer ka.Release()
		columns = append(columns, ka)
		fields = append(fields, arrow.Field{Name: "__clustering_sort_key", Type: ka.DataType()})
		record := array.NewRecord(arrow.NewSchema(fields, nil), columns, int64(len(keys)))
		defer record.Release()
		if writer == nil {
			writer = ipc.NewWriter(file, ipc.WithSchema(record.Schema()))
		}
		if err := writer.Write(record); err != nil {
			return merr.WrapErrIoFailed("write clustering run", err)
		}
		keys = nil
		return nil
	}
	// A merge retains at most fan-in input batches, plus one output batch.
	batchBudget := max(uint64(1), s.budget/(clusteringMergeFanIn+1))
	err = produce(func(r storage.Record, row int, key clusteringSortKey) error {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		if err := builder.Append(r, row, row+1); err != nil {
			return err
		}
		keys = append(keys, key)
		if builder.GetMemorySize()+uint64(cap(keys))*32 >= batchBudget || len(keys) >= 1024 {
			return flush()
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if err := flush(); err != nil {
		return "", err
	}
	if writer == nil {
		return "", merr.WrapErrServiceInternalMsg("empty clustering run")
	}
	if err := writer.Close(); err != nil {
		return "", merr.WrapErrIoFailed("close clustering run", err)
	}
	writer = nil
	if err := file.Close(); err != nil {
		return "", merr.WrapErrIoFailed("close clustering run file", err)
	}
	return filepath.Clean(file.Name()), nil
}

type clusteringRunCursor struct {
	file     *os.File
	reader   *ipc.Reader
	record   storage.Record
	row      int
	keys     *array.FixedSizeBinary
	key      clusteringSortKey
	previous *clusteringSortKey
}

func (c *clusteringRunCursor) advance(columns map[int64]int) (bool, error) {
	c.row++
	for c.record == nil || c.row >= c.record.Len() {
		if c.record != nil {
			c.record.Release()
			c.record = nil
		}
		if !c.reader.Next() {
			if err := c.reader.Err(); err != nil && err != io.EOF {
				return false, merr.WrapErrDataIntegrity(err, "read clustering run")
			}
			return false, nil
		}
		r := c.reader.Record()
		if r.NumCols() != int64(len(columns)+1) {
			return false, merr.WrapErrDataIntegrityMsg("clustering run column count mismatch")
		}
		keys, ok := r.Column(int(r.NumCols()) - 1).(*array.FixedSizeBinary)
		if !ok || keys.DataType().(*arrow.FixedSizeBinaryType).ByteWidth != 28 {
			return false, merr.WrapErrDataIntegrityMsg("invalid clustering run sort keys")
		}
		r.Retain()
		c.record = storage.NewSimpleArrowRecord(r, columns)
		c.keys, c.row = keys, 0
	}
	if c.keys.IsNull(c.row) {
		return false, merr.WrapErrDataIntegrityMsg("null clustering run sort key")
	}
	c.key = clusteringKeyFromBytes(c.keys.Value(c.row))
	if math.IsNaN(float64(c.key.distance)) || math.IsInf(float64(c.key.distance), 0) || c.key.distance < 0 {
		return false, merr.WrapErrDataIntegrityMsg("invalid clustering run distance")
	}

	if c.previous != nil && c.previous.compare(c.key) > 0 {
		return false, merr.WrapErrDataIntegrityMsg("clustering run is not sorted")
	}
	previous := c.key
	c.previous = &previous
	return true, nil
}

type clusteringRunHeap []*clusteringRunCursor

func (h clusteringRunHeap) Len() int           { return len(h) }
func (h clusteringRunHeap) Less(i, j int) bool { return h[i].key.compare(h[j].key) < 0 }
func (h clusteringRunHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *clusteringRunHeap) Push(v any)        { *h = append(*h, v.(*clusteringRunCursor)) }
func (h *clusteringRunHeap) Pop() any {
	old := *h
	v := old[len(old)-1]
	*h = old[:len(old)-1]
	return v
}

func (s *clusteringMappingSorter) merge(paths []string, emit clusteringRowEmitter) error {
	if len(paths) > clusteringMergeFanIn {
		return merr.WrapErrServiceInternalMsg("clustering merge fan-in exceeded")
	}
	s.peakOpenRuns = max(s.peakOpenRuns, len(paths))
	var cursors []*clusteringRunCursor
	defer func() {
		for _, c := range cursors {
			if c.record != nil {
				c.record.Release()
			}
			if c.reader != nil {
				c.reader.Release()
			}
			_ = c.file.Close()
		}
	}()
	var queue clusteringRunHeap
	for _, path := range paths {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		file, err := os.Open(path)
		if err != nil {
			return merr.WrapErrIoFailed("open clustering run", err)
		}
		c := &clusteringRunCursor{file: file}
		cursors = append(cursors, c)
		c.reader, err = ipc.NewReader(file)
		if err != nil {
			return merr.WrapErrDataIntegrity(err, "decode clustering run")
		}
		ok, err := c.advance(s.fieldColumns)
		if err != nil {
			return err
		}
		if ok {
			heap.Push(&queue, c)
		}
	}
	for len(queue) > 0 {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		c := heap.Pop(&queue).(*clusteringRunCursor)
		if err := emit(c.record, c.row, c.key); err != nil {
			return err
		}
		ok, err := c.advance(s.fieldColumns)
		if err != nil {
			return err
		}
		if ok {
			heap.Push(&queue, c)
		}
	}
	return nil
}
