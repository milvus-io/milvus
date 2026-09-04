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
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// exhaustedChunkReader is a minimal RecordReader that is already at EOF and
// records whether it was closed. It stands in for a binlog chunk that opened
// successfully and has been fully consumed.
type exhaustedChunkReader struct {
	closed bool
}

func (r *exhaustedChunkReader) Next() (Record, error) { return nil, io.EOF }

func (r *exhaustedChunkReader) Close() error {
	r.closed = true
	return nil
}

// TestIterativeRecordReader_MissingChunkDoesNotPanic reproduces #50927: when a
// later binlog chunk's object is missing in object storage, newPackedRecordReader
// returns a nil *packedRecordReader together with an error, and iterate() boxes
// that nil pointer into a non-nil RecordReader interface. The reader must surface
// the error from Next() and must NOT panic on the deferred Close() (previously a
// nil-pointer dereference that crashed the DataNode into CrashLoopBackOff).
func TestIterativeRecordReader_MissingChunkDoesNotPanic(t *testing.T) {
	missingErr := errors.New("IOError: Path does not exist")
	chunk := &exhaustedChunkReader{}
	call := 0
	ir := &IterativeRecordReader{
		iterate: func() (RecordReader, error) {
			call++
			if call == 1 {
				// first chunk opens fine (and is already exhausted)
				return chunk, nil
			}
			// second chunk's object is missing: mirror newPackedRecordReader
			// returning a typed-nil *packedRecordReader together with an error.
			var pr *packedRecordReader
			return pr, missingErr
		},
	}

	// Drive it the way storage.Sort does: read until a non-nil error.
	var gotErr error
	for {
		if _, err := ir.Next(); err != nil {
			gotErr = err
			break
		}
	}
	assert.ErrorIs(t, gotErr, missingErr, "the missing-object error must be surfaced, not swallowed/recovered")
	assert.True(t, chunk.closed, "the exhausted first chunk must have been closed")

	// The deferred Close() in sortSegment must not panic on the failed chunk.
	assert.NotPanics(t, func() {
		assert.NoError(t, ir.Close())
	})
}

// TestPackedRecordReader_CloseNilReceiverDoesNotPanic pins the defense-in-depth
// half of the #50927 fix: Close() on a typed-nil reader (the shape produced when
// newPackedRecordReader fails and its nil result is boxed into a RecordReader
// interface) must be a safe no-op, not a nil-pointer dereference.
func TestPackedRecordReader_CloseNilReceiverDoesNotPanic(t *testing.T) {
	var pr *packedRecordReader
	assert.NotPanics(t, func() {
		assert.NoError(t, pr.Close())
	})

	var fpr *ffiPackedRecordReader
	assert.NotPanics(t, func() {
		assert.NoError(t, fpr.Close())
	})

	// Boxed into the interface, exactly as the deferred Close in sortSegment sees it.
	var rr RecordReader = pr
	assert.NotPanics(t, func() {
		assert.NoError(t, rr.Close())
	})
}

func TestCompositeBinlogRecordReaderOwnsCurrentRecord(t *testing.T) {
	newCurrent := func(allocator *memory.CheckedAllocator) Record {
		builder := array.NewInt64Builder(allocator)
		builder.Append(1)
		column := builder.NewArray()
		builder.Release()
		return &compositeRecord{
			index: map[FieldID]int16{100: 0},
			recs:  []arrow.Array{column},
		}
	}

	t.Run("next releases previous current", func(t *testing.T) {
		allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
		reader := &CompositeBinlogRecordReader{
			fields:  map[FieldID]*schemapb.FieldSchema{},
			index:   map[FieldID]int16{},
			current: newCurrent(allocator),
		}

		_, err := reader.Next()
		require.NoError(t, err)
		allocator.AssertSize(t, 0)
		require.NoError(t, reader.Close())
	})

	t.Run("close releases current", func(t *testing.T) {
		allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
		reader := &CompositeBinlogRecordReader{current: newCurrent(allocator)}

		require.NoError(t, reader.Close())
		allocator.AssertSize(t, 0)
	})
}

func TestAbsentFieldFillReaderFillsDefaultAndNull(t *testing.T) {
	// inner returns ONLY the present field (id); the two added fields are absent.
	const idField, defField, nullField = FieldID(100), FieldID(101), FieldID(102)
	idb := array.NewInt64Builder(memory.DefaultAllocator)
	defer idb.Release()
	idb.AppendValues([]int64{1, 2, 3}, nil)
	idArr := idb.NewArray()
	defer idArr.Release()
	inner := &compositeRecord{index: map[FieldID]int16{idField: 0}, recs: []arrow.Array{idArr}}

	readSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: idField, Name: "id", DataType: schemapb.DataType_Int64},
		{FieldID: defField, Name: "added_def", DataType: schemapb.DataType_Int64, Nullable: true, DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 42}}},
		{FieldID: nullField, Name: "added_nulldef", DataType: schemapb.DataType_Int64, Nullable: true},
	}}
	present := map[FieldID]struct{}{idField: {}}

	rr := NewAbsentFieldFillRecordReader(&sliceRecordReader{recs: []Record{inner}}, readSchema, present)
	defer rr.Close()
	rec, err := rr.Next()
	require.NoError(t, err)

	def := rec.Column(defField).(*array.Int64)
	require.Equal(t, 0, def.NullN())
	require.Equal(t, []int64{42, 42, 42}, []int64{def.Value(0), def.Value(1), def.Value(2)}) // default materialized
	require.Equal(t, 3, rec.Column(nullField).(*array.Int64).NullN())                        // no default -> null
	require.Equal(t, int64(2), rec.Column(idField).(*array.Int64).Value(1))                  // present passthrough
}

func TestAbsentFieldFillReaderRejectsNonNullableAbsent(t *testing.T) {
	const idField, badField = FieldID(100), FieldID(101)
	idb := array.NewInt64Builder(memory.DefaultAllocator)
	defer idb.Release()
	idb.AppendValues([]int64{1}, nil)
	idArr := idb.NewArray()
	defer idArr.Release()
	inner := &compositeRecord{index: map[FieldID]int16{idField: 0}, recs: []arrow.Array{idArr}}
	readSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: idField, Name: "id", DataType: schemapb.DataType_Int64},
		{FieldID: badField, Name: "bad", DataType: schemapb.DataType_Int64, Nullable: false}, // non-nullable, absent
	}}
	rr := NewAbsentFieldFillRecordReader(&sliceRecordReader{recs: []Record{inner}}, readSchema, map[FieldID]struct{}{idField: {}})
	defer rr.Close()
	_, err := rr.Next()
	require.Error(t, err)
}

// countedRecord wraps a Record and tracks its net Retain/Release balance so a test
// can assert the wrapper neither over-releases (missing Retain -> live < 0) nor
// leaks (stray Retain -> live > 0) a borrowed base. It still forwards to the
// embedded Record, so arrow's own buffer refcount stays consistent. This is needed
// because CheckedAllocator only sees NET bytes: a mis-timed / negative-refcount
// release is byte-balanced (the buffer is still freed exactly once) and slips past
// AssertSize.
type countedRecord struct {
	Record
	live int
}

func (r *countedRecord) Retain() {
	r.live++
	r.Record.Retain()
}

func (r *countedRecord) Release() {
	r.live--
	r.Record.Release()
}

// borrowingRecordReader models the real borrowed-record contract: it owns the
// records handed to it and releases the previously returned one on the next
// Next()/Close(), the way a packed/binlog reader frees or reuses its buffer when
// advanced. A wrapper that keeps a returned record past an advance must therefore
// Retain it; otherwise the inner release below drops it under the wrapper.
// sliceRecordReader is a minimal RecordReader that hands out a fixed slice of
// records one per Next() call and then reports io.EOF. It does not take ownership
// of the records (no Release on advance/Close), so callers keep their own refs.
type sliceRecordReader struct {
	recs []Record
	pos  int
}

func (r *sliceRecordReader) Next() (Record, error) {
	if r.pos >= len(r.recs) {
		return nil, io.EOF
	}
	rec := r.recs[r.pos]
	r.pos++
	return rec, nil
}

func (r *sliceRecordReader) Close() error { return nil }

type borrowingRecordReader struct {
	recs []Record
	pos  int
	held Record
}

func (r *borrowingRecordReader) Next() (Record, error) {
	if r.held != nil {
		r.held.Release()
		r.held = nil
	}
	if r.pos >= len(r.recs) {
		return nil, io.EOF
	}
	r.held = r.recs[r.pos]
	r.pos++
	return r.held, nil
}

func (r *borrowingRecordReader) Close() error {
	if r.held != nil {
		r.held.Release()
		r.held = nil
	}
	return nil
}

// TestAbsentFieldFillReaderBaseRefcountBalanced drives the wrapper across TWO
// borrowed base records + EOF + Close with a CheckedAllocator behind base and an
// inner that releases each base when advanced (the borrowed contract). The wrapper
// must Retain base when it stores it and Release it on the next advance/Close;
// without that, the inner's release drops base under the wrapper (double free).
func TestAbsentFieldFillReaderBaseRefcountBalanced(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	newBase := func(vals []int64) *countedRecord {
		b := array.NewInt64Builder(alloc)
		b.AppendValues(vals, nil)
		arr := b.NewArray()
		b.Release()
		return &countedRecord{Record: &compositeRecord{index: map[FieldID]int16{100: 0}, recs: []arrow.Array{arr}}, live: 1}
	}
	// the inner owns both bases and releases them as it is advanced/closed.
	base1, base2 := newBase([]int64{1, 2}), newBase([]int64{3, 4, 5})
	inner := &borrowingRecordReader{recs: []Record{base1, base2}}

	readSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
		{FieldID: 101, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true},
	}}
	rr := NewAbsentFieldFillRecordReader(inner, readSchema, map[FieldID]struct{}{100: {}})

	_, err := rr.Next()
	require.NoError(t, err)
	_, err = rr.Next() // wrapper releases its ref on base1, inner releases its own
	require.NoError(t, err)
	_, err = rr.Next() // EOF; same for base2
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, rr.Close())

	require.Zero(t, base1.live, "base1 wrapper Retain/Release must balance (negative = missing Retain)")
	require.Zero(t, base2.live, "base2 wrapper Retain/Release must balance (negative = missing Retain)")
	alloc.AssertSize(t, 0)
}

// TestAbsentFieldFillReaderErrorPathDoesNotRetainBase pins that base.Retain runs
// only AFTER the fill loop succeeds: when a later absent field fails to generate,
// Next returns the error without retaining base, so the inner's own release on
// Close frees base exactly once — a base retained on the error path would leak.
func TestAbsentFieldFillReaderErrorPathDoesNotRetainBase(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	b := array.NewInt64Builder(alloc)
	b.AppendValues([]int64{1}, nil)
	arr := b.NewArray()
	b.Release()
	base := &countedRecord{Record: &compositeRecord{index: map[FieldID]int16{100: 0}, recs: []arrow.Array{arr}}, live: 1}
	inner := &borrowingRecordReader{recs: []Record{base}}

	// GetAllFieldSchemas preserves Fields order: 101 (nullable, fills) then 102
	// (non-nullable, GenerateEmptyArrayFromSchema errors) — the failure is mid-loop.
	readSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
		{FieldID: 101, Name: "ok", DataType: schemapb.DataType_Int64, Nullable: true},
		{FieldID: 102, Name: "bad", DataType: schemapb.DataType_Int64, Nullable: false},
	}}
	rr := NewAbsentFieldFillRecordReader(inner, readSchema, map[FieldID]struct{}{100: {}})
	_, err := rr.Next()
	require.Error(t, err)
	require.NoError(t, rr.Close())

	require.Zero(t, base.live, "error path must not retain base (positive = leaked Retain)")
	alloc.AssertSize(t, 0)
}

// TestAbsentFieldFillReaderFillsStructChildren pins that struct array fields are
// covered: their first-level child columns are flattened by GetAllFieldSchemas, so
// an absent whole struct has each child filled (null here, no default) rather than
// left missing.
func TestAbsentFieldFillReaderFillsStructChildren(t *testing.T) {
	idb := array.NewInt64Builder(memory.DefaultAllocator)
	defer idb.Release()
	idb.AppendValues([]int64{1, 2}, nil)
	idArr := idb.NewArray()
	defer idArr.Release()
	base := &compositeRecord{index: map[FieldID]int16{100: 0}, recs: []arrow.Array{idArr}}

	readSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64}},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{FieldID: 200, Name: "st", Fields: []*schemapb.FieldSchema{
				{FieldID: 201, Name: "st[a]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, Nullable: true},
				{FieldID: 202, Name: "st[b]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar, Nullable: true},
			}},
		},
	}
	// present = only the top-level id; the whole struct (children 201,202) is absent.
	rr := NewAbsentFieldFillRecordReader(&sliceRecordReader{recs: []Record{base}}, readSchema, map[FieldID]struct{}{100: {}})
	defer rr.Close()
	rec, err := rr.Next()
	require.NoError(t, err)

	require.Equal(t, 2, rec.Column(201).Len())
	require.Equal(t, 2, rec.Column(201).NullN())
	require.Equal(t, 2, rec.Column(202).Len())
	require.Equal(t, 2, rec.Column(202).NullN())
	require.Equal(t, int64(2), rec.Column(100).(*array.Int64).Value(1)) // present passthrough
}

// TestAbsentFieldFillReaderReturnsInnerWhenAllPresent pins the short-circuit: when
// every read-schema field is present there is nothing to fill, so inner is returned
// unwrapped (no per-record overlay allocation).
func TestAbsentFieldFillReaderReturnsInnerWhenAllPresent(t *testing.T) {
	inner := &sliceRecordReader{recs: []Record{&compositeRecord{index: map[FieldID]int16{100: 0}}}}
	readSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
	}}
	rr := NewAbsentFieldFillRecordReader(inner, readSchema, map[FieldID]struct{}{100: {}})
	_, wrapped := rr.(*absentFieldFillRecordReader)
	require.False(t, wrapped, "nothing absent -> inner must be returned unwrapped")
	require.Same(t, inner, rr)
}
