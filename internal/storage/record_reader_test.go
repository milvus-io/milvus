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
	"context"
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

type importFragmentTestReader struct {
	records []Record
	index   int
	closed  bool
}

func (r *importFragmentTestReader) Next() (Record, error) {
	if r.index >= len(r.records) {
		return nil, io.EOF
	}
	record := r.records[r.index]
	r.index++
	return record, nil
}

func (r *importFragmentTestReader) Close() error {
	r.closed = true
	return nil
}

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

func TestImportFragmentRecordReaderChecksExactRows(t *testing.T) {
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	builder.AppendValues([]int64{1, 2}, nil)
	column := builder.NewArray()
	builder.Release()
	record := array.NewRecord(arrow.NewSchema([]arrow.Field{{Name: "100", Type: arrow.PrimitiveTypes.Int64}}, nil),
		[]arrow.Array{column}, 2)
	column.Release()
	defer record.Release()

	inner := &importFragmentTestReader{records: []Record{NewSimpleArrowRecord(record, map[int64]int{100: 0})}}
	reader := &importFragmentRecordReader{ctx: context.Background(), reader: inner, expectedRows: 3}
	got, err := reader.Next()
	require.NoError(t, err)
	require.Equal(t, 2, got.Len())
	_, err = reader.Next()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "row count mismatch")
	require.NoError(t, reader.Close())
	require.True(t, inner.closed)
}

func TestImportFragmentRecordReaderHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	inner := &importFragmentTestReader{}
	reader := &importFragmentRecordReader{ctx: ctx, reader: inner, expectedRows: 1}
	_, err := reader.Next()
	assert.ErrorIs(t, err, context.Canceled)
}

func TestNewInsertDataRecordReader(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
	}}}
	data, err := NewInsertData(schema)
	require.NoError(t, err)
	require.NoError(t, data.Data[100].AppendDataRows([]int64{3, 1, 2}))

	reader, err := NewInsertDataRecordReader(data, schema)
	require.NoError(t, err)
	record, err := reader.Next()
	require.NoError(t, err)
	require.Equal(t, []int64{3, 1, 2}, record.Column(100).(*array.Int64).Int64Values())
	_, err = reader.Next()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, reader.Close())
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
