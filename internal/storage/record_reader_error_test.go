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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/compress"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCompositeBinlogRecordReaderTerminalError(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		wantEOF bool
	}{
		{name: "normal EOF with nil error", wantEOF: true},
		{name: "normal EOF with EOF error", err: io.EOF, wantEOF: true},
		{name: "normal EOF with wrapped EOF error", err: fmt.Errorf("field exhausted: %w", io.EOF), wantEOF: true},
		{name: "decode error", err: io.ErrUnexpectedEOF},
		{name: "wrapped decode error", err: fmt.Errorf("decode failed: %w", io.ErrUnexpectedEOF)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			composite := &CompositeBinlogRecordReader{
				fields: map[FieldID]*schemapb.FieldSchema{100: {FieldID: 100, DataType: schemapb.DataType_Int64}},
				index:  map[FieldID]int16{100: 0},
				rrs:    []array.RecordReader{&erroringRecordReader{err: tc.err}},
			}
			nextChunkCalls := 0
			reader := &IterativeRecordReader{
				cur: composite,
				iterate: func() (RecordReader, error) {
					nextChunkCalls++
					return nil, io.EOF
				},
			}
			defer reader.Close()

			record, err := reader.Next()
			require.Nil(t, record)
			if tc.wantEOF {
				require.Same(t, io.EOF, err)
				require.Equal(t, 1, nextChunkCalls)
			} else {
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
				require.ErrorIs(t, err, tc.err)
				require.ErrorContains(t, err, "read V1 insert binlog field 100")
				require.Zero(t, nextChunkCalls)
			}
		})
	}
}

func TestCompositeBinlogRecordReaderFieldAlignment(t *testing.T) {
	type step struct {
		rows int // A negative row count means Next returns false.
		err  error
	}
	for _, tc := range []struct {
		name     string
		steps    []step
		wantRows int
		wantErr  error
	}{
		{name: "aligned records with absent field", steps: []step{{rows: 2}, {rows: 2}}, wantRows: 2},
		{name: "aligned empty records", steps: []step{{rows: 0}, {rows: 0}}},
		{name: "all readers end normally", steps: []step{{rows: -1}, {rows: -1, err: io.EOF}}, wantErr: io.EOF},
		{name: "all readers end with wrapped EOF", steps: []step{{rows: -1, err: fmt.Errorf("first field: %w", io.EOF)}, {rows: -1, err: fmt.Errorf("second field: %w", io.EOF)}}, wantErr: io.EOF},
		{name: "EOF before records", steps: []step{{rows: -1}, {rows: 2}}, wantErr: merr.ErrDataIntegrity},
		{name: "wrapped EOF before records", steps: []step{{rows: -1, err: fmt.Errorf("field exhausted: %w", io.EOF)}, {rows: 2}}, wantErr: merr.ErrDataIntegrity},
		{name: "records before EOF", steps: []step{{rows: 2}, {rows: -1, err: io.EOF}}, wantErr: merr.ErrDataIntegrity},
		{name: "records before wrapped EOF", steps: []step{{rows: 2}, {rows: -1, err: fmt.Errorf("field exhausted: %w", io.EOF)}}, wantErr: merr.ErrDataIntegrity},
		{name: "EOF before empty record", steps: []step{{rows: -1}, {rows: 0}}, wantErr: merr.ErrDataIntegrity},
		{name: "EOF before read error", steps: []step{{rows: -1, err: io.EOF}, {rows: -1, err: io.ErrUnexpectedEOF}}, wantErr: io.ErrUnexpectedEOF},
		{name: "wrapped EOF before read error", steps: []step{{rows: -1, err: fmt.Errorf("field exhausted: %w", io.EOF)}, {rows: -1, err: io.ErrUnexpectedEOF}}, wantErr: io.ErrUnexpectedEOF},
		{name: "records before read error", steps: []step{{rows: 2}, {rows: -1, err: io.ErrUnexpectedEOF}}, wantErr: io.ErrUnexpectedEOF},
		{name: "EOF mismatch before read error", steps: []step{{rows: -1}, {rows: 2}, {rows: -1, err: io.ErrUnexpectedEOF}}, wantErr: io.ErrUnexpectedEOF},
		{name: "different batch row counts", steps: []step{{rows: 2}, {rows: 1}}, wantErr: merr.ErrServiceInternal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
			t.Cleanup(func() { allocator.AssertSize(t, 0) })
			schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
			records := make([]arrow.Record, len(tc.steps))
			for i, s := range tc.steps {
				if s.rows < 0 {
					continue
				}
				builder := array.NewInt64Builder(allocator)
				for j := 0; j < s.rows; j++ {
					builder.Append(int64(j))
				}
				column := builder.NewArray()
				builder.Release()
				record := array.NewRecord(schema, []arrow.Array{column}, int64(s.rows))
				column.Release()
				t.Cleanup(record.Release)
				records[i] = record
			}

			// Assign outcomes by visitation order to exercise EOF-before-error
			// deterministically, regardless of the fields map's iteration order.
			nextCalls := 0
			next := func() (arrow.Record, error) {
				require.Less(t, nextCalls, len(tc.steps))
				i := nextCalls
				nextCalls++
				return records[i], tc.steps[i].err
			}
			reader := &CompositeBinlogRecordReader{
				fields: make(map[FieldID]*schemapb.FieldSchema),
				index:  make(map[FieldID]int16),
				rrs:    make([]array.RecordReader, len(tc.steps)+1),
			}
			for i := range tc.steps {
				fieldID := FieldID(100 + i)
				reader.fields[fieldID] = &schemapb.FieldSchema{FieldID: fieldID, DataType: schemapb.DataType_Int64}
				reader.index[fieldID] = int16(i)
				reader.rrs[i] = &compositeStepRecordReader{next: next}
			}
			// Missing fields must neither count as EOF nor interfere with alignment.
			reader.fields[200] = &schemapb.FieldSchema{FieldID: 200, DataType: schemapb.DataType_Int64, Nullable: true}
			reader.index[200] = int16(len(tc.steps))
			defer reader.Close()

			record, err := reader.Next()
			require.Equal(t, len(tc.steps), nextCalls)
			if tc.wantErr != nil {
				require.Nil(t, record)
				require.ErrorIs(t, err, tc.wantErr)
				switch tc.wantErr {
				case io.EOF:
					require.Same(t, tc.wantErr, err)
				case io.ErrUnexpectedEOF:
					require.ErrorIs(t, err, merr.ErrDataIntegrity)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, record)
				require.Equal(t, tc.wantRows, record.Len())
				require.Equal(t, tc.wantRows, record.Column(200).NullN())
			}
		})
	}
}

// The test owns the records; the composite reader borrows them and retains columns.
type compositeStepRecordReader struct {
	erroringRecordReader
	next   func() (arrow.Record, error)
	record arrow.Record
}

func (r *compositeStepRecordReader) Next() bool {
	r.record, r.err = r.next()
	return r.record != nil
}

func (r *compositeStepRecordReader) Record() arrow.Record { return r.record }

func TestCompositeBinlogRecordReaderLateParquetError(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		name := "normal EOF advances to next chunk"
		if corrupt {
			name = "decode error does not skip to next chunk"
		}
		t.Run(name, func(t *testing.T) {
			payload := makeRecordReaderErrorPayload(t, corrupt)
			payloadReader, err := NewPayloadReader(schemapb.DataType_VarChar, payload, false)
			require.NoError(t, err, "the Parquet footer must remain readable")
			defer payloadReader.Close()
			arrowReader, err := payloadReader.GetArrowRecordReader()
			require.NoError(t, err, "the failure must occur during iteration, not initialization")
			composite := &CompositeBinlogRecordReader{
				fields: map[FieldID]*schemapb.FieldSchema{100: {FieldID: 100, DataType: schemapb.DataType_VarChar}},
				index:  map[FieldID]int16{100: 0},
				rrs:    []array.RecordReader{arrowReader},
			}
			nextChunkCalls := 0
			reader := &IterativeRecordReader{
				cur: composite,
				iterate: func() (RecordReader, error) {
					nextChunkCalls++
					return nil, io.EOF
				},
			}
			defer reader.Close()

			record, err := reader.Next()
			require.NoError(t, err)
			require.Equal(t, 1024, record.Len())
			require.Equal(t, "first-group", record.Column(100).(*array.String).Value(0))

			if corrupt {
				record, err = reader.Next()
				require.Nil(t, record)
				require.Error(t, err)
				require.NotErrorIs(t, err, io.EOF)
				require.ErrorContains(t, err, "parquet: invalid BYTE_ARRAY value")
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
				require.ErrorIs(t, err, arrowReader.Err(), "preserve the original decoder error chain")
				require.ErrorContains(t, err, "read V1 insert binlog field 100")
				require.Zero(t, nextChunkCalls, "a failed chunk must not be treated as exhausted")
				return
			}

			for _, expected := range []string{"broken-group", "last-group"} {
				record, err = reader.Next()
				require.NoError(t, err)
				require.Equal(t, 1024, record.Len())
				require.Equal(t, expected, record.Column(100).(*array.String).Value(0))
			}
			record, err = reader.Next()
			require.Nil(t, record)
			require.Equal(t, io.EOF, err)
			require.Equal(t, 1, nextChunkCalls)
		})
	}
}

func makeRecordReaderErrorPayload(t *testing.T, corrupt bool) []byte {
	t.Helper()
	var buf bytes.Buffer
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil)
	writer, err := pqarrow.NewFileWriter(schema, &buf, parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(false),
		parquet.WithCompression(compress.Codecs.Uncompressed),
	), pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	for _, value := range []string{"first-group", "broken-group", "last-group"} {
		builder := array.NewStringBuilder(memory.DefaultAllocator)
		for i := 0; i < 1024; i++ {
			builder.Append(value)
		}
		column := builder.NewArray()
		builder.Release()
		record := array.NewRecord(schema, []arrow.Array{column}, 1024)
		column.Release()
		err = writer.Write(record)
		record.Release()
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())
	payload := buf.Bytes()
	if corrupt {
		// PLAIN BYTE_ARRAY stores a four-byte length immediately before the value;
		// match both to avoid the same string in page statistics, then invalidate
		// the second row group's first value without changing the footer.
		encoded := binary.LittleEndian.AppendUint32(nil, uint32(len("broken-group")))
		encoded = append(encoded, "broken-group"...)
		offset := bytes.Index(payload, encoded)
		require.NotEqual(t, -1, offset)
		binary.LittleEndian.PutUint32(payload[offset:offset+4], ^uint32(0))
	}
	return payload
}
