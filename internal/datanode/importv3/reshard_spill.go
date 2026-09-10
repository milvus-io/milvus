// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

import (
	"io"
	"os"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// SpillWriter serializes normalized InsertData batches into one immutable local
// Arrow IPC stream file. Each Write appends one record; the file must be closed
// before it can be read back. This is Import V3's bounded-memory escape hatch:
// bucket tails that would exceed the task-local working set are flushed here
// instead of being kept in memory.
type SpillWriter struct {
	file   *os.File
	writer *ipc.Writer
	schema *schemapb.CollectionSchema
	arrow  *arrow.Schema
}

// NewSpillWriter creates a spill file at path and prepares it to serialize
// batches described by schema.
func NewSpillWriter(path string, schema *schemapb.CollectionSchema) (*SpillWriter, error) {
	arrowSchema, err := storage.ConvertToArrowSchema(schema, false)
	if err != nil {
		return nil, err
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, merr.Wrapf(err, "create spill file %s", path)
	}
	return &SpillWriter{
		file:   file,
		writer: ipc.NewWriter(file, ipc.WithSchema(arrowSchema)),
		schema: schema,
		arrow:  arrowSchema,
	}, nil
}

// Write appends one normalized batch as an Arrow record. Empty batches are
// skipped and do not create a record in the stream.
func (w *SpillWriter) Write(data *storage.InsertData) error {
	if data == nil || data.GetRowNum() == 0 {
		return nil
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, w.arrow)
	defer builder.Release()
	if err := storage.BuildRecord(builder, data, w.schema); err != nil {
		return err
	}
	record := builder.NewRecord()
	defer record.Release()
	return w.writer.Write(record)
}

// Close finalizes the IPC stream and closes the underlying file. The spill file
// becomes immutable and read-only after this returns nil.
func (w *SpillWriter) Close() error {
	if w == nil {
		return nil
	}
	var firstErr error
	if w.writer != nil {
		firstErr = w.writer.Close()
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// NewSpillReader opens an immutable spill file and replays it as a
// storage.RecordReader. The reader is compatible with storage.Sort/MergeSort and
// must be closed by the caller.
func NewSpillReader(path string, schema *schemapb.CollectionSchema) (storage.RecordReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, merr.Wrapf(err, "open spill file %s", path)
	}
	reader, err := ipc.NewReader(file)
	if err != nil {
		_ = file.Close()
		return nil, merr.Wrapf(err, "read spill file %s", path)
	}
	field2Col := make(map[storage.FieldID]int)
	for i, field := range typeutil.GetAllFieldSchemas(schema) {
		field2Col[field.GetFieldID()] = i
	}
	return &spillRecordReader{file: file, reader: reader, field2Col: field2Col}, nil
}

// spillRecordReader replays one Arrow IPC stream file as a storage.RecordReader.
type spillRecordReader struct {
	file      *os.File
	reader    *ipc.Reader
	field2Col map[storage.FieldID]int
}

var _ storage.RecordReader = (*spillRecordReader)(nil)

func (r *spillRecordReader) Next() (storage.Record, error) {
	if !r.reader.Next() {
		if err := r.reader.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	// The record is borrowed until the next Next; storage.Sort retains it.
	return storage.NewSimpleArrowRecord(r.reader.Record(), r.field2Col), nil
}

func (r *spillRecordReader) Close() error {
	if r == nil {
		return nil
	}
	var firstErr error
	if r.reader != nil {
		r.reader.Release()
	}
	if r.file != nil {
		if err := r.file.Close(); err != nil {
			firstErr = err
		}
	}
	return firstErr
}
