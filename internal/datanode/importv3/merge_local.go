// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0.

package importv3

import (
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The ImportTaskV3 merge re-reads every intermediate product on the next round,
// so the intermediates stay on the node's local disk instead of the object
// store: the doubled merge IO of a bounded-fan-in k-way merge then costs local
// IO, and the object store sees each fragment exactly once.
func importV3MergeLocalRunDir(jobID, taskID, runID int64) string {
	return filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), importV3SpillRootDir,
		strconv.FormatInt(jobID, 10), strconv.FormatInt(taskID, 10), strconv.FormatInt(runID, 10))
}

func importV3MergeLocalDir(jobID, taskID, runID int64, segmentIndex int) string {
	return filepath.Join(importV3MergeLocalRunDir(jobID, taskID, runID), "merge", strconv.Itoa(segmentIndex))
}

// localFragmentWriter writes one merge intermediate as a single Arrow IPC file
// on local disk. It satisfies the same surface the object-store packed writer
// exposes to the merge factory, so the hierarchical merge is backend-agnostic.
type localFragmentWriter struct {
	file   *os.File
	iw     *ipc.Writer
	schema *arrow.Schema
	fields []*schemapb.FieldSchema
	rowNum int64
}

var _ importV3PackedRecordWriter = (*localFragmentWriter)(nil)

func newLocalFragmentWriter(path string, schema *schemapb.CollectionSchema) (*localFragmentWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, merr.Wrapf(err, "create import v3 merge intermediate directory")
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, merr.Wrapf(err, "create import v3 merge intermediate file")
	}
	arrowSchema, err := storage.ConvertToArrowSchema(schema, false)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &localFragmentWriter{
		file:   file,
		iw:     ipc.NewWriter(file, ipc.WithSchema(arrowSchema)),
		schema: arrowSchema,
		fields: typeutil.GetAllFieldSchemas(schema),
	}, nil
}

func (w *localFragmentWriter) Write(r storage.Record) error {
	arrays := make([]arrow.Array, len(w.fields))
	for i, field := range w.fields {
		arrays[i] = r.Column(field.GetFieldID())
	}
	rec := array.NewRecord(w.schema, arrays, int64(r.Len()))
	defer rec.Release()
	w.rowNum += int64(r.Len())
	if err := w.iw.Write(rec); err != nil {
		return merr.Wrapf(err, "write import v3 merge intermediate")
	}
	return nil
}

func (w *localFragmentWriter) GetWrittenUncompressed() uint64 { return 0 }

func (w *localFragmentWriter) GetWrittenRowNum() int64 { return w.rowNum }

func (w *localFragmentWriter) GetWrittenPaths(typeutil.UniqueID) string { return "" }

func (w *localFragmentWriter) Close() error {
	if w.iw != nil {
		err := w.iw.Close()
		w.iw = nil
		if err != nil {
			_ = w.file.Close()
			w.file = nil
			return err
		}
	}
	if w.file != nil {
		err := w.file.Close()
		w.file = nil
		return err
	}
	return nil
}

// localFragmentReader replays one local IPC intermediate as a storage.RecordReader
// compatible with storage.MergeSort.
type localFragmentReader struct {
	file      *os.File
	reader    *ipc.Reader
	field2Col map[storage.FieldID]int
}

var _ storage.RecordReader = (*localFragmentReader)(nil)

func newLocalFragmentReader(path string, schema *schemapb.CollectionSchema) (*localFragmentReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, merr.Wrapf(err, "open import v3 merge intermediate")
	}
	reader, err := ipc.NewReader(file)
	if err != nil {
		_ = file.Close()
		return nil, merr.Wrapf(err, "read import v3 merge intermediate")
	}
	field2Col := make(map[storage.FieldID]int)
	for i, field := range typeutil.GetAllFieldSchemas(schema) {
		field2Col[field.GetFieldID()] = i
	}
	return &localFragmentReader{file: file, reader: reader, field2Col: field2Col}, nil
}

func (r *localFragmentReader) Next() (storage.Record, error) {
	if !r.reader.Next() {
		if err := r.reader.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	return storage.NewSimpleArrowRecord(r.reader.Record(), r.field2Col), nil
}

func (r *localFragmentReader) Close() error {
	if r == nil {
		return nil
	}
	r.reader.Release()
	if r.file != nil {
		err := r.file.Close()
		r.file = nil
		return err
	}
	return nil
}
