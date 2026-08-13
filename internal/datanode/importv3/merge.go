// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

// Package importv3 contains the DataNode-side execution primitives for the
// V3 import path.  The package intentionally keeps the execution contracts
// independent of datapb while the wire messages are being rolled out: the
// coordinator can adapt the generated request/result messages at the edge.
package importv3

import (
	"context"
	"io"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Source is an immutable, already sorted fragment or intermediate run.
// Open must return a fresh reader on every call.  A source is never split or
// re-ordered by the hierarchical merge executor.
type Source struct {
	ID   string
	Rows int64
	Open func(context.Context) (storage.RecordReader, error)
}

// IntermediateWriterFactory creates a writer for one contiguous merge group.
// Commit is manifest-last: it must only publish a Source after writer.Close
// has succeeded.  This gives a caller a precise place to add a future digest
// validation hook without forcing SHA-256 into the first implementation.
type IntermediateWriterFactory func(
	ctx context.Context,
	round int,
	group int,
	inputs []Source,
) (writer storage.RecordWriter, commit func(rows int64) (Source, error), err error)

// FinalWriterFactory is lazy: Import V3 must not create or close a formal
// segment writer until the final predicate keeps at least one row.
type FinalWriterFactory func(context.Context) (storage.RecordWriter, error)

// MergeExecutor performs the strict one-head merge described by Import V3.
// It uses storage.MergeSort (one heap head per reader) and only performs
// contiguous, bounded fan-in intermediate rounds.  It never applies a
// predicate or transform to an intermediate run; those belong to final merge.
type MergeExecutor struct {
	FanIn        int
	BatchSize    uint64
	Schema       *schemapb.CollectionSchema
	SortFields   []int64
	Predicate    func(storage.Record, int, int) bool
	Intermediate IntermediateWriterFactory
}

func (e *MergeExecutor) validate() error {
	if e == nil {
		return merr.WrapErrImportSysFailedMsg("nil import merge executor")
	}
	if e.FanIn < 2 || e.FanIn > 1024 {
		return merr.WrapErrImportSysFailedMsg("invalid import merge fan-in %d, expected [2,1024]", e.FanIn)
	}
	if e.BatchSize == 0 {
		return merr.WrapErrImportSysFailedMsg("import merge batch size must be positive")
	}
	if e.Schema == nil {
		return merr.WrapErrImportSysFailedMsg("import merge schema is nil")
	}
	return nil
}

// Execute merges sources into finalWriter.  The final predicate is evaluated
// exactly once per input row by storage.MergeSort.  An empty source list is a
// valid empty segment plan and does not invoke the writer.
func (e *MergeExecutor) Execute(
	ctx context.Context,
	sources []Source,
	finalWriterFactory FinalWriterFactory,
) (int64, error) {
	if err := e.validate(); err != nil {
		return 0, err
	}
	if ctx == nil {
		return 0, merr.WrapErrImportSysFailedMsg("import merge context is nil")
	}
	if finalWriterFactory == nil {
		return 0, merr.WrapErrImportSysFailedMsg("import merge final writer factory is nil")
	}
	if err := validateSources(sources); err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	inputs := append([]Source(nil), sources...)
	round := 0
	for len(inputs) > e.FanIn {
		next := make([]Source, 0, (len(inputs)+e.FanIn-1)/e.FanIn)
		for groupStart, groupIndex := 0, 0; groupStart < len(inputs); groupIndex++ {
			groupEnd := groupStart + e.FanIn
			if groupEnd > len(inputs) {
				groupEnd = len(inputs)
			}
			group := append([]Source(nil), inputs[groupStart:groupEnd]...)
			groupStart = groupEnd
			if len(group) == 1 {
				next = append(next, group[0])
				continue
			}
			source, err := e.mergeIntermediate(ctx, round, groupIndex, group)
			if err != nil {
				return 0, err
			}
			next = append(next, source)
		}
		inputs = next
		round++
	}

	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if len(inputs) == 0 {
		return 0, nil
	}
	readers, err := openSources(ctx, inputs)
	if err != nil {
		return 0, err
	}
	defer closeReaders(readers)
	predicate := e.Predicate
	if predicate == nil {
		predicate = func(storage.Record, int, int) bool { return true }
	}
	lazyWriter := &lazyRecordWriter{ctx: ctx, factory: finalWriterFactory}
	rows, err := storage.MergeSort(e.BatchSize, e.Schema, readers, lazyWriter,
		predicate, e.SortFields)
	if err != nil {
		_ = lazyWriter.Close()
		return 0, err
	}
	if err := lazyWriter.Close(); err != nil {
		return 0, err
	}
	return int64(rows), nil
}

type lazyRecordWriter struct {
	ctx     context.Context
	factory FinalWriterFactory
	writer  storage.RecordWriter
}

func (w *lazyRecordWriter) Write(record storage.Record) error {
	if w.writer == nil {
		writer, err := w.factory(w.ctx)
		if err != nil {
			return err
		}
		if writer == nil {
			return merr.WrapErrImportSysFailedMsg("final writer factory returned nil writer")
		}
		w.writer = writer
	}
	return w.writer.Write(record)
}

func (w *lazyRecordWriter) GetWrittenUncompressed() uint64 {
	if w.writer == nil {
		return 0
	}
	return w.writer.GetWrittenUncompressed()
}

func (w *lazyRecordWriter) Close() error {
	if w.writer == nil {
		return nil
	}
	return w.writer.Close()
}

func (e *MergeExecutor) mergeIntermediate(
	ctx context.Context,
	round, group int,
	inputs []Source,
) (Source, error) {
	if e.Intermediate == nil {
		return Source{}, merr.WrapErrImportSysFailedMsg("import merge intermediate writer factory is nil")
	}
	writer, commit, err := e.Intermediate(ctx, round, group, inputs)
	if err != nil {
		return Source{}, err
	}
	if writer == nil || commit == nil {
		return Source{}, merr.WrapErrImportSysFailedMsg(
			"intermediate writer factory returned nil writer or commit callback")
	}
	readers, err := openSources(ctx, inputs)
	if err != nil {
		_ = writer.Close()
		return Source{}, err
	}
	defer closeReaders(readers)
	rows, mergeErr := storage.MergeSort(e.BatchSize, e.Schema, readers, writer,
		func(storage.Record, int, int) bool { return true }, e.SortFields)
	if mergeErr != nil {
		_ = writer.Close()
		return Source{}, mergeErr
	}
	if int64(rows) != sumRows(inputs) {
		_ = writer.Close()
		return Source{}, merr.WrapErrDataIntegrityMsg(
			"intermediate merge row count mismatch: expected=%d actual=%d", sumRows(inputs), rows)
	}
	if err := ctx.Err(); err != nil {
		_ = writer.Close()
		return Source{}, err
	}
	if err := writer.Close(); err != nil {
		return Source{}, err
	}
	source, err := commit(int64(rows))
	if err != nil {
		return Source{}, err
	}
	if err := validateSource(source); err != nil {
		return Source{}, err
	}
	if source.Rows != int64(rows) {
		return Source{}, merr.WrapErrDataIntegrityMsg(
			"intermediate source rows mismatch: committed=%d actual=%d", source.Rows, rows)
	}
	return source, nil
}

func validateSources(sources []Source) error {
	for _, source := range sources {
		if err := validateSource(source); err != nil {
			return err
		}
	}
	return nil
}

func validateSource(source Source) error {
	if source.ID == "" {
		return merr.WrapErrImportSysFailedMsg("import merge source ID is empty")
	}
	if source.Rows <= 0 {
		return merr.WrapErrDataIntegrityMsg("import merge source %q has invalid rows=%d", source.ID, source.Rows)
	}
	if source.Open == nil {
		return merr.WrapErrImportSysFailedMsg("import merge source %q has nil opener", source.ID)
	}
	return nil
}

func openSources(ctx context.Context, sources []Source) ([]storage.RecordReader, error) {
	readers := make([]storage.RecordReader, 0, len(sources))
	for _, source := range sources {
		if err := ctx.Err(); err != nil {
			closeReaders(readers)
			return nil, err
		}
		reader, err := source.Open(ctx)
		if err != nil {
			closeReaders(readers)
			return nil, merr.Wrapf(err, "open import merge source %q", source.ID)
		}
		if reader == nil {
			closeReaders(readers)
			return nil, merr.WrapErrDataIntegrityMsg("source %q returned nil reader", source.ID)
		}
		readers = append(readers, reader)
	}
	return readers, nil
}

func closeReaders(readers []storage.RecordReader) {
	for _, reader := range readers {
		if reader != nil {
			_ = reader.Close()
		}
	}
}

func sumRows(sources []Source) int64 {
	var rows int64
	for _, source := range sources {
		rows += source.Rows
	}
	return rows
}

// Compile-time assertion documenting the EOF contract expected by this
// package.  It also prevents accidental import of a reader API that returns
// a different terminal value.
var _ = io.EOF
