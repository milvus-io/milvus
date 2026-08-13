// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type testReader struct {
	records []storage.Record
	index   int
	closed  bool
}

func (r *testReader) Next() (storage.Record, error) {
	if r.index == len(r.records) {
		return nil, io.EOF
	}
	record := r.records[r.index]
	r.index++
	return record, nil
}

func (r *testReader) Close() error {
	if !r.closed {
		for _, record := range r.records {
			record.Release()
		}
		r.closed = true
	}
	return nil
}

type testWriter struct {
	values []int64
}

func (w *testWriter) Write(record storage.Record) error {
	column := record.Column(100).(*array.Int64)
	for i := 0; i < column.Len(); i++ {
		w.values = append(w.values, column.Value(i))
	}
	return nil
}

func (w *testWriter) GetWrittenUncompressed() uint64 { return uint64(len(w.values) * 8) }
func (*testWriter) Close() error                     { return nil }

func testSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
	}}}
}

func testRecord(values ...int64) storage.Record {
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	builder.AppendValues(values, nil)
	column := builder.NewArray()
	builder.Release()
	record := array.NewRecord(arrow.NewSchema([]arrow.Field{{Name: "100", Type: arrow.PrimitiveTypes.Int64}}, nil),
		[]arrow.Array{column}, int64(len(values)))
	column.Release()
	return storage.NewSimpleArrowRecord(record, map[int64]int{100: 0})
}

func sourceFromValues(id string, values ...int64) Source {
	return Source{ID: id, Rows: int64(len(values)), Open: func(context.Context) (storage.RecordReader, error) {
		return &testReader{records: []storage.Record{testRecord(values...)}}, nil
	}}
}

func TestMergeExecutorHierarchicalFanIn(t *testing.T) {
	inputs := []Source{
		sourceFromValues("a", 1, 4),
		sourceFromValues("b", 2, 5),
		sourceFromValues("c", 3, 6),
		sourceFromValues("d", 7, 8),
		sourceFromValues("e", 9, 10),
	}
	intermediateCount := 0
	executor := &MergeExecutor{
		FanIn:      2,
		BatchSize:  1024,
		Schema:     testSchema(),
		SortFields: []int64{100},
		Intermediate: func(_ context.Context, round, group int, sources []Source) (storage.RecordWriter, func(int64) (Source, error), error) {
			intermediateCount++
			writer := &testWriter{}
			return writer, func(rows int64) (Source, error) {
				values := append([]int64(nil), writer.values...)
				source := sourceFromValues(fmt.Sprintf("r%d-g%d", round, group), values...)
				require.Equal(t, rows, source.Rows)
				return source, nil
			}, nil
		},
	}
	finalWriter := &testWriter{}
	rows, err := executor.Execute(context.Background(), inputs, func(context.Context) (storage.RecordWriter, error) {
		return finalWriter, nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(10), rows)
	require.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, finalWriter.values)
	require.Equal(t, 3, intermediateCount)
}

func TestMergeExecutorRejectsInvalidFanIn(t *testing.T) {
	executor := &MergeExecutor{FanIn: 1, BatchSize: 1, Schema: testSchema(), Intermediate: func(context.Context, int, int, []Source) (storage.RecordWriter, func(int64) (Source, error), error) {
		return &testWriter{}, func(int64) (Source, error) { return Source{}, nil }, nil
	}}
	_, err := executor.Execute(context.Background(), nil, func(context.Context) (storage.RecordWriter, error) {
		return &testWriter{}, nil
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, merr.ErrImportSysFailed))
}

func TestMergeExecutorDoesNotCreateWriterForZeroRows(t *testing.T) {
	executor := &MergeExecutor{
		FanIn:      2,
		BatchSize:  1024,
		Schema:     testSchema(),
		SortFields: []int64{100},
		Predicate:  func(storage.Record, int, int) bool { return false },
	}
	created := false
	rows, err := executor.Execute(context.Background(), []Source{sourceFromValues("a", 1, 2)}, func(context.Context) (storage.RecordWriter, error) {
		created = true
		return &testWriter{}, nil
	})
	require.NoError(t, err)
	require.Zero(t, rows)
	require.False(t, created)
}
