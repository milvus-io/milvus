// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0.

package importv3

import (
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestLocalFragmentWriterReaderRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "0_0.arrow")
	writer, err := newLocalFragmentWriter(path, testSchema())
	require.NoError(t, err)
	rec := testRecord(3, 1, 2)
	require.NoError(t, writer.Write(rec))
	rec.Release()
	require.Equal(t, int64(3), writer.GetWrittenRowNum())
	require.NoError(t, writer.Close())

	reader, err := newLocalFragmentReader(path, testSchema())
	require.NoError(t, err)
	defer reader.Close()
	got, err := reader.Next()
	require.NoError(t, err)
	column := got.Column(100).(*array.Int64)
	require.Equal(t, 3, column.Len())
	require.Equal(t, []int64{3, 1, 2}, []int64{column.Value(0), column.Value(1), column.Value(2)})
	got.Release()
	_, err = reader.Next()
	require.ErrorIs(t, err, io.EOF)
}

// TestMergeExecutorLocalIntermediates runs the real intermediate factory so the
// hierarchical merge writes its intermediate products to local disk and reads
// them back, covering the D2 path end to end.
func TestMergeExecutorLocalIntermediates(t *testing.T) {
	paramtable.Init()
	pathKey := paramtable.Get().LocalStorageCfg.Path.Key
	paramtable.Get().Save(pathKey, t.TempDir())
	t.Cleanup(func() { paramtable.Get().Reset(pathKey) })

	inputs := []Source{
		sourceFromValues("a", 1, 4),
		sourceFromValues("b", 2, 5),
		sourceFromValues("c", 3, 6),
		sourceFromValues("d", 7, 8),
		sourceFromValues("e", 9, 10),
	}
	executor := &MergeExecutor{
		FanIn:        2,
		BatchSize:    1024,
		Schema:       testSchema(),
		SortFields:   []int64{100},
		Intermediate: newImportV3IntermediateFactory(&datapb.ImportTaskV3Request{JobId: 1, TaskId: 2, RunId: 3}, 0, testSchema()),
	}
	finalWriter := &testWriter{}
	rows, err := executor.Execute(context.Background(), inputs, func(context.Context) (storage.RecordWriter, error) {
		return finalWriter, nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(10), rows)
	require.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, finalWriter.values)
}
