// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package importv3

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	importcommon "github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/internal/util/importutilv2/numpy"
	"github.com/milvus-io/milvus/internal/util/importutilv2/parquet"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// TestPreImportV3TaskProducesImportV3FileStats pins the count-only preimport
// worker output: it answers with the slim V3 stats type (file id + counts),
// never the V2 ImportFileStats the old regrouping path used.
func TestPreImportV3TaskProducesImportV3FileStats(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
		{
			FieldID: 101, Name: "note", DataType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "128"}},
		},
	}}
	content := "pk,note\n1,a\n2,b\n3,c\n"

	cm := mocks.NewChunkManager(t)
	cm.EXPECT().Reader(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, _ string) (storage.FileReader, error) {
		return importcommon.NewMockReader(content), nil
	})
	cm.EXPECT().Size(mock.Anything, mock.Anything).Return(int64(len(content)), nil)

	req := &datapb.PreImportRequest{
		JobID: 1, TaskID: 2, CollectionID: 3,
		Schema:        schema,
		ImportFiles:   []*internalpb.ImportFile{{Id: 7, Paths: []string{"data.csv"}}},
		StorageConfig: &indexpb.StorageConfig{},
	}
	task := NewPreImportTask(req, cm)

	res, err := task.Execute(context.Background())
	require.NoError(t, err)

	stats, ok := res.([]*datapb.ImportV3FileStats)
	require.True(t, ok, "count-only preimport must answer with []*datapb.ImportV3FileStats, got %T", res)
	require.Len(t, stats, 1)
	require.Equal(t, int64(7), stats[0].GetFileId())
	require.Equal(t, int64(3), stats[0].GetTotalRows())
	require.Equal(t, int64(len(content)), stats[0].GetFileSize())
}

// TestPreImportV3TaskSizeOnlyBackup pins the backup preimport mode: it expands
// each source's insert+delta object list and sums the object bytes, reading no
// content, and reports zero rows with the summed size in file_size.
func TestPreImportV3TaskSizeOnlyBackup(t *testing.T) {
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().WalkWithPrefix(mock.Anything, "insert", mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ string, _ bool, walkFunc storage.ChunkObjectWalkFunc) error {
			_ = walkFunc(&storage.ChunkObjectInfo{FilePath: "insert/100/log1"})
			_ = walkFunc(&storage.ChunkObjectInfo{FilePath: "insert/100/log2"})
			return nil
		}).Once()
	cm.EXPECT().WalkWithPrefix(mock.Anything, "delta", mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ string, _ bool, walkFunc storage.ChunkObjectWalkFunc) error {
			_ = walkFunc(&storage.ChunkObjectInfo{FilePath: "delta/log1"})
			return nil
		}).Once()
	sizes := map[string]int64{"insert/100/log1": 10, "insert/100/log2": 20, "delta/log1": 5}
	cm.EXPECT().Size(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, p string) (int64, error) {
			return sizes[p], nil
		})

	req := &datapb.PreImportRequest{
		JobID: 1, TaskID: 2, CollectionID: 3,
		ImportFiles: []*internalpb.ImportFile{{Id: 7, Paths: []string{"insert", "delta"}}},
		Options:     []*commonpb.KeyValuePair{{Key: importutilv2.BackupFlag, Value: "true"}},
	}
	task := NewPreImportTask(req, cm)

	res, err := task.Execute(context.Background())
	require.NoError(t, err)

	stats, ok := res.([]*datapb.ImportV3FileStats)
	require.True(t, ok, "the size-only mode must answer with []*datapb.ImportV3FileStats, got %T", res)
	require.Len(t, stats, 1)
	require.Equal(t, int64(7), stats[0].GetFileId())
	require.Equal(t, int64(35), stats[0].GetFileSize())
	require.Equal(t, int64(0), stats[0].GetTotalRows())
	require.Equal(t, int64(0), stats[0].GetTotalMemorySize())
}

// TestPreImportCheapRowCountDispatchesByFileType pins the metadata fast-path
// dispatch: parquet reads its footer, numpy reads its header, and every other
// type (CSV/JSON) returns cheap=false so the caller falls back to the row scan.
// An unknown extension is a hard error from GetFileType, not a silent scan.
func TestPreImportCheapRowCountDispatchesByFileType(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	task := &preImportTask{req: &datapb.PreImportRequest{
		JobID: 1, TaskID: 2, CollectionID: 3, Schema: schema,
	}}

	var parquetCalls, numpyCalls int
	parquetMock := mockey.Mock(parquet.NumRowsAndBytes).To(
		func(context.Context, storage.ChunkManager, string) (int64, int64, int64, error) {
			parquetCalls++
			return 11, 22, 33, nil
		}).Build()
	defer parquetMock.UnPatch()
	numpyMock := mockey.Mock(numpy.NumRowsAndBytes).To(
		func(context.Context, storage.ChunkManager, *schemapb.CollectionSchema, []string) (int64, int64, int64, error) {
			numpyCalls++
			return 44, 55, 66, nil
		}).Build()
	defer numpyMock.UnPatch()

	t.Run("parquet reads the footer", func(t *testing.T) {
		rows, bytes, size, cheap, err := task.simpleRowCount(context.Background(),
			&internalpb.ImportFile{Paths: []string{"a.parquet"}})
		require.NoError(t, err)
		require.True(t, cheap)
		require.Equal(t, int64(11), rows)
		require.Equal(t, int64(22), bytes)
		require.Equal(t, int64(33), size)
	})

	t.Run("numpy reads the header", func(t *testing.T) {
		rows, bytes, size, cheap, err := task.simpleRowCount(context.Background(),
			&internalpb.ImportFile{Id: 9, Paths: []string{"a.npy"}})
		require.NoError(t, err)
		require.True(t, cheap)
		require.Equal(t, int64(44), rows)
		require.Equal(t, int64(55), bytes)
		require.Equal(t, int64(66), size)
	})

	t.Run("csv and json take the scan path", func(t *testing.T) {
		for _, path := range []string{"a.csv", "a.jsonl"} {
			rows, bytes, size, cheap, err := task.simpleRowCount(context.Background(),
				&internalpb.ImportFile{Paths: []string{path}})
			require.NoError(t, err)
			require.False(t, cheap, "%s has no metadata fast path", path)
			require.Zero(t, rows)
			require.Zero(t, bytes)
			require.Zero(t, size)
		}
	})

	t.Run("unknown extension is rejected", func(t *testing.T) {
		_, _, _, cheap, err := task.simpleRowCount(context.Background(),
			&internalpb.ImportFile{Paths: []string{"a.txt"}})
		require.Error(t, err)
		require.False(t, cheap)
	})

	require.Equal(t, 1, parquetCalls, "only the parquet case may touch the parquet footer")
	require.Equal(t, 1, numpyCalls, "only the numpy case may touch the numpy header")
}
