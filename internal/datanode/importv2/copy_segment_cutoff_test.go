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

package importv2

import (
	"context"
	"fmt"
	"io"
	"math"
	"path"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type cutoffFakeRecordReader struct {
	records []storage.Record
	idx     int
}

func (r *cutoffFakeRecordReader) Next() (storage.Record, error) {
	if r.idx >= len(r.records) {
		return nil, io.EOF
	}
	record := r.records[r.idx]
	r.idx++
	return record, nil
}

func (r *cutoffFakeRecordReader) Close() error {
	return nil
}

type cutoffFakeBinlogRecordWriter struct {
	fieldBinlogs map[storage.FieldID]*datapb.FieldBinlog
	statsLog     *datapb.FieldBinlog
	bm25StatsLog map[storage.FieldID]*datapb.FieldBinlog
	manifest     string
	schema       *schemapb.CollectionSchema
}

func (w *cutoffFakeBinlogRecordWriter) Write(storage.Record) error {
	return nil
}

func (w *cutoffFakeBinlogRecordWriter) GetWrittenUncompressed() uint64 {
	return 0
}

func (w *cutoffFakeBinlogRecordWriter) Close() error {
	return nil
}

func (w *cutoffFakeBinlogRecordWriter) GetLogs() (
	map[storage.FieldID]*datapb.FieldBinlog,
	*datapb.FieldBinlog,
	map[storage.FieldID]*datapb.FieldBinlog,
	string,
	[]int64,
) {
	return w.fieldBinlogs, w.statsLog, w.bm25StatsLog, w.manifest, nil
}

func (w *cutoffFakeBinlogRecordWriter) GetRowNum() int64 {
	return 0
}

func (w *cutoffFakeBinlogRecordWriter) FlushChunk() error {
	return nil
}

func (w *cutoffFakeBinlogRecordWriter) GetBufferUncompressed() uint64 {
	return 0
}

func (w *cutoffFakeBinlogRecordWriter) Schema() *schemapb.CollectionSchema {
	return w.schema
}

func TestBuildCutoffTextColumnConfigs_UsesRewriteMode(t *testing.T) {
	paramtable.Init()

	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_Text},
			{FieldID: 102, Name: "varchar", DataType: schemapb.DataType_VarChar},
		},
	}

	configs := buildCutoffTextColumnConfigs(schema, "files/insert_log/444/555")

	require.Len(t, configs, 1)
	assert.Equal(t, int64(101), configs[0].FieldID)
	assert.Equal(t, "files/insert_log/444/555/lobs/101", configs[0].LobBasePath)
	assert.True(t, configs[0].RewriteMode)
	assert.Equal(t, paramtable.Get().DataNodeCfg.TextInlineThreshold.GetAsInt64(), configs[0].InlineThreshold)
	assert.Equal(t, paramtable.Get().DataNodeCfg.TextMaxLobFileBytes.GetAsInt64(), configs[0].MaxLobFileBytes)
	assert.Equal(t, paramtable.Get().DataNodeCfg.TextFlushThresholdBytes.GetAsInt64(), configs[0].FlushThresholdBytes)
}

func TestRewriteV2InsertBinlogsForCutoff_RewritesCrossingBatch(t *testing.T) {
	paramtable.Init()

	const dim = 2
	ctx := context.Background()
	storageConfig := &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    t.TempDir(),
		BucketName:  "a-bucket",
	}
	schemaWithSystemFields := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}},
		},
	}
	userSchema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}},
		},
	}
	insertData := &storage.InsertData{Data: map[storage.FieldID]storage.FieldData{
		common.RowIDField:     &storage.Int64FieldData{Data: []int64{1, 2, 3}},
		common.TimeStampField: &storage.Int64FieldData{Data: []int64{90, 110, 120}},
		100:                   &storage.Int64FieldData{Data: []int64{1, 2, 3}},
		101:                   &storage.FloatVectorFieldData{Data: []float32{0.1, 0.2, 1.1, 1.2, 2.1, 2.2}, Dim: dim},
	}}

	arrowSchema, err := storage.ConvertToArrowSchema(schemaWithSystemFields, true)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	require.NoError(t, storage.BuildRecord(builder, insertData, schemaWithSystemFields))
	rec := storage.NewSimpleArrowRecord(builder.NewRecord(), map[storage.FieldID]int{
		common.RowIDField:     0,
		common.TimeStampField: 1,
		100:                   2,
		101:                   3,
	})
	defer rec.Release()

	writer, err := storage.NewBinlogRecordWriter(
		ctx,
		111,
		222,
		333,
		schemaWithSystemFields,
		allocator.NewLocalAllocator(10, math.MaxInt64),
		1024,
		1000,
		storage.WithVersion(storage.StorageV2),
		storage.WithBufferSize(1024*1024),
		storage.WithMultiPartUploadSize(0),
		storage.WithColumnGroups([]storagecommon.ColumnGroup{
			{GroupID: storagecommon.DefaultShortColumnGroupID, Columns: []int{0, 1, 2}, Fields: []int64{common.RowIDField, common.TimeStampField, 100}},
			{GroupID: 101, Columns: []int{3}, Fields: []int64{101}},
		}),
		storage.WithStorageConfig(storageConfig),
		storage.WithUploader(func(context.Context, map[string][]byte) error { return nil }),
	)
	require.NoError(t, err)
	require.NoError(t, writer.Write(rec))
	require.NoError(t, writer.Close())
	fieldBinlogs, _, _, _, _ := writer.GetLogs()

	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		InsertBinlogs:  storage.SortFieldBinlogs(fieldBinlogs),
		CutoffTs:       100,
		StorageVersion: storage.StorageV2,
	}
	target := &datapb.CopySegmentTarget{CollectionId: 444, PartitionId: 555, SegmentId: 666}
	copyInsert, metaInsert, mappings, copiedFiles, mutated, err := rewriteV2InsertBinlogsForCutoff(
		ctx, source, target, userSchema, storageConfig)
	require.NoError(t, err)
	assert.True(t, mutated)
	assert.Empty(t, copyInsert)
	require.Len(t, metaInsert, 2)
	assert.Len(t, mappings, 2)
	assert.Len(t, copiedFiles, 2)
	for _, field := range metaInsert {
		require.Len(t, field.GetBinlogs(), 1)
		assert.Equal(t, int64(1), field.GetBinlogs()[0].GetEntriesNum())
		sourcePath := field.GetBinlogs()[0].GetLogPath()
		targetPath := mappings[sourcePath]
		require.NotEmpty(t, targetPath)
		field.GetBinlogs()[0].LogPath = targetPath
	}

	rewriteSchema := buildCutoffRewriteSchema(userSchema)
	reader, err := storage.NewBinlogRecordReader(ctx, metaInsert, rewriteSchema,
		storage.WithVersion(storage.StorageV2),
		storage.WithStorageConfig(storageConfig),
		storage.WithCollectionID(target.GetCollectionId()),
		storage.WithCollectionProperties(rewriteSchema.GetProperties()))
	require.NoError(t, err)
	defer reader.Close()
	got, err := reader.Next()
	require.NoError(t, err)
	defer got.Release()
	require.Equal(t, 1, got.Len())
	assert.Equal(t, int64(90), got.Column(common.TimeStampField).(*array.Int64).Value(0))
	assert.Equal(t, int64(1), got.Column(100).(*array.Int64).Value(0))
	_, err = reader.Next()
	assert.Equal(t, io.EOF, err)
}

func TestRewriteManifestSegmentForCutoff_FiltersRows(t *testing.T) {
	paramtable.Init()

	const dim = 2
	ctx := context.Background()
	storageConfig := &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    t.TempDir(),
		BucketName:  "a-bucket",
	}
	schemaWithSystemFields := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}},
		},
	}
	userSchema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}},
		},
	}
	insertData := &storage.InsertData{Data: map[storage.FieldID]storage.FieldData{
		common.RowIDField:     &storage.Int64FieldData{Data: []int64{1, 2}},
		common.TimeStampField: &storage.Int64FieldData{Data: []int64{90, 110}},
		100:                   &storage.Int64FieldData{Data: []int64{1, 2}},
		101:                   &storage.FloatVectorFieldData{Data: []float32{0.1, 0.2, 1.1, 1.2}, Dim: dim},
	}}

	arrowSchema, err := storage.ConvertToArrowSchema(schemaWithSystemFields, true)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	require.NoError(t, storage.BuildRecord(builder, insertData, schemaWithSystemFields))
	rec := storage.NewSimpleArrowRecord(builder.NewRecord(), map[storage.FieldID]int{
		common.RowIDField:     0,
		common.TimeStampField: 1,
		100:                   2,
		101:                   3,
	})
	defer rec.Release()

	writer, err := storage.NewBinlogRecordWriter(
		ctx,
		111,
		222,
		333,
		schemaWithSystemFields,
		allocator.NewLocalAllocator(10, math.MaxInt64),
		1024,
		1000,
		storage.WithVersion(storage.StorageV3),
		storage.WithStorageConfig(storageConfig),
		storage.WithBufferSize(1024*1024),
		storage.WithMultiPartUploadSize(0),
		storage.WithCollectionID(111),
		storage.WithCollectionProperties(schemaWithSystemFields.GetProperties()),
	)
	require.NoError(t, err)
	require.NoError(t, writer.Write(rec))
	require.NoError(t, writer.Close())
	_, _, _, manifestPath, _ := writer.GetLogs()
	require.NotEmpty(t, manifestPath)

	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		StorageVersion: storage.StorageV3,
		ManifestPath:   manifestPath,
		CutoffTs:       100,
	}
	target := &datapb.CopySegmentTarget{CollectionId: 444, PartitionId: 555, SegmentId: 666}

	result, copiedFiles, err := rewriteManifestSegmentForCutoff(ctx, source, target, userSchema, storageConfig)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.GetImportedRows())
	targetBasePath, _, err := packed.UnmarshalManifestPath(result.GetManifestPath())
	require.NoError(t, err)
	assert.Equal(t, path.Join(storageConfig.GetRootPath(), common.SegmentInsertLogPath, "444/555/666"), targetBasePath)
	assert.NotEmpty(t, copiedFiles)

	rewriteSchema := buildCutoffRewriteSchema(userSchema)
	reader, err := storage.NewManifestRecordReader(ctx, result.GetManifestPath(), rewriteSchema,
		storage.WithVersion(storage.StorageV3),
		storage.WithStorageConfig(storageConfig),
		storage.WithBufferSize(packed.DefaultReadBufferSize),
		storage.WithCollectionID(target.GetCollectionId()),
		storage.WithCollectionProperties(rewriteSchema.GetProperties()))
	require.NoError(t, err)
	defer reader.Close()
	got, err := reader.Next()
	require.NoError(t, err)
	defer got.Release()
	require.Equal(t, 1, got.Len())
	assert.Equal(t, int64(90), got.Column(common.TimeStampField).(*array.Int64).Value(0))
	assert.Equal(t, int64(1), got.Column(100).(*array.Int64).Value(0))
	_, err = reader.Next()
	assert.Equal(t, io.EOF, err)
}

func TestRewriteManifestSegmentForCutoff_AddsWriterStatsToManifest(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	storageConfig := &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    t.TempDir(),
		BucketName:  "a-bucket",
	}
	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath("files/insert_log/111/222/333", 1),
		CutoffTs:       100,
	}
	target := &datapb.CopySegmentTarget{CollectionId: 444, PartitionId: 555, SegmentId: 666}
	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_Text},
			{FieldID: 102, Name: "bm25", DataType: schemapb.DataType_SparseFloatVector},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Name:           "bm25",
				Type:           schemapb.FunctionType_BM25,
				InputFieldIds:  []int64{101},
				OutputFieldIds: []int64{102},
			},
		},
	}

	targetBasePath := path.Join(storageConfig.GetRootPath(), common.SegmentInsertLogPath, "444/555/666")
	targetManifest := packed.MarshalManifestPath(targetBasePath, 1)
	statsLog := &datapb.FieldBinlog{
		FieldID: 100,
		Binlogs: []*datapb.Binlog{{
			LogPath:    path.Join(targetBasePath, "_stats/bloom_filter.100/1"),
			MemorySize: 64,
		}},
	}
	bm25StatsLog := &datapb.FieldBinlog{
		FieldID: 102,
		Binlogs: []*datapb.Binlog{{
			LogPath:    path.Join(targetBasePath, "_stats/bm25.102/2"),
			MemorySize: 32,
		}},
	}
	writer := &cutoffFakeBinlogRecordWriter{
		fieldBinlogs: map[storage.FieldID]*datapb.FieldBinlog{
			storagecommon.DefaultShortColumnGroupID: {
				FieldID: storagecommon.DefaultShortColumnGroupID,
				Binlogs: []*datapb.Binlog{{
					LogPath:    path.Join(targetBasePath, "_data/0"),
					EntriesNum: 1,
				}},
			},
		},
		statsLog:     statsLog,
		bm25StatsLog: map[storage.FieldID]*datapb.FieldBinlog{102: bm25StatsLog},
		manifest:     targetManifest,
		schema:       buildCutoffRewriteSchema(schema),
	}

	readerPatch := mockey.Mock(storage.NewManifestRecordReader).Return(&cutoffFakeRecordReader{}, nil).Build()
	defer readerPatch.UnPatch()
	writerPatch := mockey.Mock(storage.NewBinlogRecordWriter).Return(writer, nil).Build()
	defer writerPatch.UnPatch()
	sortPatch := mockey.Mock(storage.Sort).Return(1, &storage.SortTimings{}, nil).Build()
	defer sortPatch.UnPatch()
	deltaPatch := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return(nil, nil).Build()
	defer deltaPatch.UnPatch()

	var gotStats []packed.StatEntry
	addStatsPatch := mockey.Mock(packed.AddStatsToManifest).To(
		func(manifestPath string, _ *indexpb.StorageConfig, stats []packed.StatEntry) (string, error) {
			require.Equal(t, targetManifest, manifestPath)
			gotStats = append(gotStats, stats...)
			return packed.MarshalManifestPath(targetBasePath, 2), nil
		}).Build()
	defer addStatsPatch.UnPatch()

	result, copiedFiles, err := rewriteManifestSegmentForCutoff(ctx, source, target, schema, storageConfig)

	require.NoError(t, err)
	assert.Equal(t, packed.MarshalManifestPath(targetBasePath, 2), result.GetManifestPath())
	require.Len(t, gotStats, 2)
	assert.Equal(t, "bloom_filter.100", gotStats[0].Key)
	assert.Equal(t, statsLog.GetBinlogs()[0].GetLogPath(), gotStats[0].Files[0])
	assert.Equal(t, "bm25.102", gotStats[1].Key)
	assert.Equal(t, bm25StatsLog.GetBinlogs()[0].GetLogPath(), gotStats[1].Files[0])
	assert.Contains(t, copiedFiles, statsLog.GetBinlogs()[0].GetLogPath())
	assert.Contains(t, copiedFiles, bm25StatsLog.GetBinlogs()[0].GetLogPath())
}

func TestWriteAndScanDeltaLog(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(dir))
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}
	deltaPath := fmt.Sprintf("%s/delta_log/444/555/666/900", dir)
	deletes := []cutoffDeleteEntry{
		{pk: storage.NewInt64PrimaryKey(1), ts: 90},
		{pk: storage.NewInt64PrimaryKey(2), ts: 110},
	}

	deltaLog, err := writeDeltaLog(ctx, cm, target, &schemapb.CollectionSchema{Name: "test"}, schemapb.DataType_Int64, 900, deltaPath, deletes, storage.StorageV1, &indexpb.StorageConfig{})
	require.NoError(t, err)
	require.Len(t, deltaLog.GetBinlogs(), 1)
	assert.Equal(t, int64(2), deltaLog.GetBinlogs()[0].GetEntriesNum())

	retained, err := scanDeltaLogPathsForCutoff(
		[]string{deltaPath},
		schemapb.DataType_Int64,
		100,
		storage.WithVersion(storage.StorageV1),
		storage.WithDownloader(cm.MultiRead),
	)
	require.NoError(t, err)
	require.Len(t, retained, 1)
	assert.Equal(t, int64(1), retained[0].pk.GetValue())
	assert.Equal(t, uint64(90), retained[0].ts)
}

func TestCutoffHelpers(t *testing.T) {
	manifestPath := packed.MarshalManifestPath("files/insert_log/1/2/3", 9)
	assert.Equal(t, "files/insert_log/1/2/3/_metadata/manifest-9.avro", manifestPhysicalPath(manifestPath))
	assert.Empty(t, manifestPhysicalPath("not-json"))
	assert.True(t, isManifestControlFile("files/insert_log/1/2/3", "files/insert_log/1/2/3/_metadata/manifest-1.avro"))
	assert.True(t, isManifestControlFile("files/insert_log/1/2/3", "files/insert_log/1/2/3/_delta/99"))
	assert.False(t, isManifestControlFile("files/insert_log/1/2/3", "files/insert_log/1/2/3/_data/part"))

	_, err := findPrimaryKeyField(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64},
	}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "primary key field not found")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "pk", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).Append(120)
	builder.Field(1).(*array.StringBuilder).Append("pk-a")
	rec := storage.NewSimpleArrowRecord(builder.NewRecord(), map[storage.FieldID]int{
		common.TimeStampField: 0,
		100:                   1,
	})
	defer rec.Release()

	visited := false
	err = visitPKTimestampRecord(rec, 100, schemapb.DataType_VarChar, func(pk storage.PrimaryKey, ts uint64) error {
		visited = true
		assert.Equal(t, "pk-a", pk.GetValue())
		assert.Equal(t, uint64(120), ts)
		return nil
	})
	require.NoError(t, err)
	assert.True(t, visited)

	err = visitPKTimestampRecord(rec, 100, schemapb.DataType_Bool, func(storage.PrimaryKey, uint64) error {
		return nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported primary key type")
}

func TestCopySegmentAndIndexFiles_V2WithCutoff(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, DataType: schemapb.DataType_FloatVector},
		},
	}
	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    222,
		SegmentId:      333,
		CutoffTs:       100,
		StorageVersion: storage.StorageV2,
		InsertBinlogs:  []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "files/insert_log/111/222/333/0/11", EntriesNum: 3}}}},
		StatsBinlogs:   []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "files/stats_log/111/222/333/100/21"}}}},
		DeltaBinlogs:   []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "files/delta_log/111/222/333/31"}}}},
		ManifestPath:   "",
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  555,
		SegmentId:    666,
	}
	copySource := proto.Clone(source).(*datapb.CopySegmentSource)
	copySource.DeltaBinlogs = nil
	metaSource := proto.Clone(copySource).(*datapb.CopySegmentSource)
	metaSource.DeltaBinlogs = []*datapb.FieldBinlog{{
		FieldID: 100,
		Binlogs: []*datapb.Binlog{{
			LogPath:    "files/delta_log/111/222/333/31",
			EntriesNum: 1,
		}},
	}}
	mockPrepare := mockey.Mock(prepareNonManifestRestoreCutoff).Return(&restoreCutoffPlan{
		copySource:  copySource,
		metaSource:  metaSource,
		mappings:    map[string]string{"files/delta_log/111/222/333/31": "files/delta_log/444/555/666/31"},
		copiedFiles: []string{"files/delta_log/444/555/666/31"},
	}, nil).Build()
	defer mockPrepare.UnPatch()

	copiedSrcPaths := make([]string, 0)
	mockCopy := mockey.Mock(copyFile).To(func(_ context.Context, _ storage.ChunkManager, src, dst string) error {
		copiedSrcPaths = append(copiedSrcPaths, src)
		return nil
	}).Build()
	defer mockCopy.UnPatch()

	result, copiedFiles, err := CopySegmentAndIndexFiles(
		context.Background(),
		&struct{ storage.ChunkManager }{},
		source,
		target,
		nil,
		CopySegmentFileOptions{
			Schema:        schema,
			StorageConfig: &indexpb.StorageConfig{RootPath: "files"},
		},
	)

	assert.NoError(t, err)
	assert.Equal(t, int64(3), result.GetImportedRows())
	assert.Len(t, result.GetDeltalogs(), 1)
	assert.Equal(t, int64(100), result.GetDeltalogs()[0].GetFieldID())
	assert.Equal(t, int64(1), result.GetDeltalogs()[0].GetBinlogs()[0].GetEntriesNum())
	assert.Contains(t, copiedSrcPaths, "files/insert_log/111/222/333/0/11")
	assert.Contains(t, copiedSrcPaths, "files/stats_log/111/222/333/100/21")
	assert.NotContains(t, copiedSrcPaths, "files/delta_log/111/222/333/31")
	assert.Contains(t, copiedFiles, "files/delta_log/444/555/666/31")
}

func TestCopySegmentAndIndexFiles_L0WithCutoff(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	}
	source := &datapb.CopySegmentSource{
		CollectionId:   111,
		PartitionId:    -1,
		SegmentId:      333,
		CutoffTs:       100,
		DeltaBinlogs:   []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: "files/delta_log/111/-1/333/1"}}}},
		StorageVersion: storage.StorageV3,
	}
	target := &datapb.CopySegmentTarget{
		CollectionId: 444,
		PartitionId:  -1,
		SegmentId:    666,
	}

	t.Run("rewrite retained deletes", func(t *testing.T) {
		copySource := proto.Clone(source).(*datapb.CopySegmentSource)
		copySource.DeltaBinlogs = nil
		metaSource := proto.Clone(copySource).(*datapb.CopySegmentSource)
		metaSource.DeltaBinlogs = []*datapb.FieldBinlog{{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{{
				LogPath:    "files/delta_log/111/-1/333/1",
				EntriesNum: 2,
			}},
		}}
		mockPrepare := mockey.Mock(prepareNonManifestRestoreCutoff).Return(&restoreCutoffPlan{
			copySource:   copySource,
			metaSource:   metaSource,
			mappings:     map[string]string{"files/delta_log/111/-1/333/1": "files/delta_log/444/-1/666/1"},
			copiedFiles:  []string{"files/delta_log/444/-1/666/1"},
			importedRows: 2,
		}, nil).Build()
		defer mockPrepare.UnPatch()

		result, copiedFiles, err := CopySegmentAndIndexFiles(
			context.Background(),
			&struct{ storage.ChunkManager }{},
			source,
			target,
			nil,
			CopySegmentFileOptions{
				Schema:        schema,
				StorageConfig: &indexpb.StorageConfig{RootPath: "files"},
			},
		)

		assert.NoError(t, err)
		assert.Equal(t, int64(2), result.GetImportedRows())
		assert.Len(t, result.GetDeltalogs(), 1)
		assert.Equal(t, []string{"files/delta_log/444/-1/666/1"}, copiedFiles)
	})

	t.Run("all deletes after cutoff", func(t *testing.T) {
		copySource := proto.Clone(source).(*datapb.CopySegmentSource)
		copySource.DeltaBinlogs = nil
		metaSource := proto.Clone(copySource).(*datapb.CopySegmentSource)
		mockPrepare := mockey.Mock(prepareNonManifestRestoreCutoff).Return(&restoreCutoffPlan{
			copySource:   copySource,
			metaSource:   metaSource,
			mappings:     map[string]string{},
			importedRows: 0,
		}, nil).Build()
		defer mockPrepare.UnPatch()

		result, copiedFiles, err := CopySegmentAndIndexFiles(
			context.Background(),
			&struct{ storage.ChunkManager }{},
			source,
			target,
			nil,
			CopySegmentFileOptions{
				Schema:        schema,
				StorageConfig: &indexpb.StorageConfig{RootPath: "files"},
			},
		)

		assert.NoError(t, err)
		assert.Equal(t, int64(0), result.GetImportedRows())
		assert.Empty(t, result.GetDeltalogs())
		assert.Empty(t, copiedFiles)
	})
}
