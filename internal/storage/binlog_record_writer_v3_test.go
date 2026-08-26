// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package storage

import (
	"context"
	"path"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// TestPackedManifestRecordWriter_CloseWithoutWrite verifies the no-data
// short-circuit: Close on a freshly-constructed PackedManifestRecordWriter
// must not call into the FFI commit path and must not produce a manifest.
func TestPackedManifestRecordWriter_CloseWithoutWrite(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.TimeStampField, DataType: schemapb.DataType_Int64},
		{FieldID: common.RowIDField, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}

	dir := t.TempDir()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: dir}

	w, err := newPackedManifestRecordWriter(1, 2, 3, schema,
		ChunkedBlobsWriter(func(_ []*Blob) error { return nil }),
		allocator.NewLocalAllocator(1, 1<<20),
		1024, 0, 0, nil, cfg, nil, false, "")
	require.NoError(t, err)

	// No Write before Close. The internal `writer` field stays nil so
	// Close must return immediately without invoking the FFI commit.
	require.NoError(t, w.Close())

	_, statsLog, _, manifestPath, _ := w.GetLogs()
	assert.Empty(t, manifestPath, "no Write means no manifest should be produced")
	assert.Nil(t, statsLog, "no Write means no statsLog should be produced")
}

// TestPackedTextManifestRecordWriter_CloseWithoutWrite is the parallel
// short-circuit test for the text writer: Close with no Writes must
// still succeed without touching the FFI segment writer.
func TestPackedTextManifestRecordWriter_CloseWithoutWrite(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.TimeStampField, DataType: schemapb.DataType_Int64},
		{FieldID: common.RowIDField, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, DataType: schemapb.DataType_Text, Name: "doc"},
	}}

	dir := t.TempDir()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: dir}

	w, err := NewPackedTextManifestRecordWriter(1, 2, 3, schema,
		ChunkedBlobsWriter(func(_ []*Blob) error { return nil }),
		allocator.NewLocalAllocator(1, 1<<20),
		1024, 0, 0, nil, cfg, nil, "")
	require.NoError(t, err)

	// No Write before Close. The text writer's nil-handling path must
	// exit cleanly without producing legacy stats.
	require.NoError(t, w.Close())

	_, _, _, manifestPath, _ := w.GetLogs()
	assert.Empty(t, manifestPath, "no Write means no manifest should be produced")
}

func TestPackedTextManifestRecordWriter_AppendsV3StatsToManifest(t *testing.T) {
	dir := t.TempDir()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: dir}
	schema := genCollectionSchemaWithBM25()
	collectionID := UniqueID(10)
	partitionID := UniqueID(20)
	segmentID := UniqueID(30)

	w, err := NewPackedTextManifestRecordWriter(collectionID, partitionID, segmentID, schema,
		ChunkedBlobsWriter(func(_ []*Blob) error { return nil }),
		allocator.NewLocalAllocator(1000, 1<<20),
		1024, 0, 0, nil, cfg, nil, "")
	require.NoError(t, err)

	value := &Value{
		PK:        NewVarCharPrimaryKey("0"),
		Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday())),
		Value:     genRowWithBM25(0),
	}
	rec, err := ValueSerializer([]*Value{value}, schema)
	require.NoError(t, err)
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())

	_, statsLog, bm25Logs, manifestPath, _ := w.GetLogs()
	require.NotEmpty(t, manifestPath)

	stats, err := packed.GetManifestStats(manifestPath, cfg)
	require.NoError(t, err)

	bfKey := "bloom_filter.100"
	bfStat, ok := stats[bfKey]
	require.True(t, ok, "TEXT manifest writer must register PK BF stats under %q", bfKey)
	require.NotEmpty(t, bfStat.Paths)
	bfMemorySize, err := strconv.ParseInt(bfStat.Metadata["memory_size"], 10, 64)
	require.NoError(t, err)
	require.Positive(t, bfMemorySize)
	assert.True(t, strings.Contains(bfStat.Paths[0], "/_stats/bloom_filter.100/"))
	assert.NotContains(t, bfStat.Paths[0], "stats_log")

	bm25Key := "bm25.102"
	bm25Stat, ok := stats[bm25Key]
	require.True(t, ok, "TEXT manifest writer must register BM25 stats under %q", bm25Key)
	require.NotEmpty(t, bm25Stat.Paths)
	assert.True(t, strings.Contains(bm25Stat.Paths[0], "/_stats/bm25.102/"))
	assert.NotContains(t, bm25Stat.Paths[0], "stats_log")

	assert.Nil(t, statsLog, "V3 manifest stats must not be returned as legacy PK statslog")
	assert.Empty(t, bm25Logs, "V3 manifest stats must not be returned as legacy BM25 statslog")
}

func TestPackedManifestRecordWriter_FillsV3ColumnGroupFormats(t *testing.T) {
	params := paramtable.Get()
	require.NoError(t, params.Save(params.DataNodeCfg.StorageFormat.Key, "vortex"))
	defer params.Reset(params.DataNodeCfg.StorageFormat.Key)

	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.TimeStampField, DataType: schemapb.DataType_Int64},
		{FieldID: common.RowIDField, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, DataType: schemapb.DataType_Int64},
	}}
	columnGroups := []storagecommon.ColumnGroup{
		{GroupID: 0, Columns: []int{0, 1}, Fields: []int64{common.TimeStampField, common.RowIDField}},
		{GroupID: 101, Columns: []int{2}, Fields: []int64{101}, Format: "parquet"},
	}
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}

	var gotWriterFormat string
	var gotSchemaBasedFormats []string
	var gotColumnGroups []storagecommon.ColumnGroup
	patch := mockey.Mock(newPackedRecordBatchWriter).To(
		func(_ string, _ *schemapb.CollectionSchema, _, _ int64, groups []storagecommon.ColumnGroup,
			_ *indexpb.StorageConfig, _ *indexcgopb.StoragePluginContext, validatePK bool, textRefsAsBinary bool,
			writerFormat string, schemaBasedFormats []string,
		) (*packedRecordBatchWriter, error) {
			assert.True(t, validatePK)
			assert.False(t, textRefsAsBinary)
			gotWriterFormat = writerFormat
			gotSchemaBasedFormats = append([]string(nil), schemaBasedFormats...)
			gotColumnGroups = append([]storagecommon.ColumnGroup(nil), groups...)
			return &packedRecordBatchWriter{}, nil
		}).Build()
	defer patch.UnPatch()

	w, err := newPackedManifestRecordWriter(1, 2, 3, schema,
		ChunkedBlobsWriter(func(_ []*Blob) error { return nil }),
		allocator.NewLocalAllocator(1, 1<<20),
		1024, 0, 0, columnGroups, cfg, nil, false, "")
	require.NoError(t, err)
	require.NoError(t, w.initWriters(nil))

	assert.Equal(t, "vortex", gotWriterFormat)
	assert.Equal(t, []string{"vortex", "parquet"}, gotSchemaBasedFormats)
	require.Len(t, gotColumnGroups, 2)
	assert.Equal(t, "vortex", gotColumnGroups[0].Format)
	assert.Equal(t, "parquet", gotColumnGroups[1].Format)
	assert.Equal(t, gotColumnGroups, w.columnGroups)
}

func TestPackedManifestRecordWriter_TextRefsUseBinarySchema(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.TimeStampField, DataType: schemapb.DataType_Int64},
		{FieldID: common.RowIDField, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, DataType: schemapb.DataType_Text, Name: "doc"},
	}}
	columnGroups := []storagecommon.ColumnGroup{
		{GroupID: 0, Columns: []int{0, 1, 2}, Fields: []int64{common.TimeStampField, common.RowIDField, 101}},
	}
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}

	var gotSchema *arrow.Schema
	patch := mockey.Mock(packed.NewFFIPackedWriter).To(
		func(_ string, schema *arrow.Schema, _ []storagecommon.ColumnGroup,
			_ *indexpb.StorageConfig, _ *indexcgopb.StoragePluginContext,
			_ ...map[string]string,
		) (*packed.FFIPackedWriter, error) {
			gotSchema = schema
			return &packed.FFIPackedWriter{}, nil
		}).Build()
	defer patch.UnPatch()

	_, err := NewPackedRecordBatchWriter(
		t.TempDir(),
		schema,
		0,
		0,
		columnGroups,
		cfg,
		nil,
		"vortex",
		nil,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires TEXT-aware writer")
	require.Nil(t, gotSchema)

	gotSchema = nil
	w, err := newPackedManifestRecordWriter(1, 2, 3, schema,
		ChunkedBlobsWriter(func(_ []*Blob) error { return nil }),
		allocator.NewLocalAllocator(1, 1<<20),
		1024, 0, 0, columnGroups, cfg, nil, true, "")
	require.NoError(t, err)
	err = w.initWriters(nil)
	require.NoError(t, err)
	require.NotNil(t, gotSchema)
	require.Equal(t, arrow.BINARY, gotSchema.Field(2).Type.ID())
}

func TestPartialPackedRecordBatchWriter_TextRefsUseBinarySchema(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 101, DataType: schemapb.DataType_Text, Name: "doc", Nullable: true},
	}}
	columnGroups := []storagecommon.ColumnGroup{
		{GroupID: 101, Columns: []int{0}, Fields: []int64{101}},
	}
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}

	var gotSchema *arrow.Schema
	patch := mockey.Mock(packed.NewFFIPackedWriter).To(
		func(_ string, schema *arrow.Schema, _ []storagecommon.ColumnGroup,
			_ *indexpb.StorageConfig, _ *indexcgopb.StoragePluginContext,
			_ ...map[string]string,
		) (*packed.FFIPackedWriter, error) {
			gotSchema = schema
			return &packed.FFIPackedWriter{}, nil
		}).Build()
	defer patch.UnPatch()

	_, err := NewPartialPackedRecordBatchWriter(
		t.TempDir(), schema, 0, 0, columnGroups, cfg, nil, "vortex", nil,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires TEXT-aware writer")
	require.Nil(t, gotSchema)

	w, err := NewPartialPackedRecordBatchWriterWithTextRefsAsBinary(
		t.TempDir(), schema, 0, 0, columnGroups, cfg, nil, "vortex", nil,
	)
	require.NoError(t, err)
	require.NotNil(t, w)
	require.NotNil(t, gotSchema)
	require.Equal(t, arrow.BINARY, gotSchema.Field(0).Type.ID())
}

func TestPackedManifestRecordWriter_UsesExplicitWriterFormat(t *testing.T) {
	params := paramtable.Get()
	require.NoError(t, params.Save(params.DataNodeCfg.StorageFormat.Key, "vortex"))
	defer params.Reset(params.DataNodeCfg.StorageFormat.Key)

	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.TimeStampField, DataType: schemapb.DataType_Int64},
		{FieldID: common.RowIDField, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, DataType: schemapb.DataType_Int64},
	}}
	columnGroups := []storagecommon.ColumnGroup{
		{GroupID: 0, Columns: []int{0, 1}, Fields: []int64{common.TimeStampField, common.RowIDField}},
		{GroupID: 101, Columns: []int{2}, Fields: []int64{101}},
	}
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}

	var gotWriterFormat string
	var gotSchemaBasedFormats []string
	patch := mockey.Mock(newPackedRecordBatchWriter).To(
		func(_ string, _ *schemapb.CollectionSchema, _, _ int64, _ []storagecommon.ColumnGroup,
			_ *indexpb.StorageConfig, _ *indexcgopb.StoragePluginContext, validatePK bool, textRefsAsBinary bool,
			writerFormat string, schemaBasedFormats []string,
		) (*packedRecordBatchWriter, error) {
			assert.True(t, validatePK)
			assert.False(t, textRefsAsBinary)
			gotWriterFormat = writerFormat
			gotSchemaBasedFormats = append([]string(nil), schemaBasedFormats...)
			return &packedRecordBatchWriter{}, nil
		}).Build()
	defer patch.UnPatch()

	w, err := newPackedManifestRecordWriter(1, 2, 3, schema,
		ChunkedBlobsWriter(func(_ []*Blob) error { return nil }),
		allocator.NewLocalAllocator(1, 1<<20),
		1024, 0, 0, columnGroups, cfg, nil, false, "parquet")
	require.NoError(t, err)
	require.NoError(t, w.initWriters(nil))

	assert.Equal(t, "parquet", gotWriterFormat)
	assert.Equal(t, []string{"parquet", "parquet"}, gotSchemaBasedFormats)
}

func TestPackedTextManifestRecordWriter_FillsV3ColumnGroupFormats(t *testing.T) {
	params := paramtable.Get()
	require.NoError(t, params.Save(params.DataNodeCfg.StorageFormat.Key, "vortex"))
	defer params.Reset(params.DataNodeCfg.StorageFormat.Key)

	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.TimeStampField, DataType: schemapb.DataType_Int64},
		{FieldID: common.RowIDField, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, DataType: schemapb.DataType_Text, Name: "doc"},
	}}
	columnGroups := []storagecommon.ColumnGroup{
		{GroupID: 0, Columns: []int{0, 1}, Fields: []int64{common.TimeStampField, common.RowIDField}},
		{GroupID: 101, Columns: []int{2}, Fields: []int64{101}, Format: "parquet"},
	}
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}

	var gotWriterFormat string
	var gotSchemaBasedFormats []string
	var gotColumnGroups []storagecommon.ColumnGroup
	patch := mockey.Mock(NewPackedTextBatchWriter).To(
		func(_ string, _ string, _ *schemapb.CollectionSchema, _, _ int64, groups []storagecommon.ColumnGroup,
			_ *indexpb.StorageConfig, _ []packed.TextColumnConfig, writerFormat string, schemaBasedFormats []string,
		) (*packedTextBatchWriter, error) {
			gotWriterFormat = writerFormat
			gotSchemaBasedFormats = append([]string(nil), schemaBasedFormats...)
			gotColumnGroups = append([]storagecommon.ColumnGroup(nil), groups...)
			return &packedTextBatchWriter{}, nil
		}).Build()
	defer patch.UnPatch()

	w, err := NewPackedTextManifestRecordWriter(1, 2, 3, schema,
		ChunkedBlobsWriter(func(_ []*Blob) error { return nil }),
		allocator.NewLocalAllocator(1, 1<<20),
		1024, 0, 0, columnGroups, cfg, nil, "")
	require.NoError(t, err)
	require.NoError(t, w.initWriters(nil))

	assert.Equal(t, "vortex", gotWriterFormat)
	assert.Equal(t, []string{"vortex", "parquet"}, gotSchemaBasedFormats)
	require.Len(t, gotColumnGroups, 2)
	assert.Equal(t, "vortex", gotColumnGroups[0].Format)
	assert.Equal(t, "parquet", gotColumnGroups[1].Format)
	assert.Equal(t, gotColumnGroups, w.columnGroups)
}

func TestManifestRecordReader_ResolvesOutOfLineTextLob(t *testing.T) {
	const textFieldID = int64(101)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.RowIDField, Name: "row_id", DataType: schemapb.DataType_Int64},
		{FieldID: common.TimeStampField, Name: "timestamp", DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: textFieldID, Name: "text", DataType: schemapb.DataType_Text},
	}}
	rootPath := t.TempDir()
	segmentPath := path.Join("insert_log", "1", "2", "3")
	config := &indexpb.StorageConfig{StorageType: "local", RootPath: rootPath}
	columnGroups := []storagecommon.ColumnGroup{{
		GroupID: 0,
		Columns: []int{0, 1, 2, 3},
		Fields:  []int64{common.RowIDField, common.TimeStampField, 100, textFieldID},
	}}
	textConfig := []packed.TextColumnConfig{{
		FieldID:             textFieldID,
		LobBasePath:         path.Join("insert_log", "1", "2", "lobs", "101"),
		InlineThreshold:     1,
		MaxLobFileBytes:     1 << 20,
		FlushThresholdBytes: 1,
	}}

	w, err := NewPackedTextBatchWriter("", segmentPath, schema, 0, 0,
		columnGroups, config, textConfig, "parquet", []string{"parquet"})
	require.NoError(t, err)

	arrowSchema, err := ConvertToArrowSchema(schema, true)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	builder.Field(1).(*array.Int64Builder).AppendValues([]int64{10, 20}, nil)
	builder.Field(2).(*array.Int64Builder).AppendValues([]int64{1000, 2000}, nil)
	wantText := []string{"external text one", "external text two"}
	builder.Field(3).(*array.StringBuilder).AppendValues(wantText, nil)
	rawRecord := builder.NewRecord()
	builder.Release()
	record := NewSimpleArrowRecord(rawRecord, map[FieldID]int{
		common.RowIDField:     0,
		common.TimeStampField: 1,
		100:                   2,
		textFieldID:           3,
	})
	defer record.Release()
	require.NoError(t, w.Write(record))
	output, err := w.Close()
	require.NoError(t, err)
	require.NotNil(t, output)
	defer output.Destroy()

	manifest, err := packed.CommitManifestUpdates(segmentPath, packed.ManifestEarliest, config,
		&packed.ManifestUpdates{NewFiles: output})
	require.NoError(t, err)
	lobFiles, err := packed.GetManifestLobFiles(manifest, config)
	require.NoError(t, err)
	require.NotEmpty(t, lobFiles)

	reader, err := NewManifestRecordReader(context.Background(), manifest, schema,
		WithVersion(StorageV3), WithStorageConfig(config), WithResolveTextLob())
	require.NoError(t, err)
	defer reader.Close()

	got, err := reader.Next()
	require.NoError(t, err)
	defer got.Release()
	textColumn, ok := got.Column(textFieldID).(*array.String)
	require.True(t, ok)
	gotText := make([]string, textColumn.Len())
	for i := range gotText {
		gotText[i] = textColumn.Value(i)
	}
	assert.Equal(t, wantText, gotText)
}

func TestManifestRecordReader_RejectsCMEKTextLob(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "text", DataType: schemapb.DataType_Text},
	}}
	pluginContext := &indexcgopb.StoragePluginContext{
		EncryptionZoneId: 1,
		CollectionId:     2,
		EncryptionKey:    "key",
	}

	reader, err := NewManifestRecordReader(
		context.Background(),
		packed.MarshalManifestPath("insert_log/1/2/3", 1),
		schema,
		WithVersion(StorageV3),
		WithStorageConfig(&indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}),
		WithPluginContext(pluginContext),
		WithResolveTextLob(),
	)
	require.Nil(t, reader)
	require.ErrorIs(t, err, merr.ErrOperationNotSupported)
	require.Contains(t, err.Error(), "CMEK-protected StorageV3 backup import is not supported")
}
