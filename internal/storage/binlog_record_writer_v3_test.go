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
	"path"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
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
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
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
	// exit cleanly without producing a manifest or legacy statslogs.
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
	require.NoError(t, w.pkCollector.Collect(rec))
	require.NoError(t, w.bm25Collector.Collect(rec))
	w.rowNum = int64(rec.Len())
	w.basePath = path.Join(dir, common.SegmentInsertLogPath,
		metautil.JoinIDPath(collectionID, partitionID, segmentID))

	updates := &packed.ManifestUpdates{}
	require.NoError(t, w.appendV3Stats(updates))
	require.Len(t, updates.Stats, 2)
	manifestPath, err := packed.CommitManifestUpdates(w.basePath, packed.ManifestEarliest, cfg, updates)
	require.NoError(t, err)

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

	_, statsLog, bm25Logs, _, _ := w.GetLogs()
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
