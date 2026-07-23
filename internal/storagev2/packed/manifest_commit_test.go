// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package packed

import (
	"path"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// manifestTestStorageConfig returns a storage config wired to a per-test
// temp dir for use with CommitManifestUpdates / FFIPackedWriter.
func manifestTestStorageConfig(t *testing.T) *indexpb.StorageConfig {
	t.Helper()
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	dir := t.TempDir()
	pt.Save(pt.LocalStorageCfg.Path.Key, dir)
	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.LocalStorageCfg.Path.Key)
	})
	return &indexpb.StorageConfig{StorageType: "local", RootPath: dir}
}

// TestCommitManifestUpdates_EmptyShortCircuit verifies that an empty
// ManifestUpdates returns the unchanged manifest path without opening a
// loon transaction (and therefore without bumping the version).
func TestCommitManifestUpdates_EmptyShortCircuit(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	basePath := "files/commit_empty_test/seg1"

	cases := []struct {
		name string
		u    *ManifestUpdates
	}{
		{"nil-updates", nil},
		{"zero-updates", &ManifestUpdates{}},
		{"empty-slices", &ManifestUpdates{DeltaLogs: []DeltaLogEntry{}, Stats: []StatEntry{}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CommitManifestUpdates(basePath, ManifestEarliest, cfg, tc.u)
			require.NoError(t, err)
			require.Equal(t, MarshalManifestPath(basePath, ManifestEarliest), got,
				"empty updates must return the unchanged manifest path")
		})
	}
}

// TestCommitManifestUpdates_StatsOnly exercises the stats-without-inserts
// path: a stat blob is written to storage and registered in the manifest
// via a single CommitManifestUpdates call.
func TestCommitManifestUpdates_StatsOnly(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	basePath := "files/commit_stats_only/seg1"

	statPath := path.Join(cfg.RootPath, basePath, "_stats/bloom_filter.100/1")
	require.NoError(t, WriteFile(cfg, statPath, []byte("bloom-blob")))

	got, err := CommitManifestUpdates(basePath, ManifestEarliest, cfg, &ManifestUpdates{
		Stats: []StatEntry{{
			Key:      "bloom_filter.100",
			Files:    []string{statPath},
			Metadata: map[string]string{"memory_size": "10"},
		}},
	})
	require.NoError(t, err)
	_, version, err := UnmarshalManifestPath(got)
	require.NoError(t, err)
	require.Equal(t, int64(1), version, "stats-only commit must bump version exactly once")

	stats, err := GetManifestStats(got, cfg)
	require.NoError(t, err)
	require.Contains(t, stats, "bloom_filter.100")
	require.Equal(t, "10", stats["bloom_filter.100"].Metadata["memory_size"])
}

// TestStatsBinlogSizeFromManifest verifies the aggregate sums bloom-filter and
// BM25 memory_size while excluding text/JSON index stats.
func TestStatsBinlogSizeFromManifest(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	basePath := "files/stats_binlog_size/seg1"

	bloomPath := path.Join(cfg.RootPath, basePath, "_stats/bloom_filter.100/1")
	bm25Path := path.Join(cfg.RootPath, basePath, "_stats/bm25.101/1")
	textPath := path.Join(cfg.RootPath, basePath, "_stats/text.102/1")
	require.NoError(t, WriteFile(cfg, bloomPath, []byte("bloom-blob")))
	require.NoError(t, WriteFile(cfg, bm25Path, []byte("bm25-blob")))
	require.NoError(t, WriteFile(cfg, textPath, []byte("text-blob")))

	got, err := CommitManifestUpdates(basePath, ManifestEarliest, cfg, &ManifestUpdates{
		Stats: []StatEntry{
			{Key: "bloom_filter.100", Files: []string{bloomPath}, Metadata: map[string]string{"memory_size": "10"}},
			{Key: "bm25.101", Files: []string{bm25Path}, Metadata: map[string]string{"memory_size": "20"}},
			// text index stats must NOT count toward StatsBinlogSize.
			{Key: "text.102", Files: []string{textPath}, Metadata: map[string]string{"memory_size": "5"}},
		},
	})
	require.NoError(t, err)

	size, err := StatsBinlogSizeFromManifest(got, cfg)
	require.NoError(t, err)
	require.Equal(t, int64(30), size, "should sum bloom_filter(10)+bm25(20), excluding text(5)")
}

// TestStatsBinlogSizeFromManifest_ReadError returns an error on an unreadable manifest.
func TestStatsBinlogSizeFromManifest_ReadError(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	_, err := StatsBinlogSizeFromManifest("not-a-manifest-path", cfg)
	require.Error(t, err)
}

// TestCommitManifestUpdates_DeltaOnly exercises the delta-only path.
func TestCommitManifestUpdates_DeltaOnly(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	basePath := "files/commit_delta_only/seg1"

	deltaPath := path.Join(cfg.RootPath, basePath, "_delta/delta-1")
	require.NoError(t, WriteFile(cfg, deltaPath, []byte("delta-payload")))

	got, err := CommitManifestUpdates(basePath, ManifestEarliest, cfg, &ManifestUpdates{
		DeltaLogs: []DeltaLogEntry{{Path: deltaPath, NumEntries: 7}},
	})
	require.NoError(t, err)
	_, version, err := UnmarshalManifestPath(got)
	require.NoError(t, err)
	require.Equal(t, int64(1), version)

	paths, err := GetDeltaLogPathsFromManifest(got, cfg)
	require.NoError(t, err)
	require.Len(t, paths, 1)
}

// TestCommitManifestUpdates_AllSections is the integration variant: one
// commit covering inserts (column-groups from a real FFIPackedWriter),
// stats, and delta entries — asserts a single version bump for the whole
// bundle.
func TestCommitManifestUpdates_AllSections(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	basePath := "files/commit_all_sections/seg1"

	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "pk",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"100"}),
		},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	pkb := b.Field(0).(*array.Int64Builder)
	for i := 0; i < 4; i++ {
		pkb.Append(int64(i))
	}
	rec := b.NewRecord()
	defer rec.Release()

	columnGroups := []storagecommon.ColumnGroup{
		{Columns: []int{0}, GroupID: storagecommon.DefaultShortColumnGroupID},
	}

	w, err := NewFFIPackedWriter(basePath, schema, columnGroups, cfg, nil)
	require.NoError(t, err)
	require.NoError(t, w.WriteRecordBatch(rec))
	out, err := w.Close()
	require.NoError(t, err)
	defer out.Destroy()

	statPath := path.Join(cfg.RootPath, basePath, "_stats/bloom_filter.100/1")
	require.NoError(t, WriteFile(cfg, statPath, []byte("bloom-blob")))
	deltaPath := path.Join(cfg.RootPath, basePath, "_delta/delta-1")
	require.NoError(t, WriteFile(cfg, deltaPath, []byte("delta-payload")))

	got, err := CommitManifestUpdates(basePath, ManifestEarliest, cfg, &ManifestUpdates{
		NewFiles:  out,
		DeltaLogs: []DeltaLogEntry{{Path: deltaPath, NumEntries: 4}},
		Stats: []StatEntry{{
			Key:      "bloom_filter.100",
			Files:    []string{statPath},
			Metadata: map[string]string{"memory_size": "10"},
		}},
	})
	require.NoError(t, err)

	_, version, err := UnmarshalManifestPath(got)
	require.NoError(t, err)
	require.Equal(t, int64(1), version,
		"inserts + delta + stats together must bump version exactly once")
}

// TestCommitManifestUpdates_AddNewColumnGroups exercises the
// add_column_group (function-backfill) branch in applyManifestUpdates.
func TestCommitManifestUpdates_AddNewColumnGroups(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	basePath := "files/commit_new_cgs/seg1"

	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "pk",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"100"}),
		},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(int64(1))
	rec := b.NewRecord()
	defer rec.Release()

	columnGroups := []storagecommon.ColumnGroup{
		{Columns: []int{0}, GroupID: storagecommon.DefaultShortColumnGroupID},
	}

	w, err := NewFFIPackedWriter(basePath, schema, columnGroups, cfg, nil)
	require.NoError(t, err)
	w.AsNewColumnGroups()
	require.NoError(t, w.WriteRecordBatch(rec))
	out, err := w.Close()
	require.NoError(t, err)
	defer out.Destroy()
	cgs, ok := out.(*ColumnGroups)
	require.True(t, ok, "Close should return *ColumnGroups for FFIPackedWriter")
	require.True(t, cgs.addNewColumnGroups,
		"AsNewColumnGroups should propagate into ColumnGroups handle")

	got, err := CommitManifestUpdates(basePath, ManifestEarliest, cfg,
		&ManifestUpdates{NewFiles: out})
	require.NoError(t, err)
	_, version, err := UnmarshalManifestPath(got)
	require.NoError(t, err)
	require.Equal(t, int64(1), version)
}

// TestColumnGroups_DestroyIdempotent verifies that calling Destroy on a
// ColumnGroups handle multiple times (and on a nil receiver) is safe.
func TestColumnGroups_DestroyIdempotent(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	basePath := "files/cg_destroy_test/seg1"

	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "pk",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"100"}),
		},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(int64(0))
	rec := b.NewRecord()
	defer rec.Release()

	w, err := NewFFIPackedWriter(basePath, schema,
		[]storagecommon.ColumnGroup{{Columns: []int{0}, GroupID: storagecommon.DefaultShortColumnGroupID}},
		cfg, nil)
	require.NoError(t, err)
	require.NoError(t, w.WriteRecordBatch(rec))
	out, err := w.Close()
	require.NoError(t, err)
	cgs, ok := out.(*ColumnGroups)
	require.True(t, ok, "Close should return *ColumnGroups")
	require.NotNil(t, cgs.cColumnGroups, "Close should yield non-nil cColumnGroups")

	cgs.Destroy()
	require.Nil(t, cgs.cColumnGroups, "first Destroy must clear cColumnGroups")
	cgs.Destroy() // second call must be a no-op
	assert.Nil(t, cgs.cColumnGroups)

	var nilHandle *ColumnGroups
	assert.NotPanics(t, func() { nilHandle.Destroy() },
		"Destroy on nil receiver must be safe")
}

// TestFFIPackedWriter_DoubleCloseRejected verifies that the second Close
// after a successful first Close returns an error rather than re-running
// the close FFI on an exhausted handle.
func TestFFIPackedWriter_DoubleCloseRejected(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	basePath := "files/ffi_double_close/seg1"

	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "pk",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"100"}),
		},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(int64(0))
	rec := b.NewRecord()
	defer rec.Release()

	w, err := NewFFIPackedWriter(basePath, schema,
		[]storagecommon.ColumnGroup{{Columns: []int{0}, GroupID: storagecommon.DefaultShortColumnGroupID}},
		cfg, nil)
	require.NoError(t, err)
	require.NoError(t, w.WriteRecordBatch(rec))

	out, err := w.Close()
	require.NoError(t, err)
	defer out.Destroy()

	_, err = w.Close()
	require.Error(t, err, "second Close must return an error")
	require.Contains(t, err.Error(), "already closed")
}
