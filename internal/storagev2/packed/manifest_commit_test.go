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
	"fmt"
	"path"
	"testing"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	basePath := path.Join(cfg.RootPath, "commit_empty_test/seg1")

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

func TestRemoveUnpublishedManifestKeepsSuccessorReadable(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	basePath := "files/remove_unpublished_manifest/seg1"
	firstStatPath := path.Join(cfg.RootPath, basePath, "_stats/text_index.100/1")
	secondStatPath := path.Join(cfg.RootPath, basePath, "_stats/json_stats.101/1")
	require.NoError(t, WriteFile(cfg, firstStatPath, []byte("text-stats")))
	require.NoError(t, WriteFile(cfg, secondStatPath, []byte("json-stats")))

	intermediate, err := CommitManifestUpdates(basePath, ManifestEarliest, cfg, &ManifestUpdates{
		Stats: []StatEntry{{Key: "text_index.100", Files: []string{firstStatPath}}},
	})
	require.NoError(t, err)
	_, intermediateVersion, err := UnmarshalManifestPath(intermediate)
	require.NoError(t, err)

	finalManifest, err := CommitManifestUpdates(basePath, intermediateVersion, cfg, &ManifestUpdates{
		Stats: []StatEntry{{Key: "json_stats.101", Files: []string{secondStatPath}}},
	})
	require.NoError(t, err)

	intermediateFilePath := fmt.Sprintf("%s/_metadata/manifest-%d.avro", basePath, intermediateVersion)
	_, err = ReadFile(cfg, intermediateFilePath)
	require.NoError(t, err, "the intermediate manifest must exist before removal")

	require.NoError(t, RemoveUnpublishedManifest(intermediate, cfg))
	// Read through the filesystem instead of GetManifestStats: manifests are
	// immutable and milvus-storage may still serve a previously opened version
	// from its in-process manifest cache after the physical file is deleted.
	_, err = ReadFile(cfg, intermediateFilePath)
	require.Error(t, err, "the exact unpublished manifest version must be removed")

	stats, err := GetManifestStats(finalManifest, cfg)
	require.NoError(t, err, "a manifest is a complete snapshot and must not depend on its predecessor")
	require.Contains(t, stats, "text_index.100")
	require.Contains(t, stats, "json_stats.101")

	_, finalVersion, err := UnmarshalManifestPath(finalManifest)
	require.NoError(t, err)
	nextManifest, err := CommitManifestUpdates(basePath, finalVersion, cfg, &ManifestUpdates{
		Stats: []StatEntry{{Key: "text_index.100", Files: []string{firstStatPath}}},
	})
	require.NoError(t, err, "future commits must tolerate a gap in manifest versions")
	_, nextVersion, err := UnmarshalManifestPath(nextManifest)
	require.NoError(t, err)
	require.Greater(t, nextVersion, finalVersion)
}

func TestRemoveUnpublishedManifestErrors(t *testing.T) {
	cfg := manifestTestStorageConfig(t)

	err := DeleteFile(cfg, "")
	require.ErrorIs(t, err, merr.ErrServiceInternal)

	err = RemoveUnpublishedManifest("not-a-manifest", cfg)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)

	err = RemoveUnpublishedManifest(MarshalManifestPath("files/non_persisted", ManifestEarliest), cfg)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)

	err = RemoveUnpublishedManifest(MarshalManifestPath("files/missing_manifest", 1), cfg)
	require.ErrorIs(t, err, merr.ErrStorage)
}

func TestReadManifestColumnGroupEntries(t *testing.T) {
	paramtable.InitWithBaseTable(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	cfg := manifestTestStorageConfig(t)
	basePath := "files/read_column_group_entries/seg1"
	groups := []ColumnGroupEntry{
		{Columns: []string{"100"}, Format: "parquet", Files: []ColumnGroupFileEntry{{
			Path: path.Join(cfg.RootPath, basePath, "input.parquet"), EndIndex: 10,
		}}},
		{Columns: []string{"101", "102"}, Format: "vortex", Files: []ColumnGroupFileEntry{{
			Path: path.Join(cfg.RootPath, basePath, "output.vortex"), StartIndex: 2, EndIndex: 10,
			Properties: map[string]string{"file_size": "1234", "footer_size": "256", "custom_property": "preserved"},
		}}},
	}
	manifest, err := CommitManifestUpdates(basePath, ManifestEarliest, cfg, &ManifestUpdates{ColumnGroups: groups})
	require.NoError(t, err)

	t.Run("preserve properties after releasing native manifest", func(t *testing.T) {
		entries, err := readManifestColumnGroupEntries(manifest, cfg, []string{"101", "102", "101"})
		require.NoError(t, err)
		require.Equal(t, groups[1:], entries)
		// Commit only Go-owned descriptors after the reader has freed its C handle.
		roundTrip, err := CommitManifestUpdates(basePath, ManifestEarliest, cfg, &ManifestUpdates{ColumnGroups: entries})
		require.NoError(t, err)
		actual, err := readManifestColumnGroupEntries(roundTrip, cfg, []string{"101", "102"})
		require.NoError(t, err)
		require.Equal(t, entries, actual)
	})
	for _, tc := range []struct {
		name    string
		columns []string
		message string
	}{
		{name: "no requested columns"},
		{name: "missing columns", columns: []string{"999", "888"}, message: "missing columns [888 999]"},
		{name: "mixed column group", columns: []string{"101"}, message: "mixes requested columns"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entries, err := readManifestColumnGroupEntries(manifest, cfg, tc.columns)
			require.Empty(t, entries)
			if tc.message == "" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
				require.ErrorContains(t, err, tc.message)
			}
		})
	}
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "nil manifest"},
		{name: "read error", err: merr.WrapErrStorageMsg("read failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			patch := mockey.Mock(GetManifestHandle).Return(nil, tc.err).Build()
			defer patch.UnPatch()
			entries, err := readManifestColumnGroupEntries(manifest, cfg, []string{"101"})
			require.Empty(t, entries)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
			} else {
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
			}
		})
	}
	for _, tc := range []struct {
		name  string
		group ColumnGroupEntry
		err   error
	}{
		{name: "decoder failure", err: merr.WrapErrServiceInternalMsg("incomplete property")},
		{name: "no files", group: ColumnGroupEntry{Columns: []string{"101"}, Format: "vortex"}},
		{name: "no format", group: ColumnGroupEntry{Columns: []string{"101"}, Files: groups[1].Files}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			patch := mockey.Mock(columnGroupEntriesFromC).Return([]ColumnGroupEntry{tc.group}, tc.err).Build()
			defer patch.UnPatch()
			entries, err := readManifestColumnGroupEntries(manifest, cfg, []string{"101"})
			require.Empty(t, entries)
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
		})
	}
}

func TestColumnGroupEntriesFromCValidatesNativeArrays(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	schema := arrow.NewSchema([]arrow.Field{{Name: "101", Type: arrow.PrimitiveTypes.Int64}}, nil)
	writer, err := NewFFIPackedWriter("files/column_group_entries/seg1", schema,
		[]storagecommon.ColumnGroup{{Columns: []int{0}, GroupID: 101}}, cfg, nil)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).Append(42)
	record := builder.NewRecord()
	defer record.Release()
	require.NoError(t, writer.WriteRecordBatch(record))
	output, err := writer.Close()
	require.NoError(t, err)
	defer output.Destroy()
	cgroups := output.(*ColumnGroups).cColumnGroups
	require.EqualValues(t, 1, cgroups.num_of_column_groups)
	group := &unsafe.Slice(cgroups.column_group_array, 1)[0]
	require.EqualValues(t, 1, group.num_of_files)
	file := &unsafe.Slice(group.files, 1)[0]
	require.Positive(t, int(file.num_properties))
	columns := unsafe.Slice(group.columns, int(group.num_of_columns))
	keys := unsafe.Slice(file.property_keys, int(file.num_properties))
	values := unsafe.Slice(file.property_values, int(file.num_properties))
	originalGroups, originalGroup, originalFile := *cgroups, *group, *file
	originalColumn, originalKey, originalValue := columns[0], keys[0], values[0]

	entries, err := columnGroupEntriesFromC(nil)
	require.NoError(t, err)
	require.Nil(t, entries)
	for _, tc := range []struct {
		name    string
		mutate  func()
		wantErr bool
	}{
		{name: "valid", mutate: func() {}},
		{name: "no groups", mutate: func() { cgroups.num_of_column_groups = 0 }},
		{name: "missing groups", mutate: func() { cgroups.column_group_array = nil }, wantErr: true},
		{name: "no columns", mutate: func() { group.columns = nil; group.num_of_columns = 0 }},
		{name: "missing columns", mutate: func() { group.columns = nil }, wantErr: true},
		{name: "nil column", mutate: func() { columns[0] = nil }, wantErr: true},
		{name: "no files", mutate: func() { group.files = nil; group.num_of_files = 0 }},
		{name: "missing files", mutate: func() { group.files = nil }, wantErr: true},
		{name: "nil path", mutate: func() { file.path = nil }, wantErr: true},
		{name: "no properties", mutate: func() { file.num_properties = 0 }},
		{name: "missing keys", mutate: func() { file.property_keys = nil }, wantErr: true},
		{name: "missing values", mutate: func() { file.property_values = nil }, wantErr: true},
		{name: "nil key", mutate: func() { keys[0] = nil }, wantErr: true},
		{name: "nil value", mutate: func() { values[0] = nil }, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Restore every native pointer before the owning writer output is freed.
			defer func() {
				*cgroups, *group, *file = originalGroups, originalGroup, originalFile
				columns[0], keys[0], values[0] = originalColumn, originalKey, originalValue
			}()
			tc.mutate()
			entries, err := columnGroupEntriesFromC(cgroups)
			if tc.wantErr {
				require.ErrorIs(t, err, merr.ErrServiceInternal)
				require.Nil(t, entries)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCarryManifestArtifactsErrorsReturnEmptyResult(t *testing.T) {
	basePath := "files/carry_errors/seg1"
	source := MarshalManifestPath(basePath, 1)
	target := MarshalManifestPath(basePath, 2)
	final := MarshalManifestPath(basePath, 3)
	for _, stage := range []string{"columns", "source stats", "target stats", "commit", "final stats"} {
		t.Run(stage, func(t *testing.T) {
			failure := merr.WrapErrStorageMsg("injected %s failure", stage)
			var columns []string
			if stage == "columns" {
				columns = []string{"101"}
				patch := mockey.Mock(readManifestColumnGroupEntries).Return(nil, failure).Build()
				defer patch.UnPatch()
			}
			reads := map[string]string{source: "source stats", target: "target stats", final: "final stats"}
			readPatch := mockey.Mock(GetManifestStats).To(func(manifest string, _ *indexpb.StorageConfig) (map[string]ManifestStat, error) {
				if reads[manifest] == stage {
					return nil, failure
				}
				return nil, nil
			}).Build()
			defer readPatch.UnPatch()
			commitPatch := mockey.Mock(CommitManifestUpdates).To(func(_ string, _ int64, _ *indexpb.StorageConfig, _ *ManifestUpdates) (string, error) {
				if stage == "commit" {
					return "", failure
				}
				return final, nil
			}).Build()
			defer commitPatch.UnPatch()
			result, err := CarryManifestArtifacts(source, target, nil, columns)
			require.ErrorIs(t, err, failure)
			require.Equal(t, ManifestArtifactCarryResult{}, result)
		})
	}
}

func TestCarryManifestArtifactsFinalStatsFailureKeepsCommittedVersion(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	basePath := "files/carry_final_stats_failure/seg1"
	source, err := CommitManifestUpdates(basePath, ManifestEarliest, cfg, &ManifestUpdates{
		Stats: []StatEntry{{Key: "text_index.100", Files: []string{path.Join(cfg.RootPath, basePath, "_stats/text")}}},
	})
	require.NoError(t, err)
	target, err := CommitManifestUpdates(basePath, ManifestEarliest, cfg, &ManifestUpdates{
		DeltaLogs: []DeltaLogEntry{{Path: path.Join(cfg.RootPath, basePath, "_delta/new"), NumEntries: 1}},
	})
	require.NoError(t, err)
	_, targetVersion, err := UnmarshalManifestPath(target)
	require.NoError(t, err)
	sourceStats, err := GetManifestStats(source, cfg)
	require.NoError(t, err)
	targetStats, err := GetManifestStats(target, cfg)
	require.NoError(t, err)
	readFailure := merr.WrapErrStorageMsg("final stats read failed")
	readPatch := mockey.Mock(GetManifestStats).To(func(manifest string, _ *indexpb.StorageConfig) (map[string]ManifestStat, error) {
		switch manifest {
		case source:
			return sourceStats, nil
		case target:
			return targetStats, nil
		default:
			return nil, readFailure
		}
	}).Build()
	defer readPatch.UnPatch()

	// The carry transaction is real; only the metadata read after it fails.
	result, err := CarryManifestArtifacts(source, target, cfg, nil)
	require.ErrorIs(t, err, readFailure)
	require.Equal(t, ManifestArtifactCarryResult{}, result)
	committedVersion := targetVersion + 1
	_, err = ReadFile(cfg, fmt.Sprintf("%s/_metadata/manifest-%d.avro", basePath, committedVersion))
	require.NoError(t, err, "an empty error result must not imply rollback of the committed manifest")
	next, err := CommitManifestUpdates(basePath, ManifestEarliest, cfg, &ManifestUpdates{
		DeltaLogs: []DeltaLogEntry{{Path: path.Join(cfg.RootPath, basePath, "_delta/retry"), NumEntries: 2}},
	})
	require.NoError(t, err)
	_, nextVersion, err := UnmarshalManifestPath(next)
	require.NoError(t, err)
	require.Greater(t, nextVersion, committedVersion)
}

// TestCommitManifestUpdates_StatsOnly exercises the stats-without-inserts
// path: a stat blob is written to storage and registered in the manifest
// via a single CommitManifestUpdates call.
func TestCommitManifestUpdates_StatsOnly(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	basePath := path.Join(cfg.RootPath, "commit_stats_only/seg1")

	statPath := path.Join(basePath, "_stats/bloom_filter.100/1")
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
	basePath := path.Join(cfg.RootPath, "stats_binlog_size/seg1")

	bloomPath := path.Join(basePath, "_stats/bloom_filter.100/1")
	bm25Path := path.Join(basePath, "_stats/bm25.101/1")
	textPath := path.Join(basePath, "_stats/text.102/1")
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
	basePath := path.Join(cfg.RootPath, "commit_delta_only/seg1")

	deltaPath := path.Join(basePath, "_delta/delta-1")
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
	basePath := path.Join(cfg.RootPath, "commit_all_sections/seg1")

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

	statPath := path.Join(basePath, "_stats/bloom_filter.100/1")
	require.NoError(t, WriteFile(cfg, statPath, []byte("bloom-blob")))
	deltaPath := path.Join(basePath, "_delta/delta-1")
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
	basePath := path.Join(cfg.RootPath, "commit_new_cgs/seg1")

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

func TestCommitManifestUpdates_AddEmptyColumnGroup(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	basePath := path.Join(cfg.RootPath, "commit_empty_cg/seg1")

	schema := arrow.NewSchema([]arrow.Field{{
		Name:     "101",
		Type:     arrow.PrimitiveTypes.Int64,
		Nullable: true,
		Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"101"}),
	}}, nil)
	columnGroups := []storagecommon.ColumnGroup{{Columns: []int{0}, Fields: []int64{101}, GroupID: 101}}

	w, err := NewFFIPackedWriter(basePath, schema, columnGroups, cfg, nil)
	require.NoError(t, err)
	w.AsNewColumnGroups()
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	rec := b.NewRecord()
	defer rec.Release()
	require.NoError(t, w.WriteRecordBatch(rec))

	out, err := w.Close()
	require.NoError(t, err)
	defer out.Destroy()
	manifest, err := CommitManifestUpdates(basePath, ManifestEarliest, cfg, &ManifestUpdates{NewFiles: out})
	require.NoError(t, err)

	fields, err := GetManifestFieldIDs(manifest, cfg)
	require.NoError(t, err)
	require.Contains(t, fields, int64(101))
}

// TestColumnGroups_DestroyIdempotent verifies that calling Destroy on a
// ColumnGroups handle multiple times (and on a nil receiver) is safe.
func TestColumnGroups_DestroyIdempotent(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	basePath := path.Join(cfg.RootPath, "cg_destroy_test/seg1")

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
	basePath := path.Join(cfg.RootPath, "ffi_double_close/seg1")

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
