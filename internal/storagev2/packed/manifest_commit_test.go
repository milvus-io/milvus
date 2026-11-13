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
	"context"
	"fmt"
	"path"
	"testing"
	"time"
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
// Local Loon filesystems are rooted at "/", so callers must include RootPath
// in each complete file key instead of relying on an implicit root prefix.
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
// loon transaction (and therefore without bumping the version). The path comes
// back verbatim rather than re-marshaled, so a pointer spelled differently from
// MarshalManifestPath keeps its exact text.
func TestCommitManifestUpdates_EmptyShortCircuit(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	basePath := path.Join(cfg.RootPath, "commit_empty_test/seg1")
	baseManifest := fmt.Sprintf(`{"base_path":%q,"ver":%d}`, basePath, ManifestEarliest)

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
			got, err := CommitManifestUpdates(baseManifest, cfg, tc.u)
			require.NoError(t, err)
			require.Equal(t, baseManifest, got,
				"empty updates must return the unchanged manifest path verbatim")
		})
	}
}

// A commit on object storage records the size of the manifest object it wrote,
// and a read through the recorded path gets the committed revision.
func TestCommitManifestUpdates_RecordsManifestSize(t *testing.T) {
	paramtable.Init()
	cfg := CreateStorageConfig()
	if cfg.GetStorageType() == "local" {
		t.Skip("local storage records no manifest size")
	}
	basePath := path.Join(cfg.GetRootPath(), fmt.Sprintf("manifest_size_record/%d", time.Now().UnixNano()))
	statPath := path.Join(basePath, "_stats/bloom_filter.100/1")
	require.NoError(t, WriteFile(cfg, statPath, []byte("stats")))

	manifest, err := CommitManifestUpdates(MarshalManifestPath(basePath, ManifestEarliest), cfg,
		&ManifestUpdates{Stats: []StatEntry{{Key: "bloom_filter.100", Files: []string{statPath}}}})
	require.NoError(t, err)
	assert.Equal(t, MarshalManifestPath(basePath, 1), ManifestPathKey(manifest))

	manifestFile, err := ManifestFilePath(manifest)
	require.NoError(t, err)
	data, err := ReadFile(cfg, manifestFile)
	require.NoError(t, err)
	recorded, err := ParseManifestPath(manifest)
	require.NoError(t, err)
	assert.EqualValues(t, len(data), recorded.Size)

	stats, err := GetManifestStats(manifest, cfg)
	require.NoError(t, err)
	assert.Equal(t, []string{statPath}, stats["bloom_filter.100"].Paths)
}

// TestCommitManifestUpdates_InvalidBaseManifest verifies that a base pointer
// that is not a manifest path is reported as corrupt metadata before any
// transaction is opened.
func TestCommitManifestUpdates_InvalidBaseManifest(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	_, err := CommitManifestUpdates("not a manifest path", cfg, &ManifestUpdates{
		DeltaLogs: []DeltaLogEntry{{Path: "delta/1", NumEntries: 1}},
	})
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
}

func TestReadManifestColumnGroupEntries(t *testing.T) {
	paramtable.InitWithBaseTable(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	cfg := manifestTestStorageConfig(t)
	basePath := path.Join(cfg.RootPath, "read_column_group_entries/seg1")
	groups := []ColumnGroupEntry{
		{Columns: []string{"100"}, Format: "parquet", Files: []ColumnGroupFileEntry{{
			Path: path.Join(basePath, "input.parquet"), EndIndex: 10,
		}}},
		{Columns: []string{"101", "102"}, Format: "vortex", Files: []ColumnGroupFileEntry{{
			Path: path.Join(basePath, "output.vortex"), StartIndex: 2, EndIndex: 10,
			Properties: map[string]string{"file_size": "1234", "footer_size": "256", "custom_property": "preserved"},
		}}},
	}
	manifest, err := CommitManifestUpdates(MarshalManifestPath(basePath, ManifestEarliest), cfg, &ManifestUpdates{ColumnGroups: groups})
	require.NoError(t, err)

	t.Run("preserve properties after releasing native manifest", func(t *testing.T) {
		entries, err := readManifestColumnGroupEntries(manifest, cfg, []string{"101", "102", "101"})
		require.NoError(t, err)
		require.Equal(t, groups[1:], entries)
		// Commit only Go-owned descriptors after the reader has freed its C handle.
		roundTrip, err := CommitManifestUpdates(MarshalManifestPath(basePath, ManifestEarliest), cfg, &ManifestUpdates{ColumnGroups: entries})
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
	basePath := path.Join(cfg.RootPath, "column_group_entries/seg1")
	writer, err := NewFFIPackedWriter(basePath, schema,
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
		{name: "nil key is skipped", mutate: func() { keys[0] = nil }},
		{name: "nil value is skipped", mutate: func() { values[0] = nil }},
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

func TestRefreshMilvusTableManifestErrorsReturnEmptyResult(t *testing.T) {
	oldManifest := MarshalManifestPath("files/refresh_errors/seg1", 1)
	for _, stage := range []string{"invalid path", "canceled", "columns", "stats", "indexes", "index path", "transaction"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			manifest := oldManifest
			if stage == "invalid path" {
				manifest = "invalid"
			}
			if stage == "canceled" {
				cancel()
			}
			failure := merr.WrapErrStorageMsg("injected %s failure", stage)
			var columns []string
			if stage == "columns" {
				columns = []string{"101"}
				patch := mockey.Mock(readManifestColumnGroupEntries).Return(nil, failure).Build()
				defer patch.UnPatch()
			}
			readPatch := mockey.Mock(GetManifestStats).
				To(func(_ string, _ *indexpb.StorageConfig) (map[string]ManifestStat, error) {
					if stage == "stats" {
						return nil, failure
					}
					return nil, nil
				}).Build()
			defer readPatch.UnPatch()
			indexPatch := mockey.Mock(GetManifestIndexInfos).
				To(func(_ string, _ *indexpb.StorageConfig) ([]ManifestIndexInfo, error) {
					if stage == "indexes" {
						return nil, failure
					}
					if stage == "index path" {
						return []ManifestIndexInfo{{Path: "index"}}, nil
					}
					return nil, nil
				}).Build()
			defer indexPatch.UnPatch()
			if stage == "index path" {
				patch := mockey.Mock(ManifestIndexRelativePath).Return("", failure).Build()
				defer patch.UnPatch()
			}
			commitCalls := 0
			commitPatch := mockey.Mock(createMilvusTableManifest).
				To(func(_ context.Context, _ string, _ []string, _ []Fragment, _ *indexpb.StorageConfig,
					_ ExternalSpecContext, _ *ManifestUpdates,
				) (string, error) {
					commitCalls++
					return "", failure
				}).Build()
			defer commitPatch.UnPatch()
			result, err := RefreshMilvusTableManifest(ctx, manifest, nil, nil, nil, ExternalSpecContext{}, columns, nil)
			switch stage {
			case "invalid path":
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
			case "canceled":
				require.ErrorIs(t, err, context.Canceled)
			default:
				require.ErrorIs(t, err, failure)
			}
			require.Equal(t, MilvusTableRefreshResult{}, result)
			if stage == "transaction" {
				require.Equal(t, 1, commitCalls)
			} else {
				require.Zero(t, commitCalls, "preparation failure must not reach the transaction")
			}
		})
	}
}

func TestRefreshMilvusTableManifestSingleCommit(t *testing.T) {
	for _, mode := range []MilvusTablePrimaryKeyMode{MilvusTablePrimaryKeyModeExternal, MilvusTablePrimaryKeyModeVirtual} {
		for _, withStats := range []bool{false, true} {
			t.Run(fmt.Sprintf("mode_%d_stats_%t", mode, withStats), func(t *testing.T) {
				cfg := manifestTestStorageConfig(t)
				sourceBase := path.Join(cfg.RootPath, "source")
				source := createBaseManifest(t, sourceBase, cfg)
				sourceDelta := path.Join(sourceBase, "_delta/22")
				source, err := AddDeltaLogsToManifestOverwrite(source, cfg, []DeltaLogEntry{{Path: sourceDelta, NumEntries: 2}})
				require.NoError(t, err)
				source, err = AddStatsToManifest(source, cfg, []StatEntry{
					{Key: "bloom_filter.100", Files: []string{path.Join(sourceBase, "_stats/bloom")}, Metadata: map[string]string{"generation": "source"}},
					{Key: "text_index.101", Files: []string{path.Join(sourceBase, "_stats/text")}, Metadata: map[string]string{"version": "99"}},
				})
				require.NoError(t, err)
				base := path.Join(cfg.RootPath, "target")
				updates := &ManifestUpdates{DeltaLogs: []DeltaLogEntry{{Path: path.Join(base, "_delta/old"), NumEntries: 1}}}
				if withStats {
					// Manifest-published indexes live under this segment's _index;
					// only the build/version suffix is stored in each entry.
					for i := int64(0); i < 2; i++ {
						entry := indexEntry(t, base, path.Join(base, ManifestIndexDir, fmt.Sprintf("%d/1", 500+i)), 10+i, 500+i)
						entry.ColumnName = "pk"
						entry.NumRows = 1
						updates.Indexes = append(updates.Indexes, entry)
					}
					updates.Stats = []StatEntry{
						{Key: "bloom_filter.100", Files: []string{path.Join(base, "_stats/bloom")}, Metadata: map[string]string{"generation": "target"}},
						{Key: "text_index.101", Files: []string{path.Join(base, "_stats/text")}, Metadata: map[string]string{"version": "7", "build_id": "8"}},
						{Key: "json_stats.102", Files: []string{path.Join(base, "_stats/json_stats.102/keys")}, Metadata: map[string]string{"version": "9", "build_id": "10", "json_key_stats_data_format": "1"}},
					}
				}
				oldManifest, err := CommitManifestUpdates(MarshalManifestPath(base, ManifestEarliest), cfg, updates)
				require.NoError(t, err)
				_, oldVersion, err := UnmarshalManifestPath(oldManifest)
				require.NoError(t, err)
				oldStats, err := GetManifestStats(oldManifest, cfg)
				require.NoError(t, err)
				oldIndexes, err := GetManifestIndexInfos(oldManifest, cfg)
				require.NoError(t, err)
				// A post-commit stats read must not be necessary for a successful
				// refresh. Fail any read other than the old, retained snapshot.
				reads := 0
				readPatch := mockey.Mock(GetManifestStats).
					To(func(manifest string, _ *indexpb.StorageConfig) (map[string]ManifestStat, error) {
						reads++
						require.Equal(t, oldManifest, manifest)
						if manifest != oldManifest {
							return nil, merr.WrapErrStorageMsg("unexpected post-commit stats read")
						}
						return oldStats, nil
					}).Build()
				defer readPatch.UnPatch()
				newDelta := path.Join(base, "_delta/new")
				result, err := RefreshMilvusTableManifest(context.Background(), oldManifest, []string{"pk", "ts"},
					[]Fragment{{FilePath: source, EndRow: 1, RowCount: 1}}, cfg,
					ExternalSpecContext{MilvusTablePKMode: mode}, nil, []DeltaLogEntry{{Path: newDelta, NumEntries: 3}})
				require.NoError(t, err)
				require.Equal(t, 1, reads)
				readPatch.UnPatch()
				actualBase, version, err := UnmarshalManifestPath(result.ManifestPath)
				require.NoError(t, err)
				require.Equal(t, base, actualBase)
				require.Equal(t, oldVersion+1, version, "one refresh must write exactly one manifest")
				// Read the persisted index section, as startup recovery does, rather
				// than trusting the unchanged in-memory SegmentIndex records.
				indexes, err := GetManifestIndexInfos(result.ManifestPath, cfg)
				require.NoError(t, err)
				require.Equal(t, oldIndexes, indexes, "retain all index metadata and resolved artifact paths")
				paths, err := GetDeltaLogPathsFromManifest(result.ManifestPath, cfg)
				require.NoError(t, err)
				if mode == MilvusTablePrimaryKeyModeExternal {
					require.ElementsMatch(t, []string{newDelta, sourceDelta}, paths)
				} else {
					require.Equal(t, []string{newDelta}, paths, "virtual PK must not import raw source-PK deletes")
				}
				stats, err := GetManifestStats(result.ManifestPath, cfg)
				require.NoError(t, err)
				if mode == MilvusTablePrimaryKeyModeExternal {
					require.Equal(t, "source", stats["bloom_filter.100"].Metadata["generation"])
				} else if withStats {
					require.Equal(t, "target", stats["bloom_filter.100"].Metadata["generation"])
				}
				if withStats {
					require.Equal(t, oldStats["text_index.101"], stats["text_index.101"])
					require.Equal(t, oldStats["json_stats.102"], stats["json_stats.102"])
					require.Contains(t, result.TextStatsLogs, int64(101))
					require.Contains(t, result.JSONKeyStats, int64(102))
					require.EqualValues(t, 7, result.TextStatsLogs[101].Version)
					require.EqualValues(t, 9, result.JSONKeyStats[102].Version)
					require.Equal(t, []string{"keys"}, result.JSONKeyStats[102].Files)
				} else {
					require.NotContains(t, stats, "text_index.101")
					require.Empty(t, result.TextStatsLogs)
					require.Empty(t, result.JSONKeyStats)
				}
				groups, err := readManifestColumnGroupEntries(result.ManifestPath, cfg, []string{"pk", "ts"})
				require.NoError(t, err)
				require.Equal(t, source, groups[0].Files[0].Properties[milvusTableSourceManifestPathProperty])
				_, err = ReadFile(cfg, manifestObjectPath(base, oldVersion))
				require.NoError(t, err, "old published manifest must stay readable")
			})
		}
	}
}

func TestRefreshMilvusTableManifestRetainsRemoteIndexPaths(t *testing.T) {
	base := "files/insert_log/1/2/3"
	oldManifest := MarshalManifestPath(base, 1)
	for _, tc := range []struct {
		name    string
		prefix  string
		wantErr bool
	}{
		{name: "segment rooted", prefix: base + "/_index/500/1"},
		{name: "external namespace", prefix: "files/index_files/500/1", wantErr: true},
		{name: "another segment", prefix: "files/insert_log/1/2/4/_index/500/1", wantErr: true},
		{name: "parent traversal", prefix: base + "/_index/../500/1", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			index := ManifestIndexInfo{
				ColumnName: "vec", IndexID: 10, BuildID: 500,
				Path: tc.prefix, IndexFileKeys: []string{"index.v3"},
			}
			statsPatch := mockey.Mock(GetManifestStats).Return(nil, nil).Build()
			defer statsPatch.UnPatch()
			indexPatch := mockey.Mock(GetManifestIndexInfos).Return([]ManifestIndexInfo{index}, nil).Build()
			defer indexPatch.UnPatch()
			committed := MarshalManifestPath(base, 2)
			commitCalls := 0
			commitPatch := mockey.Mock(createMilvusTableManifest).To(func(_ context.Context, gotBase string,
				_ []string, _ []Fragment, _ *indexpb.StorageConfig, _ ExternalSpecContext, updates *ManifestUpdates,
			) (string, error) {
				commitCalls++
				require.Equal(t, base, gotBase)
				// Remote complete keys are not absolute filesystem paths. Re-encode
				// against this segment's _index without duplicating the root prefix.
				expected := index
				expected.Path = "500/1"
				require.Equal(t, []ManifestIndexInfo{expected}, updates.Indexes)
				return committed, nil
			}).Build()
			defer commitPatch.UnPatch()
			// Keep path validation real: invalid retained entries must fail before
			// opening a transaction, rather than escaping _index through "..".
			result, err := RefreshMilvusTableManifest(context.Background(), oldManifest, nil, nil, nil, ExternalSpecContext{}, nil, nil)
			if tc.wantErr {
				require.ErrorIs(t, err, merr.ErrServiceInternal)
				require.ErrorContains(t, err, "outside segment index directory")
				require.Equal(t, MilvusTableRefreshResult{}, result)
				require.Zero(t, commitCalls)
				return
			}
			require.NoError(t, err)
			require.Equal(t, committed, result.ManifestPath)
			require.Equal(t, 1, commitCalls)
		})
	}
}

func TestCreateMilvusTableManifestRejectsInvalidRetainedGroups(t *testing.T) {
	group := func(column string, rows int64) ColumnGroupEntry {
		return ColumnGroupEntry{Columns: []string{column}, Format: "parquet", Files: []ColumnGroupFileEntry{{Path: "output", EndIndex: rows}}}
	}
	for _, tc := range []struct {
		name   string
		groups []ColumnGroupEntry
	}{
		{name: "overlaps source column", groups: []ColumnGroupEntry{group("pk", 1)}},
		{name: "duplicate output ownership", groups: []ColumnGroupEntry{group("101", 1), group("101", 1)}},
		{name: "row count mismatch", groups: []ColumnGroupEntry{group("101", 2)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Run the real boundary, with no storage configuration: malformed
			// bundles must be rejected before any native transaction is opened.
			manifest, err := createMilvusTableManifest(context.Background(), "target", []string{"pk"},
				[]Fragment{{FilePath: "source", EndRow: 1, RowCount: 1}}, nil, ExternalSpecContext{},
				&ManifestUpdates{ColumnGroups: tc.groups})
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
			require.Empty(t, manifest)
		})
	}
}

func TestRefreshMilvusTableManifestPreparationFailureDoesNotCommit(t *testing.T) {
	for _, stage := range []string{"missing source", "canceled after stats"} {
		t.Run(stage, func(t *testing.T) {
			cfg := manifestTestStorageConfig(t)
			base := path.Join(cfg.RootPath, "target")
			oldManifest := createBaseManifest(t, base, cfg)
			source := createBaseManifest(t, path.Join(cfg.RootPath, "source"), cfg)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if stage == "missing source" {
				source = MarshalManifestPath(path.Join(cfg.RootPath, "missing"), 1)
			} else {
				stats, err := GetManifestStats(oldManifest, cfg)
				require.NoError(t, err)
				readPatch := mockey.Mock(GetManifestStats).
					To(func(_ string, _ *indexpb.StorageConfig) (map[string]ManifestStat, error) {
						cancel()
						return stats, nil
					}).Build()
				defer readPatch.UnPatch()
			}
			result, err := RefreshMilvusTableManifest(ctx, oldManifest, []string{"pk", "ts"},
				[]Fragment{{FilePath: source, EndRow: 1, RowCount: 1}}, cfg, ExternalSpecContext{}, nil, nil)
			require.Error(t, err)
			require.Equal(t, MilvusTableRefreshResult{}, result)
			_, oldVersion, err := UnmarshalManifestPath(oldManifest)
			require.NoError(t, err)
			_, err = ReadFile(cfg, manifestObjectPath(base, oldVersion+1))
			require.Error(t, err, "source preparation/cancellation must not write an intermediate manifest")
			_, err = ReadFile(cfg, manifestObjectPath(base, oldVersion))
			require.NoError(t, err)
		})
	}
}

// TestCommitManifestUpdates_StatsOnly exercises the stats-without-inserts
// path: a stat blob is written to storage and registered in the manifest
// via a single CommitManifestUpdates call.
func TestCommitManifestUpdates_StatsOnly(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	basePath := path.Join(cfg.RootPath, "commit_stats_only/seg1")

	statPath := path.Join(basePath, "_stats/bloom_filter.100/1")
	require.NoError(t, WriteFile(cfg, statPath, []byte("bloom-blob")))

	got, err := CommitManifestUpdates(MarshalManifestPath(basePath, ManifestEarliest), cfg, &ManifestUpdates{
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

	got, err := CommitManifestUpdates(MarshalManifestPath(basePath, ManifestEarliest), cfg, &ManifestUpdates{
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

	got, err := CommitManifestUpdates(MarshalManifestPath(basePath, ManifestEarliest), cfg, &ManifestUpdates{
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

	got, err := CommitManifestUpdates(MarshalManifestPath(basePath, ManifestEarliest), cfg, &ManifestUpdates{
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

	got, err := CommitManifestUpdates(MarshalManifestPath(basePath, ManifestEarliest), cfg,
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
	manifest, err := CommitManifestUpdates(MarshalManifestPath(basePath, ManifestEarliest), cfg, &ManifestUpdates{NewFiles: out})
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

// TestCommitManifestUpdates_DropColumns exercises the loon_transaction_drop_column
// branch of applyManifestUpdates: a column dropped from a manifest is removed
// from the resulting field set, and a replace (drop + re-add of the same
// column in one transaction) keeps the column while advancing one revision.
func TestCommitManifestUpdates_DropColumns(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	basePath := path.Join(cfg.RootPath, "commit_drop_columns/seg1")

	// Seed a manifest with two single-field column groups (100 and 101).
	schema := arrow.NewSchema([]arrow.Field{
		{
			Name: "100", Type: arrow.PrimitiveTypes.Int64, Nullable: true,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"100"}),
		},
		{
			Name: "101", Type: arrow.PrimitiveTypes.Int64, Nullable: true,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"101"}),
		},
	}, nil)
	columnGroups := []storagecommon.ColumnGroup{
		{Columns: []int{0}, Fields: []int64{100}, GroupID: 100},
		{Columns: []int{1}, Fields: []int64{101}, GroupID: 101},
	}
	w, err := NewFFIPackedWriter(basePath, schema, columnGroups, cfg, nil)
	require.NoError(t, err)
	w.AsNewColumnGroups()
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(int64(1))
	b.Field(1).(*array.Int64Builder).Append(int64(2))
	rec := b.NewRecord()
	defer rec.Release()
	require.NoError(t, w.WriteRecordBatch(rec))
	out, err := w.Close()
	require.NoError(t, err)
	defer out.Destroy()
	cgs, ok := out.(*ColumnGroups)
	require.True(t, ok, "Close should return *ColumnGroups for FFIPackedWriter")
	seedEntries, err := cgs.ColumnGroupEntries()
	require.NoError(t, err)
	require.Len(t, seedEntries, 2)

	base, err := CommitManifestUpdates(MarshalManifestPath(basePath, ManifestEarliest), cfg, &ManifestUpdates{NewFiles: out})
	require.NoError(t, err)
	fields, err := GetManifestFieldIDs(base, cfg)
	require.NoError(t, err)
	require.Contains(t, fields, int64(100))
	require.Contains(t, fields, int64(101))

	// Replace: drop field 100 and re-register it in one transaction (the
	// backfill "replace" shape) -> both fields survive, version advances once.
	replaced, err := CommitManifestUpdates(MarshalManifestPath(basePath, 1), cfg, &ManifestUpdates{
		DropColumns: []string{"100"},
		ColumnGroups: []ColumnGroupEntry{{
			Columns: []string{"100"},
			Format:  "parquet",
			Files: []ColumnGroupFileEntry{{
				Path:       seedEntries[0].Files[0].Path,
				StartIndex: 0,
				EndIndex:   1,
			}},
		}},
	})
	require.NoError(t, err)
	_, version, err := UnmarshalManifestPath(replaced)
	require.NoError(t, err)
	require.Equal(t, int64(2), version)
	fields, err = GetManifestFieldIDs(replaced, cfg)
	require.NoError(t, err)
	require.Contains(t, fields, int64(100))
	require.Contains(t, fields, int64(101))

	// Drop: removing a column from the latest revision retracts it from the
	// field set.
	dropped, err := CommitManifestUpdates(MarshalManifestPath(basePath, 2), cfg, &ManifestUpdates{
		DropColumns: []string{"100"},
	})
	require.NoError(t, err)
	_, version, err = UnmarshalManifestPath(dropped)
	require.NoError(t, err)
	require.Equal(t, int64(3), version)
	fields, err = GetManifestFieldIDs(dropped, cfg)
	require.NoError(t, err)
	require.NotContains(t, fields, int64(100))
	require.Contains(t, fields, int64(101))
}

// TestCommitManifestUpdates_DropColumns_AbsentColumnAndIndexAutoDrop covers the
// remaining DropColumns semantics: dropping a column that is absent is a no-op
// that must not break a transaction carrying other work, and dropping a column
// auto-drops the indexes attached to it.
func TestCommitManifestUpdates_DropColumns_AbsentColumnAndIndexAutoDrop(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	basePath := path.Join(cfg.RootPath, "commit_drop_columns_extra/seg1")

	// Seed a manifest with a single column group (100).
	schema := arrow.NewSchema([]arrow.Field{
		{
			Name: "100", Type: arrow.PrimitiveTypes.Int64, Nullable: true,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"100"}),
		},
	}, nil)
	columnGroups := []storagecommon.ColumnGroup{
		{Columns: []int{0}, Fields: []int64{100}, GroupID: 100},
	}
	w, err := NewFFIPackedWriter(basePath, schema, columnGroups, cfg, nil)
	require.NoError(t, err)
	w.AsNewColumnGroups()
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(int64(1))
	rec := b.NewRecord()
	defer rec.Release()
	require.NoError(t, w.WriteRecordBatch(rec))
	out, err := w.Close()
	require.NoError(t, err)
	defer out.Destroy()

	base, err := CommitManifestUpdates(MarshalManifestPath(basePath, ManifestEarliest), cfg, &ManifestUpdates{NewFiles: out})
	require.NoError(t, err)
	_ = base

	// Dropping an absent column alongside real work is tolerated (no-op drop);
	// the real work here is registering a brand-new column group 102.
	got, err := CommitManifestUpdates(MarshalManifestPath(basePath, 1), cfg, &ManifestUpdates{
		DropColumns: []string{"999"},
		ColumnGroups: []ColumnGroupEntry{{
			Columns: []string{"102"},
			Format:  "parquet",
			Files: []ColumnGroupFileEntry{{
				Path:       "_data/102_new.parquet",
				StartIndex: 0,
				EndIndex:   1,
			}},
		}},
	})
	require.NoError(t, err)
	fields, err := GetManifestFieldIDs(got, cfg)
	require.NoError(t, err)
	require.Contains(t, fields, int64(100))
	require.Contains(t, fields, int64(102))

	// Seed an index on column 100, then drop the column: the index must be
	// auto-dropped with it.
	indexed, err := CommitManifestUpdates(MarshalManifestPath(basePath, 2), cfg, &ManifestUpdates{
		Indexes: []ManifestIndexInfo{{
			ColumnName:                "100",
			IndexName:                 "vec_index",
			IndexType:                 "HNSW",
			Path:                      "_index/vec.idx",
			FieldID:                   100,
			IndexID:                   10,
			BuildID:                   500,
			IndexVersion:              1,
			NumRows:                   1,
			SerializedSize:            2048,
			MemSize:                   4096,
			CurrentIndexVersion:       5,
			CurrentScalarIndexVersion: 6,
			IndexStorePathVersion:     indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
			IndexFileKeys:             []string{"0"},
		}},
	})
	require.NoError(t, err)
	indexes, err := GetManifestIndexInfos(indexed, cfg)
	require.NoError(t, err)
	require.Len(t, indexes, 1)
	require.Equal(t, "100", indexes[0].ColumnName)

	dropped, err := CommitManifestUpdates(MarshalManifestPath(basePath, 3), cfg, &ManifestUpdates{
		DropColumns: []string{"100"},
	})
	require.NoError(t, err)
	indexes, err = GetManifestIndexInfos(dropped, cfg)
	require.NoError(t, err)
	require.Empty(t, indexes, "dropping the column must auto-drop its index")
	fields, err = GetManifestFieldIDs(dropped, cfg)
	require.NoError(t, err)
	require.NotContains(t, fields, int64(100))
}

// TestReadManifestColumnGroups round-trips column-group descriptors through the
// real manifest FFI: a group written with absolute <base>/_data/<file> paths
// must read back with those absolute paths, so pre-commit descriptor
// comparison (backfill delta replay detection) can match op files exactly.
func TestReadManifestColumnGroups(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	basePath := path.Join(cfg.RootPath, "read_column_groups/seg1")
	dataPath := path.Join(basePath, "_data", "100_a.parquet")

	committed, err := CommitManifestUpdates(MarshalManifestPath(basePath, ManifestEarliest), cfg, &ManifestUpdates{
		ColumnGroups: []ColumnGroupEntry{{
			Columns: []string{"100"},
			Format:  "parquet",
			Files:   []ColumnGroupFileEntry{{Path: dataPath, StartIndex: 0, EndIndex: 5}},
		}},
	})
	require.NoError(t, err)

	groups, err := ReadManifestColumnGroups(committed, cfg)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	require.Equal(t, []string{"100"}, groups[0].Columns)
	require.Equal(t, "parquet", groups[0].Format)
	require.Len(t, groups[0].Files, 1)
	require.Equal(t, dataPath, groups[0].Files[0].Path)
	require.Equal(t, int64(0), groups[0].Files[0].StartIndex)
	require.Equal(t, int64(5), groups[0].Files[0].EndIndex)
}
