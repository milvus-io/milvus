// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package packed

import (
	"fmt"
	"path/filepath"
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

func TestDeltaLogEntry(t *testing.T) {
	entry := DeltaLogEntry{
		Path:       "/data/delta_log/123/456/789/1",
		NumEntries: 100,
	}

	assert.Equal(t, "/data/delta_log/123/456/789/1", entry.Path)
	assert.Equal(t, int64(100), entry.NumEntries)
}

// createBaseManifest creates a base manifest via FFIPackedWriter for delta log tests.
// basePath should follow production pattern: filepath.Join(rootPath, "insert_log/collID/partID/segID")
func createBaseManifest(t *testing.T, basePath string, storageConfig *indexpb.StorageConfig) string {
	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "pk",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"100"}),
		},
		{
			Name:     "ts",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"101"}),
		},
	}, nil)

	columnGroups := []storagecommon.ColumnGroup{
		{Columns: []int{0, 1}, GroupID: storagecommon.DefaultShortColumnGroupID},
	}

	pw, err := NewFFIPackedWriter(basePath, 0, schema, columnGroups, storageConfig, nil)
	require.NoError(t, err)

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	b.Field(1).(*array.Int64Builder).Append(1000)
	rec := b.NewRecord()
	defer rec.Release()

	err = pw.WriteRecordBatch(rec)
	require.NoError(t, err)

	manifestPath, err := pw.Close()
	require.NoError(t, err)
	return manifestPath
}

func TestStatsRoundtrip(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	dir := t.TempDir()
	pt.Save(pt.LocalStorageCfg.Path.Key, dir)
	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.LocalStorageCfg.Path.Key)
	})

	storageConfig := &indexpb.StorageConfig{
		RootPath:    dir,
		StorageType: "local",
	}

	t.Run("no stats", func(t *testing.T) {
		bp := filepath.Join(dir, "insert_log/1/2/3_no_stats")
		manifestPath := createBaseManifest(t, bp, storageConfig)

		stats, err := GetManifestStats(manifestPath, storageConfig)
		require.NoError(t, err)
		assert.Empty(t, stats)
	})

	t.Run("single bloom filter stat", func(t *testing.T) {
		bp := filepath.Join(dir, "insert_log/1/2/3_bf")
		manifestPath := createBaseManifest(t, bp, storageConfig)

		statPath := filepath.Join(bp, "_stats/bloom_filter.100/1")
		newManifest, err := AddStatsToManifest(manifestPath, storageConfig, []StatEntry{
			{
				Key:      "bloom_filter.100",
				Files:    []string{statPath},
				Metadata: map[string]string{"memory_size": "4096"},
			},
		})
		require.NoError(t, err)

		stats, err := GetManifestStats(newManifest, storageConfig)
		require.NoError(t, err)
		require.Contains(t, stats, "bloom_filter.100")

		stat := stats["bloom_filter.100"]
		require.Equal(t, 1, len(stat.Paths))
		assert.Equal(t, statPath, stat.Paths[0])
		assert.Equal(t, "4096", stat.Metadata["memory_size"])
	})

	t.Run("multiple stats with metadata", func(t *testing.T) {
		bp := filepath.Join(dir, "insert_log/1/2/3_multi_stats")
		manifestPath := createBaseManifest(t, bp, storageConfig)

		entries := []StatEntry{
			{
				Key:      "bloom_filter.100",
				Files:    []string{filepath.Join(bp, "_stats/bloom_filter.100/1")},
				Metadata: map[string]string{"memory_size": "2048"},
			},
			{
				Key:   "bm25.200",
				Files: []string{filepath.Join(bp, "_stats/bm25.200/1")},
			},
			{
				Key:   "text_index.300",
				Files: []string{filepath.Join(bp, "_stats/text_index.300/f1"), filepath.Join(bp, "_stats/text_index.300/f2")},
				Metadata: map[string]string{
					"version":  "5",
					"build_id": "42",
				},
			},
		}
		newManifest, err := AddStatsToManifest(manifestPath, storageConfig, entries)
		require.NoError(t, err)

		stats, err := GetManifestStats(newManifest, storageConfig)
		require.NoError(t, err)
		assert.Equal(t, 3, len(stats))

		// bloom filter
		bf := stats["bloom_filter.100"]
		require.Equal(t, 1, len(bf.Paths))
		assert.Equal(t, filepath.Join(bp, "_stats/bloom_filter.100/1"), bf.Paths[0])
		assert.Equal(t, "2048", bf.Metadata["memory_size"])

		// bm25
		bm := stats["bm25.200"]
		require.Equal(t, 1, len(bm.Paths))
		assert.Equal(t, filepath.Join(bp, "_stats/bm25.200/1"), bm.Paths[0])

		// text index with multiple files
		ti := stats["text_index.300"]
		require.Equal(t, 2, len(ti.Paths))
		assert.Equal(t, filepath.Join(bp, "_stats/text_index.300/f1"), ti.Paths[0])
		assert.Equal(t, filepath.Join(bp, "_stats/text_index.300/f2"), ti.Paths[1])
		assert.Equal(t, "5", ti.Metadata["version"])
		assert.Equal(t, "42", ti.Metadata["build_id"])
	})

	t.Run("empty stats input returns original manifest", func(t *testing.T) {
		bp := filepath.Join(dir, "insert_log/1/2/3_empty_stats")
		manifestPath := createBaseManifest(t, bp, storageConfig)

		sameManifest, err := AddStatsToManifest(manifestPath, storageConfig, []StatEntry{})
		require.NoError(t, err)
		assert.Equal(t, manifestPath, sameManifest)
	})
}

// TestStatsUpdateReplacesEntry verifies that calling AddStatsToManifest
// with the same key in separate transactions replaces (not appends) files.
// This is the underlying behavior that caused the multi-batch import bug:
// each batch overwrote the previous batch's bloom filter entry.
func TestStatsUpdateReplacesEntry(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	dir := t.TempDir()
	pt.Save(pt.LocalStorageCfg.Path.Key, dir)
	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.LocalStorageCfg.Path.Key)
	})

	storageConfig := &indexpb.StorageConfig{
		RootPath:    dir,
		StorageType: "local",
	}

	bp := filepath.Join(dir, "insert_log/1/2/3_replace_test")
	manifestPath := createBaseManifest(t, bp, storageConfig)

	// Transaction 1: write bloom_filter.100 with file1
	file1 := filepath.Join(bp, "_stats/bloom_filter.100/1001")
	m1, err := AddStatsToManifest(manifestPath, storageConfig, []StatEntry{
		{
			Key:      "bloom_filter.100",
			Files:    []string{file1},
			Metadata: map[string]string{"memory_size": "100"},
		},
	})
	require.NoError(t, err)

	// Transaction 2: write bloom_filter.100 with file2 only (simulating naive per-batch write)
	file2 := filepath.Join(bp, "_stats/bloom_filter.100/1002")
	m2, err := AddStatsToManifest(m1, storageConfig, []StatEntry{
		{
			Key:      "bloom_filter.100",
			Files:    []string{file2},
			Metadata: map[string]string{"memory_size": "200"},
		},
	})
	require.NoError(t, err)

	// Verify: only file2 survives (update_stat replaces the entry)
	stats, err := GetManifestStats(m2, storageConfig)
	require.NoError(t, err)
	bf := stats["bloom_filter.100"]
	assert.Equal(t, 1, len(bf.Paths), "update_stat should replace, leaving only the last write's file")
	assert.Equal(t, file2, bf.Paths[0])
	assert.Equal(t, "200", bf.Metadata["memory_size"])
}

// TestStatsAccumulationAcrossTransactions verifies the fix: when files
// from the previous manifest are merged before update, all files survive.
func TestStatsAccumulationAcrossTransactions(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	dir := t.TempDir()
	pt.Save(pt.LocalStorageCfg.Path.Key, dir)
	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.LocalStorageCfg.Path.Key)
	})

	storageConfig := &indexpb.StorageConfig{
		RootPath:    dir,
		StorageType: "local",
	}

	bp := filepath.Join(dir, "insert_log/1/2/3_accum_test")
	manifestPath := createBaseManifest(t, bp, storageConfig)

	// Simulate 3 import batches with the accumulation fix:
	// Before each update, read existing files and merge.
	currentManifest := manifestPath
	allBloomFiles := []string{}
	allBM25Files := []string{}

	for batch := 0; batch < 3; batch++ {
		bfFile := filepath.Join(bp, fmt.Sprintf("_stats/bloom_filter.100/%d", 1001+batch))
		bm25File := filepath.Join(bp, fmt.Sprintf("_stats/bm25.200/%d", 2001+batch))

		// Read existing stats from current manifest
		existingStats, err := GetManifestStats(currentManifest, storageConfig)
		require.NoError(t, err)

		// Merge existing bloom filter files
		bfFiles := []string{}
		if existing, ok := existingStats["bloom_filter.100"]; ok {
			bfFiles = append(bfFiles, existing.Paths...)
		}
		bfFiles = append(bfFiles, bfFile)
		allBloomFiles = append(allBloomFiles, bfFile)

		// Merge existing BM25 files
		bmFiles := []string{}
		if existing, ok := existingStats["bm25.200"]; ok {
			bmFiles = append(bmFiles, existing.Paths...)
		}
		bmFiles = append(bmFiles, bm25File)
		allBM25Files = append(allBM25Files, bm25File)

		newManifest, err := AddStatsToManifest(currentManifest, storageConfig, []StatEntry{
			{
				Key:      "bloom_filter.100",
				Files:    bfFiles,
				Metadata: map[string]string{"memory_size": fmt.Sprintf("%d", (batch+1)*100)},
			},
			{
				Key:   "bm25.200",
				Files: bmFiles,
			},
		})
		require.NoError(t, err)
		currentManifest = newManifest
	}

	// Verify: all 3 bloom filter files and 3 BM25 files survive
	stats, err := GetManifestStats(currentManifest, storageConfig)
	require.NoError(t, err)

	bf := stats["bloom_filter.100"]
	require.Equal(t, 3, len(bf.Paths), "bloom filter should have files from all 3 batches")
	assert.Equal(t, allBloomFiles, bf.Paths)
	assert.Equal(t, "300", bf.Metadata["memory_size"])

	bm := stats["bm25.200"]
	require.Equal(t, 3, len(bm.Paths), "bm25 stats should have files from all 3 batches")
	assert.Equal(t, allBM25Files, bm.Paths)
}

func TestGetDeltaLogPathsFromManifest(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	dir := t.TempDir()
	pt.Save(pt.LocalStorageCfg.Path.Key, dir)
	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.LocalStorageCfg.Path.Key)
	})

	storageConfig := &indexpb.StorageConfig{
		RootPath:    dir,
		StorageType: "local",
	}

	// Use basePath that includes rootPath, matching production pattern:
	// basePath = path.Join(storageConfig.GetRootPath(), "insert_log", collID, partID, segID)
	basePath := filepath.Join(dir, "insert_log/1/2/3")

	t.Run("no delta logs", func(t *testing.T) {
		manifestPath := createBaseManifest(t, basePath, storageConfig)
		paths, err := GetDeltaLogPathsFromManifest(manifestPath, storageConfig)
		assert.NoError(t, err)
		assert.Nil(t, paths)
	})

	t.Run("single delta log", func(t *testing.T) {
		bp := filepath.Join(dir, "insert_log/1/2/3_single")
		manifestPath := createBaseManifest(t, bp, storageConfig)

		// deltaPath follows production pattern: path.Join(rootPath, "delta_log", collID, partID, segID, logID)
		deltaFullPath := filepath.Join(dir, "delta_log/1/2/3/101")
		newManifest, err := AddDeltaLogsToManifest(manifestPath, storageConfig, []DeltaLogEntry{
			{Path: deltaFullPath, NumEntries: 5},
		})
		require.NoError(t, err)

		paths, err := GetDeltaLogPathsFromManifest(newManifest, storageConfig)
		assert.NoError(t, err)
		require.Equal(t, 1, len(paths))
		// Verify the returned path resolves to the correct delta log location
		assert.Contains(t, paths[0], "delta_log/1/2/3/101")
	})

	t.Run("multiple delta logs", func(t *testing.T) {
		bp := filepath.Join(dir, "insert_log/1/2/3_multi")
		manifestPath := createBaseManifest(t, bp, storageConfig)

		deltaLogs := []DeltaLogEntry{
			{Path: filepath.Join(dir, "delta_log/1/2/3/201"), NumEntries: 5},
			{Path: filepath.Join(dir, "delta_log/1/2/3/202"), NumEntries: 3},
		}
		newManifest, err := AddDeltaLogsToManifest(manifestPath, storageConfig, deltaLogs)
		require.NoError(t, err)

		paths, err := GetDeltaLogPathsFromManifest(newManifest, storageConfig)
		assert.NoError(t, err)
		require.Equal(t, 2, len(paths))
		assert.Contains(t, paths[0], "delta_log/1/2/3/201")
		assert.Contains(t, paths[1], "delta_log/1/2/3/202")
	})

	t.Run("v3 delta log under basePath/_delta/", func(t *testing.T) {
		bp := filepath.Join(dir, "insert_log/1/2/3_v3delta")
		manifestPath := createBaseManifest(t, bp, storageConfig)

		// V3 deltaPath: basePath/_delta/{logID}
		deltaFullPath := filepath.Join(bp, "_delta/501")
		newManifest, err := AddDeltaLogsToManifest(manifestPath, storageConfig, []DeltaLogEntry{
			{Path: deltaFullPath, NumEntries: 8},
		})
		require.NoError(t, err)

		paths, err := GetDeltaLogPathsFromManifest(newManifest, storageConfig)
		assert.NoError(t, err)
		require.Equal(t, 1, len(paths))
		assert.Contains(t, paths[0], "_delta/501")
	})

	t.Run("empty deltaLogs input returns original manifest", func(t *testing.T) {
		manifestPath := createBaseManifest(t, basePath+"_empty", storageConfig)
		sameManifest, err := AddDeltaLogsToManifest(manifestPath, storageConfig, []DeltaLogEntry{})
		require.NoError(t, err)
		assert.Equal(t, manifestPath, sameManifest)
	})
}
