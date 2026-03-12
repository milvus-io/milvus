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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestParseStatKey(t *testing.T) {
	t.Run("valid bloom_filter key", func(t *testing.T) {
		prefix, fieldID, ok := parseStatKey("bloom_filter.100")
		assert.True(t, ok)
		assert.Equal(t, "bloom_filter", prefix)
		assert.Equal(t, int64(100), fieldID)
	})

	t.Run("valid bm25 key", func(t *testing.T) {
		prefix, fieldID, ok := parseStatKey("bm25.200")
		assert.True(t, ok)
		assert.Equal(t, "bm25", prefix)
		assert.Equal(t, int64(200), fieldID)
	})

	t.Run("valid text_index key", func(t *testing.T) {
		prefix, fieldID, ok := parseStatKey("text_index.50")
		assert.True(t, ok)
		assert.Equal(t, "text_index", prefix)
		assert.Equal(t, int64(50), fieldID)
	})

	t.Run("valid json_key_index key", func(t *testing.T) {
		prefix, fieldID, ok := parseStatKey("json_key_index.30")
		assert.True(t, ok)
		assert.Equal(t, "json_key_index", prefix)
		assert.Equal(t, int64(30), fieldID)
	})

	t.Run("invalid key no dot", func(t *testing.T) {
		_, _, ok := parseStatKey("bloom_filter")
		assert.False(t, ok)
	})

	t.Run("invalid key non-numeric fieldID", func(t *testing.T) {
		_, _, ok := parseStatKey("bloom_filter.abc")
		assert.False(t, ok)
	})

	t.Run("empty string", func(t *testing.T) {
		_, _, ok := parseStatKey("")
		assert.False(t, ok)
	})
}

func TestFilterPKStatsBinlogs(t *testing.T) {
	t.Run("matching fieldID", func(t *testing.T) {
		binlogs := []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{LogPath: "path/100"},
					{LogPath: "path/200"},
				},
			},
			{
				FieldID: 200,
				Binlogs: []*datapb.Binlog{
					{LogPath: "path/300"},
				},
			},
		}
		paths := filterPKStatsBinlogs(binlogs, 100)
		assert.Equal(t, []string{"path/100", "path/200"}, paths)
	})

	t.Run("non-matching fieldID", func(t *testing.T) {
		binlogs := []*datapb.FieldBinlog{
			{
				FieldID: 200,
				Binlogs: []*datapb.Binlog{{LogPath: "path/100"}},
			},
		}
		paths := filterPKStatsBinlogs(binlogs, 100)
		assert.Empty(t, paths)
	})

	t.Run("CompoundStatsType returns single path", func(t *testing.T) {
		binlogs := []*datapb.FieldBinlog{
			{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{
					{LogPath: "path/0"},
					{LogPath: "path/" + compoundStatsLogIdx},
				},
			},
		}
		paths := filterPKStatsBinlogs(binlogs, 100)
		assert.Len(t, paths, 1)
		assert.Contains(t, paths[0], compoundStatsLogIdx)
	})

	t.Run("empty input", func(t *testing.T) {
		paths := filterPKStatsBinlogs(nil, 100)
		assert.Empty(t, paths)
	})
}

func TestFilterBM25Stats(t *testing.T) {
	t.Run("multiple fields", func(t *testing.T) {
		binlogs := []*datapb.FieldBinlog{
			{
				FieldID: 10,
				Binlogs: []*datapb.Binlog{
					{LogPath: "bm25/10/100"},
					{LogPath: "bm25/10/200"},
				},
			},
			{
				FieldID: 20,
				Binlogs: []*datapb.Binlog{
					{LogPath: "bm25/20/100"},
				},
			},
		}
		result := filterBM25Stats(binlogs)
		assert.Len(t, result, 2)
		assert.Equal(t, []string{"bm25/10/100", "bm25/10/200"}, result[10])
		assert.Equal(t, []string{"bm25/20/100"}, result[20])
	})

	t.Run("CompoundStatsType in BM25", func(t *testing.T) {
		binlogs := []*datapb.FieldBinlog{
			{
				FieldID: 10,
				Binlogs: []*datapb.Binlog{
					{LogPath: "bm25/10/100"},
					{LogPath: "bm25/10/" + compoundStatsLogIdx},
				},
			},
		}
		result := filterBM25Stats(binlogs)
		assert.Len(t, result[10], 1)
		assert.Contains(t, result[10][0], compoundStatsLogIdx)
	})

	t.Run("empty input", func(t *testing.T) {
		result := filterBM25Stats(nil)
		assert.Empty(t, result)
	})
}

func TestStatsResolverLegacy(t *testing.T) {
	statslogs := []*datapb.FieldBinlog{
		{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{
				{LogPath: "stats/100/10", MemorySize: 1024},
				{LogPath: "stats/100/20", MemorySize: 2048},
			},
		},
	}
	bm25Logs := []*datapb.FieldBinlog{
		{
			FieldID: 50,
			Binlogs: []*datapb.Binlog{
				{LogPath: "bm25/50/10"},
			},
		},
	}
	textStats := map[int64]*datapb.TextIndexStats{
		10: {FieldID: 10, Version: 1, Files: []string{"text/10/f1"}},
	}
	jsonStats := map[int64]*datapb.JsonKeyStats{
		20: {FieldID: 20, Version: 1, Files: []string{"json/20/f1"}},
	}

	resolver := NewStatsResolver("", nil).
		WithStatslogs(statslogs).
		WithBM25Logs(bm25Logs).
		WithTextStatsLogs(textStats).
		WithJSONKeyStats(jsonStats)

	t.Run("isManifest", func(t *testing.T) {
		assert.False(t, resolver.isManifest())
	})

	t.Run("BloomFilterPaths", func(t *testing.T) {
		paths, err := resolver.BloomFilterPaths(100)
		assert.NoError(t, err)
		assert.Equal(t, []string{"stats/100/10", "stats/100/20"}, paths)
	})

	t.Run("BloomFilterPaths non-matching", func(t *testing.T) {
		paths, err := resolver.BloomFilterPaths(999)
		assert.NoError(t, err)
		assert.Empty(t, paths)
	})

	t.Run("BloomFilterMemorySize", func(t *testing.T) {
		memSize, err := resolver.BloomFilterMemorySize(100)
		assert.NoError(t, err)
		assert.Equal(t, int64(3072), memSize)
	})

	t.Run("BloomFilterMemorySize non-matching", func(t *testing.T) {
		memSize, err := resolver.BloomFilterMemorySize(999)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), memSize)
	})

	t.Run("BM25StatsPaths", func(t *testing.T) {
		paths, err := resolver.BM25StatsPaths()
		assert.NoError(t, err)
		assert.Equal(t, []string{"bm25/50/10"}, paths[50])
	})

	t.Run("TextAndJSONIndexStats", func(t *testing.T) {
		text, json, err := resolver.TextAndJSONIndexStats()
		assert.NoError(t, err)
		assert.Equal(t, textStats, text)
		assert.Equal(t, jsonStats, json)
	})
}

// TestStatsResolverManifest exercises the V3 manifest-based code path
// end-to-end: write stats to a manifest via AddStatsToManifest, then
// read them back through StatsResolver and verify exact paths.
func TestStatsResolverManifest(t *testing.T) {
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

	bp := filepath.Join(dir, "insert_log/1/2/3_resolver")
	manifestPath := createBaseManifest(t, bp, storageConfig)

	// Add bloom filter + BM25 stats to manifest
	bfPath := filepath.Join(bp, "_stats/bloom_filter.100/42")
	bm25Path := filepath.Join(bp, "_stats/bm25.200/43")
	newManifest, err := AddStatsToManifest(manifestPath, storageConfig, []StatEntry{
		{
			Key:      "bloom_filter.100",
			Files:    []string{bfPath},
			Metadata: map[string]string{"memory_size": "8192"},
		},
		{
			Key:   "bm25.200",
			Files: []string{bm25Path},
		},
	})
	require.NoError(t, err)

	// Use StatsResolver (V3 path) to read back
	resolver := NewStatsResolver(newManifest, storageConfig)
	assert.True(t, resolver.isManifest())

	t.Run("BloomFilterPaths returns exact path", func(t *testing.T) {
		paths, err := resolver.BloomFilterPaths(100)
		require.NoError(t, err)
		require.Equal(t, 1, len(paths))
		assert.Equal(t, bfPath, paths[0])
	})

	t.Run("BloomFilterMemorySize", func(t *testing.T) {
		memSize, err := resolver.BloomFilterMemorySize(100)
		require.NoError(t, err)
		assert.Equal(t, int64(8192), memSize)
	})

	t.Run("BloomFilterPaths non-matching fieldID", func(t *testing.T) {
		paths, err := resolver.BloomFilterPaths(999)
		require.NoError(t, err)
		assert.Nil(t, paths)
	})

	t.Run("BM25StatsPaths returns exact path", func(t *testing.T) {
		paths, err := resolver.BM25StatsPaths()
		require.NoError(t, err)
		require.Equal(t, 1, len(paths[200]))
		assert.Equal(t, bm25Path, paths[200][0])
	})
}
