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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// indexEntry builds a manifest index entry whose Path is expressed the way
// DataCoord expresses it: relative to <basePath>/_index, walking back out to
// the legacy index layout that lives outside the segment directory.
func indexEntry(t *testing.T, basePath, indexPrefix string, indexID, buildID int64) ManifestIndexInfo {
	t.Helper()
	relativePath, err := filepath.Rel(path.Join(basePath, "_index"), indexPrefix)
	require.NoError(t, err)
	return ManifestIndexInfo{
		ColumnName:                "vec",
		IndexName:                 "vec_index",
		IndexType:                 "HNSW",
		Path:                      relativePath,
		FieldID:                   101,
		IndexID:                   indexID,
		BuildID:                   buildID,
		IndexVersion:              1,
		NumRows:                   1000,
		SerializedSize:            2048,
		MemSize:                   4096,
		CurrentIndexVersion:       5,
		CurrentScalarIndexVersion: 6,
		IndexStorePathVersion:     indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
		IndexFileKeys:             []string{"0", "1"},
		Properties:                map[string]string{"index_type": "HNSW", "metric_type": "L2"},
	}
}

// TestCommitManifestUpdates_IndexRoundTrip exercises the real FFI: an index
// entry written with a manifest-relative path must read back resolved to its
// absolute legacy prefix, with every scalar field intact.
func TestCommitManifestUpdates_IndexRoundTrip(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	basePath := "files/commit_index_roundtrip/seg1"
	indexPrefix := "files/index_v1/1/2/3/500/1"

	committed, err := CommitManifestUpdates(basePath, ManifestEarliest, cfg, &ManifestUpdates{
		Indexes: []ManifestIndexInfo{indexEntry(t, basePath, indexPrefix, 10, 500)},
	})
	require.NoError(t, err)

	indexes, err := GetManifestIndexInfos(committed, cfg)
	require.NoError(t, err)
	require.Len(t, indexes, 1)
	got := indexes[0]

	// The `..` walk is resolved on read, so callers can join file keys onto
	// Path directly and reach the artifact bytes.
	require.Equal(t, indexPrefix, got.Path)
	require.Equal(t, "vec", got.ColumnName)
	require.Equal(t, "vec_index", got.IndexName)
	require.Equal(t, "HNSW", got.IndexType)
	require.EqualValues(t, 101, got.FieldID)
	require.EqualValues(t, 10, got.IndexID)
	require.EqualValues(t, 500, got.BuildID)
	require.EqualValues(t, 1, got.IndexVersion)
	require.EqualValues(t, 1000, got.NumRows)
	require.EqualValues(t, 2048, got.SerializedSize)
	require.EqualValues(t, 4096, got.MemSize)
	require.EqualValues(t, 5, got.CurrentIndexVersion)
	require.EqualValues(t, 6, got.CurrentScalarIndexVersion)
	require.Equal(t, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED, got.IndexStorePathVersion)
	require.Equal(t, []string{"0", "1"}, got.IndexFileKeys)
	require.Equal(t, map[string]string{"index_type": "HNSW", "metric_type": "L2"}, got.Properties)
}

// A rebuild republishes the same index_id under a new build. milvus-storage
// replaces the entry rather than accumulating both, which is what lets
// DataCoord publish a rebuild without an explicit drop.
func TestCommitManifestUpdates_IndexRepublishReplaces(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	basePath := "files/commit_index_republish/seg1"

	first, err := CommitManifestUpdates(basePath, ManifestEarliest, cfg, &ManifestUpdates{
		Indexes: []ManifestIndexInfo{indexEntry(t, basePath, "files/index_v1/1/2/3/500/1", 10, 500)},
	})
	require.NoError(t, err)
	_, version, err := UnmarshalManifestPath(first)
	require.NoError(t, err)

	second, err := CommitManifestUpdates(basePath, version, cfg, &ManifestUpdates{
		Indexes: []ManifestIndexInfo{indexEntry(t, basePath, "files/index_v1/1/2/3/501/2", 10, 501)},
	})
	require.NoError(t, err)

	indexes, err := GetManifestIndexInfos(second, cfg)
	require.NoError(t, err)
	require.Len(t, indexes, 1)
	require.EqualValues(t, 501, indexes[0].BuildID)
	require.Equal(t, "files/index_v1/1/2/3/501/2", indexes[0].Path)
}

func TestCommitManifestUpdates_DropIndex(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	basePath := "files/commit_index_drop/seg1"

	published, err := CommitManifestUpdates(basePath, ManifestEarliest, cfg, &ManifestUpdates{
		Indexes: []ManifestIndexInfo{
			indexEntry(t, basePath, "files/index_v1/1/2/3/500/1", 10, 500),
			indexEntry(t, basePath, "files/index_v1/1/2/3/600/1", 11, 600),
		},
	})
	require.NoError(t, err)
	_, version, err := UnmarshalManifestPath(published)
	require.NoError(t, err)

	t.Run("rejects a build the manifest no longer holds", func(t *testing.T) {
		// GC collected build 499's metadata; a rebuild already published 500
		// under the same index. Dropping by index_id would delete the live one.
		_, err := CommitManifestUpdates(basePath, version, cfg, &ManifestUpdates{
			DropIndexes: []DropIndexEntry{{IndexID: 10, ExpectedBuildID: 499}},
		})
		require.Error(t, err)
	})

	t.Run("absent index is a no-op, not an empty commit", func(t *testing.T) {
		// loon refuses a transaction with zero staged ops, so a retry of an
		// already-completed drop must not open one.
		got, err := CommitManifestUpdates(basePath, version, cfg, &ManifestUpdates{
			DropIndexes: []DropIndexEntry{{IndexID: 99, ExpectedBuildID: 999}},
		})
		require.NoError(t, err)
		require.Equal(t, MarshalManifestPath(basePath, version), got)
	})

	t.Run("drops only the matching index", func(t *testing.T) {
		got, err := CommitManifestUpdates(basePath, version, cfg, &ManifestUpdates{
			DropIndexes: []DropIndexEntry{{IndexID: 10, ExpectedBuildID: 500}},
		})
		require.NoError(t, err)

		indexes, err := GetManifestIndexInfos(got, cfg)
		require.NoError(t, err)
		require.Len(t, indexes, 1)
		require.EqualValues(t, 11, indexes[0].IndexID)
	})
}
