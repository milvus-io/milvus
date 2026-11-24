// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func TestManifestIndexFileKeysRejectPathTraversal(t *testing.T) {
	entry := packed.ManifestIndexInfo{
		IndexName:             "index",
		IndexType:             "HNSW",
		Path:                  "index_v1/1/2/3/102/4",
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}
	for _, key := range []string{"", ".", "..", "/absolute", "dir/file", "../other-build/file"} {
		entry.IndexFileKeys = []string{key}
		_, ok := manifestIndexFilePathInfo(3, entry)
		require.False(t, ok, key)
	}
}

// validateManifestIndexPublishable must apply exactly the reader predicate.
func TestValidateManifestIndexPublishable(t *testing.T) {
	good := packed.ManifestIndexInfo{
		IndexID:               500,
		BuildID:               5100,
		IndexName:             "idx",
		IndexType:             "HNSW",
		Path:                  "/tmp/test-reload/index_files/5100/1/10/5001",
		IndexFileKeys:         []string{"0"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}
	require.NoError(t, ValidateManifestIndexPublishable(5001, good))

	nameless := good
	nameless.IndexName = ""
	require.Error(t, ValidateManifestIndexPublishable(5001, nameless))

	pathless := good
	pathless.Path = ""
	require.Error(t, ValidateManifestIndexPublishable(5001, pathless))
}

func TestManifestIndexFilePathInfoIgnoresLegacyProperties(t *testing.T) {
	info, ok := manifestIndexFilePathInfo(3, packed.ManifestIndexInfo{
		IndexName: "index", IndexType: "FLAT",
		Path:                  "files/insert_log/1/2/3/_index/102/4",
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
		IndexFileKeys:         []string{"index.bin"},
		Properties:            map[string]string{common.IndexTypeKey: "HNSW", "M": "16", common.DimKey: "8"},
	})
	require.True(t, ok)
	require.Equal(t, []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "FLAT"}}, info.IndexParams)
}

func TestManifestIndexFilePathInfoPreservesIndexType(t *testing.T) {
	info, ok := manifestIndexFilePathInfo(3, packed.ManifestIndexInfo{
		IndexName: "custom_vector", IndexType: "HNSW_SQ",
		Path: "index_v1/1/2/3/102/4", IndexFileKeys: []string{"index.bin"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
		Properties:            map[string]string{common.IndexTypeKey: "HNSW"},
	})
	require.True(t, ok)
	require.Equal(t, "HNSW_SQ", info.GetIndexType())
	require.Equal(t, "HNSW_SQ", GetIndexType(info.GetIndexParams()))
	require.Equal(t, "custom_vector", info.GetIndexName())
}

func TestManifestIndexMetadataProjection(t *testing.T) {
	prefix := "root/index_v1/1/2/3/500/1"
	entry := packed.ManifestIndexInfo{
		IndexID: 10, BuildID: 500, IndexVersion: 1, FieldID: 101,
		IndexName: "vec_index", IndexType: "HNSW", NumRows: 1000, Path: prefix,
		SerializedSize: 2048, MemSize: 4096, CurrentIndexVersion: 5,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
		IndexFileKeys:         []string{"0", "1"}, Properties: map[string]string{"metric_type": "L2"},
	}
	got, ok := manifestIndexFilePathInfo(3, entry)
	require.True(t, ok)
	require.Equal(t, []string{prefix + "/0", prefix + "/1"}, got.GetIndexFilePaths())
	require.Equal(t, int64(10), got.GetIndexID())
	require.Equal(t, int64(500), got.GetBuildID())
	require.Equal(t, uint64(2048), got.GetSerializedSize())
	require.Equal(t, uint64(4096), got.GetMemSize())
	require.Equal(t, int32(5), got.GetCurrentIndexVersion())
	require.Equal(t, []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}}, got.IndexParams)
	for _, mode := range []string{"path", "name", "type", "version", "rows", "serialized-size", "memory-size", "keys"} {
		t.Run(mode, func(t *testing.T) {
			bad := entry
			switch mode {
			case "path":
				bad.Path = ""
			case "name":
				bad.IndexName = ""
			case "type":
				bad.IndexType = ""
			case "version":
				bad.IndexStorePathVersion = -1
			case "rows":
				bad.NumRows = -1
			case "serialized-size":
				bad.SerializedSize = -1
			case "memory-size":
				bad.MemSize = -1
			case "keys":
				bad.IndexFileKeys = nil
			}
			_, ok := manifestIndexFilePathInfo(3, bad)
			require.False(t, ok)
		})
	}
}
