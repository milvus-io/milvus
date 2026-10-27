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

package packed

import (
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func TestManifestIndexPathConfinement(t *testing.T) {
	const base = "files/insert_log/1/2/3"
	got, err := ManifestIndexRelativePath(base, base+"/_index/100/1")
	require.NoError(t, err)
	require.Equal(t, "100/1", got)
	for _, prefix := range []string{
		"files/index_files/100/1/2/3", "files/index_v1/1/2/3/100/1",
		base + "/_index/../outside", base + "/_index", base + "/_index-other/100/1",
		base + "/_index//100/1", base + "/_index/100/./1",
		"files/insert_log/1/2/30/_index/100/1",
	} {
		_, err := ManifestIndexRelativePath(base, prefix)
		require.Error(t, err, prefix)
	}
}

func TestManifestSegmentRootedIndexRoundTrip(t *testing.T) {
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
	base := path.Join(cfg.RootPath, "insert_log/1/2/3")
	entry := func(id int64, rel string) ManifestIndexInfo {
		return ManifestIndexInfo{
			ColumnName: "vector", IndexName: "index", IndexType: "HNSW",
			Path: rel, IndexID: id, BuildID: id, IndexVersion: 1, FieldID: 101,
			IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
			IndexFileKeys:         []string{"index.bin"},
		}
	}
	manifest, err := CommitManifestUpdates(MarshalManifestPath(base, ManifestEarliest), cfg,
		&ManifestUpdates{Indexes: []ManifestIndexInfo{entry(100, "100/1")}})
	require.NoError(t, err)
	// A second commit must preserve the first index's complete path.
	manifest, err = CommitManifestUpdates(manifest, cfg,
		&ManifestUpdates{Indexes: []ManifestIndexInfo{entry(200, "200/1")}})
	require.NoError(t, err)
	indexes, err := GetManifestIndexInfos(manifest, cfg)
	require.NoError(t, err)
	require.Len(t, indexes, 2)
	got := map[int64]string{}
	for _, index := range indexes {
		got[index.BuildID] = index.Path
	}
	require.Equal(t, map[int64]string{100: base + "/_index/100/1", 200: base + "/_index/200/1"}, got)
	// A rejected incremental entry must not replace the committed revision.
	_, err = CommitManifestUpdates(manifest, cfg,
		&ManifestUpdates{Indexes: []ManifestIndexInfo{entry(300, "../../index_files/300")}})
	require.Error(t, err)
	indexes, err = GetManifestIndexInfos(manifest, cfg)
	require.NoError(t, err)
	require.Len(t, indexes, 2)
}
