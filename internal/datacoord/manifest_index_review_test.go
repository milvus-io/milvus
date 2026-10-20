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

package datacoord

import (
	"path"

	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storagev2/packed"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"

	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestManifestIndexPathMatchesSegmentIdentity(t *testing.T) {
	segment := &datapb.SegmentInfo{CollectionID: 1, PartitionID: 2, ID: 3}
	for _, layout := range []indexpb.IndexStorePathVersion{0, 1} {
		entry := packed.ManifestIndexInfo{
			IndexID: 4, BuildID: 5, IndexVersion: 6, IndexName: "idx", IndexType: "HNSW",
			IndexStorePathVersion: layout, IndexFileKeys: []string{"data"},
		}
		prefix := metautil.NewIndexPathBuilder("root", layout, 1, 2, 3, 5, 6).BuildPrefix()
		entry.Path = prefix
		info, ok := manifestIndexFilePathInfoForSegment("root", segment, entry)
		require.True(t, ok)
		require.Equal(t, []string{path.Join(prefix, "data")}, info.GetIndexFilePaths())
		for _, badPath := range []string{
			"other/root", prefix + "/child", path.Dir(prefix),
			metautil.NewIndexPathBuilder("root", layout, 1, 2, 99, 5, 6).BuildPrefix(),
		} {
			entry.Path = badPath
			_, ok = manifestIndexFilePathInfoForSegment("root", segment, entry)
			require.False(t, ok, badPath)
		}
		entry.Path = prefix
		entry.IndexFileKeys = []string{".."}
		_, ok = manifestIndexFilePathInfoForSegment("root", segment, entry)
		require.False(t, ok)
	}
}

func TestManifestReadConcurrencyUsesStorageBudget(t *testing.T) {
	params := paramtable.Get()
	scan := params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue("64")
	defer params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue(scan)
	connections := params.MinioCfg.MaxConnections.SwapTempValue("7")
	defer params.MinioCfg.MaxConnections.SwapTempValue(connections)
	require.Equal(t, 7, segmentIndexManifestReadConcurrency())
	params.MinioCfg.MaxConnections.SwapTempValue("100")
	require.Equal(t, 64, segmentIndexManifestReadConcurrency())
	params.MinioCfg.MaxConnections.SwapTempValue("0")
	require.Equal(t, 64, segmentIndexManifestReadConcurrency())
}
