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
	"context"
	"path"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestReadCompletedIndexesUsesSelectedManifest(t *testing.T) {
	root := t.TempDir()
	base := path.Join(root, "insert_log/1/2/3")
	segment := &datapb.SegmentInfo{ID: 3, CollectionID: 1, PartitionID: 2,
		ManifestPath: packed.MarshalManifestPath(base, 7), ManifestHasIndex: true,
		IndexSummaries: []*datapb.SegmentIndexSummary{{IndexId: 4, FieldId: 100, BuildId: 5, HasArtifact: true, IndexVersion: 1}},
	}
	mt := &Meta{ChunkManager: storage.NewLocalChunkManager(objectstorage.RootPath(root))}
	entry := packed.ManifestIndexInfo{IndexID: 4, FieldID: 100, BuildID: 5, IndexVersion: 1, IndexName: "idx", IndexType: "HNSW",
		Path: path.Join(base, "_index/5/1"), IndexFileKeys: []string{"index"}, IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED}
	read := mockey.Mock(packed.GetManifestIndexInfosAsync).To(func(_ context.Context, _ *packed.ManifestIOContext, manifest string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
		require.Equal(t, segment.GetManifestPath(), manifest)
		return []packed.ManifestIndexInfo{entry}, nil
	}).Build()
	defer read.UnPatch()
	records, err := mt.ReadCompletedIndexes(t.Context(), segment)
	require.NoError(t, err)
	require.Equal(t, []string{"index"}, records[4].IndexFileKeys)
	records[4].IndexFileKeys[0] = "changed"
	require.True(t, segment.IndexSummaries[0].GetHasArtifact())
	entry.BuildID = 6
	_, err = mt.ReadCompletedIndexes(t.Context(), segment)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	read.UnPatch()
	failed := mockey.Mock(packed.GetManifestIndexInfosAsync).Return(nil, merr.ErrServiceUnavailable).Build()
	defer failed.UnPatch()
	_, err = mt.ReadCompletedIndexes(t.Context(), segment)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
}
