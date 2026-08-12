// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestManifestIndexFallback(t *testing.T) {
	const (
		segmentID    = int64(3)
		fieldID      = int64(100)
		indexID      = int64(101)
		buildID      = int64(102)
		indexVersion = int64(4)
	)
	segmentIndex := &model.SegmentIndex{
		SegmentID:    segmentID,
		IndexID:      indexID,
		BuildID:      buildID,
		IndexVersion: indexVersion,
		IndexType:    "HNSW",
	}
	manifestIndex := packed.ManifestIndexInfo{
		IndexName:             "vector_hnsw",
		IndexType:             "HNSW",
		Path:                  "index_v1/1/2/3/102/4",
		FieldID:               fieldID,
		IndexID:               indexID,
		BuildID:               buildID,
		IndexVersion:          indexVersion,
		NumRows:               1000,
		SerializedSize:        2000,
		MemSize:               3000,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
		IndexFileKeys:         []string{"0", "1"},
		Properties:            map[string]string{"metric_type": "COSINE"},
	}

	segments := NewSegmentsInfo()
	segments.SetSegment(segmentID, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:             segmentID,
		StorageVersion: storage.StorageV3,
		ManifestPath:   `{"base_path":"segments/1/2/3","ver":2}`,
	}})
	server := &Server{meta: &meta{segments: segments}}
	patch := mockey.Mock(packed.GetManifestIndexInfos).
		Return([]packed.ManifestIndexInfo{manifestIndex}, nil).
		Build()
	defer patch.UnPatch()

	manifestIndexes, manifestPath := server.getManifestIndexesForSegment(context.Background(), segmentID)
	info, ok := resolveManifestIndexFilePathInfo(context.Background(), manifestPath, manifestIndexes, segmentIndex, fieldID)
	require.True(t, ok)
	require.Equal(t, []string{"index_v1/1/2/3/102/4/0", "index_v1/1/2/3/102/4/1"}, info.GetIndexFilePaths())
	require.Equal(t, "vector_hnsw", info.GetIndexName())
	require.EqualValues(t, 2000, info.GetSerializedSize())

	duplicate := manifestIndex
	duplicate.IndexName = "duplicate_hnsw"
	_, ok = resolveManifestIndexFilePathInfo(context.Background(), manifestPath, []packed.ManifestIndexInfo{manifestIndex, duplicate}, segmentIndex, fieldID)
	require.False(t, ok)
}

func TestManifestIndexFileKeysRejectPathTraversal(t *testing.T) {
	_, ok := manifestIndexFilePathInfo(3, packed.ManifestIndexInfo{
		IndexName:             "index",
		IndexType:             "HNSW",
		Path:                  "index_v1/1/2/3/102/4",
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
		IndexFileKeys:         []string{"../other-build/file"},
	})
	require.False(t, ok)
}

func TestServerGetIndexInfosManifestFallback(t *testing.T) {
	const (
		collectionID = int64(1)
		partitionID  = int64(2)
		segmentID    = int64(3)
		fieldID      = int64(100)
		indexID      = int64(101)
		buildID      = int64(102)
		indexVersion = int64(4)
		fieldID2     = int64(200)
		indexID2     = int64(201)
		buildID2     = int64(202)
	)

	segmentIndex := &model.SegmentIndex{
		SegmentID:             segmentID,
		CollectionID:          collectionID,
		PartitionID:           partitionID,
		IndexID:               indexID,
		BuildID:               buildID,
		IndexVersion:          indexVersion,
		IndexState:            commonpb.IndexState_Finished,
		IndexType:             "HNSW",
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}
	segmentIndex2 := &model.SegmentIndex{
		SegmentID:             segmentID,
		CollectionID:          collectionID,
		PartitionID:           partitionID,
		IndexID:               indexID2,
		BuildID:               buildID2,
		IndexVersion:          indexVersion,
		IndexState:            commonpb.IndexState_Finished,
		IndexType:             "HNSW",
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}
	segmentIndexes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segmentIndexes.Insert(indexID, segmentIndex)
	segmentIndexes.Insert(indexID2, segmentIndex2)
	segments := NewSegmentsInfo()
	segments.SetSegment(segmentID, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collectionID,
		PartitionID:    partitionID,
		StorageVersion: storage.StorageV3,
		ManifestPath:   `{"base_path":"segments/1/2/3","ver":2}`,
	}})
	server := &Server{meta: &meta{
		segments: segments,
		indexMeta: &indexMeta{
			indexes: map[UniqueID]map[UniqueID]*model.Index{
				collectionID: {
					indexID: {
						CollectionID: collectionID,
						FieldID:      fieldID,
						IndexID:      indexID,
						IndexName:    "vector_hnsw",
					},
					indexID2: {
						CollectionID: collectionID,
						FieldID:      fieldID2,
						IndexID:      indexID2,
						IndexName:    "vector_hnsw_2",
					},
				},
			},
			segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		},
	}}
	server.meta.indexMeta.segmentIndexes.Insert(segmentID, segmentIndexes)
	server.stateCode.Store(commonpb.StateCode_Healthy)
	req := &indexpb.GetIndexInfoRequest{CollectionID: collectionID, SegmentIDs: []int64{segmentID}}
	manifestIndex := packed.ManifestIndexInfo{
		IndexName:             "vector_hnsw",
		IndexType:             "HNSW",
		Path:                  "index_v1/1/2/3/102/4",
		FieldID:               fieldID,
		IndexID:               indexID,
		BuildID:               buildID,
		IndexVersion:          indexVersion,
		NumRows:               1000,
		SerializedSize:        2000,
		MemSize:               3000,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
		IndexFileKeys:         []string{"0", "1"},
	}
	manifestIndex2 := manifestIndex
	manifestIndex2.IndexName = "vector_hnsw_2"
	manifestIndex2.Path = "index_v1/1/2/3/202/4"
	manifestIndex2.FieldID = fieldID2
	manifestIndex2.IndexID = indexID2
	manifestIndex2.BuildID = buildID2

	t.Run("falls back to manifest when etcd index file keys are absent", func(t *testing.T) {
		manifestReadCount := 0
		patch := mockey.Mock(packed.GetManifestIndexInfos).To(
			func(_ string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
				manifestReadCount++
				return []packed.ManifestIndexInfo{manifestIndex, manifestIndex2}, nil
			}).Build()
		defer patch.UnPatch()

		resp, err := server.GetIndexInfos(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, 1, manifestReadCount)
		info := resp.GetSegmentInfo()[segmentID]
		require.True(t, info.GetEnableIndex())
		require.Len(t, info.GetIndexInfos(), 2)
	})

	t.Run("uses etcd index file keys without reading manifest", func(t *testing.T) {
		segmentIndex.IndexFileKeys = []string{"index.bin"}
		segmentIndex.IndexSerializedSize = 2000
		segmentIndex.IndexMemSize = 3000
		segmentIndex2.IndexFileKeys = []string{"index.bin"}
		segmentIndex2.IndexSerializedSize = 2000
		segmentIndex2.IndexMemSize = 3000
		server.meta.chunkManager = storage.NewLocalChunkManager()
		manifestReadCount := 0
		patch := mockey.Mock(packed.GetManifestIndexInfos).To(
			func(_ string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
				manifestReadCount++
				return nil, nil
			}).Build()
		defer patch.UnPatch()

		resp, err := server.GetIndexInfos(context.Background(), req)
		require.NoError(t, err)
		require.Zero(t, manifestReadCount)
		require.True(t, resp.GetSegmentInfo()[segmentID].GetEnableIndex())
		require.Len(t, resp.GetSegmentInfo()[segmentID].GetIndexInfos(), 2)
	})
}
