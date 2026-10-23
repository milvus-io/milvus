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
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// GetIndexInfos serves the load path from in-memory SegmentIndex metadata and
// must never read a manifest: it is driven by QueryCoord's index checker every
// checkIndexInterval, so a per-segment object read would be paid on every round
// by exactly the segments that are legitimately unindexed.
func TestServerGetIndexInfosReadsNoManifest(t *testing.T) {
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
	ms := metacache.NewMetaStore(nil)
	segments := meta.NewSegmentsInfo(ms)
	segments.SetSegment(segmentID, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collectionID,
		PartitionID:    partitionID,
		StorageVersion: storage.StorageV3,
		ManifestPath:   `{"base_path":"segments/1/2/3","ver":2}`,
	}})
	im := &index.IndexMeta{
		Indexes: map[UniqueID]map[UniqueID]*model.Index{
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
		SegmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
	}
	server := &Server{meta: &meta.Meta{
		Segments: segments,
	}, domainMetas: domainMetas{indexMeta: im}}
	server.indexMeta.SegmentIndexes.Insert(segmentID, segmentIndexes)
	server.meta.IndexCatalog = server.indexMeta
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

	// A segment with no SegmentIndex record has no index artifact: a manifest
	// entry is never published without its record being installed in memory by
	// the same commit, so there is nothing to recover by reading the manifest.
	t.Run("absent segment index records yield nothing and read no manifest", func(t *testing.T) {
		server.indexMeta.SegmentIndexes.Remove(segmentID)
		manifestReadCount := 0
		patch := mockey.Mock(packed.GetManifestIndexInfosAsync).To(
			func(_ context.Context, _ *packed.ManifestIOContext, _ string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
				manifestReadCount++
				return []packed.ManifestIndexInfo{manifestIndex, manifestIndex2}, nil
			}).Build()
		defer patch.UnPatch()

		resp, err := server.GetIndexInfos(context.Background(), req)
		require.NoError(t, err)
		require.Zero(t, manifestReadCount)
		info := resp.GetSegmentInfo()[segmentID]
		require.False(t, info.GetEnableIndex())
		require.Empty(t, info.GetIndexInfos())
		server.indexMeta.SegmentIndexes.Insert(segmentID, segmentIndexes)
	})

	// A finished record with no index file keys is a fake-finished build (a
	// segment too small to train), which publishes no manifest entry either -
	// so there is nothing to look up, and the empty file list is the answer.
	t.Run("finished task missing index file keys reads no manifest", func(t *testing.T) {
		server.meta.ChunkManager = storage.NewLocalChunkManager()
		manifestReadCount := 0
		patch := mockey.Mock(packed.GetManifestIndexInfosAsync).To(
			func(_ context.Context, _ *packed.ManifestIOContext, _ string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
				manifestReadCount++
				return []packed.ManifestIndexInfo{manifestIndex, manifestIndex2}, nil
			}).Build()
		defer patch.UnPatch()

		resp, err := server.GetIndexInfos(context.Background(), req)
		require.NoError(t, err)
		require.Zero(t, manifestReadCount)
		infos := resp.GetSegmentInfo()[segmentID].GetIndexInfos()
		require.Len(t, infos, 2)
		for _, info := range infos {
			assert.Empty(t, info.GetIndexFilePaths())
		}
	})

	t.Run("uses recorded index file keys without reading manifest", func(t *testing.T) {
		segmentIndex.IndexFileKeys = []string{"index.bin"}
		segmentIndex.IndexSerializedSize = 2000
		segmentIndex.IndexMemSize = 3000
		segmentIndex2.IndexFileKeys = []string{"index.bin"}
		segmentIndex2.IndexSerializedSize = 2000
		segmentIndex2.IndexMemSize = 3000
		server.meta.ChunkManager = storage.NewLocalChunkManager()
		manifestReadCount := 0
		patch := mockey.Mock(packed.GetManifestIndexInfosAsync).To(
			func(_ context.Context, _ *packed.ManifestIOContext, _ string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
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
