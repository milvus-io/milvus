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
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// TestBuildManifestIndexInfoFromDataCoordMetadata proves the manifest entry is
// assembled entirely from DataCoord state - no worker-supplied manifest input -
// and that the artifact path is stored relative to the segment's _index
// directory so milvus-storage restores the legacy prefix on read.
func TestBuildManifestIndexInfoFromDataCoordMetadata(t *testing.T) {
	const (
		collectionID = int64(1)
		partitionID  = int64(2)
		segmentID    = int64(3)
		fieldID      = int64(100)
		indexID      = int64(101)
		buildID      = int64(102)
	)
	basePath := "files/insert_log/1/2/3"
	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collectionID, &collectionInfo{
		ID: collectionID,
		Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: fieldID, Name: "vector"},
		}},
	})
	m := &meta{
		collections:  collections,
		chunkManager: storage.NewLocalChunkManager(objectstorage.RootPath("files")),
		indexMeta: &indexMeta{
			indexes: map[UniqueID]map[UniqueID]*model.Index{
				collectionID: {indexID: {
					CollectionID: collectionID,
					FieldID:      fieldID,
					IndexID:      indexID,
					IndexName:    "vector_hnsw",
					IndexParams: []*commonpb.KeyValuePair{
						{Key: common.IndexTypeKey, Value: "HNSW"},
						{Key: "M", Value: "16"},
					},
					TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "8"}},
				}},
			},
		},
	}
	segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collectionID,
		PartitionID:    partitionID,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath(basePath, 7),
	}}
	segIdx := &model.SegmentIndex{
		CollectionID:              collectionID,
		PartitionID:               partitionID,
		SegmentID:                 segmentID,
		IndexID:                   indexID,
		BuildID:                   buildID,
		IndexVersion:              4,
		NumRows:                   1000,
		IndexSerializedSize:       2000,
		IndexMemSize:              3000,
		CurrentIndexVersion:       5,
		CurrentScalarIndexVersion: 6,
		IndexStorePathVersion:     indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
		IndexFileKeys:             []string{"0", "1"},
	}

	info, err := buildManifestIndexInfo(m, segment, segIdx)
	require.NoError(t, err)
	require.Equal(t, "vector", info.ColumnName)
	require.Equal(t, "vector_hnsw", info.IndexName)
	require.Equal(t, "HNSW", info.IndexType)
	require.EqualValues(t, fieldID, info.FieldID)
	require.EqualValues(t, indexID, info.IndexID)
	require.EqualValues(t, buildID, info.BuildID)
	require.EqualValues(t, 1000, info.NumRows)
	require.EqualValues(t, 2000, info.SerializedSize)
	require.EqualValues(t, 3000, info.MemSize)
	require.EqualValues(t, 5, info.CurrentIndexVersion)
	require.EqualValues(t, 6, info.CurrentScalarIndexVersion)
	require.Equal(t, []string{"0", "1"}, info.IndexFileKeys)
	require.Equal(t, "16", info.Properties["M"])
	require.Equal(t, "8", info.Properties[common.DimKey])
	require.Equal(t, "HNSW", info.Properties[common.IndexTypeKey])

	// The stored path is relative to <basePath>/_index and resolves back to the
	// legacy index prefix, which is what milvus-storage reproduces on read.
	prefix := "files/index_v1/1/2/3/102/4"
	expected, err := packed.ManifestIndexRelativePath(basePath, prefix)
	require.NoError(t, err)
	require.Equal(t, expected, info.Path)
	require.Equal(t, "../../../../../index_v1/1/2/3/102/4", info.Path)
}

// TestBuildManifestIndexInfoRespectsSegmentIndexTypeOverride covers a segment
// whose index type was downgraded away from the collection definition.
func TestBuildManifestIndexInfoRespectsSegmentIndexTypeOverride(t *testing.T) {
	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	m := &meta{
		collections:  collections,
		chunkManager: storage.NewLocalChunkManager(objectstorage.RootPath("files")),
		indexMeta: &indexMeta{
			indexes: map[UniqueID]map[UniqueID]*model.Index{
				1: {101: {
					CollectionID: 1,
					FieldID:      100,
					IndexID:      101,
					IndexName:    "vector_hnsw",
					IndexParams:  []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}},
				}},
			},
		},
	}
	segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 3, CollectionID: 1, PartitionID: 2,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath("files/insert_log/1/2/3", 7),
	}}
	info, err := buildManifestIndexInfo(m, segment, &model.SegmentIndex{
		CollectionID: 1, PartitionID: 2, SegmentID: 3, IndexID: 101, BuildID: 102,
		IndexVersion: 4, IndexType: "FLAT",
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
		IndexFileKeys:         []string{"0"},
	})
	require.NoError(t, err)
	require.Equal(t, "FLAT", info.IndexType)
	require.Equal(t, "FLAT", info.Properties[common.IndexTypeKey])
	// A field the schema does not describe leaves ColumnName empty rather than
	// inventing one; field_id stays the authoritative identity.
	require.Empty(t, info.ColumnName)
}

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

func TestResolveManifestIndexFilePathInfos(t *testing.T) {
	const (
		segmentID = int64(3)
		fieldID   = int64(100)
		indexID   = int64(101)
	)
	manifestIndex := packed.ManifestIndexInfo{
		IndexName:             "vector_hnsw",
		IndexType:             "HNSW",
		Path:                  "index_v1/1/2/3/102/4",
		FieldID:               fieldID,
		IndexID:               indexID,
		BuildID:               102,
		IndexVersion:          4,
		NumRows:               1000,
		SerializedSize:        2000,
		MemSize:               3000,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
		IndexFileKeys:         []string{"0"},
	}

	active := &model.Index{IndexID: indexID, FieldID: fieldID}
	dropped := &model.Index{IndexID: indexID + 1, FieldID: fieldID, IsDeleted: true}
	infos := resolveManifestIndexFilePathInfos(context.Background(), segmentID, "manifest",
		[]packed.ManifestIndexInfo{manifestIndex}, []*model.Index{active, dropped})
	require.Len(t, infos, 1)
	require.Equal(t, []string{"index_v1/1/2/3/102/4/0"}, infos[0].GetIndexFilePaths())

	infos = resolveManifestIndexFilePathInfos(context.Background(), segmentID, "manifest",
		[]packed.ManifestIndexInfo{manifestIndex}, []*model.Index{{IndexID: indexID, FieldID: fieldID, IsDeleted: true}})
	require.Empty(t, infos)
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

	t.Run("falls back to manifest when etcd segment indexes are absent", func(t *testing.T) {
		server.meta.indexMeta.segmentIndexes.Remove(segmentID)
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
		server.meta.indexMeta.segmentIndexes.Insert(segmentID, segmentIndexes)
	})

	t.Run("falls back once for finished tasks missing index file keys", func(t *testing.T) {
		server.meta.chunkManager = storage.NewLocalChunkManager()
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
		require.Len(t, resp.GetSegmentInfo()[segmentID].GetIndexInfos(), 2)
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
