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
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

	// A segment with no SegmentIndex record has no index artifact: a manifest
	// entry is never published without its record being installed in memory by
	// the same commit, so there is nothing to recover by reading the manifest.
	t.Run("absent segment index records yield nothing and read no manifest", func(t *testing.T) {
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
		require.Zero(t, manifestReadCount)
		info := resp.GetSegmentInfo()[segmentID]
		require.False(t, info.GetEnableIndex())
		require.Empty(t, info.GetIndexInfos())
		server.meta.indexMeta.segmentIndexes.Insert(segmentID, segmentIndexes)
	})

	// A finished record with no index file keys is a fake-finished build (a
	// segment too small to train), which publishes no manifest entry either -
	// so there is nothing to look up, and the empty file list is the answer.
	t.Run("finished task missing index file keys reads no manifest", func(t *testing.T) {
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

// withSegmentIndexManifestWrites flips
// dataCoord.index.writeSegmentIndexToManifest for one test and restores it
// afterwards. enabled=true publishes index records to segment manifests and
// skips their etcd writes; enabled=false is the legacy etcd-only behavior.
func withSegmentIndexManifestWrites(t *testing.T, enabled bool) {
	t.Helper()
	key := paramtable.Get().DataCoordCfg.WriteSegmentIndexToManifest.Key
	previous := paramtable.Get().DataCoordCfg.WriteSegmentIndexToManifest.GetValue()
	paramtable.Get().Save(key, strconv.FormatBool(enabled))
	t.Cleanup(func() { paramtable.Get().Save(key, previous) })
}

// setupManifestReloadMeta builds a meta holding one healthy StorageV3 segment
// and the collection's index definition, but no SegmentIndex record - the
// state a restart lands in when SegmentIndex etcd writes are off.
func setupManifestReloadMeta(t *testing.T) *meta {
	t.Helper()
	const (
		collID  = UniqueID(100)
		partID  = UniqueID(10)
		segID   = UniqueID(5001)
		fieldID = UniqueID(101)
		indexID = UniqueID(500)
	)
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, m.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:               segID,
		CollectionID:     collID,
		PartitionID:      partID,
		State:            commonpb.SegmentState_Flushed,
		NumOfRows:        100,
		StorageVersion:   storage.StorageV3,
		ManifestPath:     packed.MarshalManifestPath("/tmp/test-reload/insert_log/100/10/5001", 3),
		ManifestHasIndex: true,
	})))
	// Index definitions stay in etcd regardless of the switch: a manifest
	// cannot record "the user asked for an HNSW index on this field".
	require.NoError(t, m.indexMeta.CreateIndex(context.TODO(), &model.Index{
		CollectionID: collID,
		FieldID:      fieldID,
		IndexID:      indexID,
		IndexName:    "idx",
	}))
	return m
}

func mockReloadManifestEntry(t *testing.T, buildID int64) {
	t.Helper()
	infos := mockey.Mock(packed.GetManifestIndexInfos).Return([]packed.ManifestIndexInfo{{
		IndexID:               500,
		BuildID:               buildID,
		FieldID:               101,
		IndexName:             "idx",
		IndexType:             "HNSW",
		IndexVersion:          1,
		NumRows:               100,
		SerializedSize:        2000,
		MemSize:               3000,
		Path:                  "root/index/100/10/5001/5100/1",
		IndexFileKeys:         []string{"f0"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
	}}, nil).Build()
	t.Cleanup(func() { infos.UnPatch() })
}

// The switch's whole point: with SegmentIndex etcd writes off, a restart must
// still see the segment as indexed. Reload rebuilds the record from the
// manifest and the consumers that decide "is this segment indexed" - which
// have no manifest fallback of their own - work off that rebuilt state.
func TestReloadSegmentIndexesFromManifests(t *testing.T) {
	withSegmentIndexManifestWrites(t, true)
	m := setupManifestReloadMeta(t)
	mockReloadManifestEntry(t, 5100)

	require.Empty(t, m.indexMeta.GetSegmentIndexes(100, 5001))

	require.NoError(t, m.reloadSegmentIndexesFromManifests(context.TODO()))

	segIdxes := m.indexMeta.GetSegmentIndexes(100, 5001)
	require.Len(t, segIdxes, 1)
	recovered := segIdxes[500]
	require.NotNil(t, recovered)
	assert.Equal(t, commonpb.IndexState_Finished, recovered.IndexState)
	assert.EqualValues(t, 5100, recovered.BuildID)
	assert.Equal(t, []string{"f0"}, recovered.IndexFileKeys)
	assert.EqualValues(t, 2000, recovered.IndexSerializedSize)
	assert.EqualValues(t, 3000, recovered.IndexMemSize)
	assert.Equal(t, "HNSW", recovered.IndexType)
	assert.EqualValues(t, 100, recovered.NumRows)

	// The consumers that gate compaction, load and rebuild decisions.
	assert.Equal(t, []int64{5001}, m.indexMeta.GetIndexedSegments(100, []int64{5001}, []int64{101}))
	assert.True(t, m.indexMeta.IsIndexExist(100, 500))
	_, ok := m.indexMeta.GetIndexJob(5100)
	assert.True(t, ok)
}

// etcd wins on conflict, as it does for every other manifest-index consumer:
// a record loaded from the catalog must not be replaced by a manifest
// projection of the same build, which lacks the task history.
func TestReloadSegmentIndexesFromManifests_EtcdRecordWins(t *testing.T) {
	withSegmentIndexManifestWrites(t, true)
	m := setupManifestReloadMeta(t)
	mockReloadManifestEntry(t, 5100)

	// A second index definition with no etcd record. Every healthy V3 segment
	// is read unconditionally, so what protects the etcd record below is the
	// buildID-level precedence check in the install loop - there is no
	// candidate-narrowing filter, by design.
	require.NoError(t, m.indexMeta.CreateIndex(context.TODO(), &model.Index{
		CollectionID: 100,
		FieldID:      102,
		IndexID:      501,
		IndexName:    "idx2",
	}))

	require.NoError(t, m.indexMeta.AddSegmentIndex(context.TODO(), &model.SegmentIndex{
		CollectionID:   100,
		PartitionID:    10,
		SegmentID:      5001,
		IndexID:        500,
		BuildID:        5100,
		NodeID:         77,
		IndexState:     commonpb.IndexState_Finished,
		IndexFileKeys:  []string{"from-etcd"},
		CreatedUTCTime: 42,
	}))

	require.NoError(t, m.reloadSegmentIndexesFromManifests(context.TODO()))

	recovered := m.indexMeta.GetSegmentIndexes(100, 5001)[500]
	require.NotNil(t, recovered)
	assert.Equal(t, []string{"from-etcd"}, recovered.IndexFileKeys)
	assert.EqualValues(t, 77, recovered.NodeID, "task history must survive the manifest projection")
	assert.EqualValues(t, 42, recovered.CreatedUTCTime)
}

// An unreadable manifest must FAIL startup, not degrade to "that segment looks
// unindexed". A silently incomplete indexMeta is what lets GC delete live index
// files: recycleUnusedIndexFilesV0 reads an absent SegmentIndex as proof the
// buildID is garbage and removes its whole prefix with no time tolerance.
func TestReloadSegmentIndexesFromManifests_UnreadableManifestFailsStartup(t *testing.T) {
	withSegmentIndexManifestWrites(t, true)
	m := setupManifestReloadMeta(t)
	infos := mockey.Mock(packed.GetManifestIndexInfos).
		Return(nil, merr.WrapErrIoFailedReason("throttled")).Build()
	defer infos.UnPatch()

	err := m.reloadSegmentIndexesFromManifests(context.TODO())
	require.Error(t, err, "a transient manifest read failure must not be swallowed")
	assert.Empty(t, m.indexMeta.GetSegmentIndexes(100, 5001))
}

// The same failure must abort newMeta rather than yielding a usable meta with
// a hole in it, matching the etcd path: a ListSegmentIndexes error aborts
// newIndexMeta and therefore newMeta.
func TestUnreadableManifestAbortsMetaBoot(t *testing.T) {
	withSegmentIndexManifestWrites(t, true)
	store := newFakeManifestStore(t)
	catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	m := bootMetaForRestart(t, catalog, restartCollID)
	seedRestartFixture(t, m)

	store.failReadsFrom()

	b := broker.NewMockBroker(t)
	b.EXPECT().ShowCollectionIDs(mock.Anything).Return(&rootcoordpb.ShowCollectionIDsResponse{
		Status:        merr.Success(),
		DbCollections: []*rootcoordpb.DBCollections{{DbName: "default", CollectionIDs: []int64{restartCollID}}},
	}, nil).Maybe()
	_, err := newMeta(context.TODO(), catalog,
		storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test-restart")), b)
	require.Error(t, err, "startup must fail rather than come up with an incomplete indexMeta")
}

// fakeManifestStore is a stateful stand-in for the manifest objects: what a
// commit writes is exactly what a later read returns, so a reload round-trips
// through the manifest rather than through a fixture. Without that the "index
// survives a restart" claim would only be testing the projection helper.
type fakeManifestStore struct {
	mu        sync.Mutex
	revisions map[string][]packed.ManifestIndexInfo
	failReads bool
}

// failReadsFrom makes every subsequent manifest read fail, modeling a
// transient object-store error at the next boot. It toggles state inside the
// existing patch rather than re-mocking, which mockey forbids.
func (s *fakeManifestStore) failReadsFrom() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failReads = true
}

func newFakeManifestStore(t *testing.T) *fakeManifestStore {
	t.Helper()
	s := &fakeManifestStore{revisions: make(map[string][]packed.ManifestIndexInfo)}

	commit := mockey.Mock(packed.CommitManifestUpdates).To(
		func(basePath string, version int64, _ *indexpb.StorageConfig, updates *packed.ManifestUpdates) (string, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			// A revision is a full snapshot resolved onto the one it opens at.
			current := s.revisions[packed.MarshalManifestPath(basePath, version)]
			next := make([]packed.ManifestIndexInfo, 0, len(current)+len(updates.Indexes))
			for _, entry := range current {
				dropped := false
				for _, drop := range updates.DropIndexes {
					if drop.IndexID == entry.IndexID {
						dropped = true
						break
					}
				}
				if !dropped {
					next = append(next, entry)
				}
			}
			next = append(next, updates.Indexes...)
			published := packed.MarshalManifestPath(basePath, version+1)
			s.revisions[published] = next
			return published, nil
		}).Build()
	t.Cleanup(func() { commit.UnPatch() })

	read := mockey.Mock(packed.GetManifestIndexInfos).To(
		func(manifestPath string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.failReads {
				return nil, merr.WrapErrIoFailedReason("throttled")
			}
			return s.revisions[manifestPath], nil
		}).Build()
	t.Cleanup(func() { read.UnPatch() })
	return s
}

// bootMetaForRestart runs the real newMeta against a caller-owned catalog, so
// two calls with the same catalog model a DataCoord restart.
func bootMetaForRestart(t *testing.T, catalog metastore.DataCoordCatalog, collectionID int64) *meta {
	t.Helper()
	b := broker.NewMockBroker(t)
	b.EXPECT().ShowCollectionIDs(mock.Anything).Return(&rootcoordpb.ShowCollectionIDsResponse{
		Status:        merr.Success(),
		DbCollections: []*rootcoordpb.DBCollections{{DbName: "default", CollectionIDs: []int64{collectionID}}},
	}, nil)
	m, err := newMeta(context.TODO(), catalog,
		storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test-restart")), b)
	require.NoError(t, err)
	return m
}

const (
	restartCollID  = UniqueID(300)
	restartPartID  = UniqueID(30)
	restartSegID   = UniqueID(8001)
	restartFieldID = UniqueID(101)
	restartIndexID = UniqueID(800)
	restartBuildID = UniqueID(8100)
)

// seedRestartFixture creates the segment, its index definition and its index
// task, then publishes the finished index exactly the way task_index.go's
// publishIndexToManifest does.
func seedRestartFixture(t *testing.T, m *meta) {
	t.Helper()
	ctx := context.TODO()
	basePath := "/tmp/test-restart/insert_log/300/30/8001"

	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID:             restartSegID,
		CollectionID:   restartCollID,
		PartitionID:    restartPartID,
		State:          commonpb.SegmentState_Flushed,
		NumOfRows:      1000,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath(basePath, 1),
	})))
	require.NoError(t, m.indexMeta.CreateIndex(ctx, &model.Index{
		CollectionID: restartCollID,
		FieldID:      restartFieldID,
		IndexID:      restartIndexID,
		IndexName:    "vec_idx",
		TypeParams:   []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}},
		IndexParams:  []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}},
	}))
	require.NoError(t, m.indexMeta.AddSegmentIndex(ctx, &model.SegmentIndex{
		CollectionID:          restartCollID,
		PartitionID:           restartPartID,
		SegmentID:             restartSegID,
		NumRows:               1000,
		IndexID:               restartIndexID,
		BuildID:               restartBuildID,
		IndexVersion:          1,
		IndexState:            commonpb.IndexState_InProgress,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}))

	result := &workerpb.IndexTaskInfo{
		BuildID:               restartBuildID,
		State:                 commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"0", "1"},
		SerializedSize:        4096,
		MemSize:               8192,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}
	segIdx, ok := m.indexMeta.GetIndexJob(restartBuildID)
	require.True(t, ok)
	finished, _, err := m.indexMeta.buildFinishedSegmentIndex(segIdx, result)
	require.NoError(t, err)
	entry, err := buildManifestIndexInfo(m, m.GetSegment(ctx, restartSegID), finished)
	require.NoError(t, err)

	require.NoError(t, m.CommitSegmentManifest(ctx, SegmentManifestCommit{
		SegmentID:     restartSegID,
		StorageConfig: &indexpb.StorageConfig{},
		Mutation: ManifestMutation{
			Type:    ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{Indexes: []packed.ManifestIndexInfo{entry}},
		},
		CatalogMutation: SegmentCatalogMutation{
			SegmentIndex: &SegmentIndexMutation{
				Type:         SegmentIndexUpsert,
				BuildID:      restartBuildID,
				FinishedTask: result,
			},
		},
	}))
}

// End to end for the switch: with SegmentIndex etcd writes off, a finished
// index is published to the manifest and to nothing else, and a full DataCoord
// restart against the same catalog still comes up seeing the segment as
// indexed. This is the claim the switch has to make good on, and the reason
// the manifest-driven reload exists - the consumers asserted below have no
// manifest fallback of their own.
func TestSegmentIndexSurvivesRestartFromManifestOnly(t *testing.T) {
	withSegmentIndexManifestWrites(t, true)
	newFakeManifestStore(t)
	ctx := context.TODO()

	catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	m := bootMetaForRestart(t, catalog, restartCollID)
	seedRestartFixture(t, m)

	// Nothing about this index reached etcd.
	persisted, err := catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, err)
	require.Empty(t, persisted, "the switch must have kept the record out of etcd")

	// Restart: same catalog, brand new meta, memory rebuilt from scratch.
	restarted := bootMetaForRestart(t, catalog, restartCollID)

	recovered := restarted.indexMeta.GetSegmentIndexes(restartCollID, restartSegID)
	require.Len(t, recovered, 1, "the index must come back from the manifest alone")
	segIdx := recovered[restartIndexID]
	require.NotNil(t, segIdx)
	assert.Equal(t, commonpb.IndexState_Finished, segIdx.IndexState)
	assert.EqualValues(t, restartBuildID, segIdx.BuildID)
	assert.Equal(t, []string{"0", "1"}, segIdx.IndexFileKeys)
	assert.EqualValues(t, 4096, segIdx.IndexSerializedSize)
	assert.EqualValues(t, 1000, segIdx.NumRows)
	assert.Equal(t, "HNSW", segIdx.IndexType)

	// The consumers that gate load, compaction and rebuild decisions.
	assert.Equal(t, []int64{restartSegID},
		restarted.indexMeta.GetIndexedSegments(restartCollID, []int64{restartSegID}, []int64{restartFieldID}))
	state := restarted.indexMeta.GetSegmentIndexState(restartCollID, restartSegID, restartIndexID)
	assert.Equal(t, commonpb.IndexState_Finished, state.GetState())
	_, ok := restarted.indexMeta.GetIndexJob(restartBuildID)
	assert.True(t, ok, "the index inspector must not reissue a build for it")
}

// The control: with manifest writes off (the default), the same flow persists
// the record to etcd, so the test above is discriminating rather than
// trivially green.
func TestSegmentIndexPersistedToEtcdWhenManifestWritesOff(t *testing.T) {
	withSegmentIndexManifestWrites(t, false)
	newFakeManifestStore(t)
	ctx := context.TODO()

	catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	m := bootMetaForRestart(t, catalog, restartCollID)
	seedRestartFixture(t, m)

	persisted, err := catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, err)
	require.Len(t, persisted, 1)
	assert.EqualValues(t, restartBuildID, persisted[0].BuildID)
	assert.Equal(t, commonpb.IndexState_Finished, persisted[0].IndexState)
}

// Regression test for an ABBA deadlock between the manifest commit and every
// other SegmentIndex writer.
//
// With the switch off, the etcd-write gate resolves whether a segment is
// manifest-backed through meta, which takes segMu - and every caller of that
// gate (UpdateIndexState, FinishTask, AddSegmentIndex, ...) already holds
// keyLock(buildID). CommitSegmentManifest used to take segMu first and only
// then keyLock while staging, so the two orders formed a cycle on a shared
// buildID and hung DataCoord globally. Go's RWMutex writer preference widens
// it: a pending segMu.Lock blocks new readers, so a keyLock holder waiting on
// segMu.RLock cannot progress either.
//
// Rather than racing the two paths and hoping to hit a microsecond window,
// this asserts the invariant that makes the cycle impossible: while blocked on
// keyLock, the commit must NOT be holding segMu. Hold the key lock, let the
// commit park on it, and check segMu is still free.
func TestCommitSegmentManifestTakesKeyLockBeforeSegMu(t *testing.T) {
	withSegmentIndexManifestWrites(t, true)
	newFakeManifestStore(t)
	ctx := context.TODO()

	catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	m := bootMetaForRestart(t, catalog, restartCollID)
	seedRestartFixture(t, m)

	segIdx, ok := m.indexMeta.GetIndexJob(restartBuildID)
	require.True(t, ok)
	finished, _, err := m.indexMeta.buildFinishedSegmentIndex(segIdx, &workerpb.IndexTaskInfo{
		BuildID:       restartBuildID,
		State:         commonpb.IndexState_Finished,
		IndexFileKeys: []string{"0", "1"},
	})
	require.NoError(t, err)
	entry, err := buildManifestIndexInfo(m, m.GetSegment(ctx, restartSegID), finished)
	require.NoError(t, err)

	// Stand in for any index writer holding the build's key lock - exactly the
	// state UpdateIndexState is in when it calls the manifest-backed gate.
	m.indexMeta.keyLock.Lock(restartBuildID)

	committed := make(chan error, 1)
	go func() {
		committed <- m.CommitSegmentManifest(ctx, SegmentManifestCommit{
			SegmentID:     restartSegID,
			StorageConfig: &indexpb.StorageConfig{},
			Mutation: ManifestMutation{
				Type:    ManifestMutationCommitUpdates,
				Updates: &packed.ManifestUpdates{Indexes: []packed.ManifestIndexInfo{entry}},
			},
			CatalogMutation: SegmentCatalogMutation{
				SegmentIndex: &SegmentIndexMutation{
					Type:    SegmentIndexUpsert,
					BuildID: restartBuildID,
					FinishedTask: &workerpb.IndexTaskInfo{
						BuildID:       restartBuildID,
						State:         commonpb.IndexState_Finished,
						IndexFileKeys: []string{"0", "1"},
					},
				},
			},
		})
	}()

	// Let the commit run until it parks. Manifest I/O is in-memory here, so it
	// reaches its blocking point well within this window.
	select {
	case err := <-committed:
		m.indexMeta.keyLock.Unlock(restartBuildID)
		t.Fatalf("commit finished without waiting on the held key lock: %v", err)
	case <-time.After(2 * time.Second):
	}

	// The load-bearing assertion. Parked on keyLock, the commit must hold no
	// segMu; if it does, any keyLock holder that reads segment state - which is
	// what the manifest-backed gate does - closes the cycle.
	acquired := m.segMu.TryLock()
	if acquired {
		m.segMu.Unlock()
	}
	m.indexMeta.keyLock.Unlock(restartBuildID)

	select {
	case err := <-committed:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("commit did not complete after the key lock was released")
	}

	require.True(t, acquired,
		"commit is holding segMu while blocked on keyLock: lock order is inverted against every other SegmentIndex writer")
}

// A manifest entry whose index definition is already gone MUST still be
// recovered. GC's segment-index recycler is driven entirely by SegmentIndex
// records (recycleUnusedSegIndexes iterates GetAllSegIndexes), and so is the
// COLLECTION_ROOTED file sweep (GetDeletedIndexesWithV1Path). With the switch
// off the record lives nowhere but the manifest, so skipping these entries -
// or skipping the manifest read because the collection has no live index
// definition left - would strand the entry forever and permanently leak the
// artifact bytes whenever the deletion that precedes retraction had failed.
//
// This is the crash window the files-first drop ordering creates: bytes
// deleted (or their deletion failed), record not yet retracted, restart.
func TestReloadRecoversDroppedIndexEntriesSoGCCanRetract(t *testing.T) {
	withSegmentIndexManifestWrites(t, true)
	m := setupManifestReloadMeta(t)
	mockReloadManifestEntry(t, 5100)

	// The index was dropped and its definition already removed from etcd -
	// exactly what recycleUnusedIndexes does before the artifact is retracted.
	require.NoError(t, m.indexMeta.RemoveIndex(context.TODO(), 100, 500))
	require.False(t, m.indexMeta.IsIndexExist(100, 500))

	require.NoError(t, m.reloadSegmentIndexesFromManifests(context.TODO()))

	// The record must come back even though nothing references it any more:
	// it is the only handle GC has on the stranded artifact.
	recovered, ok := m.indexMeta.segmentBuildInfo.Get(5100)
	require.True(t, ok, "a dropped index's manifest entry must still be recovered, or GC can never retract it")
	assert.EqualValues(t, 500, recovered.IndexID)
	assert.EqualValues(t, 5001, recovered.SegmentID)
	assert.Contains(t, m.indexMeta.GetAllSegIndexes(), int64(5100),
		"the recovered record must be visible to recycleUnusedSegIndexes")
}

// The reload is the only path that promotes a manifest entry into a
// SegmentIndex record, and that record's file keys reach removeObjectFiles
// through BuildFilePath, whose path.Join normalizes "..". An entry that could
// aim a delete outside its own buildID prefix must never become a record, and
// boot is where it has to be caught: resolveManifestIndexRetraction rejects the
// same entry on every GC cycle forever while only logging a warning.
func TestReloadRejectsUnusableManifestIndexEntry(t *testing.T) {
	withSegmentIndexManifestWrites(t, true)
	m := setupManifestReloadMeta(t)
	infos := mockey.Mock(packed.GetManifestIndexInfos).Return([]packed.ManifestIndexInfo{{
		IndexID:               500,
		BuildID:               5100,
		FieldID:               101,
		IndexName:             "idx",
		IndexType:             "HNSW",
		IndexVersion:          1,
		NumRows:               100,
		SerializedSize:        2000,
		MemSize:               3000,
		Path:                  "root/index/100/10/5001/5100/1",
		IndexFileKeys:         []string{"../../../../meta/segment-index"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
	}}, nil).Build()
	t.Cleanup(func() { infos.UnPatch() })

	err := m.reloadSegmentIndexesFromManifests(context.TODO())
	require.Error(t, err, "a manifest entry that no other consumer would accept must fail startup, not install silently")
	assert.Contains(t, err.Error(), "unusable index entry")
	assert.Empty(t, m.indexMeta.GetSegmentIndexes(100, 5001))
}

// publishIndexToManifest declines a build whose segment was dropped while it
// ran (task_index.go), so no manifest entry is ever written for it. The etcd
// gate must decline too: reloadSegmentIndexesFromManifests skips unhealthy
// segments, so a record withheld here is rebuildable from nowhere. Its
// BUILD_ROOTED bytes would still be reclaimed by the V0 orphan sweep, but a
// COLLECTION_ROOTED artifact leaks permanently - recycleUnusedIndexFilesV1 is
// driven by metadata that no longer exists.
func TestDroppedSegmentKeepsSegmentIndexEtcdWrite(t *testing.T) {
	withSegmentIndexManifestWrites(t, true)
	m := setupManifestReloadMeta(t)
	m.indexMeta.manifestBackedSegment = m.isManifestBackedSegment
	require.True(t, m.isManifestBackedSegment(5001), "the healthy V3 segment is manifest-backed")

	const droppedID = UniqueID(5002)
	require.NoError(t, m.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             droppedID,
		CollectionID:   100,
		PartitionID:    10,
		State:          commonpb.SegmentState_Dropped,
		NumOfRows:      100,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath("/tmp/test-reload/insert_log/100/10/5002", 3),
	})))

	assert.False(t, m.isManifestBackedSegment(droppedID),
		"a dropped segment publishes no manifest entry and the reload skips it, so the etcd write must not be withheld")
	assert.False(t, m.indexMeta.skipSegmentIndexEtcdWrite(droppedID))
}

// seedUnpublishedBuild is seedRestartFixture up to but not including the
// publish: the segment, an index definition with the given name, and an
// in-flight build record - the state a build is in when its worker result
// arrives.
func seedUnpublishedBuild(t *testing.T, m *meta, indexName string) {
	t.Helper()
	ctx := context.TODO()
	basePath := "/tmp/test-restart/insert_log/300/30/8001"

	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID:             restartSegID,
		CollectionID:   restartCollID,
		PartitionID:    restartPartID,
		State:          commonpb.SegmentState_Flushed,
		NumOfRows:      1000,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath(basePath, 1),
	})))
	require.NoError(t, m.indexMeta.CreateIndex(ctx, &model.Index{
		CollectionID: restartCollID,
		FieldID:      restartFieldID,
		IndexID:      restartIndexID,
		IndexName:    indexName,
		TypeParams:   []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}},
		IndexParams:  []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}},
	}))
	require.NoError(t, m.indexMeta.AddSegmentIndex(ctx, &model.SegmentIndex{
		CollectionID:          restartCollID,
		PartitionID:           restartPartID,
		SegmentID:             restartSegID,
		NumRows:               1000,
		IndexID:               restartIndexID,
		BuildID:               restartBuildID,
		IndexVersion:          1,
		IndexState:            commonpb.IndexState_InProgress,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}))
}

// A user can drop an index while its build is in flight: GC's
// recycleUnusedIndexes removes the definition without waiting for the build,
// while the non-terminal SegmentIndex record survives recycleUnusedSegIndexes.
// The manifest entry is named from the definition, so publishing the late
// Finished result would mint an IndexName-less entry that every fail-closed
// reader rejects - GC could never retire it, and with SegmentIndex etcd
// writes off the reload would abort every DataCoord restart. The publish must
// decline, and setJobInfo must record the result the legacy way so the record
// goes terminal and dies with the ordinary dropped-index GC path.
func TestPublishIndexToManifestDeclinesDroppedIndexDefinition(t *testing.T) {
	withSegmentIndexManifestWrites(t, true)
	store := newFakeManifestStore(t)
	ctx := context.TODO()

	catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	m := bootMetaForRestart(t, catalog, restartCollID)
	seedUnpublishedBuild(t, m, "vec_idx")

	// The drop that races the build: definition gone, build record still live.
	require.NoError(t, m.indexMeta.RemoveIndex(ctx, restartCollID, restartIndexID))
	require.False(t, m.indexMeta.IsIndexExist(restartCollID, restartIndexID))

	segIdx, ok := m.indexMeta.GetIndexJob(restartBuildID)
	require.True(t, ok)
	it := newIndexBuildTask(segIdx, 1, m, nil, nil, nil)
	result := &workerpb.IndexTaskInfo{
		BuildID:               restartBuildID,
		State:                 commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"0", "1"},
		SerializedSize:        4096,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}

	published, err := it.publishIndexToManifest(result)
	require.NoError(t, err)
	require.False(t, published, "a build whose index definition was dropped must not take ownership of the result")
	assert.Empty(t, store.revisions, "no manifest commit may be issued for a dropped index")

	// setJobInfo therefore takes the legacy FinishTask path; the terminal
	// record is what lets recycleUnusedSegIndexes retire the build.
	require.NoError(t, it.setJobInfo(result))
	assert.Empty(t, store.revisions)
	finished, ok := m.indexMeta.GetIndexJob(restartBuildID)
	require.True(t, ok)
	assert.Equal(t, commonpb.IndexState_Finished, finished.IndexState)
	assert.Equal(t, []string{"0", "1"}, finished.IndexFileKeys)
}

// The writer-side backstop behind the gate above: an entry the fail-closed
// readers would refuse - here one whose definition still exists but carries an
// empty IndexName - must fail the publish attempt with an error rather than
// commit, and rather than silently falling back to the legacy path.
func TestPublishIndexToManifestRejectsUnusableEntry(t *testing.T) {
	withSegmentIndexManifestWrites(t, true)
	store := newFakeManifestStore(t)

	catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	m := bootMetaForRestart(t, catalog, restartCollID)
	// A definition with an empty name: IsIndexExist passes, but the entry
	// built from it is one every manifest reader rejects.
	seedUnpublishedBuild(t, m, "")

	segIdx, ok := m.indexMeta.GetIndexJob(restartBuildID)
	require.True(t, ok)
	it := newIndexBuildTask(segIdx, 1, m, nil, nil, nil)

	published, err := it.publishIndexToManifest(&workerpb.IndexTaskInfo{
		BuildID:               restartBuildID,
		State:                 commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"0", "1"},
		SerializedSize:        4096,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	})
	require.Error(t, err, "an entry the readers would refuse must fail the publish, not commit or fall back")
	require.False(t, published)
	assert.Contains(t, err.Error(), "unusable index entry")
	assert.Empty(t, store.revisions, "the unusable entry must never reach the manifest")
}

// validateManifestIndexPublishable must apply exactly the reader predicate.
func TestValidateManifestIndexPublishable(t *testing.T) {
	good := packed.ManifestIndexInfo{
		IndexID:               500,
		BuildID:               5100,
		IndexName:             "idx",
		IndexType:             "HNSW",
		Path:                  "root/index/100/10/5001/5100/1",
		IndexFileKeys:         []string{"0"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}
	require.NoError(t, validateManifestIndexPublishable(5001, good))

	nameless := good
	nameless.IndexName = ""
	require.Error(t, validateManifestIndexPublishable(5001, nameless))

	pathless := good
	pathless.Path = ""
	require.Error(t, validateManifestIndexPublishable(5001, pathless))
}
