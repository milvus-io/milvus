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
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metacache"
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
	ms := metacache.NewMetaStore(nil)
	ms.PutCollection(&collectionInfo{
		ID: collectionID,
		Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: fieldID, Name: "vector"},
		}},
	})
	im := &indexMeta{
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
	}
	m := &meta{
		metaStore:    ms,
		chunkManager: storage.NewLocalChunkManager(objectstorage.RootPath("files")),
	}
	m.indexCatalog = im
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
	ms := metacache.NewMetaStore(nil)
	im := &indexMeta{
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			1: {101: {
				CollectionID: 1,
				FieldID:      100,
				IndexID:      101,
				IndexName:    "vector_hnsw",
				IndexParams:  []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}},
			}},
		},
	}
	m := &meta{
		metaStore:    ms,
		chunkManager: storage.NewLocalChunkManager(objectstorage.RootPath("files")),
	}
	m.indexCatalog = im
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

func mockReloadManifestEntry(t *testing.T, buildID int64) {
	t.Helper()
	infos := mockey.Mock(packed.GetManifestIndexInfosAsync).Return([]packed.ManifestIndexInfo{{
		IndexID:               500,
		BuildID:               buildID,
		FieldID:               101,
		IndexName:             "idx",
		IndexType:             "HNSW",
		IndexVersion:          1,
		NumRows:               100,
		SerializedSize:        2000,
		MemSize:               3000,
		Path:                  "/tmp/test-reload/index_files/5100/1/10/5001",
		IndexFileKeys:         []string{"f0"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
	}}, nil).Build()
	t.Cleanup(func() { infos.UnPatch() })
}

// Once a successful build has retired its Finished etcd row, a restart must
// still see the segment as indexed. Reload rebuilds the record from the
// manifest and the consumers that decide "is this segment indexed" - which
// have no manifest fallback of their own - work off that rebuilt state.
func TestReloadSegmentIndexesFromManifests(t *testing.T) {
	withSegmentIndexManifestWrites(t, true)
	m, mDomains := setupManifestReloadMeta(t)
	mockReloadManifestEntry(t, 5100)

	require.Empty(t, mDomains.indexMeta.GetSegmentIndexes(100, 5001))

	require.NoError(t, m.reloadSegmentIndexesFromManifests(context.TODO()))

	segIdxes := mDomains.indexMeta.GetSegmentIndexes(100, 5001)
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
	assert.Equal(t, []int64{5001}, mDomains.indexMeta.GetIndexedSegments(100, []int64{5001}, []int64{101}))
	assert.True(t, mDomains.indexMeta.IsIndexExist(100, 500))
	_, ok := mDomains.indexMeta.GetIndexJob(5100)
	assert.True(t, ok)
}

// etcd wins on conflict, as it does for every other manifest-index consumer:
// a record loaded from the catalog must not be replaced by a manifest
// projection of the same build, which lacks the task history.
func TestReloadSegmentIndexesFromManifests_EtcdRecordWins(t *testing.T) {
	withSegmentIndexManifestWrites(t, true)
	m, mDomains := setupManifestReloadMeta(t)
	mockReloadManifestEntry(t, 5100)

	// A second index definition with no etcd record. Every healthy V3 segment
	// is read unconditionally, so what protects the etcd record below is the
	// buildID-level precedence check in the install loop - there is no
	// candidate-narrowing filter, by design.
	require.NoError(t, mDomains.indexMeta.CreateIndex(context.TODO(), &model.Index{
		CollectionID: 100,
		FieldID:      102,
		IndexID:      501,
		IndexName:    "idx2",
	}))

	require.NoError(t, mDomains.indexMeta.AddSegmentIndex(context.TODO(), &model.SegmentIndex{
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

	recovered := mDomains.indexMeta.GetSegmentIndexes(100, 5001)[500]
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
	m, mDomains := setupManifestReloadMeta(t)
	infos := mockey.Mock(packed.GetManifestIndexInfosAsync).
		Return(nil, merr.WrapErrIoFailedReason("throttled")).Build()
	defer infos.UnPatch()

	err := m.reloadSegmentIndexesFromManifests(context.TODO())
	require.Error(t, err, "a transient manifest read failure must not be swallowed")
	assert.Empty(t, mDomains.indexMeta.GetSegmentIndexes(100, 5001))
}

// The same failure must abort newMeta rather than yielding a usable meta with
// a hole in it, matching the etcd path: a ListSegmentIndexes error aborts
// newIndexMeta and therefore newMeta.
func TestUnreadableManifestAbortsMetaBoot(t *testing.T) {
	withSegmentIndexManifestWrites(t, true)
	store := newFakeManifestStore(t)
	catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	m, mDomains := bootMetaForRestart(t, catalog, restartCollID)
	seedRestartFixture(t, m, mDomains.indexMeta)

	store.failReadsFrom()

	b := broker.NewMockBroker(t)
	b.EXPECT().ShowCollectionIDs(mock.Anything).Return(&rootcoordpb.ShowCollectionIDsResponse{
		Status:        merr.Success(),
		DbCollections: []*rootcoordpb.DBCollections{{DbName: "default", CollectionIDs: []int64{restartCollID}}},
	}, nil).Maybe()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test-restart"))
	_, err := newMeta(context.TODO(), catalog, cm, metacache.NewMetaStore(catalog), b, loadIndexTestMetas(catalog, &indexTestMetas{}))
	require.Error(t, err, "startup must fail rather than come up with an incomplete indexMeta")
}

// End to end for the switch: a finished artifact is published to the manifest
// and its task row is retired from etcd, and a full DataCoord
// restart against the same catalog still comes up seeing the segment as
// indexed. This is the claim the switch has to make good on, and the reason
// the manifest-driven reload exists - the consumers asserted below have no
// manifest fallback of their own.
func TestSegmentIndexSurvivesRestartFromManifestOnly(t *testing.T) {
	withSegmentIndexManifestWrites(t, true)
	store := newFakeManifestStore(t)
	ctx := context.TODO()

	catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	m, mDomains := bootMetaForRestart(t, catalog, restartCollID)
	seedRestartFixture(t, m, mDomains.indexMeta)
	manifestEntries := store.revisions[m.GetSegment(ctx, restartSegID).GetManifestPath()]
	require.Len(t, manifestEntries, 1, "the index task must publish its artifact into the manifest")
	assert.EqualValues(t, restartBuildID, manifestEntries[0].BuildID)

	// Nothing about this index reached etcd.
	persisted, err := catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, err)
	require.Empty(t, persisted, "the switch must have kept the record out of etcd")

	// Restart: same catalog, brand new meta, memory rebuilt from scratch.
	_, restartedDomains := bootMetaForRestart(t, catalog, restartCollID)

	recovered := restartedDomains.indexMeta.GetSegmentIndexes(restartCollID, restartSegID)
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
		restartedDomains.indexMeta.GetIndexedSegments(restartCollID, []int64{restartSegID}, []int64{restartFieldID}))
	state := restartedDomains.indexMeta.GetSegmentIndexState(restartCollID, restartSegID, restartIndexID)
	assert.Equal(t, commonpb.IndexState_Finished, state.GetState())
	_, ok := restartedDomains.indexMeta.GetIndexJob(restartBuildID)
	assert.True(t, ok, "the index inspector must not reissue a build for it")

	// Turning publication off affects new completions only. The durable marker
	// still makes the already manifest-resident record recoverable.
	withSegmentIndexManifestWrites(t, false)
	_, restartedOffDomains := bootMetaForRestart(t, catalog, restartCollID)
	recoveredOff, ok := restartedOffDomains.indexMeta.GetIndexJob(restartBuildID)
	require.True(t, ok, "turning the switch off must not hide a manifest-resident index")
	assert.Equal(t, commonpb.IndexState_Finished, recoveredOff.IndexState)
	assert.Equal(t, []string{"0", "1"}, recoveredOff.IndexFileKeys)
}

// The control: with manifest writes off (the default), the same flow persists
// the record to etcd, so the test above is discriminating rather than
// trivially green.
func TestSegmentIndexPersistedToEtcdWhenManifestWritesOff(t *testing.T) {
	withSegmentIndexManifestWrites(t, false)
	store := newFakeManifestStore(t)
	ctx := context.TODO()

	catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	m, mDomains := bootMetaForRestart(t, catalog, restartCollID)
	seedRestartFixture(t, m, mDomains.indexMeta)
	assert.Empty(t, store.revisions, "the index task must not publish a manifest revision while the switch is off")

	persisted, err := catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, err)
	require.Len(t, persisted, 1)
	assert.EqualValues(t, restartBuildID, persisted[0].BuildID)
	assert.Equal(t, commonpb.IndexState_Finished, persisted[0].IndexState)

	// Recovery is always invoked to support mode changes, but an unmarked
	// all-etcd segment must cause no manifest read. Make every attempted read
	// fail and prove a disabled-mode restart still succeeds from etcd alone.
	store.failReadsFrom()
	_, restartedDomains := bootMetaForRestart(t, catalog, restartCollID)
	recovered, ok := restartedDomains.indexMeta.GetIndexJob(restartBuildID)
	require.True(t, ok)
	assert.Equal(t, commonpb.IndexState_Finished, recovered.IndexState)
}

// Regression test for the off-to-on transition with an in-flight build.
//
// A build started while the switch was off leaves an InProgress row in etcd.
// After flipping the switch on and restarting, that row reloads and the
// inspector re-dispatches the same buildID; when the build finishes, the
// record goes to the manifest and the publishing transaction deletes the task
// row. If it merely stopped updating that row, InProgress would survive forever: every later
// boot's etcd-wins dedup would resurrect it over the manifest's Finished
// entry and re-dispatch the build again - an unbounded per-restart rebuild
// loop with no GC path for the row. Publication must retire the row in
// the same transaction that publishes the manifest entry.
func TestOffEraEtcdRowRetiredOnManifestPublication(t *testing.T) {
	withSegmentIndexManifestWrites(t, false)
	newFakeManifestStore(t)
	ctx := context.TODO()

	catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	m, mDomains := bootMetaForRestart(t, catalog, restartCollID)
	seedRestartTask(t, m, mDomains.indexMeta)

	// The off-era row is in etcd, in progress.
	persisted, err := catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, err)
	require.Len(t, persisted, 1)
	require.Equal(t, commonpb.IndexState_InProgress, persisted[0].IndexState)

	// The operator flips the switch on and restarts DataCoord. The in-flight
	// row reloads from etcd; the inspector will re-dispatch the same buildID.
	withSegmentIndexManifestWrites(t, true)
	restarted, restartedDomains := bootMetaForRestart(t, catalog, restartCollID)
	segIdx, ok := restartedDomains.indexMeta.GetIndexJob(restartBuildID)
	require.True(t, ok)
	require.Equal(t, commonpb.IndexState_InProgress, segIdx.IndexState)

	// The build finishes and publishes to the manifest.
	publishRestartTask(t, restarted, restartedDomains.indexMeta)

	// The publishing transaction must have retired the off-era row.
	persisted, err = catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, err)
	require.Empty(t, persisted, "the off-era etcd row must not outlive the manifest entry that supersedes it")

	// And the next restart therefore sees the Finished manifest record, not a
	// resurrected InProgress row - the loop is closed.
	_, againDomains := bootMetaForRestart(t, catalog, restartCollID)
	recovered, ok := againDomains.indexMeta.GetIndexJob(restartBuildID)
	require.True(t, ok)
	assert.Equal(t, commonpb.IndexState_Finished, recovered.IndexState)
	assert.Equal(t, []string{"0", "1"}, recovered.IndexFileKeys)
}

// Regression test for an ABBA deadlock between the manifest commit and every
// other SegmentIndex writer.
//
// CommitSegmentManifest must take keyLock before segMu, matching every other
// SegmentIndex writer. Reversing that order forms a cycle on a shared buildID
// and can hang DataCoord globally.
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
	m, mDomains := bootMetaForRestart(t, catalog, restartCollID)
	seedRestartFixture(t, m, mDomains.indexMeta)

	segIdx, ok := mDomains.indexMeta.GetIndexJob(restartBuildID)
	require.True(t, ok)
	finished, _, err := mDomains.indexMeta.buildFinishedSegmentIndex(segIdx, &workerpb.IndexTaskInfo{
		BuildID:       restartBuildID,
		State:         commonpb.IndexState_Finished,
		IndexFileKeys: []string{"0", "1"},
	})
	require.NoError(t, err)
	entry, err := buildManifestIndexInfo(m, m.GetSegment(ctx, restartSegID), finished)
	require.NoError(t, err)

	// Stand in for any index writer holding the build's key lock - exactly the
	// state UpdateIndexState is in when it calls the manifest-backed gate.
	mDomains.indexMeta.keyLock.Lock(restartBuildID)

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
				SegmentIndexes: []SegmentIndexMutation{{
					Type:    SegmentIndexUpsert,
					BuildID: restartBuildID,
					FinishedTask: &workerpb.IndexTaskInfo{
						BuildID:       restartBuildID,
						State:         commonpb.IndexState_Finished,
						IndexFileKeys: []string{"0", "1"},
					},
				}},
			},
		})
	}()

	// Let the commit run until it parks. Manifest I/O is in-memory here, so it
	// reaches its blocking point well within this window.
	select {
	case err := <-committed:
		mDomains.indexMeta.keyLock.Unlock(restartBuildID)
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
	mDomains.indexMeta.keyLock.Unlock(restartBuildID)

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
	m, mDomains := setupManifestReloadMeta(t)
	mockReloadManifestEntry(t, 5100)

	// The index was dropped and its definition already removed from etcd -
	// exactly what recycleUnusedIndexes does before the artifact is retracted.
	require.NoError(t, mDomains.indexMeta.RemoveIndex(context.TODO(), 100, 500))
	require.False(t, mDomains.indexMeta.IsIndexExist(100, 500))

	require.NoError(t, m.reloadSegmentIndexesFromManifests(context.TODO()))

	// The record must come back even though nothing references it any more:
	// it is the only handle GC has on the stranded artifact.
	recovered, ok := mDomains.indexMeta.segmentBuildInfo.Get(5100)
	require.True(t, ok, "a dropped index's manifest entry must still be recovered, or GC can never retract it")
	assert.EqualValues(t, 500, recovered.IndexID)
	assert.EqualValues(t, 5001, recovered.SegmentID)
	assert.Contains(t, mDomains.indexMeta.GetAllSegIndexes(), int64(5100),
		"the recovered record must be visible to recycleUnusedSegIndexes")
}

// The reload is the only path that promotes a manifest entry into a
// SegmentIndex record, and that record's file keys reach removeObjectFiles
// through BuildFilePath, whose path.Join normalizes "..". An entry that could
// aim a delete outside its own buildID prefix must never become a record, and
// boot is where it has to be caught: the GC manifest-retraction path rejects
// the same entry on every cycle forever while only logging a warning.
func TestReloadRejectsUnusableManifestIndexEntry(t *testing.T) {
	withSegmentIndexManifestWrites(t, true)
	m, mDomains := setupManifestReloadMeta(t)
	infos := mockey.Mock(packed.GetManifestIndexInfosAsync).Return([]packed.ManifestIndexInfo{{
		IndexID:               500,
		BuildID:               5100,
		FieldID:               101,
		IndexName:             "idx",
		IndexType:             "HNSW",
		IndexVersion:          1,
		NumRows:               100,
		SerializedSize:        2000,
		MemSize:               3000,
		Path:                  "/tmp/test-reload/index_files/5100/1/10/5001",
		IndexFileKeys:         []string{"../../../../meta/segment-index"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
	}}, nil).Build()
	t.Cleanup(func() { infos.UnPatch() })

	err := m.reloadSegmentIndexesFromManifests(context.TODO())
	require.Error(t, err, "a manifest entry that no other consumer would accept must fail startup, not install silently")
	assert.Contains(t, err.Error(), "unusable index entry")
	assert.Empty(t, mDomains.indexMeta.GetSegmentIndexes(100, 5001))
}

// seedUnpublishedBuild is seedRestartFixture up to but not including the
// publish: the segment, an index definition with the given name, and an
// in-flight build record - the state a build is in when its worker result
// arrives.
func seedUnpublishedBuild(t *testing.T, m *meta, im *indexMeta, indexName string) {
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
	require.NoError(t, im.CreateIndex(ctx, &model.Index{
		CollectionID: restartCollID,
		FieldID:      restartFieldID,
		IndexID:      restartIndexID,
		IndexName:    indexName,
		TypeParams:   []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}},
		IndexParams:  []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}},
	}))
	require.NoError(t, im.AddSegmentIndex(ctx, &model.SegmentIndex{
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
	m, mDomains := bootMetaForRestart(t, catalog, restartCollID)
	seedUnpublishedBuild(t, m, mDomains.indexMeta, "vec_idx")

	// The drop that races the build: definition gone, build record still live.
	require.NoError(t, mDomains.indexMeta.RemoveIndex(ctx, restartCollID, restartIndexID))
	require.False(t, mDomains.indexMeta.IsIndexExist(restartCollID, restartIndexID))

	segIdx, ok := mDomains.indexMeta.GetIndexJob(restartBuildID)
	require.True(t, ok)
	it := newIndexBuildTask(segIdx, 1, m, mDomains.indexMeta, nil, nil, nil)
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
	finished, ok := mDomains.indexMeta.GetIndexJob(restartBuildID)
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
	m, mDomains := bootMetaForRestart(t, catalog, restartCollID)
	// A definition with an empty name: IsIndexExist passes, but the entry
	// built from it is one every manifest reader rejects.
	seedUnpublishedBuild(t, m, mDomains.indexMeta, "")

	segIdx, ok := mDomains.indexMeta.GetIndexJob(restartBuildID)
	require.True(t, ok)
	it := newIndexBuildTask(segIdx, 1, m, mDomains.indexMeta, nil, nil, nil)

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

// With the switch off, publishIndexToManifest declines before touching
// anything: zero manifest I/O, and setJobInfo records the result the legacy
// way - an etcd row and no manifest entry, so the segment stays unmarked.
func TestPublishIndexToManifestDeclinedWhenSwitchOff(t *testing.T) {
	withSegmentIndexManifestWrites(t, false)
	store := newFakeManifestStore(t)
	ctx := context.TODO()

	catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	m, mDomains := bootMetaForRestart(t, catalog, restartCollID)
	seedUnpublishedBuild(t, m, mDomains.indexMeta, "vec_idx")

	segIdx, ok := mDomains.indexMeta.GetIndexJob(restartBuildID)
	require.True(t, ok)
	it := newIndexBuildTask(segIdx, 1, m, mDomains.indexMeta, nil, nil, nil)
	result := &workerpb.IndexTaskInfo{
		BuildID:               restartBuildID,
		State:                 commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"0", "1"},
		SerializedSize:        4096,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}

	published, err := it.publishIndexToManifest(result)
	require.NoError(t, err)
	require.False(t, published)
	require.NoError(t, it.setJobInfo(result))

	assert.Empty(t, store.revisions, "off means no manifest revision, not a declined-then-retried one")
	persisted, err := catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, err)
	require.Len(t, persisted, 1, "the record must take the legacy etcd path")
	assert.Equal(t, commonpb.IndexState_Finished, persisted[0].IndexState)
}
