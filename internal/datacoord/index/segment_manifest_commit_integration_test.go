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

package index

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// TestCommitSegmentManifestPublishesIndexTaskAtomically proves the index task
// record and the manifest pointer that publishes its artifact land in one
// catalog transaction, and that in-memory index metadata is installed only
// after that write.
func manifestIndexEntryForCommitTest(t *testing.T, m *meta.Meta, indexMeta *IndexMeta, segmentID, buildID int64, taskInfo *workerpb.IndexTaskInfo) packed.ManifestIndexInfo {
	t.Helper()
	if m.ChunkManager == nil {
		m.ChunkManager = storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/milvus"))
	}
	segIdx, ok := indexMeta.GetIndexJob(buildID)
	require.True(t, ok)
	finished, _, err := indexMeta.buildFinishedSegmentIndex(segIdx, taskInfo)
	require.NoError(t, err)
	entry, err := meta.BuildManifestIndexInfo(m, m.GetSegment(context.Background(), segmentID), finished)
	require.NoError(t, err)
	// These focused commit tests do not load collection/index definitions; the
	// task-owned fields above are the contract under test.
	entry.ColumnName = "field"
	entry.IndexName = "index"
	entry.IndexType = "HNSW"
	return entry
}

func TestCommitSegmentManifestPublishesIndexTaskAtomically(t *testing.T) {
	const (
		collectionID = int64(1)
		partitionID  = int64(10)
		segmentID    = int64(210)
		indexID      = int64(101)
		buildID      = int64(102)
	)
	basePath := "/tmp/milvus/insert_log/1/10/210"
	oldManifest := packed.MarshalManifestPath(basePath, 3)
	newManifest := packed.MarshalManifestPath(basePath, 4)

	mt, dm, err := NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	require.NoError(t, mt.AddSegment(context.Background(), meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collectionID,
		PartitionID:    partitionID,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   oldManifest,
	})))
	require.NoError(t, dm.IndexMeta.AddSegmentIndex(context.Background(), &model.SegmentIndex{
		CollectionID:          collectionID,
		PartitionID:           partitionID,
		SegmentID:             segmentID,
		IndexID:               indexID,
		BuildID:               buildID,
		IndexVersion:          4,
		IndexState:            commonpb.IndexState_InProgress,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}))

	commit := mockey.Mock(packed.CommitManifestUpdatesWithResultAsync).To(
		func(_ context.Context, _ *packed.ManifestIOContext, base string, version int64, _ *indexpb.StorageConfig, updates *packed.ManifestUpdates) (packed.ManifestUpdateResult, error) {
			// The transaction opens at the segment's currently published
			// revision, not at whatever revision the build was issued against.
			require.Equal(t, basePath, base)
			require.EqualValues(t, 3, version)
			require.Len(t, updates.Indexes, 1)
			require.EqualValues(t, indexID, updates.Indexes[0].IndexID)
			return meta.MockManifestUpdateResult(newManifest, updates), nil
		},
	).Build()
	defer commit.UnPatch()

	taskInfo := &workerpb.IndexTaskInfo{
		BuildID:               buildID,
		State:                 commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"0", "1"},
		SerializedSize:        2000,
		MemSize:               3000,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}
	entry := manifestIndexEntryForCommitTest(t, mt, dm.IndexMeta, segmentID, buildID, taskInfo)
	require.NoError(t, mt.CommitSegmentManifest(context.Background(), meta.SegmentManifestCommit{
		SegmentID:     segmentID,
		StorageConfig: &indexpb.StorageConfig{},
		Mutation: meta.ManifestMutation{
			Type:    meta.ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{Indexes: []packed.ManifestIndexInfo{entry}},
		},
		CatalogMutation: meta.SegmentCatalogMutation{
			SegmentIndexes: []meta.SegmentIndexMutation{{
				Type:         meta.SegmentIndexUpsert,
				BuildID:      buildID,
				FinishedTask: taskInfo,
			}},
		},
	}))

	require.Equal(t, newManifest, mt.GetSegment(context.Background(), segmentID).GetManifestPath())
	require.True(t, mt.GetSegment(context.Background(), segmentID).GetManifestHasIndex())
	persistedSegments, err := mt.Catalog.ListSegments(context.Background(), collectionID)
	require.NoError(t, err)
	require.Len(t, persistedSegments, 1)
	require.True(t, persistedSegments[0].GetManifestHasIndex())
	published, ok := dm.IndexMeta.GetIndexJob(buildID)
	require.True(t, ok)
	require.Equal(t, commonpb.IndexState_Finished, published.IndexState)
	require.Equal(t, []string{"0", "1"}, published.IndexFileKeys)
	require.EqualValues(t, 2000, published.IndexSerializedSize)
}

// The manifest entry is assembled before CommitSegmentManifest takes the
// build lock. If the task is reset in that window, publishing the old entry
// would pair the manifest with a newer SegmentIndex projection. Reject the
// stale entry before creating any manifest revision.
func TestCommitSegmentManifestRejectsStaleIndexTaskProjection(t *testing.T) {
	const (
		collectionID = int64(1)
		partitionID  = int64(10)
		segmentID    = int64(218)
		indexID      = int64(103)
		buildID      = int64(104)
	)
	basePath := "/tmp/milvus/insert_log/1/10/218"
	oldManifest := packed.MarshalManifestPath(basePath, 3)

	mt, dm, err := NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	require.NoError(t, mt.AddSegment(context.Background(), meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collectionID,
		PartitionID:    partitionID,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   oldManifest,
	})))
	require.NoError(t, dm.IndexMeta.AddSegmentIndex(context.Background(), &model.SegmentIndex{
		CollectionID:          collectionID,
		PartitionID:           partitionID,
		SegmentID:             segmentID,
		IndexID:               indexID,
		BuildID:               buildID,
		IndexVersion:          4,
		IndexState:            commonpb.IndexState_InProgress,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}))

	taskInfo := &workerpb.IndexTaskInfo{
		BuildID:               buildID,
		State:                 commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"0"},
		SerializedSize:        100,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}
	entry := manifestIndexEntryForCommitTest(t, mt, dm.IndexMeta, segmentID, buildID, taskInfo)

	// Simulate a reset after the caller built entry. The same build ID now has
	// a newer task version, which must not be represented by the old artifact.
	require.NoError(t, dm.IndexMeta.UpdateVersion(buildID, 1000))

	manifestCalls := 0
	commit := mockey.Mock(packed.CommitManifestUpdatesWithResultAsync).To(
		func(context.Context, *packed.ManifestIOContext, string, int64, *indexpb.StorageConfig, *packed.ManifestUpdates) (packed.ManifestUpdateResult, error) {
			manifestCalls++
			return meta.MockManifestUpdateResult(packed.MarshalManifestPath(basePath, 4), nil), nil
		},
	).Build()
	defer commit.UnPatch()

	err = mt.CommitSegmentManifest(context.Background(), meta.SegmentManifestCommit{
		SegmentID:     segmentID,
		StorageConfig: &indexpb.StorageConfig{},
		Mutation: meta.ManifestMutation{
			Type:    meta.ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{Indexes: []packed.ManifestIndexInfo{entry}},
		},
		CatalogMutation: meta.SegmentCatalogMutation{
			SegmentIndexes: []meta.SegmentIndexMutation{{
				Type:         meta.SegmentIndexUpsert,
				BuildID:      buildID,
				FinishedTask: taskInfo,
			}},
		},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Zero(t, manifestCalls)
	require.Equal(t, oldManifest, mt.GetSegment(context.Background(), segmentID).GetManifestPath())
	current, ok := dm.IndexMeta.GetIndexJob(buildID)
	require.True(t, ok)
	require.EqualValues(t, 5, current.IndexVersion)
	require.Equal(t, commonpb.IndexState_InProgress, current.IndexState)
}

// A worker result for a task that was deleted mid-flight must not advance the
// pointer: publishing it would strand a manifest index entry with no
// SegmentIndex record to drive its GC.
func TestCommitSegmentManifestDiscardsResultForDeletedIndexTask(t *testing.T) {
	basePath := "/tmp/milvus/insert_log/1/10/211"
	oldManifest := packed.MarshalManifestPath(basePath, 3)
	mt, _, err := NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	require.NoError(t, mt.AddSegment(context.Background(), meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             211,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   oldManifest,
	})))

	commit := mockey.Mock(packed.CommitManifestUpdatesWithResultAsync).
		Return(meta.MockManifestUpdateResult(packed.MarshalManifestPath(basePath, 4), nil), nil).Build()
	defer commit.UnPatch()

	require.NoError(t, mt.CommitSegmentManifest(context.Background(), meta.SegmentManifestCommit{
		SegmentID:     211,
		StorageConfig: &indexpb.StorageConfig{},
		Mutation: meta.ManifestMutation{
			Type:    meta.ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{Indexes: []packed.ManifestIndexInfo{{IndexID: 1, BuildID: 999}}},
		},
		CatalogMutation: meta.SegmentCatalogMutation{
			SegmentIndexes: []meta.SegmentIndexMutation{{
				Type:    meta.SegmentIndexUpsert,
				BuildID: 999,
				FinishedTask: &workerpb.IndexTaskInfo{
					BuildID: 999,
					State:   commonpb.IndexState_Finished,
				},
			}},
		},
	}))
	require.Equal(t, oldManifest, mt.GetSegment(context.Background(), 211).GetManifestPath())
}

// The GC counterpart of the index-task commit: the revision that retracts an
// index artifact and the removal of the SegmentIndex record claiming it must
// become visible together, so a reader can never load an index whose artifact
// the published manifest no longer carries.
func TestCommitSegmentManifestRemovesSegmentIndexWithRetraction(t *testing.T) {
	const (
		collectionID = int64(1)
		partitionID  = int64(10)
		segmentID    = int64(214)
		indexID      = int64(700)
		buildID      = int64(701)
	)
	basePath := "/tmp/milvus/insert_log/1/10/214"
	oldManifest := packed.MarshalManifestPath(basePath, 3)
	newManifest := packed.MarshalManifestPath(basePath, 4)

	mt, dm, err := NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	require.NoError(t, mt.AddSegment(context.Background(), meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:               segmentID,
		CollectionID:     collectionID,
		PartitionID:      partitionID,
		State:            commonpb.SegmentState_Flushed,
		StorageVersion:   storage.StorageV3,
		ManifestPath:     oldManifest,
		ManifestHasIndex: true,
	})))
	require.NoError(t, dm.IndexMeta.AddSegmentIndex(context.Background(), &model.SegmentIndex{
		CollectionID:  collectionID,
		PartitionID:   partitionID,
		SegmentID:     segmentID,
		IndexID:       indexID,
		BuildID:       buildID,
		IndexVersion:  1,
		IndexState:    commonpb.IndexState_Finished,
		IndexFileKeys: []string{"0"},
	}))

	commit := mockey.Mock(packed.CommitManifestUpdatesWithResultAsync).To(
		func(_ context.Context, _ *packed.ManifestIOContext, base string, version int64, _ *indexpb.StorageConfig, updates *packed.ManifestUpdates) (packed.ManifestUpdateResult, error) {
			require.Equal(t, basePath, base)
			require.EqualValues(t, 3, version)
			require.Len(t, updates.DropIndexes, 1)
			require.EqualValues(t, indexID, updates.DropIndexes[0].IndexID)
			return meta.MockManifestUpdateResult(newManifest, updates), nil
		},
	).Build()
	defer commit.UnPatch()
	read := mockey.Mock(packed.GetManifestIndexInfosAsync).Return(nil, merr.ErrServiceInternal).Build()
	defer read.UnPatch()
	defer func() { require.Zero(t, read.Times(), "use the commit result without reopening its manifest") }()

	require.NoError(t, mt.CommitSegmentManifest(context.Background(), meta.SegmentManifestCommit{
		SegmentID:     segmentID,
		StorageConfig: &indexpb.StorageConfig{},
		Mutation: meta.ManifestMutation{
			Type: meta.ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{
				DropIndexes: []packed.DropIndexEntry{{IndexID: indexID, ExpectedBuildID: buildID}},
			},
		},
		CatalogMutation: meta.SegmentCatalogMutation{
			SegmentIndexes: []meta.SegmentIndexMutation{{
				Type:    meta.SegmentIndexRemove,
				BuildID: buildID,
			}},
		},
	}))

	require.Equal(t, newManifest, mt.GetSegment(context.Background(), segmentID).GetManifestPath())
	require.False(t, mt.GetSegment(context.Background(), segmentID).GetManifestHasIndex(),
		"a verified empty revision clears the recovery marker atomically")
	_, ok := dm.IndexMeta.GetIndexJob(buildID)
	require.False(t, ok)
	require.Empty(t, dm.IndexMeta.GetSegmentIndexes(collectionID, segmentID))
}

// A removal whose record is already gone is not an error: the record's absence
// is the mutation's intended end state, and the manifest may still carry the
// entry an earlier interrupted attempt failed to retract. Publish anyway.
func TestCommitSegmentManifestRemovesMissingSegmentIndexStillPublishes(t *testing.T) {
	basePath := "/tmp/milvus/insert_log/1/10/215"
	oldManifest := packed.MarshalManifestPath(basePath, 3)
	newManifest := packed.MarshalManifestPath(basePath, 4)

	mt, _, err := NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	require.NoError(t, mt.AddSegment(context.Background(), meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             215,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   oldManifest,
	})))

	commit := mockey.Mock(packed.CommitManifestUpdatesWithResultAsync).Return(meta.MockManifestUpdateResult(newManifest, nil), nil).Build()
	defer commit.UnPatch()

	require.NoError(t, mt.CommitSegmentManifest(context.Background(), meta.SegmentManifestCommit{
		SegmentID:     215,
		StorageConfig: &indexpb.StorageConfig{},
		Mutation: meta.ManifestMutation{
			Type: meta.ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{
				DropIndexes: []packed.DropIndexEntry{{IndexID: 800, ExpectedBuildID: 801}},
			},
		},
		CatalogMutation: meta.SegmentCatalogMutation{
			SegmentIndexes: []meta.SegmentIndexMutation{{Type: meta.SegmentIndexRemove, BuildID: 801}},
		},
	}))
	require.Equal(t, newManifest, mt.GetSegment(context.Background(), 215).GetManifestPath())
}

// The mutation is the framework's only SegmentIndex contract, so its shape is
// validated before any manifest I/O rather than producing a half-applied
// commit.
func TestCommitSegmentManifestRejectsMalformedSegmentIndexMutation(t *testing.T) {
	basePath := "/tmp/milvus/insert_log/1/10/216"
	newMeta := func(t *testing.T) *meta.Meta {
		m, _, err := NewMemoryMetaWithIndex(t)
		require.NoError(t, err)
		require.NoError(t, m.AddSegment(context.Background(), meta.NewSegmentInfo(&datapb.SegmentInfo{
			ID:             216,
			State:          commonpb.SegmentState_Flushed,
			StorageVersion: storage.StorageV3,
			ManifestPath:   packed.MarshalManifestPath(basePath, 3),
		})))
		return m
	}

	cases := []struct {
		name     string
		mutation *meta.SegmentIndexMutation
	}{
		{"no build ID", &meta.SegmentIndexMutation{Type: meta.SegmentIndexUpsert, FinishedTask: &workerpb.IndexTaskInfo{}}},
		{"unknown type", &meta.SegmentIndexMutation{BuildID: 1}},
		{"upsert without task", &meta.SegmentIndexMutation{Type: meta.SegmentIndexUpsert, BuildID: 1}},
		{"upsert with mismatched task", &meta.SegmentIndexMutation{
			Type: meta.SegmentIndexUpsert, BuildID: 1,
			FinishedTask: &workerpb.IndexTaskInfo{BuildID: 2},
		}},
		{"removal with task", &meta.SegmentIndexMutation{
			Type: meta.SegmentIndexRemove, BuildID: 1,
			FinishedTask: &workerpb.IndexTaskInfo{BuildID: 1},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newMeta(t)
			commit := mockey.Mock(packed.CommitManifestUpdatesWithResultAsync).
				Return(meta.MockManifestUpdateResult(packed.MarshalManifestPath(basePath, 4), nil), nil).Build()
			defer commit.UnPatch()

			err := m.CommitSegmentManifest(context.Background(), meta.SegmentManifestCommit{
				SegmentID:       216,
				StorageConfig:   &indexpb.StorageConfig{},
				Mutation:        meta.ManifestMutation{Type: meta.ManifestMutationCommitUpdates, Updates: &packed.ManifestUpdates{}},
				CatalogMutation: meta.SegmentCatalogMutation{SegmentIndexes: []meta.SegmentIndexMutation{*tc.mutation}},
			})
			require.Error(t, err)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			// The pointer must not have advanced.
			require.Equal(t, packed.MarshalManifestPath(basePath, 3),
				m.GetSegment(context.Background(), 216).GetManifestPath())
		})
	}
}

// After successful manifest publication, that revision is the sole record of
// the finished artifact - which is the migration's end state. The commit must
// then carry no SegmentIndex upsert,
// but it must carry a SegmentIndex *delete*: a row for this buildID may
// pre-date the switch flipping on, and leaving it would let the next boot's
// etcd-wins dedup resurrect a non-terminal state over the manifest's Finished
// entry. indexMeta's in-memory view still completes the task.
func TestCommitSegmentManifestRetiresIndexEtcdRowWhenGated(t *testing.T) {
	meta.WithSegmentIndexManifestWrites(t, true)

	const (
		collectionID = int64(1)
		partitionID  = int64(10)
		segmentID    = int64(217)
		indexID      = int64(900)
		buildID      = int64(901)
	)
	basePath := "/tmp/milvus/insert_log/1/10/217"
	newManifest := packed.MarshalManifestPath(basePath, 4)

	mt, dm, err := NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	require.NoError(t, mt.AddSegment(context.Background(), meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collectionID,
		PartitionID:    partitionID,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath(basePath, 3),
	})))
	require.NoError(t, dm.IndexMeta.AddSegmentIndex(context.Background(), &model.SegmentIndex{
		CollectionID: collectionID,
		PartitionID:  partitionID,
		SegmentID:    segmentID,
		IndexID:      indexID,
		BuildID:      buildID,
		IndexVersion: 1,
		IndexState:   commonpb.IndexState_InProgress,
	}))

	commit := mockey.Mock(packed.CommitManifestUpdatesWithResultAsync).Return(meta.MockManifestUpdateResult(newManifest, nil), nil).Build()
	defer commit.UnPatch()

	var actionCount int
	var indexDeletes int
	update := mockey.Mock((*datacoord.Catalog).Update).To(
		func(_ *datacoord.Catalog, _ context.Context, actions ...metastore.UpdateAction) error {
			actionCount = len(actions)
			for _, action := range actions {
				entry, ok := action.Entry.(metastore.SegmentIndexEntry)
				if !ok {
					continue
				}
				switch action.Type {
				case metastore.ActionDelete:
					indexDeletes++
					assert.Equal(t, buildID, entry.SegmentIndex.BuildID)
				default:
					t.Fatal("a manifest-published finished index must retire its etcd task row")
				}
			}
			return nil
		},
	).Build()
	defer update.UnPatch()

	taskInfo := &workerpb.IndexTaskInfo{
		BuildID:        buildID,
		State:          commonpb.IndexState_Finished,
		IndexFileKeys:  []string{"0"},
		SerializedSize: 100,
	}
	entry := manifestIndexEntryForCommitTest(t, mt, dm.IndexMeta, segmentID, buildID, taskInfo)
	require.NoError(t, mt.CommitSegmentManifest(context.Background(), meta.SegmentManifestCommit{
		SegmentID:     segmentID,
		StorageConfig: &indexpb.StorageConfig{},
		Mutation: meta.ManifestMutation{
			Type:    meta.ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{Indexes: []packed.ManifestIndexInfo{entry}},
		},
		CatalogMutation: meta.SegmentCatalogMutation{
			SegmentIndexes: []meta.SegmentIndexMutation{{
				Type:         meta.SegmentIndexUpsert,
				BuildID:      buildID,
				FinishedTask: taskInfo,
			}},
		},
	}))

	assert.Equal(t, 2, actionCount, "the segment record plus the retiring index delete")
	assert.Equal(t, 1, indexDeletes, "any pre-switch etcd row must be retired in the same transaction")
	assert.Equal(t, newManifest, mt.GetSegment(context.Background(), segmentID).GetManifestPath())
	// The in-memory task still completes: the switch is durability-only.
	published, ok := dm.IndexMeta.GetIndexJob(buildID)
	require.True(t, ok)
	assert.Equal(t, commonpb.IndexState_Finished, published.IndexState)
	assert.Equal(t, []string{"0"}, published.IndexFileKeys)
}

// Regression test for a stall between the manifest commit and every index DDL.
//
// The staged index install updates the stored-index-size gauge, which must
// serialize against MarkIndexAsDeleted via indexMeta.fieldIndexLock — and every
// index DDL (CreateIndex, AlterIndex, MarkIndexAsDeleted, RemoveIndex) holds
// that lock's WRITE side across an etcd round trip. The install used to run the
// gauge update inside the commit's segMu critical section, so one concurrent
// DDL made the commit hold segMu for up to the etcd request timeout, stalling
// every GetSegment/SelectSegments/SaveBinlogPaths. The fix defers the
// fieldIndexLock-guarded gauge until after segMu is released (still under the
// buildID key lock).
//
// In the style of TestCommitSegmentManifestTakesKeyLockBeforeSegMu, this
// asserts the invariant rather than racing a window: hold fieldIndexLock's
// write side, let the commit park on the deferred gauge, and check that segMu
// is free and publication already completed.
func TestCommitSegmentManifestDefersFieldIndexLockOutsideSegMu(t *testing.T) {
	meta.WithSegmentIndexManifestWrites(t, true)
	meta.NewFakeManifestStore(t)
	ctx := context.TODO()

	catalog := datacoord.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	m, mDomains := BootMetaForRestart(t, catalog, meta.RestartCollID)
	SeedRestartFixture(t, m, mDomains.IndexMeta)

	segIdx, ok := mDomains.IndexMeta.GetIndexJob(meta.RestartBuildID)
	require.True(t, ok)
	finished, _, err := mDomains.IndexMeta.buildFinishedSegmentIndex(segIdx, &workerpb.IndexTaskInfo{
		BuildID:       meta.RestartBuildID,
		State:         commonpb.IndexState_Finished,
		IndexFileKeys: []string{"0", "1"},
	})
	require.NoError(t, err)
	entry, err := meta.BuildManifestIndexInfo(m, m.GetSegment(ctx, meta.RestartSegID), finished)
	require.NoError(t, err)
	baseManifest := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()

	// Stand in for any index DDL mid-etcd-round-trip: CreateIndex, AlterIndex,
	// MarkIndexAsDeleted and RemoveIndex all hold fieldIndexLock's write side
	// across their catalog write.
	mDomains.IndexMeta.fieldIndexLock.Lock()

	committed := make(chan error, 1)
	go func() {
		committed <- m.CommitSegmentManifest(ctx, meta.SegmentManifestCommit{
			SegmentID:     meta.RestartSegID,
			StorageConfig: &indexpb.StorageConfig{},
			Mutation: meta.ManifestMutation{
				Type:    meta.ManifestMutationCommitUpdates,
				Updates: &packed.ManifestUpdates{Indexes: []packed.ManifestIndexInfo{entry}},
			},
			CatalogMutation: meta.SegmentCatalogMutation{
				SegmentIndexes: []meta.SegmentIndexMutation{{
					Type:    meta.SegmentIndexUpsert,
					BuildID: meta.RestartBuildID,
					FinishedTask: &workerpb.IndexTaskInfo{
						BuildID:       meta.RestartBuildID,
						State:         commonpb.IndexState_Finished,
						IndexFileKeys: []string{"0", "1"},
					},
				}},
			},
		})
	}()

	// Let the commit run until it parks on the deferred gauge. Manifest I/O and
	// the catalog write are in-memory here, so it reaches its blocking point
	// well within this window.
	select {
	case err := <-committed:
		mDomains.IndexMeta.fieldIndexLock.Unlock()
		t.Fatalf("commit finished without waiting on the held field index lock: %v", err)
	case <-time.After(2 * time.Second):
	}

	// The load-bearing assertion. Parked on fieldIndexLock, the commit must
	// hold no segMu; otherwise every segment reader queues behind the DDL's
	// etcd round trip for its whole duration.
	acquired := m.SegMu.TryLock()
	if acquired {
		m.SegMu.Unlock()
	}
	var publishedManifest string
	if acquired {
		// segMu is provably free, so reading through it cannot hang; the
		// pointer must already have advanced, proving the commit finished its
		// entire publication section before waiting on fieldIndexLock.
		publishedManifest = m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()
	}
	mDomains.IndexMeta.fieldIndexLock.Unlock()

	select {
	case err := <-committed:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("commit did not complete after the field index lock was released")
	}

	require.True(t, acquired,
		"commit is holding segMu while blocked on fieldIndexLock: a concurrent index DDL would stall every segment reader")
	assert.NotEqual(t, baseManifest, publishedManifest,
		"publication must complete before the commit waits on fieldIndexLock")
}
