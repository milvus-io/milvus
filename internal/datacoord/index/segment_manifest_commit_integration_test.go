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
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
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
	finished, err := indexMeta.buildFinishedSegmentIndex(segIdx, taskInfo)
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
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
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
		Level:          datapb.SegmentLevel_L1,
		InsertChannel:  "index-publication-vchannel",
		NumOfRows:      4096,
		StorageVersion: storage.StorageV3,
		ManifestPath:   oldManifest,
	})))
	manager, err := dataview.RecoverManager(ctx, mt.Catalog.(dataview.Catalog), func(context.Context, int64) (bool, error) { return true, nil }, mt.LoadableProjection, nil, nil)
	require.NoError(t, err)
	mt.DataViewManager = manager
	loadable, err := mt.LoadableProjection(ctx, collectionID, nil)
	require.NoError(t, err)
	_, err = manager.OnBootstrapCollection(ctx, dataview.BootstrapCollectionDataViewEvent{CollectionID: collectionID, VChannels: []string{"index-publication-vchannel"}, Segments: loadable})
	require.NoError(t, err)
	previous, err := manager.Latest(ctx, collectionID)
	require.NoError(t, err)
	defer previous.Deref()
	require.NoError(t, dm.IndexMeta.AddSegmentIndex(context.Background(), &model.SegmentIndex{
		CollectionID:          collectionID,
		PartitionID:           partitionID,
		SegmentID:             segmentID,
		IndexID:               indexID,
		BuildID:               buildID,
		IndexVersion:          4,
		IndexState:            commonpb.IndexState_InProgress,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
	}))

	commit := mockey.Mock(packed.CommitManifestUpdatesWithResultAsync).To(
		func(_ context.Context, _ *packed.ManifestIOContext, baseManifest string, _ *indexpb.StorageConfig, updates *packed.ManifestUpdates) (packed.ManifestUpdateResult, error) {
			// The transaction opens at the segment's currently published
			// revision, not at whatever revision the build was issued against.
			require.Equal(t, oldManifest, baseManifest)
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
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
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
	// The same physical-index commit must drive a new DataView even though
	// SegmentID and SchemaVersion did not change. Retained Views keep revision 3.
	require.Eventually(t, func() bool {
		next, err := manager.Latest(ctx, collectionID)
		if err != nil || next == nil {
			return false
		}
		defer next.Deref()
		return next.Version().GetStreamingVersion() == previous.Version().GetStreamingVersion() &&
			next.Version().GetCompactVersion() == previous.Version().GetCompactVersion()+1 &&
			next.DataView().GetShards()[0].GetPartitions()[0].GetSegmentManifestVersions()[0] == 4
	}, 5*time.Second, time.Millisecond)
	require.EqualValues(t, 3, previous.DataView().GetShards()[0].GetPartitions()[0].GetSegmentManifestVersions()[0])
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
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
	}))

	taskInfo := &workerpb.IndexTaskInfo{
		BuildID:               buildID,
		State:                 commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"0"},
		SerializedSize:        100,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
	}
	entry := manifestIndexEntryForCommitTest(t, mt, dm.IndexMeta, segmentID, buildID, taskInfo)

	// Simulate a reset after the caller built entry. The same build ID now has
	// a newer task version, which must not be represented by the old artifact.
	require.NoError(t, dm.IndexMeta.UpdateVersion(buildID, 1000))

	manifestCalls := 0
	commit := mockey.Mock(packed.CommitManifestUpdatesWithResultAsync).To(
		func(context.Context, *packed.ManifestIOContext, string, *indexpb.StorageConfig, *packed.ManifestUpdates) (packed.ManifestUpdateResult, error) {
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
		func(_ context.Context, _ *packed.ManifestIOContext, baseManifest string, _ *indexpb.StorageConfig, updates *packed.ManifestUpdates) (packed.ManifestUpdateResult, error) {
			require.Equal(t, oldManifest, baseManifest)
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
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
		CollectionID:          collectionID,
		PartitionID:           partitionID,
		SegmentID:             segmentID,
		IndexID:               indexID,
		BuildID:               buildID,
		IndexVersion:          1,
		IndexState:            commonpb.IndexState_InProgress,
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
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
		BuildID:               buildID,
		State:                 commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"0"},
		SerializedSize:        100,
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
// Index DDL takes indexMeta.fieldIndexLock's write side to install field index
// definitions. The commit installs its staged SegmentIndex mutations inside its
// segMu critical section, so it must never wait for that lock: sync.RWMutex is
// writer-preferring, and a commit parked on it with segMu held stalls every
// GetSegment/SelectSegments/SaveBinlogPaths. The install updates indexMeta's
// memory-only task counters, and the stored-index-size gauge is published from
// them without the lock, so the whole commit completes while the lock is held.
func TestCommitSegmentManifestDoesNotWaitForIndexDDL(t *testing.T) {
	meta.WithSegmentIndexManifestWrites(t, true)
	meta.NewFakeManifestStore(t)
	ctx := context.TODO()

	catalog := datacoord.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	m, mDomains := BootMetaForRestart(t, catalog, meta.RestartCollID)
	SeedRestartFixture(t, m, mDomains.IndexMeta)

	segIdx, ok := mDomains.IndexMeta.GetIndexJob(meta.RestartBuildID)
	require.True(t, ok)
	finished, err := mDomains.IndexMeta.buildFinishedSegmentIndex(segIdx, &workerpb.IndexTaskInfo{
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
		BuildID:               meta.RestartBuildID,
		State:                 commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"0", "1"},
	})
	require.NoError(t, err)
	entry, err := meta.BuildManifestIndexInfo(m, m.GetSegment(ctx, meta.RestartSegID), finished)
	require.NoError(t, err)
	baseManifest := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()

	// Stand in for an index DDL installing definitions.
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
						IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
						BuildID:               meta.RestartBuildID,
						State:                 commonpb.IndexState_Finished,
						IndexFileKeys:         []string{"0", "1"},
					},
				}},
			},
		})
	}()

	select {
	case err = <-committed:
		mDomains.IndexMeta.fieldIndexLock.Unlock()
	case <-time.After(10 * time.Second):
		mDomains.IndexMeta.fieldIndexLock.Unlock()
		<-committed
		t.Fatal("commit waited on the field index lock")
	}
	require.NoError(t, err)
	assert.NotEqual(t, baseManifest, m.GetSegment(ctx, meta.RestartSegID).GetManifestPath(),
		"publication must complete while index DDL holds the field index lock")
}

// Regression test for a stall between the manifest commit and a Prometheus
// scrape. A scrape holds the stored-index-size gauge vector's read lock while it
// sends series, and creating or deleting a series waits for it, so a gauge write
// inside the commit's segMu critical section would stall every segment reader
// for the length of the scrape. The commit publishes the gauge only after segMu
// is released.
func TestCommitSegmentManifestPublishesIndexSizeOutsideSegMu(t *testing.T) {
	meta.WithSegmentIndexManifestWrites(t, true)
	meta.NewFakeManifestStore(t)
	ctx := context.TODO()

	catalog := datacoord.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	m, mDomains := BootMetaForRestart(t, catalog, meta.RestartCollID)
	SeedRestartTask(t, m, mDomains.IndexMeta)
	baseManifest := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()

	metrics.DataCoordStoredIndexFilesSize.Reset()
	t.Cleanup(metrics.DataCoordStoredIndexFilesSize.Reset)
	release := holdStoredIndexSizeScrape(t)

	// Publishing the finished task creates the collection's series.
	segIdx, ok := mDomains.IndexMeta.GetIndexJob(meta.RestartBuildID)
	require.True(t, ok)
	it := NewIndexBuildTask(segIdx, 1, m, mDomains.IndexMeta, nil, nil, nil)
	committed := make(chan error, 1)
	go func() {
		committed <- it.SetJobInfo(&workerpb.IndexTaskInfo{
			BuildID:               meta.RestartBuildID,
			State:                 commonpb.IndexState_Finished,
			IndexFileKeys:         []string{"0", "1"},
			SerializedSize:        4096,
			IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
		})
	}()

	require.Eventually(t, func() bool {
		if !m.SegMu.TryLock() {
			return false
		}
		defer m.SegMu.Unlock()
		return m.Segments.GetSegment(meta.RestartSegID).GetManifestPath() != baseManifest
	}, 10*time.Second, time.Millisecond, "commit held segMu while its gauge write waited for the scrape")
	require.Empty(t, committed, "the gauge write must wait for the scrape")
	release()
	require.NoError(t, <-committed)
	require.Equal(t, map[int64]int64{meta.RestartCollID: 4096}, storedIndexSizeSeries(t))
}

// A backfill replace holds fieldIndexLock's read side across its publication
// section, so a CreateIndex for the replaced field that starts while the
// replace is committing is blocked until the new pointer is published. Without
// that hold, the definition could be created between the verify-set
// re-resolution and the check, and its build would read the pre-replace
// manifest in that gap — FinishTask would then accept an index built from the
// old column values after backfill succeeded.
func TestCommitSegmentManifestReplaceBlocksIndexCreationUntilPublished(t *testing.T) {
	const (
		collectionID = int64(1)
		segmentID    = int64(310)
	)
	basePath := "/tmp/milvus/insert_log/1/10/310"
	oldManifest := packed.MarshalManifestPath(basePath, 3)
	newManifest := packed.MarshalManifestPath(basePath, 4)

	mt, dm, err := NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	require.NoError(t, mt.AddSegment(context.Background(), meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collectionID,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   oldManifest,
	})))

	commit := mockey.Mock(packed.CommitManifestUpdatesWithResultAsync).To(
		func(_ context.Context, _ *packed.ManifestIOContext, baseManifest string, _ *indexpb.StorageConfig, updates *packed.ManifestUpdates) (packed.ManifestUpdateResult, error) {
			require.Equal(t, oldManifest, baseManifest)
			require.Equal(t, []string{"100"}, updates.DropColumns)
			return meta.MockManifestUpdateResult(newManifest, updates), nil
		},
	).Build()
	defer commit.UnPatch()

	// Park the commit inside its segMu publication section (blocked in the
	// catalog write). At that point it holds fieldIndexLock's read side, which
	// the fix keeps through the pointer advance.
	enteredCatalog := make(chan struct{})
	releaseCatalog := make(chan struct{})
	mt.Catalog = &meta.BlockingManifestCatalog{
		DataCoordCatalog: mt.Catalog,
		Entered:          enteredCatalog,
		Release:          releaseCatalog,
	}

	result := make(chan error, 1)
	go func() {
		result <- mt.CommitSegmentManifest(context.Background(), meta.SegmentManifestCommit{
			SegmentID:     segmentID,
			StorageConfig: &indexpb.StorageConfig{},
			Mutation: meta.ManifestMutation{
				Type:    meta.ManifestMutationCommitUpdates,
				Updates: &packed.ManifestUpdates{DropColumns: []string{"100"}},
			},
		})
	}()

	select {
	case <-enteredCatalog:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "commit did not reach the publication section")
	}

	created := make(chan error, 1)
	go func() {
		created <- dm.IndexMeta.CreateIndex(context.Background(), &model.Index{
			CollectionID: collectionID, FieldID: 100, IndexID: 10, IndexName: "idx_100",
		})
	}()

	// The CreateIndex must wait on fieldIndexLock until the replace publishes;
	// without the fix nothing held fieldIndexLock during publication, so the
	// definition could slip in between the verify-set re-resolution and the
	// pointer advance.
	select {
	case err := <-created:
		require.FailNow(t, "CreateIndex completed while the replace was committing", "err=%v", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(releaseCatalog)
	require.NoError(t, <-result)

	select {
	case err := <-created:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("CreateIndex did not complete after the replace published")
	}

	// The replace published and the index definition arrived only afterwards, so
	// its build reads the new column values, never the pre-replace ones.
	require.Equal(t, newManifest, mt.GetSegment(context.Background(), segmentID).GetManifestPath())
	require.True(t, dm.IndexMeta.IsIndexExist(collectionID, 10))
}

// A backfill replace drops a column whose text/json-key stats were built from
// the OLD values; milvus-storage's DropColumns leaves the segment's stats
// entries alone, so the commit must retire them atomically with the manifest
// pointer or needDoTextIndex/needDoJSONKeyIndex would never rebuild and
// text-match / JSON filters would keep answering from data that no longer
// exists.
func TestCommitSegmentManifestClearsDroppedColumnStats(t *testing.T) {
	const segmentID = 300
	basePath := "/tmp/milvus/insert_log/1/10/300"
	oldManifest := packed.MarshalManifestPath(basePath, 1)
	newManifest := packed.MarshalManifestPath(basePath, 2)
	mt, _, err := NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	require.NoError(t, mt.AddSegment(context.Background(), meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   oldManifest,
		TextStatsLogs: map[int64]*datapb.TextIndexStats{
			100: {FieldID: 100, BuildID: 11, Files: []string{"text_100.parquet"}},
			101: {FieldID: 101, BuildID: 21, Files: []string{"text_101.parquet"}},
		},
		JsonKeyStats: map[int64]*datapb.JsonKeyStats{
			100: {FieldID: 100, BuildID: 12, Files: []string{"json_100.parquet"}},
		},
	})))

	commit := mockey.Mock(packed.CommitManifestUpdatesWithResultAsync).To(
		func(context.Context, *packed.ManifestIOContext, string, *indexpb.StorageConfig, *packed.ManifestUpdates) (packed.ManifestUpdateResult, error) {
			return meta.MockManifestUpdateResult(newManifest, nil), nil
		},
	).Build()
	defer commit.UnPatch()

	require.NoError(t, mt.CommitSegmentManifest(context.Background(), meta.SegmentManifestCommit{
		SegmentID:     segmentID,
		StorageConfig: &indexpb.StorageConfig{},
		Mutation: meta.ManifestMutation{
			Type:    meta.ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{DropColumns: []string{"100"}},
		},
	}))

	updated := mt.GetSegment(context.Background(), segmentID)
	require.Equal(t, newManifest, updated.GetManifestPath())
	// The stale entries for the dropped field are gone so the next stats pass
	// rebuilds them from the replacement column data.
	require.Nil(t, updated.GetTextStatsLogs()[100])
	require.Nil(t, updated.GetJsonKeyStats()[100])
	// Entries for fields the commit does NOT drop are preserved.
	require.Equal(t, int64(21), updated.GetTextStatsLogs()[101].GetBuildID())
}
