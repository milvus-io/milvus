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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
)

// A build published through the manifest commit records that fact durably, and
// the flag survives a restart. Without the durable half, a backfill scan would
// have to re-read every segment's manifest to learn the same thing.
func TestManifestPublishedFlagSurvivesRestart(t *testing.T) {
	withSegmentIndexManifestWrites(t, true)
	newFakeManifestStore(t)
	catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	m := bootMetaForRestart(t, catalog, restartCollID)
	seedRestartFixture(t, m)

	segIdx, ok := m.indexMeta.GetIndexJob(restartBuildID)
	require.True(t, ok)
	assert.True(t, segIdx.ManifestPublished,
		"a build published in the same commit as its manifest revision must be marked published")

	restarted := bootMetaForRestart(t, catalog, restartCollID)
	reloaded, ok := restarted.indexMeta.GetIndexJob(restartBuildID)
	require.True(t, ok)
	assert.True(t, reloaded.ManifestPublished, "the flag must be persisted, not process-local")
	assert.False(t, restarted.GetSegment(context.TODO(), restartSegID).NeedsManifestIndexBackfill(),
		"a segment whose only index is published has no backfill work")
}

// The flag tracks the revision's contents, not the mutation type: an upsert
// composed with a revision that does not carry the build's entry must not claim
// a publication. Otherwise the backfill would skip the segment forever.
func TestManifestPublishedFlagRequiresMatchingEntry(t *testing.T) {
	commit := SegmentManifestCommit{
		SegmentID: restartSegID,
		Mutation: ManifestMutation{
			Type: ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{
				Indexes: []packed.ManifestIndexInfo{{IndexID: 800, BuildID: 8100}},
			},
		},
	}
	assert.True(t, commitPublishesIndexEntry(commit, 8100))
	assert.False(t, commitPublishesIndexEntry(commit, 8101),
		"another build's entry must not mark this one published")

	noop := SegmentManifestCommit{
		SegmentID: restartSegID,
		Mutation:  ManifestMutation{Type: ManifestMutationNoop, ManifestPath: "base/1"},
	}
	assert.False(t, commitPublishesIndexEntry(noop, 8100),
		"a revision this framework did not build vouches for nothing")

	empty := SegmentManifestCommit{
		SegmentID: restartSegID,
		Mutation:  ManifestMutation{Type: ManifestMutationCommitUpdates, Updates: &packed.ManifestUpdates{}},
	}
	assert.False(t, commitPublishesIndexEntry(empty, 8100))
}

// The fixture setupManifestReloadMeta builds: one healthy StorageV3 segment and
// its index definition, on collection 100.
const (
	unpublishedSegID   = UniqueID(5001)
	unpublishedIndexID = UniqueID(500)
	unpublishedBuildID = UniqueID(5100)
)

// seedIssuedIndexTask adds the in-progress SegmentIndex record an index build
// starts from, so a test can drive it to completion through either path.
func seedIssuedIndexTask(t *testing.T, m *meta) {
	t.Helper()
	require.NoError(t, m.indexMeta.AddSegmentIndex(context.TODO(), &model.SegmentIndex{
		CollectionID:          100,
		PartitionID:           10,
		SegmentID:             unpublishedSegID,
		NumRows:               100,
		IndexID:               unpublishedIndexID,
		BuildID:               unpublishedBuildID,
		IndexVersion:          1,
		IndexState:            commonpb.IndexState_InProgress,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}))
}

func finishIndexTaskResult() *workerpb.IndexTaskInfo {
	return &workerpb.IndexTaskInfo{
		BuildID:               unpublishedBuildID,
		State:                 commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"0"},
		SerializedSize:        1024,
		MemSize:               2048,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}
}

// A result recorded the legacy way - a dropped segment, a StorageV1/V2 segment,
// a dropped index definition - has no manifest entry. It must stay unpublished
// and flag its segment, which is exactly the state a backfill exists to fix.
func TestLegacyFinishTaskLeavesRecordUnpublished(t *testing.T) {
	ctx := context.TODO()
	m := setupManifestReloadMeta(t)
	seedIssuedIndexTask(t, m)

	require.NoError(t, m.indexMeta.FinishTask(finishIndexTaskResult()))
	m.markManifestIndexBackfillPending(unpublishedSegID)

	segIdx, ok := m.indexMeta.GetIndexJob(unpublishedBuildID)
	require.True(t, ok)
	assert.False(t, segIdx.ManifestPublished,
		"a record written without a manifest revision must not claim to be published")
	assert.True(t, m.GetSegment(ctx, unpublishedSegID).NeedsManifestIndexBackfill())
	assert.True(t, m.segmentHasUnpublishedIndex(unpublishedSegID))
}

// A record projected out of a manifest is published by construction.
func TestSegmentIndexFromManifestIsPublished(t *testing.T) {
	segment := NewSegmentInfo(&datapb.SegmentInfo{
		ID: 7001, CollectionID: 100, PartitionID: 10, StorageVersion: storage.StorageV3,
	})
	projected := segmentIndexFromManifest(segment, packed.ManifestIndexInfo{
		IndexID: 900, BuildID: 9100, IndexFileKeys: []string{"0"},
	})
	assert.True(t, projected.ManifestPublished)
	assert.False(t, segmentIndexNeedsManifestBackfill(projected))
}

// The backfill predicate must exclude the cases nothing will ever publish, or
// they would hold their segment pending forever.
func TestSegmentIndexNeedsManifestBackfillPredicate(t *testing.T) {
	finished := func() *model.SegmentIndex {
		return &model.SegmentIndex{
			SegmentID:     7001,
			IndexID:       900,
			BuildID:       9100,
			IndexState:    commonpb.IndexState_Finished,
			IndexFileKeys: []string{"0"},
		}
	}
	assert.True(t, segmentIndexNeedsManifestBackfill(finished()))

	published := finished()
	published.ManifestPublished = true
	assert.False(t, segmentIndexNeedsManifestBackfill(published))

	// A fake-finished build (segment too small to train) uploads no files, so it
	// has no artifact to record - publishIndexToManifest skips it too.
	fakeFinished := finished()
	fakeFinished.IndexFileKeys = nil
	assert.False(t, segmentIndexNeedsManifestBackfill(fakeFinished))

	inProgress := finished()
	inProgress.IndexState = commonpb.IndexState_InProgress
	assert.False(t, segmentIndexNeedsManifestBackfill(inProgress))

	deleted := finished()
	deleted.IsDeleted = true
	assert.False(t, segmentIndexNeedsManifestBackfill(deleted))

	assert.False(t, segmentIndexNeedsManifestBackfill(nil))
}

// The flag is set, never cleared: an ordinary task-state transition clones the
// record forward and must carry it along, or a published entry would look like
// backfill work on the next scan.
func TestManifestPublishedFlagSurvivesStateTransitions(t *testing.T) {
	m := setupManifestReloadMeta(t)
	seedIssuedIndexTask(t, m)
	segIdx, ok := m.indexMeta.GetIndexJob(unpublishedBuildID)
	require.True(t, ok)

	published := model.CloneSegmentIndex(segIdx)
	published.ManifestPublished = true
	published.IndexState = commonpb.IndexState_Finished
	published.IndexFileKeys = []string{"0"}
	require.NoError(t, m.indexMeta.alterSegmentIndexes([]*model.SegmentIndex{published}))

	require.NoError(t, m.indexMeta.UpdateVersion(unpublishedBuildID, 42))
	after, ok := m.indexMeta.GetIndexJob(unpublishedBuildID)
	require.True(t, ok)
	assert.True(t, after.ManifestPublished, "a node reassignment must not lose the publication fact")

	require.NoError(t, m.indexMeta.BuildIndex(unpublishedBuildID))
	after, ok = m.indexMeta.GetIndexJob(unpublishedBuildID)
	require.True(t, ok)
	assert.True(t, after.ManifestPublished)
}

// The segment-level hint is what makes a backfill scan a pure in-memory pass.
// It must be seeded at boot from records that predate manifest publication,
// which is exactly how an upgraded cluster's backlog becomes visible.
func TestManifestIndexBackfillHintSeededAtBoot(t *testing.T) {
	ctx := context.TODO()
	m := setupManifestReloadMeta(t)
	seedIssuedIndexTask(t, m)
	require.NoError(t, m.indexMeta.FinishTask(finishIndexTaskResult()))
	require.False(t, m.GetSegment(ctx, unpublishedSegID).NeedsManifestIndexBackfill(),
		"precondition: the hint is not set yet")

	m.initManifestIndexBackfillPending(ctx)
	assert.True(t, m.GetSegment(ctx, unpublishedSegID).NeedsManifestIndexBackfill(),
		"an unpublished finished record must surface as backfill work")

	// Removing the record - GC collecting it - is what may clear the hint, and
	// only a recompute against the records may do so.
	require.NoError(t, m.indexMeta.RemoveSegmentIndex(ctx, unpublishedBuildID))
	m.refreshManifestIndexBackfillPending(unpublishedSegID)
	assert.False(t, m.GetSegment(ctx, unpublishedSegID).NeedsManifestIndexBackfill())
}

// The hint is a worklist for the backfill inspector, whose candidate filter is
// healthy + StorageV3 + not-L0. Flagging a segment outside that shape - every
// indexed segment of an upgraded StorageV1/V2 cluster, for instance - is work
// no consumer will ever pick up, and it makes the pending count (the signal
// that the migration is done) permanently non-zero.
func TestManifestIndexBackfillHintSkipsIneligibleSegments(t *testing.T) {
	ctx := context.TODO()

	cases := []struct {
		name    string
		segment *datapb.SegmentInfo
	}{
		{"legacy storage version", &datapb.SegmentInfo{
			ID: unpublishedSegID, CollectionID: 100, PartitionID: 10,
			State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV2,
		}},
		{"dropped segment", &datapb.SegmentInfo{
			ID: unpublishedSegID, CollectionID: 100, PartitionID: 10,
			State: commonpb.SegmentState_Dropped, StorageVersion: storage.StorageV3,
		}},
		{"L0 segment", &datapb.SegmentInfo{
			ID: unpublishedSegID, CollectionID: 100, PartitionID: 10,
			State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3,
			Level: datapb.SegmentLevel_L0,
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, err := newMemoryMeta(t)
			require.NoError(t, err)
			require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(tc.segment)))
			require.NoError(t, m.indexMeta.CreateIndex(ctx, &model.Index{
				CollectionID: 100, FieldID: 101, IndexID: unpublishedIndexID, IndexName: "idx",
			}))
			seedIssuedIndexTask(t, m)
			require.NoError(t, m.indexMeta.FinishTask(finishIndexTaskResult()))

			// The record itself is real backfill work; only the segment's shape
			// makes it unreachable.
			require.True(t, m.segmentHasUnpublishedIndex(unpublishedSegID))

			m.markManifestIndexBackfillPending(unpublishedSegID)
			assert.False(t, m.GetSegment(ctx, unpublishedSegID).NeedsManifestIndexBackfill())

			m.initManifestIndexBackfillPending(ctx)
			assert.False(t, m.GetSegment(ctx, unpublishedSegID).NeedsManifestIndexBackfill())

			m.refreshManifestIndexBackfillPending(unpublishedSegID)
			assert.False(t, m.GetSegment(ctx, unpublishedSegID).NeedsManifestIndexBackfill())
		})
	}
}

// The hint rides on the process-local half of SegmentInfo, so both clone forms
// must carry it; a clone that drops it would silently exempt the segment.
func TestManifestIndexBackfillHintClones(t *testing.T) {
	segment := NewSegmentInfo(&datapb.SegmentInfo{ID: 7001, CollectionID: 100})
	segment.manifestIndexBackfillPending = true
	assert.True(t, segment.Clone().NeedsManifestIndexBackfill())
	assert.True(t, segment.ShadowClone().NeedsManifestIndexBackfill())
	assert.False(t, (*SegmentInfo)(nil).NeedsManifestIndexBackfill())
}
