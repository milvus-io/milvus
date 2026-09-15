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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	metastorekv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func withManifestIndexBackfillEnabled(t *testing.T, enabled bool) {
	t.Helper()
	key := Params.DataCoordCfg.ManifestIndexBackfillEnabled.Key
	previous := Params.DataCoordCfg.ManifestIndexBackfillEnabled.GetValue()
	Params.Save(key, fmt.Sprintf("%t", enabled))
	t.Cleanup(func() { Params.Save(key, previous) })
}

func (s *fakeManifestStore) backfillEntriesAt(manifestPath string) []packed.ManifestIndexInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]packed.ManifestIndexInfo(nil), s.revisions[manifestPath]...)
}

func seedBackfillIndexDefinition(t *testing.T, m *meta) {
	t.Helper()
	if m.indexMeta.IsIndexExist(restartCollID, restartIndexID) {
		return
	}
	require.NoError(t, m.indexMeta.CreateIndex(context.TODO(), &model.Index{
		CollectionID: restartCollID,
		FieldID:      restartFieldID,
		IndexID:      restartIndexID,
		IndexName:    "vec_idx",
		TypeParams:   []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}},
		IndexParams:  []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}},
	}))
}

// seedLegacyBackfillRecord creates the pre-migration state: a healthy
// StorageV3 segment whose completed artifact exists only as a catalog row.
func seedLegacyBackfillRecord(t *testing.T, m *meta, segmentID, buildID int64) {
	t.Helper()
	ctx := context.TODO()
	seedBackfillIndexDefinition(t, m)
	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   restartCollID,
		PartitionID:    restartPartID,
		State:          commonpb.SegmentState_Flushed,
		NumOfRows:      1000,
		StorageVersion: storage.StorageV3,
		ManifestPath: packed.MarshalManifestPath(
			fmt.Sprintf("/tmp/test-backfill/insert_log/%d/%d/%d", restartCollID, restartPartID, segmentID), 1),
	})))
	require.NoError(t, m.indexMeta.AddSegmentIndex(ctx, &model.SegmentIndex{
		CollectionID:          restartCollID,
		PartitionID:           restartPartID,
		SegmentID:             segmentID,
		NumRows:               1000,
		IndexID:               restartIndexID,
		BuildID:               buildID,
		IndexVersion:          1,
		IndexState:            commonpb.IndexState_InProgress,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}))
	require.NoError(t, m.indexMeta.FinishTask(&workerpb.IndexTaskInfo{
		BuildID:               buildID,
		State:                 commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"0", "1"},
		SerializedSize:        4096,
		MemSize:               8192,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}))
}

// The final-state matrix is the migration contract: before the tick the index
// is in etcd only; after it, the same record is in the manifest only and still
// available in memory; after restart, it is rebuilt from that manifest.
func TestManifestIndexBackfillMovesCatalogRowAtomically(t *testing.T) {
	withSegmentIndexManifestWrites(t, false)
	withManifestIndexBackfillEnabled(t, true)
	store := newFakeManifestStore(t)
	ctx := context.TODO()

	catalog := metastorekv.NewCatalog(NewMetaMemoryKV(), "", "")
	m := bootMetaForRestart(t, catalog, restartCollID)
	seedLegacyBackfillRecord(t, m, restartSegID, restartBuildID)

	baseManifest := m.GetSegment(ctx, restartSegID).GetManifestPath()
	require.Empty(t, store.backfillEntriesAt(baseManifest))
	persisted, err := catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, err)
	require.Len(t, persisted, 1)
	assert.False(t, m.indexMeta.isSegmentIndexCatalogAbsent(restartBuildID))

	// Model the real upgrade boundary: provenance must be reconstructed from
	// the catalog source, not inherited from the process that finished the
	// legacy task.
	m = bootMetaForRestart(t, catalog, restartCollID)
	_, ok := m.indexMeta.GetIndexJob(restartBuildID)
	require.True(t, ok)
	assert.False(t, m.indexMeta.isSegmentIndexCatalogAbsent(restartBuildID))

	// Backfill follows the new exclusive placement and therefore runs only in
	// manifest-publication mode.
	withSegmentIndexManifestWrites(t, true)
	inspector := newManifestIndexBackfillInspector(ctx, m)
	inspector.runOnce(ctx)

	published := m.GetSegment(ctx, restartSegID).GetManifestPath()
	require.NotEqual(t, baseManifest, published)
	entries := store.backfillEntriesAt(published)
	require.Len(t, entries, 1)
	assert.EqualValues(t, restartIndexID, entries[0].IndexID)
	assert.EqualValues(t, restartBuildID, entries[0].BuildID)
	assert.Equal(t, []string{"0", "1"}, entries[0].IndexFileKeys)
	assert.True(t, m.GetSegment(ctx, restartSegID).GetManifestHasIndex())

	persisted, err = catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, err)
	assert.Empty(t, persisted, "the manifest pointer and row deletion must commit together")
	_, ok = m.indexMeta.GetIndexJob(restartBuildID)
	assert.True(t, ok, "backfill changes durable placement, not the live DataCoord view")
	assert.True(t, m.indexMeta.isSegmentIndexCatalogAbsent(restartBuildID))

	// Convergence is process-local immediately; a second tick creates no new
	// revision and reports no remaining catalog row.
	inspector.runOnce(ctx)
	assert.Equal(t, published, m.GetSegment(ctx, restartSegID).GetManifestPath())
	assert.Equal(t, 0, inspector.lastPending)

	// And it is reconstructed exactly after failover from durable source
	// placement, without a persisted per-build marker.
	restarted := bootMetaForRestart(t, catalog, restartCollID)
	recovered, ok := restarted.indexMeta.GetIndexJob(restartBuildID)
	require.True(t, ok)
	assert.Equal(t, commonpb.IndexState_Finished, recovered.IndexState)
	assert.Equal(t, []string{"0", "1"}, recovered.IndexFileKeys)
	assert.True(t, restarted.indexMeta.isSegmentIndexCatalogAbsent(restartBuildID))
	_, pending := newManifestIndexBackfillInspector(ctx, restarted).scan(ctx)
	assert.Zero(t, pending)
}

func TestManifestIndexBackfillRequiresBothSwitches(t *testing.T) {
	t.Run("migration disabled", func(t *testing.T) {
		withSegmentIndexManifestWrites(t, false)
		withManifestIndexBackfillEnabled(t, false)
		store := newFakeManifestStore(t)
		ctx := context.TODO()
		m := bootMetaForRestart(t, metastorekv.NewCatalog(NewMetaMemoryKV(), "", ""), restartCollID)
		seedLegacyBackfillRecord(t, m, restartSegID, restartBuildID)
		base := m.GetSegment(ctx, restartSegID).GetManifestPath()

		withSegmentIndexManifestWrites(t, true)
		newManifestIndexBackfillInspector(ctx, m).runOnce(ctx)
		assert.Equal(t, base, m.GetSegment(ctx, restartSegID).GetManifestPath())
		assert.Empty(t, store.backfillEntriesAt(base))
	})

	t.Run("manifest publication disabled", func(t *testing.T) {
		withSegmentIndexManifestWrites(t, false)
		withManifestIndexBackfillEnabled(t, true)
		store := newFakeManifestStore(t)
		ctx := context.TODO()
		m := bootMetaForRestart(t, metastorekv.NewCatalog(NewMetaMemoryKV(), "", ""), restartCollID)
		seedLegacyBackfillRecord(t, m, restartSegID, restartBuildID)
		base := m.GetSegment(ctx, restartSegID).GetManifestPath()

		newManifestIndexBackfillInspector(ctx, m).runOnce(ctx)
		assert.Equal(t, base, m.GetSegment(ctx, restartSegID).GetManifestPath())
		assert.Empty(t, store.backfillEntriesAt(base))
	})
}

func TestManifestIndexBackfillBatchRotatesPastFailures(t *testing.T) {
	withSegmentIndexManifestWrites(t, false)
	ctx := context.TODO()
	m := bootMetaForRestart(t, metastorekv.NewCatalog(NewMetaMemoryKV(), "", ""), restartCollID)
	for offset := int64(0); offset < 3; offset++ {
		seedLegacyBackfillRecord(t, m, restartSegID+offset, restartBuildID+offset)
	}

	key := Params.DataCoordCfg.ManifestIndexBackfillBatchSize.Key
	previous := Params.DataCoordCfg.ManifestIndexBackfillBatchSize.GetValue()
	Params.Save(key, "1")
	t.Cleanup(func() { Params.Save(key, previous) })

	inspector := newManifestIndexBackfillInspector(ctx, m)
	seen := make([]int64, 0, 3)
	for idx := 0; idx < 3; idx++ {
		work, pending := inspector.scan(ctx)
		require.Len(t, work, 1)
		require.Len(t, work[0].records, 1)
		assert.Equal(t, 3, pending, "batch limiting must not cap the completion signal")
		seen = append(seen, work[0].records[0].BuildID)
	}
	assert.Equal(t, []int64{restartBuildID, restartBuildID + 1, restartBuildID + 2}, seen)
}

func TestManifestIndexBackfillScansOnlyHealthyNonL0StorageV3(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*SegmentInfo)
		pending int
	}{
		{name: "healthy StorageV3", mutate: func(*SegmentInfo) {}, pending: 1},
		{name: "StorageV2", mutate: func(segment *SegmentInfo) {
			segment.StorageVersion = storage.StorageV2
		}},
		{name: "L0", mutate: func(segment *SegmentInfo) {
			segment.Level = datapb.SegmentLevel_L0
		}},
		{name: "dropped", mutate: func(segment *SegmentInfo) {
			segment.State = commonpb.SegmentState_Dropped
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.TODO()
			m := bootMetaForRestart(t, metastorekv.NewCatalog(NewMetaMemoryKV(), "", ""), restartCollID)
			seedLegacyBackfillRecord(t, m, restartSegID, restartBuildID)
			segment := m.GetSegment(ctx, restartSegID).Clone()
			test.mutate(segment)
			m.segments.SetSegment(segment.GetID(), segment)

			work, pending := newManifestIndexBackfillInspector(ctx, m).scan(ctx)
			assert.Equal(t, test.pending, pending)
			assert.Equal(t, test.pending, countManifestIndexBackfillRecords(work))
		})
	}
}

func TestManifestIndexBackfillSkipsTaskOnlyAndManifestOnlyRecords(t *testing.T) {
	indexMeta := &indexMeta{}
	base := &model.SegmentIndex{
		BuildID:       1,
		IndexState:    commonpb.IndexState_Finished,
		IndexFileKeys: []string{"0"},
	}
	assert.True(t, segmentIndexNeedsManifestBackfill(indexMeta, base))

	for name, mutate := range map[string]func(*model.SegmentIndex){
		"deleted":       func(index *model.SegmentIndex) { index.IsDeleted = true },
		"in progress":   func(index *model.SegmentIndex) { index.IndexState = commonpb.IndexState_InProgress },
		"failed":        func(index *model.SegmentIndex) { index.IndexState = commonpb.IndexState_Failed },
		"fake finished": func(index *model.SegmentIndex) { index.IndexFileKeys = nil },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := model.CloneSegmentIndex(base)
			mutate(candidate)
			assert.False(t, segmentIndexNeedsManifestBackfill(indexMeta, candidate))
		})
	}

	indexMeta.segmentIndexCatalogAbsent.Upsert(base.BuildID)
	assert.False(t, segmentIndexNeedsManifestBackfill(indexMeta, base),
		"a manifest-resident record has no catalog row to migrate")
}

// Copy/restore installs manifest-resident records directly. They must seed the
// same process-local placement state as startup reload so the optional
// backfill never mistakes them for historical etcd rows.
func TestManifestIndexBackfillIgnoresCopiedManifestRecord(t *testing.T) {
	newFakeManifestStore(t)
	ctx := context.TODO()
	catalog := metastorekv.NewCatalog(NewMetaMemoryKV(), "", "")
	m := bootMetaForRestart(t, catalog, restartCollID)
	seedBackfillIndexDefinition(t, m)
	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID:               restartSegID,
		CollectionID:     restartCollID,
		PartitionID:      restartPartID,
		State:            commonpb.SegmentState_Flushed,
		StorageVersion:   storage.StorageV3,
		ManifestPath:     packed.MarshalManifestPath("/tmp/test-backfill/copied", 1),
		ManifestHasIndex: true,
	})))
	require.NoError(t, m.indexMeta.AddSegmentIndexFromManifest(ctx, &model.SegmentIndex{
		CollectionID:          restartCollID,
		PartitionID:           restartPartID,
		SegmentID:             restartSegID,
		NumRows:               1000,
		IndexID:               restartIndexID,
		BuildID:               restartBuildID,
		IndexVersion:          1,
		IndexState:            commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"0"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}))

	work, pending := newManifestIndexBackfillInspector(ctx, m).scan(ctx)
	assert.Empty(t, work)
	assert.Zero(t, pending)
	persisted, err := catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, err)
	assert.Empty(t, persisted)
}

type failBackfillCatalogKV struct {
	*metaMemoryKV
	failAtomicUpdate bool
}

func (kv *failBackfillCatalogKV) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	if kv.failAtomicUpdate {
		return merr.WrapErrIoFailedReason("injected backfill catalog failure")
	}
	return kv.metaMemoryKV.MultiSaveAndRemove(ctx, saves, removals, preds...)
}

// A failed catalog transaction may leave an orphan manifest revision, but it
// must expose neither the pointer nor the row deletion. The same row remains a
// candidate and drives a clean retry from the still-published base.
func TestManifestIndexBackfillCatalogFailureKeepsRowAndPointer(t *testing.T) {
	withSegmentIndexManifestWrites(t, false)
	withManifestIndexBackfillEnabled(t, true)
	store := newFakeManifestStore(t)
	ctx := context.TODO()

	kv := &failBackfillCatalogKV{metaMemoryKV: NewMetaMemoryKV()}
	catalog := metastorekv.NewCatalog(kv, "", "")
	m := bootMetaForRestart(t, catalog, restartCollID)
	seedLegacyBackfillRecord(t, m, restartSegID, restartBuildID)
	base := m.GetSegment(ctx, restartSegID).GetManifestPath()

	withSegmentIndexManifestWrites(t, true)
	kv.failAtomicUpdate = true
	inspector := newManifestIndexBackfillInspector(ctx, m)
	inspector.runOnce(ctx)

	assert.Equal(t, base, m.GetSegment(ctx, restartSegID).GetManifestPath())
	persisted, err := catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, err)
	require.Len(t, persisted, 1)
	assert.False(t, m.indexMeta.isSegmentIndexCatalogAbsent(restartBuildID))
	_, pending := inspector.scan(ctx)
	assert.Equal(t, 1, pending)

	// The immutable orphan does not block retry; only the catalog pointer is
	// visible, and it still names the original base.
	kv.failAtomicUpdate = false
	inspector.runOnce(ctx)
	published := m.GetSegment(ctx, restartSegID).GetManifestPath()
	assert.NotEqual(t, base, published)
	require.Len(t, store.backfillEntriesAt(published), 1)
	persisted, err = catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, err)
	assert.Empty(t, persisted)
}

func TestBackfillMutationRequiresMatchingManifestEntry(t *testing.T) {
	withSegmentIndexManifestWrites(t, false)
	newFakeManifestStore(t)
	ctx := context.TODO()
	catalog := metastorekv.NewCatalog(NewMetaMemoryKV(), "", "")
	m := bootMetaForRestart(t, catalog, restartCollID)
	seedLegacyBackfillRecord(t, m, restartSegID, restartBuildID)
	base := m.GetSegment(ctx, restartSegID).GetManifestPath()

	err := m.CommitSegmentManifest(ctx, SegmentManifestCommit{
		SegmentID:     restartSegID,
		StorageConfig: &indexpb.StorageConfig{},
		Mutation: ManifestMutation{
			Type:    ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{},
		},
		CatalogMutation: SegmentCatalogMutation{
			SegmentIndexes: []SegmentIndexMutation{{Type: SegmentIndexBackfill, BuildID: restartBuildID}},
		},
	})
	require.Error(t, err)
	assert.Equal(t, base, m.GetSegment(ctx, restartSegID).GetManifestPath())
	persisted, listErr := catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, listErr)
	assert.Len(t, persisted, 1)
}

func TestBackfillMutationRejectsMismatchedSegmentIdentity(t *testing.T) {
	withSegmentIndexManifestWrites(t, false)
	newFakeManifestStore(t)
	ctx := context.TODO()
	catalog := metastorekv.NewCatalog(NewMetaMemoryKV(), "", "")
	m := bootMetaForRestart(t, catalog, restartCollID)
	seedLegacyBackfillRecord(t, m, restartSegID, restartBuildID)
	base := m.GetSegment(ctx, restartSegID).GetManifestPath()

	corrupt, ok := m.indexMeta.GetIndexJob(restartBuildID)
	require.True(t, ok)
	corrupt.PartitionID++
	m.indexMeta.updateSegmentIndex(corrupt)

	err := newManifestIndexBackfillInspector(ctx, m).backfillIndex(ctx, restartSegID, restartBuildID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "targets collection/partition/segment")
	assert.Equal(t, base, m.GetSegment(ctx, restartSegID).GetManifestPath())
	persisted, listErr := catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, listErr)
	assert.Len(t, persisted, 1)
	assert.False(t, m.indexMeta.isSegmentIndexCatalogAbsent(restartBuildID))
}

func TestBackfillMutationRejectsUnsupportedShapes(t *testing.T) {
	for _, shape := range []string{"noop", "multiple publications", "worker result"} {
		t.Run(shape, func(t *testing.T) {
			withSegmentIndexManifestWrites(t, false)
			store := newFakeManifestStore(t)
			ctx := context.TODO()
			catalog := metastorekv.NewCatalog(NewMetaMemoryKV(), "", "")
			m := bootMetaForRestart(t, catalog, restartCollID)
			seedLegacyBackfillRecord(t, m, restartSegID, restartBuildID)
			segment := m.GetSegment(ctx, restartSegID)
			record, _ := m.indexMeta.GetIndexJob(restartBuildID)
			entry, err := buildManifestIndexInfo(m, segment, record)
			require.NoError(t, err)
			commit := SegmentManifestCommit{
				SegmentID: restartSegID,
				Mutation: ManifestMutation{
					Type:    ManifestMutationCommitUpdates,
					Updates: &packed.ManifestUpdates{Indexes: []packed.ManifestIndexInfo{entry}},
				},
				CatalogMutation: SegmentCatalogMutation{SegmentIndexes: []SegmentIndexMutation{{
					Type: SegmentIndexBackfill, BuildID: restartBuildID,
				}}},
			}
			switch shape {
			case "noop":
				commit.Mutation = ManifestMutation{Type: ManifestMutationNoop, ManifestPath: segment.GetManifestPath()}
			case "multiple publications":
				second := entry
				second.BuildID++
				commit.Mutation.Updates.Indexes = append(commit.Mutation.Updates.Indexes, second)
				commit.CatalogMutation.SegmentIndexes = append(commit.CatalogMutation.SegmentIndexes,
					SegmentIndexMutation{Type: SegmentIndexBackfill, BuildID: second.BuildID})
			case "worker result":
				commit.CatalogMutation.SegmentIndexes[0].FinishedTask = &workerpb.IndexTaskInfo{BuildID: restartBuildID}
			}
			require.Error(t, m.CommitSegmentManifest(ctx, commit))
			assert.Zero(t, store.commitCount)
			rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
			require.NoError(t, err)
			assert.Len(t, rows, 1)
			assert.Equal(t, segment.GetManifestPath(), m.GetSegment(ctx, restartSegID).GetManifestPath())
		})
	}
}
