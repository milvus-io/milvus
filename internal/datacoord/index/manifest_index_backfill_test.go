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
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	metastorekv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestManifestIndexBackfillBatchesWholeSegment(t *testing.T) {
	for _, txnLimit := range []int{128, 3} {
		t.Run(fmt.Sprintf("txn_%d", txnLimit), func(t *testing.T) {
			meta.WithSegmentIndexManifestWrites(t, false)
			store := meta.NewFakeManifestStore(t)
			ctx := context.Background()
			kv := &FailBackfillCatalogKV{MetaMemoryKV: meta.NewMetaMemoryKV()}
			catalog := metastorekv.NewCatalog(kv, "", "")
			m := BootMigrationMeta(t, catalog, meta.RestartCollID)
			records := SeedLegacyBackfillIndexes(t, m, 5)
			SeedLegacyBackfillRecord(t, m, meta.RestartSegID+1, meta.RestartBuildID-1)
			for item, value := range map[*paramtable.ParamItem]string{
				&Params.DataCoordCfg.ManifestIndexBackfillBatchSize: "1",
				&Params.MetaStoreCfg.MaxEtcdTxnNum:                  fmt.Sprint(txnLimit),
			} {
				previous := item.GetValue()
				Params.Save(item.Key, value)
				t.Cleanup(func() { Params.Save(item.Key, previous) })
			}
			meta.WithSegmentIndexManifestWrites(t, true)
			inspector := NewMigrationBackfillInspector(ctx, m)
			work, pending := inspector.scan(ctx)
			require.Len(t, work, 1)
			require.Len(t, work[0].records, 5, "the record budget must not split a segment")
			assert.Equal(t, 6, pending)
			next, pending := inspector.scan(ctx)
			require.Len(t, next, 1)
			assert.Equal(t, meta.RestartSegID+1, next[0].segment.GetID(), "a failing group must not starve other segments")
			assert.Equal(t, 6, pending)
			kv.AtomicUpdateSizes = nil
			require.Equal(t, 5, inspector.execute(ctx, work))
			expectedCommits := (5 + txnLimit - 2) / (txnLimit - 1)
			assert.Equal(t, expectedCommits, store.CommitCount)
			require.Len(t, kv.AtomicUpdateSizes, expectedCommits)
			for _, size := range kv.AtomicUpdateSizes {
				assert.LessOrEqual(t, size, txnLimit)
			}
			rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			assert.Equal(t, meta.RestartBuildID-1, rows[0].BuildID)
			published := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()
			require.Len(t, store.BackfillEntriesAt(published), 5)
			restarted := BootMigrationMeta(t, catalog, meta.RestartCollID)
			for _, record := range records {
				assert.True(t, IndexManifestPublished(t, restarted.IndexCatalog.(*IndexMeta), record.BuildID))
			}
		})
	}
}

func TestManifestIndexBackfillMovesCatalogRowAtomically(t *testing.T) {
	meta.WithSegmentIndexManifestWrites(t, false)
	WithManifestIndexBackfillEnabled(t, true)
	store := meta.NewFakeManifestStore(t)
	ctx := context.TODO()

	catalog := metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	m := BootMigrationMeta(t, catalog, meta.RestartCollID)
	SeedLegacyBackfillRecord(t, m, meta.RestartSegID, meta.RestartBuildID)

	baseManifest := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()
	require.Empty(t, store.BackfillEntriesAt(baseManifest))
	persisted, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
	require.NoError(t, err)
	require.Len(t, persisted, 1)
	assert.False(t, IndexManifestPublished(t, m.IndexCatalog.(*IndexMeta), meta.RestartBuildID))

	// Model the real upgrade boundary: provenance must be reconstructed from
	// the catalog source, not inherited from the process that finished the
	// legacy task.
	m = BootMigrationMeta(t, catalog, meta.RestartCollID)
	_, ok := m.IndexCatalog.(*IndexMeta).GetIndexJob(meta.RestartBuildID)
	require.True(t, ok)
	beforePublication, ok := m.IndexCatalog.(*IndexMeta).SegmentBuildInfo.Get(meta.RestartBuildID)
	require.True(t, ok)
	assert.False(t, IndexManifestPublished(t, m.IndexCatalog.(*IndexMeta), meta.RestartBuildID))

	// Backfill follows the new exclusive placement and therefore runs only in
	// manifest-publication mode.
	meta.WithSegmentIndexManifestWrites(t, true)
	inspector := NewMigrationBackfillInspector(ctx, m)
	inspector.runOnce(ctx)

	published := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()
	require.NotEqual(t, baseManifest, published)
	entries := store.BackfillEntriesAt(published)
	require.Len(t, entries, 1)
	assert.EqualValues(t, meta.RestartIndexID, entries[0].IndexID)
	assert.EqualValues(t, meta.RestartBuildID, entries[0].BuildID)
	assert.Equal(t, []string{"0", "1"}, entries[0].IndexFileKeys)
	assert.Empty(t, entries[0].Properties)
	assert.True(t, m.GetSegment(ctx, meta.RestartSegID).GetManifestHasIndex())

	persisted, err = catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
	require.NoError(t, err)
	assert.Empty(t, persisted, "the manifest pointer and row deletion must commit together")
	_, ok = m.IndexCatalog.(*IndexMeta).GetIndexJob(meta.RestartBuildID)
	assert.True(t, ok, "backfill changes durable placement, not the live DataCoord view")
	assert.True(t, IndexManifestPublished(t, m.IndexCatalog.(*IndexMeta), meta.RestartBuildID))
	assert.False(t, beforePublication.ManifestPublished, "publication must not mutate a reader's snapshot")

	// Convergence is process-local immediately; a second tick creates no new
	// revision and reports no remaining catalog row.
	inspector.runOnce(ctx)
	assert.Equal(t, published, m.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
	assert.Equal(t, 0, inspector.lastPending)

	// And it is reconstructed exactly after failover from durable source
	// placement, without a persisted per-build marker.
	restarted := BootMigrationMeta(t, catalog, meta.RestartCollID)
	recovered, ok := restarted.IndexCatalog.(*IndexMeta).GetIndexJob(meta.RestartBuildID)
	require.True(t, ok)
	assert.Equal(t, commonpb.IndexState_Finished, recovered.IndexState)
	assert.Equal(t, []string{"0", "1"}, recovered.IndexFileKeys)
	assert.True(t, IndexManifestPublished(t, restarted.IndexCatalog.(*IndexMeta), meta.RestartBuildID))
	_, pending := NewMigrationBackfillInspector(ctx, restarted).scan(ctx)
	assert.Zero(t, pending)
}

func TestManifestIndexBackfillRequiresBothSwitches(t *testing.T) {
	t.Run("migration disabled", func(t *testing.T) {
		meta.WithSegmentIndexManifestWrites(t, false)
		WithManifestIndexBackfillEnabled(t, false)
		store := meta.NewFakeManifestStore(t)
		ctx := context.TODO()
		m := BootMigrationMeta(t, metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", ""), meta.RestartCollID)
		SeedLegacyBackfillRecord(t, m, meta.RestartSegID, meta.RestartBuildID)
		base := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()

		meta.WithSegmentIndexManifestWrites(t, true)
		NewMigrationBackfillInspector(ctx, m).runOnce(ctx)
		assert.Equal(t, base, m.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
		assert.Empty(t, store.BackfillEntriesAt(base))
	})

	t.Run("manifest publication disabled", func(t *testing.T) {
		meta.WithSegmentIndexManifestWrites(t, false)
		WithManifestIndexBackfillEnabled(t, true)
		store := meta.NewFakeManifestStore(t)
		ctx := context.TODO()
		m := BootMigrationMeta(t, metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", ""), meta.RestartCollID)
		SeedLegacyBackfillRecord(t, m, meta.RestartSegID, meta.RestartBuildID)
		base := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()

		NewMigrationBackfillInspector(ctx, m).runOnce(ctx)
		assert.Equal(t, base, m.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
		assert.Empty(t, store.BackfillEntriesAt(base))
	})
}

func TestManifestIndexBackfillBatchRotatesPastFailures(t *testing.T) {
	meta.WithSegmentIndexManifestWrites(t, false)
	ctx := context.TODO()
	m := BootMigrationMeta(t, metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", ""), meta.RestartCollID)
	for offset := int64(0); offset < 3; offset++ {
		SeedLegacyBackfillRecord(t, m, meta.RestartSegID+offset, meta.RestartBuildID+offset)
	}

	key := Params.DataCoordCfg.ManifestIndexBackfillBatchSize.Key
	previous := Params.DataCoordCfg.ManifestIndexBackfillBatchSize.GetValue()
	Params.Save(key, "1")
	t.Cleanup(func() { Params.Save(key, previous) })

	inspector := NewMigrationBackfillInspector(ctx, m)
	seen := make([]int64, 0, 3)
	for idx := 0; idx < 3; idx++ {
		work, pending := inspector.scan(ctx)
		require.Len(t, work, 1)
		require.Len(t, work[0].records, 1)
		assert.Equal(t, 3, pending, "batch limiting must not cap the completion signal")
		seen = append(seen, work[0].records[0].BuildID)
	}
	assert.Equal(t, []int64{meta.RestartBuildID, meta.RestartBuildID + 1, meta.RestartBuildID + 2}, seen)
}

func TestManifestIndexBackfillScansOnlyHealthyNonL0StorageV3(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*meta.SegmentInfo)
		pending int
	}{
		{name: "healthy StorageV3", mutate: func(*meta.SegmentInfo) {}, pending: 1},
		{name: "StorageV2", mutate: func(segment *meta.SegmentInfo) {
			segment.StorageVersion = storage.StorageV2
		}},
		{name: "L0", mutate: func(segment *meta.SegmentInfo) {
			segment.Level = datapb.SegmentLevel_L0
		}},
		{name: "dropped", mutate: func(segment *meta.SegmentInfo) {
			segment.State = commonpb.SegmentState_Dropped
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.TODO()
			m := BootMigrationMeta(t, metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", ""), meta.RestartCollID)
			SeedLegacyBackfillRecord(t, m, meta.RestartSegID, meta.RestartBuildID)
			segment := m.GetSegment(ctx, meta.RestartSegID).Clone()
			test.mutate(segment)
			m.Segments.SetSegment(segment.GetID(), segment)

			work, pending := NewMigrationBackfillInspector(ctx, m).scan(ctx)
			assert.Equal(t, test.pending, pending)
			assert.Equal(t, test.pending, countManifestIndexBackfillRecords(work))
		})
	}
}

func TestManifestIndexBackfillSkipsTaskOnlyAndManifestOnlyRecords(t *testing.T) {
	base := &model.SegmentIndex{
		BuildID:       1,
		IndexState:    commonpb.IndexState_Finished,
		IndexFileKeys: []string{"0"},
	}
	assert.True(t, segmentIndexNeedsManifestBackfill(base))

	for name, mutate := range map[string]func(*model.SegmentIndex){
		"deleted":       func(index *model.SegmentIndex) { index.IsDeleted = true },
		"in progress":   func(index *model.SegmentIndex) { index.IndexState = commonpb.IndexState_InProgress },
		"failed":        func(index *model.SegmentIndex) { index.IndexState = commonpb.IndexState_Failed },
		"fake finished": func(index *model.SegmentIndex) { index.IndexFileKeys = nil },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := model.CloneSegmentIndex(base)
			mutate(candidate)
			assert.False(t, segmentIndexNeedsManifestBackfill(candidate))
		})
	}

	base.ManifestPublished = true
	assert.False(t, segmentIndexNeedsManifestBackfill(base),
		"a manifest-resident record has no catalog row to migrate")
}

func TestManifestIndexBackfillIgnoresCopiedManifestRecord(t *testing.T) {
	meta.NewFakeManifestStore(t)
	ctx := context.TODO()
	catalog := metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	m := BootMigrationMeta(t, catalog, meta.RestartCollID)
	SeedBackfillIndexDefinition(t, m)
	require.NoError(t, m.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:               meta.RestartSegID,
		CollectionID:     meta.RestartCollID,
		PartitionID:      RestartPartID,
		State:            commonpb.SegmentState_Flushed,
		StorageVersion:   storage.StorageV3,
		ManifestPath:     packed.MarshalManifestPath("/tmp/test-backfill/copied", 1),
		ManifestHasIndex: true,
	})))
	require.NoError(t, m.IndexCatalog.(*IndexMeta).AddSegmentIndexFromManifest(ctx, &model.SegmentIndex{
		CollectionID:          meta.RestartCollID,
		PartitionID:           RestartPartID,
		SegmentID:             meta.RestartSegID,
		NumRows:               1000,
		IndexID:               meta.RestartIndexID,
		BuildID:               meta.RestartBuildID,
		IndexVersion:          1,
		IndexState:            commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"0"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}))

	work, pending := NewMigrationBackfillInspector(ctx, m).scan(ctx)
	assert.Empty(t, work)
	assert.Zero(t, pending)
	persisted, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
	require.NoError(t, err)
	assert.Empty(t, persisted)
}

func TestManifestIndexBackfillCatalogFailureKeepsRowAndPointer(t *testing.T) {
	meta.WithSegmentIndexManifestWrites(t, false)
	WithManifestIndexBackfillEnabled(t, true)
	store := meta.NewFakeManifestStore(t)
	ctx := context.TODO()

	kv := &FailBackfillCatalogKV{MetaMemoryKV: meta.NewMetaMemoryKV()}
	catalog := metastorekv.NewCatalog(kv, "", "")
	m := BootMigrationMeta(t, catalog, meta.RestartCollID)
	records := SeedLegacyBackfillIndexes(t, m, 3)
	base := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()

	meta.WithSegmentIndexManifestWrites(t, true)
	kv.FailAtomicUpdate = true
	inspector := NewMigrationBackfillInspector(ctx, m)
	assert.True(t, inspector.runOnce(ctx), "catalog failure must keep periodic retries active")

	assert.Equal(t, base, m.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
	persisted, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
	require.NoError(t, err)
	require.Len(t, persisted, 3)
	for _, record := range records {
		assert.False(t, IndexManifestPublished(t, m.IndexCatalog.(*IndexMeta), record.BuildID))
	}
	_, pending := inspector.scan(ctx)
	assert.Equal(t, 3, pending)

	// The immutable orphan does not block retry; only the catalog pointer is
	// visible, and it still names the original base.
	kv.FailAtomicUpdate = false
	inspector.runOnce(ctx)
	published := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()
	assert.NotEqual(t, base, published)
	require.Len(t, store.BackfillEntriesAt(published), 3)
	persisted, err = catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
	require.NoError(t, err)
	assert.Empty(t, persisted)
}

func TestBackfillMutationRequiresMatchingManifestEntry(t *testing.T) {
	meta.WithSegmentIndexManifestWrites(t, false)
	meta.NewFakeManifestStore(t)
	ctx := context.TODO()
	catalog := metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	m := BootMigrationMeta(t, catalog, meta.RestartCollID)
	SeedLegacyBackfillRecord(t, m, meta.RestartSegID, meta.RestartBuildID)
	base := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()

	err := m.CommitSegmentManifest(ctx, meta.SegmentManifestCommit{
		SegmentID:     meta.RestartSegID,
		StorageConfig: &indexpb.StorageConfig{},
		Mutation: meta.ManifestMutation{
			Type:    meta.ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{},
		},
		CatalogMutation: meta.SegmentCatalogMutation{
			SegmentIndexes: []meta.SegmentIndexMutation{{Type: meta.SegmentIndexBackfill, BuildID: meta.RestartBuildID}},
		},
	})
	require.Error(t, err)
	assert.Equal(t, base, m.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
	persisted, listErr := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
	require.NoError(t, listErr)
	assert.Len(t, persisted, 1)
}

func TestBackfillMutationRejectsMismatchedSegmentIdentity(t *testing.T) {
	meta.WithSegmentIndexManifestWrites(t, false)
	meta.NewFakeManifestStore(t)
	ctx := context.TODO()
	catalog := metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	m := BootMigrationMeta(t, catalog, meta.RestartCollID)
	SeedLegacyBackfillRecord(t, m, meta.RestartSegID, meta.RestartBuildID)
	base := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()

	corrupt, ok := m.IndexCatalog.(*IndexMeta).GetIndexJob(meta.RestartBuildID)
	require.True(t, ok)
	corrupt.PartitionID++
	m.IndexCatalog.(*IndexMeta).updateSegmentIndex(corrupt)

	err := NewMigrationBackfillInspector(ctx, m).backfillIndexes(ctx, meta.RestartSegID, meta.RestartBuildID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "targets collection/partition/segment")
	assert.Equal(t, base, m.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
	persisted, listErr := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
	require.NoError(t, listErr)
	assert.Len(t, persisted, 1)
	assert.False(t, IndexManifestPublished(t, m.IndexCatalog.(*IndexMeta), meta.RestartBuildID))
}

func TestBackfillMutationRejectsUnsupportedShapes(t *testing.T) {
	for _, shape := range []string{"noop", "duplicate index IDs", "worker result"} {
		t.Run(shape, func(t *testing.T) {
			meta.WithSegmentIndexManifestWrites(t, false)
			store := meta.NewFakeManifestStore(t)
			ctx := context.TODO()
			catalog := metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", "")
			m := BootMigrationMeta(t, catalog, meta.RestartCollID)
			SeedLegacyBackfillRecord(t, m, meta.RestartSegID, meta.RestartBuildID)
			segment := m.GetSegment(ctx, meta.RestartSegID)
			record, _ := m.IndexCatalog.(*IndexMeta).GetIndexJob(meta.RestartBuildID)
			record.IndexStorePathVersion = indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED
			entry, err := meta.BuildManifestIndexInfo(m, segment, record)
			require.NoError(t, err)
			commit := meta.SegmentManifestCommit{
				SegmentID: meta.RestartSegID,
				Mutation: meta.ManifestMutation{
					Type:    meta.ManifestMutationCommitUpdates,
					Updates: &packed.ManifestUpdates{Indexes: []packed.ManifestIndexInfo{entry}},
				},
				CatalogMutation: meta.SegmentCatalogMutation{SegmentIndexes: []meta.SegmentIndexMutation{{
					Type: meta.SegmentIndexBackfill, BuildID: meta.RestartBuildID,
				}}},
			}
			switch shape {
			case "noop":
				commit.Mutation = meta.ManifestMutation{Type: meta.ManifestMutationNoop, ManifestPath: segment.GetManifestPath()}
			case "duplicate index IDs":
				second := entry
				second.BuildID++
				commit.Mutation.Updates.Indexes = append(commit.Mutation.Updates.Indexes, second)
				commit.CatalogMutation.SegmentIndexes = append(commit.CatalogMutation.SegmentIndexes,
					meta.SegmentIndexMutation{Type: meta.SegmentIndexBackfill, BuildID: second.BuildID})
			case "worker result":
				commit.CatalogMutation.SegmentIndexes[0].FinishedTask = &workerpb.IndexTaskInfo{BuildID: meta.RestartBuildID}
			}
			require.Error(t, m.CommitSegmentManifest(ctx, commit))
			assert.Zero(t, store.CommitCount)
			rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
			require.NoError(t, err)
			assert.Len(t, rows, 1)
			assert.Equal(t, segment.GetManifestPath(), m.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
		})
	}
}

func TestManifestIndexBackfillScanDoesNotDuplicateArtifactMetadata(t *testing.T) {
	meta.WithSegmentIndexManifestWrites(t, false)
	meta.NewFakeManifestStore(t)
	ctx := context.Background()
	m := BootMigrationMeta(t, metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", ""), meta.RestartCollID)
	SetRollbackTestParam(t, &Params.DataCoordCfg.ManifestIndexBackfillBatchSize, "1")
	keys := make([]string, 65536)
	for idx := range keys {
		keys[idx] = "index-file"
	}
	for offset := range int64(4) {
		SeedLegacyBackfillRecord(t, m, meta.RestartSegID+offset, meta.RestartBuildID+offset)
		record, ok := m.IndexCatalog.(*IndexMeta).GetIndexJob(meta.RestartBuildID + offset)
		require.True(t, ok)
		record.IndexFileKeys = keys
		// Install a large immutable file list without serializing a synthetic
		// large catalog record. Scan still traverses the real build table.
		m.IndexCatalog.(*IndexMeta).updateSegmentIndex(record)
	}
	inspector := NewMigrationBackfillInspector(ctx, m)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	work, pending := inspector.scan(ctx)
	runtime.ReadMemStats(&after)
	require.Equal(t, 4, pending)
	require.Len(t, work, 1)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20), "batch selection must not copy the backlog's file-key slices")
}
