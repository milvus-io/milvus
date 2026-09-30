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
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	metastorekv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestManifestIndexRollbackRealManifestBatchesAndRestart(t *testing.T) {
	for _, layout := range []indexpb.IndexStorePathVersion{
		indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
		indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	} {
		t.Run(layout.String(), func(t *testing.T) {
			withManifestIndexRollback(t, false)
			withSegmentIndexManifestWrites(t, false)
			ctx := context.TODO()
			root := t.TempDir()
			setRollbackTestParam(t, &Params.CommonCfg.StorageType, "local")
			setRollbackTestParam(t, &Params.LocalStorageCfg.Path, root)
			kv := &failBackfillCatalogKV{metaMemoryKV: NewMetaMemoryKV()}
			catalog := metastorekv.NewCatalog(kv, "", "")
			boot := func() *meta {
				b := broker.NewMockBroker(t)
				b.EXPECT().ShowCollectionIDs(mock.Anything).Return(&rootcoordpb.ShowCollectionIDsResponse{
					Status: merr.Success(), DbCollections: []*rootcoordpb.DBCollections{{DbName: "default", CollectionIDs: []int64{restartCollID}}},
				}, nil)
				m, err := newMeta(ctx, catalog, storage.NewLocalChunkManager(objectstorage.RootPath(root)), b)
				require.NoError(t, err)
				return m
			}
			m := boot()
			seedLegacyBackfillRecord(t, m, restartSegID, restartBuildID)
			m.AddCollection(&collectionInfo{ID: restartCollID, Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{{FieldID: restartFieldID, Name: "vec", DataType: schemapb.DataType_FloatVector}},
			}})
			cfg := createStorageConfig()
			base := path.Join(root, "insert_log/300/30/8001")
			stat := path.Join(base, "_stats/bloom_filter.100/1")
			require.NoError(t, packed.WriteFile(cfg, stat, []byte("stats")))
			initial, err := packed.CommitManifestUpdates(base, packed.ManifestEarliest, cfg, &packed.ManifestUpdates{
				Stats: []packed.StatEntry{{Key: "bloom_filter.100", Files: []string{stat}}},
			})
			require.NoError(t, err)
			require.NoError(t, m.UpdateSegmentsInfo(ctx, UpdateManifest(restartSegID, initial)))
			first, _ := m.indexMeta.GetIndexJob(restartBuildID)
			first.IndexStorePathVersion = layout
			require.NoError(t, m.indexMeta.alterSegmentIndexes([]*model.SegmentIndex{first}))
			records := []*model.SegmentIndex{first}
			for offset := int64(1); offset < 3; offset++ {
				definition := model.CloneIndex(m.indexMeta.GetIndexesForCollection(restartCollID, "")[0])
				definition.IndexID = restartIndexID + offset
				definition.IndexName = "index_" + definition.IndexName
				require.NoError(t, m.indexMeta.CreateIndex(ctx, definition))
				record := model.CloneSegmentIndex(first)
				record.IndexID += offset
				record.BuildID += offset
				require.NoError(t, m.indexMeta.AddSegmentIndex(ctx, record))
				records = append(records, record)
			}
			gc := newGarbageCollector(m, nil, GcOption{cli: m.chunkManager})
			t.Cleanup(gc.close)
			files := make(map[string]struct{})
			for _, record := range records {
				for file := range gc.getAllIndexFilesOfIndex(record) {
					files[file] = struct{}{}
					require.NoError(t, m.chunkManager.Write(ctx, file, []byte("artifact")))
				}
				require.NoError(t, newManifestIndexBackfillInspector(ctx, m).backfillIndexes(ctx, restartSegID, record.BuildID))
			}
			manifestOnly := m.GetSegment(ctx, restartSegID).GetManifestPath()
			// Snapshot-pinned immutable revisions retain their old index section.
			oldEntries, err := packed.GetManifestIndexInfos(manifestOnly, cfg)
			require.NoError(t, err)
			require.Len(t, oldEntries, 3)
			withManifestIndexRollback(t, true)
			setRollbackTestParam(t, &Params.MetaStoreCfg.MaxEtcdTxnNum, "2")
			n, err := m.rollbackSegmentIndexes(ctx, restartSegID, restartBuildID, restartBuildID+1, restartBuildID+2)
			require.NoError(t, err)
			assert.Equal(t, 1, n)
			partial := m.GetSegment(ctx, restartSegID).GetManifestPath()
			assert.True(t, m.GetSegment(ctx, restartSegID).GetManifestHasIndex())
			kv.failAtomicUpdate = true
			_, err = m.rollbackSegmentIndexes(ctx, restartSegID, restartBuildID, restartBuildID+1, restartBuildID+2)
			require.Error(t, err)
			assert.Equal(t, partial, m.GetSegment(ctx, restartSegID).GetManifestPath())
			rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			kv.failAtomicUpdate = false
			m = boot()
			for range 2 {
				n, err = m.rollbackSegmentIndexes(ctx, restartSegID, restartBuildID, restartBuildID+1, restartBuildID+2)
				require.NoError(t, err)
				assert.Equal(t, 1, n)
			}
			final := m.GetSegment(ctx, restartSegID).GetManifestPath()
			assert.False(t, m.GetSegment(ctx, restartSegID).GetManifestHasIndex())
			entries, err := packed.GetManifestIndexInfos(final, cfg)
			require.NoError(t, err)
			assert.Empty(t, entries)
			stats, err := packed.GetManifestStats(final, cfg)
			require.NoError(t, err)
			assert.Contains(t, stats, "bloom_filter.100")
			stillOld, err := packed.GetManifestIndexInfos(manifestOnly, cfg)
			require.NoError(t, err)
			assert.Equal(t, oldEntries, stillOld)
			for file := range files {
				data, err := m.chunkManager.Read(ctx, file)
				require.NoError(t, err)
				assert.Equal(t, []byte("artifact"), data)
			}
			// Fully migrated records recover from their catalog rows.
			noRead := mockey.Mock(packed.GetManifestIndexInfos).Return(nil, merr.ErrIoTooManyRequests).Build()
			t.Cleanup(func() { noRead.UnPatch() })
			restarted := boot()
			assert.Zero(t, noRead.Times())
			for _, record := range records {
				recovered, found := restarted.indexMeta.GetIndexJob(record.BuildID)
				require.True(t, found)
				assert.Equal(t, record.IndexFileKeys, recovered.IndexFileKeys)
				assert.Equal(t, layout, recovered.IndexStorePathVersion)
				assert.False(t, indexManifestPublished(t, restarted.indexMeta, record.BuildID))
			}
		})
	}
}

func TestManifestIndexRollbackDoesNotWaitForCopyAndRejectsDuplicateResult(t *testing.T) {
	m, catalog, _, _ := rollbackFixture(t)
	ctx := context.TODO()
	copies, err := NewCopySegmentMeta(ctx, catalog, m, nil, nil)
	require.NoError(t, err)
	task := createTestCopyTask(restartCollID, restartSegID).(*copySegmentTask)
	task.task.Load().IndexWriteToManifest = proto.Bool(true)
	require.NoError(t, copies.AddTask(ctx, task))
	mode, present := taskIndexWriteToManifest(task)
	assert.True(t, present)
	assert.True(t, mode, "old dispatch placement must survive rollback activation")
	newTask := createTestCopyTask(restartCollID, restartSegID+1)
	mode, present = taskIndexWriteToManifest(newTask)
	assert.False(t, mode, "new tasks choose etcd")
	assert.False(t, present)
	before := m.GetSegment(ctx, restartSegID).GetManifestPath()
	inspector := newManifestIndexRollbackInspector(ctx, m)
	inspector.runOnce(ctx)
	assert.False(t, inspector.ready)
	assert.NotEqual(t, before, m.GetSegment(ctx, restartSegID).GetManifestPath(), "active copy tasks do not block installed records")
	inspector.runOnce(ctx)
	assert.True(t, inspector.ready, "copy task completion is independent of the current record backlog")
	require.NoError(t, copies.UpdateTask(ctx, task.GetTaskId(), UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskCompleted)))
	inspector.runOnce(ctx)
	inspector.runOnce(ctx)
	assert.True(t, inspector.ready)
	final := m.GetSegment(ctx, restartSegID).GetManifestPath()
	// Supply the old task snapshot: only the current durable task state proves
	// that a repeated worker result must not reinstall its pre-rollback pointer.
	require.NoError(t, SyncCopySegmentTask(task, &datapb.QueryCopySegmentResponse{
		State:          datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		SegmentResults: []*datapb.CopySegmentResult{{SegmentId: restartSegID, ManifestPath: before}},
	}, copies, m))
	assert.Equal(t, final, m.GetSegment(ctx, restartSegID).GetManifestPath())
	assert.False(t, indexManifestPublished(t, m.indexMeta, restartBuildID))
	assert.False(t, copies.GetTask(ctx, task.GetTaskId()).GetCleanupRequired())
}

func TestManifestIndexRollbackRejectsLateCleanedCopyResult(t *testing.T) {
	for _, prefix := range []string{"files/owned-copy-prefix/", ""} {
		t.Run(prefix, func(t *testing.T) {
			m, catalog, _, _ := rollbackFixture(t)
			ctx := context.TODO()
			copies, err := NewCopySegmentMeta(ctx, catalog, m, nil, nil)
			require.NoError(t, err)
			task := createTestCopyTask(restartCollID, restartSegID).(*copySegmentTask)
			task.task.Load().IndexWriteToManifest = proto.Bool(true)
			if prefix != "" {
				task.task.Load().CleanupPrefixes = []string{prefix}
			}
			require.NoError(t, copies.AddTask(ctx, task))
			require.NoError(t, copies.UpdateTask(ctx, task.GetTaskId(),
				UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed), updateCopyTaskCleanup(false)))
			inspector := newManifestIndexRollbackInspector(ctx, m)
			inspector.runOnce(ctx)
			inspector.runOnce(ctx)
			require.True(t, inspector.ready)
			before := m.GetSegment(ctx, restartSegID).GetManifestPath()
			err = SyncCopySegmentTask(task, &datapb.QueryCopySegmentResponse{
				State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
			}, copies, m)
			require.ErrorContains(t, err, "cannot publish a failed copy task")
			assert.Equal(t, before, m.GetSegment(ctx, restartSegID).GetManifestPath())
			assert.False(t, indexManifestPublished(t, m.indexMeta, restartBuildID))
			assert.True(t, copies.GetTask(ctx, task.GetTaskId()).GetCleanupRequired())
			inspector.runOnce(ctx)
			assert.True(t, inspector.ready, "copy cleanup is independent of index rollback")
		})
	}
}

func TestManifestIndexRollbackIgnoresEmptyMarkerAndUnpublishedCopy(t *testing.T) {
	m, catalog, store, _ := rollbackFixture(t)
	ctx := context.TODO()
	_, err := m.rollbackSegmentIndexes(ctx, restartSegID, restartBuildID)
	require.NoError(t, err)
	before := m.GetSegment(ctx, restartSegID).GetManifestPath()
	require.Empty(t, store.backfillEntriesAt(before))
	require.NoError(t, m.UpdateSegmentsInfo(ctx, UpdateManifestHasIndex(restartSegID)))
	copies, err := NewCopySegmentMeta(ctx, catalog, m, nil, nil)
	require.NoError(t, err)
	task := createTestCopyTask(restartCollID, restartSegID+1)
	require.NoError(t, copies.AddTask(ctx, task))
	inspector := newManifestIndexRollbackInspector(ctx, m)
	assert.False(t, inspector.runOnce(ctx), "no records remain, so periodic scans stop")
	assert.True(t, m.GetSegment(ctx, restartSegID).GetManifestHasIndex(), "marker cleanup is not rollback work")
	assert.Equal(t, before, m.GetSegment(ctx, restartSegID).GetManifestPath())
	assert.True(t, inspector.ready, "an unpublished copy target has no record to migrate yet")
	assert.Zero(t, testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackPending))
}

func TestManifestIndexRollbackBoundedShutdown(t *testing.T) {
	m, catalog, _, _ := rollbackFixture(t)
	ctx := context.TODO()
	for offset := int64(1); offset < 4; offset++ {
		seedLegacyBackfillRecord(t, m, restartSegID+offset, restartBuildID+offset)
		require.NoError(t, newManifestIndexBackfillInspector(ctx, m).backfillIndexes(ctx, restartSegID+offset, restartBuildID+offset))
	}
	setRollbackTestParam(t, &Params.DataCoordCfg.ManifestIndexRollbackConcurrency, "2")
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	var once sync.Once
	patch := mockey.Mock(commitManifestMutation).When(func(_ string, _ SegmentManifestCommit) bool {
		entered <- struct{}{}
		<-release
		return false
	}).Build()
	inspector := newManifestIndexRollbackInspector(ctx, m)
	t.Cleanup(func() {
		inspector.cancel()
		once.Do(func() { close(release) })
		inspector.Stop()
		patch.UnPatch()
	})
	inspector.Start()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("rollback did not fill its two worker slots")
		}
	}
	assert.Empty(t, entered)
	inspector.cancel()
	stopped := make(chan struct{})
	go func() { inspector.Stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("Stop returned before in-flight manifest work drained")
	default:
	}
	once.Do(func() { close(release) })
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("rollback failed to drain")
	}
	assert.Empty(t, entered, "canceled queued groups must not start I/O")
	assert.Zero(t, testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackReady))
	rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(rows), 2)
}

func TestManifestIndexRollbackDroppedGCWaitsForPublication(t *testing.T) {
	m, _, _, _ := rollbackFixture(t)
	ctx := context.TODO()
	require.NoError(t, m.SetState(ctx, restartSegID, commonpb.SegmentState_Dropped))
	before := m.GetSegment(ctx, restartSegID)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	patch := mockey.Mock(commitManifestMutation).When(func(_ string, _ SegmentManifestCommit) bool {
		close(entered)
		<-release
		return false
	}).Build()
	gc := newGarbageCollector(m, nil, GcOption{cli: m.chunkManager})
	t.Cleanup(gc.close)
	deleted := make(chan string, 1)
	deletePatch := mockey.Mock((*garbageCollector).removeDroppedSegmentFiles).To(
		func(_ *garbageCollector, _ context.Context, segment *SegmentInfo, _ map[string]struct{}) error {
			deleted <- segment.GetManifestPath()
			return merr.ErrIoTooManyRequests
		}).Build()
	rolled := make(chan error, 1)
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		_, err := m.rollbackSegmentIndexes(ctx, restartSegID, restartBuildID)
		rolled <- err
	}()
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		workers.Wait()
		patch.UnPatch()
		deletePatch.UnPatch()
	})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("rollback did not enter manifest I/O")
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		gc.recycleDroppedSegment(ctx, restartSegID)
	}()
	select {
	case <-deleted:
		t.Fatal("GC deleted the prefix while rollback was creating a revision")
	case <-time.After(50 * time.Millisecond):
	}
	once.Do(func() { close(release) })
	require.NoError(t, <-rolled)
	select {
	case pointer := <-deleted:
		assert.Equal(t, m.GetSegment(ctx, restartSegID).GetManifestPath(), pointer)
		assert.NotEqual(t, before.GetManifestPath(), pointer)
	case <-time.After(5 * time.Second):
		t.Fatal("GC did not resume after rollback")
	}
}

func TestManifestIndexRollbackAfterDroppedIndexGC(t *testing.T) {
	for _, tc := range []struct {
		name        string
		keepLive    bool
		failCatalog bool
	}{
		{name: "only retired index"},
		{name: "mixed retired and live indexes", keepLive: true},
		{name: "catalog failure and retry", keepLive: true, failCatalog: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, catalog, store, kv := rollbackFixture(t)
			ctx := context.TODO()
			retired, _ := m.indexMeta.GetIndexJob(restartBuildID)
			// A second, live index is the only rollback candidate.
			definition := model.CloneIndex(m.indexMeta.GetIndexesForCollection(restartCollID, "")[0])
			definition.IndexID++
			definition.IndexName = "live_index"
			live := model.CloneSegmentIndex(retired)
			live.IndexID = definition.IndexID
			live.BuildID++
			liveCount := 0
			if tc.keepLive {
				require.NoError(t, m.indexMeta.CreateIndex(ctx, definition))
				require.NoError(t, m.indexMeta.AddSegmentIndex(ctx, live))
				require.NoError(t, newManifestIndexBackfillInspector(ctx, m).backfillIndexes(ctx, restartSegID, live.BuildID))
				liveCount = 1
			}

			// A snapshot taken before these indexes were built pins the segment,
			// but does not pin the subsequently deleted index's files.
			m.snapshotMeta.segmentReferencedByGC.Insert(restartSegID)
			require.True(t, m.snapshotMeta.IsSegmentGCBlocked(restartCollID, restartSegID))
			require.False(t, m.snapshotMeta.IsBuildIDGCBlocked(restartCollID, restartBuildID))
			require.NoError(t, m.SetState(ctx, restartSegID, commonpb.SegmentState_Dropped))
			require.NoError(t, m.indexMeta.MarkIndexAsDeleted(ctx, restartCollID, []int64{restartIndexID}))
			gc := newGarbageCollector(m, nil, GcOption{cli: m.chunkManager})
			t.Cleanup(gc.close)
			files := gc.getAllIndexFilesOfIndex(retired)
			for file := range files {
				require.NoError(t, m.chunkManager.Write(ctx, file, []byte("retired artifact")))
				t.Cleanup(func() { _ = m.chunkManager.Remove(ctx, file) })
			}
			gc.recycleUnusedSegIndexes(ctx, nil)
			_, exists := m.indexMeta.GetIndexJob(restartBuildID)
			require.False(t, exists)
			for file := range files {
				exists, err := m.chunkManager.Exist(ctx, file)
				require.NoError(t, err)
				require.False(t, exists)
			}
			before := m.GetSegment(ctx, restartSegID).GetManifestPath()
			require.Len(t, store.backfillEntriesAt(before), liveCount+1, "Dropped index GC leaves manifest entries behind")
			restoredBefore := testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackRecords)
			inspector := newManifestIndexRollbackInspector(ctx, m)
			if tc.failCatalog {
				kv.failAtomicUpdate = true
				inspector.runOnce(ctx)
				require.False(t, inspector.ready)
				require.Equal(t, before, m.GetSegment(ctx, restartSegID).GetManifestPath())
				rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
				require.NoError(t, err)
				require.Empty(t, rows)
				require.Equal(t, restoredBefore, testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackRecords))
				kv.failAtomicUpdate = false
			}
			inspector.runOnce(ctx)
			segment := m.GetSegment(ctx, restartSegID)
			require.True(t, segment.GetManifestHasIndex())
			remaining := store.backfillEntriesAt(segment.GetManifestPath())
			require.Len(t, remaining, 1)
			assert.Equal(t, retired.BuildID, remaining[0].BuildID, "GC-owned entries are untouched")
			if !tc.keepLive {
				assert.Equal(t, before, segment.GetManifestPath())
			}
			require.Equal(t, commonpb.SegmentState_Dropped, segment.GetState())
			rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
			require.NoError(t, err)
			require.Len(t, rows, liveCount)
			if tc.keepLive {
				assert.Equal(t, live.BuildID, rows[0].BuildID)
			}
			assert.Equal(t, restoredBefore+float64(liveCount), testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackRecords), "only the live record was restored")
			inspector.runOnce(ctx)
			assert.True(t, inspector.ready)
			assert.Equal(t, float64(1), testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackReady))
			assert.True(t, m.snapshotMeta.IsSegmentGCBlocked(restartCollID, restartSegID))

			_, exists = m.indexMeta.GetIndexJob(restartBuildID)
			assert.False(t, exists, "rollback must not recreate retired records")
		})
	}
}

func TestManifestIndexRollbackRecordsFirstAndIdleWake(t *testing.T) {
	m, catalog, _, _ := rollbackFixture(t)
	ctx := context.Background()
	inspector := newManifestIndexRollbackInspector(ctx, m)
	var segmentScans atomic.Int32
	segmentPatch := mockey.Mock((*meta).SelectSegments).When(func(*meta, context.Context, ...SegmentFilter) bool {
		segmentScans.Add(1)
		return false
	}).Build()
	t.Cleanup(func() { segmentPatch.UnPatch() })
	work, records := inspector.scan(ctx)
	require.Equal(t, 1, records)
	require.Len(t, work, 1)
	assert.Equal(t, []int64{restartBuildID}, work[0].buildIDs)
	assert.Zero(t, segmentScans.Load(), "ordinary rollback must discover work from index records")

	interval := mockey.Mock(manifestIndexRollbackInterval).Return(10 * time.Millisecond).Build()
	t.Cleanup(func() { interval.UnPatch() })
	var scans atomic.Int32
	scanPatch := mockey.Mock((*manifestIndexRollbackInspector).scan).When(func(*manifestIndexRollbackInspector, context.Context) bool {
		scans.Add(1)
		return false
	}).Build()
	t.Cleanup(func() { scanPatch.UnPatch() })
	inspector.Start()
	t.Cleanup(inspector.Stop)
	require.Eventually(t, func() bool {
		return scans.Load() >= 2 && testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackReady) == 1
	}, 5*time.Second, time.Millisecond)
	assert.Zero(t, segmentScans.Load(), "rollback never enumerates segments for residual cleanup")
	require.Never(t, func() bool { return scans.Load() != 2 }, 100*time.Millisecond, time.Millisecond)

	// Emulate a late manifest publication after idle. The publication path
	// must wake rollback; it cannot depend on a still-running periodic scan.
	seedLegacyBackfillRecord(t, m, restartSegID+1, restartBuildID+1)
	require.NoError(t, newManifestIndexBackfillInspector(ctx, m).backfillIndexes(ctx, restartSegID+1, restartBuildID+1))
	require.Eventually(t, func() bool {
		record, ok := m.indexMeta.GetIndexJob(restartBuildID + 1)
		return ok && !record.ManifestPublished && scans.Load() >= 4 &&
			testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackReady) == 1
	}, 5*time.Second, time.Millisecond)
	require.Never(t, func() bool { return scans.Load() != 4 }, 100*time.Millisecond, time.Millisecond)
	inspector.Stop()
	rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, err)
	assert.Len(t, rows, 2)
	assert.False(t, m.GetSegment(ctx, restartSegID+1).GetManifestHasIndex())
}

func TestManifestIndexRollbackMissingSegmentStaysPending(t *testing.T) {
	m, _, _, _ := rollbackFixture(t)
	m.segMu.Lock()
	m.segments.DropSegment(restartSegID)
	m.segMu.Unlock()
	inspector := newManifestIndexRollbackInspector(context.Background(), m)
	assert.True(t, inspector.runOnce(context.Background()), "orphaned manifest-only records must block readiness and keep retries active")
	assert.False(t, inspector.ready)
	assert.EqualValues(t, 1, testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackPendingRecords))
}

func TestManifestIndexRollbackLeavesUninstalledCopyIndex(t *testing.T) {
	m, catalog, store, _ := rollbackFixture(t)
	ctx := context.Background()
	first, _ := m.indexMeta.GetIndexJob(restartBuildID)
	late := model.CloneSegmentIndex(first)
	late.BuildID++
	late.IndexID++
	definition := model.CloneIndex(m.indexMeta.GetIndexesForCollection(restartCollID, "")[0])
	definition.IndexID = late.IndexID
	definition.IndexName = "late_copy_index"
	require.NoError(t, m.indexMeta.CreateIndex(ctx, definition))
	entry, err := buildManifestIndexInfo(m, m.GetSegment(ctx, restartSegID), late)
	require.NoError(t, err)
	before := m.GetSegment(ctx, restartSegID).GetManifestPath()
	// A copy result has published both entries, but has only installed the
	// first record. Rollback may process that record while copy continues.
	store.revisions[before] = append(store.revisions[before], entry)
	inspector := newManifestIndexRollbackInspector(ctx, m)
	inspector.runOnce(ctx)
	assert.False(t, inspector.runOnce(ctx))
	assert.True(t, inspector.ready)
	remaining := store.backfillEntriesAt(m.GetSegment(ctx, restartSegID).GetManifestPath())
	require.Len(t, remaining, 1)
	assert.Equal(t, late.BuildID, remaining[0].BuildID)
	assert.True(t, m.GetSegment(ctx, restartSegID).GetManifestHasIndex())
	rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, first.BuildID, rows[0].BuildID)

	// The remaining record arrives after the inspector has gone idle.
	interval := mockey.Mock(manifestIndexRollbackInterval).Return(10 * time.Millisecond).Build()
	t.Cleanup(func() { interval.UnPatch() })
	inspector.Start()
	t.Cleanup(inspector.Stop)
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackReady) == 1
	}, 5*time.Second, time.Millisecond)
	require.NoError(t, m.indexMeta.AddSegmentIndexFromManifest(ctx, late))
	require.Eventually(t, func() bool {
		record, ok := m.indexMeta.GetIndexJob(late.BuildID)
		return ok && !record.ManifestPublished && testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackReady) == 1
	}, 5*time.Second, time.Millisecond)
	inspector.Stop()
	assert.Empty(t, store.backfillEntriesAt(m.GetSegment(ctx, restartSegID).GetManifestPath()))
	rows, err = catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, err)
	assert.Len(t, rows, 2)
}

func TestManifestIndexRollbackRevalidatesSelectedRecord(t *testing.T) {
	for _, retire := range []bool{false, true} {
		t.Run(map[bool]string{false: "rewritten to catalog", true: "retired"}[retire], func(t *testing.T) {
			m, catalog, store, _ := rollbackFixture(t)
			ctx := context.Background()
			before := m.GetSegment(ctx, restartSegID).GetManifestPath()
			commits := store.commitCount
			record, _ := m.indexMeta.GetIndexJob(restartBuildID)
			// Change the selected record after selection but before staging
			// takes its BuildID lock. No manifest revision should be published.
			patch := mockey.Mock((*meta).readManifestIndexes).When(func(_ *meta, _ context.Context, _ string, _ *indexpb.StorageConfig) bool {
				if retire {
					require.NoError(t, m.indexMeta.RemoveSegmentIndex(ctx, restartBuildID))
				} else {
					require.NoError(t, m.indexMeta.AddSegmentIndex(ctx, record))
				}
				return false
			}).Build()
			t.Cleanup(func() { patch.UnPatch() })
			n, err := m.rollbackSegmentIndexes(ctx, restartSegID, restartBuildID)
			require.ErrorIs(t, err, errSegmentIndexRollbackSkipped)
			assert.Zero(t, n)
			assert.Equal(t, commits, store.commitCount)
			assert.Equal(t, before, m.GetSegment(ctx, restartSegID).GetManifestPath())
			assert.Len(t, store.backfillEntriesAt(before), 1)
			rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
			require.NoError(t, err)
			if retire {
				assert.Empty(t, rows)
			} else {
				require.Len(t, rows, 1)
				assert.Equal(t, record.BuildID, rows[0].BuildID)
			}
		})
	}
}
