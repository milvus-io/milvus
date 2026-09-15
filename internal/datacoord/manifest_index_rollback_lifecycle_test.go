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
				require.NoError(t, newManifestIndexBackfillInspector(ctx, m).backfillIndex(ctx, restartSegID, record.BuildID))
			}
			manifestOnly := m.GetSegment(ctx, restartSegID).GetManifestPath()
			// Snapshot-pinned immutable revisions retain their old index section.
			oldEntries, err := packed.GetManifestIndexInfos(manifestOnly, cfg)
			require.NoError(t, err)
			require.Len(t, oldEntries, 3)
			withManifestIndexRollback(t, true)
			setRollbackTestParam(t, &Params.MetaStoreCfg.MaxEtcdTxnNum, "2")
			n, err := m.rollbackSegmentIndexes(ctx, restartSegID)
			require.NoError(t, err)
			assert.Equal(t, 1, n)
			partial := m.GetSegment(ctx, restartSegID).GetManifestPath()
			assert.True(t, m.GetSegment(ctx, restartSegID).GetManifestHasIndex())
			kv.failAtomicUpdate = true
			_, err = m.rollbackSegmentIndexes(ctx, restartSegID)
			require.Error(t, err)
			assert.Equal(t, partial, m.GetSegment(ctx, restartSegID).GetManifestPath())
			rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			kv.failAtomicUpdate = false
			m = boot()
			for range 2 {
				n, err = m.rollbackSegmentIndexes(ctx, restartSegID)
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
			// Model the downgrade boundary: no manifest index reads are allowed.
			noRead := mockey.Mock(packed.GetManifestIndexInfos).Return(nil, merr.ErrIoTooManyRequests).Build()
			t.Cleanup(func() { noRead.UnPatch() })
			restarted := boot()
			assert.Zero(t, noRead.Times())
			for _, record := range records {
				recovered, found := restarted.indexMeta.GetIndexJob(record.BuildID)
				require.True(t, found)
				assert.Equal(t, record.IndexFileKeys, recovered.IndexFileKeys)
				assert.Equal(t, layout, recovered.IndexStorePathVersion)
				assert.False(t, restarted.indexMeta.isSegmentIndexCatalogAbsent(record.BuildID))
			}
		})
	}
}

func TestManifestIndexRollbackCopyDrainAndDuplicateResult(t *testing.T) {
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
	inspector := newManifestIndexRollbackInspector(ctx, m, copies)
	inspector.runOnce(ctx)
	assert.False(t, inspector.ready)
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackPendingCopies))
	assert.Equal(t, before, m.GetSegment(ctx, restartSegID).GetManifestPath())
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
	assert.False(t, m.indexMeta.isSegmentIndexCatalogAbsent(restartBuildID))
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
			inspector := newManifestIndexRollbackInspector(ctx, m, copies)
			inspector.runOnce(ctx)
			inspector.runOnce(ctx)
			require.True(t, inspector.ready)
			before := m.GetSegment(ctx, restartSegID).GetManifestPath()
			err = SyncCopySegmentTask(task, &datapb.QueryCopySegmentResponse{
				State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
			}, copies, m)
			require.ErrorContains(t, err, "cannot publish a failed copy task")
			assert.Equal(t, before, m.GetSegment(ctx, restartSegID).GetManifestPath())
			assert.False(t, m.indexMeta.isSegmentIndexCatalogAbsent(restartBuildID))
			assert.True(t, copies.GetTask(ctx, task.GetTaskId()).GetCleanupRequired())
			inspector.runOnce(ctx)
			assert.False(t, inspector.ready, "rearmed cleanup must remain pending")
		})
	}
}

func TestManifestIndexRollbackEmptyMarkerAndUnpublishedCopy(t *testing.T) {
	m, catalog, store, _ := rollbackFixture(t)
	ctx := context.TODO()
	_, err := m.rollbackSegmentIndexes(ctx, restartSegID)
	require.NoError(t, err)
	before := m.GetSegment(ctx, restartSegID).GetManifestPath()
	require.Empty(t, store.backfillEntriesAt(before))
	require.NoError(t, m.UpdateSegmentsInfo(ctx, UpdateManifestHasIndex(restartSegID)))
	copies, err := NewCopySegmentMeta(ctx, catalog, m, nil, nil)
	require.NoError(t, err)
	task := createTestCopyTask(restartCollID, restartSegID+1)
	require.NoError(t, copies.AddTask(ctx, task))
	inspector := newManifestIndexRollbackInspector(ctx, m, copies)
	inspector.runOnce(ctx)
	assert.False(t, m.GetSegment(ctx, restartSegID).GetManifestHasIndex())
	assert.Equal(t, before, m.GetSegment(ctx, restartSegID).GetManifestPath())
	inspector.runOnce(ctx)
	assert.False(t, inspector.ready, "an unpublished copy target blocks readiness")
	assert.Zero(t, testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackPending))
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackPendingCopies))
}

func TestManifestIndexRollbackBoundedShutdown(t *testing.T) {
	m, catalog, _, _ := rollbackFixture(t)
	ctx := context.TODO()
	for offset := int64(1); offset < 4; offset++ {
		seedLegacyBackfillRecord(t, m, restartSegID+offset, restartBuildID+offset)
		require.NoError(t, newManifestIndexBackfillInspector(ctx, m).backfillIndex(ctx, restartSegID+offset, restartBuildID+offset))
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
	inspector := newManifestIndexRollbackInspector(ctx, m, nil)
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
		_, err := m.rollbackSegmentIndexes(ctx, restartSegID)
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
