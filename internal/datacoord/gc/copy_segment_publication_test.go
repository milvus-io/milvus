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

package gc

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/copysegment"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore"
	metastorekv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

type copyCompletionFailCatalog struct {
	metastore.DataCoordCatalog
	failCompletion bool
}

func (c *copyCompletionFailCatalog) SaveCopySegmentTask(ctx context.Context, task *datapb.CopySegmentTask) error {
	if c.failCompletion && task.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskCompleted {
		return errors.New("injected task completion persistence failure")
	}
	return c.DataCoordCatalog.SaveCopySegmentTask(ctx, task)
}

func copyPublicationFixture(t *testing.T) (*meta.Meta, *copyCompletionFailCatalog, copysegment.CopySegmentMeta, copysegment.CopySegmentTask, *datapb.QueryCopySegmentResponse, *meta.FakeManifestStore) {
	t.Helper()
	meta.WithSegmentIndexManifestWrites(t, true)
	index.WithManifestIndexRollback(t, false)
	store := meta.NewFakeManifestStore(t)
	ctx := context.Background()
	catalog := &copyCompletionFailCatalog{DataCoordCatalog: metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", "")}
	m := index.BootMigrationMeta(t, catalog, meta.RestartCollID)
	index.SeedBackfillIndexDefinition(t, m)
	segment := meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: meta.RestartSegID, CollectionID: meta.RestartCollID, PartitionID: index.RestartPartID,
		State: commonpb.SegmentState_Importing, IsImporting: true,
		NumOfRows: 1000, StorageVersion: storage.StorageV3,
		ManifestPath: packed.MarshalManifestPath("/tmp/test-restart/insert_log/300/30/8001", 0),
	})
	require.NoError(t, m.AddSegment(ctx, segment))
	record := &model.SegmentIndex{
		SegmentID: meta.RestartSegID, CollectionID: meta.RestartCollID, PartitionID: index.RestartPartID,
		IndexID: meta.RestartIndexID, BuildID: meta.RestartBuildID, IndexState: commonpb.IndexState_Finished,
		IndexVersion: 1, NumRows: 1000, IndexFileKeys: []string{"index.bin"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
	}
	entry, err := meta.BuildManifestIndexInfo(m, segment, record)
	require.NoError(t, err)
	manifest := packed.MarshalManifestPath("/tmp/test-restart/insert_log/300/30/8001", 1)
	store.Revisions[manifest] = []packed.ManifestIndexInfo{entry}
	task := copysegment.CreateTestCopyTask(meta.RestartCollID, meta.RestartSegID).(*copysegment.Task)
	task.Task.Load().State = datapb.CopySegmentTaskState_CopySegmentTaskInProgress
	task.Task.Load().IndexWriteToManifest = proto.Bool(true)
	task.Task.Load().IdMappings[0].PartitionId = index.RestartPartID
	copies, err := copysegment.NewCopySegmentMeta(ctx, catalog, m, m.IndexCatalog.(*index.IndexMeta), nil, nil)
	require.NoError(t, err)
	require.NoError(t, copies.AddTask(ctx, task))
	result := &datapb.QueryCopySegmentResponse{
		State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		SegmentResults: []*datapb.CopySegmentResult{{
			SegmentId: meta.RestartSegID, ManifestPath: manifest,
			ManifestIndexRewritten: proto.Bool(true), ManifestIndexBuildIds: []int64{meta.RestartBuildID},
			IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{meta.RestartBuildID: {
				FieldId: meta.RestartFieldID, BuildId: meta.RestartBuildID, IndexName: entry.IndexName,
				Version: record.IndexVersion, IndexFilePaths: record.IndexFileKeys,
				IndexStorePathVersion: record.IndexStorePathVersion,
			}},
		}},
	}
	return m, catalog, copies, task, result, store
}

func TestCopyResultReplayAfterRollbackAndRestart(t *testing.T) {
	ctx := context.Background()
	m, catalog, copies, task, result, _ := copyPublicationFixture(t)
	catalog.failCompletion = true
	require.ErrorContains(t, copysegment.SyncCopySegmentTask(task, result, copies, m, m.IndexCatalog.(*index.IndexMeta)), "injected task completion")
	require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskInProgress, copies.GetTask(ctx, task.GetTaskId()).GetState())
	require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(ctx, meta.RestartSegID).GetState())
	index.WithManifestIndexRollback(t, true)
	inspector := index.NewMigrationRollbackInspector(ctx, m)
	require.True(t, inspector.RunOnceForTest(ctx))
	final := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()
	require.NotEqual(t, result.GetSegmentResults()[0].GetManifestPath(), final)
	require.False(t, index.IndexManifestPublished(t, m.IndexCatalog.(*index.IndexMeta), meta.RestartBuildID))

	// Recover the durable InProgress task together with the rolled-back target.
	restarted := index.BootMigrationMeta(t, catalog, meta.RestartCollID)
	recoveredCopies, err := copysegment.NewCopySegmentMeta(ctx, catalog, restarted, restarted.IndexCatalog.(*index.IndexMeta), nil, nil)
	require.NoError(t, err)
	recoveredTask := recoveredCopies.GetTask(ctx, task.GetTaskId())
	require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskInProgress, recoveredTask.GetState())
	catalog.failCompletion = false
	require.NoError(t, copysegment.SyncCopySegmentTask(recoveredTask, result, recoveredCopies, restarted, restarted.IndexCatalog.(*index.IndexMeta)))
	require.Equal(t, final, restarted.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
	require.False(t, restarted.GetSegment(ctx, meta.RestartSegID).GetManifestHasIndex())
	record, ok := restarted.IndexCatalog.(*index.IndexMeta).GetIndexJob(meta.RestartBuildID)
	require.True(t, ok)
	require.False(t, record.ManifestPublished)
	require.Equal(t, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED, record.IndexStorePathVersion)
	rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, record, rows[0])
	require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskCompleted, recoveredCopies.GetTask(ctx, task.GetTaskId()).GetState())
}

func TestCopyResultRestoreReplayKeepsImportProtection(t *testing.T) {
	ctx := t.Context()
	m, catalog, copies, task, result, store := copyPublicationFixture(t)
	job := copysegment.NewTestCopyJob(task.GetJobId(), datapb.CopySegmentJobState_CopySegmentJobExecuting).(*copysegment.Job)
	job.CreatesCollection = true
	job.CollectionId = meta.RestartCollID
	job.IdMappings = task.GetIdMappings()
	require.NoError(t, copies.AddJob(ctx, job))
	catalog.failCompletion = true
	require.ErrorContains(t, copysegment.SyncCopySegmentTask(task, result, copies, m, m.IndexCatalog.(*index.IndexMeta)), "injected task completion")
	segment := m.GetSegment(ctx, meta.RestartSegID)
	require.Equal(t, commonpb.SegmentState_Flushed, segment.GetState())
	require.True(t, segment.GetIsImporting())

	restarted := index.BootMigrationMeta(t, catalog, meta.RestartCollID)
	recovered, err := copysegment.NewCopySegmentMeta(ctx, catalog, restarted, restarted.IndexCatalog.(*index.IndexMeta), nil, nil)
	require.NoError(t, err)
	catalog.failCompletion = false
	store.FailReadsFrom()
	require.NoError(t, copysegment.SyncCopySegmentTask(recovered.GetTask(ctx, task.GetTaskId()), result, recovered, restarted, restarted.IndexCatalog.(*index.IndexMeta)))
	require.True(t, restarted.GetSegment(ctx, meta.RestartSegID).GetIsImporting())
	require.Equal(t, segment.GetManifestPath(), restarted.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
	require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskCompleted, recovered.GetTask(ctx, task.GetTaskId()).GetState())
}

func TestCopyResultKeepsImportProtectionThroughIndexInstall(t *testing.T) {
	ctx := context.Background()
	m, _, copies, task, result, _ := copyPublicationFixture(t)
	gc := &GarbageCollector{Meta: m, Option: GcOption{Cli: m.ChunkManager, IndexMeta: m.IndexCatalog.(*index.IndexMeta)}}
	var original func(*index.IndexMeta, context.Context, *model.SegmentIndex) error
	patch := mockey.Mock((*index.IndexMeta).AddSegmentIndexFromManifest).Origin(&original).To(func(im *index.IndexMeta, ctx context.Context, record *model.SegmentIndex) error {
		segment := m.GetSegment(ctx, meta.RestartSegID)
		require.True(t, segment.GetIsImporting())
		require.Equal(t, commonpb.SegmentState_Importing, segment.GetState())
		// Import protection applies both before any build exists and after a
		// partial installation, whose manifest base has not yet been published.
		check := gc.newIndexFileReclaimChecker(ctx)
		require.False(t, check("copied-index", meta.RestartCollID, meta.RestartSegID, meta.RestartBuildID))
		require.NoError(t, original(im, ctx, record))
		require.False(t, gc.newIndexFileReclaimChecker(ctx)("copied-index", meta.RestartCollID, meta.RestartSegID, meta.RestartBuildID))
		require.False(t, m.GetSegmentManifestLocks().TryLock(meta.RestartSegID), "migration must wait until target finalization")
		return nil
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	require.NoError(t, copysegment.SyncCopySegmentTask(task, result, copies, m, m.IndexCatalog.(*index.IndexMeta)))
	require.False(t, m.GetSegment(ctx, meta.RestartSegID).GetIsImporting())
	require.True(t, index.IndexManifestPublished(t, m.IndexCatalog.(*index.IndexMeta), meta.RestartBuildID))
}

func TestCopyResultGCFinalizationReadOrdering(t *testing.T) {
	ctx := context.Background()
	m, _, copies, task, result, _ := copyPublicationFixture(t)
	gc := &GarbageCollector{Meta: m, Option: GcOption{Cli: m.ChunkManager, IndexMeta: m.IndexCatalog.(*index.IndexMeta)}}
	var original func(*meta.Meta, context.Context, int64) *meta.SegmentInfo
	finalized := false
	patch := mockey.Mock((*meta.Meta).GetSegment).Origin(&original).To(func(receiver *meta.Meta, ctx context.Context, segmentID int64) *meta.SegmentInfo {
		if receiver == m && !finalized {
			finalized = true
			// Force completion just before GC reads the segment state. A prior
			// missing-build observation must not be used with this finalized state.
			require.NoError(t, copysegment.SyncCopySegmentTask(task, result, copies, m, m.IndexCatalog.(*index.IndexMeta)))
		}
		return original(receiver, ctx, segmentID)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	file := "/tmp/test-restart/insert_log/300/30/8001/_index/8100/1/index.bin"
	require.False(t, gc.newIndexFileReclaimChecker(ctx)(file, meta.RestartCollID, meta.RestartSegID, meta.RestartBuildID))
	require.True(t, finalized)
}

func TestCopyResultEtcdFinalizationWakesBackfill(t *testing.T) {
	ctx := context.Background()
	m, _, copies, task, result, _ := copyPublicationFixture(t)
	task.(*copysegment.Task).Task.Load().IndexWriteToManifest = proto.Bool(false)
	result.SegmentResults[0].ManifestIndexBuildIds = nil
	result.SegmentResults[0].IndexInfos[meta.RestartBuildID].IndexStorePathVersion = indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED
	var original func(*index.IndexMeta, context.Context, *model.SegmentIndex) error
	patch := mockey.Mock((*index.IndexMeta).AddSegmentIndex).Origin(&original).To(func(im *index.IndexMeta, ctx context.Context, record *model.SegmentIndex) error {
		require.NoError(t, original(im, ctx, record))
		select {
		case <-m.IndexCatalog.(*index.IndexMeta).ManifestIndexBackfillNotifyForTest():
		default:
			t.Fatal("index install did not notify")
		}
		work, pending := index.NewMigrationBackfillInspector(ctx, m).ScanForTest(ctx)
		require.Empty(t, work)
		require.Zero(t, pending, "an importing target must not be migrated")
		return nil
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	require.NoError(t, copysegment.SyncCopySegmentTask(task, result, copies, m, m.IndexCatalog.(*index.IndexMeta)))
	select {
	case <-m.IndexCatalog.(*index.IndexMeta).ManifestIndexBackfillNotifyForTest():
	default:
		t.Fatal("finalized target did not wake backfill")
	}
	work, pending := index.NewMigrationBackfillInspector(ctx, m).ScanForTest(ctx)
	require.Len(t, work, 1)
	require.Equal(t, 1, pending)
}

func TestCopyResultRevalidatesTargetBeforeFinalization(t *testing.T) {
	for _, dropped := range []bool{true, false} {
		name := "manifest advanced"
		if dropped {
			name = "segment dropped"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			m, _, copies, task, result, _ := copyPublicationFixture(t)
			changedPath := packed.MarshalManifestPath("/tmp/test-restart/insert_log/300/30/8001", 2)
			var original func(*index.IndexMeta, context.Context, *model.SegmentIndex) error
			patch := mockey.Mock((*index.IndexMeta).AddSegmentIndexFromManifest).Origin(&original).To(func(im *index.IndexMeta, ctx context.Context, record *model.SegmentIndex) error {
				require.NoError(t, original(im, ctx, record))
				if dropped {
					return m.SetState(ctx, meta.RestartSegID, commonpb.SegmentState_Dropped)
				}
				return m.UpdateSegmentsInfo(ctx, meta.UpdateManifest(meta.RestartSegID, changedPath))
			}).Build()
			t.Cleanup(func() { patch.UnPatch() })
			require.Error(t, copysegment.SyncCopySegmentTask(task, result, copies, m, m.IndexCatalog.(*index.IndexMeta)))
			segment := m.GetSegment(ctx, meta.RestartSegID)
			require.True(t, segment.GetIsImporting())
			if dropped {
				require.Equal(t, commonpb.SegmentState_Dropped, segment.GetState())
			} else {
				require.Equal(t, changedPath, segment.GetManifestPath())
			}
		})
	}
}
