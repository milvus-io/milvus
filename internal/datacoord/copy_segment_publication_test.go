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

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
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

func copyPublicationFixture(t *testing.T) (*meta, *copyCompletionFailCatalog, CopySegmentMeta, CopySegmentTask, *datapb.QueryCopySegmentResponse) {
	t.Helper()
	withSegmentIndexManifestWrites(t, true)
	withManifestIndexRollback(t, false)
	store := newFakeManifestStore(t)
	ctx := context.Background()
	catalog := &copyCompletionFailCatalog{DataCoordCatalog: metastorekv.NewCatalog(NewMetaMemoryKV(), "", "")}
	m := bootMetaForRestart(t, catalog, restartCollID)
	seedBackfillIndexDefinition(t, m)
	segment := NewSegmentInfo(&datapb.SegmentInfo{
		ID: restartSegID, CollectionID: restartCollID, PartitionID: restartPartID,
		State: commonpb.SegmentState_Importing, IsImporting: true,
		NumOfRows: 1000, StorageVersion: storage.StorageV3,
		ManifestPath: packed.MarshalManifestPath("/tmp/test-restart/insert_log/300/30/8001", 0),
	})
	require.NoError(t, m.AddSegment(ctx, segment))
	record := &model.SegmentIndex{
		SegmentID: restartSegID, CollectionID: restartCollID, PartitionID: restartPartID,
		IndexID: restartIndexID, BuildID: restartBuildID, IndexState: commonpb.IndexState_Finished,
		IndexVersion: 1, NumRows: 1000, IndexFileKeys: []string{"index.bin"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
	}
	entry, err := buildManifestIndexInfo(m, segment, record)
	require.NoError(t, err)
	manifest := packed.MarshalManifestPath("/tmp/test-restart/insert_log/300/30/8001", 1)
	store.revisions[manifest] = []packed.ManifestIndexInfo{entry}
	task := createTestCopyTask(restartCollID, restartSegID).(*copySegmentTask)
	task.task.Load().State = datapb.CopySegmentTaskState_CopySegmentTaskInProgress
	task.task.Load().IndexWriteToManifest = proto.Bool(true)
	task.task.Load().IdMappings[0].PartitionId = restartPartID
	copies, err := NewCopySegmentMeta(ctx, catalog, m, nil, nil)
	require.NoError(t, err)
	require.NoError(t, copies.AddTask(ctx, task))
	result := &datapb.QueryCopySegmentResponse{
		State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		SegmentResults: []*datapb.CopySegmentResult{{
			SegmentId: restartSegID, ManifestPath: manifest,
			ManifestIndexRewritten: proto.Bool(true), ManifestIndexBuildIds: []int64{restartBuildID},
			IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{restartBuildID: {
				FieldId: restartFieldID, BuildId: restartBuildID, IndexName: entry.IndexName,
				Version: record.IndexVersion, IndexFilePaths: record.IndexFileKeys,
				IndexStorePathVersion: record.IndexStorePathVersion,
			}},
		}},
	}
	return m, catalog, copies, task, result
}

func TestCopyResultReplayAfterRollbackAndRestart(t *testing.T) {
	ctx := context.Background()
	m, catalog, copies, task, result := copyPublicationFixture(t)
	catalog.failCompletion = true
	require.ErrorContains(t, SyncCopySegmentTask(task, result, copies, m), "injected task completion")
	require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskInProgress, copies.GetTask(ctx, task.GetTaskId()).GetState())
	require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(ctx, restartSegID).GetState())
	withManifestIndexRollback(t, true)
	inspector := newManifestIndexRollbackInspector(ctx, m)
	require.True(t, inspector.runOnce(ctx))
	final := m.GetSegment(ctx, restartSegID).GetManifestPath()
	require.NotEqual(t, result.GetSegmentResults()[0].GetManifestPath(), final)
	require.False(t, indexManifestPublished(t, m.indexMeta, restartBuildID))

	// Recover the durable InProgress task together with the rolled-back target.
	restarted := bootMetaForRestart(t, catalog, restartCollID)
	recoveredCopies, err := NewCopySegmentMeta(ctx, catalog, restarted, nil, nil)
	require.NoError(t, err)
	recoveredTask := recoveredCopies.GetTask(ctx, task.GetTaskId())
	require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskInProgress, recoveredTask.GetState())
	catalog.failCompletion = false
	require.NoError(t, SyncCopySegmentTask(recoveredTask, result, recoveredCopies, restarted))
	require.Equal(t, final, restarted.GetSegment(ctx, restartSegID).GetManifestPath())
	require.False(t, restarted.GetSegment(ctx, restartSegID).GetManifestHasIndex())
	record, ok := restarted.indexMeta.GetIndexJob(restartBuildID)
	require.True(t, ok)
	require.False(t, record.ManifestPublished)
	require.Equal(t, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED, record.IndexStorePathVersion)
	rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, record, rows[0])
	require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskCompleted, recoveredCopies.GetTask(ctx, task.GetTaskId()).GetState())
}

func TestCopyResultKeepsImportProtectionThroughIndexInstall(t *testing.T) {
	ctx := context.Background()
	m, _, copies, task, result := copyPublicationFixture(t)
	gc := &garbageCollector{meta: m, option: GcOption{cli: m.chunkManager}}
	var original func(context.Context, *datapb.CopySegmentResult, CopySegmentTask, *meta, CopySegmentMeta, map[int64]int64) error
	patch := mockey.Mock(syncVectorScalarIndexes).Origin(&original).To(func(ctx context.Context, result *datapb.CopySegmentResult,
		task CopySegmentTask, m *meta, copies CopySegmentMeta, published map[int64]int64,
	) error {
		segment := m.GetSegment(ctx, restartSegID)
		require.True(t, segment.GetIsImporting())
		require.Equal(t, commonpb.SegmentState_Importing, segment.GetState())
		// Import protection applies both before any build exists and after a
		// partial installation, whose manifest base has not yet been published.
		check := gc.newIndexFileReclaimChecker(ctx)
		require.False(t, check("copied-index", restartCollID, restartSegID, restartBuildID))
		require.NoError(t, original(ctx, result, task, m, copies, published))
		require.False(t, gc.newIndexFileReclaimChecker(ctx)("copied-index", restartCollID, restartSegID, restartBuildID))
		require.False(t, m.getSegmentManifestLocks().TryLock(restartSegID), "migration must wait until target finalization")
		return nil
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	require.NoError(t, SyncCopySegmentTask(task, result, copies, m))
	require.False(t, m.GetSegment(ctx, restartSegID).GetIsImporting())
	require.True(t, indexManifestPublished(t, m.indexMeta, restartBuildID))
}

func TestCopyResultGCFinalizationReadOrdering(t *testing.T) {
	ctx := context.Background()
	m, _, copies, task, result := copyPublicationFixture(t)
	gc := &garbageCollector{meta: m, option: GcOption{cli: m.chunkManager}}
	var original func(*meta, context.Context, int64) *SegmentInfo
	finalized := false
	patch := mockey.Mock((*meta).GetSegment).Origin(&original).To(func(receiver *meta, ctx context.Context, segmentID int64) *SegmentInfo {
		if receiver == m && !finalized {
			finalized = true
			// Force completion just before GC reads the segment state. A prior
			// missing-build observation must not be used with this finalized state.
			require.NoError(t, SyncCopySegmentTask(task, result, copies, m))
		}
		return original(receiver, ctx, segmentID)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	file := "/tmp/test-restart/insert_log/300/30/8001/_index/8100/1/index.bin"
	require.False(t, gc.newIndexFileReclaimChecker(ctx)(file, restartCollID, restartSegID, restartBuildID))
	require.True(t, finalized)
}

func TestCopyResultEtcdFinalizationWakesBackfill(t *testing.T) {
	ctx := context.Background()
	m, _, copies, task, result := copyPublicationFixture(t)
	task.(*copySegmentTask).task.Load().IndexWriteToManifest = proto.Bool(false)
	result.SegmentResults[0].ManifestIndexBuildIds = nil
	result.SegmentResults[0].IndexInfos[restartBuildID].IndexStorePathVersion = indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED
	var original func(context.Context, *datapb.CopySegmentResult, CopySegmentTask, *meta, CopySegmentMeta, map[int64]int64) error
	patch := mockey.Mock(syncVectorScalarIndexes).Origin(&original).To(func(ctx context.Context, result *datapb.CopySegmentResult,
		task CopySegmentTask, m *meta, copies CopySegmentMeta, published map[int64]int64,
	) error {
		require.NoError(t, original(ctx, result, task, m, copies, published))
		select {
		case <-m.indexMeta.manifestIndexBackfillNotify:
		default:
			t.Fatal("index install did not notify")
		}
		work, pending := newManifestIndexBackfillInspector(ctx, m).scan(ctx)
		require.Empty(t, work)
		require.Zero(t, pending, "an importing target must not be migrated")
		return nil
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	require.NoError(t, SyncCopySegmentTask(task, result, copies, m))
	select {
	case <-m.indexMeta.manifestIndexBackfillNotify:
	default:
		t.Fatal("finalized target did not wake backfill")
	}
	work, pending := newManifestIndexBackfillInspector(ctx, m).scan(ctx)
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
			m, _, copies, task, result := copyPublicationFixture(t)
			changedPath := packed.MarshalManifestPath("/tmp/test-restart/insert_log/300/30/8001", 2)
			patch := mockey.Mock(syncJSONKeyIndexes).To(func(context.Context, *datapb.CopySegmentResult, CopySegmentTask, *meta, CopySegmentMeta) error {
				if dropped {
					return m.SetState(ctx, restartSegID, commonpb.SegmentState_Dropped)
				}
				return m.UpdateSegmentsInfo(ctx, UpdateManifest(restartSegID, changedPath))
			}).Build()
			t.Cleanup(func() { patch.UnPatch() })
			require.Error(t, SyncCopySegmentTask(task, result, copies, m))
			segment := m.GetSegment(ctx, restartSegID)
			require.True(t, segment.GetIsImporting())
			if dropped {
				require.Equal(t, commonpb.SegmentState_Dropped, segment.GetState())
			} else {
				require.Equal(t, changedPath, segment.GetManifestPath())
			}
		})
	}
}
