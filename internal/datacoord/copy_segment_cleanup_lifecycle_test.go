// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"io/fs"
	"path"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newCopyCleanupLifecycle(t *testing.T) (*copySegmentTask, CopySegmentMeta, *meta) {
	t.Helper()
	ctx := context.Background()
	catalog := kvdatacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	m := bootMetaForRestart(t, catalog, 100)
	root := t.TempDir()
	m.chunkManager = storage.NewLocalChunkManager(objectstorage.RootPath(root))
	copies, err := NewCopySegmentMeta(ctx, catalog, m, nil, nil)
	require.NoError(t, err)
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	require.NoError(t, copies.AddTask(ctx, task))
	require.NoError(t, copies.AddJob(ctx, newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting)))
	segment := newTestCopySegment(2001)
	segment.StorageVersion = storage.StorageV3
	segment.IsImporting = true
	require.NoError(t, m.AddSegment(ctx, segment))
	require.NoError(t, task.prepareCopyCleanup(ctx, &datapb.CopySegmentRequest{
		StorageConfig: &indexpb.StorageConfig{RootPath: root},
		Sources: []*datapb.CopySegmentSource{{StorageVersion: storage.StorageV3, IndexFiles: []*indexpb.IndexFilePathInfo{{
			BuildID: 5, IndexVersion: 1, IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
		}}}},
		Targets: []*datapb.CopySegmentTarget{{CollectionId: 100, PartitionId: 10, SegmentId: 2001, NewBuildIds: map[int64]int64{5: 6}}},
	}))
	require.NoError(t, copies.UpdateTask(ctx, task.GetTaskId(), UpdateCopyTaskNodeID(42), UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskInProgress)))
	return task, copies, m
}

func TestRejectedCopyCleanupWorkerLossAfterAdmission(t *testing.T) {
	for _, scenario := range []struct {
		name                                   string
		admitted, failStateSave, failClearSave bool
	}{
		{name: "ordinary worker loss remains retryable"},
		{name: "interrupted completed result", admitted: true},
		{name: "failed-state save retries", admitted: true, failStateSave: true},
		{name: "worker-clear save retries", admitted: true, failClearSave: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx := context.Background()
			task, copies, m := newCopyCleanupLifecycle(t)
			require.NoError(t, copies.UpdateTask(ctx, task.GetTaskId(), updateCopyTaskCleanup(scenario.admitted)))
			// Crash cut: completion admission is durable; installation/terminal save is not.
			copies, err := NewCopySegmentMeta(ctx, m.catalog, m, nil, nil)
			require.NoError(t, err)
			task = copies.GetTask(ctx, task.GetTaskId()).(*copySegmentTask)
			prefixes := append([]string(nil), task.GetCleanupPrefixes()...)
			cluster := session.NewMockCluster(t)
			cluster.EXPECT().QueryCopySegment(int64(42), mock.Anything).Return(nil, merr.WrapErrNodeNotFound(42))
			if scenario.failStateSave || scenario.failClearSave {
				var original func(*kvdatacoord.Catalog, context.Context, *datapb.CopySegmentTask) error
				patch := mockey.Mock((*kvdatacoord.Catalog).SaveCopySegmentTask).Origin(&original).To(func(c *kvdatacoord.Catalog, ctx context.Context, record *datapb.CopySegmentTask) error {
					if (scenario.failStateSave && record.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskFailed) || (scenario.failClearSave && record.GetNodeId() == NullNodeID) {
						return merr.ErrServiceUnavailable
					}
					return original(c, ctx, record)
				}).Build()
				task.QueryTaskOnWorker(cluster)
				patch.UnPatch()
				require.Equal(t, taskcommon.InProgress, task.GetTaskState(), "failed saves must retain scheduler polling")
				require.EqualValues(t, 42, task.GetNodeId())
				require.True(t, task.GetCleanupRequired())
				if scenario.failClearSave {
					require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskFailed, task.GetState())
				}
			}
			task.QueryTaskOnWorker(cluster)
			require.EqualValues(t, NullNodeID, task.GetNodeId())
			require.Equal(t, prefixes, task.GetCleanupPrefixes())
			if scenario.admitted {
				require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskFailed, task.GetState())
				require.Equal(t, taskcommon.Failed, task.GetTaskState())
				require.True(t, task.GetCleanupRequired())
				require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobFailed, copies.GetJob(ctx, task.GetJobId()).GetState())
				task.DropTaskOnWorker(cluster) // Assignment was durably released; no Drop RPC.
			} else {
				require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskPending, task.GetState())
				require.False(t, task.GetCleanupRequired())
			}
			recovered, err := NewCopySegmentMeta(ctx, m.catalog, m, nil, nil)
			require.NoError(t, err)
			require.Equal(t, task.GetState(), recovered.GetTask(ctx, task.GetTaskId()).GetState())
			require.Equal(t, scenario.admitted, recovered.GetTask(ctx, task.GetTaskId()).GetCleanupRequired())
		})
	}
}

type copyCleanupLoadedHandler struct {
	*mockHandler
	loaded []int64
}

func (h *copyCleanupLoadedHandler) ListLoadedSegments(context.Context) ([]int64, error) {
	return h.loaded, nil
}

func TestRejectedCopyCleanupWaitsForOrdinaryGC(t *testing.T) {
	for _, protection := range []string{"snapshot", "loaded reader", "paused collection", "retention"} {
		t.Run(protection, func(t *testing.T) {
			ctx := context.Background()
			task, copies, m := newCopyCleanupLifecycle(t)
			root := m.chunkManager.RootPath()
			base := path.Join(root, "insert_log/100/10/2001")
			data := path.Join(base, "_data/data.parquet")
			orphanIndex := path.Join(root, "index_v1/100/10/2001/6/1/index.bin")
			require.NoError(t, m.chunkManager.Write(ctx, data, []byte("published segment")))
			require.NoError(t, m.chunkManager.Write(ctx, orphanIndex, []byte("uninstalled index")))
			// The target publication succeeds, then the final task save fails.
			var original func(*kvdatacoord.Catalog, context.Context, *datapb.CopySegmentTask) error
			patch := mockey.Mock((*kvdatacoord.Catalog).SaveCopySegmentTask).Origin(&original).To(func(c *kvdatacoord.Catalog, ctx context.Context, record *datapb.CopySegmentTask) error {
				if record.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskCompleted {
					return merr.ErrServiceUnavailable
				}
				return original(c, ctx, record)
			}).Build()
			cluster := session.NewMockCluster(t)
			rewritten := true
			cluster.EXPECT().QueryCopySegment(int64(42), mock.Anything).Return(&datapb.QueryCopySegmentResponse{
				State:          datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
				SegmentResults: []*datapb.CopySegmentResult{{SegmentId: 2001, ManifestPath: packed.MarshalManifestPath(base, 3), ManifestIndexRewritten: &rewritten}},
			}, nil)
			task.QueryTaskOnWorker(cluster)
			patch.UnPatch()
			require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskFailed, task.GetState())
			require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(ctx, 2001).GetState())
			require.False(t, m.GetSegment(ctx, 2001).GetIsImporting())
			// This is the same visible-segment shape selected by GenSnapshot.
			handler := &copyCleanupLoadedHandler{mockHandler: newMockHandler()}
			gc := newGarbageCollector(m, handler, GcOption{cli: m.chunkManager})
			defer gc.option.removeObjectPool.Release()
			release := func() {}
			switch protection {
			case "snapshot":
				m.snapshotMeta = createTestSnapshotMeta(t)
				info := &datapb.SnapshotInfo{Id: 900, CollectionId: 100, Name: "retained"}
				insertTestSnapshot(m.snapshotMeta, info, []int64{2001}, nil)
				m.snapshotMeta.registerSnapshotProtection(info, []int64{2001}, nil)
				require.True(t, m.snapshotMeta.IsSegmentGCBlocked(100, 2001))
				release = func() { m.snapshotMeta.snapshotID2Info.Remove(900); m.snapshotMeta.rebuildAllSegmentProtection() }
			case "loaded reader":
				handler.loaded = []int64{2001}
				release = func() { handler.loaded = nil }
			case "paused collection":
				records := NewGCPauseRecords()
				_, err := records.Insert("copy test", time.Now().Add(time.Hour))
				require.NoError(t, err)
				gc.pausedCollection.Insert(100, records)
				release = func() { gc.pausedCollection.Remove(100) }
			case "retention":
				gc.option.dropTolerance = time.Hour
				release = func() { gc.option.dropTolerance = 0 }
			}
			(&copySegmentInspector{ctx: ctx, meta: m, copyMeta: copies}).processFailed(task)
			gc.recycleDroppedSegments(ctx, nil)
			require.NotNil(t, m.GetSegment(ctx, 2001), "ordinary GC must preserve protected metadata")
			require.NoError(t, cleanupRejectedCopy(ctx, task, m, copies))
			for _, file := range []string{data, orphanIndex} {
				exists, err := m.chunkManager.Exist(ctx, file)
				require.NoError(t, err)
				require.True(t, exists, "copy cleanup must preserve protected bytes")
			}
			require.True(t, task.GetCleanupRequired())
			release()
			gc.recycleDroppedSegments(ctx, nil)
			require.Nil(t, m.GetSegment(ctx, 2001), "ordinary GC must durably retire the target after protection ends")
			require.Nil(t, bootMetaForRestart(t, m.catalog, 100).GetSegment(ctx, 2001))
			require.NoError(t, cleanupRejectedCopy(ctx, task, m, copies))
			exists, err := m.chunkManager.Exist(ctx, orphanIndex)
			require.NoError(t, err)
			require.False(t, exists)
			require.False(t, task.GetCleanupRequired())
		})
	}
}

func TestRejectedCopyCleanupNeverCreatedManifestBase(t *testing.T) {
	for _, failure := range []string{"absent root", "missing child", "permission", "remote failure"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			task, copies, m := newCopyCleanupLifecycle(t)
			require.NoError(t, copies.UpdateTask(ctx, task.GetTaskId(), UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed), updateCopyTaskCleanup(true)))
			(&copySegmentInspector{ctx: ctx, meta: m, copyMeta: copies}).processFailed(task)
			base, version, err := packed.UnmarshalManifestPath(m.GetSegment(ctx, 2001).GetManifestPath())
			require.NoError(t, err)
			require.Zero(t, version)
			sibling := base + "0/_data/sibling.parquet"
			require.NoError(t, m.chunkManager.Write(ctx, sibling, []byte("another segment")))
			gc := newGarbageCollector(m, newMockHandler(), GcOption{cli: m.chunkManager})
			defer gc.option.removeObjectPool.Release()
			if failure != "absent root" {
				var failureErr error
				switch failure {
				case "missing child":
					failureErr = &fs.PathError{Op: "lstat", Path: path.Join(base, "child"), Err: fs.ErrNotExist}
				case "permission":
					failureErr = &fs.PathError{Op: "lstat", Path: base, Err: fs.ErrPermission}
				default:
					failureErr = merr.ErrServiceUnavailable
				}
				patch := mockey.Mock((*storage.LocalChunkManager).RemoveWithPrefix).Return(failureErr).Build()
				gc.recycleDroppedSegments(ctx, nil)
				require.NotNil(t, m.GetSegment(ctx, 2001), "failed walks must preserve the GC marker")
				require.NoError(t, cleanupRejectedCopy(ctx, task, m, copies))
				require.True(t, task.GetCleanupRequired())
				patch.UnPatch()
			}
			gc.recycleDroppedSegments(ctx, nil)
			require.Nil(t, m.GetSegment(ctx, 2001), "a never-created version-zero base is an idempotent GC no-op")
			require.Nil(t, bootMetaForRestart(t, m.catalog, 100).GetSegment(ctx, 2001))
			exists, err := m.chunkManager.Exist(ctx, sibling)
			require.NoError(t, err)
			require.True(t, exists, "GC must not match a neighboring segment ID prefix")
			require.NoError(t, cleanupRejectedCopy(ctx, task, m, copies))
			require.False(t, task.GetCleanupRequired())
		})
	}
}
