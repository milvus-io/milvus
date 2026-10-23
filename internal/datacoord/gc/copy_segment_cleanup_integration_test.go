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

package gc

import (
	"context"
	"path"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/copysegment"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	dctask "github.com/milvus-io/milvus/internal/datacoord/task"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRejectedCopyCleanupLateCompletion(t *testing.T) {
	for _, scenario := range []struct {
		name          string
		failSave      bool
		restart       bool
		workerLost    bool
		workerRunning bool
	}{
		{name: "late completed result"},
		{name: "retry admission", failSave: true},
		{name: "restart before admission", failSave: true, restart: true},
		{name: "restart while worker running", restart: true, workerRunning: true},
		{name: "worker lost before retry", failSave: true, restart: true, workerLost: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx := context.Background()
			task := copysegment.CreateTestCopyTask(100, 2001).(*copysegment.Task)
			copies, m := copysegment.NewCopySegmentTaskTestMeta(t, task)
			root := t.TempDir()
			m.ChunkManager = storage.NewLocalChunkManager(objectstorage.RootPath(root))
			segment := copysegment.NewTestCopySegment(2001)
			segment.StorageVersion = storage.StorageV3
			require.NoError(t, m.AddSegment(ctx, segment))
			require.NoError(t, copies.AddJob(ctx, copysegment.NewTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobFailed)))
			require.NoError(t, copies.UpdateTask(ctx, task.GetTaskId(), copysegment.UpdateCopyTaskNodeID(42)))
			require.NoError(t, task.PrepareCopyCleanup(ctx, &datapb.CopySegmentRequest{
				StorageConfig: &indexpb.StorageConfig{RootPath: root},
				Sources: []*datapb.CopySegmentSource{{StorageVersion: storage.StorageV3, IndexFiles: []*indexpb.IndexFilePathInfo{{
					BuildID: 5, IndexVersion: 1, IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
				}}}},
				Targets: []*datapb.CopySegmentTarget{{CollectionId: 100, PartitionId: 10, SegmentId: 2001, NewBuildIds: map[int64]int64{5: 6}}},
			}))
			for _, prefix := range task.GetCleanupPrefixes() {
				require.NoError(t, m.ChunkManager.Write(ctx, prefix+"artifact", []byte("completed worker output")))
			}
			checker := &copysegment.Checker{Ctx: ctx, Meta: m, CopyMeta: copies}
			checker.CheckFailedJob(copies.GetJob(ctx, 100))
			require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskFailed, task.GetState())
			require.False(t, task.GetCleanupRequired())
			cluster := session.NewMockCluster(t)
			cluster.EXPECT().QueryCopySegment(int64(42), mock.Anything).Return(&datapb.QueryCopySegmentResponse{
				State:          datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
				SegmentResults: []*datapb.CopySegmentResult{{SegmentId: 2001}},
			}, nil)
			if scenario.failSave {
				failure := mockey.Mock((*kvdatacoord.Catalog).SaveCopySegmentTask).Return(merr.ErrServiceUnavailable).Build()
				t.Cleanup(func() { failure.UnPatch() })
				task.QueryTaskOnWorker(cluster)
				require.False(t, task.GetCleanupRequired())
				require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskFailed, task.GetState())
				require.Equal(t, taskcommon.InProgress, task.GetTaskState(), "scheduler must retain the task for another query")
				require.NoError(t, copysegment.CleanupRejectedCopy(ctx, task, m, copies))
				for _, prefix := range task.GetCleanupPrefixes() {
					exists, err := m.ChunkManager.Exist(ctx, prefix+"artifact")
					require.NoError(t, err)
					require.True(t, exists)
				}
				failure.UnPatch()
			}
			if scenario.restart {
				var err error
				copies, err = copysegment.NewCopySegmentMeta(ctx, m.Catalog, m, nil, nil, nil)
				require.NoError(t, err)
				task = copies.GetTask(ctx, task.GetTaskId()).(*copysegment.Task)
				require.Equal(t, taskcommon.InProgress, task.GetTaskState())
				scheduler := &copysegment.CleanupEnqueueRecorder{Enqueued: make(chan int64, 1)}
				inspector := &copysegment.Inspector{Ctx: ctx, Meta: m, CopyMeta: copies, Scheduler: scheduler}
				inspector.ReloadFromMeta()
				select {
				case id := <-scheduler.Enqueued:
					require.Equal(t, task.GetTaskId(), id)
				default:
					t.Fatal("restart must re-enqueue the failed task for cleanup admission")
				}
				transientCluster := session.NewMockCluster(t)
				transientCluster.EXPECT().QueryCopySegment(int64(42), mock.Anything).Return(nil, merr.ErrServiceUnavailable)
				task.QueryTaskOnWorker(transientCluster)
				require.Equal(t, taskcommon.InProgress, task.GetTaskState())
				require.False(t, task.GetCleanupRequired(), "transient query failures must not authorize deletion")
			}
			if scenario.workerRunning {
				for _, state := range []datapb.CopySegmentTaskState{
					datapb.CopySegmentTaskState_CopySegmentTaskPending,
					datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
				} {
					runningCluster := session.NewMockCluster(t)
					runningCluster.EXPECT().QueryCopySegment(int64(42), mock.Anything).Return(
						&datapb.QueryCopySegmentResponse{State: state}, nil).Once()
					task.QueryTaskOnWorker(runningCluster)
					require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskFailed, task.GetState())
					require.Equal(t, taskcommon.InProgress, task.GetTaskState(), "nonterminal worker response must retain cleanup polling")
					require.EqualValues(t, 42, task.GetNodeId())
					require.False(t, task.GetCleanupRequired(), "a running worker has not transferred cleanup responsibility")
				}
			}
			if scenario.workerLost {
				lostCluster := session.NewMockCluster(t)
				lostCluster.EXPECT().QueryCopySegment(int64(42), mock.Anything).Return(nil, merr.WrapErrNodeNotFound(42))
				failure := mockey.Mock((*kvdatacoord.Catalog).SaveCopySegmentTask).Return(merr.ErrServiceUnavailable).Build()
				t.Cleanup(func() { failure.UnPatch() })
				task.QueryTaskOnWorker(lostCluster)
				require.Equal(t, taskcommon.InProgress, task.GetTaskState())
				require.Equal(t, int64(42), task.GetNodeId())
				require.False(t, task.GetCleanupRequired())
				failure.UnPatch()
				task.QueryTaskOnWorker(lostCluster)
				require.Equal(t, int64(meta.NullNodeID), task.GetNodeId())
				task.DropTaskOnWorker(lostCluster) // No RPC after confirmed worker loss.
			} else if scenario.failSave || scenario.workerRunning {
				cluster.EXPECT().QuerySlot().Return(map[int64]*session.WorkerSlots{}).Maybe()
				cluster.EXPECT().DropCopySegment(int64(42), task.GetTaskId()).Return(nil).Once()
				scheduler := dctask.NewGlobalTaskScheduler(ctx, cluster)
				scheduler.Enqueue(task)
				scheduler.Start()
				t.Cleanup(scheduler.Stop)
				require.Eventually(t, func() bool { return task.GetNodeId() == meta.NullNodeID }, 10*time.Second, 10*time.Millisecond,
					"scheduler must retry cleanup admission before releasing the completed worker task")
				scheduler.Stop()
			} else {
				task.QueryTaskOnWorker(cluster)
			}
			require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskFailed, task.GetState())
			require.Equal(t, taskcommon.Failed, task.GetTaskState())
			require.True(t, task.GetCleanupRequired())
			require.Equal(t, commonpb.SegmentState_Importing, m.GetSegment(ctx, 2001).GetState(), "late result must not publish")
			// Reload the durable intent before exercising deletion.
			recovered, err := copysegment.NewCopySegmentMeta(ctx, m.Catalog, m, nil, nil, nil)
			require.NoError(t, err)
			recoveredTask := recovered.GetTask(ctx, task.GetTaskId())
			require.True(t, recoveredTask.GetCleanupRequired())
			inspector := &copysegment.Inspector{Ctx: ctx, Meta: m, CopyMeta: recovered}
			inspector.ProcessFailed(recoveredTask)
			retireRejectedCopyTargets(t, m, recoveredTask)
			require.NoError(t, copysegment.CleanupRejectedCopy(ctx, recoveredTask, m, recovered))
			for _, prefix := range recoveredTask.GetCleanupPrefixes() {
				exists, err := m.ChunkManager.Exist(ctx, prefix+"artifact")
				require.NoError(t, err)
				require.False(t, exists, "task-owned index_v1 files must be deleted")
			}
			require.False(t, recoveredTask.GetCleanupRequired())
		})
	}
}

func TestRejectedCopyCleanupAllowsAbsentLocalPrefixes(t *testing.T) {
	ctx := context.Background()
	task := copysegment.CreateTestCopyTask(100, 2001).(*copysegment.Task)
	copies, m := copysegment.NewCopySegmentTaskTestMeta(t, task)
	task.Meta, task.CopyMeta = m, copies
	root := t.TempDir()
	m.ChunkManager = storage.NewLocalChunkManager(objectstorage.RootPath(root))
	segment := copysegment.NewTestCopySegment(2001)
	segment.StorageVersion = storage.StorageV3
	require.NoError(t, m.AddSegment(ctx, segment))
	require.NoError(t, task.PrepareCopyCleanup(ctx, &datapb.CopySegmentRequest{
		StorageConfig: &indexpb.StorageConfig{RootPath: root},
		Sources:       []*datapb.CopySegmentSource{{StorageVersion: storage.StorageV3}},
		Targets:       []*datapb.CopySegmentTarget{{CollectionId: 100, PartitionId: 10, SegmentId: 2001}},
	}))
	require.NoError(t, m.ChunkManager.Write(ctx, path.Join(root, "insert_log/100/10/2001/_data/data.parquet"), []byte("data")))
	require.NoError(t, copies.UpdateTask(ctx, task.GetTaskId(), copysegment.UpdateCopyTaskCleanup(true), copysegment.UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed)))
	inspector := &copysegment.Inspector{Ctx: ctx, Meta: m, CopyMeta: copies}
	// Never delete until ordinary GC has retired the target.
	require.NoError(t, copysegment.CleanupRejectedCopy(ctx, task, m, copies))
	require.True(t, task.GetCleanupRequired())
	inspector.ProcessFailed(task)
	retireRejectedCopyTargets(t, m, task)
	require.NoError(t, copysegment.CleanupRejectedCopy(ctx, task, m, copies), "unused stats/delta/BM25 prefixes must be a successful no-op")
	require.False(t, task.GetCleanupRequired())
	exists, err := m.ChunkManager.Exist(ctx, path.Join(root, "insert_log/100/10/2001/_data/data.parquet"))
	require.NoError(t, err)
	require.False(t, exists)
	// A delayed completed response after cleanup (and after metadata reload) must
	// not publish the now-deleted artifact, even though the intent is cleared.
	restarted, err := copysegment.NewCopySegmentMeta(ctx, m.Catalog, m, nil, nil, nil)
	require.NoError(t, err)
	err = copysegment.SyncCopySegmentTask(restarted.GetTask(ctx, task.GetTaskId()), &datapb.QueryCopySegmentResponse{
		State:          datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		SegmentResults: []*datapb.CopySegmentResult{{SegmentId: 2001}},
	}, restarted, m, nil)
	require.Error(t, err)
	require.Nil(t, m.GetSegment(ctx, 2001))
}

func retireRejectedCopyTargets(t *testing.T, m *meta.Meta, task copysegment.CopySegmentTask) {
	t.Helper()
	gc := NewGarbageCollector(m, meta.NewMockHandler(), GcOption{Cli: m.ChunkManager})
	defer gc.Option.RemoveObjectPool.Release()
	for _, mapping := range task.GetIdMappings() {
		id := mapping.GetTargetSegmentId()
		if segment := m.GetSegment(context.Background(), id); segment != nil {
			gc.RecycleDroppedSegment(context.Background(), NewGCManifestReadIO(t), id, segment)
		}
		require.Nil(t, m.GetSegment(context.Background(), id))
	}
}
