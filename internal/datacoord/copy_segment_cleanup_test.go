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
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	dctask "github.com/milvus-io/milvus/internal/datacoord/task"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
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
			task := createTestCopyTask(100, 2001).(*copySegmentTask)
			copies, m := newCopySegmentTaskTestMeta(t, task)
			root := t.TempDir()
			m.chunkManager = storage.NewLocalChunkManager(objectstorage.RootPath(root))
			segment := newTestCopySegment(2001)
			segment.StorageVersion = storage.StorageV3
			require.NoError(t, m.AddSegment(ctx, segment))
			require.NoError(t, copies.AddJob(ctx, newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobFailed)))
			require.NoError(t, copies.UpdateTask(ctx, task.GetTaskId(), UpdateCopyTaskNodeID(42)))
			require.NoError(t, task.prepareCopyCleanup(ctx, &datapb.CopySegmentRequest{
				StorageConfig: &indexpb.StorageConfig{RootPath: root},
				Sources: []*datapb.CopySegmentSource{{StorageVersion: storage.StorageV3, IndexFiles: []*indexpb.IndexFilePathInfo{{
					BuildID: 5, IndexVersion: 1, IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
				}}}},
				Targets: []*datapb.CopySegmentTarget{{CollectionId: 100, PartitionId: 10, SegmentId: 2001, NewBuildIds: map[int64]int64{5: 6}}},
			}))
			for _, prefix := range task.GetCleanupPrefixes() {
				require.NoError(t, m.chunkManager.Write(ctx, prefix+"artifact", []byte("completed worker output")))
			}
			checker := &copySegmentChecker{ctx: ctx, meta: m, copyMeta: copies}
			checker.checkFailedJob(copies.GetJob(ctx, 100))
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
				require.NoError(t, cleanupRejectedCopy(ctx, task, m, copies))
				for _, prefix := range task.GetCleanupPrefixes() {
					exists, err := m.chunkManager.Exist(ctx, prefix+"artifact")
					require.NoError(t, err)
					require.True(t, exists)
				}
				failure.UnPatch()
			}
			if scenario.restart {
				var err error
				copies, err = NewCopySegmentMeta(ctx, m.catalog, m, nil, nil)
				require.NoError(t, err)
				task = copies.GetTask(ctx, task.GetTaskId()).(*copySegmentTask)
				require.Equal(t, taskcommon.InProgress, task.GetTaskState())
				scheduler := &cleanupEnqueueRecorder{enqueued: make(chan int64, 1)}
				inspector := &copySegmentInspector{ctx: ctx, meta: m, copyMeta: copies, scheduler: scheduler}
				inspector.reloadFromMeta()
				select {
				case id := <-scheduler.enqueued:
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
				require.Equal(t, int64(NullNodeID), task.GetNodeId())
				task.DropTaskOnWorker(lostCluster) // No RPC after confirmed worker loss.
			} else if scenario.failSave || scenario.workerRunning {
				cluster.EXPECT().QuerySlot().Return(map[int64]*session.WorkerSlots{}).Maybe()
				cluster.EXPECT().DropCopySegment(int64(42), task.GetTaskId()).Return(nil).Once()
				scheduler := dctask.NewGlobalTaskScheduler(ctx, cluster)
				scheduler.Enqueue(task)
				scheduler.Start()
				t.Cleanup(scheduler.Stop)
				require.Eventually(t, func() bool { return task.GetNodeId() == NullNodeID }, 10*time.Second, 10*time.Millisecond,
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
			recovered, err := NewCopySegmentMeta(ctx, m.catalog, m, nil, nil)
			require.NoError(t, err)
			recoveredTask := recovered.GetTask(ctx, task.GetTaskId())
			require.True(t, recoveredTask.GetCleanupRequired())
			inspector := &copySegmentInspector{ctx: ctx, meta: m, copyMeta: recovered}
			inspector.processFailed(recoveredTask)
			retireRejectedCopyTargets(t, m, recoveredTask)
			require.NoError(t, cleanupRejectedCopy(ctx, recoveredTask, m, recovered))
			for _, prefix := range recoveredTask.GetCleanupPrefixes() {
				exists, err := m.chunkManager.Exist(ctx, prefix+"artifact")
				require.NoError(t, err)
				require.False(t, exists, "task-owned index_v1 files must be deleted")
			}
			require.False(t, recoveredTask.GetCleanupRequired())
		})
	}
}

func TestRejectedCopyCleanupSkipsActivePublication(t *testing.T) {
	ctx := context.Background()
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copies, m := newCopySegmentTaskTestMeta(t, task)
	task.meta, task.copyMeta = m, copies
	root := t.TempDir()
	m.chunkManager = storage.NewLocalChunkManager(objectstorage.RootPath(root))
	segment := newTestCopySegment(2001)
	segment.StorageVersion = storage.StorageV3
	segment.IsImporting = true
	require.NoError(t, m.AddSegment(ctx, segment))
	req := &datapb.CopySegmentRequest{
		StorageConfig: &indexpb.StorageConfig{RootPath: root},
		Sources:       []*datapb.CopySegmentSource{{StorageVersion: storage.StorageV3}},
		Targets:       []*datapb.CopySegmentTarget{{CollectionId: 100, PartitionId: 10, SegmentId: 2001}},
	}
	require.NoError(t, task.prepareCopyCleanup(ctx, req))
	for _, prefix := range task.GetCleanupPrefixes() {
		require.NoError(t, m.chunkManager.Write(ctx, prefix+"owned", []byte("owned")))
	}
	base := path.Join(root, "insert_log/100/10/2001")
	dataFile := path.Join(base, "_data/data.parquet")
	require.NoError(t, m.chunkManager.Write(ctx, dataFile, []byte("completed worker output")))
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	patch := mockey.Mock(packed.GetManifestIndexInfos).To(func(string, *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
		close(entered)
		<-release
		return nil, nil
	}).Build()
	defer patch.UnPatch()
	finished := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		finished <- SyncCopySegmentTask(task, &datapb.QueryCopySegmentResponse{
			State:          datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
			SegmentResults: []*datapb.CopySegmentResult{{SegmentId: 2001, ManifestPath: packed.MarshalManifestPath(base, 3)}},
		}, copies, m)
	}()
	defer func() { unblock(); <-done }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("verification did not start")
	}
	require.True(t, task.GetCleanupRequired())
	checker := &copySegmentChecker{ctx: ctx, meta: m, copyMeta: copies}
	checker.checkFailedJob(newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobFailed))
	inspector := &copySegmentInspector{ctx: ctx, meta: m, copyMeta: copies}
	inspector.processFailed(task)
	require.NoError(t, cleanupRejectedCopy(ctx, task, m, copies))
	require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, 2001).GetState())
	exists, err := m.chunkManager.Exist(ctx, dataFile)
	require.NoError(t, err)
	require.True(t, exists, "cleanup must skip an in-flight publication")
	unblock()
	require.NoError(t, <-finished)
	// This change preserves the old late-publication behavior, but cannot delete
	// files from under it. A subsequent cleanup pass must recheck task state.
	require.NoError(t, cleanupRejectedCopy(ctx, task, m, copies))
	exists, err = m.chunkManager.Exist(ctx, dataFile)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskCompleted, task.GetState())
}

type cleanupEnqueueRecorder struct {
	dctask.GlobalScheduler
	enqueued chan int64
}

func (s *cleanupEnqueueRecorder) Enqueue(task dctask.Task) {
	select {
	case s.enqueued <- task.GetTaskID():
	default:
	}
}

func TestRejectedCopyCleanupDoesNotBlockDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	failed := createTestCopyTask(100, 2001).(*copySegmentTask)
	copies, m := newCopySegmentTaskTestMeta(t, failed)
	m.chunkManager = storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	require.NoError(t, copies.UpdateTask(ctx, failed.GetTaskId(), UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed), updateCopyTaskCleanup(true), appendCopyTaskCleanupPrefixes([]string{"task-owned/"})))
	require.NoError(t, copies.AddJob(ctx, newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobFailed)))
	pending := createTestCopyTask(101, 2002).(*copySegmentTask)
	pending.task.Load().TaskId = 1002
	pending.task.Load().JobId = 101
	pending.task.Load().State = datapb.CopySegmentTaskState_CopySegmentTaskPending
	require.NoError(t, copies.AddTask(ctx, pending))
	require.NoError(t, copies.AddJob(ctx, newTestCopyJob(101, datapb.CopySegmentJobState_CopySegmentJobExecuting)))
	scheduler := &cleanupEnqueueRecorder{enqueued: make(chan int64, 1)}
	inspector := NewCopySegmentInspector(ctx, m, copies, scheduler).(*copySegmentInspector)
	old := Params.DataCoordCfg.CopySegmentCheckInterval.SwapTempValue("0.01")
	defer Params.DataCoordCfg.CopySegmentCheckInterval.SwapTempValue(old)
	entered := make(chan struct{})
	var enterOnce sync.Once
	patch := mockey.Mock((*storage.LocalChunkManager).RemoveWithPrefix).To(func(_ *storage.LocalChunkManager, ctx context.Context, _ string) error {
		enterOnce.Do(func() { close(entered) })
		<-ctx.Done()
		return ctx.Err()
	}).Build()
	defer patch.UnPatch()
	finished := make(chan struct{})
	go func() { inspector.Start(); close(finished) }()
	defer func() { cancel(); inspector.Close(); <-finished }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("cleanup worker did not start")
	}
	// A late result must not make the scheduler wait for the cleanup worker.
	lateResult := make(chan error, 1)
	go func() {
		lateResult <- SyncCopySegmentTask(failed, &datapb.QueryCopySegmentResponse{
			State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		}, copies, m)
	}()
	select {
	case err := <-lateResult:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Error("late result blocked on cleanup storage I/O")
	}
	require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskFailed, failed.GetState())
	// Drop any enqueue from the first tick, then require another while deletion
	// remains blocked. Keep the recorder nonblocking for subsequent ticks.
	select {
	case <-scheduler.enqueued:
	default:
	}
	select {
	case <-scheduler.enqueued:
	case <-time.After(10 * time.Second):
		t.Error("cleanup blocked pending dispatch")
	}
	inspector.Close()
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("inspector did not cancel and join cleanup")
	}
	require.True(t, failed.GetCleanupRequired(), "canceled cleanup must remain retryable")
}

func TestRejectedCopyCleanupAllowsAbsentLocalPrefixes(t *testing.T) {
	ctx := context.Background()
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copies, m := newCopySegmentTaskTestMeta(t, task)
	task.meta, task.copyMeta = m, copies
	root := t.TempDir()
	m.chunkManager = storage.NewLocalChunkManager(objectstorage.RootPath(root))
	segment := newTestCopySegment(2001)
	segment.StorageVersion = storage.StorageV3
	require.NoError(t, m.AddSegment(ctx, segment))
	require.NoError(t, task.prepareCopyCleanup(ctx, &datapb.CopySegmentRequest{
		StorageConfig: &indexpb.StorageConfig{RootPath: root},
		Sources:       []*datapb.CopySegmentSource{{StorageVersion: storage.StorageV3}},
		Targets:       []*datapb.CopySegmentTarget{{CollectionId: 100, PartitionId: 10, SegmentId: 2001}},
	}))
	require.NoError(t, m.chunkManager.Write(ctx, path.Join(root, "insert_log/100/10/2001/_data/data.parquet"), []byte("data")))
	require.NoError(t, copies.UpdateTask(ctx, task.GetTaskId(), updateCopyTaskCleanup(true), UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed)))
	inspector := &copySegmentInspector{ctx: ctx, meta: m, copyMeta: copies}
	// Never delete until ordinary GC has retired the target.
	require.NoError(t, cleanupRejectedCopy(ctx, task, m, copies))
	require.True(t, task.GetCleanupRequired())
	inspector.processFailed(task)
	retireRejectedCopyTargets(t, m, task)
	require.NoError(t, cleanupRejectedCopy(ctx, task, m, copies), "unused stats/delta/BM25 prefixes must be a successful no-op")
	require.False(t, task.GetCleanupRequired())
	exists, err := m.chunkManager.Exist(ctx, path.Join(root, "insert_log/100/10/2001/_data/data.parquet"))
	require.NoError(t, err)
	require.False(t, exists)
	// A delayed completed response after cleanup (and after metadata reload) must
	// not publish the now-deleted artifact, even though the intent is cleared.
	restarted, err := NewCopySegmentMeta(ctx, m.catalog, m, nil, nil)
	require.NoError(t, err)
	err = SyncCopySegmentTask(restarted.GetTask(ctx, task.GetTaskId()), &datapb.QueryCopySegmentResponse{
		State:          datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		SegmentResults: []*datapb.CopySegmentResult{{SegmentId: 2001}},
	}, restarted, m)
	require.Error(t, err)
	require.Nil(t, m.GetSegment(ctx, 2001))
}

func retireRejectedCopyTargets(t *testing.T, m *meta, task CopySegmentTask) {
	t.Helper()
	gc := newGarbageCollector(m, newMockHandler(), GcOption{cli: m.chunkManager})
	defer gc.option.removeObjectPool.Release()
	for _, mapping := range task.GetIdMappings() {
		id := mapping.GetTargetSegmentId()
		if segment := m.GetSegment(context.Background(), id); segment != nil {
			gc.recycleDroppedSegment(context.Background(), id, segment)
		}
		require.Nil(t, m.GetSegment(context.Background(), id))
	}
}

func TestRejectedCopyCleanupPreservesStorageFailures(t *testing.T) {
	for _, failure := range []error{
		&fs.PathError{Op: "lstat", Path: "owned/child", Err: fs.ErrNotExist},
		&fs.PathError{Op: "lstat", Path: "owned", Err: fs.ErrPermission},
		merr.ErrServiceUnavailable,
	} {
		t.Run(failure.Error(), func(t *testing.T) {
			ctx := context.Background()
			task := createTestCopyTask(100, 2001).(*copySegmentTask)
			copies, m := newCopySegmentTaskTestMeta(t, task)
			m.chunkManager = storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
			require.NoError(t, copies.UpdateTask(ctx, task.GetTaskId(), UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed), updateCopyTaskCleanup(true), appendCopyTaskCleanupPrefixes([]string{"owned/"})))
			patch := mockey.Mock((*storage.LocalChunkManager).RemoveWithPrefix).Return(failure).Build()
			defer patch.UnPatch()
			require.ErrorIs(t, cleanupRejectedCopy(ctx, task, m, copies), failure)
			require.True(t, task.GetCleanupRequired())
		})
	}
}
