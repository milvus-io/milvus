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

package copysegment

import (
	"context"
	"io/fs"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRejectedCopyCleanupSkipsActivePublication(t *testing.T) {
	meta.MockManifestIndexSubmissions(t)
	ctx := context.Background()
	task := CreateTestCopyTask(100, 2001).(*Task)
	copies, m := NewCopySegmentTaskTestMeta(t, task)
	task.Meta, task.CopyMeta = m, copies
	im := index.CreateTestIndexMeta(t, 100, map[int64]*model.Index{
		300: {CollectionID: 100, FieldID: 101, IndexID: 300, IndexName: "vec_idx"},
	}, m.Catalog)
	m.IndexCatalog = im
	root := t.TempDir()
	m.ChunkManager = storage.NewLocalChunkManager(objectstorage.RootPath(root))
	// Publication exercises the real readback validator against a fixed inventory.
	entry := copiedManifestReviewEntry()
	entry.IndexType, entry.Properties = meta.GetIndexType(nil), nil
	entry.Path = path.Join(root, "index_files/9001/0/10/2001")
	defer mockey.Mock(packed.GetManifestIndexInfosAsync).Return([]packed.ManifestIndexInfo{entry}, nil).Build().UnPatch()
	segment := NewTestCopySegment(2001)
	segment.StorageVersion = storage.StorageV3
	segment.IsImporting = true
	require.NoError(t, m.AddSegment(ctx, segment))
	req := &datapb.CopySegmentRequest{
		StorageConfig: &indexpb.StorageConfig{RootPath: root},
		Sources:       []*datapb.CopySegmentSource{{StorageVersion: storage.StorageV3}},
		Targets:       []*datapb.CopySegmentTarget{{CollectionId: 100, PartitionId: 10, SegmentId: 2001}},
	}
	require.NoError(t, task.PrepareCopyCleanup(ctx, req))
	for _, prefix := range task.GetCleanupPrefixes() {
		require.NoError(t, m.ChunkManager.Write(ctx, prefix+"owned", []byte("owned")))
	}
	base := path.Join(root, "insert_log/100/10/2001")
	dataFile := path.Join(base, "_data/data.parquet")
	require.NoError(t, m.ChunkManager.Write(ctx, dataFile, []byte("completed worker output")))
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	var install func(*index.IndexMeta, context.Context, *model.SegmentIndex) error
	patch := mockey.Mock((*index.IndexMeta).AddSegmentIndexFromManifest).To(func(im *index.IndexMeta, ctx context.Context, index *model.SegmentIndex) error {
		close(entered)
		<-release
		return install(im, ctx, index)
	}).Origin(&install).Build()
	defer patch.UnPatch()
	finished := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		finished <- SyncCopySegmentTask(task, &datapb.QueryCopySegmentResponse{
			State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
			SegmentResults: []*datapb.CopySegmentResult{{
				SegmentId: 2001, ManifestPath: packed.MarshalManifestPath(base, 3),
				ManifestIndexRewritten: proto.Bool(true), ManifestIndexBuildIds: []int64{9001},
				IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{
					9001: {BuildId: 9001, FieldId: 101, IndexName: "vec_idx", IndexFilePaths: []string{"index.bin"}},
				},
			}},
		}, copies, m, im)
	}()
	defer func() { unblock(); <-done }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("index installation did not start")
	}
	require.True(t, task.GetCleanupRequired())
	checker := &Checker{Ctx: ctx, Meta: m, CopyMeta: copies}
	checker.CheckFailedJob(NewTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobFailed))
	inspector := &Inspector{Ctx: ctx, Meta: m, CopyMeta: copies}
	inspector.ProcessFailed(task)
	require.NoError(t, CleanupRejectedCopy(ctx, task, m, copies))
	require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, 2001).GetState())
	exists, err := m.ChunkManager.Exist(ctx, dataFile)
	require.NoError(t, err)
	require.True(t, exists, "cleanup must skip an in-flight publication")
	unblock()
	require.NoError(t, <-finished)
	// This change preserves the old late-publication behavior, but cannot delete
	// files from under it. A subsequent cleanup pass must recheck task state.
	require.NoError(t, CleanupRejectedCopy(ctx, task, m, copies))
	exists, err = m.ChunkManager.Exist(ctx, dataFile)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskCompleted, task.GetState())
}

func TestRejectedCopyCleanupDoesNotBlockDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	failed := CreateTestCopyTask(100, 2001).(*Task)
	copies, m := NewCopySegmentTaskTestMeta(t, failed)
	m.ChunkManager = storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	require.NoError(t, copies.UpdateTask(ctx, failed.GetTaskId(), UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed), UpdateCopyTaskCleanup(true), appendCopyTaskCleanupPrefixes([]string{"task-owned/"})))
	require.NoError(t, copies.AddJob(ctx, NewTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobFailed)))
	pending := CreateTestCopyTask(101, 2002).(*Task)
	pending.Task.Load().TaskId = 1002
	pending.Task.Load().JobId = 101
	pending.Task.Load().State = datapb.CopySegmentTaskState_CopySegmentTaskPending
	require.NoError(t, copies.AddTask(ctx, pending))
	require.NoError(t, copies.AddJob(ctx, NewTestCopyJob(101, datapb.CopySegmentJobState_CopySegmentJobExecuting)))
	scheduler := &CleanupEnqueueRecorder{Enqueued: make(chan int64, 1)}
	inspector := NewCopySegmentInspector(ctx, m, copies, scheduler).(*Inspector)
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
		}, copies, m, nil)
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
	case <-scheduler.Enqueued:
	default:
	}
	select {
	case <-scheduler.Enqueued:
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

func TestRejectedCopyCleanupPreservesStorageFailures(t *testing.T) {
	for _, failure := range []error{
		&fs.PathError{Op: "lstat", Path: "owned/child", Err: fs.ErrNotExist},
		&fs.PathError{Op: "lstat", Path: "owned", Err: fs.ErrPermission},
		merr.ErrServiceUnavailable,
	} {
		t.Run(failure.Error(), func(t *testing.T) {
			ctx := context.Background()
			task := CreateTestCopyTask(100, 2001).(*Task)
			copies, m := NewCopySegmentTaskTestMeta(t, task)
			m.ChunkManager = storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
			require.NoError(t, copies.UpdateTask(ctx, task.GetTaskId(), UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed), UpdateCopyTaskCleanup(true), appendCopyTaskCleanupPrefixes([]string{"owned/"})))
			patch := mockey.Mock((*storage.LocalChunkManager).RemoveWithPrefix).Return(failure).Build()
			defer patch.UnPatch()
			require.ErrorIs(t, CleanupRejectedCopy(ctx, task, m, copies), failure)
			require.True(t, task.GetCleanupRequired())
		})
	}
}
