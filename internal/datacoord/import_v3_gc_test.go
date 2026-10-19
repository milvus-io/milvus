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
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// TestPrepareImportJobKeepsTasksDuringRetention pins that the prepare phase leaves
// unbound task records alone: they back the terminal job's row accounting
// (getImportRowsInfo) during the retention window, like V2 GC, and are removed
// by the metadata phase only.
func TestPrepareImportJobKeepsTasksDuringRetention(t *testing.T) {
	ctx := context.Background()
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, importMeta: importMeta}

	task := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: 1, TaskId: 10, State: datapb.ImportTaskStateV2_Completed, NodeId: NullNodeID, SegmentId: 100, Rows: 42,
	}, importMeta, nil, nil)

	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, State: internalpb.ImportJobState_Completed,
		CleanupTs: tsoutil.ComposeTSByTime(time.Now().Add(time.Hour)),
	}}
	importMeta.EXPECT().GetTaskByJob(mock.Anything, int64(1)).Return([]ImportTask{task}).Once()
	require.True(t, prepareImportV3JobGC(newImportV3JobContext(checker, job)))

	importMeta.EXPECT().GetTaskByJob(mock.Anything, int64(1)).Return([]ImportTask{task}).Once()
	importMeta.EXPECT().RemoveTask(mock.Anything, int64(10)).Return(nil).Once()
	importMeta.EXPECT().RemoveJob(mock.Anything, int64(1)).Return(nil).Once()
	deleteImportV3JobMeta(newImportV3JobContext(checker, job))
}

func TestImportCheckerV3CheckGCIgnoresNonTerminalJob(t *testing.T) {
	ctx := context.Background()
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, State: internalpb.ImportJobState_Importing}}

	// No catalog interaction: checkGC returns before touching tasks or objects.
	checker.checkGC(job)
}

func TestImportCheckerV3PrepareDropsBoundTask(t *testing.T) {
	ctx := context.Background()
	importMeta := NewMockImportMeta(t)
	cluster := session.NewMockCluster(t)
	checker := &importCheckerV3{ctx: ctx, importMeta: importMeta, cluster: cluster}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, State: internalpb.ImportJobState_Failed}}
	task := newReshardTask(&datapb.ReshardTask{
		JobId: 1, TaskId: 10, State: datapb.ImportTaskStateV2_InProgress, NodeId: 5,
	}, importMeta, nil, nil)

	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything).Return([]ImportTask{task}).Once()
	// The GC loop re-issues a version-aware best-effort Drop every tick so a
	// transient RPC failure or a lost node cannot pin the job forever.
	cluster.EXPECT().DropReshard(int64(5), mock.Anything).Return(nil).Once()
	importMeta.EXPECT().UpdateTask(mock.Anything, int64(10), mock.Anything).Return(nil).Once()

	require.False(t, prepareImportV3JobGC(newImportV3JobContext(checker, job)))
}

func TestImportCheckerV3PrepareDropsFailedTaskSegments(t *testing.T) {
	ctx := context.Background()
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	segment, err := addImportSegment(ctx, meta, 100, 1, 10, 2, 3, "v0", datapb.SegmentLevel_L1, 1, 4)
	require.NoError(t, err)
	require.Equal(t, commonpb.SegmentState_Importing, segment.GetState())
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, meta: meta, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, State: internalpb.ImportJobState_Failed}}
	task := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: 1, TaskId: 10, State: datapb.ImportTaskStateV2_Completed, NodeId: NullNodeID, SegmentId: 100,
	}, importMeta, meta, nil)

	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything).Return([]ImportTask{task}).Once()

	require.True(t, prepareImportV3JobGC(newImportV3JobContext(checker, job)))
	require.Equal(t, commonpb.SegmentState_Dropped, meta.GetSegment(ctx, 100).GetState())
}

// TestImportCheckerV3CheckGCReleasesObjectsBeforeCleanup pins the split between
// the object phase and the metadata phase: the object prefix goes away on the
// first tick whose prepare phase finds no bound task, while the job and its task
// records keep their retention window (idempotency, progress row accounting).
func TestImportCheckerV3CheckGCReleasesObjectsBeforeCleanup(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, meta: &meta{chunkManager: cm}, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, State: internalpb.ImportJobState_Failed, AutoCommit: true,
		CleanupTs: tsoutil.ComposeTSByTime(time.Now().Add(time.Hour)),
	}}

	planPath := path.Join(cm.RootPath(), metautil.BuildImportReshardResultPath(1, 10, 1))
	require.NoError(t, cm.Write(ctx, planPath, []byte("plan")))

	// No task is bound, so this tick releases the prefix; the retention window
	// still keeps the job. RemoveJob is un-mocked on purpose: a call would fail
	// the test.
	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything).Return(nil).Once()
	checker.checkGC(job)
	exist, err := cm.Exist(ctx, planPath)
	require.NoError(t, err)
	require.False(t, exist)
	require.True(t, job.GetObjCleaned())
}

// TestImportCheckerV3CheckGCHoldsObjectsWhileTaskBound pins that the object
// phase waits for every task to be unbound: the tick that drops a task does not
// touch the prefix.
func TestImportCheckerV3CheckGCHoldsObjectsWhileTaskBound(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	importMeta := NewMockImportMeta(t)
	cluster := session.NewMockCluster(t)
	checker := &importCheckerV3{ctx: ctx, meta: &meta{chunkManager: cm}, importMeta: importMeta, cluster: cluster}
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, State: internalpb.ImportJobState_Failed, AutoCommit: true,
		CleanupTs: tsoutil.ComposeTSByTime(time.Now().Add(time.Hour)),
	}}
	task := newReshardTask(&datapb.ReshardTask{
		JobId: 1, TaskId: 10, State: datapb.ImportTaskStateV2_InProgress, NodeId: 5,
	}, importMeta, nil, nil)

	planPath := path.Join(cm.RootPath(), metautil.BuildImportReshardResultPath(1, 10, 1))
	require.NoError(t, cm.Write(ctx, planPath, []byte("plan")))

	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything).Return([]ImportTask{task}).Once()
	cluster.EXPECT().DropReshard(int64(5), mock.Anything).Return(nil).Once()
	importMeta.EXPECT().UpdateTask(mock.Anything, int64(10), mock.Anything).Return(nil).Once()
	checker.checkGC(job)

	exist, err := cm.Exist(ctx, planPath)
	require.NoError(t, err)
	require.True(t, exist)
	require.False(t, job.GetObjCleaned())

	// Once the drop has cleared the binding, the next tick releases the prefix.
	task.task.Load().NodeId = NullNodeID
	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything).Return([]ImportTask{task}).Once()
	checker.checkGC(job)
	exist, err = cm.Exist(ctx, planPath)
	require.NoError(t, err)
	require.False(t, exist)
}

func TestImportCheckerV3CheckGCDeletesPastCleanup(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, meta: &meta{chunkManager: cm}, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, State: internalpb.ImportJobState_Failed, AutoCommit: true,
		CleanupTs: tsoutil.ComposeTSByTime(time.Now().Add(-time.Hour)),
	}}

	planPath := path.Join(cm.RootPath(), metautil.BuildImportReshardResultPath(1, 10, 1))
	require.NoError(t, cm.Write(ctx, planPath, []byte("plan")))
	// One tick removes the prefix and, because cleanupTs has passed, the
	// metadata too (prepare and metadata task lookups).
	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything).Return(nil).Twice()
	importMeta.EXPECT().RemoveJob(mock.Anything, int64(1)).Return(nil).Once()

	checker.checkGC(job)
	exist, err := cm.Exist(ctx, planPath)
	require.NoError(t, err)
	require.False(t, exist) // prefix removed
}

func TestImportCheckerV3RollbackGateBlocksDelete(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, meta: &meta{chunkManager: cm}, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, State: internalpb.ImportJobState_Failed, AutoCommit: false,
		CleanupTs: tsoutil.ComposeTSByTime(time.Now().Add(-time.Hour)),
	}}
	rollbackCalled := false
	checker.hooks.rollbackImport = func(ctx context.Context, j ImportJob) error {
		rollbackCalled = true
		return merr.WrapErrServiceUnavailable("transient")
	}
	checker.hooks.getReplicationRole = func(ctx context.Context) (replicateutil.Role, bool, error) {
		return replicateutil.RolePrimary, true, nil
	}

	planPath := path.Join(cm.RootPath(), metautil.BuildImportReshardResultPath(1, 10, 1))
	require.NoError(t, cm.Write(ctx, planPath, []byte("plan")))

	// The tick releases the objects even though the rollback broadcast fails
	// transiently: the prefix is local and no peer is involved. The job stays.
	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything).Return(nil).Once()
	checker.checkGC(job)
	require.True(t, rollbackCalled)
	exist, err := cm.Exist(ctx, planPath)
	require.NoError(t, err)
	require.False(t, exist)

	// Next tick rollback succeeds; metadata delete proceeds.
	checker.hooks.rollbackImport = func(ctx context.Context, j ImportJob) error { return nil }
	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything).Return(nil).Twice()
	importMeta.EXPECT().RemoveJob(mock.Anything, int64(1)).Return(nil).Once()
	checker.checkGC(job)
}
