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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	importv3pb "github.com/milvus-io/milvus/pkg/v3/proto/importv3pb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// TestImportTaskV3PrepareRetryRecordsOldSegmentInOneWrite pins the new retry
// contract: a retry allocates only the new run's identity (a fresh segment id
// and log range) and persists it in ONE task write that also records the
// superseded segment id in old_segment_ids. It creates and drops no segment
// record -- the next dispatch drops the recorded old segment and creates the
// new one.
func TestImportTaskV3PrepareRetryRecordsOldSegmentInOneWrite(t *testing.T) {
	ctx := context.Background()
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	importMeta := NewMockImportMeta(t)
	cluster := session.NewMockCluster(t)
	alloc := allocator.NewMockAllocator(t)

	// The superseded segment exists (created by the previous run's dispatch).
	_, err = addImportSegment(ctx, meta, 100, 1, 10, 2, 3, "v0", datapb.SegmentLevel_L1, 1, 4)
	require.NoError(t, err)

	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, CollectionID: 2, State: internalpb.ImportJobState_Importing, DataTs: 1, Schema: retryTestSchema,
	}}
	task := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_InProgress,
		RunId: 1, NodeId: 5, SegmentId: 100, OldSegmentIds: []int64{77},
		LogRange: &datapb.IDRange{Begin: 1000, End: 2000},
	}, importMeta, meta, alloc)

	importMeta.EXPECT().GetJob(mock.Anything, int64(1)).Return(job).Once()
	cluster.EXPECT().DropImportV3(int64(5), mock.Anything).Return(nil).Once()
	alloc.EXPECT().AllocID(mock.Anything).Return(int64(200), nil).Once()
	alloc.EXPECT().AllocN(mock.Anything).Return(int64(5000), int64(6000), nil).Once()

	var updates int
	// Exactly one task write, carrying the new run/segment/log range, the old
	// segment id appended to old_segment_ids, and the reset state/node/reason.
	importMeta.EXPECT().UpdateTask(mock.Anything, int64(10),
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).RunAndReturn(func(_ context.Context, _ int64, actions ...UpdateAction) error {
		updates++
		clone := task.Clone()
		for _, action := range actions {
			action(clone)
		}
		task.task.Store(clone.(*importTaskV3).task.Load())
		return nil
	}).Once()

	task.prepareRetry(cluster)

	require.Equal(t, 1, updates, "prepareRetry must issue exactly one task write")
	p := task.task.Load()
	require.Equal(t, int64(2), p.GetRunId())
	require.Equal(t, int64(200), p.GetSegmentId())
	require.Equal(t, []int64{77, 100}, p.GetOldSegmentIds())
	require.Equal(t, int64(5000), p.GetLogRange().GetBegin())
	require.Equal(t, int64(6000), p.GetLogRange().GetEnd())
	require.Equal(t, datapb.ImportTaskStateV2_Pending, p.GetState())
	require.Equal(t, int64(NullNodeID), p.GetNodeId())
	// No segment record was created for the new run, and the superseded segment
	// was not dropped here: the next dispatch owns both.
	require.Nil(t, meta.GetSegment(ctx, 200))
	require.Equal(t, commonpb.SegmentState_Importing, meta.GetSegment(ctx, 100).GetState())
}

// TestImportTaskV3PrepareRetrySurvivesTransientAllocN pins that a transient
// AllocN failure (mixCoord RPC timeout) does not fail the task or the job: V2
// parity counts the attempt and retries on the next tick. A retry creates no
// segment record, so there is nothing to roll back and no leak across rounds.
func TestImportTaskV3PrepareRetrySurvivesTransientAllocN(t *testing.T) {
	ctx := context.Background()
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	importMeta := NewMockImportMeta(t)
	cluster := session.NewMockCluster(t)
	alloc := allocator.NewMockAllocator(t)

	_, err = addImportSegment(ctx, meta, 100, 1, 10, 2, 3, "v0", datapb.SegmentLevel_L1, 1, 4)
	require.NoError(t, err)

	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, CollectionID: 2, State: internalpb.ImportJobState_Importing, DataTs: 1, Schema: retryTestSchema,
	}}
	task := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_InProgress,
		RunId: 1, NodeId: 5, SegmentId: 100, LogRange: &datapb.IDRange{Begin: 1000, End: 2000},
	}, importMeta, meta, alloc)

	importMeta.EXPECT().GetJob(mock.Anything, int64(1)).Return(job).Once()
	cluster.EXPECT().DropImportV3(int64(5), mock.Anything).Return(nil).Once()
	alloc.EXPECT().AllocID(mock.Anything).Return(int64(200), nil).Once()
	alloc.EXPECT().AllocN(mock.Anything).Return(int64(0), int64(0), errRetryUpdate).Once()

	// t.fail must never run: no UpdateTask(UpdateState(Failed)) and no
	// UpdateJob(UpdateJobState(Failed)) expectations are registered, so any
	// fail call fails the test with an unexpected mock call.

	task.prepareRetry(cluster)

	// Task untouched (same run/segment, still InProgress). No segment record was
	// created for the new run, so no rollback was needed; the old segment
	// survives for the next tick.
	p := task.task.Load()
	require.Equal(t, int64(1), p.GetRunId())
	require.Equal(t, int64(100), p.GetSegmentId())
	require.Equal(t, datapb.ImportTaskStateV2_InProgress, p.GetState())
	require.Nil(t, meta.GetSegment(ctx, 200))
	require.Equal(t, commonpb.SegmentState_Importing, meta.GetSegment(ctx, 100).GetState())
	require.Equal(t, internalpb.ImportJobState_Importing, job.GetState())
}

// TestImportTaskV3PrepareRetrySurvivesTransientRepoint pins that a failed
// (possibly timed-out-but-committed) repoint write does not fail the job. The
// retry creates no segment record, so there is nothing to roll back; the next
// tick re-enters prepareRetry through the query path.
func TestImportTaskV3PrepareRetrySurvivesTransientRepoint(t *testing.T) {
	ctx := context.Background()
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	importMeta := NewMockImportMeta(t)
	cluster := session.NewMockCluster(t)
	alloc := allocator.NewMockAllocator(t)

	_, err = addImportSegment(ctx, meta, 100, 1, 10, 2, 3, "v0", datapb.SegmentLevel_L1, 1, 4)
	require.NoError(t, err)

	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, CollectionID: 2, State: internalpb.ImportJobState_Importing, DataTs: 1, Schema: retryTestSchema,
	}}
	task := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_InProgress,
		RunId: 1, NodeId: NullNodeID, SegmentId: 100, LogRange: &datapb.IDRange{Begin: 1000, End: 2000},
	}, importMeta, meta, alloc)

	importMeta.EXPECT().GetJob(mock.Anything, int64(1)).Return(job).Once()
	alloc.EXPECT().AllocID(mock.Anything).Return(int64(200), nil).Once()
	alloc.EXPECT().AllocN(mock.Anything).Return(int64(5000), int64(6000), nil).Once()
	importMeta.EXPECT().UpdateTask(mock.Anything, int64(10),
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(errRetryUpdate).Once()

	task.prepareRetry(cluster)

	p := task.task.Load()
	require.Equal(t, int64(1), p.GetRunId())
	require.Equal(t, int64(100), p.GetSegmentId())
	require.Equal(t, datapb.ImportTaskStateV2_InProgress, p.GetState())
	// No segment record was created for the new run, so the unknown-outcome
	// write leaves nothing to clean up; the old segment still backs the task.
	require.Nil(t, meta.GetSegment(ctx, 200))
	require.Equal(t, commonpb.SegmentState_Importing, meta.GetSegment(ctx, 100).GetState())
	require.Equal(t, internalpb.ImportJobState_Importing, job.GetState())
}

var errRetryUpdate = errors.New("update task failed")

// retryTestSchema gives the retry and dispatch re-derivations a valid target
// schema; production jobs always carry one, but the tests construct minimal jobs.
var retryTestSchema = &schemapb.CollectionSchema{
	Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	},
}

// TestImportTaskV3CreateTaskOnWorkerCreatesSegmentBeforeRunning pins the new
// dispatch order: the reserved segment record is created before the task is
// bound InProgress and before the Create RPC, then reused on a later dispatch.
// The later dispatch also drops the superseded segments recorded in
// old_segment_ids and clears the list in the same task write.
func TestImportTaskV3CreateTaskOnWorkerCreatesSegmentBeforeRunning(t *testing.T) {
	ctx := context.Background()
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	importMeta := NewMockImportMeta(t)
	cluster := session.NewMockCluster(t)
	alloc := allocator.NewMockAllocator(t)

	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, CollectionID: 2, State: internalpb.ImportJobState_Importing, DataTs: 1, Schema: retryTestSchema,
	}}
	task := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_Pending,
		RunId: 1, NodeId: NullNodeID, SegmentId: 100, PartitionId: 3, Vchannel: "v0",
		LogRange: &datapb.IDRange{Begin: 1000, End: 2000},
		Rows:     5, Fragments: []*importv3pb.FragmentRef{{Path: "f0", Rows: 5}},
	}, importMeta, meta, alloc)

	importMeta.EXPECT().GetJob(mock.Anything, int64(1)).Return(job).Maybe()
	// First dispatch: the segment does not exist yet, so it must be created
	// before the InProgress write and the Create RPC.
	importMeta.EXPECT().UpdateTask(mock.Anything, int64(10),
		mock.Anything, mock.Anything, mock.Anything,
	).RunAndReturn(func(_ context.Context, _ int64, actions ...UpdateAction) error {
		require.NotNil(t, meta.GetSegment(ctx, 100), "segment must exist before the InProgress write")
		for _, action := range actions {
			action(task)
		}
		return nil
	}).Once()
	cluster.EXPECT().CreateImportV3(int64(7), mock.Anything, int64(2)).Return(nil).Once()

	task.CreateTaskOnWorker(7, cluster)

	seg := meta.GetSegment(ctx, 100)
	require.NotNil(t, seg)
	require.True(t, seg.GetIsImporting())
	require.Equal(t, commonpb.SegmentState_Importing, seg.GetState())
	require.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
	require.Equal(t, int64(7), task.GetNodeID())
	require.Empty(t, task.task.Load().GetOldSegmentIds())

	// Second dispatch: the reserved segment already exists (reused, not
	// recreated) and a superseded segment recorded in old_segment_ids is
	// dropped, with the list cleared in the same write.
	_, err = addImportSegment(ctx, meta, 101, 1, 10, 2, 3, "v0", datapb.SegmentLevel_L1, 1, 4)
	require.NoError(t, err)
	// A marker that a re-create would reset to 0: reuse must preserve it.
	seg.NumOfRows = 7
	p := task.task.Load()
	p.State = datapb.ImportTaskStateV2_Pending
	p.NodeId = NullNodeID
	p.OldSegmentIds = []int64{101}

	importMeta.EXPECT().UpdateTask(mock.Anything, int64(10),
		mock.Anything, mock.Anything, mock.Anything,
	).RunAndReturn(func(_ context.Context, _ int64, actions ...UpdateAction) error {
		for _, action := range actions {
			action(task)
		}
		return nil
	}).Once()
	cluster.EXPECT().CreateImportV3(int64(8), mock.Anything, int64(2)).Return(nil).Once()

	task.CreateTaskOnWorker(8, cluster)

	require.Equal(t, int64(7), meta.GetSegment(ctx, 100).GetNumOfRows(), "existing segment must be reused, not recreated")
	require.Equal(t, commonpb.SegmentState_Dropped, meta.GetSegment(ctx, 101).GetState())
	require.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
	require.Equal(t, int64(8), task.GetNodeID())
	require.Empty(t, task.task.Load().GetOldSegmentIds())
}

// TestImportTaskV3CreateTaskOnWorkerUsesPlanStorageVersion pins that the
// dispatch-created segment takes the storage version of THIS dispatch's plan
// writer spec (the same config read that produced the plan), so a useLoonFFI
// flip between Planning and dispatch cannot make the record disagree with the
// written layout.
func TestImportTaskV3CreateTaskOnWorkerUsesPlanStorageVersion(t *testing.T) {
	ctx := context.Background()
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	importMeta := NewMockImportMeta(t)
	cluster := session.NewMockCluster(t)
	alloc := allocator.NewMockAllocator(t)

	paramtable.Get().Save("common.storage.useLoonFFI", "true")
	defer paramtable.Get().Reset("common.storage.useLoonFFI")

	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, CollectionID: 2, State: internalpb.ImportJobState_Importing, DataTs: 1, Schema: retryTestSchema,
	}}
	task := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_Pending,
		RunId: 1, NodeId: NullNodeID, SegmentId: 100, PartitionId: 3, Vchannel: "v0",
		LogRange: &datapb.IDRange{Begin: 1000, End: 2000},
		Rows:     5, Fragments: []*importv3pb.FragmentRef{{Path: "f0", Rows: 5}},
	}, importMeta, meta, alloc)

	importMeta.EXPECT().GetJob(mock.Anything, int64(1)).Return(job).Maybe()
	importMeta.EXPECT().UpdateTask(mock.Anything, int64(10), mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	cluster.EXPECT().CreateImportV3(int64(7), mock.Anything, int64(2)).Return(nil).Once()

	task.CreateTaskOnWorker(7, cluster)

	require.Equal(t, storage.StorageV3, meta.GetSegment(ctx, 100).GetStorageVersion())
}

// TestImportTaskV3AcceptanceNotifiesIndexBuild pins the per-segment index
// wakeup: accepting a Completed worker result persists the segment as
// Flushed+IsSorted and must push it to the indexInspector's build channel in
// the same tick. Without the push, a burst of finishing import tasks leaves
// every accepted segment waiting for the next TaskCheckInterval (60s) scan,
// which makes index building effectively start only after the whole merge
// stage (V2 parity: postFlush and mixCompaction push each finished segment).
// A zero-row placeholder is dropped at acceptance and must NOT be pushed:
// the buildIndexCh consumer does not re-check the segment state.
func TestImportTaskV3AcceptanceNotifiesIndexBuild(t *testing.T) {
	ctx := context.Background()
	drainBuildIndexCh()
	defer drainBuildIndexCh()

	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	meta.AddCollection(&collectionInfo{ID: 2, Schema: retryTestSchema})
	importMeta := NewMockImportMeta(t)
	cluster := session.NewMockCluster(t)

	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, CollectionID: 2, State: internalpb.ImportJobState_Importing, DataTs: 1, Schema: retryTestSchema,
	}}

	// Non-empty result: accepted segment is Flushed and pushed.
	_, err = addImportSegment(ctx, meta, 100, 1, 10, 2, 3, "v0", datapb.SegmentLevel_L1, storage.StorageV2, 0)
	require.NoError(t, err)
	task := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_InProgress,
		RunId: 1, NodeId: 5, SegmentId: 100, LogRange: &datapb.IDRange{Begin: 1000, End: 2000},
	}, importMeta, meta, nil)

	cluster.EXPECT().QueryImportV3(int64(5), mock.Anything).Return(&datapb.QueryImportTaskV3Response{
		Status: merr.Success(), State: datapb.ImportTaskStateV2_Completed,
		Segments: []*datapb.ImportTaskV3Result{{
			Rows:       10,
			Statistics: &datapb.Statistics{TimestampFrom: 10, TimestampTo: 20},
		}},
	}, nil).Once()
	// One GetJob for the Completed-race guard, one inside acceptResult.
	importMeta.EXPECT().GetJob(mock.Anything, int64(1)).Return(job).Twice()
	importMeta.EXPECT().UpdateTask(mock.Anything, int64(10), mock.Anything).Return(nil).Once()

	task.QueryTaskOnWorker(cluster)

	seg := meta.GetSegment(ctx, 100)
	require.Equal(t, commonpb.SegmentState_Flushed, seg.GetState())
	require.True(t, seg.GetIsSorted())
	require.Equal(t, []int64{100}, drainBuildIndexCh())

	// Zero-row result: placeholder dropped at acceptance, nothing pushed.
	_, err = addImportSegment(ctx, meta, 101, 1, 11, 2, 3, "v0", datapb.SegmentLevel_L1, storage.StorageV2, 0)
	require.NoError(t, err)
	task2 := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: 1, TaskId: 11, CollectionId: 2, State: datapb.ImportTaskStateV2_InProgress,
		RunId: 1, NodeId: 5, SegmentId: 101, LogRange: &datapb.IDRange{Begin: 2000, End: 3000},
	}, importMeta, meta, nil)
	cluster.EXPECT().QueryImportV3(int64(5), mock.Anything).Return(&datapb.QueryImportTaskV3Response{
		Status: merr.Success(), State: datapb.ImportTaskStateV2_Completed,
		Segments: []*datapb.ImportTaskV3Result{{Rows: 0}},
	}, nil).Once()
	importMeta.EXPECT().GetJob(mock.Anything, int64(1)).Return(job).Twice()
	importMeta.EXPECT().UpdateTask(mock.Anything, int64(11), mock.Anything).Return(nil).Once()

	task2.QueryTaskOnWorker(cluster)

	require.Equal(t, commonpb.SegmentState_Dropped, meta.GetSegment(ctx, 101).GetState())
	require.Empty(t, drainBuildIndexCh())
}

func TestReconcileOrphanImportSegments(t *testing.T) {
	ctx := context.Background()
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListReshardTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportTaskV3(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Maybe()

	importMeta, err := NewImportMeta(ctx, catalog, nil, meta)
	require.NoError(t, err)

	// Referenced by a V3 task -> kept. V3 creates its segment at dispatch for a
	// task that already carries its id, so it never orphans one, but the pass
	// must still count the referenced id or it would sweep an in-flight segment.
	_, err = addImportSegment(ctx, meta, 100, 1, 10, 2, 3, "v0", datapb.SegmentLevel_L1, 1, 4)
	require.NoError(t, err)
	// Referenced by a V2 task -> kept.
	_, err = addImportSegment(ctx, meta, 101, 1, 11, 2, 3, "v0", datapb.SegmentLevel_L1, 1, 4)
	require.NoError(t, err)
	// Orphan Importing segment (no task references it) -> dropped.
	_, err = addImportSegment(ctx, meta, 102, 999, 999, 2, 3, "v0", datapb.SegmentLevel_L1, 1, 4)
	require.NoError(t, err)
	// A Dropped segment stays Dropped and is never re-touched.
	_, err = addImportSegment(ctx, meta, 103, 998, 998, 2, 3, "v0", datapb.SegmentLevel_L1, 1, 4)
	require.NoError(t, err)
	require.NoError(t, meta.UpdateSegmentsInfo(ctx, UpdateStatusOperator(103, commonpb.SegmentState_Dropped)))

	v3task := newImportTaskV3(&datapb.ImportTaskV3{JobId: 1, TaskId: 10, CollectionId: 2, SegmentId: 100}, importMeta, meta, nil)
	require.NoError(t, importMeta.AddTask(ctx, v3task))
	v2taskProto := &datapb.ImportTaskV2{JobID: 1, TaskID: 11, CollectionID: 2, SegmentIDs: []int64{101}}
	v2task := &importTask{meta: meta, importMeta: importMeta}
	v2task.task.Store(v2taskProto)
	require.NoError(t, importMeta.AddTask(ctx, v2task))

	inspector := &importInspector{ctx: ctx, meta: meta, importMeta: importMeta}
	inspector.reconcileOrphanImportSegments()

	require.Equal(t, commonpb.SegmentState_Importing, meta.GetSegment(ctx, 100).GetState())
	require.Equal(t, commonpb.SegmentState_Importing, meta.GetSegment(ctx, 101).GetState())
	require.Equal(t, commonpb.SegmentState_Dropped, meta.GetSegment(ctx, 102).GetState())
	require.Equal(t, commonpb.SegmentState_Dropped, meta.GetSegment(ctx, 103).GetState())
}

// TestReshardTaskWaitsForPreReshardingJobState pins the fix for the
// AssigningIDRange race: a ReshardTask is persisted just before the job
// advances to Resharding (createReshardTasks), so the async
// dispatch loop can observe it while the job is still PreImporting or
// AssigningIDRange. The task must wait (stay Pending) for a later tick instead
// of failing the whole job.
func TestReshardTaskWaitsForPreReshardingJobState(t *testing.T) {
	for _, state := range []internalpb.ImportJobState{
		internalpb.ImportJobState_PreImporting,
		internalpb.ImportJobState_AssigningIDRange,
	} {
		t.Run(state.String(), func(t *testing.T) {
			importMeta := NewMockImportMeta(t)
			job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2, State: state}}
			importMeta.EXPECT().GetJob(mock.Anything, int64(1)).Return(job).Once()
			task := newReshardTask(&datapb.ReshardTask{
				JobId: 1, TaskId: 10, CollectionId: 2, RunId: 1,
				State: datapb.ImportTaskStateV2_Pending,
			}, importMeta, nil, nil)

			// Must not call fail(): that would hit UpdateTask/UpdateJob, which the
			// mock is not set up for, and the mock would fail the test.
			task.CreateTaskOnWorker(1, nil)
			require.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
		})
	}
}
