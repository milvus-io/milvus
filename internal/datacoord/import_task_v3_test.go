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
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// TestImportTaskV3PrepareRetryRepointsBeforeDroppingOld pins the crash-safe
// ordering of prepareRetry: the task is repointed to the new segment BEFORE the
// old segment is dropped, so a crash between the two catalog writes leaves the
// task referencing a live segment and only the superseded old segment orphaned
// (reclaimed by importInspector.reconcileOrphanImportSegments on restart).
func TestImportTaskV3PrepareRetryRepointsBeforeDroppingOld(t *testing.T) {
	ctx := context.Background()
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	importMeta := NewMockImportMeta(t)
	cluster := session.NewMockCluster(t)
	alloc := allocator.NewMockAllocator(t)

	oldSeg, err := addImportSegment(ctx, meta, 100, 1, 10, 2, 3, "v0", datapb.SegmentLevel_L1, 1, 4)
	require.NoError(t, err)
	require.Equal(t, commonpb.SegmentState_Importing, oldSeg.GetState())

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
	alloc.EXPECT().AllocN(mock.Anything).Return(int64(5000), int64(6000), nil).Once()

	var repointBeforeDrop bool
	importMeta.EXPECT().UpdateTask(mock.Anything, int64(10),
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).RunAndReturn(func(_ context.Context, _ int64, actions ...UpdateAction) error {
		// At repoint time the old segment must still be Importing: the drop only
		// happens after the task points at the new segment.
		require.Equal(t, commonpb.SegmentState_Importing, meta.GetSegment(ctx, 100).GetState())
		repointBeforeDrop = true
		// Simulate the real UpdateTask: apply the actions to the task in place so
		// the post-repoint assertions below observe the new run/segment/log range.
		clone := task.Clone()
		for _, action := range actions {
			action(clone)
		}
		task.task.Store(clone.(*importTaskV3).task.Load())
		return nil
	}).Once()

	task.prepareRetry(cluster)

	require.True(t, repointBeforeDrop)
	require.Equal(t, commonpb.SegmentState_Dropped, meta.GetSegment(ctx, 100).GetState())
	newSeg := meta.GetSegment(ctx, 200)
	require.NotNil(t, newSeg)
	require.Equal(t, commonpb.SegmentState_Importing, newSeg.GetState())
	require.Equal(t, int64(2), task.task.Load().GetRunId())
	require.Equal(t, int64(200), task.task.Load().GetSegmentId())
	require.Equal(t, int64(5000), task.task.Load().GetLogRange().GetBegin())
	require.Equal(t, int64(6000), task.task.Load().GetLogRange().GetEnd())
	require.Equal(t, datapb.ImportTaskStateV2_Pending, task.task.Load().GetState())
	require.Equal(t, int64(NullNodeID), task.task.Load().GetNodeId())
}

// TestImportTaskV3PrepareRetrySurvivesTransientAllocN pins that a transient
// AllocN failure (mixCoord RPC timeout) does not fail the task or the job: V2
// parity counts the attempt and retries on the next tick, and the fresh
// segment of THIS round is dropped so nothing leaks across rounds.
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

	// Task untouched (same run/segment, still InProgress) and this round's
	// fresh segment was dropped; the old segment survives for the next tick.
	p := task.task.Load()
	require.Equal(t, int64(1), p.GetRunId())
	require.Equal(t, int64(100), p.GetSegmentId())
	require.Equal(t, datapb.ImportTaskStateV2_InProgress, p.GetState())
	require.Equal(t, commonpb.SegmentState_Dropped, meta.GetSegment(ctx, 200).GetState())
	require.Equal(t, commonpb.SegmentState_Importing, meta.GetSegment(ctx, 100).GetState())
	require.Equal(t, internalpb.ImportJobState_Importing, job.GetState())
}

// TestImportTaskV3PrepareRetrySurvivesTransientRepoint pins that a failed
// (possibly timed-out-but-committed) repoint UpdateTask does not fail the job
// and does NOT drop the fresh segment: if the write landed, the task points at
// the new segment and dropping it would kill the job on the next tick; if it
// did not, the segment is orphaned and reclaimed on restart either way.
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
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(errRetryUpdate).Once()

	task.prepareRetry(cluster)

	p := task.task.Load()
	require.Equal(t, int64(1), p.GetRunId())
	require.Equal(t, int64(100), p.GetSegmentId())
	require.Equal(t, datapb.ImportTaskStateV2_InProgress, p.GetState())
	// The unknown-outcome write leaves the fresh segment Importing (NOT
	// Dropped): the restart reconciliation owns it if the repoint never
	// landed, and the next tick's prepareRetry re-enters through the query
	// path if it did.
	require.Equal(t, commonpb.SegmentState_Importing, meta.GetSegment(ctx, 200).GetState())
	require.Equal(t, commonpb.SegmentState_Importing, meta.GetSegment(ctx, 100).GetState())
	require.Equal(t, internalpb.ImportJobState_Importing, job.GetState())
}

var errRetryUpdate = errors.New("update task failed")

// retryTestSchema gives prepareRetry's LogRange-width re-derivation
// (buildImportV3WriterSpec) a valid target schema; production jobs always
// carry one, but the retry tests construct minimal jobs.
// TestImportTaskV3PrepareRetrySelfHealsStorageVersion pins that the
// replacement segment takes the CURRENT storage version: a transient retry
// after an operator hot-flips common.storage.useLoonFFI must re-plan onto the
// new version instead of letting the acceptance post-validation fail the
// whole job for a record/writer mismatch the retry could have healed.
func TestImportTaskV3PrepareRetrySelfHealsStorageVersion(t *testing.T) {
	ctx := context.Background()
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	importMeta := NewMockImportMeta(t)
	cluster := session.NewMockCluster(t)
	alloc := allocator.NewMockAllocator(t)

	// Old segment was planned with useLoonFFI=false (V2).
	_, err = addImportSegment(ctx, meta, 100, 1, 10, 2, 3, "v0", datapb.SegmentLevel_L1, storage.StorageV2, 4)
	require.NoError(t, err)
	paramtable.Get().Save("common.storage.useLoonFFI", "true")
	defer paramtable.Get().Reset("common.storage.useLoonFFI")

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
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(nil).Once()

	task.prepareRetry(cluster)

	newSeg := meta.GetSegment(ctx, 200)
	require.NotNil(t, newSeg)
	require.Equal(t, storage.StorageV3, newSeg.GetStorageVersion())
}

var retryTestSchema = &schemapb.CollectionSchema{
	Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	},
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
		Segments: []*datapb.SegmentResult{{
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
		Segments: []*datapb.SegmentResult{{Rows: 0}},
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
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportTaskV3(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Maybe()

	importMeta, err := NewImportMeta(ctx, catalog, nil, meta)
	require.NoError(t, err)

	// Referenced by a V3 task -> kept.
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
