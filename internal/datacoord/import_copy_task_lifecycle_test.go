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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	dctask "github.com/milvus-io/milvus/internal/datacoord/task"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestImportPendingDispatchAfterJobTerminalPreservesCleanup(t *testing.T) {
	for _, terminalState := range []internalpb.ImportJobState{
		internalpb.ImportJobState_Failed,
		internalpb.ImportJobState_Completed,
	} {
		t.Run(terminalState.String(), func(t *testing.T) {
			ctx := context.Background()
			retention := &Params.DataCoordCfg.ImportTaskRetention
			oldRetention := retention.GetValue()
			require.NoError(t, Params.Save(retention.Key, "0"))
			t.Cleanup(func() { require.NoError(t, Params.Save(retention.Key, oldRetention)) })

			catalog := kvdatacoord.NewCatalog(NewMetaMemoryKV(), "", "")
			segmentMeta := &meta{ctx: ctx, catalog: catalog, segments: NewSegmentsInfo()}
			im, err := NewImportMeta(ctx, catalog, nil, segmentMeta)
			require.NoError(t, err)
			job := &importJob{ImportJob: &datapb.ImportJob{
				JobID: 1, CollectionID: 2, State: internalpb.ImportJobState_Importing,
				TimeoutTs: tsoutil.ComposeTSByTime(time.Now().Add(-time.Minute)), RequestedDiskSize: 1024,
			}, tr: timerecord.NewTimeRecorder("job")}
			require.NoError(t, im.AddJob(ctx, job))
			pending := &importTask{
				ctx: ctx, meta: segmentMeta, importMeta: im,
				tr: timerecord.NewTimeRecorder("task"), times: taskcommon.NewTimes(),
			}
			pending.task.Store(&datapb.ImportTaskV2{
				JobID: 1, TaskID: 3, CollectionID: 2, NodeID: NullNodeID,
				State: datapb.ImportTaskStateV2_Pending, SegmentIDs: []int64{4},
			})
			segment := NewSegmentInfo(&datapb.SegmentInfo{
				ID: 4, CollectionID: 2, InsertChannel: "ch", State: commonpb.SegmentState_Importing, IsImporting: true,
			})
			require.NoError(t, im.(*importMeta).addImportTasks(ctx, segmentMeta, []ImportTask{pending}, []*SegmentInfo{segment}))

			cluster := session.NewMockCluster(t)
			cluster.EXPECT().QuerySlot().Return(map[int64]*session.WorkerSlots{7: {AvailableSlots: 1}}).Once()
			scheduler := dctask.NewGlobalTaskScheduler(ctx, cluster)
			defer scheduler.Stop()
			inspector := NewImportInspector(ctx, segmentMeta, im, scheduler).(*importInspector)
			checker := &importChecker{ctx: ctx, meta: segmentMeta, importMeta: im, scheduler: scheduler, cluster: cluster}

			// The inspector enqueues the metadata's actual wrapper. The job ends
			// after enqueue and before the scheduler enters its Create callback.
			inspector.inspect()
			require.Same(t, pending, im.GetTask(ctx, 3))
			require.Equal(t, 1, scheduler.GetPendingTaskCount(taskcommon.Import))
			if terminalState == internalpb.ImportJobState_Failed {
				checker.tryTimeoutJob(job)
			} else {
				// A Completed job must preserve published outputs even if an old
				// Pending dispatch is still queued when its terminal state is seen.
				require.NoError(t, segmentMeta.UpdateSegmentsInfo(ctx,
					UpdateStatusOperator(4, commonpb.SegmentState_Flushed), UpdateIsImporting(4, false)))
				require.NoError(t, im.UpdateJob(ctx, 1, UpdateJobState(terminalState)))
			}
			require.Equal(t, terminalState, im.GetJob(ctx, 1).GetState())
			pending.SetTaskTime(taskcommon.TimeEnd, time.Time{})
			scheduler.Start()
			require.Eventually(t, func() bool {
				return !pending.GetTaskTime(taskcommon.TimeEnd).IsZero() && scheduler.GetPendingTaskCount(taskcommon.Import) == 0
			}, 5*time.Second, 10*time.Millisecond)
			assert.Equal(t, datapb.ImportTaskStateV2_Pending, im.GetTask(ctx, 3).GetState(),
				"discarding scheduler ownership must not change the shared metadata wrapper")

			terminalJob := im.GetJob(ctx, 1)
			if terminalState == internalpb.ImportJobState_Failed {
				checker.checkFailedJob(terminalJob)
			}
			inspector.inspect()
			if terminalState == internalpb.ImportJobState_Failed {
				assert.Equal(t, commonpb.SegmentState_Dropped, segmentMeta.GetSegment(ctx, 4).GetState())
				assert.False(t, segmentMeta.GetSegment(ctx, 4).GetIsImporting())
				assert.Empty(t, pending.GetSegmentIDs(), "the existing failed-task inspector must release cleanup anchors")
			} else {
				assert.Equal(t, commonpb.SegmentState_Flushed, segmentMeta.GetSegment(ctx, 4).GetState())
			}
			checker.checkGC(terminalJob)
			assert.Nil(t, im.GetTask(ctx, 3))
			assert.Nil(t, im.GetJob(ctx, 1))
			persistedTasks, err := catalog.ListImportTasks(ctx)
			require.NoError(t, err)
			assert.Empty(t, persistedTasks)
			persistedJobs, err := catalog.ListImportJobs(ctx)
			require.NoError(t, err)
			assert.Empty(t, persistedJobs)
		})
	}
}

type copyMetaAfterTaskSnapshot struct {
	CopySegmentMeta
	afterSnapshot func()
}

func (m *copyMetaAfterTaskSnapshot) GetTasksByJobID(ctx context.Context, jobID int64) []CopySegmentTask {
	tasks := m.CopySegmentMeta.GetTasksByJobID(ctx, jobID)
	m.afterSnapshot()
	return tasks
}

func TestCopyRetryReplacementPreservesCheckerTaskSnapshot(t *testing.T) {
	ctx := context.Background()
	const oldTargetID = int64(2001)
	oldTask := createTestCopyTask(100, oldTargetID).(*copySegmentTask)
	copyMeta, segmentMeta := newCopySegmentTaskTestMetaWithAlloc(t, oldTask, 9000)
	require.NoError(t, segmentMeta.AddSegment(ctx, newTestCopySegment(oldTargetID)))
	job := newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting)
	job.(*copySegmentJob).IdMappings = oldTask.Clone().GetIdMappings()
	require.NoError(t, copyMeta.AddJob(ctx, job))

	// Let the real scheduler process Retry and release the old execution. Its
	// worker is dropped once before the business inspector builds a replacement.
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().QueryCopySegment(int64(1), mock.Anything).Return(&datapb.QueryCopySegmentResponse{
		TaskID: oldTask.GetTaskId(), State: datapb.CopySegmentTaskState_CopySegmentTaskRetry,
	}, nil).Once()
	cluster.EXPECT().DropCopySegment(int64(1), oldTask.GetTaskId()).Return(nil).Once()
	scheduler := dctask.NewGlobalTaskScheduler(ctx, cluster)
	oldTask.SetTaskTime(taskcommon.TimeEnd, time.Time{})
	scheduler.Enqueue(oldTask)
	scheduler.Start()
	completed := assert.Eventually(t, func() bool { return !oldTask.GetTaskTime(taskcommon.TimeEnd).IsZero() },
		5*time.Second, 10*time.Millisecond)
	scheduler.Stop()
	if !completed {
		return
	}
	require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskRetry, oldTask.GetState())

	var replacement dctask.Task
	inspectionScheduler := dctask.NewMockGlobalScheduler(t)
	inspectionScheduler.EXPECT().Enqueue(mock.Anything).Run(func(task dctask.Task) { replacement = task }).Once()
	inspector := &copySegmentInspector{ctx: ctx, meta: segmentMeta, copyMeta: copyMeta, scheduler: inspectionScheduler}
	checker := &copySegmentChecker{
		ctx: ctx, meta: segmentMeta,
		copyMeta: &copyMetaAfterTaskSnapshot{CopySegmentMeta: copyMeta, afterSnapshot: func() {
			// Preserve the actual slice returned to the checker, then run the
			// replacement before the checker examines those shared task objects.
			inspector.processRetry(oldTask)
		}},
	}
	checker.checkCopyingJob(copyMeta.GetJob(ctx, 100))

	require.NotNil(t, replacement)
	assert.Nil(t, copyMeta.GetTask(ctx, oldTask.GetTaskId()))
	assert.NotNil(t, copyMeta.GetTask(ctx, replacement.GetTaskID()))
	assert.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskRetry, oldTask.GetState())
	assert.Equal(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, copyMeta.GetJob(ctx, 100).GetState(),
		"an abandoned Retry attempt must not become a failed job through an older task slice")
	assert.Equal(t, commonpb.SegmentState_Dropped, segmentMeta.GetSegment(ctx, oldTargetID).GetState())
	newTargets := copyMeta.GetJob(ctx, 100).GetIdMappings()
	require.Len(t, newTargets, 1)
	assert.NotEqual(t, oldTargetID, newTargets[0].GetTargetSegmentId())
	assert.Equal(t, commonpb.SegmentState_Importing, segmentMeta.GetSegment(ctx, newTargets[0].GetTargetSegmentId()).GetState())
}
