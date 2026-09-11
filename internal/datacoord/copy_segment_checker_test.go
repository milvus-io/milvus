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
	"sort"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	dcTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type CopySegmentCheckerSuite struct {
	suite.Suite

	collectionID int64
	jobID        int64

	catalog  *mocks.DataCoordCatalog
	alloc    *allocator.MockAllocator
	broker   *broker.MockBroker
	meta     *meta
	copyMeta CopySegmentMeta
	cluster  *session.MockCluster
	checker  *copySegmentChecker
}

func (s *CopySegmentCheckerSuite) SetupTest() {
	var err error

	s.collectionID = 1
	s.jobID = 100

	s.catalog = mocks.NewDataCoordCatalog(s.T())
	s.catalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListCopySegmentTasks(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	s.catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
	s.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

	s.alloc = allocator.NewMockAllocator(s.T())
	s.broker = broker.NewMockBroker(s.T())
	s.broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)

	s.meta, err = newMeta(context.TODO(), s.catalog, nil, s.broker)
	s.NoError(err)
	s.copyMeta, err = NewCopySegmentMeta(context.TODO(), s.catalog, s.meta, nil, nil)
	s.NoError(err)

	s.cluster = session.NewMockCluster(s.T())
	s.checker = NewCopySegmentChecker(
		context.TODO(),
		s.meta,
		s.broker,
		s.alloc,
		s.copyMeta,
		s.cluster,
		nil,
	).(*copySegmentChecker)
}

func (s *CopySegmentCheckerSuite) TearDownTest() {
	s.checker.Close()
}

func TestCopySegmentChecker(t *testing.T) {
	suite.Run(t, new(CopySegmentCheckerSuite))
}

func (s *CopySegmentCheckerSuite) TestCheckPendingJob_NoIdMappings() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)

	// Create a job with no id mappings
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobPending,
			IdMappings:   nil,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	err := s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Check pending job
	s.checker.checkPendingJob(job)

	// Job should be completed since there are no segments to copy
	updatedJob := s.copyMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobCompleted, updatedJob.GetState())
	s.Equal("no segments to copy", updatedJob.GetReason())
}

func (s *CopySegmentCheckerSuite) TestCheckPendingJob_CreateTasks() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(2)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(2)
	s.alloc.EXPECT().AllocID(mock.Anything).Return(int64(1001), nil).Times(1)
	s.alloc.EXPECT().AllocID(mock.Anything).Return(int64(1002), nil).Times(1)

	// Create a job with multiple id mappings
	idMappings := []*datapb.CopySegmentIDMapping{
		{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10},
		{SourceSegmentId: 2, TargetSegmentId: 102, PartitionId: 10},
		{SourceSegmentId: 3, TargetSegmentId: 103, PartitionId: 10},
	}

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobPending,
			IdMappings:   idMappings,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	err := s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Set max segments per task to 2 to create multiple tasks
	Params.DataCoordCfg.MaxSegmentsPerCopyTask.SwapTempValue("2")
	defer Params.DataCoordCfg.MaxSegmentsPerCopyTask.SwapTempValue("10")

	// Check pending job
	s.checker.checkPendingJob(job)

	// Job should be in executing state
	updatedJob := s.copyMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobExecuting, updatedJob.GetState())

	// Should create 2 tasks (2 segments + 1 segment)
	tasks := s.copyMeta.GetTasksByJobID(context.TODO(), s.jobID)
	s.Len(tasks, 2)

	// Collect mapping counts from all tasks (order doesn't matter)
	var mappingCounts []int
	for _, task := range tasks {
		mappingCounts = append(mappingCounts, len(task.GetIdMappings()))
		s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskPending, task.GetState())
	}

	// Sort and verify: one task should have 1 mapping, another should have 2
	sort.Ints(mappingCounts)
	s.Equal([]int{1, 2}, mappingCounts, "should have one task with 1 mapping and one with 2 mappings")
}

func (s *CopySegmentCheckerSuite) TestCheckPendingJob_ResumesPartialTaskCreation() {
	// Simulate a previous round that persisted only the first task before
	// failing (etcd hiccup / DataCoord restart): checkPendingJob must create
	// tasks for the remaining uncovered segments and move the job to Executing
	// instead of returning early and leaving the job Pending forever.
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(2)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(2)
	s.alloc.EXPECT().AllocID(mock.Anything).Return(int64(1002), nil).Times(1)

	idMappings := []*datapb.CopySegmentIDMapping{
		{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10},
		{SourceSegmentId: 2, TargetSegmentId: 102, PartitionId: 10},
		{SourceSegmentId: 3, TargetSegmentId: 103, PartitionId: 10},
	}

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobPending,
			IdMappings:   idMappings,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))

	// Pre-existing task from the failed round covers the first two mappings.
	partialTask := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}
	partialTask.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		NodeId:       NullNodeID,
		TaskSlot:     1,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
		IdMappings:   idMappings[:2],
	})
	s.NoError(s.copyMeta.AddTask(context.TODO(), partialTask))

	Params.DataCoordCfg.MaxSegmentsPerCopyTask.SwapTempValue("2")
	defer Params.DataCoordCfg.MaxSegmentsPerCopyTask.SwapTempValue("10")

	s.checker.checkPendingJob(job)

	// Job must reach Executing.
	updatedJob := s.copyMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobExecuting, updatedJob.GetState())

	// The uncovered mapping (source segment 3) must now have a task.
	tasks := s.copyMeta.GetTasksByJobID(context.TODO(), s.jobID)
	s.Len(tasks, 2)
	coveredSources := make(map[int64]int)
	for _, task := range tasks {
		for _, mapping := range task.GetIdMappings() {
			coveredSources[mapping.GetSourceSegmentId()]++
		}
	}
	s.Equal(map[int64]int{1: 1, 2: 1, 3: 1}, coveredSources,
		"each source segment must be covered exactly once")
}

func (s *CopySegmentCheckerSuite) TestCheckPendingJob_AllTasksExistTransitionsToExecuting() {
	// Simulate a previous round that created all tasks but failed to persist
	// the Pending→Executing transition: checkPendingJob must not create any
	// new task (no AllocID expectation) and must retry the transition.
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(2)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(1)

	idMappings := []*datapb.CopySegmentIDMapping{
		{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10},
		{SourceSegmentId: 2, TargetSegmentId: 102, PartitionId: 10},
	}

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobPending,
			IdMappings:   idMappings,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))

	fullTask := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}
	fullTask.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		NodeId:       NullNodeID,
		TaskSlot:     1,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
		IdMappings:   idMappings,
	})
	s.NoError(s.copyMeta.AddTask(context.TODO(), fullTask))

	s.checker.checkPendingJob(job)

	updatedJob := s.copyMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobExecuting, updatedJob.GetState())
	s.Len(s.copyMeta.GetTasksByJobID(context.TODO(), s.jobID), 1)
}

func (s *CopySegmentCheckerSuite) TestCheckPendingJob_StaleSnapshotDoesNotResurrectFailedJob() {
	// Regression test: checkPendingJob receives a job snapshot taken before it
	// runs, while tasks of a Pending job can already be dispatched and fail
	// concurrently — markTaskAndJobFailed then moves the job to Failed and
	// releases its snapshot pin. The checker, still holding the stale Pending
	// snapshot, must neither create more tasks nor resurrect the job as
	// Executing.
	// AddJob + the concurrent Failed transition each persist once; the stale
	// checkPendingJob call must not persist anything (and must not AllocID —
	// the allocator mock has no expectation and would fail the test).
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(2)

	staleJob := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobPending,
			IdMappings: []*datapb.CopySegmentIDMapping{
				{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10},
			},
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), staleJob))

	// Concurrent failure path moves the cached job to Failed. The cached entry
	// is replaced with a Failed clone; `staleJob` keeps its Pending state and
	// plays the stale snapshot below.
	applied, err := s.copyMeta.UpdateJobStateAndReleasePin(context.TODO(), s.jobID,
		datapb.CopySegmentJobState_CopySegmentJobPending,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
		UpdateCopyJobReason("task failed concurrently"))
	s.NoError(err)
	s.True(applied)

	s.checker.checkPendingJob(staleJob)

	updatedJob := s.copyMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobFailed, updatedJob.GetState())
	s.Equal("task failed concurrently", updatedJob.GetReason())
	s.Empty(s.copyMeta.GetTasksByJobID(context.TODO(), s.jobID))
}

func (s *CopySegmentCheckerSuite) TestCheckCopyingJob_UpdateProgress() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(2)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(2)

	// Create a job
	idMappings := []*datapb.CopySegmentIDMapping{
		{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10},
		{SourceSegmentId: 2, TargetSegmentId: 102, PartitionId: 10},
	}

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:          s.jobID,
			CollectionId:   s.collectionID,
			State:          datapb.CopySegmentJobState_CopySegmentJobExecuting,
			IdMappings:     idMappings,
			CopiedSegments: 0,
			TotalSegments:  2,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	err := s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Create tasks - one completed, one in progress
	task1 := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task1"),
		times:    taskcommon.NewTimes(),
	}
	task1.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		IdMappings:   idMappings[:1],
	})
	err = s.copyMeta.AddTask(context.TODO(), task1)
	s.NoError(err)

	task2 := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task2"),
		times:    taskcommon.NewTimes(),
	}
	task2.task.Store(&datapb.CopySegmentTask{
		TaskId:       1002,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
		IdMappings:   idMappings[1:],
	})
	err = s.copyMeta.AddTask(context.TODO(), task2)
	s.NoError(err)

	// Check copying job
	s.checker.checkCopyingJob(job)

	// Job progress should be updated
	updatedJob := s.copyMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobExecuting, updatedJob.GetState())
	s.Equal(int64(1), updatedJob.GetCopiedSegments())
}

func (s *CopySegmentCheckerSuite) TestCheckCopyingJob_MarkFailed() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(2)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)

	// Create a job
	idMappings := []*datapb.CopySegmentIDMapping{
		{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10},
	}

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
			IdMappings:   idMappings,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	err := s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Create a failed task
	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskFailed,
		Reason:       "test failure",
		IdMappings:   idMappings,
	})
	err = s.copyMeta.AddTask(context.TODO(), task)
	s.NoError(err)

	// Check copying job
	s.checker.checkCopyingJob(job)

	// Job should be marked as failed
	updatedJob := s.copyMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobFailed, updatedJob.GetState())
	s.Contains(updatedJob.GetReason(), "failed")
}

func (s *CopySegmentCheckerSuite) TestCheckCopyingJob_AllTasksCompleted() {
	// AddJob and progress are ordinary saves; final publication is one Update.
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(2)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	// Create segments
	seg1 := NewSegmentInfo(&datapb.SegmentInfo{
		ID:            101,
		CollectionID:  s.collectionID,
		PartitionID:   10,
		State:         commonpb.SegmentState_Importing,
		IsImporting:   true,
		NumOfRows:     100,
		InsertChannel: "ch1",
	})
	err := s.meta.AddSegment(context.TODO(), seg1)
	s.NoError(err)

	// Create a job
	idMappings := []*datapb.CopySegmentIDMapping{
		{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10},
	}

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
			IdMappings:   idMappings,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	err = s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Create a completed task
	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		IdMappings:   idMappings,
	})
	err = s.copyMeta.AddTask(context.TODO(), task)
	s.NoError(err)

	// Check copying job
	s.checker.checkCopyingJob(job)

	// Job should be completed
	updatedJob := s.copyMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobCompleted, updatedJob.GetState())
	s.Equal(int64(100), updatedJob.GetTotalRows())
}

func (s *CopySegmentCheckerSuite) TestCheckFailedJob_MarkTasksFailed() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(2)

	// Create a failed job
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobFailed,
			Reason:       "job failed",
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	err := s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Create a pending task
	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})
	err = s.copyMeta.AddTask(context.TODO(), task)
	s.NoError(err)

	// Check failed job
	s.checker.checkFailedJob(job)

	// Task should be marked as failed
	updatedTask := s.copyMeta.GetTask(context.TODO(), 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskFailed, updatedTask.GetState())
	s.Equal("job failed", updatedTask.GetReason())
}

func (s *CopySegmentCheckerSuite) TestCheckFailedJob_CleansTargetsWithoutTasks() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Once()
	s.catalog.EXPECT().DropCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Once()

	target := NewSegmentInfo(&datapb.SegmentInfo{
		ID: 101, CollectionID: s.collectionID, PartitionID: 10,
		State: commonpb.SegmentState_Importing, IsImporting: true,
	})
	s.NoError(s.meta.AddSegment(context.Background(), target))

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId: s.jobID, CollectionId: s.collectionID,
			State:     datapb.CopySegmentJobState_CopySegmentJobFailed,
			CleanupTs: tsoutil.ComposeTSByTime(time.Now().Add(-time.Hour)),
			IdMappings: []*datapb.CopySegmentIDMapping{{
				SourceSegmentId: 1, TargetSegmentId: target.GetID(), PartitionId: target.GetPartitionID(),
			}},
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.Background(), job))

	// The failure can happen before the first task is created. The job mapping
	// must still retire its target and keep the job as the cleanup owner.
	s.checker.checkFailedJob(job)
	retired := s.meta.GetSegment(context.Background(), target.GetID())
	s.Require().NotNil(retired)
	s.Equal(commonpb.SegmentState_Dropped, retired.GetState())
	s.False(retired.GetIsImporting())
	s.checker.checkGC(job)
	s.NotNil(s.copyMeta.GetJob(context.Background(), job.GetJobId()))

	// Once normal segment GC has removed the Dropped target, job GC may finish.
	s.meta.segMu.Lock()
	s.meta.segments.DropSegment(target.GetID())
	s.meta.segMu.Unlock()
	s.checker.checkGC(job)
	s.Nil(s.copyMeta.GetJob(context.Background(), job.GetJobId()))
}

func (s *CopySegmentCheckerSuite) TestTryTimeoutJob() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(2)

	// Create a job with timeout in the past
	timeoutTs := tsoutil.ComposeTSByTime(time.Now().Add(-1 * time.Hour))
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
			TimeoutTs:    timeoutTs,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	err := s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Try timeout job
	s.checker.tryTimeoutJob(job)

	// Job should be marked as failed
	updatedJob := s.copyMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobFailed, updatedJob.GetState())
	s.Equal("timeout", updatedJob.GetReason())
}

func (s *CopySegmentCheckerSuite) TestTryTimeoutJob_FiresWithProductionTimeoutTs() {
	// End-to-end unit check across the write site and the read site: the job
	// creation path composes TimeoutTs via CopyJobTimeoutTs, and tryTimeoutJob
	// must actually fire once that deadline has elapsed. Guards against the
	// regression where the write site stored UnixNano while the reader decoded
	// a hybrid TSO, so no job ever timed out.
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(2)

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
			// Same composition as the creation path in snapshot_manager, with
			// an already-elapsed deadline.
			TimeoutTs: CopyJobTimeoutTs(-time.Minute),
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))

	s.checker.tryTimeoutJob(job)

	updatedJob := s.copyMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobFailed, updatedJob.GetState())
	s.Equal("timeout", updatedJob.GetReason())
}

func (s *CopySegmentCheckerSuite) TestTryTimeoutJob_NotElapsedKeepsExecuting() {
	// A freshly composed production deadline must NOT trigger the timeout.
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(1)

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
			TimeoutTs:    CopyJobTimeoutTs(time.Hour),
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))

	s.checker.tryTimeoutJob(job)

	updatedJob := s.copyMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobExecuting, updatedJob.GetState())
}

func (s *CopySegmentCheckerSuite) TestCheckGC_RemoveCompletedJob() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().DropCopySegmentTask(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().DropCopySegmentJob(mock.Anything, mock.Anything).Return(nil)

	// Create a completed job with cleanup time in the past
	cleanupTs := tsoutil.ComposeTSByTime(time.Now().Add(-1 * time.Hour))
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobCompleted,
			CleanupTs:    cleanupTs,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	err := s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Create a completed task with no node ID
	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		NodeId:       NullNodeID,
	})
	err = s.copyMeta.AddTask(context.TODO(), task)
	s.NoError(err)

	// Check GC
	s.checker.checkGC(job)

	// Job and task should be removed
	removedJob := s.copyMeta.GetJob(context.TODO(), s.jobID)
	s.Nil(removedJob)

	removedTask := s.copyMeta.GetTask(context.TODO(), 1001)
	s.Nil(removedTask)
}

func (s *CopySegmentCheckerSuite) TestCheckGC_RetriesPinReleaseBeforeRemovingJob() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(2)
	s.catalog.EXPECT().DropCopySegmentJob(mock.Anything, s.jobID).Return(nil).Once()

	concrete := s.copyMeta.(*copySegmentMeta)
	concrete.snapshotMeta = &snapshotMeta{}
	unpinCalls := 0
	mockUnpin := mockey.Mock((*snapshotMeta).UnpinSnapshot).To(
		func(_ *snapshotMeta, _ context.Context, pinID int64) (int64, string, int, error) {
			unpinCalls++
			s.Equal(int64(7001), pinID)
			if unpinCalls == 1 {
				return 0, "", 0, errors.New("catalog unavailable")
			}
			return s.collectionID, "snapshot", 0, nil
		}).Build()
	defer mockUnpin.UnPatch()

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId: s.jobID, CollectionId: s.collectionID,
			State:     datapb.CopySegmentJobState_CopySegmentJobCompleted,
			CleanupTs: tsoutil.ComposeTSByTime(time.Now().Add(-time.Hour)),
			PinId:     7001,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.Background(), job))

	// A failed release retains both the terminal job and its durable PinId.
	s.checker.checkGC(job)
	retained := s.copyMeta.GetJob(context.Background(), s.jobID)
	s.Require().NotNil(retained)
	s.Equal(int64(7001), retained.GetPinId())

	// The next checker round retries the idempotent unpin, clears PinId, then
	// permits ordinary retention GC to remove the job.
	s.checker.checkGC(retained)
	s.Nil(s.copyMeta.GetJob(context.Background(), s.jobID))
	s.Equal(2, unpinCalls)
}

func (s *CopySegmentCheckerSuite) TestCheckGC_RetriesWorkerDropBeforeRemovingMetadata() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(3)
	s.catalog.EXPECT().DropCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(3)
	s.catalog.EXPECT().DropCopySegmentJob(mock.Anything, mock.Anything).Return(nil)

	cleanupTs := tsoutil.ComposeTSByTime(time.Now().Add(-1 * time.Hour))
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobCompleted,
			CleanupTs:    cleanupTs,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))

	for taskID, nodeID := range map[int64]int64{1001: 7, 1002: 8, 1003: 9} {
		task := &copySegmentTask{
			copyMeta: s.copyMeta,
			tr:       timerecord.NewTimeRecorder("task"),
			times:    taskcommon.NewTimes(),
		}
		task.task.Store(&datapb.CopySegmentTask{
			TaskId:       taskID,
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
			NodeId:       nodeID,
		})
		s.NoError(s.copyMeta.AddTask(context.TODO(), task))
	}

	// A transient failure keeps its task and job as durable retry evidence.
	s.cluster.EXPECT().DropCopySegment(int64(7), int64(1001)).
		Return(errors.New("connection refused")).Once()
	// A successful drop and a node that is already gone both permit task removal.
	s.cluster.EXPECT().DropCopySegment(int64(8), int64(1002)).Return(nil).Once()
	s.cluster.EXPECT().DropCopySegment(int64(9), int64(1003)).
		Return(merr.WrapErrNodeNotFound(9)).Once()
	s.checker.checkGC(job)
	s.NotNil(s.copyMeta.GetTask(context.TODO(), 1001))
	s.Nil(s.copyMeta.GetTask(context.TODO(), 1002))
	s.Nil(s.copyMeta.GetTask(context.TODO(), 1003))
	s.NotNil(s.copyMeta.GetJob(context.TODO(), s.jobID))

	// The next GC round retries the retained task and closes the job after success.
	s.cluster.EXPECT().DropCopySegment(int64(7), int64(1001)).Return(nil).Once()
	s.checker.checkGC(job)
	s.Nil(s.copyMeta.GetTask(context.TODO(), 1001))
	s.Nil(s.copyMeta.GetJob(context.TODO(), s.jobID))
}

func (s *CopySegmentCheckerSuite) TestCheckGC_FinalizeReloadsAssignedTask() {
	const taskID int64 = 1001
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Once()
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Twice()
	s.catalog.EXPECT().DropCopySegmentTask(mock.Anything, taskID).Return(nil).Once()
	s.catalog.EXPECT().DropCopySegmentJob(mock.Anything, s.jobID).Return(nil).Once()

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobCompleted,
			CleanupTs:    tsoutil.ComposeTSByTime(time.Now().Add(-time.Hour)),
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))

	pending := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	pending.task.Store(&datapb.CopySegmentTask{
		TaskId: taskID,
		JobId:  s.jobID,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskPending,
		NodeId: NullNodeID,
	})
	s.NoError(s.copyMeta.AddTask(context.TODO(), pending))

	scheduler := dcTask.NewMockGlobalScheduler(s.T())
	s.checker.scheduler = scheduler
	scheduler.EXPECT().Finalize(taskID, mock.Anything).Run(func(_ int64, fn func()) {
		// Simulate the Create callback publishing its assignment before Finalize
		// acquires the task lock. GC must use this latest owner, not its old snapshot.
		applied, err := s.copyMeta.UpdateTaskInState(context.TODO(), taskID,
			datapb.CopySegmentTaskState_CopySegmentTaskPending,
			UpdateCopyTaskNodeID(11),
			UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskInProgress))
		s.NoError(err)
		s.True(applied)
		fn()
	}).Once()
	s.cluster.EXPECT().DropCopySegment(int64(11), taskID).Return(nil).Once()

	s.checker.checkGC(job)
	s.Nil(s.copyMeta.GetTask(context.TODO(), taskID))
	s.Nil(s.copyMeta.GetJob(context.TODO(), s.jobID))
}

func (s *CopySegmentCheckerSuite) TestLogJobStats() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(3)

	// Create jobs in different states
	jobs := []*copySegmentJob{
		{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:        100,
				CollectionId: s.collectionID,
				State:        datapb.CopySegmentJobState_CopySegmentJobPending,
			},
			tr: timerecord.NewTimeRecorder("job1"),
		},
		{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:        101,
				CollectionId: s.collectionID,
				State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
			},
			tr: timerecord.NewTimeRecorder("job2"),
		},
		{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:        102,
				CollectionId: s.collectionID,
				State:        datapb.CopySegmentJobState_CopySegmentJobCompleted,
			},
			tr: timerecord.NewTimeRecorder("job3"),
		},
	}

	for _, job := range jobs {
		err := s.copyMeta.AddJob(context.TODO(), job)
		s.NoError(err)
	}

	// Convert to interface slice
	jobInterfaces := make([]CopySegmentJob, len(jobs))
	for i, job := range jobs {
		jobInterfaces[i] = job
	}

	// Log job stats should not panic
	s.NotPanics(func() {
		s.checker.LogJobStats(jobInterfaces)
	})
}

func (s *CopySegmentCheckerSuite) TestLogTaskStats() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(4)

	// Create tasks in different states
	tasks := []*copySegmentTask{
		{
			copyMeta: s.copyMeta,
			tr:       timerecord.NewTimeRecorder("task1"),
			times:    taskcommon.NewTimes(),
		},
		{
			copyMeta: s.copyMeta,
			tr:       timerecord.NewTimeRecorder("task2"),
			times:    taskcommon.NewTimes(),
		},
		{
			copyMeta: s.copyMeta,
			tr:       timerecord.NewTimeRecorder("task3"),
			times:    taskcommon.NewTimes(),
		},
		{
			copyMeta: s.copyMeta,
			tr:       timerecord.NewTimeRecorder("task4"),
			times:    taskcommon.NewTimes(),
		},
	}

	tasks[0].task.Store(&datapb.CopySegmentTask{
		TaskId: 1, JobId: s.jobID, State: datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})
	tasks[1].task.Store(&datapb.CopySegmentTask{
		TaskId: 2, JobId: s.jobID, State: datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
	})
	tasks[2].task.Store(&datapb.CopySegmentTask{
		TaskId: 3, JobId: s.jobID, State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
	})
	tasks[3].task.Store(&datapb.CopySegmentTask{
		TaskId: 4, JobId: s.jobID, State: datapb.CopySegmentTaskState_CopySegmentTaskFailed,
	})

	for _, task := range tasks {
		err := s.copyMeta.AddTask(context.TODO(), task)
		s.NoError(err)
	}

	// Log task stats should not panic
	s.NotPanics(func() {
		s.checker.LogTaskStats()
	})
}

func (s *CopySegmentCheckerSuite) TestClose() {
	// Start owns both loops and Close waits for them. Close remains idempotent.
	s.NotPanics(func() {
		s.checker.Start()
		s.checker.Close()
		s.checker.Close()
	})
	s.True(s.checker.started)
	s.True(s.checker.stopped)
}

func (s *CopySegmentCheckerSuite) TestFinishJob_UpdateSegmentStates() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Once()
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	// Create target segments in Importing state
	seg1 := NewSegmentInfo(&datapb.SegmentInfo{
		ID:            101,
		CollectionID:  s.collectionID,
		PartitionID:   10,
		State:         commonpb.SegmentState_Importing,
		IsImporting:   true,
		NumOfRows:     100,
		InsertChannel: "ch1",
	})
	err := s.meta.AddSegment(context.TODO(), seg1)
	s.NoError(err)

	// Create a job
	idMappings := []*datapb.CopySegmentIDMapping{
		{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10},
	}

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
			IdMappings:   idMappings,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	err = s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Create a completed task
	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		IdMappings:   idMappings,
	})
	err = s.copyMeta.AddTask(context.TODO(), task)
	s.NoError(err)

	// Finish job
	s.checker.finishJob(job, 100, []CopySegmentTask{task})

	// Target segment should be in Flushed state
	segment := s.meta.GetSegment(context.TODO(), 101)
	s.Equal(commonpb.SegmentState_Flushed, segment.GetState())
	s.False(segment.GetIsImporting())

	// Job should be completed
	updatedJob := s.copyMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobCompleted, updatedJob.GetState())
	s.Equal(int64(100), updatedJob.GetTotalRows())
}

func (s *CopySegmentCheckerSuite) TestCheckPendingJob_NoMappings_ReleasesPin() {
	snapshotName := "test_snapshot_no_mappings"
	jobID := int64(200)

	// Setup: Create job with no ID mappings
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:              jobID,
			CollectionId:       s.collectionID,
			SourceCollectionId: s.collectionID,
			SnapshotName:       snapshotName,
			State:              datapb.CopySegmentJobState_CopySegmentJobPending,
			IdMappings:         []*datapb.CopySegmentIDMapping{}, // Empty
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.copyMeta.AddJob(context.TODO(), job)

	// Execute: Check pending job
	s.checker.checkPendingJob(job)

	// Verify: Job marked as Completed and ref released
	updatedJob := s.copyMeta.GetJob(context.TODO(), jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobCompleted, updatedJob.GetState())
}

func (s *CopySegmentCheckerSuite) TestCheckCopyingJob_AllTasksDone_ReleasesPin() {
	snapshotName := "test_snapshot_copying_completed"
	jobID := int64(400)

	// Setup mocks: completion publishes the segment and job together.
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	// Setup: Create job in Executing state with all tasks completed
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:              jobID,
			CollectionId:       s.collectionID,
			SourceCollectionId: s.collectionID,
			SnapshotName:       snapshotName,
			State:              datapb.CopySegmentJobState_CopySegmentJobExecuting,
			IdMappings: []*datapb.CopySegmentIDMapping{
				{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10},
			},
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.copyMeta.AddJob(context.TODO(), job)

	// Create segments for target segment ID mapping
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            101,
			State:         commonpb.SegmentState_Importing,
			IsImporting:   true,
			NumOfRows:     100,
			CollectionID:  s.collectionID,
			InsertChannel: "ch1",
		},
	}
	s.meta.AddSegment(context.TODO(), segment)

	// Create a completed task
	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		meta:     s.meta,
		tr:       timerecord.NewTimeRecorder("test task"),
		times:    taskcommon.NewTimes(),
	}
	taskProto := &datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		IdMappings:   []*datapb.CopySegmentIDMapping{{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10}},
	}
	task.task.Store(taskProto)
	s.copyMeta.AddTask(context.TODO(), task)

	// Execute: Check copying job
	s.checker.checkCopyingJob(job)

	// Verify: Job marked as Completed and ref released
	updatedJob := s.copyMeta.GetJob(context.TODO(), jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobCompleted, updatedJob.GetState())
}

func (s *CopySegmentCheckerSuite) TestCheckCopyingJob_FailedTask_ReleasesPin() {
	snapshotName := "test_snapshot_task_failed"
	jobID := int64(500)

	// Setup mocks
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Maybe()

	// Setup: Create job in Executing state with a failed task
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:              jobID,
			CollectionId:       s.collectionID,
			SourceCollectionId: s.collectionID,
			SnapshotName:       snapshotName,
			State:              datapb.CopySegmentJobState_CopySegmentJobExecuting,
			IdMappings: []*datapb.CopySegmentIDMapping{
				{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10},
			},
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.copyMeta.AddJob(context.TODO(), job)

	// Create a failed task
	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		meta:     s.meta,
		tr:       timerecord.NewTimeRecorder("test task"),
		times:    taskcommon.NewTimes(),
	}
	taskProto := &datapb.CopySegmentTask{
		TaskId:       1002,
		JobId:        jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskFailed,
		Reason:       "test failure",
		IdMappings:   []*datapb.CopySegmentIDMapping{{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10}},
	}
	task.task.Store(taskProto)
	s.copyMeta.AddTask(context.TODO(), task)

	// Execute: Check copying job - should detect failed task and mark job as Failed
	s.checker.checkCopyingJob(job)

	// Verify: Job marked as Failed and ref released
	updatedJob := s.copyMeta.GetJob(context.TODO(), jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobFailed, updatedJob.GetState())
}

func (s *CopySegmentCheckerSuite) TestFinishJob_PublishFailureKeepsJobExecuting() {
	snapshotName := "test_snapshot_flush_fail"
	jobID := int64(600)

	// Setup mocks
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Maybe()
	// The atomic segment-and-job publish fails ambiguously; the checker must
	// fail-stop without changing its process-local job or segment state.
	s.catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("etcd unavailable"))

	// Setup: Create job in Executing state
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:              jobID,
			CollectionId:       s.collectionID,
			SourceCollectionId: s.collectionID,
			SnapshotName:       snapshotName,
			State:              datapb.CopySegmentJobState_CopySegmentJobExecuting,
			IdMappings: []*datapb.CopySegmentIDMapping{
				{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10},
			},
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.copyMeta.AddJob(context.TODO(), job)

	// Create target segment in Growing state (needs flush to Flushed)
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            101,
			State:         commonpb.SegmentState_Growing,
			IsImporting:   true,
			NumOfRows:     100,
			CollectionID:  s.collectionID,
			InsertChannel: "ch1",
		},
	}
	s.meta.AddSegment(context.TODO(), segment)

	// Create a completed task
	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		meta:     s.meta,
		tr:       timerecord.NewTimeRecorder("test task"),
		times:    taskcommon.NewTimes(),
	}
	taskProto := &datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		IdMappings:   []*datapb.CopySegmentIDMapping{{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10}},
	}
	task.task.Store(taskProto)
	s.copyMeta.AddTask(context.TODO(), task)

	fatalCalled := false
	mockFatal := mockey.Mock(mlog.Fatal).
		To(func(context.Context, string, ...mlog.Field) { fatalCalled = true }).
		Build()
	defer mockFatal.UnPatch()

	// Execute: Check copying job - atomic publish fails ambiguously.
	s.checker.checkCopyingJob(job)

	// Fatal does not return in production. With the test hook, verify the
	// process-local state remains untouched after the fail-stop decision.
	s.True(fatalCalled)
	updatedJob := s.copyMeta.GetJob(context.TODO(), jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobExecuting, updatedJob.GetState())
	updatedSegment := s.meta.GetSegment(context.TODO(), 101)
	s.Equal(commonpb.SegmentState_Growing, updatedSegment.GetState())
	s.True(updatedSegment.GetIsImporting())
}
