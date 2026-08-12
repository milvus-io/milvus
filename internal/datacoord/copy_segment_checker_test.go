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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
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
	s.meta.AddCollection(&collectionInfo{
		ID:     s.collectionID,
		Schema: newTestSchema(),
	})

	s.copyMeta, err = NewCopySegmentMeta(context.TODO(), s.catalog, s.meta, nil, nil)
	s.NoError(err)

	s.checker = NewCopySegmentChecker(
		context.TODO(),
		s.meta,
		s.broker,
		s.alloc,
		s.copyMeta,
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
	applied, err := s.copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), s.jobID,
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
	// One call for AddJob, one for update progress, one for finishJob
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(3)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Create segments
	seg1 := NewSegmentInfo(&datapb.SegmentInfo{
		ID:            101,
		CollectionID:  s.collectionID,
		PartitionID:   10,
		State:         commonpb.SegmentState_Importing,
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

// TestTryTimeoutJob_BoundsPublishingRetry: Publishing has no exit other than a
// successful FinalizeJobPublication, so the job deadline must apply to it —
// otherwise a persistent publication failure (etcd quota, an oversized write
// that keeps failing, a target segment gone from meta) leaves the job in
// Publishing forever: never timed out, never reclaimed by GC, reported to the
// user as an executing restore, its source pin held until the pin TTL.
func (s *CopySegmentCheckerSuite) TestTryTimeoutJob_BoundsPublishingRetry() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(2)

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobPublishing,
			TimeoutTs:    CopyJobTimeoutTs(-time.Minute),
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))

	s.checker.tryTimeoutJob(job)

	updatedJob := s.copyMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobFailed, updatedJob.GetState())
	s.Contains(updatedJob.GetReason(), "timeout")
	s.Contains(updatedJob.GetReason(), "publishing")
}

// TestTryTimeoutJob_PublishingNotElapsedKeepsRetrying: until the deadline a
// Publishing job is left alone so retryPublishingJob can complete it.
func (s *CopySegmentCheckerSuite) TestTryTimeoutJob_PublishingNotElapsedKeepsRetrying() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(1)

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobPublishing,
			TimeoutTs:    CopyJobTimeoutTs(time.Hour),
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))

	s.checker.tryTimeoutJob(job)

	s.Equal(datapb.CopySegmentJobState_CopySegmentJobPublishing,
		s.copyMeta.GetJob(context.TODO(), s.jobID).GetState())
}

// TestFinishJob_TimedOutPublishingJobIsNotPublished: once the deadline has
// converged a Publishing job to Failed, a later retry round must not publish
// its targets — the outcome fence still holds in the failure direction, and
// the inspector's failed-job cleanup owns the targets from here.
func (s *CopySegmentCheckerSuite) TestFinishJob_TimedOutPublishingJobIsNotPublished() {
	jobID := int64(601)
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Maybe()
	// No catalog.Update expectation: publication must not be attempted.

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobPublishing,
			TimeoutTs:    CopyJobTimeoutTs(-time.Minute),
			IdMappings: []*datapb.CopySegmentIDMapping{
				{SourceSegmentId: 1, TargetSegmentId: 111, PartitionId: 10},
			},
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))
	s.meta.AddSegment(context.TODO(), &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 111, State: commonpb.SegmentState_Importing, IsImporting: true,
		NumOfRows: 100, CollectionID: s.collectionID, InsertChannel: "ch1",
	}})
	task := &copySegmentTask{copyMeta: s.copyMeta, meta: s.meta, tr: timerecord.NewTimeRecorder("t"), times: taskcommon.NewTimes()}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId: 1101, JobId: jobID, CollectionId: s.collectionID,
		State:      datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		IdMappings: []*datapb.CopySegmentIDMapping{{SourceSegmentId: 1, TargetSegmentId: 111, PartitionId: 10}},
	})
	s.NoError(s.copyMeta.AddTask(context.TODO(), task))

	// Same round order as the checker loop for a stale Publishing snapshot:
	// timeout first this time, then the retry sees Failed and stops.
	s.checker.tryTimeoutJob(job)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobFailed,
		s.copyMeta.GetJob(context.TODO(), jobID).GetState())

	s.checker.retryPublishingJob(job)

	s.Equal(datapb.CopySegmentJobState_CopySegmentJobFailed,
		s.copyMeta.GetJob(context.TODO(), jobID).GetState())
	target := s.meta.GetSegment(context.TODO(), 111)
	s.Equal(commonpb.SegmentState_Importing, target.GetState())
	s.True(target.GetIsImporting())
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

// TestCheckGC_ReclaimsTaskConvergedAfterWorkerLoss is the GC end of the
// worker-loss regression: a confirmed worker loss that lands after the parent
// job already failed must not leave the task InProgress on the dead node —
// checkGC skips any task with a node assignment, so task and job would be
// retained forever. After ResolveTaskOnWorkerLoss converges the task to
// Failed + NullNodeID, GC must reclaim both.
func (s *CopySegmentCheckerSuite) TestCheckGC_ReclaimsTaskConvergedAfterWorkerLoss() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().DropCopySegmentTask(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().DropCopySegmentJob(mock.Anything, mock.Anything).Return(nil)

	// Failed job whose retention has already expired.
	cleanupTs := tsoutil.ComposeTSByTime(time.Now().Add(-1 * time.Hour))
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobFailed,
			CleanupTs:    cleanupTs,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))

	// InProgress task still assigned to the (now dead) node 10. Its target
	// segment is absent from meta (already cleaned up by the inspector).
	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
		NodeId:       10,
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 1, TargetSegmentId: 2001, PartitionId: 10},
		},
	})
	s.NoError(s.copyMeta.AddTask(context.TODO(), task))

	// Before the loss is resolved, GC must keep both: the task still has a
	// node assignment.
	s.checker.checkGC(job)
	s.NotNil(s.copyMeta.GetTask(context.TODO(), 1001))
	s.NotNil(s.copyMeta.GetJob(context.TODO(), s.jobID))

	// The delayed worker-loss response arrives; the parent job is terminal, so
	// the task converges to Failed + NullNodeID.
	outcome, err := s.copyMeta.ResolveTaskOnWorkerLoss(context.TODO(), 1001, "worker lost")
	s.NoError(err)
	s.Equal(workerLossFailed, outcome.resolution)

	// Now GC reclaims task and job.
	s.checker.checkGC(job)
	s.Nil(s.copyMeta.GetTask(context.TODO(), 1001))
	s.Nil(s.copyMeta.GetJob(context.TODO(), s.jobID))
}

// TestCheckGC_ReclaimsFailedJobWithCompletedSibling is the GC end of the
// all-or-nothing restore regression: under a Failed job, checkGC keeps every
// task — the Completed sibling included — while any of them still has a target
// segment in meta. Once the inspector has dropped those segments (job-scoped
// cleanup, not task-scoped), both tasks and the job must be reclaimed.
func (s *CopySegmentCheckerSuite) TestCheckGC_ReclaimsFailedJobWithCompletedSibling() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().DropCopySegmentTask(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().DropCopySegmentJob(mock.Anything, mock.Anything).Return(nil)

	cleanupTs := tsoutil.ComposeTSByTime(time.Now().Add(-1 * time.Hour))
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobFailed,
			CleanupTs:    cleanupTs,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))

	// The completed sibling's target segment is still in meta, Flushed.
	seg := NewSegmentInfo(&datapb.SegmentInfo{
		ID: 2002, CollectionID: s.collectionID, PartitionID: 10,
		State: commonpb.SegmentState_Flushed, NumOfRows: 100, InsertChannel: "ch1",
	})
	s.NoError(s.meta.AddSegment(context.TODO(), seg))

	addTask := func(taskID, targetSegID int64, state datapb.CopySegmentTaskState) {
		t := &copySegmentTask{
			copyMeta: s.copyMeta,
			tr:       timerecord.NewTimeRecorder("task"),
			times:    taskcommon.NewTimes(),
		}
		t.task.Store(&datapb.CopySegmentTask{
			TaskId:       taskID,
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        state,
			NodeId:       NullNodeID,
			IdMappings: []*datapb.CopySegmentIDMapping{
				{SourceSegmentId: 1, TargetSegmentId: targetSegID, PartitionId: 10},
			},
		})
		s.NoError(s.copyMeta.AddTask(context.TODO(), t))
	}
	// Completed sibling (segment still present) and the failed task whose
	// segment the inspector already dropped.
	addTask(1002, 2002, datapb.CopySegmentTaskState_CopySegmentTaskCompleted)
	addTask(1003, 2003, datapb.CopySegmentTaskState_CopySegmentTaskFailed)

	// The completed sibling's lingering segment pins that task, and pinning any
	// task keeps the whole job. (The failed task, whose segment the inspector
	// already dropped, is reclaimed right away — GC removes tasks individually.)
	s.checker.checkGC(job)
	s.NotNil(s.copyMeta.GetTask(context.TODO(), 1002),
		"the completed sibling must be retained while its segment is still in meta")
	s.NotNil(s.copyMeta.GetJob(context.TODO(), s.jobID),
		"the job must be retained as long as any of its tasks is")

	// Job-scoped cleanup drops it (what the inspector now does for a Failed
	// job's completed tasks) and removes it from meta.
	s.NoError(s.meta.UpdateSegmentsInfo(context.TODO(),
		UpdateStatusOperator(2002, commonpb.SegmentState_Dropped)))
	s.meta.segments.DropSegment(2002)

	s.checker.checkGC(job)
	s.Nil(s.copyMeta.GetTask(context.TODO(), 1002))
	s.Nil(s.copyMeta.GetTask(context.TODO(), 1003))
	s.Nil(s.copyMeta.GetJob(context.TODO(), s.jobID),
		"job and both tasks are reclaimed once no target segment is left")
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
	// Close should be idempotent
	s.NotPanics(func() {
		s.checker.Close()
		s.checker.Close()
	})
}

func (s *CopySegmentCheckerSuite) TestFinishJob_UpdateSegmentStates() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(2)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Create target segments in Importing state
	seg1 := NewSegmentInfo(&datapb.SegmentInfo{
		ID:            101,
		CollectionID:  s.collectionID,
		PartitionID:   10,
		State:         commonpb.SegmentState_Importing,
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
	s.checker.finishJob(job, 100)

	// Target segment should be in Flushed state
	segment := s.meta.GetSegment(context.TODO(), 101)
	s.Equal(commonpb.SegmentState_Flushed, segment.GetState())

	// Job should be completed
	updatedJob := s.copyMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobCompleted, updatedJob.GetState())
	s.Equal(int64(100), updatedJob.GetTotalRows())
}

func (s *CopySegmentCheckerSuite) TestUpdateJobStateAndReleaseRef_Completed() {
	snapshotName := "test_snapshot"
	jobID := int64(100)

	// Setup: Create job and increment ref count
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:              jobID,
			CollectionId:       s.collectionID,
			SourceCollectionId: s.collectionID,
			SnapshotName:       snapshotName,
			State:              datapb.CopySegmentJobState_CopySegmentJobPending,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.copyMeta.AddJob(context.TODO(), job)

	// Execute: Update job to Completed via atomic meta method
	applied, err := s.copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), jobID,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted))
	s.NoError(err)
	s.True(applied)

	// Verify: Ref count is released
}

func (s *CopySegmentCheckerSuite) TestUpdateJobStateAndReleaseRef_Failed() {
	snapshotName := "test_snapshot_fail"
	jobID := int64(101)

	// Setup: Create job and increment ref count
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:              jobID,
			CollectionId:       s.collectionID,
			SourceCollectionId: s.collectionID,
			SnapshotName:       snapshotName,
			State:              datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.copyMeta.AddJob(context.TODO(), job)

	// Execute: Update job to Failed via atomic meta method
	applied, err := s.copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), jobID,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed))
	s.NoError(err)
	s.True(applied)

	// Verify: Ref count is released
}

func (s *CopySegmentCheckerSuite) TestUpdateJobStateAndReleaseRef_Executing() {
	snapshotName := "test_snapshot_exec"
	jobID := int64(102)

	// Setup: Create job and increment ref count
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:              jobID,
			CollectionId:       s.collectionID,
			SourceCollectionId: s.collectionID,
			SnapshotName:       snapshotName,
			State:              datapb.CopySegmentJobState_CopySegmentJobPending,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.copyMeta.AddJob(context.TODO(), job)

	// Execute: Update job to Executing (non-terminal state) via atomic meta method
	applied, err := s.copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), jobID,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobExecuting))
	s.NoError(err)
	s.True(applied)

	// Verify: Ref count is NOT released
}

func (s *CopySegmentCheckerSuite) TestCheckPendingJob_NoMappings_ReleasesRef() {
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

func (s *CopySegmentCheckerSuite) TestCheckCopyingJob_AllTasksDone_ReleasesRef() {
	snapshotName := "test_snapshot_copying_completed"
	jobID := int64(400)

	// Setup mocks: SaveCopySegmentJob is called once for AddJob, once for finishJob update
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Maybe()

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
			State:         commonpb.SegmentState_Growing,
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

func (s *CopySegmentCheckerSuite) TestCheckCopyingJob_FailedTask_ReleasesRef() {
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

func (s *CopySegmentCheckerSuite) TestFinishJob_PublicationFailureStaysPublishing() {
	snapshotName := "test_snapshot_flush_fail"
	jobID := int64(600)

	// Setup mocks
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Maybe()
	// The composite publication fails before its Completed commit marker.
	s.catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("etcd unavailable")).Once()

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
				{SourceSegmentId: 2, TargetSegmentId: 102, PartitionId: 10},
			},
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.copyMeta.AddJob(context.TODO(), job)

	for _, segmentID := range []int64{101, 102} {
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segmentID,
				State:         commonpb.SegmentState_Importing,
				IsImporting:   true,
				NumOfRows:     100,
				CollectionID:  s.collectionID,
				InsertChannel: "ch1",
			},
		}
		s.meta.AddSegment(context.TODO(), segment)
	}

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
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10},
			{SourceSegmentId: 2, TargetSegmentId: 102, PartitionId: 10},
		},
	}
	task.task.Store(taskProto)
	s.copyMeta.AddTask(context.TODO(), task)

	// Execute: Check copying job - publication will fail after success is claimed.
	s.checker.checkCopyingJob(job)

	// Verify: Publishing is an outcome fence and the target remains hidden.
	updatedJob := s.copyMeta.GetJob(context.TODO(), jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobPublishing, updatedJob.GetState())
	for _, segmentID := range []int64{101, 102} {
		updatedSegment := s.meta.GetSegment(context.TODO(), segmentID)
		s.Equal(commonpb.SegmentState_Importing, updatedSegment.GetState())
		s.True(updatedSegment.GetIsImporting())
	}

	// A later checker round retries from Publishing and commits visibility plus
	// Completed together; no task work is re-run.
	s.catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	s.checker.retryPublishingJob(updatedJob)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobCompleted,
		s.copyMeta.GetJob(context.TODO(), jobID).GetState())
	for _, segmentID := range []int64{101, 102} {
		updatedSegment := s.meta.GetSegment(context.TODO(), segmentID)
		s.Equal(commonpb.SegmentState_Flushed, updatedSegment.GetState())
		s.False(updatedSegment.GetIsImporting())
	}
}

// TestFinishJob_SkipsWhenJobAlreadyTerminal: finishJob acting on a stale
// snapshot of a job that concurrently failed must not report completion — the
// guarded transition is skipped and the Failed outcome is preserved.
func (s *CopySegmentCheckerSuite) TestFinishJob_SkipsWhenJobAlreadyTerminal() {
	// Only the AddJob write; the skipped Completed transition must not reach
	// the catalog.
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(1)

	currentJob := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobFailed,
			Reason:       "task failed concurrently",
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), currentJob))

	// finishJob still holds the pre-failure Executing snapshot.
	staleJob := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.checker.finishJob(staleJob, 123)

	saved := s.copyMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobFailed, saved.GetState())
	s.Equal("task failed concurrently", saved.GetReason())
	s.Zero(saved.GetTotalRows())
}

// TestFinishJob_PublishesAllTargetSegments is the job-level publication
// barrier: individual tasks leave their targets Importing on completion, so a
// collection loaded while the restore is still running cannot serve a
// half-restored snapshot. finishJob flips every target segment to Flushed and
// clears IsImporting in one batch, once the whole job has completed.
func (s *CopySegmentCheckerSuite) TestFinishJob_PublishesAllTargetSegments() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	for _, segID := range []int64{3101, 3102} {
		s.NoError(s.meta.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{
			ID: segID, CollectionID: s.collectionID, PartitionID: 10,
			State: commonpb.SegmentState_Importing, IsImporting: true,
			NumOfRows: 100, InsertChannel: "ch1",
		})))
	}

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))

	for i, segID := range []int64{3101, 3102} {
		t := &copySegmentTask{
			copyMeta: s.copyMeta,
			tr:       timerecord.NewTimeRecorder("task"),
			times:    taskcommon.NewTimes(),
		}
		t.task.Store(&datapb.CopySegmentTask{
			TaskId:       int64(1020 + i),
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
			NodeId:       NullNodeID,
			IdMappings: []*datapb.CopySegmentIDMapping{
				{SourceSegmentId: int64(1 + i), TargetSegmentId: segID, PartitionId: 10},
			},
		})
		s.NoError(s.copyMeta.AddTask(context.TODO(), t))
	}

	s.checker.finishJob(job, 200)

	for _, segID := range []int64{3101, 3102} {
		seg := s.meta.GetSegment(context.TODO(), segID)
		s.Equal(commonpb.SegmentState_Flushed, seg.GetState(), "segment %d must be published", segID)
		s.False(seg.GetIsImporting(), "segment %d must no longer be importing", segID)
	}
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobCompleted,
		s.copyMeta.GetJob(context.TODO(), s.jobID).GetState())
}

// TestFinishJob_TerminalJobDoesNotFlushSegments: finishJob makes segments
// queryable in Step 2, before the guarded Completed transition in Step 4. If
// the job went terminal since the checker round's snapshot was taken
// (tryTimeoutJob fired, or a sibling's markTaskAndJobFailed landed), the guard
// correctly refuses the transition — but the visibility would already be
// committed and is never undone. Re-check liveness before flushing anything.
func (s *CopySegmentCheckerSuite) TestFinishJob_TerminalJobDoesNotFlushSegments() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)

	seg := NewSegmentInfo(&datapb.SegmentInfo{
		ID: 3001, CollectionID: s.collectionID, PartitionID: 10,
		State: commonpb.SegmentState_Importing, NumOfRows: 100, InsertChannel: "ch1",
	})
	s.NoError(s.meta.AddSegment(context.TODO(), seg))

	// The job in meta has already gone terminal on another path.
	s.NoError(s.copyMeta.AddJob(context.TODO(), &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobFailed,
			Reason:       "timeout",
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}))

	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1009,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		NodeId:       NullNodeID,
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 1, TargetSegmentId: 3001, PartitionId: 10},
		},
	})
	s.NoError(s.copyMeta.AddTask(context.TODO(), task))

	// finishJob still holds the pre-failure Executing snapshot.
	staleJob := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.checker.finishJob(staleJob, 123)

	s.Equal(commonpb.SegmentState_Importing, s.meta.GetSegment(context.TODO(), 3001).GetState(),
		"a job that already failed must not have its segments made queryable")
	saved := s.copyMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobFailed, saved.GetState())
	s.Equal("timeout", saved.GetReason())
}

// TestTryTimeoutJob_CatalogErrorKeepsState: a failed persist of the timeout
// transition must leave the job state unchanged (retried next round) and must
// not report a timeout that did not commit.
func (s *CopySegmentCheckerSuite) TestTryTimeoutJob_CatalogErrorKeepsState() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Once()
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).
		Return(errors.New("catalog down")).Once()

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
			TimeoutTs:    CopyJobTimeoutTs(-time.Minute),
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))

	s.checker.tryTimeoutJob(job)

	saved := s.copyMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobExecuting, saved.GetState())
	s.NotEqual("timeout", saved.GetReason())
}

// TestTryTimeoutJob_DoesNotOverwriteTerminalJob is the regression test for the
// review nit on tryTimeoutJob: `job` is the snapshot captured before the checker
// round began. In Start(), checkCopyingJob(job) runs first and may finish the
// job (-> Completed) in that same round; tryTimeoutJob(job) then runs with the
// still-Executing snapshot and, once the deadline has elapsed, would flip the
// just-Completed job to Failed. The transition must be conditional on the
// current state, not the snapshot's.
func (s *CopySegmentCheckerSuite) TestTryTimeoutJob_DoesNotOverwriteTerminalJob() {
	// AddJob + the concurrent Completed transition. The timeout transition must
	// not reach the catalog.
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(2)

	staleJob := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
			TimeoutTs:    CopyJobTimeoutTs(-time.Minute),
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), staleJob))

	// A concurrent path (finishJob in the same checker round) completes the job
	// after the snapshot was taken.
	applied, err := s.copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), s.jobID,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted),
		UpdateCopyJobTotalRows(123))
	s.NoError(err)
	s.True(applied)

	// tryTimeoutJob still holds the stale Executing snapshot with an elapsed deadline.
	s.checker.tryTimeoutJob(staleJob)

	updatedJob := s.copyMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobCompleted, updatedJob.GetState())
	s.NotEqual("timeout", updatedJob.GetReason())
	s.Equal(int64(123), updatedJob.GetTotalRows())
}
