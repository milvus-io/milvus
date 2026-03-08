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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
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
	s.catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
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

	s.copyMeta, err = NewCopySegmentMeta(context.TODO(), s.catalog, s.meta, nil)
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
	s.catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil)

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
	timeoutTs := tsoutil.ComposeTSByTime(time.Now().Add(-1*time.Hour), 0)
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

func (s *CopySegmentCheckerSuite) TestCheckGC_RemoveCompletedJob() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().DropCopySegmentTask(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().DropCopySegmentJob(mock.Anything, mock.Anything).Return(nil)

	// Create a completed job with cleanup time in the past
	cleanupTs := tsoutil.ComposeTSByTime(time.Now().Add(-1*time.Hour), 0)
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
	s.catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil)

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
			JobId:        jobID,
			CollectionId: s.collectionID,
			SnapshotName: snapshotName,
			State:        datapb.CopySegmentJobState_CopySegmentJobPending,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.copyMeta.AddJob(context.TODO(), job)
	s.copyMeta.IncrementRestoreRef(snapshotName)
	s.Equal(int32(1), s.copyMeta.GetRestoreRefCount(snapshotName))

	// Execute: Update job to Completed via atomic meta method
	err := s.copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), jobID,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted))
	s.NoError(err)

	// Verify: Ref count is released
	s.Equal(int32(0), s.copyMeta.GetRestoreRefCount(snapshotName))
}

func (s *CopySegmentCheckerSuite) TestUpdateJobStateAndReleaseRef_Failed() {
	snapshotName := "test_snapshot_fail"
	jobID := int64(101)

	// Setup: Create job and increment ref count
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        jobID,
			CollectionId: s.collectionID,
			SnapshotName: snapshotName,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.copyMeta.AddJob(context.TODO(), job)
	s.copyMeta.IncrementRestoreRef(snapshotName)
	s.Equal(int32(1), s.copyMeta.GetRestoreRefCount(snapshotName))

	// Execute: Update job to Failed via atomic meta method
	err := s.copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), jobID,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed))
	s.NoError(err)

	// Verify: Ref count is released
	s.Equal(int32(0), s.copyMeta.GetRestoreRefCount(snapshotName))
}

func (s *CopySegmentCheckerSuite) TestUpdateJobStateAndReleaseRef_Executing() {
	snapshotName := "test_snapshot_exec"
	jobID := int64(102)

	// Setup: Create job and increment ref count
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        jobID,
			CollectionId: s.collectionID,
			SnapshotName: snapshotName,
			State:        datapb.CopySegmentJobState_CopySegmentJobPending,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.copyMeta.AddJob(context.TODO(), job)
	s.copyMeta.IncrementRestoreRef(snapshotName)
	s.Equal(int32(1), s.copyMeta.GetRestoreRefCount(snapshotName))

	// Execute: Update job to Executing (non-terminal state) via atomic meta method
	err := s.copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), jobID,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobExecuting))
	s.NoError(err)

	// Verify: Ref count is NOT released
	s.Equal(int32(1), s.copyMeta.GetRestoreRefCount(snapshotName))
}

func (s *CopySegmentCheckerSuite) TestCheckPendingJob_NoMappings_ReleasesRef() {
	snapshotName := "test_snapshot_no_mappings"
	jobID := int64(200)

	// Setup: Create job with no ID mappings
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        jobID,
			CollectionId: s.collectionID,
			SnapshotName: snapshotName,
			State:        datapb.CopySegmentJobState_CopySegmentJobPending,
			IdMappings:   []*datapb.CopySegmentIDMapping{}, // Empty
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.copyMeta.AddJob(context.TODO(), job)
	s.copyMeta.IncrementRestoreRef(snapshotName)
	s.Equal(int32(1), s.copyMeta.GetRestoreRefCount(snapshotName))

	// Execute: Check pending job
	s.checker.checkPendingJob(job)

	// Verify: Job marked as Completed and ref released
	updatedJob := s.copyMeta.GetJob(context.TODO(), jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobCompleted, updatedJob.GetState())
	s.Equal(int32(0), s.copyMeta.GetRestoreRefCount(snapshotName))
}

func (s *CopySegmentCheckerSuite) TestCheckCopyingJob_AllTasksDone_ReleasesRef() {
	snapshotName := "test_snapshot_copying_completed"
	jobID := int64(400)

	// Setup mocks: SaveCopySegmentJob is called once for AddJob, once for finishJob update
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Maybe()

	// Setup: Create job in Executing state with all tasks completed
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        jobID,
			CollectionId: s.collectionID,
			SnapshotName: snapshotName,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
			IdMappings: []*datapb.CopySegmentIDMapping{
				{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10},
			},
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.copyMeta.AddJob(context.TODO(), job)
	s.copyMeta.IncrementRestoreRef(snapshotName)

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

	s.Equal(int32(1), s.copyMeta.GetRestoreRefCount(snapshotName))

	// Execute: Check copying job
	s.checker.checkCopyingJob(job)

	// Verify: Job marked as Completed and ref released
	updatedJob := s.copyMeta.GetJob(context.TODO(), jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobCompleted, updatedJob.GetState())
	s.Equal(int32(0), s.copyMeta.GetRestoreRefCount(snapshotName))
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
			JobId:        jobID,
			CollectionId: s.collectionID,
			SnapshotName: snapshotName,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
			IdMappings: []*datapb.CopySegmentIDMapping{
				{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10},
			},
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.copyMeta.AddJob(context.TODO(), job)
	s.copyMeta.IncrementRestoreRef(snapshotName)

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

	s.Equal(int32(1), s.copyMeta.GetRestoreRefCount(snapshotName))

	// Execute: Check copying job - should detect failed task and mark job as Failed
	s.checker.checkCopyingJob(job)

	// Verify: Job marked as Failed and ref released
	updatedJob := s.copyMeta.GetJob(context.TODO(), jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobFailed, updatedJob.GetState())
	s.Equal(int32(0), s.copyMeta.GetRestoreRefCount(snapshotName))
}

func (s *CopySegmentCheckerSuite) TestFinishJob_FlushFailure_FailsJob() {
	snapshotName := "test_snapshot_flush_fail"
	jobID := int64(600)

	// Setup mocks
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Maybe()
	// AlterSegments returns error to simulate flush failure
	s.catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(errors.New("etcd unavailable"))

	// Setup: Create job in Executing state
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        jobID,
			CollectionId: s.collectionID,
			SnapshotName: snapshotName,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
			IdMappings: []*datapb.CopySegmentIDMapping{
				{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10},
			},
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.copyMeta.AddJob(context.TODO(), job)
	s.copyMeta.IncrementRestoreRef(snapshotName)

	// Create target segment in Growing state (needs flush to Flushed)
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

	s.Equal(int32(1), s.copyMeta.GetRestoreRefCount(snapshotName))

	// Execute: Check copying job - flush will fail due to AlterSegments error
	s.checker.checkCopyingJob(job)

	// Verify: Job marked as Failed (not Completed) due to flush failure
	updatedJob := s.copyMeta.GetJob(context.TODO(), jobID)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobFailed, updatedJob.GetState())
	s.Contains(updatedJob.GetReason(), "failed to flush")

	// Verify: Ref count is released even on failure
	s.Equal(int32(0), s.copyMeta.GetRestoreRefCount(snapshotName))
}
