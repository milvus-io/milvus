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
	"math"
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
)

type CopySegmentMetaSuite struct {
	suite.Suite

	collectionID int64

	catalog  *mocks.DataCoordCatalog
	broker   *broker.MockBroker
	meta     *meta
	copyMeta CopySegmentMeta
}

func (s *CopySegmentMetaSuite) SetupTest() {
	var err error

	s.collectionID = 1

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
}

func TestCopySegmentMeta(t *testing.T) {
	suite.Run(t, new(CopySegmentMetaSuite))
}

func (s *CopySegmentMetaSuite) TestNewCopySegmentMeta_Success() {
	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCopySegmentTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

	broker := broker.NewMockBroker(s.T())
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)

	meta, err := newMeta(context.TODO(), catalog, nil, broker)
	s.NoError(err)

	copyMeta, err := NewCopySegmentMeta(context.TODO(), catalog, meta, nil)
	s.NoError(err)
	s.NotNil(copyMeta)
}

func (s *CopySegmentMetaSuite) TestNewCopySegmentMeta_ListJobsError() {
	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(nil, errors.New("list jobs error"))

	copyMeta, err := NewCopySegmentMeta(context.TODO(), catalog, nil, nil)
	s.Error(err)
	s.Nil(copyMeta)
	s.Contains(err.Error(), "list jobs error")
}

func (s *CopySegmentMetaSuite) TestNewCopySegmentMeta_ListTasksError() {
	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCopySegmentTasks(mock.Anything).Return(nil, errors.New("list tasks error"))

	copyMeta, err := NewCopySegmentMeta(context.TODO(), catalog, nil, nil)
	s.Error(err)
	s.Nil(copyMeta)
	s.Contains(err.Error(), "list tasks error")
}

func (s *CopySegmentMetaSuite) TestNewCopySegmentMeta_RestoreJobs() {
	catalog := mocks.NewDataCoordCatalog(s.T())

	restoredJobs := []*datapb.CopySegmentJob{
		{
			JobId:        100,
			CollectionId: 1,
			State:        datapb.CopySegmentJobState_CopySegmentJobPending,
		},
		{
			JobId:        200,
			CollectionId: 1,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
	}

	catalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(restoredJobs, nil)
	catalog.EXPECT().ListCopySegmentTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

	broker := broker.NewMockBroker(s.T())
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)

	meta, err := newMeta(context.TODO(), catalog, nil, broker)
	s.NoError(err)

	copyMeta, err := NewCopySegmentMeta(context.TODO(), catalog, meta, nil)
	s.NoError(err)

	// Verify jobs are restored
	job1 := copyMeta.GetJob(context.TODO(), 100)
	s.NotNil(job1)
	s.Equal(int64(100), job1.GetJobId())

	job2 := copyMeta.GetJob(context.TODO(), 200)
	s.NotNil(job2)
	s.Equal(int64(200), job2.GetJobId())
}

func (s *CopySegmentMetaSuite) TestNewCopySegmentMeta_RestoreTasks() {
	catalog := mocks.NewDataCoordCatalog(s.T())

	restoredTasks := []*datapb.CopySegmentTask{
		{
			TaskId:       1001,
			JobId:        100,
			CollectionId: 1,
			State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
		},
		{
			TaskId:       1002,
			JobId:        100,
			CollectionId: 1,
			State:        datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
		},
	}

	catalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCopySegmentTasks(mock.Anything).Return(restoredTasks, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

	broker := broker.NewMockBroker(s.T())
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)

	meta, err := newMeta(context.TODO(), catalog, nil, broker)
	s.NoError(err)

	copyMeta, err := NewCopySegmentMeta(context.TODO(), catalog, meta, nil)
	s.NoError(err)

	// Verify tasks are restored
	task1 := copyMeta.GetTask(context.TODO(), 1001)
	s.NotNil(task1)
	s.Equal(int64(1001), task1.GetTaskId())

	task2 := copyMeta.GetTask(context.TODO(), 1002)
	s.NotNil(task2)
	s.Equal(int64(1002), task2.GetTaskId())
}

func (s *CopySegmentMetaSuite) TestAddJob_Success() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobPending,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	err := s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Verify job is added
	retrievedJob := s.copyMeta.GetJob(context.TODO(), 100)
	s.NotNil(retrievedJob)
	s.Equal(int64(100), retrievedJob.GetJobId())
}

func (s *CopySegmentMetaSuite) TestAddJob_CatalogError() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(errors.New("catalog error"))

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: s.collectionID,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	err := s.copyMeta.AddJob(context.TODO(), job)
	s.Error(err)
	s.Contains(err.Error(), "catalog error")

	// Verify job is not added
	retrievedJob := s.copyMeta.GetJob(context.TODO(), 100)
	s.Nil(retrievedJob)
}

func (s *CopySegmentMetaSuite) TestUpdateJob_Success() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(2)

	// Add a job first
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobPending,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	err := s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Update the job
	err = s.copyMeta.UpdateJob(context.TODO(), 100,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobExecuting),
		UpdateCopyJobReason("executing"))
	s.NoError(err)

	// Verify job is updated
	updatedJob := s.copyMeta.GetJob(context.TODO(), 100)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobExecuting, updatedJob.GetState())
	s.Equal("executing", updatedJob.GetReason())
}

func (s *CopySegmentMetaSuite) TestUpdateJob_NotFound() {
	// Try to update non-existent job (should not error, just no-op)
	err := s.copyMeta.UpdateJob(context.TODO(), 999,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobExecuting))
	s.NoError(err)
}

func (s *CopySegmentMetaSuite) TestUpdateJob_CatalogError() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Once()
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(errors.New("catalog error")).Once()

	// Add a job first
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobPending,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	err := s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Update should fail
	err = s.copyMeta.UpdateJob(context.TODO(), 100,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobExecuting))
	s.Error(err)
	s.Contains(err.Error(), "catalog error")
}

func (s *CopySegmentMetaSuite) TestGetJob() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: s.collectionID,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.copyMeta.AddJob(context.TODO(), job)

	// Get existing job
	retrievedJob := s.copyMeta.GetJob(context.TODO(), 100)
	s.NotNil(retrievedJob)
	s.Equal(int64(100), retrievedJob.GetJobId())

	// Get non-existent job
	nonExistent := s.copyMeta.GetJob(context.TODO(), 999)
	s.Nil(nonExistent)
}

func (s *CopySegmentMetaSuite) TestGetJobBy() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(3)

	// Add multiple jobs
	jobs := []*copySegmentJob{
		{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:        100,
				CollectionId: 1,
				State:        datapb.CopySegmentJobState_CopySegmentJobPending,
			},
			tr: timerecord.NewTimeRecorder("job1"),
		},
		{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:        200,
				CollectionId: 1,
				State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
			},
			tr: timerecord.NewTimeRecorder("job2"),
		},
		{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:        300,
				CollectionId: 2,
				State:        datapb.CopySegmentJobState_CopySegmentJobPending,
			},
			tr: timerecord.NewTimeRecorder("job3"),
		},
	}

	for _, job := range jobs {
		s.copyMeta.AddJob(context.TODO(), job)
	}

	// Filter by collection ID
	filtered := s.copyMeta.GetJobBy(context.TODO(), WithCopyJobCollectionID(1))
	s.Len(filtered, 2)

	// Filter by state
	filtered = s.copyMeta.GetJobBy(context.TODO(),
		WithCopyJobStates(datapb.CopySegmentJobState_CopySegmentJobPending))
	s.Len(filtered, 2)

	// Filter by collection ID and state
	filtered = s.copyMeta.GetJobBy(context.TODO(),
		WithCopyJobCollectionID(1),
		WithCopyJobStates(datapb.CopySegmentJobState_CopySegmentJobPending))
	s.Len(filtered, 1)
	s.Equal(int64(100), filtered[0].GetJobId())
}

func (s *CopySegmentMetaSuite) TestCountJobBy() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(3)

	// Add multiple jobs
	for i := 0; i < 3; i++ {
		job := &copySegmentJob{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:        int64(100 + i),
				CollectionId: s.collectionID,
				State:        datapb.CopySegmentJobState_CopySegmentJobPending,
			},
			tr: timerecord.NewTimeRecorder("job"),
		}
		s.copyMeta.AddJob(context.TODO(), job)
	}

	count := s.copyMeta.CountJobBy(context.TODO(),
		WithCopyJobStates(datapb.CopySegmentJobState_CopySegmentJobPending))
	s.Equal(3, count)

	count = s.copyMeta.CountJobBy(context.TODO(),
		WithCopyJobStates(datapb.CopySegmentJobState_CopySegmentJobExecuting))
	s.Equal(0, count)
}

func (s *CopySegmentMetaSuite) TestRemoveJob_Success() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().DropCopySegmentJob(mock.Anything, int64(100)).Return(nil)

	// Add a job
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: s.collectionID,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.copyMeta.AddJob(context.TODO(), job)

	// Remove the job
	err := s.copyMeta.RemoveJob(context.TODO(), 100)
	s.NoError(err)

	// Verify job is removed
	retrievedJob := s.copyMeta.GetJob(context.TODO(), 100)
	s.Nil(retrievedJob)
}

func (s *CopySegmentMetaSuite) TestRemoveJob_NotFound() {
	// Remove non-existent job (should not error)
	err := s.copyMeta.RemoveJob(context.TODO(), 999)
	s.NoError(err)
}

func (s *CopySegmentMetaSuite) TestRemoveJob_CatalogError() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().DropCopySegmentJob(mock.Anything, int64(100)).Return(errors.New("catalog error"))

	// Add a job
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: s.collectionID,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.copyMeta.AddJob(context.TODO(), job)

	// Remove should fail
	err := s.copyMeta.RemoveJob(context.TODO(), 100)
	s.Error(err)
	s.Contains(err.Error(), "catalog error")

	// Job should still exist in memory
	retrievedJob := s.copyMeta.GetJob(context.TODO(), 100)
	s.NotNil(retrievedJob)
}

func (s *CopySegmentMetaSuite) TestAddTask_Success() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)

	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})

	err := s.copyMeta.AddTask(context.TODO(), task)
	s.NoError(err)

	// Verify task is added
	retrievedTask := s.copyMeta.GetTask(context.TODO(), 1001)
	s.NotNil(retrievedTask)
	s.Equal(int64(1001), retrievedTask.GetTaskId())
}

func (s *CopySegmentMetaSuite) TestAddTask_CatalogError() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(errors.New("catalog error"))

	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: s.collectionID,
	})

	err := s.copyMeta.AddTask(context.TODO(), task)
	s.Error(err)
	s.Contains(err.Error(), "catalog error")

	// Verify task is not added
	retrievedTask := s.copyMeta.GetTask(context.TODO(), 1001)
	s.Nil(retrievedTask)
}

func (s *CopySegmentMetaSuite) TestUpdateTask_Success() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(2)

	// Add a task first
	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})
	err := s.copyMeta.AddTask(context.TODO(), task)
	s.NoError(err)

	// Update the task
	err = s.copyMeta.UpdateTask(context.TODO(), 1001,
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskInProgress),
		UpdateCopyTaskReason("executing"))
	s.NoError(err)

	// Verify task is updated
	updatedTask := s.copyMeta.GetTask(context.TODO(), 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskInProgress, updatedTask.GetState())
	s.Equal("executing", updatedTask.GetReason())
}

func (s *CopySegmentMetaSuite) TestUpdateTask_NotFound() {
	// Try to update non-existent task (should not error, just no-op)
	err := s.copyMeta.UpdateTask(context.TODO(), 9999,
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskInProgress))
	s.NoError(err)
}

func (s *CopySegmentMetaSuite) TestGetTask() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)

	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: s.collectionID,
	})
	s.copyMeta.AddTask(context.TODO(), task)

	// Get existing task
	retrievedTask := s.copyMeta.GetTask(context.TODO(), 1001)
	s.NotNil(retrievedTask)
	s.Equal(int64(1001), retrievedTask.GetTaskId())

	// Get non-existent task
	nonExistent := s.copyMeta.GetTask(context.TODO(), 9999)
	s.Nil(nonExistent)
}

func (s *CopySegmentMetaSuite) TestGetTaskBy() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(3)

	// Add multiple tasks
	tasks := []struct {
		taskID int64
		jobID  int64
		state  datapb.CopySegmentTaskState
	}{
		{1001, 100, datapb.CopySegmentTaskState_CopySegmentTaskPending},
		{1002, 100, datapb.CopySegmentTaskState_CopySegmentTaskInProgress},
		{1003, 200, datapb.CopySegmentTaskState_CopySegmentTaskPending},
	}

	for _, t := range tasks {
		task := &copySegmentTask{
			copyMeta: s.copyMeta,
			tr:       timerecord.NewTimeRecorder("task"),
			times:    taskcommon.NewTimes(),
		}
		task.task.Store(&datapb.CopySegmentTask{
			TaskId:       t.taskID,
			JobId:        t.jobID,
			CollectionId: s.collectionID,
			State:        t.state,
		})
		s.copyMeta.AddTask(context.TODO(), task)
	}

	// Filter by job ID
	filtered := s.copyMeta.GetTaskBy(context.TODO(), WithCopyTaskJob(100))
	s.Len(filtered, 2)

	// Filter by state
	filtered = s.copyMeta.GetTaskBy(context.TODO(),
		WithCopyTaskStates(datapb.CopySegmentTaskState_CopySegmentTaskPending))
	s.Len(filtered, 2)

	// Filter by job ID and state
	filtered = s.copyMeta.GetTaskBy(context.TODO(),
		WithCopyTaskJob(100),
		WithCopyTaskStates(datapb.CopySegmentTaskState_CopySegmentTaskPending))
	s.Len(filtered, 1)
	s.Equal(int64(1001), filtered[0].GetTaskId())
}

func (s *CopySegmentMetaSuite) TestGetTasksByJobID() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(3)

	// Create tasks for different jobs
	tasks := []struct {
		taskID       int64
		jobID        int64
		collectionID int64
	}{
		{taskID: 1001, jobID: 100, collectionID: 1},
		{taskID: 1002, jobID: 100, collectionID: 1},
		{taskID: 1003, jobID: 200, collectionID: 2},
	}

	for _, t := range tasks {
		task := &copySegmentTask{
			copyMeta: s.copyMeta,
			tr:       timerecord.NewTimeRecorder("task"),
			times:    taskcommon.NewTimes(),
		}
		task.task.Store(&datapb.CopySegmentTask{
			TaskId:       t.taskID,
			JobId:        t.jobID,
			CollectionId: t.collectionID,
		})
		s.copyMeta.AddTask(context.TODO(), task)
	}

	// GetTasksByJobID should return tasks for job 100
	result := s.copyMeta.GetTasksByJobID(context.TODO(), 100)
	s.Len(result, 2)
	taskIDs := make([]int64, 0)
	for _, t := range result {
		taskIDs = append(taskIDs, t.GetTaskId())
	}
	s.ElementsMatch([]int64{1001, 1002}, taskIDs)

	// GetTasksByJobID should return tasks for job 200
	result = s.copyMeta.GetTasksByJobID(context.TODO(), 200)
	s.Len(result, 1)
	s.Equal(int64(1003), result[0].GetTaskId())

	// GetTasksByJobID should return empty for non-existent job
	result = s.copyMeta.GetTasksByJobID(context.TODO(), 999)
	s.Len(result, 0)
}

func (s *CopySegmentMetaSuite) TestGetTasksByCollectionID() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(3)

	// Create tasks for different collections
	tasks := []struct {
		taskID       int64
		jobID        int64
		collectionID int64
	}{
		{taskID: 1001, jobID: 100, collectionID: 1},
		{taskID: 1002, jobID: 100, collectionID: 1},
		{taskID: 1003, jobID: 200, collectionID: 2},
	}

	for _, t := range tasks {
		task := &copySegmentTask{
			copyMeta: s.copyMeta,
			tr:       timerecord.NewTimeRecorder("task"),
			times:    taskcommon.NewTimes(),
		}
		task.task.Store(&datapb.CopySegmentTask{
			TaskId:       t.taskID,
			JobId:        t.jobID,
			CollectionId: t.collectionID,
		})
		s.copyMeta.AddTask(context.TODO(), task)
	}

	// GetTasksByCollectionID should return tasks for collection 1
	result := s.copyMeta.GetTasksByCollectionID(context.TODO(), 1)
	s.Len(result, 2)
	taskIDs := make([]int64, 0)
	for _, t := range result {
		taskIDs = append(taskIDs, t.GetTaskId())
	}
	s.ElementsMatch([]int64{1001, 1002}, taskIDs)

	// GetTasksByCollectionID should return tasks for collection 2
	result = s.copyMeta.GetTasksByCollectionID(context.TODO(), 2)
	s.Len(result, 1)
	s.Equal(int64(1003), result[0].GetTaskId())

	// GetTasksByCollectionID should return empty for non-existent collection
	result = s.copyMeta.GetTasksByCollectionID(context.TODO(), 999)
	s.Len(result, 0)
}

func (s *CopySegmentMetaSuite) TestSecondaryIndexCleanup() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(2)
	s.catalog.EXPECT().DropCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(2)

	// Add tasks
	task1 := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task1.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 1,
	})

	task2 := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task2.task.Store(&datapb.CopySegmentTask{
		TaskId:       1002,
		JobId:        100,
		CollectionId: 1,
	})

	s.copyMeta.AddTask(context.TODO(), task1)
	s.copyMeta.AddTask(context.TODO(), task2)

	// Both tasks exist in indexes
	s.Len(s.copyMeta.GetTasksByJobID(context.TODO(), 100), 2)
	s.Len(s.copyMeta.GetTasksByCollectionID(context.TODO(), 1), 2)

	// Remove first task
	err := s.copyMeta.RemoveTask(context.TODO(), 1001)
	s.NoError(err)

	// Index should be updated
	s.Len(s.copyMeta.GetTasksByJobID(context.TODO(), 100), 1)
	s.Len(s.copyMeta.GetTasksByCollectionID(context.TODO(), 1), 1)

	// Remove second task
	err = s.copyMeta.RemoveTask(context.TODO(), 1002)
	s.NoError(err)

	// Index should be empty
	s.Len(s.copyMeta.GetTasksByJobID(context.TODO(), 100), 0)
	s.Len(s.copyMeta.GetTasksByCollectionID(context.TODO(), 1), 0)
}

func (s *CopySegmentMetaSuite) TestRemoveTask_Success() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().DropCopySegmentTask(mock.Anything, int64(1001)).Return(nil)

	// Add a task
	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: s.collectionID,
	})
	s.copyMeta.AddTask(context.TODO(), task)

	// Remove the task
	err := s.copyMeta.RemoveTask(context.TODO(), 1001)
	s.NoError(err)

	// Verify task is removed
	retrievedTask := s.copyMeta.GetTask(context.TODO(), 1001)
	s.Nil(retrievedTask)
}

func (s *CopySegmentMetaSuite) TestRemoveTask_NotFound() {
	// Remove non-existent task (should not error)
	err := s.copyMeta.RemoveTask(context.TODO(), 9999)
	s.NoError(err)
}

func (s *CopySegmentMetaSuite) TestRemoveTask_CatalogError() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().DropCopySegmentTask(mock.Anything, int64(1001)).Return(errors.New("catalog error"))

	// Add a task
	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: s.collectionID,
	})
	s.copyMeta.AddTask(context.TODO(), task)

	// Remove should fail
	err := s.copyMeta.RemoveTask(context.TODO(), 1001)
	s.Error(err)
	s.Contains(err.Error(), "catalog error")

	// Task should still exist in memory
	retrievedTask := s.copyMeta.GetTask(context.TODO(), 1001)
	s.NotNil(retrievedTask)
}

func (s *CopySegmentMetaSuite) TestCopySegmentTasks_Operations() {
	tasks := newCopySegmentTasks()

	// Test empty tasks
	s.Nil(tasks.get(1001))
	s.Empty(tasks.listTasks())

	// Test add
	task1 := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("task1"),
		times: taskcommon.NewTimes(),
	}
	task1.task.Store(&datapb.CopySegmentTask{TaskId: 1001})
	tasks.add(task1)

	task2 := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("task2"),
		times: taskcommon.NewTimes(),
	}
	task2.task.Store(&datapb.CopySegmentTask{TaskId: 1002})
	tasks.add(task2)

	// Test get
	s.NotNil(tasks.get(1001))
	s.NotNil(tasks.get(1002))
	s.Nil(tasks.get(9999))

	// Test listTasks
	allTasks := tasks.listTasks()
	s.Len(allTasks, 2)

	// Test remove
	tasks.remove(1001)
	s.Nil(tasks.get(1001))
	s.NotNil(tasks.get(1002))
	s.Len(tasks.listTasks(), 1)
}

func TestSnapshotRestoreRefTracker(t *testing.T) {
	tracker := NewSnapshotRestoreRefTracker()
	snapshotName := "test_snapshot"

	// Test initial count is 0
	assert.Equal(t, int32(0), tracker.GetRestoreRefCount(snapshotName))

	// Test increment
	tracker.IncrementRestoreRef(snapshotName)
	assert.Equal(t, int32(1), tracker.GetRestoreRefCount(snapshotName))

	tracker.IncrementRestoreRef(snapshotName)
	assert.Equal(t, int32(2), tracker.GetRestoreRefCount(snapshotName))

	// Test decrement
	tracker.DecrementRestoreRef(snapshotName)
	assert.Equal(t, int32(1), tracker.GetRestoreRefCount(snapshotName))

	tracker.DecrementRestoreRef(snapshotName)
	assert.Equal(t, int32(0), tracker.GetRestoreRefCount(snapshotName))

	// Test multiple snapshots
	tracker.IncrementRestoreRef("snapshot_a")
	tracker.IncrementRestoreRef("snapshot_b")
	assert.Equal(t, int32(1), tracker.GetRestoreRefCount("snapshot_a"))
	assert.Equal(t, int32(1), tracker.GetRestoreRefCount("snapshot_b"))

	// Test decrement different snapshots separately
	tracker.DecrementRestoreRef("snapshot_a")
	assert.Equal(t, int32(0), tracker.GetRestoreRefCount("snapshot_a"))
	assert.Equal(t, int32(1), tracker.GetRestoreRefCount("snapshot_b"))
}

func TestSnapshotRestoreRefTracker_Concurrent(t *testing.T) {
	tracker := NewSnapshotRestoreRefTracker()
	snapshotName := "concurrent_snapshot"
	concurrency := 20

	var wg sync.WaitGroup

	// Concurrent increment
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tracker.IncrementRestoreRef(snapshotName)
		}()
	}
	wg.Wait()

	assert.Equal(t, int32(concurrency), tracker.GetRestoreRefCount(snapshotName))

	// Concurrent decrement
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tracker.DecrementRestoreRef(snapshotName)
		}()
	}
	wg.Wait()

	assert.Equal(t, int32(0), tracker.GetRestoreRefCount(snapshotName))
}

func TestSnapshotRestoreRefTracker_UnderflowProtection(t *testing.T) {
	tracker := NewSnapshotRestoreRefTracker()
	snapshotName := "test_snapshot"

	// Should not go negative
	tracker.DecrementRestoreRef(snapshotName)
	assert.Equal(t, int32(0), tracker.GetRestoreRefCount(snapshotName))

	// Multiple decrements should not go negative
	tracker.IncrementRestoreRef(snapshotName)
	tracker.DecrementRestoreRef(snapshotName)
	tracker.DecrementRestoreRef(snapshotName)
	assert.Equal(t, int32(0), tracker.GetRestoreRefCount(snapshotName))
}

func TestSnapshotRestoreRefTracker_OverflowProtection(t *testing.T) {
	tracker := NewSnapshotRestoreRefTracker()
	snapshotName := "test_snapshot"

	// Set ref count to MaxInt32 directly
	tracker.refCount[snapshotName] = math.MaxInt32

	// IncrementRestoreRef should not overflow past MaxInt32
	tracker.IncrementRestoreRef(snapshotName)
	assert.Equal(t, int32(math.MaxInt32), tracker.GetRestoreRefCount(snapshotName))
}

// TestNewCopySegmentMeta_CrashRecoveryRefFiltering verifies that terminal jobs
// (Completed/Failed) do not get their ref counts re-incremented during crash recovery.
// Before the fix, ALL persisted jobs got IncrementRestoreRef, causing permanent ref leaks
// for terminal jobs (no code path decrements them again).
func (s *CopySegmentMetaSuite) TestNewCopySegmentMeta_CrashRecoveryRefFiltering() {
	catalog := mocks.NewDataCoordCatalog(s.T())

	restoredJobs := []*datapb.CopySegmentJob{
		{
			JobId:        100,
			CollectionId: 1,
			SnapshotName: "snap_active",
			State:        datapb.CopySegmentJobState_CopySegmentJobPending,
		},
		{
			JobId:        200,
			CollectionId: 1,
			SnapshotName: "snap_active",
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		{
			JobId:        300,
			CollectionId: 1,
			SnapshotName: "snap_done",
			State:        datapb.CopySegmentJobState_CopySegmentJobCompleted,
		},
		{
			JobId:        400,
			CollectionId: 1,
			SnapshotName: "snap_done",
			State:        datapb.CopySegmentJobState_CopySegmentJobFailed,
		},
	}

	catalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(restoredJobs, nil)
	catalog.EXPECT().ListCopySegmentTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

	broker := broker.NewMockBroker(s.T())
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)

	meta, err := newMeta(context.TODO(), catalog, nil, broker)
	s.NoError(err)

	copyMeta, err := NewCopySegmentMeta(context.TODO(), catalog, meta, nil)
	s.NoError(err)

	// Only active (Pending+Executing) jobs should contribute to ref count
	// snap_active: 2 active jobs => ref count = 2
	s.Equal(int32(2), copyMeta.GetRestoreRefCount("snap_active"))

	// snap_done: 2 terminal jobs (Completed+Failed) => ref count = 0
	s.Equal(int32(0), copyMeta.GetRestoreRefCount("snap_done"))
}

// TestUpdateJobStateAndReleaseRef_DoubleDecrementProtection verifies that calling
// UpdateJobStateAndReleaseRef multiple times on the same job only decrements once.
// Before the fix, multiple callers (checker + task) could both transition the same job
// to Failed, each triggering a decrement.
func (s *CopySegmentMetaSuite) TestUpdateJobStateAndReleaseRef_DoubleDecrementProtection() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)

	snapshotName := "snap_double"
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        500,
			CollectionId: s.collectionID,
			SnapshotName: snapshotName,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.copyMeta.AddJob(context.TODO(), job)
	s.copyMeta.IncrementRestoreRef(snapshotName)
	s.Equal(int32(1), s.copyMeta.GetRestoreRefCount(snapshotName))

	// First call: Executing → Failed (should decrement)
	err := s.copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), 500,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
		UpdateCopyJobReason("task failed"))
	s.NoError(err)
	s.Equal(int32(0), s.copyMeta.GetRestoreRefCount(snapshotName))

	// Second call: Failed → Failed (should NOT decrement again)
	err = s.copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), 500,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
		UpdateCopyJobReason("timeout"))
	s.NoError(err)
	// Ref count should still be 0, not -1 or underflow
	s.Equal(int32(0), s.copyMeta.GetRestoreRefCount(snapshotName))
}

// TestUpdateJobStateAndReleaseRef_NotFound verifies that updating a non-existent
// job is a no-op and does not error.
func (s *CopySegmentMetaSuite) TestUpdateJobStateAndReleaseRef_NotFound() {
	err := s.copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), 999,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed))
	s.NoError(err)
}

// TestUpdateJobStateAndReleaseRef_CatalogError verifies that if the catalog save fails,
// the ref count is NOT decremented (preserving consistency).
func (s *CopySegmentMetaSuite) TestUpdateJobStateAndReleaseRef_CatalogError() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Once()
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(errors.New("catalog error")).Once()

	snapshotName := "snap_err"
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        600,
			CollectionId: s.collectionID,
			SnapshotName: snapshotName,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.copyMeta.AddJob(context.TODO(), job)
	s.copyMeta.IncrementRestoreRef(snapshotName)
	s.Equal(int32(1), s.copyMeta.GetRestoreRefCount(snapshotName))

	// Update fails at catalog layer
	err := s.copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), 600,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed))
	s.Error(err)

	// Ref count should NOT be decremented (job state didn't actually change)
	s.Equal(int32(1), s.copyMeta.GetRestoreRefCount(snapshotName))

	// Job should still be in Executing state
	savedJob := s.copyMeta.GetJob(context.TODO(), 600)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobExecuting, savedJob.GetState())
}
