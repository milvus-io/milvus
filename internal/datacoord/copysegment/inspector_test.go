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
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	task2 "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

type CopySegmentInspectorSuite struct {
	suite.Suite

	collectionID int64
	jobID        int64

	catalog   *mocks.DataCoordCatalog
	broker    *broker.MockBroker
	meta      *meta.Meta
	copyMeta  CopySegmentMeta
	scheduler *task2.MockGlobalScheduler
	inspector *Inspector
}

func (s *CopySegmentInspectorSuite) SetupTest() {
	var err error

	s.collectionID = 1
	s.jobID = 100

	s.catalog = mocks.NewDataCoordCatalog(s.T())
	s.catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	s.catalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListCopySegmentTasks(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	s.catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil).Maybe()
	s.catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil).Maybe()
	s.catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
	s.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil).Maybe()
	s.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil).Maybe()
	s.catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil).Maybe()
	s.catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil).Maybe()
	s.catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil).Maybe()

	s.broker = broker.NewMockBroker(s.T())
	s.broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)

	dm := &index.IndexTestMetas{}
	s.meta, err = meta.NewMeta(context.TODO(), s.catalog, nil, metacache.NewMetaStore(s.catalog), s.broker, index.LoadIndexTestMetas(s.catalog, dm))
	s.NoError(err)
	s.meta.AddCollection(&meta.CollectionInfo{
		ID:     s.collectionID,
		Schema: meta.NewTestSchema(),
	})

	s.copyMeta, err = NewCopySegmentMeta(context.TODO(), s.catalog, s.meta, dm.IndexMeta, nil, nil)
	s.NoError(err)

	s.scheduler = task2.NewMockGlobalScheduler(s.T())

	s.inspector = NewCopySegmentInspector(
		context.TODO(),
		s.meta,
		s.copyMeta,
		s.scheduler,
	).(*Inspector)
}

func (s *CopySegmentInspectorSuite) TearDownTest() {
	s.inspector.Close()
}

func TestCopySegmentInspector(t *testing.T) {
	suite.Run(t, new(CopySegmentInspectorSuite))
}

func (s *CopySegmentInspectorSuite) TestReloadFromMeta_NoPendingTasks() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)

	// Create a job
	job := &Job{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		Tr: timerecord.NewTimeRecorder("test job"),
	}
	err := s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Create a completed task (should not be enqueued)
	task := &Task{
		CopyMeta: s.copyMeta,
		Tr:       timerecord.NewTimeRecorder("task"),
		Times:    taskcommon.NewTimes(),
	}
	task.Task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
	})
	err = s.copyMeta.AddTask(context.TODO(), task)
	s.NoError(err)

	// Reload should not enqueue completed tasks
	s.inspector.ReloadFromMeta()
}

func (s *CopySegmentInspectorSuite) TestReloadFromMeta_WithInProgressTasks() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(2)

	// Create a job
	job := &Job{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		Tr: timerecord.NewTimeRecorder("test job"),
	}
	err := s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Create an in-progress task
	task1 := &Task{
		CopyMeta: s.copyMeta,
		Tr:       timerecord.NewTimeRecorder("task1"),
		Times:    taskcommon.NewTimes(),
	}
	task1.Task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
	})
	err = s.copyMeta.AddTask(context.TODO(), task1)
	s.NoError(err)

	// Create a pending task (should not be enqueued by reload)
	task2 := &Task{
		CopyMeta: s.copyMeta,
		Tr:       timerecord.NewTimeRecorder("task2"),
		Times:    taskcommon.NewTimes(),
	}
	task2.Task.Store(&datapb.CopySegmentTask{
		TaskId:       1002,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})
	err = s.copyMeta.AddTask(context.TODO(), task2)
	s.NoError(err)

	// Expect only the in-progress task to be enqueued
	s.scheduler.EXPECT().Enqueue(mock.MatchedBy(func(t any) bool {
		copyTask, ok := t.(CopySegmentTask)
		return ok && copyTask.GetTaskId() == 1001
	})).Once()

	s.inspector.ReloadFromMeta()
}

func (s *CopySegmentInspectorSuite) TestProcessPending() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)

	// Create a job
	job := &Job{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		Tr: timerecord.NewTimeRecorder("test job"),
	}
	err := s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Create a pending task
	task := &Task{
		CopyMeta: s.copyMeta,
		Tr:       timerecord.NewTimeRecorder("task"),
		Times:    taskcommon.NewTimes(),
	}
	task.Task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})
	err = s.copyMeta.AddTask(context.TODO(), task)
	s.NoError(err)

	// Expect task to be enqueued
	s.scheduler.EXPECT().Enqueue(mock.MatchedBy(func(t any) bool {
		copyTask, ok := t.(CopySegmentTask)
		return ok && copyTask.GetTaskId() == 1001
	})).Once()

	s.inspector.processPending(task)
}

func (s *CopySegmentInspectorSuite) TestProcessFailed_DropTargetSegments() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil)

	// Create target segments
	seg1 := meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:            101,
		CollectionID:  s.collectionID,
		PartitionID:   10,
		State:         commonpb.SegmentState_Importing,
		NumOfRows:     100,
		InsertChannel: "ch1",
	})
	err := s.meta.AddSegment(context.TODO(), seg1)
	s.NoError(err)

	seg2 := meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:            102,
		CollectionID:  s.collectionID,
		PartitionID:   10,
		State:         commonpb.SegmentState_Dropped,
		NumOfRows:     100,
		InsertChannel: "ch1",
	})
	s.meta.Segments.SetSegment(seg2.GetID(), seg2)

	// Create a job
	job := &Job{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobFailed,
		},
		Tr: timerecord.NewTimeRecorder("test job"),
	}
	err = s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Create a failed task with target segment
	idMappings := []*datapb.CopySegmentIDMapping{
		{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10},
		{SourceSegmentId: 2, TargetSegmentId: 102, PartitionId: 10},
	}

	task := &Task{
		CopyMeta: s.copyMeta,
		Tr:       timerecord.NewTimeRecorder("task"),
		Times:    taskcommon.NewTimes(),
	}
	task.Task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskFailed,
		IdMappings:   idMappings,
		Reason:       "test failure",
	})
	err = s.copyMeta.AddTask(context.TODO(), task)
	s.NoError(err)

	// Process failed task
	s.inspector.ProcessFailed(task)

	// Target segment should be marked as Dropped
	segment := s.meta.GetSegment(context.TODO(), 101)
	s.Equal(commonpb.SegmentState_Dropped, segment.GetState())

	droppedSegment := s.meta.GetSegment(context.TODO(), 102)
	s.Equal(commonpb.SegmentState_Dropped, droppedSegment.GetState())
}

func (s *CopySegmentInspectorSuite) TestProcessFailed_NoTargetSegment() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)

	// Create a job
	job := &Job{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobFailed,
		},
		Tr: timerecord.NewTimeRecorder("test job"),
	}
	err := s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Create a failed task with non-existent target segment
	idMappings := []*datapb.CopySegmentIDMapping{
		{SourceSegmentId: 1, TargetSegmentId: 999, PartitionId: 10},
	}

	task := &Task{
		CopyMeta: s.copyMeta,
		Tr:       timerecord.NewTimeRecorder("task"),
		Times:    taskcommon.NewTimes(),
	}
	task.Task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskFailed,
		IdMappings:   idMappings,
		Reason:       "test failure",
	})
	err = s.copyMeta.AddTask(context.TODO(), task)
	s.NoError(err)

	// Process failed task should not panic when segment doesn't exist
	s.NotPanics(func() {
		s.inspector.ProcessFailed(task)
	})
}

func (s *CopySegmentInspectorSuite) TestInspect_ProcessPendingAndFailedTasks() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(3)
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil)

	// Create target segment
	seg1 := meta.NewSegmentInfo(&datapb.SegmentInfo{
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
	job := &Job{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		Tr: timerecord.NewTimeRecorder("test job"),
	}
	err = s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Create a pending task
	task1 := &Task{
		CopyMeta: s.copyMeta,
		Tr:       timerecord.NewTimeRecorder("task1"),
		Times:    taskcommon.NewTimes(),
	}
	task1.Task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})
	err = s.copyMeta.AddTask(context.TODO(), task1)
	s.NoError(err)

	// Create an in-progress task (should not be processed by inspect)
	task2 := &Task{
		CopyMeta: s.copyMeta,
		Tr:       timerecord.NewTimeRecorder("task2"),
		Times:    taskcommon.NewTimes(),
	}
	task2.Task.Store(&datapb.CopySegmentTask{
		TaskId:       1002,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
	})
	err = s.copyMeta.AddTask(context.TODO(), task2)
	s.NoError(err)

	// Create a failed task
	idMappings := []*datapb.CopySegmentIDMapping{
		{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10},
	}
	task3 := &Task{
		CopyMeta: s.copyMeta,
		Tr:       timerecord.NewTimeRecorder("task3"),
		Times:    taskcommon.NewTimes(),
	}
	task3.Task.Store(&datapb.CopySegmentTask{
		TaskId:       1003,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskFailed,
		IdMappings:   idMappings,
	})
	err = s.copyMeta.AddTask(context.TODO(), task3)
	s.NoError(err)

	// Expect only the pending task to be enqueued
	s.scheduler.EXPECT().Enqueue(mock.MatchedBy(func(t any) bool {
		copyTask, ok := t.(CopySegmentTask)
		return ok && copyTask.GetTaskId() == 1001
	})).Once()

	// Inspect should process pending and failed tasks
	s.inspector.inspect()

	// Verify failed task's target segment is dropped
	segment := s.meta.GetSegment(context.TODO(), 101)
	s.Equal(commonpb.SegmentState_Dropped, segment.GetState())
}

func (s *CopySegmentInspectorSuite) TestInspect_MultipleJobs() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(2)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(2)

	// Create two jobs
	job1 := &Job{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		Tr: timerecord.NewTimeRecorder("job1"),
	}
	err := s.copyMeta.AddJob(context.TODO(), job1)
	s.NoError(err)

	job2 := &Job{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        200,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		Tr: timerecord.NewTimeRecorder("job2"),
	}
	err = s.copyMeta.AddJob(context.TODO(), job2)
	s.NoError(err)

	// Create pending tasks for both jobs
	task1 := &Task{
		CopyMeta: s.copyMeta,
		Tr:       timerecord.NewTimeRecorder("task1"),
		Times:    taskcommon.NewTimes(),
	}
	task1.Task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})
	err = s.copyMeta.AddTask(context.TODO(), task1)
	s.NoError(err)

	task2 := &Task{
		CopyMeta: s.copyMeta,
		Tr:       timerecord.NewTimeRecorder("task2"),
		Times:    taskcommon.NewTimes(),
	}
	task2.Task.Store(&datapb.CopySegmentTask{
		TaskId:       2001,
		JobId:        200,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})
	err = s.copyMeta.AddTask(context.TODO(), task2)
	s.NoError(err)

	// Expect both tasks to be enqueued
	s.scheduler.EXPECT().Enqueue(mock.Anything).Times(2)

	s.inspector.inspect()
}

func (s *CopySegmentInspectorSuite) TestClose() {
	// Close should be idempotent
	s.NotPanics(func() {
		s.inspector.Close()
		s.inspector.Close()
	})
}

func (s *CopySegmentInspectorSuite) TestInspect_EmptyJobs() {
	// Inspect with no jobs should not panic
	s.NotPanics(func() {
		s.inspector.inspect()
	})
}

func (s *CopySegmentInspectorSuite) TestInspect_JobsAreSorted() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(3)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(3)

	// Create jobs with non-sequential IDs to test sorting
	jobs := []int64{300, 100, 200}
	for _, jobID := range jobs {
		job := &Job{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:        jobID,
				CollectionId: s.collectionID,
				State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
			},
			Tr: timerecord.NewTimeRecorder("job"),
		}
		err := s.copyMeta.AddJob(context.TODO(), job)
		s.NoError(err)

		// Create a pending task for each job
		task := &Task{
			CopyMeta: s.copyMeta,
			Tr:       timerecord.NewTimeRecorder("task"),
			Times:    taskcommon.NewTimes(),
		}
		task.Task.Store(&datapb.CopySegmentTask{
			TaskId:       jobID*10 + 1,
			JobId:        jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
		})
		err = s.copyMeta.AddTask(context.TODO(), task)
		s.NoError(err)
	}

	// Track the order of task IDs being enqueued
	var enqueuedTaskIDs []int64
	s.scheduler.EXPECT().Enqueue(mock.Anything).Run(func(t task2.Task) {
		copyTask, ok := t.(CopySegmentTask)
		s.True(ok)
		enqueuedTaskIDs = append(enqueuedTaskIDs, copyTask.GetTaskId())
	}).Times(3)

	s.inspector.inspect()

	// Verify tasks were processed in order of job IDs (100, 200, 300)
	s.Equal([]int64{1001, 2001, 3001}, enqueuedTaskIDs)
}
