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
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	task2 "github.com/milvus-io/milvus/internal/datacoord/task"
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
	meta      *meta
	copyMeta  CopySegmentMeta
	scheduler *task2.MockGlobalScheduler
	cluster   *session.MockCluster
	inspector *copySegmentInspector
}

func (s *CopySegmentInspectorSuite) SetupTest() {
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

	s.copyMeta, err = NewCopySegmentMeta(context.TODO(), s.catalog, s.meta, nil, nil)
	s.NoError(err)

	s.scheduler = task2.NewMockGlobalScheduler(s.T())

	s.cluster = session.NewMockCluster(s.T())

	s.inspector = NewCopySegmentInspector(
		context.TODO(),
		s.meta,
		s.copyMeta,
		s.scheduler,
		s.cluster,
	).(*copySegmentInspector)
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
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	err := s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Create a completed task (should not be enqueued)
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

	// Reload should not enqueue completed tasks
	s.inspector.reloadFromMeta()
}

func (s *CopySegmentInspectorSuite) TestReloadFromMeta_WithInProgressTasks() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(2)

	// Create a job
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	err := s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Create an in-progress task
	task1 := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task1"),
		times:    taskcommon.NewTimes(),
	}
	task1.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
	})
	err = s.copyMeta.AddTask(context.TODO(), task1)
	s.NoError(err)

	// Create a pending task (should not be enqueued by reload)
	task2 := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task2"),
		times:    taskcommon.NewTimes(),
	}
	task2.task.Store(&datapb.CopySegmentTask{
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

	s.inspector.reloadFromMeta()
}

func (s *CopySegmentInspectorSuite) TestProcessPending() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)

	// Create a job
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
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

	seg2 := NewSegmentInfo(&datapb.SegmentInfo{
		ID:            102,
		CollectionID:  s.collectionID,
		PartitionID:   10,
		State:         commonpb.SegmentState_Dropped,
		NumOfRows:     100,
		InsertChannel: "ch1",
	})
	s.meta.segments.SetSegment(seg2.GetID(), seg2)

	// Create a job
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobFailed,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	err = s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Create a failed task with target segment
	idMappings := []*datapb.CopySegmentIDMapping{
		{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10},
		{SourceSegmentId: 2, TargetSegmentId: 102, PartitionId: 10},
	}

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
		NodeId:       NullNodeID,
		IdMappings:   idMappings,
		Reason:       "test failure",
	})
	err = s.copyMeta.AddTask(context.TODO(), task)
	s.NoError(err)

	// Process failed task
	s.inspector.processFailed(task)

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
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobFailed,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	err := s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Create a failed task with non-existent target segment
	idMappings := []*datapb.CopySegmentIDMapping{
		{SourceSegmentId: 1, TargetSegmentId: 999, PartitionId: 10},
	}

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
		NodeId:       NullNodeID,
		IdMappings:   idMappings,
		Reason:       "test failure",
	})
	err = s.copyMeta.AddTask(context.TODO(), task)
	s.NoError(err)

	// Process failed task should not panic when segment doesn't exist
	s.NotPanics(func() {
		s.inspector.processFailed(task)
	})
}

// addTerminalTaskWithAssignment stores a terminal task that is still assigned to
// nodeID, i.e. one whose worker-side drop has not converged yet.
func (s *CopySegmentInspectorSuite) addTerminalTaskWithAssignment(
	taskID, nodeID int64, state datapb.CopySegmentTaskState,
) *copySegmentTask {
	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       taskID,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        state,
		NodeId:       nodeID,
	})
	s.NoError(s.copyMeta.AddTask(context.TODO(), task))
	return task
}

// TestProcessTerminal_RetriesDropUntilAssignmentCleared is the liveness
// regression for the review finding: the scheduler calls DropTaskOnWorker
// exactly once before removing the task from its running set, so an ambiguous
// drop error would strand the task with its node assignment intact — and
// checkGC skips any task that still carries one, keeping the task and its
// parent job in meta past retention forever. The inspector must keep retrying
// until the assignment is actually cleared.
func (s *CopySegmentInspectorSuite) TestProcessTerminal_RetriesDropUntilAssignmentCleared() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)

	task := s.addTerminalTaskWithAssignment(1001, 10,
		datapb.CopySegmentTaskState_CopySegmentTaskCompleted)

	// Round 1: the drop hits an ambiguous transport error (DataNode rolling
	// restart). The assignment must be kept — clearing it could orphan a live
	// worker-side task.
	s.cluster.EXPECT().DropCopySegment(int64(10), int64(1001)).
		Return(errors.New("rpc timeout")).Once()
	s.inspector.processTerminal(task)
	s.EqualValues(10, s.copyMeta.GetTask(context.TODO(), 1001).GetNodeId())

	// Round 2: the node is reachable again, the drop succeeds and the
	// assignment is cleared, unblocking checkGC.
	s.cluster.EXPECT().DropCopySegment(int64(10), int64(1001)).Return(nil).Once()
	s.inspector.processTerminal(task)
	s.EqualValues(NullNodeID, s.copyMeta.GetTask(context.TODO(), 1001).GetNodeId())

	// Round 3: converged — no further RPC. The mock has no expectation left, so
	// another call would fail the test.
	s.inspector.processTerminal(task)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		s.copyMeta.GetTask(context.TODO(), 1001).GetState())
}

// TestProcessTerminal_RetriesPersistFailure: the drop RPC can succeed while the
// etcd write of NullNodeID fails. That leaves the same stranded state, so it
// must be retried too.
func (s *CopySegmentInspectorSuite) TestProcessTerminal_RetriesPersistFailure() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Once()
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).
		Return(errors.New("etcd down")).Once()

	task := s.addTerminalTaskWithAssignment(1002, 11,
		datapb.CopySegmentTaskState_CopySegmentTaskFailed)

	s.cluster.EXPECT().DropCopySegment(int64(11), int64(1002)).Return(nil).Once()
	s.inspector.processTerminal(task)
	s.EqualValues(11, s.copyMeta.GetTask(context.TODO(), 1002).GetNodeId())

	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Once()
	s.cluster.EXPECT().DropCopySegment(int64(11), int64(1002)).Return(nil).Once()
	s.inspector.processTerminal(task)
	s.EqualValues(NullNodeID, s.copyMeta.GetTask(context.TODO(), 1002).GetNodeId())
}

// TestProcessTerminal_NoAssignmentIsNoop: an already-converged task must not
// issue an RPC on every inspection round.
func (s *CopySegmentInspectorSuite) TestProcessTerminal_NoAssignmentIsNoop() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)

	task := s.addTerminalTaskWithAssignment(1003, NullNodeID,
		datapb.CopySegmentTaskState_CopySegmentTaskCompleted)

	// No cluster expectation: any DropCopySegment call fails the test.
	s.inspector.processTerminal(task)
}

// TestInspect_FailedJobDropsCompletedSiblingSegments is the regression for the
// all-or-nothing restore invariant: a collection larger than
// MaxSegmentsPerCopyTask splits into several copy tasks, so one task can
// Complete — flipping its target segments Importing → Flushed, i.e. queryable —
// before a sibling fails and takes the job down. Cleanup keys off the JOB
// outcome, so the completed sibling's segments must be dropped too; otherwise a
// restore reported as failed leaves partial data visible, and checkGC's
// hasSegments guard retains the tasks and the job forever.
func (s *CopySegmentInspectorSuite) TestInspect_FailedJobDropsCompletedSiblingSegments() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil)

	// Target segment of the COMPLETED task: already Flushed and queryable.
	completedSeg := NewSegmentInfo(&datapb.SegmentInfo{
		ID: 201, CollectionID: s.collectionID, PartitionID: 10,
		State: commonpb.SegmentState_Flushed, NumOfRows: 100, InsertChannel: "ch1",
	})
	s.NoError(s.meta.AddSegment(context.TODO(), completedSeg))

	// Target segment of the FAILED task: still Importing.
	failedSeg := NewSegmentInfo(&datapb.SegmentInfo{
		ID: 202, CollectionID: s.collectionID, PartitionID: 10,
		State: commonpb.SegmentState_Importing, NumOfRows: 100, InsertChannel: "ch1",
	})
	s.NoError(s.meta.AddSegment(context.TODO(), failedSeg))

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobFailed,
			Reason:       "sibling task failed",
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))

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
	addTask(1006, 201, datapb.CopySegmentTaskState_CopySegmentTaskCompleted)
	addTask(1007, 202, datapb.CopySegmentTaskState_CopySegmentTaskFailed)

	s.inspector.inspect()

	// Both target segments must be dropped — the completed sibling's included.
	s.Equal(commonpb.SegmentState_Dropped, s.meta.GetSegment(context.TODO(), 201).GetState(),
		"a failed restore must not leave the completed sibling's data queryable")
	s.Equal(commonpb.SegmentState_Dropped, s.meta.GetSegment(context.TODO(), 202).GetState())
}

// TestInspect_CompletedTaskUnderActiveJobKeepsSegments: the drop keys off the
// job outcome, so a completed task under a still-Executing job must keep its
// segments — that is the restore succeeding, not failing.
func (s *CopySegmentInspectorSuite) TestInspect_CompletedTaskUnderActiveJobKeepsSegments() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)

	seg := NewSegmentInfo(&datapb.SegmentInfo{
		ID: 203, CollectionID: s.collectionID, PartitionID: 10,
		State: commonpb.SegmentState_Flushed, NumOfRows: 100, InsertChannel: "ch1",
	})
	s.NoError(s.meta.AddSegment(context.TODO(), seg))

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))

	t := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	t.task.Store(&datapb.CopySegmentTask{
		TaskId:       1008,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		NodeId:       NullNodeID,
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 1, TargetSegmentId: 203, PartitionId: 10},
		},
	})
	s.NoError(s.copyMeta.AddTask(context.TODO(), t))

	s.inspector.inspect()

	s.Equal(commonpb.SegmentState_Flushed, s.meta.GetSegment(context.TODO(), 203).GetState())
}

// TestInspect_RetriesDropForTerminalTasks wires the retry into the periodic
// loop: both Completed and Failed tasks that still carry an assignment must be
// picked up by inspect(), not just by a direct processTerminal call.
func (s *CopySegmentInspectorSuite) TestInspect_RetriesDropForTerminalTasks() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobFailed,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))

	s.addTerminalTaskWithAssignment(1004, 12,
		datapb.CopySegmentTaskState_CopySegmentTaskCompleted)
	s.addTerminalTaskWithAssignment(1005, 13,
		datapb.CopySegmentTaskState_CopySegmentTaskFailed)

	s.cluster.EXPECT().DropCopySegment(int64(12), int64(1004)).Return(nil).Once()
	s.cluster.EXPECT().DropCopySegment(int64(13), int64(1005)).Return(nil).Once()

	s.inspector.inspect()

	s.EqualValues(NullNodeID, s.copyMeta.GetTask(context.TODO(), 1004).GetNodeId())
	s.EqualValues(NullNodeID, s.copyMeta.GetTask(context.TODO(), 1005).GetNodeId())
}

func (s *CopySegmentInspectorSuite) TestInspect_ProcessPendingAndFailedTasks() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(3)
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil)

	// Create target segment
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
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        s.jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	err = s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Create a pending task
	task1 := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task1"),
		times:    taskcommon.NewTimes(),
	}
	task1.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})
	err = s.copyMeta.AddTask(context.TODO(), task1)
	s.NoError(err)

	// Create an in-progress task (should not be processed by inspect)
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
	})
	err = s.copyMeta.AddTask(context.TODO(), task2)
	s.NoError(err)

	// Create a failed task
	idMappings := []*datapb.CopySegmentIDMapping{
		{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10},
	}
	task3 := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task3"),
		times:    taskcommon.NewTimes(),
	}
	task3.task.Store(&datapb.CopySegmentTask{
		TaskId:       1003,
		JobId:        s.jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskFailed,
		NodeId:       NullNodeID,
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
	job1 := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		tr: timerecord.NewTimeRecorder("job1"),
	}
	err := s.copyMeta.AddJob(context.TODO(), job1)
	s.NoError(err)

	job2 := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        200,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		tr: timerecord.NewTimeRecorder("job2"),
	}
	err = s.copyMeta.AddJob(context.TODO(), job2)
	s.NoError(err)

	// Create pending tasks for both jobs
	task1 := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task1"),
		times:    taskcommon.NewTimes(),
	}
	task1.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})
	err = s.copyMeta.AddTask(context.TODO(), task1)
	s.NoError(err)

	task2 := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task2"),
		times:    taskcommon.NewTimes(),
	}
	task2.task.Store(&datapb.CopySegmentTask{
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
		job := &copySegmentJob{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:        jobID,
				CollectionId: s.collectionID,
				State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
			},
			tr: timerecord.NewTimeRecorder("job"),
		}
		err := s.copyMeta.AddJob(context.TODO(), job)
		s.NoError(err)

		// Create a pending task for each job
		task := &copySegmentTask{
			copyMeta: s.copyMeta,
			tr:       timerecord.NewTimeRecorder("task"),
			times:    taskcommon.NewTimes(),
		}
		task.task.Store(&datapb.CopySegmentTask{
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
