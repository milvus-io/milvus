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
	"sync/atomic"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	task2 "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type CopySegmentTaskSuite struct {
	suite.Suite
}

func TestCopySegmentTask(t *testing.T) {
	suite.Run(t, new(CopySegmentTaskSuite))
}

func (s *CopySegmentTaskSuite) TestCopySegmentTask_GettersAndSetters() {
	idMappings := []*datapb.CopySegmentIDMapping{
		{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10},
		{SourceSegmentId: 2, TargetSegmentId: 102, PartitionId: 10},
	}

	task := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}

	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 2,
		NodeId:       5,
		TaskVersion:  1,
		TaskSlot:     1,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
		Reason:       "test reason",
		IdMappings:   idMappings,
		CreatedTs:    12345,
		CompleteTs:   54321,
	})

	// Test all getters
	s.Equal(int64(1001), task.GetTaskId())
	s.Equal(int64(100), task.GetJobId())
	s.Equal(int64(2), task.GetCollectionId())
	s.Equal(int64(5), task.GetNodeId())
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskPending, task.GetState())
	s.Equal("test reason", task.GetReason())
	s.Equal(idMappings, task.GetIdMappings())
	s.NotNil(task.GetTR())

	// Test task.Task interface methods
	s.Equal(int64(1001), task.GetTaskID())
	s.Equal(taskcommon.CopySegment, task.GetTaskType())
	s.Equal(taskcommon.FromCopySegmentState(datapb.CopySegmentTaskState_CopySegmentTaskPending), task.GetTaskState())
	s.Equal(int64(1), task.GetTaskSlot())
	s.Equal(int64(1), task.GetTaskVersion())
}

func TestCopySegmentTask_MinimumWorkerVersion(t *testing.T) {
	task := createTestCopyTask(1, 100).(*copySegmentTask)
	copyMeta, _ := newCopySegmentTaskTestMeta(t, task)
	ctx := context.Background()
	require.NoError(t, copyMeta.AddJob(ctx, newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobPending)))

	assert.Equal(t, semver.Version{}, task.MinimumWorkerVersion())
	require.NoError(t, copyMeta.UpdateJob(ctx, 100, func(job CopySegmentJob) {
		job.(*copySegmentJob).External = true
	}))
	assert.Equal(t, externalSnapshotMinimumDataNodeVersion, task.MinimumWorkerVersion())
}

func (s *CopySegmentTaskSuite) TestCopySegmentTask_Clone() {
	original := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}

	original.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 2,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
		Reason:       "original reason",
	})

	// Clone the task
	cloned := original.Clone()

	// Verify cloned task has same values
	s.Equal(original.GetTaskId(), cloned.GetTaskId())
	s.Equal(original.GetJobId(), cloned.GetJobId())
	s.Equal(original.GetCollectionId(), cloned.GetCollectionId())
	s.Equal(original.GetState(), cloned.GetState())
	s.Equal(original.GetReason(), cloned.GetReason())

	// Verify time recorder is same reference
	s.Equal(original.GetTR(), cloned.GetTR())

	// Verify they share the same task pointer (not deep copy)
	clonedImpl := cloned.(*copySegmentTask)
	s.Equal(original.task.Load(), clonedImpl.task.Load())
}

func (s *CopySegmentTaskSuite) TestWithCopyTaskJob() {
	task1 := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("task1"),
		times: taskcommon.NewTimes(),
	}
	task1.task.Store(&datapb.CopySegmentTask{
		TaskId: 1001,
		JobId:  100,
	})

	task2 := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("task2"),
		times: taskcommon.NewTimes(),
	}
	task2.task.Store(&datapb.CopySegmentTask{
		TaskId: 1002,
		JobId:  200,
	})

	filter := WithCopyTaskJob(100)

	s.True(filter(task1))
	s.False(filter(task2))
}

func (s *CopySegmentTaskSuite) TestWithCopyTaskStates() {
	pendingTask := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("pending"),
		times: taskcommon.NewTimes(),
	}
	pendingTask.task.Store(&datapb.CopySegmentTask{
		TaskId: 1001,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})

	inProgressTask := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("inprogress"),
		times: taskcommon.NewTimes(),
	}
	inProgressTask.task.Store(&datapb.CopySegmentTask{
		TaskId: 1002,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
	})

	completedTask := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("completed"),
		times: taskcommon.NewTimes(),
	}
	completedTask.task.Store(&datapb.CopySegmentTask{
		TaskId: 1003,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
	})

	// Test filter with single state
	filter1 := WithCopyTaskStates(datapb.CopySegmentTaskState_CopySegmentTaskPending)
	s.True(filter1(pendingTask))
	s.False(filter1(inProgressTask))
	s.False(filter1(completedTask))

	// Test filter with multiple states
	filter2 := WithCopyTaskStates(
		datapb.CopySegmentTaskState_CopySegmentTaskPending,
		datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
	)
	s.True(filter2(pendingTask))
	s.True(filter2(inProgressTask))
	s.False(filter2(completedTask))
}

func (s *CopySegmentTaskSuite) TestUpdateCopyTaskState() {
	task := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId: 1001,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})

	// Apply update action
	action := UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskInProgress)
	action(task)

	// Verify state is updated
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskInProgress, task.GetState())
}

func (s *CopySegmentTaskSuite) TestUpdateCopyTaskReason() {
	task := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId: 1001,
		Reason: "",
	})

	// Apply update action
	action := UpdateCopyTaskReason("task failed")
	action(task)

	// Verify reason is updated
	s.Equal("task failed", task.GetReason())
}

func (s *CopySegmentTaskSuite) TestUpdateCopyTaskNodeID() {
	task := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId: 1001,
		NodeId: 0,
	})

	// Apply update action
	action := UpdateCopyTaskNodeID(5)
	action(task)

	// Verify node ID is updated
	s.Equal(int64(5), task.GetNodeId())
}

func (s *CopySegmentTaskSuite) TestUpdateCopyTaskCompleteTs() {
	task := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:     1001,
		CompleteTs: 0,
	})

	now := uint64(time.Now().UnixNano())

	// Apply update action
	action := UpdateCopyTaskCompleteTs(now)
	action(task)

	// Verify complete timestamp is updated
	s.Equal(now, task.task.Load().GetCompleteTs())
}

func (s *CopySegmentTaskSuite) TestMultipleUpdateActions() {
	task := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:     1001,
		State:      datapb.CopySegmentTaskState_CopySegmentTaskPending,
		NodeId:     0,
		Reason:     "",
		CompleteTs: 0,
	})

	// Apply multiple update actions
	actions := []UpdateCopySegmentTaskAction{
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskCompleted),
		UpdateCopyTaskNodeID(5),
		UpdateCopyTaskReason("completed successfully"),
		UpdateCopyTaskCompleteTs(12345),
	}

	for _, action := range actions {
		action(task)
	}

	// Verify all updates are applied
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskCompleted, task.GetState())
	s.Equal(int64(5), task.GetNodeId())
	s.Equal("completed successfully", task.GetReason())
	s.Equal(uint64(12345), task.task.Load().GetCompleteTs())
}

func (s *CopySegmentTaskSuite) TestWrapCopySegmentTaskLog() {
	task := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 2,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})

	// Test without extra fields
	fields := WrapCopySegmentTaskLog(task)
	s.Len(fields, 4)

	// Verify field names
	fieldNames := make(map[string]bool)
	for _, field := range fields {
		fieldNames[field.Key] = true
	}
	s.True(fieldNames["taskID"])
	s.True(fieldNames["jobID"])
	s.True(fieldNames["collectionID"])
	s.True(fieldNames["state"])

	// Test with extra fields
	extraFields := []mlog.Field{
		mlog.String("extra", "value"),
		mlog.Int64("number", 123),
	}
	fieldsWithExtra := WrapCopySegmentTaskLog(task, extraFields...)
	s.Len(fieldsWithExtra, 6)
}

func (s *CopySegmentTaskSuite) TestCombinedFilters() {
	tasks := []CopySegmentTask{
		&copySegmentTask{
			tr:    timerecord.NewTimeRecorder("task1"),
			times: taskcommon.NewTimes(),
		},
		&copySegmentTask{
			tr:    timerecord.NewTimeRecorder("task2"),
			times: taskcommon.NewTimes(),
		},
		&copySegmentTask{
			tr:    timerecord.NewTimeRecorder("task3"),
			times: taskcommon.NewTimes(),
		},
		&copySegmentTask{
			tr:    timerecord.NewTimeRecorder("task4"),
			times: taskcommon.NewTimes(),
		},
	}

	tasks[0].(*copySegmentTask).task.Store(&datapb.CopySegmentTask{
		TaskId: 1001,
		JobId:  100,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})
	tasks[1].(*copySegmentTask).task.Store(&datapb.CopySegmentTask{
		TaskId: 1002,
		JobId:  100,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
	})
	tasks[2].(*copySegmentTask).task.Store(&datapb.CopySegmentTask{
		TaskId: 1003,
		JobId:  200,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})
	tasks[3].(*copySegmentTask).task.Store(&datapb.CopySegmentTask{
		TaskId: 1004,
		JobId:  100,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
	})

	// Filter: jobID = 100 AND state = Pending or InProgress
	jobFilter := WithCopyTaskJob(100)
	stateFilter := WithCopyTaskStates(
		datapb.CopySegmentTaskState_CopySegmentTaskPending,
		datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
	)

	var filtered []CopySegmentTask
	for _, task := range tasks {
		if jobFilter(task) && stateFilter(task) {
			filtered = append(filtered, task)
		}
	}

	// Should match tasks 1001 and 1002
	s.Len(filtered, 2)
	s.Equal(int64(1001), filtered[0].GetTaskId())
	s.Equal(int64(1002), filtered[1].GetTaskId())
}

func (s *CopySegmentTaskSuite) TestTaskTimeOperations() {
	task := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId: 1001,
	})

	// Set and get task time
	now := time.Now()
	task.SetTaskTime(taskcommon.TimeStart, now)

	retrievedTime := task.GetTaskTime(taskcommon.TimeStart)
	s.True(retrievedTime.Equal(now))
}

func (s *CopySegmentTaskSuite) TestTaskWithEmptyIdMappings() {
	task := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:     1001,
		IdMappings: nil,
	})

	// Should return nil, not panic
	mappings := task.GetIdMappings()
	s.Nil(mappings)
}

func (s *CopySegmentTaskSuite) TestStateConversion() {
	testCases := []struct {
		copySegmentState datapb.CopySegmentTaskState
		expectedState    taskcommon.State
	}{
		{datapb.CopySegmentTaskState_CopySegmentTaskPending, taskcommon.Init},
		{datapb.CopySegmentTaskState_CopySegmentTaskInProgress, taskcommon.InProgress},
		{datapb.CopySegmentTaskState_CopySegmentTaskCompleted, taskcommon.Finished},
		{datapb.CopySegmentTaskState_CopySegmentTaskFailed, taskcommon.Failed},
	}

	for _, tc := range testCases {
		task := &copySegmentTask{
			tr:    timerecord.NewTimeRecorder("test task"),
			times: taskcommon.NewTimes(),
		}
		task.task.Store(&datapb.CopySegmentTask{
			TaskId: 1001,
			State:  tc.copySegmentState,
		})

		s.Equal(tc.expectedState, task.GetTaskState(),
			"State conversion failed for %v", tc.copySegmentState)
	}
}

func (s *CopySegmentTaskSuite) TestTaskType() {
	task := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId: 1001,
	})

	s.Equal(taskcommon.CopySegment, task.GetTaskType())
}

func (s *CopySegmentTaskSuite) TestQueryTaskOnWorker_NotCompletedKeepsTaskInProgress() {
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().QueryCopySegment(mock.Anything, mock.Anything).Return(
		&datapb.QueryCopySegmentResponse{
			TaskID: 1001,
			State:  datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
		},
		nil,
	)

	task := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test"),
		times: taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 100,
		NodeId:       10,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
	})

	task.QueryTaskOnWorker(cluster)

	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskInProgress, task.GetState())
}

func (s *CopySegmentTaskSuite) TestQueryTaskOnWorker_TransientRPCErrorKeepsInProgress() {
	// A transient transport error (network blip, RPC timeout, node briefly not
	// ready) must NOT fail the task or its parent job, and must NOT re-dispatch
	// it either: the worker-side task may still be running, and re-dispatching
	// would start a concurrent duplicate copy. The task stays InProgress on the
	// same node and is simply queried again on the next check round.
	transientErrs := []error{
		errors.New("rpc failed"),
		merr.WrapErrServiceNotReady("datanode", 10, "Initializing"),
	}
	for _, transientErr := range transientErrs {
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCopySegment(mock.Anything, mock.Anything).Return(nil, transientErr)

		task := createTestCopyTask(100, 2001).(*copySegmentTask)
		task.task.Load().NodeId = 10
		copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
		// A parent job in Executing state must remain untouched by the retry.
		s.NoError(copyMeta.AddJob(context.Background(), newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting)))

		task.QueryTaskOnWorker(cluster)

		updatedTask := copyMeta.GetTask(context.Background(), 1001)
		s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskInProgress, updatedTask.GetState())
		s.EqualValues(10, updatedTask.GetNodeId())
		s.Empty(updatedTask.GetReason())
		// The parent job must NOT be failed - the task retries by polling.
		s.Equal(datapb.CopySegmentJobState_CopySegmentJobExecuting,
			copyMeta.GetJob(context.Background(), 100).GetState())
	}
}

func (s *CopySegmentTaskSuite) TestScheduler_OneOffTransientErrorDoesNotRedispatch() {
	// Regression test at the global-scheduler level: a one-off transport error
	// while querying an InProgress copy-segment task must NOT trigger a second
	// CreateCopySegment dispatch. The task stays in runningTasks (InProgress on
	// the same node) and is simply queried again on the next check tick.
	cluster := session.NewMockCluster(s.T())
	var queries atomic.Int32
	cluster.EXPECT().QueryCopySegment(mock.Anything, mock.Anything).RunAndReturn(
		func(nodeID int64, req *datapb.QueryCopySegmentRequest) (*datapb.QueryCopySegmentResponse, error) {
			s.EqualValues(10, nodeID)
			if queries.Add(1) == 1 {
				return nil, errors.New("one-off transport error")
			}
			return &datapb.QueryCopySegmentResponse{
				TaskID: 1001,
				State:  datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
			}, nil
		})
	var creates atomic.Int32
	cluster.EXPECT().CreateCopySegment(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(int64, *datapb.CopySegmentRequest, int64) error {
			creates.Add(1)
			return nil
		}).Maybe()
	// Only reached if the task is wrongly reset to Pending and re-scheduled.
	cluster.EXPECT().QuerySlot().Return(map[int64]*session.WorkerSlots{
		10: {NodeID: 10, AvailableSlots: 16},
	}).Maybe()

	copyTask := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyTask.task.Load().NodeId = 10
	newCopySegmentTaskTestMeta(s.T(), copyTask)

	scheduler := task2.NewGlobalTaskScheduler(context.Background(), cluster)
	scheduler.Enqueue(copyTask)
	scheduler.Start()
	defer scheduler.Stop()

	// The task must survive the transient error and keep being polled on the
	// same node across multiple check ticks.
	s.Eventually(func() bool { return queries.Load() >= 3 }, 10*time.Second, 20*time.Millisecond)
	s.EqualValues(0, creates.Load())
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskInProgress, copyTask.GetState())
	s.EqualValues(10, copyTask.GetNodeId())
}

func (s *CopySegmentTaskSuite) TestQueryTaskOnWorker_NodeGoneResetsToPending() {
	// Regression test for DataNode restart/replacement: the node manager
	// returns NodeNotFound, surfaced as a query error. The worker-side task is
	// lost as far as DataCoord can tell, so the task must be reset to Pending
	// with NullNodeID so the scheduler re-dispatches it to a live node rather
	// than polling the dead node until the job-level timeout.
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().QueryCopySegment(mock.Anything, mock.Anything).Return(
		nil,
		merr.WrapErrNodeNotFound(10),
	)

	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	task.task.Load().NodeId = 10
	task.task.Load().TaskVersion = 4
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
	s.NoError(copyMeta.AddJob(context.Background(), newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting)))
	handler := &fakeUntrackedDropHandler{}
	copyMeta.(*copySegmentMeta).setUntrackedDropHandler(handler)

	// No DropCopySegment expectation: the lost dispatch must be handed to the
	// inspector's retry queue, not dropped synchronously here.
	task.QueryTaskOnWorker(cluster)

	updatedTask := copyMeta.GetTask(context.Background(), 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskPending, updatedTask.GetState())
	s.EqualValues(NullNodeID, updatedTask.GetNodeId())
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobExecuting,
		copyMeta.GetJob(context.Background(), 100).GetState())

	// ErrNodeNotFound only says DataCoord's client map lost node 10, not that
	// the DataNode — and the task entry, slot and copy goroutines it holds — is
	// gone. Once NodeId is cleared nothing else will ever address that
	// dispatch, so it must be queued for untracked cleanup at its own epoch:
	// the inspector treats ErrNodeNotFound as done if the node really is gone,
	// and aborts the stale worker task if it is not.
	s.EqualValues(10, handler.nodeID)
	s.EqualValues(1001, handler.taskID)
	s.EqualValues(4, handler.taskVersion)
}

func (s *CopySegmentTaskSuite) TestQueryTaskOnWorker_TaskNotFoundOnWorkerResetsToPending() {
	// Regression test for a DataNode that restarted but kept (or re-acquired)
	// its session: the node is reachable, but its in-memory task manager no
	// longer has the task, so QueryCopySegment surfaces the DataNode's
	// task-not-found status (importv2.WrapTaskNotFoundError -> ErrImportSysFailed)
	// as an error. The task is confirmed lost and must be reset to Pending with
	// NullNodeID for re-dispatch instead of polling until the job-level timeout.
	cluster := session.NewMockCluster(s.T())
	// Round-trip the error through Status -> Error exactly like
	// cluster.queryTask's merr.CheckRPCCall does with the DataNode's response
	// status, so the test also covers the code-preservation assumption.
	taskNotFoundErr := merr.Error(merr.Status(
		merr.WrapErrImportSysFailedMsg("cannot find import task with id %d", 1001)))
	cluster.EXPECT().QueryCopySegment(mock.Anything, mock.Anything).Return(nil, taskNotFoundErr)

	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	task.task.Load().NodeId = 10
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
	s.NoError(copyMeta.AddJob(context.Background(), newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting)))
	handler := &fakeUntrackedDropHandler{}
	copyMeta.(*copySegmentMeta).setUntrackedDropHandler(handler)

	task.QueryTaskOnWorker(cluster)

	updatedTask := copyMeta.GetTask(context.Background(), 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskPending, updatedTask.GetState())
	s.EqualValues(NullNodeID, updatedTask.GetNodeId())
	s.Empty(updatedTask.GetReason())
	// The parent job must NOT be failed - the task is re-dispatched.
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobExecuting,
		copyMeta.GetJob(context.Background(), 100).GetState())
	// The cleared dispatch is still handed to cleanup; the drop is idempotent
	// and the worker answers task-not-found again, which converges it.
	s.EqualValues(10, handler.nodeID)
	s.EqualValues(1001, handler.taskID)
}

func (s *CopySegmentTaskSuite) TestQueryTaskOnWorker_MarksFailedOnWorkerFailure() {
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().QueryCopySegment(mock.Anything, mock.Anything).Return(
		&datapb.QueryCopySegmentResponse{
			TaskID: 1001,
			State:  datapb.CopySegmentTaskState_CopySegmentTaskFailed,
			Reason: "worker failed",
		},
		nil,
	)

	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)

	task.QueryTaskOnWorker(cluster)

	updatedTask := copyMeta.GetTask(context.Background(), 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskFailed, updatedTask.GetState())
	s.Equal("worker failed", updatedTask.GetReason())
}

func (s *CopySegmentTaskSuite) TestQueryTaskOnWorker_CompletedSyncsTask() {
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().QueryCopySegment(mock.Anything, mock.Anything).Return(
		&datapb.QueryCopySegmentResponse{
			TaskID: 1001,
			State:  datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
			SegmentResults: []*datapb.CopySegmentResult{
				{
					SegmentId:    2001,
					ImportedRows: 100,
					Binlogs:      makeTestCopySegmentBinlogs(),
					ManifestPath: "manifest-path",
				},
			},
		},
		nil,
	)

	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, m := newCopySegmentTaskTestMeta(s.T(), task)
	err := m.AddSegment(context.Background(), newTestCopySegment(2001))
	s.NoError(err)

	task.QueryTaskOnWorker(cluster)

	// Binlogs and manifest are recorded, but the segment stays unpublished —
	// finishJob makes the whole restore visible in one batch.
	segment := m.GetSegment(context.Background(), 2001)
	s.Equal(commonpb.SegmentState_Importing, segment.GetState())
	s.True(segment.GetIsImporting())
	s.Equal("manifest-path", segment.GetManifestPath())

	updatedTask := copyMeta.GetTask(context.Background(), 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskCompleted, updatedTask.GetState())
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_CompletedUpdatesSegment() {
	ctx := context.Background()
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, m := newCopySegmentTaskTestMeta(s.T(), task)

	err := m.AddSegment(ctx, newTestCopySegment(2001))
	s.NoError(err)

	insertBinlogs := makeTestCopySegmentBinlogs()
	resp := &datapb.QueryCopySegmentResponse{
		TaskID: 1001,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		SegmentResults: []*datapb.CopySegmentResult{
			{
				SegmentId:    2001,
				ImportedRows: 100,
				Binlogs:      insertBinlogs,
				ManifestPath: "manifest-path",
			},
		},
	}

	err = SyncCopySegmentTask(task, resp, copyMeta, m)
	s.NoError(err)

	segment := m.GetSegment(ctx, 2001)
	s.Equal(commonpb.SegmentState_Importing, segment.GetState())
	s.True(segment.GetIsImporting())
	s.Equal("manifest-path", segment.GetManifestPath())
	s.Equal(insertBinlogs, segment.GetBinlogs())

	updatedTask := copyMeta.GetTask(ctx, 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskCompleted, updatedTask.GetState())
	s.NotZero(updatedTask.(*copySegmentTask).task.Load().GetCompleteTs())
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_FailedResponseUpdatesTask() {
	ctx := context.Background()
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)

	err := SyncCopySegmentTask(task, &datapb.QueryCopySegmentResponse{
		TaskID: 1001,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskFailed,
		Reason: "worker failed",
	}, copyMeta, nil)
	s.NoError(err)

	updatedTask := copyMeta.GetTask(ctx, 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskFailed, updatedTask.GetState())
	s.Equal("worker failed", updatedTask.GetReason())
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_DefaultStateNoop() {
	ctx := context.Background()
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)

	err := SyncCopySegmentTask(task, &datapb.QueryCopySegmentResponse{
		TaskID: 1001,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
	}, copyMeta, nil)
	s.NoError(err)

	updatedTask := copyMeta.GetTask(ctx, 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskInProgress, updatedTask.GetState())
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_UpdateSegmentsInfoErrorMarksFailed() {
	ctx := context.Background()
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, m := newCopySegmentTaskTestMeta(s.T(), task)
	err := m.AddSegment(ctx, newTestCopySegment(2001))
	s.NoError(err)

	err = SyncCopySegmentTask(task, &datapb.QueryCopySegmentResponse{
		TaskID: 1001,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		SegmentResults: []*datapb.CopySegmentResult{
			{
				SegmentId: 2001,
				Binlogs: []*datapb.FieldBinlog{
					{
						FieldID: 100,
						Binlogs: []*datapb.Binlog{
							{LogID: 1, LogPath: "invalid-log-path"},
						},
					},
				},
			},
		},
	}, copyMeta, m)
	s.Error(err)

	updatedTask := copyMeta.GetTask(ctx, 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskFailed, updatedTask.GetState())
	s.Contains(updatedTask.GetReason(), "fieldBinlog no need to store logpath")
}

// createTestIndexMeta creates an indexMeta with pre-registered index definitions for testing.
// If catalog is nil, a default mock catalog (CreateSegmentIndex returns nil) is used.
func createTestIndexMeta(t *testing.T, collectionID int64, indexes map[int64]*model.Index, catalog ...metastore.DataCoordCatalog) *indexMeta {
	var cat metastore.DataCoordCatalog
	if len(catalog) > 0 && catalog[0] != nil {
		cat = catalog[0]
	} else {
		mockCat := catalogmocks.NewDataCoordCatalog(t)
		mockCat.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil).Maybe()
		cat = mockCat
	}

	im := &indexMeta{
		ctx:              context.Background(),
		catalog:          cat,
		keyLock:          lock.NewKeyLock[UniqueID](),
		indexes:          map[UniqueID]map[UniqueID]*model.Index{collectionID: indexes},
		segmentBuildInfo: newSegmentIndexBuildInfo(),
		segmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
	}
	return im
}

func (s *CopySegmentTaskSuite) TestClone_DeepCopiesProto() {
	task := createTestCopyTask(100, 2001).(*copySegmentTask)

	cloned := task.Clone()
	// Mutating the clone (as UpdateTask actions do) must not leak into the
	// original task; otherwise a failed catalog save during UpdateTask leaves
	// the in-memory cache already mutated while etcd keeps the old state.
	UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed)(cloned)
	UpdateCopyTaskReason("mutated on clone")(cloned)

	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskInProgress, task.GetState())
	s.Empty(task.GetReason())
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskFailed, cloned.GetState())
	s.Equal("mutated on clone", cloned.GetReason())
}

// fakeUntrackedDropHandler stands in for the inspector's cleanup retry queue.
// It records the exact dispatch handed over and can report an outstanding
// cleanup so dispatch gating can be exercised without a live inspector.
type fakeUntrackedDropHandler struct {
	nodeID      int64
	taskID      int64
	taskVersion int64
	reject      bool
	pending     map[int64]bool
}

func (h *fakeUntrackedDropHandler) EnqueueUntrackedDrop(nodeID, taskID, taskVersion int64) bool {
	h.nodeID, h.taskID, h.taskVersion = nodeID, taskID, taskVersion
	return !h.reject
}

func (h *fakeUntrackedDropHandler) HasPendingUntrackedDrop(taskID int64) bool {
	return h.pending[taskID]
}

// createTestCopyTask creates a minimal CopySegmentTask for testing syncVectorScalarIndexes.
func createTestCopyTask(collectionID int64, segmentID int64) CopySegmentTask {
	task := &copySegmentTask{
		ctx:   context.Background(),
		tr:    timerecord.NewTimeRecorder("test"),
		times: taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 1, TargetSegmentId: segmentID, PartitionId: 10},
		},
	})
	return task
}

func newTestCopyJob(jobID int64, state datapb.CopySegmentJobState) CopySegmentJob {
	return &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        jobID,
			CollectionId: 100,
			State:        state,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
}

func newCopySegmentTaskTestMeta(t *testing.T, task *copySegmentTask) (CopySegmentMeta, *meta) {
	ctx := context.Background()
	catalog := kvdatacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	m := &meta{
		catalog:  catalog,
		segments: NewSegmentsInfo(),
	}
	copyMeta, err := NewCopySegmentMeta(ctx, catalog, m, nil, nil)
	assert.NoError(t, err)
	// Every task has a parent job in production (the checker creates the job
	// first), and the result-commit guards key off that job's state. Default it
	// to Executing; tests that need another state re-AddJob the same ID, which
	// replaces this one.
	assert.NoError(t, copyMeta.AddJob(ctx, newTestCopyJob(task.GetJobId(),
		datapb.CopySegmentJobState_CopySegmentJobExecuting)))
	assert.NoError(t, copyMeta.AddTask(ctx, task))
	return copyMeta, m
}

func newTestCopySegment(segmentID int64) *SegmentInfo {
	return NewSegmentInfo(&datapb.SegmentInfo{
		ID:            segmentID,
		CollectionID:  100,
		PartitionID:   10,
		State:         commonpb.SegmentState_Importing,
		IsImporting:   true,
		NumOfRows:     100,
		InsertChannel: "ch1",
	})
}

func makeTestCopySegmentBinlogs() []*datapb.FieldBinlog {
	return []*datapb.FieldBinlog{
		{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{
				{LogID: 1, EntriesNum: 100},
			},
		},
	}
}

func (s *CopySegmentTaskSuite) TestSyncVectorScalarIndexes_EmptyResult() {
	result := &datapb.CopySegmentResult{
		SegmentId:  100,
		IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{},
	}
	task := createTestCopyTask(1, 100)
	err := syncVectorScalarIndexes(context.Background(), result, task, &meta{}, nil)
	s.NoError(err)
}

func (s *CopySegmentTaskSuite) TestSyncVectorScalarIndexes_SingleIndex() {
	collectionID := int64(1)
	segmentID := int64(100)

	indexes := map[int64]*model.Index{
		300: {CollectionID: collectionID, FieldID: 101, IndexID: 300, IndexName: "vec_idx"},
	}
	im := createTestIndexMeta(s.T(), collectionID, indexes)
	m := &meta{indexMeta: im, segments: NewSegmentsInfo()}
	m.segments.SetSegment(segmentID, NewSegmentInfo(&datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		NumOfRows:    4321,
	}))

	result := &datapb.CopySegmentResult{
		SegmentId:    segmentID,
		ImportedRows: 0,
		IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{
			5001: {
				FieldId:        101,
				IndexId:        200, // source indexID
				BuildId:        5001,
				IndexName:      "vec_idx",
				IndexFilePaths: []string{"HNSW"},
				IndexSize:      10000,
			},
		},
	}
	task := createTestCopyTask(collectionID, segmentID)

	err := syncVectorScalarIndexes(context.Background(), result, task, m, nil)
	s.NoError(err)

	// Verify the segment index was added with target indexID and Finished state
	segIdx, ok := im.segmentBuildInfo.Get(5001)
	s.True(ok)
	s.Equal(int64(300), segIdx.IndexID) // target indexID, not source 200
	s.Equal(commonpb.IndexState_Finished, segIdx.IndexState)
	s.Equal(int64(4321), segIdx.NumRows)
}

func (s *CopySegmentTaskSuite) TestSyncVectorScalarIndexes_PreservesIndexStorePathVersion() {
	collectionID := int64(1)
	segmentID := int64(100)

	indexes := map[int64]*model.Index{
		300: {CollectionID: collectionID, FieldID: 101, IndexID: 300, IndexName: "idx_v0"},
		301: {CollectionID: collectionID, FieldID: 102, IndexID: 301, IndexName: "idx_v1"},
	}
	im := createTestIndexMeta(s.T(), collectionID, indexes)
	m := &meta{indexMeta: im}

	result := &datapb.CopySegmentResult{
		SegmentId:    segmentID,
		ImportedRows: 1000,
		IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{
			2001: {
				FieldId:               101,
				IndexId:               1001,
				BuildId:               2001,
				IndexName:             "idx_v0",
				IndexFilePaths:        []string{"v0_file"},
				IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
			},
			2002: {
				FieldId:               102,
				IndexId:               1002,
				BuildId:               2002,
				IndexName:             "idx_v1",
				IndexFilePaths:        []string{"v1_file"},
				IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
			},
		},
	}
	task := createTestCopyTask(collectionID, segmentID)

	err := syncVectorScalarIndexes(context.Background(), result, task, m, nil)
	s.NoError(err)

	segIdxV0, ok := im.segmentBuildInfo.Get(2001)
	s.True(ok)
	s.Equal(indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED, segIdxV0.IndexStorePathVersion)

	segIdxV1, ok := im.segmentBuildInfo.Get(2002)
	s.True(ok)
	s.Equal(indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED, segIdxV1.IndexStorePathVersion)
}

func (s *CopySegmentTaskSuite) TestSyncVectorScalarIndexes_MultipleIndexesPerField() {
	collectionID := int64(1)
	segmentID := int64(100)

	// Two JSON path indexes on the same field (fieldID=101)
	indexes := map[int64]*model.Index{
		300: {CollectionID: collectionID, FieldID: 101, IndexID: 300, IndexName: "idx_category"},
		301: {CollectionID: collectionID, FieldID: 101, IndexID: 301, IndexName: "idx_price"},
		302: {CollectionID: collectionID, FieldID: 102, IndexID: 302, IndexName: "vec_idx"},
	}
	im := createTestIndexMeta(s.T(), collectionID, indexes)
	m := &meta{indexMeta: im}

	// Result keyed by buildID (not fieldID), with IndexName for matching
	result := &datapb.CopySegmentResult{
		SegmentId:    segmentID,
		ImportedRows: 1000,
		IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{
			5001: {
				FieldId:        101,
				IndexId:        200, // source indexID
				BuildId:        5001,
				IndexName:      "idx_category",
				IndexFilePaths: []string{"inverted1"},
				IndexSize:      3000,
			},
			5002: {
				FieldId:        101, // same field!
				IndexId:        201, // different source indexID
				BuildId:        5002,
				IndexName:      "idx_price",
				IndexFilePaths: []string{"inverted2"},
				IndexSize:      4000,
			},
			5003: {
				FieldId:        102,
				IndexId:        202,
				BuildId:        5003,
				IndexName:      "vec_idx",
				IndexFilePaths: []string{"HNSW"},
				IndexSize:      10000,
			},
		},
	}
	task := createTestCopyTask(collectionID, segmentID)

	err := syncVectorScalarIndexes(context.Background(), result, task, m, nil)
	s.NoError(err)

	// All three indexes should be synced with correct target indexIDs
	for buildID, expectedIndexID := range map[int64]int64{5001: 300, 5002: 301, 5003: 302} {
		segIdx, ok := im.segmentBuildInfo.Get(buildID)
		s.True(ok, "buildID %d should exist", buildID)
		s.Equal(expectedIndexID, segIdx.IndexID, "buildID %d should map to indexID %d", buildID, expectedIndexID)
		s.Equal(commonpb.IndexState_Finished, segIdx.IndexState)
	}
}

func (s *CopySegmentTaskSuite) TestSyncVectorScalarIndexes_IndexNameNotFound() {
	collectionID := int64(1)
	segmentID := int64(100)

	indexes := map[int64]*model.Index{
		300: {CollectionID: collectionID, FieldID: 101, IndexID: 300, IndexName: "vec_idx"},
	}
	im := createTestIndexMeta(s.T(), collectionID, indexes)
	m := &meta{indexMeta: im}

	// Source has an index name that doesn't exist in target -> should skip
	result := &datapb.CopySegmentResult{
		SegmentId:    segmentID,
		ImportedRows: 1000,
		IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{
			5001: {
				FieldId:   101,
				BuildId:   5001,
				IndexName: "nonexistent_idx",
			},
		},
	}
	task := createTestCopyTask(collectionID, segmentID)

	err := syncVectorScalarIndexes(context.Background(), result, task, m, nil)
	s.NoError(err)

	// Should not be added
	_, ok := im.segmentBuildInfo.Get(5001)
	s.False(ok)
}

func (s *CopySegmentTaskSuite) TestSyncVectorScalarIndexes_AddSegmentIndexError() {
	collectionID := int64(1)
	segmentID := int64(100)

	errCatalog := catalogmocks.NewDataCoordCatalog(s.T())
	errCatalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).
		Return(errors.New("catalog error"))

	indexes := map[int64]*model.Index{
		300: {CollectionID: collectionID, FieldID: 101, IndexID: 300, IndexName: "vec_idx"},
	}
	im := createTestIndexMeta(s.T(), collectionID, indexes, errCatalog)
	m := &meta{indexMeta: im}

	result := &datapb.CopySegmentResult{
		SegmentId:    segmentID,
		ImportedRows: 1000,
		IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{
			5001: {FieldId: 101, BuildId: 5001, IndexName: "vec_idx", IndexFilePaths: []string{"HNSW"}},
		},
	}
	task := createTestCopyTask(collectionID, segmentID)

	// Mock copyMeta for error path (UpdateTask and UpdateJobStateAndReleaseRef)
	copyCatalog := catalogmocks.NewDataCoordCatalog(s.T())
	copyCatalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(nil, nil)
	copyCatalog.EXPECT().ListCopySegmentTasks(mock.Anything).Return(nil, nil)
	copyCatalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	copyCatalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Maybe()
	copyMeta, cmErr := NewCopySegmentMeta(context.TODO(), copyCatalog, nil, nil, nil)
	s.NoError(cmErr)

	syncErr := syncVectorScalarIndexes(context.Background(), result, task, m, copyMeta)
	s.Error(syncErr)
	s.Contains(syncErr.Error(), "catalog error")
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_KeepsSegmentUnpublishedOnCompletion() {
	collectionID := int64(1)
	segmentID := int64(100)

	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segs []*datapb.SegmentInfo, _ ...metastore.BinlogsIncrement) error {
		s.Require().Len(segs, 1)
		seg := segs[0]
		assert.Equal(s.T(), segmentID, seg.GetID())
		// Publishing is finishJob's job now: a completed task records binlogs
		// but leaves the segment invisible until the whole restore succeeds.
		assert.Equal(s.T(), commonpb.SegmentState_Importing, seg.GetState())
		assert.True(s.T(), seg.GetIsImporting())
		return nil
	}).Once()
	mt := &meta{ctx: context.Background(), catalog: catalog, segments: NewSegmentsInfo()}
	mt.segments.SetSegment(segmentID, NewSegmentInfo(&datapb.SegmentInfo{
		ID:            segmentID,
		CollectionID:  collectionID,
		PartitionID:   10,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Importing,
		IsImporting:   true,
	}))

	copyCatalog := catalogmocks.NewDataCoordCatalog(s.T())
	copyCatalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(nil, nil)
	copyCatalog.EXPECT().ListCopySegmentTasks(mock.Anything).Return(nil, nil)
	copyCatalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	copyMeta, err := NewCopySegmentMeta(context.Background(), copyCatalog, nil, nil, nil)
	s.Require().NoError(err)

	result := &datapb.QueryCopySegmentResponse{
		State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		SegmentResults: []*datapb.CopySegmentResult{{
			SegmentId: segmentID,
			Binlogs: []*datapb.FieldBinlog{{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{{LogID: 1, LogPath: "files/binlog/1"}},
			}},
		}},
	}
	task := createTestCopyTask(collectionID, segmentID)

	err = SyncCopySegmentTask(task, result, copyMeta, mt)
	s.Require().NoError(err)
	updated := mt.GetSegment(context.Background(), segmentID)
	s.Require().NotNil(updated)
	s.Equal(commonpb.SegmentState_Importing, updated.GetState())
	s.True(updated.GetIsImporting())
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_EmptyManifestKeepsSegmentUnpublished() {
	collectionID := int64(1)
	segmentID := int64(101)

	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	mt := &meta{ctx: context.Background(), catalog: catalog, segments: NewSegmentsInfo()}
	mt.segments.SetSegment(segmentID, NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collectionID,
		PartitionID:    10,
		InsertChannel:  "ch1",
		State:          commonpb.SegmentState_Importing,
		IsImporting:    true,
		StorageVersion: 3,
	}))

	copyCatalog := catalogmocks.NewDataCoordCatalog(s.T())
	copyCatalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(nil, nil)
	copyCatalog.EXPECT().ListCopySegmentTasks(mock.Anything).Return(nil, nil)
	copyCatalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	copyMeta, err := NewCopySegmentMeta(context.Background(), copyCatalog, nil, nil, nil)
	s.Require().NoError(err)

	result := &datapb.QueryCopySegmentResponse{
		State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		SegmentResults: []*datapb.CopySegmentResult{{
			SegmentId:    segmentID,
			ManifestPath: "",
			Binlogs: []*datapb.FieldBinlog{{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{{LogID: 2, LogPath: "files/binlog/2"}},
			}},
		}},
	}
	task := createTestCopyTask(collectionID, segmentID)

	err = SyncCopySegmentTask(task, result, copyMeta, mt)
	s.Require().NoError(err)
	updated := mt.GetSegment(context.Background(), segmentID)
	s.Require().NotNil(updated)
	s.Equal(commonpb.SegmentState_Importing, updated.GetState())
	s.True(updated.GetIsImporting())
	s.Empty(updated.GetManifestPath())
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_ManifestUpdateKeepsSegmentUnpublished() {
	collectionID := int64(1)
	segmentID := int64(102)
	manifestPath := `{"ver":3,"base_path":"files/insert_log/1/10/102"}`

	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	mt := &meta{ctx: context.Background(), catalog: catalog, segments: NewSegmentsInfo()}
	mt.segments.SetSegment(segmentID, NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collectionID,
		PartitionID:    10,
		InsertChannel:  "ch1",
		State:          commonpb.SegmentState_Importing,
		IsImporting:    true,
		StorageVersion: 3,
	}))

	copyCatalog := catalogmocks.NewDataCoordCatalog(s.T())
	copyCatalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(nil, nil)
	copyCatalog.EXPECT().ListCopySegmentTasks(mock.Anything).Return(nil, nil)
	copyCatalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	copyMeta, err := NewCopySegmentMeta(context.Background(), copyCatalog, nil, nil, nil)
	s.Require().NoError(err)

	result := &datapb.QueryCopySegmentResponse{
		State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		SegmentResults: []*datapb.CopySegmentResult{{
			SegmentId:    segmentID,
			ManifestPath: manifestPath,
			Binlogs: []*datapb.FieldBinlog{{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{{LogID: 3, LogPath: "files/binlog/3"}},
			}},
		}},
	}
	task := createTestCopyTask(collectionID, segmentID)

	err = SyncCopySegmentTask(task, result, copyMeta, mt)
	s.Require().NoError(err)
	updated := mt.GetSegment(context.Background(), segmentID)
	s.Require().NotNil(updated)
	s.Equal(commonpb.SegmentState_Importing, updated.GetState())
	s.True(updated.GetIsImporting())
	s.Equal(manifestPath, updated.GetManifestPath())
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_PreservesImportingFlagOnFailure() {
	collectionID := int64(1)
	segmentID := int64(103)

	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("alter failed")).Once()
	catalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCopySegmentTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Maybe()
	mt := &meta{ctx: context.Background(), catalog: catalog, segments: NewSegmentsInfo()}
	mt.segments.SetSegment(segmentID, NewSegmentInfo(&datapb.SegmentInfo{
		ID:            segmentID,
		CollectionID:  collectionID,
		PartitionID:   10,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Importing,
		IsImporting:   true,
	}))
	copyMeta, err := NewCopySegmentMeta(context.Background(), catalog, nil, nil, nil)
	s.Require().NoError(err)

	result := &datapb.QueryCopySegmentResponse{
		State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		SegmentResults: []*datapb.CopySegmentResult{{
			SegmentId: segmentID,
			Binlogs: []*datapb.FieldBinlog{{
				FieldID: 100,
				Binlogs: []*datapb.Binlog{{LogID: 4, LogPath: "files/binlog/4"}},
			}},
		}},
	}
	task := createTestCopyTask(collectionID, segmentID)

	err = SyncCopySegmentTask(task, result, copyMeta, mt)
	s.Require().Error(err)
	updated := mt.GetSegment(context.Background(), segmentID)
	s.Require().NotNil(updated)
	s.Equal(commonpb.SegmentState_Importing, updated.GetState())
	s.True(updated.GetIsImporting())
}

func TestAssembleCopySegmentRequest_SourceSegmentNotFound(t *testing.T) {
	// Arrange: snapshot data has segment 1, but task maps from segment 999 which doesn't exist
	snapshotData := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			Name:         "test_snapshot",
		},
		Segments: []*datapb.SegmentDescription{
			{SegmentId: 1, PartitionId: 10},
		},
	}

	sm := &snapshotMeta{}
	// Mock ReadSnapshotData to return our test data
	mock1 := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(snapshotData, nil).Build()
	defer mock1.UnPatch()

	task := &copySegmentTask{
		ctx:          context.Background(),
		snapshotMeta: sm,
		tr:           timerecord.NewTimeRecorder("test"),
		times:        taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 100,
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 999, TargetSegmentId: 2001, PartitionId: 10}, // 999 not in snapshot
		},
	})

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: 100,
			SnapshotName: "test_snapshot",
		},
		tr:            timerecord.NewTimeRecorder("test_job"),
		snapshotCache: &copySegmentSnapshotCache{},
	}

	// Act
	req, err := AssembleCopySegmentRequest(task, job)

	// Assert
	assert.Nil(t, req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "source segment 999 not found")
}

func TestAssembleCopySegmentRequest_MarksExternalCollection(t *testing.T) {
	snapshotData := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			CollectionId: 100,
			Name:         "test_snapshot",
		},
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "external_pk", ExternalField: "pk"},
				},
			},
		},
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:   1,
				PartitionId: 10,
				IndexFiles: []*indexpb.IndexFilePathInfo{{
					IndexFilePaths: []string{
						"source-root/files/files/index_files/1001/2001/3001/index",
					},
				}},
			},
		},
	}

	sm := &snapshotMeta{}
	mockReadSnapshot := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(snapshotData, nil).Build()
	defer mockReadSnapshot.UnPatch()

	mockStorageConfig := mockey.Mock(createStorageConfig).Return(nil).Build()
	defer mockStorageConfig.UnPatch()

	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	task.snapshotMeta = sm
	task.alloc = &embeddedAllocator{}
	mockAlloc := mockey.Mock((*embeddedAllocator).AllocID).Return(typeutil.UniqueID(9001), nil).Build()
	defer mockAlloc.UnPatch()
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: 100,
			SnapshotName: "test_snapshot",
		},
		tr:            timerecord.NewTimeRecorder("test_job"),
		snapshotCache: &copySegmentSnapshotCache{},
	}

	req, err := AssembleCopySegmentRequest(task, job)
	assert.NoError(t, err)
	assert.Len(t, req.GetSources(), 1)
	assert.True(t, req.GetSources()[0].GetIsExternalCollection())
}

func TestAssembleCopySegmentRequest_ExternalSnapshotRootRemap(t *testing.T) {
	snapshotData := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			Name:         "test_snapshot",
		},
		Layout: datapb.SnapshotLayout_SnapshotLayoutSelfContained,
		Segments: []*datapb.SegmentDescription{
			{SegmentId: 1, PartitionId: 10},
		},
	}

	sm := &snapshotMeta{}
	sourceCM := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
		ForeignBucket: "bucket",
		ForeignCM:     sourceCM,
	}, nil).Build()
	defer mockResolve.UnPatch()
	mockReadExternal := mockey.Mock((*snapshotMeta).ReadExternalSnapshotDataWithChunkManager).To(
		func(_ *snapshotMeta, _ context.Context, gotCM storage.ChunkManager, snapshotS3Location string, includeSegments bool) (*snapshotstorage.SnapshotData, error) {
			assert.Same(t, sourceCM, gotCM)
			assert.Equal(t, "s3://bucket/source-root/snapshots/100/metadata/1.json", snapshotS3Location)
			assert.True(t, includeSegments)
			return snapshotData, nil
		}).Build()
	defer mockReadExternal.UnPatch()
	mockReadLocal := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(nil, errors.New("must not read local snapshot")).Build()
	defer mockReadLocal.UnPatch()
	mockStorage := mockey.Mock(createStorageConfig).Return(&indexpb.StorageConfig{RootPath: "target-root"}).Build()
	defer mockStorage.UnPatch()

	task := &copySegmentTask{
		ctx:          context.Background(),
		snapshotMeta: sm,
		tr:           timerecord.NewTimeRecorder("test"),
		times:        taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 200,
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 1, TargetSegmentId: 2001, PartitionId: 20},
		},
	})

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:              100,
			CollectionId:       200,
			SnapshotName:       "test_snapshot",
			External:           true,
			SnapshotS3Location: "s3://bucket/source-root/snapshots/100/metadata/1.json",
		},
		tr:            timerecord.NewTimeRecorder("test_job"),
		snapshotCache: &copySegmentSnapshotCache{},
	}

	req, err := AssembleCopySegmentRequest(task, job)

	require.NoError(t, err)
	require.NotNil(t, req)
	assert.Len(t, req.Sources, 1)
	assert.Len(t, req.Targets, 1)
	assert.Equal(t, "s3://bucket/source-root/files", req.Sources[0].GetSourceRootPath())
	assert.Equal(t, "target-root", req.Targets[0].GetTargetRootPath())
}

func TestAssembleCopySegmentRequest_ExternalReferencedSnapshotRootRemap(t *testing.T) {
	snapshotData := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			Name:         "test_snapshot",
		},
		Layout: datapb.SnapshotLayout_SnapshotLayoutReferenced,
		Segments: []*datapb.SegmentDescription{
			{SegmentId: 1, PartitionId: 10},
		},
	}

	sm := &snapshotMeta{}
	sourceCM := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
		ForeignBucket: "bucket",
		ForeignCM:     sourceCM,
	}, nil).Build()
	defer mockResolve.UnPatch()
	mockReadExternal := mockey.Mock((*snapshotMeta).ReadExternalSnapshotDataWithChunkManager).Return(snapshotData, nil).Build()
	defer mockReadExternal.UnPatch()
	mockStorage := mockey.Mock(createStorageConfig).Return(&indexpb.StorageConfig{RootPath: "target-root"}).Build()
	defer mockStorage.UnPatch()

	task := &copySegmentTask{
		ctx:          context.Background(),
		snapshotMeta: sm,
		tr:           timerecord.NewTimeRecorder("test"),
		times:        taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 200,
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 1, TargetSegmentId: 2001, PartitionId: 20},
		},
	})

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:              100,
			CollectionId:       200,
			SnapshotName:       "test_snapshot",
			External:           true,
			SnapshotS3Location: "s3://bucket/source-root/snapshots/100/metadata/1.json",
		},
		tr:            timerecord.NewTimeRecorder("test_job"),
		snapshotCache: &copySegmentSnapshotCache{},
	}

	req, err := AssembleCopySegmentRequest(task, job)

	require.NoError(t, err)
	require.Len(t, req.GetSources(), 1)
	assert.Equal(t, "s3://bucket/source-root", req.GetSources()[0].GetSourceRootPath())
	assert.Equal(t, "target-root", req.GetTargets()[0].GetTargetRootPath())
}

func TestDeriveSnapshotSourceRootURI_ByLayout(t *testing.T) {
	tests := []struct {
		name     string
		location string
		layout   datapb.SnapshotLayout
		want     string
	}{
		{
			name:     "self-contained uses bundle files root",
			location: "s3://bucket/source-root/snapshots/100/metadata/1.json",
			layout:   datapb.SnapshotLayout_SnapshotLayoutSelfContained,
			want:     "s3://bucket/source-root/files",
		},
		{
			name:     "referenced uses snapshot source root",
			location: "s3://bucket/source-root/snapshots/100/metadata/1.json",
			layout:   datapb.SnapshotLayout_SnapshotLayoutReferenced,
			want:     "s3://bucket/source-root",
		},
		{
			name:     "unknown legacy layout behaves as referenced",
			location: "s3://bucket/source-root/snapshots/100/metadata/1.json",
			layout:   datapb.SnapshotLayout_SnapshotLayoutUnknown,
			want:     "s3://bucket/source-root",
		},
		{
			name:     "endpoint style keeps bucket in path",
			location: "https://storage.example.com/bucket/source-root/snapshots/100/metadata/1.json",
			layout:   datapb.SnapshotLayout_SnapshotLayoutReferenced,
			want:     "https://storage.example.com/bucket/source-root",
		},
		{
			name:     "self-contained bucket root uses files root",
			location: "s3://bucket/snapshots/100/metadata/1.json",
			layout:   datapb.SnapshotLayout_SnapshotLayoutSelfContained,
			want:     "s3://bucket/files",
		},
		{
			name:     "referenced bucket root uses bucket root",
			location: "s3://bucket/snapshots/100/metadata/1.json",
			layout:   datapb.SnapshotLayout_SnapshotLayoutReferenced,
			want:     "s3://bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := deriveSnapshotSourceRootURI(tt.location, tt.layout)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestAssembleCopySegmentRequest_ExternalSnapshotCarriesForeignSpecAndRef(t *testing.T) {
	snapshotData := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			Name:         "test_snapshot",
		},
		Layout: datapb.SnapshotLayout_SnapshotLayoutSelfContained,
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:   1,
				PartitionId: 10,
				IndexFiles: []*indexpb.IndexFilePathInfo{{
					IndexFilePaths: []string{
						"source-root/files/files/index_files/1001/2001/3001/index",
					},
				}},
			},
		},
	}

	sm := &snapshotMeta{}
	sourceCM := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).To(
		func(
			_ context.Context,
			_ *objectstorage.Config,
			direction snapshotstorage.Direction,
			foreignURI string,
			externalSpec string,
		) (*snapshotstorage.ResolvedForeignStorage, error) {
			assert.Equal(t, snapshotstorage.DirectionRestore, direction)
			assert.Equal(t, "s3://bucket/source-root/snapshots/100/metadata/1.json", foreignURI)
			assert.Equal(t, `{"extfs":{"region":"us-west-2"}}`, externalSpec)
			return &snapshotstorage.ResolvedForeignStorage{
				ForeignBucket: "bucket",
				ForeignCM:     sourceCM,
			}, nil
		}).Build()
	defer mockResolve.UnPatch()
	mockReadExternal := mockey.Mock((*snapshotMeta).ReadExternalSnapshotDataWithChunkManager).To(
		func(_ *snapshotMeta, _ context.Context, gotCM storage.ChunkManager, snapshotS3Location string, includeSegments bool) (*snapshotstorage.SnapshotData, error) {
			assert.Same(t, sourceCM, gotCM)
			assert.Equal(t, "s3://bucket/source-root/snapshots/100/metadata/1.json", snapshotS3Location)
			assert.True(t, includeSegments)
			return snapshotData, nil
		}).Build()
	defer mockReadExternal.UnPatch()
	mockStorage := mockey.Mock(createStorageConfig).Return(&indexpb.StorageConfig{RootPath: "target-root"}).Build()
	defer mockStorage.UnPatch()

	nextID := int64(9001)
	alloc := &embeddedAllocator{}
	mockAlloc := mockey.Mock((*embeddedAllocator).AllocID).To(func(ctx context.Context) (typeutil.UniqueID, error) {
		id := nextID
		nextID++
		return id, nil
	}).Build()
	defer mockAlloc.UnPatch()

	task := &copySegmentTask{
		ctx:          context.Background(),
		snapshotMeta: sm,
		alloc:        alloc,
		tr:           timerecord.NewTimeRecorder("test"),
		times:        taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 200,
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 1, TargetSegmentId: 2001, PartitionId: 20},
		},
	})

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:              100,
			CollectionId:       200,
			SnapshotName:       "test_snapshot",
			External:           true,
			SnapshotS3Location: "s3://bucket/source-root/snapshots/100/metadata/1.json",
			ExternalSpec:       `{"extfs":{"region":"us-west-2"}}`,
		},
		tr:            timerecord.NewTimeRecorder("test_job"),
		snapshotCache: &copySegmentSnapshotCache{},
	}

	req, err := AssembleCopySegmentRequest(task, job)

	require.NoError(t, err)
	require.NotNil(t, req)
	assert.Equal(t, `{"extfs":{"region":"us-west-2"}}`, req.GetExternalSpec())
	require.Len(t, req.GetSources(), 1)
	assert.Equal(t, "s3://bucket/source-root/files", req.GetSources()[0].GetSourceRootPath())
}

func TestAssembleCopySegmentRequest_ExternalSnapshotFingerprint(t *testing.T) {
	const snapshotLocation = "s3://bucket/source-root/snapshots/100/metadata/1.json"
	newSnapshot := func() *snapshotstorage.SnapshotData {
		return &snapshotstorage.SnapshotData{
			SnapshotInfo: &datapb.SnapshotInfo{
				Id:           1,
				CollectionId: 100,
				Name:         "test_snapshot",
				S3Location:   snapshotLocation,
			},
			Collection: &datapb.CollectionDescription{},
			Layout:     datapb.SnapshotLayout_SnapshotLayoutReferenced,
			Segments: []*datapb.SegmentDescription{{
				SegmentId:   1,
				PartitionId: 10,
			}},
		}
	}
	newTask := func(sm *snapshotMeta) *copySegmentTask {
		task := &copySegmentTask{
			ctx:          context.Background(),
			snapshotMeta: sm,
			tr:           timerecord.NewTimeRecorder("test"),
			times:        taskcommon.NewTimes(),
		}
		task.task.Store(&datapb.CopySegmentTask{
			TaskId:       1001,
			JobId:        100,
			CollectionId: 200,
			IdMappings: []*datapb.CopySegmentIDMapping{{
				SourceSegmentId: 1,
				TargetSegmentId: 2001,
				PartitionId:     20,
			}},
		})
		return task
	}

	t.Run("mismatch is rejected", func(t *testing.T) {
		snapshotData := newSnapshot()
		sourceCM := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
		mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
			ForeignBucket: "bucket",
			ForeignCM:     sourceCM,
		}, nil).Build()
		defer mockResolve.UnPatch()
		mockRead := mockey.Mock((*snapshotMeta).ReadExternalSnapshotDataWithChunkManager).
			Return(snapshotData, nil).Build()
		defer mockRead.UnPatch()

		job := &copySegmentJob{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:               100,
				CollectionId:        200,
				SnapshotName:        "test_snapshot",
				External:            true,
				SnapshotS3Location:  snapshotLocation,
				SnapshotFingerprint: "different-fingerprint",
			},
			snapshotCache: &copySegmentSnapshotCache{},
		}

		req, err := AssembleCopySegmentRequest(newTask(&snapshotMeta{}), job)

		require.Error(t, err)
		assert.Nil(t, req)
		assert.True(t, errors.Is(err, merr.ErrDataIntegrity))
	})

	t.Run("matching snapshot is cached per job", func(t *testing.T) {
		snapshotData := newSnapshot()
		fingerprint, err := snapshotstorage.SnapshotFingerprint(snapshotData)
		require.NoError(t, err)
		sourceCM := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
		mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
			ForeignBucket: "bucket",
			ForeignCM:     sourceCM,
		}, nil).Build()
		defer mockResolve.UnPatch()
		readCalls := 0
		mockRead := mockey.Mock((*snapshotMeta).ReadExternalSnapshotDataWithChunkManager).To(
			func(*snapshotMeta, context.Context, storage.ChunkManager, string, bool) (*snapshotstorage.SnapshotData, error) {
				readCalls++
				return snapshotData, nil
			}).Build()
		defer mockRead.UnPatch()
		mockStorage := mockey.Mock(createStorageConfig).Return(&indexpb.StorageConfig{RootPath: "target-root"}).Build()
		defer mockStorage.UnPatch()

		job := &copySegmentJob{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:               100,
				CollectionId:        200,
				SnapshotName:        "test_snapshot",
				External:            true,
				SnapshotS3Location:  snapshotLocation,
				SnapshotFingerprint: fingerprint,
			},
			snapshotCache: &copySegmentSnapshotCache{},
		}
		task := newTask(&snapshotMeta{})

		first, err := AssembleCopySegmentRequest(task, job)
		require.NoError(t, err)
		second, err := AssembleCopySegmentRequest(task, job)
		require.NoError(t, err)
		assert.Equal(t, first, second)
		assert.Equal(t, 1, readCalls)
	})
}

func TestAssembleCopySegmentRequest_AllocatesTextAndJsonBuildIDs(t *testing.T) {
	// Arrange: snapshot data with segment that has vector, text, and JSON key indexes
	snapshotData := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			Name:         "test_snapshot",
		},
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:   1,
				PartitionId: 10,
				IndexFiles: []*indexpb.IndexFilePathInfo{
					{BuildID: 3001, FieldID: 100, IndexID: 1001},
				},
				TextIndexFiles: map[int64]*datapb.TextIndexStats{
					200: {FieldID: 200, BuildID: 4001},
				},
				JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
					300: {FieldID: 300, BuildID: 5001},
				},
			},
		},
	}

	sm := &snapshotMeta{}
	mock1 := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(snapshotData, nil).Build()
	defer mock1.UnPatch()

	// Mock allocator to return sequential IDs starting from 9001
	nextID := int64(9001)
	alloc := &embeddedAllocator{}
	mock2 := mockey.Mock((*embeddedAllocator).AllocID).To(func(ctx context.Context) (typeutil.UniqueID, error) {
		id := nextID
		nextID++
		return id, nil
	}).Build()
	defer mock2.UnPatch()

	// Mock Params access
	mock3 := mockey.Mock(createStorageConfig).Return(nil).Build()
	defer mock3.UnPatch()

	task := &copySegmentTask{
		ctx:          context.Background(),
		snapshotMeta: sm,
		alloc:        alloc,
		tr:           timerecord.NewTimeRecorder("test"),
		times:        taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 100,
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 1, TargetSegmentId: 2001, PartitionId: 10},
		},
	})

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: 100,
			SnapshotName: "test_snapshot",
		},
		tr:            timerecord.NewTimeRecorder("test_job"),
		snapshotCache: &copySegmentSnapshotCache{},
	}

	// Act
	req, err := AssembleCopySegmentRequest(task, job)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, req)
	assert.Len(t, req.Targets, 1)

	newBuildIDs := req.Targets[0].GetNewBuildIds()
	// Should have 3 entries: one for vector index (3001), one for text (4001), one for JSON (5001)
	assert.Len(t, newBuildIDs, 3)
	assert.Contains(t, newBuildIDs, int64(3001))
	assert.Contains(t, newBuildIDs, int64(4001))
	assert.Contains(t, newBuildIDs, int64(5001))

	// All new IDs should be distinct and >= 9001
	seenIDs := make(map[int64]bool)
	for _, newID := range newBuildIDs {
		assert.True(t, newID >= 9001, "new build ID should be allocated by allocator")
		assert.False(t, seenIDs[newID], "new build IDs should be unique")
		seenIDs[newID] = true
	}
}

func TestAssembleCopySegmentRequest_RedispatchAllocatesFreshBuildIDs(t *testing.T) {
	// Regression test for re-dispatch idempotency: when a task is reset to
	// Pending after a worker loss and dispatched again, the new attempt must
	// allocate a fresh set of buildIDs. Index files written by the partial
	// earlier attempt (under the old buildIDs) are then never referenced by
	// meta — they stay orphaned and are removed by GC — while the new attempt
	// cannot collide with them.
	snapshotData := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			Name:         "test_snapshot",
		},
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:   1,
				PartitionId: 10,
				IndexFiles: []*indexpb.IndexFilePathInfo{
					{BuildID: 3001, FieldID: 100, IndexID: 1001},
				},
			},
		},
	}

	sm := &snapshotMeta{}
	mock1 := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(snapshotData, nil).Build()
	defer mock1.UnPatch()

	nextID := int64(9001)
	alloc := &embeddedAllocator{}
	mock2 := mockey.Mock((*embeddedAllocator).AllocID).To(func(ctx context.Context) (typeutil.UniqueID, error) {
		id := nextID
		nextID++
		return id, nil
	}).Build()
	defer mock2.UnPatch()

	mock3 := mockey.Mock(createStorageConfig).Return(nil).Build()
	defer mock3.UnPatch()

	task := &copySegmentTask{
		snapshotMeta: sm,
		alloc:        alloc,
		tr:           timerecord.NewTimeRecorder("test"),
		times:        taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 100,
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 1, TargetSegmentId: 2001, PartitionId: 10},
		},
	})

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: 100,
			SnapshotName: "test_snapshot",
		},
		tr:            timerecord.NewTimeRecorder("test_job"),
		snapshotCache: &copySegmentSnapshotCache{},
	}

	// First dispatch attempt
	firstReq, err := AssembleCopySegmentRequest(task, job)
	assert.NoError(t, err)
	// Second dispatch attempt after a simulated worker loss + reset
	secondReq, err := AssembleCopySegmentRequest(task, job)
	assert.NoError(t, err)

	firstIDs := firstReq.Targets[0].GetNewBuildIds()
	secondIDs := secondReq.Targets[0].GetNewBuildIds()
	assert.Len(t, firstIDs, 1)
	assert.Len(t, secondIDs, 1)
	// The re-dispatched attempt must not reuse any buildID from the first
	// attempt, otherwise its index files could collide with partial files
	// written by the lost worker.
	for srcID, newFirst := range firstIDs {
		newSecond, ok := secondIDs[srcID]
		assert.True(t, ok)
		assert.NotEqual(t, newFirst, newSecond,
			"re-dispatch must allocate a fresh buildID for source buildID %d", srcID)
	}
}

// embeddedAllocator: named type for mockey interface-method patching; avoids a
// go1.26 `go vet` printf-pass panic on method expressions of anonymous structs.
type embeddedAllocator struct{ allocator.Allocator }

// TestQueryTaskOnWorker_WorkerLossDoesNotResetWhenJobTerminal is the regression
// test for the review nit on the worker-loss reset path: the reset must be
// conditional on the parent job still being active. A confirmed-loss response
// can land long after the query was issued, by which time another task may have
// failed the job. Reviving this task to Pending would let the scheduler issue
// one more dispatch for an already-dead job before checkFailedJob converges it
// back to Failed.
func (s *CopySegmentTaskSuite) TestQueryTaskOnWorker_WorkerLossConvergesToFailedWhenJobTerminal() {
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().QueryCopySegment(mock.Anything, mock.Anything).Return(
		nil,
		merr.WrapErrNodeNotFound(10),
	)

	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	task.task.Load().NodeId = 10
	task.task.Load().TaskVersion = 2
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
	// Parent job already failed (fail-fast triggered by a sibling task).
	s.NoError(copyMeta.AddJob(context.Background(),
		newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobFailed)))
	handler := &fakeUntrackedDropHandler{}
	copyMeta.(*copySegmentMeta).setUntrackedDropHandler(handler)

	task.QueryTaskOnWorker(cluster)

	// The task must NOT be revived to Pending (the scheduler would issue one
	// extra dispatch for a dead job), and it must NOT stay InProgress on the
	// dead node either: nothing would ever clear that assignment, and checkGC
	// skips tasks with a node assignment — task and job would leak forever.
	// It converges to Failed with NullNodeID, terminal and never dispatchable.
	updatedTask := copyMeta.GetTask(context.Background(), 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskFailed, updatedTask.GetState())
	s.EqualValues(NullNodeID, updatedTask.GetNodeId())
	s.NotEmpty(updatedTask.GetReason())
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobFailed,
		copyMeta.GetJob(context.Background(), 100).GetState())
	// With NodeId cleared, processTerminal can no longer address the worker
	// task; the abort of the lost dispatch is queued at its own epoch instead.
	s.EqualValues(10, handler.nodeID)
	s.EqualValues(1001, handler.taskID)
	s.EqualValues(2, handler.taskVersion)
}

func (s *CopySegmentTaskSuite) TestQueryTaskOnWorker_WorkerLossResolveErrorLeavesTask() {
	// A resolve failure (catalog down) must leave the task untouched; the next
	// query round retries the whole worker-loss handling.
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().QueryCopySegment(mock.Anything, mock.Anything).Return(
		nil,
		merr.WrapErrNodeNotFound(10),
	)

	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	task.task.Load().NodeId = 10
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
	s.NoError(copyMeta.AddJob(context.Background(),
		newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting)))

	mockResolve := mockey.Mock((*copySegmentMeta).ResolveTaskOnWorkerLoss).
		Return(workerLossOutcome{resolution: workerLossSkipped, nodeID: NullNodeID}, errors.New("catalog down")).Build()
	defer mockResolve.UnPatch()

	task.QueryTaskOnWorker(cluster)

	updatedTask := copyMeta.GetTask(context.Background(), 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskInProgress, updatedTask.GetState())
	s.EqualValues(10, updatedTask.GetNodeId())
}

func (s *CopySegmentTaskSuite) TestQueryTaskOnWorker_WorkerLossSkippedResolution() {
	// A concurrent path already transitioned the task; the worker-loss handler
	// must accept the skip without touching the task.
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().QueryCopySegment(mock.Anything, mock.Anything).Return(
		nil,
		merr.WrapErrNodeNotFound(10),
	)

	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	task.task.Load().NodeId = 10
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
	s.NoError(copyMeta.AddJob(context.Background(),
		newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting)))

	mockResolve := mockey.Mock((*copySegmentMeta).ResolveTaskOnWorkerLoss).
		Return(workerLossOutcome{resolution: workerLossSkipped, nodeID: NullNodeID}, nil).Build()
	defer mockResolve.UnPatch()

	task.QueryTaskOnWorker(cluster)

	updatedTask := copyMeta.GetTask(context.Background(), 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskInProgress, updatedTask.GetState())
	s.EqualValues(10, updatedTask.GetNodeId())
}

// TestCreateTaskOnWorker_SkipsDispatchWhenJobTerminal: the scheduler dispatches
// on its own tick and only looks at the task state, so a task can still be
// Pending here seconds after a sibling failed the job. Starting the copy then
// burns worker slots and writes objects for a restore already reported Failed,
// against a snapshot whose pin has been released.
func (s *CopySegmentTaskSuite) TestCreateTaskOnWorker_SkipsDispatchWhenJobTerminal() {
	// No CreateCopySegment expectation: any RPC fails the test.
	cluster := session.NewMockCluster(s.T())

	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
	s.NoError(copyMeta.AddJob(context.Background(),
		newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobFailed)))
	s.NoError(copyMeta.UpdateTask(context.Background(), 1001,
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskPending),
		UpdateCopyTaskNodeID(NullNodeID)))

	task.CreateTaskOnWorker(10, cluster)

	updated := copyMeta.GetTask(context.Background(), 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskPending, updated.GetState())
	s.EqualValues(NullNodeID, updated.GetNodeId())
}

func (s *CopySegmentTaskSuite) TestCreateTaskOnWorker_DoesNotResurrectTerminalTask() {
	ctx := context.Background()
	cluster := session.NewMockCluster(s.T())
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
	s.NoError(copyMeta.UpdateTask(ctx, task.GetTaskId(),
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskPending),
		UpdateCopyTaskNodeID(NullNodeID)))

	assembleMock := mockey.Mock(AssembleCopySegmentRequest).
		Return(&datapb.CopySegmentRequest{TaskID: task.GetTaskId()}, nil).Build()
	defer assembleMock.UnPatch()

	cluster.EXPECT().CreateCopySegment(int64(10), mock.Anything, int64(100)).RunAndReturn(
		func(int64, *datapb.CopySegmentRequest, int64) error {
			// Simulate a sibling failure while CreateCopySegment is in flight.
			s.NoError(copyMeta.UpdateJob(ctx, task.GetJobId(),
				UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
				UpdateCopyJobReason("sibling failed")))
			s.NoError(copyMeta.UpdateTask(ctx, task.GetTaskId(),
				UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed),
				UpdateCopyTaskReason("sibling failed")))
			return nil
		}).Once()
	cluster.EXPECT().DropCopySegment(mock.Anything, int64(10), task.GetTaskId(), mock.Anything, mock.Anything).Return(nil).Once()

	task.CreateTaskOnWorker(10, cluster)

	updated := copyMeta.GetTask(ctx, task.GetTaskId())
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskFailed, updated.GetState())
	s.Equal("sibling failed", updated.GetReason())
	s.EqualValues(NullNodeID, updated.GetNodeId())
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobFailed,
		copyMeta.GetJob(ctx, task.GetJobId()).GetState())
}

func (s *CopySegmentTaskSuite) TestCreateTaskOnWorker_QueuesUntrackedWorkerDrop() {
	ctx := context.Background()
	cluster := session.NewMockCluster(s.T())
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
	s.NoError(copyMeta.UpdateTask(ctx, task.GetTaskId(),
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskPending),
		UpdateCopyTaskNodeID(NullNodeID)))

	assembleMock := mockey.Mock(AssembleCopySegmentRequest).
		Return(&datapb.CopySegmentRequest{TaskID: task.GetTaskId()}, nil).Build()
	defer assembleMock.UnPatch()

	handler := &fakeUntrackedDropHandler{}
	copyMeta.(*copySegmentMeta).setUntrackedDropHandler(handler)

	cluster.EXPECT().CreateCopySegment(int64(10), mock.Anything, int64(100)).RunAndReturn(
		func(int64, *datapb.CopySegmentRequest, int64) error {
			// Another dispatch becomes authoritative while this RPC is in flight.
			// Node 10 therefore cannot be stored in the task's single NodeID field.
			return copyMeta.UpdateTask(ctx, task.GetTaskId(),
				UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskInProgress),
				UpdateCopyTaskNodeID(20))
		}).Once()

	// No DropCopySegment expectation: cleanup must be handed to the inspector's
	// retry queue instead of consuming its only chance synchronously here.
	task.CreateTaskOnWorker(10, cluster)

	s.EqualValues(10, handler.nodeID)
	s.EqualValues(task.GetTaskId(), handler.taskID)
	s.EqualValues(20, copyMeta.GetTask(ctx, task.GetTaskId()).GetNodeId())
}

func (s *CopySegmentTaskSuite) TestCreateTaskOnWorker_QueuesUntrackedWorkerDropAfterCommitFailure() {
	ctx := context.Background()
	cluster := session.NewMockCluster(s.T())
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
	// The claim re-validates the dispatch preconditions, so the task has to be
	// genuinely dispatchable to reach the commit this case is about.
	s.NoError(copyMeta.UpdateTask(ctx, task.GetTaskId(),
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskPending),
		UpdateCopyTaskNodeID(NullNodeID)))

	assembleMock := mockey.Mock(AssembleCopySegmentRequest).
		Return(&datapb.CopySegmentRequest{TaskID: task.GetTaskId()}, nil).Build()
	defer assembleMock.UnPatch()
	commitMock := mockey.Mock((*copySegmentMeta).CommitTaskDispatch).
		Return(taskDispatchCleanupUntracked, errors.New("etcd unavailable")).Build()
	defer commitMock.UnPatch()

	handler := &fakeUntrackedDropHandler{}
	copyMeta.(*copySegmentMeta).setUntrackedDropHandler(handler)
	cluster.EXPECT().CreateCopySegment(int64(10), mock.Anything, int64(100)).Return(nil).Once()

	// The accepted worker task still needs retryable cleanup when the metadata
	// commit itself fails; no direct DropCopySegment call is expected here.
	task.CreateTaskOnWorker(10, cluster)

	s.EqualValues(10, handler.nodeID)
	s.EqualValues(task.GetTaskId(), handler.taskID)
}

// TestCreateTaskOnWorker_CleanupHandleCarriesDispatchEpoch pins the identity the
// cleanup is queued under. The worker only honors a drop that still matches the
// dispatch it holds, so the queued epoch must be the one this dispatch sent —
// not the task's epoch at some later point.
func (s *CopySegmentTaskSuite) TestCreateTaskOnWorker_CleanupHandleCarriesDispatchEpoch() {
	ctx := context.Background()
	cluster := session.NewMockCluster(s.T())
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
	s.NoError(copyMeta.UpdateTask(ctx, task.GetTaskId(),
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskPending),
		UpdateCopyTaskNodeID(NullNodeID)))

	assembleMock := mockey.Mock(AssembleCopySegmentRequest).
		Return(&datapb.CopySegmentRequest{TaskID: task.GetTaskId()}, nil).Build()
	defer assembleMock.UnPatch()
	commitMock := mockey.Mock((*copySegmentMeta).CommitTaskDispatch).
		Return(taskDispatchCleanupUntracked, errors.New("etcd unavailable")).Build()
	defer commitMock.UnPatch()

	handler := &fakeUntrackedDropHandler{}
	copyMeta.(*copySegmentMeta).setUntrackedDropHandler(handler)

	var dispatchedVersion int64
	cluster.EXPECT().CreateCopySegment(int64(10), mock.Anything, int64(100)).RunAndReturn(
		func(_ int64, req *datapb.CopySegmentRequest, _ int64) error {
			dispatchedVersion = req.GetTaskVersion()
			return nil
		}).Once()

	task.CreateTaskOnWorker(10, cluster)

	// The epoch is claimed and persisted before the worker can accept the task,
	// so it stays monotonic across a DataCoord restart.
	s.EqualValues(1, dispatchedVersion)
	s.EqualValues(1, copyMeta.GetTask(ctx, task.GetTaskId()).GetTaskVersion())
	s.EqualValues(dispatchedVersion, handler.taskVersion)
}

// TestCreateTaskOnWorker_SkipsDispatchWhileCleanupPending is the regression for
// the untracked-drop/redispatch race: a queued abort for an earlier dispatch and
// a fresh dispatch of the same task write the very same deterministic target
// object keys, so the abort landing later would delete output the new dispatch
// published. Dispatch must therefore wait until the earlier worker task is
// provably gone.
func (s *CopySegmentTaskSuite) TestCreateTaskOnWorker_SkipsDispatchWhileCleanupPending() {
	ctx := context.Background()
	cluster := session.NewMockCluster(s.T())
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
	s.NoError(copyMeta.UpdateTask(ctx, task.GetTaskId(),
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskPending),
		UpdateCopyTaskNodeID(NullNodeID)))

	assembleMock := mockey.Mock(AssembleCopySegmentRequest).
		Return(&datapb.CopySegmentRequest{TaskID: task.GetTaskId()}, nil).Build()
	defer assembleMock.UnPatch()

	handler := &fakeUntrackedDropHandler{pending: map[int64]bool{task.GetTaskId(): true}}
	copyMeta.(*copySegmentMeta).setUntrackedDropHandler(handler)

	// No CreateCopySegment expectation: any dispatch fails the test.
	task.CreateTaskOnWorker(20, cluster)

	// The task keeps its Pending state and unclaimed epoch, so the scheduler
	// re-queues it and the dispatch resumes once the cleanup converges.
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskPending,
		copyMeta.GetTask(ctx, task.GetTaskId()).GetState())
	s.EqualValues(0, copyMeta.GetTask(ctx, task.GetTaskId()).GetTaskVersion())

	handler.pending[task.GetTaskId()] = false
	cluster.EXPECT().CreateCopySegment(int64(20), mock.Anything, int64(100)).Return(nil).Once()
	task.CreateTaskOnWorker(20, cluster)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
		copyMeta.GetTask(ctx, task.GetTaskId()).GetState())
}

// TestCreateTaskOnWorker_SkipsDispatchWhileAnotherDispatchInFlight is the
// regression for the cross-node double-dispatch hole. The scheduler pops a task
// out of pendingTasks and only inserts it into runningTasks once
// CommitTaskDispatch has persisted InProgress; in between, Enqueue's dedup sees
// the task in neither queue, so the inspector's next round re-queues it and a
// second dispatch starts while the first is still in flight — on a different
// node, since pickNode rebuilds its slot heap every round. Neither existing
// defense covers that: the epoch fence lives on the worker runtime and so only
// fences a re-dispatch to the SAME node, and the untracked-drop gate only fences
// a dispatch beginning after the drop is queued. Both dispatches would then write
// the same deterministic target keys, and the loser's abort would delete the
// winner's published output.
func (s *CopySegmentTaskSuite) TestCreateTaskOnWorker_SkipsDispatchWhileAnotherDispatchInFlight() {
	ctx := context.Background()
	cluster := session.NewMockCluster(s.T())
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
	s.NoError(copyMeta.UpdateTask(ctx, task.GetTaskId(),
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskPending),
		UpdateCopyTaskNodeID(NullNodeID)))

	assembleMock := mockey.Mock(AssembleCopySegmentRequest).
		Return(&datapb.CopySegmentRequest{TaskID: task.GetTaskId()}, nil).Build()
	defer assembleMock.UnPatch()

	// Stand in for a dispatch that has claimed the task and is still between its
	// CreateCopySegment RPC and CommitTaskDispatch.
	inFlightVersion, err := copyMeta.ClaimTaskDispatch(ctx, task.GetTaskId())
	s.NoError(err)

	// No CreateCopySegment expectation: a second dispatch fails the test.
	task.CreateTaskOnWorker(20, cluster)

	// The task is untouched by the refused dispatch — same Pending state, same
	// epoch — so the scheduler re-queues it and it goes out once the in-flight
	// dispatch resolves.
	refused := copyMeta.GetTask(ctx, task.GetTaskId())
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskPending, refused.GetState())
	s.EqualValues(NullNodeID, refused.GetNodeId())
	s.EqualValues(inFlightVersion, refused.GetTaskVersion())

	// Once the earlier dispatch releases the task, the next one proceeds and
	// claims a fresh epoch.
	copyMeta.ReleaseTaskDispatch(task.GetTaskId())
	cluster.EXPECT().CreateCopySegment(int64(20), mock.Anything, int64(100)).Return(nil).Once()
	task.CreateTaskOnWorker(20, cluster)

	dispatched := copyMeta.GetTask(ctx, task.GetTaskId())
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskInProgress, dispatched.GetState())
	s.EqualValues(20, dispatched.GetNodeId())
	s.EqualValues(inFlightVersion+1, dispatched.GetTaskVersion())
}

// TestCreateTaskOnWorker_SkipsDispatchWhenClaimFails covers the other way a
// claim can be refused: the epoch could not be persisted. No worker may be told
// about a dispatch whose epoch metadata has forgotten, since the drop that
// fences it would then never match.
func (s *CopySegmentTaskSuite) TestCreateTaskOnWorker_SkipsDispatchWhenClaimFails() {
	ctx := context.Background()
	cluster := session.NewMockCluster(s.T())
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
	s.NoError(copyMeta.UpdateTask(ctx, task.GetTaskId(),
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskPending),
		UpdateCopyTaskNodeID(NullNodeID)))

	assembleMock := mockey.Mock(AssembleCopySegmentRequest).
		Return(&datapb.CopySegmentRequest{TaskID: task.GetTaskId()}, nil).Build()
	defer assembleMock.UnPatch()

	claimMock := mockey.Mock((*copySegmentMeta).ClaimTaskDispatch).
		Return(int64(0), errors.New("etcd unavailable")).Build()
	defer claimMock.UnPatch()

	// No CreateCopySegment expectation: any dispatch fails the test.
	task.CreateTaskOnWorker(20, cluster)

	unchanged := copyMeta.GetTask(ctx, task.GetTaskId())
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskPending, unchanged.GetState())
	s.EqualValues(NullNodeID, unchanged.GetNodeId())
}

// TestCreateTaskOnWorker_ReleasesDispatchClaimOnEveryPath guards the other side
// of the claim: it is the only thing standing between the task and the worker,
// so any exit path that forgets to release it stalls the task permanently.
func (s *CopySegmentTaskSuite) TestCreateTaskOnWorker_ReleasesDispatchClaimOnEveryPath() {
	ctx := context.Background()
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
	s.NoError(copyMeta.UpdateTask(ctx, task.GetTaskId(),
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskPending),
		UpdateCopyTaskNodeID(NullNodeID)))

	assembleMock := mockey.Mock(AssembleCopySegmentRequest).
		Return(&datapb.CopySegmentRequest{TaskID: task.GetTaskId()}, nil).Build()
	defer assembleMock.UnPatch()

	// A dispatch the worker rejects: the claim must not outlive the attempt. The
	// error is ambiguous, so the dispatch is also handed to cleanup — with no
	// retry handler registered here that falls back to a direct drop.
	rejecting := session.NewMockCluster(s.T())
	rejecting.EXPECT().CreateCopySegment(int64(20), mock.Anything, int64(100)).
		Return(errors.New("datanode unavailable")).Once()
	rejecting.EXPECT().DropCopySegment(mock.Anything, int64(20), task.GetTaskId(), int64(1), true).
		Return(nil).Once()
	task.CreateTaskOnWorker(20, rejecting)
	s.False(copyMeta.(*copySegmentMeta).hasInFlightDispatch(task.GetTaskId()))

	// A dispatch the worker accepts: the claim is released once the outcome is
	// committed, and the untracked-drop gate takes over from there.
	accepting := session.NewMockCluster(s.T())
	accepting.EXPECT().CreateCopySegment(int64(21), mock.Anything, int64(100)).Return(nil).Once()
	task.CreateTaskOnWorker(21, accepting)
	s.False(copyMeta.(*copySegmentMeta).hasInFlightDispatch(task.GetTaskId()))
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
		copyMeta.GetTask(ctx, task.GetTaskId()).GetState())
}

// TestDropTaskOnWorker_SendsCurrentDispatchEpoch keeps the tracked cleanup path
// on the same fence as the untracked one: the worker must be able to tell that
// this drop belongs to the dispatch it is actually running.
func (s *CopySegmentTaskSuite) TestDropTaskOnWorker_SendsCurrentDispatchEpoch() {
	ctx := context.Background()
	cluster := session.NewMockCluster(s.T())
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
	s.NoError(copyMeta.UpdateTask(ctx, task.GetTaskId(),
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed),
		UpdateCopyTaskNodeID(10),
		UpdateCopyTaskVersion(4)))

	var droppedVersion int64
	cluster.EXPECT().DropCopySegment(mock.Anything, int64(10), task.GetTaskId(), mock.Anything, true).
		RunAndReturn(func(_ context.Context, _ int64, _ int64, version int64, _ bool) error {
			droppedVersion = version
			return nil
		}).Once()

	task.DropTaskOnWorker(cluster)
	s.EqualValues(4, droppedVersion)
}

// TestQueryTaskOnWorker_StaleCompletedResultDiscardedAfterJobFailed is the
// regression for the stale-result race: for a restore split across tasks A and
// B, A fails -> markTaskAndJobFailed puts the job in Failed -> the next checker
// tick's checkFailedJob converges still-InProgress B to Failed. B is however
// still in the scheduler's running set, and the scheduler calls
// QueryTaskOnWorker BEFORE it inspects the task state, so B gets polled once
// more and its worker answers Completed. Applying that result would flip B's
// target segments to Flushed — queryable — and resurrect B Failed -> Completed
// underneath a job that already failed, serving part of a restore that never
// succeeded.
func (s *CopySegmentTaskSuite) TestQueryTaskOnWorker_StaleCompletedResultDiscardedAfterJobFailed() {
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().QueryCopySegment(mock.Anything, mock.Anything).Return(
		&datapb.QueryCopySegmentResponse{
			TaskID: 1001,
			State:  datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
			SegmentResults: []*datapb.CopySegmentResult{
				{SegmentId: 2001, ImportedRows: 100, Binlogs: makeTestCopySegmentBinlogs()},
			},
		},
		nil,
	)

	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, m := newCopySegmentTaskTestMeta(s.T(), task)
	s.NoError(m.AddSegment(context.Background(), newTestCopySegment(2001)))

	// A sibling failed the job, and checkFailedJob converged this task to Failed.
	s.NoError(copyMeta.AddJob(context.Background(),
		newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobFailed)))
	s.NoError(copyMeta.UpdateTask(context.Background(), 1001,
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed),
		UpdateCopyTaskReason("sibling task failed")))

	task.QueryTaskOnWorker(cluster)

	// The target segment must NOT become queryable.
	s.Equal(commonpb.SegmentState_Importing, m.GetSegment(context.Background(), 2001).GetState(),
		"a stale success must not expose the segments of a failed restore")

	// And the task must stay in its terminal state, not be resurrected.
	updated := copyMeta.GetTask(context.Background(), 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskFailed, updated.GetState())
	s.Equal("sibling task failed", updated.GetReason())
}

// TestQueryTaskOnWorker_StaleCompletedResultDiscardedAfterTaskLeftInProgress:
// the same guard also covers a task that left InProgress on its own while the
// job is still active.
func (s *CopySegmentTaskSuite) TestQueryTaskOnWorker_StaleCompletedResultDiscardedAfterTaskLeftInProgress() {
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().QueryCopySegment(mock.Anything, mock.Anything).Return(
		&datapb.QueryCopySegmentResponse{
			TaskID: 1001,
			State:  datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
			SegmentResults: []*datapb.CopySegmentResult{
				{SegmentId: 2001, ImportedRows: 100, Binlogs: makeTestCopySegmentBinlogs()},
			},
		},
		nil,
	)

	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, m := newCopySegmentTaskTestMeta(s.T(), task)
	s.NoError(m.AddSegment(context.Background(), newTestCopySegment(2001)))
	s.NoError(copyMeta.UpdateTask(context.Background(), 1001,
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskPending)))

	task.QueryTaskOnWorker(cluster)

	s.Equal(commonpb.SegmentState_Importing, m.GetSegment(context.Background(), 2001).GetState())
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskPending,
		copyMeta.GetTask(context.Background(), 1001).GetState())
}

// TestSyncCopySegmentTask_JobGoesTerminalMidApplyKeepsTaskTerminal covers the
// residual window: the pre-check passed, but the job went terminal while the
// result was being applied. The final task write is guarded under the meta
// lock, so the task must not be resurrected out of Failed. The segments it
// already flushed are the inspector's job-scoped cleanup to reclaim.
func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_JobGoesTerminalMidApplyKeepsTaskTerminal() {
	ctx := context.Background()
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, m := newCopySegmentTaskTestMeta(s.T(), task)
	s.NoError(m.AddSegment(ctx, newTestCopySegment(2001)))

	// The race lands between the pre-check and the state write.
	s.NoError(copyMeta.AddJob(ctx, newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobFailed)))
	s.NoError(copyMeta.UpdateTask(ctx, 1001,
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed)))

	resp := &datapb.QueryCopySegmentResponse{
		TaskID: 1001,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		SegmentResults: []*datapb.CopySegmentResult{
			{SegmentId: 2001, ImportedRows: 100, Binlogs: makeTestCopySegmentBinlogs()},
		},
	}
	s.NoError(SyncCopySegmentTask(task, resp, copyMeta, m))

	updated := copyMeta.GetTask(ctx, 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskFailed, updated.GetState())
	s.Zero(updated.(*copySegmentTask).task.Load().GetCompleteTs())
}

func (s *CopySegmentTaskSuite) TestDropTaskOnWorker_ClearNodeAssignmentFailureKeepsAssignment() {
	// If clearing the assignment fails to persist, the NodeID must stay so the
	// orphan is at least visible; state/reason are never touched by the drop.
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().DropCopySegment(mock.Anything, int64(10), int64(1001), mock.Anything, mock.Anything).Return(nil).Once()

	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	task.task.Load().NodeId = 10
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)

	mockUpdate := mockey.Mock((*copySegmentMeta).ClearTaskNodeAssignment).
		Return(false, errors.New("catalog down")).Build()
	defer mockUpdate.UnPatch()

	task.DropTaskOnWorker(cluster)

	updatedTask := copyMeta.GetTask(context.Background(), 1001)
	s.EqualValues(10, updatedTask.GetNodeId())
}

func (s *CopySegmentTaskSuite) TestDropTaskOnWorker_ClearsNodeAssignment() {
	// After a successful worker-side drop the meta NodeID must be cleared,
	// otherwise checkGC skips the task (and its job) forever.
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().DropCopySegment(mock.Anything, int64(10), int64(1001), mock.Anything, mock.Anything).Return(nil).Once()

	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	task.task.Load().NodeId = 10
	task.task.Load().State = datapb.CopySegmentTaskState_CopySegmentTaskCompleted
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)

	task.DropTaskOnWorker(cluster)

	updatedTask := copyMeta.GetTask(context.Background(), 1001)
	s.EqualValues(NullNodeID, updatedTask.GetNodeId())
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskCompleted, updatedTask.GetState())
}

func (s *CopySegmentTaskSuite) TestDropTaskOnWorker_NodeGoneStillClearsAssignment() {
	// ErrNodeNotFound means the node (and its in-memory task) is already gone:
	// the drop's goal is achieved, so the assignment must still be cleared.
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().DropCopySegment(mock.Anything, int64(10), int64(1001), mock.Anything, mock.Anything).
		Return(merr.WrapErrNodeNotFound(10)).Once()

	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	task.task.Load().NodeId = 10
	task.task.Load().State = datapb.CopySegmentTaskState_CopySegmentTaskFailed
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)

	task.DropTaskOnWorker(cluster)

	updatedTask := copyMeta.GetTask(context.Background(), 1001)
	s.EqualValues(NullNodeID, updatedTask.GetNodeId())
}

func (s *CopySegmentTaskSuite) TestDropTaskOnWorker_OtherErrorKeepsAssignment() {
	// A transport error does not prove the worker-side task is gone: keep the
	// assignment instead of leaking the worker task. The inspector's
	// processTerminal re-runs the drop on every round until it converges — see
	// TestProcessTerminal_RetriesDropUntilAssignmentCleared.
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().DropCopySegment(mock.Anything, int64(10), int64(1001), mock.Anything, mock.Anything).
		Return(errors.New("rpc timeout")).Once()

	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	task.task.Load().NodeId = 10
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)

	task.DropTaskOnWorker(cluster)

	updatedTask := copyMeta.GetTask(context.Background(), 1001)
	s.EqualValues(10, updatedTask.GetNodeId())
}

func (s *CopySegmentTaskSuite) TestDropTaskOnWorker_NoAssignmentIsNoop() {
	// No node assignment -> nothing to drop; the cluster must not be called
	// (mock has no expectation, an unexpected call fails the test).
	cluster := session.NewMockCluster(s.T())

	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	task.task.Load().NodeId = NullNodeID
	newCopySegmentTaskTestMeta(s.T(), task)

	task.DropTaskOnWorker(cluster)
}

// TestCreateTaskOnWorker_QueuesCleanupWhenCreateRPCFails covers the ambiguous
// CreateCopySegment failure. cluster.createTask runs the call under a request
// timeout, so an error can surface after the DataNode already registered the
// task — and the worker only ever removes a task on a Drop RPC, with no timeout
// GC. Returning silently would leave that worker task, its slot, and its copied
// objects behind forever, still writing the very target keys the re-dispatch
// will write. The dispatch therefore has to be handed to the cleanup retry loop.
func (s *CopySegmentTaskSuite) TestCreateTaskOnWorker_QueuesCleanupWhenCreateRPCFails() {
	ctx := context.Background()
	cluster := session.NewMockCluster(s.T())
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
	s.NoError(copyMeta.UpdateTask(ctx, task.GetTaskId(),
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskPending),
		UpdateCopyTaskNodeID(NullNodeID)))

	assembleMock := mockey.Mock(AssembleCopySegmentRequest).
		Return(&datapb.CopySegmentRequest{TaskID: task.GetTaskId()}, nil).Build()
	defer assembleMock.UnPatch()

	handler := &fakeUntrackedDropHandler{}
	copyMeta.(*copySegmentMeta).setUntrackedDropHandler(handler)

	var dispatchedVersion int64
	cluster.EXPECT().CreateCopySegment(int64(10), mock.Anything, int64(100)).RunAndReturn(
		func(_ int64, req *datapb.CopySegmentRequest, _ int64) error {
			dispatchedVersion = req.GetTaskVersion()
			return errors.New("context deadline exceeded")
		}).Once()

	// No direct DropCopySegment expectation: the handler owns the retry, so one
	// synchronous best-effort attempt here would consume the only chance.
	task.CreateTaskOnWorker(10, cluster)

	// Queued under the exact dispatch identity the worker would recognize.
	s.EqualValues(10, handler.nodeID)
	s.EqualValues(task.GetTaskId(), handler.taskID)
	s.EqualValues(dispatchedVersion, handler.taskVersion)

	// The task itself is untouched, so the scheduler re-queues it once the
	// cleanup converges.
	unchanged := copyMeta.GetTask(ctx, task.GetTaskId())
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskPending, unchanged.GetState())
	s.EqualValues(NullNodeID, unchanged.GetNodeId())
}

// TestCreateTaskOnWorker_SameNodeCommitDropsWithoutAbort is the regression for
// the missing same-node comparison in CommitTaskDispatch. A lagging dispatch can
// be scheduled back onto the node that already holds the authoritative task —
// and CreateCopySegment adopts this dispatch's epoch onto that shared worker
// runtime, so an unconditional abort at this epoch WOULD be honored. Routing it
// through the untracked path would therefore delete a Completed task's output,
// whose binlog paths are already in segment metadata. It must instead go through
// the task's own handle, which derives the abort flag from the task and job
// state — false here, i.e. a plain release.
func (s *CopySegmentTaskSuite) TestCreateTaskOnWorker_SameNodeCommitDropsWithoutAbort() {
	ctx := context.Background()
	cluster := session.NewMockCluster(s.T())
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
	s.NoError(copyMeta.UpdateTask(ctx, task.GetTaskId(),
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskPending),
		UpdateCopyTaskNodeID(NullNodeID)))

	assembleMock := mockey.Mock(AssembleCopySegmentRequest).
		Return(&datapb.CopySegmentRequest{TaskID: task.GetTaskId()}, nil).Build()
	defer assembleMock.UnPatch()

	// No retry handler: if this took the untracked path it would fall back to a
	// direct DropCopySegment with abort=true, which the expectation below rejects.
	cluster.EXPECT().CreateCopySegment(int64(10), mock.Anything, int64(100)).RunAndReturn(
		func(int64, *datapb.CopySegmentRequest, int64) error {
			// The task completes on this very node while the RPC is in flight.
			return copyMeta.UpdateTask(ctx, task.GetTaskId(),
				UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskCompleted),
				UpdateCopyTaskNodeID(10))
		}).Once()
	cluster.EXPECT().DropCopySegment(mock.Anything, int64(10), task.GetTaskId(), int64(1), false).
		Return(nil).Once()

	task.CreateTaskOnWorker(10, cluster)

	// The completed outcome survives, and the assignment is cleared so checkGC
	// can reclaim the task.
	settled := copyMeta.GetTask(ctx, task.GetTaskId())
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskCompleted, settled.GetState())
	s.EqualValues(NullNodeID, settled.GetNodeId())
}

// TestCreateTaskOnWorker_DifferentNodeCommitStillAborts is the other side of the
// same-node check: a dispatch that finds a DIFFERENT node authoritative must
// still be force-aborted, since metadata holds no handle on the node this
// dispatch reached.
func (s *CopySegmentTaskSuite) TestCreateTaskOnWorker_DifferentNodeCommitStillAborts() {
	ctx := context.Background()
	cluster := session.NewMockCluster(s.T())
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
	s.NoError(copyMeta.UpdateTask(ctx, task.GetTaskId(),
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskPending),
		UpdateCopyTaskNodeID(NullNodeID)))

	assembleMock := mockey.Mock(AssembleCopySegmentRequest).
		Return(&datapb.CopySegmentRequest{TaskID: task.GetTaskId()}, nil).Build()
	defer assembleMock.UnPatch()

	handler := &fakeUntrackedDropHandler{}
	copyMeta.(*copySegmentMeta).setUntrackedDropHandler(handler)

	cluster.EXPECT().CreateCopySegment(int64(10), mock.Anything, int64(100)).RunAndReturn(
		func(int64, *datapb.CopySegmentRequest, int64) error {
			return copyMeta.UpdateTask(ctx, task.GetTaskId(),
				UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskCompleted),
				UpdateCopyTaskNodeID(11))
		}).Once()

	task.CreateTaskOnWorker(10, cluster)

	// Queued against node 10, while metadata keeps node 11 as its own handle.
	s.EqualValues(10, handler.nodeID)
	s.EqualValues(task.GetTaskId(), handler.taskID)
	s.EqualValues(11, copyMeta.GetTask(ctx, task.GetTaskId()).GetNodeId())
}
