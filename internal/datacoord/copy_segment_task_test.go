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
	"github.com/milvus-io/milvus/internal/storagev2/packed"
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

type copySegmentClusterMock struct {
	session.Cluster
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

func (s *CopySegmentTaskSuite) TestCreateTaskOnWorkerUsesJobExternalFlag() {
	tests := []struct {
		name     string
		external bool
	}{
		{name: "local restore", external: false},
		{name: "external restore", external: true},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			task := createTestCopyTask(100, 2001).(*copySegmentTask)
			task.task.Load().State = datapb.CopySegmentTaskState_CopySegmentTaskPending
			copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
			task.copyMeta = copyMeta

			job := newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*copySegmentJob)
			job.External = test.external
			s.NoError(copyMeta.AddJob(context.Background(), job))

			req := &datapb.CopySegmentRequest{TaskID: task.GetTaskId()}
			mockAssemble := mockey.Mock(AssembleCopySegmentRequest).Return(req, nil).Build()
			defer mockAssemble.UnPatch()

			cluster := session.NewMockCluster(s.T())
			cluster.EXPECT().CreateCopySegment(int64(10), req, int64(100), test.external).Return(nil)

			task.CreateTaskOnWorker(10, cluster)

			updated := copyMeta.GetTask(context.Background(), task.GetTaskId())
			s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskInProgress, updated.GetState())
			s.EqualValues(10, updated.GetNodeId())
		})
	}
}

func (s *CopySegmentTaskSuite) TestCreateTaskOnWorkerFailsUnsupportedExternalTask() {
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	task.task.Load().State = datapb.CopySegmentTaskState_CopySegmentTaskPending
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
	task.copyMeta = copyMeta

	job := newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*copySegmentJob)
	job.External = true
	s.NoError(copyMeta.AddJob(context.Background(), job))

	req := &datapb.CopySegmentRequest{TaskID: task.GetTaskId()}
	assembleMock := mockey.Mock(AssembleCopySegmentRequest).Return(req, nil).Build()
	defer assembleMock.UnPatch()

	cluster := &copySegmentClusterMock{}
	createMock := mockey.Mock((*copySegmentClusterMock).CreateCopySegment).
		Return(merr.Wrapf(merr.ErrServiceUnimplemented,
			"unrecognized task type '%s'", taskcommon.ExternalCopySegment)).Build()
	defer createMock.UnPatch()

	task.CreateTaskOnWorker(10, cluster)

	updatedTask := copyMeta.GetTask(context.Background(), task.GetTaskId())
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskFailed, updatedTask.GetState())
	s.Contains(updatedTask.GetReason(), "datanode does not support external copy segment tasks")
	updatedJob := copyMeta.GetJob(context.Background(), task.GetJobId())
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobFailed, updatedJob.GetState())
	s.Equal(updatedTask.GetReason(), updatedJob.GetReason())
}

func (s *CopySegmentTaskSuite) TestCreateTaskOnWorkerRetriesOtherErrors() {
	tests := []struct {
		name     string
		external bool
		err      error
	}{
		{
			name:     "external service not ready",
			external: true,
			err:      merr.WrapErrServiceNotReady("datanode", 10, "Initializing"),
		},
		{
			name:     "local unimplemented",
			external: false,
			err:      merr.Wrapf(merr.ErrServiceUnimplemented, "unsupported local task"),
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			task := createTestCopyTask(100, 2001).(*copySegmentTask)
			task.task.Load().State = datapb.CopySegmentTaskState_CopySegmentTaskPending
			copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
			task.copyMeta = copyMeta

			job := newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*copySegmentJob)
			job.External = test.external
			s.NoError(copyMeta.AddJob(context.Background(), job))

			req := &datapb.CopySegmentRequest{TaskID: task.GetTaskId()}
			assembleMock := mockey.Mock(AssembleCopySegmentRequest).Return(req, nil).Build()
			defer assembleMock.UnPatch()

			cluster := &copySegmentClusterMock{}
			createMock := mockey.Mock((*copySegmentClusterMock).CreateCopySegment).
				Return(test.err).Build()
			defer createMock.UnPatch()

			task.CreateTaskOnWorker(10, cluster)

			updatedTask := copyMeta.GetTask(context.Background(), task.GetTaskId())
			s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskPending, updatedTask.GetState())
			s.Empty(updatedTask.GetReason())
			updatedJob := copyMeta.GetJob(context.Background(), task.GetJobId())
			s.Equal(datapb.CopySegmentJobState_CopySegmentJobExecuting, updatedJob.GetState())
		})
	}
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
	cluster.EXPECT().CreateCopySegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(int64, *datapb.CopySegmentRequest, int64, bool) error {
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
	// confirmed lost, so the task must be reset to Pending with NullNodeID so
	// the scheduler re-dispatches it to a live node rather than polling the
	// dead node until the job-level timeout.
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().QueryCopySegment(mock.Anything, mock.Anything).Return(
		nil,
		merr.WrapErrNodeNotFound(10),
	)

	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	task.task.Load().NodeId = 10
	copyMeta, _ := newCopySegmentTaskTestMeta(s.T(), task)
	s.NoError(copyMeta.AddJob(context.Background(), newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting)))

	task.QueryTaskOnWorker(cluster)

	updatedTask := copyMeta.GetTask(context.Background(), 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskPending, updatedTask.GetState())
	s.EqualValues(NullNodeID, updatedTask.GetNodeId())
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobExecuting,
		copyMeta.GetJob(context.Background(), 100).GetState())
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

	task.QueryTaskOnWorker(cluster)

	updatedTask := copyMeta.GetTask(context.Background(), 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskPending, updatedTask.GetState())
	s.EqualValues(NullNodeID, updatedTask.GetNodeId())
	s.Empty(updatedTask.GetReason())
	// The parent job must NOT be failed - the task is re-dispatched.
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobExecuting,
		copyMeta.GetJob(context.Background(), 100).GetState())
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

	segment := m.GetSegment(context.Background(), 2001)
	s.Equal(commonpb.SegmentState_Flushed, segment.GetState())
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
	s.Equal(commonpb.SegmentState_Flushed, segment.GetState())
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
	assert.NoError(t, copyMeta.AddTask(ctx, task))
	return copyMeta, m
}

func newTestCopySegment(segmentID int64) *SegmentInfo {
	return NewSegmentInfo(&datapb.SegmentInfo{
		ID:            segmentID,
		CollectionID:  100,
		PartitionID:   10,
		State:         commonpb.SegmentState_Importing,
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
	err := syncVectorScalarIndexes(context.Background(), result, task, &meta{}, nil, nil)
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

	err := syncVectorScalarIndexes(context.Background(), result, task, m, nil, nil)
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

	err := syncVectorScalarIndexes(context.Background(), result, task, m, nil, nil)
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

	err := syncVectorScalarIndexes(context.Background(), result, task, m, nil, nil)
	s.NoError(err)

	// All three indexes should be synced with correct target indexIDs
	for buildID, expectedIndexID := range map[int64]int64{5001: 300, 5002: 301, 5003: 302} {
		segIdx, ok := im.segmentBuildInfo.Get(buildID)
		s.True(ok, "buildID %d should exist", buildID)
		s.Equal(expectedIndexID, segIdx.IndexID, "buildID %d should map to indexID %d", buildID, expectedIndexID)
		s.Equal(commonpb.IndexState_Finished, segIdx.IndexState)
	}
}

// Publication follows the manifest READ-BACK, not the manifest pointer. A copy
// target whose worker re-derived the target's own entries yields records that
// are already published, so the backfill migration must not queue them; a
// target whose manifest carries no entry for the build - a StorageV1/V2 target,
// a result with no pointer, or an old DataNode that returned a pointer without
// running the republication - must stay unpublished and, when a backfill could
// still fix it, flagged.
func (s *CopySegmentTaskSuite) TestSyncVectorScalarIndexes_ManifestPublishedFollowsReadBack() {
	collectionID := int64(1)

	cases := []struct {
		name           string
		segmentID      int64
		storageVersion int64
		manifestPath   string
		// whether the read-back reported an entry for this segment's build
		inManifest bool
		published  bool
		// Only a StorageV3 segment is a backfill candidate at all, so a legacy
		// target stays unflagged even though its record is unpublished.
		hinted bool
	}{
		{"v3 target with a republished entry", 100, storage.StorageV3, "files/manifest/100/1", true, true, false},
		// The old-DataNode shape: a pointer, but an index section the copy never
		// rewrote. Trusting the pointer here would exempt the record forever.
		{"v3 target whose manifest carries no entry", 103, storage.StorageV3, "files/manifest/103/1", false, false, true},
		{"v3 target without manifest pointer", 101, storage.StorageV3, "", false, false, true},
		{"legacy storage target", 102, storage.StorageV2, "files/manifest/102/1", true, false, false},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			indexes := map[int64]*model.Index{
				300: {CollectionID: collectionID, FieldID: 101, IndexID: 300, IndexName: "vec_idx"},
			}
			im := createTestIndexMeta(s.T(), collectionID, indexes)
			m := &meta{indexMeta: im, segments: NewSegmentsInfo()}
			m.segments.SetSegment(tc.segmentID, NewSegmentInfo(&datapb.SegmentInfo{
				ID:             tc.segmentID,
				CollectionID:   collectionID,
				NumOfRows:      100,
				State:          commonpb.SegmentState_Flushed,
				StorageVersion: tc.storageVersion,
			}))

			buildID := 6000 + tc.segmentID
			var publishedBuilds map[int64]struct{}
			if tc.inManifest {
				publishedBuilds = map[int64]struct{}{buildID: {}}
			}
			result := &datapb.CopySegmentResult{
				SegmentId:    tc.segmentID,
				ManifestPath: tc.manifestPath,
				IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{
					buildID: {
						FieldId:        101,
						BuildId:        buildID,
						IndexName:      "vec_idx",
						IndexFilePaths: []string{"HNSW"},
					},
				},
			}
			task := createTestCopyTask(collectionID, tc.segmentID)

			s.NoError(syncVectorScalarIndexes(context.Background(), result, task, m, nil, publishedBuilds))

			segIdx, ok := im.segmentBuildInfo.Get(buildID)
			s.Require().True(ok)
			s.Equal(tc.published, segIdx.ManifestPublished)
			s.Equal(tc.hinted, m.GetSegment(context.Background(), tc.segmentID).NeedsManifestIndexBackfill())
		})
	}
}

// An AddSegmentIndex failure returns from the middle of the per-index loop. The
// unpublished records already installed by the earlier iterations are real work
// for the backfill, so the segment must still carry the hint - otherwise the
// inspector, which only visits flagged segments, skips it until a restart
// reseeds the hint from the records.
func (s *CopySegmentTaskSuite) TestSyncVectorScalarIndexes_HintsSegmentOnMidLoopFailure() {
	collectionID := int64(1)
	segmentID := int64(104)

	errCatalog := catalogmocks.NewDataCoordCatalog(s.T())
	errCatalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).
		Return(errors.New("catalog error"))

	im := createTestIndexMeta(s.T(), collectionID, map[int64]*model.Index{
		300: {CollectionID: collectionID, FieldID: 101, IndexID: 300, IndexName: "vec_idx"},
	}, errCatalog)
	m := &meta{indexMeta: im, segments: NewSegmentsInfo()}
	m.segments.SetSegment(segmentID, NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collectionID,
		NumOfRows:      100,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
	}))

	result := &datapb.CopySegmentResult{
		SegmentId:    segmentID,
		ManifestPath: "files/manifest/104/1",
		IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{
			7001: {FieldId: 101, BuildId: 7001, IndexName: "vec_idx", IndexFilePaths: []string{"HNSW"}},
		},
	}
	task := createTestCopyTask(collectionID, segmentID)

	copyCatalog := catalogmocks.NewDataCoordCatalog(s.T())
	copyCatalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(nil, nil)
	copyCatalog.EXPECT().ListCopySegmentTasks(mock.Anything).Return(nil, nil)
	copyCatalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	copyCatalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Maybe()
	copyMeta, cmErr := NewCopySegmentMeta(context.TODO(), copyCatalog, nil, nil, nil)
	s.Require().NoError(cmErr)

	s.Error(syncVectorScalarIndexes(context.Background(), result, task, m, copyMeta, nil))
	s.True(m.GetSegment(context.Background(), segmentID).NeedsManifestIndexBackfill())
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

	err := syncVectorScalarIndexes(context.Background(), result, task, m, nil, nil)
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

	syncErr := syncVectorScalarIndexes(context.Background(), result, task, m, copyMeta, nil)
	s.Error(syncErr)
	s.Contains(syncErr.Error(), "catalog error")
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_ClearsImportingFlagOnCompletion() {
	collectionID := int64(1)
	segmentID := int64(100)

	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segs []*datapb.SegmentInfo, _ ...metastore.BinlogsIncrement) error {
		s.Require().Len(segs, 1)
		seg := segs[0]
		assert.Equal(s.T(), segmentID, seg.GetID())
		assert.Equal(s.T(), commonpb.SegmentState_Flushed, seg.GetState())
		assert.False(s.T(), seg.GetIsImporting())
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
	s.Equal(commonpb.SegmentState_Flushed, updated.GetState())
	s.False(updated.GetIsImporting())
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_EmptyManifestStillClearsImportingFlag() {
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
	s.Equal(commonpb.SegmentState_Flushed, updated.GetState())
	s.False(updated.GetIsImporting())
	s.Empty(updated.GetManifestPath())
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_ManifestUpdateAndClearImportingFlag() {
	collectionID := int64(1)
	segmentID := int64(102)
	manifestPath := `{"ver":3,"base_path":"files/insert_log/1/10/102"}`

	// A StorageV3 pointer is read back before publication; this worker's
	// manifest carries no index entries, so it passes.
	defer mockey.Mock(createStorageConfig).Return(nil).Build().UnPatch()
	defer mockey.Mock(packed.GetManifestIndexInfos).Return(nil, nil).Build().UnPatch()

	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	// A fresh StorageV3 copy target publishes its first manifest inline via
	// UpdateManifest/UpdateSegmentsInfo, which writes through AlterSegments.
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
	s.Equal(commonpb.SegmentState_Flushed, updated.GetState())
	s.False(updated.GetIsImporting())
	s.Equal(manifestPath, updated.GetManifestPath())
}

// newCopiedManifestReadBackFixture wires a StorageV3 copy target plus a target
// index definition (indexID 300, "vec_idx") into the sync-path test meta, so a
// test only has to say what reading back the worker's manifest pointer reports.
func (s *CopySegmentTaskSuite) newCopiedManifestReadBackFixture() (CopySegmentTask, CopySegmentMeta, *meta, *datapb.QueryCopySegmentResponse) {
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, m := newCopySegmentTaskTestMeta(s.T(), task)
	m.indexMeta = createTestIndexMeta(s.T(), 100, map[int64]*model.Index{
		300: {CollectionID: 100, FieldID: 101, IndexID: 300, IndexName: "vec_idx"},
	})
	s.Require().NoError(m.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             2001,
		CollectionID:   100,
		PartitionID:    10,
		State:          commonpb.SegmentState_Importing,
		InsertChannel:  "ch1",
		StorageVersion: storage.StorageV3,
	})))

	resp := &datapb.QueryCopySegmentResponse{
		TaskID: 1001,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		SegmentResults: []*datapb.CopySegmentResult{{
			SegmentId:    2001,
			Binlogs:      makeTestCopySegmentBinlogs(),
			ManifestPath: `{"ver":3,"base_path":"files/insert_log/100/10/2001"}`,
		}},
	}
	return task, copyMeta, m, resp
}

// A worker result whose target manifest still carries an index entry foreign to
// the target collection is the fingerprint of an old DataNode that skipped the
// republication (it ignores inherited_index_ids and sends no acknowledgement).
// The pointer must be rejected before publication and surface as a hard task
// failure, not silent metadata pollution.
func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_ForeignManifestIndexEntryFailsTask() {
	ctx := context.Background()
	task, copyMeta, m, resp := s.newCopiedManifestReadBackFixture()

	defer mockey.Mock(createStorageConfig).Return(nil).Build().UnPatch()
	defer mockey.Mock(packed.GetManifestIndexInfos).Return([]packed.ManifestIndexInfo{
		// The target's own re-derived entry may coexist with the leftover; only
		// the foreign one is the failure.
		{IndexID: 300, BuildID: 9001, IndexName: "vec_idx"},
		// Inherited from the SOURCE collection - its stored path walks back to
		// the source's artifacts.
		{IndexID: 5001, BuildID: 6001, IndexName: "vec_idx"},
	}, nil).Build().UnPatch()

	err := SyncCopySegmentTask(task, resp, copyMeta, m)
	s.Require().Error(err)
	s.ErrorIs(err, merr.ErrServiceInternal)
	s.Contains(err.Error(), "2001", "the error must name the segment")
	s.Contains(err.Error(), "5001", "the error must name the foreign index ID")

	// The poisoned pointer was never published and the segment was not flushed.
	segment := m.GetSegment(ctx, 2001)
	s.Require().NotNil(segment)
	s.Equal(commonpb.SegmentState_Importing, segment.GetState())
	s.Empty(segment.GetManifestPath())

	updatedTask := copyMeta.GetTask(ctx, 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskFailed, updatedTask.GetState())
	s.Contains(updatedTask.GetReason(), "5001")
}

// A read-back that finds only target-owned entries (an upgraded worker that ran
// the republication) publishes the pointer as before.
func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_CleanManifestReadBackPublishes() {
	ctx := context.Background()
	task, copyMeta, m, resp := s.newCopiedManifestReadBackFixture()

	defer mockey.Mock(createStorageConfig).Return(nil).Build().UnPatch()
	defer mockey.Mock(packed.GetManifestIndexInfos).Return([]packed.ManifestIndexInfo{
		{IndexID: 300, BuildID: 9001, IndexName: "vec_idx"},
	}, nil).Build().UnPatch()

	err := SyncCopySegmentTask(task, resp, copyMeta, m)
	s.Require().NoError(err)

	segment := m.GetSegment(ctx, 2001)
	s.Require().NotNil(segment)
	s.Equal(commonpb.SegmentState_Flushed, segment.GetState())
	s.Equal(resp.GetSegmentResults()[0].GetManifestPath(), segment.GetManifestPath())

	updatedTask := copyMeta.GetTask(ctx, 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskCompleted, updatedTask.GetState())
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

// assembleIndexPrecedenceFixture builds a one-segment copy request whose source
// is a StorageV3 segment carrying the snapshot's own (etcd-derived) index
// metadata, so a test only has to say whether the segment is marked
// manifestHasIndex and what its manifest reports.
func assembleIndexPrecedenceFixture(t *testing.T, manifestHasIndex bool, manifestIndexes []packed.ManifestIndexInfo) *datapb.CopySegmentRequest {
	t.Helper()

	manifestPath := packed.MarshalManifestPath("files/insert_log/100/10/1", 3)
	snapshotData := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100, Name: "test_snapshot"},
		Indexes:      []*indexpb.IndexInfo{{IndexID: 1001, FieldID: 100}},
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:        1,
				PartitionId:      10,
				StorageVersion:   storage.StorageV3,
				ManifestPath:     manifestPath,
				ManifestHasIndex: manifestHasIndex,
				IndexFiles: []*indexpb.IndexFilePathInfo{
					{BuildID: 3001, FieldID: 100, IndexID: 1001, IndexName: "vec_idx"},
				},
			},
		},
	}

	defer mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(snapshotData, nil).Build().UnPatch()
	defer mockey.Mock(createStorageConfig).Return(nil).Build().UnPatch()
	defer mockey.Mock(packed.GetManifestIndexInfos).Return(manifestIndexes, nil).Build().UnPatch()

	nextID := int64(9001)
	defer mockey.Mock((*embeddedAllocator).AllocID).To(func(ctx context.Context) (typeutil.UniqueID, error) {
		id := nextID
		nextID++
		return id, nil
	}).Build().UnPatch()

	task := &copySegmentTask{
		ctx:          context.Background(),
		snapshotMeta: &snapshotMeta{},
		alloc:        &embeddedAllocator{},
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
		CopySegmentJob: &datapb.CopySegmentJob{JobId: 100, CollectionId: 100, SnapshotName: "test_snapshot"},
		tr:             timerecord.NewTimeRecorder("test_job"),
		snapshotCache:  &copySegmentSnapshotCache{},
	}

	req, err := AssembleCopySegmentRequest(task, job)
	require.NoError(t, err)
	require.Len(t, req.GetSources(), 1)
	require.Len(t, req.GetTargets(), 1)
	return req
}

// A marked segment whose entries were all retracted since (the sticky marker
// over-reads) yields an empty list, not an error. The segment still has
// indexes, recorded in etcd and captured by the snapshot, so the manifest must
// not be treated as the authority on whether they exist.
func TestAssembleCopySegmentRequest_EmptyManifestKeepsSnapshotIndexFiles(t *testing.T) {
	req := assembleIndexPrecedenceFixture(t, true, nil)

	indexFiles := req.GetSources()[0].GetIndexFiles()
	require.Len(t, indexFiles, 1, "snapshot index metadata must survive an index-less manifest")
	assert.Equal(t, int64(3001), indexFiles[0].GetBuildID())
	assert.Equal(t, "vec_idx", indexFiles[0].GetIndexName())

	// Nothing was inherited, so there is nothing for the worker to retract.
	assert.Empty(t, req.GetTargets()[0].GetInheritedIndexIds())
}

// The manifest read is not conditional on the snapshot lacking index metadata:
// in the normal case both carry entries, and the manifest's must still be
// retracted or the target would keep pointing at the source's artifacts.
func TestAssembleCopySegmentRequest_ManifestEntriesRetractedAlongsideSnapshotFiles(t *testing.T) {
	req := assembleIndexPrecedenceFixture(t, true, []packed.ManifestIndexInfo{
		{IndexID: 1001, BuildID: 3001, FieldID: 100, IndexName: "vec_idx"},
		{IndexID: 1002, BuildID: 3002, FieldID: 101, IndexName: "dropped_idx"},
	})

	// The snapshot answered which files to copy, so the manifest did not add to
	// or replace that list.
	indexFiles := req.GetSources()[0].GetIndexFiles()
	require.Len(t, indexFiles, 1)
	assert.Equal(t, int64(3001), indexFiles[0].GetBuildID())

	// Every inherited entry is retracted, including the one whose definition the
	// snapshot no longer carries - its artifact is not copied, so leaving the
	// entry would point the target at the source's files.
	assert.ElementsMatch(t, []int64{1001, 1002}, req.GetTargets()[0].GetInheritedIndexIds())
}

// An unmarked segment provably has no manifest index entries - the sticky
// marker is written in the same transaction as the first entry, and the
// snapshot captures both from the same record - so the manifest is not read at
// all. The fixture's manifest mock is poisoned with an entry precisely to
// prove the short-circuit: were the read to happen, the entry would surface in
// the inherited list. Snapshots from before the marker field existed read
// false and take this same path; their manifests predate index publication.
func TestAssembleCopySegmentRequest_UnmarkedSegmentSkipsManifestIndexRead(t *testing.T) {
	req := assembleIndexPrecedenceFixture(t, false, []packed.ManifestIndexInfo{
		{IndexID: 9999, BuildID: 9998, FieldID: 100, IndexName: "poison"},
	})

	// The snapshot's own index metadata is untouched by the skip.
	indexFiles := req.GetSources()[0].GetIndexFiles()
	require.Len(t, indexFiles, 1)
	assert.Equal(t, int64(3001), indexFiles[0].GetBuildID())

	assert.Empty(t, req.GetTargets()[0].GetInheritedIndexIds(),
		"an unmarked segment's manifest must not be consulted for inherited entries")
}

// Strict exclusivity on the copy path: with manifest publication off, the
// worker must receive no target index definitions at all, so it retracts the
// inherited entries and writes none of its own; with it on, the definitions
// flow so the worker can republish.
func (s *CopySegmentTaskSuite) TestBuildCopySegmentTargetIndexes_GatedBySwitch() {
	collectionID := int64(1)
	im := createTestIndexMeta(s.T(), collectionID, map[int64]*model.Index{
		300: {CollectionID: collectionID, FieldID: 101, IndexID: 300, IndexName: "vec_idx"},
	})
	m := &meta{indexMeta: im, segments: NewSegmentsInfo(), collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()}

	withSegmentIndexManifestWrites(s.T(), false)
	s.Nil(buildCopySegmentTargetIndexes(m, collectionID),
		"with manifest writes off the worker must get no definitions, or it would mint manifest entries the etcd records do not own")

	withSegmentIndexManifestWrites(s.T(), true)
	targets := buildCopySegmentTargetIndexes(m, collectionID)
	s.Require().Len(targets, 1)
	s.EqualValues(300, targets["vec_idx"].GetIndexId())
}

// With manifest publication on, a StorageV3 copy whose manifest read-back
// carries no entry for a synced build would produce a memory-only record (the
// etcd write is skipped for a manifest-backed segment) that silently vanishes
// on restart. The task must hard-fail with the upgrade hint instead.
func (s *CopySegmentTaskSuite) TestSyncVectorScalarIndexes_FailsWhenEntryMissingUnderManifestWrites() {
	withSegmentIndexManifestWrites(s.T(), true)
	collectionID := int64(1)
	segmentID := int64(105)

	im := createTestIndexMeta(s.T(), collectionID, map[int64]*model.Index{
		300: {CollectionID: collectionID, FieldID: 101, IndexID: 300, IndexName: "vec_idx"},
	})
	m := &meta{indexMeta: im, segments: NewSegmentsInfo()}
	m.segments.SetSegment(segmentID, NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collectionID,
		NumOfRows:      100,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
	}))

	result := &datapb.CopySegmentResult{
		SegmentId:    segmentID,
		ManifestPath: "files/manifest/105/1",
		IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{
			8001: {FieldId: 101, BuildId: 8001, IndexName: "vec_idx", IndexFilePaths: []string{"HNSW"}},
		},
	}
	task := createTestCopyTask(collectionID, segmentID)

	copyCatalog := catalogmocks.NewDataCoordCatalog(s.T())
	copyCatalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(nil, nil)
	copyCatalog.EXPECT().ListCopySegmentTasks(mock.Anything).Return(nil, nil)
	copyCatalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	copyCatalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Maybe()
	copyMeta, cmErr := NewCopySegmentMeta(context.TODO(), copyCatalog, nil, nil, nil)
	s.Require().NoError(cmErr)

	// publishedBuilds empty: the old-DataNode shape - a pointer whose index
	// section the copy never rewrote.
	err := syncVectorScalarIndexes(context.Background(), result, task, m, copyMeta, nil)
	s.Require().Error(err)
	s.Contains(err.Error(), "predates manifest index republication")
	_, installed := im.segmentBuildInfo.Get(8001)
	s.False(installed, "no memory-only record may be installed for an entry the manifest does not carry")

	// The control: the same shape with manifest writes off records to etcd
	// and merely flags the segment for backfill.
	withSegmentIndexManifestWrites(s.T(), false)
	s.NoError(syncVectorScalarIndexes(context.Background(), result, task, m, copyMeta, nil))
	_, installed = im.segmentBuildInfo.Get(8001)
	s.True(installed)
}
