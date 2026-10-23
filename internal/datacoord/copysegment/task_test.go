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
	"path"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	task2 "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
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

	task := &Task{
		Tr:    timerecord.NewTimeRecorder("test task"),
		Times: taskcommon.NewTimes(),
	}

	task.Task.Store(&datapb.CopySegmentTask{
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
	original := &Task{
		Tr:    timerecord.NewTimeRecorder("test task"),
		Times: taskcommon.NewTimes(),
	}

	original.Task.Store(&datapb.CopySegmentTask{
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
	clonedImpl := cloned.(*Task)
	s.Equal(original.Task.Load(), clonedImpl.Task.Load())
}

func (s *CopySegmentTaskSuite) TestWithCopyTaskJob() {
	task1 := &Task{
		Tr:    timerecord.NewTimeRecorder("task1"),
		Times: taskcommon.NewTimes(),
	}
	task1.Task.Store(&datapb.CopySegmentTask{
		TaskId: 1001,
		JobId:  100,
	})

	task2 := &Task{
		Tr:    timerecord.NewTimeRecorder("task2"),
		Times: taskcommon.NewTimes(),
	}
	task2.Task.Store(&datapb.CopySegmentTask{
		TaskId: 1002,
		JobId:  200,
	})

	filter := WithCopyTaskJob(100)

	s.True(filter(task1))
	s.False(filter(task2))
}

func (s *CopySegmentTaskSuite) TestWithCopyTaskStates() {
	pendingTask := &Task{
		Tr:    timerecord.NewTimeRecorder("pending"),
		Times: taskcommon.NewTimes(),
	}
	pendingTask.Task.Store(&datapb.CopySegmentTask{
		TaskId: 1001,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})

	inProgressTask := &Task{
		Tr:    timerecord.NewTimeRecorder("inprogress"),
		Times: taskcommon.NewTimes(),
	}
	inProgressTask.Task.Store(&datapb.CopySegmentTask{
		TaskId: 1002,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
	})

	completedTask := &Task{
		Tr:    timerecord.NewTimeRecorder("completed"),
		Times: taskcommon.NewTimes(),
	}
	completedTask.Task.Store(&datapb.CopySegmentTask{
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
	task := &Task{
		Tr:    timerecord.NewTimeRecorder("test task"),
		Times: taskcommon.NewTimes(),
	}
	task.Task.Store(&datapb.CopySegmentTask{
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
	task := &Task{
		Tr:    timerecord.NewTimeRecorder("test task"),
		Times: taskcommon.NewTimes(),
	}
	task.Task.Store(&datapb.CopySegmentTask{
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
	task := &Task{
		Tr:    timerecord.NewTimeRecorder("test task"),
		Times: taskcommon.NewTimes(),
	}
	task.Task.Store(&datapb.CopySegmentTask{
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
	task := &Task{
		Tr:    timerecord.NewTimeRecorder("test task"),
		Times: taskcommon.NewTimes(),
	}
	task.Task.Store(&datapb.CopySegmentTask{
		TaskId:     1001,
		CompleteTs: 0,
	})

	now := uint64(time.Now().UnixNano())

	// Apply update action
	action := UpdateCopyTaskCompleteTs(now)
	action(task)

	// Verify complete timestamp is updated
	s.Equal(now, task.Task.Load().GetCompleteTs())
}

func (s *CopySegmentTaskSuite) TestMultipleUpdateActions() {
	task := &Task{
		Tr:    timerecord.NewTimeRecorder("test task"),
		Times: taskcommon.NewTimes(),
	}
	task.Task.Store(&datapb.CopySegmentTask{
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
	s.Equal(uint64(12345), task.Task.Load().GetCompleteTs())
}

func (s *CopySegmentTaskSuite) TestWrapCopySegmentTaskLog() {
	task := &Task{
		Tr:    timerecord.NewTimeRecorder("test task"),
		Times: taskcommon.NewTimes(),
	}
	task.Task.Store(&datapb.CopySegmentTask{
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
		&Task{
			Tr:    timerecord.NewTimeRecorder("task1"),
			Times: taskcommon.NewTimes(),
		},
		&Task{
			Tr:    timerecord.NewTimeRecorder("task2"),
			Times: taskcommon.NewTimes(),
		},
		&Task{
			Tr:    timerecord.NewTimeRecorder("task3"),
			Times: taskcommon.NewTimes(),
		},
		&Task{
			Tr:    timerecord.NewTimeRecorder("task4"),
			Times: taskcommon.NewTimes(),
		},
	}

	tasks[0].(*Task).Task.Store(&datapb.CopySegmentTask{
		TaskId: 1001,
		JobId:  100,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})
	tasks[1].(*Task).Task.Store(&datapb.CopySegmentTask{
		TaskId: 1002,
		JobId:  100,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
	})
	tasks[2].(*Task).Task.Store(&datapb.CopySegmentTask{
		TaskId: 1003,
		JobId:  200,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})
	tasks[3].(*Task).Task.Store(&datapb.CopySegmentTask{
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
	task := &Task{
		Tr:    timerecord.NewTimeRecorder("test task"),
		Times: taskcommon.NewTimes(),
	}
	task.Task.Store(&datapb.CopySegmentTask{
		TaskId: 1001,
	})

	// Set and get task time
	now := time.Now()
	task.SetTaskTime(taskcommon.TimeStart, now)

	retrievedTime := task.GetTaskTime(taskcommon.TimeStart)
	s.True(retrievedTime.Equal(now))
}

func (s *CopySegmentTaskSuite) TestTaskWithEmptyIdMappings() {
	task := &Task{
		Tr:    timerecord.NewTimeRecorder("test task"),
		Times: taskcommon.NewTimes(),
	}
	task.Task.Store(&datapb.CopySegmentTask{
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
		task := &Task{
			Tr:    timerecord.NewTimeRecorder("test task"),
			Times: taskcommon.NewTimes(),
		}
		task.Task.Store(&datapb.CopySegmentTask{
			TaskId: 1001,
			State:  tc.copySegmentState,
		})

		s.Equal(tc.expectedState, task.GetTaskState(),
			"State conversion failed for %v", tc.copySegmentState)
	}
}

func (s *CopySegmentTaskSuite) TestTaskType() {
	task := &Task{
		Tr:    timerecord.NewTimeRecorder("test task"),
		Times: taskcommon.NewTimes(),
	}
	task.Task.Store(&datapb.CopySegmentTask{
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
			task := CreateTestCopyTask(100, 2001).(*Task)
			task.Task.Load().State = datapb.CopySegmentTaskState_CopySegmentTaskPending
			copyMeta, _ := NewCopySegmentTaskTestMeta(s.T(), task)
			task.CopyMeta = copyMeta

			job := NewTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*Job)
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
			placement, present := updated.GetIndexWriteToManifest()
			s.True(present)
			s.Equal(index.WriteSegmentIndexToManifest(), placement)
		})
	}
}

func (s *CopySegmentTaskSuite) TestCreateTaskOnWorkerFailsUnsupportedExternalTask() {
	task := CreateTestCopyTask(100, 2001).(*Task)
	task.Task.Load().State = datapb.CopySegmentTaskState_CopySegmentTaskPending
	copyMeta, _ := NewCopySegmentTaskTestMeta(s.T(), task)
	task.CopyMeta = copyMeta

	job := NewTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*Job)
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
			task := CreateTestCopyTask(100, 2001).(*Task)
			task.Task.Load().State = datapb.CopySegmentTaskState_CopySegmentTaskPending
			copyMeta, _ := NewCopySegmentTaskTestMeta(s.T(), task)
			task.CopyMeta = copyMeta

			job := NewTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*Job)
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

	task := &Task{
		Tr:    timerecord.NewTimeRecorder("test"),
		Times: taskcommon.NewTimes(),
	}
	task.Task.Store(&datapb.CopySegmentTask{
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

		task := CreateTestCopyTask(100, 2001).(*Task)
		task.Task.Load().NodeId = 10
		copyMeta, _ := NewCopySegmentTaskTestMeta(s.T(), task)
		// A parent job in Executing state must remain untouched by the retry.
		s.NoError(copyMeta.AddJob(context.Background(), NewTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting)))

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

	copyTask := CreateTestCopyTask(100, 2001).(*Task)
	copyTask.Task.Load().NodeId = 10
	NewCopySegmentTaskTestMeta(s.T(), copyTask)

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

	task := CreateTestCopyTask(100, 2001).(*Task)
	task.Task.Load().NodeId = 10
	copyMeta, _ := NewCopySegmentTaskTestMeta(s.T(), task)
	s.NoError(copyMeta.AddJob(context.Background(), NewTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting)))

	task.QueryTaskOnWorker(cluster)

	updatedTask := copyMeta.GetTask(context.Background(), 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskPending, updatedTask.GetState())
	s.EqualValues(meta.NullNodeID, updatedTask.GetNodeId())
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

	task := CreateTestCopyTask(100, 2001).(*Task)
	task.Task.Load().NodeId = 10
	copyMeta, _ := NewCopySegmentTaskTestMeta(s.T(), task)
	s.NoError(copyMeta.AddJob(context.Background(), NewTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting)))

	task.QueryTaskOnWorker(cluster)

	updatedTask := copyMeta.GetTask(context.Background(), 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskPending, updatedTask.GetState())
	s.EqualValues(meta.NullNodeID, updatedTask.GetNodeId())
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

	task := CreateTestCopyTask(100, 2001).(*Task)
	copyMeta, _ := NewCopySegmentTaskTestMeta(s.T(), task)

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
					Binlogs:      MakeTestCopySegmentBinlogs(),
					ManifestPath: "manifest-path",
				},
			},
		},
		nil,
	)

	task := CreateTestCopyTask(100, 2001).(*Task)
	copyMeta, m := NewCopySegmentTaskTestMeta(s.T(), task)
	err := m.AddSegment(context.Background(), NewTestCopySegment(2001))
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
	task := CreateTestCopyTask(100, 2001).(*Task)
	copyMeta, m := NewCopySegmentTaskTestMeta(s.T(), task)

	err := m.AddSegment(ctx, NewTestCopySegment(2001))
	s.NoError(err)

	insertBinlogs := MakeTestCopySegmentBinlogs()
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

	err = SyncCopySegmentTask(task, resp, copyMeta, m, nil)
	s.NoError(err)

	segment := m.GetSegment(ctx, 2001)
	s.Equal(commonpb.SegmentState_Flushed, segment.GetState())
	s.Equal("manifest-path", segment.GetManifestPath())
	s.Equal(insertBinlogs, segment.GetBinlogs())

	updatedTask := copyMeta.GetTask(ctx, 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskCompleted, updatedTask.GetState())
	s.NotZero(updatedTask.(*Task).Task.Load().GetCompleteTs())
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_ReconcilesDataView() {
	ctx := context.Background()
	task := CreateTestCopyTask(100, 2001).(*Task)
	copyMeta, m := NewCopySegmentTaskTestMeta(s.T(), task)
	s.Require().NoError(m.AddSegment(ctx, NewTestCopySegment(2001)))

	catalog, ok := m.Catalog.(dataview.Catalog)
	s.Require().True(ok)
	m.DataViewManager = dataview.NewManager(catalog, nil)

	resp := &datapb.QueryCopySegmentResponse{
		TaskID: 1001,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		SegmentResults: []*datapb.CopySegmentResult{
			{SegmentId: 2001, ImportedRows: 100, Binlogs: MakeTestCopySegmentBinlogs()},
		},
	}

	// Copy completion no longer synchronously publishes the DataView; the
	// SegmentMeta mutation commits and the async recompute converges the
	// snapshot. Drive the same projection the queue worker would run.
	err := SyncCopySegmentTask(task, resp, copyMeta, m, nil)
	s.NoError(err)

	segments, err := m.LoadableProjection(ctx, 100)
	s.NoError(err)
	_, err = m.DataViewManager.RecomputeNow(ctx, 100, func(_ context.Context, _ int64) ([]dataview.LoadableSegment, error) {
		return segments, nil
	})
	s.NoError(err)

	ref, err := m.DataViewManager.Latest(ctx, 100)
	s.NoError(err)
	s.Require().NotNil(ref)
	s.Equal([]int64{2001}, ref.DataView().GetShards()[0].GetPartitions()[0].GetSegmentIds())
	ref.Deref()
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_FailedResponseUpdatesTask() {
	ctx := context.Background()
	task := CreateTestCopyTask(100, 2001).(*Task)
	copyMeta, _ := NewCopySegmentTaskTestMeta(s.T(), task)

	err := SyncCopySegmentTask(task, &datapb.QueryCopySegmentResponse{
		TaskID: 1001,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskFailed,
		Reason: "worker failed",
	}, copyMeta, nil, nil)
	s.NoError(err)

	updatedTask := copyMeta.GetTask(ctx, 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskFailed, updatedTask.GetState())
	s.Equal("worker failed", updatedTask.GetReason())
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_DefaultStateNoop() {
	ctx := context.Background()
	task := CreateTestCopyTask(100, 2001).(*Task)
	copyMeta, _ := NewCopySegmentTaskTestMeta(s.T(), task)

	err := SyncCopySegmentTask(task, &datapb.QueryCopySegmentResponse{
		TaskID: 1001,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
	}, copyMeta, nil, nil)
	s.NoError(err)

	updatedTask := copyMeta.GetTask(ctx, 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskInProgress, updatedTask.GetState())
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_UpdateSegmentsInfoErrorMarksFailed() {
	ctx := context.Background()
	task := CreateTestCopyTask(100, 2001).(*Task)
	copyMeta, m := NewCopySegmentTaskTestMeta(s.T(), task)
	err := m.AddSegment(ctx, NewTestCopySegment(2001))
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
	}, copyMeta, m, nil)
	s.Error(err)

	updatedTask := copyMeta.GetTask(ctx, 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskFailed, updatedTask.GetState())
	s.Contains(updatedTask.GetReason(), "fieldBinlog no need to store logpath")
}

// createTestIndexMeta creates an indexMeta with pre-registered index definitions for testing.
// If catalog is nil, a default mock catalog (CreateSegmentIndex returns nil) is used.
func createTestIndexMeta(t *testing.T, collectionID int64, indexes map[int64]*model.Index, catalog ...metastore.DataCoordCatalog) *index.IndexMeta {
	var cat metastore.DataCoordCatalog
	if len(catalog) > 0 && catalog[0] != nil {
		cat = catalog[0]
	} else {
		mockCat := catalogmocks.NewDataCoordCatalog(t)
		mockCat.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil).Maybe()
		cat = mockCat
	}

	im := &index.IndexMeta{
		Ctx:              context.Background(),
		Catalog:          cat,
		KeyLock:          lock.NewKeyLock[UniqueID](),
		Indexes:          map[UniqueID]map[UniqueID]*model.Index{collectionID: indexes},
		SegmentBuildInfo: index.NewSegmentIndexBuildInfo(),
		SegmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
	}
	return im
}

func (s *CopySegmentTaskSuite) TestClone_DeepCopiesProto() {
	task := CreateTestCopyTask(100, 2001).(*Task)

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

func (s *CopySegmentTaskSuite) TestSyncVectorScalarIndexes_EmptyResult() {
	result := &datapb.CopySegmentResult{
		SegmentId:  100,
		IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{},
	}
	task := CreateTestCopyTask(1, 100)
	err := syncVectorScalarIndexes(context.Background(), result, task, &meta.Meta{}, nil, nil, nil)
	s.NoError(err)
}

func (s *CopySegmentTaskSuite) TestSyncVectorScalarIndexes_SingleIndex() {
	collectionID := int64(1)
	segmentID := int64(100)

	indexes := map[int64]*model.Index{
		300: {CollectionID: collectionID, FieldID: 101, IndexID: 300, IndexName: "vec_idx"},
	}
	im := createTestIndexMeta(s.T(), collectionID, indexes)
	copyStore := metacache.NewMetaStore(nil)
	m := &meta.Meta{IndexCatalog: im, Segments: meta.NewSegmentsInfo(copyStore), MetaStore: copyStore}
	m.Segments.SetSegment(segmentID, meta.NewSegmentInfo(&datapb.SegmentInfo{
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
	task := CreateTestCopyTask(collectionID, segmentID)

	err := syncVectorScalarIndexes(context.Background(), result, task, m, im, nil, nil)
	s.NoError(err)

	// Verify the segment index was added with target indexID and Finished state
	segIdx, ok := im.SegmentBuildInfo.Get(5001)
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
	m := &meta.Meta{IndexCatalog: im}

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
	task := CreateTestCopyTask(collectionID, segmentID)

	err := syncVectorScalarIndexes(context.Background(), result, task, m, im, nil, nil)
	s.NoError(err)

	segIdxV0, ok := im.SegmentBuildInfo.Get(2001)
	s.True(ok)
	s.Equal(indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED, segIdxV0.IndexStorePathVersion)

	segIdxV1, ok := im.SegmentBuildInfo.Get(2002)
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
	m := &meta.Meta{IndexCatalog: im}

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
	task := CreateTestCopyTask(collectionID, segmentID)

	err := syncVectorScalarIndexes(context.Background(), result, task, m, im, nil, nil)
	s.NoError(err)

	// All three indexes should be synced with correct target indexIDs
	for buildID, expectedIndexID := range map[int64]int64{5001: 300, 5002: 301, 5003: 302} {
		segIdx, ok := im.SegmentBuildInfo.Get(buildID)
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
	m := &meta.Meta{IndexCatalog: im}

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
	task := CreateTestCopyTask(collectionID, segmentID)

	err := syncVectorScalarIndexes(context.Background(), result, task, m, im, nil, nil)
	s.NoError(err)

	// Should not be added
	_, ok := im.SegmentBuildInfo.Get(5001)
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
	m := &meta.Meta{IndexCatalog: im}

	result := &datapb.CopySegmentResult{
		SegmentId:    segmentID,
		ImportedRows: 1000,
		IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{
			5001: {FieldId: 101, BuildId: 5001, IndexName: "vec_idx", IndexFilePaths: []string{"HNSW"}},
		},
	}
	task := CreateTestCopyTask(collectionID, segmentID)

	// Mock copyMeta for error path (UpdateTask and UpdateJobStateAndReleaseRef)
	copyCatalog := catalogmocks.NewDataCoordCatalog(s.T())
	copyCatalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(nil, nil)
	copyCatalog.EXPECT().ListCopySegmentTasks(mock.Anything).Return(nil, nil)
	copyCatalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	copyCatalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Maybe()
	copyMeta, cmErr := NewCopySegmentMeta(context.TODO(), copyCatalog, nil, nil, nil, nil)
	s.NoError(cmErr)

	syncErr := syncVectorScalarIndexes(context.Background(), result, task, m, im, copyMeta, nil)
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
	ms1 := metacache.NewMetaStore(catalog)
	mt := &meta.Meta{Ctx: context.Background(), Catalog: catalog, Segments: meta.NewSegmentsInfo(ms1), MetaStore: ms1}
	mt.Segments.SetSegment(segmentID, meta.NewSegmentInfo(&datapb.SegmentInfo{
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
	copyMeta, err := NewCopySegmentMeta(context.Background(), copyCatalog, nil, nil, nil, nil)
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
	task := CreateTestCopyTask(collectionID, segmentID)

	err = SyncCopySegmentTask(task, result, copyMeta, mt, nil)
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
	ms3 := metacache.NewMetaStore(catalog)
	mt := &meta.Meta{Ctx: context.Background(), Catalog: catalog, Segments: meta.NewSegmentsInfo(ms3), MetaStore: ms3}
	mt.Segments.SetSegment(segmentID, meta.NewSegmentInfo(&datapb.SegmentInfo{
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
	copyMeta, err := NewCopySegmentMeta(context.Background(), copyCatalog, nil, nil, nil, nil)
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
	task := CreateTestCopyTask(collectionID, segmentID)

	err = SyncCopySegmentTask(task, result, copyMeta, mt, nil)
	s.Require().NoError(err)
	updated := mt.GetSegment(context.Background(), segmentID)
	s.Require().NotNil(updated)
	s.Equal(commonpb.SegmentState_Flushed, updated.GetState())
	s.False(updated.GetIsImporting())
	s.Empty(updated.GetManifestPath())
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_ManifestUpdateAndClearImportingFlag() {
	meta.MockManifestIndexSubmissions(s.T())
	collectionID := int64(1)
	segmentID := int64(102)
	manifestPath := `{"ver":3,"base_path":"files/insert_log/1/10/102"}`

	// A StorageV3 pointer is read back before publication; this worker's
	// manifest carries no index entries, so it passes.
	defer mockey.Mock(meta.CreateStorageConfig).Return(nil).Build().UnPatch()
	defer mockey.Mock(packed.GetManifestIndexInfosAsync).Return(nil, nil).Build().UnPatch()

	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	// A fresh StorageV3 copy target publishes its first manifest inline via
	// UpdateManifest/UpdateSegmentsInfo, which writes through AlterSegments.
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	ms4 := metacache.NewMetaStore(catalog)
	mt := &meta.Meta{Ctx: context.Background(), Catalog: catalog, Segments: meta.NewSegmentsInfo(ms4), MetaStore: ms4}
	mt.Segments.SetSegment(segmentID, meta.NewSegmentInfo(&datapb.SegmentInfo{
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
	copyMeta, err := NewCopySegmentMeta(context.Background(), copyCatalog, nil, nil, nil, nil)
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
	task := CreateTestCopyTask(collectionID, segmentID)

	err = SyncCopySegmentTask(task, result, copyMeta, mt, nil)
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
func (s *CopySegmentTaskSuite) newCopiedManifestReadBackFixture() (CopySegmentTask, CopySegmentMeta, *meta.Meta, *index.IndexMeta, *datapb.QueryCopySegmentResponse) {
	task := CreateTestCopyTask(100, 2001).(*Task)
	copyMeta, m := NewCopySegmentTaskTestMeta(s.T(), task)
	m.ChunkManager = storage.NewLocalChunkManager(objectstorage.RootPath("files"))
	im := createTestIndexMeta(s.T(), 100, map[int64]*model.Index{
		300: {
			CollectionID: 100, FieldID: 101, IndexID: 300, IndexName: "vec_idx",
			IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}},
		},
	}, kvdatacoord.NewCatalog(meta.NewMetaMemoryKV(), "", ""))
	m.IndexCatalog = im
	task.indexMeta = im
	s.Require().NoError(m.AddSegment(context.Background(), meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             2001,
		CollectionID:   100,
		PartitionID:    10,
		State:          commonpb.SegmentState_Importing,
		IsImporting:    true,
		InsertChannel:  "ch1",
		StorageVersion: storage.StorageV3,
	})))

	resp := &datapb.QueryCopySegmentResponse{
		TaskID: 1001,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		SegmentResults: []*datapb.CopySegmentResult{{
			SegmentId:    2001,
			Binlogs:      MakeTestCopySegmentBinlogs(),
			ManifestPath: `{"ver":3,"base_path":"files/insert_log/100/10/2001"}`,
			IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{9001: {
				FieldId: 101, BuildId: 9001, IndexName: "vec_idx", IndexFilePaths: []string{"index.bin"},
			}},
		}},
	}
	return task, copyMeta, m, im, resp
}

// A worker result whose target manifest still carries an index entry foreign to
// the target collection is the fingerprint of an old DataNode that skipped the
// republication (it ignores target_indexes and sends no acknowledgement).
// The pointer must be rejected before publication and surface as a hard task
// failure, not silent metadata pollution.
func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_ForeignManifestIndexEntryFailsTask() {
	meta.MockManifestIndexSubmissions(s.T())
	ctx := context.Background()
	task, copyMeta, m, im, resp := s.newCopiedManifestReadBackFixture()

	defer mockey.Mock(meta.CreateStorageConfig).Return(nil).Build().UnPatch()
	defer mockey.Mock(packed.GetManifestIndexInfosAsync).Return([]packed.ManifestIndexInfo{
		// The target's own re-derived entry may coexist with the leftover; only
		// the foreign one is the failure.
		copiedManifestReviewEntry(),
		// Inherited from the SOURCE collection - its stored path walks back to
		// the source's artifacts.
		{IndexID: 5001, BuildID: 6001, IndexName: "vec_idx"},
	}, nil).Build().UnPatch()

	err := SyncCopySegmentTask(task, resp, copyMeta, m, im)
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
	meta.MockManifestIndexSubmissions(s.T())
	ctx := context.Background()
	task, copyMeta, m, im, resp := s.newCopiedManifestReadBackFixture()

	defer mockey.Mock(meta.CreateStorageConfig).Return(nil).Build().UnPatch()
	defer mockey.Mock(packed.GetManifestIndexInfosAsync).Return([]packed.ManifestIndexInfo{
		copiedManifestReviewEntry(),
	}, nil).Build().UnPatch()

	err := SyncCopySegmentTask(task, resp, copyMeta, m, im)
	s.Require().NoError(err)

	segment := m.GetSegment(ctx, 2001)
	s.Require().NotNil(segment)
	s.Equal(commonpb.SegmentState_Flushed, segment.GetState())
	s.Equal(resp.GetSegmentResults()[0].GetManifestPath(), segment.GetManifestPath())
	s.True(segment.GetManifestHasIndex(), "copy must publish the recovery marker with a manifest index entry")

	updatedTask := copyMeta.GetTask(ctx, 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskCompleted, updatedTask.GetState())
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_PlacementMismatchFailsBeforePublish() {
	meta.MockManifestIndexSubmissions(s.T())
	ctx := context.Background()
	task, copyMeta, m, im, resp := s.newCopiedManifestReadBackFixture()
	s.Require().NoError(copyMeta.UpdateTask(ctx, task.GetTaskId(), UpdateCopyTaskIndexWriteToManifest(true)))
	task = copyMeta.GetTask(ctx, task.GetTaskId())
	result := resp.GetSegmentResults()[0]
	result.ManifestIndexRewritten = proto.Bool(true)
	result.IndexInfos = map[int64]*datapb.VectorScalarIndexInfo{
		9001: {
			FieldId:        101,
			BuildId:        9001,
			IndexName:      "vec_idx",
			IndexFilePaths: []string{"index.bin"},
		},
	}
	manifestReads := 0
	defer mockey.Mock(packed.GetManifestIndexInfosAsync).To(
		func(context.Context, *packed.ManifestIOContext, string, *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
			manifestReads++
			return nil, merr.WrapErrIoFailedReason("must not be read")
		}).Build().UnPatch()

	err := SyncCopySegmentTask(task, resp, copyMeta, m, im)
	s.Require().Error(err)
	s.Contains(err.Error(), "enabled at dispatch")
	s.Zero(manifestReads, "current worker acknowledgement must be used without read-back")

	segment := m.GetSegment(ctx, result.GetSegmentId())
	s.Require().NotNil(segment)
	s.Equal(commonpb.SegmentState_Importing, segment.GetState())
	s.True(segment.GetIsImporting())
	s.Empty(segment.GetManifestPath())
}

// Snapshot restore installs index definitions first and then restores segment
// data through CopySegmentTask, so this is the copy/restore DataCoord-side
// placement matrix. The fake manifest store represents the exact pointer the
// DataNode returned; the real in-memory catalog proves where the corresponding
// SegmentIndex record was persisted.
func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_IndexWritePlacementMatrix() {
	const (
		collectionID = int64(100)
		partitionID  = int64(10)
		segmentID    = int64(2001)
		indexID      = int64(300)
		buildID      = int64(9001)
	)

	for _, tc := range []struct {
		name    string
		enabled bool
	}{
		{name: "manifest writes disabled", enabled: false},
		{name: "manifest writes enabled", enabled: true},
	} {
		s.Run(tc.name, func() {
			// The runtime switch has flipped by the time the result is handled.
			// The task's persisted dispatch-time value must still win.
			meta.WithSegmentIndexManifestWrites(s.T(), !tc.enabled)
			store := meta.NewFakeManifestStore(s.T())
			ctx := context.Background()
			catalog := kvdatacoord.NewCatalog(meta.NewMetaMemoryKV(), "", "")
			m, dm := index.BootMetaForRestart(s.T(), catalog, collectionID)
			s.Require().NoError(dm.IndexMeta.CreateIndex(ctx, &model.Index{
				CollectionID: collectionID,
				FieldID:      101,
				IndexID:      indexID,
				IndexName:    "vec_idx",
				IndexParams:  []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}},
			}))
			s.Require().NoError(m.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
				ID:             segmentID,
				CollectionID:   collectionID,
				PartitionID:    partitionID,
				State:          commonpb.SegmentState_Importing,
				IsImporting:    true,
				NumOfRows:      100,
				InsertChannel:  "ch1",
				StorageVersion: storage.StorageV3,
			})))

			task := CreateTestCopyTask(collectionID, segmentID).(*Task)
			task.Task.Load().IndexWriteToManifest = proto.Bool(tc.enabled)
			copyMeta, err := NewCopySegmentMeta(ctx, catalog, m, dm.IndexMeta, nil, nil)
			s.Require().NoError(err)
			s.Require().NoError(copyMeta.AddTask(ctx, task))
			// Reload the task from etcd to cover a DataCoord restart between
			// dispatch and worker completion.
			copyMeta, err = NewCopySegmentMeta(ctx, catalog, m, dm.IndexMeta, nil, nil)
			s.Require().NoError(err)
			task = copyMeta.GetTask(ctx, task.GetTaskId()).(*Task)
			persistedPlacement, present := task.GetIndexWriteToManifest()
			s.True(present)
			s.Equal(tc.enabled, persistedPlacement)

			manifestPath := packed.MarshalManifestPath("/tmp/test-restart/files/insert_log/100/10/2001", 3)
			var manifestBuildIDs []int64
			if tc.enabled {
				store.Revisions[manifestPath] = []packed.ManifestIndexInfo{{
					ColumnName:            "vector",
					IndexName:             "vec_idx",
					IndexType:             "HNSW",
					Properties:            map[string]string{common.IndexTypeKey: "HNSW"},
					Path:                  "/tmp/test-restart/index_files/9001/0/10/2001",
					FieldID:               101,
					IndexID:               indexID,
					BuildID:               buildID,
					NumRows:               100,
					SerializedSize:        1024,
					MemSize:               1024,
					IndexFileKeys:         []string{"index.bin"},
					IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
				}}
				manifestBuildIDs = []int64{buildID}
			}
			resp := &datapb.QueryCopySegmentResponse{
				TaskID: task.GetTaskId(),
				State:  datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
				SegmentResults: []*datapb.CopySegmentResult{{
					SegmentId:              segmentID,
					ImportedRows:           100,
					Binlogs:                MakeTestCopySegmentBinlogs(),
					ManifestPath:           manifestPath,
					ManifestIndexRewritten: proto.Bool(true),
					ManifestIndexBuildIds:  manifestBuildIDs,
					IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{
						buildID: {
							FieldId:        101,
							BuildId:        buildID,
							IndexName:      "vec_idx",
							IndexFilePaths: []string{"index.bin"},
						},
					},
				}},
			}

			s.Require().NoError(SyncCopySegmentTask(task, resp, copyMeta, m, dm.IndexMeta))
			if tc.enabled {
				s.Equal(1, store.ReadCount, "artifact-bearing results must verify the dispatched index identity")
			} else {
				s.Zero(store.ReadCount, "acknowledged empty manifests need no read-back")
			}

			io := packed.NewManifestIOContext(1)
			manifestEntries, err := packed.GetManifestIndexInfosAsync(ctx, io, manifestPath, nil)
			io.Close()
			s.Require().NoError(err)
			persisted, err := catalog.ListSegmentIndexes(ctx, collectionID)
			s.Require().NoError(err)
			if tc.enabled {
				s.Require().Len(manifestEntries, 1)
				s.EqualValues(buildID, manifestEntries[0].BuildID)
				s.Empty(persisted, "manifest-resident copied index must not remain in etcd")
			} else {
				s.Empty(manifestEntries, "disabled switch must leave the copied target manifest index-free")
				s.Require().Len(persisted, 1)
				s.EqualValues(buildID, persisted[0].BuildID)
				s.Equal(commonpb.IndexState_Finished, persisted[0].IndexState)
			}

			_, restartedDM := index.BootMetaForRestart(s.T(), catalog, collectionID)
			recovered := restartedDM.IndexMeta.GetSegmentIndexes(collectionID, segmentID)
			s.Require().Len(recovered, 1)
			s.EqualValues(buildID, recovered[indexID].BuildID)
			s.Equal(commonpb.IndexState_Finished, recovered[indexID].IndexState)
		})
	}
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_LegacyStorageRetainsEtcdIndexes() {
	meta.MockManifestIndexSubmissions(s.T())
	for _, version := range []int64{storage.StorageV1, storage.StorageV2} {
		for _, enabled := range []bool{false, true} {
			s.Run(strconv.FormatInt(version, 10)+"/manifest="+strconv.FormatBool(enabled), func() {
				ctx := context.Background()
				// Result handling must follow persisted placement even after a switch flip.
				meta.WithSegmentIndexManifestWrites(s.T(), !enabled)
				catalog := kvdatacoord.NewCatalog(meta.NewMetaMemoryKV(), "", "")
				m, dm := index.BootMetaForRestart(s.T(), catalog, 100)
				s.Require().NoError(dm.IndexMeta.CreateIndex(ctx, &model.Index{
					CollectionID: 100, FieldID: 101, IndexID: 300, IndexName: "vec_idx",
					IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}},
				}))
				segment := NewTestCopySegment(2001)
				segment.StorageVersion = version
				segment.IsImporting = true
				s.Require().NoError(m.AddSegment(ctx, segment))
				task := CreateTestCopyTask(100, 2001).(*Task)
				task.Task.Load().IndexWriteToManifest = proto.Bool(enabled)
				copies, err := NewCopySegmentMeta(ctx, catalog, m, dm.IndexMeta, nil, nil)
				s.Require().NoError(err)
				s.Require().NoError(copies.AddTask(ctx, task))
				copies, err = NewCopySegmentMeta(ctx, catalog, m, dm.IndexMeta, nil, nil)
				s.Require().NoError(err)
				task = copies.GetTask(ctx, task.GetTaskId()).(*Task)
				reader := mockey.Mock(packed.GetManifestIndexInfosAsync).Return(nil, merr.ErrServiceUnavailable).Build()
				defer reader.UnPatch()
				s.Require().NoError(SyncCopySegmentTask(task, &datapb.QueryCopySegmentResponse{
					State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
					SegmentResults: []*datapb.CopySegmentResult{{
						SegmentId: 2001, ImportedRows: 100, Binlogs: MakeTestCopySegmentBinlogs(),
						IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{9001: {
							BuildId: 9001, FieldId: 101, IndexName: "vec_idx", IndexFilePaths: []string{"index.bin"},
						}},
					}},
				}, copies, m, dm.IndexMeta))
				s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskCompleted, task.GetState())
				s.Equal(commonpb.SegmentState_Flushed, m.GetSegment(ctx, 2001).GetState())
				s.False(m.GetSegment(ctx, 2001).GetIsImporting())
				persisted, err := catalog.ListSegmentIndexes(ctx, 100)
				s.Require().NoError(err)
				s.Require().Len(persisted, 1)
				s.EqualValues(9001, persisted[0].BuildID)
				s.Equal([]string{"index.bin"}, persisted[0].IndexFileKeys)
				_, restartedDM := index.BootMetaForRestart(s.T(), catalog, 100)
				indexes := restartedDM.IndexMeta.GetSegmentIndexes(100, 2001)
				s.Require().Len(indexes, 1)
				s.EqualValues(9001, indexes[300].BuildID)
				s.Equal(commonpb.IndexState_Finished, indexes[300].IndexState)
			})
		}
	}
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
	ms5 := metacache.NewMetaStore(catalog)
	mt := &meta.Meta{Ctx: context.Background(), Catalog: catalog, Segments: meta.NewSegmentsInfo(ms5), MetaStore: ms5}
	mt.Segments.SetSegment(segmentID, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:            segmentID,
		CollectionID:  collectionID,
		PartitionID:   10,
		InsertChannel: "ch1",
		State:         commonpb.SegmentState_Importing,
		IsImporting:   true,
	}))
	copyMeta, err := NewCopySegmentMeta(context.Background(), catalog, nil, nil, nil, nil)
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
	task := CreateTestCopyTask(collectionID, segmentID)

	err = SyncCopySegmentTask(task, result, copyMeta, mt, nil)
	s.Require().Error(err)
	updated := mt.GetSegment(context.Background(), segmentID)
	s.Require().NotNil(updated)
	s.Equal(commonpb.SegmentState_Importing, updated.GetState())
	s.True(updated.GetIsImporting())
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

func newManifestFallbackAssembleFixture(ctx context.Context, segmentIDs ...int64) (*Task, *Job) {
	snapshotData := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100, Name: "test_snapshot"},
	}
	mappings := make([]*datapb.CopySegmentIDMapping, 0, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		snapshotData.Indexes = append(snapshotData.Indexes, &indexpb.IndexInfo{
			IndexID: 1000 + segmentID,
			FieldID: 100,
		})
		snapshotData.Segments = append(snapshotData.Segments, &datapb.SegmentDescription{
			SegmentId:        segmentID,
			PartitionId:      10,
			StorageVersion:   storage.StorageV3,
			ManifestPath:     packed.MarshalManifestPath("files/insert_log/100/10/"+strconv.FormatInt(segmentID, 10), 3),
			ManifestHasIndex: proto.Bool(true),
		})
		mappings = append(mappings, &datapb.CopySegmentIDMapping{
			SourceSegmentId: segmentID,
			TargetSegmentId: 2000 + segmentID,
			PartitionId:     10,
		})
	}

	task := &Task{
		Ctx:   ctx,
		Meta:  &meta.Meta{},
		Alloc: &EmbeddedAllocator{},
		Tr:    timerecord.NewTimeRecorder("test"),
		Times: taskcommon.NewTimes(),
	}
	task.Task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 100,
		IdMappings:   mappings,
	})
	job := &Job{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: 100,
			SnapshotName: "test_snapshot",
		},
		Tr:            timerecord.NewTimeRecorder("test_job"),
		SnapshotCache: &CopySegmentSnapshotCache{data: snapshotData},
	}
	return task, job
}

func TestAssembleCopySegmentRequest_ManifestFallbackBoundsConcurrency(t *testing.T) {
	meta.MockManifestIndexSubmissions(t)
	originalConcurrency := Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue("2")
	defer Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue(originalConcurrency)
	defer mockey.Mock(meta.CreateStorageConfig).Return(nil).Build().UnPatch()

	var active int32
	var maximum int32
	started := make(chan struct{}, 6)
	release := make(chan struct{})
	defer mockey.Mock(packed.GetManifestIndexInfosAsync).To(
		func(context.Context, *packed.ManifestIOContext, string, *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
			current := atomic.AddInt32(&active, 1)
			defer atomic.AddInt32(&active, -1)
			for {
				observed := atomic.LoadInt32(&maximum)
				if current <= observed || atomic.CompareAndSwapInt32(&maximum, observed, current) {
					break
				}
			}
			started <- struct{}{}
			<-release
			return nil, nil
		}).Build().UnPatch()

	task, job := newManifestFallbackAssembleFixture(context.Background(), 1, 2, 3)
	otherTask, otherJob := newManifestFallbackAssembleFixture(context.Background(), 4, 5, 6)
	otherTask.Meta = task.Meta
	result := make(chan error, 2)
	go func() { _, err := AssembleCopySegmentRequest(otherTask, otherJob); result <- err }()
	go func() {
		_, err := AssembleCopySegmentRequest(task, job)
		result <- err
	}()

	for range 2 {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for bounded manifest reads")
		}
	}
	select {
	case <-started:
		t.Fatal("a third manifest read exceeded the configured concurrency")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-result)
	require.NoError(t, <-result)
	assert.Equal(t, int32(2), atomic.LoadInt32(&maximum))
	assert.Zero(t, atomic.LoadInt32(&active))
}

func TestAssembleCopySegmentRequest_ManifestFallbackPreservesMappingOrder(t *testing.T) {
	meta.MockManifestIndexSubmissions(t)
	originalConcurrency := Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue("3")
	defer Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue(originalConcurrency)
	defer mockey.Mock(meta.CreateStorageConfig).Return(nil).Build().UnPatch()

	task, job := newManifestFallbackAssembleFixture(context.Background(), 1, 2, 3)
	manifestToSegment := make(map[string]int64)
	for _, segment := range job.SnapshotCache.data.Segments {
		manifestToSegment[segment.GetManifestPath()] = segment.GetSegmentId()
	}
	started := make(chan int64, 3)
	completed := make(chan int64, 3)
	releases := map[int64]chan struct{}{1: make(chan struct{}), 2: make(chan struct{}), 3: make(chan struct{})}
	defer mockey.Mock(packed.GetManifestIndexInfosAsync).To(
		func(_ context.Context, _ *packed.ManifestIOContext, manifestPath string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
			segmentID := manifestToSegment[manifestPath]
			started <- segmentID
			<-releases[segmentID]
			completed <- segmentID
			return []packed.ManifestIndexInfo{{
				IndexID:               1000 + segmentID,
				BuildID:               3000 + segmentID,
				FieldID:               100,
				IndexName:             "vec_idx",
				IndexType:             "HNSW",
				Path:                  "index_files/" + strconv.FormatInt(3000+segmentID, 10) + "/0/10/" + strconv.FormatInt(segmentID, 10),
				IndexFileKeys:         []string{"index.bin"},
				IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
			}}, nil
		}).Build().UnPatch()

	nextID := int64(9001)
	defer mockey.Mock((*EmbeddedAllocator).AllocID).To(func(context.Context) (typeutil.UniqueID, error) {
		id := nextID
		nextID++
		return id, nil
	}).Build().UnPatch()

	type assembleResult struct {
		request *datapb.CopySegmentRequest
		err     error
	}
	result := make(chan assembleResult, 1)
	go func() {
		request, err := AssembleCopySegmentRequest(task, job)
		result <- assembleResult{request: request, err: err}
	}()
	seen := make(map[int64]struct{})
	for range 3 {
		select {
		case segmentID := <-started:
			seen[segmentID] = struct{}{}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for parallel manifest reads")
		}
	}
	require.Len(t, seen, 3)
	for _, segmentID := range []int64{3, 2, 1} {
		close(releases[segmentID])
		assert.Equal(t, segmentID, <-completed)
	}
	assembled := <-result
	require.NoError(t, assembled.err)
	require.Len(t, assembled.request.GetSources(), 3)
	require.Len(t, assembled.request.GetTargets(), 3)
	for i, segmentID := range []int64{1, 2, 3} {
		require.Equal(t, segmentID, assembled.request.GetSources()[i].GetSegmentId())
		require.Len(t, assembled.request.GetSources()[i].GetIndexFiles(), 1)
		assert.Equal(t, 3000+segmentID, assembled.request.GetSources()[i].GetIndexFiles()[0].GetBuildID())
		assert.Equal(t, int64(9001+i), assembled.request.GetTargets()[i].GetNewBuildIds()[3000+segmentID])
	}
}

func TestAssembleCopySegmentRequest_ManifestFallbackDrainsAfterError(t *testing.T) {
	meta.MockManifestIndexSubmissions(t)
	originalConcurrency := Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue("2")
	defer Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue(originalConcurrency)
	defer mockey.Mock(meta.CreateStorageConfig).Return(nil).Build().UnPatch()

	task, job := newManifestFallbackAssembleFixture(context.Background(), 1, 2)
	manifestToSegment := make(map[string]int64)
	for _, segment := range job.SnapshotCache.data.Segments {
		manifestToSegment[segment.GetManifestPath()] = segment.GetSegmentId()
	}
	sentinel := errors.New("manifest read failed")
	blocked := make(chan struct{})
	release := make(chan struct{})
	var active int32
	defer mockey.Mock(packed.GetManifestIndexInfosAsync).To(
		func(_ context.Context, _ *packed.ManifestIOContext, manifestPath string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
			atomic.AddInt32(&active, 1)
			defer atomic.AddInt32(&active, -1)
			if manifestToSegment[manifestPath] == 1 {
				<-blocked // Ensure the peer was accepted before failure cancels admission.
				return nil, sentinel
			}
			close(blocked)
			<-release
			return nil, nil
		}).Build().UnPatch()

	result := make(chan error, 1)
	go func() {
		_, err := AssembleCopySegmentRequest(task, job)
		result <- err
	}()
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the second manifest read")
	}
	select {
	case err := <-result:
		t.Fatalf("assembly returned before the started manifest read drained: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	err := <-result
	require.ErrorIs(t, err, sentinel)
	assert.Zero(t, atomic.LoadInt32(&active))
}

func TestAssembleCopySegmentRequest_ManifestFallbackPreCanceledSkipsRead(t *testing.T) {
	meta.MockManifestIndexSubmissions(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	task, job := newManifestFallbackAssembleFixture(ctx, 1)
	defer mockey.Mock(meta.CreateStorageConfig).Return(nil).Build().UnPatch()
	var reads int32
	defer mockey.Mock(packed.GetManifestIndexInfosAsync).To(
		func(context.Context, *packed.ManifestIOContext, string, *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
			atomic.AddInt32(&reads, 1)
			return nil, nil
		}).Build().UnPatch()

	request, err := AssembleCopySegmentRequest(task, job)
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, request)
	assert.Zero(t, atomic.LoadInt32(&reads))
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
	ms6 := metacache.NewMetaStore(nil)
	m := &meta.Meta{IndexCatalog: im, Segments: meta.NewSegmentsInfo(ms6), MetaStore: ms6}

	s.Nil(buildCopySegmentTargetIndexes(m, im, collectionID, false),
		"with manifest writes off the worker must get no definitions, or it would mint manifest entries the etcd records do not own")

	targets := buildCopySegmentTargetIndexes(m, im, collectionID, true)
	s.Require().Len(targets, 1)
	s.Equal("vec_idx", targets[0].GetIndexName())
	s.EqualValues(300, targets[0].GetIndexId())
}

// With manifest publication on, a StorageV3 copy may install an artifact-bearing
// finished record without etcd only when worker evidence proves the entry is
// durable there. Otherwise the task must hard-fail with the upgrade hint.
func (s *CopySegmentTaskSuite) TestValidateCopiedManifestIndexPlacement_FailsWhenEntryMissing() {
	collectionID := int64(1)
	segmentID := int64(105)

	im := createTestIndexMeta(s.T(), collectionID, map[int64]*model.Index{
		300: {CollectionID: collectionID, FieldID: 101, IndexID: 300, IndexName: "vec_idx"},
	})
	ms7 := metacache.NewMetaStore(nil)
	m := &meta.Meta{IndexCatalog: im, Segments: meta.NewSegmentsInfo(ms7), MetaStore: ms7}
	m.Segments.SetSegment(segmentID, meta.NewSegmentInfo(&datapb.SegmentInfo{
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
	task := CreateTestCopyTask(collectionID, segmentID)
	task.(*Task).Task.Load().IndexWriteToManifest = proto.Bool(true)

	copyCatalog := catalogmocks.NewDataCoordCatalog(s.T())
	copyCatalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(nil, nil)
	copyCatalog.EXPECT().ListCopySegmentTasks(mock.Anything).Return(nil, nil)
	copyCatalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	copyCatalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Maybe()
	copyMeta, cmErr := NewCopySegmentMeta(context.TODO(), copyCatalog, nil, nil, nil, nil)
	s.Require().NoError(cmErr)

	// publishedBuilds empty: the old-DataNode shape - a pointer whose index
	// section the copy never rewrote.
	err := validateCopiedManifestIndexPlacement(result, task, m, im, nil)
	s.Require().Error(err)
	s.Contains(err.Error(), "predates manifest index republication")
	_, installed := im.SegmentBuildInfo.Get(8001)
	s.False(installed, "no memory-only record may be installed for an entry the manifest does not carry")

	// The control: the same shape dispatched with manifest writes off records to etcd.
	task.(*Task).Task.Load().IndexWriteToManifest = proto.Bool(false)
	s.NoError(validateCopiedManifestIndexPlacement(result, task, m, im, nil))
	s.NoError(syncVectorScalarIndexes(context.Background(), result, task, m, im, copyMeta, nil))
	_, installed = im.SegmentBuildInfo.Get(8001)
	s.True(installed)
}

func copiedManifestReviewEntry() packed.ManifestIndexInfo {
	return packed.ManifestIndexInfo{
		IndexID: 300, BuildID: 9001, FieldID: 101, IndexName: "vec_idx", IndexType: "HNSW",
		Properties: map[string]string{common.IndexTypeKey: "HNSW"},
		Path:       "files/index_files/9001/0/10/2001", IndexFileKeys: []string{"index.bin"},
	}
}

func (s *CopySegmentTaskSuite) TestCopiedManifestRejectsChangedIdentityAndPaths() {
	meta.MockManifestIndexSubmissions(s.T())
	for _, acknowledged := range []bool{false, true} {
		for _, scenario := range []string{"recreated", "parameters", "foreign path", "wrong files"} {
			s.Run(strconv.FormatBool(acknowledged)+"/"+scenario, func() {
				task, copyMeta, m, im, resp := s.newCopiedManifestReadBackFixture()
				result := resp.GetSegmentResults()[0]
				if acknowledged {
					result.ManifestIndexRewritten = proto.Bool(true)
					result.ManifestIndexBuildIds = []int64{9001}
				}
				entry := copiedManifestReviewEntry()
				switch scenario {
				case "recreated":
					s.Require().NoError(im.MarkIndexAsDeleted(context.Background(), 100, []int64{300}))
					s.Require().NoError(im.CreateIndex(context.Background(), &model.Index{
						CollectionID: 100, FieldID: 101, IndexID: 301, IndexName: "vec_idx",
						IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}},
					}))
				case "parameters":
					entry.Properties["M"] = "32"
				case "foreign path":
					// IDs collide, but the directory still names the source segment.
					entry.Path = "files/index_files/9001/0/10/9999"
				case "wrong files":
					entry.IndexFileKeys = []string{"other.bin"}
				}
				defer mockey.Mock(packed.GetManifestIndexInfosAsync).Return([]packed.ManifestIndexInfo{entry}, nil).Build().UnPatch()
				s.Error(SyncCopySegmentTask(task, resp, copyMeta, m, im))
				segment := m.GetSegment(context.Background(), 2001)
				s.Equal(commonpb.SegmentState_Importing, segment.GetState())
				s.False(segment.GetManifestHasIndex())
				s.Empty(im.GetSegmentIndexes(100, 2001))
				s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskFailed, copyMeta.GetTask(context.Background(), task.GetTaskId()).GetState())
			})
		}
	}
}

func (s *CopySegmentTaskSuite) TestCopiedManifestKeepsVerifiedIndexIDDuringInstall() {
	ctx := context.Background()
	task, _, m, im, resp := s.newCopiedManifestReadBackFixture()
	result := resp.GetSegmentResults()[0]
	verified, err := verifyCopiedManifestIndexOwnership(result, task, m, im, m.GetSegment(ctx, result.GetSegmentId()), meta.CreateStorageConfig().GetRootPath(), []packed.ManifestIndexInfo{copiedManifestReviewEntry()})
	s.Require().NoError(err)
	// Recreate after verification but before the in-memory record is installed.
	s.Require().NoError(im.MarkIndexAsDeleted(ctx, 100, []int64{300}))
	s.Require().NoError(im.CreateIndex(ctx, &model.Index{
		CollectionID: 100, FieldID: 101, IndexID: 301, IndexName: "vec_idx",
	}))
	s.Require().NoError(syncVectorScalarIndexes(ctx, result, task, m, im, nil, verified))
	record, ok := im.GetIndexJob(9001)
	s.Require().True(ok)
	s.EqualValues(300, record.IndexID, "the artifact belongs to the retired definition, never its replacement")
	s.NotContains(im.GetSegmentIndexes(100, 2001), int64(301))
}

func (s *CopySegmentTaskSuite) TestCopyManifestVerificationParallelAndFailClosed() {
	meta.MockManifestIndexSubmissions(s.T())
	old := Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue("2")
	defer Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue(old)
	task, copies, m, im, resp := s.newCopiedManifestReadBackFixture()
	original := resp.GetSegmentResults()[0]
	for _, id := range []int64{2002, 2003} {
		segment := m.GetSegment(context.Background(), 2001).Clone()
		segment.ID = id
		s.Require().NoError(m.AddSegment(context.Background(), segment))
		result := proto.Clone(original).(*datapb.CopySegmentResult)
		result.SegmentId = id
		result.ManifestPath = packed.MarshalManifestPath("files/insert_log/100/10/"+strconv.FormatInt(id, 10), 3)
		resp.SegmentResults = append(resp.SegmentResults, result)
	}
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	defer mockey.Mock(packed.GetManifestIndexInfosAsync).To(func(context.Context, *packed.ManifestIOContext, string, *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
		started <- struct{}{}
		<-release
		return nil, merr.ErrServiceUnavailable
	}).Build().UnPatch()
	finished := make(chan error, 1)
	go func() { finished <- SyncCopySegmentTask(task, resp, copies, m, im) }()
	for range 2 {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			s.FailNow("verification was not parallel")
		}
	}
	select {
	case <-started:
		s.FailNow("verification exceeded shared budget")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	s.ErrorIs(<-finished, merr.ErrServiceUnavailable)
	for _, id := range []int64{2001, 2002, 2003} {
		s.Equal(commonpb.SegmentState_Importing, m.GetSegment(context.Background(), id).GetState())
	}
	s.True(copies.GetTask(context.Background(), task.GetTaskId()).GetCleanupRequired())
}

func (s *CopySegmentTaskSuite) TestQueryCompletedCopyRetriesCleanupIntentFailure() {
	task := CreateTestCopyTask(100, 2001).(*Task)
	copies, m := NewCopySegmentTaskTestMeta(s.T(), task)
	s.Require().NoError(m.AddSegment(context.Background(), NewTestCopySegment(2001)))
	s.Require().NoError(copies.UpdateTask(context.Background(), task.GetTaskId(), UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskInProgress)))
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().QueryCopySegment(mock.Anything, mock.Anything).Return(&datapb.QueryCopySegmentResponse{
		State:          datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		SegmentResults: []*datapb.CopySegmentResult{{SegmentId: 2001, ImportedRows: 100}},
	}, nil)
	failure := mockey.Mock((*kvdatacoord.Catalog).SaveCopySegmentTask).Return(merr.ErrServiceUnavailable).Build()
	defer failure.UnPatch()
	task.QueryTaskOnWorker(cluster)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskInProgress, task.GetState())
	s.Equal(commonpb.SegmentState_Importing, m.GetSegment(context.Background(), 2001).GetState())
	s.False(task.GetCleanupRequired())
	failure.UnPatch()
	task.QueryTaskOnWorker(cluster)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskCompleted, task.GetState())
	s.False(task.GetCleanupRequired())
}

func TestAssembleCopySegmentRequest_ManifestFallbackSingleWorker(t *testing.T) {
	original := Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue("1")
	defer Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue(original)
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
	defer mockey.Mock(meta.CreateStorageConfig).Return(cfg).Build().UnPatch()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	task, job := newManifestFallbackAssembleFixture(ctx, 1, 2, 3)
	for _, segment := range job.SnapshotCache.data.Segments {
		base := path.Join(cfg.RootPath, "insert_log/100/10", strconv.FormatInt(segment.GetSegmentId(), 10))
		manifest, err := packed.CommitManifestUpdates(base, 0, cfg, &packed.ManifestUpdates{
			DeltaLogs: []packed.DeltaLogEntry{{Path: base + "/_delta/1", NumEntries: 1}},
		})
		require.NoError(t, err)
		segment.ManifestPath = manifest
	}
	request, err := AssembleCopySegmentRequest(task, job)
	require.NoError(t, err)
	require.Len(t, request.GetSources(), 3)
	for i, source := range request.GetSources() {
		require.EqualValues(t, i+1, source.GetSegmentId())
		require.Equal(t, job.SnapshotCache.data.Segments[i].GetManifestPath(), source.GetManifestPath())
		require.Empty(t, source.GetIndexFiles())
	}
}
