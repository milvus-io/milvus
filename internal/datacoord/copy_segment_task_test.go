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
	"fmt"
	"reflect"
	"strconv"
	"sync"
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
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	task2 "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	snapshotadapter "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	snapshotstorage "github.com/milvus-io/milvus/pkg/v3/snapshotio"
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
			placement, present := updated.GetIndexWriteToManifest()
			s.True(present)
			s.Equal(writeSegmentIndexToManifest(), placement)
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
	drainBuildIndexChForTest()
	defer drainBuildIndexChForTest()

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

	// The restored segment is enqueued so indexes missing from the snapshot
	// get built.
	assertBuildIndexEvents(s.T(), 2001)
	assertNoBuildIndexEvent(s.T())
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_ReconcilesDataView() {
	ctx := context.Background()
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, m := newCopySegmentTaskTestMeta(s.T(), task)
	s.Require().NoError(m.AddSegment(ctx, newTestCopySegment(2001)))

	catalog, ok := m.catalog.(dataview.Catalog)
	s.Require().True(ok)
	m.dataViewManager = dataview.NewManager(catalog, nil)

	resp := &datapb.QueryCopySegmentResponse{
		TaskID: 1001,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		SegmentResults: []*datapb.CopySegmentResult{
			{SegmentId: 2001, ImportedRows: 100, Binlogs: makeTestCopySegmentBinlogs()},
		},
	}

	// Copy completion no longer synchronously publishes the DataView; the
	// SegmentMeta mutation commits and the async recompute converges the
	// snapshot. Drive the same projection the queue worker would run.
	err := SyncCopySegmentTask(task, resp, copyMeta, m)
	s.NoError(err)

	segments, err := m.loadableProjection(ctx, 100)
	s.NoError(err)
	_, err = m.dataViewManager.RecomputeNow(ctx, 100, func(_ context.Context, _ int64) ([]dataview.LoadableSegment, error) {
		return segments, nil
	})
	s.NoError(err)

	ref, err := m.dataViewManager.Latest(ctx, 100)
	s.NoError(err)
	s.Require().NotNil(ref)
	s.Equal([]int64{2001}, ref.DataView().GetShards()[0].GetPartitions()[0].GetSegmentIds())
	ref.Deref()
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
// If catalog is nil, use the real catalog backed by an isolated memory KV.
func createTestIndexMeta(t *testing.T, collectionID int64, indexes map[int64]*model.Index, catalog ...metastore.DataCoordCatalog) *indexMeta {
	var cat metastore.DataCoordCatalog
	if len(catalog) > 0 && catalog[0] != nil {
		cat = catalog[0]
	} else {
		cat = kvdatacoord.NewCatalog(NewMetaMemoryKV(), "", "")
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
	store := metacache.NewMetaStore(catalog)
	m := &meta{
		catalog:   catalog,
		segments:  NewSegmentsInfo(store),
		metaStore: store,
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
	err := syncVectorScalarIndexes(context.Background(), result, task, &meta{}, nil, copySegmentIndexLookup{})
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
	m := &meta{indexMeta: im, segments: NewSegmentsInfo(copyStore), metaStore: copyStore}
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

	err := syncVectorScalarIndexes(context.Background(), result, task, m, nil, buildCopySegmentIndexLookup(task, m))
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

	err := syncVectorScalarIndexes(context.Background(), result, task, m, nil, buildCopySegmentIndexLookup(task, m))
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

	err := syncVectorScalarIndexes(context.Background(), result, task, m, nil, buildCopySegmentIndexLookup(task, m))
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

	err := syncVectorScalarIndexes(context.Background(), result, task, m, nil, buildCopySegmentIndexLookup(task, m))
	s.NoError(err)

	// Should not be added
	_, ok := im.segmentBuildInfo.Get(5001)
	s.False(ok)
}

func (s *CopySegmentTaskSuite) TestSyncVectorScalarIndexes_AddSegmentIndexError() {
	collectionID := int64(1)
	segmentID := int64(100)

	errCatalog := kvdatacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	defer mockey.Mock((*kvdatacoord.Catalog).CreateSegmentIndex).Return(merr.ErrServiceUnavailable).Build().UnPatch()

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

	syncErr := syncVectorScalarIndexes(context.Background(), result, task, m, nil, buildCopySegmentIndexLookup(task, m))
	s.Error(syncErr)
	s.ErrorIs(syncErr, merr.ErrServiceUnavailable)
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_ClearsImportingFlagOnCompletion() {
	for _, tc := range []struct {
		name     string
		version  int64
		manifest string
	}{
		{"legacy", storage.StorageV1, ""},
		{"empty manifest", storage.StorageV3, ""},
		{"acknowledged manifest", storage.StorageV3, `{"ver":3,"base_path":"files/insert_log/1/10/100"}`},
	} {
		s.Run(tc.name, func() {
			// An explicit empty index acknowledgement publishes without storage I/O.
			reader := mockey.Mock(packed.GetManifestIndexInfosAsync).Return(nil, merr.ErrServiceUnavailable).Build()
			defer reader.UnPatch()
			catalog := kvdatacoord.NewCatalog(NewMetaMemoryKV(), "", "")
			ms := metacache.NewMetaStore(catalog)
			mt := &meta{ctx: context.Background(), catalog: catalog, segments: NewSegmentsInfo(ms), metaStore: ms}
			mt.segments.SetSegment(100, NewSegmentInfo(&datapb.SegmentInfo{
				ID: 100, CollectionID: 1, PartitionID: 10, InsertChannel: "ch1",
				State: commonpb.SegmentState_Importing, IsImporting: true, StorageVersion: tc.version,
			}))
			copyMeta, err := NewCopySegmentMeta(context.Background(), catalog, nil, nil, nil)
			s.Require().NoError(err)
			result := &datapb.QueryCopySegmentResponse{
				State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
				SegmentResults: []*datapb.CopySegmentResult{{
					SegmentId: 100, ManifestPath: tc.manifest,
					Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 1}}}},
				}},
			}
			if tc.manifest != "" {
				result.SegmentResults[0].ManifestIndexRewritten = proto.Bool(true)
			}
			s.Require().NoError(SyncCopySegmentTask(createTestCopyTask(1, 100), result, copyMeta, mt))
			updated := mt.GetSegment(context.Background(), 100)
			s.Require().NotNil(updated)
			s.Equal(commonpb.SegmentState_Flushed, updated.GetState())
			s.False(updated.GetIsImporting())
			s.Equal(tc.manifest, updated.GetManifestPath())
			s.Zero(reader.Times())
		})
	}
}

// newCopiedManifestResultFixture wires a StorageV3 copy target plus a target
// index definition (indexID 300, "vec_idx") into the sync-path test meta, so a
// test can vary the worker acknowledgement and mocked manifest contents.
func (s *CopySegmentTaskSuite) newCopiedManifestResultFixture() (CopySegmentTask, CopySegmentMeta, *meta, *datapb.QueryCopySegmentResponse) {
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copyMeta, m := newCopySegmentTaskTestMeta(s.T(), task)
	m.chunkManager = storage.NewLocalChunkManager(objectstorage.RootPath("files"))
	m.indexMeta = createTestIndexMeta(s.T(), 100, map[int64]*model.Index{
		300: {
			CollectionID: 100, FieldID: 101, IndexID: 300, IndexName: "vec_idx",
			IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}},
		},
	}, kvdatacoord.NewCatalog(NewMetaMemoryKV(), "", ""))
	s.Require().NoError(m.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
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
			SegmentId:              2001,
			Binlogs:                makeTestCopySegmentBinlogs(),
			ManifestPath:           `{"ver":3,"base_path":"files/insert_log/100/10/2001"}`,
			ManifestIndexRewritten: proto.Bool(true),
			ManifestIndexBuildIds:  []int64{9001},
			IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{9001: {
				FieldId: 101, BuildId: 9001, IndexName: "vec_idx", IndexFilePaths: []string{"index.bin"},
			}},
		}},
	}
	return task, copyMeta, m, resp
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_CleanManifestReadBackPublishes() {
	mockManifestIndexSubmissions(s.T())
	ctx := context.Background()
	task, copyMeta, m, resp := s.newCopiedManifestResultFixture()
	resp.SegmentResults[0].ManifestIndexRewritten = nil

	defer mockey.Mock(createStorageConfig).Return(nil).Build().UnPatch()
	defer mockey.Mock(packed.GetManifestIndexInfosAsync).Return([]packed.ManifestIndexInfo{
		copiedManifestReviewEntry(),
	}, nil).Build().UnPatch()

	err := SyncCopySegmentTask(task, resp, copyMeta, m)
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
	mockManifestIndexSubmissions(s.T())
	ctx := context.Background()
	task, copyMeta, m, resp := s.newCopiedManifestResultFixture()
	s.Require().NoError(copyMeta.UpdateTask(ctx, task.GetTaskId(), UpdateCopyTaskIndexWriteToManifest(true)))
	task = copyMeta.GetTask(ctx, task.GetTaskId())
	result := resp.GetSegmentResults()[0]
	result.ManifestIndexRewritten = proto.Bool(true)
	result.ManifestIndexBuildIds = nil
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

	err := SyncCopySegmentTask(task, resp, copyMeta, m)
	s.Require().Error(err)
	s.Contains(err.Error(), "enabled at dispatch")
	s.Zero(manifestReads, "explicitly acknowledged empty indexes need no readback")

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
			withSegmentIndexManifestWrites(s.T(), !tc.enabled)
			store := newFakeManifestStore(s.T())
			ctx := context.Background()
			catalog := kvdatacoord.NewCatalog(NewMetaMemoryKV(), "", "")
			m := bootMetaForRestart(s.T(), catalog, collectionID)
			s.Require().NoError(m.indexMeta.CreateIndex(ctx, &model.Index{
				CollectionID: collectionID,
				FieldID:      101,
				IndexID:      indexID,
				IndexName:    "vec_idx",
				IndexParams:  []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}},
			}))
			s.Require().NoError(m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
				ID:             segmentID,
				CollectionID:   collectionID,
				PartitionID:    partitionID,
				State:          commonpb.SegmentState_Importing,
				IsImporting:    true,
				NumOfRows:      100,
				InsertChannel:  "ch1",
				StorageVersion: storage.StorageV3,
			})))

			task := createTestCopyTask(collectionID, segmentID).(*copySegmentTask)
			task.task.Load().IndexWriteToManifest = proto.Bool(tc.enabled)
			copyMeta, err := NewCopySegmentMeta(ctx, catalog, m, nil, nil)
			s.Require().NoError(err)
			s.Require().NoError(copyMeta.AddTask(ctx, task))
			// Reload the task from etcd to cover a DataCoord restart between
			// dispatch and worker completion.
			copyMeta, err = NewCopySegmentMeta(ctx, catalog, m, nil, nil)
			s.Require().NoError(err)
			task = copyMeta.GetTask(ctx, task.GetTaskId()).(*copySegmentTask)
			persistedPlacement, present := task.GetIndexWriteToManifest()
			s.True(present)
			s.Equal(tc.enabled, persistedPlacement)

			manifestPath := packed.MarshalManifestPath("/tmp/test-restart/files/insert_log/100/10/2001", 3)
			var manifestBuildIDs []int64
			if tc.enabled {
				store.revisions[manifestPath] = []packed.ManifestIndexInfo{{
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
					Binlogs:                makeTestCopySegmentBinlogs(),
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

			s.Require().NoError(SyncCopySegmentTask(task, resp, copyMeta, m))
			if tc.enabled {
				s.Greater(store.readCount, 0, "nonempty manifest indexes must be read back")
			} else {
				s.Zero(store.readCount, "explicitly acknowledged empty indexes need no readback")
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

			restarted := bootMetaForRestart(s.T(), catalog, collectionID)
			recovered := restarted.indexMeta.GetSegmentIndexes(collectionID, segmentID)
			s.Require().Len(recovered, 1)
			s.EqualValues(buildID, recovered[indexID].BuildID)
			s.Equal(commonpb.IndexState_Finished, recovered[indexID].IndexState)
		})
	}
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_LegacyStorageRetainsEtcdIndexes() {
	mockManifestIndexSubmissions(s.T())
	for _, version := range []int64{storage.StorageV1, storage.StorageV2} {
		for _, enabled := range []bool{false, true} {
			s.Run(strconv.FormatInt(version, 10)+"/manifest="+strconv.FormatBool(enabled), func() {
				ctx := context.Background()
				// Result handling must follow persisted placement even after a switch flip.
				withSegmentIndexManifestWrites(s.T(), !enabled)
				catalog := kvdatacoord.NewCatalog(NewMetaMemoryKV(), "", "")
				m := bootMetaForRestart(s.T(), catalog, 100)
				s.Require().NoError(m.indexMeta.CreateIndex(ctx, &model.Index{
					CollectionID: 100, FieldID: 101, IndexID: 300, IndexName: "vec_idx",
					IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}},
				}))
				segment := newTestCopySegment(2001)
				segment.StorageVersion = version
				segment.IsImporting = true
				s.Require().NoError(m.AddSegment(ctx, segment))
				task := createTestCopyTask(100, 2001).(*copySegmentTask)
				task.task.Load().IndexWriteToManifest = proto.Bool(enabled)
				copies, err := NewCopySegmentMeta(ctx, catalog, m, nil, nil)
				s.Require().NoError(err)
				s.Require().NoError(copies.AddTask(ctx, task))
				copies, err = NewCopySegmentMeta(ctx, catalog, m, nil, nil)
				s.Require().NoError(err)
				task = copies.GetTask(ctx, task.GetTaskId()).(*copySegmentTask)
				reader := mockey.Mock(packed.GetManifestIndexInfosAsync).Return(nil, merr.ErrServiceUnavailable).Build()
				defer reader.UnPatch()
				s.Require().NoError(SyncCopySegmentTask(task, &datapb.QueryCopySegmentResponse{
					State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
					SegmentResults: []*datapb.CopySegmentResult{{
						SegmentId: 2001, ImportedRows: 100, Binlogs: makeTestCopySegmentBinlogs(),
						IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{9001: {
							BuildId: 9001, FieldId: 101, IndexName: "vec_idx", IndexFilePaths: []string{"index.bin"},
						}},
					}},
				}, copies, m))
				s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskCompleted, task.GetState())
				s.Equal(commonpb.SegmentState_Flushed, m.GetSegment(ctx, 2001).GetState())
				s.False(m.GetSegment(ctx, 2001).GetIsImporting())
				persisted, err := catalog.ListSegmentIndexes(ctx, 100)
				s.Require().NoError(err)
				s.Require().Len(persisted, 1)
				s.EqualValues(9001, persisted[0].BuildID)
				s.Equal([]string{"index.bin"}, persisted[0].IndexFileKeys)
				restarted := bootMetaForRestart(s.T(), catalog, 100)
				indexes := restarted.indexMeta.GetSegmentIndexes(100, 2001)
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
	mt := &meta{ctx: context.Background(), catalog: catalog, segments: NewSegmentsInfo(ms5), metaStore: ms5}
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
	mockAlloc := mockey.Mock((*embeddedAllocator).AllocN).To(func(count int64) (typeutil.UniqueID, typeutil.UniqueID, error) { return 9001, 9001 + count, nil }).Build()
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
	mockResolve := mockey.Mock(snapshotadapter.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
		ForeignBucket: "bucket",
		ForeignCM:     sourceCM,
	}, nil).Build()
	defer mockResolve.UnPatch()
	mockReadExternal := mockey.Mock((*snapshotMeta).ReadExternalSnapshotDataWithChunkManager).To(
		func(_ *snapshotMeta, _ context.Context, gotCM snapshotstorage.ChunkManager, snapshotS3Location string, includeSegments bool) (*snapshotstorage.SnapshotData, error) {
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
	mockResolve := mockey.Mock(snapshotadapter.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
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
	key := Params.DataCoordCfg.SnapshotCopyConcurrency.Key
	Params.Save(key, "3")
	defer Params.Reset(key)
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
	mockResolve := mockey.Mock(snapshotadapter.ResolveForeignStorage).To(
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
		func(_ *snapshotMeta, _ context.Context, gotCM snapshotstorage.ChunkManager, snapshotS3Location string, includeSegments bool) (*snapshotstorage.SnapshotData, error) {
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
	mockAlloc := mockey.Mock((*embeddedAllocator).AllocN).To(func(count int64) (typeutil.UniqueID, typeutil.UniqueID, error) {
		start := nextID
		nextID += count
		return start, nextID, nil
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
		TaskSlot:     16,
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
	assert.EqualValues(t, 3, req.GetCopyConcurrency())
	assert.EqualValues(t, 16, req.GetTaskSlot())
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

	t.Run("recovered snapshot mismatch is rejected", func(t *testing.T) {
		snapshotData := newSnapshot()
		sourceCM := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
		mockResolve := mockey.Mock(snapshotadapter.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
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
		mockResolve := mockey.Mock(snapshotadapter.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
			ForeignBucket: "bucket",
			ForeignCM:     sourceCM,
		}, nil).Build()
		defer mockResolve.UnPatch()
		readCalls := 0
		mockRead := mockey.Mock((*snapshotMeta).ReadExternalSnapshotDataWithChunkManager).To(
			func(*snapshotMeta, context.Context, snapshotstorage.ChunkManager, string, bool) (*snapshotstorage.SnapshotData, error) {
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
	task, job := newAvroAssembleFixture(context.Background(), 1)
	segment := job.snapshotCache.data.Segments[0]
	segment.IndexFiles = []*indexpb.IndexFilePathInfo{{BuildID: 3001, FieldID: 100, IndexID: 1001}}
	segment.TextIndexFiles = map[int64]*datapb.TextIndexStats{200: {FieldID: 200, BuildID: 4001}}
	segment.JsonKeyIndexFiles = map[int64]*datapb.JsonKeyStats{300: {FieldID: 300, BuildID: 5001}}
	defer mockey.Mock(createStorageConfig).Return(nil).Build().UnPatch()
	next := int64(9001)
	defer mockey.Mock((*embeddedAllocator).AllocN).To(func(count int64) (int64, int64, error) {
		start := next
		next += count
		return start, next, nil
	}).Build().UnPatch()
	// Re-dispatch must not reference artifacts from the previous attempt.
	seen := map[int64]bool{}
	for attempt := 0; attempt < 2; attempt++ {
		req, err := AssembleCopySegmentRequest(task, job)
		require.NoError(t, err)
		require.Len(t, req.Targets, 1)
		ids := req.Targets[0].GetNewBuildIds()
		require.Len(t, ids, 3)
		for _, sourceID := range []int64{3001, 4001, 5001} {
			id, ok := ids[sourceID]
			require.True(t, ok)
			require.GreaterOrEqual(t, id, int64(9001))
			require.False(t, seen[id], "build IDs must be unique across kinds and dispatch attempts")
			seen[id] = true
		}
	}
}

// embeddedAllocator: named type for mockey interface-method patching; avoids a
// go1.26 `go vet` printf-pass panic on method expressions of anonymous structs.
type embeddedAllocator struct{ allocator.Allocator }

// assembleIndexPrecedenceFixture builds a one-segment copy request whose source
// is a StorageV3 segment carrying the snapshot's own (etcd-derived) index
// metadata. Native index discovery must not run during assembly.
func assembleIndexPrecedenceFixture(t *testing.T,
	mutateSegment ...func(*datapb.SegmentDescription),
) (*datapb.CopySegmentRequest, int) {
	mockManifestIndexSubmissions(t)
	t.Helper()

	task, job := newAvroAssembleFixture(context.Background(), 1)
	segment := job.snapshotCache.data.Segments[0]
	segment.ManifestHasIndex = nil
	segment.IndexFiles = []*indexpb.IndexFilePathInfo{{BuildID: 3001, FieldID: 100, IndexID: 1001, IndexName: "vec_idx"}}
	for _, mutate := range mutateSegment {
		mutate(segment)
	}
	defer mockey.Mock(createStorageConfig).Return(nil).Build().UnPatch()
	manifestReads := 0
	defer mockey.Mock(packed.GetManifestIndexInfosAsync).To(
		func(context.Context, *packed.ManifestIOContext, string, *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
			manifestReads++
			return nil, nil
		}).Build().UnPatch()

	nextID := int64(9001)
	defer mockey.Mock((*embeddedAllocator).AllocN).To(func(count int64) (typeutil.UniqueID, typeutil.UniqueID, error) {
		start := nextID
		nextID += count
		return start, nextID, nil
	}).Build().UnPatch()

	req, err := AssembleCopySegmentRequest(task, job)
	require.NoError(t, err)
	require.Len(t, req.GetSources(), 1)
	require.Len(t, req.GetTargets(), 1)
	return req, manifestReads
}

// Snapshot-captured index metadata is already complete, so DataCoord does not
// need to read the source manifest. DataNode handles inherited-entry retraction
// against the copied target manifest.
func TestAssembleCopySegmentRequest_SnapshotIndexFilesSkipManifestRead(t *testing.T) {
	req, manifestReads := assembleIndexPrecedenceFixture(t)

	// The snapshot answered which files to copy, so the manifest did not add to
	// or replace that list.
	indexFiles := req.GetSources()[0].GetIndexFiles()
	require.Len(t, indexFiles, 1)
	assert.Equal(t, int64(3001), indexFiles[0].GetBuildID())
	assert.Equal(t, "vec_idx", indexFiles[0].GetIndexName())
	assert.Zero(t, manifestReads)
}

func TestAssembleCopySegmentRequest_ManifestHasIndexFalseSkipsRead(t *testing.T) {
	req, manifestReads := assembleIndexPrecedenceFixture(t,
		func(segment *datapb.SegmentDescription) {
			segment.IndexFiles = nil
			segment.ManifestHasIndex = proto.Bool(false)
		})

	require.Len(t, req.GetSources(), 1)
	assert.NotNil(t, req.GetSources()[0].ManifestHasIndex)
	assert.False(t, req.GetSources()[0].GetManifestHasIndex())
	assert.Empty(t, req.GetSources()[0].GetIndexFiles())
	assert.Zero(t, manifestReads)
}

func newAvroAssembleFixture(ctx context.Context, segmentIDs ...int64) (*copySegmentTask, *copySegmentJob) {
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

	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	task.ctx, task.meta, task.alloc = ctx, &meta{}, &embeddedAllocator{}
	task.task.Load().State = datapb.CopySegmentTaskState_CopySegmentTaskNone
	task.task.Load().IdMappings = mappings
	job := newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobNone).(*copySegmentJob)
	job.SnapshotName = snapshotData.SnapshotInfo.Name
	job.snapshotCache = &copySegmentSnapshotCache{data: snapshotData}
	return task, job
}

func TestAssembleCopySegmentRequest_PreCanceledSkipsRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	task, job := newAvroAssembleFixture(ctx, 1)
	defer mockey.Mock(createStorageConfig).Return(nil).Build().UnPatch()
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
	m := &meta{indexMeta: im, segments: NewSegmentsInfo(ms6), metaStore: ms6}

	s.Nil(buildCopySegmentTargetIndexes(m, collectionID, false),
		"with manifest writes off the worker must get no definitions, or it would mint manifest entries the etcd records do not own")

	targets := buildCopySegmentTargetIndexes(m, collectionID, true)
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
	m := &meta{indexMeta: im, segments: NewSegmentsInfo(ms7), metaStore: ms7}
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
	task.(*copySegmentTask).task.Load().IndexWriteToManifest = proto.Bool(true)

	// publishedBuilds empty: the old-DataNode shape - a pointer whose index
	// section the copy never rewrote.
	err := validateCopiedManifestIndexPlacement(result, task, m, nil, buildCopySegmentIndexLookup(task, m))
	s.Require().Error(err)
	s.Contains(err.Error(), "predates manifest index republication")
	_, installed := im.segmentBuildInfo.Get(8001)
	s.False(installed, "no memory-only record may be installed for an entry the manifest does not carry")

	// A record with no target definition is skipped using the same task-local
	// lookup as legacy installation; it does not require a manifest entry.
	result.IndexInfos[8001].IndexName = "unselected_idx"
	s.NoError(validateCopiedManifestIndexPlacement(result, task, m, nil, buildCopySegmentIndexLookup(task, m)))
	result.IndexInfos[8001].IndexName = "vec_idx"

	// The control: the same shape dispatched with manifest writes off records to etcd.
	task.(*copySegmentTask).task.Load().IndexWriteToManifest = proto.Bool(false)
	s.NoError(validateCopiedManifestIndexPlacement(result, task, m, nil, buildCopySegmentIndexLookup(task, m)))
	s.NoError(syncVectorScalarIndexes(context.Background(), result, task, m, nil, buildCopySegmentIndexLookup(task, m)))
	_, installed = im.segmentBuildInfo.Get(8001)
	s.True(installed)
}

func TestAssembleCopySegmentRequest_ExternalAvroIndexes(t *testing.T) {
	for _, layout := range []datapb.SnapshotLayout{datapb.SnapshotLayout_SnapshotLayoutReferenced, datapb.SnapshotLayout_SnapshotLayoutSelfContained} {
		for _, hasIndexes := range []bool{false, true} {
			for _, cached := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/indexes-%t/cached-%t", layout, hasIndexes, cached), func(t *testing.T) {
					root := "moved"
					if layout == datapb.SnapshotLayout_SnapshotLayoutSelfContained {
						root += "/files"
					}
					task, job := newAvroAssembleFixture(context.Background(), 1)
					snapshot := job.snapshotCache.data
					snapshot.Layout, snapshot.ManifestFormatVersion = layout, snapshotstorage.SnapshotFormatVersion
					segment := snapshot.Segments[0]
					segment.ManifestPath = packed.MarshalManifestPath(root+"/insert_log/100/10/1", 3)
					if hasIndexes {
						segment.IndexFiles = []*indexpb.IndexFilePathInfo{{IndexID: 1001, BuildID: 3001, IndexFilePaths: []string{root + "/index_files/3001/0/10/1/index.bin"}}}
					} else {
						segment.ManifestHasIndex = nil
					}
					before := proto.Clone(segment)
					job.External, job.SnapshotS3Location = true, "s3://foreign/moved/snapshots/100/metadata/1.json"
					if !cached {
						job.snapshotCache = &copySegmentSnapshotCache{}
					}
					defer mockey.Mock(createStorageConfig).Return(&indexpb.StorageConfig{RootPath: "files"}).Build().UnPatch()
					defer mockey.Mock((*embeddedAllocator).AllocN).To(func(count int64) (int64, int64, error) { return 9001, 9001 + count, nil }).Build().UnPatch()
					resolves := 0
					defer mockey.Mock(snapshotadapter.ResolveForeignStorage).To(func(context.Context, *objectstorage.Config, snapshotstorage.Direction, string, string) (*snapshotstorage.ResolvedForeignStorage, error) {
						resolves++
						return &snapshotstorage.ResolvedForeignStorage{}, nil
					}).Build().UnPatch()
					defer mockey.Mock((*snapshotMeta).ReadExternalSnapshotDataWithChunkManager).Return(snapshot, nil).Build().UnPatch()
					defer mockey.Mock(packed.GetManifestIndexInfosAsync).To(func(context.Context, *packed.ManifestIOContext, string, *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
						t.Error("source native indexes must never be read during request assembly")
						return nil, merr.ErrServiceUnavailable
					}).Build().UnPatch()
					for range 2 {
						req, err := AssembleCopySegmentRequest(task, job)
						require.NoError(t, err)
						require.Equal(t, "s3://foreign/"+root, req.Sources[0].SourceRootPath)
						require.Equal(t, segment.IndexFiles, req.Sources[0].IndexFiles)
						require.True(t, proto.Equal(before, segment))
						if hasIndexes {
							require.EqualValues(t, 9001, req.Targets[0].NewBuildIds[3001])
						} else {
							require.Empty(t, req.Targets[0].NewBuildIds)
						}
					}
					if cached {
						require.Zero(t, resolves)
					} else {
						require.Equal(t, 1, resolves)
					}
				})
			}
		}
	}
}

func TestCopySegmentSnapshotCache_SharedIndexAndRetry(t *testing.T) {
	data := &snapshotstorage.SnapshotData{Segments: []*datapb.SegmentDescription{{SegmentId: 1}, {SegmentId: 2}}}
	for _, recovered := range []bool{false, true} {
		t.Run(strconv.FormatBool(recovered), func(t *testing.T) {
			cache := &copySegmentSnapshotCache{data: data}
			if recovered {
				cache = &copySegmentSnapshotCache{}
			}
			if recovered {
				_, index, err := cache.load(func() (*snapshotstorage.SnapshotData, error) { return nil, merr.ErrServiceUnavailable })
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
				require.Nil(t, index)
				require.Nil(t, cache.data, "failed loads must not poison the cache")
			}
			var loads atomic.Int64
			var wg sync.WaitGroup
			indexes := make([]map[int64]*datapb.SegmentDescription, 8)
			for i := range indexes {
				wg.Add(1)
				go func() {
					defer wg.Done()
					got, index, err := cache.load(func() (*snapshotstorage.SnapshotData, error) {
						loads.Add(1)
						return data, nil
					})
					assert.NoError(t, err)
					assert.Same(t, data, got)
					indexes[i] = index
				}()
			}
			wg.Wait()
			if recovered {
				require.EqualValues(t, 1, loads.Load())
			} else {
				require.Zero(t, loads.Load())
			}
			for _, index := range indexes {
				require.Same(t, data.Segments[0], index[1])
				require.Equal(t, reflect.ValueOf(indexes[0]).Pointer(), reflect.ValueOf(index).Pointer(), "all tasks reuse the same lookup table")
			}
			job := &copySegmentJob{CopySegmentJob: &datapb.CopySegmentJob{}, snapshotCache: cache}
			require.Same(t, cache, job.Clone().(*copySegmentJob).snapshotCache)
		})
	}
}

// Every result has independent partition/build IDs so publication cannot
// accidentally install one segment's mapping or stats on a different target.
func (s *CopySegmentTaskSuite) newCopiedResultsFixture(count int, version int64) (CopySegmentTask, CopySegmentMeta, *meta, *datapb.QueryCopySegmentResponse, *mockey.Mocker) {
	ctx := context.Background()
	task, copies, m, resp := s.newCopiedManifestResultFixture()
	m.ctx = ctx
	s.Require().NoError(copies.AddJob(ctx, newTestCopyJob(task.GetJobId(), datapb.CopySegmentJobState_CopySegmentJobExecuting)))
	base := resp.SegmentResults[0]
	resp.SegmentResults = nil
	mappings := make([]*datapb.CopySegmentIDMapping, 0, count)
	for i := 0; i < count; i++ {
		id, partition, build := int64(2001+i), int64(10+i), int64(9001+i)
		segment := NewSegmentInfo(&datapb.SegmentInfo{
			ID: id, CollectionID: 100, PartitionID: partition, NumOfRows: int64(100 + i),
			State: commonpb.SegmentState_Importing, IsImporting: true, InsertChannel: "ch1", StorageVersion: version,
		})
		if i == 0 {
			m.segments.SetSegment(id, segment)
		} else {
			s.Require().NoError(m.AddSegment(ctx, segment))
		}
		result := proto.Clone(base).(*datapb.CopySegmentResult)
		result.SegmentId = id
		result.ManifestPath = packed.MarshalManifestPath(fmt.Sprintf("files/insert_log/100/%d/%d", partition, id), 1)
		result.ManifestIndexBuildIds = []int64{build}
		index := proto.Clone(base.IndexInfos[9001]).(*datapb.VectorScalarIndexInfo)
		index.BuildId = build
		result.IndexInfos = map[int64]*datapb.VectorScalarIndexInfo{build: index}
		result.TextIndexInfos = map[int64]*datapb.TextIndexStats{102: {FieldID: 102, BuildID: build + 1000, Files: []string{fmt.Sprintf("text-%d", id)}}}
		result.JsonKeyIndexInfos = map[int64]*datapb.JsonKeyStats{103: {FieldID: 103, BuildID: build + 2000, Files: []string{fmt.Sprintf("json-%d", id)}}}
		if version < storage.StorageV3 {
			result.ManifestPath, result.ManifestIndexBuildIds, result.ManifestIndexRewritten = "", nil, nil
		}
		resp.SegmentResults = append(resp.SegmentResults, result)
		mappings = append(mappings, &datapb.CopySegmentIDMapping{SourceSegmentId: int64(i + 1), TargetSegmentId: id, PartitionId: partition})
	}
	s.Require().NoError(copies.UpdateTask(ctx, task.GetTaskId(), func(t CopySegmentTask) {
		t.(*copySegmentTask).task.Load().IdMappings = mappings
		t.(*copySegmentTask).task.Load().IndexWriteToManifest = proto.Bool(true)
	}))
	mockManifestIndexSubmissions(s.T())
	reader := mockey.Mock(packed.GetManifestIndexInfosAsync).To(func(_ context.Context, _ *packed.ManifestIOContext, manifest string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
		for i, result := range resp.SegmentResults {
			if result.ManifestPath == manifest {
				entry := copiedManifestReviewEntry()
				entry.BuildID = int64(9001 + i)
				entry.Path = fmt.Sprintf("files/index_files/%d/0/%d/%d", entry.BuildID, 10+i, result.SegmentId)
				return []packed.ManifestIndexInfo{entry}, nil
			}
		}
		return nil, merr.ErrServiceInternal
	}).Build()
	s.T().Cleanup(func() { reader.UnPatch() })

	return copies.GetTask(ctx, task.GetTaskId()), copies, m, resp, reader
}

type copyResultDataViewManager struct{ dataview.Manager }

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_PerSegmentPublication() {
	for _, tc := range []struct {
		count           int
		version         int64
		failure         string
		withoutDataView bool
	}{
		{0, storage.StorageV3, "", false}, {1, storage.StorageV3, "", false}, {1, storage.StorageV3, "", true},
		{3, storage.StorageV3, "", false}, {3, storage.StorageV2, "", false},
		{1, storage.StorageV3, "prepare", false}, {1, storage.StorageV3, "vector", false}, {1, storage.StorageV3, "visibility", false},
	} {
		s.Run(fmt.Sprintf("count-%d-v%d-%s-no-view-%t", tc.count, tc.version, tc.failure, tc.withoutDataView), func() {
			ctx := context.Background()
			task, copies, m, resp, _ := s.newCopiedResultsFixture(tc.count, tc.version)
			for _, result := range resp.SegmentResults {
				segment := m.GetSegment(ctx, result.SegmentId)
				segment.TextStatsLogs = map[int64]*datapb.TextIndexStats{99: {BuildID: 10}}
				segment.JsonKeyStats = map[int64]*datapb.JsonKeyStats{99: {BuildID: 20}}
			}
			if !tc.withoutDataView {
				m.dataViewManager = &copyResultDataViewManager{}
			}
			var writes int
			var alter func(*kvdatacoord.Catalog, context.Context, []*datapb.SegmentInfo, ...metastore.BinlogsIncrement) error
			defer mockey.Mock((*kvdatacoord.Catalog).AlterSegments).To(func(c *kvdatacoord.Catalog, ctx context.Context, segments []*datapb.SegmentInfo, logs ...metastore.BinlogsIncrement) error {
				writes++
				s.Len(segments, 1)
				for _, segment := range segments {
					// The catalog callback holds segMu; inspect the protected map.
					s.True(m.segments.GetSegment(segment.GetID()).GetIsImporting())
					s.Len(segment.GetTextStatsLogs(), 2)
					s.Len(segment.GetJsonKeyStats(), 2)
					if !segment.GetIsImporting() {
						s.Len(m.indexMeta.GetSegmentIndexes(100, segment.GetID()), 1)
					}
					if tc.failure == "prepare" && segment.GetIsImporting() || tc.failure == "visibility" && !segment.GetIsImporting() {
						return merr.ErrServiceUnavailable
					}
				}
				return alter(c, ctx, segments, logs...)
			}).Origin(&alter).Build().UnPatch()
			var notifications int
			defer mockey.Mock((*copyResultDataViewManager).Recompute).To(func(_ *copyResultDataViewManager, _ context.Context, collection int64) error {
				notifications++
				s.EqualValues(100, collection)
				return nil
			}).Build().UnPatch()
			// Count real lookups without Origin's shared per-call argument adapter.
			lookups := mockey.Mock((*indexMeta).GetIndexesForCollection).Build()
			defer lookups.UnPatch()
			var install func(*indexMeta, context.Context, *model.SegmentIndex) error
			defer mockey.Mock((*indexMeta).AddSegmentIndexFromManifest).To(func(im *indexMeta, ctx context.Context, index *model.SegmentIndex) error {
				segment := m.GetSegment(ctx, index.SegmentID)
				result := resp.SegmentResults[index.SegmentID-2001]
				s.True(segment.GetIsImporting())
				s.True(proto.Equal(segment.TextStatsLogs[102], result.TextIndexInfos[102]))
				s.True(proto.Equal(segment.JsonKeyStats[103], result.JsonKeyIndexInfos[103]))
				if tc.failure == "vector" {
					return merr.ErrServiceUnavailable
				}
				return install(im, ctx, index)
			}).Origin(&install).Build().UnPatch()
			err := SyncCopySegmentTask(task, resp, copies, m)
			if tc.failure != "" {
				s.ErrorIs(err, merr.ErrServiceUnavailable)
				s.True(m.GetSegment(ctx, 2001).GetIsImporting())
				s.True(copies.GetTask(ctx, task.GetTaskId()).GetCleanupRequired())
				s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskFailed, copies.GetTask(ctx, task.GetTaskId()).GetState())
				return
			}
			s.Require().NoError(err)
			expectedLookups := 1
			if tc.version >= storage.StorageV3 {
				expectedLookups += tc.count
			}
			s.EqualValues(expectedLookups, lookups.Times())
			s.Equal(tc.count*2, writes)
			if tc.withoutDataView {
				s.Zero(notifications)
			} else {
				s.Equal(tc.count, notifications)
			}
			for _, result := range resp.SegmentResults {
				segment := m.GetSegment(ctx, result.SegmentId)
				s.False(segment.GetIsImporting())
				s.EqualValues(10, segment.TextStatsLogs[99].BuildID)
				s.EqualValues(20, segment.JsonKeyStats[99].BuildID)
				s.True(proto.Equal(segment.TextStatsLogs[102], result.TextIndexInfos[102]))
				s.True(proto.Equal(segment.JsonKeyStats[103], result.JsonKeyIndexInfos[103]))
				index, ok := m.indexMeta.GetIndexJob(result.SegmentId + 7000)
				s.Require().True(ok)
				s.Equal(segment.GetPartitionID(), index.PartitionID)
				indexes := uncompressIndexFiles(&ServerHandler{s: &Server{meta: m}}, 100, result.SegmentId)
				s.Require().Len(indexes, 1)
				s.Equal(index.BuildID, indexes[0].GetBuildID())
			}
			s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskCompleted, copies.GetTask(ctx, task.GetTaskId()).GetState())
			s.False(copies.GetTask(ctx, task.GetTaskId()).GetCleanupRequired())
			persisted, err := m.catalog.ListSegments(ctx, 100)
			s.Require().NoError(err)
			byID := make(map[int64]*datapb.SegmentInfo)
			for _, segment := range persisted {
				byID[segment.GetID()] = segment
			}
			for _, result := range resp.SegmentResults {
				segment := byID[result.SegmentId]
				s.Require().NotNil(segment)
				s.False(segment.GetIsImporting())
				s.EqualValues(10, segment.TextStatsLogs[99].BuildID)
				s.EqualValues(20, segment.JsonKeyStats[99].BuildID)
				s.Len(segment.GetTextStatsLogs(), 2)
				s.Len(segment.GetJsonKeyStats(), 2)
			}
		})
	}
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_LaterSegmentFailureAndRecovery() {
	for _, failure := range []string{"partial prepare", "index", "partial visibility"} {
		s.Run(failure, func() {
			ctx := context.Background()
			task, copies, m, resp, reader := s.newCopiedResultsFixture(5, storage.StorageV3)
			m.dataViewManager = &copyResultDataViewManager{}
			notifications := 0
			defer mockey.Mock((*copyResultDataViewManager).Recompute).To(func(*copyResultDataViewManager, context.Context, int64) error {
				notifications++
				return nil
			}).Build().UnPatch()
			writes := 0
			var alter func(*kvdatacoord.Catalog, context.Context, []*datapb.SegmentInfo, ...metastore.BinlogsIncrement) error
			patch := mockey.Mock((*kvdatacoord.Catalog).AlterSegments).To(func(c *kvdatacoord.Catalog, ctx context.Context, segments []*datapb.SegmentInfo, logs ...metastore.BinlogsIncrement) error {
				writes++
				if failure == "partial prepare" && writes == 3 || failure == "partial visibility" && writes == 4 {
					// Model a catalog save that persists the segment before returning
					// an error. Memory must stay unchanged until a successful save.
					s.Require().NoError(alter(c, ctx, segments[:1], logs...))
					return merr.ErrServiceUnavailable
				}
				return alter(c, ctx, segments, logs...)
			}).Origin(&alter).Build()
			defer patch.UnPatch()
			if failure == "index" {
				var install func(*indexMeta, context.Context, *model.SegmentIndex) error
				defer mockey.Mock((*indexMeta).AddSegmentIndexFromManifest).To(func(im *indexMeta, ctx context.Context, index *model.SegmentIndex) error {
					if index.SegmentID == 2004 {
						return merr.ErrServiceUnavailable
					}
					return install(im, ctx, index)
				}).Origin(&install).Build().UnPatch()
			}
			s.ErrorIs(SyncCopySegmentTask(task, resp, copies, m), merr.ErrServiceUnavailable)
			patch.UnPatch()
			visible := 1
			if failure == "index" {
				visible = 3
			}
			s.Equal(visible, notifications, "successful segments were reconciled before the failure")
			for _, result := range resp.SegmentResults {
				segment := m.GetSegment(ctx, result.SegmentId)
				s.Equal(result.SegmentId >= int64(2001+visible), segment.GetIsImporting())
				if result.SegmentId == 2005 {
					s.Equal(commonpb.SegmentState_Importing, segment.GetState(), "later segments must not be attempted")
				}
			}
			persisted, err := m.catalog.ListSegments(ctx, 100)
			s.Require().NoError(err)
			recoveredStore := metacache.NewMetaStore(m.catalog)
			recovered := &meta{
				ctx: ctx, catalog: m.catalog, segments: NewSegmentsInfo(recoveredStore), metaStore: recoveredStore,
				chunkManager: storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test-restart")),
				indexMeta:    createTestIndexMeta(s.T(), 100, m.indexMeta.indexes[100], m.catalog),
			}
			// Use the real startup manifest-index recovery against persisted
			// segment rows. Only immutable native storage is replaced by a fake.
			entries := make(map[string][]packed.ManifestIndexInfo)
			for _, result := range resp.SegmentResults {
				id := result.SegmentId
				build := result.ManifestIndexBuildIds[0]
				entries[result.ManifestPath] = []packed.ManifestIndexInfo{{
					IndexName: "vec_idx", IndexType: "HNSW", ColumnName: "vector", FieldID: 101,
					IndexID: 300, BuildID: build, NumRows: 100,
					Path:          fmt.Sprintf("/tmp/test-restart/index_files/%d/0/%d/%d", build, id-1991, id),
					IndexFileKeys: []string{"index.bin"},
				}}
			}
			for _, segment := range persisted {
				recovered.segments.SetSegment(segment.GetID(), NewSegmentInfo(segment))
				if !segment.GetIsImporting() {
					s.Len(segment.GetTextStatsLogs(), 1)
					s.Len(segment.GetJsonKeyStats(), 1)
					s.True(segment.GetManifestHasIndex())
				}
			}
			reader.UnPatch()
			defer mockey.Mock(packed.GetManifestIndexInfosAsync).To(func(_ context.Context, _ *packed.ManifestIOContext, path string, _ *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
				return entries[path], nil
			}).Build().UnPatch()
			s.Require().NoError(recovered.reloadSegmentIndexesFromManifests(ctx))
			for _, segment := range persisted {
				if segment.GetManifestHasIndex() {
					s.Len(recovered.indexMeta.GetSegmentIndexes(100, segment.GetID()), 1)
				}
			}
			recoveredCopies, err := NewCopySegmentMeta(ctx, m.catalog, recovered, nil, nil)
			s.Require().NoError(err)
			recoveredTask := recoveredCopies.GetTask(ctx, task.GetTaskId())
			s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskFailed, recoveredTask.GetState())
			s.True(recoveredTask.GetCleanupRequired())
			s.Equal(datapb.CopySegmentJobState_CopySegmentJobFailed, recoveredCopies.GetJob(ctx, task.GetJobId()).GetState())
			s.Require().NoError(cleanupRejectedCopy(ctx, recoveredTask, recovered, recoveredCopies))
			s.True(recoveredCopies.GetTask(ctx, task.GetTaskId()).GetCleanupRequired(), "existing segment metadata must prevent prefix deletion")
		})
	}
}

func TestAssembleCopySegmentRequest_BuildIDRanges(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		fields, segments, builds    int
		failCall, expectedCalls     int
		err                         error
		rangeSize                   int64
		cancelBefore, cancelInAlloc bool
	}{
		{name: "one range for task", fields: 2, segments: 3, builds: 7, expectedCalls: 1},
		{name: "refill", fields: 1, segments: 2, builds: 25, rangeSize: 20, expectedCalls: 3},
		{name: "no indexes", fields: 1, segments: 2},
		{name: "missing schema", segments: 2, builds: 3, expectedCalls: 1},
		{name: "allocation error", fields: 1, segments: 2, builds: 1, failCall: 1, expectedCalls: 1, err: merr.ErrServiceUnavailable},
		{name: "refill error", fields: 1, segments: 2, builds: 21, failCall: 2, rangeSize: 20, expectedCalls: 2, err: merr.ErrServiceUnavailable},
		{name: "cancel before allocation", fields: 1, segments: 1, builds: 1, cancelBefore: true, err: context.Canceled},
		{name: "cancel during allocation", fields: 1, segments: 1, builds: 1, cancelInAlloc: true, expectedCalls: 1, err: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			snapshot := &snapshotstorage.SnapshotData{
				SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100, Name: "snapshot"},
			}
			if tc.fields > 0 {
				snapshot.Collection = &datapb.CollectionDescription{Schema: &schemapb.CollectionSchema{}}
				for i := 0; i < tc.fields; i++ {
					snapshot.Collection.Schema.Fields = append(snapshot.Collection.Schema.Fields,
						&schemapb.FieldSchema{FieldID: int64(100 + i), DataType: schemapb.DataType_JSON})
				}
			}
			task := createTestCopyTask(100, 2001).(*copySegmentTask)
			task.ctx, task.alloc = ctx, &embeddedAllocator{}
			task.task.Load().IdMappings = nil
			for i := 0; i < tc.segments; i++ {
				segment := &datapb.SegmentDescription{
					SegmentId: int64(i + 1), PartitionId: 10,
					TextIndexFiles:    map[int64]*datapb.TextIndexStats{102: {BuildID: 0}},
					JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{103: {BuildID: 0}},
				}
				for j := 0; j < tc.builds; j++ {
					segment.IndexFiles = append(segment.IndexFiles,
						&indexpb.IndexFilePathInfo{BuildID: int64(3001 + j), FieldID: 100})
				}
				if tc.builds > 0 {
					// Duplicate references and kinds consume one ID within a segment;
					// the same source build in another target consumes a fresh ID.
					segment.IndexFiles = append(segment.IndexFiles, segment.IndexFiles[0])
					segment.TextIndexFiles[104] = &datapb.TextIndexStats{BuildID: 3001}
					segment.JsonKeyIndexFiles[105] = &datapb.JsonKeyStats{BuildID: 3001}
				}
				snapshot.Segments = append(snapshot.Segments, segment)
				task.task.Load().IdMappings = append(task.task.Load().IdMappings,
					&datapb.CopySegmentIDMapping{SourceSegmentId: segment.SegmentId, TargetSegmentId: int64(2001 + i), PartitionId: 10})
			}
			job := &copySegmentJob{
				CopySegmentJob: &datapb.CopySegmentJob{JobId: 100, CollectionId: 100, SnapshotName: "snapshot"},
				snapshotCache:  &copySegmentSnapshotCache{data: snapshot},
			}
			defer mockey.Mock(createStorageConfig).To(func() *indexpb.StorageConfig {
				if tc.cancelBefore {
					cancel()
				}
				return nil
			}).Build().UnPatch()
			read := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(nil, merr.ErrServiceUnavailable).Build()
			defer read.UnPatch()
			individual := mockey.Mock((*embeddedAllocator).AllocID).Return(int64(0), merr.ErrServiceUnavailable).Build()
			defer individual.UnPatch()
			var counts []int64
			next := int64(9001)
			defer mockey.Mock((*embeddedAllocator).AllocN).To(func(count int64) (int64, int64, error) {
				counts = append(counts, count)
				if len(counts) == tc.failCall {
					return 0, 0, merr.ErrServiceUnavailable
				}
				if tc.cancelInAlloc {
					cancel()
				}
				start := next
				allocated := count
				if tc.rangeSize > 0 {
					// Inject a shorter returned range to exercise exhaustion without
					// constructing a million-index snapshot. Every request is still 1M.
					allocated = tc.rangeSize
				}
				next += allocated
				return start, next, nil
			}).Build().UnPatch()

			request, err := AssembleCopySegmentRequest(task, job)
			for _, count := range counts {
				require.EqualValues(t, 1_000_000, count)
			}
			require.Zero(t, read.Times(), "assembly must reuse the existing snapshot cache")
			require.Zero(t, individual.Times(), "build IDs must never use individual allocation")
			require.Len(t, counts, tc.expectedCalls)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				require.Nil(t, request)
				return
			}
			require.NoError(t, err)
			needed := tc.builds * tc.segments
			seen := make(map[int64]struct{}, needed)
			for _, target := range request.Targets {
				require.Len(t, target.NewBuildIds, tc.builds)
				for _, id := range target.NewBuildIds {
					require.GreaterOrEqual(t, id, int64(9001))
					require.Less(t, id, next)
					_, exists := seen[id]
					require.False(t, exists, "different target builds must not share an ID")
					seen[id] = struct{}{}
				}
			}
			require.Len(t, seen, needed)
		})
	}
}

func (s *CopySegmentTaskSuite) TestSyncCopySegmentTask_ForeignManifestIndexEntryFailsTask() {
	mockManifestIndexSubmissions(s.T())
	ctx := context.Background()
	task, copyMeta, m, resp := s.newCopiedManifestResultFixture()
	resp.SegmentResults[0].ManifestIndexRewritten = nil

	defer mockey.Mock(createStorageConfig).Return(nil).Build().UnPatch()
	defer mockey.Mock(packed.GetManifestIndexInfosAsync).Return([]packed.ManifestIndexInfo{
		// The target's own re-derived entry may coexist with the leftover; only
		// the foreign one is the failure.
		copiedManifestReviewEntry(),
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

func copiedManifestReviewEntry() packed.ManifestIndexInfo {
	return packed.ManifestIndexInfo{
		IndexID: 300, BuildID: 9001, FieldID: 101, IndexName: "vec_idx", IndexType: "HNSW",
		Properties: map[string]string{common.IndexTypeKey: "HNSW"},
		Path:       "files/index_files/9001/0/10/2001", IndexFileKeys: []string{"index.bin"},
	}
}

func (s *CopySegmentTaskSuite) TestCopiedManifestRejectsChangedIdentityAndPaths() {
	mockManifestIndexSubmissions(s.T())
	for _, acknowledged := range []bool{false, true} {
		for _, scenario := range []string{"recreated", "parameters", "foreign path", "wrong files", "duplicate build", "missing acknowledged build", "unacknowledged build"} {
			s.Run(strconv.FormatBool(acknowledged)+"/"+scenario, func() {
				task, copyMeta, m, resp := s.newCopiedManifestResultFixture()
				result := resp.GetSegmentResults()[0]
				result.ManifestIndexRewritten = nil
				if acknowledged {
					result.ManifestIndexRewritten = proto.Bool(true)
					result.ManifestIndexBuildIds = []int64{9001}
				}
				entry := copiedManifestReviewEntry()
				switch scenario {
				case "recreated":
					s.Require().NoError(m.indexMeta.MarkIndexAsDeleted(context.Background(), 100, []int64{300}))
					s.Require().NoError(m.indexMeta.CreateIndex(context.Background(), &model.Index{
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
				entries := []packed.ManifestIndexInfo{entry}
				switch scenario {
				case "duplicate build":
					entries = append(entries, entry)
				case "missing acknowledged build":
					result.ManifestIndexRewritten = proto.Bool(true)
					result.ManifestIndexBuildIds = []int64{1234}
				case "unacknowledged build":
					result.ManifestIndexRewritten = proto.Bool(true)
					result.ManifestIndexBuildIds = []int64{9001}
					extra := entry
					extra.BuildID = 9002
					extra.Path = "files/index_files/9002/0/10/2001"
					entries = append(entries, extra)
					info := proto.Clone(result.IndexInfos[9001]).(*datapb.VectorScalarIndexInfo)
					info.BuildId = 9002
					result.IndexInfos[9002] = info
				}
				defer mockey.Mock(packed.GetManifestIndexInfosAsync).Return(entries, nil).Build().UnPatch()
				s.Error(SyncCopySegmentTask(task, resp, copyMeta, m))
				segment := m.GetSegment(context.Background(), 2001)
				s.Equal(commonpb.SegmentState_Importing, segment.GetState())
				s.False(segment.GetManifestHasIndex())
				s.Empty(m.indexMeta.GetSegmentIndexes(100, 2001))
				s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskFailed, copyMeta.GetTask(context.Background(), task.GetTaskId()).GetState())
			})
		}
	}
}

func (s *CopySegmentTaskSuite) TestCopiedManifestKeepsVerifiedIndexIDDuringInstall() {
	ctx := context.Background()
	task, _, m, resp := s.newCopiedManifestResultFixture()
	result := resp.GetSegmentResults()[0]
	verified, err := verifyCopiedManifestIndexOwnership(result, task, m, m.GetSegment(ctx, result.GetSegmentId()), createStorageConfig().GetRootPath(), []packed.ManifestIndexInfo{copiedManifestReviewEntry()})
	s.Require().NoError(err)
	// Recreate after verification but before the in-memory record is installed.
	s.Require().NoError(m.indexMeta.MarkIndexAsDeleted(ctx, 100, []int64{300}))
	s.Require().NoError(m.indexMeta.CreateIndex(ctx, &model.Index{
		CollectionID: 100, FieldID: 101, IndexID: 301, IndexName: "vec_idx",
	}))
	s.Require().NoError(syncVectorScalarIndexes(ctx, result, task, m, verified, buildCopySegmentIndexLookup(task, m)))
	record, ok := m.indexMeta.GetIndexJob(9001)
	s.Require().True(ok)
	s.EqualValues(300, record.IndexID, "the artifact belongs to the retired definition, never its replacement")
	s.NotContains(m.indexMeta.GetSegmentIndexes(100, 2001), int64(301))
}

func (s *CopySegmentTaskSuite) TestCopyManifestVerificationParallelAndFailClosed() {
	mockManifestIndexSubmissions(s.T())
	old := Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue("2")
	defer Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue(old)
	task, copies, m, resp := s.newCopiedManifestResultFixture()
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
	go func() { finished <- SyncCopySegmentTask(task, resp, copies, m) }()
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

func (s *CopySegmentTaskSuite) TestCopiedManifestEmptyAndExplicitFailure() {
	mockManifestIndexSubmissions(s.T())
	for _, scenario := range []string{"legacy empty", "acknowledged empty", "explicit failure", "no manifest"} {
		s.Run(scenario, func() {
			task, copies, m, resp := s.newCopiedManifestResultFixture()
			result := resp.SegmentResults[0]
			result.IndexInfos, result.ManifestIndexBuildIds = nil, nil
			result.ManifestIndexRewritten = nil
			switch scenario {
			case "acknowledged empty":
				result.ManifestIndexRewritten = proto.Bool(true)
			case "explicit failure":
				result.ManifestIndexRewritten = proto.Bool(false)
			case "no manifest":
				result.ManifestPath = ""
			}
			reader := mockey.Mock(packed.GetManifestIndexInfosAsync).Return(nil, nil).Build()
			defer reader.UnPatch()
			err := SyncCopySegmentTask(task, resp, copies, m)
			if scenario == "explicit failure" {
				s.ErrorContains(err, "rewrite was not completed")
				s.True(m.GetSegment(context.Background(), 2001).GetIsImporting())
			} else {
				s.NoError(err)
				s.False(m.GetSegment(context.Background(), 2001).GetIsImporting())
			}
			if scenario == "legacy empty" {
				s.Equal(1, reader.Times())
			} else {
				s.Zero(reader.Times())
			}
		})
	}
}
func (s *CopySegmentTaskSuite) TestQueryCompletedCopyRetriesCleanupIntentFailure() {
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copies, m := newCopySegmentTaskTestMeta(s.T(), task)
	s.Require().NoError(m.AddSegment(context.Background(), newTestCopySegment(2001)))
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
