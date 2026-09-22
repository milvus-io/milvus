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
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	taskcommon "github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

func TestPreImportTask_GetTaskType(t *testing.T) {
	task := &preImportTask{}
	assert.Equal(t, taskcommon.PreImport, task.GetTaskType())
}

func TestPreImportRetryIsOwnedByBusinessInspector(t *testing.T) {
	task := &preImportTask{}
	task.task.Store(&datapb.PreImportTask{State: datapb.ImportTaskStateV2_Retry, TaskVersion: 3})
	assert.Equal(t, taskcommon.Retry, task.GetTaskState())
	assert.Equal(t, int64(3), task.GetTaskVersion())
}

func TestPreImportTask_TaskTime(t *testing.T) {
	task := &preImportTask{
		times: taskcommon.NewTimes(),
	}
	startTime := time.Now()
	endTime := time.Now()
	queueTime := time.Now()
	task.SetTaskTime(taskcommon.TimeStart, startTime)
	task.SetTaskTime(taskcommon.TimeEnd, endTime)
	task.SetTaskTime(taskcommon.TimeQueue, queueTime)

	assert.Equal(t, task.GetTaskTime(taskcommon.TimeStart), startTime)
	assert.Equal(t, task.GetTaskTime(taskcommon.TimeEnd), endTime)
	assert.Equal(t, task.GetTaskTime(taskcommon.TimeQueue), queueTime)
}

func TestPreImportTask_CreateTaskOnWorker(t *testing.T) {
	t.Run("stale task removed by terminal GC", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		require.NoError(t, err)

		task := &preImportTask{importMeta: im, tr: timerecord.NewTimeRecorder("")}
		task.task.Store(&datapb.PreImportTask{
			JobID: 1, TaskID: 2, CollectionID: 3,
			State: datapb.ImportTaskStateV2_Pending,
		})

		task.CreateTaskOnWorker(1, session.NewMockCluster(t))

		assert.Equal(t, datapb.ImportTaskStateV2_None, task.GetState())
	})

	t.Run("CreatePreImportTask rpc failed", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)

		var job ImportJob = &importJob{
			ImportJob: &datapb.ImportJob{
				JobID: 1,
			},
		}
		err = im.AddJob(context.TODO(), job)
		assert.NoError(t, err)

		taskProto := &datapb.PreImportTask{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			State:        datapb.ImportTaskStateV2_Pending,
		}
		task := &preImportTask{
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().CreatePreImport(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("test"))
		cluster.EXPECT().DropImport(int64(1), int64(2)).Return(errors.New("worker outcome is still unknown"))
		task.CreateTaskOnWorker(1, cluster)
		// The ambiguous worker attempt is recorded as retry debt first. Its
		// best-effort Drop may fail without blocking the fresh-ID replacement.
		assert.Equal(t, datapb.ImportTaskStateV2_Retry, task.GetState())
		assert.EqualValues(t, 1, task.GetNodeID())
	})

	t.Run("UpdateTask failed", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)

		var job ImportJob = &importJob{
			ImportJob: &datapb.ImportJob{
				JobID: 1,
			},
		}
		err = im.AddJob(context.TODO(), job)
		assert.NoError(t, err)

		taskProto := &datapb.PreImportTask{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			State:        datapb.ImportTaskStateV2_Pending,
		}
		task := &preImportTask{
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		// CreatePreImport is deliberately not declared: a task whose assignment
		// cannot be persisted must not be sent at all, or an accepted request
		// would leave an attempt nobody can reclaim.
		cluster := session.NewMockCluster(t)

		catalog = mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(errors.New("mock err"))
		task.importMeta.(*importMeta).catalog = catalog
		task.CreateTaskOnWorker(1, cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
		assert.NotEqualValues(t, 1, task.GetNodeID(),
			"a task that was never sent must not name the node it would have gone to")
	})

	t.Run("normal", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)

		var job ImportJob = &importJob{
			ImportJob: &datapb.ImportJob{
				JobID: 1,
			},
		}
		err = im.AddJob(context.TODO(), job)
		assert.NoError(t, err)

		taskProto := &datapb.PreImportTask{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			State:        datapb.ImportTaskStateV2_Pending,
		}
		task := &preImportTask{
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().CreatePreImport(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		task.CreateTaskOnWorker(1, cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
	})
}

func TestPreImportTask_QueryTaskOnWorker(t *testing.T) {
	t.Run("stale task removed by terminal GC", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		require.NoError(t, err)

		task := &preImportTask{importMeta: im, tr: timerecord.NewTimeRecorder("")}
		task.task.Store(&datapb.PreImportTask{
			JobID: 1, TaskID: 2, CollectionID: 3, NodeID: 7,
			State: datapb.ImportTaskStateV2_InProgress,
		})

		task.QueryTaskOnWorker(session.NewMockCluster(t))

		assert.Equal(t, datapb.ImportTaskStateV2_None, task.GetState())
	})

	t.Run("QueryPreImport rpc failed", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		// AddTask plus one persisted Retry handoff for each of the three worker
		// outcomes below.
		catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil).Times(4)

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)

		taskProto := &datapb.PreImportTask{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			NodeID:       7,
			State:        datapb.ImportTaskStateV2_InProgress,
		}
		task := &preImportTask{
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		// Retry debt is durable before the best-effort worker Drop. The old node
		// assignment remains available for cleanup until the fresh-ID swap.
		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryPreImport(mock.Anything, mock.Anything).Return(nil, errors.New("mock err")).Once()
		cluster.EXPECT().DropImport(int64(7), int64(2)).Return(nil).Once()
		task.QueryTaskOnWorker(cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Retry, task.GetState())
		assert.EqualValues(t, 7, task.GetNodeID())

		// And likewise when the node is gone: it took the task with it, so the
		// reclaim succeeds.
		taskProto.NodeID = 7
		task.task.Store(taskProto)
		cluster.EXPECT().QueryPreImport(mock.Anything, mock.Anything).
			Return(nil, merr.WrapErrNodeNotFound(7)).Once()
		cluster.EXPECT().DropImport(int64(7), int64(2)).Return(merr.WrapErrNodeNotFound(7)).Once()
		task.QueryTaskOnWorker(cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Retry, task.GetState())
		assert.EqualValues(t, 7, task.GetNodeID())

		// A lost coordinator followed by worker cleanup can leave the persisted
		// assignment pointing at a task the worker no longer knows. None is the
		// same reclaimable outcome as NodeNotFound.
		taskProto.NodeID = 7
		taskProto.State = datapb.ImportTaskStateV2_InProgress
		task.task.Store(taskProto)
		cluster.EXPECT().QueryPreImport(mock.Anything, mock.Anything).Return(&datapb.QueryPreImportResponse{
			State: datapb.ImportTaskStateV2_None,
		}, nil).Once()
		cluster.EXPECT().DropImport(int64(7), int64(2)).Return(nil).Once()
		task.QueryTaskOnWorker(cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Retry, task.GetState())
		assert.EqualValues(t, 7, task.GetNodeID())
	})

	t.Run("retry handoff metadata write failed", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil).Once()
		catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(errors.New("mock save err")).Once()

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)
		persisted := &datapb.PreImportTask{
			JobID: 1, TaskID: 2, CollectionID: 3, NodeID: 7,
			State: datapb.ImportTaskStateV2_InProgress,
		}
		task := &preImportTask{importMeta: im, tr: timerecord.NewTimeRecorder("")}
		task.task.Store(persisted)
		assert.NoError(t, im.AddTask(context.TODO(), task))

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryPreImport(int64(7), mock.Anything).Return(nil, errors.New("mock query err")).Once()
		cluster.EXPECT().DropImport(int64(7), int64(2)).Return(nil).Once()
		task.QueryTaskOnWorker(cluster)

		assert.Equal(t, datapb.ImportTaskStateV2_Retry, task.GetState(), "the scheduler must release retry debt even if its catalog write was ambiguous")
		assert.EqualValues(t, 7, task.GetNodeID())
		assert.Equal(t, datapb.ImportTaskStateV2_InProgress, persisted.GetState(), "a restart must recover from the last durable assignment")
		assert.EqualValues(t, 7, persisted.GetNodeID())
	})

	t.Run("preimport failed", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)

		var job ImportJob = &importJob{
			ImportJob: &datapb.ImportJob{
				JobID: 1,
				State: internalpb.ImportJobState_PreImporting,
			},
		}
		err = im.AddJob(context.TODO(), job)
		assert.NoError(t, err)

		taskProto := &datapb.PreImportTask{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			NodeID:       7,
			State:        datapb.ImportTaskStateV2_InProgress,
		}
		task := &preImportTask{
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryPreImport(mock.Anything, mock.Anything).Return(&datapb.QueryPreImportResponse{
			State: datapb.ImportTaskStateV2_Failed,
		}, nil)
		task.QueryTaskOnWorker(cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Failed, task.GetState())
		job = im.GetJob(context.TODO(), 1)
		assert.Equal(t, internalpb.ImportJobState_Failed, job.GetState())
	})

	t.Run("persist job failure before task failure", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)

		var (
			writeOrder       []string
			failTaskWrite    = true
			durableTaskState datapb.ImportTaskStateV2
		)
		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, job *datapb.ImportJob) error {
				if job.GetState() == internalpb.ImportJobState_Failed {
					writeOrder = append(writeOrder, "job")
				}
				return nil
			})
		catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, task *datapb.PreImportTask) error {
				if task.GetState() == datapb.ImportTaskStateV2_Failed {
					writeOrder = append(writeOrder, "task")
					if failTaskWrite {
						failTaskWrite = false
						return errors.New("mock task write failure")
					}
				}
				durableTaskState = task.GetState()
				return nil
			})

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)
		job := &importJob{ImportJob: &datapb.ImportJob{
			JobID: 1,
			State: internalpb.ImportJobState_PreImporting,
		}}
		assert.NoError(t, im.AddJob(context.TODO(), job))

		task := &preImportTask{importMeta: im, tr: timerecord.NewTimeRecorder("")}
		task.task.Store(&datapb.PreImportTask{
			JobID: 1, TaskID: 2, CollectionID: 3, NodeID: 7,
			State: datapb.ImportTaskStateV2_InProgress,
		})
		assert.NoError(t, im.AddTask(context.TODO(), task))

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryPreImport(mock.Anything, mock.Anything).Return(&datapb.QueryPreImportResponse{
			State:  datapb.ImportTaskStateV2_Failed,
			Reason: "bad input",
		}, nil)
		task.QueryTaskOnWorker(cluster)

		assert.Equal(t, []string{"job", "task"}, writeOrder)
		assert.Equal(t, internalpb.ImportJobState_Failed, im.GetJob(context.TODO(), 1).GetState())
		assert.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
		assert.Equal(t, datapb.ImportTaskStateV2_InProgress, durableTaskState)

		// The durable Failed job owns convergence after the second write fails.
		checker := &importChecker{ctx: context.TODO(), importMeta: im}
		checker.tryFailingTasks(im.GetJob(context.TODO(), 1))
		assert.Equal(t, datapb.ImportTaskStateV2_Failed, task.GetState())
		assert.Equal(t, datapb.ImportTaskStateV2_Failed, durableTaskState)
	})

	t.Run("normal", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)

		taskProto := &datapb.PreImportTask{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			NodeID:       7,
			State:        datapb.ImportTaskStateV2_InProgress,
		}
		task := &preImportTask{
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryPreImport(mock.Anything, mock.Anything).Return(&datapb.QueryPreImportResponse{
			State: datapb.ImportTaskStateV2_Completed,
		}, nil)
		task.QueryTaskOnWorker(cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Completed, task.GetState())
	})
}

func TestPreImportTask_DropTaskOnWorker(t *testing.T) {
	t.Run("DropImport rpc failed", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)

		taskProto := &datapb.PreImportTask{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			NodeID:       7,
			State:        datapb.ImportTaskStateV2_Completed,
		}
		task := &preImportTask{
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().DropImport(mock.Anything, mock.Anything).Return(errors.New("mock err"))
		task.DropTaskOnWorker(cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Completed, task.GetState())
		assert.Equal(t, int64(7), task.GetNodeID())
	})

	t.Run("normal", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)

		taskProto := &datapb.PreImportTask{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			NodeID:       7,
			State:        datapb.ImportTaskStateV2_Completed,
		}
		task := &preImportTask{
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().DropImport(mock.Anything, mock.Anything).Return(nil)
		task.DropTaskOnWorker(cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Completed, task.GetState())
		assert.Equal(t, int64(NullNodeID), task.GetNodeID())
	})
}
