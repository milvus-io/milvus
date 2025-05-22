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

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	taskcommon "github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
)

func TestPreImportTask_GetTaskType(t *testing.T) {
	task := &preImportTask{}
	assert.Equal(t, taskcommon.PreImport, task.GetTaskType())
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
			imeta: im,
			tr:    timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().CreatePreImport(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("test"))
		task.CreateTaskOnWorker(1, cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
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
			imeta: im,
			tr:    timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().CreatePreImport(mock.Anything, mock.Anything, mock.Anything).Return(nil)

		catalog = mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(errors.New("mock err"))
		task.imeta.(*importMeta).catalog = catalog
		task.CreateTaskOnWorker(1, cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
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
			imeta: im,
			tr:    timerecord.NewTimeRecorder(""),
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
	t.Run("QueryPreImport rpc failed", func(t *testing.T) {
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
			imeta: im,
			tr:    timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryPreImport(mock.Anything, mock.Anything).Return(nil, errors.New("mock err"))
		task.QueryTaskOnWorker(cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
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
			imeta: im,
			tr:    timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryPreImport(mock.Anything, mock.Anything).Return(&datapb.QueryPreImportResponse{
			State: datapb.ImportTaskStateV2_Failed,
		}, nil)
		task.QueryTaskOnWorker(cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
		job = im.GetJob(context.TODO(), 1)
		assert.Equal(t, internalpb.ImportJobState_Failed, job.GetState())
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
			imeta: im,
			tr:    timerecord.NewTimeRecorder(""),
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
			imeta: im,
			tr:    timerecord.NewTimeRecorder(""),
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
			imeta: im,
			tr:    timerecord.NewTimeRecorder(""),
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
