// Licensed to the LF AI & Data foundation under one
// or more contributor license agreementassert. See the NOTICE file
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
	"math/rand"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
)

func TestImportMeta_Restore(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return([]*datapb.ImportJob{{JobID: 0}}, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return([]*datapb.PreImportTask{{TaskID: 1}}, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return([]*datapb.ImportTaskV2{{TaskID: 2}}, nil)
	ctx := context.TODO()

	im, err := NewImportMeta(ctx, catalog, nil, nil)
	assert.NoError(t, err)

	jobs := im.GetJobBy(ctx)
	assert.Equal(t, 1, len(jobs))
	assert.Equal(t, int64(0), jobs[0].GetJobID())
	tasks := im.GetTaskBy(ctx)
	assert.Equal(t, 2, len(tasks))
	tasks = im.GetTaskBy(ctx, WithType(PreImportTaskType))
	assert.Equal(t, 1, len(tasks))
	assert.Equal(t, int64(1), tasks[0].GetTaskID())
	tasks = im.GetTaskBy(ctx, WithType(ImportTaskType))
	assert.Equal(t, 1, len(tasks))
	assert.Equal(t, int64(2), tasks[0].GetTaskID())

	// new meta failed
	mockErr := errors.New("mock error")
	catalog = mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return([]*datapb.PreImportTask{{TaskID: 1}}, mockErr)
	_, err = NewImportMeta(ctx, catalog, nil, nil)
	assert.Error(t, err)

	catalog = mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return([]*datapb.ImportTaskV2{{TaskID: 2}}, mockErr)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return([]*datapb.PreImportTask{{TaskID: 1}}, nil)
	_, err = NewImportMeta(ctx, catalog, nil, nil)
	assert.Error(t, err)

	catalog = mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return([]*datapb.ImportJob{{JobID: 0}}, mockErr)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return([]*datapb.PreImportTask{{TaskID: 1}}, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return([]*datapb.ImportTaskV2{{TaskID: 2}}, nil)
	_, err = NewImportMeta(ctx, catalog, nil, nil)
	assert.Error(t, err)
}

func TestImportMeta_Job(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().DropImportJob(mock.Anything, mock.Anything).Return(nil)

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	jobIDs := []int64{1000, 2000, 3000}

	for i, jobID := range jobIDs {
		channel := fmt.Sprintf("ch-%d", rand.Int63())
		var job ImportJob = &importJob{
			ImportJob: &datapb.ImportJob{
				JobID:          jobID,
				CollectionID:   rand.Int63(),
				PartitionIDs:   []int64{rand.Int63()},
				Vchannels:      []string{channel},
				ReadyVchannels: []string{channel},
				State:          internalpb.ImportJobState_Pending,
			},
		}
		err = im.AddJob(context.TODO(), job)
		assert.NoError(t, err)
		ret := im.GetJob(context.TODO(), jobID)
		assert.Equal(t, job, ret)
		jobs := im.GetJobBy(context.TODO())
		assert.Equal(t, i+1, len(jobs))

		// Add again, test idempotency
		err = im.AddJob(context.TODO(), job)
		assert.NoError(t, err)
		ret = im.GetJob(context.TODO(), jobID)
		assert.EqualValues(t, job, ret)
		jobs = im.GetJobBy(context.TODO())
		assert.Equal(t, i+1, len(jobs))
	}

	jobs := im.GetJobBy(context.TODO())
	assert.Equal(t, 3, len(jobs))

	err = im.UpdateJob(context.TODO(), jobIDs[0], UpdateJobState(internalpb.ImportJobState_Completed))
	assert.NoError(t, err)
	job0 := im.GetJob(context.TODO(), jobIDs[0])
	assert.NotNil(t, job0)
	assert.Equal(t, internalpb.ImportJobState_Completed, job0.GetState())

	err = im.UpdateJob(context.TODO(), jobIDs[1], UpdateJobState(internalpb.ImportJobState_Importing))
	assert.NoError(t, err)
	job1 := im.GetJob(context.TODO(), jobIDs[1])
	assert.NotNil(t, job1)
	assert.Equal(t, internalpb.ImportJobState_Importing, job1.GetState())

	jobs = im.GetJobBy(context.TODO(), WithJobStates(internalpb.ImportJobState_Pending))
	assert.Equal(t, 1, len(jobs))
	jobs = im.GetJobBy(context.TODO(), WithoutJobStates(internalpb.ImportJobState_Pending))
	assert.Equal(t, 2, len(jobs))
	count := im.CountJobBy(context.TODO())
	assert.Equal(t, 3, count)
	count = im.CountJobBy(context.TODO(), WithJobStates(internalpb.ImportJobState_Pending))
	assert.Equal(t, 1, count)
	count = im.CountJobBy(context.TODO(), WithoutJobStates(internalpb.ImportJobState_Pending))
	assert.Equal(t, 2, count)

	err = im.RemoveJob(context.TODO(), jobIDs[0])
	assert.NoError(t, err)
	jobs = im.GetJobBy(context.TODO())
	assert.Equal(t, 2, len(jobs))
	count = im.CountJobBy(context.TODO())
	assert.Equal(t, 2, count)
}

func TestImportMetaAddJob(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	var job ImportJob = &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:          10000,
			CollectionID:   rand.Int63(),
			PartitionIDs:   []int64{rand.Int63()},
			Vchannels:      []string{"ch-1", "ch-2"},
			ReadyVchannels: []string{"ch-1"},
			State:          internalpb.ImportJobState_Pending,
		},
	}
	err = im.AddJob(context.TODO(), job)
	assert.NoError(t, err)

	job = &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:          10000,
			CollectionID:   rand.Int63(),
			PartitionIDs:   []int64{rand.Int63()},
			Vchannels:      []string{"ch-1", "ch-2"},
			ReadyVchannels: []string{"ch-2"},
			State:          internalpb.ImportJobState_Pending,
		},
	}
	err = im.AddJob(context.TODO(), job)
	assert.NoError(t, err)

	job = im.GetJob(context.TODO(), 10000)
	assert.NotNil(t, job)
	assert.Equal(t, []string{"ch-1", "ch-2"}, job.GetVchannels())
	assert.Equal(t, []string{"ch-1", "ch-2"}, job.GetReadyVchannels())
}

func TestImportMeta_ImportTask(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().DropImportTask(mock.Anything, mock.Anything).Return(nil)

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	taskProto := &datapb.ImportTaskV2{
		JobID:        1,
		TaskID:       2,
		CollectionID: 3,
		SegmentIDs:   []int64{5, 6},
		NodeID:       7,
		State:        datapb.ImportTaskStateV2_Pending,
	}
	task1 := &importTask{}
	task1.task.Store(taskProto)
	err = im.AddTask(context.TODO(), task1)
	assert.NoError(t, err)
	err = im.AddTask(context.TODO(), task1)
	assert.NoError(t, err)
	res := im.GetTask(context.TODO(), task1.GetTaskID())
	assert.Equal(t, task1, res)

	task2 := task1.Clone()
	task2.(*importTask).task.Load().TaskID = 8
	task2.(*importTask).task.Load().State = datapb.ImportTaskStateV2_Completed
	err = im.AddTask(context.TODO(), task2)
	assert.NoError(t, err)

	tasks := im.GetTaskBy(context.TODO(), WithJob(task1.GetJobID()))
	assert.Equal(t, 2, len(tasks))
	tasks = im.GetTaskBy(context.TODO(), WithType(ImportTaskType), WithStates(datapb.ImportTaskStateV2_Completed))
	assert.Equal(t, 1, len(tasks))
	assert.Equal(t, task2.GetTaskID(), tasks[0].GetTaskID())

	err = im.UpdateTask(context.TODO(), task1.GetTaskID(), UpdateNodeID(9),
		UpdateState(datapb.ImportTaskStateV2_InProgress),
		UpdateFileStats([]*datapb.ImportFileStats{1: {
			FileSize: 100,
		}}))
	assert.NoError(t, err)
	task := im.GetTask(context.TODO(), task1.GetTaskID())
	assert.Equal(t, int64(9), task.GetNodeID())
	assert.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
	assert.Equal(t, int64(9), task1.GetNodeID())
	assert.Equal(t, datapb.ImportTaskStateV2_InProgress, task1.GetState())

	err = im.UpdateTask(context.TODO(), task1.GetTaskID(), UpdateNodeID(10),
		UpdateState(datapb.ImportTaskStateV2_Completed))
	assert.NoError(t, err)
	assert.Equal(t, int64(10), task1.GetNodeID())
	assert.Equal(t, datapb.ImportTaskStateV2_Completed, task1.GetState())

	err = im.RemoveTask(context.TODO(), task1.GetTaskID())
	assert.NoError(t, err)
	tasks = im.GetTaskBy(context.TODO())
	assert.Equal(t, 1, len(tasks))
	err = im.RemoveTask(context.TODO(), 10)
	assert.NoError(t, err)
	tasks = im.GetTaskBy(context.TODO())
	assert.Equal(t, 1, len(tasks))
}

func TestImportMeta_Task_Failed(t *testing.T) {
	mockErr := errors.New("mock err")
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(mockErr)
	catalog.EXPECT().DropImportTask(mock.Anything, mock.Anything).Return(mockErr)

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)
	im.(*importMeta).catalog = catalog

	taskProto := &datapb.ImportTaskV2{
		JobID:        1,
		TaskID:       2,
		CollectionID: 3,
		SegmentIDs:   []int64{5, 6},
		NodeID:       7,
		State:        datapb.ImportTaskStateV2_Pending,
	}
	task := &importTask{}
	task.task.Store(taskProto)

	err = im.AddTask(context.TODO(), task)
	assert.Error(t, err)
	im.(*importMeta).tasks.add(task)
	err = im.UpdateTask(context.TODO(), task.GetTaskID(), UpdateNodeID(9))
	assert.Error(t, err)
	err = im.RemoveTask(context.TODO(), task.GetTaskID())
	assert.Error(t, err)
}

func TestTaskStatsJSON(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	statsJSON := im.TaskStatsJSON(context.TODO())
	assert.Equal(t, "[]", statsJSON)

	taskProto := &datapb.ImportTaskV2{
		TaskID: 1,
	}
	task1 := &importTask{}
	task1.task.Store(taskProto)
	err = im.AddTask(context.TODO(), task1)
	assert.NoError(t, err)

	taskProto.TaskID = 2
	task2 := &importTask{}
	task2.task.Store(taskProto)
	err = im.AddTask(context.TODO(), task2)
	assert.NoError(t, err)

	err = im.UpdateTask(context.TODO(), 1, UpdateState(datapb.ImportTaskStateV2_Completed))
	assert.NoError(t, err)

	statsJSON = im.TaskStatsJSON(context.TODO())
	var tasks []*metricsinfo.ImportTask
	err = json.Unmarshal([]byte(statsJSON), &tasks)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(tasks))

	taskMeta := im.(*importMeta).tasks
	taskMeta.remove(1)
	assert.Nil(t, taskMeta.get(1))
	assert.NotNil(t, taskMeta.get(2))
	assert.Equal(t, 2, len(taskMeta.listTaskStats()))
}
