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
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
)

func TestImportMeta_Restore(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs().Return([]*datapb.ImportJob{{JobID: 0}}, nil)
	catalog.EXPECT().ListPreImportTasks().Return([]*datapb.PreImportTask{{TaskID: 1}}, nil)
	catalog.EXPECT().ListImportTasks().Return([]*datapb.ImportTaskV2{{TaskID: 2}}, nil)

	im, err := NewImportMeta(catalog)
	assert.NoError(t, err)

	jobs := im.GetJobBy()
	assert.Equal(t, 1, len(jobs))
	assert.Equal(t, int64(0), jobs[0].GetJobID())
	tasks := im.GetTaskBy()
	assert.Equal(t, 2, len(tasks))
	tasks = im.GetTaskBy(WithType(PreImportTaskType))
	assert.Equal(t, 1, len(tasks))
	assert.Equal(t, int64(1), tasks[0].GetTaskID())
	tasks = im.GetTaskBy(WithType(ImportTaskType))
	assert.Equal(t, 1, len(tasks))
	assert.Equal(t, int64(2), tasks[0].GetTaskID())

	// new meta failed
	mockErr := errors.New("mock error")
	catalog = mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListPreImportTasks().Return([]*datapb.PreImportTask{{TaskID: 1}}, mockErr)
	_, err = NewImportMeta(catalog)
	assert.Error(t, err)

	catalog = mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportTasks().Return([]*datapb.ImportTaskV2{{TaskID: 2}}, mockErr)
	catalog.EXPECT().ListPreImportTasks().Return([]*datapb.PreImportTask{{TaskID: 1}}, nil)
	_, err = NewImportMeta(catalog)
	assert.Error(t, err)

	catalog = mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs().Return([]*datapb.ImportJob{{JobID: 0}}, mockErr)
	catalog.EXPECT().ListPreImportTasks().Return([]*datapb.PreImportTask{{TaskID: 1}}, nil)
	catalog.EXPECT().ListImportTasks().Return([]*datapb.ImportTaskV2{{TaskID: 2}}, nil)
	_, err = NewImportMeta(catalog)
	assert.Error(t, err)
}

func TestImportMeta_Job(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs().Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks().Return(nil, nil)
	catalog.EXPECT().ListImportTasks().Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything).Return(nil)
	catalog.EXPECT().DropImportJob(mock.Anything).Return(nil)

	im, err := NewImportMeta(catalog)
	assert.NoError(t, err)

	jobIDs := []int64{1000, 2000, 3000}

	for i, jobID := range jobIDs {
		var job ImportJob = &importJob{
			ImportJob: &datapb.ImportJob{
				JobID:        jobID,
				CollectionID: rand.Int63(),
				PartitionIDs: []int64{rand.Int63()},
				Vchannels:    []string{fmt.Sprintf("ch-%d", rand.Int63())},
				State:        internalpb.ImportJobState_Pending,
			},
		}
		err = im.AddJob(job)
		assert.NoError(t, err)
		ret := im.GetJob(jobID)
		assert.Equal(t, job, ret)
		jobs := im.GetJobBy()
		assert.Equal(t, i+1, len(jobs))

		// Add again, test idempotency
		err = im.AddJob(job)
		assert.NoError(t, err)
		ret = im.GetJob(jobID)
		assert.Equal(t, job, ret)
		jobs = im.GetJobBy()
		assert.Equal(t, i+1, len(jobs))
	}

	jobs := im.GetJobBy()
	assert.Equal(t, 3, len(jobs))

	err = im.UpdateJob(jobIDs[0], UpdateJobState(internalpb.ImportJobState_Completed))
	assert.NoError(t, err)
	job0 := im.GetJob(jobIDs[0])
	assert.NotNil(t, job0)
	assert.Equal(t, internalpb.ImportJobState_Completed, job0.GetState())

	err = im.UpdateJob(jobIDs[1], UpdateJobState(internalpb.ImportJobState_Importing))
	assert.NoError(t, err)
	job1 := im.GetJob(jobIDs[1])
	assert.NotNil(t, job1)
	assert.Equal(t, internalpb.ImportJobState_Importing, job1.GetState())

	jobs = im.GetJobBy(WithJobStates(internalpb.ImportJobState_Pending))
	assert.Equal(t, 1, len(jobs))
	jobs = im.GetJobBy(WithoutJobStates(internalpb.ImportJobState_Pending))
	assert.Equal(t, 2, len(jobs))
	count := im.CountJobBy()
	assert.Equal(t, 3, count)
	count = im.CountJobBy(WithJobStates(internalpb.ImportJobState_Pending))
	assert.Equal(t, 1, count)
	count = im.CountJobBy(WithoutJobStates(internalpb.ImportJobState_Pending))
	assert.Equal(t, 2, count)

	err = im.RemoveJob(jobIDs[0])
	assert.NoError(t, err)
	jobs = im.GetJobBy()
	assert.Equal(t, 2, len(jobs))
	count = im.CountJobBy()
	assert.Equal(t, 2, count)
}

func TestImportMeta_ImportTask(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs().Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks().Return(nil, nil)
	catalog.EXPECT().ListImportTasks().Return(nil, nil)
	catalog.EXPECT().SaveImportTask(mock.Anything).Return(nil)
	catalog.EXPECT().DropImportTask(mock.Anything).Return(nil)

	im, err := NewImportMeta(catalog)
	assert.NoError(t, err)

	task1 := &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			SegmentIDs:   []int64{5, 6},
			NodeID:       7,
			State:        datapb.ImportTaskStateV2_Pending,
		},
	}
	err = im.AddTask(task1)
	assert.NoError(t, err)
	err = im.AddTask(task1)
	assert.NoError(t, err)
	res := im.GetTask(task1.GetTaskID())
	assert.Equal(t, task1, res)

	task2 := task1.Clone()
	task2.(*importTask).TaskID = 8
	task2.(*importTask).State = datapb.ImportTaskStateV2_Completed
	err = im.AddTask(task2)
	assert.NoError(t, err)

	tasks := im.GetTaskBy(WithJob(task1.GetJobID()))
	assert.Equal(t, 2, len(tasks))
	tasks = im.GetTaskBy(WithType(ImportTaskType), WithStates(datapb.ImportTaskStateV2_Completed))
	assert.Equal(t, 1, len(tasks))
	assert.Equal(t, task2.GetTaskID(), tasks[0].GetTaskID())

	err = im.UpdateTask(task1.GetTaskID(), UpdateNodeID(9),
		UpdateState(datapb.ImportTaskStateV2_Failed),
		UpdateFileStats([]*datapb.ImportFileStats{1: {
			FileSize: 100,
		}}))
	assert.NoError(t, err)
	task := im.GetTask(task1.GetTaskID())
	assert.Equal(t, int64(9), task.GetNodeID())
	assert.Equal(t, datapb.ImportTaskStateV2_Failed, task.GetState())

	err = im.RemoveTask(task1.GetTaskID())
	assert.NoError(t, err)
	tasks = im.GetTaskBy()
	assert.Equal(t, 1, len(tasks))
	err = im.RemoveTask(10)
	assert.NoError(t, err)
	tasks = im.GetTaskBy()
	assert.Equal(t, 1, len(tasks))
}

func TestImportMeta_Task_Failed(t *testing.T) {
	mockErr := errors.New("mock err")
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs().Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks().Return(nil, nil)
	catalog.EXPECT().ListImportTasks().Return(nil, nil)
	catalog.EXPECT().SaveImportTask(mock.Anything).Return(mockErr)
	catalog.EXPECT().DropImportTask(mock.Anything).Return(mockErr)

	im, err := NewImportMeta(catalog)
	assert.NoError(t, err)
	im.(*importMeta).catalog = catalog

	task := &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			SegmentIDs:   []int64{5, 6},
			NodeID:       7,
			State:        datapb.ImportTaskStateV2_Pending,
		},
	}

	err = im.AddTask(task)
	assert.Error(t, err)
	im.(*importMeta).tasks[task.GetTaskID()] = task
	err = im.UpdateTask(task.GetTaskID(), UpdateNodeID(9))
	assert.Error(t, err)
	err = im.RemoveTask(task.GetTaskID())
	assert.Error(t, err)
}
