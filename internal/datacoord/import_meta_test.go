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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
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

func TestHandleCommitVchannel(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Maybe()

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	jobID := int64(100)
	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:     jobID,
			State:     internalpb.ImportJobState_Committing,
			Vchannels: []string{"ch1", "ch2"},
		},
	}
	err = im.AddJob(context.TODO(), job)
	assert.NoError(t, err)

	callCount := 0
	cb := func() error { callCount++; return nil }

	// First commit of ch1 — should succeed and persist
	err = im.HandleCommitVchannel(context.TODO(), jobID, "ch1", cb)
	assert.NoError(t, err)
	assert.Equal(t, 1, callCount)
	assert.Contains(t, im.GetJob(context.TODO(), jobID).GetCommittedVchannels(), "ch1")

	// Idempotent second commit of ch1 — callback should NOT fire again
	err = im.HandleCommitVchannel(context.TODO(), jobID, "ch1", cb)
	assert.NoError(t, err)
	assert.Equal(t, 1, callCount) // still 1, not 2

	// Unknown job returns error
	err = im.HandleCommitVchannel(context.TODO(), int64(9999), "ch1", cb)
	assert.Error(t, err)
	assert.Equal(t, 1, callCount) // callback not called for missing job
}

func TestHandleCommitVchannel_BeforeUncommitted_RetryWithoutMutation(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Maybe()

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	const jobID int64 = 102
	err = im.AddJob(context.TODO(), &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:     jobID,
			State:     internalpb.ImportJobState_Importing,
			Vchannels: []string{"ch1"},
		},
	})
	assert.NoError(t, err)

	callCount := 0
	err = im.HandleCommitVchannel(context.TODO(), jobID, "ch1", func() error {
		callCount++
		return nil
	})

	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrImportSysFailed))
	assert.Equal(t, 0, callCount)
	updated := im.GetJob(context.TODO(), jobID)
	assert.Equal(t, internalpb.ImportJobState_Importing, updated.GetState())
	assert.NotContains(t, updated.GetCommittedVchannels(), "ch1")
}

func TestHandleCommitVchannel_RetryAfterUncommitted(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Maybe()

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	const jobID int64 = 103
	err = im.AddJob(context.TODO(), &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:     jobID,
			State:     internalpb.ImportJobState_Importing,
			Vchannels: []string{"ch1"},
		},
	})
	assert.NoError(t, err)

	callCount := 0
	err = im.HandleCommitVchannel(context.TODO(), jobID, "ch1", func() error {
		callCount++
		return nil
	})
	assert.Error(t, err)
	assert.Equal(t, 0, callCount)

	err = im.UpdateJob(context.TODO(), jobID, UpdateJobState(internalpb.ImportJobState_Uncommitted))
	assert.NoError(t, err)

	err = im.HandleCommitVchannel(context.TODO(), jobID, "ch1", func() error {
		callCount++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, callCount)
	updated := im.GetJob(context.TODO(), jobID)
	assert.Equal(t, internalpb.ImportJobState_Committing, updated.GetState())
	assert.Contains(t, updated.GetCommittedVchannels(), "ch1")
}

func TestHandleCommitVchannelTransitionsUncommittedToCommittingBeforeCallback(t *testing.T) {
	jobID := int64(101)
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)

	type savedJob struct {
		state          internalpb.ImportJobState
		committed      []string
		callbackCalled bool
	}
	var (
		recordSaves    bool
		callbackCalled bool
		saves          []savedJob
	)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Run(func(ctx context.Context, job *datapb.ImportJob) {
		if recordSaves && job.GetJobID() == jobID {
			saves = append(saves, savedJob{
				state:          job.GetState(),
				committed:      append([]string(nil), job.GetCommittedVchannels()...),
				callbackCalled: callbackCalled,
			})
		}
	}).Return(nil).Maybe()

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:     jobID,
			State:     internalpb.ImportJobState_Uncommitted,
			Vchannels: []string{"ch1"},
		},
	}
	err = im.AddJob(context.TODO(), job)
	assert.NoError(t, err)

	recordSaves = true
	err = im.HandleCommitVchannel(context.TODO(), jobID, "ch1", func() error {
		callbackCalled = true
		return nil
	})
	assert.NoError(t, err)
	if assert.Len(t, saves, 2) {
		assert.Equal(t, internalpb.ImportJobState_Committing, saves[0].state)
		assert.Empty(t, saves[0].committed)
		assert.False(t, saves[0].callbackCalled)
		assert.Equal(t, internalpb.ImportJobState_Committing, saves[1].state)
		assert.Contains(t, saves[1].committed, "ch1")
		assert.True(t, saves[1].callbackCalled)
	}
	updated := im.GetJob(context.TODO(), jobID)
	assert.Equal(t, internalpb.ImportJobState_Committing, updated.GetState())
	assert.Contains(t, updated.GetCommittedVchannels(), "ch1")
}

// ---------------------------------------------------------------------------
// AddTasksToJob - job + task batch persisted as one composite write
// ---------------------------------------------------------------------------

func newAddTasksToJobMeta(t *testing.T) (*mocks.DataCoordCatalog, ImportMeta) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	var job ImportJob = &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        1,
			CollectionID: 2,
			State:        internalpb.ImportJobState_Pending,
		},
	}
	assert.NoError(t, im.AddJob(context.TODO(), job))
	return catalog, im
}

func newTestPreImportTask(jobID, taskID int64) ImportTask {
	task := &preImportTask{}
	task.task.Store(&datapb.PreImportTask{JobID: jobID, TaskID: taskID, State: datapb.ImportTaskStateV2_Pending})
	return task
}

// TestImportMeta_AddTasksToJob proves the task batch and the job update are
// persisted through ONE composite catalog write, with every task save
// composed before the job save (the commit marker), and that in-memory state
// is applied only after the write succeeds.
func TestImportMeta_AddTasksToJob(t *testing.T) {
	catalog, im := newAddTasksToJobMeta(t)

	var actions []metastore.UpdateAction
	catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, acts ...metastore.UpdateAction) error {
			actions = acts
			return nil
		}).Once()

	err := im.AddTasksToJob(context.TODO(), 1,
		[]ImportTask{newTestPreImportTask(1, 100), newTestPreImportTask(1, 101)},
		UpdateJobState(internalpb.ImportJobState_PreImporting))
	assert.NoError(t, err)

	assert.Len(t, actions, 3)
	for _, action := range actions[:2] {
		entry, ok := action.Entry.(metastore.ImportTaskEntry)
		assert.True(t, ok)
		assert.Equal(t, metastore.ActionAdd, action.Type)
		assert.NotNil(t, entry.PreImportTask)
	}
	jobEntry, ok := actions[2].Entry.(metastore.ImportJobEntry)
	assert.True(t, ok)
	assert.Equal(t, metastore.ActionUpdate, actions[2].Type)
	assert.Equal(t, internalpb.ImportJobState_PreImporting, jobEntry.Job.GetState())

	assert.Len(t, im.GetTaskBy(context.TODO(), WithJob(1)), 2)
	assert.Equal(t, internalpb.ImportJobState_PreImporting, im.GetJob(context.TODO(), 1).GetState())
}

// TestImportMeta_AddTasksToJob_FailureLeavesMemoryUntouched proves a failed
// composite write applies nothing in memory - no tasks appear and the job
// stays in its previous state - while the batch's task records are rolled
// back best-effort (a failing drop is ignored, the original error surfaces).
func TestImportMeta_AddTasksToJob_FailureLeavesMemoryUntouched(t *testing.T) {
	catalog, im := newAddTasksToJobMeta(t)

	mockErr := errors.New("mock error")
	catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return(mockErr).Once()
	catalog.EXPECT().DropPreImportTask(mock.Anything, int64(100)).Return(errors.New("drop failed too")).Once()

	err := im.AddTasksToJob(context.TODO(), 1,
		[]ImportTask{newTestPreImportTask(1, 100)},
		UpdateJobState(internalpb.ImportJobState_PreImporting))
	assert.ErrorIs(t, err, mockErr)

	assert.Empty(t, im.GetTaskBy(context.TODO(), WithJob(1)))
	assert.Equal(t, internalpb.ImportJobState_Pending, im.GetJob(context.TODO(), 1).GetState())
}

// TestImportMeta_AddTasksToJob_TerminalJobNoop proves a terminal job
// (Completed/Failed) rejects the batch without any catalog write, mirroring
// UpdateJob's guard - no task may be added under a job that already ended.
func TestImportMeta_AddTasksToJob_TerminalJobNoop(t *testing.T) {
	_, im := newAddTasksToJobMeta(t)

	err := im.UpdateJob(context.TODO(), 1, UpdateJobState(internalpb.ImportJobState_Failed))
	assert.NoError(t, err)

	// no catalog.Update expectation: any composite write would fail the mock.
	err = im.AddTasksToJob(context.TODO(), 1,
		[]ImportTask{newTestPreImportTask(1, 100)},
		UpdateJobState(internalpb.ImportJobState_PreImporting))
	assert.NoError(t, err)
	assert.Empty(t, im.GetTaskBy(context.TODO(), WithJob(1)))
	assert.Equal(t, internalpb.ImportJobState_Failed, im.GetJob(context.TODO(), 1).GetState())
}

// importPartialFlushKV forces the composite import write onto the chunked
// fallback path (MaxTxnOps=1) and fails one MultiSave chunk, simulating a
// partial task flush that leaves earlier chunks persisted.
type importPartialFlushKV struct {
	*metaMemoryKV
	multiSaves       int
	failNthMultiSave int
}

func (f *importPartialFlushKV) MaxTxnOps() int { return 1 }

func (f *importPartialFlushKV) MultiSave(ctx context.Context, kvs map[string]string) error {
	f.multiSaves++
	if f.multiSaves == f.failNthMultiSave {
		return errors.New("injected partial-flush failure")
	}
	return f.metaMemoryKV.MultiSave(ctx, kvs)
}

// TestImportMeta_AddTasksToJob_PartialFlushLeavesNoOrphanTaskKeys pins the
// in-process retry path against a real catalog: a composite write that dies
// mid-flush (first task chunk persisted, second failed) leaves memory
// untouched, so the checker's next tick re-allocates FRESH task IDs and
// retries with a different key set - NOT the same composite write. The
// first-generation records must therefore be rolled back on failure, or they
// become orphans no memory-driven GC can ever reach.
func TestImportMeta_AddTasksToJob_PartialFlushLeavesNoOrphanTaskKeys(t *testing.T) {
	ctx := context.TODO()
	kv := &importPartialFlushKV{metaMemoryKV: NewMetaMemoryKV(), failNthMultiSave: 2}
	catalog := kvdatacoord.NewCatalog(kv, "", "")
	im, err := NewImportMeta(ctx, catalog, nil, nil)
	assert.NoError(t, err)

	var job ImportJob = &importJob{
		ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2, State: internalpb.ImportJobState_Pending},
	}
	assert.NoError(t, im.AddJob(ctx, job))

	// Generation 1: two tasks + the job marker against a 1-op txn limit take
	// the fallback; chunk 1 (task 1001) lands, chunk 2 (task 1002) fails.
	err = im.AddTasksToJob(ctx, 1,
		[]ImportTask{newTestPreImportTask(1, 1001), newTestPreImportTask(1, 1002)},
		UpdateJobState(internalpb.ImportJobState_PreImporting))
	assert.Error(t, err)
	assert.Equal(t, 2, kv.multiSaves)

	// The failed batch's task keys must be rolled back: the retry below will
	// not reuse them.
	keys, _, err := kv.LoadWithPrefix(ctx, kvdatacoord.PreImportTaskPrefix)
	assert.NoError(t, err)
	assert.Empty(t, keys)

	// Generation 2: the checker recomputes the full missing set and retries
	// with newly-allocated task IDs; this one succeeds.
	err = im.AddTasksToJob(ctx, 1,
		[]ImportTask{newTestPreImportTask(1, 2001), newTestPreImportTask(1, 2002)},
		UpdateJobState(internalpb.ImportJobState_PreImporting))
	assert.NoError(t, err)

	// Exactly the second generation is on disk - no first-generation orphans.
	keys, _, err = kv.LoadWithPrefix(ctx, kvdatacoord.PreImportTaskPrefix)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{
		fmt.Sprintf("%s/%d", kvdatacoord.PreImportTaskPrefix, 2001),
		fmt.Sprintf("%s/%d", kvdatacoord.PreImportTaskPrefix, 2002),
	}, keys)

	// The job marker landed with the surviving generation.
	value, err := kv.Load(ctx, fmt.Sprintf("%s/%d", kvdatacoord.ImportJobPrefix, 1))
	assert.NoError(t, err)
	savedJob := &datapb.ImportJob{}
	assert.NoError(t, proto.Unmarshal([]byte(value), savedJob))
	assert.Equal(t, internalpb.ImportJobState_PreImporting, savedJob.GetState())
}

// TestImportMeta_AddTasksToJob_AmbiguousFailureAdoptsCommittedMarker: an etcd
// write can be applied on the store while the client still gets an error
// (applied-but-timed-out, leader switch). The job marker - same txn on the
// atomic path, last chunk on the fallback - may then already reference the
// task batch; rolling the task records back would leave a PreImporting job
// with zero tasks, which a restarted checker completes EMPTY (totalRows==0
// auto-commit). AddTasksToJob must read the job record back, recognize the
// committed marker, adopt the write instead of rolling back, and report
// success.
func TestImportMeta_AddTasksToJob_AmbiguousFailureAdoptsCommittedMarker(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)
	var job ImportJob = &importJob{
		ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2, State: internalpb.ImportJobState_Pending},
	}
	assert.NoError(t, im.AddJob(context.TODO(), job))

	// The composite write COMMITS on the store, but the client sees an error.
	var committed *datapb.ImportJob
	catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, acts ...metastore.UpdateAction) error {
			committed = acts[len(acts)-1].Entry.(metastore.ImportJobEntry).Job
			return errors.New("etcdserver: request timed out")
		}).Once()
	// The read-back sees the committed marker. No DropPreImportTask
	// expectation: rolling back the committed batch would fail the mock.
	catalog.EXPECT().ListImportJobs(mock.Anything).
		RunAndReturn(func(context.Context) ([]*datapb.ImportJob, error) {
			return []*datapb.ImportJob{committed}, nil
		}).Once()

	err = im.AddTasksToJob(context.TODO(), 1,
		[]ImportTask{newTestPreImportTask(1, 100)},
		UpdateJobState(internalpb.ImportJobState_PreImporting))
	assert.NoError(t, err)

	// Memory adopted the committed write: task present, job state flipped.
	assert.Len(t, im.GetTaskBy(context.TODO(), WithJob(1)), 1)
	assert.Equal(t, internalpb.ImportJobState_PreImporting, im.GetJob(context.TODO(), 1).GetState())
}

// TestImportMeta_AddTasksToJob_ReadBackFailureSkipsRollback: when the write
// fails AND the read-back cannot determine whether the marker landed, neither
// adopting nor rolling back is safe - dropping the task records could empty a
// committed job. The records must be left alone (restart reload adopts them,
// same terminal state as a failed drop) and the original error surfaces.
func TestImportMeta_AddTasksToJob_ReadBackFailureSkipsRollback(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)
	var job ImportJob = &importJob{
		ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2, State: internalpb.ImportJobState_Pending},
	}
	assert.NoError(t, im.AddJob(context.TODO(), job))

	mockErr := errors.New("etcdserver: request timed out")
	catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return(mockErr).Once()
	// The read-back fails too. No DropPreImportTask expectation: a rollback
	// under an unknown marker state would fail the mock.
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, errors.New("read back failed")).Once()

	err = im.AddTasksToJob(context.TODO(), 1,
		[]ImportTask{newTestPreImportTask(1, 100)},
		UpdateJobState(internalpb.ImportJobState_PreImporting))
	assert.ErrorIs(t, err, mockErr)

	// Memory untouched: the next tick retries with fresh task IDs.
	assert.Empty(t, im.GetTaskBy(context.TODO(), WithJob(1)))
	assert.Equal(t, internalpb.ImportJobState_Pending, im.GetJob(context.TODO(), 1).GetState())
}
