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

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

// ==================== Helper Functions for Meta Tests ====================

func createMetaTestRefreshMeta(t *testing.T, jobs []*datapb.ExternalCollectionRefreshJob, tasks []*datapb.ExternalCollectionRefreshTask) *externalCollectionRefreshMeta {
	catalog := &stubCatalog{
		jobs:  jobs,
		tasks: tasks,
	}
	meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
	assert.NoError(t, err)
	return meta
}

// ==================== Test Functions ====================

func TestExternalCollectionRefreshMeta_NewMeta(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, CollectionName: "test_collection"},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, CollectionId: 100},
		}
		catalog := &stubCatalog{jobs: jobs, tasks: tasks}
		meta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.NoError(t, err)
		assert.NotNil(t, meta)

		// Verify job loaded
		job := meta.GetJob(1)
		assert.NotNil(t, job)
		assert.Equal(t, int64(1), job.GetJobId())
		assert.Equal(t, int64(100), job.GetCollectionId())

		// Verify task loaded
		task := meta.GetTask(1001)
		assert.NotNil(t, task)
		assert.Equal(t, int64(1001), task.GetTaskId())
	})

	t.Run("list_jobs_failed", func(t *testing.T) {
		catalog := &stubCatalog{}

		// Mock ListExternalCollectionRefreshJobs to return error
		mockList := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(nil, errors.New("list jobs error")).Build()
		defer mockList.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.Error(t, err)
		assert.Nil(t, meta)
	})

	t.Run("list_tasks_failed", func(t *testing.T) {
		catalog := &stubCatalog{}

		// Mock ListExternalCollectionRefreshTasks to return error
		mockList := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, errors.New("list tasks error")).Build()
		defer mockList.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.Error(t, err)
		assert.Nil(t, meta)
	})
}

func TestExternalCollectionRefreshMeta_AddJob(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		job := &datapb.ExternalCollectionRefreshJob{
			JobId:          1,
			CollectionId:   100,
			CollectionName: "test_collection",
			State:          indexpb.JobState_JobStateInit,
			StartTime:      time.Now().UnixMilli(),
		}

		err := meta.AddJob(job)
		assert.NoError(t, err)

		// Verify job added
		got := meta.GetJob(1)
		assert.NotNil(t, got)
		assert.Equal(t, int64(1), got.GetJobId())
		assert.Equal(t, int64(100), got.GetCollectionId())
	})

	t.Run("save_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		meta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.NoError(t, err)

		// Mock SaveExternalCollectionRefreshJob to return error
		mockSave := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshJob")).Return(errors.New("save error")).Build()
		defer mockSave.UnPatch()

		job := &datapb.ExternalCollectionRefreshJob{
			JobId:        1,
			CollectionId: 100,
		}

		err = meta.AddJob(job)
		assert.Error(t, err)

		// Verify job not added
		got := meta.GetJob(1)
		assert.Nil(t, got)
	})
}

func TestExternalCollectionRefreshMeta_GetJob(t *testing.T) {
	ctx := context.Background()

	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100, CollectionName: "test_collection"},
	}
	catalog := &stubCatalog{jobs: jobs}
	meta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)

	t.Run("job_exists", func(t *testing.T) {
		job := meta.GetJob(1)
		assert.NotNil(t, job)
		assert.Equal(t, int64(1), job.GetJobId())
	})

	t.Run("job_not_exists", func(t *testing.T) {
		job := meta.GetJob(999)
		assert.Nil(t, job)
	})
}

func TestExternalCollectionRefreshMeta_GetActiveJobByCollectionID(t *testing.T) {
	now := time.Now().UnixMilli()
	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit, StartTime: now - 1000},
		{JobId: 2, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, StartTime: now},
		{JobId: 3, CollectionId: 100, State: indexpb.JobState_JobStateFinished, StartTime: now + 1000},
		{JobId: 4, CollectionId: 200, State: indexpb.JobState_JobStateInit, StartTime: now},
	}
	meta := createMetaTestRefreshMeta(t, jobs, nil)

	t.Run("has_active_job", func(t *testing.T) {
		job := meta.GetActiveJobByCollectionID(100)
		assert.NotNil(t, job)
		// Should return the newest active job (jobId=2 with InProgress state)
		assert.Equal(t, int64(2), job.GetJobId())
	})

	t.Run("no_active_job", func(t *testing.T) {
		// Collection 300 has no jobs
		job := meta.GetActiveJobByCollectionID(300)
		assert.Nil(t, job)
	})
}

func TestExternalCollectionRefreshMeta_ListJobsByCollectionID(t *testing.T) {
	now := time.Now().UnixMilli()
	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100, StartTime: now - 2000},
		{JobId: 2, CollectionId: 100, StartTime: now - 1000},
		{JobId: 3, CollectionId: 100, StartTime: now},
		{JobId: 4, CollectionId: 200, StartTime: now},
	}
	meta := createMetaTestRefreshMeta(t, jobs, nil)

	t.Run("all_jobs", func(t *testing.T) {
		jobs := meta.ListJobsByCollectionID(100)
		assert.Len(t, jobs, 3)
		// Should be sorted by StartTime descending
		assert.Equal(t, int64(3), jobs[0].GetJobId())
		assert.Equal(t, int64(2), jobs[1].GetJobId())
		assert.Equal(t, int64(1), jobs[2].GetJobId())
	})

	t.Run("no_jobs", func(t *testing.T) {
		jobs := meta.ListJobsByCollectionID(300)
		assert.Nil(t, jobs)
	})
}

func TestExternalCollectionRefreshMeta_GetAllJobs(t *testing.T) {
	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100},
		{JobId: 2, CollectionId: 200},
	}
	meta := createMetaTestRefreshMeta(t, jobs, nil)

	allJobs := meta.GetAllJobs()
	assert.Len(t, allJobs, 2)
	assert.NotNil(t, allJobs[1])
	assert.NotNil(t, allJobs[2])
}

func TestExternalCollectionRefreshMeta_UpdateJobState(t *testing.T) {
	t.Run("save_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit},
		}
		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		// Mock save to fail
		mockSave := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshJob")).Return(errors.New("save error")).Build()
		defer mockSave.UnPatch()

		err = meta.UpdateJobState(1, indexpb.JobState_JobStateInProgress, "")
		assert.Error(t, err)

		// State should remain Init
		assert.Equal(t, indexpb.JobState_JobStateInit, meta.GetJob(1).GetState())
	})

	t.Run("success", func(t *testing.T) {
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit},
		}
		meta := createMetaTestRefreshMeta(t, jobs, nil)

		err := meta.UpdateJobState(1, indexpb.JobState_JobStateInProgress, "")
		assert.NoError(t, err)

		job := meta.GetJob(1)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, job.GetState())
	})

	t.Run("finished_sets_end_time_and_progress", func(t *testing.T) {
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress},
		}
		meta := createMetaTestRefreshMeta(t, jobs, nil)

		err := meta.UpdateJobState(1, indexpb.JobState_JobStateFinished, "")
		assert.NoError(t, err)

		job := meta.GetJob(1)
		assert.Equal(t, indexpb.JobState_JobStateFinished, job.GetState())
		assert.Equal(t, int64(100), job.GetProgress())
		assert.Greater(t, job.GetEndTime(), int64(0))
	})

	t.Run("failed_sets_end_time_and_reason", func(t *testing.T) {
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress},
		}
		meta := createMetaTestRefreshMeta(t, jobs, nil)

		err := meta.UpdateJobState(1, indexpb.JobState_JobStateFailed, "timeout")
		assert.NoError(t, err)

		job := meta.GetJob(1)
		assert.Equal(t, indexpb.JobState_JobStateFailed, job.GetState())
		assert.Equal(t, "timeout", job.GetFailReason())
		assert.Greater(t, job.GetEndTime(), int64(0))
	})

	t.Run("job_not_found", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		err := meta.UpdateJobState(999, indexpb.JobState_JobStateInProgress, "")
		assert.Error(t, err)
	})
}

func TestExternalCollectionRefreshMeta_UpdateJobProgress(t *testing.T) {
	t.Run("save_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, Progress: 0},
		}
		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		mockSave := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshJob")).Return(errors.New("save error")).Build()
		defer mockSave.UnPatch()

		err = meta.UpdateJobProgress(1, 50)
		assert.Error(t, err)
		assert.Equal(t, int64(0), meta.GetJob(1).GetProgress())
	})

	t.Run("success", func(t *testing.T) {
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, Progress: 0},
		}
		meta := createMetaTestRefreshMeta(t, jobs, nil)

		err := meta.UpdateJobProgress(1, 50)
		assert.NoError(t, err)

		job := meta.GetJob(1)
		assert.Equal(t, int64(50), job.GetProgress())
	})

	t.Run("job_not_found", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		err := meta.UpdateJobProgress(999, 50)
		assert.Error(t, err)
	})
}

func TestExternalCollectionRefreshMeta_AddTaskIDToJob(t *testing.T) {
	t.Run("save_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, TaskIds: []int64{}},
		}
		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		mockSave := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshJob")).Return(errors.New("save error")).Build()
		defer mockSave.UnPatch()

		err = meta.AddTaskIDToJob(1, 1001)
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, TaskIds: []int64{}},
		}
		meta := createMetaTestRefreshMeta(t, jobs, nil)

		err := meta.AddTaskIDToJob(1, 1001)
		assert.NoError(t, err)

		job := meta.GetJob(1)
		assert.Contains(t, job.GetTaskIds(), int64(1001))
	})

	t.Run("job_not_found", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		err := meta.AddTaskIDToJob(999, 1001)
		assert.Error(t, err)
	})
}

func TestExternalCollectionRefreshMeta_DropJob(t *testing.T) {
	ctx := context.Background()

	t.Run("drop_task_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, CollectionId: 100},
		}
		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.NoError(t, err)

		mockDropTask := mockey.Mock(mockey.GetMethod(catalog, "DropExternalCollectionRefreshTask")).Return(errors.New("drop task error")).Build()
		defer mockDropTask.UnPatch()

		err = meta.DropJob(ctx, 1)
		assert.Error(t, err)

		// Job should still exist
		assert.NotNil(t, meta.GetJob(1))
	})

	t.Run("drop_job_catalog_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100},
		}
		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.NoError(t, err)

		// Mock drop job to fail
		mockDropJob := mockey.Mock(mockey.GetMethod(catalog, "DropExternalCollectionRefreshJob")).Return(errors.New("drop job error")).Build()
		defer mockDropJob.UnPatch()

		err = meta.DropJob(ctx, 1)
		assert.Error(t, err)

		// Job should still exist
		assert.NotNil(t, meta.GetJob(1))
	})

	t.Run("success_with_tasks", func(t *testing.T) {
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, CollectionId: 100},
			{TaskId: 1002, JobId: 1, CollectionId: 100},
		}
		meta := createMetaTestRefreshMeta(t, jobs, tasks)

		err := meta.DropJob(ctx, 1)
		assert.NoError(t, err)

		// Verify job and tasks removed
		assert.Nil(t, meta.GetJob(1))
		assert.Nil(t, meta.GetTask(1001))
		assert.Nil(t, meta.GetTask(1002))
	})

	t.Run("job_not_exists", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		// Should not error if job doesn't exist
		err := meta.DropJob(ctx, 999)
		assert.NoError(t, err)
	})
}

func TestExternalCollectionRefreshMeta_AddTask(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		task := &datapb.ExternalCollectionRefreshTask{
			TaskId:       1001,
			JobId:        1,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateInit,
		}

		err := meta.AddTask(task)
		assert.NoError(t, err)

		// Verify task added
		got := meta.GetTask(1001)
		assert.NotNil(t, got)
		assert.Equal(t, int64(1001), got.GetTaskId())
	})

	t.Run("save_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		// Mock SaveExternalCollectionRefreshTask to return error
		mockSave := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshTask")).Return(errors.New("save error")).Build()
		defer mockSave.UnPatch()

		task := &datapb.ExternalCollectionRefreshTask{
			TaskId: 1001,
			JobId:  1,
		}

		err = meta.AddTask(task)
		assert.Error(t, err)

		// Verify task not added
		got := meta.GetTask(1001)
		assert.Nil(t, got)
	})
}

func TestExternalCollectionRefreshMeta_GetTask(t *testing.T) {
	tasks := []*datapb.ExternalCollectionRefreshTask{
		{TaskId: 1001, JobId: 1, CollectionId: 100},
	}
	meta := createMetaTestRefreshMeta(t, nil, tasks)

	t.Run("task_exists", func(t *testing.T) {
		task := meta.GetTask(1001)
		assert.NotNil(t, task)
		assert.Equal(t, int64(1001), task.GetTaskId())
	})

	t.Run("task_not_exists", func(t *testing.T) {
		task := meta.GetTask(9999)
		assert.Nil(t, task)
	})
}

func TestExternalCollectionRefreshMeta_GetTasksByJobID(t *testing.T) {
	tasks := []*datapb.ExternalCollectionRefreshTask{
		{TaskId: 1001, JobId: 1, CollectionId: 100},
		{TaskId: 1002, JobId: 1, CollectionId: 100},
		{TaskId: 2001, JobId: 2, CollectionId: 200},
	}
	meta := createMetaTestRefreshMeta(t, nil, tasks)

	t.Run("has_tasks", func(t *testing.T) {
		tasks := meta.GetTasksByJobID(1)
		assert.Len(t, tasks, 2)
	})

	t.Run("no_tasks", func(t *testing.T) {
		tasks := meta.GetTasksByJobID(999)
		assert.Nil(t, tasks)
	})
}

func TestExternalCollectionRefreshMeta_GetAllTasks(t *testing.T) {
	tasks := []*datapb.ExternalCollectionRefreshTask{
		{TaskId: 1001, JobId: 1},
		{TaskId: 1002, JobId: 1},
		{TaskId: 2001, JobId: 2},
	}
	meta := createMetaTestRefreshMeta(t, nil, tasks)

	allTasks := meta.GetAllTasks()
	assert.Len(t, allTasks, 3)
}

func TestExternalCollectionRefreshMeta_GetTaskState(t *testing.T) {
	tasks := []*datapb.ExternalCollectionRefreshTask{
		{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInProgress},
	}
	meta := createMetaTestRefreshMeta(t, nil, tasks)

	t.Run("task_exists", func(t *testing.T) {
		state := meta.GetTaskState(1001)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, state)
	})

	t.Run("task_not_exists", func(t *testing.T) {
		state := meta.GetTaskState(9999)
		assert.Equal(t, indexpb.JobState_JobStateNone, state)
	})
}

func TestExternalCollectionRefreshMeta_UpdateTaskState(t *testing.T) {
	t.Run("save_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInit},
		}
		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(nil, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		mockSave := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshTask")).Return(errors.New("save error")).Build()
		defer mockSave.UnPatch()

		err = meta.UpdateTaskState(1001, indexpb.JobState_JobStateInProgress, "")
		assert.Error(t, err)
		assert.Equal(t, indexpb.JobState_JobStateInit, meta.GetTask(1001).GetState())
	})

	t.Run("success", func(t *testing.T) {
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInit},
		}
		meta := createMetaTestRefreshMeta(t, nil, tasks)

		err := meta.UpdateTaskState(1001, indexpb.JobState_JobStateInProgress, "")
		assert.NoError(t, err)

		task := meta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, task.GetState())
	})

	t.Run("failed_state", func(t *testing.T) {
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInProgress},
		}
		meta := createMetaTestRefreshMeta(t, nil, tasks)

		err := meta.UpdateTaskState(1001, indexpb.JobState_JobStateFailed, "connection timeout")
		assert.NoError(t, err)

		task := meta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateFailed, task.GetState())
		assert.Equal(t, "connection timeout", task.GetFailReason())
	})

	t.Run("task_not_found", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		err := meta.UpdateTaskState(9999, indexpb.JobState_JobStateInProgress, "")
		assert.Error(t, err)
	})
}

func TestExternalCollectionRefreshMeta_UpdateTaskProgress(t *testing.T) {
	t.Run("save_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, Progress: 0},
		}
		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(nil, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		mockSave := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshTask")).Return(errors.New("save error")).Build()
		defer mockSave.UnPatch()

		err = meta.UpdateTaskProgress(1001, 50)
		assert.Error(t, err)
		assert.Equal(t, int64(0), meta.GetTask(1001).GetProgress())
	})

	t.Run("success", func(t *testing.T) {
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, Progress: 0},
		}
		meta := createMetaTestRefreshMeta(t, nil, tasks)

		err := meta.UpdateTaskProgress(1001, 50)
		assert.NoError(t, err)

		task := meta.GetTask(1001)
		assert.Equal(t, int64(50), task.GetProgress())
	})

	t.Run("task_not_found", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		err := meta.UpdateTaskProgress(9999, 50)
		assert.Error(t, err)
	})
}

func TestExternalCollectionRefreshMeta_UpdateTaskVersion(t *testing.T) {
	t.Run("save_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, Version: 0, NodeId: 0},
		}
		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(nil, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		mockSave := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshTask")).Return(errors.New("save error")).Build()
		defer mockSave.UnPatch()

		err = meta.UpdateTaskVersion(1001, 10)
		assert.Error(t, err)
		// Version and NodeId should remain unchanged
		task := meta.GetTask(1001)
		assert.Equal(t, int64(0), task.GetVersion())
		assert.Equal(t, int64(0), task.GetNodeId())
	})

	t.Run("success", func(t *testing.T) {
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, Version: 0, NodeId: 0},
		}
		meta := createMetaTestRefreshMeta(t, nil, tasks)

		err := meta.UpdateTaskVersion(1001, 10)
		assert.NoError(t, err)

		task := meta.GetTask(1001)
		assert.Equal(t, int64(1), task.GetVersion())
		assert.Equal(t, int64(10), task.GetNodeId())
	})

	t.Run("task_not_found", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		err := meta.UpdateTaskVersion(9999, 10)
		assert.Error(t, err)
	})
}

func TestExternalCollectionRefreshMeta_DropTask(t *testing.T) {
	ctx := context.Background()

	t.Run("drop_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1},
		}
		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(nil, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.NoError(t, err)

		mockDrop := mockey.Mock(mockey.GetMethod(catalog, "DropExternalCollectionRefreshTask")).Return(errors.New("drop error")).Build()
		defer mockDrop.UnPatch()

		err = meta.DropTask(ctx, 1001)
		assert.Error(t, err)

		// Task should still exist
		assert.NotNil(t, meta.GetTask(1001))
	})

	t.Run("success", func(t *testing.T) {
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1},
		}
		meta := createMetaTestRefreshMeta(t, nil, tasks)

		err := meta.DropTask(ctx, 1001)
		assert.NoError(t, err)

		// Verify task removed
		assert.Nil(t, meta.GetTask(1001))
	})

	t.Run("task_not_exists", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		// Should not error if task doesn't exist
		err := meta.DropTask(ctx, 9999)
		assert.NoError(t, err)
	})
}

func TestExternalCollectionRefreshMeta_AggregateJobStateFromTasks(t *testing.T) {
	testCases := []struct {
		name             string
		tasks            []*datapb.ExternalCollectionRefreshTask
		expectedState    indexpb.JobState
		expectedProgress int64
	}{
		{
			name:             "no_tasks",
			tasks:            nil,
			expectedState:    indexpb.JobState_JobStateNone,
			expectedProgress: 0,
		},
		{
			name: "all_init",
			tasks: []*datapb.ExternalCollectionRefreshTask{
				{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInit, Progress: 0},
				{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateInit, Progress: 0},
			},
			expectedState:    indexpb.JobState_JobStateInit,
			expectedProgress: 0,
		},
		{
			name: "all_finished",
			tasks: []*datapb.ExternalCollectionRefreshTask{
				{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
				{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
			},
			expectedState:    indexpb.JobState_JobStateFinished,
			expectedProgress: 100,
		},
		{
			name: "has_in_progress",
			tasks: []*datapb.ExternalCollectionRefreshTask{
				{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
				{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateInProgress, Progress: 50},
			},
			expectedState:    indexpb.JobState_JobStateInProgress,
			expectedProgress: 75, // (100+50)/2
		},
		{
			name: "has_failed",
			tasks: []*datapb.ExternalCollectionRefreshTask{
				{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
				{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateInProgress, Progress: 50},
				{TaskId: 1003, JobId: 1, State: indexpb.JobState_JobStateFailed, Progress: 30},
			},
			expectedState:    indexpb.JobState_JobStateFailed,
			expectedProgress: 60, // (100+50+30)/3
		},
		{
			name: "has_retry",
			tasks: []*datapb.ExternalCollectionRefreshTask{
				{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
				{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateRetry, Progress: 20},
			},
			expectedState:    indexpb.JobState_JobStateRetry,
			expectedProgress: 60, // (100+20)/2
		},
		{
			name: "init_over_finished",
			tasks: []*datapb.ExternalCollectionRefreshTask{
				{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
				{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateInit, Progress: 0},
			},
			expectedState:    indexpb.JobState_JobStateInit,
			expectedProgress: 50, // (100+0)/2
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			meta := createMetaTestRefreshMeta(t, nil, tc.tasks)

			state, progress := meta.AggregateJobStateFromTasks(1)
			assert.Equal(t, tc.expectedState, state)
			assert.Equal(t, tc.expectedProgress, progress)
		})
	}
}
