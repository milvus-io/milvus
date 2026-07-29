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
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ==================== Helper Functions for Meta Tests ====================

func createMetaTestRefreshMeta(t *testing.T, jobs []*datapb.ExternalCollectionRefreshJob, tasks []*datapb.ExternalCollectionRefreshTask) *externalCollectionRefreshMeta {
	catalog := &stubCatalog{
		jobs:  committedRefreshJobsForTasks(jobs, tasks),
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
		catalog := &stubCatalog{jobs: committedRefreshJobsForTasks(jobs, tasks), tasks: tasks}
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
		mockList := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(nil, errors.New("list jobs error")).Build()
		defer mockList.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.Error(t, err)
		assert.Nil(t, meta)
	})

	t.Run("list_tasks_failed", func(t *testing.T) {
		catalog := &stubCatalog{}

		// Mock ListExternalCollectionRefreshTasks to return error
		mockList := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, errors.New("list tasks error")).Build()
		defer mockList.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.Error(t, err)
		assert.Nil(t, meta)
	})
}

func TestExternalCollectionRefreshMeta_ReloadUsesJobTaskIDsAsCommitMarker(t *testing.T) {
	t.Run("ignores_task_prefix_when_job_marker_did_not_land", func(t *testing.T) {
		jobs := []*datapb.ExternalCollectionRefreshJob{{
			JobId:        1,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateInit,
			TaskIds:      nil,
		}}
		tasks := []*datapb.ExternalCollectionRefreshTask{{
			TaskId:       1001,
			JobId:        1,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateFinished,
			ResultReady:  true,
		}}

		meta, err := newExternalCollectionRefreshMeta(context.Background(), &stubCatalog{jobs: jobs, tasks: tasks})
		require.NoError(t, err)
		assert.Nil(t, meta.GetTask(1001))
		state, progress := meta.AggregateJobStateFromTasks(1)
		assert.Equal(t, indexpb.JobState_JobStateNone, state)
		assert.Zero(t, progress)
	})

	t.Run("never_finishes_when_a_committed_task_record_is_missing", func(t *testing.T) {
		jobs := []*datapb.ExternalCollectionRefreshJob{{
			JobId:        1,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateInProgress,
			TaskIds:      []int64{1001, 1002},
		}}
		tasks := []*datapb.ExternalCollectionRefreshTask{{
			TaskId:       1001,
			JobId:        1,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateFinished,
			ResultReady:  true,
		}}

		meta, err := newExternalCollectionRefreshMeta(context.Background(), &stubCatalog{jobs: jobs, tasks: tasks})
		require.NoError(t, err)
		state, progress := meta.AggregateJobStateFromTasks(1)
		assert.Equal(t, indexpb.JobState_JobStateInit, state)
		assert.Zero(t, progress)
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
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(errors.New("save error")).Build()
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
		{JobId: 1, CollectionId: 100, StartTime: 100},
		{JobId: 2, CollectionId: 200, StartTime: 200},
	}
	meta := createMetaTestRefreshMeta(t, jobs, nil)

	allJobs := meta.GetAllJobs()
	assert.Len(t, allJobs, 2)
	assert.NotNil(t, allJobs[1])
	assert.NotNil(t, allJobs[2])

	listedJobs := meta.ListAllJobs()
	assert.Len(t, listedJobs, 2)
	assert.Equal(t, int64(2), listedJobs[0].GetJobId())
	assert.Equal(t, int64(1), listedJobs[1].GetJobId())
}

func TestExternalCollectionRefreshMeta_UpdateJobState(t *testing.T) {
	t.Run("save_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		// Mock save to fail
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(errors.New("save error")).Build()
		defer mockSave.UnPatch()

		applied, err := meta.UpdateJobState(1, indexpb.JobState_JobStateInProgress, "")
		assert.Error(t, err)
		assert.False(t, applied)

		// State should remain Init
		assert.Equal(t, indexpb.JobState_JobStateInit, meta.GetJob(1).GetState())
	})

	t.Run("success", func(t *testing.T) {
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit},
		}
		meta := createMetaTestRefreshMeta(t, jobs, nil)

		applied, err := meta.UpdateJobState(1, indexpb.JobState_JobStateInProgress, "")
		assert.NoError(t, err)
		assert.True(t, applied)

		job := meta.GetJob(1)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, job.GetState())
	})

	t.Run("finished_sets_end_time_and_progress", func(t *testing.T) {
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress},
		}
		meta := createMetaTestRefreshMeta(t, jobs, nil)

		applied, err := meta.UpdateJobState(1, indexpb.JobState_JobStateFinished, "")
		assert.NoError(t, err)
		assert.True(t, applied)

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

		applied, err := meta.UpdateJobState(1, indexpb.JobState_JobStateFailed, "timeout")
		assert.NoError(t, err)
		assert.True(t, applied)

		job := meta.GetJob(1)
		assert.Equal(t, indexpb.JobState_JobStateFailed, job.GetState())
		assert.Equal(t, "timeout", job.GetFailReason())
		assert.Greater(t, job.GetEndTime(), int64(0))
	})

	t.Run("terminal_state_guard_skips_write", func(t *testing.T) {
		// Once a job is Finished, a follow-up UpdateJobState(Failed) must
		// NOT persist the transition and MUST return applied=false so the
		// caller can distinguish "silently skipped" from "persisted". This
		// is the signal tryTimeoutJob relies on to avoid poisoning the
		// manager's notifiedJobs dedup map during a race with the eager
		// Finished path.
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFinished, Progress: 100},
		}
		meta := createMetaTestRefreshMeta(t, jobs, nil)

		applied, err := meta.UpdateJobState(1, indexpb.JobState_JobStateFailed, "timeout")
		assert.NoError(t, err)
		assert.False(t, applied, "terminal-state guard must report applied=false")

		job := meta.GetJob(1)
		assert.Equal(t, indexpb.JobState_JobStateFinished, job.GetState(), "state must remain Finished")
	})

	t.Run("job_not_found", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		applied, err := meta.UpdateJobState(999, indexpb.JobState_JobStateInProgress, "")
		assert.Error(t, err)
		assert.False(t, applied)
	})
}

func TestExternalCollectionRefreshMeta_FinishJobWithApply(t *testing.T) {
	// Every sub-test needs a fully-finished task set: FinishJobWithApply re-derives
	// the aggregate under the lock and refuses to apply unless it reads Finished.
	finishedTasks := func() []*datapb.ExternalCollectionRefreshTask {
		return []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFinished, Progress: 100, ResultReady: true},
		}
	}

	t.Run("pre_apply_failure_marks_job_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, TaskIds: []int64{1001}},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(finishedTasks(), nil).Build()
		defer mockListTasks.UnPatch()

		var savedJob *datapb.ExternalCollectionRefreshJob
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).
			To(func(_ context.Context, job *datapb.ExternalCollectionRefreshJob) error {
				savedJob = job
				return nil
			}).Build()
		defer mockSave.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		applied, err := meta.FinishJobWithApply(1, func(*datapb.ExternalCollectionRefreshJob, *datapb.ExternalCollectionRefreshJob) error {
			return errors.New("apply failed")
		})

		assert.True(t, applied)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "apply failed")
		assert.NotNil(t, savedJob)
		assert.Equal(t, indexpb.JobState_JobStateFailed, savedJob.GetState())
		assert.Equal(t, "apply failed", savedJob.GetFailReason())
		assert.Equal(t, indexpb.JobState_JobStateFailed, meta.GetJob(1).GetState())
	})

	t.Run("pre_apply_success_save_job_failure_keeps_original_job", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, TaskIds: []int64{1001}},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(finishedTasks(), nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(errors.New("save error")).Build()
		defer mockSave.UnPatch()

		preApplyCalled := false
		applied, err := meta.FinishJobWithApply(1, func(_ *datapb.ExternalCollectionRefreshJob, finishedJob *datapb.ExternalCollectionRefreshJob) error {
			preApplyCalled = true
			return catalog.SaveExternalCollectionRefreshJob(context.Background(), finishedJob)
		})

		assert.False(t, applied)
		assert.Error(t, err)
		assert.True(t, preApplyCalled)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, meta.GetJob(1).GetState())
	})

	t.Run("pre_apply_success_commits_finished", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 80, TaskIds: []int64{1001}},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(finishedTasks(), nil).Build()
		defer mockListTasks.UnPatch()
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(nil).Build()
		defer mockSave.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		applied, err := meta.FinishJobWithApply(1, func(_ *datapb.ExternalCollectionRefreshJob, finishedJob *datapb.ExternalCollectionRefreshJob) error {
			return catalog.SaveExternalCollectionRefreshJob(context.Background(), finishedJob)
		})

		assert.True(t, applied)
		assert.NoError(t, err)
		got := meta.GetJob(1)
		assert.Equal(t, indexpb.JobState_JobStateFinished, got.GetState())
		assert.Equal(t, int64(100), got.GetProgress())
		assert.NotZero(t, got.GetEndTime())
	})

	t.Run("stale_manifest_resets_tasks_and_keeps_job_running", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 80, TaskIds: []int64{1001}},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(finishedTasks(), nil).Build()
		defer mockListTasks.UnPatch()
		mockUpdate := mockey.Mock((*stubCatalog).Update).Return(nil).Build()
		defer mockUpdate.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		applied, err := meta.FinishJobWithApply(1, func(*datapb.ExternalCollectionRefreshJob, *datapb.ExternalCollectionRefreshJob) error {
			return errExternalRefreshStaleManifest
		})

		// A manifest race is neither a completion nor a failure: nothing is owed.
		assert.False(t, applied)
		assert.NoError(t, err)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, meta.GetJob(1).GetState())

		// The finished task is rolled back for re-dispatch, with its stale result
		// payload cleared so job aggregation cannot adopt it.
		task := meta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInit, task.GetState())
		assert.Zero(t, task.GetProgress())
		assert.False(t, task.GetResultReady())
		assert.Empty(t, task.GetKeptSegments())
		assert.Empty(t, task.GetBaseManifests())
	})

	// A catalog write that failed says "this attempt did not land", not "this
	// result is wrong". Terminalizing it would discard work every worker already
	// finished over an etcd blip that the next tick would very likely survive.
	t.Run("transient_commit_failure_resets_tasks_instead_of_failing_job", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 80, TaskIds: []int64{1001}},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(finishedTasks(), nil).Build()
		defer mockListTasks.UnPatch()
		mockUpdate := mockey.Mock((*stubCatalog).Update).Return(nil).Build()
		defer mockUpdate.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		applied, err := meta.FinishJobWithApply(1, func(*datapb.ExternalCollectionRefreshJob, *datapb.ExternalCollectionRefreshJob) error {
			return errors.Mark(
				merr.WrapErrIoFailed("segment-index", errors.New("etcd unavailable")),
				errExternalRefreshTransientCommit)
		})

		assert.False(t, applied)
		assert.NoError(t, err)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, meta.GetJob(1).GetState(),
			"a transient commit failure must not terminalize the job")
		task := meta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInit, task.GetState())
		assert.False(t, task.GetResultReady())
	})

	// The counterpart: a deterministic apply failure still terminalizes, so it
	// cannot spin until the job timeout with no signal to act on.
	t.Run("deterministic_apply_failure_still_fails_job", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 80, TaskIds: []int64{1001}},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(finishedTasks(), nil).Build()
		defer mockListTasks.UnPatch()
		mockUpdate := mockey.Mock((*stubCatalog).Update).Return(nil).Build()
		defer mockUpdate.UnPatch()
		mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(nil).Build()
		defer mockSaveJob.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		applied, err := meta.FinishJobWithApply(1, func(*datapb.ExternalCollectionRefreshJob, *datapb.ExternalCollectionRefreshJob) error {
			return merr.WrapErrServiceInternalMsg("external refresh atomic commit needs 65 operations, metastore limit is 64")
		})

		assert.True(t, applied)
		assert.Error(t, err)
		assert.Equal(t, indexpb.JobState_JobStateFailed, meta.GetJob(1).GetState())
	})

	t.Run("stale_manifest_reset_write_failure_leaves_memory_intact", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, TaskIds: []int64{1001}},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(finishedTasks(), nil).Build()
		defer mockListTasks.UnPatch()
		mockUpdate := mockey.Mock((*stubCatalog).Update).Return(errors.New("etcd down")).Build()
		defer mockUpdate.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		applied, err := meta.FinishJobWithApply(1, func(*datapb.ExternalCollectionRefreshJob, *datapb.ExternalCollectionRefreshJob) error {
			return errExternalRefreshStaleManifest
		})

		assert.False(t, applied)
		assert.NoError(t, err)
		// Memory must match what is on disk, and the job stays non-terminal so a
		// later tick re-runs the apply and retries the reset.
		assert.Equal(t, indexpb.JobState_JobStateFinished, meta.GetTask(1001).GetState())
		assert.Equal(t, indexpb.JobState_JobStateInProgress, meta.GetJob(1).GetState())
	})
}

func TestExternalCollectionRefreshMeta_UpdateJobProgress(t *testing.T) {
	t.Run("save_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, Progress: 0},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(errors.New("save error")).Build()
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

func TestExternalCollectionRefreshMeta_DropJob(t *testing.T) {
	ctx := context.Background()

	t.Run("catalog_update_failed", func(t *testing.T) {
		catalog := &stubCatalog{updateErr: errors.New("update error")}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, CollectionId: 100},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(committedRefreshJobsForTasks(jobs, tasks), nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.NoError(t, err)

		err = meta.DropJob(ctx, 1)
		assert.Error(t, err)

		// A failed composite write must not desync memory from disk: job and
		// task are still present.
		assert.NotNil(t, meta.GetJob(1))
		assert.NotNil(t, meta.GetTask(1001))
	})

	t.Run("success_with_tasks", func(t *testing.T) {
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, CollectionId: 100},
			{TaskId: 1002, JobId: 1, CollectionId: 100},
		}
		catalog := &stubCatalog{jobs: committedRefreshJobsForTasks(jobs, tasks), tasks: tasks}
		meta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.NoError(t, err)

		err = meta.DropJob(ctx, 1)
		assert.NoError(t, err)

		// Verify job and tasks removed
		assert.Nil(t, meta.GetJob(1))
		assert.Nil(t, meta.GetTask(1001))
		assert.Nil(t, meta.GetTask(1002))

		// One composite catalog.Update call: a DropRefreshTask action per
		// task, followed by a single DropRefreshJob action landing last (the
		// job is the failover anchor).
		assert.Len(t, catalog.updateActions, 1)
		actions := catalog.updateActions[0]
		assert.Len(t, actions, 3)

		taskIDs := make(map[int64]bool)
		for _, action := range actions[:2] {
			assert.Equal(t, metastore.ActionDelete, action.Type)
			entry, ok := action.Entry.(metastore.RefreshTaskEntry)
			assert.True(t, ok)
			taskIDs[entry.TaskID] = true
		}
		assert.Equal(t, map[int64]bool{1001: true, 1002: true}, taskIDs)

		lastAction := actions[2]
		assert.Equal(t, metastore.ActionDelete, lastAction.Type)
		jobEntry, ok := lastAction.Entry.(metastore.RefreshJobEntry)
		assert.True(t, ok)
		assert.Equal(t, int64(1), jobEntry.JobID)
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
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).Return(errors.New("save error")).Build()
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

func TestExternalCollectionRefreshMeta_AddTasksToTerminalJobRejected(t *testing.T) {
	for _, state := range []indexpb.JobState{
		indexpb.JobState_JobStateFailed,
		indexpb.JobState_JobStateFinished,
	} {
		t.Run(state.String(), func(t *testing.T) {
			catalog := &stubCatalog{jobs: []*datapb.ExternalCollectionRefreshJob{{
				JobId: 1, CollectionId: 100, State: state,
			}}}
			meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
			require.NoError(t, err)

			err = meta.AddTasksToJob(1, []*datapb.ExternalCollectionRefreshTask{{
				TaskId: 1001, JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit,
			}})

			require.Error(t, err)
			assert.True(t, errors.Is(err, errExternalRefreshJobTerminal))
			assert.Nil(t, meta.GetTask(1001))
			assert.Empty(t, meta.GetJob(1).GetTaskIds())
			assert.Empty(t, catalog.updateActions)
		})
	}
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
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(committedRefreshJobsForTasks(nil, tasks), nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).Return(errors.New("save error")).Build()
		defer mockSave.UnPatch()

		_, err = meta.UpdateTaskState(1001, 0, indexpb.JobState_JobStateInProgress, "")
		assert.Error(t, err)
		assert.Equal(t, indexpb.JobState_JobStateInit, meta.GetTask(1001).GetState())
	})

	t.Run("success", func(t *testing.T) {
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInit},
		}
		meta := createMetaTestRefreshMeta(t, nil, tasks)

		_, err := meta.UpdateTaskState(1001, 0, indexpb.JobState_JobStateInProgress, "")
		assert.NoError(t, err)

		task := meta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, task.GetState())
	})

	t.Run("failed_state", func(t *testing.T) {
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInProgress},
		}
		meta := createMetaTestRefreshMeta(t, nil, tasks)

		_, err := meta.UpdateTaskState(1001, 0, indexpb.JobState_JobStateFailed, "connection timeout")
		assert.NoError(t, err)

		task := meta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateFailed, task.GetState())
		assert.Equal(t, "connection timeout", task.GetFailReason())
	})

	t.Run("task_not_found", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		_, err := meta.UpdateTaskState(9999, 0, indexpb.JobState_JobStateInProgress, "")
		assert.Error(t, err)
	})
}

func TestExternalCollectionRefreshMeta_UpdateTaskResult(t *testing.T) {
	t.Run("persists_result_and_clones_segments", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{
				TaskId: 1001,
				JobId:  1,
				State:  indexpb.JobState_JobStateInProgress,
				UpdatedSegments: []*datapb.SegmentInfo{{
					ID: 99,
				}},
				BaseManifests: map[int64]string{10: "base-manifest"},
			},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(committedRefreshJobsForTasks(nil, tasks), nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		var savedTask *datapb.ExternalCollectionRefreshTask
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).
			To(func(_ context.Context, task *datapb.ExternalCollectionRefreshTask) error {
				savedTask = task
				return nil
			}).Build()
		defer mockSave.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		updatedSegment := &datapb.SegmentInfo{ID: 10, NumOfRows: 7}
		_, err = meta.UpdateTaskResult(
			1001,
			0,
			indexpb.JobState_JobStateFinished,
			"",
			[]int64{1, 2},
			[]*datapb.SegmentInfo{updatedSegment},
		)
		assert.NoError(t, err)
		assert.NotNil(t, savedTask)
		assert.Equal(t, indexpb.JobState_JobStateFinished, savedTask.GetState())
		assert.Equal(t, int64(100), savedTask.GetProgress())
		assert.True(t, savedTask.GetResultReady())
		assert.Equal(t, []int64{1, 2}, savedTask.GetKeptSegments())
		assert.Len(t, savedTask.GetUpdatedSegments(), 1)
		assert.Equal(t, int64(10), savedTask.GetUpdatedSegments()[0].GetID())
		assert.Equal(t, int64(7), savedTask.GetUpdatedSegments()[0].GetNumOfRows())
		assert.Equal(t, "base-manifest", savedTask.GetBaseManifests()[10])

		updatedSegment.NumOfRows = 99
		task := meta.GetTask(1001)
		assert.Len(t, task.GetUpdatedSegments(), 1)
		assert.Equal(t, int64(7), task.GetUpdatedSegments()[0].GetNumOfRows())
		assert.Equal(t, "base-manifest", task.GetBaseManifests()[10])
	})

	t.Run("save_failed_keeps_original_task", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInProgress},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(committedRefreshJobsForTasks(nil, tasks), nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).Return(errors.New("save error")).Build()
		defer mockSave.UnPatch()

		_, err = meta.UpdateTaskResult(
			1001,
			0,
			indexpb.JobState_JobStateFinished,
			"",
			[]int64{1},
			[]*datapb.SegmentInfo{{ID: 10, NumOfRows: 7}},
		)
		assert.Error(t, err)

		task := meta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, task.GetState())
		assert.Empty(t, task.GetKeptSegments())
		assert.Empty(t, task.GetBaseManifests())
	})

	t.Run("task_not_found", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		_, err := meta.UpdateTaskResult(
			9999,
			0,
			indexpb.JobState_JobStateFinished,
			"",
			[]int64{1},
			[]*datapb.SegmentInfo{{ID: 10}},
		)
		assert.Error(t, err)
	})
}

// A failed atomic segment/Finished commit leaves every task result intact. The
// manager then persists a Failed job marker, so operators can see the failure
// without losing the payload needed for diagnosis or restart recovery.
func TestExternalCollectionRefreshMeta_FinishJobWithApply_CommitFailurePreservesTaskResults(t *testing.T) {
	catalog := &stubCatalog{}
	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, TaskIds: []int64{1001, 1002}},
	}
	tasks := []*datapb.ExternalCollectionRefreshTask{
		{
			TaskId:          1001,
			JobId:           1,
			CollectionId:    100,
			State:           indexpb.JobState_JobStateFinished,
			ResultReady:     true,
			KeptSegments:    []int64{1},
			UpdatedSegments: []*datapb.SegmentInfo{{ID: 10}},
		},
		{
			TaskId:          1002,
			JobId:           1,
			CollectionId:    100,
			State:           indexpb.JobState_JobStateFinished,
			ResultReady:     true,
			KeptSegments:    []int64{2},
			UpdatedSegments: []*datapb.SegmentInfo{{ID: 20}},
		},
	}
	mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
	defer mockListTasks.UnPatch()

	saveCalls := 0
	mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).
		To(func(_ *stubCatalog, _ context.Context, job *datapb.ExternalCollectionRefreshJob) error {
			saveCalls++
			assert.Equal(t, indexpb.JobState_JobStateFailed, job.GetState())
			return nil
		}).Build()
	defer mockSave.UnPatch()

	meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
	assert.NoError(t, err)

	applied, err := meta.FinishJobWithApply(1, func(_ *datapb.ExternalCollectionRefreshJob, _ *datapb.ExternalCollectionRefreshJob) error {
		return errors.New("etcd down")
	})

	assert.True(t, applied)
	assert.Error(t, err)
	assert.Equal(t, 1, saveCalls)

	// The atomic apply did not land; both payloads remain intact and the separate
	// failure marker is visible.
	assert.Equal(t, []int64{1}, meta.GetTask(1001).GetKeptSegments())
	assert.Len(t, meta.GetTask(1001).GetUpdatedSegments(), 1)
	assert.Equal(t, []int64{2}, meta.GetTask(1002).GetKeptSegments())
	assert.Len(t, meta.GetTask(1002).GetUpdatedSegments(), 1)
	assert.Equal(t, indexpb.JobState_JobStateFailed, meta.GetJob(1).GetState())
}

// The happy path keeps task results until retention GC. They are the recovery
// input if the Finished job write fails and do not affect a terminal job after
// the write succeeds.
func TestExternalCollectionRefreshMeta_FinishJobWithApply_PreservesResultsOnCommit(t *testing.T) {
	catalog := &stubCatalog{}
	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, TaskIds: []int64{1001}},
	}
	tasks := []*datapb.ExternalCollectionRefreshTask{
		{
			TaskId:          1001,
			JobId:           1,
			CollectionId:    100,
			State:           indexpb.JobState_JobStateFinished,
			Progress:        100,
			ResultReady:     true,
			KeptSegments:    []int64{1},
			UpdatedSegments: []*datapb.SegmentInfo{{ID: 10}},
		},
	}
	mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
	defer mockListTasks.UnPatch()
	mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(nil).Build()
	defer mockSave.UnPatch()

	meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
	assert.NoError(t, err)

	applied, err := meta.FinishJobWithApply(1, func(_ *datapb.ExternalCollectionRefreshJob, finishedJob *datapb.ExternalCollectionRefreshJob) error {
		return catalog.SaveExternalCollectionRefreshJob(context.Background(), finishedJob)
	})

	assert.True(t, applied)
	assert.NoError(t, err)
	assert.Equal(t, indexpb.JobState_JobStateFinished, meta.GetJob(1).GetState())

	task := meta.GetTask(1001)
	assert.Equal(t, []int64{1}, task.GetKeptSegments())
	assert.Len(t, task.GetUpdatedSegments(), 1)
	assert.Equal(t, int64(10), task.GetUpdatedSegments()[0].GetID())
	assert.Equal(t, indexpb.JobState_JobStateFinished, task.GetState())
	assert.Equal(t, int64(100), task.GetProgress())
}

func TestExternalCollectionRefreshMeta_UpdateTaskProgress(t *testing.T) {
	t.Run("save_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, Progress: 0},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(committedRefreshJobsForTasks(nil, tasks), nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).Return(errors.New("save error")).Build()
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
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(committedRefreshJobsForTasks(nil, tasks), nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).Return(errors.New("save error")).Build()
		defer mockSave.UnPatch()

		err = meta.UpdateTaskVersion(1001, 10, &datapb.IDRange{Begin: 100, End: 200}, "files/insert_log/1/10", map[int64]string{1: "manifest-1"})
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

		err := meta.UpdateTaskVersion(1001, 10, &datapb.IDRange{Begin: 100, End: 200}, "files/insert_log/1/10", map[int64]string{1: "manifest-1"})
		assert.NoError(t, err)

		task := meta.GetTask(1001)
		assert.Equal(t, int64(1), task.GetVersion())
		assert.Equal(t, int64(10), task.GetNodeId())
		assert.Equal(t, indexpb.JobState_JobStateInProgress, task.GetState())
		assert.Equal(t, int64(100), task.GetPreallocatedSegmentIds().GetBegin())
		assert.Equal(t, int64(200), task.GetPreallocatedSegmentIds().GetEnd())
		assert.Equal(t, "files/insert_log/1/10", task.GetTargetSegmentBase())
		assert.Equal(t, "manifest-1", task.GetBaseManifests()[1])
	})

	t.Run("task_not_found", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		err := meta.UpdateTaskVersion(9999, 10, &datapb.IDRange{Begin: 100, End: 200}, "files/insert_log/1/10", nil)
		assert.Error(t, err)
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

// TestExternalCollectionRefreshMeta_VersionFencedWrites verifies that
// attempt-scoped writes are dropped when the persisted task has been
// re-dispatched under a newer version (expectedVersion != current), while a
// matching version and the unconditional version 0 both land. This is the
// coordinator half of the attempt fence: a stale/late Query response from a
// superseded attempt must not overwrite the current attempt's state.
func TestExternalCollectionRefreshMeta_VersionFencedWrites(t *testing.T) {
	tasks := []*datapb.ExternalCollectionRefreshTask{
		{TaskId: 1001, JobId: 1, Version: 2, State: indexpb.JobState_JobStateInProgress},
	}
	meta := createMetaTestRefreshMeta(t, nil, tasks)

	// Stale result from attempt v1 is dropped (task is at v2).
	applied, err := meta.UpdateTaskResult(1001, 1, indexpb.JobState_JobStateFinished, "", []int64{9}, nil)
	assert.NoError(t, err)
	assert.False(t, applied)
	assert.Equal(t, indexpb.JobState_JobStateInProgress, meta.GetTask(1001).GetState())
	assert.False(t, meta.GetTask(1001).GetResultReady())

	// Stale state write from v1 is dropped too.
	applied, err = meta.UpdateTaskState(1001, 1, indexpb.JobState_JobStateFailed, "stale")
	assert.NoError(t, err)
	assert.False(t, applied)
	assert.Equal(t, indexpb.JobState_JobStateInProgress, meta.GetTask(1001).GetState())

	// Stale reset from v1 is dropped.
	applied, err = meta.ResetTaskForRetry(1001, 1, "stale reset")
	assert.NoError(t, err)
	assert.False(t, applied)
	assert.Equal(t, indexpb.JobState_JobStateInProgress, meta.GetTask(1001).GetState())

	// The current attempt's write (v2) lands.
	applied, err = meta.UpdateTaskResult(1001, 2, indexpb.JobState_JobStateFinished, "", []int64{9}, nil)
	assert.NoError(t, err)
	assert.True(t, applied)
	assert.Equal(t, indexpb.JobState_JobStateFinished, meta.GetTask(1001).GetState())
	assert.True(t, meta.GetTask(1001).GetResultReady())

	// Unconditional (version 0) job-scoped write always lands (e.g. timeout).
	applied, err = meta.UpdateTaskState(1001, 0, indexpb.JobState_JobStateFailed, "job timeout")
	assert.NoError(t, err)
	assert.True(t, applied)
	assert.Equal(t, indexpb.JobState_JobStateFailed, meta.GetTask(1001).GetState())
}
