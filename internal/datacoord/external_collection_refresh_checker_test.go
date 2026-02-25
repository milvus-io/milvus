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
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// ==================== Test Functions ====================

func TestExternalCollectionRefreshChecker_NewChecker(t *testing.T) {
	ctx := context.Background()
	catalog := &stubCatalog{}

	// Mock catalog methods
	mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(nil, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
	defer mockListTasks.UnPatch()

	refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)

	closeChan := make(chan struct{})
	checker := newRefreshChecker(ctx, refreshMeta, closeChan)
	assert.NotNil(t, checker)
}

func TestExternalCollectionRefreshChecker_AggregateJobState(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	t.Run("skip_finished_job", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFinished},
		}

		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan)

		job := meta.GetJob(1)
		checker.aggregateJobState(job)
		// Should not change state for finished job
		assert.Equal(t, indexpb.JobState_JobStateFinished, meta.GetJob(1).GetState())
	})

	t.Run("skip_failed_job", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFailed},
		}

		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan)

		job := meta.GetJob(1)
		checker.aggregateJobState(job)
		// Should not change state for failed job
		assert.Equal(t, indexpb.JobState_JobStateFailed, meta.GetJob(1).GetState())
	})

	t.Run("no_tasks", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit},
		}

		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan)

		job := meta.GetJob(1)
		checker.aggregateJobState(job)
		// Should not change state if no tasks
		assert.Equal(t, indexpb.JobState_JobStateInit, meta.GetJob(1).GetState())
	})

	t.Run("update_to_in_progress", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit, Progress: 0},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInProgress, Progress: 50},
		}

		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshJob")).Return(nil).Build()
		defer mockSaveJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan)

		job := meta.GetJob(1)
		checker.aggregateJobState(job)

		// Should update to InProgress
		updatedJob := meta.GetJob(1)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, updatedJob.GetState())
		assert.Equal(t, int64(50), updatedJob.GetProgress())
	})

	t.Run("update_to_finished", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 80},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
		}

		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshJob")).Return(nil).Build()
		defer mockSaveJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan)

		job := meta.GetJob(1)
		checker.aggregateJobState(job)

		// Should update to Finished with progress 100
		updatedJob := meta.GetJob(1)
		assert.Equal(t, indexpb.JobState_JobStateFinished, updatedJob.GetState())
		assert.Equal(t, int64(100), updatedJob.GetProgress())
	})

	t.Run("update_to_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 50},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFailed, Progress: 30, FailReason: "connection timeout"},
		}

		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshJob")).Return(nil).Build()
		defer mockSaveJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan)

		job := meta.GetJob(1)
		checker.aggregateJobState(job)

		// Should update to Failed with fail reason
		updatedJob := meta.GetJob(1)
		assert.Equal(t, indexpb.JobState_JobStateFailed, updatedJob.GetState())
		assert.Equal(t, "connection timeout", updatedJob.GetFailReason())
	})

	t.Run("update_progress_only", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 30},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInProgress, Progress: 60},
		}

		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshJob")).Return(nil).Build()
		defer mockSaveJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan)

		job := meta.GetJob(1)
		checker.aggregateJobState(job)

		// Should update progress only, state remains InProgress
		updatedJob := meta.GetJob(1)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, updatedJob.GetState())
		assert.Equal(t, int64(60), updatedJob.GetProgress())
	})
}

func TestExternalCollectionRefreshChecker_TryTimeoutJob(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	t.Run("skip_no_start_time", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, StartTime: 0},
		}

		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan)

		job := meta.GetJob(1)
		checker.tryTimeoutJob(job)

		// Should not change state
		assert.Equal(t, indexpb.JobState_JobStateInProgress, meta.GetJob(1).GetState())
	})

	t.Run("job_not_timeout", func(t *testing.T) {
		now := time.Now().UnixMilli()
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, StartTime: now},
		}

		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan)

		job := meta.GetJob(1)
		checker.tryTimeoutJob(job)

		// Should not timeout recent job
		assert.Equal(t, indexpb.JobState_JobStateInProgress, meta.GetJob(1).GetState())
	})

	t.Run("job_timeout", func(t *testing.T) {
		// Set start time to be older than timeout
		timeout := Params.DataCoordCfg.ExternalCollectionJobTimeout.GetAsDuration(time.Second)
		oldStartTime := time.Now().Add(-timeout - time.Hour).UnixMilli()

		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, StartTime: oldStartTime},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInProgress},
		}

		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshJob")).Return(nil).Build()
		defer mockSaveJob.UnPatch()
		mockSaveTask := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshTask")).Return(nil).Build()
		defer mockSaveTask.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan)

		job := meta.GetJob(1)
		checker.tryTimeoutJob(job)

		// Should mark job as failed with timeout reason
		updatedJob := meta.GetJob(1)
		assert.Equal(t, indexpb.JobState_JobStateFailed, updatedJob.GetState())
		assert.Equal(t, "timeout", updatedJob.GetFailReason())

		// Task should also be marked as failed
		updatedTask := meta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateFailed, updatedTask.GetState())
		assert.Equal(t, "job timeout", updatedTask.GetFailReason())
	})
}

func TestExternalCollectionRefreshChecker_CheckGC(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	t.Run("skip_non_terminal_state", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress},
		}

		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan)

		job := meta.GetJob(1)
		checker.checkGC(job)

		// Should not GC non-terminal job
		assert.NotNil(t, meta.GetJob(1))
	})

	t.Run("skip_no_end_time", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFinished, EndTime: 0},
		}

		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan)

		job := meta.GetJob(1)
		checker.checkGC(job)

		// Should not GC job without EndTime
		assert.NotNil(t, meta.GetJob(1))
	})

	t.Run("gc_finished_job_after_retention", func(t *testing.T) {
		// Set end time to be older than retention
		retention := Params.DataCoordCfg.ExternalCollectionJobRetention.GetAsDuration(time.Second)
		oldEndTime := time.Now().Add(-retention - time.Hour).UnixMilli()

		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFinished, EndTime: oldEndTime},
		}

		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()
		mockDropJob := mockey.Mock(mockey.GetMethod(catalog, "DropExternalCollectionRefreshJob")).Return(nil).Build()
		defer mockDropJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan)

		job := meta.GetJob(1)
		checker.checkGC(job)

		// Should GC the job
		assert.Nil(t, meta.GetJob(1))
	})

	t.Run("gc_failed_job_after_retention", func(t *testing.T) {
		// Set end time to be older than retention
		retention := Params.DataCoordCfg.ExternalCollectionJobRetention.GetAsDuration(time.Second)
		oldEndTime := time.Now().Add(-retention - time.Hour).UnixMilli()

		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFailed, EndTime: oldEndTime},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFailed},
		}

		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()
		mockDropTask := mockey.Mock(mockey.GetMethod(catalog, "DropExternalCollectionRefreshTask")).Return(nil).Build()
		defer mockDropTask.UnPatch()
		mockDropJob := mockey.Mock(mockey.GetMethod(catalog, "DropExternalCollectionRefreshJob")).Return(nil).Build()
		defer mockDropJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan)

		job := meta.GetJob(1)
		checker.checkGC(job)

		// Should GC both job and tasks
		assert.Nil(t, meta.GetJob(1))
		assert.Nil(t, meta.GetTask(1001))
	})

	t.Run("not_gc_recent_job", func(t *testing.T) {
		// Set end time to be recent
		recentEndTime := time.Now().UnixMilli()

		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFinished, EndTime: recentEndTime},
		}

		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan)

		job := meta.GetJob(1)
		checker.checkGC(job)

		// Should not GC recent job
		assert.NotNil(t, meta.GetJob(1))
	})
}

func TestExternalCollectionRefreshChecker_Run(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	catalog := &stubCatalog{}

	mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(nil, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
	defer mockListTasks.UnPatch()

	refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)

	closeChan := make(chan struct{})
	checker := newRefreshChecker(ctx, refreshMeta, closeChan)

	// Run checker in goroutine and close immediately to test the run loop
	done := make(chan struct{})
	go func() {
		checker.run()
		close(done)
	}()

	// Close immediately to test exit path
	close(closeChan)

	// Wait for checker to exit
	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("checker did not exit in time")
	}
}

func TestExternalCollectionRefreshChecker_AggregateJobState_UpdateStateFailed(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	catalog := &stubCatalog{}
	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit, Progress: 0},
	}
	tasks := []*datapb.ExternalCollectionRefreshTask{
		{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInProgress, Progress: 50},
	}

	mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(tasks, nil).Build()
	defer mockListTasks.UnPatch()

	// Mock save to fail
	mockSaveJob := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshJob")).Return(errors.New("save failed")).Build()
	defer mockSaveJob.UnPatch()

	meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
	closeChan := make(chan struct{})
	checker := newRefreshChecker(ctx, meta, closeChan)

	job := meta.GetJob(1)
	checker.aggregateJobState(job)

	// State should remain Init because save failed
	assert.Equal(t, indexpb.JobState_JobStateInit, meta.GetJob(1).GetState())
}

func TestExternalCollectionRefreshChecker_AggregateJobState_FailedWithProgressUpdate(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	catalog := &stubCatalog{}
	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 10},
	}
	tasks := []*datapb.ExternalCollectionRefreshTask{
		{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFailed, Progress: 30, FailReason: "worker error"},
	}

	mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(tasks, nil).Build()
	defer mockListTasks.UnPatch()
	mockSaveJob := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshJob")).Return(nil).Build()
	defer mockSaveJob.UnPatch()

	meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
	closeChan := make(chan struct{})
	checker := newRefreshChecker(ctx, meta, closeChan)

	job := meta.GetJob(1)
	checker.aggregateJobState(job)

	// Should update to Failed with progress snapshot
	updatedJob := meta.GetJob(1)
	assert.Equal(t, indexpb.JobState_JobStateFailed, updatedJob.GetState())
	assert.Equal(t, "worker error", updatedJob.GetFailReason())
}

func TestExternalCollectionRefreshChecker_TryTimeoutJob_UpdateStateFailed(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	timeout := Params.DataCoordCfg.ExternalCollectionJobTimeout.GetAsDuration(time.Second)
	oldStartTime := time.Now().Add(-timeout - time.Hour).UnixMilli()

	catalog := &stubCatalog{}
	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, StartTime: oldStartTime},
	}

	mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
	defer mockListTasks.UnPatch()

	// Mock save to fail
	mockSaveJob := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshJob")).Return(errors.New("save failed")).Build()
	defer mockSaveJob.UnPatch()

	meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
	closeChan := make(chan struct{})
	checker := newRefreshChecker(ctx, meta, closeChan)

	job := meta.GetJob(1)
	checker.tryTimeoutJob(job)

	// State should remain InProgress because save failed
	assert.Equal(t, indexpb.JobState_JobStateInProgress, meta.GetJob(1).GetState())
}

func TestExternalCollectionRefreshChecker_CheckGC_DropJobFailed(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	retention := Params.DataCoordCfg.ExternalCollectionJobRetention.GetAsDuration(time.Second)
	oldEndTime := time.Now().Add(-retention - time.Hour).UnixMilli()

	catalog := &stubCatalog{}
	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFinished, EndTime: oldEndTime},
	}

	mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
	defer mockListTasks.UnPatch()

	// Mock drop to fail
	mockDropJob := mockey.Mock(mockey.GetMethod(catalog, "DropExternalCollectionRefreshJob")).Return(errors.New("drop failed")).Build()
	defer mockDropJob.UnPatch()

	meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
	closeChan := make(chan struct{})
	checker := newRefreshChecker(ctx, meta, closeChan)

	job := meta.GetJob(1)
	checker.checkGC(job)

	// Job should still exist because drop failed
	assert.NotNil(t, meta.GetJob(1))
}

func TestExternalCollectionRefreshChecker_LogJobStats(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	t.Run("empty_jobs", func(t *testing.T) {
		catalog := &stubCatalog{}

		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(nil, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan)

		// Should not panic on empty jobs
		checker.logJobStats(map[int64]*datapb.ExternalCollectionRefreshJob{})
	})

	t.Run("multiple_states", func(t *testing.T) {
		catalog := &stubCatalog{}

		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(nil, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan)

		jobs := map[int64]*datapb.ExternalCollectionRefreshJob{
			1: {JobId: 1, State: indexpb.JobState_JobStateInit},
			2: {JobId: 2, State: indexpb.JobState_JobStateInProgress},
			3: {JobId: 3, State: indexpb.JobState_JobStateFinished},
			4: {JobId: 4, State: indexpb.JobState_JobStateFailed},
		}

		// Should not panic and should log stats
		checker.logJobStats(jobs)
	})
}
