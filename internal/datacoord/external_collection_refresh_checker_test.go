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
	checker := newRefreshChecker(ctx, refreshMeta, closeChan, nil, nil, nil, nil)
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
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

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

	// Regression: when the checker's tryTimeoutJob races with the eager
	// path and observes a stale InProgress snapshot that a concurrent
	// task-success path has already transitioned to Finished, the
	// UpdateJobState terminal guard silently returns applied=false. In
	// that case tryTimeoutJob MUST NOT fire onJobFailed — firing it would
	// poison the manager's notifiedJobs dedup map and cause the eager
	// path's later handleJobFinished to short-circuit, so schemaUpdater
	// would never be called and the external collection would silently
	// never pick up its refreshed schema.
	t.Run("timeout_guard_skip_does_not_fire_onJobFailed", func(t *testing.T) {
		timeout := Params.DataCoordCfg.ExternalCollectionJobTimeout.GetAsDuration(time.Second)
		oldStartTime := time.Now().Add(-timeout - time.Hour).UnixMilli()

		catalog := &stubCatalog{}
		// Snapshot claims InProgress so tryTimeoutJob enters the timeout
		// branch, but the underlying meta entry is already Finished —
		// simulating the race where aggregateJobState already transitioned
		// the job between GetAllJobs() and tryTimeoutJob.
		snapshotJob := &datapb.ExternalCollectionRefreshJob{
			JobId:        77,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateInProgress,
			StartTime:    oldStartTime,
		}
		committedJob := &datapb.ExternalCollectionRefreshJob{
			JobId:        77,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateFinished,
			Progress:     100,
			StartTime:    oldStartTime,
			EndTime:      time.Now().UnixMilli(),
		}
		jobs := []*datapb.ExternalCollectionRefreshJob{committedJob}

		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshJob")).Return(nil).Build()
		defer mockSaveJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})

		var failedCalls []int64
		onFailed := func(jobID int64) { failedCalls = append(failedCalls, jobID) }
		checker := newRefreshChecker(ctx, meta, closeChan, nil, onFailed, nil, nil)

		// Feed tryTimeoutJob the STALE InProgress snapshot, not the
		// committed Finished entry.
		checker.tryTimeoutJob(snapshotJob)

		// The terminal-state guard inside UpdateJobState must have kept
		// the committed state Finished, AND tryTimeoutJob must NOT have
		// fired onJobFailed, since the path that actually owns the
		// transition (the eager Finished path) is responsible for any
		// per-job callback.
		assert.Empty(t, failedCalls, "tryTimeoutJob must not fire onJobFailed when the terminal guard skipped the write")
		assert.Equal(t, indexpb.JobState_JobStateFinished, meta.GetJob(77).GetState(), "committed state must remain Finished")
	})

	// Regression for #48626 Failed-path cleanup: tryTimeoutJob must fire
	// onJobFailed so the per-job explore temp dir gets reclaimed without
	// waiting for the retention-gated GC path.
	t.Run("timeout_fires_onJobFailed", func(t *testing.T) {
		timeout := Params.DataCoordCfg.ExternalCollectionJobTimeout.GetAsDuration(time.Second)
		oldStartTime := time.Now().Add(-timeout - time.Hour).UnixMilli()

		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 99, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, StartTime: oldStartTime},
		}

		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshJob")).Return(nil).Build()
		defer mockSaveJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})

		var failedCalls []int64
		onFailed := func(jobID int64) { failedCalls = append(failedCalls, jobID) }
		checker := newRefreshChecker(ctx, meta, closeChan, nil, onFailed, nil, nil)

		job := meta.GetJob(99)
		checker.tryTimeoutJob(job)

		assert.Equal(t, []int64{99}, failedCalls, "timeout path must fire onJobFailed with the jobID")
		assert.Equal(t, indexpb.JobState_JobStateFailed, meta.GetJob(99).GetState())
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
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

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
	checker := newRefreshChecker(ctx, refreshMeta, closeChan, nil, nil, nil, nil)

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
	checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

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
	checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

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
	checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

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
	checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

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

func TestExternalCollectionRefreshChecker_OnJobFinishedCallback(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	t.Run("callback_called_on_finished", func(t *testing.T) {
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

		callbackCalled := false
		var callbackJob *datapb.ExternalCollectionRefreshJob
		onFinished := func(_ context.Context, job *datapb.ExternalCollectionRefreshJob) {
			callbackCalled = true
			callbackJob = job
		}
		checker := newRefreshChecker(ctx, meta, closeChan, onFinished, nil, nil, nil)

		// Drive a full processing pass: aggregateJobState transitions the
		// job to Finished, then ensureJobFinishedNotified fires the callback.
		checker.processJobs()

		// Callback should have been called
		assert.True(t, callbackCalled, "onJobFinished callback should be called when job transitions to Finished")
		assert.NotNil(t, callbackJob)
		assert.Equal(t, int64(1), callbackJob.GetJobId())

		// Job state should be Finished
		updatedJob := meta.GetJob(1)
		assert.Equal(t, indexpb.JobState_JobStateFinished, updatedJob.GetState())
	})

	t.Run("callback_not_called_on_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 50},
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

		callbackCalled := false
		onFinished := func(_ context.Context, _ *datapb.ExternalCollectionRefreshJob) {
			callbackCalled = true
		}
		checker := newRefreshChecker(ctx, meta, closeChan, onFinished, nil, nil, nil)

		checker.processJobs()

		// Callback should NOT have been called for failed state
		assert.False(t, callbackCalled, "onJobFinished callback should NOT be called when job transitions to Failed")

		// Job state should be Failed
		updatedJob := meta.GetJob(1)
		assert.Equal(t, indexpb.JobState_JobStateFailed, updatedJob.GetState())
	})

	t.Run("callback_not_called_on_progress_only", func(t *testing.T) {
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

		callbackCalled := false
		onFinished := func(_ context.Context, _ *datapb.ExternalCollectionRefreshJob) {
			callbackCalled = true
		}
		checker := newRefreshChecker(ctx, meta, closeChan, onFinished, nil, nil, nil)

		checker.processJobs()

		// Callback should NOT have been called for progress-only update
		assert.False(t, callbackCalled, "onJobFinished callback should NOT be called for progress-only updates")
	})

	t.Run("nil_callback_no_panic", func(t *testing.T) {
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

		// nil onJobFinished - should not panic
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

		assert.NotPanics(t, func() {
			checker.processJobs()
		})

		// Job should still transition to Finished
		updatedJob := meta.GetJob(1)
		assert.Equal(t, indexpb.JobState_JobStateFinished, updatedJob.GetState())
	})

	// Regression: the Failed transition path must fire onJobFailed so the
	// manager can reclaim the per-job explore temp dir immediately instead
	// of waiting 24h for the retention-gated GC path.
	t.Run("onJobFailed_fired_on_aggregate_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 42, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 40},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 42, State: indexpb.JobState_JobStateFailed, Progress: 40, FailReason: "boom"},
		}

		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshJob")).Return(nil).Build()
		defer mockSaveJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})

		var failedJobs []int64
		onFailed := func(jobID int64) {
			failedJobs = append(failedJobs, jobID)
		}
		checker := newRefreshChecker(ctx, meta, closeChan, nil, onFailed, nil, nil)

		checker.processJobs()

		assert.Equal(t, []int64{42}, failedJobs, "onJobFailed must fire exactly once with the transitioning jobID")
		assert.Equal(t, indexpb.JobState_JobStateFailed, meta.GetJob(42).GetState())
	})

	t.Run("onJobFailed_not_fired_on_finished", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 43, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 80},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 43, State: indexpb.JobState_JobStateFinished, Progress: 100},
		}

		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshJob")).Return(nil).Build()
		defer mockSaveJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})

		failedCalled := false
		onFailed := func(_ int64) { failedCalled = true }
		checker := newRefreshChecker(ctx, meta, closeChan, nil, onFailed, nil, nil)

		checker.processJobs()

		assert.False(t, failedCalled, "onJobFailed must NOT fire when job transitions to Finished")
	})
}

// TestExternalCollectionRefreshChecker_RunGracefulShutdown covers the closeChan exit
// path inside run() (lines 99-101).
func TestExternalCollectionRefreshChecker_RunGracefulShutdown(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	catalog := &stubCatalog{}

	mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(nil, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
	defer mockListTasks.UnPatch()

	meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
	closeChan := make(chan struct{})
	checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

	done := make(chan struct{})
	go func() {
		checker.run()
		close(done)
	}()

	// Signal shutdown immediately
	close(closeChan)

	select {
	case <-done:
		// run() returned via closeChan — success
	case <-time.After(5 * time.Second):
		t.Fatal("checker.run() did not exit after closeChan was closed")
	}
}

// TestExternalCollectionRefreshChecker_AggregateJobState_ProgressOnlyUpdateFailed
// covers the else-if branch where only progress changed but UpdateJobProgress fails (lines 224-230).
func TestExternalCollectionRefreshChecker_AggregateJobState_ProgressOnlyUpdateFailed(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	catalog := &stubCatalog{}
	// Job in InProgress with progress=10
	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 10},
	}
	// Task also in InProgress with higher progress — triggers progress-only update
	tasks := []*datapb.ExternalCollectionRefreshTask{
		{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInProgress, Progress: 50},
	}

	mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(tasks, nil).Build()
	defer mockListTasks.UnPatch()

	// Save succeeds for job state queries, but we'll mock UpdateJobProgress to fail
	mockSaveJob := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshJob")).Return(errors.New("progress save failed")).Build()
	defer mockSaveJob.UnPatch()

	meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
	closeChan := make(chan struct{})
	checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil)

	job := meta.GetJob(1)
	checker.aggregateJobState(job)

	// Progress update failed — job progress should stay at original value
	assert.Equal(t, int64(10), meta.GetJob(1).GetProgress())
}

// TestExternalCollectionRefreshChecker_OnInitJobPending verifies the lazy
// retry hook for Phase B of job submission: when a checker tick visits a
// job still in Init with no tasks, it must call onInitJobPending so the
// manager can re-run the async explore + task creation. This is the safety
// net that guarantees a transient S3 failure in the WAL ack path doesn't
// strand the job forever.
func TestExternalCollectionRefreshChecker_OnInitJobPending(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	t.Run("fires_for_init_job_without_tasks", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 42, CollectionId: 100, State: indexpb.JobState_JobStateInit, TaskIds: nil},
		}
		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})

		var gotJobID int64
		onInit := func(jobID int64) { gotJobID = jobID }
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, onInit)

		checker.processJob(meta.GetJob(42))
		assert.Equal(t, int64(42), gotJobID, "onInitJobPending should be called for Init job without tasks")
	})

	t.Run("does_not_fire_for_init_job_with_tasks", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 43, CollectionId: 100, State: indexpb.JobState_JobStateInit, TaskIds: []int64{1001}},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 43, State: indexpb.JobState_JobStateInit},
		}
		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})

		called := false
		onInit := func(jobID int64) { called = true }
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, onInit)

		checker.processJob(meta.GetJob(43))
		assert.False(t, called, "onInitJobPending must not fire once tasks exist")
	})

	t.Run("does_not_fire_for_in_progress_job", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 44, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, TaskIds: nil},
		}
		mockListJobs := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshJobs")).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock(mockey.GetMethod(catalog, "ListExternalCollectionRefreshTasks")).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})

		called := false
		onInit := func(jobID int64) { called = true }
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, onInit)

		checker.processJob(meta.GetJob(44))
		assert.False(t, called, "onInitJobPending must only fire for Init state")
	})
}
