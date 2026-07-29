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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// ==================== Test Functions ====================

func TestExternalCollectionRefreshChecker_NewChecker(t *testing.T) {
	ctx := context.Background()
	catalog := &stubCatalog{}

	// Mock catalog methods
	mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(nil, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
	defer mockListTasks.UnPatch()

	refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)

	closeChan := make(chan struct{})
	checker := newRefreshChecker(ctx, refreshMeta, closeChan, nil, nil, nil, nil, nil)
	assert.NotNil(t, checker)
}

func TestReconcileTerminalJobTasksRetainsOwnershipUntilDropSucceeds(t *testing.T) {
	ctx := context.Background()
	catalog := &stubCatalog{
		jobs: []*datapb.ExternalCollectionRefreshJob{{
			JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFailed, TaskIds: []int64{1001},
		}},
		tasks: []*datapb.ExternalCollectionRefreshTask{{
			TaskId: 1001, JobId: 1, CollectionId: 100, NodeId: 7, State: indexpb.JobState_JobStateInProgress,
		}},
	}
	meta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)
	checker := newRefreshChecker(ctx, meta, make(chan struct{}), nil, nil, nil, nil, nil)
	checker.cluster = &stubCluster{}

	mockDropFailure := mockey.Mock((*stubCluster).DropRefreshExternalCollectionTask).
		Return(errors.New("temporary drop failure")).Build()
	checker.reconcileTerminalJobTasks(meta.GetJob(1))
	mockDropFailure.UnPatch()
	assert.Equal(t, indexpb.JobState_JobStateInProgress, meta.GetTask(1001).GetState(),
		"failed Drop must retain the persisted cleanup owner")

	mockDropSuccess := mockey.Mock((*stubCluster).DropRefreshExternalCollectionTask).Return(nil).Build()
	defer mockDropSuccess.UnPatch()
	checker.reconcileTerminalJobTasks(meta.GetJob(1))
	assert.Equal(t, indexpb.JobState_JobStateFailed, meta.GetTask(1001).GetState())
}

func TestCheckGCRetainsJobWhileWorkerCleanupIsPending(t *testing.T) {
	paramtable.Init()
	retention := Params.DataCoordCfg.ExternalCollectionJobRetention.GetAsDuration(time.Second)
	oldEndTime := time.Now().Add(-retention - time.Hour).UnixMilli()
	catalog := &stubCatalog{
		jobs: []*datapb.ExternalCollectionRefreshJob{{
			JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFailed,
			EndTime: oldEndTime, TaskIds: []int64{1001},
		}},
		tasks: []*datapb.ExternalCollectionRefreshTask{{
			TaskId: 1001, JobId: 1, CollectionId: 100, NodeId: 7,
			State: indexpb.JobState_JobStateInProgress,
		}},
	}
	meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
	require.NoError(t, err)
	checker := newRefreshChecker(context.Background(), meta, make(chan struct{}), nil, nil, nil, nil, nil)

	checker.checkGC(meta.GetJob(1))

	assert.NotNil(t, meta.GetJob(1))
	assert.NotNil(t, meta.GetTask(1001))
	assert.Empty(t, catalog.updateActions, "retention must not delete the durable worker cleanup owner")
}

func TestExternalCollectionRefreshChecker_AggregateJobState(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	t.Run("skip_finished_job", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFinished},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(committedRefreshJobsForTasks(jobs, tasks), nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(nil).Build()
		defer mockSaveJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(nil).Build()
		defer mockSaveJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(nil).Build()
		defer mockSaveJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(nil).Build()
		defer mockSaveJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

		job := meta.GetJob(1)
		checker.aggregateJobState(job)

		// Should update progress only, state remains InProgress
		updatedJob := meta.GetJob(1)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, updatedJob.GetState())
		assert.Equal(t, int64(60), updatedJob.GetProgress())
	})
}

// TestExternalCollectionRefreshChecker_AggregateJobState_StaleManifestRetry verifies
// the manifest-conflict re-drive: when the job-level apply returns the stale sentinel,
// the checker resets the job's finished tasks to Init (for re-dispatch) and leaves the
// job non-terminal — neither Finished nor Failed.
func TestExternalCollectionRefreshChecker_AggregateJobState_StaleManifestRetry(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	catalog := &stubCatalog{}
	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 80, TaskIds: []int64{1001, 1002}},
	}
	tasks := []*datapb.ExternalCollectionRefreshTask{
		{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100, ResultReady: true},
		{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100, ResultReady: true},
	}

	mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
	defer mockListTasks.UnPatch()
	mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(nil).Build()
	defer mockSaveJob.UnPatch()
	// The rebuild resets every finished task as one composite catalog write.
	var resetActions int
	mockUpdate := mockey.Mock((*stubCatalog).Update).
		To(func(_ *stubCatalog, _ context.Context, actions ...metastore.UpdateAction) error {
			resetActions += len(actions)
			return nil
		}).Build()
	defer mockUpdate.UnPatch()

	meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
	closeChan := make(chan struct{})
	applyJobInfo := func(context.Context, *datapb.ExternalCollectionRefreshJob, *datapb.ExternalCollectionRefreshJob) error {
		return errExternalRefreshStaleManifest
	}
	checker := newRefreshChecker(ctx, meta, closeChan, nil, applyJobInfo, nil, nil, nil)

	checker.aggregateJobState(meta.GetJob(1))

	// Both tasks reset in a single composite write, not one write per task.
	assert.Equal(t, 2, resetActions)

	got := meta.GetJob(1)
	assert.NotEqual(t, indexpb.JobState_JobStateFinished, got.GetState())
	assert.NotEqual(t, indexpb.JobState_JobStateFailed, got.GetState())
	// Both finished tasks are reset to Init so the scheduler re-dispatches them.
	assert.Equal(t, indexpb.JobState_JobStateInit, meta.GetTask(1001).GetState())
	assert.Equal(t, indexpb.JobState_JobStateInit, meta.GetTask(1002).GetState())
}

// TestExternalCollectionRefreshMeta_FinishJobWithApply_SkipsTerminalJob verifies the
// terminal-state guard: a concurrent path (e.g. tryTimeoutJob) may have already driven
// the job to a terminal state, and it owns the one-time side effects. The apply must not
// run at all, and the job's finished tasks must not be resurrected to Init — that would
// leave the inspector with work for a terminal job.
func TestExternalCollectionRefreshMeta_FinishJobWithApply_SkipsTerminalJob(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	catalog := &stubCatalog{}
	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFailed, Progress: 80, TaskIds: []int64{1001}},
	}
	tasks := []*datapb.ExternalCollectionRefreshTask{
		{TaskId: 1001, JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFinished, Progress: 100, ResultReady: true},
	}

	mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
	defer mockListTasks.UnPatch()

	meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)

	preApplyCalled := false
	applied, err := meta.FinishJobWithApply(1, func(*datapb.ExternalCollectionRefreshJob, *datapb.ExternalCollectionRefreshJob) error {
		preApplyCalled = true
		return errExternalRefreshStaleManifest
	})

	assert.False(t, applied)
	assert.NoError(t, err)
	assert.False(t, preApplyCalled)
	assert.Equal(t, indexpb.JobState_JobStateFinished, meta.GetTask(1001).GetState())
	assert.Equal(t, indexpb.JobState_JobStateFailed, meta.GetJob(1).GetState())
}

// TestExternalCollectionRefreshMeta_FinishJobWithApply_SkipsWhenTasksNotAllFinished
// verifies the in-lock aggregate re-derivation. A caller can reach the apply on a
// snapshot taken before a concurrent stale-manifest rebuild reset some of the job's
// tasks; re-deriving the aggregate under the lock catches that and backs off, so the
// apply never runs on a partial result set (which would drop the rebuilding tasks'
// segments) and the job is left neither Finished nor Failed.
func TestExternalCollectionRefreshMeta_FinishJobWithApply_SkipsWhenTasksNotAllFinished(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	catalog := &stubCatalog{}
	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 80, TaskIds: []int64{1001, 1002}},
	}
	tasks := []*datapb.ExternalCollectionRefreshTask{
		{TaskId: 1001, JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFinished, Progress: 100, ResultReady: true},
		// Mid-rebuild: reset to Init by a concurrent stale-manifest conflict.
		{TaskId: 1002, JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit},
	}

	mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
	defer mockListTasks.UnPatch()

	meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)

	preApplyCalled := false
	applied, err := meta.FinishJobWithApply(1, func(*datapb.ExternalCollectionRefreshJob, *datapb.ExternalCollectionRefreshJob) error {
		preApplyCalled = true
		return nil
	})

	assert.False(t, applied)
	assert.NoError(t, err)
	assert.False(t, preApplyCalled)
	got := meta.GetJob(1)
	assert.NotEqual(t, indexpb.JobState_JobStateFinished, got.GetState())
	assert.NotEqual(t, indexpb.JobState_JobStateFailed, got.GetState())
	// Both tasks untouched — the rebuild path owns them.
	assert.Equal(t, indexpb.JobState_JobStateFinished, meta.GetTask(1001).GetState())
	assert.Equal(t, indexpb.JobState_JobStateInit, meta.GetTask(1002).GetState())
}

func TestExternalCollectionRefreshChecker_TryTimeoutJob(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	t.Run("skip_no_start_time", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, StartTime: 0},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(nil).Build()
		defer mockSaveJob.UnPatch()
		mockSaveTask := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).Return(nil).Build()
		defer mockSaveTask.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

		job := meta.GetJob(1)
		checker.tryTimeoutJob(job)

		// Should mark job as failed with timeout reason
		updatedJob := meta.GetJob(1)
		assert.Equal(t, indexpb.JobState_JobStateFailed, updatedJob.GetState())
		assert.Equal(t, "timeout", updatedJob.GetFailReason())

		// tryTimeoutJob transitions the JOB only. Retiring the job's tasks is a
		// terminal-job invariant owned by reconcileTerminalJobTasks, which
		// processJob runs on this and every later tick.
		assert.Equal(t, indexpb.JobState_JobStateInProgress, meta.GetTask(1001).GetState())

		checker.reconcileTerminalJobTasks(meta.GetJob(1))
		updatedTask := meta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateFailed, updatedTask.GetState())
		assert.Equal(t, "owning job reached JobStateFailed: timeout", updatedTask.GetFailReason())
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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(nil).Build()
		defer mockSaveJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})

		var failedCalls []int64
		onFailed := func(jobID int64) { failedCalls = append(failedCalls, jobID) }
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, onFailed, nil, nil)

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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(nil).Build()
		defer mockSaveJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})

		var failedCalls []int64
		onFailed := func(jobID int64) { failedCalls = append(failedCalls, jobID) }
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, onFailed, nil, nil)

		job := meta.GetJob(99)
		checker.tryTimeoutJob(job)

		assert.Equal(t, []int64{99}, failedCalls, "timeout path must fire onJobFailed with the jobID")
		assert.Equal(t, indexpb.JobState_JobStateFailed, meta.GetJob(99).GetState())
	})
}

// TestExternalCollectionRefreshChecker_ReconcileTerminalJobTasks covers the
// invariant "a terminal job owns no dispatchable task". It is a per-tick
// reconcile rather than a one-shot cleanup, which is what lets the inspector
// dispatch on task state alone without joining the job table.
func TestExternalCollectionRefreshChecker_ReconcileTerminalJobTasks(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	// newChecker wires a meta over the given jobs/tasks. saveTaskErr, when set,
	// makes every task write fail so the retry-next-tick property is testable.
	newChecker := func(t *testing.T,
		jobs []*datapb.ExternalCollectionRefreshJob,
		tasks []*datapb.ExternalCollectionRefreshTask,
	) (*externalCollectionRefreshChecker, *externalCollectionRefreshMeta) {
		t.Helper()
		catalog := &stubCatalog{}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		t.Cleanup(func() { mockListJobs.UnPatch() })
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		t.Cleanup(func() { mockListTasks.UnPatch() })

		meta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.NoError(t, err)
		return newRefreshChecker(ctx, meta, make(chan struct{}), nil, nil, nil, nil, nil), meta
	}

	t.Run("non_terminal_job_is_untouched", func(t *testing.T) {
		checker, meta := newChecker(t,
			[]*datapb.ExternalCollectionRefreshJob{
				{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress},
			},
			[]*datapb.ExternalCollectionRefreshTask{
				{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInit},
			})

		checker.reconcileTerminalJobTasks(meta.GetJob(1))

		assert.Equal(t, indexpb.JobState_JobStateInit, meta.GetTask(1001).GetState())
	})

	t.Run("retires_every_active_state_and_leaves_terminal_ones", func(t *testing.T) {
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).Return(nil).Build()
		defer mockSave.UnPatch()

		checker, meta := newChecker(t,
			[]*datapb.ExternalCollectionRefreshJob{
				{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFailed, FailReason: "timeout"},
			},
			[]*datapb.ExternalCollectionRefreshTask{
				{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInit},
				{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateRetry},
				{TaskId: 1003, JobId: 1, State: indexpb.JobState_JobStateInProgress},
				{TaskId: 1004, JobId: 1, State: indexpb.JobState_JobStateFinished},
				{TaskId: 1005, JobId: 1, State: indexpb.JobState_JobStateFailed, FailReason: "original cause"},
			})

		checker.reconcileTerminalJobTasks(meta.GetJob(1))

		for _, taskID := range []int64{1001, 1002, 1003} {
			retired := meta.GetTask(taskID)
			assert.Equal(t, indexpb.JobState_JobStateFailed, retired.GetState(), "taskID=%d", taskID)
			// The job's own fail reason is carried through, so the retired task
			// says WHY rather than just that it was retired.
			assert.Equal(t, "owning job reached JobStateFailed: timeout", retired.GetFailReason(), "taskID=%d", taskID)
		}
		// Already-terminal tasks keep their own outcome and reason.
		assert.Equal(t, indexpb.JobState_JobStateFinished, meta.GetTask(1004).GetState())
		assert.Equal(t, "original cause", meta.GetTask(1005).GetFailReason())
	})

	t.Run("other_jobs_tasks_are_untouched", func(t *testing.T) {
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).Return(nil).Build()
		defer mockSave.UnPatch()

		checker, meta := newChecker(t,
			[]*datapb.ExternalCollectionRefreshJob{
				{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFailed},
				{JobId: 2, CollectionId: 200, State: indexpb.JobState_JobStateInProgress},
			},
			[]*datapb.ExternalCollectionRefreshTask{
				{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInit},
				{TaskId: 2001, JobId: 2, State: indexpb.JobState_JobStateInit},
			})

		checker.reconcileTerminalJobTasks(meta.GetJob(1))

		assert.Equal(t, indexpb.JobState_JobStateFailed, meta.GetTask(1001).GetState())
		assert.Equal(t, indexpb.JobState_JobStateInit, meta.GetTask(2001).GetState())
	})

	t.Run("converges_after_a_failed_write", func(t *testing.T) {
		// The property a one-shot cleanup cannot provide: a task write that fails
		// once is retried by the next tick instead of stranding the task until GC.
		checker, meta := newChecker(t,
			[]*datapb.ExternalCollectionRefreshJob{
				{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFailed, FailReason: "timeout"},
			},
			[]*datapb.ExternalCollectionRefreshTask{
				{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInit},
			})

		failing := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).Return(errors.New("etcd unavailable")).Build()
		checker.reconcileTerminalJobTasks(meta.GetJob(1))
		failing.UnPatch()

		assert.Equal(t, indexpb.JobState_JobStateInit, meta.GetTask(1001).GetState(),
			"a failed write must leave the task untouched, not half-applied")

		ok := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).Return(nil).Build()
		defer ok.UnPatch()
		checker.reconcileTerminalJobTasks(meta.GetJob(1))

		assert.Equal(t, indexpb.JobState_JobStateFailed, meta.GetTask(1001).GetState())
	})

	t.Run("stale_manifest_reset_under_a_timed_out_job_converges", func(t *testing.T) {
		// End-to-end shape of the race this replaced: resetFinishedTasksLocked
		// returns a Finished task to Init while a concurrent timeout drives the
		// job terminal. The old one-shot loop in tryTimeoutJob had already run by
		// then, so the task was stranded and the inspector re-dispatched it every
		// tick. processJob now retires it on the very next pass.
		mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(nil).Build()
		defer mockSaveJob.UnPatch()
		mockSaveTask := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).Return(nil).Build()
		defer mockSaveTask.UnPatch()

		checker, meta := newChecker(t,
			[]*datapb.ExternalCollectionRefreshJob{
				{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFailed, FailReason: "timeout"},
			},
			[]*datapb.ExternalCollectionRefreshTask{
				{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInit},
			})

		checker.processJob(meta.GetJob(1))

		assert.Equal(t, indexpb.JobState_JobStateFailed, meta.GetTask(1001).GetState())
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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

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

	mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(nil, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
	defer mockListTasks.UnPatch()

	refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)

	closeChan := make(chan struct{})
	checker := newRefreshChecker(ctx, refreshMeta, closeChan, nil, nil, nil, nil, nil)

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

	mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
	defer mockListTasks.UnPatch()

	// Mock save to fail
	mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(errors.New("save failed")).Build()
	defer mockSaveJob.UnPatch()

	meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
	closeChan := make(chan struct{})
	checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

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

	mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
	defer mockListTasks.UnPatch()
	mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(nil).Build()
	defer mockSaveJob.UnPatch()

	meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
	closeChan := make(chan struct{})
	checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

	job := meta.GetJob(1)
	checker.aggregateJobState(job)

	// Should update to Failed with progress snapshot
	updatedJob := meta.GetJob(1)
	assert.Equal(t, indexpb.JobState_JobStateFailed, updatedJob.GetState())
	assert.Equal(t, "worker error", updatedJob.GetFailReason())
}

func TestExternalCollectionRefreshChecker_AggregateJobState_FinishedApplyOnce(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	catalog := &stubCatalog{}
	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 80},
	}
	tasks := []*datapb.ExternalCollectionRefreshTask{
		{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100, ResultReady: true},
		{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100, ResultReady: true},
	}

	mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
	defer mockListTasks.UnPatch()
	mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(nil).Build()
	defer mockSaveJob.UnPatch()

	meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
	closeChan := make(chan struct{})
	applyStarted := make(chan struct{})
	applyRelease := make(chan struct{})
	var applyCalls atomic.Int32
	applyJobInfo := func(context.Context, *datapb.ExternalCollectionRefreshJob, *datapb.ExternalCollectionRefreshJob) error {
		if applyCalls.Add(1) == 1 {
			close(applyStarted)
		}
		<-applyRelease
		return nil
	}
	checker := newRefreshChecker(ctx, meta, closeChan, nil, applyJobInfo, nil, nil, nil)

	firstJob := meta.GetJob(1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		checker.aggregateJobState(firstJob)
	}()

	select {
	case <-applyStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first apply did not start")
	}

	secondJob := meta.GetJob(1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		checker.aggregateJobState(secondJob)
	}()

	assert.Never(t, func() bool {
		return applyCalls.Load() > 1
	}, 50*time.Millisecond, 5*time.Millisecond)

	close(applyRelease)
	wg.Wait()

	assert.Equal(t, int32(1), applyCalls.Load())
	assert.Equal(t, indexpb.JobState_JobStateFinished, meta.GetJob(1).GetState())
}

func TestExternalCollectionRefreshChecker_AggregateJobState_PreservesTaskResultsAfterFinishedPersisted(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	catalog := &stubCatalog{}
	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 80},
	}
	tasks := []*datapb.ExternalCollectionRefreshTask{
		{
			TaskId:          1001,
			JobId:           1,
			State:           indexpb.JobState_JobStateFinished,
			Progress:        100,
			ResultReady:     true,
			KeptSegments:    []int64{1},
			UpdatedSegments: []*datapb.SegmentInfo{{ID: 10, NumOfRows: 7}},
		},
		{
			TaskId:          1002,
			JobId:           1,
			State:           indexpb.JobState_JobStateFinished,
			Progress:        100,
			ResultReady:     true,
			UpdatedSegments: []*datapb.SegmentInfo{{ID: 20, NumOfRows: 8}},
		},
	}

	mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
	defer mockListTasks.UnPatch()
	mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(nil).Build()
	defer mockSaveJob.UnPatch()
	meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
	closeChan := make(chan struct{})
	checker := newRefreshChecker(ctx, meta, closeChan, nil, func(context.Context, *datapb.ExternalCollectionRefreshJob, *datapb.ExternalCollectionRefreshJob) error {
		return nil
	}, nil, nil, nil)

	checker.aggregateJobState(meta.GetJob(1))

	assert.Equal(t, indexpb.JobState_JobStateFinished, meta.GetJob(1).GetState())
	task1 := meta.GetTask(1001)
	assert.Equal(t, []int64{1}, task1.GetKeptSegments())
	assert.Len(t, task1.GetUpdatedSegments(), 1)
	assert.Equal(t, int64(10), task1.GetUpdatedSegments()[0].GetID())
	task2 := meta.GetTask(1002)
	assert.Empty(t, task2.GetKeptSegments())
	assert.Len(t, task2.GetUpdatedSegments(), 1)
	assert.Equal(t, int64(20), task2.GetUpdatedSegments()[0].GetID())
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

	mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
	defer mockListTasks.UnPatch()

	// Mock save to fail
	mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(errors.New("save failed")).Build()
	defer mockSaveJob.UnPatch()

	meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
	closeChan := make(chan struct{})
	checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

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

	catalog := &stubCatalog{updateErr: errors.New("drop failed")}
	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFinished, EndTime: oldEndTime},
	}

	mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
	defer mockListTasks.UnPatch()

	meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
	closeChan := make(chan struct{})
	checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(nil, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

		// Should not panic on empty jobs
		checker.logJobStats(map[int64]*datapb.ExternalCollectionRefreshJob{})
	})

	t.Run("multiple_states", func(t *testing.T) {
		catalog := &stubCatalog{}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(nil, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(nil).Build()
		defer mockSaveJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})

		callbackCalled := false
		var callbackJob *datapb.ExternalCollectionRefreshJob
		onFinished := func(_ context.Context, job *datapb.ExternalCollectionRefreshJob) {
			callbackCalled = true
			callbackJob = job
		}
		checker := newRefreshChecker(ctx, meta, closeChan, onFinished, nil, nil, nil, nil)

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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(nil).Build()
		defer mockSaveJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})

		callbackCalled := false
		onFinished := func(_ context.Context, _ *datapb.ExternalCollectionRefreshJob) {
			callbackCalled = true
		}
		checker := newRefreshChecker(ctx, meta, closeChan, onFinished, nil, nil, nil, nil)

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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(nil).Build()
		defer mockSaveJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})

		callbackCalled := false
		onFinished := func(_ context.Context, _ *datapb.ExternalCollectionRefreshJob) {
			callbackCalled = true
		}
		checker := newRefreshChecker(ctx, meta, closeChan, onFinished, nil, nil, nil, nil)

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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(nil).Build()
		defer mockSaveJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})

		// nil onJobFinished - should not panic
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(nil).Build()
		defer mockSaveJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})

		var failedJobs []int64
		onFailed := func(jobID int64) {
			failedJobs = append(failedJobs, jobID)
		}
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, onFailed, nil, nil)

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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(nil).Build()
		defer mockSaveJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})

		failedCalled := false
		onFailed := func(_ int64) { failedCalled = true }
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, onFailed, nil, nil)

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

	mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(nil, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
	defer mockListTasks.UnPatch()

	meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
	closeChan := make(chan struct{})
	checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

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

	mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
	defer mockListTasks.UnPatch()

	// Save succeeds for job state queries, but we'll mock UpdateJobProgress to fail
	mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(errors.New("progress save failed")).Build()
	defer mockSaveJob.UnPatch()

	meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
	closeChan := make(chan struct{})
	checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, nil)

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
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})

		var gotJobID int64
		onInit := func(jobID int64) { gotJobID = jobID }
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, onInit)

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
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})

		called := false
		onInit := func(jobID int64) { called = true }
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, onInit)

		checker.processJob(meta.GetJob(43))
		assert.False(t, called, "onInitJobPending must not fire once tasks exist")
	})

	t.Run("does_not_fire_for_in_progress_job", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 44, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, TaskIds: nil},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})

		called := false
		onInit := func(jobID int64) { called = true }
		checker := newRefreshChecker(ctx, meta, closeChan, nil, nil, nil, nil, onInit)

		checker.processJob(meta.GetJob(44))
		assert.False(t, called, "onInitJobPending must only fire for Init state")
	})
}
