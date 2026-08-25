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
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
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
	checker := newRefreshChecker(ctx, nil, refreshMeta, closeChan, nil, nil, nil, nil, nil)
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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

		job := meta.GetJob(1)
		checker.aggregateJobState(job)
		// Should not change state if no tasks
		assert.Equal(t, indexpb.JobState_JobStateInit, meta.GetJob(1).GetState())
	})

	t.Run("update_to_in_progress", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit, Progress: 0, TaskIds: []int64{1001}},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInProgress, Progress: 50},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()
		mockSaveJob := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(nil).Build()
		defer mockSaveJob.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 80, TaskIds: []int64{1001}},
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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 50, TaskIds: []int64{1001}},
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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 30, TaskIds: []int64{1001}},
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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, StartTime: oldStartTime, TaskIds: []int64{1001}},
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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, onFailed, nil, nil)

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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, onFailed, nil, nil)

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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		closeChan := make(chan struct{})
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFailed, EndTime: oldEndTime, TaskIds: []int64{1001}},
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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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
	checker := newRefreshChecker(ctx, nil, refreshMeta, closeChan, nil, nil, nil, nil, nil)

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
		{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit, Progress: 0, TaskIds: []int64{1001}},
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
	checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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
		{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 10, TaskIds: []int64{1001}},
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
	checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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

	meta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
	closeChan := make(chan struct{})
	applyStarted := make(chan struct{})
	applyRelease := make(chan struct{})
	var applyCalls atomic.Int32
	applyJobInfo := func(context.Context, *datapb.ExternalCollectionRefreshJob) error {
		if applyCalls.Add(1) == 1 {
			close(applyStarted)
		}
		<-applyRelease
		return nil
	}
	checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, applyJobInfo, nil, nil, nil)

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

func TestExternalCollectionRefreshChecker_AggregateJobState_ClearsTaskResultsAfterFinishedPersisted(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	catalog := &stubCatalog{}
	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 80, TaskIds: []int64{1001, 1002}},
	}
	tasks := []*datapb.ExternalCollectionRefreshTask{
		{
			TaskId:          1001,
			JobId:           1,
			State:           indexpb.JobState_JobStateFinished,
			Progress:        100,
			ResultReady:     true,
			KeptSegments:    []int64{1},
			UpdatedSegments: []*datapb.SegmentInfo{{ID: 10, CollectionID: 100, NumOfRows: 7}},
		},
		{
			TaskId:          1002,
			JobId:           1,
			State:           indexpb.JobState_JobStateFinished,
			Progress:        100,
			ResultReady:     true,
			UpdatedSegments: []*datapb.SegmentInfo{{ID: 20, CollectionID: 100, NumOfRows: 8}},
		},
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
	checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, func(context.Context, *datapb.ExternalCollectionRefreshJob) error {
		return nil
	}, nil, nil, nil)

	checker.aggregateJobState(meta.GetJob(1))

	assert.Equal(t, indexpb.JobState_JobStateFinished, meta.GetJob(1).GetState())
	task1 := meta.GetTask(1001)
	assert.Empty(t, task1.GetKeptSegments())
	assert.Empty(t, task1.GetUpdatedSegments())
	task2 := meta.GetTask(1002)
	assert.Empty(t, task2.GetKeptSegments())
	assert.Empty(t, task2.GetUpdatedSegments())
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
	checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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
	checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 80, TaskIds: []int64{1001}},
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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, onFinished, nil, nil, nil, nil)

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
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 50, TaskIds: []int64{1001}},
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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, onFinished, nil, nil, nil, nil)

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
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 30, TaskIds: []int64{1001}},
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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, onFinished, nil, nil, nil, nil)

		checker.processJobs()

		// Callback should NOT have been called for progress-only update
		assert.False(t, callbackCalled, "onJobFinished callback should NOT be called for progress-only updates")
	})

	t.Run("nil_callback_no_panic", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 80, TaskIds: []int64{1001}},
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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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
			{JobId: 42, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 40, TaskIds: []int64{1001}},
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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, onFailed, nil, nil)

		checker.processJobs()

		assert.Equal(t, []int64{42}, failedJobs, "onJobFailed must fire exactly once with the transitioning jobID")
		assert.Equal(t, indexpb.JobState_JobStateFailed, meta.GetJob(42).GetState())
	})

	t.Run("onJobFailed_not_fired_on_finished", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 43, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 80, TaskIds: []int64{1001}},
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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, onFailed, nil, nil)

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
	checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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
		{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 10, TaskIds: []int64{1001}},
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
	checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, nil)

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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, onInit)

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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, onInit)

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
		checker := newRefreshChecker(ctx, nil, meta, closeChan, nil, nil, nil, nil, onInit)

		checker.processJob(meta.GetJob(44))
		assert.False(t, called, "onInitJobPending must only fire for Init state")
	})
}

// The index wait. With refreshWaitForIndex on, a refresh applies its segments
// and publishes the refreshed source/spec exactly as it always did, then keeps
// reporting InProgress until those segments are indexed. Off, the native
// transition is untouched.
func TestExternalCollectionRefreshChecker_IndexWait(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	// The fixture models a finished ingest: one task, Finished, results ready.
	// mt carries the segments the apply would have produced.
	// debt is read through a pointer so a test can clear it mid-run: mockey
	// refuses a second mock of the same target.
	stage := func(t *testing.T, indexedSegments []int64, debt *[]int64) (
		*externalCollectionRefreshMeta, *meta, *datapb.ExternalCollectionRefreshJob,
	) {
		t.Helper()
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, TaskIds: []int64{1001}},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFinished, Progress: 100},
		}
		mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		if err != nil {
			t.Fatal(err)
		}

		mt := &meta{segments: NewSegmentsInfo()}
		for _, id := range append(append([]int64{}, indexedSegments...), *debt...) {
			mt.segments.SetSegment(id, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID: id, CollectionID: 100, State: commonpb.SegmentState_Flushed,
			}})
		}
		// GetUnindexedSegments is the debt oracle; stub it rather than build a
		// full index meta - what matters here is the wait, not index bookkeeping.
		mockey.Mock((*indexMeta).GetUnindexedSegments).To(
			func(_ *indexMeta, _ int64, _ []int64) []int64 { return *debt }).Build()
		mt.indexMeta = &indexMeta{}

		return refreshMeta, mt, refreshMeta.GetJob(1)
	}

	t.Run("an L0 segment does not count as unindexed debt", func(t *testing.T) {
		// createIndexesForSegment skips L0 outright, so an L0 segment never
		// acquires an index record - counting it would make the wait run to
		// the timeout and fail a refresh whose real segments are all indexed.
		mockey.PatchConvey("l0", t, func() {
			pt := paramtable.Get()
			pt.Save(pt.DataCoordCfg.RefreshWaitForIndex.Key, "true")
			defer pt.Reset(pt.DataCoordCfg.RefreshWaitForIndex.Key)

			var debt []int64 // the debt oracle sees whatever we hand it
			refreshMeta, mt, job := stage(t, []int64{555}, &debt)
			mt.segments.SetSegment(999, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID: 999, CollectionID: 100, State: commonpb.SegmentState_Flushed,
				Level: datapb.SegmentLevel_L0,
			}})

			var asked []int64
			mockey.Mock((*meta).SelectSegments).To(
				func(m *meta, ctx context.Context, filters ...SegmentFilter) []*SegmentInfo {
					out := make([]*SegmentInfo, 0)
					for _, s := range m.segments.segments {
						keep := true
						for _, f := range filters {
							if ff, ok := f.(SegmentFilterFunc); ok && !ff(s) {
								keep = false
							}
						}
						if keep {
							out = append(out, s)
						}
					}
					asked = lo.Map(out, func(s *SegmentInfo, _ int) int64 { return s.GetID() })
					return out
				}).Build()

			checker := newRefreshChecker(ctx, mt, refreshMeta, make(chan struct{}), nil,
				func(context.Context, *datapb.ExternalCollectionRefreshJob) error { return nil },
				nil, nil, nil)

			checker.aggregateJobState(job)                   // enter the wait
			checker.aggregateJobState(refreshMeta.GetJob(1)) // evaluate the debt

			assert.NotContains(t, asked, int64(999),
				"an L0 segment can never be indexed; asking about it would hold the job until the timeout")
			assert.Equal(t, indexpb.JobState_JobStateFinished, refreshMeta.GetJob(1).GetState())
		})
	})

	t.Run("a job whose ingest reported 100 does not stay at 100 while waiting", func(t *testing.T) {
		// An InProgress job reporting 100 reads as done to a poller waiting for
		// it. The entry write moves progress into the band, so there is never a
		// tick where the job is waiting and still shows what the ingest left.
		mockey.PatchConvey("no 100 while waiting", t, func() {
			pt := paramtable.Get()
			pt.Save(pt.DataCoordCfg.RefreshWaitForIndex.Key, "true")
			defer pt.Reset(pt.DataCoordCfg.RefreshWaitForIndex.Key)

			debt := []int64{556}
			refreshMeta, mt, job := stage(t, nil, &debt)
			require.NoError(t, refreshMeta.UpdateJobProgress(1, 100))
			job = refreshMeta.GetJob(1)

			checker := newRefreshChecker(ctx, mt, refreshMeta, make(chan struct{}), nil,
				func(context.Context, *datapb.ExternalCollectionRefreshJob) error { return nil },
				nil, nil, nil)
			checker.aggregateJobState(job)

			held := refreshMeta.GetJob(1)
			require.Equal(t, indexpb.JobState_JobStateInProgress, held.GetState())
			assert.Less(t, held.GetProgress(), int64(100),
				"a waiting job must never report 100")
		})
	})

	t.Run("off keeps the native transition", func(t *testing.T) {
		mockey.PatchConvey("off", t, func() {
			pt := paramtable.Get()
			pt.Save(pt.DataCoordCfg.RefreshWaitForIndex.Key, "false")
			defer pt.Reset(pt.DataCoordCfg.RefreshWaitForIndex.Key)

			debt := []int64{555}
			refreshMeta, mt, job := stage(t, nil, &debt)
			applied := 0
			checker := newRefreshChecker(ctx, mt, refreshMeta, make(chan struct{}), nil,
				func(context.Context, *datapb.ExternalCollectionRefreshJob) error { applied++; return nil },
				nil, nil, nil)

			checker.aggregateJobState(job)

			done := refreshMeta.GetJob(1)
			assert.Equal(t, indexpb.JobState_JobStateFinished, done.GetState(),
				"with the wait off, ingest done is finished - exactly as before")
			assert.Equal(t, int64(100), done.GetProgress())
			assert.Equal(t, 1, applied)
			assert.Zero(t, done.GetIndexWaitStartedTime(), "the wait marker belongs to the wait")
		})
	})

	t.Run("on applies once and holds while segments are unindexed", func(t *testing.T) {
		mockey.PatchConvey("holding", t, func() {
			pt := paramtable.Get()
			pt.Save(pt.DataCoordCfg.RefreshWaitForIndex.Key, "true")
			defer pt.Reset(pt.DataCoordCfg.RefreshWaitForIndex.Key)

			debt := []int64{556}
			refreshMeta, mt, job := stage(t, []int64{555}, &debt)
			applied := 0
			checker := newRefreshChecker(ctx, mt, refreshMeta, make(chan struct{}), nil,
				func(context.Context, *datapb.ExternalCollectionRefreshJob) error { applied++; return nil },
				nil, nil, nil)

			checker.aggregateJobState(job) // enter the wait
			held := refreshMeta.GetJob(1)
			require.Equal(t, indexpb.JobState_JobStateInProgress, held.GetState())
			assert.NotZero(t, held.GetIndexWaitStartedTime())
			assert.Equal(t, 1, applied, "the apply lands in the same write that enters the wait")
			assert.Equal(t, indexWaitProgressFloor, held.GetProgress(),
				"the same write moves progress into the reserved band, so an InProgress job never reports 100")

			checker.aggregateJobState(refreshMeta.GetJob(1)) // a held tick
			checker.aggregateJobState(refreshMeta.GetJob(1))

			still := refreshMeta.GetJob(1)
			assert.Equal(t, indexpb.JobState_JobStateInProgress, still.GetState(),
				"an unindexed segment holds the job open")
			assert.Equal(t, 1, applied, "and the apply is never replayed")
			assert.Equal(t, int64(95), still.GetProgress(),
				"progress tracks the indexed fraction: one of two indexed is 95")
		})
	})

	t.Run("finishes once every segment is indexed, without replaying the apply", func(t *testing.T) {
		mockey.PatchConvey("cleared", t, func() {
			pt := paramtable.Get()
			pt.Save(pt.DataCoordCfg.RefreshWaitForIndex.Key, "true")
			defer pt.Reset(pt.DataCoordCfg.RefreshWaitForIndex.Key)

			debt := []int64{556}
			refreshMeta, mt, job := stage(t, []int64{555}, &debt)
			applied := 0
			checker := newRefreshChecker(ctx, mt, refreshMeta, make(chan struct{}), nil,
				func(context.Context, *datapb.ExternalCollectionRefreshJob) error {
					applied++
					if applied > 1 {
						// A replay would be a second apply of the same results;
						// model it as fatal so the test cannot pass silently.
						return errors.New("apply replayed")
					}
					return nil
				},
				nil, nil, nil)

			checker.aggregateJobState(job)
			require.Equal(t, indexpb.JobState_JobStateInProgress, refreshMeta.GetJob(1).GetState())

			debt = nil // every segment indexed now

			checker.aggregateJobState(refreshMeta.GetJob(1))

			done := refreshMeta.GetJob(1)
			assert.Equal(t, indexpb.JobState_JobStateFinished, done.GetState())
			assert.Equal(t, int64(100), done.GetProgress())
			assert.Equal(t, 1, applied)
		})
	})

	t.Run("the refreshed schema is published when the segments are applied, not when the wait ends", func(t *testing.T) {
		// Index requests take ExternalSource/ExternalSpec from the COLLECTION
		// schema. Holding the publish until the wait ended would point every
		// build this wait is waiting for at the pre-refresh endpoint.
		mockey.PatchConvey("publish on apply", t, func() {
			pt := paramtable.Get()
			pt.Save(pt.DataCoordCfg.RefreshWaitForIndex.Key, "true")
			defer pt.Reset(pt.DataCoordCfg.RefreshWaitForIndex.Key)

			debt := []int64{556}
			refreshMeta, mt, job := stage(t, nil, &debt)
			notified := 0
			checker := newRefreshChecker(ctx, mt, refreshMeta, make(chan struct{}),
				func(context.Context, *datapb.ExternalCollectionRefreshJob) { notified++ },
				func(context.Context, *datapb.ExternalCollectionRefreshJob) error { return nil },
				nil, nil, nil)

			checker.aggregateJobState(job)
			require.Equal(t, indexpb.JobState_JobStateInProgress, refreshMeta.GetJob(1).GetState())

			checker.ensureJobFinishedNotified(refreshMeta.GetJob(1))
			assert.Equal(t, 1, notified,
				"the callback that publishes the schema must fire while the job is still waiting")
		})
	})

	t.Run("a job that outruns the timeout fails, as any refresh does", func(t *testing.T) {
		mockey.PatchConvey("timeout", t, func() {
			pt := paramtable.Get()
			pt.Save(pt.DataCoordCfg.RefreshWaitForIndex.Key, "true")
			defer pt.Reset(pt.DataCoordCfg.RefreshWaitForIndex.Key)

			debt := []int64{556}
			refreshMeta, mt, job := stage(t, nil, &debt)
			job.StartTime = time.Now().Add(-1000 * time.Hour).UnixMilli()
			checker := newRefreshChecker(ctx, mt, refreshMeta, make(chan struct{}), nil,
				func(context.Context, *datapb.ExternalCollectionRefreshJob) error { return nil },
				nil, nil, nil)

			checker.aggregateJobState(job)
			require.Equal(t, indexpb.JobState_JobStateInProgress, refreshMeta.GetJob(1).GetState())

			checker.tryTimeoutJob(job)

			assert.Equal(t, indexpb.JobState_JobStateFailed, refreshMeta.GetJob(1).GetState(),
				"the wait runs on the job's own clock; outrunning it is an ordinary refresh timeout")
		})
	})

	t.Run("two callers holding a pre-marker snapshot apply once between them", func(t *testing.T) {
		// The eager task path and the periodic tick run on different
		// goroutines, and two tasks of one job can finish at once - so two
		// callers can both read the job before either wrote the marker.
		// Checking the marker in the caller cannot order that; only the job
		// lock can. Master is safe for free because its Finished write IS the
		// guard; this transition stays InProgress and needs its own.
		mockey.PatchConvey("apply once under concurrency", t, func() {
			pt := paramtable.Get()
			pt.Save(pt.DataCoordCfg.RefreshWaitForIndex.Key, "true")
			defer pt.Reset(pt.DataCoordCfg.RefreshWaitForIndex.Key)

			debt := []int64{556}
			refreshMeta, mt, job := stage(t, nil, &debt)

			var applies int32
			checker := newRefreshChecker(ctx, mt, refreshMeta, make(chan struct{}), nil,
				func(context.Context, *datapb.ExternalCollectionRefreshJob) error {
					atomic.AddInt32(&applies, 1)
					return nil
				}, nil, nil, nil)

			snapshotA, snapshotB := job, refreshMeta.GetJob(1)
			checker.aggregateJobState(snapshotA)
			checker.aggregateJobState(snapshotB)

			assert.Equal(t, int32(1), atomic.LoadInt32(&applies),
				"segment results must be applied exactly once")
		})
	})

	t.Run("turning the parameter off mid-wait releases the job without re-applying", func(t *testing.T) {
		// Keying the branch on the parameter rather than the marker would send
		// an already-applied job down the generic transition, which carries a
		// pre-apply - and applyExternalRefreshPatch clears TextStatsLogs and
		// JsonKeyStats, so the replay would discard indexes built during the
		// very wait being disabled. An operator turning the hold off wants the
		// held jobs released, so the job finishes at once instead.
		mockey.PatchConvey("parameter off mid-wait", t, func() {
			pt := paramtable.Get()
			pt.Save(pt.DataCoordCfg.RefreshWaitForIndex.Key, "true")
			defer pt.Reset(pt.DataCoordCfg.RefreshWaitForIndex.Key)

			debt := []int64{556}
			refreshMeta, mt, job := stage(t, nil, &debt)

			var applies int32
			checker := newRefreshChecker(ctx, mt, refreshMeta, make(chan struct{}), nil,
				func(context.Context, *datapb.ExternalCollectionRefreshJob) error {
					atomic.AddInt32(&applies, 1)
					return nil
				}, nil, nil, nil)

			checker.aggregateJobState(job)
			require.Equal(t, int32(1), atomic.LoadInt32(&applies))
			require.NotZero(t, refreshMeta.GetJob(1).GetIndexWaitStartedTime())

			pt.Save(pt.DataCoordCfg.RefreshWaitForIndex.Key, "false")
			checker.aggregateJobState(refreshMeta.GetJob(1))

			assert.Equal(t, int32(1), atomic.LoadInt32(&applies),
				"a job that already applied must never apply again")
			assert.Equal(t, indexpb.JobState_JobStateFinished, refreshMeta.GetJob(1).GetState(),
				"the debt is unpaid, but the hold was disabled - release the job")
		})
	})
}
