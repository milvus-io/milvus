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
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// externalCollectionRefreshChecker drives the external collection job state machine.
//
// This is an internal component of ExternalCollectionRefreshManager, responsible for:
// 1. Job timeout detection
// 2. Garbage collection for completed/failed jobs (including associated tasks)
// 3. Job statistics reporting
//
// JOB STATE MACHINE:
// Init → InProgress → Finished
//
//	↓        ↓            ↓
//
// Failed  Failed        GC
//
//	↓        ↓            ↓
//
// GC       GC       (removed)
//
// STATE TRANSITIONS:
// 1. Init → InProgress: Task dispatched to DataNode (handled by scheduler)
// 2. InProgress → Finished: All tasks completed successfully
// 3. InProgress → Failed: Any task failed or job timeout
// 4. Finished/Failed → GC: Remove job and tasks after retention period
type externalCollectionRefreshChecker struct {
	ctx         context.Context
	refreshMeta *externalCollectionRefreshMeta
	closeChan   chan struct{}
	// onJobFinished is the manager-side callback that pushes the refreshed
	// schema (ExternalSource/ExternalSpec) into RootCoord via the WAL
	// broadcast. The manager holds a notifiedJobs dedup map so this callback
	// is delivered exactly once per jobID even when called concurrently
	// from the eager task path and the periodic checker tick.
	onJobFinished func(ctx context.Context, job *datapb.ExternalCollectionRefreshJob)
	// applyJobInfo is invoked exactly before a job is persisted as Finished.
	// It performs the collection-global segment update from all finished task
	// results so progress polls cannot observe Finished before segments are visible.
	applyJobInfo func(ctx context.Context, job *datapb.ExternalCollectionRefreshJob) error
	// onJobFailed is the manager-side callback invoked when a job first
	// transitions into Failed state (via aggregateJobState or tryTimeoutJob).
	// Used to reclaim per-job resources (e.g. the explore temp directory)
	// without waiting for the retention-gated GC path. The callback itself
	// is idempotent — the manager dedups against notifiedJobs so concurrent
	// eager and periodic paths only fire one cleanup per jobID.
	onJobFailed func(jobID int64)
	// onJobGC is invoked after the checker successfully drops a job during
	// GC so the manager can release any per-job bookkeeping (notifiedJobs
	// dedup entry). Keeps the dedup map bounded across DataCoord lifetime.
	onJobGC func(jobID int64)
	// onInitJobPending is fired for jobs still in Init state with no tasks
	// yet. This is the retry hook for the two-phase submission scheme: the
	// WAL ack callback persists the Job record in Init state and kicks off
	// Phase B (explore + task creation) asynchronously; if that first attempt
	// fails, the checker tick calls this callback to trigger a new attempt.
	// MUST be non-blocking — the manager's implementation dedups concurrent
	// invocations and runs the actual work in a background goroutine.
	onInitJobPending func(jobID int64)
}

func newRefreshChecker(
	ctx context.Context,
	refreshMeta *externalCollectionRefreshMeta,
	closeChan chan struct{},
	onJobFinished func(ctx context.Context, job *datapb.ExternalCollectionRefreshJob),
	applyJobInfo func(ctx context.Context, job *datapb.ExternalCollectionRefreshJob) error,
	onJobFailed func(jobID int64),
	onJobGC func(jobID int64),
	onInitJobPending func(jobID int64),
) *externalCollectionRefreshChecker {
	return &externalCollectionRefreshChecker{
		ctx:              ctx,
		refreshMeta:      refreshMeta,
		closeChan:        closeChan,
		onJobFinished:    onJobFinished,
		applyJobInfo:     applyJobInfo,
		onJobFailed:      onJobFailed,
		onJobGC:          onJobGC,
		onInitJobPending: onInitJobPending,
	}
}

// run starts the checker loop. The checker periodically scans every refresh
// job and runs the same per-job processing function (processJob) that the
// eager task path invokes via processJobByID. The periodic pass acts as a
// safety net for any state transition the eager path missed (e.g., DataCoord
// restart between task completion and the eager call).
func (c *externalCollectionRefreshChecker) run() {
	checkInterval := Params.DataCoordCfg.ExternalCollectionCheckInterval.GetAsDuration(time.Second)
	log.Info("start external collection checker", zap.Duration("checkInterval", checkInterval))
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.closeChan:
			log.Info("external collection checker exited")
			return
		case <-ticker.C:
			c.processJobs()
		}
	}
}

// processJobs runs one full inspection cycle over all refresh jobs. Called
// from the periodic tick. Idempotent — running it back-to-back is safe
// (each step short-circuits when there's nothing to do).
func (c *externalCollectionRefreshChecker) processJobs() {
	jobs := c.refreshMeta.GetAllJobs()

	for _, job := range jobs {
		c.processJob(job)
	}

	// Report statistics and metrics. Re-read from meta so state transitions
	// that happened inside the loop above are reflected in the stats.
	c.logJobStats(c.refreshMeta.GetAllJobs())
}

// processJob runs one inspection pass for a single job: state aggregation,
// timeout check, finished-callback firing, and GC. Both the periodic loop
// and the eager task path call this so the same code drives every job
// state transition. Idempotent — repeated calls short-circuit on terminal
// state and source/spec equality.
func (c *externalCollectionRefreshChecker) processJob(job *datapb.ExternalCollectionRefreshJob) {
	// Retry Phase B task creation for jobs that are still in Init with no
	// tasks (i.e. the async submission attempt did not land tasks yet).
	// This is the safety-net retry path: the WAL ack callback already kicked
	// off one async attempt after AddJob, and tryTimeoutJob is the terminal
	// bound if we keep failing. The callback itself is non-blocking and
	// dedups concurrent calls, so firing it every tick is safe.
	if c.onInitJobPending != nil &&
		job.GetState() == indexpb.JobState_JobStateInit &&
		len(job.GetTaskIds()) == 0 {
		c.onInitJobPending(job.GetJobId())
	}

	// Aggregate task states to update job state. This is where a job
	// transitions to Finished/Failed once all its tasks have completed.
	c.aggregateJobState(job)

	// Re-read the job from meta after aggregateJobState. The local `job`
	// pointer is a snapshot from the periodic GetAllJobs() pass; if
	// aggregateJobState just transitioned the job to Finished/Failed, the
	// stale snapshot would still report InProgress and the timeout switch
	// below would erroneously mark a freshly-finished job as timed out.
	latestJob := c.refreshMeta.GetJob(job.GetJobId())
	if latestJob == nil {
		// Job removed (e.g. concurrent GC) — nothing more to do.
		return
	}

	// Check timeout for active jobs (Init, Retry, InProgress) using the
	// freshly-read state, not the stale snapshot.
	switch latestJob.GetState() {
	case indexpb.JobState_JobStateInit, indexpb.JobState_JobStateRetry, indexpb.JobState_JobStateInProgress:
		c.tryTimeoutJob(latestJob)
	}

	// Fire the finished callback. ensureJobFinishedNotified is a no-op
	// when the job isn't in Finished state, and the manager-side callback
	// short-circuits when source/spec already match (so re-firing across
	// cycles before GC is harmless).
	c.ensureJobFinishedNotified(latestJob)

	// Check GC for terminal states (Finished/Failed)
	c.checkGC(latestJob)
}

// processJobByID looks up a job and runs one inspection pass for it
// synchronously. Used by the eager task path after a task transitions to
// a terminal state, so the schemaUpdater fires before the task call returns
// and progress polls observe a consistent state. Returns silently if the
// job is missing (e.g., already GC'd).
func (c *externalCollectionRefreshChecker) processJobByID(jobID int64) {
	job := c.refreshMeta.GetJob(jobID)
	if job == nil {
		return
	}
	c.processJob(job)
}

// aggregateJobState updates job state based on its tasks.
func (c *externalCollectionRefreshChecker) aggregateJobState(job *datapb.ExternalCollectionRefreshJob) {
	// Skip if job is already in terminal state
	if job.GetState() == indexpb.JobState_JobStateFinished ||
		job.GetState() == indexpb.JobState_JobStateFailed {
		return
	}

	// Get aggregated state from tasks
	state, progress := c.refreshMeta.AggregateJobStateFromTasks(job.GetJobId())
	if state == indexpb.JobState_JobStateNone {
		// No tasks yet
		return
	}

	// Update job if state or progress changed
	if state != job.GetState() {
		// State changed - handle state transition
		var failReason string
		if state == indexpb.JobState_JobStateFailed {
			// Get fail reason from first failed task
			tasks := c.refreshMeta.GetTasksByJobID(job.GetJobId())
			for _, task := range tasks {
				if task.GetState() == indexpb.JobState_JobStateFailed {
					failReason = task.GetFailReason()
					break
				}
			}
			// Persist progress snapshot BEFORE transitioning to Failed
			// This captures the last known progress at failure time
			if progress != job.GetProgress() {
				if err := c.refreshMeta.UpdateJobProgress(job.GetJobId(), progress); err != nil {
					log.Warn("failed to update job progress before failure",
						zap.Int64("jobID", job.GetJobId()),
						zap.Error(err))
				}
			}
		}

		if state == indexpb.JobState_JobStateFinished && c.applyJobInfo != nil {
			applied, err := c.refreshMeta.UpdateJobStateWithPreApply(
				job.GetJobId(),
				state,
				failReason,
				func(latestJob *datapb.ExternalCollectionRefreshJob) error {
					return c.applyJobInfo(c.ctx, latestJob)
				})
			if err != nil {
				log.Warn("failed to apply external collection refresh result",
					zap.Int64("jobID", job.GetJobId()),
					zap.Error(err))
				if applied && c.onJobFailed != nil {
					c.onJobFailed(job.GetJobId())
				}
				return
			}
			if !applied {
				// A concurrent path already drove the job into a terminal state
				// and owns the one-time segment apply / callback side effects.
				return
			}

			if err := c.refreshMeta.ClearTaskResultsByJobID(job.GetJobId()); err != nil {
				log.Warn("failed to clear external collection refresh task results",
					zap.Int64("jobID", job.GetJobId()),
					zap.Error(err))
			}

			// processJobs calls ensureJobFinishedNotified right after this
			// function returns, so we don't fire the callback here.
			return
		}

		applied, err := c.refreshMeta.UpdateJobState(job.GetJobId(), state, failReason)
		if err != nil {
			log.Warn("failed to update job state from task aggregation",
				zap.Int64("jobID", job.GetJobId()),
				zap.Error(err))
			return
		}
		if !applied {
			// Terminal-state guard skipped the write — a concurrent path
			// already drove the job to a terminal state. Do not fire any
			// per-job side effects here; the path that actually persisted
			// the transition owns the follow-up (onJobFinished or onJobFailed).
			return
		}

		// Fire onJobFailed right after the state transition persists, so
		// per-job resources (explore temp dir) get reclaimed immediately
		// instead of waiting for the retention-gated GC path (default 24h).
		// The manager dedups so concurrent eager + periodic paths only
		// clean once per jobID.
		if state == indexpb.JobState_JobStateFailed && c.onJobFailed != nil {
			c.onJobFailed(job.GetJobId())
		}

		// processJobs calls ensureJobFinishedNotified right after this
		// function returns, so we don't fire the callback here — keeping
		// the notification firing in exactly one place per cycle.

		// For Finished state, UpdateJobState sets Progress=100
		// For Failed state, progress was already persisted above
		// For non-terminal states, update progress if needed
		if state != indexpb.JobState_JobStateFailed && state != indexpb.JobState_JobStateFinished {
			if progress != job.GetProgress() {
				if err := c.refreshMeta.UpdateJobProgress(job.GetJobId(), progress); err != nil {
					log.Warn("failed to update job progress",
						zap.Int64("jobID", job.GetJobId()),
						zap.Error(err))
				}
			}
		}
	} else if progress != job.GetProgress() {
		// Only progress changed
		if err := c.refreshMeta.UpdateJobProgress(job.GetJobId(), progress); err != nil {
			log.Warn("failed to update job progress",
				zap.Int64("jobID", job.GetJobId()),
				zap.Error(err))
		}
	}
}

// ensureJobFinishedNotified calls onJobFinished for a finished job. The checker
// is the single processing path, so this fires once per job per cycle; the
// callback itself is idempotent (the manager short-circuits when source/spec
// already match), so re-firing on a later tick before GC is harmless.
func (c *externalCollectionRefreshChecker) ensureJobFinishedNotified(job *datapb.ExternalCollectionRefreshJob) {
	if c.onJobFinished == nil {
		return
	}
	// Re-read job from meta to get latest state (may have been updated eagerly)
	latestJob := c.refreshMeta.GetJob(job.GetJobId())
	if latestJob == nil || latestJob.GetState() != indexpb.JobState_JobStateFinished {
		return
	}
	c.onJobFinished(c.ctx, latestJob)
}

// logJobStats reports job statistics grouped by state.
func (c *externalCollectionRefreshChecker) logJobStats(jobs map[int64]*datapb.ExternalCollectionRefreshJob) {
	// Group jobs by state
	byState := lo.GroupBy(lo.Values(jobs), func(job *datapb.ExternalCollectionRefreshJob) string {
		return job.GetState().String()
	})

	// Count jobs in each state
	stateNum := make(map[string]int)
	for state := range indexpb.JobState_value {
		if state == indexpb.JobState_JobStateNone.String() {
			continue
		}
		stateNum[state] = len(byState[state])
	}

	if len(jobs) > 0 {
		log.Info("external collection job stats", zap.Any("stateNum", stateNum))
	}
}

// tryTimeoutJob checks if job has exceeded timeout and marks it as failed.
func (c *externalCollectionRefreshChecker) tryTimeoutJob(job *datapb.ExternalCollectionRefreshJob) {
	// Skip if StartTime is not set
	if job.GetStartTime() == 0 {
		return
	}

	// Get timeout configuration
	timeout := Params.DataCoordCfg.ExternalCollectionJobTimeout.GetAsDuration(time.Second)

	// Calculate job age
	startTime := time.UnixMilli(job.GetStartTime())
	age := time.Since(startTime)

	if age > timeout {
		log.Warn("external collection job timeout",
			zap.Int64("jobID", job.GetJobId()),
			zap.Int64("collectionID", job.GetCollectionId()),
			zap.Duration("age", age),
			zap.Duration("timeout", timeout))

		applied, err := c.refreshMeta.UpdateJobState(
			job.GetJobId(),
			indexpb.JobState_JobStateFailed,
			"timeout")
		if err != nil {
			log.Warn("failed to mark job as timed out",
				zap.Int64("jobID", job.GetJobId()),
				zap.Error(err))
			return
		}
		if !applied {
			// Terminal-state guard fired — while the checker was about to
			// time out this job a concurrent eager path already transitioned
			// it to Finished/Failed. Firing onJobFailed here would poison
			// the manager's notifiedJobs dedup map and cause a subsequent
			// handleJobFinished to skip the schemaUpdater. Bail out and let
			// the path that actually persisted the transition do cleanup.
			log.Info("skip timeout fail path, job already in terminal state",
				zap.Int64("jobID", job.GetJobId()))
			return
		}

		// Also mark all active tasks as failed
		tasks := c.refreshMeta.GetTasksByJobID(job.GetJobId())
		for _, task := range tasks {
			if task.GetState() == indexpb.JobState_JobStateInit ||
				task.GetState() == indexpb.JobState_JobStateRetry ||
				task.GetState() == indexpb.JobState_JobStateInProgress {
				_ = c.refreshMeta.UpdateTaskState(task.GetTaskId(), indexpb.JobState_JobStateFailed, "job timeout")
			}
		}

		// Reclaim per-job resources (explore temp dir) immediately on
		// timeout instead of waiting 24h for the retention-gated GC path.
		if c.onJobFailed != nil {
			c.onJobFailed(job.GetJobId())
		}
	}
}

// checkGC performs garbage collection for completed/failed jobs.
func (c *externalCollectionRefreshChecker) checkGC(job *datapb.ExternalCollectionRefreshJob) {
	// Only GC terminal states
	if job.GetState() != indexpb.JobState_JobStateFinished &&
		job.GetState() != indexpb.JobState_JobStateFailed {
		return
	}

	// Check if job has EndTime set
	if job.GetEndTime() == 0 {
		return
	}

	// Get retention configuration
	retention := Params.DataCoordCfg.ExternalCollectionJobRetention.GetAsDuration(time.Second)

	// Calculate time since job ended
	endTime := time.UnixMilli(job.GetEndTime())
	age := time.Since(endTime)

	if age > retention {
		log.Info("external collection job has reached GC retention",
			zap.Int64("jobID", job.GetJobId()),
			zap.Int64("collectionID", job.GetCollectionId()),
			zap.Duration("age", age),
			zap.Duration("retention", retention))

		// DropJob drops job and associated tasks. No in-loop retry: checkGC runs periodically,
		// so the next tick will naturally retry if etcd was temporarily unavailable.
		err := c.refreshMeta.DropJob(c.ctx, job.GetJobId())
		if err != nil {
			log.Warn("failed to remove external collection job during GC, will retry on next check",
				zap.Int64("jobID", job.GetJobId()),
				zap.Error(err))
			return
		}
		log.Info("external collection job removed", zap.Int64("jobID", job.GetJobId()))
		// Release per-job bookkeeping in the manager (notifiedJobs dedup map)
		// so it stays bounded across DataCoord lifetime.
		if c.onJobGC != nil {
			c.onJobGC(job.GetJobId())
		}
	}
}
