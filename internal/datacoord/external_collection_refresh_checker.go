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
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	ctx context.Context
	// mt is datacoord's segment/index meta, read directly for the index wait -
	// the same way importChecker reads it. Nil in tests that do not exercise
	// the wait.
	mt          *meta
	refreshMeta *externalCollectionRefreshMeta
	closeChan   chan struct{}
	// updateJobSchema pushes ExternalSource/ExternalSpec into RootCoord through
	// the WAL before the job is persisted as Finished. A failed delivery leaves
	// the job non-terminal so the next checker pass retries it.
	updateJobSchema func(ctx context.Context, job *datapb.ExternalCollectionRefreshJob) error
	// applyJobInfo performs the collection-global segment update from all
	// finished task results before the schema update and Finished transition.
	applyJobInfo func(ctx context.Context, job *datapb.ExternalCollectionRefreshJob) error
	// onJobFailed is the manager-side callback invoked when a job first
	// transitions into Failed state (via aggregateJobState or tryTimeoutJob).
	// Used to reclaim per-job resources (e.g. the explore temp directory)
	// without waiting for the retention-gated GC path. The callback cleanup is
	// idempotent; retention GC retries it.
	onJobFailed func(jobID int64)
	// cleanupJobResources removes idempotent per-job resources before Finished is
	// published and again before terminal metadata is dropped. Returning an error
	// leaves the job as the retry anchor for the next checker pass.
	cleanupJobResources func(jobID int64) error
	// onInitJobPending is fired for jobs still in Init state with no tasks
	// yet. This is the retry hook for the two-phase submission scheme: the
	// WAL ack callback persists the Job record in Init state and kicks off
	// Phase B (explore + task creation) asynchronously; if that first attempt
	// fails, the checker tick calls this callback to trigger a new attempt.
	// MUST be non-blocking — the manager's implementation dedups concurrent
	// invocations and runs the actual work in a background goroutine.
	onInitJobPending func(jobID int64)
	// dropJobTasks stops scheduler callbacks and confirms that every assigned
	// worker task has accepted Drop before terminal metadata is removed. An error
	// keeps the job and tasks as the next checker pass's retry anchor.
	dropJobTasks func(jobID int64) error
}

func newRefreshChecker(
	ctx context.Context,
	mt *meta,
	refreshMeta *externalCollectionRefreshMeta,
	closeChan chan struct{},
	updateJobSchema func(ctx context.Context, job *datapb.ExternalCollectionRefreshJob) error,
	applyJobInfo func(ctx context.Context, job *datapb.ExternalCollectionRefreshJob) error,
	onJobFailed func(jobID int64),
	cleanupJobResources func(jobID int64) error,
	onInitJobPending func(jobID int64),
) *externalCollectionRefreshChecker {
	return &externalCollectionRefreshChecker{
		ctx:                 ctx,
		mt:                  mt,
		refreshMeta:         refreshMeta,
		closeChan:           closeChan,
		updateJobSchema:     updateJobSchema,
		applyJobInfo:        applyJobInfo,
		onJobFailed:         onJobFailed,
		cleanupJobResources: cleanupJobResources,
		onInitJobPending:    onInitJobPending,
	}
}

// run starts the checker loop. The checker periodically scans every refresh
// job and runs the same per-job processing function (processJob) that the
// eager task path invokes via processJobByID. The periodic pass acts as a
// safety net for any state transition the eager path missed (e.g., DataCoord
// restart between task completion and the eager call).
func (c *externalCollectionRefreshChecker) run() {
	checkInterval := Params.DataCoordCfg.ExternalCollectionCheckInterval.GetAsDuration(time.Second)
	mlog.Info(c.ctx, "start external collection checker", mlog.Duration("checkInterval", checkInterval))
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.closeChan:
			mlog.Info(c.ctx, "external collection checker exited")
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
// finalization, timeout check, and GC. The eager task path uses the same flow
// through processJobEager but leaves retention GC to the periodic checker.
func (c *externalCollectionRefreshChecker) processJob(job *datapb.ExternalCollectionRefreshJob) {
	c.processJobWithGC(job, true)
}

// processJobEager is the eager task path: same pass, minus GC.
func (c *externalCollectionRefreshChecker) processJobEager(job *datapb.ExternalCollectionRefreshJob) {
	c.processJobWithGC(job, false)
}

func (c *externalCollectionRefreshChecker) processJobWithGC(job *datapb.ExternalCollectionRefreshJob, runGC bool) {
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

	// Entering the optional index wait publishes the segment set before the
	// job becomes terminal. Publish its matching external source/spec before
	// any index build can be nudged, and keep the applied InProgress job as the
	// retry anchor until that idempotent publication succeeds.
	// Only an active wait can owe schema publication. Terminal jobs are kept
	// for retention/inspection after that publication has already succeeded;
	// replaying an old job here could overwrite a newer refresh's schema.
	if latestJob.GetState() == indexpb.JobState_JobStateInProgress &&
		latestJob.GetIndexWaitStartedTime() != 0 &&
		!c.syncAppliedJobSchema(latestJob) {
		return
	}

	// Check timeout for active jobs (Init, Retry, InProgress) using the
	// freshly-read state, not the stale snapshot.
	switch latestJob.GetState() {
	case indexpb.JobState_JobStateInit, indexpb.JobState_JobStateRetry, indexpb.JobState_JobStateInProgress:
		c.tryTimeoutJob(latestJob)
	}

	if runGC {
		// Check GC for terminal states (Finished/Failed).
		//
		// Only on the periodic tick. GC hands every task of the job back from
		// the scheduler, and the eager caller is a worker callback that already
		// holds one of those tasks' scheduler locks -- taking it again would
		// deadlock on itself. Nothing here is latency-sensitive: GC only drops
		// metadata for a job that already reached a terminal state, so waiting
		// for the next tick costs nothing a caller can observe.
		c.checkGC(latestJob)
	}
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
	c.processJobEager(job)
}

// indexWaitProgressFloor is what a job reports the moment it enters the index
// wait; it walks from there to 99 as segments get indexed. 100 is reserved for
// done, which pollers key on.
const indexWaitProgressFloor = int64(90)

func (c *externalCollectionRefreshChecker) indexWaitEnabled() bool {
	return c.mt != nil &&
		c.mt.indexMeta != nil &&
		c.applyJobInfo != nil &&
		Params.DataCoordCfg.RefreshWaitForIndex.GetAsBool()
}

// beginIndexWait applies the job's results and puts it into the index wait, in
// one transition under the job lock. Segment adoption and task-result
// consumption are atomic with each other; the following job-marker write is
// ordered but separate (see BeginIndexWait). The job stays InProgress. Once the
// marker is durable, this function synchronously publishes the refreshed
// source/spec before any index build can be nudged.
//
// Reports whether THIS call is the one that entered the wait, so the caller
// owns the follow-up exactly once.
func (c *externalCollectionRefreshChecker) beginIndexWait(job *datapb.ExternalCollectionRefreshJob) bool {
	applied, err := c.refreshMeta.BeginIndexWait(
		job.GetJobId(),
		func(latestJob *datapb.ExternalCollectionRefreshJob) error {
			return c.applyJobInfo(c.ctx, latestJob)
		})
	if err != nil {
		c.handleFinalizationError(job.GetJobId(), "apply refresh result before index wait", err)
		return false
	}
	if !applied {
		// Either a concurrent path already drove the job terminal, or it
		// already entered the wait - BeginIndexWait rejects a second entry
		// under the job lock. Both mean another caller owns the one-time
		// side effects.
		return false
	}
	mlog.Info(c.ctx, "external collection refresh applied, waiting for its segments to be indexed",
		mlog.FieldJobID(job.GetJobId()),
		mlog.FieldCollectionID(job.GetCollectionId()))
	entered := c.refreshMeta.GetJob(job.GetJobId())
	return entered != nil && c.syncAppliedJobSchema(entered)
}

// syncAppliedJobSchema keeps the schema publication ordered after segment
// adoption and before index dispatch. It stores no separate notification
// state: collection metadata is authoritative, and updateJobSchema is
// replay-safe because it skips an already-matching source/spec.
func (c *externalCollectionRefreshChecker) syncAppliedJobSchema(job *datapb.ExternalCollectionRefreshJob) bool {
	if c.updateJobSchema == nil {
		return true
	}
	if err := c.updateJobSchema(c.ctx, job); err != nil {
		c.handleFinalizationError(job.GetJobId(), "update collection schema", err)
		return false
	}
	return true
}

// indexWaitDone reports whether every segment of the collection carries an
// index, nudging the unindexed ones into the build channel on the way and
// tracking the indexed fraction as progress.
//
// The debt is the collection's whole flushed segment set, not a snapshot of
// what this refresh produced: a refresh DEFINES the collection's contents - it
// keeps, patches, adds and drops until what remains is exactly the external
// source - so once its apply has landed, "this refresh's segments" and "the
// collection's segments" are the same set. That is what lets this stay
// stateless where import needs a per-task segment list.
func (c *externalCollectionRefreshChecker) indexWaitDone(job *datapb.ExternalCollectionRefreshJob) bool {
	segments := c.mt.SelectSegments(c.ctx,
		WithCollection(job.GetCollectionId()),
		SegmentFilterFunc(func(s *SegmentInfo) bool {
			// Flushed only, and never L0: createIndexesForSegment skips L0
			// outright, so an L0 segment never acquires an index record and
			// would read as unindexed for as long as the wait lasts.
			return isFlushed(s) && s.GetLevel() != datapb.SegmentLevel_L0
		}))
	segmentIDs := lo.Map(segments, func(s *SegmentInfo, _ int) int64 { return s.GetID() })
	if len(segmentIDs) == 0 {
		return true
	}

	unindexed := c.mt.indexMeta.GetUnindexedSegments(job.GetCollectionId(), segmentIDs)
	if len(unindexed) == 0 {
		return true
	}

	c.nudgeIndexBuilds(job, unindexed)

	held := indexWaitProgressFloor +
		int64(10*(len(segmentIDs)-len(unindexed))/len(segmentIDs))
	if held > 99 {
		held = 99
	}
	// UpdateJobProgress skips a write that changes nothing, so this may be
	// called every tick; it persists at most ten times across a whole wait.
	if err := c.refreshMeta.UpdateJobProgress(job.GetJobId(), held); err != nil {
		mlog.Warn(c.ctx, "failed to update job progress while waiting for indexes",
			mlog.FieldJobID(job.GetJobId()), mlog.Err(err))
	}

	// Log when the band moves, not every tick: a wait lasts for as long as the
	// index builds do, and one line per tick per waiting job says nothing new
	// in between. `job` is the tick's snapshot and can be one tick stale, which
	// only ever costs a duplicated or skipped line - the write above is the
	// authoritative one. The id list is sampled because the debt can be the
	// whole collection.
	if held != job.GetProgress() {
		mlog.Info(c.ctx, "waiting for external collection refresh segments building index...",
			mlog.FieldJobID(job.GetJobId()),
			mlog.FieldCollectionID(job.GetCollectionId()),
			mlog.Int("unindexedCount", len(unindexed)),
			mlog.Int("totalSegments", len(segmentIDs)),
			mlog.Int64s("unindexedSample", lo.Slice(unindexed, 0, 10)))
	}
	return false
}

// nudgeIndexBuilds pushes unindexed segments into the build-acceleration
// channel, exactly as importChecker.checkIndexBuildingJob does. The caller
// synchronizes the refreshed source/spec through RootCoord before reaching
// this function, and index request preparation reads that authoritative owner.
//
// An index build resolves the external source/spec at DISPATCH time, not at
// enqueue time: prepareJobRequest calls ServerHandler.GetCollection, which reads
// authoritative RootCoord metadata, and copies the source/spec into the worker
// request. syncAppliedJobSchema completes the RootCoord update before
// indexWaitDone reaches this nudge; a failed update returns from the checker
// before index debt is evaluated. A build accelerated here therefore observes
// the refreshed endpoint and credentials from the same authoritative owner.
//
// The periodic index inspector may independently discover a segment before the
// refresh reaches this publication point. That pre-existing concurrency is out
// of scope here; this path only guarantees that the refresh wait's own nudge is
// ordered after schema publication.
func (c *externalCollectionRefreshChecker) nudgeIndexBuilds(job *datapb.ExternalCollectionRefreshJob, unindexed []int64) {
	for _, segmentID := range unindexed {
		select {
		case getBuildIndexChSingleton() <- segmentID: // accelerate index building
		default:
		}
	}
}

// finishJob removes planning resources, publishes Finished, and releases task
// results. Cleanup runs first so observing Finished also means the Explore temp
// directory is gone. A cleanup error leaves the non-terminal job for retry.
func (c *externalCollectionRefreshChecker) finishJob(job *datapb.ExternalCollectionRefreshJob) {
	if c.cleanupJobResources != nil {
		if err := c.cleanupJobResources(job.GetJobId()); err != nil {
			mlog.Warn(c.ctx, "failed to clean external collection refresh resources before finish, will retry",
				mlog.FieldJobID(job.GetJobId()),
				mlog.Err(err))
			return
		}
	}

	applied, err := c.refreshMeta.UpdateJobState(job.GetJobId(), indexpb.JobState_JobStateFinished, "")
	if err != nil {
		mlog.Warn(c.ctx, "failed to finish external collection refresh",
			mlog.FieldJobID(job.GetJobId()), mlog.Err(err))
		return
	}
	if !applied {
		return
	}
	if err := c.refreshMeta.ClearTaskResultsByJobID(job.GetJobId()); err != nil {
		mlog.Warn(c.ctx, "failed to clear external collection refresh task results",
			mlog.FieldJobID(job.GetJobId()),
			mlog.Err(err))
	}
	mlog.Info(c.ctx, "external collection refresh finished",
		mlog.FieldJobID(job.GetJobId()),
		mlog.FieldCollectionID(job.GetCollectionId()))
}

// aggregateJobState updates job state based on its tasks.
func (c *externalCollectionRefreshChecker) aggregateJobState(job *datapb.ExternalCollectionRefreshJob) {
	// Skip if job is already in terminal state
	if job.GetState() == indexpb.JobState_JobStateFinished ||
		job.GetState() == indexpb.JobState_JobStateFailed {
		return
	}

	// Get aggregated state from tasks
	state, progress, err := c.refreshMeta.AggregateJobStateFromTasks(job.GetJobId())
	if err != nil {
		applied, updateErr := c.refreshMeta.UpdateJobState(
			job.GetJobId(),
			indexpb.JobState_JobStateFailed,
			err.Error(),
		)
		if updateErr != nil {
			mlog.Warn(c.ctx, "failed to mark invalid external refresh task plan as failed",
				mlog.FieldJobID(job.GetJobId()),
				mlog.Err(updateErr))
			return
		}
		if applied && c.onJobFailed != nil {
			c.onJobFailed(job.GetJobId())
		}
		return
	}
	if state == indexpb.JobState_JobStateNone {
		// No tasks yet
		return
	}

	// The index wait. Off (the default) neither branch is reached and the
	// generic transition below is untouched.
	//
	// Both sit ahead of that transition because while the wait is on, the
	// aggregate says Finished and the job says InProgress on every tick - the
	// generic path would leave the index-wait phase and finish immediately.

	// A job that has already applied its segments belongs to this path for the
	// rest of its life, whatever the parameter says NOW. Keying this on the
	// persisted marker preserves the phase boundary and avoids routing an
	// already-applied job back through generic finalization. Off now simply
	// means release the job: finish it at once without waiting, which is what
	// an operator disabling the hold wants.
	if state == indexpb.JobState_JobStateFinished && job.GetIndexWaitStartedTime() != 0 {
		if !c.syncAppliedJobSchema(job) {
			return
		}
		if c.indexWaitEnabled() && !c.indexWaitDone(job) {
			return
		}
		// Finish without pre-apply: BeginIndexWait already consumed and applied
		// the segment results.
		c.finishJob(job)
		return
	}
	if state == indexpb.JobState_JobStateFinished && c.indexWaitEnabled() {
		if !c.beginIndexWait(job) {
			return
		}
		// Settle the debt in the SAME pass. The eager task path calls this
		// synchronously when the last task finishes, so deferring the first
		// debt evaluation to the periodic tick costs every refresh a full
		// externalCollectionCheckInterval - including a re-scan that changed
		// nothing and whose segments are all already indexed, which is the
		// case this feature's clients hit most often. The debt is knowable the
		// moment the apply lands, so evaluate it now.
		//
		// Re-read rather than reuse the snapshot: the snapshot predates the
		// marker and still carries whatever progress the ingest last reported.
		entered := c.refreshMeta.GetJob(job.GetJobId())
		if entered == nil || !c.indexWaitDone(entered) {
			return
		}
		c.finishJob(entered)
		return
	}

	// Update job if state or progress changed
	if state != job.GetState() {
		// State changed - handle state transition
		var failReason string
		if state == indexpb.JobState_JobStateFailed {
			// Get fail reason from first failed task
			tasks, err := c.refreshMeta.GetCommittedTasksByJobID(job.GetJobId())
			if err != nil {
				failReason = err.Error()
			}
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
					mlog.Warn(c.ctx, "failed to update job progress before failure",
						mlog.FieldJobID(job.GetJobId()),
						mlog.Err(err))
				}
			}
		}

		if state == indexpb.JobState_JobStateFinished {
			latestJob := c.refreshMeta.GetJob(job.GetJobId())
			if latestJob == nil || latestJob.GetState() == indexpb.JobState_JobStateFinished ||
				latestJob.GetState() == indexpb.JobState_JobStateFailed {
				return
			}

			// Concurrent or recovered finalizers may enter this block more than once.
			// Segment publication is not replayed: its transaction consumes the task
			// result references, so later calls observe that the result was applied.
			// The schema publication remains independently idempotent.
			if c.applyJobInfo != nil {
				if err := c.applyJobInfo(c.ctx, latestJob); err != nil {
					c.handleFinalizationError(job.GetJobId(), "apply refresh result", err)
					return
				}
			}
			if c.updateJobSchema != nil {
				if err := c.updateJobSchema(c.ctx, latestJob); err != nil {
					c.handleFinalizationError(job.GetJobId(), "update collection schema", err)
					return
				}
			}

			c.finishJob(latestJob)
			return
		}

		applied, err := c.refreshMeta.UpdateJobState(job.GetJobId(), state, failReason)
		if err != nil {
			mlog.Warn(c.ctx, "failed to update job state from task aggregation",
				mlog.FieldJobID(job.GetJobId()),
				mlog.Err(err))
			return
		}
		if !applied {
			// A concurrent path already drove the job to a terminal state.
			return
		}

		// Fire onJobFailed right after the state transition persists, so
		// per-job resources (explore temp dir) get reclaimed immediately
		// instead of waiting for the retention-gated GC path (default 24h).
		// Cleanup is idempotent, so concurrent calls are harmless.
		if state == indexpb.JobState_JobStateFailed && c.onJobFailed != nil {
			c.onJobFailed(job.GetJobId())
		}

		// For Finished state, UpdateJobState sets Progress=100
		// For Failed state, progress was already persisted above
		// For non-terminal states, update progress if needed
		if state != indexpb.JobState_JobStateFailed && state != indexpb.JobState_JobStateFinished {
			if progress != job.GetProgress() {
				if err := c.refreshMeta.UpdateJobProgress(job.GetJobId(), progress); err != nil {
					mlog.Warn(c.ctx, "failed to update job progress",
						mlog.FieldJobID(job.GetJobId()),
						mlog.Err(err))
				}
			}
		}
	} else if progress != job.GetProgress() {
		// Only progress changed
		if err := c.refreshMeta.UpdateJobProgress(job.GetJobId(), progress); err != nil {
			mlog.Warn(c.ctx, "failed to update job progress",
				mlog.FieldJobID(job.GetJobId()),
				mlog.Err(err))
		}
	}
}

// handleFinalizationError terminates a job only when the error already carries
// an existing permanent classification. Raw catalog/RPC errors and typed
// retriable availability errors stay active for the next checker pass.
func (c *externalCollectionRefreshChecker) handleFinalizationError(jobID int64, operation string, err error) {
	var retryTask *externalRefreshRetryTaskError
	if errors.As(err, &retryTask) {
		applied, updateErr := c.refreshMeta.RetryFinishedTaskOnManifestConflict(retryTask)
		if updateErr != nil {
			mlog.Warn(c.ctx, "failed to persist external refresh manifest-conflict retry",
				mlog.FieldJobID(jobID),
				mlog.FieldTaskID(retryTask.taskID),
				mlog.FieldSegmentID(retryTask.segmentID),
				mlog.Err(updateErr))
			return
		}
		if applied {
			mlog.Info(c.ctx, "external refresh task will retry with a fresh task ID after manifest conflict",
				mlog.FieldJobID(jobID),
				mlog.FieldTaskID(retryTask.taskID),
				mlog.FieldSegmentID(retryTask.segmentID))
		}
		return
	}
	if !isPermanentExternalRefreshFinalizationError(err) {
		mlog.Warn(c.ctx, "external collection refresh finalization failed, will retry",
			mlog.FieldJobID(jobID),
			mlog.String("operation", operation),
			mlog.Err(err))
		return
	}
	applied, updateErr := c.refreshMeta.UpdateJobState(
		jobID,
		indexpb.JobState_JobStateFailed,
		err.Error(),
	)
	if updateErr != nil {
		mlog.Warn(c.ctx, "failed to persist permanent external refresh finalization failure, will retry",
			mlog.FieldJobID(jobID),
			mlog.String("operation", operation),
			mlog.Err(updateErr))
		return
	}
	if !applied {
		return
	}
	mlog.Warn(c.ctx, "external collection refresh finalization failed permanently",
		mlog.FieldJobID(jobID),
		mlog.String("operation", operation),
		mlog.Err(err))
	if c.onJobFailed != nil {
		c.onJobFailed(jobID)
	}
}

func isPermanentExternalRefreshFinalizationError(err error) bool {
	return errors.Is(err, merr.ErrDataIntegrity) ||
		merr.GetErrorType(err) == merr.InputError ||
		merr.IsNonRetryableErr(err)
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

	// A job in the index wait is InProgress like any other, so the state
	// histogram alone cannot tell "still ingesting" from "waiting for indexes"
	// - the distinction an operator looking at a long-lived InProgress count
	// actually needs.
	waitingForIndex := lo.CountBy(lo.Values(jobs), func(job *datapb.ExternalCollectionRefreshJob) bool {
		return job.GetState() == indexpb.JobState_JobStateInProgress &&
			job.GetIndexWaitStartedTime() != 0
	})

	if len(jobs) > 0 {
		mlog.Info(c.ctx, "external collection job stats",
			mlog.Any("stateNum", stateNum),
			mlog.Int("waitingForIndex", waitingForIndex))
	}
}

// timeoutFailReason distinguishes the two timeouts a refresh can hit, because
// they mean opposite things to whoever reads the job.
//
// Timing out during the ingest is the ordinary case: nothing was applied, the
// collection is untouched, re-running starts over. Timing out during the index
// wait is not - the segments were applied when the wait began and are the
// collection's contents already, being served (brute-force scanned for whatever
// is still unindexed). Reporting both as "timeout" makes the second read as if
// nothing had happened, which is the opposite of the truth.
//
// The distinction is also machine-readable without a new API: the job returned
// by DescribeRefresh / ListRefreshJobs carries index_wait_started_time, and a
// non-zero value on a Failed job means exactly "the data landed". That field is
// the contract; this string is best-effort, because the snapshot it reads can
// be a tick behind a concurrent path that just entered the wait - in which case
// the generic message is written while the field still says the truth.
func timeoutFailReason(job *datapb.ExternalCollectionRefreshJob) string {
	if job.GetIndexWaitStartedTime() == 0 {
		return "timeout"
	}
	return "timeout waiting for indexes: the refreshed data is applied and serving, " +
		"but its indexes did not finish within dataCoord.externalCollectionJobTimeout. " +
		"Index building continues on its own; re-running the refresh waits again " +
		"without re-ingesting (index_wait_started_time is set on this job)"
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
		// Once every worker task is Finished, the execution itself is done. A
		// job outside the index wait is only replaying finalization. A job in
		// the index wait may still time out while indexes are outstanding, but
		// once that debt is clear it too is only retrying terminal cleanup.
		// Neither finalization path may be overwritten by the execution timeout.
		state, _, err := c.refreshMeta.AggregateJobStateFromTasks(job.GetJobId())
		if err == nil && state == indexpb.JobState_JobStateFinished {
			if job.GetIndexWaitStartedTime() == 0 ||
				!c.indexWaitEnabled() ||
				c.indexWaitDone(job) {
				mlog.Debug(c.ctx, "skip timeout while finalizing external collection refresh job",
					mlog.FieldJobID(job.GetJobId()))
				return
			}
		}

		mlog.Warn(c.ctx, "external collection job timeout",
			mlog.FieldJobID(job.GetJobId()),
			mlog.FieldCollectionID(job.GetCollectionId()),
			mlog.Duration("age", age),
			mlog.Duration("timeout", timeout))

		// Keep whatever the job last recorded. A job that timed out while
		// retrying a transient explore failure carries the real cause (bad
		// bucket, denied credential); reporting a bare "timeout" would throw
		// away the only thing that tells an operator what to fix.
		reason := timeoutFailReason(job)
		if job.GetIndexWaitStartedTime() == 0 {
			if last := job.GetFailReason(); last != "" {
				reason = fmt.Sprintf("timeout, last failure: %s", last)
			}
		}
		applied, err := c.refreshMeta.UpdateJobState(
			job.GetJobId(),
			indexpb.JobState_JobStateFailed,
			reason)
		if err != nil {
			mlog.Warn(c.ctx, "failed to mark job as timed out",
				mlog.FieldJobID(job.GetJobId()),
				mlog.Err(err))
			return
		}
		if !applied {
			// Terminal-state guard fired — while the checker was about to
			// time out this job a concurrent eager path already transitioned
			// it to Finished/Failed. Let the path that actually persisted the
			// transition perform its terminal callback and task updates.
			mlog.Info(c.ctx, "skip timeout fail path, job already in terminal state",
				mlog.FieldJobID(job.GetJobId()))
			return
		}

		// Also mark all active tasks as failed
		tasks, err := c.refreshMeta.GetCommittedTasksByJobID(job.GetJobId())
		if err != nil {
			mlog.Warn(c.ctx, "failed to resolve committed tasks while timing out refresh job",
				mlog.FieldJobID(job.GetJobId()),
				mlog.Err(err))
			return
		}
		for _, task := range tasks {
			if task.GetState() == indexpb.JobState_JobStateInit ||
				task.GetState() == indexpb.JobState_JobStateRetry ||
				task.GetState() == indexpb.JobState_JobStateInProgress {
				if err := c.refreshMeta.UpdateTaskState(task.GetTaskId(), indexpb.JobState_JobStateFailed, "job timeout"); err != nil {
					mlog.Warn(c.ctx, "failed to mark task failed on job timeout",
						mlog.FieldTaskID(task.GetTaskId()), mlog.Err(err))
				}
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
		mlog.Info(c.ctx, "external collection job has reached GC retention",
			mlog.FieldJobID(job.GetJobId()),
			mlog.FieldCollectionID(job.GetCollectionId()),
			mlog.Duration("age", age),
			mlog.Duration("retention", retention))

		// Clean external objects before dropping the durable job. If cleanup fails,
		// retaining the job gives the next checker pass an authoritative retry anchor.
		if c.cleanupJobResources != nil {
			if err := c.cleanupJobResources(job.GetJobId()); err != nil {
				mlog.Warn(c.ctx, "failed to clean external collection job resources during GC, will retry on next check",
					mlog.FieldJobID(job.GetJobId()),
					mlog.Err(err))
				return
			}
		}

		if c.dropJobTasks != nil {
			if err := c.dropJobTasks(job.GetJobId()); err != nil {
				mlog.Warn(c.ctx, "failed to drop external collection refresh tasks during GC, will retry on next check",
					mlog.FieldJobID(job.GetJobId()),
					mlog.Err(err))
				return
			}
		}

		// DropJob drops job and associated tasks. No in-loop retry: checkGC runs periodically,
		// so the next tick will naturally retry if etcd was temporarily unavailable.
		err := c.refreshMeta.DropJob(c.ctx, job.GetJobId())
		if err != nil {
			mlog.Warn(c.ctx, "failed to remove external collection job during GC, will retry on next check",
				mlog.FieldJobID(job.GetJobId()),
				mlog.Err(err))
			return
		}
		mlog.Info(c.ctx, "external collection job removed", mlog.FieldJobID(job.GetJobId()))
	}
}
