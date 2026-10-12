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

	"github.com/milvus-io/milvus/pkg/v3/mlog"
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
	ctx context.Context
	// mt is datacoord's segment/index meta, read directly for the index wait -
	// the same way importChecker reads it. Nil in tests that do not exercise
	// the wait.
	mt          *meta
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
	// is idempotent — the manager dedups on its own cleanup key so concurrent
	// eager and periodic paths only fire one cleanup per jobID. That key is
	// deliberately NOT the schema-publish key: a job that applied its
	// segments and then failed still owes the publish.
	onJobFailed func(jobID int64)
	// onJobGC is invoked after the checker successfully drops a job during
	// GC so the manager can release any per-job bookkeeping (the publish and
	// cleanup dedup entries). Keeps those maps bounded across DataCoord
	// lifetime.
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
	mt *meta,
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
		mt:               mt,
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
// one transition under the job lock - two catalog writes, ordered, not atomic;
// see BeginIndexWait. The job stays InProgress; processJob fires the finished
// callback right after aggregateJobState returns, which publishes the refreshed
// source/spec exactly as the ungated path does.
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
		mlog.Warn(c.ctx, "failed to apply external collection refresh result",
			mlog.FieldJobID(job.GetJobId()),
			mlog.Err(err))
		// applied here means the failure was persisted as a terminal Failed
		// state, exactly as on the ungated path.
		if applied && c.onJobFailed != nil {
			c.onJobFailed(job.GetJobId())
		}
		return false
	}
	if !applied {
		// Either a concurrent path already drove the job terminal, or it
		// already entered the wait - BeginIndexWait rejects a second entry
		// under the job lock. Both mean another caller owns the one-time
		// side effects.
		return false
	}
	// Release the task results now, not at the end of the wait. They are dead
	// weight the moment the apply lands - the marker guarantees it is never
	// replayed, and the state aggregate reads task states, never results - and
	// they are not small: each carries a SegmentInfo per produced segment,
	// inline in the task's catalog record plus a blob in the result store.
	// Holding them for the length of an index build, and for the retention
	// period on top of that when a job times out mid-wait, is a cost the
	// ungated path never pays, because there the apply and Finished are the
	// same transition and this clear follows immediately.
	if err := c.refreshMeta.ClearTaskResultsByJobID(job.GetJobId()); err != nil {
		// Not fatal: finishAfterIndexWait clears again, and DropJob sweeps the
		// job prefix at GC.
		mlog.Warn(c.ctx, "failed to clear external collection refresh task results on index wait entry",
			mlog.FieldJobID(job.GetJobId()),
			mlog.Err(err))
	}

	mlog.Info(c.ctx, "external collection refresh applied, waiting for its segments to be indexed",
		mlog.FieldJobID(job.GetJobId()),
		mlog.FieldCollectionID(job.GetCollectionId()))
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
// channel, exactly as importChecker.checkIndexBuildingJob does - but only once
// DataCoord's collection meta already carries THIS refresh's source/spec.
//
// An index build resolves the external source/spec at DISPATCH time, not at
// enqueue time: prepareJobRequest calls handler.GetCollection and copies
// schema.GetExternalSource()/GetExternalSpec() into the CreateJobRequest, and
// on the worker those drive the external filesystem endpoint, bucket and
// credentials. The publish is a round trip (AlterCollection -> WAL ack ->
// BroadcastAlteredCollection -> meta.AddCollection) while the scheduler ticks
// every 100ms, so a nudge issued before the publish lands dispatches builds
// against the PRE-refresh location. Those builds fail terminally, and nothing
// retries them: createIndexesForSegment skips an index that already has a
// segIndex record, and GetUnindexedSegments only counts Finished - so the
// segment never becomes indexed and the wait burns the whole job timeout.
//
// The gate is authoritative because it reads the same place the dispatch will:
// ServerHandler.GetCollection is a plain meta.GetCollection. A refresh that
// does not change source/spec passes it immediately and loses no acceleration.
// A collection missing from meta reads as "not yet" - the dispatch would resolve
// it from RootCoord and be safe, but skipping one nudge only costs latency and
// the periodic index inspector still creates the tasks either way.
//
// This narrows the window; it does not close it. The inspector's own 60s tick
// scans every flushed segment and needs no notification, so a build can still
// be created inside the window without this nudge. That hazard predates this
// feature and is out of scope here - what this guarantees is that the wait does
// not actively dispatch into it.
func (c *externalCollectionRefreshChecker) nudgeIndexBuilds(job *datapb.ExternalCollectionRefreshJob, unindexed []int64) {
	if !c.refreshedSchemaVisible(job) {
		mlog.Debug(c.ctx, "refreshed external schema not visible yet, holding the index build nudge",
			mlog.FieldJobID(job.GetJobId()),
			mlog.FieldCollectionID(job.GetCollectionId()))
		return
	}
	for _, segmentID := range unindexed {
		select {
		case getBuildIndexChSingleton() <- segmentID: // accelerate index building
		default:
		}
	}
}

// refreshedSchemaVisible reports whether DataCoord's collection meta already
// carries this refresh's external source/spec - i.e. whether an index build
// dispatched now would read the refreshed location rather than the previous one.
func (c *externalCollectionRefreshChecker) refreshedSchemaVisible(job *datapb.ExternalCollectionRefreshJob) bool {
	if c.mt == nil {
		return false
	}
	collection := c.mt.GetCollection(job.GetCollectionId())
	if collection == nil || collection.Schema == nil {
		return false
	}
	return collection.Schema.GetExternalSource() == job.GetExternalSource() &&
		collection.Schema.GetExternalSpec() == job.GetExternalSpec()
}

// finishAfterIndexWait completes a job whose wait is over. No pre-apply: the
// segments were applied when the wait began.
func (c *externalCollectionRefreshChecker) finishAfterIndexWait(job *datapb.ExternalCollectionRefreshJob) {
	applied, err := c.refreshMeta.UpdateJobState(job.GetJobId(), indexpb.JobState_JobStateFinished, "")
	if err != nil {
		mlog.Warn(c.ctx, "failed to finish external collection refresh after the index wait",
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
	mlog.Info(c.ctx, "external collection refresh finished, its segments are indexed",
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
	// generic path would replay the apply and finish the job immediately.

	// A job that has already applied its segments belongs to this path for the
	// rest of its life, whatever the parameter says NOW. This is the only exit
	// that does not re-run the apply, so keying it on the parameter instead of
	// the marker would make turning the parameter off mid-wait replay the
	// apply - and applyExternalRefreshPatch clears TextStatsLogs/JsonKeyStats,
	// so that replay would discard indexes built during the very wait it was
	// disabling. Off now simply means release the job: finish it at once
	// without waiting, which is what an operator disabling the hold wants.
	if state == indexpb.JobState_JobStateFinished && job.GetIndexWaitStartedTime() != 0 {
		if c.indexWaitEnabled() && !c.indexWaitDone(job) {
			return
		}
		// Finish WITHOUT a pre-apply: the segments were applied by
		// BeginIndexWait, and replaying that here would be a second apply of
		// the same results.
		c.finishAfterIndexWait(job)
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
		c.finishAfterIndexWait(entered)
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

		if state == indexpb.JobState_JobStateFinished && c.applyJobInfo != nil {
			applied, err := c.refreshMeta.UpdateJobStateWithPreApply(
				job.GetJobId(),
				state,
				failReason,
				func(latestJob *datapb.ExternalCollectionRefreshJob) error {
					return c.applyJobInfo(c.ctx, latestJob)
				})
			if err != nil {
				mlog.Warn(c.ctx, "failed to apply external collection refresh result",
					mlog.FieldJobID(job.GetJobId()),
					mlog.Err(err))
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
				mlog.Warn(c.ctx, "failed to clear external collection refresh task results",
					mlog.FieldJobID(job.GetJobId()),
					mlog.Err(err))
			}

			// processJobs calls ensureJobFinishedNotified right after this
			// function returns, so we don't fire the callback here.
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
	if latestJob == nil {
		return
	}
	// Finished, OR applied - waiting for indexes, or done waiting one way or
	// the other. The callback publishes the refreshed source/spec and reclaims
	// the explore temp dir, and both become due the moment the segments are
	// applied - which with the index wait on happens before Finished. Waiting
	// for Finished would hold the schema back for the whole wait, and index
	// builds take the external source/spec from the collection schema.
	// Publishing here narrows the window in which a build can read the previous
	// source/spec; it does not close it, because the index inspector's own tick
	// needs no notification. See nudgeIndexBuilds.
	//
	// The marker also holds for a job that FAILED after applying (it outran
	// the job timeout mid-wait). Such a job still owes the publish: its
	// segments are the collection's contents and are being served, so the
	// schema must describe them - Failed only says the wait did not finish in
	// budget. That is why the Failed path may not claim the publish dedup key;
	// see handleJobFailed.
	if latestJob.GetState() != indexpb.JobState_JobStateFinished &&
		latestJob.GetIndexWaitStartedTime() == 0 {
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
		mlog.Warn(c.ctx, "external collection job timeout",
			mlog.FieldJobID(job.GetJobId()),
			mlog.FieldCollectionID(job.GetCollectionId()),
			mlog.Duration("age", age),
			mlog.Duration("timeout", timeout))

		applied, err := c.refreshMeta.UpdateJobState(
			job.GetJobId(),
			indexpb.JobState_JobStateFailed,
			timeoutFailReason(job))
		if err != nil {
			mlog.Warn(c.ctx, "failed to mark job as timed out",
				mlog.FieldJobID(job.GetJobId()),
				mlog.Err(err))
			return
		}
		if !applied {
			// Terminal-state guard fired — while the checker was about to
			// time out this job a concurrent eager path already transitioned
			// it to Finished/Failed. Bail out and let the path that actually
			// persisted the transition own the one-time side effects.
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
		mlog.Info(c.ctx, "external collection job has reached GC retention",
			mlog.FieldJobID(job.GetJobId()),
			mlog.FieldCollectionID(job.GetCollectionId()),
			mlog.Duration("age", age),
			mlog.Duration("retention", retention))

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
		// Release per-job bookkeeping in the manager (the publish and cleanup
		// dedup entries) so it stays bounded across DataCoord lifetime.
		if c.onJobGC != nil {
			c.onJobGC(job.GetJobId())
		}
	}
}
