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
	ctx         context.Context
	refreshMeta *externalCollectionRefreshMeta
	closeChan   chan struct{}
	// The manager-side hooks. Embedded so the checker keeps addressing them
	// as c.onJobFinished etc.
	refreshCheckerHooks

	// indexGates is the per-job bookkeeping of the index gate. The wait gets
	// its own clock (enteredAt): the ingest spent most of the job timeout
	// already, and failing a COMPLETED ingest because index building started
	// near the deadline would throw the work away. Guarded by indexGateMu:
	// processJob runs both on the periodic checker tick and on the eager
	// per-task path (processJobByID), which are different goroutines. Lost
	// on restart, which merely restarts the wait clock.
	//
	// An entry is released only AFTER the job's terminal transition has
	// PERSISTED (releaseIndexGate at every persist site, with GC as the
	// backstop) - never when the debt merely clears. Releasing early opens a
	// window where a transiently failed finish write leaves the job
	// InProgress with no gate clock, and the next timeout check falls back
	// to the ingest clock and fails a completed, fully indexed refresh.
	indexGateMu sync.Mutex
	indexGates  map[int64]indexGateState
}

// refreshCheckerHooks bundles the manager-side callbacks the checker fires.
// A struct with named fields rather than a positional parameter list: several
// callbacks share a signature, so a call site transposing two of them would
// still compile.
type refreshCheckerHooks struct {
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
	// unindexedSegments answers which of the given segments still lack a
	// finished index, from datacoord's index meta. Injected so the index
	// gate is testable without a full meta; nil disables the gate outright.
	unindexedSegments func(collectionID int64, segmentIDs []int64) []int64
}

// indexGateState is the index gate's per-job bookkeeping. Stored by value in
// the map so readers copy a consistent snapshot under indexGateMu.
type indexGateState struct {
	// enteredAt is when the gate began waiting for indexes - zero while the
	// pre-gate segment apply is still failing (the wait clock must not start
	// until an apply landed, or the job timeout would lose its terminal-bound
	// role over a never-applying job).
	enteredAt time.Time
	// appliedAt is when the pre-gate segment apply landed. Non-zero means
	// the refreshed segments are the collection's committed contents: the
	// terminal transition must not replay the apply (a transient replay
	// failure would persist this committed refresh as Failed, and a replay
	// re-patch erases text/JSON-key stats rebuilt during the hold), and the
	// job is exempt from the ingest-clock timeout.
	appliedAt time.Time
	// applyTriedAt is when THIS process first attempted the pre-gate apply.
	// A failing apply retries on this clock, not the persisted StartTime:
	// after a DataCoord restart the StartTime is typically already past the
	// job timeout, and judging the first post-restart attempt on it would
	// fail a committed refresh while object storage / etcd are still
	// warming up.
	applyTriedAt time.Time
	// entryInFlight dedups the entry pass across the eager and periodic
	// paths: without it both goroutines can observe an empty gate and each
	// run a full apply (duplicate object-storage reads and meta writes).
	entryInFlight bool
	// segmentIDs is the one-time snapshot of the segments the refresh
	// produced, taken from the result store at gate entry. The results are
	// immutable once committed, so held ticks reuse this instead of
	// re-reading the store (object-storage I/O per task) every tick.
	segmentIDs []int64
	// lastApplyErr remembers the most recent pre-gate apply failure. The
	// gate retries every tick because the failure may be transient, but a
	// permanent one (a validation error in the task results) then rides the
	// retry loop into the job timeout - which must surface this cause
	// instead of a bare "timeout".
	lastApplyErr string
}

func newRefreshChecker(
	ctx context.Context,
	refreshMeta *externalCollectionRefreshMeta,
	closeChan chan struct{},
	hooks refreshCheckerHooks,
) *externalCollectionRefreshChecker {
	return &externalCollectionRefreshChecker{
		ctx:                 ctx,
		refreshMeta:         refreshMeta,
		closeChan:           closeChan,
		refreshCheckerHooks: hooks,
		indexGates:          make(map[int64]indexGateState),
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

// releaseIndexGate drops a job's index-gate clock. Called only after a
// terminal state (Finished/Failed) has PERSISTED, plus GC as the backstop;
// releasing before the persist would hand a transiently failed finish write
// back to the ingest clock (see the indexGateEntered field comment).
func (c *externalCollectionRefreshChecker) releaseIndexGate(jobID int64) {
	c.indexGateMu.Lock()
	delete(c.indexGates, jobID)
	c.indexGateMu.Unlock()
}

// loadIndexGate returns a snapshot of the job's gate bookkeeping.
func (c *externalCollectionRefreshChecker) loadIndexGate(jobID int64) (indexGateState, bool) {
	c.indexGateMu.Lock()
	defer c.indexGateMu.Unlock()
	gs, ok := c.indexGates[jobID]
	return gs, ok
}

// ensureGateApplied runs the gate's entry pass at most once per process: it
// lands the segment apply and snapshots the produced segment ids into the
// gate state. Returns the gate snapshot and whether the caller may proceed to
// judge index debt - false while the apply is failing (retried next tick,
// with the error recorded for the timeout fail reason) or while another
// goroutine (eager vs periodic) is mid-entry.
func (c *externalCollectionRefreshChecker) ensureGateApplied(job *datapb.ExternalCollectionRefreshJob) (indexGateState, bool) {
	jobID := job.GetJobId()
	c.indexGateMu.Lock()
	gs := c.indexGates[jobID]
	if !gs.appliedAt.IsZero() {
		c.indexGateMu.Unlock()
		return gs, true
	}
	if gs.entryInFlight {
		c.indexGateMu.Unlock()
		return gs, false
	}
	gs.entryInFlight = true
	if gs.applyTriedAt.IsZero() {
		gs.applyTriedAt = time.Now()
	}
	c.indexGates[jobID] = gs
	c.indexGateMu.Unlock()

	if c.applyJobInfo != nil {
		if err := c.applyJobInfo(c.ctx, job); err != nil {
			// Possibly transient (a catalog write): retry next tick, bounded
			// by the job-timeout param on the applyTriedAt clock - and the
			// recorded error rides along so a permanent failure that
			// exhausts that budget reports its actual cause.
			mlog.Warn(c.ctx, "failed to apply refresh segments ahead of the index gate; retrying next tick",
				mlog.FieldJobID(jobID), mlog.Err(err))
			c.indexGateMu.Lock()
			gs = c.indexGates[jobID]
			gs.entryInFlight = false
			gs.lastApplyErr = err.Error()
			c.indexGates[jobID] = gs
			c.indexGateMu.Unlock()
			return gs, false
		}
	}
	segIDs := c.refreshedSegmentIDs(job)

	c.indexGateMu.Lock()
	gs = c.indexGates[jobID]
	gs.entryInFlight = false
	gs.lastApplyErr = ""
	gs.appliedAt = time.Now()
	gs.segmentIDs = segIDs
	c.indexGates[jobID] = gs
	c.indexGateMu.Unlock()
	return gs, true
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
		if applied {
			c.releaseIndexGate(job.GetJobId())
			if c.onJobFailed != nil {
				c.onJobFailed(job.GetJobId())
			}
		}
		return
	}
	if state == indexpb.JobState_JobStateNone {
		// No tasks yet
		return
	}

	// The index gate, ahead of the Finished transition. With
	// refreshWaitForIndex on, a refresh whose ingest finished but whose new
	// segments are not yet indexed is not done: marking it Finished would
	// broadcast the schema update and expose those segments to queries that
	// can only brute-force them.
	//
	// The segments are APPLIED to the collection meta first - the same
	// idempotent apply the Finished transition replays - because the index
	// machinery can only see, judge and build segments that exist in
	// meta.segments: judging them before the apply reads every new segment
	// as unindexed forever (the inspector drops unknown ids), which is a
	// held job that can never advance. Only then is the debt computed, from
	// the tasks' externalized results (snapshotted once at gate entry);
	// stragglers go to the index-build
	// acceleration channel and the job holds InProgress with progress
	// tracking the indexed fraction (90-100) until a later pass finds the
	// debt cleared - or the gate's own budget expired - and lets the
	// Finished transition run, whose own apply replay is a no-op by then.
	if state == indexpb.JobState_JobStateFinished && Params.DataCoordCfg.RefreshWaitForIndex.GetAsBool() {
		// Entry pass at most once per process: land the apply and snapshot
		// the produced segment ids. The results are immutable once
		// committed, so held ticks reuse the snapshot - replaying the apply
		// and re-reading the results (object-storage I/O per task) on every
		// tick of a potentially hours-long hold is waste.
		gs, proceed := c.ensureGateApplied(job)
		if !proceed {
			return
		}
		var unindexed []int64
		if len(gs.segmentIDs) > 0 && c.unindexedSegments != nil {
			unindexed = c.unindexedSegments(job.GetCollectionId(), gs.segmentIDs)
		}
		if len(unindexed) > 0 {
			total := len(gs.segmentIDs)
			c.indexGateMu.Lock()
			cur := c.indexGates[job.GetJobId()]
			if cur.enteredAt.IsZero() {
				cur.enteredAt = time.Now()
			}
			c.indexGates[job.GetJobId()] = cur
			entered := cur.enteredAt
			c.indexGateMu.Unlock()
			// The gate enforces its own budget (the job-timeout param on the
			// gate's clock) and expires into FINISHED, not Failed: the
			// segments were applied at gate entry and are the collection's
			// committed contents already, so failing would misreport
			// committed work and skip the schema broadcast for data that is
			// actually being served. The index backlog keeps building in the
			// background; only the wait for it ends.
			if waited := time.Since(entered); waited > Params.DataCoordCfg.ExternalCollectionJobTimeout.GetAsDuration(time.Second) {
				mlog.Warn(c.ctx, "index gate expired; finishing the refresh with segments still unindexed",
					mlog.FieldJobID(job.GetJobId()),
					mlog.FieldCollectionID(job.GetCollectionId()),
					mlog.Duration("waited", waited),
					mlog.Int64s("unindexedSegments", unindexed))
				// Fall through to the Finished transition below.
			} else {
				for _, segID := range unindexed {
					select {
					case getBuildIndexChSingleton() <- segID:
					default:
					}
				}
				held := int64(90)
				if total > 0 {
					held = 90 + int64(10*(total-len(unindexed))/total)
				}
				// Keep progress monotonic for pollers: the task-average
				// ingest progress can legitimately sit above the indexed
				// fraction when the hold begins, and must never show 100
				// while held - pollers treat 100 as done - so a carried-over
				// 100 pins to 99 until the gate opens.
				if cur := job.GetProgress(); cur > held {
					held = cur
				}
				if held > 99 {
					held = 99
				}
				if held != job.GetProgress() {
					if err := c.refreshMeta.UpdateJobProgress(job.GetJobId(), held); err != nil {
						mlog.Warn(c.ctx, "failed to update job progress while waiting for indexes",
							mlog.FieldJobID(job.GetJobId()), mlog.Err(err))
					}
				}
				return
			}
		}
		// Debt cleared: fall through to the Finished transition WITHOUT
		// releasing the gate clock. The release happens only after the
		// transition persists; if the finish write fails transiently, the
		// retained clock keeps the next timeout check on the gate's clock
		// instead of the (likely exhausted) ingest clock.
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
			// When the index gate already landed the apply at entry, do NOT
			// replay it as a pre-apply: UpdateJobStateWithPreApply persists a
			// pre-apply failure as terminal Failed, so a transient
			// result-store or catalog error on the release tick would fail a
			// committed refresh (segments applied and served, schema
			// broadcast permanently skipped) - and a replay re-patch erases
			// text/JSON-key stats rebuilt during the hold.
			preApply := func(latestJob *datapb.ExternalCollectionRefreshJob) error {
				return c.applyJobInfo(c.ctx, latestJob)
			}
			if gs, ok := c.loadIndexGate(job.GetJobId()); ok && !gs.appliedAt.IsZero() {
				preApply = nil
			}
			applied, err := c.refreshMeta.UpdateJobStateWithPreApply(
				job.GetJobId(),
				state,
				failReason,
				preApply)
			if err != nil {
				mlog.Warn(c.ctx, "failed to apply external collection refresh result",
					mlog.FieldJobID(job.GetJobId()),
					mlog.Err(err))
				// applied here means the pre-apply failure was persisted as a
				// Failed terminal state - release the gate clock with it.
				if applied {
					c.releaseIndexGate(job.GetJobId())
					if c.onJobFailed != nil {
						c.onJobFailed(job.GetJobId())
					}
				}
				return
			}
			if !applied {
				// A concurrent path already drove the job into a terminal state
				// and owns the one-time segment apply / callback side effects.
				return
			}
			// Terminal state persisted: the gate clock (if any) may go now.
			c.releaseIndexGate(job.GetJobId())

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

		// Terminal state persisted through the plain path (Finished without
		// an applyJobInfo hook, or Failed): release the gate clock with it.
		if state == indexpb.JobState_JobStateFinished || state == indexpb.JobState_JobStateFailed {
			c.releaseIndexGate(job.GetJobId())
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
		mlog.Info(c.ctx, "external collection job stats", mlog.Any("stateNum", stateNum))
	}
}

// tryTimeoutJob checks if job has exceeded timeout and marks it as failed.
func (c *externalCollectionRefreshChecker) tryTimeoutJob(job *datapb.ExternalCollectionRefreshJob) {
	// Skip if StartTime is not set
	if job.GetStartTime() == 0 {
		return
	}

	// A job whose gate apply has landed is exempt from the ingest timeout:
	// its ingest COMPLETED and its segments are the collection's committed
	// contents, so failing it here would misreport committed work. The gate
	// enforces its own budget (same param, the gate's clock) inside
	// aggregateJobState and always terminates the job - the debt clears,
	// the read fails open, or the gate expires into Finished.
	gs, _ := c.loadIndexGate(job.GetJobId())
	if !gs.appliedAt.IsZero() {
		return
	}

	// Get timeout configuration
	timeout := Params.DataCoordCfg.ExternalCollectionJobTimeout.GetAsDuration(time.Second)

	// Calculate job age. A job whose pre-gate apply is still failing runs on
	// the applyTriedAt clock - when THIS process first attempted the apply -
	// not the persisted StartTime: after a DataCoord restart the StartTime
	// is typically already past the timeout, and judging the first
	// post-restart attempt on it would fail a committed refresh while object
	// storage / etcd are still warming up. The recorded apply error
	// surfaces in the fail reason below when the retry budget runs out.
	startTime := time.UnixMilli(job.GetStartTime())
	if !gs.applyTriedAt.IsZero() {
		startTime = gs.applyTriedAt
	}
	age := time.Since(startTime)

	if age > timeout {
		mlog.Warn(c.ctx, "external collection job timeout",
			mlog.FieldJobID(job.GetJobId()),
			mlog.FieldCollectionID(job.GetCollectionId()),
			mlog.Duration("age", age),
			mlog.Duration("timeout", timeout))

		failReason := "timeout"
		if gs.lastApplyErr != "" {
			failReason = "timeout; the index-gate segment apply kept failing, last error: " + gs.lastApplyErr
		}
		applied, err := c.refreshMeta.UpdateJobState(
			job.GetJobId(),
			indexpb.JobState_JobStateFailed,
			failReason)
		if err != nil {
			mlog.Warn(c.ctx, "failed to mark job as timed out",
				mlog.FieldJobID(job.GetJobId()),
				mlog.Err(err))
			return
		}
		if !applied {
			// Terminal-state guard fired — while the checker was about to
			// time out this job a concurrent eager path already transitioned
			// it to Finished/Failed. Firing onJobFailed here would poison
			// the manager's notifiedJobs dedup map and cause a subsequent
			// handleJobFinished to skip the schemaUpdater. Bail out and let
			// the path that actually persisted the transition do cleanup.
			mlog.Info(c.ctx, "skip timeout fail path, job already in terminal state",
				mlog.FieldJobID(job.GetJobId()))
			return
		}
		// Failed state persisted: release the gate clock with it.
		c.releaseIndexGate(job.GetJobId())

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
		// Backstop release of the index-gate clock: every terminal persist
		// site releases it already, but a terminal state written by a path
		// that bypasses the checker would otherwise leak the entry.
		c.releaseIndexGate(job.GetJobId())
		// Release per-job bookkeeping in the manager (notifiedJobs dedup map)
		// so it stays bounded across DataCoord lifetime.
		if c.onJobGC != nil {
			c.onJobGC(job.GetJobId())
		}
	}
}

// refreshedSegmentIDs snapshots the ids of the segments the job's tasks
// produced. Read from the RESULT store, not the task headers: a runtime task
// externalizes its results and writes the header's UpdatedSegments back as
// nil, so reading the headers sees an empty set and the gate silently never
// holds. A read failure reports no segments - the gate fails open rather than
// holding the job on a guess.
func (c *externalCollectionRefreshChecker) refreshedSegmentIDs(job *datapb.ExternalCollectionRefreshJob) []int64 {
	if c.unindexedSegments == nil {
		// No index meta wired - the gate is inert, skip the read.
		return nil
	}
	tasks, err := c.refreshMeta.GetCommittedTaskResultsByJobID(job.GetJobId())
	if err != nil {
		mlog.Warn(c.ctx, "failed to read task results for the index gate; not holding the job on a guess",
			mlog.FieldJobID(job.GetJobId()), mlog.Err(err))
		return nil
	}
	segIDs := make([]int64, 0)
	for _, task := range tasks {
		for _, seg := range task.GetUpdatedSegments() {
			segIDs = append(segIDs, seg.GetID())
		}
	}
	return segIDs
}
