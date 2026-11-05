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

package copysegment

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// Copy Segment Job Checker and State Machine
//
// This file implements the checker component that drives the copy segment job state machine.
// It periodically monitors all copy segment jobs and progresses them through their lifecycle.
//
// JOB STATE MACHINE:
// Pending → Executing → Completed
//    ↓          ↓           ↓
//  Failed    Failed       GC
//    ↓          ↓           ↓
//   GC        GC      (removed)
//
// STATE TRANSITIONS:
// 1. Pending → Executing: Create tasks by grouping segment ID mappings
// 2. Executing → Completed: All tasks completed, update segments to Flushed
// 3. Executing → Failed: Any task failed or job timeout
// 4. Completed/Failed → GC: Remove job and tasks after retention period
//
// TASK CREATION:
// - Pending jobs are split into tasks (max segments per task configurable)
// - Each task contains lightweight ID mappings (source segment → target segment)
// - Tasks are assigned to DataNodes by the inspector component
//
// PROGRESS TRACKING:
// - Monitor task completion and update job progress
// - Collect total row counts from completed segments
// - Report metrics for job and task states
//
// GARBAGE COLLECTION:
// - Completed/Failed jobs are retained for configurable duration
// - Jobs are removed only after all tasks are cleaned up
// - Failed jobs with remaining segments are retained longer
//
// INTEGRATION:
// - Works with Inspector to assign tasks to DataNodes
// - Works with CopySegmentMeta for job/task state persistence
// - Reports metrics for monitoring and alerting

// CopySegmentChecker defines the interface for the copy segment job checker.
// The checker runs in a background goroutine and drives job state transitions.
type CopySegmentChecker interface {
	Start() // Start the background checker loop
	Close() // Stop the checker gracefully
}

type RestoreCompactionScheduler interface {
	EnqueueCompaction(*datapb.CompactionTask) error
	HasCompactionTask(int64) bool
}
type RestoreCompactionTasks interface {
	GetCompactionTasksByTriggerID(int64) []*datapb.CompactionTask
}

// Checker implements the copy segment job state machine and monitoring.
//
// This runs as a background service in DataCoord, checking all copy segment jobs
// periodically and progressing them through their state machine.
type Checker struct {
	Compactions        RestoreCompactionScheduler
	CompactionTasks    RestoreCompactionTasks
	SnapshotSource     SnapshotSource
	CollectionRestorer interface {
		CompleteRestoreCollection(context.Context, int64) error
		IsCollectionDropped(context.Context, int64) (bool, error)
	}
	Ctx      context.Context     // Context for lifecycle management
	Meta     *meta.Meta          // Segment metadata for state updates
	broker   broker.Broker       // Broker for coordinator communication
	alloc    allocator.Allocator // ID allocator for creating tasks
	CopyMeta CopySegmentMeta     // Copy segment job/task metadata store

	closeOnce sync.Once     // Ensures Close is called only once
	closeChan chan struct{} // Channel for signaling shutdown
}

// NewCopySegmentChecker creates a new copy segment job checker.
//
// This is called during DataCoord initialization to set up the checker service.
// The checker must be started explicitly by calling Start().
//
// Parameters:
//   - ctx: Context for lifecycle management
//   - meta: Segment metadata for state updates
//   - broker: Broker for coordinator communication
//   - alloc: ID allocator for creating task IDs
//   - copyMeta: Copy segment job/task metadata store
//
// Returns:
//   - CopySegmentChecker: Initialized checker ready to start
func NewCopySegmentChecker(
	ctx context.Context,
	mt *meta.Meta,
	broker broker.Broker,
	alloc allocator.Allocator,
	copyMeta CopySegmentMeta,
) CopySegmentChecker {
	return &Checker{
		Ctx:       ctx,
		Meta:      mt,
		broker:    broker,
		alloc:     alloc,
		CopyMeta:  copyMeta,
		closeChan: make(chan struct{}),
	}
}

// Start begins the background checker loop that drives job state transitions.
//
// This runs in a goroutine and periodically checks all copy segment jobs,
// progressing them through their state machine. The loop continues until
// Close() is called.
//
// Process flow (each tick):
//  1. Fetch all jobs from metadata store
//  2. For each job, run state-specific checks:
//     - Pending: Create tasks by grouping segments
//     - Executing: Monitor task completion and update progress
//     - Failed: Mark associated tasks as failed
//  3. Check for job timeout (applies to all states)
//  4. Check for garbage collection (Completed/Failed jobs)
//  5. Log job and task statistics with metrics
//
// Tick interval: Configured by CopySegmentCheckInterval parameter (default: 2 seconds)
func (c *Checker) Start() {
	checkInterval := Params.DataCoordCfg.CopySegmentCheckInterval.GetAsDuration(time.Second)
	mlog.Info(c.Ctx, "start copy segment checker", mlog.Duration("checkInterval", checkInterval))
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.closeChan:
			mlog.Info(c.Ctx, "copy segment checker exited")
			return
		case <-ticker.C:
			// Fetch all jobs from metadata
			jobs := c.CopyMeta.GetJobBy(c.Ctx)

			// Process each job based on its state
			for _, job := range jobs {
				switch job.GetState() {
				case datapb.CopySegmentJobState_CopySegmentJobPending:
					c.checkPendingJob(job)
				case datapb.CopySegmentJobState_CopySegmentJobExecuting:
					c.checkCopyingJob(job)
				case datapb.CopySegmentJobState_CopySegmentJobFailed:
					c.CheckFailedJob(job)
				}
				// Check timeout for all states
				c.tryTimeoutJob(job)
				// Check GC for terminal states (Completed/Failed)
				c.checkGC(job)
			}

			// Report statistics and metrics
			c.LogJobStats(jobs)
			c.LogTaskStats()
		}
	}
}

// Close stops the checker gracefully.
// This can be called multiple times safely (only closes once).
func (c *Checker) Close() {
	c.closeOnce.Do(func() {
		close(c.closeChan)
	})
}

// ============================================================================
// Statistics and Metrics
// ============================================================================

// LogJobStats reports job statistics grouped by state.
//
// This reports metrics on every checker tick and logs non-empty job stats.
//
// Metrics reported:
//   - CopySegmentJobs gauge with state label
//   - Counts for Pending, Executing, Completed, Failed states
func (c *Checker) LogJobStats(jobs []CopySegmentJob) {
	// Group jobs by state
	byState := lo.GroupBy(jobs, func(job CopySegmentJob) string {
		return job.GetState().String()
	})

	// Count jobs in each state and report metrics
	stateNum := make(map[string]int)
	for state := range datapb.CopySegmentJobState_value {
		if state == datapb.CopySegmentJobState_CopySegmentJobNone.String() {
			continue
		}
		num := len(byState[state])
		stateNum[state] = num
		metrics.CopySegmentJobs.WithLabelValues(state).Set(float64(num))
	}
	if len(jobs) > 0 {
		mlog.Info(c.Ctx, "copy segment job stats", mlog.Any("stateNum", stateNum))
	}
}

// LogTaskStats reports task statistics grouped by state.
//
// This reports metrics on every checker tick and logs non-empty task stats.
//
// Metrics reported:
//   - CopySegmentTasks gauge with state label
//   - Counts for Pending, InProgress, Completed, Failed states
func (c *Checker) LogTaskStats() {
	// Fetch all tasks from metadata
	tasks := c.CopyMeta.GetTaskBy(c.Ctx)

	// Group tasks by state
	byState := lo.GroupBy(tasks, func(t CopySegmentTask) datapb.CopySegmentTaskState {
		return t.GetState()
	})

	// Count tasks in each state
	pending := len(byState[datapb.CopySegmentTaskState_CopySegmentTaskPending])
	inProgress := len(byState[datapb.CopySegmentTaskState_CopySegmentTaskInProgress])
	completed := len(byState[datapb.CopySegmentTaskState_CopySegmentTaskCompleted])
	failed := len(byState[datapb.CopySegmentTaskState_CopySegmentTaskFailed])

	// Report metrics
	metrics.CopySegmentTasks.WithLabelValues(datapb.CopySegmentTaskState_CopySegmentTaskPending.String()).Set(float64(pending))
	metrics.CopySegmentTasks.WithLabelValues(datapb.CopySegmentTaskState_CopySegmentTaskInProgress.String()).Set(float64(inProgress))
	metrics.CopySegmentTasks.WithLabelValues(datapb.CopySegmentTaskState_CopySegmentTaskCompleted.String()).Set(float64(completed))
	metrics.CopySegmentTasks.WithLabelValues(datapb.CopySegmentTaskState_CopySegmentTaskFailed.String()).Set(float64(failed))

	if len(tasks) > 0 {
		mlog.Info(c.Ctx, "copy segment task stats",
			mlog.Int("pending", pending), mlog.Int("inProgress", inProgress),
			mlog.Int("completed", completed), mlog.Int("failed", failed))
	}
}

// ============================================================================
// State Machine: Pending → Executing
// ============================================================================

// checkRestoreTarget uses the same authoritative lifecycle check as Import.
// Public HasCollection/Describe cannot distinguish a still-restoring target from
// a deleted one, and process-local DataView drop markers do not survive recovery.
func (c *Checker) checkRestoreTarget(job CopySegmentJob) bool {
	if !job.(*Job).GetCreatesCollection() {
		return true
	}
	dropped, err := c.CollectionRestorer.IsCollectionDropped(c.Ctx, job.GetCollectionId())
	if err != nil {
		mlog.Warn(c.Ctx, "retrying restore target lifecycle check", mlog.FieldJobID(job.GetJobId()), mlog.Err(err))
		return false
	}
	if !dropped {
		return true
	}
	if err := c.CopyMeta.FailJobForDroppedCollection(c.Ctx, job.GetJobId()); err != nil {
		mlog.Warn(c.Ctx, "retrying restore termination for deleted target", mlog.FieldJobID(job.GetJobId()), mlog.Err(err))
	}
	return false
}

// checkPendingJob transitions job from Pending to Executing by creating tasks.
//
// This is the first state transition in the job lifecycle. It groups segment ID
// mappings into tasks (to avoid tasks that are too large) and creates task metadata.
// The actual file copying is triggered later by the inspector component.
//
// Process flow:
//  1. Check if tasks already exist (idempotent - don't create duplicates)
//  2. Validate job has segment mappings (empty jobs are marked completed)
//  3. Split mappings into groups (max segments per task configurable)
//  4. For each group:
//     a. Allocate task ID
//     b. Create task metadata with lightweight ID mappings
//     c. Save task to metadata store
//  5. Update job state to Executing with initial progress (0/total)
//
// Task grouping:
//   - Controlled by MaxSegmentsPerCopyTask parameter
//   - Prevents tasks from becoming too large and timing out
//   - Enables parallel execution across multiple DataNodes
//
// Why lightweight ID mappings:
//   - Task metadata only stores source→target segment ID mappings
//   - Full segment metadata (binlogs, indexes) is fetched by DataNode when executing
//   - Keeps task metadata small and efficient to persist
//
// Idempotency and crash recovery:
//   - Task creation is a multi-step sequence (per-group AllocID + AddTask, then
//     the Executing transition), each step persisted individually. A failure
//     mid-way (etcd hiccup, DataCoord restart) leaves the job Pending with only
//     a subset of tasks persisted.
//   - To be resume-safe, each round creates tasks only for source segments not
//     yet covered by persisted tasks, then (re-)applies the idempotent
//     Pending → Executing transition.
func (c *Checker) checkPendingJob(job CopySegmentJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobId()))

	// Step 0: Re-read the cached job. The `job` argument is a snapshot taken
	// before this function ran, but tasks of a Pending job can already be
	// dispatched (the inspector does not filter by job state) and a concurrent
	// failure (markTaskAndJobFailed) may have moved the job to a terminal
	// state and released its snapshot pin. Creating more tasks for such a job
	// would be wasted work at best.
	current := c.CopyMeta.GetJob(c.Ctx, job.GetJobId())
	if current == nil {
		log.Info(c.Ctx, "job no longer exists, skip pending check")
		return
	}
	if current.GetState() != datapb.CopySegmentJobState_CopySegmentJobPending {
		log.Info(c.Ctx, "job is no longer pending, skip pending check",
			mlog.String("currentState", current.GetState().String()))
		return
	}

	if !c.checkRestoreTarget(current) {
		return
	}

	if job.(*Job).GetCreatesCollection() && len(job.GetIdMappings()) > 0 {
		if err := c.registerRestoreSegments(job); err != nil {
			log.Warn(c.Ctx, "retrying restored segment registration", mlog.Err(err))
			return
		}
	}

	// Step 1: Validate job has segment mappings
	idMappings := job.GetIdMappings()
	if len(idMappings) == 0 && job.(*Job).GetCreatesCollection() {
		c.finishJob(job, 0)
		return
	}
	if len(idMappings) == 0 {
		log.Warn(c.Ctx, "no id mappings to copy, mark job as completed")
		if err := c.CopyMeta.UpdateJobStateAndReleaseRef(c.Ctx, job.GetJobId(),
			UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted),
			UpdateCopyJobReason("no segments to copy")); err != nil {
			log.Error(c.Ctx, "failed to update empty job state to Completed", mlog.Err(err))
		}
		return
	}

	// Step 2: Compute source segments already covered by persisted tasks,
	// so a partially created job resumes instead of duplicating tasks.
	tasks := c.CopyMeta.GetTasksByJobID(c.Ctx, job.GetJobId())
	coveredSourceIDs := make(map[int64]struct{})
	for _, task := range tasks {
		for _, mapping := range task.GetIdMappings() {
			coveredSourceIDs[mapping.GetSourceSegmentId()] = struct{}{}
		}
	}
	pendingMappings := lo.Filter(idMappings, func(mapping *datapb.CopySegmentIDMapping, _ int) bool {
		_, covered := coveredSourceIDs[mapping.GetSourceSegmentId()]
		return !covered
	})

	// Step 3: Split uncovered mappings into groups (max segments per task)
	maxSegmentsPerTask := Params.DataCoordCfg.MaxSegmentsPerCopyTask.GetAsInt()
	groups := lo.Chunk(pendingMappings, maxSegmentsPerTask)

	// Step 4: Create task for each group
	for i, group := range groups {
		taskID, err := c.alloc.AllocID(c.Ctx)
		if err != nil {
			log.Warn(c.Ctx, "failed to alloc task ID", mlog.Err(err))
			return
		}

		// Create task with lightweight ID mappings
		task := &Task{
			CopyMeta: c.CopyMeta,
			Tr:       timerecord.NewTimeRecorder("copy segment task"),
			Times:    taskcommon.NewTimes(),
		}
		task.Task.Store(&datapb.CopySegmentTask{
			TaskId:       taskID,
			JobId:        job.GetJobId(),
			CollectionId: job.GetCollectionId(),
			NodeId:       meta.NullNodeID,                                    // Not assigned yet
			TaskVersion:  0,                                                  // Initial version
			TaskSlot:     1,                                                  // Each copy task uses 1 slot
			State:        datapb.CopySegmentTaskState_CopySegmentTaskPending, // Initial state
			Reason:       "",
			IdMappings:   group, // Lightweight: only source→target segment IDs
			CreatedTs:    uint64(time.Now().UnixNano()),
			CompleteTs:   0,
		})

		// Save task to metadata store
		err = c.CopyMeta.AddTask(c.Ctx, task)
		if err != nil {
			log.Warn(c.Ctx, "failed to add copy segment task",
				mlog.Int("groupIndex", i),
				mlog.Int("segmentCount", len(group)),
				mlog.Err(err))
			return
		}
		log.Info(c.Ctx, "created copy segment task",
			mlog.FieldTaskID(taskID),
			mlog.Int("groupIndex", i),
			mlog.Int("segmentCount", len(group)))
	}

	// Step 5: Update job state to Executing. This also runs when all segments
	// were already covered (groups is empty), retrying a transition that a
	// previous round failed to persist.
	// The transition is state-guarded (Pending -> Executing only): a task
	// dispatched during Step 4 can fail concurrently, and markTaskAndJobFailed
	// then moves the job to Failed and releases its snapshot pin. An
	// unconditional update here would resurrect that Failed job as Executing.
	updated, err := c.CopyMeta.UpdateJobInState(c.Ctx, job.GetJobId(),
		datapb.CopySegmentJobState_CopySegmentJobPending,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobExecuting),
		UpdateCopyJobTotalSegments(int64(len(idMappings))))
	if err != nil {
		log.Warn(c.Ctx, "failed to update job state to Executing", mlog.Err(err))
		return
	}
	if !updated {
		log.Info(c.Ctx, "job left Pending state concurrently, skip transition to Executing")
		return
	}
	log.Info(c.Ctx, "copy segment job started",
		mlog.Int("newTaskCount", len(groups)),
		mlog.Int("resumedTaskCount", len(tasks)),
		mlog.Int("totalSegments", len(idMappings)))
}

// ============================================================================
// State Machine: Executing → Completed/Failed
// ============================================================================

// checkCopyingJob monitors task progress and transitions job to Completed or Failed.
//
// This is called periodically for jobs in Executing state. It monitors all associated
// tasks and updates job progress. When all tasks complete successfully, it transitions
// the job to Completed. If any task fails, it transitions to Failed immediately.
//
// Process flow:
//  1. Fetch all tasks for this job
//  2. Count tasks by state (Completed/Failed)
//  3. Update job progress if changed (copiedSegments/totalSegments)
//  4. Check for failures:
//     - If any task failed → mark job as Failed
//  5. Check for completion:
//     - If all tasks completed → finish job (collect rows, update segments, mark Completed)
//  6. Otherwise → wait for more tasks to complete
//
// Progress tracking:
//   - copiedSegments = sum of segments in Completed tasks
//   - totalSegments = total segments in job
//   - Progress is updated only when changed (avoid unnecessary metadata writes)
//
// Fail-fast behavior:
//   - Any task failure immediately fails the entire job
//   - Remaining tasks will be marked as Failed by checkFailedJob
//
// Completion:
//   - Collects total row count from all target segments
//   - Updates all target segments to Flushed state (makes them queryable)
//   - Records completion timestamp and metrics
func (c *Checker) checkCopyingJob(job CopySegmentJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobId()))
	if !c.checkRestoreTarget(job) {
		return
	}

	// Step 1: Fetch all tasks for this job
	tasks := c.CopyMeta.GetTasksByJobID(c.Ctx, job.GetJobId())
	totalTasks := len(tasks)
	completedTasks := 0
	failedTasks := 0
	copiedSegments := int64(0)
	totalSegments := int64(len(job.GetIdMappings()))

	// Step 2: Count tasks by state
	for _, task := range tasks {
		switch task.GetState() {
		case datapb.CopySegmentTaskState_CopySegmentTaskCompleted:
			completedTasks++
			copiedSegments += int64(len(task.GetIdMappings()))
		case datapb.CopySegmentTaskState_CopySegmentTaskFailed:
			failedTasks++
		}
	}

	// Step 3: Update job progress if changed
	if copiedSegments != job.GetCopiedSegments() {
		err := c.CopyMeta.UpdateJob(c.Ctx, job.GetJobId(),
			UpdateCopyJobProgress(copiedSegments, totalSegments))
		if err != nil {
			log.Warn(c.Ctx, "failed to update job progress", mlog.Err(err))
		} else {
			log.Debug(c.Ctx, "updated job progress",
				mlog.Int64("copiedSegments", copiedSegments),
				mlog.Int64("totalSegments", totalSegments),
				mlog.Int("completedTasks", completedTasks),
				mlog.Int("totalTasks", totalTasks))
		}
	}

	// Step 4: Check for failures (fail-fast)
	if failedTasks > 0 {
		log.Warn(c.Ctx, "copy segment job has failed tasks",
			mlog.Int("failedTasks", failedTasks),
			mlog.Int("totalTasks", totalTasks))
		if err := c.CopyMeta.UpdateJobStateAndReleaseRef(c.Ctx, job.GetJobId(),
			UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
			UpdateCopyJobReason(fmt.Sprintf("%d/%d tasks failed", failedTasks, totalTasks))); err != nil {
			log.Error(c.Ctx, "failed to update job state to Failed", mlog.Err(err))
		}
		return
	}

	// Step 5: Wait for all tasks to complete
	if completedTasks < totalTasks {
		log.Debug(c.Ctx, "waiting for copy segment tasks to complete",
			mlog.Int("completed", completedTasks),
			mlog.Int("total", totalTasks))
		return
	}

	if job.(*Job).GetCreatesCollection() && !allRestoreSegmentsCopied(job, tasks) {
		return
	}

	// Step 6: All tasks completed - collect total rows and finish job
	var totalRows int64
	for _, task := range tasks {
		for _, mapping := range task.GetIdMappings() {
			targetSegID := mapping.GetTargetSegmentId()
			segment := c.Meta.GetSegment(c.Ctx, targetSegID)
			if segment != nil {
				totalRows += segment.GetNumOfRows()
			}
		}
	}

	c.finishJob(job, totalRows)
}

// finishJob completes the job by updating segments to Flushed and marking job as Completed.
//
// This is called when all tasks have completed successfully. It performs the final
// steps to make the copied segments visible for querying.
//
// Process flow:
//  1. Collect all target segment IDs from task ID mappings
//  2. Update each target segment state to Flushed (makes them queryable)
//  3. Update job state to Completed with completion timestamp and total rows
//  4. Record job latency metrics
//
// Why update segments to Flushed:
//   - Copied segments start in Growing state (not queryable)
//   - Flushed state makes them available for query operations
//   - This is the final step to complete the restore operation
//
// Parameters:
//   - job: The job to finish
//   - totalRows: Total row count across all copied segments
func (c *Checker) finishJob(job CopySegmentJob, totalRows int64) {
	log := mlog.With(mlog.FieldJobID(job.GetJobId()))
	if job.(*Job).GetCreatesCollection() {
		ready, err := c.materializeRestoreDeletes(job)
		if err != nil || !ready {
			log.Warn(c.Ctx, "waiting for restored delete materialization", mlog.Err(err))
			return
		}
	}

	// Step 1: Collect all target segment IDs from task ID mappings
	tasks := c.CopyMeta.GetTasksByJobID(c.Ctx, job.GetJobId())
	targetSegmentIDs := make([]int64, 0)
	for _, task := range tasks {
		for _, mapping := range task.GetIdMappings() {
			targetSegmentIDs = append(targetSegmentIDs, mapping.GetTargetSegmentId())
		}
	}

	// Step 2: Update segment states to Flushed (make them visible for query)
	var flushFailures int
	if len(targetSegmentIDs) > 0 {
		for _, segID := range targetSegmentIDs {
			segment := c.Meta.GetSegment(c.Ctx, segID)
			if job.(*Job).GetCreatesCollection() {
				// L0 inputs have been retired by the materialization transaction.
				// Never resurrect them while publishing the restored L1 bases.
				if segment == nil {
					flushFailures++
					continue
				}
				if segment.GetLevel() == datapb.SegmentLevel_L0 {
					continue // Retain the retirement proof until Job completion is durable.
				}
				if err := c.Meta.UpdateSegmentsInfo(c.Ctx, meta.UpdateIsImporting(segID, false)); err != nil {
					flushFailures++
				}
				continue
			}
			if segment != nil && segment.GetState() != commonpb.SegmentState_Flushed {
				op := meta.UpdateStatusOperator(segID, commonpb.SegmentState_Flushed)
				if err := c.Meta.UpdateSegmentsInfo(c.Ctx, op); err != nil {
					log.Error(c.Ctx, "failed to update segment state to Flushed",
						mlog.FieldSegmentID(segID),
						mlog.Err(err))
					flushFailures++
				} else {
					log.Info(c.Ctx, "updated segment state to Flushed",
						mlog.FieldSegmentID(segID))
					meta.NotifySegmentIndexBuild(segID)
					meta.NotifySegmentStatsBuild(segID)
				}
			}
		}
	}

	// Step 3: Fail the job if any segment flush failed (prevents silent data availability issues)
	if flushFailures > 0 && job.(*Job).GetCreatesCollection() {
		return // Copied data is durable; retry metadata publication.
	}
	if flushFailures > 0 {
		reason := fmt.Sprintf("%d/%d segments failed to flush to Flushed state", flushFailures, len(targetSegmentIDs))
		log.Error(c.Ctx, "finishJob: failing job due to segment flush failures",
			mlog.Int("flushFailures", flushFailures),
			mlog.Int("totalSegments", len(targetSegmentIDs)))
		if err := c.CopyMeta.UpdateJobStateAndReleaseRef(c.Ctx, job.GetJobId(),
			UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
			UpdateCopyJobReason(reason)); err != nil {
			log.Error(c.Ctx, "failed to update job state to Failed after flush failures", mlog.Err(err))
		}
		return
	}

	c.publishRestoredCollection(job, totalRows)
}

func (c *Checker) publishRestoredCollection(job CopySegmentJob, totalRows int64) {
	log := mlog.With(mlog.FieldJobID(job.GetJobId()))
	// Completion is downstream of the durable DataView publication. The target
	// remains unavailable if reconciliation fails; a checker retry resumes it.
	if c.Meta.DataViewManager != nil {
		version, err := c.Meta.DataViewManager.RecomputeNow(c.Ctx, job.GetCollectionId(), c.Meta.LoadableProjection)
		if err != nil || version == nil {
			if !c.checkRestoreTarget(job) {
				return
			}
			log.Warn(c.Ctx, "waiting for restored DataView publication", mlog.Err(err))
			return
		}
	}
	if job.(*Job).GetCreatesCollection() {
		if err := c.CollectionRestorer.CompleteRestoreCollection(c.Ctx, job.GetCollectionId()); err != nil {
			if !c.checkRestoreTarget(job) {
				return
			}
			log.Warn(c.Ctx, "waiting to publish restored collection", mlog.Err(err))
			return
		}
	}

	// Step 4: Update job state to Completed
	completeTs := uint64(time.Now().UnixNano())
	err := c.CopyMeta.UpdateJobStateAndReleaseRef(c.Ctx, job.GetJobId(),
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted),
		UpdateCopyJobCompleteTs(completeTs),
		UpdateCopyJobTotalRows(totalRows))
	if err != nil {
		log.Error(c.Ctx, "failed to update job state to Completed", mlog.Err(err))
		return
	}

	// Step 4: Record metrics
	totalDuration := job.GetTR().ElapseSpan()
	metrics.CopySegmentJobLatency.Observe(float64(totalDuration.Milliseconds()))
	log.Info(c.Ctx, "copy segment job completed",
		mlog.Int64("totalRows", totalRows),
		mlog.Duration("totalDuration", totalDuration))
}

// ============================================================================
// State Machine: Failed Job Handling
// ============================================================================

// checkFailedJob marks all pending/in-progress tasks as failed when job fails.
//
// This ensures that when a job fails (due to timeout or task failures),
// all remaining tasks are also marked as failed. This prevents orphaned
// tasks from continuing to execute.
//
// Process flow:
//  1. Find all Pending/InProgress tasks for this job
//  2. Mark each task as Failed with job's failure reason
//  3. Inspector will trigger cleanup for failed tasks
//
// Why mark tasks as failed:
//   - Prevents orphaned tasks from continuing execution
//   - Enables inspector to trigger cleanup (DropCopySegment)
//   - Maintains consistent state across job and tasks
func (c *Checker) CheckFailedJob(job CopySegmentJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobId()))

	// Find all Pending/InProgress tasks
	allTasks := c.CopyMeta.GetTasksByJobID(c.Ctx, job.GetJobId())
	tasks := lo.Filter(allTasks, func(t CopySegmentTask, _ int) bool {
		return t.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskPending ||
			t.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskInProgress
	})

	if len(tasks) == 0 {
		return
	}

	log.Warn(c.Ctx, "copy segment job has failed, marking all tasks as failed",
		mlog.String("reason", job.GetReason()),
		mlog.Int("taskCount", len(tasks)))

	// Mark each task as failed
	for _, task := range tasks {
		err := c.CopyMeta.UpdateTask(c.Ctx, task.GetTaskId(),
			UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed),
			UpdateCopyTaskReason(job.GetReason()))
		if err != nil {
			log.Warn(c.Ctx, "failed to update task state to failed",
				WrapCopySegmentTaskLog(task, mlog.Err(err))...)
		}
	}
}

// ============================================================================
// Job Timeout and Garbage Collection
// ============================================================================

// tryTimeoutJob checks if job has exceeded timeout and marks it as failed.
//
// Only applies to non-terminal jobs (Pending/Executing).
// Timeout prevents jobs from running indefinitely due to stuck tasks.
//
// Timeout is set when job is created based on configuration.
func (c *Checker) tryTimeoutJob(job CopySegmentJob) {
	job = c.CopyMeta.GetJob(c.Ctx, job.GetJobId())
	if job == nil {
		return
	}
	// Only apply timeout to non-terminal jobs
	switch job.GetState() {
	case datapb.CopySegmentJobState_CopySegmentJobPending,
		datapb.CopySegmentJobState_CopySegmentJobExecuting:
		// Continue to check timeout
	default:
		// Skip timeout check for terminal states (Completed/Failed)
		return
	}

	if job.(*Job).GetCreatesCollection() && allRestoreSegmentsCopied(job, c.CopyMeta.GetTasksByJobID(c.Ctx, job.GetJobId())) {
		return // Only idempotent metadata publication remains.
	}
	timeoutTime := tsoutil.PhysicalTime(job.GetTimeoutTs())
	if job.GetTimeoutTs() == 0 || time.Now().Before(timeoutTime) {
		return
	}

	mlog.Warn(c.Ctx, "copy segment job timeout",
		mlog.FieldJobID(job.GetJobId()),
		mlog.Time("timeoutTime", timeoutTime))
	if err := c.CopyMeta.UpdateJobStateAndReleaseRef(c.Ctx, job.GetJobId(),
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
		UpdateCopyJobReason("timeout")); err != nil {
		mlog.Error(c.Ctx, "failed to update timed-out job state to Failed",
			mlog.FieldJobID(job.GetJobId()), mlog.Err(err))
	}
}

// checkGC performs garbage collection for completed/failed jobs.
//
// Jobs and tasks are retained for a configurable duration (CopySegmentTaskRetention)
// to allow users to query job status. After retention expires, they are removed
// from metadata store.
//
// Process flow:
//  1. Check if job is in terminal state (Completed/Failed)
//  2. Check if cleanup time has passed
//  3. For each task:
//     a. Skip if job failed and task has segments in metadata (wait for cleanup)
//     b. Skip if task is still assigned to a node (wait for unassignment)
//     c. Remove task from metadata
//  4. If all tasks removed, remove job from metadata
//
// Why wait conditions:
//   - Failed jobs with segments: Wait for segment cleanup before removing task metadata
//   - Tasks on nodes: Wait for inspector to unassign before removing
//   - This ensures all resources are properly cleaned before removing metadata
//
// Retention period: Configured by CopySegmentTaskRetention parameter (default: 10800s = 3 hours)
func (c *Checker) checkGC(job CopySegmentJob) {
	// Only GC terminal states
	if job.GetState() != datapb.CopySegmentJobState_CopySegmentJobCompleted &&
		job.GetState() != datapb.CopySegmentJobState_CopySegmentJobFailed {
		return
	}
	if job.(*Job).GetCreatesCollection() {
		for _, mapping := range job.GetIdMappings() {
			segment := c.Meta.GetSegment(c.Ctx, mapping.GetTargetSegmentId())
			if segment != nil && segment.GetIsImporting() && segment.GetCompacted() {
				if err := c.Meta.UpdateSegmentsInfo(c.Ctx, meta.UpdateIsImporting(segment.GetID(), false)); err != nil {
					mlog.Warn(c.Ctx, "retrying restored L0 retirement cleanup", mlog.Err(err))
					return
				}
			}
		}
	}

	cleanupTime := tsoutil.PhysicalTime(job.GetCleanupTs())
	if time.Now().After(cleanupTime) {
		log := mlog.With(mlog.FieldJobID(job.GetJobId()))
		GCRetention := Params.DataCoordCfg.CopySegmentTaskRetention.GetAsDuration(time.Second)
		log.Info(c.Ctx, "copy segment job has reached GC retention",
			mlog.Time("cleanupTime", cleanupTime), mlog.Duration("GCRetention", GCRetention))

		tasks := c.CopyMeta.GetTasksByJobID(c.Ctx, job.GetJobId())
		shouldRemoveJob := true

		for _, task := range tasks {
			if task.GetCleanupRequired() {
				shouldRemoveJob = false
				continue
			}
			// If job failed and task has target segments in meta, don't remove yet
			// (wait for segments to be cleaned up first)
			if job.GetState() == datapb.CopySegmentJobState_CopySegmentJobFailed {
				hasSegments := false
				for _, mapping := range task.GetIdMappings() {
					segment := c.Meta.GetSegment(c.Ctx, mapping.GetTargetSegmentId())
					if segment != nil {
						hasSegments = true
						break
					}
				}
				if hasSegments {
					shouldRemoveJob = false
					continue
				}
			}

			// If task is still assigned to a node, don't remove yet
			// (wait for inspector to unassign)
			if task.GetNodeId() != meta.NullNodeID {
				shouldRemoveJob = false
				continue
			}

			// Remove task from metadata
			err := c.CopyMeta.RemoveTask(c.Ctx, task.GetTaskId())
			if err != nil {
				log.Warn(c.Ctx, "failed to remove copy segment task during GC",
					WrapCopySegmentTaskLog(task, mlog.Err(err))...)
				shouldRemoveJob = false
				continue
			}
			log.Info(c.Ctx, "copy segment task removed", WrapCopySegmentTaskLog(task)...)
		}

		// Remove job only if all tasks removed
		if !shouldRemoveJob {
			return
		}

		err := c.CopyMeta.RemoveJob(c.Ctx, job.GetJobId())
		if err != nil {
			log.Warn(c.Ctx, "failed to remove copy segment job", mlog.Err(err))
			return
		}
		log.Info(c.Ctx, "copy segment job removed")
	}
}
