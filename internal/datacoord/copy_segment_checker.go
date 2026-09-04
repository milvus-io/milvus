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
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
// - Executing jobs are checked for task creation and completion
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

// copySegmentChecker implements the copy segment job state machine and monitoring.
//
// This runs as a background service in DataCoord, checking all copy segment jobs
// periodically and progressing them through their state machine.
type copySegmentChecker struct {
	ctx       context.Context      // Context for lifecycle management
	meta      *meta                // Segment metadata for state updates
	broker    broker.Broker        // Broker for coordinator communication
	alloc     allocator.Allocator  // ID allocator for creating tasks
	copyMeta  CopySegmentMeta      // Copy segment job/task metadata store
	cluster   session.Cluster      // Worker cluster, for retrying a drop the scheduler could not land
	scheduler task.GlobalScheduler // Fences worker callbacks before terminal task GC

	lifecycleMu sync.Mutex
	started     bool
	stopped     bool
	wg          sync.WaitGroup
	closeChan   chan struct{} // Channel for signaling shutdown
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
//   - cluster: DataNode RPC dispatcher used by terminal cleanup
//   - scheduler: Global task scheduler that drains callbacks before cleanup
//
// Returns:
//   - CopySegmentChecker: Initialized checker ready to start
func NewCopySegmentChecker(
	ctx context.Context,
	meta *meta,
	broker broker.Broker,
	alloc allocator.Allocator,
	copyMeta CopySegmentMeta,
	cluster session.Cluster,
	scheduler task.GlobalScheduler,
) CopySegmentChecker {
	return &copySegmentChecker{
		ctx:       ctx,
		meta:      meta,
		broker:    broker,
		alloc:     alloc,
		copyMeta:  copyMeta,
		cluster:   cluster,
		scheduler: scheduler,
		closeChan: make(chan struct{}),
	}
}

// Start begins the background checker loop that drives job state transitions.
//
// This starts tracked goroutines that periodically check all copy segment jobs,
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
func (c *copySegmentChecker) Start() {
	c.lifecycleMu.Lock()
	if c.started || c.stopped {
		c.lifecycleMu.Unlock()
		return
	}
	c.started = true
	c.wg.Add(2)
	c.lifecycleMu.Unlock()

	checkInterval := Params.DataCoordCfg.CopySegmentCheckInterval.GetAsDuration(time.Second)
	mlog.Info(c.ctx, "start copy segment checker", mlog.Duration("checkInterval", checkInterval))

	go func() {
		defer c.wg.Done()
		c.runGCLoop(checkInterval)
	}()
	go func() {
		defer c.wg.Done()
		c.runStateMachineLoop(checkInterval)
	}()
}

// runGCLoop is isolated from job progression because each worker drop attempt
// is a synchronous RPC. It checks closeChan between jobs so shutdown waits
// for at most the RPC already in flight, not the whole retained-job backlog.
func (c *copySegmentChecker) runGCLoop(checkInterval time.Duration) {
	gcTicker := time.NewTicker(checkInterval)
	defer gcTicker.Stop()
	for {
		select {
		case <-c.closeChan:
			return
		case <-gcTicker.C:
			for _, job := range c.copyMeta.GetJobBy(c.ctx) {
				select {
				case <-c.closeChan:
					return
				default:
				}
				c.checkGC(job)
			}
		}
	}
}

func (c *copySegmentChecker) runStateMachineLoop(checkInterval time.Duration) {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.closeChan:
			mlog.Info(c.ctx, "copy segment checker exited")
			return
		case <-ticker.C:
			// Fetch all jobs from metadata
			jobs := c.copyMeta.GetJobBy(c.ctx)

			// Process each job based on its state
			for _, job := range jobs {
				switch job.GetState() {
				case datapb.CopySegmentJobState_CopySegmentJobPending:
					c.checkPendingJob(job)
				case datapb.CopySegmentJobState_CopySegmentJobExecuting:
					c.checkCopyingJob(job)
				case datapb.CopySegmentJobState_CopySegmentJobFailed:
					c.checkFailedJob(job)
				}
				// Check timeout for all states
				c.tryTimeoutJob(job)
			}

			// Report statistics and metrics
			c.LogJobStats(jobs)
			c.LogTaskStats()
		}
	}
}

// Close stops the checker gracefully.
// This can be called multiple times safely (only closes once).
func (c *copySegmentChecker) Close() {
	c.lifecycleMu.Lock()
	if !c.stopped {
		c.stopped = true
		close(c.closeChan)
	}
	c.lifecycleMu.Unlock()
	c.wg.Wait()
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
func (c *copySegmentChecker) LogJobStats(jobs []CopySegmentJob) {
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
		mlog.Debug(c.ctx, "copy segment job stats", mlog.Any("stateNum", stateNum))
	}
}

// LogTaskStats reports task statistics grouped by state.
//
// This reports metrics on every checker tick and logs non-empty task stats.
//
// Metrics reported:
//   - CopySegmentTasks gauge with state label
//   - Counts for Pending, InProgress, Completed, Failed states
func (c *copySegmentChecker) LogTaskStats() {
	// Fetch all tasks from metadata
	tasks := c.copyMeta.GetTaskBy(c.ctx)

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
		mlog.Info(c.ctx, "copy segment task stats",
			mlog.Int("pending", pending), mlog.Int("inProgress", inProgress),
			mlog.Int("completed", completed), mlog.Int("failed", failed))
	}
}

// ============================================================================
// State Machine: Pending → Executing
// ============================================================================

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
//     mid-way leaves the job Pending with only a subset of tasks persisted.
//   - Each round creates tasks only for source segments not already covered by
//     persisted tasks, then retries the Pending -> Executing transition.
func (c *copySegmentChecker) checkPendingJob(job CopySegmentJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobId()))

	// Re-read because the argument is a snapshot taken before this function ran.
	current := c.copyMeta.GetJob(c.ctx, job.GetJobId())
	if current == nil {
		log.Info(c.ctx, "job no longer exists, skip pending check")
		return
	}
	if current.GetState() != datapb.CopySegmentJobState_CopySegmentJobPending {
		log.Info(c.ctx, "job is no longer pending, skip pending check",
			mlog.String("currentState", current.GetState().String()))
		return
	}

	idMappings := job.GetIdMappings()
	if len(idMappings) == 0 {
		log.Warn(c.ctx, "no id mappings to copy, mark job as completed")
		if _, err := c.copyMeta.UpdateJobStateAndReleasePin(c.ctx, job.GetJobId(),
			datapb.CopySegmentJobState_CopySegmentJobPending,
			UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted),
			UpdateCopyJobReason("no segments to copy")); err != nil {
			log.Error(c.ctx, "failed to update empty job state to Completed", mlog.Err(err))
		}
		return
	}

	tasks := c.copyMeta.GetTasksByJobID(c.ctx, job.GetJobId())
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

	maxSegmentsPerTask := Params.DataCoordCfg.MaxSegmentsPerCopyTask.GetAsInt()
	groups := lo.Chunk(pendingMappings, maxSegmentsPerTask)
	for i, group := range groups {
		taskID, err := c.alloc.AllocID(c.ctx)
		if err != nil {
			log.Warn(c.ctx, "failed to alloc task ID", mlog.Err(err))
			return
		}

		task := &copySegmentTask{
			copyMeta: c.copyMeta,
			tr:       timerecord.NewTimeRecorder("copy segment task"),
			times:    taskcommon.NewTimes(),
		}
		task.task.Store(&datapb.CopySegmentTask{
			TaskId:       taskID,
			JobId:        job.GetJobId(),
			CollectionId: job.GetCollectionId(),
			NodeId:       NullNodeID,                                         // Not assigned yet
			TaskVersion:  0,                                                  // Initial version
			TaskSlot:     1,                                                  // Each copy task uses 1 slot
			State:        datapb.CopySegmentTaskState_CopySegmentTaskPending, // Initial state
			Reason:       "",
			IdMappings:   group, // Lightweight: only source→target segment IDs
			CreatedTs:    uint64(time.Now().UnixNano()),
			CompleteTs:   0,
		})

		if err := c.copyMeta.AddTask(c.ctx, task); err != nil {
			log.Warn(c.ctx, "failed to add copy segment task",
				mlog.Int("groupIndex", i),
				mlog.Int("segmentCount", len(group)),
				mlog.Err(err))
			return
		}
		log.Info(c.ctx, "created copy segment task",
			mlog.FieldTaskID(taskID),
			mlog.Int("groupIndex", i),
			mlog.Int("segmentCount", len(group)))
	}

	updated, err := c.copyMeta.UpdateJobInState(c.ctx, job.GetJobId(),
		datapb.CopySegmentJobState_CopySegmentJobPending,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobExecuting),
		UpdateCopyJobTotalSegments(int64(len(idMappings))))
	if err != nil {
		log.Warn(c.ctx, "failed to update job state to Executing", mlog.Err(err))
		return
	}
	if !updated {
		log.Info(c.ctx, "job left Pending state concurrently, skip transition to Executing")
		return
	}
	log.Info(c.ctx, "copy segment job started",
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
func (c *copySegmentChecker) checkCopyingJob(job CopySegmentJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobId()))

	// Step 1: Fetch all tasks for this job
	tasks := c.copyMeta.GetTasksByJobID(c.ctx, job.GetJobId())
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
		err := c.copyMeta.UpdateJob(c.ctx, job.GetJobId(),
			UpdateCopyJobProgress(copiedSegments, totalSegments))
		if err != nil {
			log.Warn(c.ctx, "failed to update job progress", mlog.Err(err))
		} else {
			log.Debug(c.ctx, "updated job progress",
				mlog.Int64("copiedSegments", copiedSegments),
				mlog.Int64("totalSegments", totalSegments),
				mlog.Int("completedTasks", completedTasks),
				mlog.Int("totalTasks", totalTasks))
		}
	}

	// Step 4: Check for failures (fail-fast)
	if failedTasks > 0 {
		log.Warn(c.ctx, "copy segment job has failed tasks",
			mlog.Int("failedTasks", failedTasks),
			mlog.Int("totalTasks", totalTasks))
		if _, err := c.copyMeta.UpdateJobStateAndReleasePin(c.ctx, job.GetJobId(),
			datapb.CopySegmentJobState_CopySegmentJobExecuting,
			UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
			UpdateCopyJobReason(fmt.Sprintf("%d/%d tasks failed", failedTasks, totalTasks))); err != nil {
			log.Error(c.ctx, "failed to update job state to Failed", mlog.Err(err))
		}
		return
	}

	// Step 5: Wait for all tasks to complete
	if completedTasks < totalTasks {
		log.Debug(c.ctx, "waiting for copy segment tasks to complete",
			mlog.Int("completed", completedTasks),
			mlog.Int("total", totalTasks))
		return
	}

	var totalRows int64
	for _, task := range tasks {
		for _, mapping := range task.GetIdMappings() {
			targetSegID := mapping.GetTargetSegmentId()
			segment := c.meta.GetSegment(c.ctx, targetSegID)
			if segment != nil {
				totalRows += segment.GetNumOfRows()
			}
		}
	}

	c.finishJob(job, totalRows, tasks)
}

// finishJob completes the job by publishing all target segments and the job in
// one catalog update.
//
// This is called when all tasks have completed successfully. It performs the final
// steps to make the copied segments visible for querying.
//
// Process flow:
//  1. Collect all target segment IDs from task ID mappings
//  2. Update every target to Flushed and not-importing
//  3. Update the job to Completed with completion timestamp and total rows
//  4. Commit those changes atomically, then record job latency metrics
//
// Why update segments to Flushed:
//   - Copied segments start in Growing state (not queryable)
//   - Flushed state makes them available for query operations
//   - This is the final step to complete the restore operation
func (c *copySegmentChecker) finishJob(job CopySegmentJob, totalRows int64, tasks []CopySegmentTask) {
	log := mlog.With(mlog.FieldJobID(job.GetJobId()))
	targetSegmentIDs := make([]int64, 0)
	for _, task := range tasks {
		for _, mapping := range task.GetIdMappings() {
			targetSegmentIDs = append(targetSegmentIDs, mapping.GetTargetSegmentId())
		}
	}

	completeTs := uint64(time.Now().UnixNano())
	applied, err := c.copyMeta.CompleteJob(c.ctx, job.GetJobId(), targetSegmentIDs,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted),
		UpdateCopyJobCompleteTs(completeTs),
		UpdateCopyJobTotalRows(totalRows))
	if err != nil {
		// All targets remain importing, so retrying this final catalog operation
		// on the next checker interval is safe and does not expose a partial job.
		log.Error(c.ctx, "failed to publish completed copy segment job", mlog.Err(err))
		return
	}
	if !applied {
		return
	}

	totalDuration := job.GetTR().ElapseSpan()
	metrics.CopySegmentJobLatency.Observe(float64(totalDuration.Milliseconds()))
	log.Info(c.ctx, "copy segment job completed",
		mlog.Int64("totalRows", totalRows),
		mlog.Int("targetSegments", len(targetSegmentIDs)),
		mlog.Duration("totalDuration", totalDuration))
}

// ============================================================================
// State Machine: Failed Job Handling
// ============================================================================

// checkFailedJob stops active tasks and drops every target of a failed job.
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
func (c *copySegmentChecker) checkFailedJob(job CopySegmentJob) {
	log := mlog.With(mlog.FieldJobID(job.GetJobId()))

	allTasks := c.copyMeta.GetTasksByJobID(c.ctx, job.GetJobId())
	tasks := lo.Filter(allTasks, func(t CopySegmentTask, _ int) bool {
		return t.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskPending ||
			t.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskInProgress
	})

	if len(tasks) > 0 {
		log.Warn(c.ctx, "copy segment job has failed, marking all tasks as failed",
			mlog.String("reason", job.GetReason()),
			mlog.Int("taskCount", len(tasks)))

		for _, task := range tasks {
			err := c.copyMeta.UpdateTask(c.ctx, task.GetTaskId(),
				UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed),
				UpdateCopyTaskReason(job.GetReason()))
			if err != nil {
				log.Warn(c.ctx, "failed to update task state to failed",
					WrapCopySegmentTaskLog(task, mlog.Err(err))...)
			}
		}
	}

	// Use the job mapping as the cleanup inventory because task creation may
	// have stopped after only a prefix of the mappings was scheduled.
	operators := make([]UpdateOperator, 0, len(job.GetIdMappings())*2)
	for _, mapping := range job.GetIdMappings() {
		if mapping == nil || c.meta.GetSegment(c.ctx, mapping.GetTargetSegmentId()) == nil {
			continue
		}
		operators = append(operators,
			UpdateStatusOperator(mapping.GetTargetSegmentId(), commonpb.SegmentState_Dropped),
			UpdateIsImporting(mapping.GetTargetSegmentId(), false))
	}
	if err := c.meta.UpdateSegmentsInfo(c.ctx, operators...); err != nil {
		log.Warn(c.ctx, "failed to retire targets of failed copy segment job; will retry",
			mlog.Err(err))
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
func (c *copySegmentChecker) tryTimeoutJob(job CopySegmentJob) {
	// Only apply timeout to non-terminal jobs
	switch job.GetState() {
	case datapb.CopySegmentJobState_CopySegmentJobPending,
		datapb.CopySegmentJobState_CopySegmentJobExecuting:
		// Continue to check timeout
	default:
		// Skip timeout check for terminal states (Completed/Failed)
		return
	}

	timeoutTime := tsoutil.PhysicalTime(job.GetTimeoutTs())
	if job.GetTimeoutTs() == 0 || time.Now().Before(timeoutTime) {
		return
	}

	mlog.Warn(c.ctx, "copy segment job timeout",
		mlog.FieldJobID(job.GetJobId()),
		mlog.Time("timeoutTime", timeoutTime))
	if _, err := c.copyMeta.UpdateJobStateAndReleasePin(c.ctx, job.GetJobId(), job.GetState(),
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
		UpdateCopyJobReason("timeout")); err != nil {
		mlog.Error(c.ctx, "failed to update timed-out job state to Failed",
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
//     a. Retain it while a failed job still has unretired mapped targets
//     b. If it is assigned, ask its worker to drop it
//     c. Retain it when the drop fails, so the next GC round retries
//     d. Remove it after the drop succeeds or its worker is gone
//  4. If all tasks removed, remove job from metadata
//
// Why wait conditions:
//   - Failed jobs with segments: Wait for segment cleanup before removing task metadata
//   - Tasks on nodes: Wait until the drop is acknowledged or the worker is gone
//   - This preserves durable retry evidence while worker cleanup is still uncertain
//
// Retention period: Configured by CopySegmentTaskRetention parameter (default: 10800s = 3 hours)
func (c *copySegmentChecker) checkGC(job CopySegmentJob) {
	// Only GC terminal states
	if job.GetState() != datapb.CopySegmentJobState_CopySegmentJobCompleted &&
		job.GetState() != datapb.CopySegmentJobState_CopySegmentJobFailed {
		return
	}

	// Terminal job metadata is the durable owner of a permanent restore pin.
	// Retry release on every checker round and never GC the job while PinId is
	// still present; otherwise a transient unpin failure would become permanent.
	if err := c.copyMeta.ReleaseJobPin(c.ctx, job.GetJobId()); err != nil {
		mlog.Warn(c.ctx, "failed to release terminal copy segment job pin; will retry",
			mlog.FieldJobID(job.GetJobId()), mlog.Err(err))
		return
	}

	cleanupTime := tsoutil.PhysicalTime(job.GetCleanupTs())
	if time.Now().After(cleanupTime) {
		log := mlog.With(mlog.FieldJobID(job.GetJobId()))
		gcRetention := Params.DataCoordCfg.CopySegmentTaskRetention.GetAsDuration(time.Second)
		log.Info(c.ctx, "copy segment job has reached GC retention",
			mlog.Time("cleanupTime", cleanupTime), mlog.Duration("gcRetention", gcRetention))

		tasks := c.copyMeta.GetTasksByJobID(c.ctx, job.GetJobId())
		shouldRemoveJob := true

		if job.GetState() == datapb.CopySegmentJobState_CopySegmentJobFailed {
			for _, mapping := range job.GetIdMappings() {
				if mapping != nil && c.meta.GetSegment(c.ctx, mapping.GetTargetSegmentId()) != nil {
					shouldRemoveJob = false
					break
				}
			}
		}

		for _, task := range tasks {
			select {
			case <-c.closeChan:
				return
			default:
			}
			if !c.cleanupTaskForGC(job, task.GetTaskId()) {
				shouldRemoveJob = false
			}
		}

		// Remove job only after every task is gone and segment GC has removed
		// every failed target named by the job's authoritative mapping list.
		if !shouldRemoveJob {
			return
		}

		err := c.copyMeta.RemoveJob(c.ctx, job.GetJobId())
		if err != nil {
			log.Warn(c.ctx, "failed to remove copy segment job", mlog.Err(err))
			return
		}
		log.Info(c.ctx, "copy segment job removed")
	}
}

func (c *copySegmentChecker) cleanupTaskForGC(job CopySegmentJob, taskID int64) bool {
	cleaned := false
	cleanup := func() {
		// Create may have persisted an assignment while Finalize waited for its
		// callback. Resolve worker ownership only after that callback drains.
		latest := c.copyMeta.GetTask(c.ctx, taskID)
		if latest == nil {
			cleaned = true
			return
		}
		// A failed job retires its targets from the job's authoritative mapping
		// list, not from task state. Retain the task record while any mapped
		// target is still present so the failure evidence and the retry
		// inventory survive until segment GC has retired every target.
		// This mirrors import's failed-task segment anchor.
		if job.GetState() == datapb.CopySegmentJobState_CopySegmentJobFailed {
			for _, mapping := range job.GetIdMappings() {
				if mapping != nil && c.meta.GetSegment(c.ctx, mapping.GetTargetSegmentId()) != nil {
					mlog.Info(c.ctx, "retain copy segment task until failed targets are retired",
						WrapCopySegmentTaskLog(latest, mlog.Int64("targetSegmentID", mapping.GetTargetSegmentId()))...)
					return
				}
			}
		}
		if isNodeAssigned(latest.GetNodeId()) {
			if c.cluster == nil {
				mlog.Warn(c.ctx, "cannot drop assigned copy segment task during GC",
					WrapCopySegmentTaskLog(latest)...)
				return
			}
			err := c.cluster.DropCopySegment(latest.GetNodeId(), taskID)
			if err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
				mlog.Warn(c.ctx, "failed to drop copy segment task during GC",
					WrapCopySegmentTaskLog(latest, mlog.Err(err))...)
				return
			}
		}
		if err := c.copyMeta.RemoveTask(c.ctx, taskID); err != nil {
			mlog.Warn(c.ctx, "failed to remove copy segment task during GC",
				WrapCopySegmentTaskLog(latest, mlog.Err(err))...)
			return
		}
		mlog.Info(c.ctx, "copy segment task removed", WrapCopySegmentTaskLog(latest)...)
		cleaned = true
	}
	if c.scheduler == nil {
		// Tests that do not exercise scheduler concurrency use the direct path.
		cleanup()
	} else {
		c.scheduler.Finalize(taskID, cleanup)
	}
	return cleaned
}
