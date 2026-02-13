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
	"strconv"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
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

// copySegmentChecker implements the copy segment job state machine and monitoring.
//
// This runs as a background service in DataCoord, checking all copy segment jobs
// periodically and progressing them through their state machine.
type copySegmentChecker struct {
	ctx      context.Context     // Context for lifecycle management
	meta     *meta               // Segment metadata for state updates
	broker   broker.Broker       // Broker for coordinator communication
	alloc    allocator.Allocator // ID allocator for creating tasks
	copyMeta CopySegmentMeta     // Copy segment job/task metadata store

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
	meta *meta,
	broker broker.Broker,
	alloc allocator.Allocator,
	copyMeta CopySegmentMeta,
) CopySegmentChecker {
	return &copySegmentChecker{
		ctx:       ctx,
		meta:      meta,
		broker:    broker,
		alloc:     alloc,
		copyMeta:  copyMeta,
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
func (c *copySegmentChecker) Start() {
	checkInterval := Params.DataCoordCfg.CopySegmentCheckInterval.GetAsDuration(time.Second)
	log.Info("start copy segment checker", zap.Duration("checkInterval", checkInterval))
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.closeChan:
			log.Info("copy segment checker exited")
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
func (c *copySegmentChecker) Close() {
	c.closeOnce.Do(func() {
		close(c.closeChan)
	})
}

// ============================================================================
// Statistics and Metrics
// ============================================================================

// LogJobStats reports job statistics grouped by state.
//
// This logs the count of jobs in each state and reports metrics for monitoring.
// Called on every checker tick to provide visibility into job progress.
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
	log.Info("copy segment job stats", zap.Any("stateNum", stateNum))

	// Report snapshot restore jobs by state (only jobs with snapshot source)
	snapshotJobs := lo.Filter(jobs, func(job CopySegmentJob, _ int) bool {
		return job.GetSnapshotName() != ""
	})
	snapshotByState := lo.GroupBy(snapshotJobs, func(job CopySegmentJob) string {
		return job.GetState().String()
	})
	for state := range datapb.CopySegmentJobState_value {
		if state == datapb.CopySegmentJobState_CopySegmentJobNone.String() {
			continue
		}
		metrics.DataCoordSnapshotRestoreJobsTotal.WithLabelValues(state).Set(float64(len(snapshotByState[state])))
	}
}

// LogTaskStats reports task statistics grouped by state.
//
// This logs the count of tasks in each state and reports metrics for monitoring.
// Called on every checker tick to provide visibility into task execution.
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

	log.Info("copy segment task stats",
		zap.Int("pending", pending), zap.Int("inProgress", inProgress),
		zap.Int("completed", completed), zap.Int("failed", failed))

	// Report metrics
	metrics.CopySegmentTasks.WithLabelValues(datapb.CopySegmentTaskState_CopySegmentTaskPending.String()).Set(float64(pending))
	metrics.CopySegmentTasks.WithLabelValues(datapb.CopySegmentTaskState_CopySegmentTaskInProgress.String()).Set(float64(inProgress))
	metrics.CopySegmentTasks.WithLabelValues(datapb.CopySegmentTaskState_CopySegmentTaskCompleted.String()).Set(float64(completed))
	metrics.CopySegmentTasks.WithLabelValues(datapb.CopySegmentTaskState_CopySegmentTaskFailed.String()).Set(float64(failed))
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
// Idempotency:
//   - Safe to call multiple times - only creates tasks on first call
//   - Subsequent calls return early if tasks already exist
func (c *copySegmentChecker) checkPendingJob(job CopySegmentJob) {
	log := log.With(zap.Int64("jobID", job.GetJobId()))

	// Step 1: Check if tasks already created (idempotent operation)
	tasks := c.copyMeta.GetTasksByJobID(c.ctx, job.GetJobId())
	if len(tasks) > 0 {
		return
	}

	// Step 2: Validate job has segment mappings
	idMappings := job.GetIdMappings()
	if len(idMappings) == 0 {
		log.Warn("no id mappings to copy, mark job as completed")
		c.copyMeta.UpdateJob(c.ctx, job.GetJobId(),
			UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted),
			UpdateCopyJobReason("no segments to copy"))
		return
	}

	// Step 3: Split mappings into groups (max segments per task)
	maxSegmentsPerTask := Params.DataCoordCfg.MaxSegmentsPerCopyTask.GetAsInt()
	groups := lo.Chunk(idMappings, maxSegmentsPerTask)

	// Step 4: Create task for each group
	for i, group := range groups {
		taskID, err := c.alloc.AllocID(c.ctx)
		if err != nil {
			log.Warn("failed to alloc task ID", zap.Error(err))
			return
		}

		// Create task with lightweight ID mappings
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

		// Save task to metadata store
		err = c.copyMeta.AddTask(c.ctx, task)
		if err != nil {
			log.Warn("failed to add copy segment task",
				zap.Int("groupIndex", i),
				zap.Int("segmentCount", len(group)),
				zap.Error(err))
			return
		}
		log.Info("created copy segment task",
			zap.Int64("taskID", taskID),
			zap.Int("groupIndex", i),
			zap.Int("segmentCount", len(group)))
	}

	// Step 5: Update job state to Executing
	err := c.copyMeta.UpdateJob(c.ctx, job.GetJobId(),
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobExecuting),
		UpdateCopyJobProgress(0, int64(len(idMappings))))
	if err != nil {
		log.Warn("failed to update job state to Executing", zap.Error(err))
		return
	}
	log.Info("copy segment job started",
		zap.Int("taskCount", len(groups)),
		zap.Int("totalSegments", len(idMappings)))
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
	log := log.With(zap.Int64("jobID", job.GetJobId()))

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
			log.Warn("failed to update job progress", zap.Error(err))
		} else {
			log.Debug("updated job progress",
				zap.Int64("copiedSegments", copiedSegments),
				zap.Int64("totalSegments", totalSegments),
				zap.Int("completedTasks", completedTasks),
				zap.Int("totalTasks", totalTasks))
		}
	}

	// Report restore progress ratio for snapshot jobs
	if job.GetSnapshotName() != "" && totalSegments > 0 {
		ratio := float64(copiedSegments) / float64(totalSegments)
		metrics.DataCoordSnapshotRestoreProgressRatio.WithLabelValues(
			strconv.FormatInt(job.GetJobId(), 10),
			job.GetSnapshotName(),
		).Set(ratio)
	}

	// Step 4: Check for failures (fail-fast)
	if failedTasks > 0 {
		log.Warn("copy segment job has failed tasks",
			zap.Int("failedTasks", failedTasks),
			zap.Int("totalTasks", totalTasks))
		c.copyMeta.UpdateJob(c.ctx, job.GetJobId(),
			UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
			UpdateCopyJobReason(fmt.Sprintf("%d/%d tasks failed", failedTasks, totalTasks)))
		return
	}

	// Step 5: Wait for all tasks to complete
	if completedTasks < totalTasks {
		log.Debug("waiting for copy segment tasks to complete",
			zap.Int("completed", completedTasks),
			zap.Int("total", totalTasks))
		return
	}

	// Step 6: All tasks completed - collect total rows and finish job
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

	c.finishJob(job, totalRows)
	log.Info("all copy segment tasks completed, job finished")
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
func (c *copySegmentChecker) finishJob(job CopySegmentJob, totalRows int64) {
	log := log.With(zap.Int64("jobID", job.GetJobId()))

	// Step 1: Collect all target segment IDs from task ID mappings
	tasks := c.copyMeta.GetTasksByJobID(c.ctx, job.GetJobId())
	targetSegmentIDs := make([]int64, 0)
	for _, task := range tasks {
		for _, mapping := range task.GetIdMappings() {
			targetSegmentIDs = append(targetSegmentIDs, mapping.GetTargetSegmentId())
		}
	}

	// Step 2: Update segment states to Flushed (make them visible for query)
	if len(targetSegmentIDs) > 0 {
		for _, segID := range targetSegmentIDs {
			segment := c.meta.GetSegment(c.ctx, segID)
			if segment != nil && segment.GetState() != commonpb.SegmentState_Flushed {
				op := UpdateStatusOperator(segID, commonpb.SegmentState_Flushed)
				if err := c.meta.UpdateSegmentsInfo(c.ctx, op); err != nil {
					log.Warn("failed to update segment state to Flushed",
						zap.Int64("segmentID", segID),
						zap.Error(err))
				} else {
					log.Info("updated segment state to Flushed",
						zap.Int64("segmentID", segID))
				}
			}
		}
	}

	// Step 3: Update job state to Completed
	completeTs := uint64(time.Now().UnixNano())
	err := c.copyMeta.UpdateJob(c.ctx, job.GetJobId(),
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted),
		UpdateCopyJobCompleteTs(completeTs),
		UpdateCopyJobTotalRows(totalRows))
	if err != nil {
		log.Warn("failed to update job state to Completed", zap.Error(err))
		return
	}

	// Step 4: Record metrics
	totalDuration := job.GetTR().ElapseSpan()
	metrics.CopySegmentJobLatency.Observe(float64(totalDuration.Milliseconds()))
	// Set progress to 1.0 on completion; metric is deleted later by GC.
	if job.GetSnapshotName() != "" {
		metrics.DataCoordSnapshotRestoreProgressRatio.WithLabelValues(
			strconv.FormatInt(job.GetJobId(), 10),
			job.GetSnapshotName(),
		).Set(1.0)
	}
	log.Info("copy segment job completed",
		zap.Int64("totalRows", totalRows),
		zap.Int("targetSegments", len(targetSegmentIDs)),
		zap.Duration("totalDuration", totalDuration))
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
func (c *copySegmentChecker) checkFailedJob(job CopySegmentJob) {
	log := log.With(zap.Int64("jobID", job.GetJobId()))

	// Find all Pending/InProgress tasks
	allTasks := c.copyMeta.GetTasksByJobID(c.ctx, job.GetJobId())
	tasks := lo.Filter(allTasks, func(t CopySegmentTask, _ int) bool {
		return t.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskPending ||
			t.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskInProgress
	})

	if len(tasks) == 0 {
		return
	}

	log.Warn("copy segment job has failed, marking all tasks as failed",
		zap.String("reason", job.GetReason()),
		zap.Int("taskCount", len(tasks)))

	// Mark each task as failed
	for _, task := range tasks {
		err := c.copyMeta.UpdateTask(c.ctx, task.GetTaskId(),
			UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed),
			UpdateCopyTaskReason(job.GetReason()))
		if err != nil {
			log.Warn("failed to update task state to failed",
				WrapCopySegmentTaskLog(task, zap.Error(err))...)
		}
	}

	cleanupSnapshotProgressMetric(job)
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

	log.Warn("copy segment job timeout",
		zap.Int64("jobID", job.GetJobId()),
		zap.Time("timeoutTime", timeoutTime))
	c.copyMeta.UpdateJob(c.ctx, job.GetJobId(),
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
		UpdateCopyJobReason("timeout"))
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
func (c *copySegmentChecker) checkGC(job CopySegmentJob) {
	// Only GC terminal states
	if job.GetState() != datapb.CopySegmentJobState_CopySegmentJobCompleted &&
		job.GetState() != datapb.CopySegmentJobState_CopySegmentJobFailed {
		return
	}

	cleanupTime := tsoutil.PhysicalTime(job.GetCleanupTs())
	if time.Now().After(cleanupTime) {
		log := log.With(zap.Int64("jobID", job.GetJobId()))
		GCRetention := Params.DataCoordCfg.CopySegmentTaskRetention.GetAsDuration(time.Second)
		log.Info("copy segment job has reached GC retention",
			zap.Time("cleanupTime", cleanupTime), zap.Duration("GCRetention", GCRetention))

		tasks := c.copyMeta.GetTasksByJobID(c.ctx, job.GetJobId())
		shouldRemoveJob := true

		for _, task := range tasks {
			// If job failed and task has target segments in meta, don't remove yet
			// (wait for segments to be cleaned up first)
			if job.GetState() == datapb.CopySegmentJobState_CopySegmentJobFailed {
				hasSegments := false
				for _, mapping := range task.GetIdMappings() {
					segment := c.meta.GetSegment(c.ctx, mapping.GetTargetSegmentId())
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
			if task.GetNodeId() != NullNodeID {
				shouldRemoveJob = false
				continue
			}

			// Remove task from metadata
			err := c.copyMeta.RemoveTask(c.ctx, task.GetTaskId())
			if err != nil {
				log.Warn("failed to remove copy segment task during GC",
					WrapCopySegmentTaskLog(task, zap.Error(err))...)
				shouldRemoveJob = false
				continue
			}
			log.Info("copy segment task removed", WrapCopySegmentTaskLog(task)...)
		}

		// Remove job only if all tasks removed
		if !shouldRemoveJob {
			return
		}

		err := c.copyMeta.RemoveJob(c.ctx, job.GetJobId())
		if err != nil {
			log.Warn("failed to remove copy segment job", zap.Error(err))
			return
		}
		cleanupSnapshotProgressMetric(job)
		log.Info("copy segment job removed")
	}
}

// cleanupSnapshotProgressMetric removes the per-job progress gauge to prevent
// cardinality leak after the job finishes, fails, or is garbage collected.
func cleanupSnapshotProgressMetric(job CopySegmentJob) {
	if job.GetSnapshotName() != "" {
		metrics.DataCoordSnapshotRestoreProgressRatio.DeleteLabelValues(
			strconv.FormatInt(job.GetJobId(), 10),
			job.GetSnapshotName(),
		)
	}
}
