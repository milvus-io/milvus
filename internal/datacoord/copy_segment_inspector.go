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
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

// Copy Segment Task Inspector
//
// The inspector is responsible for task-level scheduling and failure handling during
// snapshot restore operations. It runs in a periodic loop to monitor task states and
// take appropriate actions.
//
// RESPONSIBILITIES:
// 1. Reload InProgress tasks to scheduler on DataCoord restart (idempotent recovery)
// 2. Enqueue Pending tasks to the global task scheduler for execution
// 3. Clean up target segments when tasks fail (drop incomplete segments)
//
// TASK STATE TRANSITIONS:
// Pending → InProgress (inspector enqueues to scheduler)
// InProgress → Completed/Failed (datanode reports execution result)
// Failed → Dropped (inspector drops target segments)
//
// INSPECTION INTERVAL:
// Configured by Params.DataCoordCfg.CopySegmentCheckInterval (default: 2 seconds)
//
// COORDINATION:
// - Works with CopySegmentChecker which manages job-level state machine
// - Uses GlobalScheduler to dispatch tasks to DataNodes
// - Updates segment metadata to mark failed segments as Dropped

// ===========================================================================================
// Inspector Interface and Implementation
// ===========================================================================================

// CopySegmentInspector defines the interface for task-level scheduling and monitoring.
type CopySegmentInspector interface {
	// Start begins the periodic inspection loop in a background goroutine.
	// It first reloads any InProgress tasks from metadata, then enters the inspection loop.
	Start()

	// Close gracefully stops the inspector, ensuring no goroutine leaks.
	// Safe to call multiple times (uses sync.Once).
	Close()
}

// copySegmentInspector implements the CopySegmentInspector interface.
type copySegmentInspector struct {
	ctx       context.Context      // Context for cancellation and logging
	meta      *meta                // Segment metadata (for dropping failed target segments)
	copyMeta  CopySegmentMeta      // Copy job and task metadata
	scheduler task.GlobalScheduler // Task scheduler for dispatching to DataNodes

	closeOnce sync.Once     // Ensures Close is idempotent
	closeChan chan struct{} // Channel to signal inspector shutdown
}

// ===========================================================================================
// Constructor
// ===========================================================================================

// NewCopySegmentInspector creates a new inspector instance.
//
// Parameters:
//   - ctx: Context for cancellation and logging
//   - meta: Segment metadata for updating segment states
//   - copyMeta: Copy job and task metadata store
//   - scheduler: Global task scheduler for dispatching tasks
//
// Returns:
//
//	A new CopySegmentInspector instance ready to Start.
func NewCopySegmentInspector(
	ctx context.Context,
	meta *meta,
	copyMeta CopySegmentMeta,
	scheduler task.GlobalScheduler,
) CopySegmentInspector {
	return &copySegmentInspector{
		ctx:       ctx,
		meta:      meta,
		copyMeta:  copyMeta,
		scheduler: scheduler,
		closeChan: make(chan struct{}),
	}
}

// ===========================================================================================
// Lifecycle Management
// ===========================================================================================

// Start begins the periodic inspection loop.
//
// Process flow:
//  1. Reload InProgress tasks from metadata (for recovery after DataCoord restart)
//  2. Log inspection interval for observability
//  3. Enter periodic inspection loop:
//     a. Wait for ticker or close signal
//     b. Run inspect() to process all pending/failed tasks
//     c. Repeat until Close() is called
//
// Why this design:
// - Reloading ensures tasks don't get lost on DataCoord restart
// - Periodic inspection handles tasks that may have been missed during transitions
// - Separate ticker allows tuning inspection frequency independently
func (s *copySegmentInspector) Start() {
	// Reload tasks on startup for idempotent recovery
	s.reloadFromMeta()

	// Log inspection interval for observability
	inspectInterval := Params.DataCoordCfg.CopySegmentCheckInterval.GetAsDuration(time.Second)
	log.Ctx(s.ctx).Info("start copy segment inspector", zap.Duration("inspectInterval", inspectInterval))

	ticker := time.NewTicker(inspectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.closeChan:
			log.Ctx(s.ctx).Info("copy segment inspector exited")
			return
		case <-ticker.C:
			s.inspect()
		}
	}
}

// Close gracefully shuts down the inspector.
//
// This signals the inspection loop to exit and ensures the goroutine terminates.
// Safe to call multiple times (uses sync.Once internally).
func (s *copySegmentInspector) Close() {
	s.closeOnce.Do(func() {
		close(s.closeChan)
	})
}

// ===========================================================================================
// Task Recovery and Inspection
// ===========================================================================================

// reloadFromMeta reloads InProgress tasks to scheduler on DataCoord restart.
//
// Process flow:
//  1. Retrieve all copy segment jobs from metadata
//  2. Sort jobs by ID for deterministic processing order
//  3. For each job, retrieve all associated tasks
//  4. Enqueue any InProgress tasks to the scheduler
//  5. Log the number of jobs processed for observability
//
// Why this is needed:
// - DataCoord may restart while tasks are executing on DataNodes
// - InProgress tasks need to be re-added to scheduler to continue monitoring
// - This ensures no tasks are orphaned after restart
//
// Idempotency:
// - Safe to call multiple times (scheduler handles duplicate enqueues)
// - Only InProgress tasks are reloaded (Pending will be handled by inspect loop)
func (s *copySegmentInspector) reloadFromMeta() {
	// Retrieve all jobs (no filters)
	jobs := s.copyMeta.GetJobBy(s.ctx)
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].GetJobId() < jobs[j].GetJobId()
	})

	for _, job := range jobs {
		tasks := s.copyMeta.GetTaskBy(s.ctx, WithCopyTaskJob(job.GetJobId()))
		for _, task := range tasks {
			if task.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskInProgress {
				s.scheduler.Enqueue(task)
			}
		}
	}
	log.Info("copy segment inspector reloaded tasks from meta",
		zap.Int("jobCount", len(jobs)))
}

// inspect runs a single inspection cycle to process all pending and failed tasks.
//
// Process flow:
//  1. Retrieve all copy segment jobs from metadata
//  2. Sort jobs by ID for deterministic processing order
//  3. For each job, retrieve all associated tasks
//  4. Process tasks based on state:
//     - Pending: Enqueue to scheduler for execution
//     - Failed: Drop target segments to clean up incomplete data
//
// Why periodic inspection:
// - Tasks may transition to Pending state at any time (when checker creates them)
// - Failed tasks need prompt cleanup to prevent orphaned segments
// - Periodic inspection ensures no tasks are missed during state transitions
func (s *copySegmentInspector) inspect() {
	// Retrieve all jobs (no filters)
	jobs := s.copyMeta.GetJobBy(s.ctx)
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].GetJobId() < jobs[j].GetJobId()
	})

	for _, job := range jobs {
		tasks := s.copyMeta.GetTaskBy(s.ctx, WithCopyTaskJob(job.GetJobId()))
		for _, task := range tasks {
			switch task.GetState() {
			case datapb.CopySegmentTaskState_CopySegmentTaskPending:
				s.processPending(task)
			case datapb.CopySegmentTaskState_CopySegmentTaskFailed:
				s.processFailed(task)
			}
		}
	}
}

// ===========================================================================================
// Task State Processing
// ===========================================================================================

// processPending enqueues a pending task to the scheduler for execution.
//
// Process flow:
//  1. Enqueue task to global scheduler
//  2. Scheduler will assign task to available DataNode
//  3. DataNode executes CopySegmentTask and reports results
//
// Why this design:
// - Decouples task scheduling from task execution
// - Scheduler handles load balancing across DataNodes
// - Enables concurrent execution of multiple tasks
//
// Idempotency:
// - Safe to enqueue same task multiple times (scheduler handles duplicates)
// - Task state will transition to InProgress when actually dispatched
func (s *copySegmentInspector) processPending(task CopySegmentTask) {
	s.scheduler.Enqueue(task)
}

// processFailed handles cleanup for failed copy segment tasks.
//
// Process flow:
//  1. Iterate through all segment ID mappings in the task
//  2. For each target segment:
//     a. Retrieve segment metadata
//     b. Mark segment as Dropped if it exists
//     c. Log success/failure of drop operation
//
// Why drop target segments:
// - Failed tasks may have partially copied data to target segments
// - Incomplete segments should not be visible to queries
// - Dropping ensures consistent state and prevents data corruption
//
// Error handling:
// - Logs warnings if drop fails but continues processing other segments
// - Failed drops will be retried on next inspection cycle
func (s *copySegmentInspector) processFailed(task CopySegmentTask) {
	// Drop target segments if copy failed
	for _, mapping := range task.GetIdMappings() {
		targetSegID := mapping.GetTargetSegmentId()
		segment := s.meta.GetSegment(s.ctx, targetSegID)
		if segment != nil {
			op := UpdateStatusOperator(targetSegID, commonpb.SegmentState_Dropped)
			err := s.meta.UpdateSegmentsInfo(s.ctx, op)
			if err != nil {
				log.Warn("failed to drop target segment after copy task failed",
					WrapCopySegmentTaskLog(task, zap.Int64("segmentID", targetSegID), zap.Error(err))...)
			} else {
				log.Info("dropped target segment after copy task failed",
					WrapCopySegmentTaskLog(task, zap.Int64("segmentID", targetSegID))...)
			}
		}
	}
}
