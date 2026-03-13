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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// External Collection Refresh Manager
//
// The manager is the facade for external collection refresh operations. It encapsulates
// all internal components (inspector and checker) and provides a unified interface
// for job management.
//
// ARCHITECTURE:
// ┌─────────────────────────────────────────────────────────────────┐
// │            ExternalCollectionRefreshManager [Facade]             │
// │                                                                  │
// │  Public APIs:                                                    │
// │  ├─ Start()                    // Start all internal components  │
// │  ├─ Stop()                     // Stop all internal components   │
// │  ├─ SubmitRefreshJobWithID()   // Job submission                 │
// │  ├─ GetJobProgress()           // Job progress query             │
// │  └─ ListJobs()                 // Job list query                 │
// │                                                                  │
// │  Internal Components (private, composed):                        │
// │  ├─ refreshMeta: Job and Task metadata management                │
// │  ├─ inspector: Task scheduling and recovery                      │
// │  └─ checker: Job timeout detection and garbage collection        │
// └─────────────────────────────────────────────────────────────────┘
//
// JOB/TASK SEPARATION:
// - Job: User-initiated refresh operation (API level), 1 job can have N tasks
// - Task: Execution unit dispatched to workers (scheduler level)

// ExternalCollectionRefreshManager defines the interface for managing external table refresh jobs.
type ExternalCollectionRefreshManager interface {
	// Lifecycle management
	Start() // Start all internal components (inspector and checker loops)
	Stop()  // Stop all internal components gracefully

	// SubmitRefreshJobWithID creates a refresh job with a pre-allocated job ID (from WAL).
	// This ensures idempotency - if the job already exists, it returns without error.
	// If there's an existing active job for the same collection, it will be cancelled
	// and replaced by the new job (the old job will show "superseded by new job" as fail reason).
	// This method is called from the WAL callback to ensure distributed consistency.
	SubmitRefreshJobWithID(ctx context.Context, jobID int64, collectionID int64, collectionName string, externalSource, externalSpec string) (int64, error)

	// GetJobProgress returns the job info for the given job_id
	GetJobProgress(ctx context.Context, jobID int64) (*datapb.ExternalCollectionRefreshJob, error)

	// ListJobs returns jobs for the given collection, sorted by start_time descending
	ListJobs(ctx context.Context, collectionID int64) ([]*datapb.ExternalCollectionRefreshJob, error)
}

var _ ExternalCollectionRefreshManager = (*externalCollectionRefreshManager)(nil)

type externalCollectionRefreshManager struct {
	ctx       context.Context
	mt        *meta
	scheduler task.GlobalScheduler
	allocator allocator.Allocator

	// collectionGetter retrieves collection metadata, with lazy-loading from RootCoord
	// on cache miss. This handles the race condition where a refresh is triggered
	// before the collection metadata has been synced to DataCoord.
	collectionGetter func(ctx context.Context, collectionID int64) (*collectionInfo, error)

	// Unified refresh meta for Job and Task management
	refreshMeta *externalCollectionRefreshMeta

	// Internal components (private, composed)
	inspector *externalCollectionRefreshInspector
	checker   *externalCollectionRefreshChecker

	// Lifecycle management
	closeOnce sync.Once
	closeChan chan struct{}
	wg        sync.WaitGroup
}

// NewExternalCollectionRefreshManager creates a new external table refresh manager.
// collectionGetter retrieves collection info with lazy-loading from RootCoord on cache miss.
func NewExternalCollectionRefreshManager(
	ctx context.Context,
	mt *meta,
	scheduler task.GlobalScheduler,
	allocator allocator.Allocator,
	refreshMeta *externalCollectionRefreshMeta,
	collectionGetter func(ctx context.Context, collectionID int64) (*collectionInfo, error),
) ExternalCollectionRefreshManager {
	closeChan := make(chan struct{})

	m := &externalCollectionRefreshManager{
		ctx:              ctx,
		mt:               mt,
		scheduler:        scheduler,
		allocator:        allocator,
		refreshMeta:      refreshMeta,
		collectionGetter: collectionGetter,
		closeChan:        closeChan,
	}

	// Create internal components with shared refreshMeta
	m.inspector = newRefreshInspector(ctx, refreshMeta, mt, scheduler, allocator, closeChan)
	m.checker = newRefreshChecker(ctx, refreshMeta, closeChan)

	return m
}

// Start begins all internal component loops (inspector and checker).
// This should be called once during DataCoord startup.
func (m *externalCollectionRefreshManager) Start() {
	// Start inspector loop
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.inspector.run()
	}()

	// Start checker loop
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.checker.run()
	}()
}

// Stop gracefully shuts down all internal components.
// Safe to call multiple times (uses sync.Once internally).
func (m *externalCollectionRefreshManager) Stop() {
	m.closeOnce.Do(func() {
		close(m.closeChan)
	})
	m.wg.Wait()
}

// ============================================================================
// Job APIs
// ============================================================================

// SubmitRefreshJobWithID creates a refresh job with a pre-allocated job ID (from WAL).
// This ensures idempotency - if the job already exists, it returns without error.
// Only one active refresh job is allowed per collection at a time. If there's already
// an active job, submission will fail with an error.
// This method is called from the WAL callback to ensure distributed consistency.
func (m *externalCollectionRefreshManager) SubmitRefreshJobWithID(
	ctx context.Context,
	jobID int64,
	collectionID int64,
	collectionName string,
	externalSource, externalSpec string,
) (int64, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("jobID", jobID),
		zap.Int64("collectionID", collectionID),
		zap.String("collectionName", collectionName))

	// Idempotency: if job already exists, return. TOCTOU between this check and AddJob
	// is mitigated by WAL idempotency (same JobID on retry) and per-collection lock in AddJob.
	existingJob := m.refreshMeta.GetJob(jobID)
	if existingJob != nil {
		log.Info("job already exists, skip creating")
		return jobID, nil
	}

	// Get collection info to validate it's an external collection.
	// collectionGetter handles cache miss by lazy-loading from RootCoord,
	// which covers the race condition where refresh is triggered before
	// DataCoord syncs the newly created collection.
	collection, err := m.collectionGetter(ctx, collectionID)
	if err != nil || collection == nil {
		log.Warn("collection not found", zap.Error(err))
		return 0, merr.WrapErrCollectionNotFound(collectionID)
	}

	// Validate it's an external collection
	if !typeutil.IsExternalCollection(collection.Schema) {
		log.Warn("not an external collection")
		return 0, merr.WrapErrCollectionIllegalSchema(collectionName, "not an external collection")
	}

	// Use provided source/spec or fall back to collection's current values
	if externalSource == "" {
		externalSource = collection.Schema.GetExternalSource()
	}
	if externalSpec == "" {
		externalSpec = collection.Schema.GetExternalSpec()
	}

	// Check if there's already an active job for this collection
	// Only one active refresh job is allowed at a time
	activeJob := m.refreshMeta.GetActiveJobByCollectionID(collectionID)
	if activeJob != nil {
		log.Warn("refresh job already in progress",
			zap.Int64("existingJobID", activeJob.GetJobId()),
			zap.String("existingJobState", activeJob.GetState().String()))
		return 0, merr.WrapErrTaskDuplicate("refresh_external_collection", fmt.Sprintf("refresh job %d is already in progress for collection %s, please wait for it to complete or cancel it first",
			activeJob.GetJobId(), collectionName))
	}

	startTime := time.Now().UnixMilli()

	// Create job
	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          jobID,
		CollectionId:   collectionID,
		CollectionName: collectionName,
		ExternalSource: externalSource,
		ExternalSpec:   externalSpec,
		State:          indexpb.JobState_JobStateInit,
		StartTime:      startTime,
		Progress:       0,
		TaskIds:        []int64{},
	}

	if err := m.refreshMeta.AddJob(job); err != nil {
		log.Warn("failed to add job to meta", zap.Error(err))
		return 0, err
	}

	// Create task(s) for this job
	tasks, err := m.createTasksForJob(ctx, job)
	if err != nil {
		// Rollback: remove the already-persisted job to avoid leaving it stuck in Init state
		if rollbackErr := m.refreshMeta.DropJob(ctx, jobID); rollbackErr != nil {
			log.Warn("failed to rollback job after task creation failure",
				zap.Int64("jobID", jobID),
				zap.Error(rollbackErr))
		}
		return 0, err
	}

	// Enqueue all created tasks for scheduling
	for _, t := range tasks {
		m.scheduler.Enqueue(t)
	}

	log.Info("external collection refresh job submitted with pre-allocated ID",
		zap.String("externalSource", externalSource),
		zap.Int("taskCount", len(tasks)))

	return jobID, nil
}

// createTasksForJob creates task(s) for a job and persists them to meta.
// Returns the created tasks for subsequent scheduling.
//
// Current implementation: 1 job = 1 task
// Future extension point: This method can be modified to create multiple tasks for parallel execution.
func (m *externalCollectionRefreshManager) createTasksForJob(
	ctx context.Context,
	job *datapb.ExternalCollectionRefreshJob,
) ([]*refreshExternalCollectionTask, error) {
	log := log.Ctx(ctx).With(zap.Int64("jobID", job.GetJobId()), zap.Int64("collectionID", job.GetCollectionId()))

	// Allocate task ID
	taskID, err := m.allocator.AllocID(ctx)
	if err != nil {
		log.Warn("failed to allocate task ID", zap.Error(err))
		return nil, err
	}

	// Create task
	task := &datapb.ExternalCollectionRefreshTask{
		TaskId:         taskID,
		JobId:          job.GetJobId(),
		CollectionId:   job.GetCollectionId(),
		Version:        0,
		NodeId:         0,
		State:          indexpb.JobState_JobStateInit,
		ExternalSource: job.GetExternalSource(),
		ExternalSpec:   job.GetExternalSpec(),
		Progress:       0,
	}

	// Add task to meta
	if err = m.refreshMeta.AddTask(task); err != nil {
		log.Warn("failed to add task to meta", zap.Error(err))
		return nil, err
	}

	// Add taskID to job
	if err = m.refreshMeta.AddTaskIDToJob(job.GetJobId(), taskID); err != nil {
		log.Warn("failed to add taskID to job", zap.Error(err))
		return nil, err
	}

	// Create task wrapper
	taskWrapper := newRefreshExternalCollectionTask(task, m.refreshMeta, m.mt, m.allocator)

	log.Info("task created for job",
		zap.Int64("taskID", taskID),
		zap.Int64("jobID", job.GetJobId()))

	return []*refreshExternalCollectionTask{taskWrapper}, nil
}

// GetJobProgress returns the job info for the given job_id
func (m *externalCollectionRefreshManager) GetJobProgress(ctx context.Context, jobID int64) (*datapb.ExternalCollectionRefreshJob, error) {
	job := m.refreshMeta.GetJob(jobID)
	if job == nil {
		return nil, fmt.Errorf("job %d not found", jobID)
	}

	// Aggregate state and progress from tasks
	state, progress := m.refreshMeta.AggregateJobStateFromTasks(jobID)
	// Only update if tasks exist. If state is None (no tasks yet), keep persisted state.
	if state != indexpb.JobState_JobStateNone {
		job.State = state
		job.Progress = progress
	}
	return job, nil
}

// ListJobs returns jobs for the given collection, sorted by start_time descending
func (m *externalCollectionRefreshManager) ListJobs(ctx context.Context, collectionID int64) ([]*datapb.ExternalCollectionRefreshJob, error) {
	jobs := m.refreshMeta.ListJobsByCollectionID(collectionID)

	result := make([]*datapb.ExternalCollectionRefreshJob, 0, len(jobs))
	for _, job := range jobs {
		// Aggregate state and progress from tasks
		state, progress := m.refreshMeta.AggregateJobStateFromTasks(job.GetJobId())
		// Only update if tasks exist. If state is None (no tasks yet), keep persisted state.
		if state != indexpb.JobState_JobStateNone {
			job.State = state
			job.Progress = progress
		}
		result = append(result, job)
	}

	return result, nil
}
