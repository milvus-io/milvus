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
	"math"
	"sync"

	"go.uber.org/zap"
	"golang.org/x/exp/maps"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
)

// Copy Segment Metadata Manager
//
// This file implements the metadata management layer for copy segment jobs and tasks
// during snapshot restore operations. It provides thread-safe CRUD operations for both
// jobs (user-facing operations) and tasks (internal execution units).
//
// ARCHITECTURE:
// - CopySegmentMeta: Interface defining all metadata operations
// - copySegmentMeta: Implementation with in-memory cache and persistent storage
// - copySegmentTasks: Helper struct for managing task collections
//
// DATA MODEL:
// Job: User-initiated snapshot restore operation
//   - Contains collection ID, snapshot name, state, progress
//   - Parent of multiple tasks
// Task: Internal execution unit dispatched to DataNodes
//   - Contains segment ID mappings, assigned node, state
//   - Child of one job
//
// CONCURRENCY:
// - All operations are protected by RWMutex for thread safety
// - Read operations use RLock for concurrent reads
// - Write operations use Lock for exclusive writes
//
// PERSISTENCE:
// - All changes are persisted to metastore (etcd) before updating memory
// - Memory state is restored from metastore on DataCoord restart
// - Provides crash recovery and consistency guarantees

// ===========================================================================================
// Snapshot Restore Reference Tracking
// ===========================================================================================

// SnapshotRestoreRefTracker manages snapshot restore reference counts.
// It maintains a mapping from snapshot name to the count of active restore operations.
// When a CopySegmentJob is created, the count increments. When the job is garbage
// collected (after 3-hour retention), the count decrements. DropSnapshot checks this
// count and rejects deletion if non-zero.
type SnapshotRestoreRefTracker struct {
	mu       sync.RWMutex
	refCount map[string]int32 // key: snapshotName, value: active restore operation count
}

// NewSnapshotRestoreRefTracker creates a new snapshot restore reference tracker.
func NewSnapshotRestoreRefTracker() *SnapshotRestoreRefTracker {
	return &SnapshotRestoreRefTracker{
		refCount: make(map[string]int32),
	}
}

// IncrementRestoreRef increments the restore reference count for a snapshot.
func (t *SnapshotRestoreRefTracker) IncrementRestoreRef(snapshotName string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Protect against integer overflow
	if t.refCount[snapshotName] >= math.MaxInt32 {
		log.Warn("snapshot restore ref count reached maximum, cannot increment",
			zap.String("snapshot", snapshotName),
			zap.Int32("currentCount", t.refCount[snapshotName]))
		return
	}

	t.refCount[snapshotName]++
}

// DecrementRestoreRef decrements the restore reference count for a snapshot.
func (t *SnapshotRestoreRefTracker) DecrementRestoreRef(snapshotName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if count, ok := t.refCount[snapshotName]; ok && count > 0 {
		t.refCount[snapshotName]--
		if t.refCount[snapshotName] == 0 {
			delete(t.refCount, snapshotName)
		}
	} else {
		log.Warn("attempted to decrement snapshot restore ref that is already zero or does not exist",
			zap.String("snapshot", snapshotName))
	}
}

// GetRestoreRefCount returns the restore reference count for a snapshot.
func (t *SnapshotRestoreRefTracker) GetRestoreRefCount(snapshotName string) int32 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.refCount[snapshotName]
}

// ===========================================================================================
// Metadata Interface
// ===========================================================================================

// CopySegmentMeta defines the interface for managing copy segment jobs and tasks.
//
// Job operations manage the lifecycle of snapshot restore operations:
//   - AddJob: Create a new copy segment job
//   - UpdateJob: Modify job state, progress, or completion time
//   - GetJob/GetJobBy: Query jobs by ID or filters
//   - CountJobBy: Count jobs matching filters (for quota enforcement)
//   - RemoveJob: Delete job from metadata (garbage collection)
//
// Task operations manage execution units dispatched to DataNodes:
//   - AddTask: Create a new copy segment task
//   - UpdateTask: Modify task state, assigned node, or completion time
//   - GetTask/GetTaskBy: Query tasks by ID or filters
//   - RemoveTask: Delete task from metadata (garbage collection)
type CopySegmentMeta interface {
	// Job operations
	AddJob(ctx context.Context, job CopySegmentJob) error
	UpdateJob(ctx context.Context, jobID int64, actions ...UpdateCopySegmentJobAction) error
	UpdateJobStateAndReleaseRef(ctx context.Context, jobID int64, actions ...UpdateCopySegmentJobAction) error
	GetJob(ctx context.Context, jobID int64) CopySegmentJob
	GetJobBy(ctx context.Context, filters ...CopySegmentJobFilter) []CopySegmentJob
	CountJobBy(ctx context.Context, filters ...CopySegmentJobFilter) int
	RemoveJob(ctx context.Context, jobID int64) error

	// Task operations
	AddTask(ctx context.Context, task CopySegmentTask) error
	UpdateTask(ctx context.Context, taskID int64, actions ...UpdateCopySegmentTaskAction) error
	GetTask(ctx context.Context, taskID int64) CopySegmentTask
	GetTasksByJobID(ctx context.Context, jobID int64) []CopySegmentTask
	GetTasksByCollectionID(ctx context.Context, collectionID int64) []CopySegmentTask
	GetTaskBy(ctx context.Context, filters ...CopySegmentTaskFilter) []CopySegmentTask
	RemoveTask(ctx context.Context, taskID int64) error

	// Snapshot restore reference tracking
	IncrementRestoreRef(snapshotName string)
	DecrementRestoreRef(snapshotName string)
	GetRestoreRefCount(snapshotName string) int32
}

// ===========================================================================================
// Task Collection Management
// ===========================================================================================

// copySegmentTasks manages a collection of copy segment tasks with efficient lookup.
// It maintains secondary indexes for O(1) lookup by jobID and collectionID.
type copySegmentTasks struct {
	tasks           map[int64]CopySegmentTask    // Task ID -> Task mapping (primary index)
	jobIndex        map[int64]map[int64]struct{} // Job ID -> Task IDs (secondary index)
	collectionIndex map[int64]map[int64]struct{} // Collection ID -> Task IDs (secondary index)
}

// newCopySegmentTasks creates a new empty task collection.
func newCopySegmentTasks() *copySegmentTasks {
	return &copySegmentTasks{
		tasks:           make(map[int64]CopySegmentTask),
		jobIndex:        make(map[int64]map[int64]struct{}),
		collectionIndex: make(map[int64]map[int64]struct{}),
	}
}

// get retrieves a task by ID, returns nil if not found.
func (t *copySegmentTasks) get(taskID int64) CopySegmentTask {
	ret, ok := t.tasks[taskID]
	if !ok {
		return nil
	}
	return ret
}

// add inserts or updates a task in the collection and maintains secondary indexes.
func (t *copySegmentTasks) add(task CopySegmentTask) {
	taskID := task.GetTaskId()

	// If updating existing task, remove from old indexes first
	if oldTask, exists := t.tasks[taskID]; exists {
		t.removeFromIndexes(oldTask)
	}

	// Add to primary index
	t.tasks[taskID] = task

	// Add to secondary indexes
	t.addToIndexes(task)
}

// addToIndexes adds the task to secondary indexes (jobIndex and collectionIndex).
func (t *copySegmentTasks) addToIndexes(task CopySegmentTask) {
	taskID := task.GetTaskId()
	jobID := task.GetJobId()
	collectionID := task.GetCollectionId()

	// Add to job index
	if _, ok := t.jobIndex[jobID]; !ok {
		t.jobIndex[jobID] = make(map[int64]struct{})
	}
	t.jobIndex[jobID][taskID] = struct{}{}

	// Add to collection index
	if _, ok := t.collectionIndex[collectionID]; !ok {
		t.collectionIndex[collectionID] = make(map[int64]struct{})
	}
	t.collectionIndex[collectionID][taskID] = struct{}{}
}

// removeFromIndexes removes the task from secondary indexes.
func (t *copySegmentTasks) removeFromIndexes(task CopySegmentTask) {
	taskID := task.GetTaskId()
	jobID := task.GetJobId()
	collectionID := task.GetCollectionId()

	// Remove from job index
	if taskIDs, ok := t.jobIndex[jobID]; ok {
		delete(taskIDs, taskID)
		if len(taskIDs) == 0 {
			delete(t.jobIndex, jobID)
		}
	}

	// Remove from collection index
	if taskIDs, ok := t.collectionIndex[collectionID]; ok {
		delete(taskIDs, taskID)
		if len(taskIDs) == 0 {
			delete(t.collectionIndex, collectionID)
		}
	}
}

// remove deletes a task from the collection by ID and cleans up secondary indexes.
func (t *copySegmentTasks) remove(taskID int64) {
	if task, exists := t.tasks[taskID]; exists {
		t.removeFromIndexes(task)
		delete(t.tasks, taskID)
	}
}

// listTasks returns all tasks as a slice (unordered).
func (t *copySegmentTasks) listTasks() []CopySegmentTask {
	return maps.Values(t.tasks)
}

// getByJobID retrieves all tasks belonging to a specific job using secondary index.
// Returns nil if no tasks found for the job.
// Time complexity: O(M) where M is the number of tasks for this job.
func (t *copySegmentTasks) getByJobID(jobID int64) []CopySegmentTask {
	taskIDs, ok := t.jobIndex[jobID]
	if !ok {
		return nil
	}
	result := make([]CopySegmentTask, 0, len(taskIDs))
	for taskID := range taskIDs {
		if task, exists := t.tasks[taskID]; exists {
			result = append(result, task)
		}
	}
	return result
}

// getByCollectionID retrieves all tasks belonging to a specific collection using secondary index.
// Returns nil if no tasks found for the collection.
// Time complexity: O(M) where M is the number of tasks for this collection.
func (t *copySegmentTasks) getByCollectionID(collectionID int64) []CopySegmentTask {
	taskIDs, ok := t.collectionIndex[collectionID]
	if !ok {
		return nil
	}
	result := make([]CopySegmentTask, 0, len(taskIDs))
	for taskID := range taskIDs {
		if task, exists := t.tasks[taskID]; exists {
			result = append(result, task)
		}
	}
	return result
}

// ===========================================================================================
// Metadata Implementation
// ===========================================================================================

// copySegmentMeta implements CopySegmentMeta with in-memory caching and persistent storage.
type copySegmentMeta struct {
	mu           lock.RWMutex               // Protects jobs and tasks maps
	jobs         map[int64]CopySegmentJob   // Job ID -> Job mapping (in-memory cache)
	tasks        *copySegmentTasks          // Task collection (in-memory cache)
	catalog      metastore.DataCoordCatalog // Persistent storage backend (etcd)
	meta         *meta                      // Segment metadata for task execution
	snapshotMeta *snapshotMeta              // Snapshot metadata for reading source data

	// Snapshot restore reference tracker
	restoreRefTracker *SnapshotRestoreRefTracker
}

// ===========================================================================================
// Constructor
// ===========================================================================================

// NewCopySegmentMeta creates a new CopySegmentMeta instance and restores state from catalog.
//
// Process flow:
//  1. Load all jobs from persistent storage (catalog)
//  2. Load all tasks from persistent storage
//  3. Reconstruct in-memory task objects with metadata references
//  4. Reconstruct in-memory job objects with time recorders
//  5. Return initialized metadata manager
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - catalog: Persistent storage backend (etcd)
//   - meta: Segment metadata for task execution
//   - snapshotMeta: Snapshot metadata for reading source data
//
// Returns:
//   - CopySegmentMeta instance with restored state
//   - Error if unable to load from catalog
//
// Why this design:
// - Restoring state on startup enables crash recovery
// - In-memory cache provides fast lookups without etcd round trips
// - Metadata references enable tasks to access segment/snapshot data
func NewCopySegmentMeta(ctx context.Context, catalog metastore.DataCoordCatalog, meta *meta, snapshotMeta *snapshotMeta) (CopySegmentMeta, error) {
	// Load jobs and tasks from persistent storage
	restoredJobs, err := catalog.ListCopySegmentJobs(ctx)
	if err != nil {
		return nil, err
	}
	restoredTasks, err := catalog.ListCopySegmentTasks(ctx)
	if err != nil {
		return nil, err
	}

	tasks := newCopySegmentTasks()
	copySegmentMeta := &copySegmentMeta{
		catalog:           catalog,
		meta:              meta,
		snapshotMeta:      snapshotMeta,
		restoreRefTracker: NewSnapshotRestoreRefTracker(),
	}

	// Reconstruct task objects with metadata references
	for _, task := range restoredTasks {
		t := &copySegmentTask{
			copyMeta:     copySegmentMeta,
			meta:         meta,
			snapshotMeta: snapshotMeta,
			tr:           timerecord.NewTimeRecorder("copy segment task"),
			times:        taskcommon.NewTimes(),
		}
		t.task.Store(task)
		tasks.add(t)
	}

	// Reconstruct job objects with time recorders
	jobs := make(map[int64]CopySegmentJob)
	for _, job := range restoredJobs {
		jobs[job.GetJobId()] = &copySegmentJob{
			CopySegmentJob: job,
			tr:             timerecord.NewTimeRecorder("copy segment job"),
		}
	}

	copySegmentMeta.jobs = jobs
	copySegmentMeta.tasks = tasks

	// Rebuild snapshot restore reference counts from non-terminal restored jobs.
	// Only Pending and Executing jobs need ref tracking — terminal jobs (Completed/Failed)
	// already had their refs released before the crash, so re-incrementing them would
	// cause a permanent leak (no code path exists to decrement them again).
	for _, job := range restoredJobs {
		state := job.GetState()
		if state == datapb.CopySegmentJobState_CopySegmentJobCompleted ||
			state == datapb.CopySegmentJobState_CopySegmentJobFailed {
			continue
		}
		snapshotName := job.GetSnapshotName()
		copySegmentMeta.IncrementRestoreRef(snapshotName)
		log.Info("rebuilt snapshot restore ref count from active job",
			zap.String("snapshot", snapshotName),
			zap.Int64("jobID", job.GetJobId()),
			zap.String("state", state.String()))
	}

	return copySegmentMeta, nil
}

// ===========================================================================================
// Job Operations
// ===========================================================================================

// AddJob creates a new copy segment job in both persistent storage and memory cache.
//
// Process flow:
//  1. Acquire write lock
//  2. Persist job to catalog (etcd)
//  3. Add job to in-memory cache
//  4. Release lock
//
// Thread safety: Protected by write lock
// Idempotency: Not idempotent - duplicate adds will fail at catalog layer
func (m *copySegmentMeta) AddJob(ctx context.Context, job CopySegmentJob) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.catalog.SaveCopySegmentJob(ctx, job.(*copySegmentJob).CopySegmentJob)
	if err != nil {
		return err
	}
	m.jobs[job.GetJobId()] = job
	return nil
}

// updateJob applies actions to a job and persists the result.
// Must be called with m.mu write lock held.
// Returns (previous job, updated job, error). If job not found, returns (nil, nil, nil).
func (m *copySegmentMeta) updateJob(ctx context.Context, jobID int64, actions ...UpdateCopySegmentJobAction) (CopySegmentJob, CopySegmentJob, error) {
	job, ok := m.jobs[jobID]
	if !ok {
		return nil, nil, nil
	}
	updatedJob := job.Clone()
	for _, action := range actions {
		action(updatedJob)
	}
	err := m.catalog.SaveCopySegmentJob(ctx, updatedJob.(*copySegmentJob).CopySegmentJob)
	if err != nil {
		return nil, nil, err
	}
	m.jobs[updatedJob.GetJobId()] = updatedJob
	return job, updatedJob, nil
}

// UpdateJob modifies an existing job using functional update actions.
//
// Thread safety: Protected by write lock
func (m *copySegmentMeta) UpdateJob(ctx context.Context, jobID int64, actions ...UpdateCopySegmentJobAction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, _, err := m.updateJob(ctx, jobID, actions...)
	return err
}

// GetJob retrieves a job by ID from in-memory cache.
//
// Thread safety: Protected by read lock (allows concurrent reads)
// Returns: Job if found, nil if not found
func (m *copySegmentMeta) GetJob(ctx context.Context, jobID int64) CopySegmentJob {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.jobs[jobID]
}

// GetJobBy retrieves all jobs matching the provided filters.
//
// Process flow:
//  1. Acquire read lock
//  2. Iterate through all jobs
//  3. Apply each filter - job must pass ALL filters to be included
//  4. Return matching jobs
//  5. Release lock
//
// Parameters:
//   - ctx: Context for cancellation
//   - filters: Filter functions (e.g., WithCopyJobCollectionID, WithCopyJobStates)
//
// Thread safety: Protected by read lock
// Filter logic: AND (job must satisfy all filters)
func (m *copySegmentMeta) GetJobBy(ctx context.Context, filters ...CopySegmentJobFilter) []CopySegmentJob {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.getJobBy(filters...)
}

// getJobBy is the internal implementation of GetJobBy without locking.
//
// Why separate function:
// - Allows internal callers to use it with existing lock held
// - Reduces lock contention by avoiding nested locks
func (m *copySegmentMeta) getJobBy(filters ...CopySegmentJobFilter) []CopySegmentJob {
	ret := make([]CopySegmentJob, 0)
OUTER:
	for _, job := range m.jobs {
		for _, f := range filters {
			if !f(job) {
				continue OUTER // Skip this job if any filter fails
			}
		}
		ret = append(ret, job)
	}
	return ret
}

// CountJobBy counts jobs matching the provided filters.
//
// Thread safety: Protected by read lock
// Use case: Enforcing quota limits on concurrent jobs
func (m *copySegmentMeta) CountJobBy(ctx context.Context, filters ...CopySegmentJobFilter) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.getJobBy(filters...))
}

// UpdateJobStateAndReleaseRef updates job state and releases snapshot reference
// if the job transitions to a terminal state (Completed/Failed).
//
// This ensures snapshot references are released immediately when restore jobs finish,
// while Job records are retained for audit purposes (3 hours).
//
// Thread safety: Protected by write lock
func (m *copySegmentMeta) UpdateJobStateAndReleaseRef(ctx context.Context, jobID int64, actions ...UpdateCopySegmentJobAction) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	prevJob, updatedJob, err := m.updateJob(ctx, jobID, actions...)
	if err != nil {
		return err
	}
	if prevJob == nil {
		log.Warn("UpdateJobStateAndReleaseRef: job not found", zap.Int64("jobID", jobID))
		return nil
	}

	previousState := prevJob.GetState()
	newState := updatedJob.GetState()
	isTerminal := newState == datapb.CopySegmentJobState_CopySegmentJobCompleted ||
		newState == datapb.CopySegmentJobState_CopySegmentJobFailed
	wasTerminal := previousState == datapb.CopySegmentJobState_CopySegmentJobCompleted ||
		previousState == datapb.CopySegmentJobState_CopySegmentJobFailed

	// Only decrement on actual transition TO terminal state (not if already terminal)
	// This prevents double-decrement when multiple paths try to fail the same job
	if isTerminal && !wasTerminal {
		m.restoreRefTracker.DecrementRestoreRef(updatedJob.GetSnapshotName())
		log.Info("released snapshot reference on job completion",
			zap.Int64("jobID", jobID),
			zap.String("snapshot", updatedJob.GetSnapshotName()),
			zap.String("previousState", previousState.String()),
			zap.String("newState", newState.String()))
	}

	return nil
}

// RemoveJob deletes a job from both persistent storage and memory cache.
//
// Process flow:
//  1. Acquire write lock
//  2. Check if job exists
//  3. Delete from catalog (etcd)
//  4. Delete from in-memory cache
//  5. Release lock
//
// Thread safety: Protected by write lock
// Use case: Garbage collection of completed/failed jobs after retention period
func (m *copySegmentMeta) RemoveJob(ctx context.Context, jobID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if job exists
	_, ok := m.jobs[jobID]
	if ok {
		// Remove from persistent storage first to maintain consistency
		// If this fails, we return error without modifying in-memory state
		err := m.catalog.DropCopySegmentJob(ctx, jobID)
		if err != nil {
			return err
		}

		// Note: Snapshot restore reference was already decremented when the job
		// transitioned to a terminal state (Completed/Failed), not here at removal.
		// This decouples reference lifetime from job metadata cleanup.
		log.Info("removed copy segment job",
			zap.Int64("jobID", jobID))

		// Remove from in-memory cache
		delete(m.jobs, jobID)
	}
	return nil
}

// ===========================================================================================
// Task Operations
// ===========================================================================================

// AddTask creates a new copy segment task in both persistent storage and memory cache.
//
// Process flow:
//  1. Acquire write lock
//  2. Inject metadata references into task (for accessing segment/snapshot data)
//  3. Persist task to catalog (etcd)
//  4. Add task to in-memory cache
//  5. Release lock
//
// Why inject metadata:
// - Tasks need access to segment metadata for binlog updates
// - Tasks need access to snapshot metadata for reading source data
// - Injecting at add time ensures all tasks have required dependencies
//
// Thread safety: Protected by write lock
func (m *copySegmentMeta) AddTask(ctx context.Context, task CopySegmentTask) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Ensure the task has meta references
	t := task.(*copySegmentTask)
	t.copyMeta = m
	t.meta = m.meta
	t.snapshotMeta = m.snapshotMeta

	err := m.catalog.SaveCopySegmentTask(ctx, t.task.Load())
	if err != nil {
		return err
	}
	m.tasks.add(task)
	return nil
}

// UpdateTask modifies an existing task using functional update actions.
//
// Process flow:
//  1. Acquire write lock
//  2. Clone the task to avoid race conditions
//  3. Apply all update actions to the clone
//  4. Persist updated task to catalog
//  5. Update in-memory task atomically (using atomic.Pointer)
//  6. Release lock
//
// Parameters:
//   - ctx: Context for cancellation
//   - taskID: ID of task to update
//   - actions: Functional updates to apply (e.g., UpdateCopyTaskState)
//
// Thread safety: Protected by write lock + atomic operations
// Idempotency: Safe to call with same updates (last write wins)
func (m *copySegmentMeta) UpdateTask(ctx context.Context, taskID int64, actions ...UpdateCopySegmentTaskAction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task := m.tasks.get(taskID); task != nil {
		updatedTask := task.Clone()
		for _, action := range actions {
			action(updatedTask)
		}
		err := m.catalog.SaveCopySegmentTask(ctx, updatedTask.(*copySegmentTask).task.Load())
		if err != nil {
			return err
		}
		// update memory task atomically
		task.(*copySegmentTask).task.Store(updatedTask.(*copySegmentTask).task.Load())
	}
	return nil
}

// GetTask retrieves a task by ID from in-memory cache.
//
// Thread safety: Protected by read lock
// Returns: Task if found, nil if not found
func (m *copySegmentMeta) GetTask(ctx context.Context, taskID int64) CopySegmentTask {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tasks.get(taskID)
}

// GetTasksByJobID retrieves all tasks belonging to a specific job using secondary index.
//
// This method provides O(M) lookup where M is the number of tasks for this job,
// compared to O(N) for GetTaskBy with filter where N is total number of tasks.
//
// Thread safety: Protected by read lock
// Returns: Tasks for the job, empty slice if no tasks found
func (m *copySegmentMeta) GetTasksByJobID(ctx context.Context, jobID int64) []CopySegmentTask {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tasks.getByJobID(jobID)
}

// GetTasksByCollectionID retrieves all tasks belonging to a specific collection using secondary index.
//
// This method provides O(M) lookup where M is the number of tasks for this collection,
// compared to O(N) for GetTaskBy with filter where N is total number of tasks.
//
// Thread safety: Protected by read lock
// Returns: Tasks for the collection, empty slice if no tasks found
func (m *copySegmentMeta) GetTasksByCollectionID(ctx context.Context, collectionID int64) []CopySegmentTask {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tasks.getByCollectionID(collectionID)
}

// GetTaskBy retrieves all tasks matching the provided filters.
//
// Process flow:
//  1. Acquire read lock
//  2. Iterate through all tasks
//  3. Apply each filter - task must pass ALL filters to be included
//  4. Return matching tasks
//  5. Release lock
//
// Parameters:
//   - ctx: Context for cancellation
//   - filters: Filter functions (e.g., WithCopyTaskJob, WithCopyTaskStates)
//
// Thread safety: Protected by read lock
// Filter logic: AND (task must satisfy all filters)
func (m *copySegmentMeta) GetTaskBy(ctx context.Context, filters ...CopySegmentTaskFilter) []CopySegmentTask {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ret := make([]CopySegmentTask, 0)
OUTER:
	for _, task := range m.tasks.listTasks() {
		for _, f := range filters {
			if !f(task) {
				continue OUTER // Skip this task if any filter fails
			}
		}
		ret = append(ret, task)
	}
	return ret
}

// RemoveTask deletes a task from both persistent storage and memory cache.
//
// Process flow:
//  1. Acquire write lock
//  2. Check if task exists
//  3. Delete from catalog (etcd)
//  4. Delete from in-memory cache
//  5. Release lock
//
// Thread safety: Protected by write lock
// Use case: Garbage collection of completed/failed tasks after retention period
func (m *copySegmentMeta) RemoveTask(ctx context.Context, taskID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task := m.tasks.get(taskID); task != nil {
		err := m.catalog.DropCopySegmentTask(ctx, taskID)
		if err != nil {
			return err
		}
		m.tasks.remove(taskID)
	}
	return nil
}

// ===========================================================================================
// Snapshot Restore Reference Tracking
// ===========================================================================================

// IncrementRestoreRef increments the restore reference count for a snapshot.
// This is called when a new CopySegmentJob is created for snapshot restore.
func (m *copySegmentMeta) IncrementRestoreRef(snapshotName string) {
	m.restoreRefTracker.IncrementRestoreRef(snapshotName)
}

// DecrementRestoreRef decrements the restore reference count for a snapshot.
// This is called when a CopySegmentJob transitions to a terminal state (Completed/Failed),
// not during garbage collection.
func (m *copySegmentMeta) DecrementRestoreRef(snapshotName string) {
	m.restoreRefTracker.DecrementRestoreRef(snapshotName)
}

// GetRestoreRefCount returns the restore reference count for a snapshot.
// This is used in DropSnapshot to check if there are active restore operations.
func (m *copySegmentMeta) GetRestoreRefCount(snapshotName string) int32 {
	return m.restoreRefTracker.GetRestoreRefCount(snapshotName)
}
