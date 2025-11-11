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

	"golang.org/x/exp/maps"

	"github.com/milvus-io/milvus/internal/metastore"
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
	GetJob(ctx context.Context, jobID int64) CopySegmentJob
	GetJobBy(ctx context.Context, filters ...CopySegmentJobFilter) []CopySegmentJob
	CountJobBy(ctx context.Context, filters ...CopySegmentJobFilter) int
	RemoveJob(ctx context.Context, jobID int64) error

	// Task operations
	AddTask(ctx context.Context, task CopySegmentTask) error
	UpdateTask(ctx context.Context, taskID int64, actions ...UpdateCopySegmentTaskAction) error
	GetTask(ctx context.Context, taskID int64) CopySegmentTask
	GetTaskBy(ctx context.Context, filters ...CopySegmentTaskFilter) []CopySegmentTask
	RemoveTask(ctx context.Context, taskID int64) error
}

// ===========================================================================================
// Task Collection Management
// ===========================================================================================

// copySegmentTasks manages a collection of copy segment tasks with efficient lookup.
type copySegmentTasks struct {
	tasks map[int64]CopySegmentTask // Task ID -> Task mapping
}

// newCopySegmentTasks creates a new empty task collection.
func newCopySegmentTasks() *copySegmentTasks {
	return &copySegmentTasks{
		tasks: make(map[int64]CopySegmentTask),
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

// add inserts or updates a task in the collection.
func (t *copySegmentTasks) add(task CopySegmentTask) {
	t.tasks[task.GetTaskId()] = task
}

// remove deletes a task from the collection by ID.
func (t *copySegmentTasks) remove(taskID int64) {
	delete(t.tasks, taskID)
}

// listTasks returns all tasks as a slice (unordered).
func (t *copySegmentTasks) listTasks() []CopySegmentTask {
	return maps.Values(t.tasks)
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
		catalog:      catalog,
		meta:         meta,
		snapshotMeta: snapshotMeta,
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

// UpdateJob modifies an existing job using functional update actions.
//
// Process flow:
//  1. Acquire write lock
//  2. Clone the job to avoid modifying the original
//  3. Apply all update actions to the clone
//  4. Persist updated job to catalog
//  5. Update in-memory cache with the new job
//  6. Release lock
//
// Parameters:
//   - ctx: Context for cancellation
//   - jobID: ID of job to update
//   - actions: Functional updates to apply (e.g., UpdateCopyJobState)
//
// Thread safety: Protected by write lock
// Idempotency: Safe to call with same updates (last write wins)
//
// Why functional updates:
// - Composable: Can combine multiple updates in one call
// - Type-safe: Each action has specific purpose
// - Flexible: Easy to add new update types
func (m *copySegmentMeta) UpdateJob(ctx context.Context, jobID int64, actions ...UpdateCopySegmentJobAction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if job, ok := m.jobs[jobID]; ok {
		updatedJob := job.Clone()
		for _, action := range actions {
			action(updatedJob)
		}
		err := m.catalog.SaveCopySegmentJob(ctx, updatedJob.(*copySegmentJob).CopySegmentJob)
		if err != nil {
			return err
		}
		m.jobs[updatedJob.GetJobId()] = updatedJob
	}
	return nil
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
	if _, ok := m.jobs[jobID]; ok {
		err := m.catalog.DropCopySegmentJob(ctx, jobID)
		if err != nil {
			return err
		}
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
