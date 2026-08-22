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

	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
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
	UpdateJobInState(ctx context.Context, jobID int64, expectedState datapb.CopySegmentJobState, actions ...UpdateCopySegmentJobAction) (bool, error)
	UpdateJobStateAndReleaseRef(ctx context.Context, jobID int64, actions ...UpdateCopySegmentJobAction) error
	UpdateJobStateAndReleaseRefInState(ctx context.Context, jobID int64, expectedState datapb.CopySegmentJobState, actions ...UpdateCopySegmentJobAction) (bool, error)
	GetJob(ctx context.Context, jobID int64) CopySegmentJob
	GetJobBy(ctx context.Context, filters ...CopySegmentJobFilter) []CopySegmentJob
	CountJobBy(ctx context.Context, filters ...CopySegmentJobFilter) int
	RemoveJob(ctx context.Context, jobID int64) error

	// Task operations
	AddTask(ctx context.Context, task CopySegmentTask) error
	PublishReplan(ctx context.Context, task CopySegmentTask, targets []*SegmentInfo) error
	UpdateTask(ctx context.Context, taskID int64, actions ...UpdateCopySegmentTaskAction) error
	UpdateTaskInState(ctx context.Context, taskID int64, expectedState datapb.CopySegmentTaskState, actions ...UpdateCopySegmentTaskAction) (bool, error)
	GetTask(ctx context.Context, taskID int64) CopySegmentTask
	GetReplacementByPredecessor(ctx context.Context, predecessorTaskID int64) CopySegmentTask
	GetTasksByJobID(ctx context.Context, jobID int64) []CopySegmentTask
	GetTasksByCollectionID(ctx context.Context, collectionID int64) []CopySegmentTask
	GetTaskBy(ctx context.Context, filters ...CopySegmentTaskFilter) []CopySegmentTask
	RemoveTask(ctx context.Context, taskID int64) error
}

// ===========================================================================================
// Task Collection Management
// ===========================================================================================

// copySegmentTasks manages a collection of copy segment tasks with efficient lookup.
// It maintains secondary indexes for O(1) lookup by jobID and collectionID.
type copySegmentTasks struct {
	tasks            map[int64]CopySegmentTask    // Task ID -> Task mapping (primary index)
	jobIndex         map[int64]map[int64]struct{} // Job ID -> Task IDs (secondary index)
	collectionIndex  map[int64]map[int64]struct{} // Collection ID -> Task IDs (secondary index)
	predecessorIndex map[int64]map[int64]struct{} // Predecessor task ID -> successor Task IDs (secondary index)
}

// newCopySegmentTasks creates a new empty task collection.
func newCopySegmentTasks() *copySegmentTasks {
	return &copySegmentTasks{
		tasks:            make(map[int64]CopySegmentTask),
		jobIndex:         make(map[int64]map[int64]struct{}),
		collectionIndex:  make(map[int64]map[int64]struct{}),
		predecessorIndex: make(map[int64]map[int64]struct{}),
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

// addToIndexes adds the task to secondary indexes (jobIndex, collectionIndex
// and predecessorIndex).
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

	// Add to predecessor index (replans only; ordinary tasks have no edge)
	if pred := task.GetPredecessorTaskId(); pred != 0 {
		if _, ok := t.predecessorIndex[pred]; !ok {
			t.predecessorIndex[pred] = make(map[int64]struct{})
		}
		t.predecessorIndex[pred][taskID] = struct{}{}
	}
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

	// Remove from predecessor index
	if pred := task.GetPredecessorTaskId(); pred != 0 {
		if taskIDs, ok := t.predecessorIndex[pred]; ok {
			delete(taskIDs, taskID)
			if len(taskIDs) == 0 {
				delete(t.predecessorIndex, pred)
			}
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

// getByPredecessorID retrieves the successor(s) naming this task as their
// predecessor. Source-side uniqueness in replanUnderFreshIdentity means at
// most one exists; the slice form keeps the caller honest about the invariant.
func (t *copySegmentTasks) getByPredecessorID(predecessorID int64) []CopySegmentTask {
	taskIDs, ok := t.predecessorIndex[predecessorID]
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
	mu           lock.RWMutex // Protects jobs and tasks maps
	ctx          context.Context
	jobs         map[int64]CopySegmentJob   // Job ID -> Job mapping (in-memory cache)
	tasks        *copySegmentTasks          // Task collection (in-memory cache)
	catalog      metastore.DataCoordCatalog // Persistent storage backend (etcd)
	meta         *meta                      // Segment metadata for task execution
	snapshotMeta *snapshotMeta              // Snapshot metadata for reading source data
	alloc        allocator.Allocator        // For allocating new build IDs in copy segment tasks
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
func NewCopySegmentMeta(ctx context.Context, catalog metastore.DataCoordCatalog, meta *meta, snapshotMeta *snapshotMeta, alloc allocator.Allocator) (CopySegmentMeta, error) {
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
		ctx:          ctx,
		catalog:      catalog,
		meta:         meta,
		snapshotMeta: snapshotMeta,
		alloc:        alloc,
	}

	// Reconstruct task objects with metadata references
	for _, task := range restoredTasks {
		t := &copySegmentTask{
			ctx:          ctx,
			copyMeta:     copySegmentMeta,
			meta:         meta,
			snapshotMeta: snapshotMeta,
			alloc:        alloc,
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
			snapshotCache:  &copySegmentSnapshotCache{},
		}
	}

	copySegmentMeta.jobs = jobs
	copySegmentMeta.tasks = tasks

	// Note: no ref-count rebuild is needed on restart. Restore protection is provided
	// by pins persisted on SnapshotInfo (see createRestoreJob / RestoreSnapshot phase 0),
	// which survive restart automatically via snapshotMeta reload. Terminal jobs have
	// already released their pin; active jobs still hold theirs.
	//
	// Upgrade caveat: CopySegmentJob rows persisted by pre-pin-refactor datacoord carry
	// PinId=0 (proto default). The terminal-transition guard at UpdateJobStateAndReleaseRef
	// skips Unpin for such jobs (no pin existed). DropSnapshot protection for these
	// in-flight legacy jobs is NOT retroactively established — plan upgrades during
	// quiet periods, or drain active restores before switching binaries.
	//
	// Rollback caveat: if post-pin-refactor data is read by a pre-refactor binary, the
	// old code ignores CopySegmentJob.PinId and uses its in-memory ref counter. Pins
	// persisted on SnapshotInfo remain but are never unpinned → orphan pins. The pin
	// TTL (dataCoord.snapshot.restorePinTTLSeconds) caps the blast radius.

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

// UpdateJobInState applies the actions only if the cached job is currently in
// expectedState, with the check and the update under the same write lock.
//
// Callers that hold a job snapshot taken before a slow operation (e.g. the
// checker creating tasks) must use this instead of UpdateJob for state
// transitions: a concurrent failure path (markTaskAndJobFailed) may have moved
// the job to a terminal state in the meantime, and an unconditional update
// would resurrect it — e.g. Failed -> Executing after the job's snapshot pin
// was already released.
//
// Returns (false, nil) when the job is missing or not in expectedState (the
// update is skipped), (true, nil) on success.
func (m *copySegmentMeta) UpdateJobInState(ctx context.Context, jobID int64, expectedState datapb.CopySegmentJobState, actions ...UpdateCopySegmentJobAction) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, ok := m.jobs[jobID]
	if !ok || job.GetState() != expectedState {
		return false, nil
	}
	_, _, err := m.updateJob(ctx, jobID, actions...)
	if err != nil {
		return false, err
	}
	return true, nil
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

// UpdateJobStateAndReleaseRef updates job state and unpins the source snapshot
// if the job transitions to a terminal state (Completed/Failed).
//
// This ensures snapshot pins are released immediately when restore jobs finish,
// while Job records are retained for audit purposes (3 hours).
//
// Locking strategy: the state-mutate section takes m.mu; the Unpin call (an etcd
// roundtrip via snapshotMeta.SaveSnapshot) runs AFTER releasing m.mu to avoid
// blocking all copy-segment job operations on an external write. Double-unpin is
// prevented because only one caller observes the `!wasTerminal → isTerminal`
// transition under m.mu; every subsequent caller sees wasTerminal=true.
func (m *copySegmentMeta) UpdateJobStateAndReleaseRef(ctx context.Context, jobID int64, actions ...UpdateCopySegmentJobAction) error {
	_, err := m.updateJobStateAndReleaseRef(ctx, jobID, nil, actions...)
	return err
}

// updateJobStateAndReleaseRef is the shared implementation of
// UpdateJobStateAndReleaseRef and its CAS variant. When expectedState is
// non-nil, the transition (and unpin) happens only if the job is currently in
// that state, with the check and the update under the same write lock.
func (m *copySegmentMeta) updateJobStateAndReleaseRef(ctx context.Context, jobID int64, expectedState *datapb.CopySegmentJobState, actions ...UpdateCopySegmentJobAction) (bool, error) {
	m.mu.Lock()
	if expectedState != nil {
		job, ok := m.jobs[jobID]
		if !ok || job.GetState() != *expectedState {
			m.mu.Unlock()
			return false, nil
		}
	}
	prevJob, updatedJob, err := m.updateJob(ctx, jobID, actions...)
	if err != nil {
		m.mu.Unlock()
		return false, err
	}
	if prevJob == nil {
		m.mu.Unlock()
		mlog.Warn(ctx, "updateJobStateAndReleaseRef: job not found", mlog.FieldJobID(jobID))
		return false, nil
	}

	previousState := prevJob.GetState()
	newState := updatedJob.GetState()
	isTerminal := newState == datapb.CopySegmentJobState_CopySegmentJobCompleted ||
		newState == datapb.CopySegmentJobState_CopySegmentJobFailed
	wasTerminal := previousState == datapb.CopySegmentJobState_CopySegmentJobCompleted ||
		previousState == datapb.CopySegmentJobState_CopySegmentJobFailed

	if isTerminal && !wasTerminal {
		updatedJob.(*copySegmentJob).snapshotCache = nil
	}
	shouldUnpin := isTerminal && !wasTerminal && updatedJob.GetPinId() > 0
	pinID := updatedJob.GetPinId()
	sourceCollectionID := updatedJob.GetSourceCollectionId()
	snapshotName := updatedJob.GetSnapshotName()
	m.mu.Unlock()

	if !shouldUnpin {
		return true, nil
	}

	unpinCollID, unpinName, remaining, unpinErr := m.snapshotMeta.UnpinSnapshot(ctx, pinID)
	if unpinErr != nil {
		// Unpin failure is non-fatal for the state transition (already persisted).
		// Pins carry a TTL (dataCoord.snapshot.restorePinTTLSeconds) so an orphan
		// left here will self-heal; we still log loudly so operators can detect
		// a broken unpin path early instead of waiting for TTL expiry.
		mlog.Warn(ctx, "failed to unpin source snapshot on job terminal transition, orphan pin will expire via TTL",
			mlog.FieldJobID(jobID),
			mlog.Int64("pinID", pinID),
			mlog.Int64("sourceCollectionID", sourceCollectionID),
			mlog.String("snapshot", snapshotName),
			mlog.Err(unpinErr))
		return true, nil
	}
	if unpinName != "" {
		setSnapshotActivePinsGauge(unpinCollID, unpinName, remaining)
	}
	mlog.Info(ctx, "unpinned source snapshot on job completion",
		mlog.FieldJobID(jobID),
		mlog.Int64("pinID", pinID),
		mlog.Int64("sourceCollectionID", sourceCollectionID),
		mlog.String("snapshot", snapshotName),
		mlog.String("previousState", previousState.String()),
		mlog.String("newState", newState.String()))
	return true, nil
}

// UpdateJobStateAndReleaseRefInState is the CAS variant of
// UpdateJobStateAndReleaseRef: the state transition (and the snapshot unpin)
// happen only when the job is still in expectedState. A caller holding a
// snapshot taken before slow work must use this for terminal transitions — a
// concurrent replan or failure path may have moved the job on, and an
// unconditional write would resurrect it (e.g. Failed -> Completed).
func (m *copySegmentMeta) UpdateJobStateAndReleaseRefInState(ctx context.Context, jobID int64, expectedState datapb.CopySegmentJobState, actions ...UpdateCopySegmentJobAction) (bool, error) {
	return m.updateJobStateAndReleaseRef(ctx, jobID, &expectedState, actions...)
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
		mlog.Info(ctx, "removed copy segment job",
			mlog.FieldJobID(jobID))

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
//  2. Inject runtime dependencies into task
//  3. Persist task to catalog (etcd)
//  4. Add task to in-memory cache
//  5. Release lock
//
// Injecting at add time ensures scheduler-owned tasks use DataCoord's context,
// metadata, snapshot reader, and allocator.
//
// Thread safety: Protected by write lock
func (m *copySegmentMeta) AddTask(ctx context.Context, task CopySegmentTask) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Ensure the task has meta references
	t := task.(*copySegmentTask)
	t.ctx = m.ctx
	t.copyMeta = m
	t.meta = m.meta
	t.snapshotMeta = m.snapshotMeta
	t.alloc = m.alloc

	err := m.catalog.SaveCopySegmentTask(ctx, t.task.Load())
	if err != nil {
		return err
	}
	m.tasks.add(task)
	return nil
}

// PublishReplan publishes a fresh-identity copy task and all of its target
// segments through one composite catalog write. The task action is deliberately
// first: if the operation exceeds the backend transaction limit and the
// ordered fallback is interrupted, recovery sees an exact owner before it sees
// any replacement target. In-memory visibility is all-or-nothing because both
// caches are updated only after the full catalog operation succeeds.
//
// The method is idempotent for crash recovery. A task or target left by an
// ambiguous earlier write is accepted only when its persisted value is exactly
// the value being republished; an ID collision with different metadata is an
// invariant violation, not an upsert.
func (m *copySegmentMeta) PublishReplan(ctx context.Context, task CopySegmentTask, targets []*SegmentInfo) error {
	if m.meta == nil {
		return merr.WrapErrServiceInternalMsg("cannot publish copy segment replan without segment meta")
	}
	incoming, ok := task.(*copySegmentTask)
	if !ok {
		return merr.WrapErrServiceInternalMsg("unsupported copy segment task implementation %T", task)
	}
	if incoming.GetState() != datapb.CopySegmentTaskState_CopySegmentTaskPending || incoming.GetPredecessorTaskId() == 0 {
		return merr.WrapErrServiceInternalMsg(
			"copy segment replan %d must be Pending and name a predecessor", incoming.GetTaskId())
	}

	expectedTargets := make(map[int64]struct{}, len(incoming.GetIdMappings()))
	for _, mapping := range incoming.GetIdMappings() {
		targetID := mapping.GetTargetSegmentId()
		if targetID == 0 {
			return merr.WrapErrServiceInternalMsg("copy segment replan %d contains an empty target ID", incoming.GetTaskId())
		}
		if _, duplicate := expectedTargets[targetID]; duplicate {
			return merr.WrapErrServiceInternalMsg(
				"copy segment replan %d contains duplicate target segment %d", incoming.GetTaskId(), targetID)
		}
		expectedTargets[targetID] = struct{}{}
	}
	if len(targets) != len(expectedTargets) {
		return merr.WrapErrServiceInternalMsg(
			"copy segment replan %d has %d mappings but %d target records",
			incoming.GetTaskId(), len(expectedTargets), len(targets))
	}
	providedTargets := make(map[int64]*SegmentInfo, len(targets))
	for _, target := range targets {
		if target == nil || target.SegmentInfo == nil {
			return merr.WrapErrServiceInternalMsg("copy segment replan %d contains a nil target", incoming.GetTaskId())
		}
		if _, expected := expectedTargets[target.GetID()]; !expected {
			return merr.WrapErrServiceInternalMsg(
				"copy segment replan %d does not map target segment %d", incoming.GetTaskId(), target.GetID())
		}
		if _, duplicate := providedTargets[target.GetID()]; duplicate {
			return merr.WrapErrServiceInternalMsg(
				"copy segment replan %d contains duplicate target record %d", incoming.GetTaskId(), target.GetID())
		}
		providedTargets[target.GetID()] = target
	}

	// Lock order is segment meta -> copy meta. No copy-meta operation calls into
	// segment meta while holding m.mu, so publication cannot invert this order.
	m.meta.segMu.Lock()
	defer m.meta.segMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()

	existingTask := m.tasks.get(incoming.GetTaskId())
	if existingTask != nil &&
		!proto.Equal(existingTask.(*copySegmentTask).task.Load(), incoming.task.Load()) {
		return merr.WrapErrServiceInternalMsg(
			"copy segment replan task ID %d is already owned by different metadata", incoming.GetTaskId())
	}

	missingTargets := make([]*SegmentInfo, 0, len(targets))
	for targetID, target := range providedTargets {
		existing := m.meta.segments.GetSegment(targetID)
		if existing == nil {
			missingTargets = append(missingTargets, target)
			continue
		}
		if !proto.Equal(existing.SegmentInfo, target.SegmentInfo) {
			return merr.WrapErrServiceInternalMsg(
				"copy segment replan target ID %d is already owned by different metadata", targetID)
		}
	}
	if existingTask != nil && len(missingTargets) == 0 {
		return nil
	}

	actions := make([]metastore.UpdateAction, 0, len(missingTargets)+1)
	// Keep the owner first for txn.Commit's ordered over-limit fallback.
	actions = append(actions, metastore.AddCopySegmentTask(incoming.task.Load()))
	for _, target := range missingTargets {
		actions = append(actions, metastore.AddSegment(target.SegmentInfo))
	}
	// This is the one fresh-identity composite swap on the copy path: the task
	// record and all of its fresh targets are published in one write. An
	// ambiguous failure (etcd committed, response lost) leaves the in-memory
	// cache on the predecessor while the durable record names the replacement;
	// continuing could mint a second replacement whose targets collide with
	// the committed first one. Nothing can safely interpret the error as "not
	// committed", so the process aborts and restart reloads the authoritative
	// outcome. Guards: only abort while neither the caller nor the component
	// is being cancelled -- a write error that merely reflects a cancellation
	// must not crash the process. (A non-transport error is unreachable here:
	// every action type this path sends is implemented and the payloads are
	// marshal-safe protos, so every realistic failure is a storage write whose
	// outcome is ambiguous.)
	if err := m.catalog.Update(ctx, actions...); err != nil {
		if ctx.Err() == nil && m.ctx.Err() == nil {
			mlog.Fatal(ctx, "copy segment replan publication failed; terminating process", mlog.Err(err))
		}
		return err
	}

	for _, target := range missingTargets {
		m.meta.segments.SetSegment(target.GetID(), target)
		metrics.DataCoordNumSegments.WithLabelValues(segmentMetricLabelValues(target)...).Inc()
	}
	if existingTask == nil {
		incoming.ctx = m.ctx
		incoming.copyMeta = m
		incoming.meta = m.meta
		incoming.snapshotMeta = m.snapshotMeta
		incoming.alloc = m.alloc
		m.tasks.add(incoming)
	}
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
// A task that is not in the cache is silently skipped and the call returns nil:
// callers hold the scheduler's per-task lock, which is what guarantees the task
// exists, so the no-op path only fires for a record retired out from under an
// already-lost race.
func (m *copySegmentMeta) UpdateTask(ctx context.Context, taskID int64, actions ...UpdateCopySegmentTaskAction) error {
	_, err := m.updateTask(ctx, taskID, nil, actions...)
	return err
}

// UpdateTaskInState is the CAS variant of UpdateTask: the actions apply only
// when the cached task is currently in expectedState, with the check and the
// update under the same write lock. Used on the dispatch path so a task that
// a concurrent failure path (checkFailedJob) already marked Failed is not
// re-dispatched and left InProgress under a terminal job.
//
// Returns (false, nil) when the task is missing or not in expectedState.
func (m *copySegmentMeta) UpdateTaskInState(ctx context.Context, taskID int64, expectedState datapb.CopySegmentTaskState, actions ...UpdateCopySegmentTaskAction) (bool, error) {
	return m.updateTask(ctx, taskID, &expectedState, actions...)
}

func (m *copySegmentMeta) updateTask(ctx context.Context, taskID int64, expectedState *datapb.CopySegmentTaskState, actions ...UpdateCopySegmentTaskAction) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.tasks.get(taskID)
	if task == nil {
		return false, nil
	}
	if expectedState != nil && task.GetState() != *expectedState {
		return false, nil
	}
	updatedTask := task.Clone()
	for _, action := range actions {
		action(updatedTask)
	}
	err := m.catalog.SaveCopySegmentTask(ctx, updatedTask.(*copySegmentTask).task.Load())
	if err != nil {
		return false, err
	}
	// update memory task atomically
	task.(*copySegmentTask).task.Store(updatedTask.(*copySegmentTask).task.Load())
	return true, nil
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

// GetReplacementByPredecessor returns the successor naming predecessorTaskID
// as its predecessor, or nil. replanUnderFreshIdentity adopts an existing
// successor instead of minting a second identity, so at most one exists.
func (m *copySegmentMeta) GetReplacementByPredecessor(ctx context.Context, predecessorTaskID int64) CopySegmentTask {
	m.mu.RLock()
	defer m.mu.RUnlock()
	successors := m.tasks.getByPredecessorID(predecessorTaskID)
	if len(successors) == 0 {
		return nil
	}
	// Deterministic pick for any pre-uniqueness residue: the earliest attempt.
	sort.Slice(successors, func(i, j int) bool {
		return successors[i].GetTaskId() < successors[j].GetTaskId()
	})
	return successors[0]
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
// A task that is not in the cache is silently skipped and the call returns nil:
// removal is idempotent by design, so a double remove is not an error.
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
