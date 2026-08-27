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

	"github.com/cockroachdb/errors"
	"golang.org/x/exp/maps"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore"
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
	UpdateJobStateAndReleaseRef(ctx context.Context, jobID int64, actions ...UpdateCopySegmentJobAction) (bool, error)
	TimeoutJob(ctx context.Context, jobID int64, reason string) (bool, error)
	FinalizeJobPublication(ctx context.Context, jobID int64, totalRows int64, completeTs uint64, segmentOperators ...[]UpdateOperator) (bool, error)
	GetJob(ctx context.Context, jobID int64) CopySegmentJob
	GetJobBy(ctx context.Context, filters ...CopySegmentJobFilter) []CopySegmentJob
	CountJobBy(ctx context.Context, filters ...CopySegmentJobFilter) int
	RemoveJob(ctx context.Context, jobID int64) error

	// Task operations
	AddTask(ctx context.Context, task CopySegmentTask) error
	UpdateTask(ctx context.Context, taskID int64, actions ...UpdateCopySegmentTaskAction) error
	ClaimTaskDispatch(ctx context.Context, taskID int64) (int64, error)
	ReleaseTaskDispatch(taskID int64)
	CommitTaskDispatch(ctx context.Context, taskID, nodeID int64, inactiveReason string) (taskDispatchResolution, error)
	ResolveTaskOnWorkerLoss(ctx context.Context, taskID int64, failReason string) (workerLossOutcome, error)
	// EnqueueUntrackedDrop hands the cleanup of one exact worker dispatch
	// (nodeID, taskID, taskVersion) to the retry owner and reports whether it
	// took ownership. HasPendingUntrackedDrop reports whether such a cleanup is
	// still outstanding for taskID. They are part of the dispatch fence, not an
	// optimization: a dispatch must not start while an earlier one's abort may
	// still land on the shared target keys, and a worker task that metadata can
	// no longer address must still be dropped.
	EnqueueUntrackedDrop(nodeID, taskID, taskVersion int64) bool
	HasPendingUntrackedDrop(taskID int64) bool
	TaskAcceptsWorkerResult(ctx context.Context, taskID int64) bool
	CompleteTaskIfActive(ctx context.Context, taskID int64, completeTs uint64) (bool, error)
	ClearTaskNodeAssignment(ctx context.Context, taskID, expectedNodeID int64) (bool, error)
	GetTask(ctx context.Context, taskID int64) CopySegmentTask
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
	mu           lock.RWMutex // Protects jobs and tasks maps
	ctx          context.Context
	jobs         map[int64]CopySegmentJob   // Job ID -> Job mapping (in-memory cache)
	tasks        *copySegmentTasks          // Task collection (in-memory cache)
	catalog      metastore.DataCoordCatalog // Persistent storage backend (etcd)
	meta         *meta                      // Segment metadata for task execution
	snapshotMeta *snapshotMeta              // Snapshot metadata for reading source data
	alloc        allocator.Allocator        // For allocating new build IDs in copy segment tasks

	// A worker task accepted after metadata moved elsewhere cannot be represented
	// by the task's single NodeID field. The inspector registers this handler so
	// dispatch can hand the exact (nodeID, taskID, taskVersion) cleanup target to
	// its retry loop instead of taking one synchronous, lossy attempt.
	untrackedDropMu      lock.RWMutex
	untrackedDropHandler UntrackedCopySegmentDropHandler

	// Task IDs whose dispatch has been claimed but not yet resolved. The
	// scheduler pops a task out of pendingTasks and only inserts it into
	// runningTasks once CommitTaskDispatch has persisted InProgress, so in
	// between the task is invisible to Enqueue's dedup and the inspector's next
	// round re-queues it — a second dispatch then starts, on a different node,
	// while the first is still in flight. Neither the worker-side epoch fence
	// (same node only) nor the untracked-drop gate (drops queued earlier only)
	// covers that window. Guarded by mu so claiming a dispatch and stamping its
	// epoch are one atomic step.
	dispatching map[int64]struct{}
}

// errCopySegmentDispatchInFlight reports that another dispatch of this task has
// already been handed to a worker and has not resolved yet. It is a scheduling
// signal, not a failure: the caller abandons its attempt and leaves the task
// Pending so the scheduler re-queues it.
var errCopySegmentDispatchInFlight = errors.New("copy segment task dispatch already in flight")

// errCopySegmentDispatchStale reports that the preconditions this dispatch was
// started under no longer hold — the task has moved on, is already assigned, its
// job is over, or cleanup of an earlier dispatch is still outstanding. Like
// errCopySegmentDispatchInFlight it is a scheduling signal, not a failure: the
// caller abandons its attempt without touching the task.
var errCopySegmentDispatchStale = errors.New("copy segment task dispatch preconditions no longer hold")

// UntrackedCopySegmentDropHandler owns the retry of worker-side cleanup for a
// dispatch that metadata cannot represent. Implemented by the copy segment
// inspector.
type UntrackedCopySegmentDropHandler interface {
	// EnqueueUntrackedDrop takes ownership of the cleanup of one exact worker
	// dispatch. It returns false when the handler can no longer own the retry
	// (inspector closed), in which case the caller must fall back to a direct
	// attempt.
	EnqueueUntrackedDrop(nodeID, taskID, taskVersion int64) bool

	// HasPendingUntrackedDrop reports whether cleanup of an earlier dispatch of
	// this task is still outstanding.
	HasPendingUntrackedDrop(taskID int64) bool
}

func (m *copySegmentMeta) setUntrackedDropHandler(handler UntrackedCopySegmentDropHandler) {
	m.untrackedDropMu.Lock()
	defer m.untrackedDropMu.Unlock()
	m.untrackedDropHandler = handler
}

// EnqueueUntrackedDrop delegates to the registered handler (the inspector).
// It returns false when no handler is registered or the handler can no longer
// own the retry, in which case the caller must fall back to a direct attempt.
func (m *copySegmentMeta) EnqueueUntrackedDrop(nodeID, taskID, taskVersion int64) bool {
	m.untrackedDropMu.RLock()
	handler := m.untrackedDropHandler
	m.untrackedDropMu.RUnlock()
	return handler != nil && handler.EnqueueUntrackedDrop(nodeID, taskID, taskVersion)
}

// HasPendingUntrackedDrop delegates to the registered handler (the inspector).
// Uses untrackedDropMu and the handler's own state, never m.mu, so it is safe
// to consult while holding m.mu.
func (m *copySegmentMeta) HasPendingUntrackedDrop(taskID int64) bool {
	m.untrackedDropMu.RLock()
	handler := m.untrackedDropHandler
	m.untrackedDropMu.RUnlock()
	return handler != nil && handler.HasPendingUntrackedDrop(taskID)
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
		// Deliberately not restored from the catalog: no dispatch survives a
		// DataCoord restart, and the persisted epoch is monotonic, so a task
		// reloaded as InProgress is handled by the existing worker-loss paths.
		dispatching: make(map[int64]struct{}),
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
	if err := copySegmentMeta.reconcileRestoredTargetSegments(ctx); err != nil {
		return nil, err
	}

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

// reconcileRestoredTargetSegments repairs the visibility state left by a
// crash during a batched restore publication before DataCoord starts serving.
//
// meta.UpdateSegmentsInfo keeps the in-memory update atomic under segMu, but
// the catalog may split a large AlterSegments call across several etcd
// transactions. If DataCoord crashes after one batch, some target segments can
// be persisted as Flushed while the parent job is still Executing/Failed. On
// restart those segments must be hidden (active job) or Dropped (failed job)
// before QueryCoord can observe them.
func (m *copySegmentMeta) reconcileRestoredTargetSegments(ctx context.Context) error {
	if m.meta == nil {
		return nil
	}

	operators := make([]UpdateOperator, 0)
	for _, job := range m.jobs {
		if job.GetState() == datapb.CopySegmentJobState_CopySegmentJobCompleted {
			continue
		}

		for _, mapping := range job.GetIdMappings() {
			segmentID := mapping.GetTargetSegmentId()
			segment := m.meta.GetSegment(ctx, segmentID)
			if segment == nil {
				continue
			}

			if job.GetState() == datapb.CopySegmentJobState_CopySegmentJobFailed {
				if segment.GetState() != commonpb.SegmentState_Dropped {
					operators = append(operators, UpdateStatusOperator(segmentID, commonpb.SegmentState_Dropped))
				}
				continue
			}

			// Active jobs must keep every target hidden. Only reset states that
			// can result from a partial publication; never resurrect a segment
			// that was already Dropped by failure cleanup.
			if segment.GetState() == commonpb.SegmentState_Flushed {
				operators = append(operators, UpdateStatusOperator(segmentID, commonpb.SegmentState_Importing))
			}
			if segment.GetState() != commonpb.SegmentState_Dropped && !segment.GetIsImporting() {
				operators = append(operators, UpdateIsImporting(segmentID, true))
			}
		}
	}

	if len(operators) == 0 {
		return nil
	}
	if err := m.meta.UpdateSegmentsInfo(ctx, operators...); err != nil {
		return merr.Wrap(err, "reconcile restored copy-segment target visibility")
	}
	mlog.Info(ctx, "reconciled restored copy-segment target visibility",
		mlog.Int("operatorCount", len(operators)))
	return nil
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

// isTerminalCopyJobState reports whether a copy segment job state is terminal.
// Terminal states (Completed/Failed) are final: no transition out of them, and
// no transition between them, is ever legitimate.
func isTerminalCopyJobState(state datapb.CopySegmentJobState) bool {
	return state == datapb.CopySegmentJobState_CopySegmentJobCompleted ||
		state == datapb.CopySegmentJobState_CopySegmentJobFailed
}

// isActiveCopyJobState reports states in which copy work may still fail or
// time out. Publishing has already claimed the successful outcome and is only
// allowed to retry its final visibility commit.
func isActiveCopyJobState(state datapb.CopySegmentJobState) bool {
	return state == datapb.CopySegmentJobState_CopySegmentJobPending ||
		state == datapb.CopySegmentJobState_CopySegmentJobExecuting
}

// UpdateJobStateAndReleaseRef updates job state and unpins the source snapshot
// if the job transitions to a terminal state (Completed/Failed).
//
// This ensures snapshot pins are released immediately when restore jobs finish,
// while Job records are retained for audit purposes (3 hours).
//
// Terminal-state guard: the update is skipped when the *current* cached job is
// already terminal or Publishing. Publishing has claimed the successful
// outcome and may only transition to Completed through FinalizeJobPublication;
// failure/timeout paths observing it are stale and must be rejected. Applying
// the update anyway would rewrite the winner's outcome. The concrete case reported
// in review: the checker loop processes one job snapshot through
// checkCopyingJob (which may finishJob -> Completed) and then tryTimeoutJob in
// the same round; the latter still sees the pre-loop Executing snapshot and,
// once the deadline has elapsed, would flip the just-Completed job to Failed.
// The check and the mutate share m.mu, so the decision cannot go stale.
//
// Returns (true, nil) when the transition was applied, and (false, nil) when it
// was skipped — the job is missing, or the terminal-state guard fired because a
// concurrent path already committed a terminal outcome. Callers must gate any
// outcome-specific side effect (timeout warning, completion metrics/logs) on the
// applied flag: after a skipped update the caller's outcome did NOT happen.
//
// Locking strategy: the state-mutate section takes m.mu; the Unpin call (an etcd
// roundtrip via snapshotMeta.SaveSnapshot) runs AFTER releasing m.mu to avoid
// blocking all copy-segment job operations on an external write. Double-unpin is
// prevented by the terminal-state guard above: the first caller flips the job to
// a terminal state under m.mu, so every later caller returns at the guard and
// never reaches the Unpin call. Past the guard prevJob is therefore always
// active and wasTerminal is always false; the `!wasTerminal` term in
// shouldUnpin is kept only as a local invariant assertion.
func (m *copySegmentMeta) UpdateJobStateAndReleaseRef(ctx context.Context, jobID int64, actions ...UpdateCopySegmentJobAction) (bool, error) {
	return m.transitionJobAndReleaseRef(ctx, jobID, func(state datapb.CopySegmentJobState) bool {
		return isTerminalCopyJobState(state) || state == datapb.CopySegmentJobState_CopySegmentJobPublishing
	}, actions...)
}

// TimeoutJob converges a job whose deadline has elapsed to Failed. Unlike
// UpdateJobStateAndReleaseRef it is allowed to take a Publishing job: the
// deadline is the bound on publication retry. Publishing rejects every other
// failure path because those observe a stale view of a job that has claimed
// the successful outcome, but a job that cannot persist that outcome before
// its deadline must still converge — otherwise a persistent catalog failure
// (etcd quota, an oversized write that keeps failing, a target segment gone
// from meta) leaves the job in Publishing forever: never timed out, never
// reclaimed by GC, reported to the user as an executing restore, and its
// source pin held until the pin TTL expires.
//
// Failing a Publishing job is safe with respect to the outcome fence:
// FinalizeJobPublication re-checks the Publishing state under m.mu on every
// chunk and before the Completed record, so this transition either lands
// between chunks (the publication stops at the next boundary and never writes
// Completed) or after the Completed record (and the terminal-state guard here
// rejects). Target segments that earlier chunks already persisted as Flushed
// are dropped by the inspector's failed-job cleanup and hidden by
// reconcileRestoredTargetSegments on restart, exactly as for any other Failed
// job.
func (m *copySegmentMeta) TimeoutJob(ctx context.Context, jobID int64, reason string) (bool, error) {
	return m.transitionJobAndReleaseRef(ctx, jobID, isTerminalCopyJobState,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
		UpdateCopyJobReason(reason))
}

// transitionJobAndReleaseRef applies actions unless fenced(currentState) holds,
// with the check and the mutate under m.mu, and unpins the source snapshot when
// the job thereby becomes terminal. See UpdateJobStateAndReleaseRef.
func (m *copySegmentMeta) transitionJobAndReleaseRef(ctx context.Context, jobID int64, fenced func(datapb.CopySegmentJobState) bool, actions ...UpdateCopySegmentJobAction) (bool, error) {
	m.mu.Lock()
	if current, ok := m.jobs[jobID]; ok && fenced(current.GetState()) {
		currentState := current.GetState()
		m.mu.Unlock()
		mlog.Info(ctx, "copy segment job outcome already fenced, skip state transition",
			mlog.FieldJobID(jobID), mlog.String("currentState", currentState.String()))
		return false, nil
	}
	prevJob, updatedJob, err := m.updateJob(ctx, jobID, actions...)
	if err != nil {
		m.mu.Unlock()
		return false, err
	}
	if prevJob == nil {
		m.mu.Unlock()
		mlog.Warn(ctx, "UpdateJobStateAndReleaseRef: job not found", mlog.FieldJobID(jobID))
		return false, nil
	}

	previousState := prevJob.GetState()
	newState := updatedJob.GetState()
	isTerminal := isTerminalCopyJobState(newState)
	wasTerminal := isTerminalCopyJobState(previousState)

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

// copySegmentPublicationChunk bounds how many target segments one publication
// write covers. Each chunk is cloned, persisted, and applied under one
// m.mu + meta.segMu window; 64 matches the etcd single-transaction op budget
// (metastore's maxEtcdTxnNum), so a chunk commits in one transaction instead
// of the catalog's sequential fallback. Without the bound, publishing an
// N-segment restore held the GLOBAL segment lock across ceil(N/64) sequential
// etcd transactions — for a 10k-segment restore that is one segMu window over
// ~157 transactions, stalling every segment reader and writer (AssignSegmentID,
// SaveBinlogPaths, compaction commit, GC) for the whole publication.
const copySegmentPublicationChunk = 64

// FinalizeJobPublication persists all target segment visibility changes in
// bounded chunks and then commits the Completed job record as the marker.
//
// Trade-off, made deliberately: publication is no longer one atomic catalog
// write, so a reader can transiently observe some target segments Flushed
// while later chunks are still Importing and the job is still Publishing.
// What stays guaranteed:
//   - Completed is written only after EVERY chunk landed, and only under a
//     re-check that the job is still Publishing (serialized on m.mu against
//     TimeoutJob), so the outcome fence holds unchanged.
//   - A crash or failure between chunks leaves the durable job in Publishing:
//     startup reconciliation (reconcileRestoredTargetSegments) re-hides the
//     partially published targets, and the checker retries publication —
//     finishJob rebuilds only the operators still needed, so the retry is
//     idempotent. A Publishing job that exhausts its deadline converges to
//     Failed (TimeoutJob) and the inspector's failed-job cleanup drops every
//     target, published chunks included.
//
// Each chunk re-checks the job state, so a concurrent Failed transition stops
// the publication between chunks instead of racing it.
func (m *copySegmentMeta) FinalizeJobPublication(ctx context.Context, jobID int64, totalRows int64, completeTs uint64, segmentOperators ...[]UpdateOperator) (bool, error) {
	for start := 0; start < len(segmentOperators); start += copySegmentPublicationChunk {
		end := min(start+copySegmentPublicationChunk, len(segmentOperators))
		chunk := make([]UpdateOperator, 0, (end-start)*2)
		for _, group := range segmentOperators[start:end] {
			chunk = append(chunk, group...)
		}
		applied, err := m.publishTargetSegmentsChunk(ctx, jobID, chunk)
		if err != nil || !applied {
			return false, err
		}
	}

	m.mu.Lock()
	job, ok := m.jobs[jobID]
	if !ok || job.GetState() != datapb.CopySegmentJobState_CopySegmentJobPublishing {
		m.mu.Unlock()
		return false, nil
	}

	updatedJob := job.Clone()
	UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted)(updatedJob)
	UpdateCopyJobCompleteTs(completeTs)(updatedJob)
	UpdateCopyJobTotalRows(totalRows)(updatedJob)

	if err := m.catalog.Update(ctx, metastore.SaveCopySegmentJob(updatedJob.(*copySegmentJob).CopySegmentJob)); err != nil {
		m.mu.Unlock()
		return false, err
	}

	// Completed is terminal, so the snapshot cache can never be read again.
	// Clone() shares it by pointer and it holds the whole SnapshotData, so
	// keeping it here would pin that memory on the finished job until checkGC
	// collects it CopySegmentTaskRetention later. The sibling terminal path,
	// UpdateJobStateAndReleaseRef, drops it for the same reason.
	updatedJob.(*copySegmentJob).snapshotCache = nil
	m.jobs[jobID] = updatedJob
	m.mu.Unlock()

	if updatedJob.GetPinId() > 0 {
		unpinCollID, unpinName, remaining, unpinErr := m.snapshotMeta.UnpinSnapshot(ctx, updatedJob.GetPinId())
		if unpinErr != nil {
			mlog.Warn(ctx, "failed to unpin source snapshot after copy-segment publication; pin will expire via TTL",
				mlog.FieldJobID(jobID), mlog.Int64("pinID", updatedJob.GetPinId()), mlog.Err(unpinErr))
			return true, nil
		}
		if unpinName != "" {
			setSnapshotActivePinsGauge(unpinCollID, unpinName, remaining)
		}
	}
	return true, nil
}

// publishTargetSegmentsChunk persists and applies one bounded batch of target
// visibility updates. The Publishing re-check runs under m.mu on every chunk,
// so a concurrent terminal transition (TimeoutJob, a late failure) stops the
// publication at the next chunk boundary; (false, nil) tells the caller the
// outcome was claimed elsewhere. meta.segMu is held only for this chunk's
// prepare + single catalog transaction + in-memory apply.
func (m *copySegmentMeta) publishTargetSegmentsChunk(ctx context.Context, jobID int64, operators []UpdateOperator) (bool, error) {
	if len(operators) == 0 {
		return true, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	job, ok := m.jobs[jobID]
	if !ok || job.GetState() != datapb.CopySegmentJobState_CopySegmentJobPublishing {
		return false, nil
	}

	m.meta.segMu.Lock()
	defer m.meta.segMu.Unlock()
	updatePack, err := m.meta.prepareUpdateSegmentsInfoLocked(ctx, operators...)
	if err != nil {
		return false, err
	}
	if updatePack == nil {
		return true, nil
	}
	// This path hand-builds its catalog actions instead of going through
	// meta.UpdateSegmentsInfo, so it must not silently diverge from it. The
	// publication operators only flip visibility (state, IsImporting); binlog
	// increments would be dropped on the floor here, so reject any operator
	// that produces one rather than persist a segment record without its logs.
	if len(updatePack.increments) > 0 {
		return false, merr.WrapErrServiceInternalMsg(
			"copy segment publication of job %d must not carry binlog increments (%d segments)",
			jobID, len(updatePack.increments))
	}

	actions := make([]metastore.UpdateAction, 0, len(updatePack.segments))
	for _, segment := range updatePack.segments {
		// AlterSegment is the encoding meta.UpdateSegmentsInfo uses
		// (catalog.AlterSegments): it keeps the V3 guard on binlog-based
		// row-count reconciliation, which the record-only UpdateSegment
		// encoding bypasses — and every restore target looks V3 from
		// pre-registration onward.
		actions = append(actions, metastore.AlterSegment(segment.SegmentInfo))
	}
	if len(actions) == 0 {
		return true, nil
	}
	if err := m.catalog.Update(ctx, actions...); err != nil {
		return false, err
	}
	m.meta.applyUpdateSegmentsInfoLocked(updatePack)
	return true, nil
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
		return m.updateTaskLocked(ctx, task, actions...)
	}
	return nil
}

// ClaimTaskDispatch takes exclusive ownership of a task's dispatch and returns
// the epoch the claiming dispatch must carry. It is the single authoritative
// gate on whether a dispatch may reach a worker: it returns
// errCopySegmentDispatchInFlight when another dispatch already holds the task,
// and errCopySegmentDispatchStale when the preconditions the caller sampled
// before its snapshot read no longer hold.
//
// The epoch fences worker-side cleanup: DataNode binds an accepted task to the
// epoch carried by its CreateCopySegment request and ignores a drop whose epoch
// no longer matches. Without it, a drop issued for an earlier dispatch can be
// delivered after the task was re-dispatched, and — because target object keys
// are a deterministic transform of the source keys, identical across dispatches
// — its abort would delete the output the current dispatch already published.
//
// The epoch alone is not enough, because it is bound to the worker's task
// runtime and therefore only recognizes a re-dispatch landing on the same node.
// Two dispatches racing onto two different nodes both pass it: the loser's
// CommitTaskDispatch finds the winner already tracked, queues an abort for its
// own node, and that abort deletes the winner's output from the shared target
// keys. The claim closes that window at the source by letting only one dispatch
// of a task reach a worker at a time; ownership is released once the outcome is
// committed, after which the untracked-drop gate takes over.
//
// The epoch is persisted before the RPC, so it is monotonic across a DataCoord
// restart: an epoch is never reused for a worker task that may still exist.
func (m *copySegmentMeta) ClaimTaskDispatch(ctx context.Context, taskID int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.tasks.get(taskID)
	if task == nil {
		return 0, merr.WrapErrParameterInvalidMsg("copy segment task %d not found", taskID)
	}
	if _, inFlight := m.dispatching[taskID]; inFlight {
		return 0, errCopySegmentDispatchInFlight
	}

	// Re-check every dispatch precondition here rather than trusting the
	// caller's pre-flight checks. Those are sampled before
	// AssembleCopySegmentRequest, whose remote snapshot read can outlast several
	// inspector rounds while the task is invisible to the scheduler's dedup
	// (popped out of pendingTasks, not yet in runningTasks), so a second closure
	// can arrive here holding a view of the task from before the first dispatch
	// even started. The claim is the only point at which a dispatch is
	// serialized against its predecessor's committed outcome, so it is the only
	// place these can be observed and acted on atomically. The caller keeps its
	// checks purely to skip the snapshot read early.
	//
	// Without this, a stale Pending observation is promoted into a real second
	// dispatch on another node, and that dispatch's own cleanup deletes the
	// winner's output — the target object keys are a deterministic transform of
	// the source keys, so both dispatches write the very same objects.
	if task.GetState() != datapb.CopySegmentTaskState_CopySegmentTaskPending ||
		task.GetNodeId() != NullNodeID {
		return 0, errCopySegmentDispatchStale
	}
	job, jobExists := m.jobs[task.GetJobId()]
	if !jobExists || !isActiveCopyJobState(job.GetState()) {
		return 0, errCopySegmentDispatchStale
	}
	// Uses untrackedDropMu and the inspector's own set, never m.mu, so it is
	// safe to consult while holding the write lock here.
	if m.HasPendingUntrackedDrop(taskID) {
		return 0, errCopySegmentDispatchStale
	}

	version := task.GetTaskVersion() + 1
	if err := m.updateTaskLocked(ctx, task, UpdateCopyTaskVersion(version)); err != nil {
		// The claim is not taken when the epoch could not be persisted: no
		// worker can be holding this dispatch, and holding the task would stall
		// it for good since nothing would ever release it.
		return 0, err
	}
	m.dispatching[taskID] = struct{}{}
	return version, nil
}

// ReleaseTaskDispatch gives up ownership taken by ClaimTaskDispatch. It must run
// on every path out of a dispatch, including the failed ones — the claim is the
// only thing standing between the task and a worker, so a leaked claim stalls
// the task permanently.
func (m *copySegmentMeta) ReleaseTaskDispatch(taskID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.dispatching, taskID)
}

// hasInFlightDispatch reports whether a dispatch of this task is currently
// claimed. Test-facing view of the claim set.
func (m *copySegmentMeta) hasInFlightDispatch(taskID int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, inFlight := m.dispatching[taskID]
	return inFlight
}

// ClearTaskNodeAssignment clears a terminal cleanup handle only when it still
// points at the node whose Drop RPC just succeeded. This prevents an old
// inspector or delayed RPC from erasing a newer assignment.
func (m *copySegmentMeta) ClearTaskNodeAssignment(ctx context.Context, taskID, expectedNodeID int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.tasks.get(taskID)
	if task == nil || task.GetNodeId() != expectedNodeID {
		return false, nil
	}
	if err := m.updateTaskLocked(ctx, task, UpdateCopyTaskNodeID(NullNodeID)); err != nil {
		return false, err
	}
	return true, nil
}

// updateTaskLocked clones the task, applies the actions, persists the clone and
// swaps it into the cache. Callers must hold m.mu.
func (m *copySegmentMeta) updateTaskLocked(ctx context.Context, task CopySegmentTask, actions ...UpdateCopySegmentTaskAction) error {
	updatedTask := task.Clone()
	for _, action := range actions {
		action(updatedTask)
	}
	if err := m.catalog.SaveCopySegmentTask(ctx, updatedTask.(*copySegmentTask).task.Load()); err != nil {
		return err
	}
	// update memory task atomically
	task.(*copySegmentTask).task.Store(updatedTask.(*copySegmentTask).task.Load())
	return nil
}

// taskDispatchResolution describes how DataCoord accounted for a copy task
// after the worker accepted its CreateCopySegment RPC.
type taskDispatchResolution int

const (
	// taskDispatchApplied: the task was still Pending under an active job and
	// is now persisted as InProgress on the selected node.
	taskDispatchApplied taskDispatchResolution = iota
	// taskDispatchAlreadyTracked: another concurrent/idempotent dispatch
	// already persisted the same task on the same node. No cleanup or metrics
	// should be emitted by this caller.
	taskDispatchAlreadyTracked
	// taskDispatchCleanupTracked: the task/job became terminal while the RPC
	// was in flight. The accepted node assignment was persisted on a terminal
	// task so DropTaskOnWorker (and later the inspector) can clean it up.
	taskDispatchCleanupTracked
	// taskDispatchCleanupUntracked: the accepted worker task cannot safely be
	// represented in the current metadata (missing task or another node is
	// already tracked). The caller must drop this exact node directly.
	taskDispatchCleanupUntracked
)

// isTerminalCopyTaskState reports whether a copy task state is final.
func isTerminalCopyTaskState(state datapb.CopySegmentTaskState) bool {
	return state == datapb.CopySegmentTaskState_CopySegmentTaskCompleted ||
		state == datapb.CopySegmentTaskState_CopySegmentTaskFailed
}

// CommitTaskDispatch atomically commits the result of a successful worker
// CreateCopySegment RPC without resurrecting a task or dispatching a dead job.
//
// The RPC necessarily happens outside m.mu. While it is in flight, a sibling
// task can fail the parent job and checkFailedJob can move this task to Failed.
// An unconditional Pending/Failed -> InProgress write would then revive a
// terminal task. This method re-checks task and job state under one write lock:
//
//   - Pending task + active job -> InProgress + nodeID;
//   - terminal/inactive task with no assignment -> preserve/converge terminal
//     state and persist nodeID so cleanup is retryable;
//   - same task already InProgress on this node -> idempotent no-op;
//   - task already assigned to this same node -> metadata is already the
//     cleanup handle for this worker, so hand it to the tracked drop path
//     rather than forcing an abort on it;
//   - a different node is assigned -> do not overwrite existing metadata;
//     caller directly drops the newly accepted worker task.
func (m *copySegmentMeta) CommitTaskDispatch(ctx context.Context, taskID, nodeID int64, inactiveReason string) (taskDispatchResolution, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	task := m.tasks.get(taskID)
	if task == nil {
		return taskDispatchCleanupUntracked, nil
	}

	job, jobExists := m.jobs[task.GetJobId()]
	jobActive := jobExists && isActiveCopyJobState(job.GetState())
	if jobActive && task.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskPending {
		if err := m.updateTaskLocked(ctx, task,
			UpdateCopyTaskNodeID(nodeID),
			UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskInProgress)); err != nil {
			return taskDispatchCleanupUntracked, err
		}
		return taskDispatchApplied, nil
	}

	if jobActive && task.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskInProgress && task.GetNodeId() == nodeID {
		return taskDispatchAlreadyTracked, nil
	}

	// An assignment pointing at a *different* node is already authoritative.
	// Never overwrite it: it is the only retry handle for that worker-side task,
	// so the caller has to drop the node it just dispatched to directly.
	//
	// The same node is a different situation and must not take that path. The
	// untracked drop is an unconditional abort at this dispatch's epoch, and the
	// worker accepts it — CreateCopySegment folded this dispatch into the task
	// already registered there and adopted its epoch onto the shared runtime.
	// The abort would then delete the output of the very task the assignment
	// points at, including a Completed one whose binlogs are already in segment
	// metadata and about to be published. Metadata already tracks this exact
	// worker, so cleanup is retryable through the task's own assignment; fall
	// through to the tracked paths below, which drop with an abort flag derived
	// from the task and job state instead of forcing one.
	if task.GetNodeId() != NullNodeID && task.GetNodeId() != nodeID {
		return taskDispatchCleanupUntracked, nil
	}

	if isTerminalCopyTaskState(task.GetState()) {
		if task.GetNodeId() == nodeID {
			// Already the persisted handle; nothing to write.
			return taskDispatchCleanupTracked, nil
		}
		if err := m.updateTaskLocked(ctx, task, UpdateCopyTaskNodeID(nodeID)); err != nil {
			return taskDispatchCleanupUntracked, err
		}
		return taskDispatchCleanupTracked, nil
	}

	if !jobActive {
		if err := m.updateTaskLocked(ctx, task,
			UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed),
			UpdateCopyTaskNodeID(nodeID),
			UpdateCopyTaskReason(inactiveReason)); err != nil {
			return taskDispatchCleanupUntracked, err
		}
		return taskDispatchCleanupTracked, nil
	}

	return taskDispatchCleanupUntracked, nil
}

// workerLossResolution is the outcome of ResolveTaskOnWorkerLoss.
type workerLossResolution int

const (
	// workerLossSkipped: the task is missing or no longer InProgress — a
	// concurrent path (checkFailedJob, a worker result) already transitioned it,
	// so there is nothing left to resolve.
	workerLossSkipped workerLossResolution = iota
	// workerLossRedispatched: the parent job is still active; the task was reset
	// to Pending with NullNodeID so the scheduler re-dispatches it to a live node.
	workerLossRedispatched
	// workerLossFailed: the parent job is terminal or missing; the task was
	// converged to Failed with NullNodeID. It can never be dispatched again
	// (only Pending tasks are), and with no node assignment left it no longer
	// blocks checkGC from reclaiming the task and its job.
	workerLossFailed
)

// workerLossOutcome is what ResolveTaskOnWorkerLoss decided and, when it
// cleared a node assignment, which exact worker dispatch that assignment
// named. The caller queues the worker-side cleanup of that dispatch: the loss
// was inferred from DataCoord's view (a QueryCopySegment error), which does
// not prove the DataNode process — and the task entry, its slot and its copy
// goroutines — is gone. Once NodeId is cleared nothing else will ever address
// that dispatch, so it must be handed to the untracked cleanup path here.
type workerLossOutcome struct {
	resolution workerLossResolution
	// nodeID is the worker whose assignment was cleared; NullNodeID when the
	// resolution is workerLossSkipped.
	nodeID int64
	// taskVersion is the epoch of the dispatch that was lost, i.e. the epoch
	// the cleanup must carry so a later dispatch is never aborted by it.
	taskVersion int64
}

// ResolveTaskOnWorkerLoss atomically decides the fate of a task whose
// worker-side counterpart is confirmed lost (DataNode restarted/replaced, or
// its task manager dropped the task). All checks and the resulting update run
// under one write lock, so the decision cannot go stale:
//
//   - task missing or no longer InProgress -> workerLossSkipped (no-op);
//   - parent job still active -> reset to Pending + NullNodeID for re-dispatch
//     (workerLossRedispatched);
//   - parent job terminal or missing -> converge to Failed + NullNodeID with
//     failReason (workerLossFailed).
//
// The last branch matters for GC: a delayed loss response can land after the
// parent job already failed. Leaving the task InProgress on the dead node would
// block checkGC forever — it skips any task whose NodeID is still set, and no
// later path clears an assignment that never reaches a worker again.
// The task must never be revived to Pending here: the scheduler would issue one
// more dispatch for an already-dead job before checkFailedJob converges it.
//
// Both non-skip outcomes clear the task's NodeId. The outcome carries the
// cleared node and the epoch of the lost dispatch so the caller can queue an
// untracked drop for it: "confirmed lost" is DataCoord's inference from a
// query error, and if the worker is in fact alive its task entry, slot and
// copy goroutines would otherwise be orphaned — nothing addresses a dispatch
// whose NodeId is gone. Re-dispatch is then withheld until that cleanup
// converges (ClaimTaskDispatch consults HasPendingUntrackedDrop), which is
// what keeps a live stale worker from writing the shared target keys
// alongside the new dispatch. A node that is really gone converges once its
// absence from the client map has outlasted a full session-lease interval
// (the inspector's nodeConfirmedGone criterion).
func (m *copySegmentMeta) ResolveTaskOnWorkerLoss(ctx context.Context, taskID int64, failReason string) (workerLossOutcome, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	skipped := workerLossOutcome{resolution: workerLossSkipped, nodeID: NullNodeID}
	task := m.tasks.get(taskID)
	if task == nil || task.GetState() != datapb.CopySegmentTaskState_CopySegmentTaskInProgress {
		return skipped, nil
	}
	lost := workerLossOutcome{nodeID: task.GetNodeId(), taskVersion: task.GetTaskVersion()}

	job, ok := m.jobs[task.GetJobId()]
	if ok && isActiveCopyJobState(job.GetState()) {
		if err := m.updateTaskLocked(ctx, task,
			UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskPending),
			UpdateCopyTaskNodeID(NullNodeID)); err != nil {
			return skipped, err
		}
		lost.resolution = workerLossRedispatched
		return lost, nil
	}

	if err := m.updateTaskLocked(ctx, task,
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed),
		UpdateCopyTaskNodeID(NullNodeID),
		UpdateCopyTaskReason(failReason)); err != nil {
		return skipped, err
	}
	lost.resolution = workerLossFailed
	return lost, nil
}

// taskAcceptsResultLocked reports whether a worker result for taskID may still
// be committed: the task must still be InProgress and its parent job must still
// be active. Callers must hold m.mu.
func (m *copySegmentMeta) taskAcceptsResultLocked(taskID int64) bool {
	task := m.tasks.get(taskID)
	if task == nil || task.GetState() != datapb.CopySegmentTaskState_CopySegmentTaskInProgress {
		return false
	}
	job, ok := m.jobs[task.GetJobId()]
	return ok && isActiveCopyJobState(job.GetState())
}

// TaskAcceptsWorkerResult reports whether a worker result for taskID is still
// relevant, i.e. the task is InProgress under an active job.
//
// A worker response can arrive long after the query that asked for it. In the
// meantime a sibling task may have failed the parent job, and the next checker
// round converges every Pending/InProgress task of a failed job to Failed
// (checkFailedJob). The scheduler nonetheless polls the task once more — it
// calls QueryTaskOnWorker before inspecting the task's state — so without this
// check a stale Completed response would flip the task's target segments to
// Flushed (queryable) and resurrect the task Failed -> Completed underneath a
// job that has already failed, exposing part of a restore that never succeeded.
//
// This is a check, not a reservation: the caller acts on the result outside
// m.mu, so an instantaneous overlap with a concurrent failure is still possible.
// CompleteTaskIfActive re-checks under the lock before the state write, and the
// inspector's job-scoped cleanup drops the target segments of a failed job on
// its next round, so the residual window converges on its own.
func (m *copySegmentMeta) TaskAcceptsWorkerResult(ctx context.Context, taskID int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.taskAcceptsResultLocked(taskID)
}

// CompleteTaskIfActive marks a task Completed only if it is still InProgress
// under an active job, with the check and the write under one lock.
//
// Returns (false, nil) when the transition was skipped because the result went
// stale while it was being applied — the task keeps whatever terminal state the
// winning path gave it, and is never resurrected out of it.
func (m *copySegmentMeta) CompleteTaskIfActive(ctx context.Context, taskID int64, completeTs uint64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.taskAcceptsResultLocked(taskID) {
		return false, nil
	}
	if err := m.updateTaskLocked(ctx, m.tasks.get(taskID),
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskCompleted),
		UpdateCopyTaskCompleteTs(completeTs)); err != nil {
		return false, err
	}
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
