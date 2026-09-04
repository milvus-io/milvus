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
	AddJobWithSegments(ctx context.Context, job CopySegmentJob, segments []*SegmentInfo) error
	UpdateJob(ctx context.Context, jobID int64, actions ...UpdateCopySegmentJobAction) error
	UpdateJobInState(ctx context.Context, jobID int64, expectedState datapb.CopySegmentJobState, actions ...UpdateCopySegmentJobAction) (bool, error)
	UpdateJobStateAndReleasePin(ctx context.Context, jobID int64, expectedState datapb.CopySegmentJobState, actions ...UpdateCopySegmentJobAction) (bool, error)
	ReleaseJobPin(ctx context.Context, jobID int64) error
	CompleteJob(ctx context.Context, jobID int64, targetSegmentIDs []int64, actions ...UpdateCopySegmentJobAction) (bool, error)
	GetJob(ctx context.Context, jobID int64) CopySegmentJob
	GetJobBy(ctx context.Context, filters ...CopySegmentJobFilter) []CopySegmentJob
	CountJobBy(ctx context.Context, filters ...CopySegmentJobFilter) int
	RemoveJob(ctx context.Context, jobID int64) error

	// Task operations
	AddTask(ctx context.Context, task CopySegmentTask) error
	ReplaceRetryTask(ctx context.Context, oldTaskID int64, replacement CopySegmentTask) (bool, error)
	UpdateTask(ctx context.Context, taskID int64, actions ...UpdateCopySegmentTaskAction) error
	UpdateTaskInState(ctx context.Context, taskID int64, expectedState datapb.CopySegmentTaskState, actions ...UpdateCopySegmentTaskAction) (bool, error)
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
	// PinId=0 (proto default). The terminal-transition guard at UpdateJobStateAndReleasePin
	// skips Unpin for such jobs (no pin existed). DropSnapshot protection for these
	// in-flight legacy jobs is NOT retroactively established — plan upgrades during
	// quiet periods, or drain active restores before switching binaries.
	//
	// Rollback caveat: if post-pin-refactor data is read by a pre-refactor binary, the
	// old code ignores CopySegmentJob.PinId and uses its in-memory ref counter. Pins
	// persisted on SnapshotInfo remain but are never unpinned. Do not roll back while
	// active restore jobs created by this binary still exist.

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

// AddJobWithSegments publishes a restore job and every target segment it owns
// in one catalog update. Memory is updated only after the catalog succeeds, so
// a retry can never observe ownerless Importing segments in this process.
func (m *copySegmentMeta) AddJobWithSegments(ctx context.Context, job CopySegmentJob, segments []*SegmentInfo) error {
	if m.meta == nil {
		return merr.WrapErrServiceInternalMsg("cannot add copy segment job with segments without segment meta")
	}
	copyJob, ok := job.(*copySegmentJob)
	if !ok {
		return merr.WrapErrServiceInternalMsg("unsupported copy segment job implementation %T", job)
	}

	m.meta.segMu.Lock()
	defer m.meta.segMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()

	actions := make([]metastore.UpdateAction, 0, len(segments)+1)
	metricMutation := &segMetricMutation{stateChange: make(segmentMetricStateChange)}
	for _, segment := range segments {
		if segment == nil {
			return merr.WrapErrServiceInternalMsg("copy segment job %d has a nil target segment", job.GetJobId())
		}
		if existing := m.meta.segments.GetSegment(segment.GetID()); existing != nil {
			return merr.WrapErrServiceInternalMsg(
				"copy segment job %d target segment %d already exists", job.GetJobId(), segment.GetID())
		}
		actions = append(actions, metastore.AddSegment(segment.SegmentInfo))
		metricMutation.addNewSeg(segment.GetState(), segment.GetLevel(), segment.GetIsSorted(),
			segment.GetStorageVersion(), segmentMetricFormatLabel(segment), segment.GetNumOfRows())
	}
	actions = append(actions, metastore.SaveCopySegmentJob(copyJob.CopySegmentJob))
	if err := m.catalog.Update(ctx, actions...); err != nil {
		componentCtx := m.ctx
		if componentCtx == nil {
			componentCtx = ctx
		}
		if componentCtx != nil && componentCtx.Err() == nil {
			mlog.Fatal(componentCtx, "copy segment job publication failed; terminating process", mlog.Err(err))
		}
		return err
	}

	metricMutation.commit()
	for _, segment := range segments {
		m.meta.segments.SetSegment(segment.GetID(), segment)
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
		return job, updatedJob, err
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

// UpdateJobStateAndReleasePin applies the actions only while the job is still in
// expectedState. If that update is the first transition to Completed or Failed,
// it also releases the source snapshot pin owned by the job.
//
// This ensures snapshot pins are released immediately when restore jobs finish,
// while Job records are retained for audit purposes (3 hours).
//
// Locking strategy: the state-mutate section takes m.mu; the Unpin call (an etcd
// roundtrip via snapshotMeta.SaveSnapshot) runs AFTER releasing m.mu to avoid
// blocking all copy-segment job operations on an external write. Double-unpin is
// prevented because only one caller can match expectedState and observe the
// `!wasTerminal → isTerminal` transition under m.mu. Returns false without an
// error when the job is missing or has already moved to another state.
func (m *copySegmentMeta) UpdateJobStateAndReleasePin(ctx context.Context, jobID int64, expectedState datapb.CopySegmentJobState, actions ...UpdateCopySegmentJobAction) (bool, error) {
	m.mu.Lock()
	job, ok := m.jobs[jobID]
	if !ok || job.GetState() != expectedState {
		m.mu.Unlock()
		return false, nil
	}
	prevJob, updatedJob, err := m.updateJob(ctx, jobID, actions...)
	if err != nil {
		newState := updatedJob.GetState()
		isTerminal := newState == datapb.CopySegmentJobState_CopySegmentJobCompleted ||
			newState == datapb.CopySegmentJobState_CopySegmentJobFailed
		componentCtx := m.ctx
		if componentCtx == nil {
			componentCtx = ctx
		}
		if isTerminal && componentCtx != nil && componentCtx.Err() == nil {
			// The terminal write may already be durable even when its response is
			// lost. Stop while the job meta lock is still held so a waiting timeout
			// or failure path cannot overwrite that authoritative terminal state.
			mlog.Fatal(componentCtx, "copy segment terminal job publication failed; terminating process",
				mlog.FieldJobID(jobID),
				mlog.String("state", newState.String()),
				mlog.Err(err))
			// Fatal does not return in production. Keep the error out of callers'
			// ordinary failure paths when Fatal is replaced by a test hook.
			m.mu.Unlock()
			return false, nil
		}
		m.mu.Unlock()
		return false, err
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
	m.mu.Unlock()

	if !shouldUnpin {
		return true, nil
	}

	if err := m.ReleaseJobPin(ctx, jobID); err != nil {
		// The terminal state is already durable. Keep PinId on the job so the
		// checker retries the release and never garbage-collects its owner first.
		mlog.Warn(ctx, "failed to release source snapshot pin on job terminal transition; will retry",
			mlog.FieldJobID(jobID),
			mlog.String("previousState", previousState.String()),
			mlog.String("newState", newState.String()),
			mlog.Err(err))
	}
	return true, nil
}

// ReleaseJobPin releases a terminal job's source snapshot pin and then clears
// PinId durably. Keeping PinId until both operations succeed makes the release
// recoverable across transient catalog failures and DataCoord restarts.
// UnpinSnapshot is idempotent, so a crash between unpin and clearing PinId is
// safe: the next checker round repeats the unpin and finishes the metadata
// update.
func (m *copySegmentMeta) ReleaseJobPin(ctx context.Context, jobID int64) error {
	m.mu.RLock()
	job, ok := m.jobs[jobID]
	if !ok || job.GetPinId() == 0 {
		m.mu.RUnlock()
		return nil
	}
	if job.GetState() != datapb.CopySegmentJobState_CopySegmentJobCompleted &&
		job.GetState() != datapb.CopySegmentJobState_CopySegmentJobFailed {
		m.mu.RUnlock()
		return nil
	}
	pinID := job.GetPinId()
	sourceCollectionID := job.GetSourceCollectionId()
	snapshotName := job.GetSnapshotName()
	m.mu.RUnlock()

	unpinCollID, unpinName, remaining, err := m.snapshotMeta.UnpinSnapshot(ctx, pinID)
	if err != nil {
		return err
	}
	if unpinName != "" {
		setSnapshotActivePinsGauge(unpinCollID, unpinName, remaining)
	}

	m.mu.Lock()
	current, ok := m.jobs[jobID]
	if !ok || current.GetPinId() != pinID {
		m.mu.Unlock()
		return nil
	}
	_, _, err = m.updateJob(ctx, jobID, UpdateCopyJobPinID(0))
	m.mu.Unlock()
	if err != nil {
		return err
	}

	mlog.Info(ctx, "released source snapshot pin owned by terminal copy job",
		mlog.FieldJobID(jobID),
		mlog.Int64("pinID", pinID),
		mlog.Int64("sourceCollectionID", sourceCollectionID),
		mlog.String("snapshot", snapshotName))
	return nil
}

// CompleteJob publishes every copied target and the Completed job in one
// catalog update. Until this succeeds, completed worker tasks have populated
// the target metadata but the segments remain Importing and are not queryable.
func (m *copySegmentMeta) CompleteJob(ctx context.Context, jobID int64, targetSegmentIDs []int64, actions ...UpdateCopySegmentJobAction) (bool, error) {
	m.meta.segMu.Lock()
	m.mu.Lock()

	job, ok := m.jobs[jobID]
	if !ok || job.GetState() != datapb.CopySegmentJobState_CopySegmentJobExecuting {
		m.mu.Unlock()
		m.meta.segMu.Unlock()
		return false, nil
	}

	updatedJob := job.Clone()
	for _, action := range actions {
		action(updatedJob)
	}

	metricMutation := &segMetricMutation{stateChange: make(segmentMetricStateChange)}
	segments := make([]*SegmentInfo, 0, len(targetSegmentIDs))
	seen := make(map[int64]struct{}, len(targetSegmentIDs))
	for _, segmentID := range targetSegmentIDs {
		if _, ok := seen[segmentID]; ok {
			continue
		}
		seen[segmentID] = struct{}{}
		current := m.meta.segments.GetSegment(segmentID)
		if current == nil {
			m.mu.Unlock()
			m.meta.segMu.Unlock()
			return false, merr.WrapErrSegmentNotFound(segmentID, "complete copy segment job")
		}
		segment := current.Clone()
		if segment.GetState() != commonpb.SegmentState_Flushed {
			updateSegStateAndPrepareMetrics(segment, commonpb.SegmentState_Flushed, metricMutation)
		}
		segment.IsImporting = false
		segments = append(segments, segment)
	}

	catalogActions := make([]metastore.UpdateAction, 0, len(segments)+1)
	for _, segment := range segments {
		catalogActions = append(catalogActions, metastore.UpdateSegment(segment.SegmentInfo))
	}
	catalogActions = append(catalogActions, metastore.SaveCopySegmentJob(updatedJob.(*copySegmentJob).CopySegmentJob))
	if err := m.catalog.Update(ctx, catalogActions...); err != nil {
		// The catalog transaction may already have published both the targets and
		// the Completed job even when its response is lost. Continuing with the
		// stale in-memory Executing state would let the timeout path overwrite that
		// durable completion and retire the published targets. Fail while still
		// holding the existing publication locks so a waiting timeout path cannot
		// write Failed before the process exits; restart then recovers the
		// authoritative catalog state.
		if ctx != nil && ctx.Err() == nil && (m.ctx == nil || m.ctx.Err() == nil) {
			mlog.Fatal(ctx, "copy segment completion publication failed; terminating process",
				mlog.FieldJobID(jobID),
				mlog.Err(err))
		}
		m.mu.Unlock()
		m.meta.segMu.Unlock()
		return false, err
	}

	metricMutation.commit()
	for _, segment := range segments {
		m.meta.segments.SetSegment(segment.GetID(), segment)
	}
	updatedJob.(*copySegmentJob).snapshotCache = nil
	m.jobs[jobID] = updatedJob

	m.mu.Unlock()
	m.meta.segMu.Unlock()

	if err := m.ReleaseJobPin(ctx, jobID); err != nil {
		mlog.Warn(ctx, "failed to release source snapshot pin on copy job completion; will retry",
			mlog.FieldJobID(jobID), mlog.Err(err))
	}
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
		// The write may have reached catalog even when its response was lost.
		// Continuing in this process would let the Pending job create another
		// task for the same mappings. Restart and rebuild the authoritative task
		// set from catalog instead. Context cancellation during shutdown is not
		// an ambiguous live write and should return normally.
		if ctx.Err() == nil && m.ctx.Err() == nil {
			mlog.Fatal(ctx, "copy segment task publication failed; terminating process", mlog.Err(err))
		}
		return err
	}
	m.tasks.add(task)
	return nil
}

// ReplaceRetryTask removes the old task and publishes its replacement and
// target segments in one catalog update. The old execution may remain on its
// DataNode, but its task ID no longer exists in coordinator metadata.
func (m *copySegmentMeta) ReplaceRetryTask(ctx context.Context, oldTaskID int64, task CopySegmentTask) (bool, error) {
	if m.meta == nil {
		return false, merr.WrapErrServiceInternalMsg("cannot replace copy segment task without segment meta")
	}
	incoming, ok := task.(*copySegmentTask)
	if !ok {
		return false, merr.WrapErrServiceInternalMsg("unsupported copy segment task implementation %T", task)
	}

	m.meta.segMu.Lock()
	defer m.meta.segMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()

	oldTask := m.tasks.get(oldTaskID)
	if oldTask == nil {
		return false, nil
	}
	if oldTask.GetState() != datapb.CopySegmentTaskState_CopySegmentTaskRetry {
		return false, nil
	}
	job, ok := m.jobs[oldTask.GetJobId()]
	if !ok || job.GetState() != datapb.CopySegmentJobState_CopySegmentJobExecuting {
		return false, nil
	}
	oldMappings := oldTask.GetIdMappings()
	newMappings := incoming.GetIdMappings()
	if len(oldMappings) != len(newMappings) {
		return false, merr.WrapErrServiceInternalMsg(
			"copy segment replacement %d mapping count does not match retry task %d",
			incoming.GetTaskId(), oldTaskID)
	}

	metricMutation := &segMetricMutation{stateChange: make(segmentMetricStateChange)}
	droppedTargets := make([]*SegmentInfo, 0, len(oldMappings))
	newTargets := make([]*SegmentInfo, 0, len(oldMappings))
	updatedJob := job.Clone()
	for i, oldMapping := range oldMappings {
		newMapping := newMappings[i]
		if oldMapping == nil || newMapping == nil {
			return false, merr.WrapErrServiceInternalMsg("copy segment task %d has a nil mapping", oldTaskID)
		}
		oldTargetID := oldMapping.GetTargetSegmentId()
		oldTarget := m.meta.segments.GetSegment(oldTargetID)
		if oldTarget == nil {
			return false, merr.WrapErrSegmentNotFound(oldTargetID, "copy segment retry target is gone")
		}
		dropped := oldTarget.Clone()
		updateSegStateAndPrepareMetrics(dropped, commonpb.SegmentState_Dropped, metricMutation)
		dropped.IsImporting = false
		droppedTargets = append(droppedTargets, dropped)

		newTargetID := newMapping.GetTargetSegmentId()
		fresh := copySegmentReplanTarget(oldTarget, newTargetID)
		newTargets = append(newTargets, fresh)
		metricMutation.addNewSeg(fresh.GetState(), fresh.GetLevel(), fresh.GetIsSorted(),
			fresh.GetStorageVersion(), segmentMetricFormatLabel(fresh), fresh.GetNumOfRows())

		for _, jobMapping := range updatedJob.GetIdMappings() {
			if jobMapping != nil && jobMapping.GetTargetSegmentId() == oldTargetID {
				jobMapping.TargetSegmentId = newTargetID
				break
			}
		}
	}
	actions := make([]metastore.UpdateAction, 0, 3+len(droppedTargets)+len(newTargets))
	actions = append(actions,
		metastore.DropCopySegmentTask(oldTaskID),
		metastore.AddCopySegmentTask(incoming.task.Load()))
	for _, target := range droppedTargets {
		actions = append(actions, metastore.AlterSegment(target.SegmentInfo))
	}
	for _, target := range newTargets {
		actions = append(actions, metastore.AddSegment(target.SegmentInfo))
	}
	actions = append(actions, metastore.SaveCopySegmentJob(updatedJob.(*copySegmentJob).CopySegmentJob))
	if err := m.catalog.Update(ctx, actions...); err != nil {
		if ctx.Err() == nil && m.ctx.Err() == nil {
			mlog.Fatal(ctx, "copy segment retry task replacement failed; terminating process", mlog.Err(err))
		}
		return false, err
	}

	metricMutation.commit()
	for _, target := range droppedTargets {
		m.meta.segments.SetSegment(target.GetID(), target)
	}
	for _, target := range newTargets {
		m.meta.segments.SetSegment(target.GetID(), target)
	}
	m.tasks.remove(oldTaskID)
	oldTaskState := oldTask.Clone().(*copySegmentTask)
	oldTaskState.task.Load().State = datapb.CopySegmentTaskState_CopySegmentTaskFailed
	oldTask.(*copySegmentTask).task.Store(oldTaskState.task.Load())
	incoming.ctx = m.ctx
	incoming.copyMeta = m
	incoming.meta = m.meta
	incoming.snapshotMeta = m.snapshotMeta
	incoming.alloc = m.alloc
	m.tasks.add(incoming)
	m.jobs[job.GetJobId()] = updatedJob
	mlog.Info(ctx, "replaced copy segment task",
		mlog.FieldJobID(oldTask.GetJobId()),
		mlog.Int64("oldTaskID", oldTaskID),
		mlog.FieldTaskID(incoming.GetTaskId()),
		mlog.Int64("attempt", incoming.GetTaskVersion()))
	return true, nil
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
		if updatedTask.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskCompleted &&
			ctx != nil && ctx.Err() == nil && (m.ctx == nil || m.ctx.Err() == nil) {
			// The Completed write may already be durable even when its response is
			// lost. Returning this error to QueryTaskOnWorker would immediately run
			// markTaskAndJobFailed from stale InProgress memory and overwrite that
			// completion. Stop while the task meta lock is still held; restart reloads
			// the authoritative catalog state.
			mlog.Fatal(ctx, "copy segment task completion publication failed; terminating process",
				mlog.FieldJobID(updatedTask.GetJobId()),
				mlog.FieldTaskID(taskID),
				mlog.Err(err))
			// Fatal does not return in production. Keeping the error out of the
			// caller's ordinary failure path also makes that invariant testable when
			// Fatal is replaced by a test hook.
			return false, nil
		}
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
