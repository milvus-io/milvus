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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// nonRetriableJobError marks a refresh-job submission failure that must NOT
// be retried by the checker tick (e.g. empty source, zero-row source, bucket
// not found). ensureTasksForInitJob recognizes it and transitions the job
// straight to Failed instead of leaving it in Init for endless retry.
type nonRetriableJobError struct {
	reason string
}

func (e *nonRetriableJobError) Error() string { return e.reason }

func newNonRetriableJobError(format string, args ...interface{}) error {
	return &nonRetriableJobError{reason: fmt.Sprintf(format, args...)}
}

// exploreTempDirForJob returns the per-job explore temp directory path used
// both when datacoord writes the explore manifest and when cleanupExploreTempForJob
// reclaims it after the job reaches a terminal state. Keep the two call sites in
// sync by routing both through this helper.
func exploreTempDirForJob(jobID int64) string {
	return fmt.Sprintf("__explore_temp__/coord_%d", jobID)
}

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
	// If there's an existing active job for the same collection, it will be canceled
	// and replaced by the new job (the old job will show "superseded by new job" as fail reason).
	// This method is called from the WAL callback to ensure distributed consistency.
	SubmitRefreshJobWithID(ctx context.Context, jobID int64, collectionID int64, collectionName string, externalSource, externalSpec string) (int64, error)

	// GetJobProgress returns the job info for the given job_id
	GetJobProgress(ctx context.Context, jobID int64) (*datapb.ExternalCollectionRefreshJob, error)

	// ListJobs returns jobs for the given collection, sorted by start_time descending
	ListJobs(ctx context.Context, collectionID int64) ([]*datapb.ExternalCollectionRefreshJob, error)

	// GetActiveJobByCollectionID returns the in-progress (Init/InProgress/Retry)
	// refresh job for the collection if one exists, or nil. Used by the RPC
	// handler to surface duplicate refresh requests synchronously instead of
	// allocating a fresh jobID that the WAL ack callback will silently drop.
	GetActiveJobByCollectionID(collectionID int64) *datapb.ExternalCollectionRefreshJob
}

var _ ExternalCollectionRefreshManager = (*externalCollectionRefreshManager)(nil)

type externalCollectionRefreshManager struct {
	ctx       context.Context
	mt        *meta
	scheduler task.GlobalScheduler
	allocator allocator.Allocator
	cluster   session.Cluster

	// collectionGetter retrieves collection metadata, with lazy-loading from RootCoord
	// on cache miss. This handles the race condition where a refresh is triggered
	// before the collection metadata has been synced to DataCoord.
	collectionGetter func(ctx context.Context, collectionID int64) (*collectionInfo, error)

	// schemaUpdater broadcasts schema changes to RootCoord via WAL after refresh
	// completes with updated external_source or external_spec.
	schemaUpdater func(ctx context.Context, collectionID int64, externalSource, externalSpec string) error

	// Unified refresh meta for Job and Task management
	refreshMeta *externalCollectionRefreshMeta

	// chunkManager is used to clean up the per-job explore temp directory on
	// shared storage after the job reaches a terminal state. Both the FFI
	// explore path and ChunkManager use the same storage config (bucket +
	// rootPath), so a RemoveWithPrefix on the explore base dir reaches the
	// same physical location the FFI wrote to.
	chunkManager storage.ChunkManager

	// Internal components (private, composed)
	inspector *externalCollectionRefreshInspector
	checker   *externalCollectionRefreshChecker

	// Lifecycle management
	closeOnce sync.Once
	closeChan chan struct{}
	wg        sync.WaitGroup

	// notifiedJobs tracks jobs whose schema-update callback has already been
	// delivered. It guards against concurrent invocations of handleJobFinished
	// from the eager task path and the periodic checker tick — both paths read
	// collection.Schema before calling schemaUpdater, so without this dedup
	// they could both observe a stale snapshot (before the WAL broadcast
	// propagates back into the DataCoord cache) and both broadcast.
	// forgetJob on GC prevents unbounded growth.
	notifiedMu   sync.Mutex
	notifiedJobs map[int64]struct{}

	// initJobsInFlight tracks jobs whose async task-creation (Phase B) is
	// currently running. SubmitRefreshJobWithID persists the job record in
	// Init state on the WAL ack callback path and returns immediately; the
	// S3 explore + task split + scheduler enqueue run in a background
	// goroutine so the broadcaster is never blocked on object-store I/O.
	// Both the eager Submit path and the periodic checker tick drive the
	// same entry point (ensureTasksForInitJob) and this map dedups them so
	// at most one explore is in flight per jobID at any moment.
	initMu           sync.Mutex
	initJobsInFlight map[int64]struct{}
}

// NewExternalCollectionRefreshManager creates a new external table refresh manager.
// collectionGetter retrieves collection info with lazy-loading from RootCoord on cache miss.
func NewExternalCollectionRefreshManager(
	ctx context.Context,
	mt *meta,
	scheduler task.GlobalScheduler,
	allocator allocator.Allocator,
	refreshMeta *externalCollectionRefreshMeta,
	cluster session.Cluster,
	collectionGetter func(ctx context.Context, collectionID int64) (*collectionInfo, error),
	schemaUpdater func(ctx context.Context, collectionID int64, externalSource, externalSpec string) error,
	chunkManager storage.ChunkManager,
) ExternalCollectionRefreshManager {
	closeChan := make(chan struct{})

	m := &externalCollectionRefreshManager{
		ctx:              ctx,
		mt:               mt,
		scheduler:        scheduler,
		allocator:        allocator,
		cluster:          cluster,
		refreshMeta:      refreshMeta,
		collectionGetter: collectionGetter,
		schemaUpdater:    schemaUpdater,
		chunkManager:     chunkManager,
		closeChan:        closeChan,
		notifiedJobs:     make(map[int64]struct{}),
		initJobsInFlight: make(map[int64]struct{}),
	}

	// Create internal components with shared refreshMeta. The checker owns
	// the per-job processing function that drives state aggregation, finish
	// notification, timeout, and GC. Tasks wired by the inspector call the
	// checker's per-job entry point synchronously when they reach a terminal
	// state, so the schema-update callback fires before the task method
	// returns. The checker still runs the same per-job function periodically
	// as a safety net for missed events (e.g., after a DataCoord restart).
	// forgetJob cleans up the notifiedJobs dedup map when the checker GC's
	// a job, preventing unbounded growth.
	m.inspector = newRefreshInspector(ctx, refreshMeta, mt, scheduler, allocator, closeChan)
	m.checker = newRefreshChecker(ctx, refreshMeta, closeChan, m.handleJobFinished, m.applyFinishedJobSegments, m.handleJobFailed, m.forgetJob, m.ensureTasksForInitJob)
	m.inspector.wrapTask = m.wrapTask

	return m
}

// forgetJob removes a jobID from the notifiedJobs dedup map. Called by the
// checker after successfully dropping a GC'd job, so the map does not grow
// unboundedly across DataCoord lifetime.
//
// Also serves as a fallback cleanup path for Failed/Timeout jobs whose temp
// dir was never reclaimed by the eager Finished path. Finished jobs already
// had cleanup fired inside handleJobFinished (presence in notifiedJobs is
// the signal), so forgetJob skips the redundant second cleanup for them.
func (m *externalCollectionRefreshManager) forgetJob(jobID int64) {
	m.notifiedMu.Lock()
	_, alreadyCleaned := m.notifiedJobs[jobID]
	delete(m.notifiedJobs, jobID)
	m.notifiedMu.Unlock()

	if alreadyCleaned {
		// Terminal handler (handleJobFinished or handleJobFailed) already
		// reclaimed this job's explore temp dir; skip the redundant pass.
		return
	}
	m.cleanupExploreTempForJob(jobID)
}

// handleJobFailed reclaims per-job resources when the checker transitions
// a job into Failed state (via aggregateJobState or tryTimeoutJob). It is
// the Failed-path symmetric companion to handleJobFinished: both paths add
// the jobID to notifiedJobs so forgetJob knows cleanup already ran, and
// both paths fire cleanupExploreTempForJob exactly once per jobID.
//
// Unlike handleJobFinished, this path does NOT touch schemaUpdater — a
// failed refresh leaves the collection schema unchanged by design.
func (m *externalCollectionRefreshManager) handleJobFailed(jobID int64) {
	m.notifiedMu.Lock()
	if _, already := m.notifiedJobs[jobID]; already {
		m.notifiedMu.Unlock()
		return
	}
	m.notifiedJobs[jobID] = struct{}{}
	m.notifiedMu.Unlock()

	m.cleanupExploreTempForJob(jobID)
}

// cleanupExploreTempForJob removes the per-job explore temp directory on
// shared storage. The directory layout is `__explore_temp__/coord_{jobID}`,
// matching the path the datacoord wrote via the loon FFI in fetchFiles.
//
// Both passes are required because LocalChunkManager and RemoteChunkManager
// have different removal semantics:
//   - RemoveWithPrefix walks every object under the prefix and deletes each
//     one. On MinIO/S3 this also catches the 0-byte placeholder objects (with
//     trailing `/`) that surfaced as the orphaned `_metadata/` entries in
//     issue #48626. On local FS it deletes the regular files but leaves the
//     parent directory entry behind.
//   - Remove on the prefix itself finishes the job: LocalChunkManager.Remove
//     calls os.RemoveAll which recursively drops the directory; the remote
//     manager treats the call as an idempotent DeleteObject on a key that
//     does not exist, returning success.
//
// The function is safe to call multiple times for the same jobID; both passes
// are idempotent and a missing prefix is not an error.
func (m *externalCollectionRefreshManager) cleanupExploreTempForJob(jobID int64) {
	if m.chunkManager == nil {
		return
	}
	exploreBaseDir := exploreTempDirForJob(jobID)
	// Derive from m.ctx so shutdown cancels in-flight cleanup instead of
	// blocking Stop() on a slow object-store call.
	ctx, cancel := context.WithTimeout(m.ctx, 30*time.Second)
	defer cancel()

	if err := m.chunkManager.RemoveWithPrefix(ctx, exploreBaseDir); err != nil {
		log.Warn("failed to remove explore temp prefix",
			zap.Int64("jobID", jobID),
			zap.String("dir", exploreBaseDir),
			zap.Error(err))
	}
	if err := m.chunkManager.Remove(ctx, exploreBaseDir); err != nil {
		log.Warn("failed to remove explore temp root",
			zap.Int64("jobID", jobID),
			zap.String("dir", exploreBaseDir),
			zap.Error(err))
	}
}

func (m *externalCollectionRefreshManager) applyFinishedJobSegments(ctx context.Context, job *datapb.ExternalCollectionRefreshJob) error {
	tasks := m.refreshMeta.GetTasksByJobID(job.GetJobId())
	if len(tasks) == 0 {
		return fmt.Errorf("job %d has no tasks to apply", job.GetJobId())
	}

	keptSet := make(map[int64]struct{})
	updatedSet := make(map[int64]struct{})
	keptSegments := make([]int64, 0)
	updatedSegments := make([]*datapb.SegmentInfo, 0)
	for _, task := range tasks {
		if task.GetState() != indexpb.JobState_JobStateFinished {
			return fmt.Errorf("job %d has non-finished task %d in state %s",
				job.GetJobId(), task.GetTaskId(), task.GetState().String())
		}
		for _, segmentID := range task.GetKeptSegments() {
			if _, ok := keptSet[segmentID]; ok {
				continue
			}
			keptSet[segmentID] = struct{}{}
			keptSegments = append(keptSegments, segmentID)
		}
		for _, segment := range task.GetUpdatedSegments() {
			if segment == nil {
				continue
			}
			if _, ok := updatedSet[segment.GetID()]; ok {
				return fmt.Errorf("job %d has duplicate updated segment %d from task %d",
					job.GetJobId(), segment.GetID(), task.GetTaskId())
			}
			updatedSet[segment.GetID()] = struct{}{}
			updatedSegments = append(updatedSegments, segment)
		}
	}

	return applyExternalCollectionSegmentUpdate(
		ctx,
		m.mt,
		job.GetCollectionId(),
		keptSegments,
		updatedSegments,
		zap.Int64("jobID", job.GetJobId()),
	)
}

// wrapTask builds a scheduler-facing task wrapper around a persisted proto
// task, wiring the processFinishedJob callback so terminal transitions drive
// per-job processing synchronously. Single source of truth for task wiring;
// used by both createTasksForJob (initial submission) and the inspector
// (reload/re-enqueue paths).
func (m *externalCollectionRefreshManager) wrapTask(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
	taskWrapper := newRefreshExternalCollectionTask(t, m.refreshMeta, m.mt, m.allocator)
	taskWrapper.processFinishedJob = m.checker.processJobByID
	return taskWrapper
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

// handleJobFinished is invoked when a refresh job transitions to Finished.
// It is called both eagerly (synchronously from the task path via
// processJobByID) and from the periodic checker tick. The notifiedJobs
// dedup map below guarantees exactly-once schemaUpdater invocation per
// jobID: concurrent calls from the two paths race on the mutex, the loser
// sees the jobID already present and short-circuits. The source/spec
// equality check is a cheap secondary guard (e.g., for jobs that finished
// with the same schema as the current collection).
func (m *externalCollectionRefreshManager) handleJobFinished(ctx context.Context, job *datapb.ExternalCollectionRefreshJob) {
	if m.schemaUpdater == nil {
		return
	}

	// Exactly-once dedup across concurrent eager + periodic paths.
	m.notifiedMu.Lock()
	if _, already := m.notifiedJobs[job.GetJobId()]; already {
		m.notifiedMu.Unlock()
		return
	}
	m.notifiedJobs[job.GetJobId()] = struct{}{}
	mapSize := len(m.notifiedJobs)
	m.notifiedMu.Unlock()
	if mapSize > 1000 {
		log.Warn("notifiedJobs dedup map is large, GC may be lagging",
			zap.Int("size", mapSize))
	}

	// Reclaim the per-job explore temp dir now that all datanode tasks have
	// finished consuming the manifest. The Failed/Timeout path is covered
	// later by forgetJob when the checker GCs the job.
	defer m.cleanupExploreTempForJob(job.GetJobId())

	// Get current collection info
	collection, err := m.collectionGetter(ctx, job.GetCollectionId())
	if err != nil || collection == nil {
		log.Warn("failed to get collection for schema update after refresh",
			zap.Int64("jobID", job.GetJobId()),
			zap.Int64("collectionID", job.GetCollectionId()),
			zap.Error(err))
		return
	}

	// Check if external_source or external_spec changed
	currentSource := collection.Schema.GetExternalSource()
	currentSpec := collection.Schema.GetExternalSpec()
	newSource := job.GetExternalSource()
	newSpec := job.GetExternalSpec()

	if currentSource == newSource && currentSpec == newSpec {
		return // No change, skip
	}

	log.Info("updating collection schema after refresh",
		zap.Int64("jobID", job.GetJobId()),
		zap.Int64("collectionID", job.GetCollectionId()),
		zap.String("oldSource", currentSource),
		zap.String("newSource", newSource),
		zap.String("oldSpec", externalspec.RedactExternalSpec(currentSpec)),
		zap.String("newSpec", externalspec.RedactExternalSpec(newSpec)))

	if err := m.schemaUpdater(ctx, job.GetCollectionId(), newSource, newSpec); err != nil {
		log.Warn("failed to update external schema after refresh, schema may be stale until next refresh",
			zap.Int64("jobID", job.GetJobId()),
			zap.Int64("collectionID", job.GetCollectionId()),
			zap.Error(err))
	}
}

// ============================================================================
// Job APIs
// ============================================================================

// SubmitRefreshJobWithID creates a refresh job with a pre-allocated job ID (from WAL).
// This ensures idempotency - if the job already exists, it returns without error.
// Only one active refresh job is allowed per collection at a time. If there's already
// an active job, submission will fail with an error.
// This method is called from the WAL callback to ensure distributed consistency.
//
// Two-phase submission:
//
//  1. Phase A (synchronous, this method): validate collection, dedup against
//     active jobs, and persist the Job record in Init state. No S3 I/O, no
//     task creation. The caller (WAL ack callback) is unblocked the moment
//     the meta write returns.
//  2. Phase B (asynchronous, ensureTasksForInitJob): explore the external
//     source, split files into task chunks, persist tasks, and enqueue them.
//     Kicked off from this method via a background goroutine AND retried by
//     the checker tick if the first attempt fails. The `tryTimeoutJob` path
//     acts as the final safety net — a job that never advances past Init
//     eventually transitions to Failed("timeout") after
//     ExternalCollectionJobTimeout.
//
// Why two phases: the ack callback runs inside the broadcaster's per-broadcast
// processing loop (see ackCallbackScheduler.callMessageAckCallbackUntilDone).
// A slow or flaky S3 LIST on a bucket with thousands of files would block
// the broadcast task for seconds-to-minutes and trip the scheduler's infinite
// backoff retry, compounding WAL stalls. Moving the I/O off the ack path
// keeps the broadcaster responsive and isolates object-store latency to a
// bounded background retry.
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
		// Retry Phase B in case the prior submission failed to create tasks
		// and left the job stuck in Init. ensureTasksForInitJob dedups
		// concurrent invocations internally.
		m.ensureTasksForInitJob(jobID)
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

	// Phase A: persist the job record in Init state. No explore, no tasks.
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

	log.Info("external collection refresh job accepted (Init), task creation deferred to async phase",
		zap.String("externalSource", externalSource))

	// Phase B: kick off async task creation so this call returns immediately.
	// The checker tick drives the same path as a retry safety net, and
	// tryTimeoutJob is the terminal bound if task creation never succeeds.
	m.ensureTasksForInitJob(jobID)

	return jobID, nil
}

// ensureTasksForInitJob drives the asynchronous Phase B of job submission
// for a job that was created in Init state by Phase A. It is safe to call
// from multiple paths concurrently — the SubmitRefreshJobWithID eager path
// after AddJob, and the checker tick that re-triggers Init-stuck jobs.
// initJobsInFlight dedups concurrent invocations so at most one explore +
// task split runs per jobID at any moment.
//
// All work runs in a background goroutine tracked by the manager's wait
// group so Stop() waits for in-flight explores to finish (or the derived
// context to cancel). Errors are logged but not returned: the checker tick
// will retry on the next cycle, and tryTimeoutJob is the final safety net.
func (m *externalCollectionRefreshManager) ensureTasksForInitJob(jobID int64) {
	m.initMu.Lock()
	if _, running := m.initJobsInFlight[jobID]; running {
		m.initMu.Unlock()
		return
	}
	// Snapshot job state under the same lock so we can cheaply short-circuit
	// non-Init / already-has-tasks cases without spawning a goroutine.
	job := m.refreshMeta.GetJob(jobID)
	if job == nil ||
		job.GetState() != indexpb.JobState_JobStateInit ||
		len(job.GetTaskIds()) > 0 {
		m.initMu.Unlock()
		return
	}
	m.initJobsInFlight[jobID] = struct{}{}
	m.initMu.Unlock()

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer func() {
			m.initMu.Lock()
			delete(m.initJobsInFlight, jobID)
			m.initMu.Unlock()
		}()

		// Derive from m.ctx so Stop() can unblock a slow object-store call.
		// Bound to ExternalCollectionJobTimeout so a wedged explore cannot
		// hold goroutine resources indefinitely; the checker tick will
		// retry on the next cycle if this attempt returns early.
		timeout := Params.DataCoordCfg.ExternalCollectionJobTimeout.GetAsDuration(time.Second)
		ctx, cancel := context.WithTimeout(m.ctx, timeout)
		defer cancel()

		log := log.Ctx(ctx).With(zap.Int64("jobID", jobID))

		// Re-read under goroutine to catch race where state changed between
		// the cheap pre-check above and actual work start.
		freshJob := m.refreshMeta.GetJob(jobID)
		if freshJob == nil {
			log.Info("init job gone before async task creation ran")
			return
		}
		if freshJob.GetState() != indexpb.JobState_JobStateInit {
			log.Info("init job no longer in Init state, skip async task creation",
				zap.String("state", freshJob.GetState().String()))
			return
		}
		if len(freshJob.GetTaskIds()) > 0 {
			log.Info("init job already has tasks, skip async task creation",
				zap.Int("taskCount", len(freshJob.GetTaskIds())))
			return
		}

		tasks, err := m.createTasksForJob(ctx, freshJob)
		if err != nil {
			// Non-retriable failures (empty source, zero-row source, etc.)
			// must transition the job to Failed immediately. Otherwise the
			// checker tick keeps re-running the same explore that will fail
			// the same way forever, giving operators no signal to act on.
			var perm *nonRetriableJobError
			if errors.As(err, &perm) {
				log.Warn("non-retriable error in task creation, marking job failed",
					zap.Error(err))
				if _, uerr := m.refreshMeta.UpdateJobState(jobID,
					indexpb.JobState_JobStateFailed, perm.Error()); uerr != nil {
					log.Warn("failed to mark job failed", zap.Error(uerr))
				}
				return
			}
			// Transient failures (e.g. S3 blip) — leave in Init so the
			// checker tick / WAL redelivery path retries. tryTimeoutJob
			// bounds how long a stuck job can linger.
			log.Warn("async task creation failed, will retry on next checker tick",
				zap.Error(err))
			return
		}

		// Enqueue all created tasks for scheduling.
		for _, t := range tasks {
			m.scheduler.Enqueue(t)
		}
		log.Info("async task creation completed",
			zap.Int("taskCount", len(tasks)))
	}()
}

// createTasksForJob creates task(s) for a job and persists them to meta.
// Returns the created tasks for subsequent scheduling.
//
// Task count is ceil(totalFiles / ExternalCollectionFilesPerTask), driven by
// the config — it is independent of the current DataNode count. Each task
// carries the shared manifest path plus a [FileIndexBegin, FileIndexEnd)
// slice; DataNodes then read the manifest from object storage once and
// process only their assigned range, so the FFI explore runs exactly once
// on DataCoord.
func (m *externalCollectionRefreshManager) createTasksForJob(
	ctx context.Context,
	job *datapb.ExternalCollectionRefreshJob,
) ([]*refreshExternalCollectionTask, error) {
	log := log.Ctx(ctx).With(zap.Int64("jobID", job.GetJobId()), zap.Int64("collectionID", job.GetCollectionId()))

	// ExploreFiles once on DataCoord to get the full file list and manifest path.
	// Manifest is written to S3 so DataNodes can read file info by range.
	allFiles, manifestPath, err := m.exploreExternalFiles(ctx, job)
	if err != nil {
		// Any FFI failure during explore is terminal for this job: the
		// source is unreachable, denied, malformed, or absent and no
		// amount of in-loop retrying can change that. Surface as
		// non-retriable so the user gets a clear RefreshFailed signal
		// (with the underlying error attached) and can re-issue refresh
		// after fixing the source. Pure in-process errors (ctx cancel,
		// etcd unavailable, etc.) keep the existing transient path so
		// a real outage still gets retried.
		if errors.Is(err, packed.ErrLoonTransient) {
			return nil, newNonRetriableJobError("explore external files failed: %v", err)
		}
		return nil, fmt.Errorf("failed to explore external files: %w", err)
	}
	if len(allFiles) == 0 {
		return nil, newNonRetriableJobError("no files found in external source: %s", job.GetExternalSource())
	}
	// NOTE: zero-total-rows cannot be detected here. PlainFormat::explore
	// hardcodes start_index/end_index to -1 as sentinels and never reads
	// parquet metadata, so FileInfo.NumRows carries -1, not a real row count.
	// The real guard lives at datanode's balanceFragmentsToSegments, where
	// fragment RowCount is populated from manifest (endRow - startRow).
	log.Info("explored external files for task splitting",
		zap.Int("totalFiles", len(allFiles)),
		zap.String("manifestPath", manifestPath))

	// Determine task count: ceil(totalFiles/filesPerTask).
	// - filesPerTask: configurable via dataCoord.externalCollectionFilesPerTask
	// In standalone mode, multiple tasks run concurrently on the single DN's worker pool.
	minFilesPerTask := int(paramtable.Get().DataCoordCfg.ExternalCollectionFilesPerTask.GetAsInt64())

	type taskChunk struct {
		fileIndexBegin int64
		fileIndexEnd   int64
	}
	var chunks []taskChunk
	numTasks := (len(allFiles) + minFilesPerTask - 1) / minFilesPerTask
	if numTasks < 1 {
		numTasks = 1
	}
	filesPerTask := (len(allFiles) + numTasks - 1) / numTasks // ceil division
	for i := 0; i < len(allFiles); i += filesPerTask {
		end := i + filesPerTask
		if end > len(allFiles) {
			end = len(allFiles)
		}
		chunks = append(chunks, taskChunk{
			fileIndexBegin: int64(i),
			fileIndexEnd:   int64(end),
		})
	}

	log.Info("splitting refresh job into tasks",
		zap.Int("totalFiles", len(allFiles)),
		zap.Int("numTasks", len(chunks)))

	var tasks []*refreshExternalCollectionTask
	for _, chunk := range chunks {
		taskID, err := m.allocator.AllocID(ctx)
		if err != nil {
			log.Warn("failed to allocate task ID", zap.Error(err))
			return nil, err
		}

		task := &datapb.ExternalCollectionRefreshTask{
			TaskId:              taskID,
			JobId:               job.GetJobId(),
			CollectionId:        job.GetCollectionId(),
			Version:             0,
			NodeId:              0,
			State:               indexpb.JobState_JobStateInit,
			ExternalSource:      job.GetExternalSource(),
			ExternalSpec:        job.GetExternalSpec(),
			Progress:            0,
			ExploreManifestPath: manifestPath,
			FileIndexBegin:      chunk.fileIndexBegin,
			FileIndexEnd:        chunk.fileIndexEnd,
		}

		if err = m.refreshMeta.AddTask(task); err != nil {
			log.Warn("failed to add task to meta", zap.Error(err))
			return nil, err
		}

		if err = m.refreshMeta.AddTaskIDToJob(job.GetJobId(), taskID); err != nil {
			log.Warn("failed to add taskID to job", zap.Error(err))
			return nil, err
		}

		taskWrapper := m.wrapTask(task)
		tasks = append(tasks, taskWrapper)
	}

	log.Info("tasks created for job",
		zap.Int("numTasks", len(tasks)),
		zap.Int64("jobID", job.GetJobId()))

	return tasks, nil
}

func normalizeRefreshJobProgress(job *datapb.ExternalCollectionRefreshJob, state indexpb.JobState, progress int64) {
	if state == indexpb.JobState_JobStateNone {
		return
	}

	switch job.GetState() {
	case indexpb.JobState_JobStateFinished, indexpb.JobState_JobStateFailed:
		return
	}

	if state == indexpb.JobState_JobStateFinished {
		job.State = indexpb.JobState_JobStateInProgress
		if progress > 99 {
			progress = 99
		}
		job.Progress = progress
		return
	}

	job.State = state
	job.Progress = progress
}

// GetJobProgress returns the job info for the given job_id
func (m *externalCollectionRefreshManager) GetJobProgress(ctx context.Context, jobID int64) (*datapb.ExternalCollectionRefreshJob, error) {
	job := m.refreshMeta.GetJob(jobID)
	if job == nil {
		return nil, fmt.Errorf("job %d not found", jobID)
	}

	// Aggregate state and progress from tasks
	state, progress := m.refreshMeta.AggregateJobStateFromTasks(jobID)
	normalizeRefreshJobProgress(job, state, progress)
	return job, nil
}

// ListJobs returns jobs for the given collection, sorted by start_time descending.
// A zero collectionID lists jobs for all external collections.
func (m *externalCollectionRefreshManager) ListJobs(ctx context.Context, collectionID int64) ([]*datapb.ExternalCollectionRefreshJob, error) {
	var jobs []*datapb.ExternalCollectionRefreshJob
	if collectionID == 0 {
		jobs = m.refreshMeta.ListAllJobs()
	} else {
		jobs = m.refreshMeta.ListJobsByCollectionID(collectionID)
	}

	result := make([]*datapb.ExternalCollectionRefreshJob, 0, len(jobs))
	for _, job := range jobs {
		// Aggregate state and progress from tasks
		state, progress := m.refreshMeta.AggregateJobStateFromTasks(job.GetJobId())
		normalizeRefreshJobProgress(job, state, progress)
		result = append(result, job)
	}

	return result, nil
}

// GetActiveJobByCollectionID delegates to the meta layer. The underlying meta
// query takes the per-collection job lock, so concurrent AddJob calls observe
// a consistent view.
func (m *externalCollectionRefreshManager) GetActiveJobByCollectionID(collectionID int64) *datapb.ExternalCollectionRefreshJob {
	return m.refreshMeta.GetActiveJobByCollectionID(collectionID)
}

// exploreExternalFiles calls ExploreFiles once on DataCoord and returns the full file list.
func (m *externalCollectionRefreshManager) exploreExternalFiles(
	ctx context.Context,
	job *datapb.ExternalCollectionRefreshJob,
) ([]*datapb.ExternalFileInfo, string, error) {
	// Revalidate source+spec at refresh time: etcd is not a trusted boundary,
	// and validation rules may have tightened since the collection was created.
	// Empty source is legal (see typeutil.IsExternalCollection); only validate
	// when both present.
	if job.GetExternalSource() != "" {
		if err := externalspec.ValidateSourceAndSpec(job.GetExternalSource(), job.GetExternalSpec()); err != nil {
			return nil, "", fmt.Errorf("external source/spec failed revalidation: %w", err)
		}
	}
	spec, err := externalspec.ParseExternalSpec(job.GetExternalSpec())
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse external spec: %w", err)
	}

	collInfo := m.mt.GetCollection(job.GetCollectionId())
	if collInfo == nil {
		return nil, "", fmt.Errorf("collection %d not found", job.GetCollectionId())
	}

	columns := packed.GetColumnNamesFromSchema(collInfo.Schema)
	storageConfig := createStorageConfig()
	extfs := packed.ExternalSpecContext{
		CollectionID: job.GetCollectionId(),
		Source:       job.GetExternalSource(),
		Spec:         job.GetExternalSpec(),
	}

	exploreBaseDir := exploreTempDirForJob(job.GetJobId())
	fileInfos, manifestPath, err := packed.ExploreFilesReturnManifestPath(
		columns,
		spec.Format,
		exploreBaseDir,
		job.GetExternalSource(),
		storageConfig,
		extfs,
	)
	if err != nil {
		return nil, "", fmt.Errorf("ExploreFilesReturnManifestPath failed: %w", err)
	}

	// Convert to proto type
	result := make([]*datapb.ExternalFileInfo, len(fileInfos))
	for i, fi := range fileInfos {
		result[i] = &datapb.ExternalFileInfo{
			FilePath: fi.FilePath,
			NumRows:  fi.NumRows,
		}
	}
	return result, manifestPath, nil
}
