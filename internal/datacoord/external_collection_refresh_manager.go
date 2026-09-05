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
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var errMilvusTableRefreshSchemaInvalid = errors.New("milvus-table refresh schema invalid")

// externalRefreshRetryTaskError carries the exact persisted result that lost a
// baseline-manifest CAS. The checker uses that token to move only the still-
// matching Finished task to Retry; a result already consumed or replaced is an
// idempotent no-op.
type externalRefreshRetryTaskError struct {
	taskID               int64
	segmentID            int64
	resultStorageVersion int32
	resultPath           string
	resultChecksum       []byte
	cause                error
}

func (e *externalRefreshRetryTaskError) Error() string {
	return fmt.Sprintf(
		"external refresh task %d must retry after manifest conflict on segment %d: %v",
		e.taskID,
		e.segmentID,
		e.cause,
	)
}

func (e *externalRefreshRetryTaskError) Unwrap() error {
	return e.cause
}

// Bound DataCoord's job-level manifest reads without multiplying the per-task
// object-storage concurrency already used by DataNodes.
const externalRefreshManifestReadConcurrency = 16

// exploreTempDirForJob returns the root directory for every Explore attempt of
// one refresh job. Terminal cleanup removes this root and all attempt manifests.
func exploreTempDirForJob(jobID int64) string {
	return fmt.Sprintf("__explore_temp__/coord_%d", jobID)
}

// exploreTempDirForAttempt isolates manifests produced by retried planning
// attempts while keeping the parent job directory as the cleanup boundary.
func exploreTempDirForAttempt(jobID, attemptID int64) string {
	return fmt.Sprintf("%s/attempt_%d", exploreTempDirForJob(jobID), attemptID)
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
	cancel    context.CancelFunc
	mt        *meta
	scheduler task.GlobalScheduler
	allocator allocator.Allocator
	cluster   session.Cluster

	// collectionGetter reads collection metadata from its authoritative owner.
	// Results are used only by the current operation and are not retained here.
	collectionGetter func(ctx context.Context, collectionID int64) (*collectionInfo, error)

	// schemaUpdater broadcasts schema changes to RootCoord via WAL during job
	// finalization, before the job is persisted as Finished.
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

	// initJobsInFlight tracks jobs whose async task-creation (Phase B) is
	// currently running. SubmitRefreshJobWithID persists the job record in
	// Init state on the WAL ack callback path and returns immediately; the
	// S3 explore + task split + scheduler enqueue run in a background
	// goroutine so the broadcaster is never blocked on object-store I/O.
	// Both the eager Submit path and the periodic checker tick drive the
	// same entry point (ensureTasksForInitJob) and this map dedups them so
	// at most one explore is in flight per jobID at any moment.
	initMu           sync.Mutex
	stopped          bool
	initJobsInFlight map[int64]struct{}
}

// NewExternalCollectionRefreshManager creates a new external table refresh manager.
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
	managerCtx, cancel := context.WithCancel(ctx) //nolint:gosec // cancel is retained by the manager and invoked by Stop.
	closeChan := make(chan struct{})

	m := &externalCollectionRefreshManager{
		ctx:              managerCtx,
		cancel:           cancel,
		mt:               mt,
		scheduler:        scheduler,
		allocator:        allocator,
		cluster:          cluster,
		refreshMeta:      refreshMeta,
		collectionGetter: collectionGetter,
		schemaUpdater:    schemaUpdater,
		chunkManager:     chunkManager,
		closeChan:        closeChan,
		initJobsInFlight: make(map[int64]struct{}),
	}

	// Create internal components with shared refreshMeta. The checker owns
	// the per-job processing function that drives state aggregation,
	// finalization, timeout, and GC. Tasks wired by the inspector call the
	// checker's per-job entry point synchronously when they reach a terminal
	// state, so segment publication and the schema WAL complete before the job
	// becomes Finished. The checker still runs the same function periodically
	// as a safety net for missed events (e.g., after a DataCoord restart).
	m.inspector = newRefreshInspector(managerCtx, refreshMeta, scheduler, closeChan)
	m.checker = newRefreshChecker(managerCtx, mt, refreshMeta, closeChan, m.syncJobSchema, m.applyFinishedJobSegments, m.handleJobFailed, m.handleJobCleanup, m.ensureTasksForInitJob)
	m.inspector.allocateTaskID = m.allocator.AllocID
	m.inspector.wrapTask = m.wrapTask
	m.checker.dropJobTasks = m.dropJobTasks

	return m
}

// handleJobCleanup removes the idempotent temp directory before Finished is
// published and before retention GC drops the job metadata. A transient
// object-store failure keeps the job as the retry anchor for the next pass.
func (m *externalCollectionRefreshManager) handleJobCleanup(jobID int64) error {
	return m.cleanupExploreTempForJob(jobID)
}

// handleJobFailed reclaims per-job resources when the checker transitions
// a job into Failed state (via aggregateJobState or tryTimeoutJob). The durable
// terminal-state transition already owns this callback, so no second in-memory
// dedup is needed. Cleanup is idempotent and GC retries it once more.
func (m *externalCollectionRefreshManager) handleJobFailed(jobID int64) {
	_ = m.cleanupExploreTempForJob(jobID)
}

// cleanupExploreTempForJob removes the per-job explore temp directory on
// shared storage. Every planning attempt writes below
// `__explore_temp__/coord_{jobID}/attempt_{attemptID}`; removing the job root
// reclaims successful and abandoned attempts together.
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
func (m *externalCollectionRefreshManager) cleanupExploreTempForJob(jobID int64) error {
	if m.chunkManager == nil {
		return nil
	}
	exploreBaseDir := exploreTempDirForJob(jobID)
	explorePrefix := exploreBaseDir + "/"
	// Derive from m.ctx so shutdown cancels in-flight cleanup instead of
	// blocking Stop() on a slow object-store call.
	ctx, cancel := context.WithTimeout(m.ctx, 30*time.Second)
	defer cancel()

	if err := m.chunkManager.RemoveWithPrefix(ctx, explorePrefix); err != nil {
		mlog.Warn(m.ctx, "failed to remove explore temp prefix",
			mlog.FieldJobID(jobID),
			mlog.String("dir", explorePrefix),
			mlog.Err(err))
		return err
	}
	if err := m.chunkManager.Remove(ctx, exploreBaseDir); err != nil {
		mlog.Warn(m.ctx, "failed to remove explore temp root",
			mlog.FieldJobID(jobID),
			mlog.String("dir", exploreBaseDir),
			mlog.Err(err))
		return err
	}
	return nil
}

// applyFinishedJobSegments validates durable task results against the published
// ownership plan, aggregates them, and applies the complete result as one
// job-level metadata mutation.
// An owned baseline segment absent from both kept and updated results is treated
// as removed, but a task may classify only the baseline segments it owns.
func (m *externalCollectionRefreshManager) applyFinishedJobSegments(ctx context.Context, job *datapb.ExternalCollectionRefreshJob) error {
	if m.collectionGetter == nil {
		return merr.WrapErrServiceInternalMsg("collection getter is not configured")
	}
	collection, err := m.collectionGetter(ctx, job.GetCollectionId())
	if err != nil {
		if errors.Is(err, merr.ErrCollectionNotFound) {
			return nil
		}
		return err
	}
	if collection == nil {
		return merr.WrapErrServiceNotReadyMsg(
			"collection %d lookup returned no metadata during external refresh finalization",
			job.GetCollectionId(),
		)
	}

	return m.refreshMeta.ConsumeCommittedTaskResults(
		job.GetJobId(),
		func(tasks []*datapb.ExternalCollectionRefreshTask, taskActions []metastore.UpdateAction) error {
			return m.applyLoadedFinishedJobSegments(ctx, job, collection, tasks, taskActions)
		},
	)
}

func (m *externalCollectionRefreshManager) applyLoadedFinishedJobSegments(
	ctx context.Context,
	job *datapb.ExternalCollectionRefreshJob,
	collection *collectionInfo,
	tasks []*datapb.ExternalCollectionRefreshTask,
	taskActions []metastore.UpdateAction,
) error {
	if len(tasks) == 0 {
		return merr.WrapErrDataIntegrityMsg("external refresh job %d has no tasks to apply", job.GetJobId())
	}

	// Reconstruct the immutable refresh baseline and its exclusive task owners
	// from persisted metadata instead of the collection's current segment set.
	ownerBySegment := make(map[int64]int64)
	taskByID := make(map[int64]*datapb.ExternalCollectionRefreshTask, len(tasks))
	baseManifestBySegment := make(map[int64]string)
	baselineSegmentIDs := make([]int64, 0)
	for _, task := range tasks {
		taskByID[task.GetTaskId()] = task
		if !isSupportedExternalRefreshOwnershipPlanVersion(task.GetOwnershipPlanVersion()) {
			return merr.WrapErrDataIntegrityMsg(
				"job %d contains external refresh task %d with unsupported ownership plan version %d; retry refresh",
				job.GetJobId(),
				task.GetTaskId(),
				task.GetOwnershipPlanVersion(),
			)
		}
		for _, segmentID := range task.GetOwnedSegmentIds() {
			if segmentID <= 0 {
				return merr.WrapErrDataIntegrityMsg("task %d owns invalid segment ID %d", task.GetTaskId(), segmentID)
			}
			if ownerTaskID, ok := ownerBySegment[segmentID]; ok {
				return merr.WrapErrDataIntegrityMsg(
					"segment %d is owned by both external refresh tasks %d and %d",
					segmentID,
					ownerTaskID,
					task.GetTaskId(),
				)
			}
			ownerBySegment[segmentID] = task.GetTaskId()
			if baseManifest, ok := task.GetBaseManifests()[segmentID]; ok {
				baseManifestBySegment[segmentID] = baseManifest
			}
			baselineSegmentIDs = append(baselineSegmentIDs, segmentID)
		}
		for segmentID := range task.GetBaseManifests() {
			if ownerTaskID, ok := ownerBySegment[segmentID]; ok && ownerTaskID == task.GetTaskId() {
				continue
			}
			owned := false
			for _, ownedSegmentID := range task.GetOwnedSegmentIds() {
				if segmentID == ownedSegmentID {
					owned = true
					break
				}
			}
			if !owned {
				return merr.WrapErrDataIntegrityMsg(
					"task %d carries a base manifest for unowned segment %d",
					task.GetTaskId(),
					segmentID,
				)
			}
		}
	}
	// Validate that every baseline classification came from its owner task while
	// allowing newly allocated segment IDs that are outside the baseline.
	keptSet := make(map[int64]struct{})
	updatedSet := make(map[int64]struct{})
	classifiedBaselineCount := 0
	patchedSegmentCount := 0
	createdSegmentCount := 0
	keptSegments := make([]int64, 0)
	updatedSegments := make([]*datapb.SegmentInfo, 0)
	for _, task := range tasks {
		if task.GetState() != indexpb.JobState_JobStateFinished {
			return merr.WrapErrServiceInternalMsg("job %d has non-finished task %d in state %s",
				job.GetJobId(), task.GetTaskId(), task.GetState().String())
		}
		if !task.GetResultReady() {
			return merr.WrapErrDataIntegrityMsg("job %d has finished task %d without persisted refresh result",
				job.GetJobId(), task.GetTaskId())
		}
		for _, segmentID := range task.GetKeptSegments() {
			ownerTaskID, ok := ownerBySegment[segmentID]
			if !ok || ownerTaskID != task.GetTaskId() {
				return merr.WrapErrDataIntegrityMsg(
					"task %d returned kept segment %d owned by task %d",
					task.GetTaskId(),
					segmentID,
					ownerTaskID,
				)
			}
			if _, ok := keptSet[segmentID]; ok {
				return merr.WrapErrDataIntegrityMsg("job %d has duplicate kept segment %d from task %d",
					job.GetJobId(), segmentID, task.GetTaskId())
			}
			keptSet[segmentID] = struct{}{}
			classifiedBaselineCount++
			keptSegments = append(keptSegments, segmentID)
		}
		for _, segment := range task.GetUpdatedSegments() {
			if segment == nil {
				return merr.WrapErrDataIntegrityMsg("task %d returned a nil updated segment", task.GetTaskId())
			}
			if _, ok := updatedSet[segment.GetID()]; ok {
				return merr.WrapErrDataIntegrityMsg("job %d has duplicate updated segment %d from task %d",
					job.GetJobId(), segment.GetID(), task.GetTaskId())
			}
			if ownerTaskID, ok := ownerBySegment[segment.GetID()]; ok {
				if ownerTaskID != task.GetTaskId() {
					return merr.WrapErrDataIntegrityMsg(
						"task %d returned updated segment %d owned by task %d",
						task.GetTaskId(),
						segment.GetID(),
						ownerTaskID,
					)
				}
				if _, kept := keptSet[segment.GetID()]; kept {
					return merr.WrapErrDataIntegrityMsg("segment %d cannot be both kept and updated", segment.GetID())
				}
				classifiedBaselineCount++
				patchedSegmentCount++
			} else {
				createdSegmentCount++
			}
			updatedSet[segment.GetID()] = struct{}{}
			updatedSegments = append(updatedSegments, segment)
		}
	}

	// Task results carry physical row counts for every patched or newly created
	// segment. Unchanged segments are represented only by ID, so read their
	// baseline row counts from one metadata snapshot before applying the job.
	baselineRowsBySegment := make(map[int64]int64, len(baselineSegmentIDs))
	var baselineRows int64
	if m.mt != nil {
		baselineSegments := getExternalRefreshSegmentSnapshots(m.mt, baselineSegmentIDs)
		for index, segment := range baselineSegments {
			if segment == nil {
				continue
			}
			rows := segment.GetNumOfRows()
			baselineRowsBySegment[baselineSegmentIDs[index]] = rows
			baselineRows += rows
		}
	}
	var refreshedRows int64
	for _, segmentID := range keptSegments {
		refreshedRows += baselineRowsBySegment[segmentID]
	}
	for _, segment := range updatedSegments {
		refreshedRows += segment.GetNumOfRows()
	}

	mlog.Info(ctx, "aggregated ownership-scoped external refresh results",
		mlog.FieldJobID(job.GetJobId()),
		mlog.FieldCollectionID(job.GetCollectionId()),
		mlog.Int("numTasks", len(tasks)),
		mlog.Int("baselineSegments", len(baselineSegmentIDs)),
		mlog.Int("keptSegments", len(keptSegments)),
		mlog.Int("updatedSegments", len(updatedSegments)),
		mlog.Int("patchedSegments", patchedSegmentCount),
		mlog.Int("createdSegments", createdSegmentCount),
		mlog.Int("removedSegments", len(baselineSegmentIDs)-classifiedBaselineCount),
		mlog.Int("finalSegments", len(keptSegments)+len(updatedSegments)),
		mlog.Int64("baselineRows", baselineRows),
		mlog.Int64("refreshedRows", refreshedRows),
		mlog.Int64("rowDelta", refreshedRows-baselineRows))

	// Intentionally allow the collection schema to advance while tasks are
	// running. For the current additive-only scope, an older-schema refresh can
	// be applied; it may miss newly added external columns, and the next refresh
	// self-heals them. Segment-level validation still rejects schema-version
	// rollback, but drop, rename, or type changes need a schema gate or lock
	// before they are supported.
	err := applyExternalCollectionSegmentUpdateForBaseline(
		ctx,
		m.mt,
		collection,
		job.GetCollectionId(),
		baselineSegmentIDs,
		baseManifestBySegment,
		keptSegments,
		updatedSegments,
		taskActions,
		mlog.FieldJobID(job.GetJobId()),
	)
	var conflict *externalRefreshManifestConflictError
	if !errors.As(err, &conflict) {
		return err
	}
	ownerTaskID, ok := ownerBySegment[conflict.segmentID]
	if !ok {
		return merr.WrapErrDataIntegrityMsg(
			"manifest conflict for segment %d has no owning external refresh task",
			conflict.segmentID,
		)
	}
	ownerTask := taskByID[ownerTaskID]
	if ownerTask == nil {
		return merr.WrapErrDataIntegrityMsg("external refresh task %d not found for manifest conflict", ownerTaskID)
	}
	return &externalRefreshRetryTaskError{
		taskID:               ownerTaskID,
		segmentID:            conflict.segmentID,
		resultStorageVersion: ownerTask.GetResultStorageVersion(),
		resultPath:           ownerTask.GetResultPath(),
		resultChecksum:       append([]byte(nil), ownerTask.GetResultChecksum()...),
		cause:                conflict,
	}
}

// wrapTask builds a scheduler-facing task wrapper around a persisted proto
// task, wiring the processFinishedJob callback so terminal transitions drive
// per-job processing synchronously. Single source of truth for task wiring;
// used by both createTasksForJob (initial submission) and the inspector
// (reload/re-enqueue paths).
func (m *externalCollectionRefreshManager) wrapTask(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
	taskWrapper := newRefreshExternalCollectionTask(t, m.refreshMeta, m.mt, m.allocator)
	taskWrapper.processFinishedJob = m.checker.processJobByID
	taskWrapper.collectionGetter = m.collectionGetter
	return taskWrapper
}

// dropJobTasks first removes each task from scheduler dispatch under its task
// lock, then asks the assigned DataNode to drop it while that same lock is held.
// This prevents a last in-flight scheduler callback from racing the Drop. The
// terminal job and task records remain the retry anchor until every assigned
// DataNode has acknowledged Drop (or left the cluster).
func (m *externalCollectionRefreshManager) dropJobTasks(jobID int64) error {
	tasks, err := m.refreshMeta.GetCommittedTasksByJobID(jobID)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		if m.scheduler == nil {
			return merr.WrapErrServiceNotReadyMsg("external refresh task scheduler is unavailable")
		}
		taskID := task.GetTaskId()
		var dropErr error
		m.scheduler.Finalize(taskID, func() {
			// A Create callback that was already holding the scheduler task lock may
			// have persisted its worker assignment while Finalize waited. Resolve the
			// owner only after that callback drains; the pre-Finalize snapshot may
			// still carry NodeId=0 and must not make us skip the real worker task.
			latest := m.refreshMeta.GetTask(taskID)
			if latest == nil || latest.GetNodeId() <= 0 {
				return
			}
			if m.cluster == nil {
				dropErr = merr.WrapErrServiceNotReadyMsg("external refresh worker cluster is unavailable")
				return
			}
			if err := m.cluster.DropRefreshExternalCollectionTask(latest.GetNodeId(), taskID); err != nil &&
				!errors.Is(err, merr.ErrNodeNotFound) {
				dropErr = merr.Wrapf(err, "drop external refresh task %d on worker %d", taskID, latest.GetNodeId())
			}
		})
		if dropErr != nil {
			return dropErr
		}
	}
	return nil
}

// Start begins all internal component loops (inspector and checker).
// This should be called once during DataCoord startup.
func (m *externalCollectionRefreshManager) Start() {
	m.initMu.Lock()
	if m.stopped {
		m.initMu.Unlock()
		return
	}
	m.wg.Add(2)
	m.initMu.Unlock()

	// Start inspector loop
	go func() {
		defer m.wg.Done()
		m.inspector.run()
	}()

	// Start checker loop
	go func() {
		defer m.wg.Done()
		m.checker.run()
	}()
}

// Stop gracefully shuts down all internal components.
// Safe to call multiple times (uses sync.Once internally).
func (m *externalCollectionRefreshManager) Stop() {
	m.closeOnce.Do(func() {
		m.initMu.Lock()
		m.stopped = true
		m.cancel()
		close(m.closeChan)
		m.initMu.Unlock()
	})
	m.wg.Wait()
}

// syncJobSchema publishes the refreshed source/spec before the job transitions
// to Finished. Replaying it is harmless: once RootCoord observes the
// WAL update, the equality check makes subsequent calls no-ops.
func (m *externalCollectionRefreshManager) syncJobSchema(ctx context.Context, job *datapb.ExternalCollectionRefreshJob) error {
	if m.schemaUpdater == nil {
		return nil
	}

	// Get current collection info
	collection, err := m.collectionGetter(ctx, job.GetCollectionId())
	if err != nil {
		mlog.Warn(ctx, "failed to get collection for external refresh schema update",
			mlog.FieldJobID(job.GetJobId()),
			mlog.FieldCollectionID(job.GetCollectionId()),
			mlog.Err(err))
		if errors.Is(err, merr.ErrCollectionNotFound) {
			// The collection was dropped while its refresh was finishing. There
			// is no remaining schema to update, so the job may settle normally.
			return nil
		}
		return err
	}
	if collection == nil {
		// The collection was dropped while its refresh was finishing. There is
		// no remaining schema to update, so the job may settle normally.
		return nil
	}

	// Check if external_source or external_spec changed
	currentSource := collection.Schema.GetExternalSource()
	currentSpec := collection.Schema.GetExternalSpec()
	newSource := job.GetExternalSource()
	newSpec := job.GetExternalSpec()

	if currentSource == newSource && currentSpec == newSpec {
		return nil
	}

	mlog.Info(ctx, "updating collection schema while finalizing refresh",
		mlog.FieldJobID(job.GetJobId()),
		mlog.FieldCollectionID(job.GetCollectionId()),
		mlog.String("oldSource", externalspec.RedactExternalSource(currentSource)),
		mlog.String("newSource", externalspec.RedactExternalSource(newSource)),
		mlog.String("oldSpec", externalspec.RedactExternalSpecForLog(currentSpec)),
		mlog.String("newSpec", externalspec.RedactExternalSpecForLog(newSpec)))

	if err := m.schemaUpdater(ctx, job.GetCollectionId(), newSource, newSpec); err != nil {
		mlog.Warn(ctx, "failed to update external schema before completing refresh",
			mlog.FieldJobID(job.GetJobId()),
			mlog.FieldCollectionID(job.GetCollectionId()),
			mlog.Err(err))
		return err
	}
	return nil
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
	log := mlog.With(
		mlog.FieldJobID(jobID),
		mlog.FieldCollectionID(collectionID),
		mlog.FieldCollectionName(collectionName))

	// Idempotency: if job already exists, return. TOCTOU between this check and AddJob
	// is mitigated by WAL idempotency (same JobID on retry) and per-collection lock in AddJob.
	existingJob := m.refreshMeta.GetJob(jobID)
	if existingJob != nil {
		log.Info(ctx, "job already exists, skip creating")
		// Retry Phase B in case the prior submission failed to create tasks
		// and left the job stuck in Init. ensureTasksForInitJob dedups
		// concurrent invocations internally.
		m.ensureTasksForInitJob(jobID)
		return jobID, nil
	}

	// Get current RootCoord metadata to validate the collection.
	collection, err := m.collectionGetter(ctx, collectionID)
	if err != nil {
		if errors.Is(err, merr.ErrCollectionNotFound) {
			log.Warn(ctx, "collection not found", mlog.Err(err))
		} else {
			log.Warn(ctx, "failed to get collection metadata", mlog.Err(err))
		}
		return 0, err
	}
	if collection == nil {
		log.Warn(ctx, "collection metadata is unavailable")
		return 0, merr.WrapErrServiceNotReadyMsg(
			"collection %d lookup returned no metadata during external refresh submission",
			collectionID,
		)
	}

	// Validate it's an external collection
	if !typeutil.IsExternalCollection(collection.Schema) {
		log.Warn(ctx, "not an external collection")
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
		log.Warn(ctx, "refresh job already in progress",
			mlog.Int64("existingJobID", activeJob.GetJobId()),
			mlog.String("existingJobState", activeJob.GetState().String()))
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
		log.Warn(ctx, "failed to add job to meta", mlog.Err(err))
		return 0, err
	}

	log.Info(ctx, "external collection refresh job accepted (Init), task creation deferred to async phase",
		mlog.String("externalSource", externalspec.RedactExternalSource(externalSource)))

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
	if m.stopped {
		m.initMu.Unlock()
		return
	}
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
	m.wg.Add(1)
	m.initMu.Unlock()

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

		log := mlog.With(mlog.FieldJobID(jobID))

		// Re-read under goroutine to catch race where state changed between
		// the cheap pre-check above and actual work start.
		freshJob := m.refreshMeta.GetJob(jobID)
		if freshJob == nil {
			log.Info(m.ctx, "init job gone before async task creation ran")
			return
		}
		if freshJob.GetState() != indexpb.JobState_JobStateInit {
			log.Info(m.ctx, "init job no longer in Init state, skip async task creation",
				mlog.String("state", freshJob.GetState().String()))
			return
		}
		if len(freshJob.GetTaskIds()) > 0 {
			log.Info(m.ctx, "init job already has tasks, skip async task creation",
				mlog.Int("taskCount", len(freshJob.GetTaskIds())))
			return
		}

		tasks, err := m.createTasksForJob(ctx, freshJob)
		if err != nil {
			if errors.Is(err, errExternalRefreshTaskPlanNotPublishable) {
				log.Info(m.ctx, "async task creation stopped because job is no longer publishable",
					mlog.Err(err))
				return
			}
			// An InputError blames the request: the source the caller named
			// is empty, or its schema/manifest cannot be read as the requested
			// format. No retry changes that, so the job goes to Failed at once
			// instead of having the checker tick re-run the same explore
			// forever with no signal for operators to act on.
			//
			// This is merr's own Input-vs-System split, the same one the
			// DataNode uses to decide Failed vs Retry for a refresh task. There
			// is deliberately no separate "non-retriable" error type: a second
			// taxonomy alongside merr's is one more thing to keep in agreement.
			if merr.GetErrorType(err) == merr.InputError {
				log.Warn(m.ctx, "non-retriable error in task creation, marking job failed",
					mlog.Err(err))
				if _, uerr := m.refreshMeta.UpdateJobState(jobID,
					indexpb.JobState_JobStateFailed, err.Error()); uerr != nil {
					log.Warn(m.ctx, "failed to mark job failed", mlog.Err(uerr))
				}
				return
			}
			// Transient failures (e.g. S3 blip) — leave in Init so the
			// checker tick / WAL redelivery path retries. tryTimeoutJob
			// bounds how long a stuck job can linger.
			//
			// Record why on the job while leaving it in Init. Without this the
			// only thing a caller ever sees for a source that never comes back
			// is the checker's bare "timeout", with the actual error (a bad
			// bucket, a denied credential) buried in DataNode logs.
			//
			// First cause only: explore errors embed the attempt-scoped
			// manifest path, so re-recording each attempt would defeat the
			// identical-write guard in UpdateJobState and cost one catalog
			// write per checker tick for the job's whole stuck lifetime. The
			// first cause is what an operator needs; the per-attempt detail
			// stays in this Warn.
			log.RatedWarn(m.ctx, rate.Limit(1.0/60), "async task creation failed, will retry on next checker tick",
				mlog.Err(err))
			if job := m.refreshMeta.GetJob(jobID); job != nil && job.GetFailReason() == "" {
				if _, uerr := m.refreshMeta.UpdateJobState(jobID,
					indexpb.JobState_JobStateInit, err.Error()); uerr != nil {
					log.Warn(m.ctx, "failed to record the transient task creation failure", mlog.Err(uerr))
				}
			}
			return
		}

		// Enqueue all created tasks for scheduling.
		for _, t := range tasks {
			m.scheduler.Enqueue(t)
		}
		log.Info(m.ctx, "async task creation completed",
			mlog.Int("taskCount", len(tasks)))
	}()
}

// createTasksForJob creates task(s) for a job and persists them to meta.
// Returns the created tasks for subsequent scheduling.
//
// Task ranges use ExternalCollectionFilesPerTask as a target, but ownership
// closure may make a protected range larger. Each task carries the manifest
// produced by this planning attempt plus a [FileIndexBegin, FileIndexEnd)
// slice. All tasks in the plan share that manifest; if publication fails, a
// later planning retry may run Explore again and produce another manifest.
func (m *externalCollectionRefreshManager) createTasksForJob(
	ctx context.Context,
	job *datapb.ExternalCollectionRefreshJob,
) ([]*refreshExternalCollectionTask, error) {
	log := mlog.With(mlog.FieldJobID(job.GetJobId()), mlog.FieldCollectionID(job.GetCollectionId()))

	// Explore once for this planning attempt to get the full file list and
	// manifest path. The manifest is written to shared storage so all DataNodes
	// in the resulting plan can read their assigned ranges.
	allFiles, manifestPath, err := m.exploreExternalFiles(ctx, job)
	if err != nil {
		// TODO(milvus-storage): classify deterministic milvus-table content
		// failures (invalid snapshot JSON/version, invalid or empty manifests,
		// and inconsistent segment metadata) at the storage boundary. Once those
		// errors carry a stable structured category, preserve it as InputError
		// here instead of string-matching messages. Unknown storage/transport
		// failures must remain retryable.
		// Hard explore failures are terminal for this job: the source's
		// snapshot metadata is malformed, absent, or incompatible with the
		// requested external format. Surface them as non-retriable so the
		// user gets a clear RefreshFailed signal and can re-issue refresh
		// after fixing the source. Pure in-process errors (ctx cancel, etcd
		// unavailable, etc.) keep the existing transient path so a real
		// outage still gets retried.
		//
		// ErrLoonTransient is deliberately not in this set. packed declares it
		// as "treat all loon failures as retryable for now" precisely because
		// milvus-storage can lose the structured error detail and fall back to
		// a generic code, so a transient object-storage fault is
		// indistinguishable from a permanent one at this boundary. Calling it
		// terminal here inverted that contract and failed refreshes that a
		// retry would have completed.
		if errors.Is(err, errMilvusTableRefreshSchemaInvalid) ||
			packed.IsMilvusTableStorageV2ManifestListMissing(err) {
			// Marked rather than re-originated: WrapErrAsInputError relabels the
			// classification and nothing else, so errors.Is still finds
			// errMilvusTableRefreshSchemaInvalid underneath.
			return nil, merr.WrapErrAsInputError(merr.Wrap(err, "explore external files failed"))
		}
		// Source/spec validation already returns typed merr InputErrors. Preserve
		// that classification so the caller can fail the job instead of retrying
		// a deterministic request error. Unknown packed/FFI errors remain on the
		// existing SystemError path below.
		if merr.GetErrorType(err) == merr.InputError {
			return nil, merr.Wrap(err, "failed to explore external files")
		}
		return nil, merr.WrapErrServiceInternalErr(err, "failed to explore external files")
	}
	if len(allFiles) == 0 {
		// ErrParameterInvalid is baked InputError, which is the whole
		// classification: the source the request named has nothing to refresh
		// from, and no retry changes that. The DataNode's sibling check on a
		// zero-row source uses the same factory.
		return nil, merr.WrapErrParameterInvalidMsg("no files found in external source")
	}
	// NOTE: zero-total-rows cannot be detected here. PlainFormat::explore
	// hardcodes start_index/end_index to -1 as sentinels and never reads
	// parquet metadata, so FileInfo.NumRows carries -1, not a real row count.
	// The real guard lives at datanode's balanceFragmentsToSegments, where
	// fragment RowCount is populated from manifest (endRow - startRow).
	log.Info(ctx, "explored external files for task splitting",
		mlog.Int("totalFiles", len(allFiles)),
		mlog.String("manifestPath", manifestPath))

	currentSegments := m.mt.SelectSegments(
		ctx,
		CollectionFilter(job.GetCollectionId()),
		SegmentFilterFunc(isSegmentHealthy),
	)
	baselineSegments := make([]*datapb.SegmentInfo, 0, len(currentSegments))
	baselineManifestSegments := 0
	for _, segment := range currentSegments {
		baselineSegments = append(baselineSegments, segment.SegmentInfo)
		if segment.GetManifestPath() != "" {
			baselineManifestSegments++
		}
	}

	manifestReadStart := time.Now()
	segmentFragments, err := packed.BuildCurrentSegmentFragmentsConcurrently(
		ctx,
		baselineSegments,
		createStorageConfig(),
		nil,
		externalRefreshManifestReadConcurrency,
	)
	if err != nil {
		return nil, merr.Wrap(err, "read external refresh baseline manifests")
	}
	log.Info(ctx, "read external refresh baseline manifests",
		mlog.Int("baselineSegments", len(baselineSegments)),
		mlog.Int("manifestSegments", baselineManifestSegments),
		mlog.Int("maxConcurrency", externalRefreshManifestReadConcurrency),
		mlog.Duration("duration", time.Since(manifestReadStart)))

	filesPerTask := paramtable.Get().DataCoordCfg.ExternalCollectionFilesPerTask.GetAsInt64()
	taskPlans, ownershipSummary, err := planExternalRefreshOwnership(
		allFiles,
		segmentFragments,
		filesPerTask,
	)
	if err != nil {
		return nil, err
	}

	log.Info(ctx, "splitting refresh job into tasks",
		mlog.Int("totalFiles", len(allFiles)),
		mlog.Int64("filesPerTask", filesPerTask),
		mlog.Int("baselineSegments", len(baselineSegments)),
		mlog.Int("baseNumTasks", ownershipSummary.BaseTaskCount),
		mlog.Int("numTasks", ownershipSummary.FinalTaskCount),
		mlog.Int("closureRemovedBoundaries", ownershipSummary.ClosureRemovedBoundaries),
		mlog.Int("maxTaskFiles", ownershipSummary.MaxTaskFiles),
		mlog.Int("maxOwnedSegments", ownershipSummary.MaxOwnedSegments),
		mlog.Int("tasksWithoutOwnedSegments", ownershipSummary.TasksWithoutOwnedSegments),
		mlog.Int("baselineFilePaths", ownershipSummary.BaselineFilePaths),
		mlog.Int("addedFilePaths", ownershipSummary.AddedFilePaths),
		mlog.Int("removedFilePaths", ownershipSummary.RemovedFilePaths),
		mlog.Int("unchangedFilePaths", ownershipSummary.UnchangedFilePaths))

	// Allocate IDs and build every task first (ID allocation order preserved),
	// then persist all task saves plus the job's updated TaskIds as a single
	// composite catalog write - the job written last as the commit marker - so
	// a partial failure can no longer desync the job's TaskIds from the
	// persisted task set. In-memory bookkeeping is applied only after that
	// write succeeds.
	rawTasks := make([]*datapb.ExternalCollectionRefreshTask, 0, len(taskPlans))
	for _, plan := range taskPlans {
		taskID, err := m.allocator.AllocID(ctx)
		if err != nil {
			log.Warn(ctx, "failed to allocate task ID", mlog.Err(err))
			return nil, err
		}

		task := &datapb.ExternalCollectionRefreshTask{
			TaskId:               taskID,
			JobId:                job.GetJobId(),
			CollectionId:         job.GetCollectionId(),
			NodeId:               0,
			State:                indexpb.JobState_JobStateInit,
			ExternalSource:       job.GetExternalSource(),
			ExternalSpec:         job.GetExternalSpec(),
			Progress:             0,
			ExploreManifestPath:  manifestPath,
			FileIndexBegin:       plan.FileIndexBegin,
			FileIndexEnd:         plan.FileIndexEnd,
			OwnershipPlanVersion: externalRefreshOwnershipPlanVersion,
			OwnedSegmentIds:      append([]int64(nil), plan.OwnedSegmentIDs...),
		}
		log.Debug(ctx, "planned external refresh task",
			mlog.FieldTaskID(taskID),
			mlog.Int64("fileIndexBegin", plan.FileIndexBegin),
			mlog.Int64("fileIndexEnd", plan.FileIndexEnd),
			mlog.Int64("fileCount", plan.FileIndexEnd-plan.FileIndexBegin),
			mlog.Int("ownedSegments", len(plan.OwnedSegmentIDs)))
		rawTasks = append(rawTasks, task)
	}

	if err = m.refreshMeta.AddTasksToJob(job.GetJobId(), rawTasks); err != nil {
		if errors.Is(err, errExternalRefreshTaskPlanNotPublishable) {
			latestJob := m.refreshMeta.GetJob(job.GetJobId())
			if latestJob == nil ||
				latestJob.GetState() == indexpb.JobState_JobStateFinished ||
				latestJob.GetState() == indexpb.JobState_JobStateFailed {
				// A terminal transition may have cleaned the job directory while
				// Explore was still writing. Re-run the idempotent cleanup after the
				// definitive pre-write rejection to remove any late manifest.
				_ = m.cleanupExploreTempForJob(job.GetJobId())
			}
		}
		log.Warn(ctx, "failed to add tasks to job", mlog.Err(err))
		return nil, err
	}

	tasks := make([]*refreshExternalCollectionTask, 0, len(rawTasks))
	for _, task := range rawTasks {
		tasks = append(tasks, m.wrapTask(task))
	}

	log.Info(ctx, "tasks created for job",
		mlog.Int("numTasks", len(tasks)),
		mlog.FieldJobID(job.GetJobId()))

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
		// A job in the index wait has every task Finished, so the task
		// aggregate is a flat 100 and says nothing about the wait. Its
		// persisted progress is the indexed fraction - the only signal there
		// is - so prefer it. Keyed on the wait marker, not on the value: below
		// the wait, the persisted number is just the last ingest progress and
		// the brief pre-transition window must still read "as good as done".
		if job.GetIndexWaitStartedTime() != 0 {
			progress = job.GetProgress()
		}
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
		return nil, merr.WrapErrParameterInvalidMsg("refresh job %d not found", jobID)
	}

	// Aggregate state and progress from tasks
	state, progress, err := m.refreshMeta.AggregateJobStateFromTasks(jobID)
	if err != nil {
		return nil, err
	}
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
		state, progress, err := m.refreshMeta.AggregateJobStateFromTasks(job.GetJobId())
		if err != nil {
			return nil, err
		}
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

// exploreExternalFiles runs one DataCoord-side Explore for the current planning
// attempt and returns its full file list and shared manifest path.
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
			return nil, "", merr.Wrap(err, "external source/spec failed revalidation")
		}
	}
	spec, err := externalspec.ParseExternalSpec(job.GetExternalSpec())
	if err != nil {
		return nil, "", merr.Wrap(err, "failed to parse external spec")
	}

	if m.collectionGetter == nil {
		return nil, "", merr.WrapErrServiceInternalMsg("collection getter is not configured")
	}
	collInfo, err := m.collectionGetter(ctx, job.GetCollectionId())
	if err != nil {
		return nil, "", err
	}
	if collInfo == nil {
		return nil, "", merr.WrapErrCollectionNotFound(job.GetCollectionId())
	}
	if spec.Format == externalspec.FormatMilvusTable {
		if err := validateMilvusTableRefreshSchema(job, collInfo.Schema); err != nil {
			return nil, "", err
		}
	}

	columns := packed.GetColumnNamesFromSchema(collInfo.Schema)
	storageConfig := createStorageConfig()
	extfs := packed.ExternalSpecContext{
		CollectionID:      job.GetCollectionId(),
		Source:            job.GetExternalSource(),
		Spec:              job.GetExternalSpec(),
		MilvusTablePKMode: packed.MilvusTablePrimaryKeyModeFromSchema(collInfo.Schema),
	}

	attemptID, err := m.allocator.AllocID(ctx)
	if err != nil {
		return nil, "", merr.Wrap(err, "allocate external refresh Explore attempt ID")
	}
	exploreBaseDir := exploreTempDirForAttempt(job.GetJobId(), attemptID)
	fileInfos, manifestPath, err := packed.ExploreFilesReturnManifestPath(
		columns,
		spec.Format,
		exploreBaseDir,
		job.GetExternalSource(),
		storageConfig,
		extfs,
	)
	if err != nil {
		// Preserve any typed error produced by packed. createTasksForJob owns the
		// final retry classification and turns an untyped error into a system
		// error there.
		return nil, "", merr.Wrap(err, "failed to explore files returning manifest path")
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

func validateMilvusTableRefreshSchema(job *datapb.ExternalCollectionRefreshJob, targetSchema *schemapb.CollectionSchema) error {
	metadata, err := packed.ReadMilvusTableSnapshotMetadata(
		job.GetExternalSource(),
		job.GetExternalSpec(),
		createStorageConfig(),
		packed.ExternalSpecContext{
			CollectionID: job.GetCollectionId(),
			Source:       job.GetExternalSource(),
			Spec:         job.GetExternalSpec(),
		},
	)
	if err != nil {
		return merr.Wrap(err, "read milvus-table snapshot metadata for schema validation")
	}
	sourceSchema := metadata.GetCollection().GetSchema()
	if sourceSchema == nil {
		return merr.Wrap(errMilvusTableRefreshSchemaInvalid, "missing collection schema")
	}
	if typeutil.IsExternalCollection(sourceSchema) {
		return merr.Wrap(errMilvusTableRefreshSchemaInvalid, "source snapshot is an external collection")
	}
	if err := typeutil.ValidateMilvusTableSchemaIdentity(targetSchema, sourceSchema, true); err != nil {
		return merr.Wrap(errMilvusTableRefreshSchemaInvalid,
			"source schema does not match target collection schema: "+err.Error())
	}
	return nil
}
