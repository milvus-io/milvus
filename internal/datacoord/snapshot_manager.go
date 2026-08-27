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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ============================================================================
// Type Definitions
// ============================================================================

// StartBroadcasterFunc creates a broadcaster for restore operations.
// Used by RestoreSnapshot to delegate broadcaster creation to the caller (Server).
type StartBroadcasterFunc func(ctx context.Context, collectionID int64, snapshotName string) (broadcaster.BroadcastAPI, error)

// StartRestoreLockFunc acquires the full restore lock set before any snapshot data
// is read or any target resources are created. The returned broadcaster is used
// only to hold the locks — Close() releases them without broadcasting.
//
// The lock set is:
//   - Shared lock on target database
//   - Exclusive lock on target collection name (reserves the name before creation)
//   - Exclusive lock on (sourceCollectionID, snapshotName) — serializes against
//     DropSnapshot of the same source snapshot
//
// This closes the TOCTOU window where a concurrent DropSnapshot could delete the
// source snapshot between Phase 1 (ReadSnapshotData) and Phase 4 (broadcast restore).
type StartRestoreLockFunc func(ctx context.Context, sourceCollectionID int64, snapshotName, targetDbName, targetCollectionName string) (broadcaster.BroadcastAPI, error)

type StartExternalRestoreLockFunc func(ctx context.Context, targetDbName, targetCollectionName string) (broadcaster.BroadcastAPI, error)

// RollbackFunc performs rollback on restore failure.
// Used by RestoreSnapshot to delegate collection cleanup to the caller (Server).
type RollbackFunc func(ctx context.Context, dbName, collectionName string) error

// ValidateResourcesFunc validates that all required resources exist.
// Used by RestoreSnapshot to validate snapshot, collection, partitions, and indexes.
type ValidateResourcesFunc func(ctx context.Context, collectionID int64, snapshotData *snapshotstorage.SnapshotData) error

const snapshotPinCleanupTimeout = 5 * time.Second

// ============================================================================
// Interface Definition
// ============================================================================

// SnapshotManager centralizes all snapshot-related business logic.
// It provides a unified interface for snapshot lifecycle management (create, drop, describe, list)
// and restore operations (restore, query restore state, list restore jobs).
//
// Design principles:
// - Encapsulates business logic from RPC handlers
// - Manages dependencies through constructor injection
// - Eliminates code duplication (state conversion, progress calculation)
// - Maintains separation from background services (Checker/Inspector)
//
// Related components (independent):
// - copySegmentChecker: Job state machine driver (runs as background service)
// - copySegmentInspector: Task scheduler (runs as background service)
// - snapshotMeta: Snapshot metadata storage (used as dependency)
// - copySegmentMeta: Restore job/task metadata storage (used as dependency)
type SnapshotManager interface {
	// Snapshot lifecycle management

	// CreateSnapshot creates a new snapshot for the specified collection.
	// It allocates a unique snapshot ID, generates snapshot data (segments, indexes, schema),
	// and persists the snapshot to storage (S3 + etcd).
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - collectionID: ID of the collection to snapshot
	//   - name: Unique name for the snapshot (globally unique)
	//   - description: Optional description of the snapshot
	//
	// Returns:
	//   - snapshotID: Allocated snapshot ID (0 on error)
	//   - error: If name already exists, allocation fails, or save fails
	CreateSnapshot(ctx context.Context, collectionID int64, name, description string, compactionProtectionSeconds int64, boundary *SnapshotBoundary, waitForSortedSegments bool) (int64, error)

	// DropSnapshot deletes an existing snapshot by name within a collection.
	// It removes the snapshot from memory cache, etcd, and S3 storage.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - collectionID: Collection ID to scope the snapshot lookup
	//   - name: Name of the snapshot to delete (unique within collection)
	//
	// Returns:
	//   - error: If snapshot not found or deletion fails
	DropSnapshot(ctx context.Context, collectionID int64, name string) error

	// DropSnapshotsByCollection deletes all snapshots for a collection.
	// Used during drop collection cascade cleanup.
	DropSnapshotsByCollection(ctx context.Context, collectionID int64) error

	// GetSnapshot retrieves basic snapshot metadata by name within a collection.
	// This is a lightweight operation that only reads from memory cache.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - collectionID: Collection ID to scope the snapshot lookup
	//   - name: Name of the snapshot (unique within collection)
	//
	// Returns:
	//   - snapshotInfo: Basic snapshot metadata (id, name, collection_id, etc.)
	//   - error: If snapshot not found
	GetSnapshot(ctx context.Context, collectionID int64, name string) (*datapb.SnapshotInfo, error)

	// DescribeSnapshot retrieves detailed information about a snapshot within a collection.
	// It reads the complete snapshot data from S3, including segments, indexes, and schema.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - collectionID: Collection ID to scope the snapshot lookup
	//   - name: Name of the snapshot to describe (unique within collection)
	//
	// Returns:
	//   - snapshotData: Complete snapshot data with collection info and index info
	//   - error: If snapshot not found or read fails
	DescribeSnapshot(ctx context.Context, collectionID int64, name string) (*snapshotstorage.SnapshotData, error)

	// ListSnapshots returns a list of snapshot names for the specified collection/partition.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - collectionID: Filter by collection ID (0 = all collections)
	//   - partitionID: Filter by partition ID (0 = all partitions)
	//
	// Returns:
	//   - snapshots: List of snapshot names
	//   - error: If listing fails
	ListSnapshots(ctx context.Context, collectionID, partitionID, dbID int64) ([]string, error)

	// Restore operations

	// RestoreSnapshot orchestrates the complete snapshot restoration process.
	// It reads snapshot data, creates collection/partitions/indexes, acquires a broadcast lock,
	// validates resources under the lock, and broadcasts the restore message.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - collectionID: Source collection ID for per-collection snapshot name lookup
	//   - snapshotName: Name of the snapshot to restore (unique within collection)
	//   - targetCollectionName: Name for the restored collection
	//   - targetDbName: Database name for the restored collection
	//   - startRestoreLock: Function to acquire the Phase 0 restore lock set
	//   - startBroadcaster: Function to start a broadcaster for DDL operations
	//   - rollback: Function to rollback on failure (drops collection)
	//   - validateResources: Function to validate that all resources exist
	//
	// Returns:
	//   - jobID: ID of the restore job (can be used for progress tracking)
	//   - error: If any step fails
	RestoreSnapshot(
		ctx context.Context,
		sourceCollectionID int64,
		snapshotName string,
		targetCollectionName string,
		targetDbName string,
		startRestoreLock StartRestoreLockFunc,
		startBroadcaster StartBroadcasterFunc,
		rollback RollbackFunc,
		validateResources ValidateResourcesFunc,
	) (int64, error)

	RestoreExternalSnapshot(
		ctx context.Context,
		snapshotS3Location string,
		targetCollectionName string,
		targetDbName string,
		externalSpec string,
		startExternalRestoreLock StartExternalRestoreLockFunc,
		startBroadcaster StartBroadcasterFunc,
		rollback RollbackFunc,
		validateResources ValidateResourcesFunc,
	) (int64, error)

	ExportSnapshot(
		ctx context.Context,
		collectionID int64,
		snapshotName string,
		dbName string,
		collectionName string,
		targetS3Path string,
		externalSpec string,
	) (int64, error)

	GetExportSnapshotState(jobID int64) (*datapb.ExportSnapshotJobInfo, error)

	// RestoreCollection creates a new collection and its user partitions based on snapshot data.
	// It marshals the schema, sets preserve field IDs property, calls RootCoord to create collection,
	// then creates user-defined partitions (filtering out default and partition-key partitions).
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - snapshotData: Snapshot data containing collection schema and partition info
	//   - targetCollectionName: Name for the new collection
	//   - targetDbName: Database name for the new collection
	//
	// Returns:
	//   - collectionID: ID of the created collection
	//   - error: If creation fails
	RestoreCollection(ctx context.Context, snapshotData *snapshotstorage.SnapshotData, targetCollectionName, targetDbName string) (int64, error)

	// RestoreIndexes restores indexes from snapshot data by broadcasting CreateIndex messages.
	// This method bypasses CreateIndex validation (e.g., ParseAndVerifyNestedPath) because
	// snapshot data already contains properly formatted index parameters.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - snapshotData: Snapshot data containing index information
	//   - collectionID: ID of the target collection
	//   - startBroadcaster: Function to create a new broadcaster for each index
	//     (each broadcaster can only be used once due to resource key lock consumption)
	//
	// Returns:
	//   - error: If any index creation fails
	RestoreIndexes(ctx context.Context, snapshotData *snapshotstorage.SnapshotData, collectionID int64, startBroadcaster StartBroadcasterFunc, snapshotName string) error

	// RestoreData handles the data restoration phase of snapshot restore.
	// It builds partition/channel mappings and creates copy segment jobs.
	// Collection/partition creation and index restore should be handled by caller (services.go).
	//
	// Process flow:
	//  1. Check if job already exists (idempotency)
	//  2. Build partition mapping
	//  3. Build channel mapping
	//  4. Create copy segment job for background execution
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - sourceCollectionID: Source collection ID for per-collection snapshot name lookup
	//   - snapshotName: Name of the snapshot to restore (unique within source collection)
	//   - collectionID: ID of the target collection (already created)
	//   - jobID: Pre-allocated job ID for idempotency (from WAL message)
	//
	// Returns:
	//   - jobID: The restore job ID (same as input if job created, or existing job ID)
	//   - error: If mapping fails or job creation fails
	RestoreData(ctx context.Context, sourceCollectionID int64, snapshotName string, collectionID int64, jobID int64, pinID int64) (int64, error)

	RestoreExternalData(
		ctx context.Context,
		sourceCollectionID int64,
		snapshotName string,
		snapshotS3Location string,
		collectionID int64,
		jobID int64,
		externalSpec string,
		snapshotFingerprint string,
	) (int64, error)

	// Restore state query

	// ReadSnapshotData reads complete snapshot data from storage.
	// This is used by services.go to get snapshot data before calling RestoreData.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - collectionID: Collection ID to scope the snapshot lookup
	//   - snapshotName: Name of the snapshot to read (unique within collection)
	//
	// Returns:
	//   - snapshotData: Complete snapshot data including segments, indexes, schema
	//   - error: If snapshot not found or read fails
	ReadSnapshotData(ctx context.Context, collectionID int64, snapshotName string) (*snapshotstorage.SnapshotData, error)

	// GetRestoreState retrieves the current state of a restore job.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - jobID: ID of the restore job
	//
	// Returns:
	//   - restoreInfo: Job information including state, progress, and time cost
	//   - error: If job not found
	GetRestoreState(ctx context.Context, jobID int64) (*datapb.RestoreSnapshotInfo, error)

	// ListRestoreJobs returns a list of all restore jobs, optionally filtered by collection ID or database ID.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - collectionIDFilter: Filter by collection ID (0 = all jobs)
	//   - dbID: Filter by database ID (0 = no filter)
	//
	// Returns:
	//   - restoreInfos: List of restore job information
	//   - error: If listing fails
	ListRestoreJobs(ctx context.Context, collectionIDFilter, dbID int64) ([]*datapb.RestoreSnapshotInfo, error)

	// PinSnapshotData pins a snapshot to prevent GC from deleting its data files.
	// Returns a unique pin ID for later Unpin. ttlSeconds=0 means no expiry.
	PinSnapshotData(ctx context.Context, collectionID int64, name string, ttlSeconds int64) (int64, error)

	// UnpinSnapshotData removes a pin by ID, allowing GC to reclaim data
	// when all pins are removed.
	UnpinSnapshotData(ctx context.Context, pinID int64) error

	// HasActivePins reports whether the named snapshot has any non-expired pins.
	// Used by service-layer DropSnapshot as a pre-broadcast pin check to fail
	// fast on pinned snapshots before acquiring the broadcast lock.
	HasActivePins(ctx context.Context, collectionID int64, name string) (bool, error)
}

// ============================================================================
// Implementation: Struct and Constructor
// ============================================================================

// snapshotManager implements the SnapshotManager interface.
type snapshotManager struct {
	// Core dependencies
	meta            *meta           // Segment metadata management
	snapshotMeta    *snapshotMeta   // Snapshot metadata management
	copySegmentMeta CopySegmentMeta // Restore job/task metadata management

	// Resource allocator (unified for all ID allocations)
	allocator allocator.Allocator // For snapshot ID, job ID, and segment ID allocation

	// External services
	handler Handler       // For generating snapshot data
	broker  broker.Broker // For querying partition information

	// Index engine version manager for compatibility checks during restore
	indexEngineVersionManager IndexEngineVersionManager

	// Helper closures
	getChannelsByCollectionID func(context.Context, int64) ([]RWChannel, error) // For channel mapping

	// Concurrency control
	//
	// createSnapshotLock serializes CreateSnapshot per collection, closing the
	// TOCTOU between the name-uniqueness check and SaveSnapshot, and keeping the
	// staging/pending flags -- which are plain set membership, not refcounts --
	// from interleaving with themselves.
	//
	// Per collection rather than process-wide: the lock is held across
	// waitForVisibleBoundary, which can run for the whole life of a snapshot,
	// and a global lock made one collection with stalled sort compaction starve
	// snapshot creation on every other collection. The keyspace it actually
	// protects is (collectionID, name), so collection scope is already wider
	// than required. It is defense in depth either way -- the broadcaster's
	// exclusive collection-name resource key already admits at most one
	// in-flight CreateSnapshot callback per collection.
	createSnapshotLockOnce sync.Once
	createSnapshotLock     *lock.KeyLock[int64]

	// captureSlots bounds how many snapshots may be in their GenSnapshot ->
	// SaveSnapshot window at once. GenSnapshot clones every in-boundary
	// SegmentInfo and expands per-segment binlog and index paths, all held live
	// until the save returns -- tens of MB for a large collection. The old
	// process-wide createSnapshotMu capped that at one by accident; making the
	// lock per-collection removes the cap, and ack callbacks are spawned one
	// goroutine per task with no parallelism limit, so a bulk "snapshot every
	// collection" could otherwise multiply peak memory without bound. The wait
	// is deliberately outside this: it is the long part, and holding a slot
	// through it would rebuild the head-of-line blocking just removed.
	captureSlotsOnce sync.Once
	captureSlots     chan struct{}

	// Serialize external restores by target name without holding RootCoord's DDL lock.
	externalRestoreTargetLockOnce sync.Once
	externalRestoreTargetLock     *lock.KeyLock[restoreTarget]
	exportManager                 *snapshotExportManager
}

type restoreTarget struct {
	dbName         string
	collectionName string
}

type snapshotExportTarget struct {
	bucket string
	root   string
}

// NewSnapshotManager creates a new SnapshotManager instance.
//
// Parameters:
//   - meta: Segment metadata manager
//   - snapshotMeta: Snapshot metadata manager (includes pin record management)
//   - copySegmentMeta: Copy segment job/task metadata manager
//   - allocator: Allocator for all ID allocations (snapshot, job, segment)
//   - handler: Handler for generating snapshot data
//   - broker: Broker for coordinator communication
//   - getChannelsFunc: Function to get channels by collection ID
//
// Returns:
//   - SnapshotManager: New snapshot manager instance
func NewSnapshotManager(
	meta *meta,
	snapshotMeta *snapshotMeta,
	copySegmentMeta CopySegmentMeta,
	allocator allocator.Allocator,
	handler Handler,
	broker broker.Broker,
	getChannelsFunc func(context.Context, int64) ([]RWChannel, error),
	ievm IndexEngineVersionManager,
) *snapshotManager {
	return &snapshotManager{
		meta:                      meta,
		snapshotMeta:              snapshotMeta,
		copySegmentMeta:           copySegmentMeta,
		allocator:                 allocator,
		handler:                   handler,
		broker:                    broker,
		getChannelsByCollectionID: getChannelsFunc,
		indexEngineVersionManager: ievm,
	}
}

// ============================================================================
// Snapshot Lifecycle Management
// ============================================================================

// CreateSnapshot creates a new snapshot for the specified collection.
func (sm *snapshotManager) CreateSnapshot(
	ctx context.Context,
	collectionID int64,
	name, description string,
	compactionProtectionSeconds int64,
	boundary *SnapshotBoundary,
	waitForSortedSegments bool,
) (int64, error) {
	// Lock to prevent TOCTOU race on snapshot name uniqueness check
	defer sm.lockCreateSnapshot(collectionID)()

	mlog.Info(context.TODO(), "create snapshot request received",
		mlog.String("description", description),
		mlog.Int64("compactionProtectionSeconds", compactionProtectionSeconds))

	// Already created: report success rather than an error (protected by
	// createSnapshotLock).
	//
	// This runs as an ack callback, whose contract is at-least-once, so a second
	// invocation for the same message is normal rather than a caller mistake. It
	// is reachable in production: doAckCallback saves the snapshot inside
	// callMessageAckCallbackUntilDone and only then calls MarkAckCallbackDone,
	// and MarkAckCallbackDone panics outright if its etcd write fails -- so one
	// etcd blip in that window deterministically replays the callback against a
	// snapshot that already exists. Returning an error there would be retried
	// forever, and since the collection's exclusive DDL resource key is released
	// only by MarkAckCallbackDone on success, every later DDL on the collection
	// would block permanently.
	//
	// A genuine duplicate request cannot reach here: Server.CreateSnapshot
	// rejects an existing name twice, the second time while holding the
	// exclusive snapshot-name resource key.
	if existing, err := sm.snapshotMeta.GetSnapshot(ctx, collectionID, name); err == nil {
		mlog.Info(context.TODO(), "snapshot already exists, treating this callback as a replay",
			mlog.Int64("snapshotID", existing.GetId()))
		return existing.GetId(), nil
	}

	// Freeze segment boundaries before anything else. The boundary was cut when
	// the CreateSnapshot message was appended; from here until the segment list is
	// captured, a compaction that merges across it would produce a segment that
	// looks like it belongs to the snapshot while carrying rows written after it.
	// Sort compaction is deliberately still allowed -- it is what the wait below
	// is waiting for, and it rewrites one segment into one segment, so it cannot
	// move a boundary.
	//
	// Staging is deliberately NOT cleared when this call fails. It is cleared
	// only once the snapshot is saved, below. This runs inside an ack callback
	// the scheduler retries forever, and the boundary stays cut across every
	// retry -- so its protection has to as well. Clearing it per attempt left
	// the collection unfrozen for the whole backoff (up to 10s out of every
	// wait attempt), which is ample for a straddling mix compaction to be
	// planned, validated and committed.
	//
	// This cannot strand a collection: the flag is in-memory only, nothing else
	// clears it, and the retry loop can be abandoned only by canceling the ack
	// scheduler's context -- which happens solely on process shutdown, the same
	// shutdown that drops the flag. On restart the callback is replayed from
	// persisted state and re-establishes staging from scratch.
	sm.snapshotMeta.SetSnapshotStaging(collectionID)

	// Wait for the boundary to be complete, and -- only if asked -- for every
	// segment inside it to be published sorted.
	//
	// Capturing an unsorted segment is not a loss of rows: it is served from the
	// growing path, so the capture gets its binlogs and manifest either way. What
	// it costs is an index, and correctness against a schema-evolution backfill,
	// which skips invisible segments -- so their manifests never gain an added
	// column. That is why a backfill asks for the wait and an ordinary snapshot
	// does not.
	//
	// Without the wait, a sort may commit between here and SetSnapshotPending
	// below. That is safe only because completeSortCompactionMutation publishes
	// the output of a stream-flushed input VISIBLE (it inherits invisibility only
	// from a CreatedByCompaction input), so the replacement is captured normally.
	// Were that to change, the retired input and the invisible output would both
	// be filtered out and the rows would vanish from the capture.
	//
	// This runs before SetSnapshotPending, and the order is not cosmetic: pending
	// blocks sort compaction too, so waiting under it would be waiting for tasks
	// this call has itself forbidden.
	if err := sm.waitForBoundary(ctx, collectionID, boundary, waitForSortedSegments); err != nil {
		return 0, err
	}

	// Block compaction commit for this collection during snapshot creation.
	// This MUST be unconditional (not gated on compactionProtectionSeconds): even when
	// the user requests zero long-term protection, the snapshot must still be atomic
	// within the GenSnapshot → SaveSnapshot window, otherwise concurrent compaction
	// could drop segments that the in-flight snapshot is about to reference, leaving
	// the freshly-created snapshot immediately broken.
	//
	// Pending is layered on top of staging rather than replacing it, so there is
	// no instant in which sort may commit while the segment list is being
	// captured: a sorted replacement landing in that gap would swap out a
	// segment the snapshot has already decided to reference. Holding both is
	// equivalent to holding pending alone -- every consumer ORs the two sets --
	// and it keeps staging continuous if this attempt fails past this point.
	//
	// Unlike staging, pending IS released per attempt. It also blocks sort
	// compaction, so leaving it set across a retry would forbid exactly the
	// tasks the next attempt's wait is waiting on.
	// Bound how many collections hold a captured segment list in memory at once.
	//
	// Queue for the slot BEFORE taking pending, never after. Pending blocks every
	// non-L0 compaction on this collection, sort included, and waiting for a slot
	// is waiting on unrelated collections -- so setting pending first would freeze
	// this collection's compaction for as long as someone else's capture runs, and
	// with N collections queueing behind a fixed number of slots that stacks into
	// exactly the head-of-line coupling the per-collection create lock removed.
	//
	// The wait is also outside this, deliberately: it is the long part, and
	// holding a slot through it would serialize snapshots cluster-wide again.
	releaseCaptureSlot, err := sm.acquireCaptureSlot(ctx)
	if err != nil {
		return 0, err
	}
	defer releaseCaptureSlot()

	sm.snapshotMeta.SetSnapshotPending(collectionID)
	defer sm.snapshotMeta.ClearSnapshotPending(collectionID)

	// Allocate snapshot ID
	snapshotID, err := sm.allocator.AllocID(ctx)
	if err != nil {
		mlog.Error(context.TODO(), "failed to allocate snapshot ID", mlog.Err(err))
		return 0, err
	}

	// Generate snapshot data at the boundary the CreateSnapshot message cut.
	snapshotData, err := sm.handler.GenSnapshot(ctx, collectionID, boundary)
	if err != nil {
		mlog.Error(context.TODO(), "failed to generate snapshot", mlog.Err(err))
		return 0, err
	}

	// Set snapshot metadata
	snapshotData.SnapshotInfo.Id = snapshotID
	snapshotData.SnapshotInfo.Name = name
	snapshotData.SnapshotInfo.Description = description
	// Recorded so a consumer can tell a backfill-ready cut from an ordinary one:
	// without the wait the capture may hold segments whose manifests predate a
	// concurrent schema-evolution backfill.
	snapshotData.SnapshotInfo.WaitedForSortedSegments = waitForSortedSegments

	// Set compaction protection if requested
	if compactionProtectionSeconds > 0 {
		snapshotData.SnapshotInfo.CompactionExpireTime = uint64(time.Now().Unix()) + uint64(compactionProtectionSeconds)
	}

	// Save to storage
	if err := sm.snapshotMeta.SaveSnapshot(ctx, snapshotData); err != nil {
		mlog.Error(context.TODO(), "failed to save snapshot", mlog.Err(err))
		return 0, err
	}

	// The boundary no longer needs freezing: the snapshot references a concrete
	// segment list now, and SaveSnapshot's registerSnapshotProtection has taken
	// over guarding those segments. This is the only place staging comes off --
	// see the comment where it goes on.
	sm.snapshotMeta.ClearSnapshotStaging(collectionID)

	mlog.Info(context.TODO(), "snapshot created successfully", mlog.Int64("snapshotID", snapshotID))
	return snapshotID, nil
}

// channelsBehindBoundary returns the boundary's channels whose checkpoint has not
// reached it yet.
//
// A channel checkpoint is DataCoord's own statement that everything before that
// position is persisted and accounted for. Until it passes the boundary, the
// segment set inside the boundary is not merely unsorted, it is incomplete:
// DataCoord may not have been told about a segment the fence sealed -- growing
// segments are not visible here as soon as they are on the streaming node, which
// is the same gap GetFlushState documents. Asking "is everything sorted" against
// a set that is still filling in answers about the wrong set.
//
// This is also why the check is a timestamp rather than a segment list: a list
// cannot express "and nothing else has arrived yet".
func (sm *snapshotManager) channelsBehindBoundary(boundary *SnapshotBoundary) []string {
	behind := make([]string, 0, len(boundary.SeekPositions))
	for _, position := range boundary.SeekPositions {
		checkpoint := sm.meta.GetChannelCheckpoint(position.GetChannelName())
		if checkpoint == nil || checkpoint.GetTimestamp() < position.GetTimestamp() {
			behind = append(behind, position.GetChannelName())
		}
	}
	return behind
}

// segmentsAwaitingVisibility returns the segments inside the boundary that the
// snapshot is not yet allowed to capture.
//
// The predicate is IsInvisible, not unsortedness. Invisibility is the flag that
// actually decides whether a segment is part of the collection a reader sees:
// handler.go routes an invisible segment into UnflushedSegmentIds, so
// GetRecoveryInfoV2 leaves it out of the sealed load set and a querynode picks
// it up on the growing path with no index; schema-bump backfill refuses it
// outright (isSchemaBumpDataSegment, and a hard reject in
// CompleteCompactionMutation); and the DDL schema-consistency gate counts only
// visible segments. Capturing one puts a segment in the snapshot that is
// unindexed and un-backfilled relative to the collection it claims to copy.
//
// Unsortedness only tracks that while sort compaction is on, because
// flushFlushingSegment stamps IsInvisible exactly when enableSortCompaction()
// holds. With sort off, a flushed segment is unsorted AND visible -- indexed,
// sealed-loaded, backfill-eligible, its manifest exactly what a reader sees --
// and there is nothing about it to wait for. Waiting on unsortedness there
// waits forever for a state change that is not coming and was never needed.
// !CreatedByCompaction narrows it to segments with no other representation.
// A compaction output that is still invisible has its inputs alive and serving
// -- clustering does not retire them until it publishes the output -- so the
// capture takes those inputs instead and there is nothing to wait for;
// dropSupersededByLineage picks the generation. Waiting on them would mean
// waiting on the clustering output's index build, which is minutes at best and
// never, if that build fails permanently: markResultSegmentsVisible is reached
// only once every index reports Finished, and the staging freeze this wait
// holds is not released on failure.
//
// A segment still on its way to Flushed is not in this set. That is covered by
// channelsBehindBoundary in front of this call: until every channel checkpoint
// has passed the boundary the segment list is still filling in, so an empty
// answer here would be about the wrong set.
//
// The predicate deliberately differs from canTriggerSortCompaction in one way:
// it does not exclude segments that are already compacting. A segment with a
// sort task in flight has not become visible yet, and the point of the wait is
// to stay until it has. It also needs no segment-id bookkeeping -- a sort
// replaces its input with a new id, and the replacement leaves this set on its
// own once it is published visible, while the input leaves it as Dropped.
func (sm *snapshotManager) segmentsAwaitingVisibility(ctx context.Context, collectionID int64, boundary *SnapshotBoundary) ([]int64, error) {
	// External collections never get sort compaction: every compaction policy
	// (single/clustering/forcemerge/storage-version) skips IsExternal() collections
	// outright, so nothing would ever publish their segments visible. Waiting on a
	// transition that structurally cannot happen would hang CreateSnapshot forever --
	// there is nothing to wait for, so report the set as already empty.
	if collection := sm.meta.GetCollection(collectionID); collection != nil && collection.IsExternal() {
		return nil, nil
	}

	candidates := sm.meta.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(info *SegmentInfo) bool {
		return info.GetState() == commonpb.SegmentState_Flushed &&
			info.GetLevel() != datapb.SegmentLevel_L0 &&
			info.GetIsInvisible() &&
			!info.GetCreatedByCompaction() &&
			!info.GetIsImporting()
	}))

	awaiting := make([]int64, 0, len(candidates))
	for _, info := range candidates {
		seekTs, ok := boundary.SeekTs(info.GetInsertChannel())
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg(
				"missing snapshot channel seek position for segment channel %s", info.GetInsertChannel())
		}
		// Same comparison GenSnapshot uses to decide membership. The two must not
		// drift: waiting on a set that is not the set being captured guarantees
		// nothing about what ends up in the snapshot.
		if segmentEffectiveTs(info.SegmentInfo) >= seekTs {
			continue
		}
		// And the same emptiness test. A segment with nothing in it is not captured
		// either way, so blocking on one that never gets data would hang the
		// snapshot on a segment it does not want.
		hasData, err := segmentHasSnapshotData(info)
		if err != nil {
			return nil, err
		}
		if hasData {
			awaiting = append(awaiting, info.GetID())
		}
	}
	return awaiting, nil
}

// segmentsWouldAwaitVisibility returns the collection's segments that the wait
// would block on if a boundary were cut right now.
//
// Unlike segmentsAwaitingVisibility it takes no boundary, because it runs before
// the CreateSnapshot message is appended and there is no boundary yet. It has to
// predict rather than observe for segments that are not flushed: the fence seals
// them at the boundary, and flushFlushingSegment publishes a sealed segment
// invisible exactly when enableSortCompaction() holds. So with sort on they will
// join the wait set, and with sort off they will flush straight to visible and
// never enter it.
//
// It mirrors segmentsAwaitingVisibility's !CreatedByCompaction exactly. Without
// that, an in-flight clustering compaction would make this refuse the snapshot
// with "stranded invisible ... can never be published" whenever sort compaction
// is off -- untrue, since the index build publishes them, and the wait does not
// block on them anyway.
func segmentsWouldAwaitVisibility(ctx context.Context, m *meta, collectionID int64) ([]int64, error) {
	sortWillRun := enableSortCompaction()
	candidates := m.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(info *SegmentInfo) bool {
		if info.GetLevel() == datapb.SegmentLevel_L0 || info.GetIsImporting() {
			return false
		}
		switch info.GetState() {
		case commonpb.SegmentState_Flushed:
			return info.GetIsInvisible() && !info.GetCreatedByCompaction()
		case commonpb.SegmentState_Growing, commonpb.SegmentState_Sealed, commonpb.SegmentState_Flushing:
			return sortWillRun
		default:
			return false
		}
	}))

	needing := make([]int64, 0, len(candidates))
	for _, info := range candidates {
		if info.GetState() != commonpb.SegmentState_Flushed {
			// Not yet flushed, so ask the cheap question only: a growing segment
			// has no binlogs until it flushes, and its row count is the evidence
			// it will become one worth waiting on. Deliberately NOT
			// segmentHasSnapshotData -- that parses the StorageV3 manifest path
			// and errors on a malformed one, which would abort this whole check
			// (and so reject the snapshot) over a segment neither
			// segmentsAwaitingVisibility nor GenSnapshot would ever have inspected.
			if info.GetNumOfRows() > 0 || len(info.GetBinlogs()) > 0 {
				needing = append(needing, info.GetID())
			}
			continue
		}
		// Flushed: the same emptiness test segmentsAwaitingVisibility applies, so
		// the pre-check and the wait agree on which segments count.
		hasData, err := segmentHasSnapshotData(info)
		if err != nil {
			return nil, err
		}
		if hasData {
			needing = append(needing, info.GetID())
		}
	}
	return needing, nil
}

// checkSnapshotVisibilityReachable rejects a CreateSnapshot whose wait could
// never finish, before the message is appended to the WAL.
//
// The placement is the whole point. Broadcast returns once the message is
// appended, not once the ack callback runs, so by the time the callback
// discovers it cannot proceed the client has already been told the call
// succeeded. The callback is then retried forever --
// callMessageAckCallbackUntilDone sets MaxElapsedTime=0 and retries every error
// -- and the collection's exclusive DDL resource key is released only by
// MarkAckCallbackDone on success. A condition the callback can never satisfy
// therefore does not fail the request: it silently wedges every later DDL on
// that collection for the life of the process. Both conditions below are exactly
// that, which is why they are caught here rather than in the wait.
//
// It refuses only what is genuinely unresolvable. A cluster running with sort
// compaction switched off is NOT refused: its segments flush straight to visible
// and the snapshot has nothing to wait for, so it is allowed through.
func checkSnapshotVisibilityReachable(ctx context.Context, m *meta, collectionID int64) error {
	// External collections never wait: no compaction policy touches them, so
	// segmentsAwaitingVisibility reports their set as empty by construction.
	if collection := m.GetCollection(collectionID); collection != nil && collection.IsExternal() {
		return nil
	}

	needing, err := segmentsWouldAwaitVisibility(ctx, m, collectionID)
	if err != nil {
		return err
	}
	if len(needing) == 0 {
		// Nothing to wait for, so neither stall below applies. This is the normal
		// answer on a cluster with sort compaction off: everything flushes
		// visible, and the snapshot proceeds immediately.
		return nil
	}

	// Reaching here with sort off means the set is entirely already-invisible
	// segments -- segmentsWouldAwaitVisibility excludes not-yet-flushed ones in
	// that configuration. Those are stranded: nothing clears IsInvisible except a
	// sort or clustering completion, and with the subsystem off neither will run.
	// EnableCompaction gates startCompaction(), the only caller of
	// compactionTriggerManager.Start() and compactionInspector.start(), so no
	// task is created and already-queued ones stop being scheduled; it is also
	// refreshable:"false", so the operator cannot undo it without restarting
	// DataCoord. Refuse rather than wait for something nothing will deliver.
	if !enableSortCompaction() {
		return merr.WrapErrServiceUnavailableMsg(
			"snapshot for collection %d has %d segment(s) stranded invisible by a previous sort compaction run, and sort "+
				"compaction is now off (dataCoord.enableCompaction=%t, dataCoord.sortCompaction.enable=%t) so they can "+
				"never be published; re-enable it to let them finish",
			collectionID, len(needing),
			Params.DataCoordCfg.EnableCompaction.GetAsBool(),
			Params.DataCoordCfg.EnableSortCompaction.GetAsBool())
	}

	// A segment pinned by an older snapshot's compaction protection is skipped by
	// both sort-triggering paths (triggerSegmentSortCompaction and
	// triggerSortCompaction), while segmentsAwaitingVisibility still waits for it
	// -- the two predicates are asymmetric, and the wait is the wider one.
	// Snapshots written before this branch existed could reference invisible
	// segments, since GenSnapshot filtered only on Dropped and IsImporting, so
	// this is reachable on any upgraded cluster. The wait would then last until
	// that protection lapses: up to
	// dataCoord.snapshot.maxCompactionProtectionSeconds, 7 days by default.
	protected := make([]int64, 0, len(needing))
	for _, segmentID := range needing {
		if m.isSegmentCompactionProtected(segmentID) {
			protected = append(protected, segmentID)
		}
	}
	if len(protected) > 0 {
		return merr.WrapErrServiceUnavailableMsg(
			"snapshot for collection %d cannot proceed: %d segment(s) are still invisible and need sort compaction, but "+
				"are pinned by an existing snapshot's compaction protection, which blocks sort until it expires "+
				"(e.g. segments %v); retry after it lapses, or drop the snapshot holding it",
			collectionID, len(protected), protected[:min(len(protected), 5)])
	}
	return nil
}

// waitForBoundary blocks until the boundary is complete, and -- only when
// waitForSorted is set -- until every segment inside it is one the collection's
// readers can see.
//
// The two halves are not equally optional. Completeness is mandatory: until
// every channel checkpoint has passed the boundary, DataCoord has not been told
// about the segments the fence just sealed, so capturing then would silently
// miss them. Visibility is the caller's choice, because an unsorted segment is
// served anyway and so is captured either way; see CreateSnapshot for what
// skipping it costs.
//
// When waitForSorted is set, the wait is on visibility itself, not on
// sortedness. See segmentsAwaitingVisibility for why the distinction matters:
// with sort compaction off, segments flush straight to visible and this returns
// on the first poll rather than waiting for a sort that is not coming.
//
// The set is closed: the CreateSnapshot message fenced its collection's growing
// segments at this boundary, so anything flushed afterwards starts after it and
// can never enter. The wait therefore terminates on its own as the set drains
// -- it is not racing ingestion.
//
// The per-attempt cap below does NOT give the collection's resource-key lock
// back. This runs inside a DDL ack callback, and the broadcaster's ack-callback
// scheduler (streamingcoord/server/broadcaster/ack_callback_scheduler.go,
// callMessageAckCallbackUntilDone) retries any error the callback returns in an
// inner loop that never returns to its caller -- so the lock guard is only
// released by MarkAckCallbackDone on success, never on an intermediate error.
// The cap's real purpose is bounding a single polling attempt's log/CPU
// footprint and refreshing "waited so far" visibility on each retry, not
// yielding the lock: an unbounded wait here would hold it for exactly as long
// either way, since the outer retry has nowhere else to hand it off to. The
// message is already in the WAL, so the snapshot is created eventually either
// way, once the set can actually drain -- see the enableSortCompaction check
// below for the one case where it structurally cannot.
func (sm *snapshotManager) waitForBoundary(ctx context.Context, collectionID int64, boundary *SnapshotBoundary, waitForSorted bool) error {
	// The budget is a deadline, so express it as one: the same select then covers
	// both running out of budget and DataCoord shutting down, and both want the
	// same thing -- give the lock back and let the scheduler come again.
	waitCtx, cancel := context.WithTimeout(ctx, Params.DataCoordCfg.SnapshotSortWaitTimeout.GetAsDuration(time.Second))
	defer cancel()

	ticker := time.NewTicker(Params.DataCoordCfg.SnapshotSortWaitPollInterval.GetAsDuration(time.Second))
	defer ticker.Stop()

	start := time.Now()
	for {
		// Completeness before cleanliness. Until every channel checkpoint has
		// passed the boundary, the segments inside it are still arriving, and
		// asking whether they are all visible answers about a set that is not yet
		// the one being captured -- typically an empty one, which reads as "done".
		behind := sm.channelsBehindBoundary(boundary)
		var awaiting []int64
		if len(behind) == 0 {
			// Completeness is mandatory; visibility is not. Skipping the
			// checkpoint gate above would capture a set DataCoord has not
			// finished hearing about -- silent loss, not a trade-off. Skipping
			// the visibility wait only means the capture may include segments
			// that are still unindexed, whose rows are served anyway.
			if !waitForSorted {
				mlog.Info(ctx, "snapshot boundary complete, not waiting for sorted segments",
					mlog.FieldCollectionID(collectionID),
					mlog.Uint64("snapshotTs", boundary.SnapshotTs),
					mlog.Duration("waited", time.Since(start)))
				return nil
			}
			var err error
			if awaiting, err = sm.segmentsAwaitingVisibility(ctx, collectionID, boundary); err != nil {
				return err
			}
			if len(awaiting) == 0 {
				mlog.Info(ctx, "snapshot boundary complete and fully visible",
					mlog.FieldCollectionID(collectionID),
					mlog.Uint64("snapshotTs", boundary.SnapshotTs),
					mlog.Duration("waited", time.Since(start)))
				return nil
			}
			// Backstop for the same condition checkSnapshotVisibilityReachable
			// refuses before the broadcast: the switches can be flipped after
			// the message is already in the WAL, and this is the only place
			// that would otherwise notice. Anything still invisible once sort
			// compaction is off is stranded -- only a sort or clustering
			// completion clears the flag, and neither will run. Checked fresh
			// on every attempt, so re-enabling lets a later retry proceed.
			//
			// This still returns a retryable error rather than abandoning the
			// snapshot: the message is in the WAL, so the snapshot has to exist
			// eventually. It cannot release the collection's resource-key lock
			// -- see the function doc -- which is exactly why the pre-broadcast
			// check matters more than this one.
			if !enableSortCompaction() {
				mlog.Warn(ctx, "snapshot cannot proceed: segments are stranded invisible with sort compaction off",
					mlog.FieldCollectionID(collectionID),
					mlog.Bool("enableCompaction", Params.DataCoordCfg.EnableCompaction.GetAsBool()),
					mlog.Bool("enableSortCompaction", Params.DataCoordCfg.EnableSortCompaction.GetAsBool()),
					mlog.Int64s("awaitingVisibility", awaiting))
				return merr.WrapErrServiceUnavailableMsg(
					"snapshot for collection %d has %d segment(s) stranded invisible, and sort compaction is off "+
						"(dataCoord.enableCompaction=%t, dataCoord.sortCompaction.enable=%t) so nothing will publish them",
					collectionID, len(awaiting),
					Params.DataCoordCfg.EnableCompaction.GetAsBool(),
					Params.DataCoordCfg.EnableSortCompaction.GetAsBool())
			}
		}

		select {
		case <-waitCtx.Done():
			// This attempt's budget is spent, not the snapshot's: the ack
			// callback that runs this holds the collection's resource-key lock
			// for as long as this call keeps returning an error, regardless of
			// whether that error comes from here or from looping past this
			// point -- see the function doc. Returning still matters for
			// bounding one attempt's log/CPU footprint and refreshing the
			// "waited" duration on the next one.
			mlog.Info(ctx, "snapshot still waiting for its boundary, will retry",
				mlog.FieldCollectionID(collectionID),
				mlog.Uint64("snapshotTs", boundary.SnapshotTs),
				mlog.Duration("waited", time.Since(start)),
				mlog.Strings("channelsBehindBoundary", behind),
				mlog.Int64s("awaitingVisibility", awaiting))
			return merr.WrapErrServiceUnavailableMsg(
				"snapshot for collection %d is waiting for %d channel(s) to reach its boundary and %d segment(s) to become visible",
				collectionID, len(behind), len(awaiting))
		case <-ticker.C:
			mlog.RatedInfo(ctx, 0.1, "snapshot waiting for its boundary",
				mlog.FieldCollectionID(collectionID),
				mlog.Duration("waited", time.Since(start)),
				mlog.Strings("channelsBehindBoundary", behind),
				mlog.Int64s("awaitingVisibility", awaiting))
		}
	}
}

// DropSnapshot deletes an existing snapshot by name.
// This operation is idempotent - if the snapshot doesn't exist, it returns nil.
func (sm *snapshotManager) DropSnapshot(ctx context.Context, collectionID int64, name string) error {
	mlog.Info(context.TODO(), "drop snapshot request received")

	// Check if snapshot exists first (idempotent for not-found, propagate other errors)
	_, err := sm.snapshotMeta.GetSnapshot(ctx, collectionID, name)
	if err != nil {
		if errors.Is(err, merr.ErrSnapshotNotFound) {
			mlog.Info(context.TODO(), "snapshot not found, skip drop (idempotent)")
			return nil
		}
		return err
	}

	// Delete snapshot
	if err := sm.snapshotMeta.DropSnapshot(ctx, collectionID, name); err != nil {
		mlog.Error(context.TODO(), "failed to drop snapshot", mlog.Err(err))
		return err
	}

	deleteSnapshotActivePinsGauge(collectionID, name)
	mlog.Info(context.TODO(), "snapshot dropped successfully")
	return nil
}

// DropSnapshotsByCollection deletes all snapshots for a collection and drops
// their active_pins gauge series. The meta layer returns the names it
// successfully dropped (pinned/not-found/failed ones are excluded), so metric
// cleanup is symmetric with the per-snapshot DropSnapshot path.
func (sm *snapshotManager) DropSnapshotsByCollection(ctx context.Context, collectionID int64) error {
	mlog.Info(context.TODO(), "drop all snapshots for collection")

	dropped, err := sm.snapshotMeta.DropSnapshotsByCollection(ctx, collectionID)
	// Clear metric series for whatever was dropped, even if the overall call
	// returned an error for other snapshots in the batch.
	for _, n := range dropped {
		deleteSnapshotActivePinsGauge(collectionID, n)
	}
	if err != nil {
		mlog.Error(context.TODO(), "failed to drop snapshots for collection", mlog.Err(err))
		return err
	}

	mlog.Info(context.TODO(), "all snapshots dropped for collection", mlog.Int("droppedCount", len(dropped)))
	return nil
}

// GetSnapshot retrieves basic snapshot metadata by name within a collection.
func (sm *snapshotManager) GetSnapshot(ctx context.Context, collectionID int64, name string) (*datapb.SnapshotInfo, error) {
	return sm.snapshotMeta.GetSnapshot(ctx, collectionID, name)
}

// DescribeSnapshot retrieves detailed information about a snapshot within a collection.
func (sm *snapshotManager) DescribeSnapshot(ctx context.Context, collectionID int64, name string) (*snapshotstorage.SnapshotData, error) {
	mlog.Info(context.TODO(), "describe snapshot request received")

	// Read snapshot data with full segment information
	snapshotData, err := sm.snapshotMeta.ReadSnapshotData(ctx, collectionID, name, false)
	if err != nil {
		mlog.Error(context.TODO(), "failed to read snapshot data", mlog.Err(err))
		return nil, err
	}

	return snapshotData, nil
}

// ListSnapshots returns a list of snapshot names for the specified collection/partition.
//
// Note: proxy/task_snapshot.go:438 hard-rejects empty collection_name before
// the request reaches this layer, so collectionID is guaranteed to be non-zero
// here. The db-level aggregation branch (collectionID==0 && dbID!=0) that used
// to live here was unreachable via any public API and has been removed.
// getDBCollectionIDs is still used by ListRestoreJobs for db-level job filtering.
func (sm *snapshotManager) ListSnapshots(ctx context.Context, collectionID, partitionID, dbID int64) ([]string, error) {
	mlog.Info(context.TODO(), "list snapshots request received")

	snapshots, err := sm.snapshotMeta.ListSnapshots(ctx, collectionID, partitionID)
	if err != nil {
		mlog.Error(context.TODO(), "failed to list snapshots", mlog.Err(err))
		return nil, err
	}

	return snapshots, nil
}

// ============================================================================
// Restore Main Flow
// ============================================================================

// validateCMEKCompatibility validates that snapshots can only be restored
// to databases with matching encryption configuration.
//
// Validation rules:
//   - Non-encrypted snapshots can only be restored to non-encrypted databases
//   - Encrypted snapshots can only be restored to databases with matching ezID
//
// Returns nil if validation passes, error with descriptive message otherwise.
func (sm *snapshotManager) validateCMEKCompatibility(
	ctx context.Context,
	snapshotData *snapshotstorage.SnapshotData,
	targetDbName string,
) error {
	// Defensive nil check - return error for corrupted/invalid snapshot data
	if snapshotData == nil || snapshotData.Collection == nil || snapshotData.Collection.Schema == nil {
		return merr.WrapErrParameterInvalidMsg("invalid snapshot data: missing collection or schema information")
	}

	// Extract source EZ ID from snapshot collection's SCHEMA properties
	// Note: cipher.ezID is the canonical indicator of CMEK encryption for collections.
	// cipher.enabled is a database-level flag and is not stored in collection properties.
	// If ezID exists, the collection was encrypted and we must validate target DB compatibility.
	sourceEzID, hasSourceEz := hookutil.ParseEzIDFromProperties(snapshotData.Collection.Schema.Properties)

	// Get target database properties first (needed for both encrypted and non-encrypted snapshots)
	dbResp, err := sm.broker.DescribeDatabase(ctx, targetDbName)
	if err != nil {
		return merr.Wrapf(err, "failed to describe target database %s", targetDbName)
	}
	targetIsEncrypted := hookutil.IsDBEncrypted(dbResp.GetProperties())

	// Case 1: Non-encrypted snapshot
	if !hasSourceEz {
		if targetIsEncrypted {
			return merr.WrapErrParameterInvalidMsg(
				"cannot restore non-encrypted collection to CMEK-encrypted database %s", targetDbName)
		}
		return nil // Non-encrypted → Non-encrypted: OK
	}

	// Case 2: Encrypted snapshot → target must be encrypted with same ezID
	if !targetIsEncrypted {
		return merr.WrapErrParameterInvalidMsg(
			"cannot restore CMEK-encrypted collection to non-encrypted database %s", targetDbName)
	}

	// Extract target EZ ID and validate match
	targetEzID, hasTargetEz := hookutil.ParseEzIDFromProperties(dbResp.GetProperties())
	if !hasTargetEz {
		return merr.WrapErrParameterInvalidMsg(
			"target database %s is marked as encrypted but has no encryption zone ID", targetDbName)
	}

	if sourceEzID != targetEzID {
		return merr.WrapErrParameterInvalidMsg(
			"cannot restore CMEK-encrypted collection to database %s with different encryption zone (source ezID=%d, target ezID=%d)",
			targetDbName, sourceEzID, targetEzID)
	}

	return nil
}

// RestoreSnapshot orchestrates the complete snapshot restoration process.
//
// Flow:
//
//	Phase 0: Acquire the full restore lock set and claim a restore reference
//	         on the source snapshot. The lock + refcount together guarantee
//	         that a concurrent DropSnapshot cannot delete the snapshot mid-flight.
//	Phase 1: Read snapshot data.
//	Phase 2: Restore collection and partitions.
//	Phase 3: Restore indexes.
//	Phase 4: Allocate job ID and broadcast the restore message.
//
// On any failure path, the claimed restore reference is released via defer and
// the partially-created target collection is rolled back.
func (sm *snapshotManager) RestoreSnapshot(
	ctx context.Context,
	sourceCollectionID int64,
	snapshotName string,
	targetCollectionName string,
	targetDbName string,
	startRestoreLock StartRestoreLockFunc,
	startBroadcaster StartBroadcasterFunc,
	rollback RollbackFunc,
	validateResources ValidateResourcesFunc,
) (jobID int64, err error) {
	// ========================================================================
	// Phase 0: Acquire serialization lock + claim restore reference
	//
	// This MUST happen before reading any snapshot data or creating any target
	// resources. Without this, a concurrent DropSnapshot could delete the
	// source snapshot between Phase 1 and Phase 4, leaving an orphan target
	// collection and an ack callback that retries forever against a missing
	// snapshot.
	// ========================================================================
	phase0Lock, err := startRestoreLock(ctx, sourceCollectionID, snapshotName, targetDbName, targetCollectionName)
	if err != nil {
		return 0, merr.Wrap(err, "failed to acquire restore lock")
	}

	// Pin the source snapshot while holding the phase-0 lock. The pin is the
	// persistent guard that any subsequent DropSnapshot (RPC / drop-collection
	// cascade / GC) observes and rejects against — pin checks already live in
	// snapshotMeta.DropSnapshot, so no separate ref-count mechanism is needed.
	//
	// TTL acts as an orphan-pin safety net: if the job fails to persist, datacoord
	// crashes between Pin and broadcast, or UnpinSnapshot fails at terminal state,
	// the pin self-expires so DropSnapshot is not blocked indefinitely. The default
	// is 24h (dataCoord.snapshot.restorePinTTLSeconds), well above the worst-case
	// restore wall time since restore is a segment-level S3 object copy (no data
	// rewrite) — even multi-TB restores complete in minutes.
	//
	// PinSnapshot also does its own GetSnapshot under pinMu, which closes the
	// TOCTOU against any DropSnapshot that committed between the proxy-level
	// check and now — replacing the previous re-validation step.
	pinTTLSeconds := Params.DataCoordCfg.SnapshotRestorePinTTLSeconds.GetAsInt64()
	pinID, activePins, err := sm.snapshotMeta.PinSnapshot(ctx, sourceCollectionID, snapshotName, pinTTLSeconds)
	if err != nil {
		phase0Lock.Close()
		return 0, merr.Wrap(err, "failed to pin source snapshot under restore lock")
	}
	setSnapshotActivePinsGauge(sourceCollectionID, snapshotName, activePins)
	phase0Lock.Close()
	mlog.Info(context.TODO(), "source snapshot pinned under phase 0 lock", mlog.Int64("pinID", pinID))

	// If any subsequent phase fails, release the pin we just claimed. On the
	// success path, ownership of the pin is transferred to the copy segment
	// job (job.PinId), which releases it upon terminal state transition. We
	// flip this flag to false before the successful return.
	pinOwned := true
	defer func() {
		if pinOwned {
			collID, snapName, remaining, unpinErr := sm.snapshotMeta.UnpinSnapshot(ctx, pinID)
			if unpinErr != nil {
				mlog.Warn(context.TODO(), "failed to release pin on failure path",
					mlog.Int64("pinID", pinID), mlog.Err(unpinErr))
				return
			}
			if snapName != "" {
				setSnapshotActivePinsGauge(collID, snapName, remaining)
			}
			mlog.Info(context.TODO(), "released pin on failure path", mlog.Int64("pinID", pinID))
		}
	}()

	// Phase 1: Read snapshot data (now protected by the refcount guard)
	snapshotData, err := sm.ReadSnapshotData(ctx, sourceCollectionID, snapshotName)
	if err != nil {
		return 0, merr.Wrap(err, "failed to read snapshot data")
	}
	mlog.Info(context.TODO(), "snapshot data loaded",
		mlog.Int("segmentCount", len(snapshotData.Segments)),
		mlog.Int("indexCount", len(snapshotData.Indexes)))

	// Phase 1.5: Validate CMEK compatibility
	// CMEK-encrypted collections can only be restored to databases with matching encryption zone
	if err := sm.validateCMEKCompatibility(ctx, snapshotData, targetDbName); err != nil {
		mlog.Warn(context.TODO(), "CMEK compatibility validation failed", mlog.Err(err))
		return 0, err
	}

	jobID, err = sm.finishRestoreSnapshot(
		ctx,
		mlog.With(
			mlog.String("snapshotName", snapshotName),
			mlog.Int64("sourceCollectionID", sourceCollectionID),
			mlog.String("targetCollection", targetCollectionName),
			mlog.String("targetDb", targetDbName),
		),
		snapshotData,
		snapshotName,
		sourceCollectionID,
		targetCollectionName,
		targetDbName,
		pinID,
		false,
		"",
		"",
		startBroadcaster,
		rollback,
		validateResources,
	)
	if err != nil {
		if jobID != 0 {
			// Broadcast may have been persisted even though waiting for its ACK
			// failed. Keep the source pin for the callback/job in that case.
			pinOwned = false
		}
		return 0, err
	}
	// Success path: transfer ownership of the pin to the copy segment job
	// (job.PinId). The job's state machine will Unpin upon terminal transition
	// via UpdateJobStateAndReleaseRef.
	pinOwned = false
	return jobID, nil
}

func (sm *snapshotManager) RestoreExternalSnapshot(
	ctx context.Context,
	snapshotS3Location string,
	targetCollectionName string,
	targetDbName string,
	externalSpec string,
	startExternalRestoreLock StartExternalRestoreLockFunc,
	startBroadcaster StartBroadcasterFunc,
	rollback RollbackFunc,
	validateResources ValidateResourcesFunc,
) (jobID int64, err error) {
	logger := mlog.With(
		mlog.String("snapshotS3Location", snapshotstorage.RedactSnapshotObjectPath(snapshotS3Location)),
		mlog.String("targetCollection", targetCollectionName),
		mlog.String("targetDb", targetDbName),
		mlog.Bool("externalSpecSet", externalSpec != ""),
	)

	if snapshotS3Location == "" {
		return 0, merr.WrapErrParameterInvalidMsg("snapshot_s3_location is required")
	}
	unlockTarget := sm.lockExternalRestoreTarget(targetDbName, targetCollectionName)
	defer unlockTarget()

	resolved, err := snapshotstorage.ResolveForeignStorage(
		ctx,
		snapshotstorage.InstanceConfigFromParamtable(Params),
		snapshotstorage.DirectionRestore,
		snapshotS3Location,
		externalSpec,
	)
	if err != nil {
		return 0, err
	}

	phase0Lock, err := startExternalRestoreLock(ctx, targetDbName, targetCollectionName)
	if err != nil {
		return 0, merr.Wrap(err, "failed to acquire external restore lock")
	}
	defer func() {
		if phase0Lock != nil {
			phase0Lock.Close()
		}
	}()
	if err := sm.validateRestoreTargetAbsent(ctx, targetDbName, targetCollectionName); err != nil {
		return 0, err
	}

	snapshotData, err := sm.snapshotMeta.ReadAndValidateExternalSnapshotDataWithChunkManager(
		ctx,
		resolved.ForeignCM,
		snapshotS3Location,
		true,
		resolved.ForeignStorageConfig,
	)
	if err != nil {
		return 0, merr.Wrap(err, "failed to read external snapshot data")
	}
	snapshotName := snapshotData.SnapshotInfo.GetName()
	if snapshotName == "" {
		snapshotName = snapshotS3Location
	}
	sourceCollectionID := snapshotData.SnapshotInfo.GetCollectionId()

	logger.Info(ctx, "external snapshot data loaded",
		mlog.String("snapshotName", snapshotName),
		mlog.Int64("sourceCollectionID", sourceCollectionID),
		mlog.Int("segmentCount", len(snapshotData.Segments)),
		mlog.Int("indexCount", len(snapshotData.Indexes)))

	if err := sm.validateCMEKCompatibility(ctx, snapshotData, targetDbName); err != nil {
		logger.Warn(ctx, "CMEK compatibility validation failed", mlog.Err(err))
		return 0, err
	}

	// RootCoord CreateCollection acquires the same broadcaster resource key, so
	// release the phase-0 lock before entering the common restore flow. The
	// per-target DataCoord lock above continues to serialize external restores.
	phase0Lock.Close()
	phase0Lock = nil

	return sm.finishRestoreSnapshot(
		ctx,
		logger,
		snapshotData,
		snapshotName,
		sourceCollectionID,
		targetCollectionName,
		targetDbName,
		0,
		true,
		snapshotS3Location,
		externalSpec,
		startBroadcaster,
		rollback,
		validateResources,
	)
}

func (sm *snapshotManager) finishRestoreSnapshot(
	ctx context.Context,
	logger *mlog.Logger,
	snapshotData *snapshotstorage.SnapshotData,
	snapshotName string,
	sourceCollectionID int64,
	targetCollectionName string,
	targetDbName string,
	pinID int64,
	external bool,
	snapshotS3Location string,
	externalSpec string,
	startBroadcaster StartBroadcasterFunc,
	rollback RollbackFunc,
	validateResources ValidateResourcesFunc,
) (jobID int64, err error) {
	snapshotFingerprint := ""
	if external {
		snapshotFingerprint, err = snapshotstorage.SnapshotFingerprint(snapshotData)
		if err != nil {
			return 0, merr.Wrap(err, "failed to fingerprint external snapshot")
		}
	}
	collectionID, err := sm.RestoreCollection(ctx, snapshotData, targetCollectionName, targetDbName)
	if err != nil {
		return 0, merr.Wrap(err, "failed to restore collection")
	}
	logger.Info(ctx, "collection and partitions restored", mlog.Int64("collectionID", collectionID))

	if err := sm.RestoreIndexes(ctx, snapshotData, collectionID, startBroadcaster, snapshotName); err != nil {
		logger.Error(ctx, "failed to restore indexes, rolling back", mlog.Err(err))
		if rollbackErr := rollback(ctx, targetDbName, targetCollectionName); rollbackErr != nil {
			logger.Error(ctx, "rollback failed", mlog.Err(rollbackErr))
		}
		return 0, merr.Wrap(err, "failed to restore indexes")
	}
	logger.Info(ctx, "indexes restored", mlog.Int("indexCount", len(snapshotData.Indexes)))

	jobID, err = sm.allocator.AllocID(ctx)
	if err != nil {
		logger.Error(ctx, "failed to allocate job ID, rolling back", mlog.Err(err))
		if rollbackErr := rollback(ctx, targetDbName, targetCollectionName); rollbackErr != nil {
			logger.Error(ctx, "rollback failed", mlog.Err(rollbackErr))
		}
		return 0, merr.Wrap(err, "failed to allocate job ID")
	}
	logger.Info(ctx, "pre-allocated job ID for restore", mlog.Int64("jobID", jobID))

	restoreBroadcaster, err := startBroadcaster(ctx, collectionID, snapshotName)
	if err != nil {
		logger.Error(ctx, "failed to start broadcaster for restore message, rolling back", mlog.Err(err))
		if rollbackErr := rollback(ctx, targetDbName, targetCollectionName); rollbackErr != nil {
			logger.Error(ctx, "rollback failed", mlog.Err(rollbackErr))
		}
		return 0, merr.Wrap(err, "failed to start broadcaster for restore message")
	}
	defer func() {
		if restoreBroadcaster != nil {
			restoreBroadcaster.Close()
		}
	}()

	if valErr := validateResources(ctx, collectionID, snapshotData); valErr != nil {
		logger.Error(ctx, "resource validation failed, rolling back", mlog.Err(valErr))
		restoreBroadcaster.Close()
		restoreBroadcaster = nil
		if rollbackErr := rollback(ctx, targetDbName, targetCollectionName); rollbackErr != nil {
			logger.Error(ctx, "rollback failed", mlog.Err(rollbackErr))
		}
		return 0, merr.Wrap(valErr, "resource validation failed")
	}

	header := &message.RestoreSnapshotMessageHeader{
		SnapshotName:        snapshotName,
		CollectionId:        collectionID,
		JobId:               jobID,
		SourceCollectionId:  sourceCollectionID,
		PinId:               pinID,
		SnapshotFingerprint: snapshotFingerprint,
	}
	if external {
		// DataNode receives only the copy-segment task from WAL. Carry the
		// external source URI and external_spec in the header so the copy phase
		// can rebuild the foreign source storage config independently.
		header.External = true
		header.SnapshotS3Location = snapshotS3Location
		header.ExternalSpec = externalSpec
	}
	msg := message.NewRestoreSnapshotMessageBuilderV2().
		WithHeader(header).
		WithBody(&message.RestoreSnapshotMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		WithUnreplicable().
		MustBuildBroadcast()

	if _, bcErr := restoreBroadcaster.Broadcast(ctx, msg); bcErr != nil {
		restoreBroadcaster.Close()
		restoreBroadcaster = nil
		if broadcaster.IsBroadcastTaskNotCreated(bcErr) {
			logger.Error(ctx, "failed to broadcast restore message", mlog.Err(bcErr))
			if rollbackErr := rollback(ctx, targetDbName, targetCollectionName); rollbackErr != nil {
				logger.Error(ctx, "rollback failed", mlog.Err(rollbackErr))
			}
			return 0, merr.Wrap(bcErr, "failed to broadcast restore message")
		}
		// The broadcaster owns the registered task and continues it asynchronously.
		// Return the job ID instead of reporting a false failure.
		logger.Warn(ctx, "restore broadcast task registered but ACK wait did not complete",
			mlog.FieldJobID(jobID),
			mlog.Err(bcErr))
		return jobID, nil
	}

	logger.Info(ctx, "restore snapshot completed",
		mlog.Int64("collectionID", collectionID),
		mlog.Int64("jobID", jobID),
		mlog.Bool("external", external))
	return jobID, nil
}

// maxConcurrentSnapshotCaptures bounds concurrent GenSnapshot -> SaveSnapshot
// windows. Sized as a safety bound against a bulk snapshot of many collections
// exhausting memory, not as a throughput knob: it caps peak footprint at a few
// collections' worth of cloned segment metadata.
//
// The window is short in the normal case -- GenSnapshot does no object I/O,
// only path computation -- but it is not unconditionally fast: SaveSnapshot
// writes the manifest set to object storage between two etcd writes, so a
// degraded backend can hold a slot for as long as those take. Callers therefore
// queue for a slot before taking any state that blocks other work on their
// collection.
const maxConcurrentSnapshotCaptures = 4

// acquireCaptureSlot blocks until a capture slot is free and returns its
// release. Safe to call on a snapshotManager built as a bare struct literal,
// which the tests do in place of NewSnapshotManager.
func (sm *snapshotManager) acquireCaptureSlot(ctx context.Context) (func(), error) {
	sm.captureSlotsOnce.Do(func() {
		if sm.captureSlots == nil {
			sm.captureSlots = make(chan struct{}, maxConcurrentSnapshotCaptures)
		}
	})
	select {
	case sm.captureSlots <- struct{}{}:
		return func() { <-sm.captureSlots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// lockCreateSnapshot serializes snapshot creation for one collection and returns
// its unlock. Lazily initialized because most snapshotManagers in tests are
// built as bare struct literals rather than through NewSnapshotManager, and
// KeyLock's zero value has a nil map.
func (sm *snapshotManager) lockCreateSnapshot(collectionID int64) func() {
	sm.createSnapshotLockOnce.Do(func() {
		if sm.createSnapshotLock == nil {
			sm.createSnapshotLock = lock.NewKeyLock[int64]()
		}
	})
	sm.createSnapshotLock.Lock(collectionID)
	return func() {
		sm.createSnapshotLock.Unlock(collectionID)
	}
}

func (sm *snapshotManager) lockExternalRestoreTarget(dbName, collectionName string) func() {
	sm.externalRestoreTargetLockOnce.Do(func() {
		if sm.externalRestoreTargetLock == nil {
			sm.externalRestoreTargetLock = lock.NewKeyLock[restoreTarget]()
		}
	})
	target := restoreTarget{dbName: dbName, collectionName: collectionName}
	sm.externalRestoreTargetLock.Lock(target)
	return func() {
		sm.externalRestoreTargetLock.Unlock(target)
	}
}

func (sm *snapshotManager) validateRestoreTargetAbsent(
	ctx context.Context,
	targetDbName string,
	targetCollectionName string,
) error {
	_, err := sm.broker.DescribeCollectionByName(ctx, targetDbName, targetCollectionName)
	if err == nil {
		return merr.WrapErrParameterInvalidMsg(
			"target collection %s already exists in database %s",
			targetCollectionName,
			targetDbName,
		)
	}
	if errors.Is(err, merr.ErrCollectionNotFound) {
		return nil
	}
	return merr.Wrap(err, "failed to validate restore target collection")
}

func (sm *snapshotManager) ExportSnapshot(
	ctx context.Context,
	collectionID int64,
	snapshotName string,
	dbName string,
	collectionName string,
	targetS3Path string,
	externalSpec string,
) (int64, error) {
	logger := mlog.With(
		mlog.Int64("collectionID", collectionID),
		mlog.String("snapshotName", snapshotName),
		mlog.String("targetS3Path", snapshotstorage.RedactSnapshotObjectPath(targetS3Path)),
		mlog.Bool("externalSpecSet", externalSpec != ""),
	)
	logger.Info(ctx, "export snapshot request received")
	if sm.exportManager == nil {
		return 0, merr.WrapErrServiceInternalMsg("snapshot export manager is not initialized")
	}
	jobID, err := sm.exportManager.Submit(
		ctx,
		collectionID,
		snapshotName,
		dbName,
		collectionName,
		targetS3Path,
		externalSpec,
	)
	if err != nil {
		logger.Warn(ctx, "failed to submit snapshot export job", mlog.Err(err))
		return 0, err
	}
	logger.Info(ctx, "snapshot export job submitted", mlog.FieldJobID(jobID))
	return jobID, nil
}

func (sm *snapshotManager) GetExportSnapshotState(jobID int64) (*datapb.ExportSnapshotJobInfo, error) {
	if sm.exportManager == nil {
		return nil, merr.WrapErrServiceInternalMsg("snapshot export manager is not initialized")
	}
	return sm.exportManager.GetJobInfo(jobID)
}

// RestoreCollection creates a new collection and its user partitions based on snapshot data.
func (sm *snapshotManager) RestoreCollection(
	ctx context.Context,
	snapshotData *snapshotstorage.SnapshotData,
	targetCollectionName, targetDbName string,
) (int64, error) {
	collection := snapshotData.Collection

	// Clone the schema to avoid modifying the original snapshot data,
	// and update the schema name and database name to match the target.
	// This is required because Milvus validates that CollectionName == Schema.Name.
	schema := proto.Clone(collection.Schema).(*schemapb.CollectionSchema)
	schema.Name = targetCollectionName
	schema.DbName = targetDbName

	schemaInBytes, err := proto.Marshal(schema)
	if err != nil {
		return 0, err
	}

	// preserve field ids
	properties := common.CloneKeyValuePairs(collection.GetProperties())
	if len(properties) == 0 {
		// Snapshots created before collection metadata was populated in
		// CollectionDescription keep these values in schema properties.
		properties = common.CloneKeyValuePairs(schema.GetProperties())
	}
	properties = append(properties, &commonpb.KeyValuePair{
		Key:   util.PreserveFieldIdsKey,
		Value: strconv.FormatBool(true),
	})

	// Build CreateCollectionRequest
	req := &milvuspb.CreateCollectionRequest{
		DbName:           targetDbName,
		CollectionName:   targetCollectionName,
		Schema:           schemaInBytes,
		ShardsNum:        int32(collection.NumShards),
		ConsistencyLevel: collection.ConsistencyLevel,
		Properties:       properties,
		NumPartitions:    collection.NumPartitions,
	}

	// Call RootCoord to create collection
	if err := sm.broker.CreateCollection(ctx, req); err != nil {
		return 0, err
	}

	// Get the new collection ID by querying with collection name
	resp, err := sm.broker.DescribeCollectionByName(ctx, targetDbName, targetCollectionName)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return 0, err
	}
	collectionID := resp.GetCollectionID()

	// Create user partitions
	if err := sm.restoreUserPartitions(ctx, snapshotData, targetCollectionName, targetDbName); err != nil {
		return 0, err
	}

	return collectionID, nil
}

// RestoreIndexes restores indexes from snapshot data by broadcasting CreateIndex messages directly to DDL WAL.
// This bypasses CreateIndex validation (e.g., ParseAndVerifyNestedPath) because snapshot data
// already contains properly formatted index parameters (e.g., json_path in JSON Pointer format).
//
// Note: Each broadcaster can only be used once due to resource key lock consumption,
// so we need to create a new broadcaster for each index.
func (sm *snapshotManager) RestoreIndexes(
	ctx context.Context,
	snapshotData *snapshotstorage.SnapshotData,
	collectionID int64,
	startBroadcaster StartBroadcasterFunc,
	snapshotName string,
) error {
	// Get collection info for dbId
	coll, err := sm.broker.DescribeCollectionInternal(ctx, collectionID)
	if err != nil {
		return merr.Wrapf(err, "failed to describe collection %d", collectionID)
	}

	for _, indexInfo := range snapshotData.Indexes {
		// Allocate new index ID
		indexID, err := sm.allocator.AllocID(ctx)
		if err != nil {
			return merr.Wrap(err, "failed to allocate index ID")
		}

		// Build index model from snapshot data
		// Note: TypeParams may contain mmap_enabled which should be filtered out
		index := &model.Index{
			CollectionID:    collectionID,
			FieldID:         indexInfo.GetFieldID(),
			IndexID:         indexID,
			IndexName:       indexInfo.GetIndexName(),
			TypeParams:      DeleteParams(indexInfo.GetTypeParams(), []string{common.MmapEnabledKey}),
			IndexParams:     indexInfo.GetIndexParams(),
			CreateTime:      uint64(time.Now().UnixNano()),
			IsAutoIndex:     indexInfo.GetIsAutoIndex(),
			UserIndexParams: indexInfo.GetUserIndexParams(),
		}

		// Validate the index params (basic validation without JSON path parsing)
		if err := indexparamcheck.ValidateIndexParams(index); err != nil {
			return merr.Wrapf(err, "failed to validate index %s", indexInfo.GetIndexName())
		}

		// Check scalar index engine version for JSON path indexes with new types
		if err := sm.checkJSONPathIndexVersion(index); err != nil {
			return merr.Wrapf(err, "failed to validate index %s", indexInfo.GetIndexName())
		}

		// Check scalar index engine version for FMINDEX — restore broadcasts the
		// same CreateIndex DDL directly, so it must apply the same gate
		// Server.CreateIndex does (checkFMIndexEngineVersion), otherwise a
		// snapshot could create an FMINDEX an old QueryNode cannot load.
		resolvedScalarVersion := int32(0)
		if sm.indexEngineVersionManager != nil {
			resolvedScalarVersion = sm.indexEngineVersionManager.ResolveScalarIndexVersion()
		}
		if err := checkFMIndexEngineVersion(index.IndexParams, resolvedScalarVersion); err != nil {
			return merr.Wrapf(err, "failed to validate index %s", indexInfo.GetIndexName())
		}

		// Create a new broadcaster for each index
		// (each broadcaster can only be used once due to resource key lock consumption)
		b, err := startBroadcaster(ctx, collectionID, snapshotName)
		if err != nil {
			return merr.Wrapf(err, "failed to start broadcaster for index %s", indexInfo.GetIndexName())
		}

		// Broadcast CreateIndex message directly to DDL WAL
		_, err = b.Broadcast(ctx, message.NewCreateIndexMessageBuilderV2().
			WithHeader(&message.CreateIndexMessageHeader{
				DbId:         coll.GetDbId(),
				CollectionId: collectionID,
				FieldId:      indexInfo.GetFieldID(),
				IndexId:      indexID,
				IndexName:    indexInfo.GetIndexName(),
			}).
			WithBody(&message.CreateIndexMessageBody{
				FieldIndex: model.MarshalIndexModel(index),
			}).
			WithBroadcast([]string{streaming.WAL().ControlChannel()}).
			MustBuildBroadcast(),
		)
		b.Close()
		if err != nil {
			return merr.Wrapf(err, "failed to broadcast create index %s", indexInfo.GetIndexName())
		}

		mlog.Info(ctx, "index restored via DDL WAL broadcast",
			mlog.String("indexName", indexInfo.GetIndexName()),
			mlog.Int64("fieldID", indexInfo.GetFieldID()),
			mlog.Int64("indexID", indexID))
	}
	return nil
}

// RestoreData handles the data restoration phase of snapshot restore.
// It builds partition/channel mappings and creates the copy segment job.
// Collection and partition creation should be handled by the caller (services.go).
//
// Process flow:
//  1. Check if job already exists (idempotency)
//  2. Build partition mapping
//  3. Build channel mapping
//  4. Create copy segment job
func (sm *snapshotManager) RestoreData(
	ctx context.Context,
	sourceCollectionID int64,
	snapshotName string,
	collectionID int64,
	jobID int64,
	pinID int64,
) (int64, error) {
	return sm.restoreData(ctx, sourceCollectionID, snapshotName, "", collectionID, jobID, pinID, false, "", "")
}

func (sm *snapshotManager) RestoreExternalData(
	ctx context.Context,
	sourceCollectionID int64,
	snapshotName string,
	snapshotS3Location string,
	collectionID int64,
	jobID int64,
	externalSpec string,
	snapshotFingerprint string,
) (int64, error) {
	return sm.restoreData(ctx, sourceCollectionID, snapshotName, snapshotS3Location, collectionID, jobID, 0, true, externalSpec, snapshotFingerprint)
}

func (sm *snapshotManager) restoreData(
	ctx context.Context,
	sourceCollectionID int64,
	snapshotName string,
	snapshotS3Location string,
	collectionID int64,
	jobID int64,
	pinID int64,
	external bool,
	externalSpec string,
	expectedSnapshotFingerprint string,
) (int64, error) {
	mlog.Info(ctx, "restore data started",
		mlog.String("snapshot", snapshotName),
		mlog.String("snapshotS3Location", snapshotstorage.RedactSnapshotObjectPath(snapshotS3Location)),
		mlog.Int64("sourceCollectionID", sourceCollectionID),
		mlog.Int64("collectionID", collectionID),
		mlog.Int64("jobID", jobID),
		mlog.Bool("external", external),
		mlog.Bool("externalSpecSet", externalSpec != ""))
	handleExternalError := func(operation string, err error) (int64, error) {
		return sm.handleExternalRestoreSourceError(
			ctx,
			sourceCollectionID,
			snapshotName,
			snapshotS3Location,
			collectionID,
			jobID,
			operation,
			err,
		)
	}

	// ========== Phase 1: Idempotency check ==========
	// Check if job already exists (WAL replay scenario)
	existingJob := sm.copySegmentMeta.GetJob(ctx, jobID)
	if existingJob != nil {
		mlog.Info(context.TODO(), "job already exists, skip creation (idempotent)")
		return jobID, nil
	}

	var snapshotData *snapshotstorage.SnapshotData
	var err error
	if external {
		resolved, resolveErr := snapshotstorage.ResolveForeignStorage(
			ctx,
			snapshotstorage.InstanceConfigFromParamtable(Params),
			snapshotstorage.DirectionRestore,
			snapshotS3Location,
			externalSpec,
		)
		if resolveErr != nil {
			return handleExternalError("failed to resolve external snapshot storage", resolveErr)
		}
		snapshotData, err = sm.snapshotMeta.ReadAndValidateExternalSnapshotDataWithChunkManager(
			ctx,
			resolved.ForeignCM,
			snapshotS3Location,
			true,
			resolved.ForeignStorageConfig,
		)
	} else {
		snapshotData, err = sm.ReadSnapshotData(ctx, sourceCollectionID, snapshotName)
	}
	if err != nil {
		mlog.Error(context.TODO(), "failed to read snapshot data", mlog.Err(err))
		if external {
			return handleExternalError("failed to read external snapshot data", err)
		}
		return 0, merr.Wrap(err, "failed to read snapshot data")
	}
	actualSnapshotFingerprint := ""
	if external {
		actualSnapshotFingerprint, err = snapshotstorage.SnapshotFingerprint(snapshotData)
		if err != nil {
			return handleExternalError("failed to fingerprint external snapshot", err)
		}
		if expectedSnapshotFingerprint != "" && actualSnapshotFingerprint != expectedSnapshotFingerprint {
			fingerprintErr := merr.WrapErrDataIntegrityMsg(
				"external snapshot changed after preflight validation",
			)
			return handleExternalError("snapshot fingerprint mismatch", fingerprintErr)
		}
	}

	// ========== Phase 2: Build partition mapping ==========
	partitionMapping, err := sm.buildPartitionMapping(ctx, snapshotData, collectionID)
	if err != nil {
		mlog.Error(context.TODO(), "failed to build partition mapping", mlog.Err(err))
		if external {
			return handleExternalError("partition mapping failed", err)
		}
		return 0, merr.Wrap(err, "partition mapping failed")
	}
	mlog.Info(context.TODO(), "partition mapping built", mlog.Any("partitionMapping", partitionMapping))

	// ========== Phase 3: Build channel mapping ==========
	channelMapping, err := sm.buildChannelMapping(ctx, snapshotData, collectionID)
	if err != nil {
		mlog.Error(context.TODO(), "failed to build channel mapping", mlog.Err(err))
		if external {
			return handleExternalError("channel mapping failed", err)
		}
		return 0, merr.Wrap(err, "channel mapping failed")
	}

	// ========== Phase 4: Create copy segment job ==========
	// Use the pre-allocated jobID from the WAL message
	if err := sm.createRestoreJob(
		ctx,
		collectionID,
		channelMapping,
		partitionMapping,
		snapshotData,
		jobID,
		pinID,
		external,
		snapshotS3Location,
		externalSpec,
		actualSnapshotFingerprint,
	); err != nil {
		mlog.Error(context.TODO(), "failed to create restore job", mlog.Err(err))
		if external {
			return handleExternalError("restore job creation failed", err)
		}
		return 0, merr.Wrap(err, "restore job creation failed")
	}

	mlog.Info(context.TODO(), "restore data completed successfully",
		mlog.Int64("jobID", jobID),
		mlog.Int64("collectionID", collectionID))

	return jobID, nil
}

func (sm *snapshotManager) handleExternalRestoreSourceError(
	ctx context.Context,
	sourceCollectionID int64,
	snapshotName string,
	snapshotS3Location string,
	collectionID int64,
	jobID int64,
	operation string,
	err error,
) (int64, error) {
	wrappedErr := merr.Wrap(err, operation)
	if !isPermanentSnapshotError(err) {
		return 0, wrappedErr
	}

	now := uint64(time.Now().UnixNano())
	failedJob := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:              jobID,
			CollectionId:       collectionID,
			State:              datapb.CopySegmentJobState_CopySegmentJobPending,
			StartTs:            now,
			CompleteTs:         now,
			SnapshotName:       snapshotName,
			SourceCollectionId: sourceCollectionID,
			External:           true,
			SnapshotS3Location: snapshotS3Location,
		},
		tr: timerecord.NewTimeRecorder("copy segment job"),
	}
	UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed)(failedJob)
	UpdateCopyJobReason(wrappedErr.Error())(failedJob)
	if addErr := sm.copySegmentMeta.AddJob(ctx, failedJob); addErr != nil {
		return 0, merr.Wrap(addErr, "failed to persist failed external restore job")
	}

	mlog.Warn(ctx, "external snapshot restore failed permanently; restore job marked failed",
		mlog.FieldJobID(jobID),
		mlog.Int64("collectionID", collectionID),
		mlog.String("snapshotS3Location", snapshotstorage.RedactSnapshotObjectPath(snapshotS3Location)),
		mlog.Err(wrappedErr))
	return jobID, nil
}

func isPermanentSnapshotError(err error) bool {
	return errors.Is(err, merr.ErrDataIntegrity) ||
		merr.GetErrorType(err) == merr.InputError ||
		merr.IsNonRetryableErr(err)
}

// ============================================================================
// Restore Helper Functions (private)
// ============================================================================

// restoreUserPartitions creates user partitions based on snapshot data.
// It creates partitions that exist in the snapshot but not in the target collection.
func (sm *snapshotManager) restoreUserPartitions(
	ctx context.Context,
	snapshotData *snapshotstorage.SnapshotData,
	targetCollectionName, targetDbName string,
) error {
	hasPartitionKey := typeutil.HasPartitionKey(snapshotData.Collection.GetSchema())
	defaultPartitionName := Params.CommonCfg.DefaultPartitionName.GetValue()
	userCreatedPartitions := make([]string, 0)
	if !hasPartitionKey {
		for partitionName := range snapshotData.Collection.GetPartitions() {
			if partitionName == defaultPartitionName {
				continue
			}

			parts := strings.Split(partitionName, "_")
			if len(parts) == 2 && parts[0] == defaultPartitionName {
				continue
			}
			userCreatedPartitions = append(userCreatedPartitions, partitionName)
		}
	}

	// Create user partitions that don't exist yet
	for _, partitionName := range userCreatedPartitions {
		// Create the partition
		req := &milvuspb.CreatePartitionRequest{
			DbName:         targetDbName,
			CollectionName: targetCollectionName,
			PartitionName:  partitionName,
		}

		if err := sm.broker.CreatePartition(ctx, req); err != nil {
			return merr.Wrapf(err, "failed to create partition %s", partitionName)
		}
	}

	return nil
}

// buildPartitionMapping builds a mapping from snapshot partition IDs to target partition IDs.
func (sm *snapshotManager) buildPartitionMapping(
	ctx context.Context,
	snapshotData *snapshotstorage.SnapshotData,
	collectionID int64,
) (map[int64]int64, error) {
	// Get current partitions
	currentPartitions, err := sm.broker.ShowPartitions(ctx, collectionID)
	if err != nil {
		return nil, err
	}

	// Build partition name to ID mapping for target collection
	currrentPartitionMap := make(map[string]int64)
	for i, name := range currentPartitions.GetPartitionNames() {
		currrentPartitionMap[name] = currentPartitions.GetPartitionIDs()[i]
	}

	// Build snapshot partition ID to target partition ID mapping with same name
	partitionMapping := make(map[int64]int64)
	for partitionName, partitionID := range snapshotData.Collection.GetPartitions() {
		targetPartID, ok := currrentPartitionMap[partitionName]
		if !ok {
			return nil, merr.WrapErrDataIntegrityMsg(
				"partition %s from snapshot not found in target collection", partitionName)
		}
		partitionMapping[partitionID] = targetPartID
	}

	return partitionMapping, nil
}

// buildChannelMapping generates a mapping from snapshot channels to target collection channels.
// It ensures that the channel count matches and returns a sorted mapping.
func (sm *snapshotManager) buildChannelMapping(
	ctx context.Context,
	snapshotData *snapshotstorage.SnapshotData,
	targetCollectionID int64,
) (map[string]string, error) {
	if len(snapshotData.Segments) == 0 {
		return make(map[string]string), nil
	}

	snapshotChannels := append([]string(nil), snapshotData.Collection.GetVirtualChannelNames()...)

	// Get target collection channels
	targetChannels, err := sm.getChannelsByCollectionID(ctx, targetCollectionID)
	if err != nil {
		mlog.Error(ctx, "failed to get channels by collection ID", mlog.Err(err))
		return nil, err
	}

	// Validate count
	if len(targetChannels) != len(snapshotChannels) {
		return nil, merr.WrapErrDataIntegrityMsg(
			"channel count mismatch between snapshot and target collection: snapshot=%d, target=%d",
			len(snapshotChannels), len(targetChannels))
	}

	// Build mapping (sorted)
	sort.Strings(snapshotChannels)

	targetChannelNames := make([]string, len(targetChannels))
	for i, ch := range targetChannels {
		targetChannelNames[i] = ch.GetName()
	}
	sort.Strings(targetChannelNames)

	mapping := make(map[string]string)
	for i, targetChannel := range targetChannelNames {
		mapping[snapshotChannels[i]] = targetChannel
	}

	return mapping, nil
}

// createRestoreJob creates a copy segment job for snapshot restore.
// This is the internal implementation of restoreSnapshotByCopy from services.go.
// The jobID must be pre-allocated by the caller.
func (sm *snapshotManager) createRestoreJob(
	ctx context.Context,
	targetCollection int64,
	channelMapping map[string]string,
	partitionMapping map[int64]int64,
	snapshotData *snapshotstorage.SnapshotData,
	jobID int64,
	pinID int64,
	external bool,
	snapshotS3Location string,
	externalSpec string,
	snapshotFingerprint string,
) error {
	// Validate that every segment the snapshot references still exists in local
	// meta. External restore reads source metadata from object storage, so its
	// source segments are legitimately absent here and are not checked.
	//
	// A missing segment is fatal, not skippable. The snapshot is a point-in-time
	// copy, so restoring it minus one segment silently produces a collection
	// with rows missing and reports success -- the caller has no way to notice.
	// Snapshot pins are supposed to keep these alive, so reaching this means
	// that protection failed and the restore cannot deliver what it promises.
	validSegments := make([]*datapb.SegmentDescription, 0, len(snapshotData.Segments))
	for _, segDesc := range snapshotData.Segments {
		sourceSegmentID := segDesc.GetSegmentId()
		if !external {
			if segInfo := sm.meta.GetSegment(ctx, sourceSegmentID); segInfo == nil {
				mlog.Error(ctx, "restore aborted: a segment this snapshot references is gone from meta",
					mlog.Int64("sourceSegmentID", sourceSegmentID))
				return merr.WrapErrDataIntegrityMsg(
					"snapshot references segment %d, which no longer exists; restoring would silently drop its rows",
					sourceSegmentID)
			}
		}
		validSegments = append(validSegments, segDesc)
	}

	// Allocate target segment IDs
	// AllocN returns (start, end, error), where end = start + count
	targetSegmentIDStart, _, err := sm.allocator.AllocN(int64(len(validSegments)))
	if err != nil {
		mlog.Error(context.TODO(), "failed to allocate segment IDs", mlog.Err(err))
		return err
	}

	// Create ID mappings and pre-register target segments
	idMappings := make([]*datapb.CopySegmentIDMapping, 0, len(validSegments))
	totalRows := int64(0)
	targetSegments := make(map[int64]*SegmentInfo, len(validSegments))
	for i, segDesc := range validSegments {
		sourceSegmentID := segDesc.GetSegmentId()
		targetSegmentID := targetSegmentIDStart + int64(i)
		totalRows += segDesc.GetNumOfRows()

		// Apply partition mapping
		sourcePartitionID := segDesc.GetPartitionId()
		targetPartitionID, ok := partitionMapping[sourcePartitionID]
		if !ok {
			// L0 segments may not have partition mapping
			if segDesc.GetSegmentLevel() == datapb.SegmentLevel_L0 {
				targetPartitionID = -1
			} else {
				err := merr.WrapErrDataIntegrityMsg(
					"partition mapping not found for segment: sourceSegmentID=%d, sourcePartitionID=%d",
					sourceSegmentID, sourcePartitionID)
				mlog.Error(context.TODO(), "partition mapping missing", mlog.Err(err))
				return err
			}
		}

		idMappings = append(idMappings, &datapb.CopySegmentIDMapping{
			SourceSegmentId: sourceSegmentID,
			TargetSegmentId: targetSegmentID,
			PartitionId:     targetPartitionID,
		})

		// Apply channel mapping
		targetChannelName, ok := channelMapping[segDesc.GetChannelName()]
		if !ok {
			err := merr.WrapErrDataIntegrityMsg(
				"channel mapping missing for channel: %s", segDesc.GetChannelName())
			mlog.Error(context.TODO(), "channel mapping not found", mlog.Err(err))
			return err
		}

		// Prepare positions with correct channel names
		startPos := segDesc.GetStartPosition()
		dmlPos := segDesc.GetDmlPosition()
		if startPos != nil {
			startPos.ChannelName = targetChannelName
		}
		if dmlPos != nil {
			dmlPos.ChannelName = targetChannelName
		}

		// Pre-register target segment in meta. NewSegmentInfo eagerly
		// populates Stats so concurrent RLock readers don't race on a
		// lazy init.
		newSegment := NewSegmentInfo(&datapb.SegmentInfo{
			ID:                  targetSegmentID,
			CollectionID:        targetCollection,
			PartitionID:         targetPartitionID,
			InsertChannel:       targetChannelName,
			NumOfRows:           segDesc.GetNumOfRows(),
			State:               commonpb.SegmentState_Importing,
			MaxRowNum:           Params.DataCoordCfg.SegmentMaxSize.GetAsInt64(),
			Level:               segDesc.GetSegmentLevel(),
			CreatedByCompaction: false,
			LastExpireTime:      math.MaxUint64,
			StartPosition:       startPos,
			DmlPosition:         dmlPos,
			StorageVersion:      segDesc.GetStorageVersion(),
			IsSorted:            segDesc.GetIsSorted(),
			CommitTimestamp:     segDesc.GetCommitTimestamp(),
			IsImporting:         true,
		})
		targetSegments[targetSegmentID] = newSegment
	}

	// Pre-register all target segments in meta to ensure they exist when copy tasks run
	for _, targetSegment := range targetSegments {
		if err := sm.meta.AddSegment(ctx, targetSegment); err != nil {
			mlog.Error(context.TODO(), "failed to pre-register target segment", mlog.Err(err))
			return err
		}
	}

	// Pre-register channel's checkpoint
	collection, err := sm.handler.GetCollection(ctx, targetCollection)
	if err != nil {
		mlog.Error(context.TODO(), "failed to get collection", mlog.Err(err))
		return err
	}
	for _, channel := range channelMapping {
		startPosition := toMsgPosition(channel, collection.StartPositions)
		if err := sm.meta.UpdateChannelCheckpoint(ctx, channel, startPosition); err != nil {
			mlog.Error(context.TODO(), "failed to pre-register channel checkpoint", mlog.Err(err))
			return err
		}
	}

	// Create copy segment job
	jobTimeout := Params.DataCoordCfg.CopySegmentJobTimeout.GetAsDuration(time.Second)
	copyJob := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        jobID,
			CollectionId: targetCollection,
			State:        datapb.CopySegmentJobState_CopySegmentJobPending,
			IdMappings:   idMappings,
			TimeoutTs:    CopyJobTimeoutTs(jobTimeout),
			StartTs:      uint64(time.Now().UnixNano()),
			Options: []*commonpb.KeyValuePair{
				{Key: "copy_index", Value: "true"},
				{Key: "source_type", Value: "snapshot"},
			},
			TotalSegments:       int64(len(idMappings)),
			TotalRows:           totalRows,
			SnapshotName:        snapshotData.SnapshotInfo.GetName(),
			SourceCollectionId:  snapshotData.SnapshotInfo.GetCollectionId(),
			PinId:               pinID,
			External:            external,
			SnapshotS3Location:  snapshotS3Location,
			ExternalSpec:        externalSpec,
			SnapshotFingerprint: snapshotFingerprint,
		},
		tr:            timerecord.NewTimeRecorder("copy segment job"),
		snapshotCache: &copySegmentSnapshotCache{},
	}

	// NOTE: The restore reference has already been claimed in Phase 0 of
	// RestoreSnapshot (service layer) before any snapshot data was read. The
	// reference is now transferred to this job and will be released by
	// UpdateJobStateAndReleaseRef when the job reaches a terminal state.
	//
	// Save job to metadata. If AddJob fails, the ack callback will retry
	// (see ack_callback_scheduler) or eventually fail terminally; the
	// ref-count release on terminal failure is the responsibility of WU-3
	// (terminal error classification) in the broadcaster layer.
	if err := sm.copySegmentMeta.AddJob(ctx, copyJob); err != nil {
		mlog.Error(context.TODO(), "failed to save copy segment job",
			mlog.Int64("sourceCollectionID", snapshotData.SnapshotInfo.GetCollectionId()),
			mlog.String("snapshot", snapshotData.SnapshotInfo.GetName()), mlog.Err(err))
		return err
	}

	mlog.Info(context.TODO(), "copy segment job created successfully",
		mlog.Int64("jobID", jobID),
		mlog.Int("totalSegments", len(idMappings)))

	return nil
}

// ============================================================================
// Restore State Query
// ============================================================================

// ReadSnapshotData reads snapshot data from storage.
// This is a convenience wrapper for snapshotMeta.ReadSnapshotData.
func (sm *snapshotManager) ReadSnapshotData(ctx context.Context, collectionID int64, snapshotName string) (*snapshotstorage.SnapshotData, error) {
	return sm.snapshotMeta.ReadSnapshotData(ctx, collectionID, snapshotName, true)
}

// GetRestoreState retrieves the current state of a restore job.
func (sm *snapshotManager) GetRestoreState(ctx context.Context, jobID int64) (*datapb.RestoreSnapshotInfo, error) {
	// Get job
	job := sm.copySegmentMeta.GetJob(ctx, jobID)
	if job == nil {
		err := merr.WrapErrImportSysFailedMsg("restore job not found: jobID=%d", jobID)
		mlog.Warn(context.TODO(), "restore job not found")
		return nil, err
	}

	// Build restore info using centralized helper
	restoreInfo := sm.buildRestoreInfo(job)

	mlog.Info(context.TODO(), "get restore state completed",
		mlog.String("state", restoreInfo.GetState().String()),
		mlog.Int32("progress", restoreInfo.GetProgress()))

	return restoreInfo, nil
}

// ListRestoreJobs returns a list of all restore jobs, optionally filtered by collection ID or database ID.
func (sm *snapshotManager) ListRestoreJobs(
	ctx context.Context,
	collectionIDFilter, dbID int64,
) ([]*datapb.RestoreSnapshotInfo, error) {
	// Get all jobs
	jobs := sm.copySegmentMeta.GetJobBy(ctx)

	// Build a set of collection IDs in the database for db-level filtering
	var dbCollections map[int64]struct{}
	if dbID != 0 && collectionIDFilter == 0 {
		dbCollections = sm.getDBCollectionIDs(dbID)
	}

	// Filter by collection/database and build restore info list
	restoreInfos := make([]*datapb.RestoreSnapshotInfo, 0)
	for _, job := range jobs {
		if collectionIDFilter != 0 && job.GetCollectionId() != collectionIDFilter {
			continue
		}
		if dbCollections != nil {
			if _, ok := dbCollections[job.GetCollectionId()]; !ok {
				continue
			}
		}

		restoreInfos = append(restoreInfos, sm.buildRestoreInfo(job))
	}

	mlog.Info(ctx, "list restore jobs completed",
		mlog.Int("totalJobs", len(restoreInfos)),
		mlog.Int64("filterCollectionId", collectionIDFilter),
		mlog.Int64("filterDbId", dbID))

	return restoreInfos, nil
}

// PinSnapshotData pins a snapshot and returns a unique pin ID.
func (sm *snapshotManager) PinSnapshotData(ctx context.Context, collectionID int64, name string, ttlSeconds int64) (int64, error) {
	pinID, active, err := sm.snapshotMeta.PinSnapshot(ctx, collectionID, name, ttlSeconds)
	if err != nil {
		return 0, err
	}
	setSnapshotActivePinsGauge(collectionID, name, active)
	return pinID, nil
}

// UnpinSnapshotData removes a pin by ID.
func (sm *snapshotManager) UnpinSnapshotData(ctx context.Context, pinID int64) error {
	collID, name, active, err := sm.snapshotMeta.UnpinSnapshot(ctx, pinID)
	if err != nil {
		return err
	}
	if name != "" {
		setSnapshotActivePinsGauge(collID, name, active)
	}
	return nil
}

// setSnapshotActivePinsGauge publishes the active-pins gauge for a snapshot.
// When active falls to zero we drop the label series to keep Prometheus
// cardinality bounded — otherwise every dropped snapshot leaves behind a
// stale series until process restart.
func setSnapshotActivePinsGauge(collectionID int64, name string, active int) {
	label := []string{strconv.FormatInt(collectionID, 10), name}
	if active == 0 {
		metrics.DataCoordSnapshotActivePins.DeleteLabelValues(label...)
		return
	}
	metrics.DataCoordSnapshotActivePins.WithLabelValues(label...).Set(float64(active))
}

// deleteSnapshotActivePinsGauge drops the active-pins gauge series for a
// dropped snapshot. Safe to call even if no series was emitted.
func deleteSnapshotActivePinsGauge(collectionID int64, name string) {
	metrics.DataCoordSnapshotActivePins.DeleteLabelValues(strconv.FormatInt(collectionID, 10), name)
}

// HasActivePins reports whether the named snapshot has any non-expired pins.
func (sm *snapshotManager) HasActivePins(ctx context.Context, collectionID int64, name string) (bool, error) {
	return sm.snapshotMeta.HasActivePins(ctx, collectionID, name)
}

// ============================================================================
// Common Helper Functions (private)
// ============================================================================

// getDBCollectionIDs returns the set of collection IDs belonging to a database.
// Used by ListSnapshots and ListRestoreJobs for database-level filtering.
func (sm *snapshotManager) getDBCollectionIDs(dbID int64) map[int64]struct{} {
	result := make(map[int64]struct{})
	for _, coll := range sm.meta.GetCollections() {
		if coll.DatabaseID == dbID {
			result[coll.ID] = struct{}{}
		}
	}
	return result
}

// buildRestoreInfo constructs a RestoreSnapshotInfo from a CopySegmentJob.
// This centralizes the conversion logic to eliminate code duplication.
func (sm *snapshotManager) buildRestoreInfo(job CopySegmentJob) *datapb.RestoreSnapshotInfo {
	return &datapb.RestoreSnapshotInfo{
		JobId:        job.GetJobId(),
		SnapshotName: job.GetSnapshotName(),
		CollectionId: job.GetCollectionId(),
		DbId:         job.GetDbId(),
		State:        sm.convertJobState(job.GetState()),
		Progress:     sm.calculateProgress(job),
		Reason:       job.GetReason(),
		TimeCost:     sm.calculateTimeCost(job),
		StartTime:    job.GetStartTs() / 1e6, // Convert nanoseconds to milliseconds
	}
}

// convertJobState converts CopySegmentJobState to RestoreSnapshotState.
// This eliminates code duplication between GetRestoreState and ListRestoreJobs.
func (sm *snapshotManager) convertJobState(jobState datapb.CopySegmentJobState) datapb.RestoreSnapshotState {
	switch jobState {
	case datapb.CopySegmentJobState_CopySegmentJobPending:
		return datapb.RestoreSnapshotState_RestoreSnapshotPending
	case datapb.CopySegmentJobState_CopySegmentJobExecuting:
		return datapb.RestoreSnapshotState_RestoreSnapshotExecuting
	case datapb.CopySegmentJobState_CopySegmentJobCompleted:
		return datapb.RestoreSnapshotState_RestoreSnapshotCompleted
	case datapb.CopySegmentJobState_CopySegmentJobFailed:
		return datapb.RestoreSnapshotState_RestoreSnapshotFailed
	default:
		return datapb.RestoreSnapshotState_RestoreSnapshotNone
	}
}

// calculateProgress computes the restore progress as a percentage (0-100).
// This eliminates code duplication between GetRestoreState and ListRestoreJobs.
func (sm *snapshotManager) calculateProgress(job CopySegmentJob) int32 {
	if job.GetTotalSegments() > 0 {
		return int32((job.GetCopiedSegments() * 100) / job.GetTotalSegments())
	}
	return 100
}

// calculateTimeCost computes the time cost in milliseconds.
// This eliminates code duplication between GetRestoreState and ListRestoreJobs.
func (sm *snapshotManager) calculateTimeCost(job CopySegmentJob) uint64 {
	if job.GetStartTs() > 0 && job.GetCompleteTs() > 0 {
		return (job.GetCompleteTs() - job.GetStartTs()) / 1e6 // Convert nanoseconds to milliseconds
	}
	return 0
}

// checkJSONPathIndexVersion rejects JSON path indexes with STL_SORT, BITMAP,
// or HYBRID if the cluster's scalar index engine version is below
// MinScalarIndexVersionForJsonPathMultiType.
func (sm *snapshotManager) checkJSONPathIndexVersion(index *model.Index) error {
	indexType := GetIndexType(index.IndexParams)
	if indexType != indexparamcheck.IndexSTLSORT &&
		indexType != indexparamcheck.IndexBitmap &&
		indexType != indexparamcheck.IndexHybrid {
		return nil
	}

	indexParams := funcutil.KeyValuePair2Map(index.IndexParams)
	if _, hasPath := indexParams[common.JSONPathKey]; !hasPath {
		return nil
	}

	if sm.indexEngineVersionManager != nil {
		resolved := sm.indexEngineVersionManager.ResolveScalarIndexVersion()
		if resolved < common.MinScalarIndexVersionForJsonPathMultiType {
			return merr.WrapErrParameterInvalidMsg(
				"JSON path index with %s requires scalar index engine version >= %d, "+
					"current resolved version: %d; please complete the rolling upgrade first",
				indexType, common.MinScalarIndexVersionForJsonPathMultiType, resolved)
		}
	}
	return nil
}
