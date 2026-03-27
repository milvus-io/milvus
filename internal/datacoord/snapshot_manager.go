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
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

// RollbackFunc performs rollback on restore failure.
// Used by RestoreSnapshot to delegate collection cleanup to the caller (Server).
type RollbackFunc func(ctx context.Context, dbName, collectionName string) error

// ValidateResourcesFunc validates that all required resources exist.
// Used by RestoreSnapshot to validate snapshot, collection, partitions, and indexes.
type ValidateResourcesFunc func(ctx context.Context, collectionID int64, snapshotData *SnapshotData) error

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
	CreateSnapshot(ctx context.Context, collectionID int64, name, description string, compactionProtectionSeconds int64) (int64, error)

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
	DescribeSnapshot(ctx context.Context, collectionID int64, name string) (*SnapshotData, error)

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
	RestoreCollection(ctx context.Context, snapshotData *SnapshotData, targetCollectionName, targetDbName string) (int64, error)

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
	RestoreIndexes(ctx context.Context, snapshotData *SnapshotData, collectionID int64, startBroadcaster StartBroadcasterFunc, snapshotName string) error

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
	ReadSnapshotData(ctx context.Context, collectionID int64, snapshotName string) (*SnapshotData, error)

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

	// Helper closures
	getChannelsByCollectionID func(context.Context, int64) ([]RWChannel, error) // For channel mapping

	// Concurrency control
	// createSnapshotMu protects CreateSnapshot to prevent TOCTOU race on snapshot name uniqueness
	createSnapshotMu sync.Mutex
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
) SnapshotManager {
	return &snapshotManager{
		meta:                      meta,
		snapshotMeta:              snapshotMeta,
		copySegmentMeta:           copySegmentMeta,
		allocator:                 allocator,
		handler:                   handler,
		broker:                    broker,
		getChannelsByCollectionID: getChannelsFunc,
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
) (int64, error) {
	// Lock to prevent TOCTOU race on snapshot name uniqueness check
	sm.createSnapshotMu.Lock()
	defer sm.createSnapshotMu.Unlock()

	log := log.Ctx(ctx).With(zap.Int64("collectionID", collectionID), zap.String("name", name))
	log.Info("create snapshot request received",
		zap.String("description", description),
		zap.Int64("compactionProtectionSeconds", compactionProtectionSeconds))

	// Validate snapshot name uniqueness within collection (protected by createSnapshotMu)
	if _, err := sm.snapshotMeta.GetSnapshot(ctx, collectionID, name); err == nil {
		return 0, merr.WrapErrParameterInvalidMsg("snapshot name %s already exists", name)
	}

	// Block compaction commit for this collection during snapshot creation.
	// This MUST be unconditional (not gated on compactionProtectionSeconds): even when
	// the user requests zero long-term protection, the snapshot must still be atomic
	// within the GenSnapshot → SaveSnapshot window, otherwise concurrent compaction
	// could drop segments that the in-flight snapshot is about to reference, leaving
	// the freshly-created snapshot immediately broken.
	sm.snapshotMeta.SetSnapshotPending(collectionID)
	defer sm.snapshotMeta.ClearSnapshotPending(collectionID)

	// Allocate snapshot ID
	snapshotID, err := sm.allocator.AllocID(ctx)
	if err != nil {
		log.Error("failed to allocate snapshot ID", zap.Error(err))
		return 0, err
	}

	// Generate snapshot data
	snapshotData, err := sm.handler.GenSnapshot(ctx, collectionID)
	if err != nil {
		log.Error("failed to generate snapshot", zap.Error(err))
		return 0, err
	}

	// Set snapshot metadata
	snapshotData.SnapshotInfo.Id = snapshotID
	snapshotData.SnapshotInfo.Name = name
	snapshotData.SnapshotInfo.Description = description

	// Set compaction protection if requested
	if compactionProtectionSeconds > 0 {
		snapshotData.SnapshotInfo.CompactionExpireTime = uint64(time.Now().Unix()) + uint64(compactionProtectionSeconds)
	}

	// Save to storage
	if err := sm.snapshotMeta.SaveSnapshot(ctx, snapshotData); err != nil {
		log.Error("failed to save snapshot", zap.Error(err))
		return 0, err
	}

	log.Info("snapshot created successfully", zap.Int64("snapshotID", snapshotID))
	return snapshotID, nil
}

// DropSnapshot deletes an existing snapshot by name.
// This operation is idempotent - if the snapshot doesn't exist, it returns nil.
func (sm *snapshotManager) DropSnapshot(ctx context.Context, collectionID int64, name string) error {
	log := log.Ctx(ctx).With(zap.String("snapshot", name), zap.Int64("collectionID", collectionID))
	log.Info("drop snapshot request received")

	// Check if snapshot exists first (idempotent for not-found, propagate other errors)
	_, err := sm.snapshotMeta.GetSnapshot(ctx, collectionID, name)
	if err != nil {
		if errors.Is(err, merr.ErrSnapshotNotFound) {
			log.Info("snapshot not found, skip drop (idempotent)")
			return nil
		}
		return err
	}

	// Delete snapshot
	if err := sm.snapshotMeta.DropSnapshot(ctx, collectionID, name); err != nil {
		log.Error("failed to drop snapshot", zap.Error(err))
		return err
	}

	deleteSnapshotActivePinsGauge(collectionID, name)
	log.Info("snapshot dropped successfully")
	return nil
}

// DropSnapshotsByCollection deletes all snapshots for a collection and drops
// their active_pins gauge series. The meta layer returns the names it
// successfully dropped (pinned/not-found/failed ones are excluded), so metric
// cleanup is symmetric with the per-snapshot DropSnapshot path.
func (sm *snapshotManager) DropSnapshotsByCollection(ctx context.Context, collectionID int64) error {
	log := log.Ctx(ctx).With(zap.Int64("collectionID", collectionID))
	log.Info("drop all snapshots for collection")

	dropped, err := sm.snapshotMeta.DropSnapshotsByCollection(ctx, collectionID)
	// Clear metric series for whatever was dropped, even if the overall call
	// returned an error for other snapshots in the batch.
	for _, n := range dropped {
		deleteSnapshotActivePinsGauge(collectionID, n)
	}
	if err != nil {
		log.Error("failed to drop snapshots for collection", zap.Error(err))
		return err
	}

	log.Info("all snapshots dropped for collection", zap.Int("droppedCount", len(dropped)))
	return nil
}

// GetSnapshot retrieves basic snapshot metadata by name within a collection.
func (sm *snapshotManager) GetSnapshot(ctx context.Context, collectionID int64, name string) (*datapb.SnapshotInfo, error) {
	return sm.snapshotMeta.GetSnapshot(ctx, collectionID, name)
}

// DescribeSnapshot retrieves detailed information about a snapshot within a collection.
func (sm *snapshotManager) DescribeSnapshot(ctx context.Context, collectionID int64, name string) (*SnapshotData, error) {
	log := log.Ctx(ctx).With(zap.String("snapshotName", name), zap.Int64("collectionID", collectionID))
	log.Info("describe snapshot request received")

	// Read snapshot data with full segment information
	snapshotData, err := sm.snapshotMeta.ReadSnapshotData(ctx, collectionID, name, false)
	if err != nil {
		log.Error("failed to read snapshot data", zap.Error(err))
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
	log := log.Ctx(ctx).With(zap.Int64("collectionID", collectionID), zap.Int64("dbID", dbID))
	log.Info("list snapshots request received")

	snapshots, err := sm.snapshotMeta.ListSnapshots(ctx, collectionID, partitionID)
	if err != nil {
		log.Error("failed to list snapshots", zap.Error(err))
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
	snapshotData *SnapshotData,
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
		return fmt.Errorf("failed to describe target database %s: %w", targetDbName, err)
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
	log := log.Ctx(ctx).With(
		zap.String("snapshotName", snapshotName),
		zap.Int64("sourceCollectionID", sourceCollectionID),
		zap.String("targetCollection", targetCollectionName),
		zap.String("targetDb", targetDbName),
	)

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
		return 0, fmt.Errorf("failed to acquire restore lock: %w", err)
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
		return 0, fmt.Errorf("failed to pin source snapshot under restore lock: %w", err)
	}
	setSnapshotActivePinsGauge(sourceCollectionID, snapshotName, activePins)
	phase0Lock.Close()
	log.Info("source snapshot pinned under phase 0 lock", zap.Int64("pinID", pinID))

	// If any subsequent phase fails, release the pin we just claimed. On the
	// success path, ownership of the pin is transferred to the copy segment
	// job (job.PinId), which releases it upon terminal state transition. We
	// flip this flag to false before the successful return.
	pinOwned := true
	defer func() {
		if pinOwned {
			collID, snapName, remaining, unpinErr := sm.snapshotMeta.UnpinSnapshot(ctx, pinID)
			if unpinErr != nil {
				log.Warn("failed to release pin on failure path",
					zap.Int64("pinID", pinID), zap.Error(unpinErr))
				return
			}
			if snapName != "" {
				setSnapshotActivePinsGauge(collID, snapName, remaining)
			}
			log.Info("released pin on failure path", zap.Int64("pinID", pinID))
		}
	}()

	// Phase 1: Read snapshot data (now protected by the refcount guard)
	snapshotData, err := sm.ReadSnapshotData(ctx, sourceCollectionID, snapshotName)
	if err != nil {
		return 0, fmt.Errorf("failed to read snapshot data: %w", err)
	}
	log.Info("snapshot data loaded",
		zap.Int("segmentCount", len(snapshotData.Segments)),
		zap.Int("indexCount", len(snapshotData.Indexes)))

	// Phase 1.5: Validate CMEK compatibility
	// CMEK-encrypted collections can only be restored to databases with matching encryption zone
	if err := sm.validateCMEKCompatibility(ctx, snapshotData, targetDbName); err != nil {
		log.Warn("CMEK compatibility validation failed", zap.Error(err))
		return 0, err
	}

	// Phase 2: Restore collection and partitions
	collectionID, err := sm.RestoreCollection(ctx, snapshotData, targetCollectionName, targetDbName)
	if err != nil {
		return 0, fmt.Errorf("failed to restore collection: %w", err)
	}
	log.Info("collection and partitions restored", zap.Int64("collectionID", collectionID))

	// Phase 3: Restore indexes
	// Note: Each broadcaster can only be used once, so we pass the factory function
	if err := sm.RestoreIndexes(ctx, snapshotData, collectionID, startBroadcaster, snapshotName); err != nil {
		log.Error("failed to restore indexes, rolling back", zap.Error(err))
		if rollbackErr := rollback(ctx, targetDbName, targetCollectionName); rollbackErr != nil {
			log.Error("rollback failed", zap.Error(rollbackErr))
		}
		return 0, fmt.Errorf("failed to restore indexes: %w", err)
	}
	log.Info("indexes restored", zap.Int("indexCount", len(snapshotData.Indexes)))

	// Phase 4: Pre-allocate job ID and broadcast restore message
	// Pre-allocating jobID ensures idempotency when WAL is replayed after restart
	jobID, err = sm.allocator.AllocID(ctx)
	if err != nil {
		log.Error("failed to allocate job ID, rolling back", zap.Error(err))
		if rollbackErr := rollback(ctx, targetDbName, targetCollectionName); rollbackErr != nil {
			log.Error("rollback failed", zap.Error(rollbackErr))
		}
		return 0, fmt.Errorf("failed to allocate job ID: %w", err)
	}
	log.Info("pre-allocated job ID for restore", zap.Int64("jobID", jobID))

	// Create broadcaster for restore message
	restoreBroadcaster, err := startBroadcaster(ctx, collectionID, snapshotName)
	if err != nil {
		log.Error("failed to start broadcaster for restore message, rolling back", zap.Error(err))
		if rollbackErr := rollback(ctx, targetDbName, targetCollectionName); rollbackErr != nil {
			log.Error("rollback failed", zap.Error(rollbackErr))
		}
		return 0, fmt.Errorf("failed to start broadcaster for restore message: %w", err)
	}
	defer func() {
		if restoreBroadcaster != nil {
			restoreBroadcaster.Close()
		}
	}()

	// Validate resources while holding broadcast lock to prevent concurrent
	// modifications between validation and message broadcast (TOCTOU race).
	if valErr := validateResources(ctx, collectionID, snapshotData); valErr != nil {
		log.Error("resource validation failed, rolling back", zap.Error(valErr))
		// Release broadcast lock before rollback: rollback calls DropCollection
		// which requires its own WAL broadcast lock on the same collection.
		restoreBroadcaster.Close()
		restoreBroadcaster = nil
		if rollbackErr := rollback(ctx, targetDbName, targetCollectionName); rollbackErr != nil {
			log.Error("rollback failed", zap.Error(rollbackErr))
		}
		err = fmt.Errorf("resource validation failed: %w", valErr)
		return 0, err
	}

	msg := message.NewRestoreSnapshotMessageBuilderV2().
		WithHeader(&message.RestoreSnapshotMessageHeader{
			SnapshotName:       snapshotName,
			CollectionId:       collectionID,
			JobId:              jobID,
			SourceCollectionId: sourceCollectionID,
			PinId:              pinID,
		}).
		WithBody(&message.RestoreSnapshotMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()

	if _, bcErr := restoreBroadcaster.Broadcast(ctx, msg); bcErr != nil {
		log.Error("failed to broadcast restore message, rolling back", zap.Error(bcErr))
		// Release broadcast lock before rollback: rollback calls DropCollection
		// which requires its own WAL broadcast lock on the same collection.
		restoreBroadcaster.Close()
		restoreBroadcaster = nil
		if rollbackErr := rollback(ctx, targetDbName, targetCollectionName); rollbackErr != nil {
			log.Error("rollback failed", zap.Error(rollbackErr))
		}
		err = fmt.Errorf("failed to broadcast restore message: %w", bcErr)
		return 0, err
	}

	// Success path: transfer ownership of the pin to the copy segment job
	// (job.PinId). The job's state machine will Unpin upon terminal transition
	// via UpdateJobStateAndReleaseRef.
	pinOwned = false

	log.Info("restore snapshot completed", zap.Int64("collectionID", collectionID), zap.Int64("jobID", jobID))
	return jobID, nil
}

// RestoreCollection creates a new collection and its user partitions based on snapshot data.
func (sm *snapshotManager) RestoreCollection(
	ctx context.Context,
	snapshotData *SnapshotData,
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
	properties := collection.Properties
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
	snapshotData *SnapshotData,
	collectionID int64,
	startBroadcaster StartBroadcasterFunc,
	snapshotName string,
) error {
	// Get collection info for dbId
	coll, err := sm.broker.DescribeCollectionInternal(ctx, collectionID)
	if err != nil {
		return fmt.Errorf("failed to describe collection %d: %w", collectionID, err)
	}

	for _, indexInfo := range snapshotData.Indexes {
		// Allocate new index ID
		indexID, err := sm.allocator.AllocID(ctx)
		if err != nil {
			return fmt.Errorf("failed to allocate index ID: %w", err)
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
		if err := ValidateIndexParams(index); err != nil {
			return fmt.Errorf("failed to validate index %s: %w", indexInfo.GetIndexName(), err)
		}

		// Create a new broadcaster for each index
		// (each broadcaster can only be used once due to resource key lock consumption)
		b, err := startBroadcaster(ctx, collectionID, snapshotName)
		if err != nil {
			return fmt.Errorf("failed to start broadcaster for index %s: %w", indexInfo.GetIndexName(), err)
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
			return fmt.Errorf("failed to broadcast create index %s: %w", indexInfo.GetIndexName(), err)
		}

		log.Ctx(ctx).Info("index restored via DDL WAL broadcast",
			zap.String("indexName", indexInfo.GetIndexName()),
			zap.Int64("fieldID", indexInfo.GetFieldID()),
			zap.Int64("indexID", indexID))
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
	log := log.Ctx(ctx).With(
		zap.String("snapshot", snapshotName),
		zap.Int64("sourceCollectionID", sourceCollectionID),
		zap.Int64("collectionID", collectionID),
		zap.Int64("jobID", jobID),
	)
	log.Info("restore data started")

	// ========== Phase 1: Idempotency check ==========
	// Check if job already exists (WAL replay scenario)
	existingJob := sm.copySegmentMeta.GetJob(ctx, jobID)
	if existingJob != nil {
		log.Info("job already exists, skip creation (idempotent)")
		return jobID, nil
	}

	snapshotData, err := sm.ReadSnapshotData(ctx, sourceCollectionID, snapshotName)
	if err != nil {
		log.Error("failed to read snapshot data", zap.Error(err))
		return 0, fmt.Errorf("failed to read snapshot data: %w", err)
	}

	// ========== Phase 2: Build partition mapping ==========
	partitionMapping, err := sm.buildPartitionMapping(ctx, snapshotData, collectionID)
	if err != nil {
		log.Error("failed to build partition mapping", zap.Error(err))
		return 0, fmt.Errorf("partition mapping failed: %w", err)
	}
	log.Info("partition mapping built", zap.Any("partitionMapping", partitionMapping))

	// ========== Phase 3: Build channel mapping ==========
	channelMapping, err := sm.buildChannelMapping(ctx, snapshotData, collectionID)
	if err != nil {
		log.Error("failed to build channel mapping", zap.Error(err))
		return 0, fmt.Errorf("channel mapping failed: %w", err)
	}

	// ========== Phase 4: Create copy segment job ==========
	// Use the pre-allocated jobID from the WAL message
	if err := sm.createRestoreJob(ctx, collectionID, channelMapping, partitionMapping, snapshotData, jobID, pinID); err != nil {
		log.Error("failed to create restore job", zap.Error(err))
		return 0, fmt.Errorf("restore job creation failed: %w", err)
	}

	log.Info("restore data completed successfully",
		zap.Int64("jobID", jobID),
		zap.Int64("collectionID", collectionID))

	return jobID, nil
}

// ============================================================================
// Restore Helper Functions (private)
// ============================================================================

// restoreUserPartitions creates user partitions based on snapshot data.
// It creates partitions that exist in the snapshot but not in the target collection.
func (sm *snapshotManager) restoreUserPartitions(
	ctx context.Context,
	snapshotData *SnapshotData,
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
			return fmt.Errorf("failed to create partition %s: %w", partitionName, err)
		}
	}

	return nil
}

// buildPartitionMapping builds a mapping from snapshot partition IDs to target partition IDs.
func (sm *snapshotManager) buildPartitionMapping(
	ctx context.Context,
	snapshotData *SnapshotData,
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
			return nil, merr.WrapErrServiceInternal(
				fmt.Sprintf("partition %s from snapshot not found in target collection", partitionName))
		}
		partitionMapping[partitionID] = targetPartID
	}

	return partitionMapping, nil
}

// buildChannelMapping generates a mapping from snapshot channels to target collection channels.
// It ensures that the channel count matches and returns a sorted mapping.
func (sm *snapshotManager) buildChannelMapping(
	ctx context.Context,
	snapshotData *SnapshotData,
	targetCollectionID int64,
) (map[string]string, error) {
	if len(snapshotData.Segments) == 0 {
		return make(map[string]string), nil
	}

	snapshotChannels := snapshotData.Collection.VirtualChannelNames

	// Get target collection channels
	targetChannels, err := sm.getChannelsByCollectionID(ctx, targetCollectionID)
	if err != nil {
		log.Ctx(ctx).Error("failed to get channels by collection ID", zap.Error(err))
		return nil, err
	}

	// Validate count
	if len(targetChannels) != len(snapshotChannels) {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf("channel count mismatch between snapshot and target collection: snapshot=%d, target=%d",
				len(snapshotChannels), len(targetChannels)))
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
	snapshotData *SnapshotData,
	jobID int64,
	pinID int64,
) error {
	log := log.Ctx(ctx).With(
		zap.String("snapshotName", snapshotData.SnapshotInfo.GetName()),
		zap.Int64("targetCollectionID", targetCollection),
		zap.Int64("jobID", jobID),
		zap.Any("channelMapping", channelMapping),
		zap.Any("partitionMapping", partitionMapping),
	)

	// Validate which segments exist in meta
	validSegments := make([]*datapb.SegmentDescription, 0, len(snapshotData.Segments))
	for _, segDesc := range snapshotData.Segments {
		sourceSegmentID := segDesc.GetSegmentId()
		segInfo := sm.meta.GetSegment(ctx, sourceSegmentID)
		if segInfo == nil {
			log.Warn("source segment not found in meta, skipping",
				zap.Int64("sourceSegmentID", sourceSegmentID))
			continue
		}
		validSegments = append(validSegments, segDesc)
	}

	// Allocate target segment IDs
	// AllocN returns (start, end, error), where end = start + count
	targetSegmentIDStart, _, err := sm.allocator.AllocN(int64(len(validSegments)))
	if err != nil {
		log.Error("failed to allocate segment IDs", zap.Error(err))
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
				err := merr.WrapErrServiceInternal(
					fmt.Sprintf("partition mapping not found for segment: sourceSegmentID=%d, sourcePartitionID=%d",
						sourceSegmentID, sourcePartitionID))
				log.Error("partition mapping missing", zap.Error(err))
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
			err := merr.WrapErrServiceInternal(
				fmt.Sprintf("channel mapping missing for channel: %s", segDesc.GetChannelName()))
			log.Error("channel mapping not found", zap.Error(err))
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

		// Pre-register target segment in meta
		newSegment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
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
			},
		}
		targetSegments[targetSegmentID] = newSegment
	}

	// Pre-register all target segments in meta to ensure they exist when copy tasks run
	for _, targetSegment := range targetSegments {
		if err := sm.meta.AddSegment(ctx, targetSegment); err != nil {
			log.Error("failed to pre-register target segment", zap.Error(err))
			return err
		}
	}

	// Pre-register channel's checkpoint
	collection, err := sm.handler.GetCollection(ctx, targetCollection)
	if err != nil {
		log.Error("failed to get collection", zap.Error(err))
		return err
	}
	for _, channel := range channelMapping {
		startPosition := toMsgPosition(channel, collection.StartPositions)
		if err := sm.meta.UpdateChannelCheckpoint(ctx, channel, startPosition); err != nil {
			log.Error("failed to pre-register channel checkpoint", zap.Error(err))
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
			TimeoutTs:    uint64(time.Now().Add(jobTimeout).UnixNano()),
			StartTs:      uint64(time.Now().UnixNano()),
			Options: []*commonpb.KeyValuePair{
				{Key: "copy_index", Value: "true"},
				{Key: "source_type", Value: "snapshot"},
			},
			TotalSegments:      int64(len(idMappings)),
			TotalRows:          totalRows,
			SnapshotName:       snapshotData.SnapshotInfo.GetName(),
			SourceCollectionId: snapshotData.SnapshotInfo.GetCollectionId(),
			PinId:              pinID,
		},
		tr: timerecord.NewTimeRecorder("copy segment job"),
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
		log.Error("failed to save copy segment job",
			zap.Int64("sourceCollectionID", snapshotData.SnapshotInfo.GetCollectionId()),
			zap.String("snapshot", snapshotData.SnapshotInfo.GetName()), zap.Error(err))
		return err
	}

	log.Info("copy segment job created successfully",
		zap.Int64("jobID", jobID),
		zap.Int("totalSegments", len(idMappings)))

	return nil
}

// ============================================================================
// Restore State Query
// ============================================================================

// ReadSnapshotData reads snapshot data from storage.
// This is a convenience wrapper for snapshotMeta.ReadSnapshotData.
func (sm *snapshotManager) ReadSnapshotData(ctx context.Context, collectionID int64, snapshotName string) (*SnapshotData, error) {
	return sm.snapshotMeta.ReadSnapshotData(ctx, collectionID, snapshotName, true)
}

// GetRestoreState retrieves the current state of a restore job.
func (sm *snapshotManager) GetRestoreState(ctx context.Context, jobID int64) (*datapb.RestoreSnapshotInfo, error) {
	log := log.Ctx(ctx).With(zap.Int64("jobID", jobID))

	// Get job
	job := sm.copySegmentMeta.GetJob(ctx, jobID)
	if job == nil {
		err := merr.WrapErrImportFailed(fmt.Sprintf("restore job not found: jobID=%d", jobID))
		log.Warn("restore job not found")
		return nil, err
	}

	// Build restore info using centralized helper
	restoreInfo := sm.buildRestoreInfo(job)

	log.Info("get restore state completed",
		zap.String("state", restoreInfo.GetState().String()),
		zap.Int32("progress", restoreInfo.GetProgress()))

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

	log.Ctx(ctx).Info("list restore jobs completed",
		zap.Int("totalJobs", len(restoreInfos)),
		zap.Int64("filterCollectionId", collectionIDFilter),
		zap.Int64("filterDbId", dbID))

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
