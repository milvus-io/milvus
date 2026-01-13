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
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
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

// RollbackFunc performs rollback on restore failure.
// Used by RestoreSnapshot to delegate collection cleanup to the caller (Server).
type RollbackFunc func(ctx context.Context, dbName, collectionName string) error

// ValidateResourcesFunc validates that all required resources exist.
// Used by RestoreSnapshot to validate partitions and indexes after creation.
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
	CreateSnapshot(ctx context.Context, collectionID int64, name, description string) (int64, error)

	// DropSnapshot deletes an existing snapshot by name.
	// It removes the snapshot from memory cache, etcd, and S3 storage.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - name: Name of the snapshot to delete
	//
	// Returns:
	//   - error: If snapshot not found or deletion fails
	DropSnapshot(ctx context.Context, name string) error

	// GetSnapshot retrieves basic snapshot metadata by name.
	// This is a lightweight operation that only reads from memory cache.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - name: Name of the snapshot
	//
	// Returns:
	//   - snapshotInfo: Basic snapshot metadata (id, name, collection_id, etc.)
	//   - error: If snapshot not found
	GetSnapshot(ctx context.Context, name string) (*datapb.SnapshotInfo, error)

	// DescribeSnapshot retrieves detailed information about a snapshot.
	// It reads the complete snapshot data from S3, including segments, indexes, and schema.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - name: Name of the snapshot to describe
	//
	// Returns:
	//   - snapshotData: Complete snapshot data with collection info and index info
	//   - error: If snapshot not found or read fails
	DescribeSnapshot(ctx context.Context, name string) (*SnapshotData, error)

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
	ListSnapshots(ctx context.Context, collectionID, partitionID int64) ([]string, error)

	// Restore operations

	// RestoreSnapshot orchestrates the complete snapshot restoration process.
	// It reads snapshot data, creates collection/partitions/indexes, validates resources,
	// and broadcasts the restore message.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - snapshotName: Name of the snapshot to restore
	//   - targetCollectionName: Name for the restored collection
	//   - targetDbName: Database name for the restored collection
	//   - startBroadcaster: Function to start a broadcaster for DDL operations
	//   - rollback: Function to rollback on failure (drops collection)
	//   - validateResources: Function to validate that all resources exist
	//
	// Returns:
	//   - jobID: ID of the restore job (can be used for progress tracking)
	//   - error: If any step fails
	RestoreSnapshot(
		ctx context.Context,
		snapshotName string,
		targetCollectionName string,
		targetDbName string,
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
	//   - snapshotName: Name of the snapshot to restore
	//   - collectionID: ID of the target collection (already created)
	//   - jobID: Pre-allocated job ID for idempotency (from WAL message)
	//
	// Returns:
	//   - jobID: The restore job ID (same as input if job created, or existing job ID)
	//   - error: If mapping fails or job creation fails
	RestoreData(ctx context.Context, snapshotName string, collectionID int64, jobID int64) (int64, error)

	// Restore state query

	// ReadSnapshotData reads complete snapshot data from storage.
	// This is used by services.go to get snapshot data before calling RestoreData.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - snapshotName: Name of the snapshot to read
	//
	// Returns:
	//   - snapshotData: Complete snapshot data including segments, indexes, schema
	//   - error: If snapshot not found or read fails
	ReadSnapshotData(ctx context.Context, snapshotName string) (*SnapshotData, error)

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

	// ListRestoreJobs returns a list of all restore jobs, optionally filtered by collection ID.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - collectionIDFilter: Filter by collection ID (0 = all jobs)
	//
	// Returns:
	//   - restoreInfos: List of restore job information
	//   - error: If listing fails
	ListRestoreJobs(ctx context.Context, collectionIDFilter int64) ([]*datapb.RestoreSnapshotInfo, error)
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
//   - snapshotMeta: Snapshot metadata manager
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
) (int64, error) {
	// Lock to prevent TOCTOU race on snapshot name uniqueness check
	sm.createSnapshotMu.Lock()
	defer sm.createSnapshotMu.Unlock()

	log := log.Ctx(ctx).With(zap.Int64("collectionID", collectionID), zap.String("name", name))
	log.Info("create snapshot request received", zap.String("description", description))

	// Validate snapshot name uniqueness (protected by createSnapshotMu)
	if _, err := sm.snapshotMeta.GetSnapshot(ctx, name); err == nil {
		return 0, merr.WrapErrParameterInvalidMsg("snapshot name %s already exists", name)
	}

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
func (sm *snapshotManager) DropSnapshot(ctx context.Context, name string) error {
	log := log.Ctx(ctx).With(zap.String("snapshot", name))
	log.Info("drop snapshot request received")

	// Check if snapshot exists first (idempotent)
	if _, err := sm.snapshotMeta.GetSnapshot(ctx, name); err != nil {
		log.Info("snapshot not found, skip drop (idempotent)")
		return nil
	}

	// Delete snapshot
	if err := sm.snapshotMeta.DropSnapshot(ctx, name); err != nil {
		log.Error("failed to drop snapshot", zap.Error(err))
		return err
	}

	log.Info("snapshot dropped successfully")
	return nil
}

// GetSnapshot retrieves basic snapshot metadata by name.
func (sm *snapshotManager) GetSnapshot(ctx context.Context, name string) (*datapb.SnapshotInfo, error) {
	return sm.snapshotMeta.GetSnapshot(ctx, name)
}

// DescribeSnapshot retrieves detailed information about a snapshot.
func (sm *snapshotManager) DescribeSnapshot(ctx context.Context, name string) (*SnapshotData, error) {
	log := log.Ctx(ctx).With(zap.String("snapshotName", name))
	log.Info("describe snapshot request received")

	// Read snapshot data with full segment information
	snapshotData, err := sm.snapshotMeta.ReadSnapshotData(ctx, name, false)
	if err != nil {
		log.Error("failed to read snapshot data", zap.Error(err))
		return nil, err
	}

	return snapshotData, nil
}

// ListSnapshots returns a list of snapshot names for the specified collection/partition.
func (sm *snapshotManager) ListSnapshots(ctx context.Context, collectionID, partitionID int64) ([]string, error) {
	log := log.Ctx(ctx).With(zap.Int64("collectionID", collectionID))
	log.Info("list snapshots request received")

	// List snapshots
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

// RestoreSnapshot orchestrates the complete snapshot restoration process.
// It reads snapshot data, creates collection/partitions/indexes, validates resources,
// and broadcasts the restore message.
func (sm *snapshotManager) RestoreSnapshot(
	ctx context.Context,
	snapshotName string,
	targetCollectionName string,
	targetDbName string,
	startBroadcaster StartBroadcasterFunc,
	rollback RollbackFunc,
	validateResources ValidateResourcesFunc,
) (int64, error) {
	log := log.Ctx(ctx).With(
		zap.String("snapshotName", snapshotName),
		zap.String("targetCollection", targetCollectionName),
		zap.String("targetDb", targetDbName),
	)

	// Phase 1: Read snapshot data
	snapshotData, err := sm.ReadSnapshotData(ctx, snapshotName)
	if err != nil {
		return 0, fmt.Errorf("failed to read snapshot data: %w", err)
	}
	log.Info("snapshot data loaded",
		zap.Int("segmentCount", len(snapshotData.Segments)),
		zap.Int("indexCount", len(snapshotData.Indexes)))

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

	// Phase 4: Validate resources
	if err := validateResources(ctx, collectionID, snapshotData); err != nil {
		log.Error("resource validation failed, rolling back", zap.Error(err))
		if rollbackErr := rollback(ctx, targetDbName, targetCollectionName); rollbackErr != nil {
			log.Error("rollback failed", zap.Error(rollbackErr))
		}
		return 0, fmt.Errorf("resource validation failed: %w", err)
	}

	// Phase 5: Pre-allocate job ID and broadcast restore message
	// Pre-allocating jobID ensures idempotency when WAL is replayed after restart
	jobID, err := sm.allocator.AllocID(ctx)
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
	defer restoreBroadcaster.Close()

	msg := message.NewRestoreSnapshotMessageBuilderV2().
		WithHeader(&message.RestoreSnapshotMessageHeader{
			SnapshotName: snapshotName,
			CollectionId: collectionID,
			JobId:        jobID,
		}).
		WithBody(&message.RestoreSnapshotMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()

	if _, err := restoreBroadcaster.Broadcast(ctx, msg); err != nil {
		log.Error("failed to broadcast restore message, rolling back", zap.Error(err))
		if rollbackErr := rollback(ctx, targetDbName, targetCollectionName); rollbackErr != nil {
			log.Error("rollback failed", zap.Error(rollbackErr))
		}
		return 0, fmt.Errorf("failed to broadcast restore message: %w", err)
	}

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
	// and update the schema name to match the target collection name.
	// This is required because Milvus validates that CollectionName == Schema.Name.
	schema := proto.Clone(collection.Schema).(*schemapb.CollectionSchema)
	schema.Name = targetCollectionName

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
	snapshotName string,
	collectionID int64,
	jobID int64,
) (int64, error) {
	log := log.Ctx(ctx).With(
		zap.String("snapshot", snapshotName),
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

	snapshotData, err := sm.ReadSnapshotData(ctx, snapshotName)
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
	if err := sm.createRestoreJob(ctx, collectionID, channelMapping, partitionMapping, snapshotData, jobID); err != nil {
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
				Level:               datapb.SegmentLevel(segDesc.GetSegmentLevel()),
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
			TotalSegments: int64(len(idMappings)),
			TotalRows:     totalRows,
			SnapshotName:  snapshotData.SnapshotInfo.GetName(),
		},
		tr: timerecord.NewTimeRecorder("copy segment job"),
	}

	// Save job to metadata
	if err := sm.copySegmentMeta.AddJob(ctx, copyJob); err != nil {
		log.Error("failed to save copy segment job", zap.Error(err))
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
func (sm *snapshotManager) ReadSnapshotData(ctx context.Context, snapshotName string) (*SnapshotData, error) {
	return sm.snapshotMeta.ReadSnapshotData(ctx, snapshotName, true)
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

// ListRestoreJobs returns a list of all restore jobs, optionally filtered by collection ID.
func (sm *snapshotManager) ListRestoreJobs(
	ctx context.Context,
	collectionIDFilter int64,
) ([]*datapb.RestoreSnapshotInfo, error) {
	// Get all jobs
	jobs := sm.copySegmentMeta.GetJobBy(ctx)

	// Filter by collection and build restore info list
	restoreInfos := make([]*datapb.RestoreSnapshotInfo, 0)
	for _, job := range jobs {
		if collectionIDFilter != 0 && job.GetCollectionId() != collectionIDFilter {
			continue
		}

		restoreInfos = append(restoreInfos, sm.buildRestoreInfo(job))
	}

	log.Ctx(ctx).Info("list restore jobs completed",
		zap.Int("totalJobs", len(restoreInfos)),
		zap.Int64("filterCollectionId", collectionIDFilter))

	return restoreInfos, nil
}

// ============================================================================
// Common Helper Functions (private)
// ============================================================================

// buildRestoreInfo constructs a RestoreSnapshotInfo from a CopySegmentJob.
// This centralizes the conversion logic to eliminate code duplication.
func (sm *snapshotManager) buildRestoreInfo(job CopySegmentJob) *datapb.RestoreSnapshotInfo {
	return &datapb.RestoreSnapshotInfo{
		JobId:        job.GetJobId(),
		SnapshotName: job.GetSnapshotName(),
		CollectionId: job.GetCollectionId(),
		State:        sm.convertJobState(job.GetState()),
		Progress:     sm.calculateProgress(job),
		Reason:       job.GetReason(),
		TimeCost:     sm.calculateTimeCost(job),
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
		return uint64((job.GetCompleteTs() - job.GetStartTs()) / 1e6) // Convert nanoseconds to milliseconds
	}
	return 0
}
