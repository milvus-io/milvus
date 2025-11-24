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
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
)

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

	// RestoreSnapshot initiates a snapshot restore operation to a target collection.
	// It creates channel/partition mappings, validates compatibility, pre-registers target segments,
	// and creates a copy segment job for background execution.
	//
	// Note: Index creation is NOT handled by this method. The caller (services.go)
	// is responsible for creating indexes before calling this method.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - snapshotName: Name of the snapshot to restore
	//   - targetCollectionID: ID of the collection to restore data into
	//   - jobID: Pre-allocated job ID from RootCoord (must be > 0)
	//
	// Returns:
	//   - error: If snapshot not found, channel/partition mismatch, or job creation fails
	//
	// Process flow:
	//  1. Read snapshot data
	//  2. Generate channel mapping (snapshot channels → target channels)
	//  3. Generate partition mapping (snapshot partitions → target partitions)
	//  4. Validate channel and partition count matches
	//  5. Create restore job with ID mappings
	//  6. Pre-register target segments in Importing state
	//  7. Background checker/inspector will execute the job
	RestoreSnapshot(ctx context.Context, snapshotName string, targetCollectionID int64, jobID int64) error

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
}

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

// CreateSnapshot creates a new snapshot for the specified collection.
func (sm *snapshotManager) CreateSnapshot(
	ctx context.Context,
	collectionID int64,
	name, description string,
) (int64, error) {
	log := log.Ctx(ctx).With(zap.Int64("collectionID", collectionID), zap.String("name", name))
	log.Info("create snapshot request received", zap.String("description", description))

	// Validate snapshot name uniqueness
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

// RestoreSnapshot initiates a snapshot restore operation to a target collection.
// Note: Index creation is handled by the caller (services.go), not this method.
// The jobID must be pre-allocated by the caller (RootCoord) and passed in.
func (sm *snapshotManager) RestoreSnapshot(
	ctx context.Context,
	snapshotName string,
	targetCollectionID int64,
	jobID int64,
) error {
	log := log.Ctx(ctx).With(
		zap.String("snapshot", snapshotName),
		zap.Int64("targetCollectionID", targetCollectionID),
		zap.Int64("jobID", jobID))
	log.Info("restore snapshot request received")

	// Validate jobID (defensive check - services.go also validates, but interface should enforce contract)
	if jobID <= 0 {
		log.Warn("invalid jobID")
		return merr.WrapErrParameterInvalidMsg("jobID must be > 0")
	}

	// Read snapshot data with full segment information for restore
	snapshotData, err := sm.snapshotMeta.ReadSnapshotData(ctx, snapshotName, true)
	if err != nil {
		log.Error("failed to read snapshot data", zap.Error(err))
		return err
	}

	// Generate channel mapping
	channelMapping, err := sm.buildChannelMapping(ctx, snapshotData, targetCollectionID)
	if err != nil {
		return err
	}

	// Generate partition mapping
	partitionMapping, err := sm.buildPartitionMapping(ctx, snapshotData, targetCollectionID)
	if err != nil {
		return err
	}

	// Create restore job using the provided jobID
	if err := sm.createRestoreJob(ctx, targetCollectionID, channelMapping, partitionMapping, snapshotData, jobID); err != nil {
		log.Error("failed to create restore job", zap.Error(err))
		return err
	}

	log.Info("restore job created successfully")
	return nil
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

// buildChannelMapping generates a mapping from snapshot channels to target collection channels.
// It maps channels by pchannel to preserve pchannel relationships from the source collection.
// For backward compatibility with old snapshots, falls back to sorted mapping if pchannel mapping fails.
func (sm *snapshotManager) buildChannelMapping(
	ctx context.Context,
	snapshotData *SnapshotData,
	targetCollectionID int64,
) (map[string]string, error) {
	channelMapping := make(map[string]string)

	if len(snapshotData.Segments) == 0 {
		return channelMapping, nil
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

	// Build pchannel -> target vchannel map
	pchannelToTarget := make(map[string]string)
	for _, ch := range targetChannels {
		pchannel := funcutil.ToPhysicalChannel(ch.GetName())
		pchannelToTarget[pchannel] = ch.GetName()
	}

	// Map snapshot vchannel to target vchannel by pchannel
	for _, snapshotVChannel := range snapshotData.Collection.VirtualChannelNames {
		pchannel := funcutil.ToPhysicalChannel(snapshotVChannel)
		targetVChannel, ok := pchannelToTarget[pchannel]
		if !ok {
			// Fallback to sorted mapping for backward compatibility with old snapshots
			// where vchannels may not preserve pchannel relationships
			log.Ctx(ctx).Warn("pchannel not found in target collection",
				zap.String("snapshotVChannel", snapshotVChannel),
				zap.String("pchannel", pchannel))
			return nil, merr.WrapErrServiceInternal(
				fmt.Sprintf("pchannel not found in target collection: snapshotVChannel=%s, pchannel=%s",
					snapshotVChannel, pchannel))
		}
		channelMapping[snapshotVChannel] = targetVChannel
	}

	return channelMapping, nil
}

// buildPartitionMapping generates a mapping from snapshot partitions to target collection partitions.
// It ensures that the partition count matches and returns a sorted mapping.
func (sm *snapshotManager) buildPartitionMapping(
	ctx context.Context,
	snapshotData *SnapshotData,
	targetCollectionID int64,
) (map[int64]int64, error) {
	partitionsInSnapshot := snapshotData.SnapshotInfo.GetPartitionIds()

	// Get target collection partitions
	targetPartitions, err := sm.broker.ShowPartitionsInternal(ctx, targetCollectionID)
	if err != nil {
		return nil, err
	}

	// Validate count
	if len(targetPartitions) != len(partitionsInSnapshot) {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf("partition count mismatch between snapshot and target collection: snapshot=%d, target=%d",
				len(partitionsInSnapshot), len(targetPartitions)))
	}

	// Build mapping (sorted)
	sort.Slice(partitionsInSnapshot, func(i, j int) bool {
		return partitionsInSnapshot[i] < partitionsInSnapshot[j]
	})
	sort.Slice(targetPartitions, func(i, j int) bool {
		return targetPartitions[i] < targetPartitions[j]
	})

	mapping := make(map[int64]int64)
	for i, partition := range targetPartitions {
		mapping[partitionsInSnapshot[i]] = partition
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

	// Create copy segment job
	copyJob := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        jobID,
			CollectionId: targetCollection,
			State:        datapb.CopySegmentJobState_CopySegmentJobPending,
			IdMappings:   idMappings,
			TimeoutTs:    uint64(time.Now().Add(5 * time.Minute).UnixNano()),
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
	return 0
}

// calculateTimeCost computes the time cost in milliseconds.
// This eliminates code duplication between GetRestoreState and ListRestoreJobs.
func (sm *snapshotManager) calculateTimeCost(job CopySegmentJob) uint64 {
	if job.GetStartTs() > 0 && job.GetCompleteTs() > 0 {
		return uint64((job.GetCompleteTs() - job.GetStartTs()) / 1e6) // Convert nanoseconds to milliseconds
	}
	return 0
}
