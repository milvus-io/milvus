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

package segments

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

// GrowingFlushManager manages the incremental flush of Growing Segments for TEXT collections.
// it periodically checks sync policies and triggers flush operations when conditions are met.
//
// key responsibilities:
//   - track Growing Segments and their unflushed data via CheckpointTracker
//   - apply sync policies to determine which segments to flush
//   - extract data from Growing Segments and write to binlog storage
//   - update metadata via DataCoord's SaveBinlogPaths API
//   - update channel checkpoints
type GrowingFlushManager struct {
	collectionID int64
	channelName  string
	schema       *schemapb.CollectionSchema

	// dependencies
	collectionManager CollectionManager
	segmentManager    SegmentManager
	broker            broker.Broker
	chunkManager      storage.ChunkManager
	allocator         allocator.Interface
	checkpointTracker *CheckpointTracker

	// sync policies
	policies []GrowingSyncPolicy

	// configuration
	flushInterval time.Duration

	// sync mutex to prevent concurrent sync on the same segment
	syncMu sync.Map // map[int64]*sync.Mutex - segmentID -> mutex

	// control
	stopCh chan struct{}
	wg     sync.WaitGroup

	// logger
	logger *log.MLogger
}

// GrowingFlushManagerOption is a function to configure GrowingFlushManager.
type GrowingFlushManagerOption func(*GrowingFlushManager)

// WithFlushInterval sets the flush check interval.
func WithFlushInterval(interval time.Duration) GrowingFlushManagerOption {
	return func(m *GrowingFlushManager) {
		m.flushInterval = interval
	}
}

// WithSyncPolicies sets custom sync policies.
func WithSyncPolicies(policies ...GrowingSyncPolicy) GrowingFlushManagerOption {
	return func(m *GrowingFlushManager) {
		m.policies = policies
	}
}

// NewGrowingFlushManager creates a new GrowingFlushManager instance.
func NewGrowingFlushManager(
	collectionID int64,
	channelName string,
	schema *schemapb.CollectionSchema,
	collectionManager CollectionManager,
	segmentManager SegmentManager,
	broker broker.Broker,
	chunkManager storage.ChunkManager,
	allocator allocator.Interface,
	checkpointTracker *CheckpointTracker,
	opts ...GrowingFlushManagerOption,
) *GrowingFlushManager {
	m := &GrowingFlushManager{
		collectionID:      collectionID,
		channelName:       channelName,
		schema:            schema,
		collectionManager: collectionManager,
		segmentManager:    segmentManager,
		broker:            broker,
		chunkManager:      chunkManager,
		allocator:         allocator,
		checkpointTracker: checkpointTracker,
		stopCh:            make(chan struct{}),
		logger: log.With(
			zap.Int64("collectionID", collectionID),
			zap.String("channel", channelName),
		),
	}

	// apply options
	for _, opt := range opts {
		opt(m)
	}

	// set defaults
	if m.flushInterval == 0 {
		m.flushInterval = 100 * time.Millisecond
	}

	if len(m.policies) == 0 {
		m.policies = m.defaultPolicies()
	}

	return m
}

// defaultPolicies returns the default sync policies.
func (m *GrowingFlushManager) defaultPolicies() []GrowingSyncPolicy {
	staleDuration := paramtable.Get().DataNodeCfg.SyncPeriod.GetAsDuration(time.Second)
	return []GrowingSyncPolicy{
		GetGrowingFullBufferPolicy(),
		GetGrowingStaleBufferPolicy(staleDuration),
	}
}

// Start begins the background flush checking loop.
func (m *GrowingFlushManager) Start(ctx context.Context) {
	m.wg.Add(1)
	go m.backgroundLoop(ctx)
	m.logger.Info("GrowingFlushManager started")
}

// Stop signals the manager to stop and waits for completion.
func (m *GrowingFlushManager) Stop() {
	close(m.stopCh)
	m.wg.Wait()
	m.logger.Info("GrowingFlushManager stopped")
}

// backgroundLoop periodically checks sync policies and triggers flushes.
func (m *GrowingFlushManager) backgroundLoop(ctx context.Context) {
	defer m.wg.Done()

	ticker := time.NewTicker(m.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.checkAndTriggerSync(ctx)
		}
	}
}

// checkAndTriggerSync checks all policies and triggers sync for matching segments.
func (m *GrowingFlushManager) checkAndTriggerSync(ctx context.Context) {
	// get all growing segments
	segments := m.getGrowingSegments()
	if len(segments) == 0 {
		return
	}

	// wrap segments as GrowingSegmentBuffer for policy evaluation
	buffers := m.wrapAsBuffers(segments)
	currentTs := tsoutil.ComposeTSByTime(time.Now(), 0)

	// track which segments to sync (dedup across policies)
	syncSet := make(map[int64]string) // segmentID -> reason

	// evaluate each policy
	for _, policy := range m.policies {
		segIDs := policy.SelectSegments(buffers, currentTs)
		for _, segID := range segIDs {
			if _, exists := syncSet[segID]; !exists {
				syncSet[segID] = policy.Reason()
			}
		}
	}

	// trigger sync for each selected segment
	for segID, reason := range syncSet {
		segment := m.findSegment(segments, segID)
		if segment == nil {
			continue
		}
		m.triggerSync(ctx, segment, reason)
	}
}

// getGrowingSegments returns all growing segments from the segment manager.
func (m *GrowingFlushManager) getGrowingSegments() []Segment {
	// get segment IDs being tracked
	trackedIDs := m.checkpointTracker.GetSegmentIDs()
	if len(trackedIDs) == 0 {
		return nil
	}

	segments := make([]Segment, 0, len(trackedIDs))
	for _, segID := range trackedIDs {
		seg := m.segmentManager.GetGrowing(segID)
		if seg != nil {
			segments = append(segments, seg)
		}
	}
	return segments
}

// wrapAsBuffers wraps segments as GrowingSegmentBuffer for policy evaluation.
func (m *GrowingFlushManager) wrapAsBuffers(segments []Segment) []*GrowingSegmentBuffer {
	buffers := make([]*GrowingSegmentBuffer, 0, len(segments))
	for _, seg := range segments {
		buf := NewGrowingSegmentBuffer(seg, m.checkpointTracker)
		if buf.HasUnflushedData() {
			buffers = append(buffers, buf)
		}
	}
	return buffers
}

// findSegment finds a segment by ID from the list.
func (m *GrowingFlushManager) findSegment(segments []Segment, segID int64) Segment {
	for _, seg := range segments {
		if seg.ID() == segID {
			return seg
		}
	}
	return nil
}

// triggerSync initiates a sync operation for the given segment.
func (m *GrowingFlushManager) triggerSync(ctx context.Context, segment Segment, reason string) {
	segID := segment.ID()
	log := m.logger.With(
		zap.Int64("segmentID", segID),
		zap.String("reason", reason),
	)

	// get or create mutex for this segment to prevent concurrent sync
	muInterface, _ := m.syncMu.LoadOrStore(segID, &sync.Mutex{})
	mu := muInterface.(*sync.Mutex)

	// try to acquire lock without blocking
	if !mu.TryLock() {
		log.Debug("sync already in progress, skipping")
		return
	}

	log.Info("triggering growing segment sync")

	// async sync to avoid blocking the check loop
	go func() {
		defer mu.Unlock()
		if err := m.doSync(ctx, segment); err != nil {
			log.Error("growing segment sync failed", zap.Error(err))
		}
	}()
}

// doSync performs the actual sync operation for a segment.
func (m *GrowingFlushManager) doSync(ctx context.Context, segment Segment) error {
	segID := segment.ID()
	log := m.logger.With(zap.Int64("segmentID", segID))

	// 1. get unflushed data range
	flushedOffset := m.checkpointTracker.GetFlushedOffset(segID)
	currentOffset := segment.RowNum()

	if flushedOffset >= currentOffset {
		log.Debug("no unflushed data")
		return nil
	}

	unflushedRows := currentOffset - flushedOffset
	log.Info("starting sync",
		zap.Int64("flushedOffset", flushedOffset),
		zap.Int64("currentOffset", currentOffset),
		zap.Int64("unflushedRows", unflushedRows),
	)

	// 2. get checkpoint position for this sync
	checkpoint := m.checkpointTracker.GetCheckpoint(segID, currentOffset)
	if checkpoint == nil {
		log.Warn("no checkpoint found for sync range")
		// continue anyway, checkpoint will be nil
	}

	// 3. flush data directly via C++ milvus-storage (unified interface)
	// This combines data extraction and writing - all logic in C++ side
	// C++ side handles: data extraction, encoding, file writing, manifest management
	flushConfig := m.buildFlushConfig(segment)
	flushResult, err := segment.FlushData(ctx, flushedOffset, currentOffset, flushConfig)
	if err != nil {
		return errors.Wrap(err, "failed to flush data via C++ milvus-storage")
	}
	if flushResult == nil || flushResult.ManifestPath == "" {
		log.Debug("no data flushed or no manifest path returned")
		return nil
	}

	// 4. save manifest path and checkpoint to DataCoord
	// In Storage V3 FFI mode, only manifest path is needed (all file info is in manifest)
	if err := m.saveManifestAndCheckpoint(ctx, segment, flushResult.ManifestPath, checkpoint, flushResult.NumRows); err != nil {
		return err
	}

	// 6. update flushed offset
	m.checkpointTracker.UpdateFlushedOffset(segID, currentOffset)

	log.Info("sync completed",
		zap.Int64("flushedRows", flushResult.NumRows),
		zap.String("manifestPath", flushResult.ManifestPath),
	)

	return nil
}

// buildFlushConfig builds FlushConfig for C++ FFI call.
// Go layer only constructs paths and config, all logic is in C++ side.
func (m *GrowingFlushManager) buildFlushConfig(segment Segment) *FlushConfig {
	// Build paths for C++ side
	// Segment path: {root}/insert_log/{coll}/{part}/{seg}
	segmentBasePath := m.chunkManager.RootPath() + "/insert_log/" +
		metautil.JoinIDPath(m.collectionID, segment.Partition(), segment.ID())

	// Partition path: {root}/insert_log/{coll}/{part}
	partitionBasePath := m.chunkManager.RootPath() + "/insert_log/" +
		metautil.JoinIDPath(m.collectionID, segment.Partition())

	return &FlushConfig{
		SegmentBasePath:   segmentBasePath,
		PartitionBasePath: partitionBasePath,
		CollectionID:      m.collectionID,
		PartitionID:       segment.Partition(),
	}
}

// saveManifestAndCheckpoint saves manifest path and checkpoint to DataCoord.
// In Storage V3 FFI mode, only manifest path is needed - all file information
// (binlog paths, column groups, etc.) is contained in the manifest file.
// C++ side has already written all files and created the manifest.
func (m *GrowingFlushManager) saveManifestAndCheckpoint(
	ctx context.Context,
	segment Segment,
	manifestPath string,
	checkpoint *msgpb.MsgPosition,
	numRows int64,
) error {
	if m.broker == nil {
		return errors.New("broker is nil, cannot save manifest and checkpoint")
	}

	segID := segment.ID()

	// In Storage V3 FFI mode, SaveBinlogPaths is only used to:
	// 1. Save manifest path (UpdateManifest)
	// 2. Save checkpoint (UpdateCheckPoint)
	// 3. Save start position (if needed)
	// All file paths are in manifest, no need to pass Field2BinlogPaths
	req := &datapb.SaveBinlogPathsRequest{
		Base:         &commonpb.MsgBase{},
		SegmentID:    segID,
		CollectionID: m.collectionID,
		PartitionID:  segment.Partition(),
		// Field2BinlogPaths: empty - not needed in Storage V3 FFI mode
		// All file information is in manifest
		CheckPoints: []*datapb.CheckPoint{{
			SegmentID: segID,
			Position:  checkpoint,
			NumOfRows: numRows,
		}},
		Channel:         m.channelName,
		Flushed:         false,        // keep as Growing segment
		WithFullBinlogs: false,        // incremental mode
		ManifestPath:    manifestPath, // Storage V3 manifest path (from C++ side)
		StorageVersion:  storage.StorageV3,
	}

	return m.broker.SaveBinlogPaths(ctx, req)
}

// ForceSync forces an immediate sync of all unflushed data for a segment.
// this is typically called when a segment is being sealed or dropped.
func (m *GrowingFlushManager) ForceSync(ctx context.Context, segID int64) error {
	segment := m.segmentManager.GetGrowing(segID)
	if segment == nil {
		return nil // segment not found, nothing to sync
	}

	return m.doSync(ctx, segment)
}

// GetCheckpointTracker returns the checkpoint tracker for external access.
func (m *GrowingFlushManager) GetCheckpointTracker() *CheckpointTracker {
	return m.checkpointTracker
}
