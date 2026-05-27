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
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// BinlogSaver is a minimal interface for saving binlog paths to DataCoord.
// This avoids depending on the full broker.Broker interface.
type BinlogSaver interface {
	SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) error
}

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
	binlogSaver       BinlogSaver
	chunkManager      storage.ChunkManager
	checkpointTracker *CheckpointTracker

	// sync policies
	policies []GrowingSyncPolicy

	// configuration
	flushInterval time.Duration

	// sync mutex to prevent concurrent sync on the same segment
	syncMu sync.Map // map[int64]*sync.Mutex - segmentID -> mutex

	// sealedSegments tracks segments that have already been sealed via ForceSyncAndSeal.
	// This prevents duplicate SaveBinlogPaths(isFlush=true) requests when the user
	// calls flush() multiple times, which can resurrect Dropped segments and cause
	// duplicate data after compaction.
	sealedSegments sync.Map // map[int64]struct{} - segmentID -> struct{}

	// control
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

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
	binlogSaver BinlogSaver,
	chunkManager storage.ChunkManager,
	checkpointTracker *CheckpointTracker,
	opts ...GrowingFlushManagerOption,
) *GrowingFlushManager {
	m := &GrowingFlushManager{
		collectionID:      collectionID,
		channelName:       channelName,
		schema:            schema,
		collectionManager: collectionManager,
		segmentManager:    segmentManager,
		binlogSaver:       binlogSaver,
		chunkManager:      chunkManager,
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
// Safe to call multiple times.
func (m *GrowingFlushManager) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})
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
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer mu.Unlock()
		if err := m.doSync(ctx, segment, false); err != nil {
			log.Error("growing segment sync failed", zap.Error(err))
		}
	}()
}

// doSync performs the actual sync operation for a segment.
// when isSeal is true, this is the final flush for the segment (user flush/seal),
// and SaveBinlogPaths will set Flushed=true to trigger DataCoord handoff.
func (m *GrowingFlushManager) doSync(ctx context.Context, segment Segment, isSeal bool) error {
	segID := segment.ID()
	log := m.logger.With(zap.Int64("segmentID", segID))

	// 1. get unflushed data range
	// Use GetMaxEndOffset (from CheckpointTracker) instead of segment.RowNum() to
	// determine how far to flush. This ensures we only flush data that has a
	flushedOffset := m.checkpointTracker.GetFlushedOffset(segID)
	currentOffset := m.checkpointTracker.GetMaxEndOffset(segID)

	if flushedOffset >= currentOffset {
		if isSeal {
			lastManifest := m.checkpointTracker.GetAcknowledgedManifest(segID)
			if lastManifest == "" {
				log.Info("empty segment with no manifest, skip seal",
					zap.Int64("flushedOffset", flushedOffset))
				return nil
			}

			log.Info("no unflushed data, but sealing segment",
				zap.String("lastManifest", lastManifest),
				zap.Int64("flushedRows", flushedOffset))
			return m.saveManifestAndCheckpoint(ctx, segment, lastManifest, nil, flushedOffset, true)
		}
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

	// 3. flush data directly via C++ milvus-storage (unified interface)
	// Use the last acknowledged manifest version as read_version.
	flushConfig := m.buildFlushConfig(segment)
	flushConfig.ReadVersion = m.checkpointTracker.GetAcknowledgedVersion(segID)
	flushResult, err := segment.FlushData(ctx, flushedOffset, currentOffset, flushConfig)
	if err != nil {
		return errors.Wrap(err, "failed to flush data via C++ milvus-storage")
	}
	if flushResult == nil || flushResult.ManifestPath == "" {
		if isSeal {
			lastManifest := m.checkpointTracker.GetAcknowledgedManifest(segID)
			if lastManifest == "" {
				log.Warn("FlushData returned no manifest and no prior manifest exists, skip seal")
				return nil
			}
			// Use flushedOffset (not currentOffset) — the last manifest only contains
			// data up to flushedOffset. Using currentOffset would over-report rows,
			// causing sort compaction to fail with "unexpected row count".
			log.Warn("no data flushed but seal requested, using last manifest",
				zap.String("lastManifest", lastManifest),
				zap.Int64("flushedOffset", flushedOffset),
				zap.Int64("currentOffset", currentOffset))
			return m.saveManifestAndCheckpoint(ctx, segment, lastManifest, checkpoint, flushedOffset, true)
		}
		log.Debug("no data flushed or no manifest path returned")
		return nil
	}

	// 4. save manifest path and checkpoint to DataCoord
	// In Storage V3 FFI mode, only manifest path is needed (all file info is in manifest)
	if err := m.saveManifestAndCheckpoint(ctx, segment, flushResult.ManifestPath, checkpoint, currentOffset, isSeal); err != nil {
		return err
	}

	// 5. update flushed offset AND acknowledged version
	// Both updates happen ONLY after SaveBinlogPaths succeeds.
	m.checkpointTracker.UpdateFlushedOffset(segID, currentOffset)
	m.checkpointTracker.UpdateAcknowledgedManifest(segID, flushResult.ManifestPath)

	log.Info("sync completed",
		zap.Int64("flushedRows", flushResult.NumRows),
		zap.String("manifest", flushResult.ManifestPath),
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

	// Collect TEXT field IDs and LOB paths from schema
	var textFieldIDs []int64
	var textLobPaths []string
	if m.schema != nil {
		for _, field := range m.schema.GetFields() {
			if field.GetDataType() == schemapb.DataType_Text {
				fieldID := field.GetFieldID()
				textFieldIDs = append(textFieldIDs, fieldID)
				// LOB path: {partition_path}/lobs/{field_id}
				lobPath := fmt.Sprintf("%s/lobs/%d", partitionBasePath, fieldID)
				textLobPaths = append(textLobPaths, lobPath)
			}
		}
	}

	return &FlushConfig{
		SegmentBasePath:   segmentBasePath,
		PartitionBasePath: partitionBasePath,
		CollectionID:      m.collectionID,
		PartitionID:       segment.Partition(),
		TextFieldIDs:      textFieldIDs,
		TextLobPaths:      textLobPaths,
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
	isSeal bool,
) error {
	if m.binlogSaver == nil {
		return errors.New("binlogSaver is nil, cannot save manifest and checkpoint")
	}

	segID := segment.ID()

	// In Storage V3 FFI mode, SaveBinlogPaths is only used to:
	// 1. Save manifest path (UpdateManifest)
	// 2. Save checkpoint (UpdateCheckPoint)
	// 3. Save start position (if needed)
	// All file paths are in manifest, no need to pass Field2BinlogPaths
	req := &datapb.SaveBinlogPathsRequest{
		Base:            &commonpb.MsgBase{},
		SegmentID:       segID,
		CollectionID:    m.collectionID,
		PartitionID:     segment.Partition(),
		Flushed:         isSeal,       // true when sealing, false for incremental flush
		WithFullBinlogs: false,        // incremental mode
		ManifestPath:    manifestPath, // Storage V3 manifest path (from C++ side)
		StorageVersion:  storage.StorageV3,
	}

	if checkpoint != nil {
		req.CheckPoints = []*datapb.CheckPoint{{
			SegmentID: segID,
			Position:  checkpoint,
			NumOfRows: numRows,
		}}
	}

	return m.binlogSaver.SaveBinlogPaths(ctx, req)
}

// ForceSync forces an immediate sync of all unflushed data for a segment.
// this is typically called when a segment is being sealed or dropped.
func (m *GrowingFlushManager) ForceSync(ctx context.Context, segID int64) error {
	segment := m.segmentManager.GetGrowing(segID)
	if segment == nil {
		return nil // segment not found, nothing to sync
	}

	// acquire per-segment mutex to prevent concurrent doSync with background triggerSync
	muInterface, _ := m.syncMu.LoadOrStore(segID, &sync.Mutex{})
	mu := muInterface.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	return m.doSync(ctx, segment, false)
}

// ForceSyncAndSeal forces an immediate sync and marks the segment as flushed/sealed.
// This is called when the segment is being sealed (user flush command).
// After this call, DataCoord will mark the segment as Flushed and trigger handoff.
//
// This method is idempotent: once a segment has been successfully sealed, subsequent
// calls are no-ops. This prevents duplicate SaveBinlogPaths(isFlush=true) requests
// when flush() is called multiple times, which would otherwise resurrect compacted
// segments via the Dropped→Flushed state transition race and cause duplicate data.
func (m *GrowingFlushManager) ForceSyncAndSeal(ctx context.Context, segID int64) error {
	// Idempotency check: skip segments that have already been sealed.
	if _, sealed := m.sealedSegments.Load(segID); sealed {
		m.logger.Debug("segment already sealed, skipping duplicate ForceSyncAndSeal",
			zap.Int64("segmentID", segID))
		return nil
	}

	segment := m.segmentManager.GetGrowing(segID)
	if segment == nil {
		return nil // segment not found, nothing to sync
	}

	// acquire per-segment mutex to prevent concurrent doSync with background triggerSync
	muInterface, _ := m.syncMu.LoadOrStore(segID, &sync.Mutex{})
	mu := muInterface.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	// Double-check after acquiring lock, another goroutine may have sealed it.
	if _, sealed := m.sealedSegments.Load(segID); sealed {
		return nil
	}

	if err := m.doSync(ctx, segment, true); err != nil {
		return err
	}

	m.sealedSegments.Store(segID, struct{}{})
	return nil
}

// RemoveSealedSegment clears the sealed state for a segment.
// This should be called when the segment is released from QueryNode.
func (m *GrowingFlushManager) RemoveSealedSegment(segID int64) {
	m.sealedSegments.Delete(segID)
}

// GetCheckpointTracker returns the checkpoint tracker for external access.
func (m *GrowingFlushManager) GetCheckpointTracker() *CheckpointTracker {
	return m.checkpointTracker
}
