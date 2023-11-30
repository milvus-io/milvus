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

// Package datacoord contains core functions in datacoord
package datacoord

import (
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/segmentutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type meta struct {
	sync.RWMutex
	ctx            context.Context
	catalog        metastore.DataCoordCatalog
	collections    map[UniqueID]*collectionInfo // collection id to collection info
	segments       *SegmentsInfo                // segment id to segment info
	channelCPLocks *lock.KeyLock[string]
	channelCPs     *typeutil.ConcurrentMap[string, *msgpb.MsgPosition] // vChannel -> channel checkpoint/see position
	chunkManager   storage.ChunkManager

	// collectionIndexes records which indexes are on the collection
	// collID -> indexID -> index
	indexes map[UniqueID]map[UniqueID]*model.Index
	// buildID2Meta records the meta information of the segment
	// buildID -> segmentIndex
	buildID2SegmentIndex map[UniqueID]*model.SegmentIndex
}

// A local cache of segment metric update. Must call commit() to take effect.
type segMetricMutation struct {
	stateChange       map[string]map[string]int // segment state, seg level -> state change count (to increase or decrease).
	rowCountChange    int64                     // Change in # of rows.
	rowCountAccChange int64                     // Total # of historical added rows, accumulated.
}

type collectionInfo struct {
	ID             int64
	Schema         *schemapb.CollectionSchema
	Partitions     []int64
	StartPositions []*commonpb.KeyDataPair
	Properties     map[string]string
	CreatedAt      Timestamp
}

// NewMeta creates meta from provided `kv.TxnKV`
func newMeta(ctx context.Context, catalog metastore.DataCoordCatalog, chunkManager storage.ChunkManager) (*meta, error) {
	mt := &meta{
		ctx:                  ctx,
		catalog:              catalog,
		collections:          make(map[UniqueID]*collectionInfo),
		segments:             NewSegmentsInfo(),
		channelCPLocks:       lock.NewKeyLock[string](),
		channelCPs:           typeutil.NewConcurrentMap[string, *msgpb.MsgPosition](),
		chunkManager:         chunkManager,
		indexes:              make(map[UniqueID]map[UniqueID]*model.Index),
		buildID2SegmentIndex: make(map[UniqueID]*model.SegmentIndex),
	}
	err := mt.reloadFromKV()
	if err != nil {
		return nil, err
	}
	return mt, nil
}

// reloadFromKV loads meta from KV storage
func (m *meta) reloadFromKV() error {
	record := timerecord.NewTimeRecorder("datacoord")
	segments, err := m.catalog.ListSegments(m.ctx)
	if err != nil {
		return err
	}
	metrics.DataCoordNumCollections.WithLabelValues().Set(0)
	metrics.DataCoordNumSegments.Reset()
	numStoredRows := int64(0)
	for _, segment := range segments {
		m.segments.SetSegment(segment.ID, NewSegmentInfo(segment))
		metrics.DataCoordNumSegments.WithLabelValues(segment.State.String(), segment.GetLevel().String()).Inc()
		if segment.State == commonpb.SegmentState_Flushed {
			numStoredRows += segment.NumOfRows

			insertFileNum := 0
			for _, fieldBinlog := range segment.GetBinlogs() {
				insertFileNum += len(fieldBinlog.GetBinlogs())
			}
			metrics.FlushedSegmentFileNum.WithLabelValues(metrics.InsertFileLabel).Observe(float64(insertFileNum))

			statFileNum := 0
			for _, fieldBinlog := range segment.GetStatslogs() {
				statFileNum += len(fieldBinlog.GetBinlogs())
			}
			metrics.FlushedSegmentFileNum.WithLabelValues(metrics.StatFileLabel).Observe(float64(statFileNum))

			deleteFileNum := 0
			for _, filedBinlog := range segment.GetDeltalogs() {
				deleteFileNum += len(filedBinlog.GetBinlogs())
			}
			metrics.FlushedSegmentFileNum.WithLabelValues(metrics.DeleteFileLabel).Observe(float64(deleteFileNum))
		}
	}
	metrics.DataCoordNumStoredRowsCounter.WithLabelValues().Add(float64(numStoredRows))

	channelCPs, err := m.catalog.ListChannelCheckpoint(m.ctx)
	if err != nil {
		return err
	}
	for vChannel, pos := range channelCPs {
		// for 2.2.2 issue https://github.com/milvus-io/milvus/issues/22181
		pos.ChannelName = vChannel
		m.channelCPs.Insert(vChannel, pos)
	}

	// load field indexes
	fieldIndexes, err := m.catalog.ListIndexes(m.ctx)
	if err != nil {
		log.Error("DataCoord meta reloadFromKV load field indexes fail", zap.Error(err))
		return err
	}
	for _, fieldIndex := range fieldIndexes {
		m.updateCollectionIndex(fieldIndex)
	}
	segmentIndexes, err := m.catalog.ListSegmentIndexes(m.ctx)
	if err != nil {
		log.Error("DataCoord meta reloadFromKV load segment indexes fail", zap.Error(err))
		return err
	}
	for _, segIdx := range segmentIndexes {
		m.updateSegmentIndex(segIdx)
		metrics.FlushedSegmentFileNum.WithLabelValues(metrics.IndexFileLabel).Observe(float64(len(segIdx.IndexFileKeys)))
	}
	log.Info("DataCoord meta reloadFromKV done", zap.Duration("duration", record.ElapseSpan()))
	return nil
}

// AddCollection adds a collection into meta
// Note that collection info is just for caching and will not be set into etcd from datacoord
func (m *meta) AddCollection(collection *collectionInfo) {
	log.Debug("meta update: add collection", zap.Int64("collectionID", collection.ID))
	m.Lock()
	defer m.Unlock()
	m.collections[collection.ID] = collection
	metrics.DataCoordNumCollections.WithLabelValues().Set(float64(len(m.collections)))
	log.Info("meta update: add collection - complete", zap.Int64("collectionID", collection.ID))
}

// GetCollection returns collection info with provided collection id from local cache
func (m *meta) GetCollection(collectionID UniqueID) *collectionInfo {
	m.RLock()
	defer m.RUnlock()
	collection, ok := m.collections[collectionID]
	if !ok {
		return nil
	}
	return collection
}

func (m *meta) GetClonedCollectionInfo(collectionID UniqueID) *collectionInfo {
	m.RLock()
	defer m.RUnlock()

	coll, ok := m.collections[collectionID]
	if !ok {
		return nil
	}

	clonedProperties := make(map[string]string)
	maps.Copy(clonedProperties, coll.Properties)
	cloneColl := &collectionInfo{
		ID:             coll.ID,
		Schema:         proto.Clone(coll.Schema).(*schemapb.CollectionSchema),
		Partitions:     coll.Partitions,
		StartPositions: common.CloneKeyDataPairs(coll.StartPositions),
		Properties:     clonedProperties,
	}

	return cloneColl
}

// GetSegmentsChanPart returns segments organized in Channel-Partition dimension with selector applied
func (m *meta) GetSegmentsChanPart(selector SegmentInfoSelector) []*chanPartSegments {
	m.RLock()
	defer m.RUnlock()
	mDimEntry := make(map[string]*chanPartSegments)

	for _, segmentInfo := range m.segments.segments {
		if !selector(segmentInfo) {
			continue
		}
		dim := fmt.Sprintf("%d-%s", segmentInfo.PartitionID, segmentInfo.InsertChannel)
		entry, ok := mDimEntry[dim]
		if !ok {
			entry = &chanPartSegments{
				collectionID: segmentInfo.CollectionID,
				partitionID:  segmentInfo.PartitionID,
				channelName:  segmentInfo.InsertChannel,
			}
			mDimEntry[dim] = entry
		}
		entry.segments = append(entry.segments, segmentInfo)
	}

	result := make([]*chanPartSegments, 0, len(mDimEntry))
	for _, entry := range mDimEntry {
		result = append(result, entry)
	}
	return result
}

// GetNumRowsOfCollection returns total rows count of segments belongs to provided collection
func (m *meta) GetNumRowsOfCollection(collectionID UniqueID) int64 {
	m.RLock()
	defer m.RUnlock()
	var ret int64
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		if isSegmentHealthy(segment) && segment.GetCollectionID() == collectionID {
			ret += segment.GetNumOfRows()
		}
	}
	return ret
}

// GetCollectionBinlogSize returns the total binlog size and binlog size of collections.
func (m *meta) GetCollectionBinlogSize() (int64, map[UniqueID]int64) {
	m.RLock()
	defer m.RUnlock()
	collectionBinlogSize := make(map[UniqueID]int64)
	collectionRowsNum := make(map[UniqueID]map[commonpb.SegmentState]int64)
	segments := m.segments.GetSegments()
	var total int64
	for _, segment := range segments {
		segmentSize := segment.getSegmentSize()
		if isSegmentHealthy(segment) {
			total += segmentSize
			collectionBinlogSize[segment.GetCollectionID()] += segmentSize
			metrics.DataCoordStoredBinlogSize.WithLabelValues(
				fmt.Sprint(segment.GetCollectionID()), fmt.Sprint(segment.GetID())).Set(float64(segmentSize))
			if _, ok := collectionRowsNum[segment.GetCollectionID()]; !ok {
				collectionRowsNum[segment.GetCollectionID()] = make(map[commonpb.SegmentState]int64)
			}
			collectionRowsNum[segment.GetCollectionID()][segment.GetState()] += segment.GetNumOfRows()
		}
	}
	for collection, statesRows := range collectionRowsNum {
		for state, rows := range statesRows {
			metrics.DataCoordNumStoredRows.WithLabelValues(fmt.Sprint(collection), state.String()).Set(float64(rows))
		}
	}
	return total, collectionBinlogSize
}

// AddSegment records segment info, persisting info into kv store
func (m *meta) AddSegment(ctx context.Context, segment *SegmentInfo) error {
	log := log.Ctx(ctx)
	log.Info("meta update: adding segment - Start", zap.Int64("segmentID", segment.GetID()))
	m.Lock()
	defer m.Unlock()
	if err := m.catalog.AddSegment(m.ctx, segment.SegmentInfo); err != nil {
		log.Error("meta update: adding segment failed",
			zap.Int64("segmentID", segment.GetID()),
			zap.Error(err))
		return err
	}
	m.segments.SetSegment(segment.GetID(), segment)
	metrics.DataCoordNumSegments.WithLabelValues(segment.GetState().String(), segment.GetLevel().String()).Inc()
	log.Info("meta update: adding segment - complete", zap.Int64("segmentID", segment.GetID()))
	return nil
}

// DropSegment remove segment with provided id, etcd persistence also removed
func (m *meta) DropSegment(segmentID UniqueID) error {
	log.Debug("meta update: dropping segment", zap.Int64("segmentID", segmentID))
	m.Lock()
	defer m.Unlock()
	segment := m.segments.GetSegment(segmentID)
	if segment == nil {
		log.Warn("meta update: dropping segment failed - segment not found",
			zap.Int64("segmentID", segmentID))
		return nil
	}
	if err := m.catalog.DropSegment(m.ctx, segment.SegmentInfo); err != nil {
		log.Warn("meta update: dropping segment failed",
			zap.Int64("segmentID", segmentID),
			zap.Error(err))
		return err
	}
	metrics.DataCoordNumSegments.WithLabelValues(segment.GetState().String(), segment.GetLevel().String()).Dec()
	m.segments.DropSegment(segmentID)
	log.Info("meta update: dropping segment - complete",
		zap.Int64("segmentID", segmentID))
	return nil
}

// GetHealthySegment returns segment info with provided id
// if not segment is found, nil will be returned
func (m *meta) GetHealthySegment(segID UniqueID) *SegmentInfo {
	m.RLock()
	defer m.RUnlock()
	segment := m.segments.GetSegment(segID)
	if segment != nil && isSegmentHealthy(segment) {
		return segment
	}
	return nil
}

// GetSegment returns segment info with provided id
// include the unhealthy segment
// if not segment is found, nil will be returned
func (m *meta) GetSegment(segID UniqueID) *SegmentInfo {
	m.RLock()
	defer m.RUnlock()
	return m.segments.GetSegment(segID)
}

// GetAllSegmentsUnsafe returns all segments
func (m *meta) GetAllSegmentsUnsafe() []*SegmentInfo {
	m.RLock()
	defer m.RUnlock()
	return m.segments.GetSegments()
}

// SetState setting segment with provided ID state
func (m *meta) SetState(segmentID UniqueID, targetState commonpb.SegmentState) error {
	log.Debug("meta update: setting segment state",
		zap.Int64("segmentID", segmentID),
		zap.Any("target state", targetState))
	m.Lock()
	defer m.Unlock()
	curSegInfo := m.segments.GetSegment(segmentID)
	if curSegInfo == nil {
		log.Warn("meta update: setting segment state - segment not found",
			zap.Int64("segmentID", segmentID),
			zap.Any("target state", targetState))
		// idempotent drop
		if targetState == commonpb.SegmentState_Dropped {
			return nil
		}
		return fmt.Errorf("segment is not exist with ID = %d", segmentID)
	}
	// Persist segment updates first.
	clonedSegment := curSegInfo.Clone()
	metricMutation := &segMetricMutation{
		stateChange: make(map[string]map[string]int),
	}
	if clonedSegment != nil && isSegmentHealthy(clonedSegment) {
		// Update segment state and prepare segment metric update.
		updateSegStateAndPrepareMetrics(clonedSegment, targetState, metricMutation)
		if err := m.catalog.AlterSegments(m.ctx, []*datapb.SegmentInfo{clonedSegment.SegmentInfo}); err != nil {
			log.Warn("meta update: setting segment state - failed to alter segments",
				zap.Int64("segmentID", segmentID),
				zap.String("target state", targetState.String()),
				zap.Error(err))
			return err
		}
		// Apply segment metric update after successful meta update.
		metricMutation.commit()
		// Update in-memory meta.
		m.segments.SetState(segmentID, targetState)
	}
	log.Info("meta update: setting segment state - complete",
		zap.Int64("segmentID", segmentID),
		zap.String("target state", targetState.String()))
	return nil
}

// UnsetIsImporting removes the `isImporting` flag of a segment.
func (m *meta) UnsetIsImporting(segmentID UniqueID) error {
	log.Debug("meta update: unsetting isImport state of segment",
		zap.Int64("segmentID", segmentID))
	m.Lock()
	defer m.Unlock()
	curSegInfo := m.segments.GetSegment(segmentID)
	if curSegInfo == nil {
		return fmt.Errorf("segment not found %d", segmentID)
	}
	// Persist segment updates first.
	clonedSegment := curSegInfo.Clone()
	clonedSegment.IsImporting = false
	if isSegmentHealthy(clonedSegment) {
		if err := m.catalog.AlterSegments(m.ctx, []*datapb.SegmentInfo{clonedSegment.SegmentInfo}); err != nil {
			log.Warn("meta update: unsetting isImport state of segment - failed to unset segment isImporting state",
				zap.Int64("segmentID", segmentID),
				zap.Error(err))
			return err
		}
	}
	// Update in-memory meta.
	m.segments.SetIsImporting(segmentID, false)
	log.Info("meta update: unsetting isImport state of segment - complete",
		zap.Int64("segmentID", segmentID))
	return nil
}

type updateSegmentPack struct {
	meta     *meta
	segments map[int64]*SegmentInfo
	// for update etcd binlog paths
	increments map[int64]metastore.BinlogsIncrement
	// for update segment metric after alter segments
	metricMutation *segMetricMutation
}

func (p *updateSegmentPack) Get(segmentID int64) *SegmentInfo {
	if segment, ok := p.segments[segmentID]; ok {
		return segment
	}

	segment := p.meta.segments.GetSegment(segmentID)
	if segment == nil || !isSegmentHealthy(segment) {
		log.Warn("meta update: get segment failed - segment not found",
			zap.Int64("segmentID", segmentID),
			zap.Bool("segment nil", segment == nil),
			zap.Bool("segment unhealthy", !isSegmentHealthy(segment)))
		return nil
	}

	p.segments[segmentID] = segment.Clone()
	return p.segments[segmentID]
}

type UpdateOperator func(*updateSegmentPack) bool

func CreateL0Operator(collectionID, partitionID, segmentID int64, channel string) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.meta.segments.GetSegment(segmentID)
		if segment == nil {
			log.Info("meta update: add new l0 segment",
				zap.Int64("collectionID", collectionID),
				zap.Int64("partitionID", partitionID),
				zap.Int64("segmentID", segmentID))

			modPack.segments[segmentID] = &SegmentInfo{
				SegmentInfo: &datapb.SegmentInfo{
					ID:            segmentID,
					CollectionID:  collectionID,
					PartitionID:   partitionID,
					InsertChannel: channel,
					NumOfRows:     0,
					State:         commonpb.SegmentState_Growing,
					Level:         datapb.SegmentLevel_L0,
				},
			}
		}
		return true
	}
}

func UpdateStorageVersionOperator(segmentID int64, version int64) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.meta.GetSegment(segmentID)
		if segment == nil {
			log.Info("meta update: update storage version - segment not found",
				zap.Int64("segmentID", segmentID))
			return false
		}

		segment.StorageVersion = version
		return true
	}
}

// Set status of segment
// and record dropped time when change segment status to dropped
func UpdateStatusOperator(segmentID int64, status commonpb.SegmentState) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			log.Warn("meta update: update status failed - segment not found",
				zap.Int64("segmentID", segmentID),
				zap.String("status", status.String()))
			return false
		}

		updateSegStateAndPrepareMetrics(segment, status, modPack.metricMutation)
		if status == commonpb.SegmentState_Dropped {
			segment.DroppedAt = uint64(time.Now().UnixNano())
		}
		return true
	}
}

// update binlogs in segmentInfo
func UpdateBinlogsOperator(segmentID int64, binlogs, statslogs, deltalogs []*datapb.FieldBinlog) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			log.Warn("meta update: update binlog failed - segment not found",
				zap.Int64("segmentID", segmentID))
			return false
		}

		segment.Binlogs = mergeFieldBinlogs(segment.GetBinlogs(), binlogs)
		segment.Statslogs = mergeFieldBinlogs(segment.GetStatslogs(), statslogs)
		segment.Deltalogs = mergeFieldBinlogs(segment.GetDeltalogs(), deltalogs)
		modPack.increments[segmentID] = metastore.BinlogsIncrement{
			Segment: segment.SegmentInfo,
		}
		return true
	}
}

// update startPosition
func UpdateStartPosition(startPositions []*datapb.SegmentStartPosition) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		for _, pos := range startPositions {
			if len(pos.GetStartPosition().GetMsgID()) == 0 {
				continue
			}
			s := modPack.Get(pos.GetSegmentID())
			if s == nil {
				continue
			}

			s.StartPosition = pos.GetStartPosition()
		}
		return true
	}
}

// update segment checkpoint and num rows
// if was importing segment
// only update rows.
func UpdateCheckPointOperator(segmentID int64, importing bool, checkpoints []*datapb.CheckPoint) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			log.Warn("meta update: update checkpoint failed - segment not found",
				zap.Int64("segmentID", segmentID))
			return false
		}

		if importing {
			segment.NumOfRows = segment.currRows
		} else {
			for _, cp := range checkpoints {
				if cp.SegmentID != segmentID {
					// Don't think this is gonna to happen, ignore for now.
					log.Warn("checkpoint in segment is not same as flush segment to update, igreo", zap.Int64("current", segmentID), zap.Int64("checkpoint segment", cp.SegmentID))
					continue
				}

				if segment.DmlPosition != nil && segment.DmlPosition.Timestamp >= cp.Position.Timestamp {
					log.Warn("checkpoint in segment is larger than reported", zap.Any("current", segment.GetDmlPosition()), zap.Any("reported", cp.GetPosition()))
					// segment position in etcd is larger than checkpoint, then dont change it
					continue
				}

				segment.NumOfRows = cp.NumOfRows
				segment.DmlPosition = cp.GetPosition()
			}
		}

		count := segmentutil.CalcRowCountFromBinLog(segment.SegmentInfo)
		if count != segment.currRows && count > 0 {
			log.Info("check point reported inconsistent with bin log row count",
				zap.Int64("current rows (wrong)", segment.currRows),
				zap.Int64("segment bin log row count (correct)", count))
			segment.NumOfRows = count
		}
		return true
	}
}

// updateSegmentsInfo update segment infos
// will exec all operators, and update all changed segments
func (m *meta) UpdateSegmentsInfo(operators ...UpdateOperator) error {
	m.Lock()
	defer m.Unlock()
	updatePack := &updateSegmentPack{
		meta:       m,
		segments:   make(map[int64]*SegmentInfo),
		increments: make(map[int64]metastore.BinlogsIncrement),
		metricMutation: &segMetricMutation{
			stateChange: make(map[string]map[string]int),
		},
	}

	for _, operator := range operators {
		ok := operator(updatePack)
		if !ok {
			return nil
		}
	}

	segments := lo.MapToSlice(updatePack.segments, func(_ int64, segment *SegmentInfo) *datapb.SegmentInfo { return segment.SegmentInfo })
	increments := lo.Values(updatePack.increments)

	if err := m.catalog.AlterSegments(m.ctx, segments, increments...); err != nil {
		log.Error("meta update: update flush segments info - failed to store flush segment info into Etcd",
			zap.Error(err))
		return err
	}
	// Apply metric mutation after a successful meta update.
	updatePack.metricMutation.commit()
	// update memory status
	for id, s := range updatePack.segments {
		m.segments.SetSegment(id, s)
	}
	log.Info("meta update: update flush segments info - update flush segments info successfully")
	return nil
}

// UpdateDropChannelSegmentInfo updates segment checkpoints and binlogs before drop
// reusing segment info to pass segment id, binlogs, statslog, deltalog, start position and checkpoint
func (m *meta) UpdateDropChannelSegmentInfo(channel string, segments []*SegmentInfo) error {
	log.Debug("meta update: update drop channel segment info",
		zap.String("channel", channel))
	m.Lock()
	defer m.Unlock()

	// Prepare segment metric mutation.
	metricMutation := &segMetricMutation{
		stateChange: make(map[string]map[string]int),
	}
	modSegments := make(map[UniqueID]*SegmentInfo)
	// save new segments flushed from buffer data
	for _, seg2Drop := range segments {
		var segment *SegmentInfo
		segment, metricMutation = m.mergeDropSegment(seg2Drop)
		if segment != nil {
			modSegments[seg2Drop.GetID()] = segment
		}
	}
	// set existed segments of channel to Dropped
	for _, seg := range m.segments.segments {
		if seg.InsertChannel != channel {
			continue
		}
		_, ok := modSegments[seg.ID]
		// seg inf mod segments are all in dropped state
		if !ok {
			clonedSeg := seg.Clone()
			updateSegStateAndPrepareMetrics(clonedSeg, commonpb.SegmentState_Dropped, metricMutation)
			modSegments[seg.ID] = clonedSeg
		}
	}
	err := m.batchSaveDropSegments(channel, modSegments)
	if err != nil {
		log.Warn("meta update: update drop channel segment info failed",
			zap.String("channel", channel),
			zap.Error(err))
	} else {
		log.Info("meta update: update drop channel segment info - complete",
			zap.String("channel", channel))
		// Apply segment metric mutation on successful meta update.
		metricMutation.commit()
	}
	return err
}

// mergeDropSegment merges drop segment information with meta segments
func (m *meta) mergeDropSegment(seg2Drop *SegmentInfo) (*SegmentInfo, *segMetricMutation) {
	metricMutation := &segMetricMutation{
		stateChange: make(map[string]map[string]int),
	}

	segment := m.segments.GetSegment(seg2Drop.ID)
	// healthy check makes sure the Idempotence
	if segment == nil || !isSegmentHealthy(segment) {
		log.Warn("UpdateDropChannel skipping nil or unhealthy", zap.Bool("is nil", segment == nil),
			zap.Bool("isHealthy", isSegmentHealthy(segment)))
		return nil, metricMutation
	}

	clonedSegment := segment.Clone()
	updateSegStateAndPrepareMetrics(clonedSegment, commonpb.SegmentState_Dropped, metricMutation)

	currBinlogs := clonedSegment.GetBinlogs()

	getFieldBinlogs := func(id UniqueID, binlogs []*datapb.FieldBinlog) *datapb.FieldBinlog {
		for _, binlog := range binlogs {
			if id == binlog.GetFieldID() {
				return binlog
			}
		}
		return nil
	}
	// binlogs
	for _, tBinlogs := range seg2Drop.GetBinlogs() {
		fieldBinlogs := getFieldBinlogs(tBinlogs.GetFieldID(), currBinlogs)
		if fieldBinlogs == nil {
			currBinlogs = append(currBinlogs, tBinlogs)
		} else {
			fieldBinlogs.Binlogs = append(fieldBinlogs.Binlogs, tBinlogs.Binlogs...)
		}
	}
	clonedSegment.Binlogs = currBinlogs
	// statlogs
	currStatsLogs := clonedSegment.GetStatslogs()
	for _, tStatsLogs := range seg2Drop.GetStatslogs() {
		fieldStatsLog := getFieldBinlogs(tStatsLogs.GetFieldID(), currStatsLogs)
		if fieldStatsLog == nil {
			currStatsLogs = append(currStatsLogs, tStatsLogs)
		} else {
			fieldStatsLog.Binlogs = append(fieldStatsLog.Binlogs, tStatsLogs.Binlogs...)
		}
	}
	clonedSegment.Statslogs = currStatsLogs
	// deltalogs
	clonedSegment.Deltalogs = append(clonedSegment.Deltalogs, seg2Drop.GetDeltalogs()...)

	// start position
	if seg2Drop.GetStartPosition() != nil {
		clonedSegment.StartPosition = seg2Drop.GetStartPosition()
	}
	// checkpoint
	if seg2Drop.GetDmlPosition() != nil {
		clonedSegment.DmlPosition = seg2Drop.GetDmlPosition()
	}
	clonedSegment.currRows = seg2Drop.currRows
	clonedSegment.NumOfRows = seg2Drop.currRows
	return clonedSegment, metricMutation
}

// batchSaveDropSegments saves drop segments info with channel removal flag
// since the channel unwatching operation is not atomic here
// ** the removal flag is always with last batch
// ** the last batch must contains at least one segment
//  1. when failure occurs between batches, failover mechanism will continue with the earliest  checkpoint of this channel
//     since the flag is not marked so DataNode can re-consume the drop collection msg
//  2. when failure occurs between save meta and unwatch channel, the removal flag shall be check before let datanode watch this channel
func (m *meta) batchSaveDropSegments(channel string, modSegments map[int64]*SegmentInfo) error {
	var modSegIDs []int64
	for k := range modSegments {
		modSegIDs = append(modSegIDs, k)
	}
	log.Info("meta update: batch save drop segments",
		zap.Int64s("drop segments", modSegIDs))
	segments := make([]*datapb.SegmentInfo, 0)
	for _, seg := range modSegments {
		segments = append(segments, seg.SegmentInfo)
	}
	err := m.catalog.SaveDroppedSegmentsInBatch(m.ctx, segments)
	if err != nil {
		return err
	}

	if err = m.catalog.MarkChannelDeleted(m.ctx, channel); err != nil {
		return err
	}

	// update memory info
	for id, segment := range modSegments {
		m.segments.SetSegment(id, segment)
	}

	return nil
}

// GetSegmentsByChannel returns all segment info which insert channel equals provided `dmlCh`
func (m *meta) GetSegmentsByChannel(channel string) []*SegmentInfo {
	return m.SelectSegments(func(segment *SegmentInfo) bool {
		return isSegmentHealthy(segment) && segment.InsertChannel == channel
	})
}

// GetSegmentsOfCollection get all segments of collection
func (m *meta) GetSegmentsOfCollection(collectionID UniqueID) []*SegmentInfo {
	return m.SelectSegments(func(segment *SegmentInfo) bool {
		return isSegmentHealthy(segment) && segment.GetCollectionID() == collectionID
	})
}

// GetSegmentsIDOfCollection returns all segment ids which collection equals to provided `collectionID`
func (m *meta) GetSegmentsIDOfCollection(collectionID UniqueID) []UniqueID {
	segments := m.SelectSegments(func(segment *SegmentInfo) bool {
		return isSegmentHealthy(segment) && segment.CollectionID == collectionID
	})

	return lo.Map(segments, func(segment *SegmentInfo, _ int) int64 {
		return segment.ID
	})
}

// GetSegmentsIDOfCollection returns all segment ids which collection equals to provided `collectionID`
func (m *meta) GetSegmentsIDOfCollectionWithDropped(collectionID UniqueID) []UniqueID {
	segments := m.SelectSegments(func(segment *SegmentInfo) bool {
		return segment != nil &&
			segment.GetState() != commonpb.SegmentState_SegmentStateNone &&
			segment.GetState() != commonpb.SegmentState_NotExist &&
			segment.CollectionID == collectionID
	})

	return lo.Map(segments, func(segment *SegmentInfo, _ int) int64 {
		return segment.ID
	})
}

// GetSegmentsIDOfPartition returns all segments ids which collection & partition equals to provided `collectionID`, `partitionID`
func (m *meta) GetSegmentsIDOfPartition(collectionID, partitionID UniqueID) []UniqueID {
	segments := m.SelectSegments(func(segment *SegmentInfo) bool {
		return isSegmentHealthy(segment) &&
			segment.CollectionID == collectionID &&
			segment.PartitionID == partitionID
	})

	return lo.Map(segments, func(segment *SegmentInfo, _ int) int64 {
		return segment.ID
	})
}

// GetSegmentsIDOfPartition returns all segments ids which collection & partition equals to provided `collectionID`, `partitionID`
func (m *meta) GetSegmentsIDOfPartitionWithDropped(collectionID, partitionID UniqueID) []UniqueID {
	segments := m.SelectSegments(func(segment *SegmentInfo) bool {
		return segment.GetState() != commonpb.SegmentState_SegmentStateNone &&
			segment.GetState() != commonpb.SegmentState_NotExist &&
			segment.CollectionID == collectionID &&
			segment.PartitionID == partitionID
	})

	return lo.Map(segments, func(segment *SegmentInfo, _ int) int64 {
		return segment.ID
	})
}

// GetNumRowsOfPartition returns row count of segments belongs to provided collection & partition
func (m *meta) GetNumRowsOfPartition(collectionID UniqueID, partitionID UniqueID) int64 {
	m.RLock()
	defer m.RUnlock()
	var ret int64
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		if isSegmentHealthy(segment) && segment.CollectionID == collectionID && segment.PartitionID == partitionID {
			ret += segment.NumOfRows
		}
	}
	return ret
}

// GetUnFlushedSegments get all segments which state is not `Flushing` nor `Flushed`
func (m *meta) GetUnFlushedSegments() []*SegmentInfo {
	return m.SelectSegments(func(segment *SegmentInfo) bool {
		return segment.GetState() == commonpb.SegmentState_Growing || segment.GetState() == commonpb.SegmentState_Sealed
	})
}

// GetFlushingSegments get all segments which state is `Flushing`
func (m *meta) GetFlushingSegments() []*SegmentInfo {
	return m.SelectSegments(func(segment *SegmentInfo) bool {
		return segment.GetState() == commonpb.SegmentState_Flushing
	})
}

// SelectSegments select segments with selector
func (m *meta) SelectSegments(selector SegmentInfoSelector) []*SegmentInfo {
	m.RLock()
	defer m.RUnlock()
	var ret []*SegmentInfo
	segments := m.segments.GetSegments()
	for _, info := range segments {
		if selector(info) {
			ret = append(ret, info)
		}
	}
	return ret
}

// AddAllocation add allocation in segment
func (m *meta) AddAllocation(segmentID UniqueID, allocation *Allocation) error {
	log.Debug("meta update: add allocation",
		zap.Int64("segmentID", segmentID),
		zap.Any("allocation", allocation))
	m.Lock()
	defer m.Unlock()
	curSegInfo := m.segments.GetSegment(segmentID)
	if curSegInfo == nil {
		// TODO: Error handling.
		log.Error("meta update: add allocation failed - segment not found", zap.Int64("segmentID", segmentID))
		return errors.New("meta update: add allocation failed - segment not found")
	}
	// As we use global segment lastExpire to guarantee data correctness after restart
	// there is no need to persist allocation to meta store, only update allocation in-memory meta.
	m.segments.AddAllocation(segmentID, allocation)
	log.Info("meta update: add allocation - complete", zap.Int64("segmentID", segmentID))
	return nil
}

// SetAllocations set Segment allocations, will overwrite ALL original allocations
// Note that allocations is not persisted in KV store
func (m *meta) SetAllocations(segmentID UniqueID, allocations []*Allocation) {
	m.Lock()
	defer m.Unlock()
	m.segments.SetAllocations(segmentID, allocations)
}

// SetCurrentRows set current row count for segment with provided `segmentID`
// Note that currRows is not persisted in KV store
func (m *meta) SetCurrentRows(segmentID UniqueID, rows int64) {
	m.Lock()
	defer m.Unlock()
	m.segments.SetCurrentRows(segmentID, rows)
}

// SetLastExpire set lastExpire time for segment
// Note that last is not necessary to store in KV meta
func (m *meta) SetLastExpire(segmentID UniqueID, lastExpire uint64) {
	m.Lock()
	defer m.Unlock()
	clonedSegment := m.segments.GetSegment(segmentID).Clone()
	clonedSegment.LastExpireTime = lastExpire
	m.segments.SetSegment(segmentID, clonedSegment)
}

// SetLastFlushTime set LastFlushTime for segment with provided `segmentID`
// Note that lastFlushTime is not persisted in KV store
func (m *meta) SetLastFlushTime(segmentID UniqueID, t time.Time) {
	m.Lock()
	defer m.Unlock()
	m.segments.SetFlushTime(segmentID, t)
}

// SetSegmentCompacting sets compaction state for segment
func (m *meta) SetSegmentCompacting(segmentID UniqueID, compacting bool) {
	m.Lock()
	defer m.Unlock()

	m.segments.SetIsCompacting(segmentID, compacting)
}

// PrepareCompleteCompactionMutation returns
// - the segment info of compactedFrom segments before compaction to revert
// - the segment info of compactedFrom segments after compaction to alter
// - the segment info of compactedTo segment after compaction to add
// The compactedTo segment could contain 0 numRows
func (m *meta) PrepareCompleteCompactionMutation(plan *datapb.CompactionPlan,
	result *datapb.CompactionPlanResult,
) ([]*SegmentInfo, []*SegmentInfo, *SegmentInfo, *segMetricMutation, error) {
	log.Info("meta update: prepare for complete compaction mutation")
	compactionLogs := plan.GetSegmentBinlogs()
	m.Lock()
	defer m.Unlock()

	var (
		oldSegments = make([]*SegmentInfo, 0, len(compactionLogs))
		modSegments = make([]*SegmentInfo, 0, len(compactionLogs))
	)

	metricMutation := &segMetricMutation{
		stateChange: make(map[string]map[string]int),
	}
	for _, cl := range compactionLogs {
		if segment := m.segments.GetSegment(cl.GetSegmentID()); segment != nil {
			oldSegments = append(oldSegments, segment.Clone())

			cloned := segment.Clone()
			updateSegStateAndPrepareMetrics(cloned, commonpb.SegmentState_Dropped, metricMutation)
			cloned.DroppedAt = uint64(time.Now().UnixNano())
			cloned.Compacted = true
			modSegments = append(modSegments, cloned)
		}
	}

	var startPosition, dmlPosition *msgpb.MsgPosition
	for _, s := range modSegments {
		if dmlPosition == nil ||
			s.GetDmlPosition() != nil && s.GetDmlPosition().GetTimestamp() < dmlPosition.GetTimestamp() {
			dmlPosition = s.GetDmlPosition()
		}

		if startPosition == nil ||
			s.GetStartPosition() != nil && s.GetStartPosition().GetTimestamp() < startPosition.GetTimestamp() {
			startPosition = s.GetStartPosition()
		}
	}

	// find new added delta logs when executing compaction
	var originDeltalogs []*datapb.FieldBinlog
	for _, s := range modSegments {
		originDeltalogs = append(originDeltalogs, s.GetDeltalogs()...)
	}

	var deletedDeltalogs []*datapb.FieldBinlog
	for _, l := range compactionLogs {
		deletedDeltalogs = append(deletedDeltalogs, l.GetDeltalogs()...)
	}

	// MixCompaction / MergeCompaction will generates one and only one segment
	compactToSegment := result.GetSegments()[0]

	newAddedDeltalogs := m.updateDeltalogs(originDeltalogs, deletedDeltalogs, nil)
	copiedDeltalogs, err := m.copyDeltaFiles(newAddedDeltalogs, modSegments[0].CollectionID, modSegments[0].PartitionID, compactToSegment.GetSegmentID())
	if err != nil {
		return nil, nil, nil, nil, err
	}
	deltalogs := append(compactToSegment.GetDeltalogs(), copiedDeltalogs...)

	compactionFrom := make([]UniqueID, 0, len(modSegments))
	for _, s := range modSegments {
		compactionFrom = append(compactionFrom, s.GetID())
	}

	segmentInfo := &datapb.SegmentInfo{
		ID:                  compactToSegment.GetSegmentID(),
		CollectionID:        modSegments[0].CollectionID,
		PartitionID:         modSegments[0].PartitionID,
		InsertChannel:       modSegments[0].InsertChannel,
		NumOfRows:           compactToSegment.NumOfRows,
		State:               commonpb.SegmentState_Flushing,
		MaxRowNum:           modSegments[0].MaxRowNum,
		Binlogs:             compactToSegment.GetInsertLogs(),
		Statslogs:           compactToSegment.GetField2StatslogPaths(),
		Deltalogs:           deltalogs,
		StartPosition:       startPosition,
		DmlPosition:         dmlPosition,
		CreatedByCompaction: true,
		CompactionFrom:      compactionFrom,
		LastExpireTime:      plan.GetStartTime(),
	}
	segment := NewSegmentInfo(segmentInfo)
	metricMutation.addNewSeg(segment.GetState(), segment.GetLevel(), segment.GetNumOfRows())
	log.Info("meta update: prepare for complete compaction mutation - complete",
		zap.Int64("collectionID", segment.GetCollectionID()),
		zap.Int64("partitionID", segment.GetPartitionID()),
		zap.Int64("new segment ID", segment.GetID()),
		zap.Int64("new segment num of rows", segment.GetNumOfRows()),
		zap.Any("compacted from", segment.GetCompactionFrom()))

	return oldSegments, modSegments, segment, metricMutation, nil
}

func (m *meta) copyDeltaFiles(binlogs []*datapb.FieldBinlog, collectionID, partitionID, targetSegmentID int64) ([]*datapb.FieldBinlog, error) {
	ret := make([]*datapb.FieldBinlog, 0, len(binlogs))
	for _, fieldBinlog := range binlogs {
		fieldBinlog = proto.Clone(fieldBinlog).(*datapb.FieldBinlog)
		for _, binlog := range fieldBinlog.Binlogs {
			blobKey := metautil.JoinIDPath(collectionID, partitionID, targetSegmentID, binlog.LogID)
			blobPath := path.Join(m.chunkManager.RootPath(), common.SegmentDeltaLogPath, blobKey)
			blob, err := m.chunkManager.Read(m.ctx, binlog.LogPath)
			if err != nil {
				return nil, err
			}
			err = m.chunkManager.Write(m.ctx, blobPath, blob)
			if err != nil {
				return nil, err
			}
			binlog.LogPath = blobPath
		}
		ret = append(ret, fieldBinlog)
	}
	return ret, nil
}

func (m *meta) alterMetaStoreAfterCompaction(segmentCompactTo *SegmentInfo, segmentsCompactFrom []*SegmentInfo) error {
	modInfos := make([]*datapb.SegmentInfo, 0, len(segmentsCompactFrom))
	for _, segment := range segmentsCompactFrom {
		modInfos = append(modInfos, segment.SegmentInfo)
	}

	newSegment := segmentCompactTo.SegmentInfo

	modSegIDs := lo.Map(modInfos, func(segment *datapb.SegmentInfo, _ int) int64 { return segment.GetID() })
	if newSegment.GetNumOfRows() == 0 {
		newSegment.State = commonpb.SegmentState_Dropped
	}

	log.Debug("meta update: alter meta store for compaction updates",
		zap.Int64s("compact from segments (segments to be updated as dropped)", modSegIDs),
		zap.Int64("new segmentID", newSegment.GetID()),
		zap.Int("binlog", len(newSegment.GetBinlogs())),
		zap.Int("stats log", len(newSegment.GetStatslogs())),
		zap.Int("delta logs", len(newSegment.GetDeltalogs())),
		zap.Int64("compact to segment", newSegment.GetID()))

	err := m.catalog.AlterSegments(m.ctx, append(modInfos, newSegment), metastore.BinlogsIncrement{
		Segment: newSegment,
	})
	if err != nil {
		log.Warn("fail to alter segments and new segment", zap.Error(err))
		return err
	}

	var compactFromIDs []int64
	for _, v := range segmentsCompactFrom {
		compactFromIDs = append(compactFromIDs, v.GetID())
	}
	m.Lock()
	defer m.Unlock()
	for _, s := range segmentsCompactFrom {
		m.segments.SetSegment(s.GetID(), s)
	}
	m.segments.SetSegment(segmentCompactTo.GetID(), segmentCompactTo)
	log.Info("meta update: alter in memory meta after compaction - complete",
		zap.Int64("compact to segment ID", segmentCompactTo.GetID()),
		zap.Int64s("compact from segment IDs", compactFromIDs))
	return nil
}

func (m *meta) updateBinlogs(origin []*datapb.FieldBinlog, removes []*datapb.FieldBinlog, adds []*datapb.FieldBinlog) []*datapb.FieldBinlog {
	fieldBinlogs := make(map[int64]map[string]*datapb.Binlog)
	for _, f := range origin {
		fid := f.GetFieldID()
		if _, ok := fieldBinlogs[fid]; !ok {
			fieldBinlogs[fid] = make(map[string]*datapb.Binlog)
		}
		for _, p := range f.GetBinlogs() {
			fieldBinlogs[fid][p.GetLogPath()] = p
		}
	}

	for _, f := range removes {
		fid := f.GetFieldID()
		if _, ok := fieldBinlogs[fid]; !ok {
			continue
		}
		for _, p := range f.GetBinlogs() {
			delete(fieldBinlogs[fid], p.GetLogPath())
		}
	}

	for _, f := range adds {
		fid := f.GetFieldID()
		if _, ok := fieldBinlogs[fid]; !ok {
			fieldBinlogs[fid] = make(map[string]*datapb.Binlog)
		}
		for _, p := range f.GetBinlogs() {
			fieldBinlogs[fid][p.GetLogPath()] = p
		}
	}

	res := make([]*datapb.FieldBinlog, 0, len(fieldBinlogs))
	for fid, logs := range fieldBinlogs {
		if len(logs) == 0 {
			continue
		}

		binlogs := make([]*datapb.Binlog, 0, len(logs))
		for _, log := range logs {
			binlogs = append(binlogs, log)
		}

		field := &datapb.FieldBinlog{FieldID: fid, Binlogs: binlogs}
		res = append(res, field)
	}
	return res
}

func (m *meta) updateDeltalogs(origin []*datapb.FieldBinlog, removes []*datapb.FieldBinlog, adds []*datapb.FieldBinlog) []*datapb.FieldBinlog {
	res := make([]*datapb.FieldBinlog, 0, len(origin))
	for _, fbl := range origin {
		logs := make(map[string]*datapb.Binlog)
		for _, d := range fbl.GetBinlogs() {
			logs[d.GetLogPath()] = d
		}
		for _, remove := range removes {
			if remove.GetFieldID() == fbl.GetFieldID() {
				for _, r := range remove.GetBinlogs() {
					delete(logs, r.GetLogPath())
				}
			}
		}
		binlogs := make([]*datapb.Binlog, 0, len(logs))
		for _, l := range logs {
			binlogs = append(binlogs, l)
		}
		if len(binlogs) > 0 {
			res = append(res, &datapb.FieldBinlog{
				FieldID: fbl.GetFieldID(),
				Binlogs: binlogs,
			})
		}
	}

	return res
}

// buildSegment utility function for compose datapb.SegmentInfo struct with provided info
func buildSegment(collectionID UniqueID, partitionID UniqueID, segmentID UniqueID, channelName string, isImporting bool) *SegmentInfo {
	info := &datapb.SegmentInfo{
		ID:            segmentID,
		CollectionID:  collectionID,
		PartitionID:   partitionID,
		InsertChannel: channelName,
		NumOfRows:     0,
		State:         commonpb.SegmentState_Growing,
		IsImporting:   isImporting,
	}
	return NewSegmentInfo(info)
}

func isSegmentHealthy(segment *SegmentInfo) bool {
	return segment != nil &&
		segment.GetState() != commonpb.SegmentState_SegmentStateNone &&
		segment.GetState() != commonpb.SegmentState_NotExist &&
		segment.GetState() != commonpb.SegmentState_Dropped
}

func (m *meta) HasSegments(segIDs []UniqueID) (bool, error) {
	m.RLock()
	defer m.RUnlock()

	for _, segID := range segIDs {
		if _, ok := m.segments.segments[segID]; !ok {
			return false, fmt.Errorf("segment is not exist with ID = %d", segID)
		}
	}
	return true, nil
}

func (m *meta) GetCompactionTo(segmentID int64) *SegmentInfo {
	m.RLock()
	defer m.RUnlock()

	segments := m.segments.GetSegments()
	for _, segment := range segments {
		parents := typeutil.NewUniqueSet(segment.GetCompactionFrom()...)
		if parents.Contain(segmentID) {
			return segment
		}
	}
	return nil
}

// UpdateChannelCheckpoint updates and saves channel checkpoint.
func (m *meta) UpdateChannelCheckpoint(vChannel string, pos *msgpb.MsgPosition) error {
	if pos == nil || pos.GetMsgID() == nil {
		return fmt.Errorf("channelCP is nil, vChannel=%s", vChannel)
	}

	m.channelCPLocks.Lock(vChannel)
	defer m.channelCPLocks.Unlock(vChannel)

	oldPosition, ok := m.channelCPs.Get(vChannel)
	if !ok || oldPosition.Timestamp < pos.Timestamp {
		err := m.catalog.SaveChannelCheckpoint(m.ctx, vChannel, pos)
		if err != nil {
			return err
		}
		m.channelCPs.Insert(vChannel, pos)
		ts, _ := tsoutil.ParseTS(pos.Timestamp)
		log.Info("UpdateChannelCheckpoint done",
			zap.String("vChannel", vChannel),
			zap.Uint64("ts", pos.GetTimestamp()),
			zap.ByteString("msgID", pos.GetMsgID()),
			zap.Time("time", ts))
		metrics.DataCoordCheckpointLag.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), vChannel).
			Set(float64(time.Since(ts).Milliseconds()))
	}
	return nil
}

func (m *meta) GetChannelCheckpoint(vChannel string) *msgpb.MsgPosition {
	m.channelCPLocks.Lock(vChannel)
	defer m.channelCPLocks.Unlock(vChannel)
	v, ok := m.channelCPs.Get(vChannel)
	if !ok {
		return nil
	}
	return proto.Clone(v).(*msgpb.MsgPosition)
}

func (m *meta) DropChannelCheckpoint(vChannel string) error {
	m.channelCPLocks.Lock(vChannel)
	defer m.channelCPLocks.Unlock(vChannel)
	err := m.catalog.DropChannelCheckpoint(m.ctx, vChannel)
	if err != nil {
		return err
	}
	m.channelCPs.Remove(vChannel)
	log.Debug("DropChannelCheckpoint done", zap.String("vChannel", vChannel))
	return nil
}

func (m *meta) GcConfirm(ctx context.Context, collectionID, partitionID UniqueID) bool {
	return m.catalog.GcConfirm(ctx, collectionID, partitionID)
}

func (m *meta) GetCompactableSegmentGroupByCollection() map[int64][]*SegmentInfo {
	allSegs := m.SelectSegments(func(segment *SegmentInfo) bool {
		return isSegmentHealthy(segment) &&
			isFlush(segment) && // sealed segment
			!segment.isCompacting && // not compacting now
			!segment.GetIsImporting() // not importing now
	})

	ret := make(map[int64][]*SegmentInfo)
	for _, seg := range allSegs {
		if _, ok := ret[seg.CollectionID]; !ok {
			ret[seg.CollectionID] = make([]*SegmentInfo, 0)
		}

		ret[seg.CollectionID] = append(ret[seg.CollectionID], seg)
	}

	return ret
}

func (m *meta) GetEarliestStartPositionOfGrowingSegments(label *CompactionGroupLabel) *msgpb.MsgPosition {
	segments := m.SelectSegments(func(segment *SegmentInfo) bool {
		return segment.GetState() == commonpb.SegmentState_Growing &&
			segment.GetCollectionID() == label.CollectionID &&
			segment.GetPartitionID() == label.PartitionID &&
			segment.GetInsertChannel() == label.Channel
	})

	var earliest *msgpb.MsgPosition
	for _, seg := range segments {
		if earliest == nil || earliest.GetTimestamp() > seg.GetStartPosition().GetTimestamp() {
			earliest = seg.GetStartPosition()
		}
	}
	return earliest
}

// addNewSeg update metrics update for a new segment.
func (s *segMetricMutation) addNewSeg(state commonpb.SegmentState, level datapb.SegmentLevel, rowCount int64) {
	if _, ok := s.stateChange[level.String()]; !ok {
		s.stateChange[level.String()] = make(map[string]int)
	}
	s.stateChange[level.String()][state.String()] += 1

	s.rowCountChange += rowCount
	s.rowCountAccChange += rowCount
}

// commit persists all updates in current segMetricMutation, should and must be called AFTER segment state change
// has persisted in Etcd.
func (s *segMetricMutation) commit() {
	for level, submap := range s.stateChange {
		for state, change := range submap {
			metrics.DataCoordNumSegments.WithLabelValues(state, level).Add(float64(change))
		}
	}
	metrics.DataCoordNumStoredRowsCounter.WithLabelValues().Add(float64(s.rowCountAccChange))
}

// append updates current segMetricMutation when segment state change happens.
func (s *segMetricMutation) append(oldState, newState commonpb.SegmentState, level datapb.SegmentLevel, rowCountUpdate int64) {
	if oldState != newState {
		if _, ok := s.stateChange[level.String()]; !ok {
			s.stateChange[level.String()] = make(map[string]int)
		}
		s.stateChange[level.String()][oldState.String()] -= 1
		s.stateChange[level.String()][newState.String()] += 1
	}
	// Update # of rows on new flush operations and drop operations.
	if isFlushState(newState) && !isFlushState(oldState) {
		// If new flush.
		s.rowCountChange += rowCountUpdate
		s.rowCountAccChange += rowCountUpdate
	} else if newState == commonpb.SegmentState_Dropped && oldState != newState {
		// If new drop.
		s.rowCountChange -= rowCountUpdate
	}
}

func isFlushState(state commonpb.SegmentState) bool {
	return state == commonpb.SegmentState_Flushing || state == commonpb.SegmentState_Flushed
}

// updateSegStateAndPrepareMetrics updates a segment's in-memory state and prepare for the corresponding metric update.
func updateSegStateAndPrepareMetrics(segToUpdate *SegmentInfo, targetState commonpb.SegmentState, metricMutation *segMetricMutation) {
	log.Debug("updating segment state and updating metrics",
		zap.Int64("segmentID", segToUpdate.GetID()),
		zap.String("old state", segToUpdate.GetState().String()),
		zap.String("new state", targetState.String()),
		zap.Int64("# of rows", segToUpdate.GetNumOfRows()))
	metricMutation.append(segToUpdate.GetState(), targetState, segToUpdate.GetLevel(), segToUpdate.GetNumOfRows())
	segToUpdate.State = targetState
}
