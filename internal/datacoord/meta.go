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
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util"
	"go.uber.org/zap"
)

const (
	metaPrefix          = "datacoord-meta"
	segmentPrefix       = metaPrefix + "/s"
	channelRemovePrefix = metaPrefix + "/channel-removal"

	removeFlagTomestone = "removed"
)

type meta struct {
	sync.RWMutex
	client      kv.TxnKV                            // client of a reliable kv service, i.e. etcd client
	collections map[UniqueID]*datapb.CollectionInfo // collection id to collection info
	segments    *SegmentsInfo                       // segment id to segment info
}

// NewMeta creates meta from provided `kv.TxnKV`
func newMeta(kv kv.TxnKV) (*meta, error) {
	mt := &meta{
		client:      kv,
		collections: make(map[UniqueID]*datapb.CollectionInfo),
		segments:    NewSegmentsInfo(),
	}
	err := mt.reloadFromKV()
	if err != nil {
		return nil, err
	}
	return mt, nil
}

// reloadFromKV loads meta from KV storage
func (m *meta) reloadFromKV() error {
	_, values, err := m.client.LoadWithPrefix(segmentPrefix)
	if err != nil {
		return err
	}
	metrics.DataCoordNumCollections.WithLabelValues().Set(0)
	metrics.DataCoordNumSegments.WithLabelValues(metrics.SealedSegmentLabel).Set(0)
	metrics.DataCoordNumSegments.WithLabelValues(metrics.GrowingSegmentLabel).Set(0)
	metrics.DataCoordNumSegments.WithLabelValues(metrics.FlushedSegmentLabel).Set(0)
	metrics.DataCoordNumSegments.WithLabelValues(metrics.FlushingSegmentLabel).Set(0)
	metrics.DataCoordNumSegments.WithLabelValues(metrics.DropedSegmentLabel).Set(0)
	metrics.DataCoordNumStoredRows.WithLabelValues().Set(0)
	numStoredRows := int64(0)
	for _, value := range values {
		segmentInfo := &datapb.SegmentInfo{}
		err = proto.Unmarshal([]byte(value), segmentInfo)
		if err != nil {
			return fmt.Errorf("DataCoord reloadFromKV UnMarshal datapb.SegmentInfo err:%w", err)
		}
		state := segmentInfo.GetState()
		m.segments.SetSegment(segmentInfo.GetID(), NewSegmentInfo(segmentInfo))
		metrics.DataCoordNumSegments.WithLabelValues(string(state)).Inc()
		if state == commonpb.SegmentState_Flushed {
			numStoredRows += segmentInfo.GetNumOfRows()
		}
	}
	metrics.DataCoordNumStoredRows.WithLabelValues().Set(float64(numStoredRows))
	metrics.DataCoordNumStoredRowsCounter.WithLabelValues().Add(float64(numStoredRows))
	return nil
}

// AddCollection adds a collection into meta
// Note that collection info is just for caching and will not be set into etcd from datacoord
func (m *meta) AddCollection(collection *datapb.CollectionInfo) {
	m.Lock()
	defer m.Unlock()
	m.collections[collection.ID] = collection
	metrics.DataCoordNumCollections.WithLabelValues().Set(float64(len(m.collections)))
}

// GetCollection returns collection info with provided collection id from local cache
func (m *meta) GetCollection(collectionID UniqueID) *datapb.CollectionInfo {
	m.RLock()
	defer m.RUnlock()
	collection, ok := m.collections[collectionID]
	if !ok {
		return nil
	}
	return collection
}

// chanPartSegments is an internal result struct, which is aggregates of SegmentInfos with same collectionID, partitionID and channelName
type chanPartSegments struct {
	collecionID UniqueID
	partitionID UniqueID
	channelName string
	segments    []*SegmentInfo
}

// GetSegmentsChanPart returns segments organized in Channel-Parition dimension with selector applied
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
				collecionID: segmentInfo.CollectionID,
				partitionID: segmentInfo.PartitionID,
				channelName: segmentInfo.InsertChannel,
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

// AddSegment records segment info, persisting info into kv store
func (m *meta) AddSegment(segment *SegmentInfo) error {
	m.Lock()
	defer m.Unlock()
	m.segments.SetSegment(segment.GetID(), segment)
	if err := m.saveSegmentInfo(segment); err != nil {
		return err
	}
	metrics.DataCoordNumSegments.WithLabelValues(string(segment.GetState())).Inc()
	return nil
}

// DropSegment remove segment with provided id, etcd persistence also removed
func (m *meta) DropSegment(segmentID UniqueID) error {
	m.Lock()
	defer m.Unlock()
	segment := m.segments.GetSegment(segmentID)
	if segment == nil {
		return nil
	}
	if err := m.removeSegmentInfo(segment); err != nil {
		return err
	}
	metrics.DataCoordNumSegments.WithLabelValues(metrics.DropedSegmentLabel).Inc()
	m.segments.DropSegment(segmentID)
	return nil
}

// GetSegment returns segment info with provided id
// if not segment is found, nil will be returned
func (m *meta) GetSegment(segID UniqueID) *SegmentInfo {
	m.RLock()
	defer m.RUnlock()
	segment := m.segments.GetSegment(segID)
	if segment != nil && isSegmentHealthy(segment) {
		return segment
	}
	return nil
}

// GetAllSegment returns segment info with provided id
// different from GetSegment, this will return unhealthy segment as well
func (m *meta) GetAllSegment(segID UniqueID) *SegmentInfo {
	m.RLock()
	defer m.RUnlock()
	segment := m.segments.GetSegment(segID)
	if segment != nil {
		return segment
	}
	return nil
}

// SetState setting segment with provided ID state
func (m *meta) SetState(segmentID UniqueID, state commonpb.SegmentState) error {
	m.Lock()
	defer m.Unlock()
	curSegInfo := m.segments.GetSegment(segmentID)
	if curSegInfo == nil {
		return nil
	}
	oldState := curSegInfo.GetState()
	m.segments.SetState(segmentID, state)
	curSegInfo = m.segments.GetSegment(segmentID)
	if curSegInfo != nil && isSegmentHealthy(curSegInfo) {
		err := m.saveSegmentInfo(curSegInfo)
		if err == nil {
			metrics.DataCoordNumSegments.WithLabelValues(string(oldState)).Dec()
			metrics.DataCoordNumSegments.WithLabelValues(string(state)).Inc()
			if state == commonpb.SegmentState_Flushed {
				metrics.DataCoordNumStoredRows.WithLabelValues().Add(float64(curSegInfo.GetNumOfRows()))
				metrics.DataCoordNumStoredRowsCounter.WithLabelValues().Add(float64(curSegInfo.GetNumOfRows()))
			} else if oldState == commonpb.SegmentState_Flushed {
				metrics.DataCoordNumStoredRows.WithLabelValues().Sub(float64(curSegInfo.GetNumOfRows()))
			}
		}
		return err
	}
	return nil
}

// UpdateFlushSegmentsInfo update segment partial/completed flush info
// `flushed` parameter indicating whether segment is flushed completely or partially
// `binlogs`, `checkpoints` and `statPositions` are persistence data for segment
func (m *meta) UpdateFlushSegmentsInfo(
	segmentID UniqueID,
	flushed bool,
	dropped bool,
	importing bool,
	binlogs, statslogs, deltalogs []*datapb.FieldBinlog,
	checkpoints []*datapb.CheckPoint,
	startPositions []*datapb.SegmentStartPosition,
) error {
	m.Lock()
	defer m.Unlock()

	log.Info("update flush segments info", zap.Int64("segmentId", segmentID),
		zap.Int("binlog", len(binlogs)),
		zap.Int("statslog", len(statslogs)),
		zap.Int("deltalogs", len(deltalogs)),
		zap.Bool("flushed", flushed),
		zap.Bool("dropped", dropped),
		zap.Bool("importing", importing))
	segment := m.segments.GetSegment(segmentID)
	if importing {
		m.segments.SetRowCount(segmentID, segment.currRows)
		segment = m.segments.GetSegment(segmentID)
	}
	if segment == nil || !isSegmentHealthy(segment) {
		return nil
	}

	clonedSegment := segment.Clone()

	modSegments := make(map[UniqueID]*SegmentInfo)

	if flushed {
		clonedSegment.State = commonpb.SegmentState_Flushing
		modSegments[segmentID] = clonedSegment
	}

	if dropped {
		clonedSegment.State = commonpb.SegmentState_Dropped
		clonedSegment.DroppedAt = uint64(time.Now().UnixNano())
		modSegments[segmentID] = clonedSegment
	}
	// TODO add diff encoding and compression
	currBinlogs := clonedSegment.GetBinlogs()

	var getFieldBinlogs = func(id UniqueID, binlogs []*datapb.FieldBinlog) *datapb.FieldBinlog {
		for _, binlog := range binlogs {
			if id == binlog.GetFieldID() {
				return binlog
			}
		}
		return nil
	}
	// binlogs
	for _, tBinlogs := range binlogs {
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
	for _, tStatsLogs := range statslogs {
		fieldStatsLog := getFieldBinlogs(tStatsLogs.GetFieldID(), currStatsLogs)
		if fieldStatsLog == nil {
			currStatsLogs = append(currStatsLogs, tStatsLogs)
		} else {
			fieldStatsLog.Binlogs = append(fieldStatsLog.Binlogs, tStatsLogs.Binlogs...)
		}
	}
	clonedSegment.Statslogs = currStatsLogs
	// deltalogs
	currDeltaLogs := clonedSegment.GetDeltalogs()
	for _, tDeltaLogs := range deltalogs {
		fieldDeltaLogs := getFieldBinlogs(tDeltaLogs.GetFieldID(), currDeltaLogs)
		if fieldDeltaLogs == nil {
			currDeltaLogs = append(currDeltaLogs, tDeltaLogs)
		} else {
			fieldDeltaLogs.Binlogs = append(fieldDeltaLogs.Binlogs, tDeltaLogs.Binlogs...)
		}
	}
	clonedSegment.Deltalogs = currDeltaLogs

	modSegments[segmentID] = clonedSegment

	var getClonedSegment = func(segmentID UniqueID) *SegmentInfo {
		if s, ok := modSegments[segmentID]; ok {
			return s
		}
		if s := m.segments.GetSegment(segmentID); s != nil && isSegmentHealthy(s) {
			return s.Clone()
		}
		return nil
	}

	for _, pos := range startPositions {
		if len(pos.GetStartPosition().GetMsgID()) == 0 {
			continue
		}
		s := getClonedSegment(pos.GetSegmentID())
		if s == nil {
			continue
		}

		s.StartPosition = pos.GetStartPosition()
		modSegments[pos.GetSegmentID()] = s
	}

	for _, cp := range checkpoints {
		s := getClonedSegment(cp.GetSegmentID())
		if s == nil {
			continue
		}

		if s.DmlPosition != nil && s.DmlPosition.Timestamp >= cp.Position.Timestamp {
			// segment position in etcd is larger than checkpoint, then dont change it
			continue
		}

		s.DmlPosition = cp.GetPosition()
		s.NumOfRows = cp.GetNumOfRows()
		modSegments[cp.GetSegmentID()] = s
	}

	kv := make(map[string]string)
	for _, segment := range modSegments {
		segBytes, err := proto.Marshal(segment.SegmentInfo)
		if err != nil {
			return fmt.Errorf("dataCoord UpdateFlushSegmentsInfo segmentID:%d, marshal failed:%w", segment.GetID(), err)
		}
		key := buildSegmentPath(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
		kv[key] = string(segBytes)
	}

	if len(kv) == 0 {
		return nil
	}

	if err := m.saveKvTxn(kv); err != nil {
		log.Error("failed to store flush segment info into Etcd", zap.Error(err))
		return err
	}
	oldSegmentState := segment.GetState()
	newSegmentState := clonedSegment.GetState()
	metrics.DataCoordNumSegments.WithLabelValues(string(oldSegmentState)).Dec()
	metrics.DataCoordNumSegments.WithLabelValues(string(newSegmentState)).Inc()
	if newSegmentState == commonpb.SegmentState_Flushed {
		metrics.DataCoordNumStoredRows.WithLabelValues().Add(float64(clonedSegment.GetNumOfRows()))
		metrics.DataCoordNumStoredRowsCounter.WithLabelValues().Add(float64(clonedSegment.GetNumOfRows()))
	} else if oldSegmentState == commonpb.SegmentState_Flushed {
		metrics.DataCoordNumStoredRows.WithLabelValues().Sub(float64(segment.GetNumOfRows()))
	}
	// update memory status
	for id, s := range modSegments {
		m.segments.SetSegment(id, s)
	}
	log.Info("update flush segments info successfully", zap.Int64("segmentId", segmentID))
	return nil
}

// UpdateDropChannelSegmentInfo updates segment checkpoints and binlogs before drop
// reusing segment info to pass segment id, binlogs, statslog, deltalog, start position and checkpoint
func (m *meta) UpdateDropChannelSegmentInfo(channel string, segments []*SegmentInfo) error {
	m.Lock()
	defer m.Unlock()

	modSegments := make(map[UniqueID]*SegmentInfo)
	originSegments := make(map[UniqueID]*SegmentInfo)

	for _, seg2Drop := range segments {
		segment := m.mergeDropSegment(seg2Drop)
		if segment != nil {
			originSegments[seg2Drop.GetID()] = seg2Drop
			modSegments[seg2Drop.GetID()] = segment
		}
	}
	// set all channels of channel to dropped
	for _, seg := range m.segments.segments {
		if seg.InsertChannel != channel {
			continue
		}
		_, ok := modSegments[seg.ID]
		// seg inf mod segments are all in dropped state
		if !ok {
			clonedSeg := seg.Clone()
			clonedSeg.State = commonpb.SegmentState_Dropped
			modSegments[seg.ID] = clonedSeg
			originSegments[seg.GetID()] = seg
		}
	}
	err := m.batchSaveDropSegments(channel, modSegments)
	if err == nil {
		for _, seg := range originSegments {
			state := seg.GetState()
			metrics.DataCoordNumSegments.WithLabelValues(
				string(state)).Dec()
			if state == commonpb.SegmentState_Flushed {
				metrics.DataCoordNumStoredRows.WithLabelValues().Sub(float64(seg.GetNumOfRows()))
			}
		}
	}
	return err
}

// mergeDropSegment merges drop segment information with meta segments
func (m *meta) mergeDropSegment(seg2Drop *SegmentInfo) *SegmentInfo {
	segment := m.segments.GetSegment(seg2Drop.ID)
	// healthy check makes sure the Idempotence
	if segment == nil || !isSegmentHealthy(segment) {
		log.Warn("UpdateDropChannel skipping nil or unhealthy", zap.Bool("is nil", segment == nil),
			zap.Bool("isHealthy", isSegmentHealthy(segment)))
		return nil
	}

	clonedSegment := segment.Clone()
	clonedSegment.State = commonpb.SegmentState_Dropped

	currBinlogs := clonedSegment.GetBinlogs()

	var getFieldBinlogs = func(id UniqueID, binlogs []*datapb.FieldBinlog) *datapb.FieldBinlog {
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
	return clonedSegment
}

// batchSaveDropSegments saves drop segments info with channel removal flag
// since the channel unwatching operation is not atomic here
// ** the removal flag is always with last batch
// ** the last batch must contains at least one segment
// 1. when failure occurs between batches, failover mechanism will continue with the earlist  checkpoint of this channel
//   since the flag is not marked so DataNode can re-consume the drop collection msg
// 2. when failure occurs between save meta and unwatch channel, the removal flag shall be check before let datanode watch this channel
func (m *meta) batchSaveDropSegments(channel string, modSegments map[int64]*SegmentInfo) error {

	// the limitation of etcd operations number per transaction is 128, since segment number might be enormous so we shall split
	// all save operations into batches

	// since the removal flag shall always be with the last batch, so the last batch shall be maxOperationNumber - 1
	for len(modSegments) > maxOperationsPerTxn-1 {
		err := m.saveDropSegmentAndRemove(channel, modSegments, false)
		if err != nil {
			return err
		}
	}

	// removal flag should be saved with last batch
	return m.saveDropSegmentAndRemove(channel, modSegments, true)
}

func (m *meta) saveDropSegmentAndRemove(channel string, modSegments map[int64]*SegmentInfo, withFlag bool) error {
	kv := make(map[string]string)
	update := make([]*SegmentInfo, 0, maxOperationsPerTxn)

	size := 0
	for id, s := range modSegments {
		key := buildSegmentPath(s.GetCollectionID(), s.GetPartitionID(), s.GetID())
		delete(modSegments, id)
		segBytes, err := proto.Marshal(s.SegmentInfo)
		if err != nil {
			return fmt.Errorf("DataCoord UpdateDropChannelSegmentInfo segmentID:%d, marshal failed:%w", s.GetID(), err)
		}
		kv[key] = string(segBytes)
		update = append(update, s)
		size += len(key) + len(segBytes)
		if len(kv) == maxOperationsPerTxn || len(modSegments) == 1 || size >= maxBytesPerTxn {
			break
		}
	}
	if withFlag {
		// add removal flag into meta, preventing non-atomic removal channel failure
		removalFlag := buildChannelRemovePath(channel)

		kv[removalFlag] = removeFlagTomestone
	}

	err := m.saveKvTxn(kv)
	if err != nil {
		log.Warn("Failed to txn save segment info batch for DropChannel", zap.Error(err))
		return err
	}

	// update memory info
	for _, s := range update {
		m.segments.SetSegment(s.GetID(), s)
	}
	metrics.DataCoordNumSegments.WithLabelValues(metrics.DropedSegmentLabel).Add(float64(len(update)))
	return nil
}

// FinishRemoveChannel removes channel remove flag after whole procedure is finished
func (m *meta) FinishRemoveChannel(channel string) error {
	key := buildChannelRemovePath(channel)
	return m.client.Remove(key)
}

// ChannelHasRemoveFlag
func (m *meta) ChannelHasRemoveFlag(channel string) bool {
	key := buildChannelRemovePath(channel)
	v, err := m.client.Load(key)
	if err != nil || v != removeFlagTomestone {
		return false
	}
	return true
}

// ListSegmentFiles lists all segments' logs
func (m *meta) ListSegmentFiles() []*datapb.Binlog {
	m.RLock()
	defer m.RUnlock()

	var logs []*datapb.Binlog

	for _, segment := range m.segments.GetSegments() {
		if !isSegmentHealthy(segment) {
			continue
		}
		for _, binlog := range segment.GetBinlogs() {
			logs = append(logs, binlog.Binlogs...)
		}

		for _, statLog := range segment.GetStatslogs() {
			logs = append(logs, statLog.Binlogs...)
		}

		for _, deltaLog := range segment.GetDeltalogs() {
			logs = append(logs, deltaLog.Binlogs...)
		}
	}
	return logs
}

// GetSegmentsByChannel returns all segment info which insert channel equals provided `dmlCh`
func (m *meta) GetSegmentsByChannel(dmlCh string) []*SegmentInfo {
	m.RLock()
	defer m.RUnlock()
	infos := make([]*SegmentInfo, 0)
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		if !isSegmentHealthy(segment) || segment.InsertChannel != dmlCh {
			continue
		}
		infos = append(infos, segment)
	}
	return infos
}

// GetSegmentsOfCollection get all segments of collection
func (m *meta) GetSegmentsOfCollection(collectionID UniqueID) []*SegmentInfo {
	m.RLock()
	defer m.RUnlock()

	ret := make([]*SegmentInfo, 0)
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		if isSegmentHealthy(segment) && segment.GetCollectionID() == collectionID {
			ret = append(ret, segment)
		}
	}
	return ret
}

// GetSegmentsIDOfCollection returns all segment ids which collection equals to provided `collectionID`
func (m *meta) GetSegmentsIDOfCollection(collectionID UniqueID) []UniqueID {
	m.RLock()
	defer m.RUnlock()
	ret := make([]UniqueID, 0)
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		if isSegmentHealthy(segment) && segment.CollectionID == collectionID {
			ret = append(ret, segment.ID)
		}
	}
	return ret
}

// GetSegmentsIDOfPartition returns all segments ids which collection & partition equals to provided `collectionID`, `partitionID`
func (m *meta) GetSegmentsIDOfPartition(collectionID, partitionID UniqueID) []UniqueID {
	m.RLock()
	defer m.RUnlock()
	ret := make([]UniqueID, 0)
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		if isSegmentHealthy(segment) && segment.CollectionID == collectionID && segment.PartitionID == partitionID {
			ret = append(ret, segment.ID)
		}
	}
	return ret
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
	m.RLock()
	defer m.RUnlock()
	ret := make([]*SegmentInfo, 0)
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		if segment.State == commonpb.SegmentState_Growing || segment.State == commonpb.SegmentState_Sealed {
			ret = append(ret, segment)
		}
	}
	return ret
}

// GetFlushingSegments get all segments which state is `Flushing`
func (m *meta) GetFlushingSegments() []*SegmentInfo {
	m.RLock()
	defer m.RUnlock()
	ret := make([]*SegmentInfo, 0)
	segments := m.segments.GetSegments()
	for _, info := range segments {
		if info.State == commonpb.SegmentState_Flushing {
			ret = append(ret, info)
		}
	}
	return ret
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
	m.Lock()
	defer m.Unlock()
	m.segments.AddAllocation(segmentID, allocation)
	if segInfo := m.segments.GetSegment(segmentID); segInfo != nil {
		return m.saveSegmentInfo(segInfo)
	}
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

func (m *meta) CompleteMergeCompaction(compactionLogs []*datapb.CompactionSegmentBinlogs, result *datapb.CompactionResult,
	canCompaction func(segment *datapb.CompactionSegmentBinlogs) bool) error {
	m.Lock()
	defer m.Unlock()

	segments := make([]*SegmentInfo, 0, len(compactionLogs))
	for _, cl := range compactionLogs {
		if !canCompaction(cl) {
			log.Warn("can not be compacted, segment has reference lock", zap.Int64("segmentID", cl.SegmentID))
			return fmt.Errorf("can not be compacted, segment with ID %d has reference lock", cl.SegmentID)
		}
		if segment := m.segments.GetSegment(cl.GetSegmentID()); segment != nil {
			cloned := segment.Clone()
			cloned.State = commonpb.SegmentState_Dropped
			cloned.DroppedAt = uint64(time.Now().UnixNano())
			segments = append(segments, cloned)
		}
	}

	var startPosition, dmlPosition *internalpb.MsgPosition
	for _, s := range segments {
		if dmlPosition == nil || s.GetDmlPosition().Timestamp < dmlPosition.Timestamp {
			dmlPosition = s.GetDmlPosition()
		}

		if startPosition == nil || s.GetStartPosition().Timestamp < startPosition.Timestamp {
			startPosition = s.GetStartPosition()
		}
	}

	// find new added delta logs when executing compaction
	var originDeltalogs []*datapb.FieldBinlog
	for _, s := range segments {
		originDeltalogs = append(originDeltalogs, s.GetDeltalogs()...)
	}

	var deletedDeltalogs []*datapb.FieldBinlog
	for _, l := range compactionLogs {
		deletedDeltalogs = append(deletedDeltalogs, l.GetDeltalogs()...)
	}

	newAddedDeltalogs := m.updateDeltalogs(originDeltalogs, deletedDeltalogs, nil)
	deltalogs := append(result.GetDeltalogs(), newAddedDeltalogs...)

	compactionFrom := make([]UniqueID, 0, len(segments))
	for _, s := range segments {
		compactionFrom = append(compactionFrom, s.GetID())
	}

	segmentInfo := &datapb.SegmentInfo{
		ID:                  result.GetSegmentID(),
		CollectionID:        segments[0].CollectionID,
		PartitionID:         segments[0].PartitionID,
		InsertChannel:       segments[0].InsertChannel,
		NumOfRows:           result.NumOfRows,
		State:               commonpb.SegmentState_Flushing,
		MaxRowNum:           segments[0].MaxRowNum,
		Binlogs:             result.GetInsertLogs(),
		Statslogs:           result.GetField2StatslogPaths(),
		Deltalogs:           deltalogs,
		StartPosition:       startPosition,
		DmlPosition:         dmlPosition,
		CreatedByCompaction: true,
		CompactionFrom:      compactionFrom,
	}

	segment := NewSegmentInfo(segmentInfo)

	data := make(map[string]string)

	for _, s := range segments {
		k, v, err := m.marshal(s)
		if err != nil {
			return err
		}
		data[k] = v
	}

	if segment.NumOfRows > 0 {
		k, v, err := m.marshal(segment)
		if err != nil {
			return err
		}
		data[k] = v
	}

	if err := m.saveKvTxn(data); err != nil {
		return err
	}

	for _, s := range segments {
		m.segments.SetSegment(s.GetID(), s)
	}

	// Handle empty segment generated by merge-compaction
	if segment.NumOfRows > 0 {
		m.segments.SetSegment(segment.GetID(), segment)
	}
	return nil
}

func (m *meta) CompleteInnerCompaction(segmentBinlogs *datapb.CompactionSegmentBinlogs, result *datapb.CompactionResult) error {
	m.Lock()
	defer m.Unlock()

	if segment := m.segments.GetSegment(segmentBinlogs.SegmentID); segment != nil {
		// The compaction deletes the entire segment
		if result.NumOfRows <= 0 {
			err := m.removeSegmentInfo(segment)
			if err != nil {
				return err
			}

			m.segments.DropSegment(segment.ID)
			return nil
		}

		cloned := segment.Clone()
		cloned.Binlogs = m.updateBinlogs(cloned.GetBinlogs(), segmentBinlogs.GetFieldBinlogs(), result.GetInsertLogs())
		cloned.Statslogs = m.updateBinlogs(cloned.GetStatslogs(), segmentBinlogs.GetField2StatslogPaths(), result.GetField2StatslogPaths())
		cloned.Deltalogs = m.updateDeltalogs(cloned.GetDeltalogs(), segmentBinlogs.GetDeltalogs(), result.GetDeltalogs())
		if err := m.saveSegmentInfo(cloned); err != nil {
			return err
		}

		cloned.isCompacting = false

		m.segments.SetSegment(cloned.GetID(), cloned)
	}
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

func (m *meta) marshal(segment *SegmentInfo) (string, string, error) {
	segBytes, err := proto.Marshal(segment.SegmentInfo)
	if err != nil {
		return "", "", fmt.Errorf("failed to marshal segment info, %v", err)
	}
	key := buildSegmentPath(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
	return key, string(segBytes), nil
}

// saveSegmentInfo utility function saving segment info into kv store
func (m *meta) saveSegmentInfo(segment *SegmentInfo) error {
	segBytes, err := proto.Marshal(segment.SegmentInfo)
	if err != nil {
		log.Error("DataCoord saveSegmentInfo marshal failed", zap.Int64("segmentID", segment.GetID()), zap.Error(err))
		return fmt.Errorf("DataCoord saveSegmentInfo segmentID:%d, marshal failed:%w", segment.GetID(), err)
	}
	kvs := make(map[string]string)
	dataKey := buildSegmentPath(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
	kvs[dataKey] = string(segBytes)
	if segment.State == commonpb.SegmentState_Flushed {
		handoffSegmentInfo := &querypb.SegmentInfo{
			SegmentID:           segment.ID,
			CollectionID:        segment.CollectionID,
			PartitionID:         segment.PartitionID,
			DmChannel:           segment.InsertChannel,
			SegmentState:        commonpb.SegmentState_Sealed,
			CreatedByCompaction: segment.GetCreatedByCompaction(),
			CompactionFrom:      segment.GetCompactionFrom(),
		}
		handoffSegBytes, err := proto.Marshal(handoffSegmentInfo)
		if err != nil {
			log.Error("DataCoord saveSegmentInfo marshal handoffSegInfo failed", zap.Int64("segmentID", segment.GetID()), zap.Error(err))
			return fmt.Errorf("DataCoord saveSegmentInfo segmentID:%d, marshal handoffSegInfo failed:%w", segment.GetID(), err)
		}
		queryKey := buildQuerySegmentPath(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
		kvs[queryKey] = string(handoffSegBytes)
	}

	return m.client.MultiSave(kvs)
}

// removeSegmentInfo utility function removing segment info from kv store
// Note that nil parameter will cause panicking
func (m *meta) removeSegmentInfo(segment *SegmentInfo) error {
	key := buildSegmentPath(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
	return m.client.Remove(key)
}

// saveKvTxn batch save kvs
func (m *meta) saveKvTxn(kv map[string]string) error {
	return m.client.MultiSave(kv)
}

// buildSegmentPath common logic mapping segment info to corresponding key in kv store
func buildSegmentPath(collectionID UniqueID, partitionID UniqueID, segmentID UniqueID) string {
	return fmt.Sprintf("%s/%d/%d/%d", segmentPrefix, collectionID, partitionID, segmentID)
}

// buildQuerySegmentPath common logic mapping segment info to corresponding key of queryCoord in kv store
func buildQuerySegmentPath(collectionID UniqueID, partitionID UniqueID, segmentID UniqueID) string {
	return fmt.Sprintf("%s/%d/%d/%d", util.HandoffSegmentPrefix, collectionID, partitionID, segmentID)
}

// buildChannelRemovePat builds vchannel remove flag path
func buildChannelRemovePath(channel string) string {
	return fmt.Sprintf("%s/%s", channelRemovePrefix, channel)
}

// buildSegment utility function for compose datapb.SegmentInfo struct with provided info
func buildSegment(collectionID UniqueID, partitionID UniqueID, segmentID UniqueID, channelName string) *SegmentInfo {
	info := &datapb.SegmentInfo{
		ID:            segmentID,
		CollectionID:  collectionID,
		PartitionID:   partitionID,
		InsertChannel: channelName,
		NumOfRows:     0,
		State:         commonpb.SegmentState_Growing,
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
