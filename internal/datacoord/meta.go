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

	"go.uber.org/zap"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

const (
	metaPrefix           = "datacoord-meta"
	segmentPrefix        = metaPrefix + "/s"
	handoffSegmentPrefix = "querycoord-handoff"
)

type meta struct {
	sync.RWMutex
	client      kv.TxnKV                            // client of a reliable kv service, i.e. etcd client
	collections map[UniqueID]*datapb.CollectionInfo // collection id to collection info
	segments    *SegmentsInfo                       // segment id to segment info
}

// NewMeta create meta from provided `kv.TxnKV`
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

// reloadFromKV load meta from KV storage
func (m *meta) reloadFromKV() error {
	_, values, err := m.client.LoadWithPrefix(segmentPrefix)
	if err != nil {
		return err
	}

	for _, value := range values {
		segmentInfo := &datapb.SegmentInfo{}
		err = proto.Unmarshal([]byte(value), segmentInfo)
		if err != nil {
			return fmt.Errorf("DataCoord reloadFromKV UnMarshal datapb.SegmentInfo err:%w", err)
		}
		if segmentInfo.State == commonpb.SegmentState_NotExist {
			continue
		}
		m.segments.SetSegment(segmentInfo.GetID(), NewSegmentInfo(segmentInfo))
	}

	return nil
}

// AddCollection add collection into meta
// Note that collection info is just for caching and will not be set into etcd from datacoord
func (m *meta) AddCollection(collection *datapb.CollectionInfo) {
	m.Lock()
	defer m.Unlock()
	m.collections[collection.ID] = collection
}

// GetCollection get collection info with provided collection id from local cache
func (m *meta) GetCollection(collectionID UniqueID) *datapb.CollectionInfo {
	m.RLock()
	defer m.RUnlock()
	collection, ok := m.collections[collectionID]
	if !ok {
		return nil
	}
	return collection
}

// GetCollections get all collections id from local cache
func (m *meta) GetCollectionsID() []UniqueID {
	m.RLock()
	defer m.RUnlock()

	res := make([]UniqueID, 0, len(m.collections))
	for _, c := range m.collections {
		res = append(res, c.GetID())
	}
	return res
}

type chanPartSegments struct {
	collecionID UniqueID
	partitionID UniqueID
	channelName string
	segments    []*SegmentInfo
}

// GetSegmentsChanPart get segments organized in Channel-Parition dimension with selector applied
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
	var ret int64 = 0
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		if segment.GetCollectionID() == collectionID {
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
	m.segments.DropSegment(segmentID)
	if err := m.removeSegmentInfo(segment); err != nil {
		return err
	}
	return nil
}

// GetSegment returns segment info with provided id
// if not segment is found, nil will be returned
func (m *meta) GetSegment(segID UniqueID) *SegmentInfo {
	m.RLock()
	defer m.RUnlock()
	return m.segments.GetSegment(segID)
}

// SetState setting segment with provided ID state
func (m *meta) SetState(segmentID UniqueID, state commonpb.SegmentState) error {
	m.Lock()
	defer m.Unlock()
	m.segments.SetState(segmentID, state)
	if segInfo := m.segments.GetSegment(segmentID); segInfo != nil {
		return m.saveSegmentInfo(segInfo)
	}
	return nil
}

// UpdateFlushSegmentsInfo update segment partial/completed flush info
// `flushed` parameter indicating whether segment is flushed completely or partially
// `binlogs`, `checkpoints` and `statPositions` are persistence data for segment
func (m *meta) UpdateFlushSegmentsInfo(segmentID UniqueID, flushed bool,
	binlogs, statslogs []*datapb.FieldBinlog, deltalogs []*datapb.DeltaLogInfo, checkpoints []*datapb.CheckPoint,
	startPositions []*datapb.SegmentStartPosition) error {
	m.Lock()
	defer m.Unlock()

	segment := m.segments.GetSegment(segmentID)
	if segment == nil {
		return nil
	}

	clonedSegment := segment.Clone()

	kv := make(map[string]string)
	modSegments := make(map[UniqueID]*SegmentInfo)

	if flushed {
		clonedSegment.State = commonpb.SegmentState_Flushing
		modSegments[segmentID] = clonedSegment
	}

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
	clonedSegment.Deltalogs = append(clonedSegment.Deltalogs, deltalogs...)

	modSegments[segmentID] = clonedSegment

	var getClonedSegment = func(segmentID UniqueID) *SegmentInfo {
		if s, ok := modSegments[segmentID]; ok {
			return s
		}
		if s := m.segments.GetSegment(segmentID); s != nil {
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

	for id := range modSegments {
		if segment := m.segments.GetSegment(id); segment != nil {
			segBytes, err := proto.Marshal(segment.SegmentInfo)
			if err != nil {
				return fmt.Errorf("DataCoord UpdateFlushSegmentsInfo segmentID:%d, marshal failed:%w", segment.GetID(), err)
			}
			key := buildSegmentPath(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
			kv[key] = string(segBytes)
		}
	}

	if len(kv) == 0 {
		return nil
	}

	if err := m.saveKvTxn(kv); err != nil {
		return err
	}

	// update memory status
	for id, s := range modSegments {
		m.segments.SetSegment(id, s)
	}
	return nil
}

// ListSegmentIDs list all segment ids stored in meta (no collection filter)
func (m *meta) ListSegmentIDs() []UniqueID {
	m.RLock()
	defer m.RUnlock()

	infos := make([]UniqueID, 0)
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		infos = append(infos, segment.GetID())
	}

	return infos
}

// GetSegmentsByChannel returns all segment info which insert channel equals provided `dmlCh`
func (m *meta) GetSegmentsByChannel(dmlCh string) []*SegmentInfo {
	m.RLock()
	defer m.RUnlock()
	infos := make([]*SegmentInfo, 0)
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		if segment.InsertChannel != dmlCh {
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
	ret = append(ret, segments...)
	return ret
}

// GetSegmentsIDOfCollection returns all segment ids which collection equals to provided `collectionID`
func (m *meta) GetSegmentsIDOfCollection(collectionID UniqueID) []UniqueID {
	m.RLock()
	defer m.RUnlock()
	ret := make([]UniqueID, 0)
	segments := m.segments.GetSegments()
	for _, info := range segments {
		if info.CollectionID == collectionID {
			ret = append(ret, info.ID)
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
	for _, info := range segments {
		if info.CollectionID == collectionID && info.PartitionID == partitionID {
			ret = append(ret, info.ID)
		}
	}
	return ret
}

// GetNumRowsOfPartition returns row count of segments belongs to provided collection & partition
func (m *meta) GetNumRowsOfPartition(collectionID UniqueID, partitionID UniqueID) int64 {
	m.RLock()
	defer m.RUnlock()
	var ret int64 = 0
	segments := m.segments.GetSegments()
	for _, info := range segments {
		if info.CollectionID == collectionID && info.PartitionID == partitionID {
			ret += info.NumOfRows
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
	for _, info := range segments {
		if info.State != commonpb.SegmentState_Flushing && info.State != commonpb.SegmentState_Flushed {
			ret = append(ret, info)
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

// MoveSegmentBinlogs migration logic, moving segment binlong information for legacy keys
func (m *meta) MoveSegmentBinlogs(segmentID UniqueID, oldPathPrefix string, field2Binlogs map[UniqueID][]string) error {
	m.Lock()
	defer m.Unlock()

	m.segments.AddSegmentBinlogs(segmentID, field2Binlogs)

	removals := []string{oldPathPrefix}
	kv := make(map[string]string)

	if segment := m.segments.GetSegment(segmentID); segment != nil {
		k := buildSegmentPath(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
		v, err := proto.Marshal(segment.SegmentInfo)
		if err != nil {
			log.Error("DataCoord MoveSegmentBinlogs marshal failed", zap.Int64("segmentID", segment.GetID()), zap.Error(err))
			return fmt.Errorf("DataCoord MoveSegmentBinlogs segmentID:%d, marshal failed:%w", segment.GetID(), err)
		}
		kv[k] = string(v)
	}
	return m.client.MultiSaveAndRemoveWithPrefix(kv, removals)
}

func (m *meta) CompleteMergeCompaction(compactionLogs []*datapb.CompactionSegmentBinlogs, result *datapb.CompactionResult) error {
	m.Lock()
	defer m.Unlock()

	segments := make([]*SegmentInfo, 0, len(compactionLogs))
	for _, cl := range compactionLogs {
		if segment := m.segments.GetSegment(cl.GetSegmentID()); segment != nil {
			cloned := segment.Clone()
			cloned.State = commonpb.SegmentState_NotExist
			segments = append(segments, cloned)
		}
	}

	var dmlPosition *internalpb.MsgPosition
	for _, s := range segments {
		if dmlPosition == nil || s.GetDmlPosition().Timestamp > dmlPosition.Timestamp {
			dmlPosition = s.GetDmlPosition()
		}
	}

	// find new added delta logs when executing compaction
	originDeltalogs := make([]*datapb.DeltaLogInfo, 0)
	for _, s := range segments {
		originDeltalogs = append(originDeltalogs, s.GetDeltalogs()...)
	}

	deletedDeltalogs := make([]*datapb.DeltaLogInfo, 0)
	for _, l := range compactionLogs {
		deletedDeltalogs = append(deletedDeltalogs, l.GetDeltalogs()...)
	}

	newAddedDeltalogs := m.updateDeltalogs(originDeltalogs, deletedDeltalogs, nil)
	deltalogs := append(result.GetDeltalogs(), newAddedDeltalogs...)

	compactionFrom := make([]UniqueID, 0, len(segments))
	for _, s := range segments {
		compactionFrom = append(compactionFrom, s.GetID())
	}

	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
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
			DmlPosition:         dmlPosition,
			CreatedByCompaction: true,
			CompactionFrom:      compactionFrom,
		},
		isCompacting: false,
	}

	data := make(map[string]string)

	for _, s := range segments {
		k, v, err := m.marshal(s)
		if err != nil {
			return err
		}
		data[k] = v
	}
	k, v, err := m.marshal(segment)
	if err != nil {
		return err
	}
	data[k] = v

	if err := m.saveKvTxn(data); err != nil {
		return err
	}

	for _, s := range segments {
		m.segments.DropSegment(s.GetID())
	}

	m.segments.SetSegment(segment.GetID(), segment)
	return nil
}

func (m *meta) CompleteInnerCompaction(segmentBinlogs *datapb.CompactionSegmentBinlogs, result *datapb.CompactionResult) error {
	m.Lock()
	defer m.Unlock()

	if segment := m.segments.GetSegment(segmentBinlogs.SegmentID); segment != nil {
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
	fieldBinlogs := make(map[int64]map[string]struct{})
	for _, f := range origin {
		fid := f.GetFieldID()
		if _, ok := fieldBinlogs[fid]; !ok {
			fieldBinlogs[fid] = make(map[string]struct{})
		}
		for _, p := range f.GetBinlogs() {
			fieldBinlogs[fid][p] = struct{}{}
		}
	}

	for _, f := range removes {
		fid := f.GetFieldID()
		if _, ok := fieldBinlogs[fid]; !ok {
			continue
		}
		for _, p := range f.GetBinlogs() {
			delete(fieldBinlogs[fid], p)
		}
	}

	for _, f := range adds {
		fid := f.GetFieldID()
		if _, ok := fieldBinlogs[fid]; !ok {
			fieldBinlogs[fid] = make(map[string]struct{})
		}
		for _, p := range f.GetBinlogs() {
			fieldBinlogs[fid][p] = struct{}{}
		}
	}

	res := make([]*datapb.FieldBinlog, 0, len(fieldBinlogs))
	for fid, logs := range fieldBinlogs {
		if len(logs) == 0 {
			continue
		}

		binlogs := make([]string, 0, len(logs))
		for path := range logs {
			binlogs = append(binlogs, path)
		}

		field := &datapb.FieldBinlog{FieldID: fid, Binlogs: binlogs}
		res = append(res, field)
	}
	return res
}

func (m *meta) updateDeltalogs(origin []*datapb.DeltaLogInfo, removes []*datapb.DeltaLogInfo, adds []*datapb.DeltaLogInfo) []*datapb.DeltaLogInfo {
	deltalogs := make(map[string]*datapb.DeltaLogInfo)
	for _, d := range origin {
		deltalogs[d.GetDeltaLogPath()] = d
	}

	for _, r := range removes {
		delete(deltalogs, r.GetDeltaLogPath())
	}

	res := make([]*datapb.DeltaLogInfo, 0, len(deltalogs))
	for _, log := range deltalogs {
		res = append(res, log)
	}
	res = append(res, adds...)
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
			ChannelID:           segment.InsertChannel,
			SegmentState:        querypb.SegmentState_sealed,
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
	return fmt.Sprintf("%s/%d/%d/%d", handoffSegmentPrefix, collectionID, partitionID, segmentID)
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
