// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

// Package datacoord contains core functions in datacoord
package datacoord

import (
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

const (
	metaPrefix    = "datacoord-meta"
	segmentPrefix = metaPrefix + "/s"
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

// realodFromKV load meta from KV storage
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
	binlogs []*datapb.FieldBinlog, checkpoints []*datapb.CheckPoint,
	startPositions []*datapb.SegmentStartPosition) error {
	m.Lock()
	defer m.Unlock()

	segment := m.segments.GetSegment(segmentID)
	if segment == nil {
		return nil
	}

	kv := make(map[string]string)
	modSegments := make(map[UniqueID]struct{})

	if flushed {
		m.segments.SetState(segmentID, commonpb.SegmentState_Flushing)
		modSegments[segmentID] = struct{}{}
	}

	currBinlogs := segment.Clone().SegmentInfo.GetBinlogs()
	var getFieldBinlogs = func(id UniqueID, binlogs []*datapb.FieldBinlog) *datapb.FieldBinlog {
		for _, binlog := range binlogs {
			if id == binlog.GetFieldID() {
				return binlog
			}
		}
		return nil
	}
	for _, tBinlogs := range binlogs {
		fieldBinlogs := getFieldBinlogs(tBinlogs.GetFieldID(), currBinlogs)
		if fieldBinlogs == nil {
			currBinlogs = append(currBinlogs, tBinlogs)
		} else {
			fieldBinlogs.Binlogs = append(fieldBinlogs.Binlogs, tBinlogs.Binlogs...)
		}
	}
	m.segments.SetBinlogs(segmentID, currBinlogs)
	modSegments[segmentID] = struct{}{}

	for _, pos := range startPositions {
		if len(pos.GetStartPosition().GetMsgID()) == 0 {
			continue
		}
		m.segments.SetStartPosition(pos.GetSegmentID(), pos.GetStartPosition())
		modSegments[segmentID] = struct{}{}
	}

	for _, cp := range checkpoints {
		if segment.DmlPosition != nil && segment.DmlPosition.Timestamp >= cp.Position.Timestamp {
			// segment position in etcd is larger than checkpoint, then dont change it
			continue
		}
		m.segments.SetDmlPosition(cp.GetSegmentID(), cp.GetPosition())
		m.segments.SetRowCount(cp.GetSegmentID(), cp.GetNumOfRows())
		modSegments[segmentID] = struct{}{}
	}

	for id := range modSegments {
		if segment := m.segments.GetSegment(id); segment != nil {
			segBytes, err := proto.Marshal(segment.SegmentInfo)
			if err != nil {
				log.Error("DataCoord UpdateFlushSegmentsInfo marshal failed", zap.Int64("segmentID", segment.GetID()), zap.Error(err))
				return fmt.Errorf("DataCoord UpdateFlushSegmentsInfo segmentID:%d, marshal failed:%w", segment.GetID(), err)
			}
			key := buildSegmentPath(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
			kv[key] = string(segBytes)
		}
	}

	if err := m.saveKvTxn(kv); err != nil {
		return err
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

// GetSegmentsOfCollection returns all segment ids which collection equals to provided `collectionID`
func (m *meta) GetSegmentsOfCollection(collectionID UniqueID) []UniqueID {
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

// GetSegmentsOfPartition returns all segments ids which collection & partition equals to provided `collectionID`, `partitionID`
func (m *meta) GetSegmentsOfPartition(collectionID, partitionID UniqueID) []UniqueID {
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

// saveSegmentInfo utility function saving segment info into kv store
func (m *meta) saveSegmentInfo(segment *SegmentInfo) error {
	segBytes, err := proto.Marshal(segment.SegmentInfo)
	if err != nil {
		log.Error("DataCoord saveSegmentInfo marshal failed", zap.Int64("segmentID", segment.GetID()), zap.Error(err))
		return fmt.Errorf("DataCoord saveSegmentInfo segmentID:%d, marshal failed:%w", segment.GetID(), err)
	}
	key := buildSegmentPath(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
	return m.client.Save(key, string(segBytes))
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
