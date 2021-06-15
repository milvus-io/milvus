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
package dataservice

import (
	"fmt"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

const (
	metaPrefix    = "dataservice-meta"
	segmentPrefix = metaPrefix + "/s"
)

type errSegmentNotFound struct {
	segmentID UniqueID
}
type errCollectionNotFound struct {
	collectionID UniqueID
}
type meta struct {
	sync.RWMutex
	client      kv.TxnKV                            // client of a reliable kv service, i.e. etcd client
	collections map[UniqueID]*datapb.CollectionInfo // collection id to collection info
	segments    map[UniqueID]*datapb.SegmentInfo    // segment id to segment info
}

func newErrSegmentNotFound(segmentID UniqueID) errSegmentNotFound {
	return errSegmentNotFound{segmentID: segmentID}
}

func (err errSegmentNotFound) Error() string {
	return fmt.Sprintf("segment %d not found", err.segmentID)
}

func newErrCollectionNotFound(collectionID UniqueID) errCollectionNotFound {
	return errCollectionNotFound{collectionID: collectionID}
}

func (err errCollectionNotFound) Error() string {
	return fmt.Sprintf("collection %d not found", err.collectionID)
}

func newMeta(kv kv.TxnKV) (*meta, error) {
	mt := &meta{
		client:      kv,
		collections: make(map[UniqueID]*datapb.CollectionInfo),
		segments:    make(map[UniqueID]*datapb.SegmentInfo),
	}
	err := mt.reloadFromKV()
	if err != nil {
		return nil, err
	}
	return mt, nil
}

func (m *meta) reloadFromKV() error {
	_, values, err := m.client.LoadWithPrefix(segmentPrefix)
	if err != nil {
		return err
	}

	for _, value := range values {
		segmentInfo := &datapb.SegmentInfo{}
		err = proto.UnmarshalText(value, segmentInfo)
		if err != nil {
			return fmt.Errorf("DataService reloadFromKV UnMarshalText datapb.SegmentInfo err:%w", err)
		}
		m.segments[segmentInfo.ID] = segmentInfo
	}

	return nil
}

func (m *meta) AddCollection(collection *datapb.CollectionInfo) error {
	m.Lock()
	defer m.Unlock()
	if _, ok := m.collections[collection.ID]; ok {
		return fmt.Errorf("collection %s with id %d already exist", collection.Schema.Name, collection.ID)
	}
	m.collections[collection.ID] = collection
	return nil
}

func (m *meta) DropCollection(collID UniqueID) error {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.collections[collID]; !ok {
		return newErrCollectionNotFound(collID)
	}
	key := fmt.Sprintf("%s/%d/", segmentPrefix, collID)
	if err := m.client.RemoveWithPrefix(key); err != nil {
		return err
	}
	delete(m.collections, collID)

	for i, info := range m.segments {
		if info.CollectionID == collID {
			delete(m.segments, i)
		}
	}
	return nil
}

func (m *meta) HasCollection(collID UniqueID) bool {
	m.RLock()
	defer m.RUnlock()
	_, ok := m.collections[collID]
	return ok
}
func (m *meta) GetCollection(collectionID UniqueID) (*datapb.CollectionInfo, error) {
	m.RLock()
	defer m.RUnlock()

	collection, ok := m.collections[collectionID]
	if !ok {
		return nil, newErrCollectionNotFound(collectionID)
	}
	return proto.Clone(collection).(*datapb.CollectionInfo), nil
}

func (m *meta) GetNumRowsOfCollection(collectionID UniqueID) (int64, error) {
	m.RLock()
	defer m.RUnlock()
	var ret int64 = 0
	for _, info := range m.segments {
		if info.CollectionID == collectionID {
			ret += info.NumOfRows
		}
	}
	return ret, nil
}

func (m *meta) AddSegment(segment *datapb.SegmentInfo) error {
	m.Lock()
	defer m.Unlock()
	if _, ok := m.segments[segment.ID]; ok {
		return fmt.Errorf("segment %d already exist", segment.ID)
	}
	m.segments[segment.ID] = segment
	if err := m.saveSegmentInfo(segment); err != nil {
		return err
	}
	return nil
}

func (m *meta) UpdateSegmentStatistic(stats *internalpb.SegmentStatisticsUpdates) error {
	m.Lock()
	defer m.Unlock()
	seg, ok := m.segments[stats.SegmentID]
	if !ok {
		return newErrSegmentNotFound(stats.SegmentID)
	}
	seg.NumOfRows = stats.NumRows
	if err := m.saveSegmentInfo(seg); err != nil {
		return err
	}
	return nil
}

func (m *meta) SetLastExpireTime(segmentID UniqueID, expireTs Timestamp) error {
	m.Lock()
	defer m.Unlock()
	seg, ok := m.segments[segmentID]
	if !ok {
		return newErrSegmentNotFound(segmentID)
	}
	seg.LastExpireTime = expireTs

	if err := m.saveSegmentInfo(seg); err != nil {
		return err
	}
	return nil
}

func (m *meta) DropSegment(segmentID UniqueID) error {
	m.Lock()
	defer m.Unlock()

	segment, ok := m.segments[segmentID]
	if !ok {
		return newErrSegmentNotFound(segmentID)
	}
	if err := m.removeSegmentInfo(segment); err != nil {
		return err
	}
	delete(m.segments, segmentID)
	return nil
}

func (m *meta) GetSegment(segID UniqueID) (*datapb.SegmentInfo, error) {
	m.RLock()
	defer m.RUnlock()

	segment, ok := m.segments[segID]
	if !ok {
		return nil, newErrSegmentNotFound(segID)
	}
	return proto.Clone(segment).(*datapb.SegmentInfo), nil
}

func (m *meta) SealSegment(segID UniqueID) error {
	m.Lock()
	defer m.Unlock()

	segInfo, ok := m.segments[segID]
	if !ok {
		return newErrSegmentNotFound(segID)
	}

	segInfo.State = commonpb.SegmentState_Sealed
	if err := m.saveSegmentInfo(segInfo); err != nil {
		return err
	}
	return nil
}

func (m *meta) SaveBinlogAndCheckPoints(segID UniqueID, flushed bool,
	binlogs map[string]string, checkpoints []*datapb.CheckPoint,
	startPositions []*datapb.SegmentStartPosition) error {
	m.Lock()
	defer m.Unlock()
	segInfo, ok := m.segments[segID]
	if !ok {
		return newErrSegmentNotFound(segID)
	}
	kv := make(map[string]string)
	for k, v := range binlogs {
		kv[k] = v
	}
	if flushed {
		segInfo.State = commonpb.SegmentState_Flushing
	}

	modifiedSegments := make(map[UniqueID]struct{})
	for _, pos := range startPositions {
		segment, ok := m.segments[pos.GetSegmentID()]
		if !ok {
			log.Warn("Failed to find segment", zap.Int64("id", pos.GetSegmentID()))
			continue
		}
		if len(pos.GetStartPosition().GetMsgID()) != 0 {
			continue
		}

		segment.StartPosition = pos.GetStartPosition()
		modifiedSegments[segment.GetID()] = struct{}{}
	}

	for _, cp := range checkpoints {
		segment, ok := m.segments[cp.SegmentID]
		if !ok {
			log.Warn("Failed to find segment", zap.Int64("id", cp.SegmentID))
			continue
		}
		if segment.DmlPosition != nil && segment.DmlPosition.Timestamp >= cp.Position.Timestamp {
			// segment position in etcd is larger than checkpoint, then dont change it
			continue
		}
		segment.DmlPosition = cp.Position
		segment.NumOfRows = cp.NumOfRows
		modifiedSegments[segment.GetID()] = struct{}{}
	}

	for id := range modifiedSegments {
		segInfo = m.segments[id]
		segBytes := proto.MarshalTextString(segInfo)
		key := m.prepareSegmentPath(segInfo)
		kv[key] = segBytes
	}

	if err := m.saveKvTxn(kv); err != nil {
		return err
	}
	return nil
}

func (m *meta) GetSegmentsByChannel(dmlCh string) []*datapb.SegmentInfo {
	infos := make([]*datapb.SegmentInfo, 0)
	for _, segment := range m.segments {
		if segment.InsertChannel != dmlCh {
			continue
		}
		cInfo := proto.Clone(segment).(*datapb.SegmentInfo)
		infos = append(infos, cInfo)
	}
	return infos
}

func (m *meta) FlushSegment(segID UniqueID) error {
	m.Lock()
	defer m.Unlock()
	segInfo, ok := m.segments[segID]
	if !ok {
		return newErrSegmentNotFound(segID)
	}
	segInfo.State = commonpb.SegmentState_Flushed
	if err := m.saveSegmentInfo(segInfo); err != nil {
		return err
	}
	return nil
}

func (m *meta) GetSegmentsOfCollection(collectionID UniqueID) []UniqueID {
	m.RLock()
	defer m.RUnlock()

	ret := make([]UniqueID, 0)
	for _, info := range m.segments {
		if info.CollectionID == collectionID {
			ret = append(ret, info.ID)
		}
	}
	return ret
}

func (m *meta) GetSegmentsOfPartition(collectionID, partitionID UniqueID) []UniqueID {
	m.RLock()
	defer m.RUnlock()

	ret := make([]UniqueID, 0)
	for _, info := range m.segments {
		if info.CollectionID == collectionID && info.PartitionID == partitionID {
			ret = append(ret, info.ID)
		}
	}
	return ret
}

func (m *meta) AddPartition(collectionID UniqueID, partitionID UniqueID) error {
	m.Lock()
	defer m.Unlock()
	coll, ok := m.collections[collectionID]
	if !ok {
		return newErrCollectionNotFound(collectionID)
	}

	for _, t := range coll.Partitions {
		if t == partitionID {
			return fmt.Errorf("partition %d already exists", partitionID)
		}
	}
	coll.Partitions = append(coll.Partitions, partitionID)
	return nil
}

func (m *meta) DropPartition(collID UniqueID, partitionID UniqueID) error {
	m.Lock()
	defer m.Unlock()

	collection, ok := m.collections[collID]
	if !ok {
		return newErrCollectionNotFound(collID)
	}
	idx := -1
	for i, id := range collection.Partitions {
		if partitionID == id {
			idx = i
			break
		}
	}
	if idx == -1 {
		return fmt.Errorf("cannot find partition id %d", partitionID)
	}

	prefix := fmt.Sprintf("%s/%d/%d/", segmentPrefix, collID, partitionID)
	if err := m.client.RemoveWithPrefix(prefix); err != nil {
		return err
	}
	collection.Partitions = append(collection.Partitions[:idx], collection.Partitions[idx+1:]...)

	for i, info := range m.segments {
		if info.PartitionID == partitionID {
			delete(m.segments, i)
		}
	}
	return nil
}

func (m *meta) HasPartition(collID UniqueID, partID UniqueID) bool {
	m.RLock()
	defer m.RUnlock()
	coll, ok := m.collections[collID]
	if !ok {
		return false
	}
	for _, id := range coll.Partitions {
		if partID == id {
			return true
		}
	}
	return false
}

func (m *meta) GetNumRowsOfPartition(collectionID UniqueID, partitionID UniqueID) (int64, error) {
	m.RLock()
	defer m.RUnlock()
	var ret int64 = 0
	for _, info := range m.segments {
		if info.CollectionID == collectionID && info.PartitionID == partitionID {
			ret += info.NumOfRows
		}
	}
	return ret, nil
}

func (m *meta) GetUnFlushedSegments() []*datapb.SegmentInfo {
	m.RLock()
	defer m.RUnlock()
	segments := make([]*datapb.SegmentInfo, 0)
	for _, info := range m.segments {
		if info.State != commonpb.SegmentState_Flushing && info.State != commonpb.SegmentState_Flushed {
			cInfo := proto.Clone(info).(*datapb.SegmentInfo)
			segments = append(segments, cInfo)
		}
	}
	return segments
}

func (m *meta) GetFlushingSegments() []*datapb.SegmentInfo {
	m.RLock()
	defer m.RUnlock()
	segments := make([]*datapb.SegmentInfo, 0)
	for _, info := range m.segments {
		if info.State == commonpb.SegmentState_Flushing {
			cInfo := proto.Clone(info).(*datapb.SegmentInfo)
			segments = append(segments, cInfo)
		}
	}
	return segments
}

func (m *meta) saveSegmentInfo(segment *datapb.SegmentInfo) error {
	segBytes := proto.MarshalTextString(segment)

	key := m.prepareSegmentPath(segment)
	return m.client.Save(key, segBytes)
}

func (m *meta) removeSegmentInfo(segment *datapb.SegmentInfo) error {
	key := m.prepareSegmentPath(segment)
	return m.client.Remove(key)
}

func (m *meta) prepareSegmentPath(segInfo *datapb.SegmentInfo) string {
	return fmt.Sprintf("%s/%d/%d/%d", segmentPrefix, segInfo.CollectionID, segInfo.PartitionID, segInfo.ID)
}

func (m *meta) saveKvTxn(kv map[string]string) error {
	return m.client.MultiSave(kv)
}

func BuildSegment(collectionID UniqueID, partitionID UniqueID, segmentID UniqueID, channelName string) (*datapb.SegmentInfo, error) {
	return &datapb.SegmentInfo{
		ID:            segmentID,
		CollectionID:  collectionID,
		PartitionID:   partitionID,
		InsertChannel: channelName,
		NumOfRows:     0,
		State:         commonpb.SegmentState_Growing,
	}, nil
}
