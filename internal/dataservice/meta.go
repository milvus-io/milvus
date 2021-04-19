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
	"github.com/zilliztech/milvus-distributed/internal/kv"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
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

func (meta *meta) reloadFromKV() error {
	_, values, err := meta.client.LoadWithPrefix(segmentPrefix)
	if err != nil {
		return err
	}

	for _, value := range values {
		segmentInfo := &datapb.SegmentInfo{}
		err = proto.UnmarshalText(value, segmentInfo)
		if err != nil {
			return fmt.Errorf("DataService reloadFromKV UnMarshalText datapb.SegmentInfo err:%w", err)
		}
		meta.segments[segmentInfo.ID] = segmentInfo
	}

	return nil
}

func (meta *meta) AddCollection(collection *datapb.CollectionInfo) error {
	meta.Lock()
	defer meta.Unlock()
	if _, ok := meta.collections[collection.ID]; ok {
		return fmt.Errorf("collection %s with id %d already exist", collection.Schema.Name, collection.ID)
	}
	meta.collections[collection.ID] = collection
	return nil
}

func (meta *meta) DropCollection(collID UniqueID) error {
	meta.Lock()
	defer meta.Unlock()

	if _, ok := meta.collections[collID]; !ok {
		return newErrCollectionNotFound(collID)
	}
	key := fmt.Sprintf("%s/%d/", segmentPrefix, collID)
	if err := meta.client.RemoveWithPrefix(key); err != nil {
		return err
	}
	delete(meta.collections, collID)

	for i, info := range meta.segments {
		if info.CollectionID == collID {
			delete(meta.segments, i)
		}
	}
	return nil
}

func (meta *meta) HasCollection(collID UniqueID) bool {
	meta.RLock()
	defer meta.RUnlock()
	_, ok := meta.collections[collID]
	return ok
}
func (meta *meta) GetCollection(collectionID UniqueID) (*datapb.CollectionInfo, error) {
	meta.RLock()
	defer meta.RUnlock()

	collection, ok := meta.collections[collectionID]
	if !ok {
		return nil, newErrCollectionNotFound(collectionID)
	}
	return proto.Clone(collection).(*datapb.CollectionInfo), nil
}

func (meta *meta) GetNumRowsOfCollection(collectionID UniqueID) (int64, error) {
	meta.RLock()
	defer meta.RUnlock()
	var ret int64 = 0
	for _, info := range meta.segments {
		if info.CollectionID == collectionID {
			ret += info.NumRows
		}
	}
	return ret, nil
}

func (meta *meta) GetMemSizeOfCollection(collectionID UniqueID) (int64, error) {
	meta.RLock()
	defer meta.RUnlock()
	var ret int64 = 0
	for _, info := range meta.segments {
		if info.CollectionID == collectionID {
			ret += info.MemSize
		}
	}
	return ret, nil
}

func (meta *meta) AddSegment(segment *datapb.SegmentInfo) error {
	meta.Lock()
	defer meta.Unlock()
	if _, ok := meta.segments[segment.ID]; ok {
		return fmt.Errorf("segment %d already exist", segment.ID)
	}
	meta.segments[segment.ID] = segment
	if err := meta.saveSegmentInfo(segment); err != nil {
		return err
	}
	return nil
}

func (meta *meta) UpdateSegment(segment *datapb.SegmentInfo) error {
	meta.Lock()
	defer meta.Unlock()
	seg, ok := meta.segments[segment.ID]
	if !ok {
		return newErrSegmentNotFound(segment.ID)
	}
	seg.OpenTime = segment.OpenTime
	seg.SealedTime = segment.SealedTime
	seg.NumRows = segment.NumRows
	seg.MemSize = segment.MemSize
	seg.StartPosition = proto.Clone(segment.StartPosition).(*internalpb.MsgPosition)
	seg.EndPosition = proto.Clone(segment.EndPosition).(*internalpb.MsgPosition)

	if err := meta.saveSegmentInfo(segment); err != nil {
		return err
	}
	return nil
}

func (meta *meta) DropSegment(segmentID UniqueID) error {
	meta.Lock()
	defer meta.Unlock()

	segment, ok := meta.segments[segmentID]
	if !ok {
		return newErrSegmentNotFound(segmentID)
	}
	if err := meta.removeSegmentInfo(segment); err != nil {
		return err
	}
	delete(meta.segments, segmentID)
	return nil
}

func (meta *meta) GetSegment(segID UniqueID) (*datapb.SegmentInfo, error) {
	meta.RLock()
	defer meta.RUnlock()

	segment, ok := meta.segments[segID]
	if !ok {
		return nil, newErrSegmentNotFound(segID)
	}
	return proto.Clone(segment).(*datapb.SegmentInfo), nil
}

func (meta *meta) OpenSegment(segmentID UniqueID, timetick Timestamp) error {
	meta.Lock()
	defer meta.Unlock()

	segInfo, ok := meta.segments[segmentID]
	if !ok {
		return newErrSegmentNotFound(segmentID)
	}

	segInfo.OpenTime = timetick
	if err := meta.saveSegmentInfo(segInfo); err != nil {
		return err
	}
	return nil
}

func (meta *meta) SealSegment(segID UniqueID, timetick Timestamp) error {
	meta.Lock()
	defer meta.Unlock()

	segInfo, ok := meta.segments[segID]
	if !ok {
		return newErrSegmentNotFound(segID)
	}

	segInfo.SealedTime = timetick
	if err := meta.saveSegmentInfo(segInfo); err != nil {
		return err
	}
	return nil
}

func (meta *meta) FlushSegment(segID UniqueID, timetick Timestamp) error {
	meta.Lock()
	defer meta.Unlock()

	segInfo, ok := meta.segments[segID]
	if !ok {
		return newErrSegmentNotFound(segID)
	}
	segInfo.FlushedTime = timetick
	segInfo.State = commonpb.SegmentState_Flushed
	if err := meta.saveSegmentInfo(segInfo); err != nil {
		return err
	}
	return nil
}

func (meta *meta) SetSegmentState(segmentID UniqueID, state commonpb.SegmentState) error {
	meta.Lock()
	defer meta.Unlock()

	segInfo, ok := meta.segments[segmentID]
	if !ok {
		return newErrSegmentNotFound(segmentID)
	}
	segInfo.State = state
	if err := meta.saveSegmentInfo(segInfo); err != nil {
		return err
	}
	return nil
}

func (meta *meta) GetSegmentsOfCollection(collectionID UniqueID) []UniqueID {
	meta.RLock()
	defer meta.RUnlock()

	ret := make([]UniqueID, 0)
	for _, info := range meta.segments {
		if info.CollectionID == collectionID {
			ret = append(ret, info.ID)
		}
	}
	return ret
}

func (meta *meta) GetSegmentsOfPartition(collectionID, partitionID UniqueID) []UniqueID {
	meta.RLock()
	defer meta.RUnlock()

	ret := make([]UniqueID, 0)
	for _, info := range meta.segments {
		if info.CollectionID == collectionID && info.PartitionID == partitionID {
			ret = append(ret, info.ID)
		}
	}
	return ret
}

func (meta *meta) AddPartition(collectionID UniqueID, partitionID UniqueID) error {
	meta.Lock()
	defer meta.Unlock()
	coll, ok := meta.collections[collectionID]
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

func (meta *meta) DropPartition(collID UniqueID, partitionID UniqueID) error {
	meta.Lock()
	defer meta.Unlock()

	collection, ok := meta.collections[collID]
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
	if err := meta.client.RemoveWithPrefix(prefix); err != nil {
		return err
	}
	collection.Partitions = append(collection.Partitions[:idx], collection.Partitions[idx+1:]...)

	for i, info := range meta.segments {
		if info.PartitionID == partitionID {
			delete(meta.segments, i)
		}
	}
	return nil
}

func (meta *meta) HasPartition(collID UniqueID, partID UniqueID) bool {
	meta.RLock()
	defer meta.RUnlock()
	coll, ok := meta.collections[collID]
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

func (meta *meta) GetNumRowsOfPartition(collectionID UniqueID, partitionID UniqueID) (int64, error) {
	meta.RLock()
	defer meta.RUnlock()
	var ret int64 = 0
	for _, info := range meta.segments {
		if info.CollectionID == collectionID && info.PartitionID == partitionID {
			ret += info.NumRows
		}
	}
	return ret, nil
}

func (meta *meta) saveSegmentInfo(segment *datapb.SegmentInfo) error {
	segBytes := proto.MarshalTextString(segment)

	key := fmt.Sprintf("%s/%d/%d/%d", segmentPrefix, segment.CollectionID, segment.PartitionID, segment.ID)
	return meta.client.Save(key, segBytes)
}

func (meta *meta) removeSegmentInfo(segment *datapb.SegmentInfo) error {
	key := fmt.Sprintf("%s/%d/%d/%d", segmentPrefix, segment.CollectionID, segment.PartitionID, segment.ID)
	return meta.client.Remove(key)
}

func BuildSegment(collectionID UniqueID, partitionID UniqueID, segmentID UniqueID, channelName string) (*datapb.SegmentInfo, error) {
	return &datapb.SegmentInfo{
		ID:            segmentID,
		CollectionID:  collectionID,
		PartitionID:   partitionID,
		InsertChannel: channelName,
		OpenTime:      0,
		SealedTime:    0,
		NumRows:       0,
		MemSize:       0,
		State:         commonpb.SegmentState_Growing,
		StartPosition: &internalpb.MsgPosition{
			ChannelName: channelName,
			MsgID:       make([]byte, 0),
			Timestamp:   0,
		},
		EndPosition: &internalpb.MsgPosition{
			ChannelName: channelName,
			MsgID:       make([]byte, 0),
			Timestamp:   0,
		},
	}, nil
}
