package dataservice

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/kv"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

type errSegmentNotFound struct {
	segmentID UniqueID
}
type errCollectionNotFound struct {
	collectionID UniqueID
}
type collectionInfo struct {
	ID         UniqueID
	Schema     *schemapb.CollectionSchema
	Partitions []UniqueID
}
type meta struct {
	client      kv.TxnBase                       // client of a reliable kv service, i.e. etcd client
	collID2Info map[UniqueID]*collectionInfo     // collection id to collection info
	segID2Info  map[UniqueID]*datapb.SegmentInfo // segment id to segment info
	ddLock      sync.RWMutex
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

func newMeta(kv kv.TxnBase) (*meta, error) {
	mt := &meta{
		client:      kv,
		collID2Info: make(map[UniqueID]*collectionInfo),
		segID2Info:  make(map[UniqueID]*datapb.SegmentInfo),
	}
	err := mt.reloadFromKV()
	if err != nil {
		return nil, err
	}
	return mt, nil
}

func (meta *meta) reloadFromKV() error {
	_, values, err := meta.client.LoadWithPrefix("segment")
	if err != nil {
		return err
	}

	for _, value := range values {
		segmentInfo := &datapb.SegmentInfo{}
		err = proto.UnmarshalText(value, segmentInfo)
		if err != nil {
			return err
		}
		meta.segID2Info[segmentInfo.SegmentID] = segmentInfo
	}

	return nil
}

func (meta *meta) AddCollection(collectionInfo *collectionInfo) error {
	meta.ddLock.Lock()
	defer meta.ddLock.Unlock()
	if _, ok := meta.collID2Info[collectionInfo.ID]; ok {
		return fmt.Errorf("collection %s with id %d already exist", collectionInfo.Schema.Name, collectionInfo.ID)
	}
	meta.collID2Info[collectionInfo.ID] = collectionInfo
	return nil
}

func (meta *meta) DropCollection(collID UniqueID) error {
	meta.ddLock.Lock()
	defer meta.ddLock.Unlock()

	if _, ok := meta.collID2Info[collID]; !ok {
		return newErrCollectionNotFound(collID)
	}
	ids := make([]UniqueID, 0)
	for i, info := range meta.segID2Info {
		if info.CollectionID == collID {
			delete(meta.segID2Info, i)
			ids = append(ids, i)
		}
	}
	if err := meta.removeSegments(ids); err != nil {
		_ = meta.reloadFromKV()
		return err
	}
	delete(meta.collID2Info, collID)
	return nil
}

func (meta *meta) HasCollection(collID UniqueID) bool {
	meta.ddLock.RLock()
	defer meta.ddLock.RUnlock()
	_, ok := meta.collID2Info[collID]
	return ok
}
func (meta *meta) GetCollection(collectionID UniqueID) (*collectionInfo, error) {
	meta.ddLock.RLock()
	defer meta.ddLock.RUnlock()

	collectionInfo, ok := meta.collID2Info[collectionID]
	if !ok {
		return nil, newErrCollectionNotFound(collectionID)
	}
	return collectionInfo, nil
}

func (meta *meta) GetNumRowsOfCollection(collectionID UniqueID) (int64, error) {
	meta.ddLock.RLock()
	defer meta.ddLock.RUnlock()
	var ret int64 = 0
	for _, info := range meta.segID2Info {
		if info.CollectionID == collectionID {
			ret += info.NumRows
		}
	}
	return ret, nil
}

func (meta *meta) GetMemSizeOfCollection(collectionID UniqueID) (int64, error) {
	meta.ddLock.RLock()
	defer meta.ddLock.RUnlock()
	var ret int64 = 0
	for _, info := range meta.segID2Info {
		if info.CollectionID == collectionID {
			ret += info.MemSize
		}
	}
	return ret, nil
}

func (meta *meta) AddSegment(segmentInfo *datapb.SegmentInfo) error {
	meta.ddLock.Lock()
	defer meta.ddLock.Unlock()
	if _, ok := meta.segID2Info[segmentInfo.SegmentID]; ok {
		return fmt.Errorf("segment %d already exist", segmentInfo.SegmentID)
	}
	meta.segID2Info[segmentInfo.SegmentID] = segmentInfo
	if err := meta.saveSegmentInfo(segmentInfo); err != nil {
		_ = meta.reloadFromKV()
		return err
	}
	return nil
}

func (meta *meta) UpdateSegment(segmentInfo *datapb.SegmentInfo) error {
	meta.ddLock.Lock()
	defer meta.ddLock.Unlock()

	meta.segID2Info[segmentInfo.SegmentID] = segmentInfo
	if err := meta.saveSegmentInfo(segmentInfo); err != nil {
		_ = meta.reloadFromKV()
		return err
	}
	return nil
}

func (meta *meta) DropSegment(segmentID UniqueID) error {
	meta.ddLock.Lock()
	defer meta.ddLock.Unlock()

	if _, ok := meta.segID2Info[segmentID]; !ok {
		return newErrSegmentNotFound(segmentID)
	}
	delete(meta.segID2Info, segmentID)
	if err := meta.removeSegmentInfo(segmentID); err != nil {
		_ = meta.reloadFromKV()
		return err
	}
	return nil
}

func (meta *meta) GetSegment(segID UniqueID) (*datapb.SegmentInfo, error) {
	meta.ddLock.RLock()
	defer meta.ddLock.RUnlock()

	segmentInfo, ok := meta.segID2Info[segID]
	if !ok {
		return nil, newErrSegmentNotFound(segID)
	}
	return segmentInfo, nil
}

func (meta *meta) OpenSegment(segmentID UniqueID, timetick Timestamp) error {
	meta.ddLock.Lock()
	defer meta.ddLock.Unlock()

	segInfo, ok := meta.segID2Info[segmentID]
	if !ok {
		return newErrSegmentNotFound(segmentID)
	}

	segInfo.OpenTime = timetick

	err := meta.saveSegmentInfo(segInfo)
	if err != nil {
		_ = meta.reloadFromKV()
		return err
	}
	return nil
}

func (meta *meta) SealSegment(segID UniqueID, timetick Timestamp) error {
	meta.ddLock.Lock()
	defer meta.ddLock.Unlock()

	segInfo, ok := meta.segID2Info[segID]
	if !ok {
		return newErrSegmentNotFound(segID)
	}

	segInfo.SealedTime = timetick

	err := meta.saveSegmentInfo(segInfo)
	if err != nil {
		_ = meta.reloadFromKV()
		return err
	}
	return nil
}

func (meta *meta) FlushSegment(segID UniqueID, timetick Timestamp) error {
	meta.ddLock.Lock()
	defer meta.ddLock.Unlock()

	segInfo, ok := meta.segID2Info[segID]
	if !ok {
		return newErrSegmentNotFound(segID)
	}

	segInfo.FlushedTime = timetick

	err := meta.saveSegmentInfo(segInfo)
	if err != nil {
		_ = meta.reloadFromKV()
		return err
	}
	return nil
}

func (meta *meta) SetSegmentState(segmentID UniqueID, state commonpb.SegmentState) error {
	meta.ddLock.Lock()
	defer meta.ddLock.Unlock()

	segInfo, ok := meta.segID2Info[segmentID]
	if !ok {
		return newErrSegmentNotFound(segmentID)
	}
	segInfo.State = state
	err := meta.saveSegmentInfo(segInfo)
	if err != nil {
		_ = meta.reloadFromKV()
		return err
	}
	return nil
}

func (meta *meta) GetSegmentsOfCollection(collectionID UniqueID) []UniqueID {
	meta.ddLock.RLock()
	defer meta.ddLock.RUnlock()

	ret := make([]UniqueID, 0)
	for _, info := range meta.segID2Info {
		if info.CollectionID == collectionID {
			ret = append(ret, info.SegmentID)
		}
	}
	return ret
}

func (meta *meta) GetSegmentsOfPartition(collectionID, partitionID UniqueID) []UniqueID {
	meta.ddLock.RLock()
	defer meta.ddLock.RUnlock()

	ret := make([]UniqueID, 0)
	for _, info := range meta.segID2Info {
		if info.CollectionID == collectionID && info.PartitionID == partitionID {
			ret = append(ret, info.SegmentID)
		}
	}
	return ret
}

func (meta *meta) AddPartition(collectionID UniqueID, partitionID UniqueID) error {
	meta.ddLock.Lock()
	defer meta.ddLock.Unlock()
	coll, ok := meta.collID2Info[collectionID]
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
	meta.ddLock.Lock()
	defer meta.ddLock.Unlock()

	collection, ok := meta.collID2Info[collID]
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

	ids := make([]UniqueID, 0)
	for i, info := range meta.segID2Info {
		if info.PartitionID == partitionID {
			delete(meta.segID2Info, i)
			ids = append(ids, i)
		}
	}
	if err := meta.removeSegments(ids); err != nil {
		_ = meta.reloadFromKV()
		return err
	}
	collection.Partitions = append(collection.Partitions[:idx], collection.Partitions[idx+1:]...)
	return nil
}

func (meta *meta) GetNumRowsOfPartition(collectionID UniqueID, partitionID UniqueID) (int64, error) {
	meta.ddLock.RLock()
	defer meta.ddLock.RUnlock()
	var ret int64 = 0
	for _, info := range meta.segID2Info {
		if info.CollectionID == collectionID && info.PartitionID == partitionID {
			ret += info.NumRows
		}
	}
	return ret, nil
}

func (meta *meta) saveSegmentInfo(segmentInfo *datapb.SegmentInfo) error {
	segBytes := proto.MarshalTextString(segmentInfo)

	return meta.client.Save("/segment/"+strconv.FormatInt(segmentInfo.SegmentID, 10), segBytes)
}

func (meta *meta) removeSegmentInfo(segID UniqueID) error {
	return meta.client.Remove("/segment/" + strconv.FormatInt(segID, 10))
}

func (meta *meta) removeSegments(segIDs []UniqueID) error {
	segmentPaths := make([]string, len(segIDs))
	for i, id := range segIDs {
		segmentPaths[i] = "/segment/" + strconv.FormatInt(id, 10)
	}
	return meta.client.MultiRemove(segmentPaths)
}

func BuildSegment(collectionID UniqueID, partitionID UniqueID, segmentID UniqueID, channelName string) (*datapb.SegmentInfo, error) {
	return &datapb.SegmentInfo{
		SegmentID:     segmentID,
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
			MsgID:       "",
			Timestamp:   0,
		},
		EndPosition: &internalpb.MsgPosition{
			ChannelName: channelName,
			MsgID:       "",
			Timestamp:   0,
		},
	}, nil
}
