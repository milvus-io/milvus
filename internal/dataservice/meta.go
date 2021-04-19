package dataservice

import (
	"fmt"
	"strconv"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"

	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
)

type (
	UniqueID       = typeutil.UniqueID
	Timestamp      = typeutil.Timestamp
	collectionInfo struct {
		ID     UniqueID
		Schema *schemapb.CollectionSchema
	}
	meta struct {
		client      kv.TxnBase                       // client of a reliable kv service, i.e. etcd client
		collID2Info map[UniqueID]*collectionInfo     // collection id to collection info
		segID2Info  map[UniqueID]*datapb.SegmentInfo // segment id to segment info

		ddLock sync.RWMutex
	}
)

func NewMetaTable(kv kv.TxnBase) (*meta, error) {
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

func (mt *meta) reloadFromKV() error {
	_, values, err := mt.client.LoadWithPrefix("segment")
	if err != nil {
		return err
	}

	for _, value := range values {
		segmentInfo := &datapb.SegmentInfo{}
		err = proto.UnmarshalText(value, segmentInfo)
		if err != nil {
			return err
		}
		mt.segID2Info[segmentInfo.SegmentID] = segmentInfo
	}

	return nil
}

func (mt *meta) AddCollection(collectionInfo *collectionInfo) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	if _, ok := mt.collID2Info[collectionInfo.ID]; ok {
		return fmt.Errorf("collection %s with id %d already exist", collectionInfo.Schema.Name, collectionInfo.ID)
	}
	mt.collID2Info[collectionInfo.ID] = collectionInfo
	return nil
}

func (mt *meta) DropCollection(collID UniqueID) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if _, ok := mt.collID2Info[collID]; !ok {
		return errors.Errorf("can't find collection. id = " + strconv.FormatInt(collID, 10))
	}
	delete(mt.collID2Info, collID)
	for id, segment := range mt.segID2Info {
		if segment.CollectionID != collID {
			continue
		}
		delete(mt.segID2Info, id)
		if err := mt.removeSegmentInfo(id); err != nil {
			log.Printf("remove segment info failed, %s", err.Error())
			_ = mt.reloadFromKV()
		}
	}
	return nil
}

func (mt *meta) HasCollection(collID UniqueID) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	_, ok := mt.collID2Info[collID]
	return ok
}

func (mt *meta) AddSegment(segmentInfo *datapb.SegmentInfo) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	if _, ok := mt.segID2Info[segmentInfo.SegmentID]; !ok {
		return fmt.Errorf("segment %d already exist", segmentInfo.SegmentID)
	}
	mt.segID2Info[segmentInfo.SegmentID] = segmentInfo
	if err := mt.saveSegmentInfo(segmentInfo); err != nil {
		_ = mt.reloadFromKV()
		return err
	}
	return nil
}

func (mt *meta) UpdateSegment(segmentInfo *datapb.SegmentInfo) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	mt.segID2Info[segmentInfo.SegmentID] = segmentInfo
	if err := mt.saveSegmentInfo(segmentInfo); err != nil {
		_ = mt.reloadFromKV()
		return err
	}
	return nil
}

func (mt *meta) GetSegmentByID(segID UniqueID) (*datapb.SegmentInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	segmentInfo, ok := mt.segID2Info[segID]
	if !ok {
		return nil, errors.Errorf("GetSegmentByID:can't find segment id = %d", segID)
	}
	return segmentInfo, nil
}

func (mt *meta) CloseSegment(segID UniqueID, closeTs Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	segInfo, ok := mt.segID2Info[segID]
	if !ok {
		return errors.Errorf("DropSegment:can't find segment id = " + strconv.FormatInt(segID, 10))
	}

	segInfo.CloseTime = closeTs

	err := mt.saveSegmentInfo(segInfo)
	if err != nil {
		_ = mt.reloadFromKV()
		return err
	}
	return nil
}

func (mt *meta) GetCollection(collectionID UniqueID) (*collectionInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	collectionInfo, ok := mt.collID2Info[collectionID]
	if !ok {
		return nil, fmt.Errorf("collection %d not found", collectionID)
	}
	return collectionInfo, nil
}

func (mt *meta) saveSegmentInfo(segmentInfo *datapb.SegmentInfo) error {
	segBytes := proto.MarshalTextString(segmentInfo)

	return mt.client.Save("/segment/"+strconv.FormatInt(segmentInfo.SegmentID, 10), segBytes)
}

func (mt *meta) removeSegmentInfo(segID UniqueID) error {
	return mt.client.Remove("/segment/" + strconv.FormatInt(segID, 10))
}
