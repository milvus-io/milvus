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

package masterservice

import (
	"errors"
	"fmt"
	"path"
	"strconv"
	"sync"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	ComponentPrefix        = "master-service"
	TenantMetaPrefix       = ComponentPrefix + "/tenant"
	ProxyMetaPrefix        = ComponentPrefix + "/proxy"
	CollectionMetaPrefix   = ComponentPrefix + "/collection"
	PartitionMetaPrefix    = ComponentPrefix + "/partition"
	SegmentIndexMetaPrefix = ComponentPrefix + "/segment-index"
	IndexMetaPrefix        = ComponentPrefix + "/index"

	TimestampPrefix = ComponentPrefix + "/timestamp"

	SegInfoMsgStartPosPrefix    = ComponentPrefix + "/seg-info-msg-start-position"
	SegInfoMsgEndPosPrefix      = ComponentPrefix + "/seg-info-msg-end-position"
	FlushedSegMsgStartPosPrefix = ComponentPrefix + "/flushed-seg-msg-start-position"
	FlushedSegMsgEndPosPrefix   = ComponentPrefix + "/flushed-seg-msg-end-position"

	DDOperationPrefix = ComponentPrefix + "/dd-operation"
	DDMsgSendPrefix   = ComponentPrefix + "/dd-msg-send"

	CreateCollectionDDType = "CreateCollection"
	DropCollectionDDType   = "DropCollection"
	CreatePartitionDDType  = "CreatePartition"
	DropPartitionDDType    = "DropPartition"
)

type metaTable struct {
	client             kv.SnapShotKV                                                    // client of a reliable kv service, i.e. etcd client
	tenantID2Meta      map[typeutil.UniqueID]pb.TenantMeta                              // tenant id to tenant meta
	proxyID2Meta       map[typeutil.UniqueID]pb.ProxyMeta                               // proxy id to proxy meta
	collID2Meta        map[typeutil.UniqueID]pb.CollectionInfo                          // collection_id -> meta
	collName2ID        map[string]typeutil.UniqueID                                     // collection name to collection id
	partitionID2Meta   map[typeutil.UniqueID]pb.PartitionInfo                           // collection_id/partition_id -> meta
	segID2IndexMeta    map[typeutil.UniqueID]*map[typeutil.UniqueID]pb.SegmentIndexInfo // collection_id/index_id/partition_id/segment_id -> meta
	indexID2Meta       map[typeutil.UniqueID]pb.IndexInfo                               // collection_id/index_id -> meta
	segID2CollID       map[typeutil.UniqueID]typeutil.UniqueID                          // segment id -> collection id
	segID2PartitionID  map[typeutil.UniqueID]typeutil.UniqueID                          // segment id -> partition id
	flushedSegID       map[typeutil.UniqueID]bool                                       // flushed segment id
	partitionID2CollID map[typeutil.UniqueID]typeutil.UniqueID                          // partition id -> collection id
	vChan2Chan         map[string]string                                                // virtual channel name to physical channel name

	tenantLock sync.RWMutex
	proxyLock  sync.RWMutex
	ddLock     sync.RWMutex
}

func NewMetaTable(kv kv.SnapShotKV) (*metaTable, error) {
	mt := &metaTable{
		client:     kv,
		tenantLock: sync.RWMutex{},
		proxyLock:  sync.RWMutex{},
		ddLock:     sync.RWMutex{},
	}
	err := mt.reloadFromKV()
	if err != nil {
		return nil, err
	}
	return mt, nil
}

func (mt *metaTable) reloadFromKV() error {

	mt.tenantID2Meta = make(map[typeutil.UniqueID]pb.TenantMeta)
	mt.proxyID2Meta = make(map[typeutil.UniqueID]pb.ProxyMeta)
	mt.collID2Meta = make(map[typeutil.UniqueID]pb.CollectionInfo)
	mt.collName2ID = make(map[string]typeutil.UniqueID)
	mt.partitionID2Meta = make(map[typeutil.UniqueID]pb.PartitionInfo)
	mt.segID2IndexMeta = make(map[typeutil.UniqueID]*map[typeutil.UniqueID]pb.SegmentIndexInfo)
	mt.indexID2Meta = make(map[typeutil.UniqueID]pb.IndexInfo)
	mt.partitionID2CollID = make(map[typeutil.UniqueID]typeutil.UniqueID)
	mt.segID2CollID = make(map[typeutil.UniqueID]typeutil.UniqueID)
	mt.segID2PartitionID = make(map[typeutil.UniqueID]typeutil.UniqueID)
	mt.flushedSegID = make(map[typeutil.UniqueID]bool)
	mt.vChan2Chan = make(map[string]string)

	_, values, err := mt.client.LoadWithPrefix(TenantMetaPrefix, 0)
	if err != nil {
		return err
	}

	for _, value := range values {
		tenantMeta := pb.TenantMeta{}
		err := proto.UnmarshalText(value, &tenantMeta)
		if err != nil {
			return fmt.Errorf("MasterService UnmarshalText pb.TenantMeta err:%w", err)
		}
		mt.tenantID2Meta[tenantMeta.ID] = tenantMeta
	}

	_, values, err = mt.client.LoadWithPrefix(ProxyMetaPrefix, 0)
	if err != nil {
		return err
	}

	for _, value := range values {
		proxyMeta := pb.ProxyMeta{}
		err = proto.UnmarshalText(value, &proxyMeta)
		if err != nil {
			return fmt.Errorf("MasterService UnmarshalText pb.ProxyMeta err:%w", err)
		}
		mt.proxyID2Meta[proxyMeta.ID] = proxyMeta
	}

	_, values, err = mt.client.LoadWithPrefix(CollectionMetaPrefix, 0)
	if err != nil {
		return err
	}

	for _, value := range values {
		collInfo := pb.CollectionInfo{}
		err = proto.UnmarshalText(value, &collInfo)
		if err != nil {
			return fmt.Errorf("MasterService UnmarshalText pb.CollectionInfo err:%w", err)
		}
		mt.collID2Meta[collInfo.ID] = collInfo
		mt.collName2ID[collInfo.Schema.Name] = collInfo.ID
		for _, partID := range collInfo.PartitionIDs {
			mt.partitionID2CollID[partID] = collInfo.ID
		}
		shardsNum := len(collInfo.VirtualChannelNames)
		for i := 0; i < shardsNum; i++ {
			mt.vChan2Chan[collInfo.VirtualChannelNames[i]] = collInfo.PhysicalChannelNames[i]
		}
	}

	_, values, err = mt.client.LoadWithPrefix(PartitionMetaPrefix, 0)
	if err != nil {
		return err
	}
	for _, value := range values {
		partitionInfo := pb.PartitionInfo{}
		err = proto.UnmarshalText(value, &partitionInfo)
		if err != nil {
			return fmt.Errorf("MasterService UnmarshalText pb.PartitionInfo err:%w", err)
		}
		collID, ok := mt.partitionID2CollID[partitionInfo.PartitionID]
		if !ok {
			log.Warn("partition does not belong to any collection", zap.Int64("partition id", partitionInfo.PartitionID))
			continue
		}
		mt.partitionID2Meta[partitionInfo.PartitionID] = partitionInfo
		for _, segID := range partitionInfo.SegmentIDs {
			mt.segID2CollID[segID] = collID
			mt.segID2PartitionID[segID] = partitionInfo.PartitionID
			mt.flushedSegID[segID] = true
		}
	}

	_, values, err = mt.client.LoadWithPrefix(SegmentIndexMetaPrefix, 0)
	if err != nil {
		return err
	}
	for _, value := range values {
		segmentIndexInfo := pb.SegmentIndexInfo{}
		err = proto.UnmarshalText(value, &segmentIndexInfo)
		if err != nil {
			return fmt.Errorf("MasterService UnmarshalText pb.SegmentIndexInfo err:%w", err)
		}
		idx, ok := mt.segID2IndexMeta[segmentIndexInfo.SegmentID]
		if ok {
			(*idx)[segmentIndexInfo.IndexID] = segmentIndexInfo
		} else {
			meta := make(map[typeutil.UniqueID]pb.SegmentIndexInfo)
			meta[segmentIndexInfo.IndexID] = segmentIndexInfo
			mt.segID2IndexMeta[segmentIndexInfo.SegmentID] = &meta
		}
	}

	_, values, err = mt.client.LoadWithPrefix(IndexMetaPrefix, 0)
	if err != nil {
		return err
	}
	for _, value := range values {
		meta := pb.IndexInfo{}
		err = proto.UnmarshalText(value, &meta)
		if err != nil {
			return fmt.Errorf("MasterService UnmarshalText pb.IndexInfo err:%w", err)
		}
		mt.indexID2Meta[meta.IndexID] = meta
	}

	return nil
}

func (mt *metaTable) getAdditionKV(op func(ts typeutil.Timestamp) (string, error), meta map[string]string) func(ts typeutil.Timestamp) (string, string, error) {
	if op == nil {
		return nil
	}
	meta[DDMsgSendPrefix] = "false"
	return func(ts typeutil.Timestamp) (string, string, error) {
		val, err := op(ts)
		if err != nil {
			return "", "", err
		}
		return DDOperationPrefix, val, nil
	}
}

func (mt *metaTable) AddTenant(te *pb.TenantMeta) (typeutil.Timestamp, error) {
	mt.tenantLock.Lock()
	defer mt.tenantLock.Unlock()

	k := fmt.Sprintf("%s/%d", TenantMetaPrefix, te.ID)
	v := proto.MarshalTextString(te)

	ts, err := mt.client.Save(k, v)
	if err != nil {
		return 0, err
	}
	mt.tenantID2Meta[te.ID] = *te
	return ts, nil
}

func (mt *metaTable) AddProxy(po *pb.ProxyMeta) (typeutil.Timestamp, error) {
	mt.proxyLock.Lock()
	defer mt.proxyLock.Unlock()

	k := fmt.Sprintf("%s/%d", ProxyMetaPrefix, po.ID)
	v := proto.MarshalTextString(po)

	ts, err := mt.client.Save(k, v)
	if err != nil {
		return 0, err
	}
	mt.proxyID2Meta[po.ID] = *po
	return ts, nil
}

func (mt *metaTable) AddCollection(coll *pb.CollectionInfo, part *pb.PartitionInfo, idx []*pb.IndexInfo, ddOpStr func(ts typeutil.Timestamp) (string, error)) (typeutil.Timestamp, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if len(part.SegmentIDs) != 0 {
		return 0, errors.New("segment should be empty when creating collection")
	}
	if len(coll.PartitionIDs) != 0 {
		return 0, errors.New("partitions should be empty when creating collection")
	}
	if _, ok := mt.collName2ID[coll.Schema.Name]; ok {
		return 0, fmt.Errorf("collection %s exist", coll.Schema.Name)
	}
	if len(coll.FieldIndexes) != len(idx) {
		return 0, fmt.Errorf("incorrect index id when creating collection")
	}
	if _, ok := mt.partitionID2Meta[part.PartitionID]; ok {
		return 0, fmt.Errorf("partition id = %d exist", part.PartitionID)
	}

	coll.PartitionIDs = append(coll.PartitionIDs, part.PartitionID)
	mt.collID2Meta[coll.ID] = *coll
	mt.collName2ID[coll.Schema.Name] = coll.ID
	mt.partitionID2Meta[part.PartitionID] = *part
	mt.partitionID2CollID[part.PartitionID] = coll.ID
	for _, i := range idx {
		mt.indexID2Meta[i.IndexID] = *i
	}

	k1 := fmt.Sprintf("%s/%d", CollectionMetaPrefix, coll.ID)
	v1 := proto.MarshalTextString(coll)
	k2 := fmt.Sprintf("%s/%d/%d", PartitionMetaPrefix, coll.ID, part.PartitionID)
	v2 := proto.MarshalTextString(part)
	meta := map[string]string{k1: v1, k2: v2}

	for _, i := range idx {
		k := fmt.Sprintf("%s/%d/%d", IndexMetaPrefix, coll.ID, i.IndexID)
		v := proto.MarshalTextString(i)
		meta[k] = v
	}

	shardsNum := len(coll.VirtualChannelNames)
	for i := 0; i < shardsNum; i++ {
		mt.vChan2Chan[coll.VirtualChannelNames[i]] = coll.PhysicalChannelNames[i]
	}

	// save ddOpStr into etcd
	addition := mt.getAdditionKV(ddOpStr, meta)
	ts, err := mt.client.MultiSave(meta, addition)
	if err != nil {
		_ = mt.reloadFromKV()
		return 0, err
	}

	return ts, nil
}

func (mt *metaTable) DeleteCollection(collID typeutil.UniqueID, ddOpStr func(ts typeutil.Timestamp) (string, error)) (typeutil.Timestamp, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	collMeta, ok := mt.collID2Meta[collID]
	if !ok {
		return 0, fmt.Errorf("can't find collection. id = %d", collID)
	}

	delete(mt.collID2Meta, collID)
	delete(mt.collName2ID, collMeta.Schema.Name)
	for _, partID := range collMeta.PartitionIDs {
		partMeta, ok := mt.partitionID2Meta[partID]
		if !ok {
			log.Warn("partition id not exist", zap.Int64("partition id", partID))
			continue
		}
		delete(mt.partitionID2Meta, partID)
		for _, segID := range partMeta.SegmentIDs {
			delete(mt.segID2CollID, segID)
			delete(mt.segID2PartitionID, segID)
			delete(mt.flushedSegID, segID)
			_, ok := mt.segID2IndexMeta[segID]
			if !ok {
				log.Warn("segment id not exist", zap.Int64("segment id", segID))
				continue
			}
			delete(mt.segID2IndexMeta, segID)
		}
	}
	for _, idxInfo := range collMeta.FieldIndexes {
		_, ok := mt.indexID2Meta[idxInfo.IndexID]
		if !ok {
			log.Warn("index id not exist", zap.Int64("index id", idxInfo.IndexID))
			continue
		}
		delete(mt.indexID2Meta, idxInfo.IndexID)
	}

	shardsNum := len(collMeta.VirtualChannelNames)
	for i := 0; i < shardsNum; i++ {
		delete(mt.vChan2Chan, collMeta.VirtualChannelNames[i])
	}

	delMetakeys := []string{
		fmt.Sprintf("%s/%d", CollectionMetaPrefix, collID),
		fmt.Sprintf("%s/%d", PartitionMetaPrefix, collID),
		fmt.Sprintf("%s/%d", SegmentIndexMetaPrefix, collID),
		fmt.Sprintf("%s/%d", IndexMetaPrefix, collID),
	}

	// save ddOpStr into etcd
	var saveMeta = map[string]string{}
	addition := mt.getAdditionKV(ddOpStr, saveMeta)
	ts, err := mt.client.MultiSaveAndRemoveWithPrefix(saveMeta, delMetakeys, addition)
	if err != nil {
		_ = mt.reloadFromKV()
		return 0, err
	}

	return ts, nil
}

func (mt *metaTable) HasCollection(collID typeutil.UniqueID, ts typeutil.Timestamp) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	if ts == 0 {
		_, ok := mt.collID2Meta[collID]
		return ok
	}
	key := fmt.Sprintf("%s/%d", CollectionMetaPrefix, collID)
	_, err := mt.client.Load(key, ts)
	return err == nil
}

func (mt *metaTable) GetCollectionByID(collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*pb.CollectionInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	if ts == 0 {
		col, ok := mt.collID2Meta[collectionID]
		if !ok {
			return nil, fmt.Errorf("can't find collection id : %d", collectionID)
		}
		colCopy := proto.Clone(&col)
		return colCopy.(*pb.CollectionInfo), nil
	}
	key := fmt.Sprintf("%s/%d", CollectionMetaPrefix, collectionID)
	val, err := mt.client.Load(key, ts)
	if err != nil {
		return nil, err
	}
	colMeta := pb.CollectionInfo{}
	err = proto.UnmarshalText(val, &colMeta)
	if err != nil {
		return nil, err
	}
	return &colMeta, nil
}

func (mt *metaTable) GetCollectionByName(collectionName string, ts typeutil.Timestamp) (*pb.CollectionInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	if ts == 0 {
		vid, ok := mt.collName2ID[collectionName]
		if !ok {
			return nil, fmt.Errorf("can't find collection: " + collectionName)
		}
		col, ok := mt.collID2Meta[vid]
		if !ok {
			return nil, fmt.Errorf("can't find collection: " + collectionName)
		}
		colCopy := proto.Clone(&col)
		return colCopy.(*pb.CollectionInfo), nil
	}
	_, vals, err := mt.client.LoadWithPrefix(CollectionMetaPrefix, ts)
	if err != nil {
		return nil, err
	}
	for _, val := range vals {
		collMeta := pb.CollectionInfo{}
		err = proto.UnmarshalText(val, &collMeta)
		if err != nil {
			log.Debug("unmarshal collection info failed", zap.Error(err))
			continue
		}
		if collMeta.Schema.Name == collectionName {
			return &collMeta, nil
		}
	}
	return nil, fmt.Errorf("can't find collection: %s, at timestamp = %d", collectionName, ts)
}

func (mt *metaTable) GetCollectionBySegmentID(segID typeutil.UniqueID) (*pb.CollectionInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	vid, ok := mt.segID2CollID[segID]
	if !ok {
		return nil, fmt.Errorf("segment id %d not belong to any collection", segID)
	}
	col, ok := mt.collID2Meta[vid]
	if !ok {
		return nil, fmt.Errorf("can't find collection id: %d", vid)
	}
	colCopy := proto.Clone(&col)
	return colCopy.(*pb.CollectionInfo), nil
}

func (mt *metaTable) ListCollections(ts typeutil.Timestamp) (map[string]typeutil.UniqueID, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	if ts == 0 {
		return mt.collName2ID, nil
	}
	_, vals, err := mt.client.LoadWithPrefix(CollectionMetaPrefix, ts)
	if err != nil {
		log.Debug("load with prefix error", zap.Uint64("timestamp", ts), zap.Error(err))
		return nil, nil
	}
	colls := make(map[string]typeutil.UniqueID)
	for _, val := range vals {
		collMeta := pb.CollectionInfo{}
		err := proto.UnmarshalText(val, &collMeta)
		if err != nil {
			log.Debug("unmarshal collection info failed", zap.Error(err))
		}
		colls[collMeta.Schema.Name] = collMeta.ID
	}
	return colls, nil
}

// ListCollectionVirtualChannels list virtual channel of all the collection
func (mt *metaTable) ListCollectionVirtualChannels() []string {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	vlist := []string{}

	for _, c := range mt.collID2Meta {
		vlist = append(vlist, c.VirtualChannelNames...)
	}
	return vlist
}

// ListCollectionPhysicalChannels list physical channel of all the collection
func (mt *metaTable) ListCollectionPhysicalChannels() []string {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	plist := []string{}

	for _, c := range mt.collID2Meta {
		plist = append(plist, c.PhysicalChannelNames...)
	}
	return plist
}

func (mt *metaTable) AddPartition(collID typeutil.UniqueID, partitionName string, partitionID typeutil.UniqueID, ddOpStr func(ts typeutil.Timestamp) (string, error)) (typeutil.Timestamp, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	coll, ok := mt.collID2Meta[collID]
	if !ok {
		return 0, fmt.Errorf("can't find collection. id = %d", collID)
	}

	// number of partition tags (except _default) should be limited to 4096 by default
	if int64(len(coll.PartitionIDs)) >= Params.MaxPartitionNum {
		return 0, fmt.Errorf("maximum partition's number should be limit to %d", Params.MaxPartitionNum)
	}
	for _, t := range coll.PartitionIDs {
		part, ok := mt.partitionID2Meta[t]
		if !ok {
			log.Warn("partition id not exist", zap.Int64("partition id", t))
			continue
		}
		if part.PartitionName == partitionName {
			return 0, fmt.Errorf("partition name = %s already exists", partitionName)
		}
		if part.PartitionID == partitionID {
			return 0, fmt.Errorf("partition id = %d already exists", partitionID)
		}
	}
	partMeta := pb.PartitionInfo{
		PartitionName: partitionName,
		PartitionID:   partitionID,
		SegmentIDs:    make([]typeutil.UniqueID, 0, 16),
	}
	coll.PartitionIDs = append(coll.PartitionIDs, partitionID)
	mt.partitionID2Meta[partitionID] = partMeta
	mt.collID2Meta[collID] = coll
	mt.partitionID2CollID[partitionID] = collID

	k1 := fmt.Sprintf("%s/%d", CollectionMetaPrefix, collID)
	v1 := proto.MarshalTextString(&coll)
	k2 := fmt.Sprintf("%s/%d/%d", PartitionMetaPrefix, collID, partitionID)
	v2 := proto.MarshalTextString(&partMeta)
	meta := map[string]string{k1: v1, k2: v2}

	// save ddOpStr into etcd
	addition := mt.getAdditionKV(ddOpStr, meta)

	ts, err := mt.client.MultiSave(meta, addition)
	if err != nil {
		_ = mt.reloadFromKV()
		return 0, err
	}
	return ts, nil
}

func (mt *metaTable) getPartitionByName(collID typeutil.UniqueID, partitionName string, ts typeutil.Timestamp) (pb.PartitionInfo, error) {
	if ts == 0 {
		collMeta, ok := mt.collID2Meta[collID]
		if !ok {
			return pb.PartitionInfo{}, fmt.Errorf("can't find collection id = %d", collID)
		}
		for _, id := range collMeta.PartitionIDs {
			partMeta, ok := mt.partitionID2Meta[id]
			if ok && partMeta.PartitionName == partitionName {
				return partMeta, nil
			}
		}
		return pb.PartitionInfo{}, fmt.Errorf("partition %s does not exist", partitionName)
	}
	collKey := fmt.Sprintf("%s/%d", CollectionMetaPrefix, collID)
	collVal, err := mt.client.Load(collKey, ts)
	if err != nil {
		return pb.PartitionInfo{}, err
	}
	collMeta := pb.CollectionMeta{}
	err = proto.UnmarshalText(collVal, &collMeta)
	if err != nil {
		return pb.PartitionInfo{}, err
	}
	for _, id := range collMeta.PartitionIDs {
		partKey := fmt.Sprintf("%s/%d/%d", PartitionMetaPrefix, collID, id)
		partVal, err := mt.client.Load(partKey, ts)
		if err != nil {
			log.Debug("load partition meta failed", zap.String("collection name", collMeta.Schema.Name), zap.Int64("partition id", id))
			continue
		}
		partMeta := pb.PartitionInfo{}
		err = proto.UnmarshalText(partVal, &partMeta)
		if err != nil {
			log.Debug("unmarshal partition meta failed", zap.Error(err))
			continue
		}
		if partMeta.PartitionName == partitionName {
			return partMeta, nil
		}
	}
	return pb.PartitionInfo{}, fmt.Errorf("partition %s does not exist", partitionName)
}

func (mt *metaTable) GetPartitionByName(collID typeutil.UniqueID, partitionName string, ts typeutil.Timestamp) (pb.PartitionInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	return mt.getPartitionByName(collID, partitionName, ts)
}

func (mt *metaTable) HasPartition(collID typeutil.UniqueID, partitionName string, ts typeutil.Timestamp) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	_, err := mt.getPartitionByName(collID, partitionName, ts)
	return err == nil
}

//return timestamp, partitionid, error
func (mt *metaTable) DeletePartition(collID typeutil.UniqueID, partitionName string, ddOpStr func(ts typeutil.Timestamp) (string, error)) (typeutil.Timestamp, typeutil.UniqueID, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if partitionName == Params.DefaultPartitionName {
		return 0, 0, fmt.Errorf("default partition cannot be deleted")
	}

	collMeta, ok := mt.collID2Meta[collID]
	if !ok {
		return 0, 0, fmt.Errorf("can't find collection id = %d", collID)
	}

	// check tag exists
	exist := false

	pd := make([]typeutil.UniqueID, 0, len(collMeta.PartitionIDs))
	var partMeta pb.PartitionInfo
	for _, t := range collMeta.PartitionIDs {
		pm, ok := mt.partitionID2Meta[t]
		if ok {
			if pm.PartitionName != partitionName {
				pd = append(pd, pm.PartitionID)
			} else {
				partMeta = pm
				exist = true
			}
		}
	}
	if !exist {
		return 0, 0, fmt.Errorf("partition %s does not exist", partitionName)
	}
	delete(mt.partitionID2Meta, partMeta.PartitionID)
	collMeta.PartitionIDs = pd
	mt.collID2Meta[collID] = collMeta

	for _, segID := range partMeta.SegmentIDs {
		delete(mt.segID2CollID, segID)
		delete(mt.segID2PartitionID, segID)
		delete(mt.flushedSegID, segID)

		_, ok := mt.segID2IndexMeta[segID]
		if !ok {
			log.Warn("segment has no index meta", zap.Int64("segment id", segID))
			continue
		}
		delete(mt.segID2IndexMeta, segID)
	}
	meta := map[string]string{path.Join(CollectionMetaPrefix, strconv.FormatInt(collID, 10)): proto.MarshalTextString(&collMeta)}
	delMetaKeys := []string{
		fmt.Sprintf("%s/%d/%d", PartitionMetaPrefix, collMeta.ID, partMeta.PartitionID),
	}
	for _, idxInfo := range collMeta.FieldIndexes {
		k := fmt.Sprintf("%s/%d/%d/%d", SegmentIndexMetaPrefix, collMeta.ID, idxInfo.IndexID, partMeta.PartitionID)
		delMetaKeys = append(delMetaKeys, k)
	}

	// save ddOpStr into etcd
	addition := mt.getAdditionKV(ddOpStr, meta)

	ts, err := mt.client.MultiSaveAndRemoveWithPrefix(meta, delMetaKeys, addition)
	if err != nil {
		_ = mt.reloadFromKV()
		return 0, 0, err
	}
	return ts, partMeta.PartitionID, nil
}

func (mt *metaTable) GetPartitionByID(collID typeutil.UniqueID, partitionID typeutil.UniqueID, ts typeutil.Timestamp) (pb.PartitionInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	if ts == 0 {
		partMeta, ok := mt.partitionID2Meta[partitionID]
		if !ok {
			return pb.PartitionInfo{}, fmt.Errorf("partition id = %d not exist", partitionID)
		}
		return partMeta, nil
	}
	partKey := fmt.Sprintf("%s/%d/%d", PartitionMetaPrefix, collID, partitionID)
	partVal, err := mt.client.Load(partKey, ts)
	if err != nil {
		return pb.PartitionInfo{}, err
	}
	partInfo := pb.PartitionInfo{}
	err = proto.UnmarshalText(partVal, &partInfo)
	if err != nil {
		return pb.PartitionInfo{}, err
	}
	return partInfo, nil

}

func (mt *metaTable) AddSegment(segInfos []*datapb.SegmentInfo, msgStartPos string, msgEndPos string) (typeutil.Timestamp, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	meta := make(map[string]string)
	for _, segInfo := range segInfos {
		collMeta, ok := mt.collID2Meta[segInfo.CollectionID]
		if !ok {
			return 0, fmt.Errorf("can't find collection id = %d", segInfo.CollectionID)
		}
		partMeta, ok := mt.partitionID2Meta[segInfo.PartitionID]
		if !ok {
			return 0, fmt.Errorf("can't find partition id = %d", segInfo.PartitionID)
		}
		exist := false
		for _, partID := range collMeta.PartitionIDs {
			if partID == segInfo.PartitionID {
				exist = true
				break
			}
		}
		if !exist {
			return 0, fmt.Errorf("partition id = %d, not belong to collection id = %d", segInfo.PartitionID, segInfo.CollectionID)
		}
		exist = false
		for _, segID := range partMeta.SegmentIDs {
			if segID == segInfo.ID {
				exist = true
			}
		}
		if exist {
			return 0, fmt.Errorf("segment id = %d exist", segInfo.ID)
		}
		partMeta.SegmentIDs = append(partMeta.SegmentIDs, segInfo.ID)
		mt.partitionID2Meta[segInfo.PartitionID] = partMeta
		mt.segID2CollID[segInfo.ID] = segInfo.CollectionID
		mt.segID2PartitionID[segInfo.ID] = segInfo.PartitionID

		k := fmt.Sprintf("%s/%d/%d", PartitionMetaPrefix, segInfo.CollectionID, segInfo.PartitionID)
		v := proto.MarshalTextString(&partMeta)
		meta[k] = v
	}

	// AddSegment is invoked from DataService
	if msgStartPos != "" && msgEndPos != "" {
		meta[SegInfoMsgStartPosPrefix] = msgStartPos
		meta[SegInfoMsgEndPosPrefix] = msgEndPos
	}

	ts, err := mt.client.MultiSave(meta, nil)
	if err != nil {
		_ = mt.reloadFromKV()
		return 0, err
	}
	return ts, nil
}

func (mt *metaTable) AddIndex(segIdxInfos []*pb.SegmentIndexInfo, msgStartPos string, msgEndPos string) (typeutil.Timestamp, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	meta := make(map[string]string)

	for _, segIdxInfo := range segIdxInfos {
		collID, ok := mt.segID2CollID[segIdxInfo.SegmentID]
		if !ok {
			return 0, fmt.Errorf("segment id = %d not belong to any collection", segIdxInfo.SegmentID)
		}
		collMeta, ok := mt.collID2Meta[collID]
		if !ok {
			return 0, fmt.Errorf("collection id = %d not found", collID)
		}
		partID, ok := mt.segID2PartitionID[segIdxInfo.SegmentID]
		if !ok {
			return 0, fmt.Errorf("segment id = %d not belong to any partition", segIdxInfo.SegmentID)
		}
		exist := false
		for _, fidx := range collMeta.FieldIndexes {
			if fidx.IndexID == segIdxInfo.IndexID {
				exist = true
				break
			}
		}
		if !exist {
			return 0, fmt.Errorf("index id = %d not found", segIdxInfo.IndexID)
		}

		segIdxMap, ok := mt.segID2IndexMeta[segIdxInfo.SegmentID]
		if !ok {
			idxMap := map[typeutil.UniqueID]pb.SegmentIndexInfo{segIdxInfo.IndexID: *segIdxInfo}
			mt.segID2IndexMeta[segIdxInfo.SegmentID] = &idxMap
		} else {
			tmpInfo, ok := (*segIdxMap)[segIdxInfo.IndexID]
			if ok {
				if SegmentIndexInfoEqual(segIdxInfo, &tmpInfo) {
					log.Debug("Identical SegmentIndexInfo already exist", zap.Int64("IndexID", segIdxInfo.IndexID))
					continue
				}
				return 0, fmt.Errorf("index id = %d exist", segIdxInfo.IndexID)
			}
		}

		if _, ok := mt.flushedSegID[segIdxInfo.SegmentID]; !ok {
			mt.flushedSegID[segIdxInfo.SegmentID] = true
		}

		(*(mt.segID2IndexMeta[segIdxInfo.SegmentID]))[segIdxInfo.IndexID] = *segIdxInfo
		k := fmt.Sprintf("%s/%d/%d/%d/%d", SegmentIndexMetaPrefix, collID, segIdxInfo.IndexID, partID, segIdxInfo.SegmentID)
		v := proto.MarshalTextString(segIdxInfo)
		meta[k] = v
	}

	// AddIndex is invoked from DataNode flush operation
	if msgStartPos != "" && msgEndPos != "" {
		meta[FlushedSegMsgStartPosPrefix] = msgStartPos
		meta[FlushedSegMsgEndPosPrefix] = msgEndPos
	}

	ts, err := mt.client.MultiSave(meta, nil)
	if err != nil {
		_ = mt.reloadFromKV()
		return 0, err
	}

	return ts, nil
}

//return timestamp, index id, is dropped, error
func (mt *metaTable) DropIndex(collName, fieldName, indexName string) (typeutil.Timestamp, typeutil.UniqueID, bool, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	collID, ok := mt.collName2ID[collName]
	if !ok {
		return 0, 0, false, fmt.Errorf("collection name = %s not exist", collName)
	}
	collMeta, ok := mt.collID2Meta[collID]
	if !ok {
		return 0, 0, false, fmt.Errorf("collection name  = %s not has meta", collName)
	}
	fieldSch, err := mt.unlockGetFieldSchema(collName, fieldName)
	if err != nil {
		return 0, 0, false, err
	}
	fieldIdxInfo := make([]*pb.FieldIndexInfo, 0, len(collMeta.FieldIndexes))
	var dropIdxID typeutil.UniqueID
	for i, info := range collMeta.FieldIndexes {
		if info.FiledID != fieldSch.FieldID {
			fieldIdxInfo = append(fieldIdxInfo, info)
			continue
		}
		idxMeta, ok := mt.indexID2Meta[info.IndexID]
		if !ok {
			fieldIdxInfo = append(fieldIdxInfo, info)
			log.Warn("index id not has meta", zap.Int64("index id", info.IndexID))
			continue
		}
		if idxMeta.IndexName != indexName {
			fieldIdxInfo = append(fieldIdxInfo, info)
			continue
		}
		dropIdxID = info.IndexID
		fieldIdxInfo = append(fieldIdxInfo, collMeta.FieldIndexes[i+1:]...)
		break
	}
	if len(fieldIdxInfo) == len(collMeta.FieldIndexes) {
		log.Warn("drop index,index not found", zap.String("collection name", collName), zap.String("filed name", fieldName), zap.String("index name", indexName))
		return 0, 0, false, nil
	}
	collMeta.FieldIndexes = fieldIdxInfo
	mt.collID2Meta[collID] = collMeta
	saveMeta := map[string]string{path.Join(CollectionMetaPrefix, strconv.FormatInt(collID, 10)): proto.MarshalTextString(&collMeta)}

	delete(mt.indexID2Meta, dropIdxID)

	for _, partID := range collMeta.PartitionIDs {
		partMeta, ok := mt.partitionID2Meta[partID]
		if !ok {
			log.Warn("partition not exist", zap.Int64("partition id", partID))
			continue
		}
		for _, segID := range partMeta.SegmentIDs {
			segInfo, ok := mt.segID2IndexMeta[segID]
			if ok {
				_, ok := (*segInfo)[dropIdxID]
				if ok {
					delete(*segInfo, dropIdxID)
				}
			}
		}
	}
	delMeta := []string{
		fmt.Sprintf("%s/%d/%d", SegmentIndexMetaPrefix, collMeta.ID, dropIdxID),
		fmt.Sprintf("%s/%d/%d", IndexMetaPrefix, collMeta.ID, dropIdxID),
	}

	ts, err := mt.client.MultiSaveAndRemoveWithPrefix(saveMeta, delMeta, nil)
	if err != nil {
		_ = mt.reloadFromKV()
		return 0, 0, false, err
	}

	return ts, dropIdxID, true, nil
}

func (mt *metaTable) GetSegmentIndexInfoByID(segID typeutil.UniqueID, filedID int64, idxName string) (pb.SegmentIndexInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	_, ok := mt.flushedSegID[segID]
	if !ok {
		return pb.SegmentIndexInfo{}, fmt.Errorf("segment id %d hasn't flushed, there is no index meta", segID)
	}

	segIdxMap, ok := mt.segID2IndexMeta[segID]
	if !ok {
		return pb.SegmentIndexInfo{
			SegmentID:   segID,
			FieldID:     filedID,
			IndexID:     0,
			BuildID:     0,
			EnableIndex: false,
		}, nil
	}
	if len(*segIdxMap) == 0 {
		return pb.SegmentIndexInfo{}, fmt.Errorf("segment id %d not has any index", segID)
	}

	if filedID == -1 && idxName == "" { // return default index
		for _, seg := range *segIdxMap {
			info, ok := mt.indexID2Meta[seg.IndexID]
			if ok && info.IndexName == Params.DefaultIndexName {
				return seg, nil
			}
		}
	} else {
		for idxID, seg := range *segIdxMap {
			idxMeta, ok := mt.indexID2Meta[idxID]
			if ok {
				if idxMeta.IndexName != idxName {
					continue
				}
				if seg.FieldID != filedID {
					continue
				}
				return seg, nil
			}
		}
	}
	return pb.SegmentIndexInfo{}, fmt.Errorf("can't find index name = %s on segment = %d, with filed id = %d", idxName, segID, filedID)
}

func (mt *metaTable) GetFieldSchema(collName string, fieldName string) (schemapb.FieldSchema, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	return mt.unlockGetFieldSchema(collName, fieldName)
}

func (mt *metaTable) unlockGetFieldSchema(collName string, fieldName string) (schemapb.FieldSchema, error) {
	collID, ok := mt.collName2ID[collName]
	if !ok {
		return schemapb.FieldSchema{}, fmt.Errorf("collection %s not found", collName)
	}
	collMeta, ok := mt.collID2Meta[collID]
	if !ok {
		return schemapb.FieldSchema{}, fmt.Errorf("collection %s not found", collName)
	}

	for _, field := range collMeta.Schema.Fields {
		if field.Name == fieldName {
			return *field, nil
		}
	}
	return schemapb.FieldSchema{}, fmt.Errorf("collection %s doesn't have filed %s", collName, fieldName)
}

//return true/false
func (mt *metaTable) IsSegmentIndexed(segID typeutil.UniqueID, fieldSchema *schemapb.FieldSchema, indexParams []*commonpb.KeyValuePair) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	return mt.unlockIsSegmentIndexed(segID, fieldSchema, indexParams)
}

func (mt *metaTable) unlockIsSegmentIndexed(segID typeutil.UniqueID, fieldSchema *schemapb.FieldSchema, indexParams []*commonpb.KeyValuePair) bool {
	segIdx, ok := mt.segID2IndexMeta[segID]
	if !ok {
		return false
	}
	exist := false
	for idxID, meta := range *segIdx {
		if meta.FieldID != fieldSchema.FieldID {
			continue
		}
		idxMeta, ok := mt.indexID2Meta[idxID]
		if !ok {
			continue
		}
		if EqualKeyPairArray(indexParams, idxMeta.IndexParams) {
			exist = true
			break
		}
	}
	return exist
}

// return segment ids, type params, error
func (mt *metaTable) GetNotIndexedSegments(collName string, fieldName string, idxInfo *pb.IndexInfo) ([]typeutil.UniqueID, schemapb.FieldSchema, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if idxInfo.IndexParams == nil {
		return nil, schemapb.FieldSchema{}, fmt.Errorf("index param is nil")
	}
	collID, ok := mt.collName2ID[collName]
	if !ok {
		return nil, schemapb.FieldSchema{}, fmt.Errorf("collection %s not found", collName)
	}
	collMeta, ok := mt.collID2Meta[collID]
	if !ok {
		return nil, schemapb.FieldSchema{}, fmt.Errorf("collection %s not found", collName)
	}
	fieldSchema, err := mt.unlockGetFieldSchema(collName, fieldName)
	if err != nil {
		return nil, fieldSchema, err
	}

	var dupIdx typeutil.UniqueID = 0
	for _, f := range collMeta.FieldIndexes {
		if info, ok := mt.indexID2Meta[f.IndexID]; ok {
			if info.IndexName == idxInfo.IndexName {
				dupIdx = info.IndexID
				break
			}
		}
	}

	exist := false
	var existInfo pb.IndexInfo
	for _, f := range collMeta.FieldIndexes {
		if f.FiledID == fieldSchema.FieldID {
			existInfo, ok = mt.indexID2Meta[f.IndexID]
			if !ok {
				return nil, schemapb.FieldSchema{}, fmt.Errorf("index id = %d not found", f.IndexID)
			}
			if EqualKeyPairArray(existInfo.IndexParams, idxInfo.IndexParams) {
				exist = true
				break
			}
		}
	}
	if !exist {
		idx := &pb.FieldIndexInfo{
			FiledID: fieldSchema.FieldID,
			IndexID: idxInfo.IndexID,
		}
		collMeta.FieldIndexes = append(collMeta.FieldIndexes, idx)
		mt.collID2Meta[collMeta.ID] = collMeta
		k1 := path.Join(CollectionMetaPrefix, strconv.FormatInt(collMeta.ID, 10))
		v1 := proto.MarshalTextString(&collMeta)

		mt.indexID2Meta[idx.IndexID] = *idxInfo
		k2 := path.Join(IndexMetaPrefix, strconv.FormatInt(idx.IndexID, 10))
		v2 := proto.MarshalTextString(idxInfo)
		meta := map[string]string{k1: v1, k2: v2}

		if dupIdx != 0 {
			dupInfo := mt.indexID2Meta[dupIdx]
			dupInfo.IndexName = dupInfo.IndexName + "_bak"
			mt.indexID2Meta[dupIdx] = dupInfo
			k := path.Join(IndexMetaPrefix, strconv.FormatInt(dupInfo.IndexID, 10))
			v := proto.MarshalTextString(&dupInfo)
			meta[k] = v
		}

		_, err = mt.client.MultiSave(meta, nil)
		if err != nil {
			_ = mt.reloadFromKV()
			return nil, schemapb.FieldSchema{}, err
		}

	} else {
		idxInfo.IndexID = existInfo.IndexID
		if existInfo.IndexName != idxInfo.IndexName { //replace index name
			existInfo.IndexName = idxInfo.IndexName
			mt.indexID2Meta[existInfo.IndexID] = existInfo
			k := path.Join(IndexMetaPrefix, strconv.FormatInt(existInfo.IndexID, 10))
			v := proto.MarshalTextString(&existInfo)
			meta := map[string]string{k: v}
			if dupIdx != 0 {
				dupInfo := mt.indexID2Meta[dupIdx]
				dupInfo.IndexName = dupInfo.IndexName + "_bak"
				mt.indexID2Meta[dupIdx] = dupInfo
				k := path.Join(IndexMetaPrefix, strconv.FormatInt(dupInfo.IndexID, 10))
				v := proto.MarshalTextString(&dupInfo)
				meta[k] = v
			}

			_, err = mt.client.MultiSave(meta, nil)
			if err != nil {
				_ = mt.reloadFromKV()
				return nil, schemapb.FieldSchema{}, err
			}
		}
	}

	rstID := make([]typeutil.UniqueID, 0, 16)
	for _, partID := range collMeta.PartitionIDs {
		partMeta, ok := mt.partitionID2Meta[partID]
		if ok {
			for _, segID := range partMeta.SegmentIDs {
				if exist := mt.unlockIsSegmentIndexed(segID, &fieldSchema, idxInfo.IndexParams); !exist {
					rstID = append(rstID, segID)
				}
			}
		}
	}
	return rstID, fieldSchema, nil
}

func (mt *metaTable) GetIndexByName(collName, indexName string) (pb.CollectionInfo, []pb.IndexInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	collID, ok := mt.collName2ID[collName]
	if !ok {
		return pb.CollectionInfo{}, nil, fmt.Errorf("collection %s not found", collName)
	}
	collMeta, ok := mt.collID2Meta[collID]
	if !ok {
		return pb.CollectionInfo{}, nil, fmt.Errorf("collection %s not found", collName)
	}

	rstIndex := make([]pb.IndexInfo, 0, len(collMeta.FieldIndexes))
	for _, idx := range collMeta.FieldIndexes {
		idxInfo, ok := mt.indexID2Meta[idx.IndexID]
		if !ok {
			return pb.CollectionInfo{}, nil, fmt.Errorf("index id = %d not found", idx.IndexID)
		}
		if indexName == "" || idxInfo.IndexName == indexName {
			rstIndex = append(rstIndex, idxInfo)
		}
	}
	return collMeta, rstIndex, nil
}

func (mt *metaTable) GetIndexByID(indexID typeutil.UniqueID) (*pb.IndexInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	indexInfo, ok := mt.indexID2Meta[indexID]
	if !ok {
		return nil, fmt.Errorf("cannot find index, id = %d", indexID)
	}
	return &indexInfo, nil
}

func (mt *metaTable) AddFlushedSegment(segID typeutil.UniqueID) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	_, ok := mt.flushedSegID[segID]
	if ok {
		return fmt.Errorf("segment id = %d exist", segID)
	}
	mt.flushedSegID[segID] = true
	return nil
}

// GetChanNameByVirtualChan return physical channel name corresponding the virtual channel
func (mt *metaTable) GetChanNameByVirtualChan(vname string) (string, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	chanName, ok := mt.vChan2Chan[vname]
	if !ok {
		return "", fmt.Errorf("cannot find virtual channel %s", vname)
	}
	return chanName, nil
}
