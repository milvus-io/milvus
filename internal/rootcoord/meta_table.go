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

package rootcoord

import (
	"fmt"
	"path"
	"strconv"
	"sync"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	// ComponentPrefix prefix for rootcoord component
	ComponentPrefix = "root-coord"

	// TenantMetaPrefix prefix for tenant meta
	TenantMetaPrefix = ComponentPrefix + "/tenant"

	// ProxyMetaPrefix prefix for proxy meta
	ProxyMetaPrefix = ComponentPrefix + "/proxy"

	// CollectionMetaPrefix prefix for collection meta
	CollectionMetaPrefix = ComponentPrefix + "/collection"

	// SegmentIndexMetaPrefix prefix for segment index meta
	SegmentIndexMetaPrefix = ComponentPrefix + "/segment-index"

	// IndexMetaPrefix prefix for index meta
	IndexMetaPrefix = ComponentPrefix + "/index"

	// CollectionAliasMetaPrefix prefix for collection alias meta
	CollectionAliasMetaPrefix = ComponentPrefix + "/collection-alias"

	// TimestampPrefix prefix for timestamp
	TimestampPrefix = ComponentPrefix + "/timestamp"

	// DDOperationPrefix prefix for DD operation
	DDOperationPrefix = ComponentPrefix + "/dd-operation"

	// DDMsgSendPrefix prefix to indicate whether DD msg has been send
	DDMsgSendPrefix = ComponentPrefix + "/dd-msg-send"

	// CreateCollectionDDType name of DD type for create collection
	CreateCollectionDDType = "CreateCollection"

	// DropCollectionDDType name of DD type for drop collection
	DropCollectionDDType = "DropCollection"

	// CreatePartitionDDType name of DD type for create partition
	CreatePartitionDDType = "CreatePartition"

	// DropPartitionDDType name of DD type for drop partition
	DropPartitionDDType = "DropPartition"
)

// MetaTable store all rootcoord meta info
type MetaTable struct {
	client          kv.SnapShotKV                                                   // client of a reliable kv service, i.e. etcd client
	tenantID2Meta   map[typeutil.UniqueID]pb.TenantMeta                             // tenant id to tenant meta
	proxyID2Meta    map[typeutil.UniqueID]pb.ProxyMeta                              // proxy id to proxy meta
	collID2Meta     map[typeutil.UniqueID]pb.CollectionInfo                         // collection_id -> meta
	collName2ID     map[string]typeutil.UniqueID                                    // collection name to collection id
	collAlias2ID    map[string]typeutil.UniqueID                                    // collection alias to collection id
	partID2SegID    map[typeutil.UniqueID]map[typeutil.UniqueID]bool                // partition_id -> segment_id -> bool
	segID2IndexMeta map[typeutil.UniqueID]map[typeutil.UniqueID]pb.SegmentIndexInfo // collection_id/index_id/partition_id/segment_id -> meta
	indexID2Meta    map[typeutil.UniqueID]pb.IndexInfo                              // collection_id/index_id -> meta

	tenantLock sync.RWMutex
	proxyLock  sync.RWMutex
	ddLock     sync.RWMutex
}

// NewMetaTable create meta table for rootcoord, which stores all in-memory information
// for collection, partion, segment, index etc.
func NewMetaTable(kv kv.SnapShotKV) (*MetaTable, error) {
	mt := &MetaTable{
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

func (mt *MetaTable) reloadFromKV() error {
	mt.tenantID2Meta = make(map[typeutil.UniqueID]pb.TenantMeta)
	mt.proxyID2Meta = make(map[typeutil.UniqueID]pb.ProxyMeta)
	mt.collID2Meta = make(map[typeutil.UniqueID]pb.CollectionInfo)
	mt.collName2ID = make(map[string]typeutil.UniqueID)
	mt.collAlias2ID = make(map[string]typeutil.UniqueID)
	mt.partID2SegID = make(map[typeutil.UniqueID]map[typeutil.UniqueID]bool)
	mt.segID2IndexMeta = make(map[typeutil.UniqueID]map[typeutil.UniqueID]pb.SegmentIndexInfo)
	mt.indexID2Meta = make(map[typeutil.UniqueID]pb.IndexInfo)

	_, values, err := mt.client.LoadWithPrefix(TenantMetaPrefix, 0)
	if err != nil {
		return err
	}

	for _, value := range values {
		tenantMeta := pb.TenantMeta{}
		err := proto.Unmarshal([]byte(value), &tenantMeta)
		if err != nil {
			return fmt.Errorf("RootCoord Unmarshal pb.TenantMeta err:%w", err)
		}
		mt.tenantID2Meta[tenantMeta.ID] = tenantMeta
	}

	_, values, err = mt.client.LoadWithPrefix(ProxyMetaPrefix, 0)
	if err != nil {
		return err
	}

	for _, value := range values {
		proxyMeta := pb.ProxyMeta{}
		err = proto.Unmarshal([]byte(value), &proxyMeta)
		if err != nil {
			return fmt.Errorf("RootCoord Unmarshal pb.ProxyMeta err:%w", err)
		}
		mt.proxyID2Meta[proxyMeta.ID] = proxyMeta
	}

	_, values, err = mt.client.LoadWithPrefix(CollectionMetaPrefix, 0)
	if err != nil {
		return err
	}

	for _, value := range values {
		collInfo := pb.CollectionInfo{}
		err = proto.Unmarshal([]byte(value), &collInfo)
		if err != nil {
			return fmt.Errorf("RootCoord Unmarshal pb.CollectionInfo err:%w", err)
		}
		mt.collID2Meta[collInfo.ID] = collInfo
		mt.collName2ID[collInfo.Schema.Name] = collInfo.ID
	}

	_, values, err = mt.client.LoadWithPrefix(SegmentIndexMetaPrefix, 0)
	if err != nil {
		return err
	}
	for _, value := range values {
		segmentIndexInfo := pb.SegmentIndexInfo{}
		err = proto.Unmarshal([]byte(value), &segmentIndexInfo)
		if err != nil {
			return fmt.Errorf("RootCoord Unmarshal pb.SegmentIndexInfo err:%w", err)
		}

		// update partID2SegID
		segIDMap, ok := mt.partID2SegID[segmentIndexInfo.PartitionID]
		if ok {
			segIDMap[segmentIndexInfo.SegmentID] = true
		} else {
			idMap := make(map[typeutil.UniqueID]bool)
			idMap[segmentIndexInfo.SegmentID] = true
			mt.partID2SegID[segmentIndexInfo.PartitionID] = idMap
		}

		// update segID2IndexMeta
		idx, ok := mt.segID2IndexMeta[segmentIndexInfo.SegmentID]
		if ok {
			idx[segmentIndexInfo.IndexID] = segmentIndexInfo
		} else {
			meta := make(map[typeutil.UniqueID]pb.SegmentIndexInfo)
			meta[segmentIndexInfo.IndexID] = segmentIndexInfo
			mt.segID2IndexMeta[segmentIndexInfo.SegmentID] = meta
		}
	}

	_, values, err = mt.client.LoadWithPrefix(IndexMetaPrefix, 0)
	if err != nil {
		return err
	}
	for _, value := range values {
		meta := pb.IndexInfo{}
		err = proto.Unmarshal([]byte(value), &meta)
		if err != nil {
			return fmt.Errorf("RootCoord Unmarshal pb.IndexInfo err:%w", err)
		}
		mt.indexID2Meta[meta.IndexID] = meta
	}

	_, values, err = mt.client.LoadWithPrefix(CollectionAliasMetaPrefix, 0)
	if err != nil {
		return err
	}
	for _, value := range values {
		aliasInfo := pb.CollectionInfo{}
		err = proto.Unmarshal([]byte(value), &aliasInfo)
		if err != nil {
			return fmt.Errorf("RootCoord Unmarshal pb.AliasInfo err:%w", err)
		}
		mt.collAlias2ID[aliasInfo.Schema.Name] = aliasInfo.ID
	}

	log.Debug("reload meta table from KV successfully")
	return nil
}

// AddTenant add tenant
func (mt *MetaTable) AddTenant(te *pb.TenantMeta, ts typeutil.Timestamp) error {
	mt.tenantLock.Lock()
	defer mt.tenantLock.Unlock()

	k := fmt.Sprintf("%s/%d", TenantMetaPrefix, te.ID)
	v, err := proto.Marshal(te)
	if err != nil {
		log.Error("AddTenant Marshal fail", zap.Error(err))
		return err
	}

	err = mt.client.Save(k, string(v), ts)
	if err != nil {
		log.Error("AddTenant Save fail", zap.Error(err))
		return err
	}
	mt.tenantID2Meta[te.ID] = *te
	return nil
}

// AddProxy add proxy
func (mt *MetaTable) AddProxy(po *pb.ProxyMeta, ts typeutil.Timestamp) error {
	mt.proxyLock.Lock()
	defer mt.proxyLock.Unlock()

	k := fmt.Sprintf("%s/%d", ProxyMetaPrefix, po.ID)
	v, err := proto.Marshal(po)
	if err != nil {
		log.Error("AddProxy Marshal fail", zap.Error(err))
		return err
	}

	err = mt.client.Save(k, string(v), ts)
	if err != nil {
		log.Error("SnapShotKV Save fail", zap.Error(err))
		panic("SnapShotKV Save fail")
	}
	mt.proxyID2Meta[po.ID] = *po
	return nil
}

// AddCollection add collection
func (mt *MetaTable) AddCollection(coll *pb.CollectionInfo, ts typeutil.Timestamp, idx []*pb.IndexInfo, ddOpStr string) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if len(coll.PartitionIDs) != len(coll.PartitionNames) ||
		len(coll.PartitionIDs) != len(coll.PartitionCreatedTimestamps) ||
		(len(coll.PartitionIDs) != 1 && len(coll.PartitionIDs) != 0) {
		return fmt.Errorf("PartitionIDs, PartitionNames and PartitionCreatedTimestmaps' length mis-match when creating collection")
	}
	if _, ok := mt.collName2ID[coll.Schema.Name]; ok {
		return fmt.Errorf("collection %s exist", coll.Schema.Name)
	}
	if len(coll.FieldIndexes) != len(idx) {
		return fmt.Errorf("incorrect index id when creating collection")
	}

	coll.CreateTime = ts
	if len(coll.PartitionCreatedTimestamps) == 1 {
		coll.PartitionCreatedTimestamps[0] = ts
	}
	mt.collID2Meta[coll.ID] = *coll
	mt.collName2ID[coll.Schema.Name] = coll.ID
	for _, i := range idx {
		mt.indexID2Meta[i.IndexID] = *i
	}

	k1 := fmt.Sprintf("%s/%d", CollectionMetaPrefix, coll.ID)
	v1, err := proto.Marshal(coll)
	if err != nil {
		log.Error("MetaTable AddCollection saveColl Marshal fail",
			zap.String("key", k1), zap.Error(err))
		return fmt.Errorf("MetaTable AddCollection Marshal fail key:%s, err:%w", k1, err)
	}
	meta := map[string]string{k1: string(v1)}

	for _, i := range idx {
		k := fmt.Sprintf("%s/%d/%d", IndexMetaPrefix, coll.ID, i.IndexID)
		v, err := proto.Marshal(i)
		if err != nil {
			log.Error("MetaTable AddCollection Marshal fail", zap.String("key", k),
				zap.String("IndexName", i.IndexName), zap.Error(err))
			return fmt.Errorf("MetaTable AddCollection Marshal fail key:%s, err:%w", k, err)
		}
		meta[k] = string(v)
	}

	// save ddOpStr into etcd
	meta[DDMsgSendPrefix] = "false"
	meta[DDOperationPrefix] = ddOpStr

	err = mt.client.MultiSave(meta, ts)
	if err != nil {
		log.Error("SnapShotKV MultiSave fail", zap.Error(err))
		panic("SnapShotKV MultiSave fail")
	}

	return nil
}

// DeleteCollection delete collection
func (mt *MetaTable) DeleteCollection(collID typeutil.UniqueID, ts typeutil.Timestamp, ddOpStr string) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	collMeta, ok := mt.collID2Meta[collID]
	if !ok {
		return fmt.Errorf("can't find collection. id = %d", collID)
	}

	delete(mt.collID2Meta, collID)
	delete(mt.collName2ID, collMeta.Schema.Name)

	// update segID2IndexMeta
	for partID := range collMeta.PartitionIDs {
		if segIDMap, ok := mt.partID2SegID[typeutil.UniqueID(partID)]; ok {
			for segID := range segIDMap {
				delete(mt.segID2IndexMeta, segID)
			}
		}
	}

	// update partID2SegID
	for partID := range collMeta.PartitionIDs {
		delete(mt.partID2SegID, typeutil.UniqueID(partID))
	}

	for _, idxInfo := range collMeta.FieldIndexes {
		_, ok := mt.indexID2Meta[idxInfo.IndexID]
		if !ok {
			log.Warn("index id not exist", zap.Int64("index id", idxInfo.IndexID))
			continue
		}
		delete(mt.indexID2Meta, idxInfo.IndexID)
	}
	var aliases []string
	// delete collection aliases
	for alias, cid := range mt.collAlias2ID {
		if cid == collID {
			aliases = append(aliases, alias)
		}
	}

	delMetakeys := []string{
		fmt.Sprintf("%s/%d", CollectionMetaPrefix, collID),
		fmt.Sprintf("%s/%d", SegmentIndexMetaPrefix, collID),
		fmt.Sprintf("%s/%d", IndexMetaPrefix, collID),
	}

	for _, alias := range aliases {
		delete(mt.collAlias2ID, alias)
		delMetakeys = append(delMetakeys,
			fmt.Sprintf("%s/%s", CollectionAliasMetaPrefix, alias),
		)
	}

	// save ddOpStr into etcd
	var saveMeta = map[string]string{
		DDMsgSendPrefix:   "false",
		DDOperationPrefix: ddOpStr,
	}

	err := mt.client.MultiSaveAndRemoveWithPrefix(saveMeta, delMetakeys, ts)
	if err != nil {
		log.Error("SnapShotKV MultiSaveAndRemoveWithPrefix fail", zap.Error(err))
		panic("SnapShotKV MultiSaveAndRemoveWithPrefix fail")
	}

	return nil
}

// HasCollection return collection existence
func (mt *MetaTable) HasCollection(collID typeutil.UniqueID, ts typeutil.Timestamp) bool {
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

// GetCollectionByID return collection meta by collection id
func (mt *MetaTable) GetCollectionByID(collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*pb.CollectionInfo, error) {
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
	err = proto.Unmarshal([]byte(val), &colMeta)
	if err != nil {
		return nil, err
	}
	return &colMeta, nil
}

// GetCollectionByName return collection meta by collection name
func (mt *MetaTable) GetCollectionByName(collectionName string, ts typeutil.Timestamp) (*pb.CollectionInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	if ts == 0 {
		vid, ok := mt.collName2ID[collectionName]
		if !ok {
			if vid, ok = mt.collAlias2ID[collectionName]; !ok {
				return nil, fmt.Errorf("can't find collection: " + collectionName)
			}
		}
		col, ok := mt.collID2Meta[vid]
		if !ok {
			return nil, fmt.Errorf("can't find collection %s with id %d", collectionName, vid)
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
		err = proto.Unmarshal([]byte(val), &collMeta)
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

// ListCollections list all collection names
func (mt *MetaTable) ListCollections(ts typeutil.Timestamp) (map[string]*pb.CollectionInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	colls := make(map[string]*pb.CollectionInfo)

	if ts == 0 {
		for collName, collID := range mt.collName2ID {
			coll := mt.collID2Meta[collID]
			colCopy := proto.Clone(&coll)
			colls[collName] = colCopy.(*pb.CollectionInfo)
		}
		return colls, nil
	}
	_, vals, err := mt.client.LoadWithPrefix(CollectionMetaPrefix, ts)
	if err != nil {
		log.Debug("load with prefix error", zap.Uint64("timestamp", ts), zap.Error(err))
		return nil, nil
	}
	for _, val := range vals {
		collMeta := pb.CollectionInfo{}
		err := proto.Unmarshal([]byte(val), &collMeta)
		if err != nil {
			log.Debug("unmarshal collection info failed", zap.Error(err))
		}
		colls[collMeta.Schema.Name] = &collMeta
	}
	return colls, nil
}

// ListAliases list all collection aliases
func (mt *MetaTable) ListAliases(collID typeutil.UniqueID) []string {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	var aliases []string
	for alias, cid := range mt.collAlias2ID {
		if cid == collID {
			aliases = append(aliases, alias)
		}
	}
	return aliases
}

// ListCollectionVirtualChannels list virtual channels of all collections
func (mt *MetaTable) ListCollectionVirtualChannels() []string {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	vlist := []string{}

	for _, c := range mt.collID2Meta {
		vlist = append(vlist, c.VirtualChannelNames...)
	}
	return vlist
}

// ListCollectionPhysicalChannels list physical channels of all collections
func (mt *MetaTable) ListCollectionPhysicalChannels() []string {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	plist := []string{}

	for _, c := range mt.collID2Meta {
		plist = append(plist, c.PhysicalChannelNames...)
	}
	return plist
}

// AddPartition add partition
func (mt *MetaTable) AddPartition(collID typeutil.UniqueID, partitionName string, partitionID typeutil.UniqueID, ts typeutil.Timestamp, ddOpStr string) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	coll, ok := mt.collID2Meta[collID]
	if !ok {
		return fmt.Errorf("can't find collection. id = %d", collID)
	}

	// number of partition tags (except _default) should be limited to 4096 by default
	if int64(len(coll.PartitionIDs)) >= Params.MaxPartitionNum {
		return fmt.Errorf("maximum partition's number should be limit to %d", Params.MaxPartitionNum)
	}

	if len(coll.PartitionIDs) != len(coll.PartitionNames) {
		return fmt.Errorf("len(coll.PartitionIDs)=%d, len(coll.PartitionNames)=%d", len(coll.PartitionIDs), len(coll.PartitionNames))
	}

	if len(coll.PartitionIDs) != len(coll.PartitionCreatedTimestamps) {
		return fmt.Errorf("len(coll.PartitionIDs)=%d, len(coll.PartitionCreatedTimestamps)=%d", len(coll.PartitionIDs), len(coll.PartitionCreatedTimestamps))
	}

	if len(coll.PartitionNames) != len(coll.PartitionCreatedTimestamps) {
		return fmt.Errorf("len(coll.PartitionNames)=%d, len(coll.PartitionCreatedTimestamps)=%d", len(coll.PartitionNames), len(coll.PartitionCreatedTimestamps))
	}

	for idx := range coll.PartitionIDs {
		if coll.PartitionIDs[idx] == partitionID {
			return fmt.Errorf("partition id = %d already exists", partitionID)
		}
		if coll.PartitionNames[idx] == partitionName {
			return fmt.Errorf("partition name = %s already exists", partitionName)
		}
		// no necessary to check created timestamp
	}

	coll.PartitionIDs = append(coll.PartitionIDs, partitionID)
	coll.PartitionNames = append(coll.PartitionNames, partitionName)
	coll.PartitionCreatedTimestamps = append(coll.PartitionCreatedTimestamps, ts)
	mt.collID2Meta[collID] = coll

	k1 := fmt.Sprintf("%s/%d", CollectionMetaPrefix, collID)
	v1, err := proto.Marshal(&coll)
	if err != nil {
		log.Error("MetaTable AddPartition saveColl Marshal fail",
			zap.String("key", k1), zap.Error(err))
		return fmt.Errorf("MetaTable AddPartition Marshal fail, k1:%s, err:%w", k1, err)
	}
	meta := map[string]string{k1: string(v1)}

	// save ddOpStr into etcd
	meta[DDMsgSendPrefix] = "false"
	meta[DDOperationPrefix] = ddOpStr

	err = mt.client.MultiSave(meta, ts)
	if err != nil {
		log.Error("SnapShotKV MultiSave fail", zap.Error(err))
		panic("SnapShotKV MultiSave fail")
	}
	return nil
}

// GetPartitionNameByID return partition name by partition id
func (mt *MetaTable) GetPartitionNameByID(collID, partitionID typeutil.UniqueID, ts typeutil.Timestamp) (string, error) {
	if ts == 0 {
		mt.ddLock.RLock()
		defer mt.ddLock.RUnlock()
		collMeta, ok := mt.collID2Meta[collID]
		if !ok {
			return "", fmt.Errorf("can't find collection id = %d", collID)
		}
		for idx := range collMeta.PartitionIDs {
			if collMeta.PartitionIDs[idx] == partitionID {
				return collMeta.PartitionNames[idx], nil
			}
		}
		return "", fmt.Errorf("partition %d does not exist", partitionID)
	}
	collKey := fmt.Sprintf("%s/%d", CollectionMetaPrefix, collID)
	collVal, err := mt.client.Load(collKey, ts)
	if err != nil {
		return "", err
	}
	collMeta := pb.CollectionInfo{}
	err = proto.Unmarshal([]byte(collVal), &collMeta)
	if err != nil {
		return "", err
	}
	for idx := range collMeta.PartitionIDs {
		if collMeta.PartitionIDs[idx] == partitionID {
			return collMeta.PartitionNames[idx], nil
		}
	}
	return "", fmt.Errorf("partition %d does not exist", partitionID)
}

func (mt *MetaTable) getPartitionByName(collID typeutil.UniqueID, partitionName string, ts typeutil.Timestamp) (typeutil.UniqueID, error) {
	if ts == 0 {
		collMeta, ok := mt.collID2Meta[collID]
		if !ok {
			return 0, fmt.Errorf("can't find collection id = %d", collID)
		}
		for idx := range collMeta.PartitionIDs {
			if collMeta.PartitionNames[idx] == partitionName {
				return collMeta.PartitionIDs[idx], nil
			}
		}
		return 0, fmt.Errorf("partition %s does not exist", partitionName)
	}
	collKey := fmt.Sprintf("%s/%d", CollectionMetaPrefix, collID)
	collVal, err := mt.client.Load(collKey, ts)
	if err != nil {
		return 0, err
	}
	collMeta := pb.CollectionInfo{}
	err = proto.Unmarshal([]byte(collVal), &collMeta)
	if err != nil {
		return 0, err
	}
	for idx := range collMeta.PartitionIDs {
		if collMeta.PartitionNames[idx] == partitionName {
			return collMeta.PartitionIDs[idx], nil
		}
	}
	return 0, fmt.Errorf("partition %s does not exist", partitionName)
}

// GetPartitionByName return partition id by partition name
func (mt *MetaTable) GetPartitionByName(collID typeutil.UniqueID, partitionName string, ts typeutil.Timestamp) (typeutil.UniqueID, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	return mt.getPartitionByName(collID, partitionName, ts)
}

// HasPartition check partition existence
func (mt *MetaTable) HasPartition(collID typeutil.UniqueID, partitionName string, ts typeutil.Timestamp) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	_, err := mt.getPartitionByName(collID, partitionName, ts)
	return err == nil
}

// DeletePartition delete partition
func (mt *MetaTable) DeletePartition(collID typeutil.UniqueID, partitionName string, ts typeutil.Timestamp, ddOpStr string) (typeutil.UniqueID, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if partitionName == Params.DefaultPartitionName {
		return 0, fmt.Errorf("default partition cannot be deleted")
	}

	collMeta, ok := mt.collID2Meta[collID]
	if !ok {
		return 0, fmt.Errorf("can't find collection id = %d", collID)
	}

	// check tag exists
	exist := false

	pd := make([]typeutil.UniqueID, 0, len(collMeta.PartitionIDs))
	pn := make([]string, 0, len(collMeta.PartitionNames))
	pts := make([]uint64, 0, len(collMeta.PartitionCreatedTimestamps))
	var partID typeutil.UniqueID
	for idx := range collMeta.PartitionIDs {
		if collMeta.PartitionNames[idx] == partitionName {
			partID = collMeta.PartitionIDs[idx]
			exist = true
		} else {
			pd = append(pd, collMeta.PartitionIDs[idx])
			pn = append(pn, collMeta.PartitionNames[idx])
			pts = append(pts, collMeta.PartitionCreatedTimestamps[idx])
		}
	}
	if !exist {
		return 0, fmt.Errorf("partition %s does not exist", partitionName)
	}
	collMeta.PartitionIDs = pd
	collMeta.PartitionNames = pn
	collMeta.PartitionCreatedTimestamps = pts
	mt.collID2Meta[collID] = collMeta

	// update segID2IndexMeta and partID2SegID
	if segIDMap, ok := mt.partID2SegID[partID]; ok {
		for segID := range segIDMap {
			delete(mt.segID2IndexMeta, segID)
		}
	}
	delete(mt.partID2SegID, partID)

	k := path.Join(CollectionMetaPrefix, strconv.FormatInt(collID, 10))
	v, err := proto.Marshal(&collMeta)
	if err != nil {
		log.Error("MetaTable DeletePartition Marshal collectionMeta fail",
			zap.String("key", k), zap.Error(err))
		return 0, fmt.Errorf("MetaTable DeletePartition Marshal collectionMeta fail key:%s, err:%w", k, err)
	}
	meta := map[string]string{k: string(v)}
	var delMetaKeys []string
	for _, idxInfo := range collMeta.FieldIndexes {
		k := fmt.Sprintf("%s/%d/%d/%d", SegmentIndexMetaPrefix, collMeta.ID, idxInfo.IndexID, partID)
		delMetaKeys = append(delMetaKeys, k)
	}

	// save ddOpStr into etcd
	meta[DDMsgSendPrefix] = "false"
	meta[DDOperationPrefix] = ddOpStr

	err = mt.client.MultiSaveAndRemoveWithPrefix(meta, delMetaKeys, ts)
	if err != nil {
		log.Error("SnapShotKV MultiSaveAndRemoveWithPrefix fail", zap.Error(err))
		panic("SnapShotKV MultiSaveAndRemoveWithPrefix fail")
	}
	return partID, nil
}

// AddIndex add index
func (mt *MetaTable) AddIndex(segIdxInfo *pb.SegmentIndexInfo, ts typeutil.Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	collMeta, ok := mt.collID2Meta[segIdxInfo.CollectionID]
	if !ok {
		return fmt.Errorf("collection id = %d not found", segIdxInfo.CollectionID)
	}
	exist := false
	for _, fidx := range collMeta.FieldIndexes {
		if fidx.IndexID == segIdxInfo.IndexID {
			exist = true
			break
		}
	}
	if !exist {
		return fmt.Errorf("index id = %d not found", segIdxInfo.IndexID)
	}

	segIdxMap, ok := mt.segID2IndexMeta[segIdxInfo.SegmentID]
	if !ok {
		idxMap := map[typeutil.UniqueID]pb.SegmentIndexInfo{segIdxInfo.IndexID: *segIdxInfo}
		mt.segID2IndexMeta[segIdxInfo.SegmentID] = idxMap

		segIDMap := map[typeutil.UniqueID]bool{segIdxInfo.SegmentID: true}
		mt.partID2SegID[segIdxInfo.PartitionID] = segIDMap
	} else {
		tmpInfo, ok := segIdxMap[segIdxInfo.IndexID]
		if ok {
			if SegmentIndexInfoEqual(segIdxInfo, &tmpInfo) {
				if segIdxInfo.BuildID == tmpInfo.BuildID {
					log.Debug("Identical SegmentIndexInfo already exist", zap.Int64("IndexID", segIdxInfo.IndexID))
					return nil
				}
				return fmt.Errorf("index id = %d exist", segIdxInfo.IndexID)
			}
		}
	}

	mt.segID2IndexMeta[segIdxInfo.SegmentID][segIdxInfo.IndexID] = *segIdxInfo
	mt.partID2SegID[segIdxInfo.PartitionID][segIdxInfo.SegmentID] = true

	k := fmt.Sprintf("%s/%d/%d/%d/%d", SegmentIndexMetaPrefix, segIdxInfo.CollectionID, segIdxInfo.IndexID, segIdxInfo.PartitionID, segIdxInfo.SegmentID)
	v, err := proto.Marshal(segIdxInfo)
	if err != nil {
		log.Error("MetaTable AddIndex Marshal segIdxInfo fail",
			zap.String("key", k), zap.Error(err))
		return fmt.Errorf("MetaTable AddIndex Marshal segIdxInfo fail key:%s, err:%w", k, err)
	}

	err = mt.client.Save(k, string(v), ts)
	if err != nil {
		log.Error("SnapShotKV Save fail", zap.Error(err))
		panic("SnapShotKV Save fail")
	}

	return nil
}

// DropIndex drop index
func (mt *MetaTable) DropIndex(collName, fieldName, indexName string, ts typeutil.Timestamp) (typeutil.UniqueID, bool, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	collID, ok := mt.collName2ID[collName]
	if !ok {
		collID, ok = mt.collAlias2ID[collName]
		if !ok {
			return 0, false, fmt.Errorf("collection name = %s not exist", collName)
		}
	}
	collMeta, ok := mt.collID2Meta[collID]
	if !ok {
		return 0, false, fmt.Errorf("collection name  = %s not has meta", collName)
	}
	fieldSch, err := mt.unlockGetFieldSchema(collName, fieldName)
	if err != nil {
		return 0, false, err
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
		return 0, false, nil
	}
	collMeta.FieldIndexes = fieldIdxInfo
	mt.collID2Meta[collID] = collMeta
	k := path.Join(CollectionMetaPrefix, strconv.FormatInt(collID, 10))
	v, err := proto.Marshal(&collMeta)
	if err != nil {
		log.Error("MetaTable DropIndex Marshal collMeta fail",
			zap.String("key", k), zap.Error(err))
		return 0, false, fmt.Errorf("MetaTable DropIndex Marshal collMeta fail key:%s, err:%w", k, err)
	}
	saveMeta := map[string]string{k: string(v)}

	delete(mt.indexID2Meta, dropIdxID)

	// update segID2IndexMeta
	for partID := range collMeta.PartitionIDs {
		if segIDMap, ok := mt.partID2SegID[typeutil.UniqueID(partID)]; ok {
			for segID := range segIDMap {
				if segIndexInfos, ok := mt.segID2IndexMeta[segID]; ok {
					delete(segIndexInfos, dropIdxID)
				}
			}
		}
	}

	delMeta := []string{
		fmt.Sprintf("%s/%d/%d", SegmentIndexMetaPrefix, collMeta.ID, dropIdxID),
		fmt.Sprintf("%s/%d/%d", IndexMetaPrefix, collMeta.ID, dropIdxID),
	}

	err = mt.client.MultiSaveAndRemoveWithPrefix(saveMeta, delMeta, ts)
	if err != nil {
		log.Error("SnapShotKV MultiSaveAndRemoveWithPrefix fail", zap.Error(err))
		panic("SnapShotKV MultiSaveAndRemoveWithPrefix fail")
	}

	return dropIdxID, true, nil
}

// GetSegmentIndexInfoByID return segment index info by segment id
func (mt *MetaTable) GetSegmentIndexInfoByID(segID typeutil.UniqueID, filedID int64, idxName string) (pb.SegmentIndexInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

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
	if len(segIdxMap) == 0 {
		return pb.SegmentIndexInfo{}, fmt.Errorf("segment id %d not has any index", segID)
	}

	if filedID == -1 && idxName == "" { // return default index
		for _, seg := range segIdxMap {
			info, ok := mt.indexID2Meta[seg.IndexID]
			if ok && info.IndexName == Params.DefaultIndexName {
				return seg, nil
			}
		}
	} else {
		for idxID, seg := range segIdxMap {
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

// GetFieldSchema return field schema
func (mt *MetaTable) GetFieldSchema(collName string, fieldName string) (schemapb.FieldSchema, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	return mt.unlockGetFieldSchema(collName, fieldName)
}

func (mt *MetaTable) unlockGetFieldSchema(collName string, fieldName string) (schemapb.FieldSchema, error) {
	collID, ok := mt.collName2ID[collName]
	if !ok {
		collID, ok = mt.collAlias2ID[collName]
		if !ok {
			return schemapb.FieldSchema{}, fmt.Errorf("collection %s not found", collName)
		}
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

// IsSegmentIndexed check if segment has index
func (mt *MetaTable) IsSegmentIndexed(segID typeutil.UniqueID, fieldSchema *schemapb.FieldSchema, indexParams []*commonpb.KeyValuePair) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	return mt.unlockIsSegmentIndexed(segID, fieldSchema, indexParams)
}

func (mt *MetaTable) unlockIsSegmentIndexed(segID typeutil.UniqueID, fieldSchema *schemapb.FieldSchema, indexParams []*commonpb.KeyValuePair) bool {
	segIdx, ok := mt.segID2IndexMeta[segID]
	if !ok {
		return false
	}
	exist := false
	for idxID, meta := range segIdx {
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

// GetNotIndexedSegments return segment ids which have no index
func (mt *MetaTable) GetNotIndexedSegments(collName string, fieldName string, idxInfo *pb.IndexInfo, segIDs []typeutil.UniqueID, ts typeutil.Timestamp) ([]typeutil.UniqueID, schemapb.FieldSchema, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if idxInfo.IndexParams == nil {
		return nil, schemapb.FieldSchema{}, fmt.Errorf("index param is nil")
	}
	collID, ok := mt.collName2ID[collName]
	if !ok {
		collID, ok = mt.collAlias2ID[collName]
		if !ok {
			return nil, schemapb.FieldSchema{}, fmt.Errorf("collection %s not found", collName)
		}
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
		v1, err := proto.Marshal(&collMeta)
		if err != nil {
			log.Error("MetaTable GetNotIndexedSegments Marshal collMeta fail",
				zap.String("key", k1), zap.Error(err))
			return nil, schemapb.FieldSchema{}, fmt.Errorf("MetaTable GetNotIndexedSegments Marshal collMeta fail key:%s, err:%w", k1, err)
		}

		mt.indexID2Meta[idx.IndexID] = *idxInfo
		k2 := path.Join(IndexMetaPrefix, strconv.FormatInt(idx.IndexID, 10))
		v2, err := proto.Marshal(idxInfo)
		if err != nil {
			log.Error("MetaTable GetNotIndexedSegments Marshal idxInfo fail",
				zap.String("key", k2), zap.Error(err))
			return nil, schemapb.FieldSchema{}, fmt.Errorf("MetaTable GetNotIndexedSegments Marshal idxInfo fail key:%s, err:%w", k2, err)
		}
		meta := map[string]string{k1: string(v1), k2: string(v2)}

		if dupIdx != 0 {
			dupInfo := mt.indexID2Meta[dupIdx]
			dupInfo.IndexName = dupInfo.IndexName + "_bak"
			mt.indexID2Meta[dupIdx] = dupInfo
			k := path.Join(IndexMetaPrefix, strconv.FormatInt(dupInfo.IndexID, 10))
			v, err := proto.Marshal(&dupInfo)
			if err != nil {
				log.Error("MetaTable GetNotIndexedSegments Marshal dupInfo fail",
					zap.String("key", k), zap.Error(err))
				return nil, schemapb.FieldSchema{}, fmt.Errorf("MetaTable GetNotIndexedSegments Marshal dupInfo fail key:%s, err:%w", k, err)
			}
			meta[k] = string(v)
		}
		err = mt.client.MultiSave(meta, ts)
		if err != nil {
			log.Error("SnapShotKV MultiSave fail", zap.Error(err))
			panic("SnapShotKV MultiSave fail")
		}
	} else {
		idxInfo.IndexID = existInfo.IndexID
		if existInfo.IndexName != idxInfo.IndexName { //replace index name
			existInfo.IndexName = idxInfo.IndexName
			mt.indexID2Meta[existInfo.IndexID] = existInfo
			k := path.Join(IndexMetaPrefix, strconv.FormatInt(existInfo.IndexID, 10))
			v, err := proto.Marshal(&existInfo)
			if err != nil {
				log.Error("MetaTable GetNotIndexedSegments Marshal existInfo fail",
					zap.String("key", k), zap.Error(err))
				return nil, schemapb.FieldSchema{}, fmt.Errorf("MetaTable GetNotIndexedSegments Marshal existInfo fail key:%s, err:%w", k, err)
			}
			meta := map[string]string{k: string(v)}
			if dupIdx != 0 {
				dupInfo := mt.indexID2Meta[dupIdx]
				dupInfo.IndexName = dupInfo.IndexName + "_bak"
				mt.indexID2Meta[dupIdx] = dupInfo
				k := path.Join(IndexMetaPrefix, strconv.FormatInt(dupInfo.IndexID, 10))
				v, err := proto.Marshal(&dupInfo)
				if err != nil {
					log.Error("MetaTable GetNotIndexedSegments Marshal dupInfo fail",
						zap.String("key", k), zap.Error(err))
					return nil, schemapb.FieldSchema{}, fmt.Errorf("MetaTable GetNotIndexedSegments Marshal dupInfo fail key:%s, err:%w", k, err)
				}
				meta[k] = string(v)
			}

			err = mt.client.MultiSave(meta, ts)
			if err != nil {
				log.Error("SnapShotKV MultiSave fail", zap.Error(err))
				panic("SnapShotKV MultiSave fail")
			}
		}
	}

	rstID := make([]typeutil.UniqueID, 0, 16)
	for _, segID := range segIDs {
		if exist := mt.unlockIsSegmentIndexed(segID, &fieldSchema, idxInfo.IndexParams); !exist {
			rstID = append(rstID, segID)
		}
	}
	return rstID, fieldSchema, nil
}

// GetIndexByName return index info by index name
func (mt *MetaTable) GetIndexByName(collName, indexName string) (pb.CollectionInfo, []pb.IndexInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	collID, ok := mt.collName2ID[collName]
	if !ok {
		collID, ok = mt.collAlias2ID[collName]
		if !ok {
			return pb.CollectionInfo{}, nil, fmt.Errorf("collection %s not found", collName)
		}
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

// GetIndexByID return index info by index id
func (mt *MetaTable) GetIndexByID(indexID typeutil.UniqueID) (*pb.IndexInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	indexInfo, ok := mt.indexID2Meta[indexID]
	if !ok {
		return nil, fmt.Errorf("cannot find index, id = %d", indexID)
	}
	return &indexInfo, nil
}

func (mt *MetaTable) dupMeta() (
	map[typeutil.UniqueID]pb.CollectionInfo,
	map[typeutil.UniqueID]map[typeutil.UniqueID]pb.SegmentIndexInfo,
	map[typeutil.UniqueID]pb.IndexInfo,
) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	collID2Meta := map[typeutil.UniqueID]pb.CollectionInfo{}
	segID2IndexMeta := map[typeutil.UniqueID]map[typeutil.UniqueID]pb.SegmentIndexInfo{}
	indexID2Meta := map[typeutil.UniqueID]pb.IndexInfo{}
	for k, v := range mt.collID2Meta {
		collID2Meta[k] = v
	}
	for k, v := range mt.segID2IndexMeta {
		segID2IndexMeta[k] = map[typeutil.UniqueID]pb.SegmentIndexInfo{}
		for k2, v2 := range v {
			segID2IndexMeta[k][k2] = v2
		}
	}
	for k, v := range mt.indexID2Meta {
		indexID2Meta[k] = v
	}
	return collID2Meta, segID2IndexMeta, indexID2Meta
}

// AddAlias add collection alias
func (mt *MetaTable) AddAlias(collectionAlias string, collectionName string, ts typeutil.Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	if _, ok := mt.collAlias2ID[collectionAlias]; ok {
		return fmt.Errorf("duplicate collection alias, alias = %s", collectionAlias)
	}

	if _, ok := mt.collName2ID[collectionAlias]; ok {
		return fmt.Errorf("collection alias collides with existing collection name. collection = %s, alias = %s", collectionAlias, collectionAlias)
	}

	id, ok := mt.collName2ID[collectionName]
	if !ok {
		return fmt.Errorf("aliased collection name does not exist, name = %s", collectionName)
	}
	mt.collAlias2ID[collectionAlias] = id

	k := fmt.Sprintf("%s/%s", CollectionAliasMetaPrefix, collectionAlias)
	v, err := proto.Marshal(&pb.CollectionInfo{ID: id, Schema: &schemapb.CollectionSchema{Name: collectionAlias}})
	if err != nil {
		log.Error("MetaTable AddAlias Marshal CollectionInfo fail",
			zap.String("key", k), zap.Error(err))
		return fmt.Errorf("MetaTable AddAlias Marshal CollectionInfo fail key:%s, err:%w", k, err)
	}

	err = mt.client.Save(k, string(v), ts)
	if err != nil {
		log.Error("SnapShotKV Save fail", zap.Error(err))
		panic("SnapShotKV Save fail")
	}
	return nil
}

// DropAlias drop collection alias
func (mt *MetaTable) DropAlias(collectionAlias string, ts typeutil.Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	if _, ok := mt.collAlias2ID[collectionAlias]; !ok {
		return fmt.Errorf("alias does not exist, alias = %s", collectionAlias)
	}
	delete(mt.collAlias2ID, collectionAlias)

	delMetakeys := []string{
		fmt.Sprintf("%s/%s", CollectionAliasMetaPrefix, collectionAlias),
	}
	meta := make(map[string]string)
	err := mt.client.MultiSaveAndRemoveWithPrefix(meta, delMetakeys, ts)
	if err != nil {
		log.Error("SnapShotKV MultiSaveAndRemoveWithPrefix fail", zap.Error(err))
		panic("SnapShotKV MultiSaveAndRemoveWithPrefix fail")
	}
	return nil
}

// AlterAlias alter collection alias
func (mt *MetaTable) AlterAlias(collectionAlias string, collectionName string, ts typeutil.Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	if _, ok := mt.collAlias2ID[collectionAlias]; !ok {
		return fmt.Errorf("alias does not exist, alias = %s", collectionAlias)
	}

	id, ok := mt.collName2ID[collectionName]
	if !ok {
		return fmt.Errorf("aliased collection name does not exist, name = %s", collectionName)
	}
	mt.collAlias2ID[collectionAlias] = id

	k := fmt.Sprintf("%s/%s", CollectionAliasMetaPrefix, collectionAlias)
	v, err := proto.Marshal(&pb.CollectionInfo{ID: id, Schema: &schemapb.CollectionSchema{Name: collectionAlias}})
	if err != nil {
		log.Error("MetaTable AlterAlias Marshal CollectionInfo fail",
			zap.String("key", k), zap.Error(err))
		return fmt.Errorf("MetaTable AlterAlias Marshal CollectionInfo fail key:%s, err:%w", k, err)
	}

	err = mt.client.Save(k, string(v), ts)
	if err != nil {
		log.Error("SnapShotKV Save fail", zap.Error(err))
		panic("SnapShotKV Save fail")
	}
	return nil
}

// IsAlias returns true if specific `collectionAlias` is an alias of collection.
func (mt *MetaTable) IsAlias(collectionAlias string) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	_, ok := mt.collAlias2ID[collectionAlias]
	return ok
}
