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

package rootcoord

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"sync"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	// ComponentPrefix prefix for rootcoord component
	ComponentPrefix = "root-coord"

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

	// UserSubPrefix subpath for credential user
	UserSubPrefix = "/credential/users"

	// CredentialPrefix prefix for credential user
	CredentialPrefix = ComponentPrefix + UserSubPrefix

	// DefaultIndexType name of default index type for scalar field
	DefaultIndexType = "STL_SORT"

	// DefaultStringIndexType name of default index type for varChar/string field
	DefaultStringIndexType = "Trie"
)

// MetaTable store all rootCoord meta info
type MetaTable struct {
	txn             kv.TxnKV                                                        // client of a reliable txnkv service, i.e. etcd client
	snapshot        kv.SnapShotKV                                                   // client of a reliable snapshotkv service, i.e. etcd client
	collID2Meta     map[typeutil.UniqueID]pb.CollectionInfo                         // collection id -> collection meta
	collName2ID     map[string]typeutil.UniqueID                                    // collection name to collection id
	collAlias2ID    map[string]typeutil.UniqueID                                    // collection alias to collection id
	partID2SegID    map[typeutil.UniqueID]map[typeutil.UniqueID]bool                // partition id -> segment_id -> bool
	segID2IndexMeta map[typeutil.UniqueID]map[typeutil.UniqueID]pb.SegmentIndexInfo // collection id/index_id/partition_id/segment_id -> meta
	indexID2Meta    map[typeutil.UniqueID]pb.IndexInfo                              // collection id/index_id -> meta

	partitionNum int

	ddLock   sync.RWMutex
	credLock sync.RWMutex
}

// NewMetaTable creates meta table for rootcoord, which stores all in-memory information
// for collection, partition, segment, index etc.
func NewMetaTable(txn kv.TxnKV, snap kv.SnapShotKV) (*MetaTable, error) {
	mt := &MetaTable{
		txn:      txn,
		snapshot: snap,
		ddLock:   sync.RWMutex{},
		credLock: sync.RWMutex{},
	}
	err := mt.reloadFromKV()
	if err != nil {
		return nil, err
	}
	return mt, nil
}

func (mt *MetaTable) reloadFromKV() error {
	mt.collID2Meta = make(map[typeutil.UniqueID]pb.CollectionInfo)
	mt.collName2ID = make(map[string]typeutil.UniqueID)
	mt.collAlias2ID = make(map[string]typeutil.UniqueID)
	mt.partID2SegID = make(map[typeutil.UniqueID]map[typeutil.UniqueID]bool)
	mt.segID2IndexMeta = make(map[typeutil.UniqueID]map[typeutil.UniqueID]pb.SegmentIndexInfo)
	mt.indexID2Meta = make(map[typeutil.UniqueID]pb.IndexInfo)
	mt.partitionNum = 0

	_, values, err := mt.snapshot.LoadWithPrefix(CollectionAliasMetaPrefix, 0)
	if err != nil {
		return err
	}
	for _, value := range values {
		aliasInfo := pb.CollectionInfo{}
		err = proto.Unmarshal([]byte(value), &aliasInfo)
		if err != nil {
			return fmt.Errorf("rootcoord Unmarshal pb.AliasInfo err:%w", err)
		}
		mt.collAlias2ID[aliasInfo.Schema.Name] = aliasInfo.ID
	}

	_, values, err = mt.snapshot.LoadWithPrefix(CollectionMetaPrefix, 0)
	if err != nil {
		return err
	}

	for _, value := range values {
		collInfo := pb.CollectionInfo{}
		err = proto.Unmarshal([]byte(value), &collInfo)
		if err != nil {
			return fmt.Errorf("rootcoord Unmarshal pb.CollectionInfo err:%w", err)
		}
		if _, ok := mt.collAlias2ID[collInfo.Schema.Name]; ok {
			continue
		}
		mt.collID2Meta[collInfo.ID] = collInfo
		mt.collName2ID[collInfo.Schema.Name] = collInfo.ID
		mt.partitionNum += len(collInfo.PartitionIDs)
	}

	_, values, err = mt.txn.LoadWithPrefix(SegmentIndexMetaPrefix)
	if err != nil {
		return err
	}
	for _, value := range values {
		if bytes.Equal([]byte(value), suffixSnapshotTombstone) {
			// backward compatibility, IndexMeta used to be in SnapshotKV
			continue
		}
		segmentIndexInfo := pb.SegmentIndexInfo{}
		err = proto.Unmarshal([]byte(value), &segmentIndexInfo)
		if err != nil {
			return fmt.Errorf("rootcoord Unmarshal pb.SegmentIndexInfo err:%w", err)
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

	_, values, err = mt.txn.LoadWithPrefix(IndexMetaPrefix)
	if err != nil {
		return err
	}
	for _, value := range values {
		if bytes.Equal([]byte(value), suffixSnapshotTombstone) {
			// backward compatibility, IndexMeta used to be in SnapshotKV
			continue
		}
		meta := pb.IndexInfo{}
		err = proto.Unmarshal([]byte(value), &meta)
		if err != nil {
			return fmt.Errorf("rootcoord Unmarshal pb.IndexInfo err:%w", err)
		}
		mt.indexID2Meta[meta.IndexID] = meta
	}

	metrics.RootCoordNumOfCollections.Set(float64(len(mt.collID2Meta)))
	metrics.RootCoordNumOfPartitions.WithLabelValues().Set(float64(mt.partitionNum))
	log.Debug("reload meta table from KV successfully")
	return nil
}

// AddCollection add collection
func (mt *MetaTable) AddCollection(coll *pb.CollectionInfo, ts typeutil.Timestamp, idx []*pb.IndexInfo, ddOpStr string) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if len(coll.PartitionIDs) != len(coll.PartitionNames) ||
		len(coll.PartitionIDs) != len(coll.PartitionCreatedTimestamps) ||
		(len(coll.PartitionIDs) != 1 && len(coll.PartitionIDs) != 0) {
		return fmt.Errorf("partition parameters' length mis-match when creating collection")
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
	metrics.RootCoordNumOfCollections.Set(float64(len(mt.collID2Meta)))
	mt.collName2ID[coll.Schema.Name] = coll.ID
	for _, i := range idx {
		mt.indexID2Meta[i.IndexID] = *i
	}

	k1 := fmt.Sprintf("%s/%d", CollectionMetaPrefix, coll.ID)
	v1, err := proto.Marshal(coll)
	if err != nil {
		log.Error("MetaTable AddCollection saveColl Marshal fail",
			zap.String("key", k1), zap.Error(err))
		return fmt.Errorf("metaTable AddCollection Marshal fail key:%s, err:%w", k1, err)
	}
	meta := map[string]string{k1: string(v1)}

	for _, i := range idx {
		k := fmt.Sprintf("%s/%d/%d", IndexMetaPrefix, coll.ID, i.IndexID)
		v, err := proto.Marshal(i)
		if err != nil {
			log.Error("MetaTable AddCollection Marshal fail", zap.String("key", k),
				zap.String("IndexName", i.IndexName), zap.Error(err))
			return fmt.Errorf("metaTable AddCollection Marshal fail key:%s, err:%w", k, err)
		}
		meta[k] = string(v)
	}

	// save ddOpStr into etcd
	meta[DDMsgSendPrefix] = "false"
	meta[DDOperationPrefix] = ddOpStr

	err = mt.snapshot.MultiSave(meta, ts)
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

	metrics.RootCoordNumOfCollections.Set(float64(len(mt.collID2Meta)))

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

	delMetakeysSnap := []string{
		fmt.Sprintf("%s/%d", CollectionMetaPrefix, collID),
	}
	delMetaKeysTxn := []string{
		fmt.Sprintf("%s/%d", SegmentIndexMetaPrefix, collID),
		fmt.Sprintf("%s/%d", IndexMetaPrefix, collID),
	}

	for _, alias := range aliases {
		delete(mt.collAlias2ID, alias)
		delMetakeysSnap = append(delMetakeysSnap,
			fmt.Sprintf("%s/%s", CollectionAliasMetaPrefix, alias),
		)
	}

	// save ddOpStr into etcd
	var saveMeta = map[string]string{
		DDMsgSendPrefix:   "false",
		DDOperationPrefix: ddOpStr,
	}

	err := mt.snapshot.MultiSaveAndRemoveWithPrefix(map[string]string{}, delMetakeysSnap, ts)
	if err != nil {
		log.Error("SnapShotKV MultiSaveAndRemoveWithPrefix fail", zap.Error(err))
		panic("SnapShotKV MultiSaveAndRemoveWithPrefix fail")
	}
	err = mt.txn.MultiSaveAndRemoveWithPrefix(saveMeta, delMetaKeysTxn)
	if err != nil {
		log.Warn("TxnKV MultiSaveAndRemoveWithPrefix fail", zap.Error(err))
		//Txn kv fail will no panic here, treated as garbage
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
	_, err := mt.snapshot.Load(key, ts)
	return err == nil
}

// GetCollectionIDByName returns the collection ID according to its name.
// Returns an error if no matching ID is found.
func (mt *MetaTable) GetCollectionIDByName(cName string) (typeutil.UniqueID, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	var cID UniqueID
	var ok bool
	if cID, ok = mt.collName2ID[cName]; !ok {
		return 0, fmt.Errorf("collection ID not found for collection name %s", cName)
	}
	return cID, nil
}

// GetCollectionNameByID returns the collection name according to its ID.
// Returns an error if no matching name is found.
func (mt *MetaTable) GetCollectionNameByID(collectionID typeutil.UniqueID) (string, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	col, ok := mt.collID2Meta[collectionID]
	if !ok {
		return "", fmt.Errorf("can't find collection id : %d", collectionID)
	}
	return col.Schema.Name, nil
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
	val, err := mt.snapshot.Load(key, ts)
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
	_, vals, err := mt.snapshot.LoadWithPrefix(CollectionMetaPrefix, ts)
	if err != nil {
		log.Warn("failed to load table from meta snapshot", zap.Error(err))
		return nil, err
	}
	for _, val := range vals {
		collMeta := pb.CollectionInfo{}
		err = proto.Unmarshal([]byte(val), &collMeta)
		if err != nil {
			log.Warn("unmarshal collection info failed", zap.Error(err))
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
	_, vals, err := mt.snapshot.LoadWithPrefix(CollectionMetaPrefix, ts)
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
func (mt *MetaTable) ListCollectionVirtualChannels() map[typeutil.UniqueID][]string {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	chanMap := make(map[typeutil.UniqueID][]string)

	for id, collInfo := range mt.collID2Meta {
		chanMap[id] = collInfo.VirtualChannelNames
	}
	return chanMap
}

// ListCollectionPhysicalChannels list physical channels of all collections
func (mt *MetaTable) ListCollectionPhysicalChannels() map[typeutil.UniqueID][]string {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	chanMap := make(map[typeutil.UniqueID][]string)

	for id, collInfo := range mt.collID2Meta {
		chanMap[id] = collInfo.PhysicalChannelNames
	}
	return chanMap
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
	if int64(len(coll.PartitionIDs)) >= Params.RootCoordCfg.MaxPartitionNum {
		return fmt.Errorf("maximum partition's number should be limit to %d", Params.RootCoordCfg.MaxPartitionNum)
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
	mt.partitionNum++
	metrics.RootCoordNumOfPartitions.WithLabelValues().Set(float64(mt.partitionNum))

	k1 := fmt.Sprintf("%s/%d", CollectionMetaPrefix, collID)
	v1, err := proto.Marshal(&coll)
	if err != nil {
		log.Error("MetaTable AddPartition saveColl Marshal fail",
			zap.String("key", k1), zap.Error(err))
		return fmt.Errorf("metaTable AddPartition Marshal fail, k1:%s, err:%w", k1, err)
	}
	meta := map[string]string{k1: string(v1)}
	metaTxn := map[string]string{}
	// save ddOpStr into etcd
	metaTxn[DDMsgSendPrefix] = "false"
	metaTxn[DDOperationPrefix] = ddOpStr

	err = mt.snapshot.MultiSave(meta, ts)
	if err != nil {
		log.Error("SnapShotKV MultiSave fail", zap.Error(err))
		panic("SnapShotKV MultiSave fail")
	}
	err = mt.txn.MultiSave(metaTxn)
	if err != nil {
		// will not panic, missing create msg
		log.Warn("TxnKV MultiSave fail", zap.Error(err))
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
	collVal, err := mt.snapshot.Load(collKey, ts)
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
	collVal, err := mt.snapshot.Load(collKey, ts)
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

	if partitionName == Params.CommonCfg.DefaultPartitionName {
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
	mt.partitionNum--
	metrics.RootCoordNumOfPartitions.WithLabelValues().Set(float64(mt.partitionNum))
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
		return 0, fmt.Errorf("metaTable DeletePartition Marshal collectionMeta fail key:%s, err:%w", k, err)
	}
	var delMetaKeys []string
	for _, idxInfo := range collMeta.FieldIndexes {
		k := fmt.Sprintf("%s/%d/%d/%d", SegmentIndexMetaPrefix, collMeta.ID, idxInfo.IndexID, partID)
		delMetaKeys = append(delMetaKeys, k)
	}

	metaTxn := make(map[string]string)
	// save ddOpStr into etcd
	metaTxn[DDMsgSendPrefix] = "false"
	metaTxn[DDOperationPrefix] = ddOpStr

	err = mt.snapshot.Save(k, string(v), ts)
	if err != nil {
		log.Error("SnapShotKV MultiSaveAndRemoveWithPrefix fail", zap.Error(err))
		panic("SnapShotKV MultiSaveAndRemoveWithPrefix fail")
	}
	err = mt.txn.MultiSaveAndRemoveWithPrefix(metaTxn, delMetaKeys)
	if err != nil {
		log.Warn("TxnKV MultiSaveAndRemoveWithPrefix fail", zap.Error(err))
		// will not panic, failed txn shall be treated by garbage related logic
	}

	return partID, nil
}

// AddIndex add index; before calling this function, you must check whether indexing is required
func (mt *MetaTable) AddIndex(segIdxInfo *pb.SegmentIndexInfo) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	log.Info("adding segment index info", zap.Any("segment index info", segIdxInfo))
	if idxInfo, ok := mt.indexID2Meta[segIdxInfo.IndexID]; !ok || idxInfo.GetDeleted() {
		log.Error("index id not found or has been deleted", zap.Int64("indexID", segIdxInfo.IndexID))
		return fmt.Errorf("index id = %d not found", segIdxInfo.IndexID)
	}

	if _, ok := mt.partID2SegID[segIdxInfo.PartitionID]; !ok {
		segIDMap := map[typeutil.UniqueID]bool{segIdxInfo.SegmentID: true}
		mt.partID2SegID[segIdxInfo.PartitionID] = segIDMap
	}

	segIdxMap, ok := mt.segID2IndexMeta[segIdxInfo.SegmentID]
	if !ok {
		idxMap := map[typeutil.UniqueID]pb.SegmentIndexInfo{segIdxInfo.IndexID: *segIdxInfo}
		mt.segID2IndexMeta[segIdxInfo.SegmentID] = idxMap
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
		return fmt.Errorf("metaTable AddIndex Marshal segIdxInfo fail key:%s, err:%w", k, err)
	}

	err = mt.txn.Save(k, string(v))
	if err != nil {
		log.Error("SnapShotKV Save fail", zap.Error(err))
		panic("SnapShotKV Save fail")
	}

	return nil
}

func (mt *MetaTable) MarkIndexDeleted(collName, fieldName, indexName string) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	collMeta, err := mt.getCollectionInfoInternal(collName)
	if err != nil {
		log.Error("get collection meta failed", zap.String("collName", collName), zap.Error(err))
		return fmt.Errorf("collection name  = %s not has meta", collName)
	}
	fieldSch, err := mt.getFieldSchemaInternal(collName, fieldName)
	if err != nil {
		return err
	}

	var clonedIndex *pb.IndexInfo
	var dropIdxID typeutil.UniqueID
	for _, info := range collMeta.FieldIndexes {
		if info.FiledID != fieldSch.FieldID {
			continue
		}
		idxMeta, ok := mt.indexID2Meta[info.IndexID]
		if !ok {
			errMsg := fmt.Errorf("index not has meta with ID = %d", info.IndexID)
			log.Error("index id not has meta", zap.Int64("index id", info.IndexID))
			return errMsg
		}
		if idxMeta.GetDeleted() {
			continue
		}
		if idxMeta.IndexName != indexName {
			continue
		}
		dropIdxID = info.IndexID
		clonedIndex = proto.Clone(&idxMeta).(*pb.IndexInfo)
		clonedIndex.Deleted = true
	}

	if dropIdxID == 0 {
		log.Warn("index not found", zap.String("collName", collName), zap.String("fieldName", fieldName),
			zap.String("indexName", indexName))
		return nil
	}
	log.Info("MarkIndexDeleted", zap.String("collName", collName), zap.String("fieldName", fieldName),
		zap.String("indexName", indexName), zap.Int64("dropIndexID", dropIdxID))

	k := fmt.Sprintf("%s/%d/%d", IndexMetaPrefix, collMeta.ID, dropIdxID)
	v, err := proto.Marshal(clonedIndex)
	if err != nil {
		log.Error("MetaTable MarkIndexDeleted Marshal idxInfo fail",
			zap.String("key", k), zap.Error(err))
		return err
	}

	err = mt.txn.Save(k, string(v))
	if err != nil {
		log.Error("MetaTable MarkIndexDeleted txn MultiSave failed", zap.Error(err))
		return err
	}

	// update meta cache
	mt.indexID2Meta[dropIdxID] = *clonedIndex
	return nil
}

// DropIndex drop index
// Deprecated, only ut are used.
func (mt *MetaTable) DropIndex(collName, fieldName, indexName string) (typeutil.UniqueID, bool, error) {
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
	fieldSch, err := mt.getFieldSchemaInternal(collName, fieldName)
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
		if !ok || idxMeta.GetDeleted() || idxMeta.IndexName != indexName {
			fieldIdxInfo = append(fieldIdxInfo, info)
			log.Warn("index id not has meta", zap.Int64("index id", info.IndexID))
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
		return 0, false, fmt.Errorf("metaTable DropIndex Marshal collMeta fail key:%s, err:%w", k, err)
	}
	saveMeta := map[string]string{k: string(v)}

	delete(mt.indexID2Meta, dropIdxID)

	// update segID2IndexMeta
	for _, partID := range collMeta.PartitionIDs {
		if segIDMap, ok := mt.partID2SegID[partID]; ok {
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

	err = mt.txn.MultiSaveAndRemoveWithPrefix(saveMeta, delMeta)
	if err != nil {
		log.Error("TxnKV MultiSaveAndRemoveWithPrefix fail", zap.Error(err))
		panic("TxnKV MultiSaveAndRemoveWithPrefix fail")
	}

	return dropIdxID, true, nil
}

func (mt *MetaTable) GetInitBuildIDs(collName, indexName string) ([]UniqueID, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	collMeta, err := mt.getCollectionInfoInternal(collName)
	if err != nil {
		return nil, err
	}

	var indexID typeutil.UniqueID
	var indexIDCreateTS uint64
	for _, info := range collMeta.FieldIndexes {
		idxMeta, ok := mt.indexID2Meta[info.IndexID]
		if ok && idxMeta.IndexName == indexName {
			indexID = info.IndexID
			indexIDCreateTS = idxMeta.CreateTime
			break
		}
	}

	if indexID == 0 {
		log.Warn("get init buildIDs, index not found", zap.String("collection name", collName),
			zap.String("index name", indexName))
		return nil, fmt.Errorf("index not found with name = %s in collection %s", indexName, collName)
	}

	initBuildIDs := make([]UniqueID, 0)
	for _, indexID2Info := range mt.segID2IndexMeta {
		segIndexInfo, ok := indexID2Info[indexID]
		if ok && segIndexInfo.EnableIndex && segIndexInfo.CreateTime <= indexIDCreateTS {
			initBuildIDs = append(initBuildIDs, segIndexInfo.BuildID)
		}
	}
	return initBuildIDs, nil
}

func (mt *MetaTable) GetBuildIDsBySegIDs(segIDs []UniqueID) []UniqueID {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	buildIDs := make([]UniqueID, 0)
	for _, segID := range segIDs {
		if idxID2segIdx, ok := mt.segID2IndexMeta[segID]; ok {
			for _, segIndex := range idxID2segIdx {
				buildIDs = append(buildIDs, segIndex.BuildID)
			}
		}
	}
	return buildIDs
}

func (mt *MetaTable) AlignSegmentsMeta(collID, partID UniqueID, segIDs map[UniqueID]struct{}) ([]UniqueID, []UniqueID) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	allIndexID := make([]UniqueID, 0)
	if collMeta, ok := mt.collID2Meta[collID]; ok {
		for _, fieldIndex := range collMeta.FieldIndexes {
			allIndexID = append(allIndexID, fieldIndex.IndexID)
		}
	}

	recycledSegIDs := make([]UniqueID, 0)
	recycledBuildIDs := make([]UniqueID, 0)
	if segMap, ok := mt.partID2SegID[partID]; ok {
		for segID := range segMap {
			if _, ok := segIDs[segID]; !ok {
				recycledSegIDs = append(recycledSegIDs, segID)
				if idxID2segIndex, ok := mt.segID2IndexMeta[segID]; ok {
					for _, segIndex := range idxID2segIndex {
						recycledBuildIDs = append(recycledBuildIDs, segIndex.BuildID)
					}
				}
			}
		}
	}

	return recycledSegIDs, recycledBuildIDs
}

func (mt *MetaTable) RemoveSegments(collID, partID UniqueID, segIDs []UniqueID) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	log.Info("RootCoord MetaTable remove segments", zap.Int64("collID", collID), zap.Int64("partID", partID),
		zap.Int64s("segIDs", segIDs))
	allIndexID := make([]UniqueID, 0)
	if collMeta, ok := mt.collID2Meta[collID]; ok {
		for _, fieldIndex := range collMeta.FieldIndexes {
			allIndexID = append(allIndexID, fieldIndex.IndexID)
		}
	}

	for _, segID := range segIDs {
		delMeta := make([]string, 0)
		for _, indexID := range allIndexID {
			delMeta = append(delMeta, fmt.Sprintf("%s/%d/%d/%d/%d", SegmentIndexMetaPrefix, collID, indexID, partID, segID))
		}
		if err := mt.txn.MultiRemove(delMeta); err != nil {
			log.Error("remove redundant segment failed, wait to retry", zap.Int64("collID", collID), zap.Int64("part", partID),
				zap.Int64("segID", segID), zap.Error(err))
			return err
		}
		delete(mt.partID2SegID[partID], segID)
		delete(mt.segID2IndexMeta, segID)
	}

	return nil
}

func (mt *MetaTable) GetDroppedIndex() map[UniqueID][]*pb.FieldIndexInfo {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	droppedIndex := make(map[UniqueID][]*pb.FieldIndexInfo)
	for collID, meta := range mt.collID2Meta {
		for _, fieldIndex := range meta.FieldIndexes {
			if indexMeta, ok := mt.indexID2Meta[fieldIndex.IndexID]; ok && indexMeta.Deleted {
				droppedIndex[collID] = append(droppedIndex[collID], proto.Clone(fieldIndex).(*pb.FieldIndexInfo))
			}
		}
	}
	return droppedIndex
}

// RecycleDroppedIndex remove the meta about index which is deleted.
func (mt *MetaTable) RecycleDroppedIndex() error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	for collID, collMeta := range mt.collID2Meta {
		meta := collMeta
		fieldIndexes := make([]*pb.FieldIndexInfo, 0)
		delMeta := make([]string, 0)
		deletedIndexIDs := make(map[UniqueID]struct{})
		for _, fieldIndex := range meta.FieldIndexes {
			// Prevent the number of transaction operations from exceeding the etcd limit (128), where a maximum of 100 are processed each time
			if len(delMeta) >= 100 {
				break
			}
			if idxInfo, ok := mt.indexID2Meta[fieldIndex.IndexID]; !ok || idxInfo.GetDeleted() {
				deletedIndexIDs[fieldIndex.IndexID] = struct{}{}
				delMeta = append(delMeta, fmt.Sprintf("%s/%d/%d", IndexMetaPrefix, collID, fieldIndex.IndexID))
				delMeta = append(delMeta, fmt.Sprintf("%s/%d/%d", SegmentIndexMetaPrefix, collID, fieldIndex.IndexID))
				continue
			}
			fieldIndexes = append(fieldIndexes, fieldIndex)
		}
		// node index is deleted
		if len(fieldIndexes) == len(meta.FieldIndexes) {
			continue
		}
		clonedCollMeta := proto.Clone(&meta).(*pb.CollectionInfo)
		clonedCollMeta.FieldIndexes = fieldIndexes

		saveMeta := make(map[string]string)
		k := path.Join(CollectionMetaPrefix, strconv.FormatInt(collID, 10))
		v, err := proto.Marshal(clonedCollMeta)
		if err != nil {
			log.Error("MetaTable RecycleDroppedIndex Marshal collMeta failed",
				zap.String("key", k), zap.Error(err))
			return err
		}
		saveMeta[k] = string(v)

		if err = mt.txn.MultiSaveAndRemoveWithPrefix(saveMeta, delMeta); err != nil {
			log.Error("MetaTable RecycleDroppedIndex MultiSaveAndRemoveWithPrefix failed", zap.Error(err))
			return err
		}
		mt.collID2Meta[collID] = *clonedCollMeta
		for indexID := range deletedIndexIDs {
			delete(mt.indexID2Meta, indexID)
		}
		// update segID2IndexMeta
		for _, partID := range meta.PartitionIDs {
			if segIDMap, ok := mt.partID2SegID[partID]; ok {
				for segID := range segIDMap {
					if segIndexInfos, ok := mt.segID2IndexMeta[segID]; ok {
						for indexID := range segIndexInfos {
							if _, ok := deletedIndexIDs[indexID]; ok {
								delete(mt.segID2IndexMeta[segID], indexID)
							}
						}
					}
				}
			}
		}
	}
	return nil
}

// GetSegmentIndexInfoByID return segment index info by segment id
func (mt *MetaTable) GetSegmentIndexInfoByID(segID typeutil.UniqueID, fieldID int64, idxName string) (pb.SegmentIndexInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	segIdxMap, ok := mt.segID2IndexMeta[segID]
	if !ok {
		return pb.SegmentIndexInfo{
			SegmentID:   segID,
			FieldID:     fieldID,
			IndexID:     0,
			BuildID:     0,
			EnableIndex: false,
		}, nil
	}
	if len(segIdxMap) == 0 {
		return pb.SegmentIndexInfo{}, fmt.Errorf("segment id %d not has any index", segID)
	}

	if fieldID == -1 && idxName == "" { // return default index
		for _, seg := range segIdxMap {
			info, ok := mt.indexID2Meta[seg.IndexID]
			if ok && !info.GetDeleted() && info.IndexName == Params.CommonCfg.DefaultIndexName {
				return seg, nil
			}
		}
	} else {
		for idxID, seg := range segIdxMap {
			idxMeta, ok := mt.indexID2Meta[idxID]
			if ok && !idxMeta.GetDeleted() && idxMeta.IndexName == idxName && seg.FieldID == fieldID {
				return seg, nil
			}
		}
	}
	return pb.SegmentIndexInfo{}, fmt.Errorf("can't find index name = %s on segment = %d, with filed id = %d", idxName, segID, fieldID)
}

func (mt *MetaTable) GetSegmentIndexInfos(segID typeutil.UniqueID) (map[typeutil.UniqueID]pb.SegmentIndexInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	ret, ok := mt.segID2IndexMeta[segID]
	if !ok {
		return nil, fmt.Errorf("segment not found in meta, segment: %d", segID)
	}

	return ret, nil
}

// GetFieldSchema return field schema
func (mt *MetaTable) GetFieldSchema(collName string, fieldName string) (schemapb.FieldSchema, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	return mt.getFieldSchemaInternal(collName, fieldName)
}

func (mt *MetaTable) getFieldSchemaInternal(collName string, fieldName string) (schemapb.FieldSchema, error) {
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

// IsSegmentIndexed check if segment has indexed
func (mt *MetaTable) IsSegmentIndexed(segID typeutil.UniqueID, fieldSchema *schemapb.FieldSchema, indexParams []*commonpb.KeyValuePair) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	return mt.isSegmentIndexedInternal(segID, fieldSchema, indexParams)
}

func (mt *MetaTable) isSegmentIndexedInternal(segID typeutil.UniqueID, fieldSchema *schemapb.FieldSchema, indexParams []*commonpb.KeyValuePair) bool {
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
		if !ok || idxMeta.GetDeleted() {
			continue
		}
		if EqualKeyPairArray(indexParams, idxMeta.IndexParams) {
			exist = true
			break
		}
	}
	return exist
}

func (mt *MetaTable) getCollectionInfoInternal(collName string) (pb.CollectionInfo, error) {
	collID, ok := mt.collName2ID[collName]
	if !ok {
		collID, ok = mt.collAlias2ID[collName]
		if !ok {
			return pb.CollectionInfo{}, fmt.Errorf("collection not found: %s", collName)
		}
	}
	collMeta, ok := mt.collID2Meta[collID]
	if !ok {
		return pb.CollectionInfo{}, fmt.Errorf("collection not found: %s", collName)
	}
	return collMeta, nil
}

func (mt *MetaTable) checkFieldCanBeIndexed(collMeta pb.CollectionInfo, fieldSchema schemapb.FieldSchema, idxInfo *pb.IndexInfo) error {
	for _, f := range collMeta.FieldIndexes {
		if f.GetFiledID() == fieldSchema.GetFieldID() {
			if info, ok := mt.indexID2Meta[f.GetIndexID()]; ok {
				if info.GetDeleted() {
					continue
				}
				if idxInfo.GetIndexName() != info.GetIndexName() {
					return fmt.Errorf(
						"creating multiple indexes on same field is not supported, "+
							"collection: %s, field: %s, index name: %s, new index name: %s",
						collMeta.GetSchema().GetName(), fieldSchema.GetName(),
						info.GetIndexName(), idxInfo.GetIndexName())
				}
			} else {
				// TODO: unexpected: what if index id not exist? Meta incomplete.
				log.Warn("index meta was incomplete, index id missing in indexID2Meta",
					zap.String("collection", collMeta.GetSchema().GetName()),
					zap.String("field", fieldSchema.GetName()),
					zap.Int64("collection id", collMeta.GetID()),
					zap.Int64("field id", fieldSchema.GetFieldID()),
					zap.Int64("index id", f.GetIndexID()))
			}
		}
	}
	return nil
}

func (mt *MetaTable) checkFieldIndexDuplicate(collMeta pb.CollectionInfo, fieldSchema schemapb.FieldSchema, idxInfo *pb.IndexInfo) (duplicate bool, dupIdxInfo *pb.IndexInfo, err error) {
	for _, f := range collMeta.FieldIndexes {
		if info, ok := mt.indexID2Meta[f.IndexID]; ok && !info.GetDeleted() {
			if info.IndexName == idxInfo.IndexName {
				// the index name must be different for different indexes
				if f.FiledID != fieldSchema.FieldID || !EqualKeyPairArray(info.IndexParams, idxInfo.IndexParams) {
					return false, nil, fmt.Errorf("index already exists, collection: %s, field: %s, index: %s", collMeta.GetSchema().GetName(), fieldSchema.GetName(), idxInfo.GetIndexName())
				}

				// same index name, index params, and fieldId
				return true, proto.Clone(&info).(*pb.IndexInfo), nil
			}
		}
	}
	return false, nil, nil
}

// GetNotIndexedSegments return segment ids which have no index
// TODO, split GetNotIndexedSegments into two calls, one is to update CollectionMetaPrefix, IndexMetaPrefix, the otherone is trigger index build task
func (mt *MetaTable) GetNotIndexedSegments(collName string, fieldName string, idxInfo *pb.IndexInfo, segIDs []typeutil.UniqueID) ([]typeutil.UniqueID, schemapb.FieldSchema, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	collMeta, err := mt.getCollectionInfoInternal(collName)
	if err != nil {
		// error here if collection not found.
		return nil, schemapb.FieldSchema{}, err
	}

	fieldSchema, err := mt.getFieldSchemaInternal(collName, fieldName)
	if err != nil {
		// error here if field not found.
		return nil, fieldSchema, err
	}

	//TODO:: check index params for sclar field
	// set default index type for scalar index
	if !typeutil.IsVectorType(fieldSchema.GetDataType()) {
		if fieldSchema.DataType == schemapb.DataType_VarChar {
			idxInfo.IndexParams = []*commonpb.KeyValuePair{{Key: "index_type", Value: DefaultStringIndexType}}
		} else {
			idxInfo.IndexParams = []*commonpb.KeyValuePair{{Key: "index_type", Value: DefaultIndexType}}
		}
	}

	if idxInfo.IndexParams == nil {
		return nil, schemapb.FieldSchema{}, fmt.Errorf("index param is nil")
	}

	if err := mt.checkFieldCanBeIndexed(collMeta, fieldSchema, idxInfo); err != nil {
		return nil, schemapb.FieldSchema{}, err
	}

	dupIdx, dupIdxInfo, err := mt.checkFieldIndexDuplicate(collMeta, fieldSchema, idxInfo)
	if err != nil {
		// error here if index already exists.
		return nil, fieldSchema, err
	}

	// if no same index exist, save new index info to etcd
	if !dupIdx {
		idx := &pb.FieldIndexInfo{
			FiledID: fieldSchema.FieldID,
			IndexID: idxInfo.IndexID,
		}
		collMeta.FieldIndexes = append(collMeta.FieldIndexes, idx)
		k1 := path.Join(CollectionMetaPrefix, strconv.FormatInt(collMeta.ID, 10))
		v1, err := proto.Marshal(&collMeta)
		if err != nil {
			log.Error("MetaTable GetNotIndexedSegments Marshal collMeta fail",
				zap.String("key", k1), zap.Error(err))
			return nil, schemapb.FieldSchema{}, fmt.Errorf("metaTable GetNotIndexedSegments Marshal collMeta fail key:%s, err:%w", k1, err)
		}

		k2 := fmt.Sprintf("%s/%d/%d", IndexMetaPrefix, collMeta.ID, idx.IndexID)
		//k2 := path.Join(IndexMetaPrefix, strconv.FormatInt(idx.IndexID, 10))
		v2, err := proto.Marshal(idxInfo)
		if err != nil {
			log.Error("MetaTable GetNotIndexedSegments Marshal idxInfo fail",
				zap.String("key", k2), zap.Error(err))
			return nil, schemapb.FieldSchema{}, fmt.Errorf("metaTable GetNotIndexedSegments Marshal idxInfo fail key:%s, err:%w", k2, err)
		}
		meta := map[string]string{k1: string(v1), k2: string(v2)}

		err = mt.txn.MultiSave(meta)
		if err != nil {
			log.Error("TxnKV MultiSave fail", zap.Error(err))
			panic("TxnKV MultiSave fail")
		}

		mt.collID2Meta[collMeta.ID] = collMeta
		mt.indexID2Meta[idx.IndexID] = *idxInfo
	} else {
		log.Info("index has been created, update timestamp for IndexID", zap.Int64("indexID", dupIdxInfo.IndexID))
		// just update create time for IndexID
		dupIdxInfo.CreateTime = idxInfo.CreateTime
		k := fmt.Sprintf("%s/%d/%d", IndexMetaPrefix, collMeta.ID, dupIdxInfo.IndexID)
		v, err := proto.Marshal(dupIdxInfo)
		if err != nil {
			log.Error("MetaTable GetNotIndexedSegments Marshal idxInfo fail",
				zap.String("key", k), zap.Error(err))
			return nil, schemapb.FieldSchema{}, fmt.Errorf("metaTable GetNotIndexedSegments Marshal idxInfo fail key:%s, err:%w", k, err)
		}
		meta := map[string]string{k: string(v)}

		err = mt.txn.MultiSave(meta)
		if err != nil {
			log.Error("TxnKV MultiSave fail", zap.Error(err))
			panic("TxnKV MultiSave fail")
		}
		mt.indexID2Meta[dupIdxInfo.IndexID] = *dupIdxInfo
	}

	rstID := make([]typeutil.UniqueID, 0, 16)
	for _, segID := range segIDs {
		if exist := mt.isSegmentIndexedInternal(segID, &fieldSchema, idxInfo.IndexParams); !exist {
			rstID = append(rstID, segID)
		}
	}
	return rstID, fieldSchema, nil
}

// GetIndexByName return index info by index name
// TODO: IndexName is unique on collection, should return (pb.CollectionInfo, pb.IndexInfo, error)
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
		if idxInfo.GetDeleted() {
			continue
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
	if !ok || indexInfo.GetDeleted() {
		return nil, fmt.Errorf("cannot find index, id = %d", indexID)
	}
	return &indexInfo, nil
}

func (mt *MetaTable) dupCollectionMeta() map[typeutil.UniqueID]pb.CollectionInfo {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	collID2Meta := map[typeutil.UniqueID]pb.CollectionInfo{}
	for k, v := range mt.collID2Meta {
		v := v
		collID2Meta[k] = *proto.Clone(&v).(*pb.CollectionInfo)
	}
	return collID2Meta
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
		return fmt.Errorf("metaTable AddAlias Marshal CollectionInfo fail key:%s, err:%w", k, err)
	}

	err = mt.snapshot.Save(k, string(v), ts)
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
	err := mt.snapshot.MultiSaveAndRemoveWithPrefix(meta, delMetakeys, ts)
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
		return fmt.Errorf("metaTable AlterAlias Marshal CollectionInfo fail key:%s, err:%w", k, err)
	}

	err = mt.snapshot.Save(k, string(v), ts)
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

// AddCredential add credential
func (mt *MetaTable) AddCredential(credInfo *internalpb.CredentialInfo) error {
	mt.credLock.Lock()
	defer mt.credLock.Unlock()

	if credInfo.Username == "" {
		return fmt.Errorf("username is empty")
	}
	k := fmt.Sprintf("%s/%s", CredentialPrefix, credInfo.Username)
	v, err := json.Marshal(&internalpb.CredentialInfo{EncryptedPassword: credInfo.EncryptedPassword})
	if err != nil {
		log.Error("MetaTable marshal credential info fail", zap.String("key", k), zap.Error(err))
		return fmt.Errorf("metaTable marshal credential info fail key:%s, err:%w", k, err)
	}
	err = mt.txn.Save(k, string(v))
	if err != nil {
		log.Error("MetaTable save fail", zap.Error(err))
		return fmt.Errorf("save credential fail key:%s, err:%w", credInfo.Username, err)
	}
	return nil
}

// GetCredential get credential by username
func (mt *MetaTable) getCredential(username string) (*internalpb.CredentialInfo, error) {
	mt.credLock.RLock()
	defer mt.credLock.RUnlock()

	k := fmt.Sprintf("%s/%s", CredentialPrefix, username)
	v, err := mt.txn.Load(k)
	if err != nil {
		log.Warn("MetaTable load fail", zap.String("key", k), zap.Error(err))
		return nil, err
	}

	credentialInfo := internalpb.CredentialInfo{}
	err = json.Unmarshal([]byte(v), &credentialInfo)
	if err != nil {
		return nil, fmt.Errorf("get credential unmarshal err:%w", err)
	}
	return &internalpb.CredentialInfo{Username: username, EncryptedPassword: credentialInfo.EncryptedPassword}, nil
}

// DeleteCredential delete credential
func (mt *MetaTable) DeleteCredential(username string) error {
	mt.credLock.Lock()
	defer mt.credLock.Unlock()

	k := fmt.Sprintf("%s/%s", CredentialPrefix, username)

	err := mt.txn.Remove(k)
	if err != nil {
		log.Error("MetaTable remove fail", zap.Error(err))
		return fmt.Errorf("remove credential fail key:%s, err:%w", username, err)
	}
	return nil
}

// ListCredentialUsernames list credential usernames
func (mt *MetaTable) ListCredentialUsernames() (*milvuspb.ListCredUsersResponse, error) {
	mt.credLock.RLock()
	defer mt.credLock.RUnlock()

	keys, _, err := mt.txn.LoadWithPrefix(CredentialPrefix)
	if err != nil {
		log.Error("MetaTable list all credential usernames fail", zap.Error(err))
		return &milvuspb.ListCredUsersResponse{}, err
	}

	var usernames []string
	for _, path := range keys {
		username := typeutil.After(path, UserSubPrefix+"/")
		if len(username) == 0 {
			log.Warn("no username extract from path:", zap.String("path", path))
			continue
		}
		usernames = append(usernames, username)
	}
	return &milvuspb.ListCredUsersResponse{Usernames: usernames}, nil
}
