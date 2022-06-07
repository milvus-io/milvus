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
	"context"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore"
	kvmetestore "github.com/milvus-io/milvus/internal/metastore/kv"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

const (
	// TimestampPrefix prefix for timestamp
	TimestampPrefix = kvmetestore.ComponentPrefix + "/timestamp"

	// DDOperationPrefix prefix for DD operation
	DDOperationPrefix = kvmetestore.ComponentPrefix + "/dd-operation"

	// DDMsgSendPrefix prefix to indicate whether DD msg has been send
	DDMsgSendPrefix = kvmetestore.ComponentPrefix + "/dd-msg-send"

	// CreateCollectionDDType name of DD type for create collection
	CreateCollectionDDType = "CreateCollection"

	// DropCollectionDDType name of DD type for drop collection
	DropCollectionDDType = "DropCollection"

	// CreatePartitionDDType name of DD type for create partition
	CreatePartitionDDType = "CreatePartition"

	// DropPartitionDDType name of DD type for drop partition
	DropPartitionDDType = "DropPartition"

	// DefaultIndexType name of default index type for scalar field
	DefaultIndexType = "STL_SORT"

	// DefaultStringIndexType name of default index type for varChar/string field
	DefaultStringIndexType = "Trie"
)

// MetaTable store all rootCoord meta info
type MetaTable struct {
	ctx      context.Context
	txn      kv.TxnKV      // client of a reliable txnkv service, i.e. etcd client
	snapshot kv.SnapShotKV // client of a reliable snapshotkv service, i.e. etcd client
	catalog  metastore.Catalog

	proxyID2Meta    map[typeutil.UniqueID]pb.ProxyMeta                      // proxy id to proxy meta
	collID2Meta     map[typeutil.UniqueID]model.Collection                  // collection id -> collection meta
	collName2ID     map[string]typeutil.UniqueID                            // collection name to collection id
	collAlias2ID    map[string]typeutil.UniqueID                            // collection alias to collection id
	partID2SegID    map[typeutil.UniqueID]map[typeutil.UniqueID]bool        // partition id -> segment_id -> bool
	segID2IndexMeta map[typeutil.UniqueID]map[typeutil.UniqueID]model.Index // collection id/index_id/partition_id/segment_id -> meta
	indexID2Meta    map[typeutil.UniqueID]model.Index                       // collection id/index_id -> meta

	proxyLock sync.RWMutex
	ddLock    sync.RWMutex
	credLock  sync.RWMutex
}

// NewMetaTable creates meta table for rootcoord, which stores all in-memory information
// for collection, partition, segment, index etc.
func NewMetaTable(ctx context.Context, txn kv.TxnKV, snap kv.SnapShotKV) (*MetaTable, error) {
	mt := &MetaTable{
		ctx:       ctx,
		txn:       txn,
		snapshot:  snap,
		catalog:   &kvmetestore.Catalog{Txn: txn, Snapshot: snap},
		proxyLock: sync.RWMutex{},
		ddLock:    sync.RWMutex{},
		credLock:  sync.RWMutex{},
	}
	err := mt.reloadFromKV()
	if err != nil {
		return nil, err
	}
	return mt, nil
}

func (mt *MetaTable) reloadFromKV() error {
	mt.proxyID2Meta = make(map[typeutil.UniqueID]pb.ProxyMeta)
	mt.collID2Meta = make(map[typeutil.UniqueID]model.Collection)
	mt.collName2ID = make(map[string]typeutil.UniqueID)
	mt.collAlias2ID = make(map[string]typeutil.UniqueID)
	mt.partID2SegID = make(map[typeutil.UniqueID]map[typeutil.UniqueID]bool)
	mt.segID2IndexMeta = make(map[typeutil.UniqueID]map[typeutil.UniqueID]model.Index)
	mt.indexID2Meta = make(map[typeutil.UniqueID]model.Index)

	collMap, err := mt.catalog.ListCollections(mt.ctx, 0)
	if err != nil {
		return err
	}
	for _, coll := range collMap {
		mt.collID2Meta[coll.CollectionID] = *coll
		mt.collName2ID[coll.Name] = coll.CollectionID
	}

	indexes, err := mt.catalog.ListIndexes(mt.ctx)
	if err != nil {
		return err
	}
	for _, index := range indexes {
		for _, segIndexInfo := range index.SegmentIndexes {
			// update partID2SegID
			segIDMap, ok := mt.partID2SegID[segIndexInfo.Segment.PartitionID]
			if ok {
				segIDMap[segIndexInfo.Segment.SegmentID] = true
			} else {
				idMap := make(map[typeutil.UniqueID]bool)
				idMap[segIndexInfo.Segment.SegmentID] = true
				mt.partID2SegID[segIndexInfo.Segment.PartitionID] = idMap
			}

			// update segID2IndexMeta
			idx, ok := mt.segID2IndexMeta[segIndexInfo.Segment.SegmentID]
			if ok {
				idx[index.IndexID] = *index
			} else {
				meta := make(map[typeutil.UniqueID]model.Index)
				meta[index.IndexID] = *index
				mt.segID2IndexMeta[segIndexInfo.Segment.SegmentID] = meta
			}
		}

		mt.indexID2Meta[index.IndexID] = *index
	}

	collAliases, err := mt.catalog.ListAliases(mt.ctx)
	if err != nil {
		return err
	}
	for _, aliasInfo := range collAliases {
		mt.collAlias2ID[aliasInfo.Name] = aliasInfo.CollectionID
	}

	log.Debug("reload meta table from KV successfully")
	return nil
}

// AddCollection add collection
func (mt *MetaTable) AddCollection(coll *model.Collection, ts typeutil.Timestamp, ddOpStr string) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if _, ok := mt.collName2ID[coll.Name]; ok {
		return fmt.Errorf("collection %s exist", coll.Name)
	}

	mt.collID2Meta[coll.CollectionID] = *coll
	mt.collName2ID[coll.Name] = coll.CollectionID

	coll.CreateTime = ts
	for _, partition := range coll.Partitions {
		partition.PartitionCreatedTimestamp = ts
	}

	meta := map[string]string{}
	meta[DDMsgSendPrefix] = "false"
	meta[DDOperationPrefix] = ddOpStr
	coll.Extra = meta
	return mt.catalog.CreateCollection(mt.ctx, coll, ts)
}

// DeleteCollection delete collection
func (mt *MetaTable) DeleteCollection(collID typeutil.UniqueID, ts typeutil.Timestamp, ddOpStr string) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	col, ok := mt.collID2Meta[collID]
	if !ok {
		return fmt.Errorf("can't find collection. id = %d", collID)
	}

	delete(mt.collID2Meta, collID)
	delete(mt.collName2ID, col.Name)

	// update segID2IndexMeta
	for _, partition := range col.Partitions {
		partID := partition.PartitionID
		if segIDMap, ok := mt.partID2SegID[typeutil.UniqueID(partID)]; ok {
			for segID := range segIDMap {
				delete(mt.segID2IndexMeta, segID)
			}
		}
		delete(mt.partID2SegID, typeutil.UniqueID(partID))
	}

	for _, idxInfo := range col.FieldIndexes {
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
			delete(mt.collAlias2ID, alias)
		}
	}

	// save ddOpStr into etcd
	var meta = map[string]string{
		DDMsgSendPrefix:   "false",
		DDOperationPrefix: ddOpStr,
	}

	collection := &model.Collection{
		CollectionID: collID,
		Aliases:      aliases,
		Extra:        meta,
	}

	return mt.catalog.DropCollection(mt.ctx, collection, ts)
}

// HasCollection return collection existence
func (mt *MetaTable) HasCollection(collID typeutil.UniqueID, ts typeutil.Timestamp) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	if ts == 0 {
		_, ok := mt.collID2Meta[collID]
		return ok
	}

	return mt.catalog.CollectionExists(mt.ctx, collID, ts)
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

// GetCollectionByID return collection meta by collection id
func (mt *MetaTable) GetCollectionByID(collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*model.Collection, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	if ts == 0 {
		col, ok := mt.collID2Meta[collectionID]
		if !ok {
			return nil, fmt.Errorf("can't find collection id : %d", collectionID)
		}
		return model.CloneCollectionModel(col), nil
	}

	return mt.catalog.GetCollectionByID(mt.ctx, collectionID, ts)
}

// GetCollectionByName return collection meta by collection name
func (mt *MetaTable) GetCollectionByName(collectionName string, ts typeutil.Timestamp) (*model.Collection, error) {
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

		return model.CloneCollectionModel(col), nil
	}

	return mt.catalog.GetCollectionByName(mt.ctx, collectionName, ts)
}

// ListCollections list all collection names
func (mt *MetaTable) ListCollections(ts typeutil.Timestamp) (map[string]*model.Collection, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	cols := make(map[string]*model.Collection)

	if ts == 0 {
		for collName, collID := range mt.collName2ID {
			col := mt.collID2Meta[collID]
			cols[collName] = model.CloneCollectionModel(col)
		}
		return cols, nil
	}

	return mt.catalog.ListCollections(mt.ctx, ts)
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
	if int64(len(coll.Partitions)) >= Params.RootCoordCfg.MaxPartitionNum {
		return fmt.Errorf("maximum partition's number should be limit to %d", Params.RootCoordCfg.MaxPartitionNum)
	}

	for _, p := range coll.Partitions {
		if p.PartitionID == partitionID {
			return fmt.Errorf("partition id = %d already exists", partitionID)
		}
		if p.PartitionName == partitionName {
			return fmt.Errorf("partition name = %s already exists", partitionName)
		}
		// no necessary to check created timestamp
	}

	coll.Partitions = append(coll.Partitions,
		&model.Partition{
			PartitionID:               partitionID,
			PartitionName:             partitionName,
			PartitionCreatedTimestamp: ts,
		})
	mt.collID2Meta[collID] = coll

	metaTxn := map[string]string{}
	// save ddOpStr into etcd
	metaTxn[DDMsgSendPrefix] = "false"
	metaTxn[DDOperationPrefix] = ddOpStr
	coll.Extra = metaTxn
	return mt.catalog.CreatePartition(mt.ctx, &coll, ts)
}

// GetPartitionNameByID return partition name by partition id
func (mt *MetaTable) GetPartitionNameByID(collID, partitionID typeutil.UniqueID, ts typeutil.Timestamp) (string, error) {
	if ts == 0 {
		mt.ddLock.RLock()
		defer mt.ddLock.RUnlock()
		col, ok := mt.collID2Meta[collID]
		if !ok {
			return "", fmt.Errorf("can't find collection id = %d", collID)
		}
		for _, partition := range col.Partitions {
			if partition.PartitionID == partitionID {
				return partition.PartitionName, nil
			}
		}
		return "", fmt.Errorf("partition %d does not exist", partitionID)
	}

	col, err := mt.catalog.GetCollectionByID(mt.ctx, collID, ts)
	if err != nil {
		return "", err
	}
	for _, partition := range col.Partitions {
		if partition.PartitionID == partitionID {
			return partition.PartitionName, nil
		}
	}
	return "", fmt.Errorf("partition %d does not exist", partitionID)
}

func (mt *MetaTable) getPartitionByName(collID typeutil.UniqueID, partitionName string, ts typeutil.Timestamp) (typeutil.UniqueID, error) {
	if ts == 0 {
		col, ok := mt.collID2Meta[collID]
		if !ok {
			return 0, fmt.Errorf("can't find collection id = %d", collID)
		}
		for _, partition := range col.Partitions {
			if partition.PartitionName == partitionName {
				return partition.PartitionID, nil
			}
		}
		return 0, fmt.Errorf("partition %s does not exist", partitionName)
	}

	col, err := mt.catalog.GetCollectionByID(mt.ctx, collID, ts)
	if err != nil {
		return 0, err
	}
	for _, partition := range col.Partitions {
		if partition.PartitionName == partitionName {
			return partition.PartitionID, nil
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

	col, ok := mt.collID2Meta[collID]
	if !ok {
		return 0, fmt.Errorf("can't find collection id = %d", collID)
	}

	// check tag exists
	exist := false

	parts := make([]*model.Partition, 0, len(col.Partitions))

	var partID typeutil.UniqueID
	for _, partition := range col.Partitions {
		if partition.PartitionName == partitionName {
			partID = partition.PartitionID
			exist = true
		} else {
			parts = append(parts, partition)
		}
	}
	if !exist {
		return 0, fmt.Errorf("partition %s does not exist", partitionName)
	}
	col.Partitions = parts
	mt.collID2Meta[collID] = col

	// update segID2IndexMeta and partID2SegID
	if segIDMap, ok := mt.partID2SegID[partID]; ok {
		for segID := range segIDMap {
			delete(mt.segID2IndexMeta, segID)
		}
	}
	delete(mt.partID2SegID, partID)

	metaTxn := make(map[string]string)
	// save ddOpStr into etcd
	metaTxn[DDMsgSendPrefix] = "false"
	metaTxn[DDOperationPrefix] = ddOpStr
	col.Extra = metaTxn

	err := mt.catalog.DropPartition(mt.ctx, &col, partID, ts)
	if err != nil {
		return 0, err
	}

	return partID, nil
}

func (mt *MetaTable) updateSegmentIndexMetaCache(index *model.Index) {
	for _, segIdxInfo := range index.SegmentIndexes {
		if _, ok := mt.partID2SegID[segIdxInfo.PartitionID]; !ok {
			segIDMap := map[typeutil.UniqueID]bool{segIdxInfo.SegmentID: true}
			mt.partID2SegID[segIdxInfo.PartitionID] = segIDMap
		} else {
			mt.partID2SegID[segIdxInfo.PartitionID][segIdxInfo.SegmentID] = true
		}

		_, ok := mt.segID2IndexMeta[segIdxInfo.SegmentID]
		if !ok {
			idxMap := map[typeutil.UniqueID]model.Index{index.IndexID: *index}
			mt.segID2IndexMeta[segIdxInfo.SegmentID] = idxMap
		} else {
			mt.segID2IndexMeta[segIdxInfo.SegmentID][index.IndexID] = *index
		}
	}
}

// AlterIndex alter index
func (mt *MetaTable) AlterIndex(newIndex *model.Index) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	_, ok := mt.collID2Meta[newIndex.CollectionID]
	if !ok {
		return fmt.Errorf("collection id = %d not found", newIndex.CollectionID)
	}

	oldIndex, ok := mt.indexID2Meta[newIndex.IndexID]
	if !ok {
		return fmt.Errorf("index id = %d not found", newIndex.IndexID)
	}

	mt.updateSegmentIndexMetaCache(newIndex)
	return mt.catalog.AlterIndex(mt.ctx, &oldIndex, newIndex)
}

// DropIndex drop index
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
	col, ok := mt.collID2Meta[collID]
	if !ok {
		return 0, false, fmt.Errorf("collection name  = %s not has meta", collName)
	}
	fieldSch, err := mt.unlockGetFieldSchema(collName, fieldName)
	if err != nil {
		return 0, false, err
	}
	fieldIdxInfo := make([]*model.Index, 0, len(col.FieldIndexes))
	var dropIdxID typeutil.UniqueID
	for i, info := range col.FieldIndexes {
		if info.FieldID != fieldSch.FieldID {
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
		fieldIdxInfo = append(fieldIdxInfo, col.FieldIndexes[i+1:]...)
		break
	}

	if len(fieldIdxInfo) == len(col.FieldIndexes) {
		log.Warn("drop index,index not found", zap.String("collection name", collName), zap.String("filed name", fieldName), zap.String("index name", indexName))
		return 0, false, nil
	}

	// update cache
	col.FieldIndexes = fieldIdxInfo
	mt.collID2Meta[collID] = col

	delete(mt.indexID2Meta, dropIdxID)
	for _, part := range col.Partitions {
		if segIDMap, ok := mt.partID2SegID[part.PartitionID]; ok {
			for segID := range segIDMap {
				if segIndexInfos, ok := mt.segID2IndexMeta[segID]; ok {
					delete(segIndexInfos, dropIdxID)
				}
			}
		}
	}

	// update metastore
	err = mt.catalog.DropIndex(mt.ctx, &col, dropIdxID, 0)
	if err != nil {
		return 0, false, err
	}

	return dropIdxID, true, nil
}

// GetSegmentIndexInfoByID return segment index info by segment id
func (mt *MetaTable) GetSegmentIndexInfoByID(segID typeutil.UniqueID, fieldID int64, idxName string) (model.Index, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	segIdxMap, ok := mt.segID2IndexMeta[segID]
	if !ok {
		return model.Index{
			SegmentIndexes: map[int64]model.SegmentIndex{
				segID: {
					Segment: model.Segment{
						SegmentID: segID,
					},
					BuildID:     0,
					EnableIndex: false,
				},
			},
			FieldID: fieldID,
			IndexID: 0,
		}, nil
	}
	if len(segIdxMap) == 0 {
		return model.Index{}, fmt.Errorf("segment id %d not has any index", segID)
	}

	if fieldID == -1 && idxName == "" { // return default index
		for _, seg := range segIdxMap {
			info, ok := mt.indexID2Meta[seg.IndexID]
			if ok && info.IndexName == Params.CommonCfg.DefaultIndexName {
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
				if seg.FieldID != fieldID {
					continue
				}
				return seg, nil
			}
		}
	}
	return model.Index{}, fmt.Errorf("can't find index name = %s on segment = %d, with filed id = %d", idxName, segID, fieldID)
}

func (mt *MetaTable) GetSegmentIndexInfos(segID typeutil.UniqueID) (map[typeutil.UniqueID]model.Index, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	ret, ok := mt.segID2IndexMeta[segID]
	if !ok {
		return nil, fmt.Errorf("segment not found in meta, segment: %d", segID)
	}

	return ret, nil
}

// GetFieldSchema return field schema
func (mt *MetaTable) GetFieldSchema(collName string, fieldName string) (model.Field, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	return mt.unlockGetFieldSchema(collName, fieldName)
}

func (mt *MetaTable) unlockGetFieldSchema(collName string, fieldName string) (model.Field, error) {
	collID, ok := mt.collName2ID[collName]
	if !ok {
		collID, ok = mt.collAlias2ID[collName]
		if !ok {
			return model.Field{}, fmt.Errorf("collection %s not found", collName)
		}
	}
	col, ok := mt.collID2Meta[collID]
	if !ok {
		return model.Field{}, fmt.Errorf("collection %s not found", collName)
	}

	for _, field := range col.Fields {
		if field.Name == fieldName {
			return *field, nil
		}
	}
	return model.Field{}, fmt.Errorf("collection %s doesn't have filed %s", collName, fieldName)
}

// IsSegmentIndexed check if segment has indexed
func (mt *MetaTable) IsSegmentIndexed(segID typeutil.UniqueID, fieldSchema *model.Field, indexParams []*commonpb.KeyValuePair) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	return mt.unlockIsSegmentIndexed(segID, fieldSchema, indexParams)
}

func (mt *MetaTable) unlockIsSegmentIndexed(segID typeutil.UniqueID, fieldSchema *model.Field, indexParams []*commonpb.KeyValuePair) bool {
	index, ok := mt.segID2IndexMeta[segID]
	if !ok {
		return false
	}
	isIndexed := false
	for idxID, meta := range index {
		if meta.FieldID != fieldSchema.FieldID {
			continue
		}
		idxMeta, ok := mt.indexID2Meta[idxID]
		if !ok {
			continue
		}

		segIndex, ok := meta.SegmentIndexes[segID]
		if !ok {
			continue
		}

		if EqualKeyPairArray(indexParams, idxMeta.IndexParams) && segIndex.EnableIndex {
			isIndexed = true
			break
		}
	}

	return isIndexed
}

func (mt *MetaTable) unlockGetCollectionInfo(collName string) (model.Collection, error) {
	collID, ok := mt.collName2ID[collName]
	if !ok {
		collID, ok = mt.collAlias2ID[collName]
		if !ok {
			return model.Collection{}, fmt.Errorf("collection not found: %s", collName)
		}
	}
	collMeta, ok := mt.collID2Meta[collID]
	if !ok {
		return model.Collection{}, fmt.Errorf("collection not found: %s", collName)
	}
	return collMeta, nil
}

func (mt *MetaTable) checkFieldCanBeIndexed(collMeta model.Collection, fieldSchema model.Field, idxInfo *model.Index) error {
	for _, f := range collMeta.FieldIndexes {
		if f.FieldID == fieldSchema.FieldID {
			if info, ok := mt.indexID2Meta[f.IndexID]; ok {
				if idxInfo.IndexName != info.IndexName {
					return fmt.Errorf(
						"creating multiple indexes on same field is not supported, "+
							"collection: %s, field: %s, index name: %s, new index name: %s",
						collMeta.Name, fieldSchema.Name,
						info.IndexName, idxInfo.IndexName)
				}
			} else {
				// TODO: unexpected: what if index id not exist? Meta incomplete.
				log.Warn("index meta was incomplete, index id missing in indexID2Meta",
					zap.String("collection", collMeta.Name),
					zap.String("field", fieldSchema.Name),
					zap.Int64("collection id", collMeta.CollectionID),
					zap.Int64("field id", fieldSchema.FieldID),
					zap.Int64("index id", f.IndexID))
			}
		}
	}
	return nil
}

func (mt *MetaTable) checkFieldIndexDuplicate(collMeta model.Collection, fieldSchema model.Field, idxInfo *model.Index) (duplicate bool, err error) {
	for _, f := range collMeta.FieldIndexes {
		if info, ok := mt.indexID2Meta[f.IndexID]; ok {
			if info.IndexName == idxInfo.IndexName {
				// the index name must be different for different indexes
				if f.FieldID != fieldSchema.FieldID || !EqualKeyPairArray(info.IndexParams, idxInfo.IndexParams) {
					return false, fmt.Errorf("index already exists, collection: %s, field: %s, index: %s", collMeta.Name, fieldSchema.Name, idxInfo.IndexName)
				}

				// same index name, index params, and fieldId
				return true, nil
			}
		}
	}
	return false, nil
}

// GetNotIndexedSegments return segment ids which have no index
func (mt *MetaTable) GetNotIndexedSegments(collName string, fieldName string, idxInfo *model.Index, segIDs []typeutil.UniqueID) ([]typeutil.UniqueID, model.Field, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	fieldSchema, err := mt.unlockGetFieldSchema(collName, fieldName)
	if err != nil {
		return nil, fieldSchema, err
	}

	rstID := make([]typeutil.UniqueID, 0, 16)
	for _, segID := range segIDs {
		if ok := mt.unlockIsSegmentIndexed(segID, &fieldSchema, idxInfo.IndexParams); !ok {
			rstID = append(rstID, segID)
		}
	}
	return rstID, fieldSchema, nil
}

// AddIndex add index
func (mt *MetaTable) AddIndex(colName string, fieldName string, idxInfo *model.Index, segIDs []typeutil.UniqueID) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	fieldSchema, err := mt.unlockGetFieldSchema(colName, fieldName)
	if err != nil {
		return err
	}

	collMeta, err := mt.unlockGetCollectionInfo(colName)
	if err != nil {
		// error here if collection not found.
		return err
	}

	//TODO:: check index params for sclar field
	// set default index type for scalar index
	if !typeutil.IsVectorType(fieldSchema.DataType) {
		if fieldSchema.DataType == schemapb.DataType_VarChar {
			idxInfo.IndexParams = []*commonpb.KeyValuePair{{Key: "index_type", Value: DefaultStringIndexType}}
		} else {
			idxInfo.IndexParams = []*commonpb.KeyValuePair{{Key: "index_type", Value: DefaultIndexType}}
		}
	}

	if idxInfo.IndexParams == nil {
		return fmt.Errorf("index param is nil")
	}

	if err := mt.checkFieldCanBeIndexed(collMeta, fieldSchema, idxInfo); err != nil {
		return err
	}

	dupIdx, err := mt.checkFieldIndexDuplicate(collMeta, fieldSchema, idxInfo)
	if err != nil {
		// error here if index already exists.
		return err
	}

	if dupIdx {
		log.Warn("due to index already exists, skip add index to metastore", zap.Int64("collectionID", collMeta.CollectionID),
			zap.Int64("indexID", idxInfo.IndexID), zap.String("indexName", idxInfo.IndexName))
		// skip already exist index
		return nil
	}

	segmentIndexes := make(map[int64]model.SegmentIndex, len(segIDs))
	for _, segID := range segIDs {
		segmentIndex := model.SegmentIndex{
			Segment: model.Segment{
				SegmentID: segID,
			},
			EnableIndex: false,
		}
		segmentIndexes[segID] = segmentIndex
	}

	idxInfo.SegmentIndexes = segmentIndexes
	idxInfo.FieldID = fieldSchema.FieldID
	idxInfo.CollectionID = collMeta.CollectionID

	idx := &model.Index{
		FieldID:   fieldSchema.FieldID,
		IndexID:   idxInfo.IndexID,
		IndexName: idxInfo.IndexName,
		Extra:     idxInfo.Extra,
	}

	collMeta.FieldIndexes = append(collMeta.FieldIndexes, idx)

	mt.catalog.CreateIndex(mt.ctx, &collMeta, idxInfo)

	mt.collID2Meta[collMeta.CollectionID] = collMeta
	mt.indexID2Meta[idxInfo.IndexID] = *idxInfo
	return nil
}

// GetIndexByName return index info by index name
func (mt *MetaTable) GetIndexByName(collName, indexName string) (model.Collection, []model.Index, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	collID, ok := mt.collName2ID[collName]
	if !ok {
		collID, ok = mt.collAlias2ID[collName]
		if !ok {
			return model.Collection{}, nil, fmt.Errorf("collection %s not found", collName)
		}
	}
	col, ok := mt.collID2Meta[collID]
	if !ok {
		return model.Collection{}, nil, fmt.Errorf("collection %s not found", collName)
	}

	rstIndex := make([]model.Index, 0, len(col.FieldIndexes))
	for _, idx := range col.FieldIndexes {
		idxInfo, ok := mt.indexID2Meta[idx.IndexID]
		if !ok {
			return model.Collection{}, nil, fmt.Errorf("index id = %d not found", idx.IndexID)
		}
		if indexName == "" || idxInfo.IndexName == indexName {
			rstIndex = append(rstIndex, idxInfo)
		}
	}
	return col, rstIndex, nil
}

// GetIndexByID return index info by index id
func (mt *MetaTable) GetIndexByID(indexID typeutil.UniqueID) (*model.Index, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	indexInfo, ok := mt.indexID2Meta[indexID]
	if !ok {
		return nil, fmt.Errorf("cannot find index, id = %d", indexID)
	}
	return &indexInfo, nil
}

func (mt *MetaTable) dupMeta() (
	map[typeutil.UniqueID]model.Collection,
	map[typeutil.UniqueID]map[typeutil.UniqueID]model.Index,
	map[typeutil.UniqueID]model.Index,
) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	collID2Meta := map[typeutil.UniqueID]model.Collection{}
	segID2IndexMeta := map[typeutil.UniqueID]map[typeutil.UniqueID]model.Index{}
	indexID2Meta := map[typeutil.UniqueID]model.Index{}
	for k, v := range mt.collID2Meta {
		collID2Meta[k] = v
	}
	for k, v := range mt.segID2IndexMeta {
		segID2IndexMeta[k] = map[typeutil.UniqueID]model.Index{}
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

	coll := &model.Collection{
		CollectionID: id,
		Aliases:      []string{collectionAlias},
	}
	return mt.catalog.CreateAlias(mt.ctx, coll, ts)
}

// DropAlias drop collection alias
func (mt *MetaTable) DropAlias(collectionAlias string, ts typeutil.Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	collectionID, ok := mt.collAlias2ID[collectionAlias]
	if !ok {
		return fmt.Errorf("alias does not exist, alias = %s", collectionAlias)
	}
	delete(mt.collAlias2ID, collectionAlias)

	return mt.catalog.DropAlias(mt.ctx, collectionID, collectionAlias, ts)
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

	coll := &model.Collection{
		CollectionID: id,
		Aliases:      []string{collectionAlias},
	}
	return mt.catalog.AlterAlias(mt.ctx, coll, ts)
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
	if credInfo.Username == "" {
		return fmt.Errorf("username is empty")
	}

	credential := &model.Credential{
		Username:          credInfo.Username,
		EncryptedPassword: credInfo.EncryptedPassword,
	}
	return mt.catalog.CreateCredential(mt.ctx, credential)
}

// GetCredential get credential by username
func (mt *MetaTable) getCredential(username string) (*internalpb.CredentialInfo, error) {
	credential, err := mt.catalog.GetCredential(mt.ctx, username)
	return model.ConvertToCredentialPB(credential), err
}

// DeleteCredential delete credential
func (mt *MetaTable) DeleteCredential(username string) error {
	return mt.catalog.DropCredential(mt.ctx, username)
}

// ListCredentialUsernames list credential usernames
func (mt *MetaTable) ListCredentialUsernames() (*milvuspb.ListCredUsersResponse, error) {
	usernames, err := mt.catalog.ListCredentials(mt.ctx)
	if err != nil {
		return nil, fmt.Errorf("list credential usernames err:%w", err)
	}
	return &milvuspb.ListCredUsersResponse{Usernames: usernames}, nil
}
