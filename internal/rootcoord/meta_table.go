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

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore"
	kvmetestore "github.com/milvus-io/milvus/internal/metastore/kv"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/contextutil"
	"github.com/milvus-io/milvus/internal/util/funcutil"
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
	ctx     context.Context
	catalog metastore.Catalog

	collID2Meta         map[typeutil.UniqueID]model.Collection           // collection id -> collection meta
	collName2ID         map[string]typeutil.UniqueID                     // collection name to collection id
	collAlias2ID        map[string]typeutil.UniqueID                     // collection alias to collection id
	partID2IndexedSegID map[typeutil.UniqueID]map[typeutil.UniqueID]bool // partition id -> segment_id -> bool
	segID2IndexID       map[typeutil.UniqueID]typeutil.UniqueID          // segment_id -> index_id
	indexID2Meta        map[typeutil.UniqueID]*model.Index               // collection id/index_id -> meta

	ddLock sync.RWMutex
}

// NewMetaTable creates meta table for rootcoord, which stores all in-memory information
// for collection, partition, segment, index etc.
func NewMetaTable(ctx context.Context, catalog metastore.Catalog) (*MetaTable, error) {
	mt := &MetaTable{
		ctx:     contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName),
		catalog: catalog,
		ddLock:  sync.RWMutex{},
	}
	err := mt.reloadFromCatalog()
	if err != nil {
		return nil, err
	}
	return mt, nil
}

func (mt *MetaTable) reloadFromCatalog() error {
	mt.collID2Meta = make(map[typeutil.UniqueID]model.Collection)
	mt.collName2ID = make(map[string]typeutil.UniqueID)
	mt.collAlias2ID = make(map[string]typeutil.UniqueID)
	mt.partID2IndexedSegID = make(map[typeutil.UniqueID]map[typeutil.UniqueID]bool)
	mt.segID2IndexID = make(map[typeutil.UniqueID]typeutil.UniqueID)
	mt.indexID2Meta = make(map[typeutil.UniqueID]*model.Index)

	collAliases, err := mt.catalog.ListAliases(mt.ctx, 0)
	if err != nil {
		return err
	}
	for _, aliasInfo := range collAliases {
		mt.collAlias2ID[aliasInfo.Name] = aliasInfo.CollectionID
	}

	collMap, err := mt.catalog.ListCollections(mt.ctx, 0)
	if err != nil {
		return err
	}

	for _, coll := range collMap {
		if _, ok := mt.collAlias2ID[coll.Name]; ok {
			continue
		}

		mt.collID2Meta[coll.CollectionID] = *coll
		mt.collName2ID[coll.Name] = coll.CollectionID
	}

	indexes, err := mt.catalog.ListIndexes(mt.ctx)
	if err != nil {
		return err
	}
	for _, index := range indexes {
		for _, segIndexInfo := range index.SegmentIndexes {
			// update partID2IndexedSegID
			segIDMap, ok := mt.partID2IndexedSegID[segIndexInfo.Segment.PartitionID]
			if ok {
				segIDMap[segIndexInfo.Segment.SegmentID] = true
			} else {
				idMap := make(map[typeutil.UniqueID]bool)
				idMap[segIndexInfo.Segment.SegmentID] = true
				mt.partID2IndexedSegID[segIndexInfo.Segment.PartitionID] = idMap
			}

			mt.segID2IndexID[segIndexInfo.Segment.SegmentID] = index.IndexID
		}

		mt.indexID2Meta[index.IndexID] = index
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

	coll.CreateTime = ts
	for _, partition := range coll.Partitions {
		partition.PartitionCreatedTimestamp = ts
	}

	if err := mt.catalog.CreateCollection(mt.ctx, coll, ts); err != nil {
		return err
	}

	mt.collID2Meta[coll.CollectionID] = *coll
	mt.collName2ID[coll.Name] = coll.CollectionID
	return nil
}

// DeleteCollection delete collection
func (mt *MetaTable) DeleteCollection(collID typeutil.UniqueID, ts typeutil.Timestamp, ddOpStr string) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	col, ok := mt.collID2Meta[collID]
	if !ok {
		return fmt.Errorf("can't find collection. id = %d", collID)
	}

	var aliases []string
	// delete collection aliases
	for alias, cid := range mt.collAlias2ID {
		if cid == collID {
			aliases = append(aliases, alias)
		}
	}

	collection := &model.Collection{
		CollectionID: collID,
		Aliases:      aliases,
	}

	if err := mt.catalog.DropCollection(mt.ctx, collection, ts); err != nil {
		return err
	}

	// update segID2IndexID
	for _, partition := range col.Partitions {
		partID := partition.PartitionID
		if segIDMap, ok := mt.partID2IndexedSegID[partID]; ok {
			for segID := range segIDMap {
				delete(mt.segID2IndexID, segID)
			}
		}
		delete(mt.partID2IndexedSegID, partID)
	}

	for _, t := range col.FieldIDToIndexID {
		delete(mt.indexID2Meta, t.Value)
	}

	// delete collection aliases
	for _, alias := range aliases {
		delete(mt.collAlias2ID, alias)
	}

	delete(mt.collID2Meta, collID)
	delete(mt.collName2ID, col.Name)

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

// GetCollectionNameByID returns the collection name according to its ID.
// Returns an error if no matching name is found.
func (mt *MetaTable) GetCollectionNameByID(collectionID typeutil.UniqueID) (string, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	col, ok := mt.collID2Meta[collectionID]
	if !ok {
		return "", fmt.Errorf("can't find collection id : %d", collectionID)
	}
	return col.Name, nil
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
		return col.Clone(), nil
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

		return col.Clone(), nil
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
			cols[collName] = col.Clone()
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

	partition := &model.Partition{
		PartitionID:               partitionID,
		PartitionName:             partitionName,
		PartitionCreatedTimestamp: ts,
		CollectionID:              collID,
	}
	coll.Partitions = append(coll.Partitions, partition)

	if err := mt.catalog.CreatePartition(mt.ctx, partition, ts); err != nil {
		return err
	}

	mt.collID2Meta[collID] = coll
	return nil
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
	if err := mt.catalog.DropPartition(mt.ctx, col.CollectionID, partID, ts); err != nil {
		return 0, err
	}

	// update cache
	mt.collID2Meta[collID] = col
	if segIDMap, ok := mt.partID2IndexedSegID[partID]; ok {
		for segID := range segIDMap {
			indexID, ok := mt.segID2IndexID[segID]
			if !ok {
				continue
			}
			delete(mt.segID2IndexID, segID)

			indexMeta, ok := mt.indexID2Meta[indexID]
			if ok {
				delete(indexMeta.SegmentIndexes, segID)
			}
		}
	}
	delete(mt.partID2IndexedSegID, partID)

	return partID, nil
}

func (mt *MetaTable) updateSegmentIndexMetaCache(oldIndex *model.Index, index *model.Index) error {
	for _, segIdxInfo := range index.SegmentIndexes {
		if _, ok := mt.partID2IndexedSegID[segIdxInfo.PartitionID]; !ok {
			segIDMap := map[typeutil.UniqueID]bool{segIdxInfo.SegmentID: true}
			mt.partID2IndexedSegID[segIdxInfo.PartitionID] = segIDMap
		} else {
			mt.partID2IndexedSegID[segIdxInfo.PartitionID][segIdxInfo.SegmentID] = true
		}

		mt.segID2IndexID[segIdxInfo.SegmentID] = index.IndexID
	}

	for segID, segmentIdx := range index.SegmentIndexes {
		oldIndex.SegmentIndexes[segID] = segmentIdx
	}

	return nil
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
	if !ok || newIndex.IsDeleted {
		log.Error("index id not found or has been deleted", zap.Int64("indexID", newIndex.IndexID))
		return fmt.Errorf("index id = %d not found", newIndex.IndexID)
	}

	if err := mt.catalog.AlterIndex(mt.ctx, oldIndex, newIndex, metastore.ADD); err != nil {
		return err
	}

	err := mt.updateSegmentIndexMetaCache(oldIndex, newIndex)
	return err
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

	var deletedIdxMeta *model.Index
	for _, t := range collMeta.FieldIDToIndexID {
		fieldID := t.Key
		indexID := t.Value
		if fieldID != fieldSch.FieldID {
			continue
		}
		idxMeta, ok := mt.indexID2Meta[indexID]
		if !ok {
			errMsg := fmt.Errorf("index not has meta with ID = %d", indexID)
			log.Error("index id not has meta", zap.Int64("index id", indexID))
			return errMsg
		}
		if idxMeta.IsDeleted {
			continue
		}
		if idxMeta.IndexName != indexName {
			continue
		}
		deletedIdxMeta = idxMeta
	}

	if deletedIdxMeta == nil {
		log.Warn("index not found",
			zap.String("collName", collName),
			zap.String("fieldName", fieldName),
			zap.String("indexName", indexName))
		return nil
	}

	log.Info("mark index deleted",
		zap.String("collName", collName),
		zap.String("fieldName", fieldName),
		zap.String("indexName", indexName),
		zap.Int64("indexID", deletedIdxMeta.IndexID))

	// update metastore
	newIndex := &model.Index{IsDeleted: true}
	if err = mt.catalog.AlterIndex(mt.ctx, deletedIdxMeta, newIndex, metastore.ADD); err != nil {
		return err
	}

	// update cache
	deletedIdxMeta.IsDeleted = true
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
	col, ok := mt.collID2Meta[collID]
	if !ok {
		return 0, false, fmt.Errorf("collection name  = %s not has meta", collName)
	}
	fieldSch, err := mt.getFieldSchemaInternal(collName, fieldName)
	if err != nil {
		return 0, false, err
	}

	fieldIDToIndexID := make([]common.Int64Tuple, 0, len(col.FieldIDToIndexID))
	var dropIdxID typeutil.UniqueID
	for i, t := range col.FieldIDToIndexID {
		fieldID := t.Key
		indexID := t.Value

		if fieldID != fieldSch.FieldID {
			fieldIDToIndexID = append(fieldIDToIndexID, t)
			continue
		}

		idxMeta, ok := mt.indexID2Meta[indexID]
		if !ok || idxMeta.IsDeleted || idxMeta.IndexName != indexName {
			fieldIDToIndexID = append(fieldIDToIndexID, t)
			log.Warn("index id not has meta", zap.Int64("index id", indexID))
			continue
		}
		dropIdxID = indexID
		fieldIDToIndexID = append(fieldIDToIndexID, col.FieldIDToIndexID[i+1:]...)
		break
	}

	if len(fieldIDToIndexID) == len(col.FieldIDToIndexID) {
		log.Warn("drop index,index not found", zap.String("collection name", collName), zap.String("filed name", fieldName), zap.String("index name", indexName))
		return 0, false, nil
	}

	col.FieldIDToIndexID = fieldIDToIndexID

	// update metastore
	err = mt.catalog.DropIndex(mt.ctx, &col, dropIdxID)
	if err != nil {
		return 0, false, err
	}

	// update cache
	mt.collID2Meta[collID] = col
	delete(mt.indexID2Meta, dropIdxID)
	for _, part := range col.Partitions {
		if segIDMap, ok := mt.partID2IndexedSegID[part.PartitionID]; ok {
			for segID := range segIDMap {
				delete(mt.segID2IndexID, segID)
			}
		}
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

	var targetIdxMeta *model.Index
	var indexIDCreateTS uint64
	for _, t := range collMeta.FieldIDToIndexID {
		idxMeta, ok := mt.indexID2Meta[t.Value]
		if ok && idxMeta.IndexName == indexName {
			targetIdxMeta = idxMeta
			indexIDCreateTS = idxMeta.CreateTime
			break
		}
	}

	if targetIdxMeta == nil {
		log.Warn("get init buildIDs, index not found", zap.String("collection name", collName),
			zap.String("index name", indexName))
		return nil, fmt.Errorf("index not found with name = %s in collection %s", indexName, collName)
	}

	initBuildIDs := make([]UniqueID, 0)
	for _, segIndexInfo := range targetIdxMeta.SegmentIndexes {
		if segIndexInfo.EnableIndex && segIndexInfo.CreateTime <= indexIDCreateTS {
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
		segmentIdx, err := mt.getSegIdxMetaBySegID(segID)
		// skip if not found corresponding index meta
		if err != nil {
			continue
		}
		buildIDs = append(buildIDs, segmentIdx.BuildID)
	}
	return buildIDs
}

func (mt *MetaTable) AlignSegmentsMeta(collID, partID UniqueID, segIDs map[UniqueID]struct{}) ([]UniqueID, []UniqueID) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	allIndexID := make([]UniqueID, 0)
	if collMeta, ok := mt.collID2Meta[collID]; ok {
		for _, tuple := range collMeta.FieldIDToIndexID {
			allIndexID = append(allIndexID, tuple.Value)
		}
	}

	recycledSegIDs := make([]UniqueID, 0)
	recycledBuildIDs := make([]UniqueID, 0)
	if segMap, ok := mt.partID2IndexedSegID[partID]; ok {
		for segID := range segMap {
			if _, ok := segIDs[segID]; !ok {
				recycledSegIDs = append(recycledSegIDs, segID)
				segmentIdx, err := mt.getSegIdxMetaBySegID(segID)
				// skip if not found corresponding index meta
				if err != nil {
					continue
				}
				recycledBuildIDs = append(recycledBuildIDs, segmentIdx.BuildID)
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
		for _, tuple := range collMeta.FieldIDToIndexID {
			allIndexID = append(allIndexID, tuple.Value)
		}
	}

	for _, indexID := range allIndexID {
		deletedSegIdx := make(map[int64]model.SegmentIndex, len(segIDs))
		for _, segID := range segIDs {
			deletedSegIdx[segID] = model.SegmentIndex{
				Segment: model.Segment{
					SegmentID:   segID,
					PartitionID: partID,
				},
			}
		}

		deletedIdxMeta := &model.Index{
			CollectionID:   collID,
			IndexID:        indexID,
			SegmentIndexes: deletedSegIdx,
		}

		if err := mt.catalog.AlterIndex(mt.ctx, nil, deletedIdxMeta, metastore.DELETE); err != nil {
			return err
		}
	}

	// update cache
	for _, segID := range segIDs {
		idxID, ok := mt.segID2IndexID[segID]
		if !ok {
			continue
		}

		idxMeta, ok := mt.indexID2Meta[idxID]
		if !ok {
			return fmt.Errorf("index meta not found in collectionID %d with idxID: %d", collID, idxID)
		}

		delete(idxMeta.SegmentIndexes, segID)
		delete(mt.segID2IndexID, segID)
		delete(mt.partID2IndexedSegID[partID], segID)
	}
	return nil
}

func (mt *MetaTable) GetDroppedIndex() map[UniqueID][]UniqueID {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	droppedIndex := make(map[UniqueID][]UniqueID)
	for collID, meta := range mt.collID2Meta {
		for _, tuple := range meta.FieldIDToIndexID {
			if indexMeta, ok := mt.indexID2Meta[tuple.Value]; ok && indexMeta.IsDeleted {
				droppedIndex[collID] = append(droppedIndex[collID], tuple.Value)
			}
		}
	}
	return droppedIndex
}

// RecycleDroppedIndex remove the meta about index which is deleted.
// TODO:: Enable time travel
func (mt *MetaTable) RecycleDroppedIndex() error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	for collID, colMeta := range mt.collID2Meta {
		filedIDToIdxID := Int64TupleSliceToMap(colMeta.FieldIDToIndexID)
		for idx, tuple := range filedIDToIdxID {
			dropIdxID := tuple.Value
			if idxInfo, ok := mt.indexID2Meta[dropIdxID]; !ok || idxInfo.IsDeleted {
				delete(filedIDToIdxID, idx)
				colMeta.FieldIDToIndexID = Int64TupleMapToSlice(filedIDToIdxID)

				// update metastore
				newColMeta := colMeta
				if err := mt.catalog.DropIndex(mt.ctx, &newColMeta, dropIdxID); err != nil {
					return err
				}

				// update cache
				mt.collID2Meta[collID] = colMeta
				delete(mt.indexID2Meta, dropIdxID)
				for _, part := range colMeta.Partitions {
					if segIDMap, ok := mt.partID2IndexedSegID[part.PartitionID]; ok {
						for segID := range segIDMap {
							delete(mt.segID2IndexID, segID)
						}
					}
				}

				log.Debug("recycle dropped index meta", zap.Int64("collID", collID), zap.Int64("indexID", dropIdxID))
			}
		}
	}

	return nil
}

// GetSegmentIndexInfoByID return segment index info by segment id
func (mt *MetaTable) GetSegmentIndexInfoByID(segID typeutil.UniqueID, fieldID int64, idxName string) (model.Index, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	idxMeta, err := mt.getIdxMetaBySegID(segID)
	if err != nil {
		return model.Index{}, err
	}

	// return default index
	if fieldID == -1 && idxName == "" && !idxMeta.IsDeleted && idxMeta.IndexName == Params.CommonCfg.DefaultIndexName {
		return idxMeta, nil
	}

	if idxMeta.IndexName == idxName && !idxMeta.IsDeleted && idxMeta.FieldID == fieldID {
		return idxMeta, nil
	}

	return model.Index{}, fmt.Errorf("can't find index name = %s on segment = %d, with filed id = %d", idxName, segID, fieldID)
}

func (mt *MetaTable) GetSegmentIndexInfos(segID typeutil.UniqueID) (model.Index, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	return mt.getIdxMetaBySegID(segID)
}

// GetFieldSchema return field schema
func (mt *MetaTable) GetFieldSchema(collName string, fieldName string) (model.Field, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	return mt.getFieldSchemaInternal(collName, fieldName)
}

func (mt *MetaTable) getFieldSchemaInternal(collName string, fieldName string) (model.Field, error) {
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
	return mt.isSegmentIndexedInternal(segID, fieldSchema, indexParams)
}

func (mt *MetaTable) isSegmentIndexedInternal(segID typeutil.UniqueID, fieldSchema *model.Field, indexParams []*commonpb.KeyValuePair) bool {
	index, err := mt.getIdxMetaBySegID(segID)
	if err != nil {
		return false
	}

	segIndex, ok := index.SegmentIndexes[segID]
	if ok && !index.IsDeleted &&
		index.FieldID == fieldSchema.FieldID &&
		EqualKeyPairArray(indexParams, index.IndexParams) &&
		segIndex.EnableIndex {
		return true
	}

	return false
}

func (mt *MetaTable) getCollectionInfoInternal(collName string) (model.Collection, error) {
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
	for _, tuple := range collMeta.FieldIDToIndexID {
		if tuple.Key == fieldSchema.FieldID {
			if info, ok := mt.indexID2Meta[tuple.Value]; ok {
				if info.IsDeleted {
					continue
				}

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
					zap.Int64("index id", tuple.Value))
			}
		}
	}
	return nil
}

func (mt *MetaTable) checkFieldIndexDuplicate(collMeta model.Collection, fieldSchema model.Field, idxInfo *model.Index) (duplicate bool, idx *model.Index, err error) {
	for _, t := range collMeta.FieldIDToIndexID {
		if info, ok := mt.indexID2Meta[t.Value]; ok && !info.IsDeleted {
			if info.IndexName == idxInfo.IndexName {
				// the index name must be different for different indexes
				if t.Key != fieldSchema.FieldID || !EqualKeyPairArray(info.IndexParams, idxInfo.IndexParams) {
					return false, nil, fmt.Errorf("index already exists, collection: %s, field: %s, index: %s", collMeta.Name, fieldSchema.Name, idxInfo.IndexName)
				}

				// same index name, index params, and fieldId
				return true, info, nil
			}
		}
	}
	return false, nil, nil
}

// GetNotIndexedSegments return segment ids which have no index
func (mt *MetaTable) GetNotIndexedSegments(collName string, fieldName string, idxInfo *model.Index, segIDs []typeutil.UniqueID) ([]typeutil.UniqueID, model.Field, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	fieldSchema, err := mt.getFieldSchemaInternal(collName, fieldName)
	if err != nil {
		return nil, fieldSchema, err
	}

	rstID := make([]typeutil.UniqueID, 0, 16)
	for _, segID := range segIDs {
		if ok := mt.isSegmentIndexedInternal(segID, &fieldSchema, idxInfo.IndexParams); !ok {
			rstID = append(rstID, segID)
		}
	}
	return rstID, fieldSchema, nil
}

// AddIndex add index
func (mt *MetaTable) AddIndex(colName string, fieldName string, idxInfo *model.Index, segIDs []typeutil.UniqueID) (bool, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	fieldSchema, err := mt.getFieldSchemaInternal(colName, fieldName)
	if err != nil {
		return false, err
	}

	collMeta, err := mt.getCollectionInfoInternal(colName)
	if err != nil {
		// error here if collection not found.
		return false, err
	}

	//TODO:: check index params for scalar field
	// set default index type for scalar index
	if !typeutil.IsVectorType(fieldSchema.DataType) {
		if fieldSchema.DataType == schemapb.DataType_VarChar {
			idxInfo.IndexParams = []*commonpb.KeyValuePair{{Key: "index_type", Value: DefaultStringIndexType}}
		} else {
			idxInfo.IndexParams = []*commonpb.KeyValuePair{{Key: "index_type", Value: DefaultIndexType}}
		}
	}

	if idxInfo.IndexParams == nil {
		return false, fmt.Errorf("index param is nil")
	}

	if err := mt.checkFieldCanBeIndexed(collMeta, fieldSchema, idxInfo); err != nil {
		return false, err
	}

	isDuplicated, dupIdxInfo, err := mt.checkFieldIndexDuplicate(collMeta, fieldSchema, idxInfo)
	if err != nil {
		return isDuplicated, err
	}

	if isDuplicated {
		log.Info("index already exists, update timestamp for IndexID",
			zap.Any("indexTs", idxInfo.CreateTime),
			zap.Int64("indexID", dupIdxInfo.IndexID))
		newIdxMeta := *dupIdxInfo
		newIdxMeta.CreateTime = idxInfo.CreateTime
		if err := mt.catalog.AlterIndex(mt.ctx, dupIdxInfo, &newIdxMeta, metastore.ADD); err != nil {
			return isDuplicated, err
		}
		mt.indexID2Meta[dupIdxInfo.IndexID] = &newIdxMeta
	} else {
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

		tuple := common.Int64Tuple{
			Key:   fieldSchema.FieldID,
			Value: idxInfo.IndexID,
		}
		collMeta.FieldIDToIndexID = append(collMeta.FieldIDToIndexID, tuple)
		if err := mt.catalog.CreateIndex(mt.ctx, &collMeta, idxInfo); err != nil {
			return isDuplicated, err
		}

		mt.collID2Meta[collMeta.CollectionID] = collMeta
		mt.indexID2Meta[idxInfo.IndexID] = idxInfo
	}

	return isDuplicated, nil
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

	rstIndex := make([]model.Index, 0, len(col.FieldIDToIndexID))
	for _, t := range col.FieldIDToIndexID {
		indexID := t.Value
		idxInfo, ok := mt.indexID2Meta[indexID]
		if !ok {
			return model.Collection{}, nil, fmt.Errorf("index id = %d not found", indexID)
		}
		if idxInfo.IsDeleted {
			continue
		}
		if indexName == "" || idxInfo.IndexName == indexName {
			rstIndex = append(rstIndex, *idxInfo)
		}
	}
	return col, rstIndex, nil
}

// GetIndexByID return index info by index id
func (mt *MetaTable) GetIndexByID(indexID typeutil.UniqueID) (*model.Index, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	indexInfo, ok := mt.indexID2Meta[indexID]
	if !ok || indexInfo.IsDeleted {
		return nil, fmt.Errorf("cannot find index, id = %d", indexID)
	}
	return indexInfo, nil
}

func (mt *MetaTable) dupCollectionMeta() map[typeutil.UniqueID]model.Collection {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	return mt.collID2Meta
}

func (mt *MetaTable) dupMeta() (
	map[typeutil.UniqueID]model.Collection,
	map[typeutil.UniqueID]typeutil.UniqueID,
	map[typeutil.UniqueID]model.Index,
) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	collID2Meta := make(map[typeutil.UniqueID]model.Collection, len(mt.collID2Meta))
	segID2IndexID := make(map[typeutil.UniqueID]typeutil.UniqueID, len(mt.segID2IndexID))
	indexID2Meta := make(map[typeutil.UniqueID]model.Index, len(mt.indexID2Meta))
	for k, v := range mt.collID2Meta {
		collID2Meta[k] = v
	}
	for k, v := range mt.segID2IndexID {
		segID2IndexID[k] = v
	}
	for k, v := range mt.indexID2Meta {
		indexID2Meta[k] = *v
	}
	return collID2Meta, segID2IndexID, indexID2Meta
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

	alias := &model.Alias{
		CollectionID: id,
		Name:         collectionAlias,
		CreatedTime:  ts,
	}
	if err := mt.catalog.CreateAlias(mt.ctx, alias, ts); err != nil {
		return err
	}

	mt.collAlias2ID[collectionAlias] = id
	return nil
}

// DropAlias drop collection alias
func (mt *MetaTable) DropAlias(collectionAlias string, ts typeutil.Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	// TODO: drop alias should be idempotent.
	_, ok := mt.collAlias2ID[collectionAlias]
	if !ok {
		return fmt.Errorf("alias does not exist, alias = %s", collectionAlias)
	}

	if err := mt.catalog.DropAlias(mt.ctx, collectionAlias, ts); err != nil {
		return err
	}
	delete(mt.collAlias2ID, collectionAlias)
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

	alias := &model.Alias{
		CollectionID: id,
		Name:         collectionAlias,
		CreatedTime:  ts,
	}
	if err := mt.catalog.AlterAlias(mt.ctx, alias, ts); err != nil {
		return err
	}
	mt.collAlias2ID[collectionAlias] = id
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
	return model.MarshalCredentialModel(credential), err
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

func (mt *MetaTable) getIdxMetaBySegID(segID int64) (model.Index, error) {
	indexID, ok := mt.segID2IndexID[segID]
	if !ok {
		return model.Index{}, fmt.Errorf("segment not found in meta, segment: %d", segID)
	}

	idxMeta, ok := mt.indexID2Meta[indexID]
	if !ok {
		return model.Index{}, fmt.Errorf("segment id: %d not has any index, request index id: %d", segID, indexID)
	}

	return *idxMeta, nil
}

func (mt *MetaTable) getSegIdxMetaBySegID(segID int64) (model.SegmentIndex, error) {
	idxMeta, err := mt.getIdxMetaBySegID(segID)
	if err != nil {
		return model.SegmentIndex{}, err
	}

	segmentIdx, ok := idxMeta.SegmentIndexes[segID]
	if !ok {
		return model.SegmentIndex{}, fmt.Errorf("segment id: %d not has any segment index,"+
			" request index id: %d", segID, idxMeta.IndexID)
	}

	return segmentIdx, nil
}

// CreateRole create role
func (mt *MetaTable) CreateRole(tenant string, entity *milvuspb.RoleEntity) error {
	if funcutil.IsEmptyString(entity.Name) {
		return fmt.Errorf("the role name in the role info is empty")
	}
	return mt.catalog.CreateRole(mt.ctx, tenant, entity)
}

// DropRole drop role info
func (mt *MetaTable) DropRole(tenant string, roleName string) error {
	return mt.catalog.DropRole(mt.ctx, tenant, roleName)
}

// OperateUserRole operate the relationship between a user and a role, including adding a user to a role and removing a user from a role
func (mt *MetaTable) OperateUserRole(tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, operateType milvuspb.OperateUserRoleType) error {
	if funcutil.IsEmptyString(userEntity.Name) {
		return fmt.Errorf("username in the user entity is empty")
	}
	if funcutil.IsEmptyString(roleEntity.Name) {
		return fmt.Errorf("role name in the role entity is empty")
	}

	return mt.catalog.OperateUserRole(mt.ctx, tenant, userEntity, roleEntity, operateType)
}

// SelectRole select role.
// Enter the role condition by the entity param. And this param is nil, which means selecting all roles.
// Get all users that are added to the role by setting the includeUserInfo param to true.
func (mt *MetaTable) SelectRole(tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error) {
	return mt.catalog.SelectRole(mt.ctx, tenant, entity, includeUserInfo)
}

// SelectUser select user.
// Enter the user condition by the entity param. And this param is nil, which means selecting all users.
// Get all roles that are added the user to by setting the includeRoleInfo param to true.
func (mt *MetaTable) SelectUser(tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error) {
	return mt.catalog.SelectUser(mt.ctx, tenant, entity, includeRoleInfo)
}

// OperatePrivilege grant or revoke privilege by setting the operateType param
func (mt *MetaTable) OperatePrivilege(tenant string, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType) error {
	if funcutil.IsEmptyString(entity.ObjectName) {
		return fmt.Errorf("the object name in the grant entity is empty")
	}
	if entity.Object == nil || funcutil.IsEmptyString(entity.Object.Name) {
		return fmt.Errorf("the object entity in the grant entity is invalid")
	}
	if entity.Role == nil || funcutil.IsEmptyString(entity.Role.Name) {
		return fmt.Errorf("the role entity in the grant entity is invalid")
	}
	if entity.Grantor == nil {
		return fmt.Errorf("the grantor in the grant entity is empty")
	}
	if entity.Grantor.Privilege == nil || funcutil.IsEmptyString(entity.Grantor.Privilege.Name) {
		return fmt.Errorf("the privilege name in the grant entity is empty")
	}
	if entity.Grantor.User == nil || funcutil.IsEmptyString(entity.Grantor.User.Name) {
		return fmt.Errorf("the grantor name in the grant entity is empty")
	}
	if !funcutil.IsRevoke(operateType) && !funcutil.IsGrant(operateType) {
		return fmt.Errorf("the operate type in the grant entity is invalid")
	}

	return mt.catalog.OperatePrivilege(mt.ctx, tenant, entity, operateType)
}

// SelectGrant select grant
// The principal entity MUST be not empty in the grant entity
// The resource entity and the resource name are optional, and the two params should be not empty together when you select some grants about the resource kind.
func (mt *MetaTable) SelectGrant(tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error) {
	var entities []*milvuspb.GrantEntity
	if entity.Role == nil || funcutil.IsEmptyString(entity.Role.Name) {
		return entities, fmt.Errorf("the role entity in the grant entity is invalid")
	}
	return mt.catalog.SelectGrant(mt.ctx, tenant, entity)
}

func (mt *MetaTable) ListPolicy(tenant string) ([]string, error) {
	return mt.catalog.ListPolicy(mt.ctx, tenant)
}

func (mt *MetaTable) ListUserRole(tenant string) ([]string, error) {
	return mt.catalog.ListUserRole(mt.ctx, tenant)
}
