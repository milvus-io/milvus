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

package indexcoord

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/metastore"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	catalog "github.com/milvus-io/milvus/internal/metastore/kv"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
)

// metaTable maintains index-related information
type metaTable struct {
	catalog          metastore.Catalog
	indexLock        sync.RWMutex
	segmentIndexLock sync.RWMutex

	// collectionIndexes records which indexes are on the collection
	collectionIndexes map[UniqueID]map[UniqueID]*model.Index
	// segmentIndexes records which indexes are on the segment
	segmentIndexes map[UniqueID]map[UniqueID]*model.SegmentIndex
	// buildID2Meta records the meta information of the segment
	buildID2SegmentIndex map[UniqueID]*model.SegmentIndex
}

// NewMetaTable is used to create a new meta table.
func NewMetaTable(kv kv.MetaKv) (*metaTable, error) {
	mt := &metaTable{
		catalog: &catalog.Catalog{
			Txn: kv,
		},
		indexLock:        sync.RWMutex{},
		segmentIndexLock: sync.RWMutex{},
	}
	err := mt.reloadFromKV()
	if err != nil {
		return nil, err
	}

	return mt, nil
}

func (mt *metaTable) updateCollectionIndex(index *model.Index) {
	if _, ok := mt.collectionIndexes[index.CollectionID]; !ok {
		mt.collectionIndexes[index.CollectionID] = make(map[UniqueID]*model.Index)
	}
	mt.collectionIndexes[index.CollectionID][index.IndexID] = index
}

func (mt *metaTable) updateSegmentIndex(segIdx *model.SegmentIndex) {
	if _, ok := mt.segmentIndexes[segIdx.SegmentID]; !ok {
		mt.segmentIndexes[segIdx.SegmentID] = make(map[UniqueID]*model.SegmentIndex)
	}
	mt.segmentIndexes[segIdx.SegmentID][segIdx.IndexID] = segIdx
	mt.buildID2SegmentIndex[segIdx.BuildID] = segIdx
}

// reloadFromKV reloads the index meta from ETCD.
func (mt *metaTable) reloadFromKV() error {
	mt.collectionIndexes = make(map[UniqueID]map[UniqueID]*model.Index)
	mt.segmentIndexes = make(map[UniqueID]map[UniqueID]*model.SegmentIndex)
	mt.buildID2SegmentIndex = make(map[UniqueID]*model.SegmentIndex)

	// load field indexes
	log.Info("IndexCoord metaTable reloadFromKV load indexes")
	fieldIndexes, err := mt.catalog.ListIndexes(context.Background())
	if err != nil {
		log.Error("IndexCoord metaTable reloadFromKV load field indexes fail", zap.Error(err))
		return err
	}
	segmentIndxes, err := mt.catalog.ListSegmentIndexes(context.Background())
	if err != nil {
		log.Error("IndexCoord metaTable reloadFromKV load segment indexes fail", zap.Error(err))
		return err
	}

	for _, fieldIndex := range fieldIndexes {
		mt.updateCollectionIndex(fieldIndex)
	}
	for _, segIdx := range segmentIndxes {
		mt.updateSegmentIndex(segIdx)
	}

	log.Info("IndexCoord metaTable reloadFromKV success")
	return nil
}

func (mt *metaTable) saveFieldIndexMeta(index *model.Index) error {
	err := mt.catalog.CreateIndex(context.Background(), index)
	if err != nil {
		log.Error("failed to save index meta in etcd", zap.Int64("buildID", index.CollectionID),
			zap.Int64("fieldID", index.FieldID), zap.Int64("indexID", index.IndexID), zap.Error(err))
		return err
	}

	mt.updateCollectionIndex(index)
	return nil
}

func (mt *metaTable) alterIndexes(indexes []*model.Index) error {
	err := mt.catalog.AlterIndex(context.Background(), indexes)
	if err != nil {
		log.Error("failed to alter index meta in meta store", zap.Int("indexes num", len(indexes)), zap.Error(err))
		return err
	}
	for _, index := range indexes {
		mt.updateCollectionIndex(index)
	}
	return nil
}

func (mt *metaTable) alterSegmentIndexes(segIdxes []*model.SegmentIndex) error {
	err := mt.catalog.AlterSegmentIndex(context.Background(), segIdxes)
	if err != nil {
		log.Error("failed to alter segments index in meta store", zap.Int("segment indexes num", len(segIdxes)),
			zap.Error(err))
		return err
	}
	for _, segIdx := range segIdxes {
		mt.updateSegmentIndex(segIdx)
	}
	return nil
}

// saveSegmentIndexMeta saves the index meta to ETCD.
// metaTable.lock.Lock() before call this function
func (mt *metaTable) saveSegmentIndexMeta(segIdx *model.SegmentIndex) error {
	err := mt.catalog.CreateSegmentIndex(context.Background(), segIdx)
	if err != nil {
		log.Error("failed to save index meta in etcd", zap.Int64("buildID", segIdx.BuildID), zap.Error(err))
		return err
	}

	mt.updateSegmentIndex(segIdx)
	log.Info("IndexCoord metaTable saveIndexMeta success", zap.Int64("buildID", segIdx.BuildID))
	return nil
}

func (mt *metaTable) updateIndexMeta(index *model.Index, updateFunc func(clonedIndex *model.Index) error) error {
	return updateFunc(model.CloneIndex(index))
}

func (mt *metaTable) updateSegIndexMeta(segIdx *model.SegmentIndex, updateFunc func(clonedSegIdx *model.SegmentIndex) error) error {
	return updateFunc(model.CloneSegmentIndex(segIdx))
}

func (mt *metaTable) GetAllIndexMeta() map[int64]*model.SegmentIndex {
	mt.segmentIndexLock.RLock()
	defer mt.segmentIndexLock.RUnlock()

	metas := map[int64]*model.SegmentIndex{}
	for buildID, segIdx := range mt.buildID2SegmentIndex {
		metas[buildID] = model.CloneSegmentIndex(segIdx)
	}

	return metas
}

func (mt *metaTable) GetMeta(buildID UniqueID) (*model.SegmentIndex, bool) {
	mt.segmentIndexLock.RLock()
	defer mt.segmentIndexLock.RUnlock()

	segIdx, ok := mt.buildID2SegmentIndex[buildID]
	if ok {
		return model.CloneSegmentIndex(segIdx), true
	}

	return nil, false
}

func (mt *metaTable) GetTypeParams(collID, indexID UniqueID) ([]*commonpb.KeyValuePair, error) {
	mt.indexLock.RLock()
	defer mt.indexLock.RUnlock()

	fieldIndexes, ok := mt.collectionIndexes[collID]
	if !ok {
		return nil, fmt.Errorf("there is no index on collection: %d", collID)
	}
	index, ok := fieldIndexes[indexID]
	if !ok {
		return nil, fmt.Errorf("there is no index on collection: %d with indexID: %d", collID, indexID)
	}
	typeParams := make([]*commonpb.KeyValuePair, len(index.TypeParams))

	for i, param := range index.TypeParams {
		typeParams[i] = proto.Clone(param).(*commonpb.KeyValuePair)
	}

	return typeParams, nil
}

func (mt *metaTable) GetIndexParams(collID, indexID UniqueID) ([]*commonpb.KeyValuePair, error) {
	mt.indexLock.RLock()
	defer mt.indexLock.RUnlock()

	fieldIndexes, ok := mt.collectionIndexes[collID]
	if !ok {
		return nil, fmt.Errorf("there is no index on collection: %d", collID)
	}
	index, ok := fieldIndexes[indexID]
	if !ok {
		return nil, fmt.Errorf("there is no index on collection: %d with indexID: %d", collID, indexID)
	}
	indexParams := make([]*commonpb.KeyValuePair, len(index.IndexParams))

	for i, param := range index.IndexParams {
		indexParams[i] = proto.Clone(param).(*commonpb.KeyValuePair)
	}

	return indexParams, nil
}

func (mt *metaTable) SetIndex(index *model.Index) {
	mt.indexLock.Lock()
	defer mt.indexLock.Unlock()

	mt.updateCollectionIndex(index)
}

func (mt *metaTable) CreateIndex(index *model.Index) error {
	mt.indexLock.Lock()
	defer mt.indexLock.Unlock()
	log.Info("IndexCoord metaTable CreateIndex", zap.Int64("collectionID", index.CollectionID),
		zap.Int64("fieldID", index.FieldID), zap.Int64("indexID", index.IndexID), zap.String("indexName", index.IndexName))

	if err := mt.saveFieldIndexMeta(index); err != nil {
		log.Error("IndexCoord metaTable CreateIndex save meta fail", zap.Int64("collectionID", index.CollectionID),
			zap.Int64("fieldID", index.FieldID), zap.Int64("indexID", index.IndexID),
			zap.String("indexName", index.IndexName), zap.Error(err))
		return err
	}
	log.Info("IndexCoord metaTable CreateIndex success", zap.Int64("collectionID", index.CollectionID),
		zap.Int64("fieldID", index.FieldID), zap.Int64("indexID", index.IndexID), zap.String("indexName", index.IndexName))
	return nil
}

// AddIndex adds the index meta corresponding the indexBuildID to meta table.
func (mt *metaTable) AddIndex(segIndex *model.SegmentIndex) error {
	mt.segmentIndexLock.Lock()
	defer mt.segmentIndexLock.Unlock()

	buildID := segIndex.BuildID
	log.Info("IndexCoord metaTable AddIndex", zap.Int64("collID", segIndex.CollectionID),
		zap.Int64("segID", segIndex.SegmentID), zap.Int64("indexID", segIndex.IndexID),
		zap.Int64("buildID", buildID))

	_, ok := mt.buildID2SegmentIndex[buildID]
	if ok {
		log.Info("index already exists", zap.Int64("buildID", buildID), zap.Int64("indexID", segIndex.IndexID))
		return nil
	}
	segIndex.IndexState = commonpb.IndexState_Unissued

	metrics.IndexCoordIndexTaskCounter.WithLabelValues(metrics.UnissuedIndexTaskLabel).Inc()
	if err := mt.saveSegmentIndexMeta(segIndex); err != nil {
		// no need to reload, no reason to compare version fail
		log.Error("IndexCoord metaTable save index meta failed", zap.Int64("buildID", buildID),
			zap.Int64("indexID", segIndex.IndexID), zap.Error(err))
		return err
	}
	log.Info("IndexCoord metaTable AddIndex success", zap.Int64("collID", segIndex.CollectionID),
		zap.Int64("segID", segIndex.SegmentID), zap.Int64("indexID", segIndex.IndexID),
		zap.Int64("buildID", buildID))
	return nil
}

func (mt *metaTable) NeedIndex(buildID UniqueID) bool {
	mt.segmentIndexLock.RLock()

	segIdx, ok := mt.buildID2SegmentIndex[buildID]
	if !ok || segIdx.IsDeleted {
		return false
	}
	indexID, collID := segIdx.IndexID, segIdx.CollectionID
	mt.segmentIndexLock.RUnlock()
	mt.indexLock.RLock()
	defer mt.indexLock.RUnlock()

	fieldIndexes, ok := mt.collectionIndexes[collID]
	if !ok {
		return false
	}
	if index, ok := fieldIndexes[indexID]; !ok || index.IsDeleted {
		return false
	}

	return true
}

func (mt *metaTable) canIndex(segIdx *model.SegmentIndex) bool {
	if segIdx.IsDeleted {
		log.Debug("Index has been deleted", zap.Int64("buildID", segIdx.BuildID))
		return false
	}

	if segIdx.NodeID != 0 {
		log.Debug("IndexCoord metaTable BuildIndex, but indexMeta's NodeID is not zero",
			zap.Int64("buildID", segIdx.BuildID), zap.Int64("nodeID", segIdx.NodeID))
		return false
	}
	if segIdx.IndexState != commonpb.IndexState_Unissued {
		log.Debug("IndexCoord metaTable BuildIndex, but indexMeta's state is not unissued",
			zap.Int64("buildID", segIdx.BuildID), zap.String("state", segIdx.IndexState.String()))
		return false
	}
	return true
}

// UpdateVersion updates the version and nodeID of the index meta, whenever the task is built once, the version will be updated once.

func (mt *metaTable) UpdateVersion(buildID UniqueID, nodeID UniqueID) error {
	mt.segmentIndexLock.Lock()
	defer mt.segmentIndexLock.Unlock()

	log.Debug("IndexCoord metaTable UpdateVersion receive", zap.Int64("buildID", buildID), zap.Int64("nodeID", nodeID))
	segIdx, ok := mt.buildID2SegmentIndex[buildID]
	if !ok {
		return fmt.Errorf("there is no index with buildID: %d", buildID)
	}
	if !mt.canIndex(segIdx) {
		return fmt.Errorf("it's no necessary to build index with buildID = %d", buildID)
	}

	updateFunc := func(segIdx *model.SegmentIndex) error {
		segIdx.NodeID = nodeID
		segIdx.IndexVersion++
		return mt.saveSegmentIndexMeta(segIdx)
	}

	return mt.updateSegIndexMeta(segIdx, updateFunc)
}

// BuildIndex set the index state to be InProgress. It means IndexNode is building the index.
func (mt *metaTable) BuildIndex(buildID UniqueID) error {
	mt.segmentIndexLock.Lock()
	defer mt.segmentIndexLock.Unlock()

	segIdx, ok := mt.buildID2SegmentIndex[buildID]
	if !ok {
		return fmt.Errorf("there is no index with buildID: %d", buildID)
	}

	updateFunc := func(segIdx *model.SegmentIndex) error {
		if segIdx.IsDeleted {
			errMsg := fmt.Sprintf("index has been marked deleted, no need to build index with ID: %d", segIdx.BuildID)
			log.Warn(errMsg, zap.Int64("BuildID", segIdx.BuildID))
			return errors.New(errMsg)
		}
		if segIdx.IndexState == commonpb.IndexState_Finished || segIdx.IndexState == commonpb.IndexState_Failed {
			errMsg := fmt.Sprintf("index has been finished, no need to build index with ID: %d", segIdx.BuildID)
			log.Warn(errMsg, zap.Int64("BuildID", segIdx.BuildID), zap.String("state", segIdx.IndexState.String()))
			return errors.New(errMsg)
		}
		segIdx.IndexState = commonpb.IndexState_InProgress

		err := mt.saveSegmentIndexMeta(segIdx)
		if err != nil {
			log.Error("IndexCoord metaTable BuildIndex fail", zap.Int64("buildID", segIdx.BuildID), zap.Error(err))
			return err
		}
		metrics.IndexCoordIndexTaskCounter.WithLabelValues(metrics.UnissuedIndexTaskLabel).Dec()
		metrics.IndexCoordIndexTaskCounter.WithLabelValues(metrics.InProgressIndexTaskLabel).Inc()
		return nil
	}
	return mt.updateSegIndexMeta(segIdx, updateFunc)
}

// GetIndexesForCollection gets all indexes info with the specified collection.
func (mt *metaTable) GetIndexesForCollection(collID UniqueID, indexName string) []*model.Index {
	mt.indexLock.RLock()
	defer mt.indexLock.RUnlock()

	indexInfos := make([]*model.Index, 0)
	for _, index := range mt.collectionIndexes[collID] {
		if index.IsDeleted {
			continue
		}
		if indexName == "" || indexName == index.IndexName {
			indexInfos = append(indexInfos, model.CloneIndex(index))
		}
	}
	return indexInfos
}

func (mt *metaTable) CanCreateIndex(req *indexpb.CreateIndexRequest) bool {
	mt.indexLock.RLock()
	defer mt.indexLock.RUnlock()

	indexes, ok := mt.collectionIndexes[req.CollectionID]
	if !ok {
		return true
	}
	for _, index := range indexes {
		if req.IndexName == index.IndexName {
			if !mt.checkParams(index, req) {
				return false
			}
			return true
		}
	}
	return true
}

func (mt *metaTable) checkParams(fieldIndex *model.Index, req *indexpb.CreateIndexRequest) bool {
	if fieldIndex.IsDeleted {
		return false
	}
	if fieldIndex.FieldID != req.FieldID {
		return false
	}

	if len(fieldIndex.TypeParams) != len(req.TypeParams) {
		return false
	}
	notEq := false
	for _, param1 := range fieldIndex.TypeParams {
		exist := false
		for _, param2 := range req.TypeParams {
			if param2.Key == param1.Key && param2.Value == param1.Value {
				exist = true
			}
		}
		if !exist {
			notEq = true
			break
		}
	}
	if notEq {
		return false
	}
	if len(fieldIndex.IndexParams) != len(req.IndexParams) {
		return false
	}
	for _, param1 := range fieldIndex.IndexParams {
		exist := false
		for _, param2 := range req.IndexParams {
			if param2.Key == param1.Key && param2.Value == param1.Value {
				exist = true
			}
		}
		if !exist {
			notEq = true
			break
		}
	}
	if notEq {
		return false
	}
	return true
}

// HasSameReq determine whether there are same indexing tasks.
func (mt *metaTable) HasSameReq(req *indexpb.CreateIndexRequest) (bool, UniqueID) {
	mt.indexLock.RLock()
	defer mt.indexLock.RUnlock()

	for _, fieldIndex := range mt.collectionIndexes[req.CollectionID] {
		if fieldIndex.IsDeleted {
			continue
		}
		if fieldIndex.FieldID != req.FieldID {
			continue
		}
		if fieldIndex.IndexName != req.IndexName {
			continue
		}

		if len(fieldIndex.TypeParams) != len(req.TypeParams) {
			continue
		}
		notEq := false
		for _, param1 := range fieldIndex.TypeParams {
			exist := false
			for _, param2 := range req.TypeParams {
				if param2.Key == param1.Key && param2.Value == param1.Value {
					exist = true
				}
			}
			if !exist {
				notEq = true
				break
			}
		}
		if notEq {
			continue
		}
		if len(fieldIndex.IndexParams) != len(req.IndexParams) {
			continue
		}
		for _, param1 := range fieldIndex.IndexParams {
			exist := false
			for _, param2 := range req.IndexParams {
				if param2.Key == param1.Key && param2.Value == param1.Value {
					exist = true
				}
			}
			if !exist {
				notEq = true
				break
			}
		}
		if notEq {
			continue
		}
		log.Debug("IndexCoord has same index", zap.Int64("collectionID", req.CollectionID),
			zap.Int64("fieldID", req.FieldID), zap.String("indexName", req.IndexName),
			zap.Int64("indexID", fieldIndex.IndexID))
		return true, fieldIndex.IndexID
	}

	return false, 0
}

func (mt *metaTable) CheckBuiltIndex(segmentID, indexID UniqueID) (bool, UniqueID) {
	mt.segmentIndexLock.RLock()
	defer mt.segmentIndexLock.RUnlock()

	if _, ok := mt.segmentIndexes[segmentID]; !ok {
		return false, 0
	}

	if index, ok := mt.segmentIndexes[segmentID][indexID]; ok {
		return true, index.BuildID
	}

	return false, 0
}

func (mt *metaTable) GetIndexIDByName(collID int64, indexName string) map[int64]uint64 {
	mt.indexLock.RLock()
	defer mt.indexLock.RUnlock()
	indexID2CreateTs := make(map[int64]uint64)

	fieldIndexes, ok := mt.collectionIndexes[collID]
	if !ok {
		return indexID2CreateTs
	}

	for _, index := range fieldIndexes {
		if !index.IsDeleted && (indexName == "" || index.IndexName == indexName) {
			indexID2CreateTs[index.IndexID] = index.CreateTime
		}
	}
	return indexID2CreateTs
}

func (mt *metaTable) GetIndexNameByID(collID, indexID UniqueID) string {
	mt.indexLock.RLock()
	defer mt.indexLock.RUnlock()
	if fieldIndexes, ok := mt.collectionIndexes[collID]; ok {
		if index, ok := fieldIndexes[indexID]; ok {
			return index.IndexName
		}
	}
	return ""
}

type IndexState struct {
	state      commonpb.IndexState
	failReason string
}

// GetIndexStates gets the index states for indexID from meta table.
func (mt *metaTable) GetIndexStates(indexID int64, createTs uint64) []*IndexState {
	mt.segmentIndexLock.RLock()
	defer mt.segmentIndexLock.RUnlock()

	segIndexStates := make([]*IndexState, 0)
	var (
		cntNone       = 0
		cntUnissued   = 0
		cntInProgress = 0
		cntFinished   = 0
		cntFailed     = 0
	)

	for _, indexID2SegIdx := range mt.segmentIndexes {
		segIdx, ok := indexID2SegIdx[indexID]
		if !ok {
			continue
		}
		if segIdx.CreateTime > createTs {
			continue
		}
		if segIdx.IsDeleted {
			// skip deleted index, deleted by compaction
			continue
		}
		switch segIdx.IndexState {
		case commonpb.IndexState_IndexStateNone:
			cntNone++
		case commonpb.IndexState_Unissued:
			cntUnissued++
		case commonpb.IndexState_InProgress:
			cntInProgress++
		case commonpb.IndexState_Finished:
			cntFinished++
		case commonpb.IndexState_Failed:
			cntFailed++
		}
		segIndexStates = append(segIndexStates, &IndexState{
			state:      segIdx.IndexState,
			failReason: segIdx.FailReason,
		})
	}

	log.Debug("IndexCoord get index states success", zap.Int64("indexID", indexID),
		zap.Int("total", len(segIndexStates)), zap.Int("None", cntNone), zap.Int("Unissued", cntUnissued),
		zap.Int("InProgress", cntInProgress), zap.Int("Finished", cntFinished), zap.Int("Failed", cntFailed))

	return segIndexStates
}

func (mt *metaTable) GetSegmentIndexState(segmentID UniqueID, indexID UniqueID) IndexState {
	mt.segmentIndexLock.RLock()
	defer mt.segmentIndexLock.RUnlock()

	if segIdxes, ok := mt.segmentIndexes[segmentID]; ok {
		if segIdx, ok := segIdxes[indexID]; ok && !segIdx.IsDeleted {
			return IndexState{
				state:      segIdx.IndexState,
				failReason: segIdx.FailReason,
			}
		}
	}

	return IndexState{
		state:      commonpb.IndexState_IndexStateNone,
		failReason: "there is no index",
	}
}

// GetIndexBuildProgress gets the index progress for indexID from meta table.
func (mt *metaTable) GetIndexBuildProgress(indexID int64, createTs uint64) (int64, int64) {
	mt.segmentIndexLock.RLock()
	defer mt.segmentIndexLock.RUnlock()

	var (
		totalRows = int64(0)
		indexRows = int64(0)
	)

	for _, indexID2SegIdx := range mt.segmentIndexes {
		segIdx, ok := indexID2SegIdx[indexID]
		if !ok {
			continue
		}
		if segIdx.CreateTime > createTs {
			continue
		}
		if segIdx.IsDeleted {
			// skip deleted index, deleted by compaction
			continue
		}

		totalRows += segIdx.NumRows
		if segIdx.IndexState == commonpb.IndexState_Finished {
			indexRows += segIdx.NumRows
		}
	}

	log.Debug("IndexCoord get index states success", zap.Int64("indexID", indexID),
		zap.Int64("totalRows", totalRows), zap.Int64("indexRows", indexRows))

	return totalRows, indexRows
}

//func (mt *metaTable) DropIndex(collID UniqueID, indexName string) error {
//	log.Info("IndexCoord metaTable DropIndex", zap.Int64("collID", collID), zap.String("indexName", indexName))
//
//	mt.indexLock.Lock()
//	defer mt.indexLock.Unlock()
//	indexes, ok := mt.collectionIndexes[collID]
//	if !ok {
//		return nil
//	}
//	deletedIndexes := make([]*model.Index, 0)
//	for _, index := range indexes {
//		if indexName == "" || indexName == index.IndexName {
//			clonedIndex := model.CloneIndex(index)
//			clonedIndex.IsDeleted = true
//			deletedIndexes = append(deletedIndexes, clonedIndex)
//		}
//	}
//
//	err :=mt.alterIndexes(deletedIndexes)
//	if err != nil {
//		log.Error("IndexCoord metaTable DropIndex fail", zap.Int64("collID", collID),
//			zap.String("indexName", indexName), zap.Error(err))
//		return err
//	}
//
//	log.Info("IndexCoord metaTable MarkIndexAsDeleted success", zap.Int64("collID", collID),
//		zap.String("indexName", indexName))
//	return nil
//}

// MarkIndexAsDeleted will mark the corresponding index as deleted, and recycleUnusedIndexFiles will recycle these tasks.
func (mt *metaTable) MarkIndexAsDeleted(collID UniqueID, indexIDs []UniqueID) error {
	log.Info("IndexCoord metaTable MarkIndexAsDeleted", zap.Int64("collID", collID),
		zap.Int64s("indexIDs", indexIDs))

	mt.indexLock.Lock()
	defer mt.indexLock.Unlock()

	fieldIndexes, ok := mt.collectionIndexes[collID]
	if !ok {
		return nil
	}
	clonedIndexes := make([]*model.Index, 0)
	for _, indexID := range indexIDs {
		index, ok := fieldIndexes[indexID]
		if !ok {
			continue
		}
		clonedIndex := model.CloneIndex(index)
		clonedIndex.IsDeleted = true
		clonedIndexes = append(clonedIndexes, clonedIndex)
	}
	err := mt.alterIndexes(clonedIndexes)
	if err != nil {
		log.Error("IndexCoord metaTable MarkIndexAsDeleted fail", zap.Int64("collID", collID),
			zap.Int64s("indexIDs", indexIDs), zap.Error(err))
		return err
	}

	log.Info("IndexCoord metaTable MarkIndexAsDeleted success", zap.Int64("collID", collID), zap.Int64s("indexIDs", indexIDs))
	return nil
}

// MarkSegIndexAsDeleted will mark the index on the segment corresponding the buildID as deleted, and recycleUnusedSegIndexes will recycle these tasks.
func (mt *metaTable) MarkSegIndexAsDeleted(buildID UniqueID) error {
	log.Info("IndexCoord metaTable MarkSegIndexAsDeleted", zap.Int64("buildID", buildID))

	mt.segmentIndexLock.Lock()
	defer mt.segmentIndexLock.Unlock()

	segIdx, ok := mt.buildID2SegmentIndex[buildID]
	if !ok {
		return nil
	}

	updateFunc := func(segIdx *model.SegmentIndex) error {
		segIdx.IsDeleted = true
		return mt.saveSegmentIndexMeta(segIdx)
	}

	if err := mt.updateSegIndexMeta(segIdx, updateFunc); err != nil {
		log.Warn("IndexCoord metaTable MarkSegIndexAsDeleted fail", zap.Int64("buildID", buildID), zap.Error(err))
		return err
	}

	log.Info("IndexCoord metaTable MarkIndexAsDeleted success", zap.Int64("collID", buildID))
	return nil
}

func (mt *metaTable) MarkSegmentsIndexAsDeleted(segIDs []UniqueID) ([]UniqueID, error) {
	log.Info("IndexCoord metaTable MarkSegmentsIndexAsDeleted", zap.Int64s("segIDs", segIDs))

	mt.segmentIndexLock.Lock()
	defer mt.segmentIndexLock.Unlock()

	buildIDs := make([]UniqueID, 0)
	clonedSegIdxes := make([]*model.SegmentIndex, 0)
	for _, segID := range segIDs {
		if segIndexes, ok := mt.segmentIndexes[segID]; ok {
			for _, segIdx := range segIndexes {
				if segIdx.IsDeleted {
					continue
				}
				clonedSegIdx := model.CloneSegmentIndex(segIdx)
				clonedSegIdx.IsDeleted = true
				clonedSegIdxes = append(clonedSegIdxes, clonedSegIdx)
				buildIDs = append(buildIDs, segIdx.BuildID)
			}
		}
	}
	if len(clonedSegIdxes) == 0 {
		log.Debug("IndexCoord metaTable MarkSegmentsIndexAsDeleted success, already have deleted",
			zap.Int64s("segIDs", segIDs))
		return buildIDs, nil
	}
	err := mt.alterSegmentIndexes(clonedSegIdxes)
	if err != nil {
		log.Error("IndexCoord metaTable MarkSegmentsIndexAsDeleted fail", zap.Int64s("segIDs", segIDs), zap.Error(err))
		return buildIDs, err
	}
	log.Info("IndexCoord metaTable MarkSegmentsIndexAsDeleted success", zap.Int64s("segIDs", segIDs))
	return buildIDs, nil
}

// GetIndexFilePathInfo gets the index file paths from meta table.
func (mt *metaTable) GetIndexFilePathInfo(segID, indexID UniqueID) (*indexpb.IndexFilePathInfo, error) {
	log.Debug("IndexCoord get index file path from meta table", zap.Int64("segmentID", segID))
	mt.segmentIndexLock.RLock()
	defer mt.segmentIndexLock.RUnlock()
	ret := &indexpb.IndexFilePathInfo{
		SegmentID: segID,
	}

	segIndexes, ok := mt.segmentIndexes[segID]
	if !ok {
		return nil, fmt.Errorf("there is no index on segment: %d", segID)
	}
	segIdx, ok := segIndexes[indexID]
	if !ok || segIdx.IsDeleted {
		return nil, fmt.Errorf("there is no index on segment: %d with indexID: %d", segID, indexID)
	}
	if segIdx.IndexState != commonpb.IndexState_Finished {
		return nil, fmt.Errorf("the index state is not finish on segment: %d, index state = %s", segID, segIdx.IndexState.String())
	}

	ret.IndexFilePaths = segIdx.IndexFilePaths
	ret.SerializedSize = segIdx.IndexSize

	log.Debug("IndexCoord get index file path success", zap.Int64("segID", segID),
		zap.Strings("index files num", ret.IndexFilePaths))
	return ret, nil
}

func (mt *metaTable) GetIndexFilePathByBuildID(buildID UniqueID) []string {
	mt.segmentIndexLock.RLock()
	defer mt.segmentIndexLock.RUnlock()
	log.Debug("IndexCoord get index file path from meta table", zap.Int64("buildID", buildID))

	segIdx, ok := mt.buildID2SegmentIndex[buildID]
	if !ok || segIdx.IsDeleted {
		return []string{}
	}

	if segIdx.IndexState != commonpb.IndexState_Finished {
		return []string{}
	}

	log.Debug("IndexCoord get index file path success", zap.Int64("buildID", buildID),
		zap.Strings("index files num", segIdx.IndexFilePaths))
	return segIdx.IndexFilePaths
}

func (mt *metaTable) IsIndexDeleted(collID, indexID UniqueID) bool {
	mt.indexLock.RLock()
	defer mt.indexLock.RUnlock()
	fieldIndexes, ok := mt.collectionIndexes[collID]
	if !ok {
		return true
	}
	if index, ok := fieldIndexes[indexID]; !ok || index.IsDeleted {
		return true
	}
	return false
}

func (mt *metaTable) IsSegIndexDeleted(buildID UniqueID) bool {
	mt.segmentIndexLock.RLock()
	defer mt.segmentIndexLock.RUnlock()

	if segIdx, ok := mt.buildID2SegmentIndex[buildID]; !ok || segIdx.IsDeleted {
		return true
	}
	return false
}

func (mt *metaTable) GetMetasByNodeID(nodeID UniqueID) []*model.SegmentIndex {
	mt.segmentIndexLock.RLock()
	defer mt.segmentIndexLock.RUnlock()

	metas := make([]*model.SegmentIndex, 0)
	for _, meta := range mt.buildID2SegmentIndex {
		if meta.IsDeleted {
			continue
		}
		if nodeID == meta.NodeID {
			metas = append(metas, model.CloneSegmentIndex(meta))
		}
	}
	return metas
}

func (mt *metaTable) GetDeletedSegIndexes() []*model.SegmentIndex {
	mt.segmentIndexLock.RLock()
	defer mt.segmentIndexLock.RUnlock()

	var segIndexes []*model.SegmentIndex
	for _, meta := range mt.buildID2SegmentIndex {
		if meta.IsDeleted {
			segIndexes = append(segIndexes, model.CloneSegmentIndex(meta))
		}
	}
	return segIndexes
}

func (mt *metaTable) GetDeletedIndexes() []*model.Index {
	mt.indexLock.RLock()
	defer mt.indexLock.RUnlock()

	var indexes []*model.Index
	for _, fieldIndexes := range mt.collectionIndexes {
		for _, index := range fieldIndexes {
			if index.IsDeleted {
				indexes = append(indexes, model.CloneIndex(index))
			}
		}
	}
	return indexes
}

func (mt *metaTable) GetBuildIDsForIndexID(indexID UniqueID) []UniqueID {
	mt.segmentIndexLock.RLock()
	defer mt.segmentIndexLock.RUnlock()

	buildIDs := make([]UniqueID, 0)
	for buildID, segIdx := range mt.buildID2SegmentIndex {
		if segIdx.IndexID == indexID {
			buildIDs = append(buildIDs, buildID)
		}
	}
	return buildIDs
}

// RemoveIndex remove the index on the collection from meta table.
func (mt *metaTable) RemoveIndex(collID, indexID UniqueID) error {
	mt.indexLock.Lock()
	defer mt.indexLock.Unlock()

	err := mt.catalog.DropIndex(context.Background(), collID, indexID, mt.collectionIndexes[collID][indexID].CreateTime)
	if err != nil {
		return err
	}

	delete(mt.collectionIndexes[collID], indexID)
	return nil
}

// RemoveSegmentIndex remove the index on the segment from meta table.
func (mt *metaTable) RemoveSegmentIndex(collID, partID, segID, buildID UniqueID) error {
	mt.segmentIndexLock.Lock()
	defer mt.segmentIndexLock.Unlock()

	err := mt.catalog.DropSegmentIndex(context.Background(), collID, partID, segID, buildID)
	if err != nil {
		return err
	}

	segIdx, ok := mt.buildID2SegmentIndex[buildID]
	if !ok {
		return nil
	}
	delete(mt.segmentIndexes[segIdx.SegmentID], segIdx.IndexID)
	delete(mt.buildID2SegmentIndex, buildID)
	return nil
}

// HasBuildID checks if there is an index corresponding the buildID in the meta table.
func (mt *metaTable) HasBuildID(buildID UniqueID) bool {
	mt.segmentIndexLock.RLock()
	defer mt.segmentIndexLock.RUnlock()

	_, ok := mt.buildID2SegmentIndex[buildID]
	return ok
}

// SegmentIndexFinish update the meta state of specified index on the segment.
func (mt *metaTable) SegmentIndexFinish(segIdx *model.SegmentIndex) error {
	mt.segmentIndexLock.Lock()
	defer mt.segmentIndexLock.Unlock()

	_, ok := mt.buildID2SegmentIndex[segIdx.BuildID]
	if !ok {
		log.Warn("index is not exist, Might have been cleaned up meta", zap.Int64("buildID", segIdx.BuildID))
		return nil
	}

	return mt.saveSegmentIndexMeta(model.CloneSegmentIndex(segIdx))
}

// ResetNodeID resets the nodeID of the index meta corresponding the buildID.
func (mt *metaTable) ResetNodeID(buildID UniqueID) error {
	mt.segmentIndexLock.Lock()
	defer mt.segmentIndexLock.Unlock()

	updateFunc := func(sedIdx *model.SegmentIndex) error {
		sedIdx.NodeID = 0
		return mt.saveSegmentIndexMeta(sedIdx)
	}
	segIdx, ok := mt.buildID2SegmentIndex[buildID]
	if !ok {
		return fmt.Errorf("there is no index with buildID: %d", buildID)
	}
	return mt.updateSegIndexMeta(segIdx, updateFunc)
}

// ResetMeta resets the nodeID and index state of the index meta  corresponding the buildID.
func (mt *metaTable) ResetMeta(buildID UniqueID) error {
	mt.segmentIndexLock.Lock()
	defer mt.segmentIndexLock.Unlock()

	segIdx, ok := mt.buildID2SegmentIndex[buildID]
	if !ok {
		return fmt.Errorf("there is no index with buildID: %d", buildID)
	}
	updateFunc := func(sedIdx *model.SegmentIndex) error {
		sedIdx.NodeID = 0
		segIdx.IndexState = commonpb.IndexState_Unissued
		return mt.saveSegmentIndexMeta(sedIdx)
	}

	return mt.updateSegIndexMeta(segIdx, updateFunc)
}

func (mt *metaTable) FinishTask(buildID UniqueID, state commonpb.IndexState, filePaths []string) error {
	mt.segmentIndexLock.Lock()
	defer mt.segmentIndexLock.Unlock()

	segIdx, ok := mt.buildID2SegmentIndex[buildID]
	if !ok || segIdx.IsDeleted {
		return fmt.Errorf("there is no index with buildID: %d", buildID)
	}
	updateFunc := func(segIdx *model.SegmentIndex) error {
		segIdx.IndexState = state
		segIdx.IndexFilePaths = filePaths
		return mt.saveSegmentIndexMeta(segIdx)
	}

	return mt.updateSegIndexMeta(segIdx, updateFunc)
}
