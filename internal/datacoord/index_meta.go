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

// Package datacoord contains core functions in datacoord
package datacoord

import (
	"fmt"
	"strconv"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

func (m *meta) updateCollectionIndex(index *model.Index) {
	if _, ok := m.indexes[index.CollectionID]; !ok {
		m.indexes[index.CollectionID] = make(map[UniqueID]*model.Index)
	}
	m.indexes[index.CollectionID][index.IndexID] = index
}

func (m *meta) updateSegmentIndex(segIdx *model.SegmentIndex) {
	m.segments.SetSegmentIndex(segIdx.SegmentID, segIdx)
	m.buildID2SegmentIndex[segIdx.BuildID] = segIdx
}

func (m *meta) alterSegmentIndexes(segIdxes []*model.SegmentIndex) error {
	err := m.catalog.AlterSegmentIndexes(m.ctx, segIdxes)
	if err != nil {
		log.Error("failed to alter segments index in meta store", zap.Int("segment indexes num", len(segIdxes)),
			zap.Error(err))
		return err
	}
	for _, segIdx := range segIdxes {
		m.updateSegmentIndex(segIdx)
	}
	return nil
}

func (m *meta) updateIndexMeta(index *model.Index, updateFunc func(clonedIndex *model.Index) error) error {
	return updateFunc(model.CloneIndex(index))
}

func (m *meta) updateSegIndexMeta(segIdx *model.SegmentIndex, updateFunc func(clonedSegIdx *model.SegmentIndex) error) error {
	return updateFunc(model.CloneSegmentIndex(segIdx))
}

func (m *meta) updateIndexTasksMetrics() {
	taskMetrics := make(map[UniqueID]map[commonpb.IndexState]int)
	for _, segIdx := range m.buildID2SegmentIndex {
		if segIdx.IsDeleted {
			continue
		}
		if _, ok := taskMetrics[segIdx.CollectionID]; !ok {
			taskMetrics[segIdx.CollectionID] = make(map[commonpb.IndexState]int)
			taskMetrics[segIdx.CollectionID][commonpb.IndexState_Unissued] = 0
			taskMetrics[segIdx.CollectionID][commonpb.IndexState_InProgress] = 0
			taskMetrics[segIdx.CollectionID][commonpb.IndexState_Finished] = 0
			taskMetrics[segIdx.CollectionID][commonpb.IndexState_Failed] = 0
		}
		taskMetrics[segIdx.CollectionID][segIdx.IndexState]++
	}
	for collID, m := range taskMetrics {
		for k, v := range m {
			switch k {
			case commonpb.IndexState_Unissued:
				metrics.IndexTaskNum.WithLabelValues(strconv.FormatInt(collID, 10), metrics.UnissuedIndexTaskLabel).Set(float64(v))
			case commonpb.IndexState_InProgress:
				metrics.IndexTaskNum.WithLabelValues(strconv.FormatInt(collID, 10), metrics.InProgressIndexTaskLabel).Set(float64(v))
			case commonpb.IndexState_Finished:
				metrics.IndexTaskNum.WithLabelValues(strconv.FormatInt(collID, 10), metrics.FinishedIndexTaskLabel).Set(float64(v))
			case commonpb.IndexState_Failed:
				metrics.IndexTaskNum.WithLabelValues(strconv.FormatInt(collID, 10), metrics.FailedIndexTaskLabel).Set(float64(v))
			}
		}
	}
}

func checkParams(fieldIndex *model.Index, req *indexpb.CreateIndexRequest) bool {
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

	return !notEq
}

func (m *meta) CanCreateIndex(req *indexpb.CreateIndexRequest) (UniqueID, error) {
	m.RLock()
	defer m.RUnlock()

	indexes, ok := m.indexes[req.CollectionID]
	if !ok {
		return 0, nil
	}
	for _, index := range indexes {
		if index.IsDeleted {
			continue
		}
		if req.IndexName == index.IndexName {
			if req.FieldID == index.FieldID && checkParams(index, req) {
				return index.IndexID, nil
			}
			errMsg := "at most one distinct index is allowed per field"
			log.Warn(errMsg,
				zap.String("source index", fmt.Sprintf("{index_name: %s, field_id: %d, index_params: %v, type_params: %v}", index.IndexName, index.FieldID, index.IndexParams, index.TypeParams)),
				zap.String("current index", fmt.Sprintf("{index_name: %s, field_id: %d, index_params: %v, type_params: %v}", req.GetIndexName(), req.GetFieldID(), req.GetIndexParams(), req.GetTypeParams())))
			return 0, fmt.Errorf("CreateIndex failed: %s", errMsg)
		}
		if req.FieldID == index.FieldID {
			// creating multiple indexes on same field is not supported
			errMsg := "CreateIndex failed: creating multiple indexes on same field is not supported"
			log.Warn(errMsg)
			return 0, fmt.Errorf(errMsg)
		}
	}
	return 0, nil
}

// HasSameReq determine whether there are same indexing tasks.
func (m *meta) HasSameReq(req *indexpb.CreateIndexRequest) (bool, UniqueID) {
	m.RLock()
	defer m.RUnlock()

	for _, fieldIndex := range m.indexes[req.CollectionID] {
		if fieldIndex.IsDeleted {
			continue
		}
		if fieldIndex.FieldID != req.FieldID || fieldIndex.IndexName != req.IndexName {
			continue
		}
		if !checkParams(fieldIndex, req) {
			continue
		}
		log.Debug("has same index", zap.Int64("collectionID", req.CollectionID),
			zap.Int64("fieldID", req.FieldID), zap.String("indexName", req.IndexName),
			zap.Int64("indexID", fieldIndex.IndexID))
		return true, fieldIndex.IndexID
	}

	return false, 0
}

func (m *meta) CreateIndex(index *model.Index) error {
	log.Info("meta update: CreateIndex", zap.Int64("collectionID", index.CollectionID),
		zap.Int64("fieldID", index.FieldID), zap.Int64("indexID", index.IndexID), zap.String("indexName", index.IndexName))
	m.Lock()
	defer m.Unlock()

	if err := m.catalog.CreateIndex(m.ctx, index); err != nil {
		log.Error("meta update: CreateIndex save meta fail", zap.Int64("collectionID", index.CollectionID),
			zap.Int64("fieldID", index.FieldID), zap.Int64("indexID", index.IndexID),
			zap.String("indexName", index.IndexName), zap.Error(err))
		return err
	}

	m.updateCollectionIndex(index)
	log.Info("meta update: CreateIndex success", zap.Int64("collectionID", index.CollectionID),
		zap.Int64("fieldID", index.FieldID), zap.Int64("indexID", index.IndexID), zap.String("indexName", index.IndexName))
	return nil
}

// AddSegmentIndex adds the index meta corresponding the indexBuildID to meta table.
func (m *meta) AddSegmentIndex(segIndex *model.SegmentIndex) error {
	m.Lock()
	defer m.Unlock()

	buildID := segIndex.BuildID
	log.Info("meta update: adding segment index", zap.Int64("collectionID", segIndex.CollectionID),
		zap.Int64("segmentID", segIndex.SegmentID), zap.Int64("indexID", segIndex.IndexID),
		zap.Int64("buildID", buildID))

	segIndex.IndexState = commonpb.IndexState_Unissued
	if err := m.catalog.CreateSegmentIndex(m.ctx, segIndex); err != nil {
		log.Warn("meta update: adding segment index failed",
			zap.Int64("segmentID", segIndex.SegmentID), zap.Int64("indexID", segIndex.IndexID),
			zap.Int64("buildID", segIndex.BuildID), zap.Error(err))
		return err
	}
	m.updateSegmentIndex(segIndex)
	log.Info("meta update: adding segment index success", zap.Int64("collectionID", segIndex.CollectionID),
		zap.Int64("segmentID", segIndex.SegmentID), zap.Int64("indexID", segIndex.IndexID),
		zap.Int64("buildID", buildID))
	m.updateIndexTasksMetrics()
	return nil
}

func (m *meta) GetIndexIDByName(collID int64, indexName string) map[int64]uint64 {
	m.RLock()
	defer m.RUnlock()
	indexID2CreateTs := make(map[int64]uint64)

	fieldIndexes, ok := m.indexes[collID]
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

type IndexState struct {
	state      commonpb.IndexState
	failReason string
}

func (m *meta) GetSegmentIndexState(collID, segmentID UniqueID) IndexState {
	m.RLock()
	defer m.RUnlock()

	state := IndexState{
		state:      commonpb.IndexState_IndexStateNone,
		failReason: "",
	}
	fieldIndexes, ok := m.indexes[collID]
	if !ok {
		state.failReason = fmt.Sprintf("collection not exist with ID: %d", collID)
		return state
	}
	segment := m.segments.GetSegment(segmentID)
	if segment != nil {
		for indexID, index := range fieldIndexes {
			if !index.IsDeleted {
				if segIdx, ok := segment.segmentIndexes[indexID]; ok {
					if segIdx.IndexState != commonpb.IndexState_Finished {
						state.state = segIdx.IndexState
						state.failReason = segIdx.FailReason
						break
					}
					state.state = commonpb.IndexState_Finished
					continue
				}
				state.state = commonpb.IndexState_Unissued
				break
			}
		}
		return state
	}
	state.failReason = fmt.Sprintf("segment is not exist with ID: %d", segmentID)
	return state
}

func (m *meta) GetSegmentIndexStateOnField(collID, segmentID, fieldID UniqueID) IndexState {
	m.RLock()
	defer m.RUnlock()

	state := IndexState{
		state:      commonpb.IndexState_IndexStateNone,
		failReason: "",
	}
	fieldIndexes, ok := m.indexes[collID]
	if !ok {
		state.failReason = fmt.Sprintf("collection not exist with ID: %d", collID)
		return state
	}
	segment := m.segments.GetSegment(segmentID)
	if segment != nil {
		for indexID, index := range fieldIndexes {
			if index.FieldID == fieldID && !index.IsDeleted {
				if segIdx, ok := segment.segmentIndexes[indexID]; ok {
					state.state = segIdx.IndexState
					state.failReason = segIdx.FailReason
					return state
				}
				state.state = commonpb.IndexState_Unissued
				return state
			}
		}
		state.failReason = fmt.Sprintf("there is no index on fieldID: %d", fieldID)
		return state
	}
	state.failReason = fmt.Sprintf("segment is not exist with ID: %d", segmentID)
	return state
}

// GetIndexesForCollection gets all indexes info with the specified collection.
func (m *meta) GetIndexesForCollection(collID UniqueID, indexName string) []*model.Index {
	m.RLock()
	defer m.RUnlock()

	indexInfos := make([]*model.Index, 0)
	for _, index := range m.indexes[collID] {
		if index.IsDeleted {
			continue
		}
		if indexName == "" || indexName == index.IndexName {
			indexInfos = append(indexInfos, model.CloneIndex(index))
		}
	}
	return indexInfos
}

// MarkIndexAsDeleted will mark the corresponding index as deleted, and recycleUnusedIndexFiles will recycle these tasks.
func (m *meta) MarkIndexAsDeleted(collID UniqueID, indexIDs []UniqueID) error {
	log.Info("IndexCoord metaTable MarkIndexAsDeleted", zap.Int64("collectionID", collID),
		zap.Int64s("indexIDs", indexIDs))

	m.Lock()
	defer m.Unlock()

	fieldIndexes, ok := m.indexes[collID]
	if !ok {
		return nil
	}
	indexes := make([]*model.Index, 0)
	for _, indexID := range indexIDs {
		index, ok := fieldIndexes[indexID]
		if !ok || index.IsDeleted {
			continue
		}
		clonedIndex := model.CloneIndex(index)
		clonedIndex.IsDeleted = true
		indexes = append(indexes, clonedIndex)
	}
	if len(indexes) == 0 {
		return nil
	}
	err := m.catalog.AlterIndexes(m.ctx, indexes)
	if err != nil {
		log.Error("failed to alter index meta in meta store", zap.Int("indexes num", len(indexes)), zap.Error(err))
		return err
	}
	for _, index := range indexes {
		m.indexes[index.CollectionID][index.IndexID] = index
	}

	log.Info("IndexCoord metaTable MarkIndexAsDeleted success", zap.Int64("collectionID", collID), zap.Int64s("indexIDs", indexIDs))
	return nil
}

func (m *meta) GetSegmentIndexes(segID UniqueID) []*model.SegmentIndex {
	m.RLock()
	defer m.RUnlock()

	segIndexInfos := make([]*model.SegmentIndex, 0)
	segment := m.segments.GetSegment(segID)
	if segment == nil {
		return segIndexInfos
	}
	fieldIndex, ok := m.indexes[segment.CollectionID]
	if !ok {
		return segIndexInfos
	}

	for _, segIdx := range segment.segmentIndexes {
		if index, ok := fieldIndex[segIdx.IndexID]; ok && !index.IsDeleted {
			segIndexInfos = append(segIndexInfos, model.CloneSegmentIndex(segIdx))
		}
	}
	return segIndexInfos
}

func (m *meta) GetFieldIDByIndexID(collID, indexID UniqueID) UniqueID {
	m.RLock()
	defer m.RUnlock()

	if fieldIndexes, ok := m.indexes[collID]; ok {
		if index, ok := fieldIndexes[indexID]; ok {
			return index.FieldID
		}
	}
	return 0
}

func (m *meta) GetIndexNameByID(collID, indexID UniqueID) string {
	m.RLock()
	defer m.RUnlock()
	if fieldIndexes, ok := m.indexes[collID]; ok {
		if index, ok := fieldIndexes[indexID]; ok {
			return index.IndexName
		}
	}
	return ""
}

func (m *meta) GetIndexParams(collID, indexID UniqueID) []*commonpb.KeyValuePair {
	m.RLock()
	defer m.RUnlock()

	fieldIndexes, ok := m.indexes[collID]
	if !ok {
		return nil
	}
	index, ok := fieldIndexes[indexID]
	if !ok {
		return nil
	}
	indexParams := make([]*commonpb.KeyValuePair, 0, len(index.IndexParams))

	for _, param := range index.IndexParams {
		indexParams = append(indexParams, proto.Clone(param).(*commonpb.KeyValuePair))
	}

	return indexParams
}

func (m *meta) GetTypeParams(collID, indexID UniqueID) []*commonpb.KeyValuePair {
	m.RLock()
	defer m.RUnlock()

	fieldIndexes, ok := m.indexes[collID]
	if !ok {
		return nil
	}
	index, ok := fieldIndexes[indexID]
	if !ok {
		return nil
	}
	typeParams := make([]*commonpb.KeyValuePair, 0, len(index.TypeParams))

	for _, param := range index.TypeParams {
		typeParams = append(typeParams, proto.Clone(param).(*commonpb.KeyValuePair))
	}

	return typeParams
}

func (m *meta) GetIndexJob(buildID UniqueID) (*model.SegmentIndex, bool) {
	m.RLock()
	defer m.RUnlock()

	segIdx, ok := m.buildID2SegmentIndex[buildID]
	if ok {
		return model.CloneSegmentIndex(segIdx), true
	}

	return nil, false
}

func (m *meta) IsIndexExist(collID, indexID UniqueID) bool {
	m.RLock()
	defer m.RUnlock()

	fieldIndexes, ok := m.indexes[collID]
	if !ok {
		return false
	}
	if index, ok := fieldIndexes[indexID]; !ok || index.IsDeleted {
		return false
	}

	return true
}

// UpdateVersion updates the version and nodeID of the index meta, whenever the task is built once, the version will be updated once.
func (m *meta) UpdateVersion(buildID UniqueID, nodeID UniqueID) error {
	m.Lock()
	defer m.Unlock()

	log.Debug("IndexCoord metaTable UpdateVersion receive", zap.Int64("buildID", buildID), zap.Int64("nodeID", nodeID))
	segIdx, ok := m.buildID2SegmentIndex[buildID]
	if !ok {
		return fmt.Errorf("there is no index with buildID: %d", buildID)
	}

	updateFunc := func(segIdx *model.SegmentIndex) error {
		segIdx.NodeID = nodeID
		segIdx.IndexVersion++
		return m.alterSegmentIndexes([]*model.SegmentIndex{segIdx})
	}

	return m.updateSegIndexMeta(segIdx, updateFunc)
}

func (m *meta) FinishTask(taskInfo *indexpb.IndexTaskInfo) error {
	m.Lock()
	defer m.Unlock()

	segIdx, ok := m.buildID2SegmentIndex[taskInfo.BuildID]
	if !ok {
		log.Warn("there is no index with buildID", zap.Int64("buildID", taskInfo.BuildID))
		return nil
	}
	updateFunc := func(segIdx *model.SegmentIndex) error {
		segIdx.IndexState = taskInfo.State
		segIdx.IndexFileKeys = common.CloneStringList(taskInfo.IndexFileKeys)
		segIdx.FailReason = taskInfo.FailReason
		segIdx.IndexSize = taskInfo.SerializedSize
		return m.alterSegmentIndexes([]*model.SegmentIndex{segIdx})
	}

	if err := m.updateSegIndexMeta(segIdx, updateFunc); err != nil {
		return err
	}

	log.Info("finish index task success", zap.Int64("buildID", taskInfo.BuildID),
		zap.String("state", taskInfo.GetState().String()), zap.String("fail reason", taskInfo.GetFailReason()))
	m.updateIndexTasksMetrics()
	metrics.FlushedSegmentFileNum.WithLabelValues(metrics.IndexFileLabel).Observe(float64(len(taskInfo.IndexFileKeys)))
	return nil
}

// BuildIndex set the index state to be InProgress. It means IndexNode is building the index.
func (m *meta) BuildIndex(buildID UniqueID) error {
	m.Lock()
	defer m.Unlock()

	segIdx, ok := m.buildID2SegmentIndex[buildID]
	if !ok {
		return fmt.Errorf("there is no index with buildID: %d", buildID)
	}

	updateFunc := func(segIdx *model.SegmentIndex) error {
		segIdx.IndexState = commonpb.IndexState_InProgress

		err := m.alterSegmentIndexes([]*model.SegmentIndex{segIdx})
		if err != nil {
			log.Error("meta Update: segment index in progress fail", zap.Int64("buildID", segIdx.BuildID), zap.Error(err))
			return err
		}
		return nil
	}
	if err := m.updateSegIndexMeta(segIdx, updateFunc); err != nil {
		return err
	}
	log.Info("meta update: segment index in progress success", zap.Int64("buildID", segIdx.BuildID),
		zap.Int64("segmentID", segIdx.SegmentID))

	m.updateIndexTasksMetrics()
	return nil
}

func (m *meta) GetAllSegIndexes() map[int64]*model.SegmentIndex {
	m.RLock()
	defer m.RUnlock()

	segIndexes := make(map[int64]*model.SegmentIndex, len(m.buildID2SegmentIndex))
	for buildID, segIndex := range m.buildID2SegmentIndex {
		segIndexes[buildID] = model.CloneSegmentIndex(segIndex)
	}
	return segIndexes
}

func (m *meta) RemoveSegmentIndex(collID, partID, segID, indexID, buildID UniqueID) error {
	m.Lock()
	defer m.Unlock()

	err := m.catalog.DropSegmentIndex(m.ctx, collID, partID, segID, buildID)
	if err != nil {
		return err
	}

	m.segments.DropSegmentIndex(segID, indexID)
	delete(m.buildID2SegmentIndex, buildID)
	m.updateIndexTasksMetrics()
	return nil
}

func (m *meta) GetDeletedIndexes() []*model.Index {
	m.RLock()
	defer m.RUnlock()

	deletedIndexes := make([]*model.Index, 0)
	for _, fieldIndexes := range m.indexes {
		for _, index := range fieldIndexes {
			if index.IsDeleted {
				deletedIndexes = append(deletedIndexes, index)
			}
		}
	}
	return deletedIndexes
}

func (m *meta) RemoveIndex(collID, indexID UniqueID) error {
	m.Lock()
	defer m.Unlock()
	log.Info("IndexCoord meta table remove index", zap.Int64("collectionID", collID), zap.Int64("indexID", indexID))
	err := m.catalog.DropIndex(m.ctx, collID, indexID)
	if err != nil {
		log.Info("IndexCoord meta table remove index fail", zap.Int64("collectionID", collID),
			zap.Int64("indexID", indexID), zap.Error(err))
		return err
	}

	delete(m.indexes[collID], indexID)
	if len(m.indexes[collID]) == 0 {
		delete(m.indexes, collID)
		metrics.IndexTaskNum.Delete(prometheus.Labels{"collection_id": strconv.FormatInt(collID, 10), "index_task_status": metrics.UnissuedIndexTaskLabel})
		metrics.IndexTaskNum.Delete(prometheus.Labels{"collection_id": strconv.FormatInt(collID, 10), "index_task_status": metrics.InProgressIndexTaskLabel})
		metrics.IndexTaskNum.Delete(prometheus.Labels{"collection_id": strconv.FormatInt(collID, 10), "index_task_status": metrics.FinishedIndexTaskLabel})
		metrics.IndexTaskNum.Delete(prometheus.Labels{"collection_id": strconv.FormatInt(collID, 10), "index_task_status": metrics.FailedIndexTaskLabel})
	}
	log.Info("IndexCoord meta table remove index success", zap.Int64("collectionID", collID), zap.Int64("indexID", indexID))
	return nil
}

func (m *meta) CleanSegmentIndex(buildID UniqueID) (bool, *model.SegmentIndex) {
	m.RLock()
	defer m.RUnlock()

	if segIndex, ok := m.buildID2SegmentIndex[buildID]; ok {
		if segIndex.IndexState == commonpb.IndexState_Finished {
			return true, model.CloneSegmentIndex(segIndex)
		}
		return false, model.CloneSegmentIndex(segIndex)
	}
	return true, nil
}

func (m *meta) GetHasUnindexTaskSegments() []*SegmentInfo {
	m.RLock()
	defer m.RUnlock()
	segments := m.segments.GetSegments()
	var ret []*SegmentInfo
	for _, segment := range segments {
		if !isFlush(segment) {
			continue
		}
		if fieldIndexes, ok := m.indexes[segment.CollectionID]; ok {
			for _, index := range fieldIndexes {
				if _, ok := segment.segmentIndexes[index.IndexID]; !index.IsDeleted && !ok {
					ret = append(ret, segment)
				}
			}
		}
	}
	return ret
}

func (m *meta) GetMetasByNodeID(nodeID UniqueID) []*model.SegmentIndex {
	m.RLock()
	defer m.RUnlock()

	metas := make([]*model.SegmentIndex, 0)
	for _, segIndex := range m.buildID2SegmentIndex {
		if segIndex.IsDeleted {
			continue
		}
		if nodeID == segIndex.NodeID {
			metas = append(metas, model.CloneSegmentIndex(segIndex))
		}
	}
	return metas
}
