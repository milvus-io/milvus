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
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type indexMeta struct {
	sync.RWMutex
	ctx     context.Context
	catalog metastore.DataCoordCatalog

	// collectionIndexes records which indexes are on the collection
	// collID -> indexID -> index
	indexes map[UniqueID]map[UniqueID]*model.Index
	// buildID2Meta records the meta information of the segment
	// buildID -> segmentIndex
	buildID2SegmentIndex map[UniqueID]*model.SegmentIndex

	// segmentID -> indexID -> segmentIndex
	segmentIndexes map[UniqueID]map[UniqueID]*model.SegmentIndex
}

// NewMeta creates meta from provided `kv.TxnKV`
func newIndexMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (*indexMeta, error) {
	mt := &indexMeta{
		ctx:                  ctx,
		catalog:              catalog,
		indexes:              make(map[UniqueID]map[UniqueID]*model.Index),
		buildID2SegmentIndex: make(map[UniqueID]*model.SegmentIndex),
		segmentIndexes:       make(map[UniqueID]map[UniqueID]*model.SegmentIndex),
	}
	err := mt.reloadFromKV()
	if err != nil {
		return nil, err
	}
	return mt, nil
}

// reloadFromKV loads meta from KV storage
func (m *indexMeta) reloadFromKV() error {
	record := timerecord.NewTimeRecorder("indexMeta-reloadFromKV")
	// load field indexes
	fieldIndexes, err := m.catalog.ListIndexes(m.ctx)
	if err != nil {
		log.Error("indexMeta reloadFromKV load field indexes fail", zap.Error(err))
		return err
	}
	for _, fieldIndex := range fieldIndexes {
		m.updateCollectionIndex(fieldIndex)
	}
	segmentIndexes, err := m.catalog.ListSegmentIndexes(m.ctx)
	if err != nil {
		log.Error("indexMeta reloadFromKV load segment indexes fail", zap.Error(err))
		return err
	}
	for _, segIdx := range segmentIndexes {
		m.updateSegmentIndex(segIdx)
		metrics.FlushedSegmentFileNum.WithLabelValues(metrics.IndexFileLabel).Observe(float64(len(segIdx.IndexFileKeys)))
	}
	log.Info("indexMeta reloadFromKV done", zap.Duration("duration", record.ElapseSpan()))
	return nil
}

func (m *indexMeta) updateCollectionIndex(index *model.Index) {
	if _, ok := m.indexes[index.CollectionID]; !ok {
		m.indexes[index.CollectionID] = make(map[UniqueID]*model.Index)
	}
	m.indexes[index.CollectionID][index.IndexID] = index
}

func (m *indexMeta) updateSegmentIndex(segIdx *model.SegmentIndex) {
	indexes, ok := m.segmentIndexes[segIdx.SegmentID]
	if ok {
		indexes[segIdx.IndexID] = segIdx
	} else {
		m.segmentIndexes[segIdx.SegmentID] = make(map[UniqueID]*model.SegmentIndex)
		m.segmentIndexes[segIdx.SegmentID][segIdx.IndexID] = segIdx
	}
	m.buildID2SegmentIndex[segIdx.BuildID] = segIdx
}

func (m *indexMeta) alterSegmentIndexes(segIdxes []*model.SegmentIndex) error {
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

func (m *indexMeta) updateIndexMeta(index *model.Index, updateFunc func(clonedIndex *model.Index) error) error {
	return updateFunc(model.CloneIndex(index))
}

func (m *indexMeta) updateSegIndexMeta(segIdx *model.SegmentIndex, updateFunc func(clonedSegIdx *model.SegmentIndex) error) error {
	return updateFunc(model.CloneSegmentIndex(segIdx))
}

func (m *indexMeta) updateIndexTasksMetrics() {
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

	useAutoIndex := false
	userIndexParamsWithoutMmapKey := make([]*commonpb.KeyValuePair, 0)
	for _, param := range fieldIndex.UserIndexParams {
		if param.Key == common.MmapEnabledKey {
			continue
		}
		if param.Key == common.IndexTypeKey && param.Value == common.AutoIndexName {
			useAutoIndex = true
		}
		userIndexParamsWithoutMmapKey = append(userIndexParamsWithoutMmapKey, param)
	}

	if len(userIndexParamsWithoutMmapKey) != len(req.GetUserIndexParams()) {
		return false
	}
	for _, param1 := range userIndexParamsWithoutMmapKey {
		exist := false
		for i, param2 := range req.GetUserIndexParams() {
			if param2.Key == param1.Key && param2.Value == param1.Value {
				exist = true
				break
			} else if param1.Key == common.MetricTypeKey && param2.Key == param1.Key && useAutoIndex && !req.GetUserAutoindexMetricTypeSpecified() {
				// when users use autoindex, metric type is the only thing they can specify
				// if they do not specify metric type, will use autoindex default metric type
				// when autoindex default config upgraded, remain the old metric type at the very first time for compatibility
				// warn! replace request metric type
				log.Warn("user not specify autoindex metric type, autoindex config has changed, use old metric for compatibility",
					zap.String("old metric type", param1.Value), zap.String("new metric type", param2.Value))
				req.GetUserIndexParams()[i].Value = param1.Value
				for j, param := range req.GetIndexParams() {
					if param.Key == common.MetricTypeKey {
						req.GetIndexParams()[j].Value = param1.Value
						break
					}
				}
				exist = true
				break
			}
		}
		if !exist {
			notEq = true
			break
		}
	}
	// Check whether new index type match old, if not, only
	// allow autoindex config changed when upgraded to new config
	// using store meta config to rewrite new config
	if !notEq && req.GetIsAutoIndex() && useAutoIndex {
		for _, param1 := range fieldIndex.IndexParams {
			if param1.Key == common.IndexTypeKey &&
				indexparamcheck.IsScalarIndexType(param1.Value) {
				for _, param2 := range req.GetIndexParams() {
					if param1.Key == param2.Key && param1.Value != param2.Value {
						req.IndexParams = make([]*commonpb.KeyValuePair, len(fieldIndex.IndexParams))
						copy(req.IndexParams, fieldIndex.IndexParams)
						break
					}
				}
			}
		}
	}
	log.Info("final request", zap.Any("create index request", req.String()))
	return !notEq
}

func (m *indexMeta) CanCreateIndex(req *indexpb.CreateIndexRequest) (UniqueID, error) {
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
				zap.String("source index", fmt.Sprintf("{index_name: %s, field_id: %d, index_params: %v, user_params: %v, type_params: %v}",
					index.IndexName, index.FieldID, index.IndexParams, index.UserIndexParams, index.TypeParams)),
				zap.String("current index", fmt.Sprintf("{index_name: %s, field_id: %d, index_params: %v, user_params: %v, type_params: %v}",
					req.GetIndexName(), req.GetFieldID(), req.GetIndexParams(), req.GetUserIndexParams(), req.GetTypeParams())))
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
func (m *indexMeta) HasSameReq(req *indexpb.CreateIndexRequest) (bool, UniqueID) {
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

func (m *indexMeta) CreateIndex(index *model.Index) error {
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

func (m *indexMeta) AlterIndex(ctx context.Context, indexes ...*model.Index) error {
	m.Lock()
	defer m.Unlock()

	err := m.catalog.AlterIndexes(ctx, indexes)
	if err != nil {
		return err
	}

	for _, index := range indexes {
		m.updateCollectionIndex(index)
	}

	return nil
}

// AddSegmentIndex adds the index meta corresponding the indexBuildID to meta table.
func (m *indexMeta) AddSegmentIndex(segIndex *model.SegmentIndex) error {
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

func (m *indexMeta) GetIndexIDByName(collID int64, indexName string) map[int64]uint64 {
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

func (m *indexMeta) GetSegmentIndexState(collID, segmentID UniqueID, indexID UniqueID) *indexpb.SegmentIndexState {
	m.RLock()
	defer m.RUnlock()

	state := &indexpb.SegmentIndexState{
		SegmentID:  segmentID,
		State:      commonpb.IndexState_IndexStateNone,
		FailReason: "",
	}
	fieldIndexes, ok := m.indexes[collID]
	if !ok {
		state.FailReason = fmt.Sprintf("collection not exist with ID: %d", collID)
		return state
	}

	indexes, ok := m.segmentIndexes[segmentID]
	if !ok {
		state.State = commonpb.IndexState_Unissued
		state.FailReason = fmt.Sprintf("segment index not exist with ID: %d", segmentID)
		return state
	}

	if index, ok := fieldIndexes[indexID]; ok && !index.IsDeleted {
		if segIdx, ok := indexes[indexID]; ok {
			state.IndexName = index.IndexName
			state.State = segIdx.IndexState
			state.FailReason = segIdx.FailReason
			return state
		}
		state.State = commonpb.IndexState_Unissued
		return state
	}

	state.FailReason = fmt.Sprintf("there is no index on indexID: %d", indexID)
	return state
}

func (m *indexMeta) GetIndexedSegments(collectionID int64, segmentIDs, fieldIDs []UniqueID) []int64 {
	m.RLock()
	defer m.RUnlock()

	fieldIndexes, ok := m.indexes[collectionID]
	if !ok {
		return nil
	}

	fieldIDSet := typeutil.NewUniqueSet(fieldIDs...)

	checkSegmentState := func(indexes map[int64]*model.SegmentIndex) bool {
		indexedFields := 0
		for indexID, index := range fieldIndexes {
			if !fieldIDSet.Contain(index.FieldID) || index.IsDeleted {
				continue
			}

			if segIdx, ok := indexes[indexID]; ok && segIdx.IndexState == commonpb.IndexState_Finished {
				indexedFields += 1
			}
		}

		return indexedFields == fieldIDSet.Len()
	}

	ret := make([]int64, 0)
	for _, sid := range segmentIDs {
		if indexes, ok := m.segmentIndexes[sid]; ok {
			if checkSegmentState(indexes) {
				ret = append(ret, sid)
			}
		}
	}

	return ret
}

// GetIndexesForCollection gets all indexes info with the specified collection.
func (m *indexMeta) GetIndexesForCollection(collID UniqueID, indexName string) []*model.Index {
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

func (m *indexMeta) GetFieldIndexes(collID, fieldID UniqueID, indexName string) []*model.Index {
	m.RLock()
	defer m.RUnlock()

	indexInfos := make([]*model.Index, 0)
	for _, index := range m.indexes[collID] {
		if index.IsDeleted || index.FieldID != fieldID {
			continue
		}
		if indexName == "" || indexName == index.IndexName {
			indexInfos = append(indexInfos, model.CloneIndex(index))
		}
	}
	return indexInfos
}

// MarkIndexAsDeleted will mark the corresponding index as deleted, and recycleUnusedIndexFiles will recycle these tasks.
func (m *indexMeta) MarkIndexAsDeleted(collID UniqueID, indexIDs []UniqueID) error {
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

func (m *indexMeta) IsUnIndexedSegment(collectionID UniqueID, segID UniqueID) bool {
	m.RLock()
	defer m.RUnlock()

	fieldIndexes, ok := m.indexes[collectionID]
	if !ok {
		return false
	}

	// the segment should be unindexed status if the fieldIndexes is not nil
	segIndexInfos, ok := m.segmentIndexes[segID]
	if !ok || len(segIndexInfos) == 0 {
		return true
	}

	for _, index := range fieldIndexes {
		if _, ok := segIndexInfos[index.IndexID]; !index.IsDeleted {
			if !ok {
				// the segment should be unindexed status if the segment index is not found within field indexes
				return true
			}
		}
	}

	return false
}

func (m *indexMeta) getSegmentIndexes(segID UniqueID) map[UniqueID]*model.SegmentIndex {
	m.RLock()
	defer m.RUnlock()

	ret := make(map[UniqueID]*model.SegmentIndex, 0)
	segIndexInfos, ok := m.segmentIndexes[segID]
	if !ok || len(segIndexInfos) == 0 {
		return ret
	}

	for _, segIdx := range segIndexInfos {
		ret[segIdx.IndexID] = model.CloneSegmentIndex(segIdx)
	}
	return ret
}

func (m *indexMeta) GetSegmentIndexes(collectionID UniqueID, segID UniqueID) map[UniqueID]*model.SegmentIndex {
	m.RLock()
	defer m.RUnlock()

	ret := make(map[UniqueID]*model.SegmentIndex, 0)
	segIndexInfos, ok := m.segmentIndexes[segID]
	if !ok || len(segIndexInfos) == 0 {
		return ret
	}

	fieldIndexes, ok := m.indexes[collectionID]
	if !ok {
		return ret
	}

	for _, segIdx := range segIndexInfos {
		if index, ok := fieldIndexes[segIdx.IndexID]; ok && !index.IsDeleted {
			ret[segIdx.IndexID] = model.CloneSegmentIndex(segIdx)
		}
	}
	return ret
}

func (m *indexMeta) GetFieldIDByIndexID(collID, indexID UniqueID) UniqueID {
	m.RLock()
	defer m.RUnlock()

	if fieldIndexes, ok := m.indexes[collID]; ok {
		if index, ok := fieldIndexes[indexID]; ok {
			return index.FieldID
		}
	}
	return 0
}

func (m *indexMeta) GetIndexNameByID(collID, indexID UniqueID) string {
	m.RLock()
	defer m.RUnlock()
	if fieldIndexes, ok := m.indexes[collID]; ok {
		if index, ok := fieldIndexes[indexID]; ok {
			return index.IndexName
		}
	}
	return ""
}

func (m *indexMeta) GetIndexParams(collID, indexID UniqueID) []*commonpb.KeyValuePair {
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

func (m *indexMeta) GetTypeParams(collID, indexID UniqueID) []*commonpb.KeyValuePair {
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

func (m *indexMeta) GetIndexJob(buildID UniqueID) (*model.SegmentIndex, bool) {
	m.RLock()
	defer m.RUnlock()

	segIdx, ok := m.buildID2SegmentIndex[buildID]
	if ok {
		return model.CloneSegmentIndex(segIdx), true
	}

	return nil, false
}

func (m *indexMeta) IsIndexExist(collID, indexID UniqueID) bool {
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
func (m *indexMeta) UpdateVersion(buildID UniqueID) error {
	m.Lock()
	defer m.Unlock()

	log.Debug("IndexCoord metaTable UpdateVersion receive", zap.Int64("buildID", buildID))
	segIdx, ok := m.buildID2SegmentIndex[buildID]
	if !ok {
		return fmt.Errorf("there is no index with buildID: %d", buildID)
	}

	updateFunc := func(segIdx *model.SegmentIndex) error {
		segIdx.IndexVersion++
		return m.alterSegmentIndexes([]*model.SegmentIndex{segIdx})
	}

	return m.updateSegIndexMeta(segIdx, updateFunc)
}

func (m *indexMeta) FinishTask(taskInfo *workerpb.IndexTaskInfo) error {
	m.Lock()
	defer m.Unlock()

	segIdx, ok := m.buildID2SegmentIndex[taskInfo.GetBuildID()]
	if !ok {
		log.Warn("there is no index with buildID", zap.Int64("buildID", taskInfo.GetBuildID()))
		return nil
	}
	updateFunc := func(segIdx *model.SegmentIndex) error {
		segIdx.IndexState = taskInfo.GetState()
		segIdx.IndexFileKeys = common.CloneStringList(taskInfo.GetIndexFileKeys())
		segIdx.FailReason = taskInfo.GetFailReason()
		segIdx.IndexSize = taskInfo.GetSerializedSize()
		segIdx.CurrentIndexVersion = taskInfo.GetCurrentIndexVersion()
		return m.alterSegmentIndexes([]*model.SegmentIndex{segIdx})
	}

	if err := m.updateSegIndexMeta(segIdx, updateFunc); err != nil {
		return err
	}

	log.Info("finish index task success", zap.Int64("buildID", taskInfo.GetBuildID()),
		zap.String("state", taskInfo.GetState().String()), zap.String("fail reason", taskInfo.GetFailReason()),
		zap.Int32("current_index_version", taskInfo.GetCurrentIndexVersion()),
	)
	m.updateIndexTasksMetrics()
	metrics.FlushedSegmentFileNum.WithLabelValues(metrics.IndexFileLabel).Observe(float64(len(taskInfo.GetIndexFileKeys())))
	return nil
}

func (m *indexMeta) DeleteTask(buildID int64) error {
	m.Lock()
	defer m.Unlock()

	segIdx, ok := m.buildID2SegmentIndex[buildID]
	if !ok {
		log.Warn("there is no index with buildID", zap.Int64("buildID", buildID))
		return nil
	}

	updateFunc := func(segIdx *model.SegmentIndex) error {
		segIdx.IsDeleted = true
		return m.alterSegmentIndexes([]*model.SegmentIndex{segIdx})
	}

	if err := m.updateSegIndexMeta(segIdx, updateFunc); err != nil {
		return err
	}

	log.Info("delete index task success", zap.Int64("buildID", buildID))
	m.updateIndexTasksMetrics()
	return nil
}

// BuildIndex set the index state to be InProgress. It means IndexNode is building the index.
func (m *indexMeta) BuildIndex(buildID, nodeID UniqueID) error {
	m.Lock()
	defer m.Unlock()

	segIdx, ok := m.buildID2SegmentIndex[buildID]
	if !ok {
		return fmt.Errorf("there is no index with buildID: %d", buildID)
	}

	updateFunc := func(segIdx *model.SegmentIndex) error {
		segIdx.NodeID = nodeID
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

func (m *indexMeta) GetAllSegIndexes() map[int64]*model.SegmentIndex {
	m.RLock()
	defer m.RUnlock()

	segIndexes := make(map[int64]*model.SegmentIndex, len(m.buildID2SegmentIndex))
	for buildID, segIndex := range m.buildID2SegmentIndex {
		segIndexes[buildID] = model.CloneSegmentIndex(segIndex)
	}
	return segIndexes
}

func (m *indexMeta) RemoveSegmentIndex(collID, partID, segID, indexID, buildID UniqueID) error {
	m.Lock()
	defer m.Unlock()

	err := m.catalog.DropSegmentIndex(m.ctx, collID, partID, segID, buildID)
	if err != nil {
		return err
	}

	if _, ok := m.segmentIndexes[segID]; ok {
		delete(m.segmentIndexes[segID], indexID)
	}

	if len(m.segmentIndexes[segID]) == 0 {
		delete(m.segmentIndexes, segID)
	}

	delete(m.buildID2SegmentIndex, buildID)
	m.updateIndexTasksMetrics()
	return nil
}

func (m *indexMeta) GetDeletedIndexes() []*model.Index {
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

func (m *indexMeta) RemoveIndex(collID, indexID UniqueID) error {
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

func (m *indexMeta) CheckCleanSegmentIndex(buildID UniqueID) (bool, *model.SegmentIndex) {
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

func (m *indexMeta) GetMetasByNodeID(nodeID UniqueID) []*model.SegmentIndex {
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

func (m *indexMeta) getSegmentsIndexStates(collectionID UniqueID, segmentIDs []UniqueID) map[int64]map[int64]*indexpb.SegmentIndexState {
	m.RLock()
	defer m.RUnlock()

	ret := make(map[int64]map[int64]*indexpb.SegmentIndexState, 0)
	fieldIndexes, ok := m.indexes[collectionID]
	if !ok {
		return ret
	}

	for _, segID := range segmentIDs {
		ret[segID] = make(map[int64]*indexpb.SegmentIndexState)
		segIndexInfos, ok := m.segmentIndexes[segID]
		if !ok || len(segIndexInfos) == 0 {
			continue
		}

		for _, segIdx := range segIndexInfos {
			if index, ok := fieldIndexes[segIdx.IndexID]; ok && !index.IsDeleted {
				ret[segID][segIdx.IndexID] = &indexpb.SegmentIndexState{
					SegmentID:  segID,
					State:      segIdx.IndexState,
					FailReason: segIdx.FailReason,
					IndexName:  index.IndexName,
				}
			}
		}
	}

	return ret
}

func (m *indexMeta) GetUnindexedSegments(collectionID int64, segmentIDs []int64) []int64 {
	indexes := m.GetIndexesForCollection(collectionID, "")
	if len(indexes) == 0 {
		// doesn't have index
		return nil
	}
	indexed := make([]int64, 0, len(segmentIDs))
	segIndexStates := m.getSegmentsIndexStates(collectionID, segmentIDs)
	for segmentID, states := range segIndexStates {
		indexStates := lo.Filter(lo.Values(states), func(state *indexpb.SegmentIndexState, _ int) bool {
			return state.GetState() == commonpb.IndexState_Finished
		})
		if len(indexStates) == len(indexes) {
			indexed = append(indexed, segmentID)
		}
	}
	return lo.Without(segmentIDs, indexed...)
}

func (m *indexMeta) AreAllDiskIndex(collectionID int64, schema *schemapb.CollectionSchema) bool {
	indexInfos := m.GetIndexesForCollection(collectionID, "")

	vectorFields := typeutil.GetVectorFieldSchemas(schema)
	fieldIndexTypes := lo.SliceToMap(indexInfos, func(t *model.Index) (int64, indexparamcheck.IndexType) {
		return t.FieldID, GetIndexType(t.IndexParams)
	})
	vectorFieldsWithDiskIndex := lo.Filter(vectorFields, func(field *schemapb.FieldSchema, _ int) bool {
		if indexType, ok := fieldIndexTypes[field.FieldID]; ok {
			return indexparamcheck.IsDiskIndex(indexType)
		}
		return false
	})

	allDiskIndex := len(vectorFields) == len(vectorFieldsWithDiskIndex)
	return allDiskIndex
}
