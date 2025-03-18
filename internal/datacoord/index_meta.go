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
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/indexparams"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type indexMeta struct {
	ctx     context.Context
	catalog metastore.DataCoordCatalog

	// collectionIndexes records which indexes are on the collection
	// collID -> indexID -> index
	fieldIndexLock sync.RWMutex
	indexes        map[UniqueID]map[UniqueID]*model.Index

	// buildID2Meta records building index meta information of the segment
	segmentBuildInfo *segmentBuildInfo

	// buildID -> lock
	keyLock *lock.KeyLock[UniqueID]
	// segmentID -> indexID -> segmentIndex
	segmentIndexes *typeutil.ConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]
}

func newIndexTaskStats(s *model.SegmentIndex) *metricsinfo.IndexTaskStats {
	return &metricsinfo.IndexTaskStats{
		IndexID:         s.IndexID,
		CollectionID:    s.CollectionID,
		SegmentID:       s.SegmentID,
		BuildID:         s.BuildID,
		IndexState:      s.IndexState.String(),
		FailReason:      s.FailReason,
		IndexSize:       s.IndexMemSize,
		IndexVersion:    s.IndexVersion,
		CreatedUTCTime:  typeutil.TimestampToString(s.CreatedUTCTime * 1000),
		FinishedUTCTime: typeutil.TimestampToString(s.FinishedUTCTime * 1000),
		NodeID:          s.NodeID,
	}
}

type segmentBuildInfo struct {
	// buildID2Meta records the meta information of the segment
	// buildID -> segmentIndex
	buildID2SegmentIndex *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]
	// taskStats records the task stats of the segment
	taskStats *expirable.LRU[UniqueID, *metricsinfo.IndexTaskStats]
}

func newSegmentIndexBuildInfo() *segmentBuildInfo {
	return &segmentBuildInfo{
		// build ID -> segment index
		buildID2SegmentIndex: typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex](),
		// build ID -> task stats
		taskStats: expirable.NewLRU[UniqueID, *metricsinfo.IndexTaskStats](1024, nil, time.Minute*30),
	}
}

func (m *segmentBuildInfo) Add(segIdx *model.SegmentIndex) {
	m.buildID2SegmentIndex.Insert(segIdx.BuildID, segIdx)
	m.taskStats.Add(segIdx.BuildID, newIndexTaskStats(segIdx))
}

func (m *segmentBuildInfo) Get(key UniqueID) (*model.SegmentIndex, bool) {
	value, exists := m.buildID2SegmentIndex.Get(key)
	return value, exists
}

func (m *segmentBuildInfo) Remove(key UniqueID) {
	m.buildID2SegmentIndex.Remove(key)
}

func (m *segmentBuildInfo) List() []*model.SegmentIndex {
	return m.buildID2SegmentIndex.Values()
}

func (m *segmentBuildInfo) GetTaskStats() []*metricsinfo.IndexTaskStats {
	return m.taskStats.Values()
}

// NewMeta creates meta from provided `kv.TxnKV`
func newIndexMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (*indexMeta, error) {
	mt := &indexMeta{
		ctx:              ctx,
		catalog:          catalog,
		indexes:          make(map[UniqueID]map[UniqueID]*model.Index),
		keyLock:          lock.NewKeyLock[UniqueID](),
		segmentBuildInfo: newSegmentIndexBuildInfo(),
		segmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
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
		if segIdx.IndexMemSize == 0 {
			segIdx.IndexMemSize = segIdx.IndexSerializedSize * paramtable.Get().DataCoordCfg.IndexMemSizeEstimateMultiplier.GetAsUint64()
		}
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
	indexes, ok := m.segmentIndexes.Get(segIdx.SegmentID)
	if ok {
		indexes.Insert(segIdx.IndexID, segIdx)
		m.segmentIndexes.Insert(segIdx.SegmentID, indexes)
	} else {
		indexes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		indexes.Insert(segIdx.IndexID, segIdx)
		m.segmentIndexes.Insert(segIdx.SegmentID, indexes)
	}
	m.segmentBuildInfo.Add(segIdx)
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
	taskMetrics := make(map[indexpb.JobState]int)
	taskMetrics[indexpb.JobState_JobStateNone] = 0
	taskMetrics[indexpb.JobState_JobStateInit] = 0
	taskMetrics[indexpb.JobState_JobStateInProgress] = 0
	taskMetrics[indexpb.JobState_JobStateFinished] = 0
	taskMetrics[indexpb.JobState_JobStateFailed] = 0
	taskMetrics[indexpb.JobState_JobStateRetry] = 0
	for _, segIdx := range m.segmentBuildInfo.List() {
		if segIdx.IsDeleted || !m.IsIndexExist(segIdx.CollectionID, segIdx.IndexID) {
			continue
		}

		switch segIdx.IndexState {
		case commonpb.IndexState_IndexStateNone:
			taskMetrics[indexpb.JobState_JobStateNone]++
		case commonpb.IndexState_Unissued:
			taskMetrics[indexpb.JobState_JobStateInit]++
		case commonpb.IndexState_InProgress:
			taskMetrics[indexpb.JobState_JobStateInProgress]++
		case commonpb.IndexState_Finished:
			taskMetrics[indexpb.JobState_JobStateFinished]++
		case commonpb.IndexState_Failed:
			taskMetrics[indexpb.JobState_JobStateFailed]++
		case commonpb.IndexState_Retry:
			taskMetrics[indexpb.JobState_JobStateRetry]++
		}
	}

	jobType := indexpb.JobType_JobTypeIndexJob.String()
	for k, v := range taskMetrics {
		metrics.TaskNum.WithLabelValues(jobType, k.String()).Set(float64(v))
	}
	log.Ctx(m.ctx).Info("update index metric", zap.Int("collectionNum", len(taskMetrics)))
}

func checkJsonParams(index *model.Index, req *indexpb.CreateIndexRequest) bool {
	castType1, err := getIndexParam(index.IndexParams, common.JSONCastTypeKey)
	if err != nil {
		return false
	}
	castType2, err := getIndexParam(req.GetIndexParams(), common.JSONCastTypeKey)
	if err != nil || castType1 != castType2 {
		return false
	}
	jsonPath1, err := getIndexParam(index.IndexParams, common.JSONPathKey)
	if err != nil {
		return false
	}
	jsonPath2, err := getIndexParam(req.GetIndexParams(), common.JSONPathKey)
	return err == nil && jsonPath1 == jsonPath2
}

func checkParams(fieldIndex *model.Index, req *indexpb.CreateIndexRequest) bool {
	metaTypeParams := DeleteParams(fieldIndex.TypeParams, []string{common.MmapEnabledKey})
	reqTypeParams := DeleteParams(req.TypeParams, []string{common.MmapEnabledKey})
	if len(metaTypeParams) != len(reqTypeParams) {
		return false
	}
	notEq := false
	for _, param1 := range metaTypeParams {
		exist := false
		for _, param2 := range reqTypeParams {
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
	userIndexParamsWithoutConfigableKey := make([]*commonpb.KeyValuePair, 0)
	for _, param := range fieldIndex.UserIndexParams {
		if indexparams.IsConfigableIndexParam(param.Key) {
			continue
		}
		if param.Key == common.IndexTypeKey && param.Value == common.AutoIndexName {
			useAutoIndex = true
		}
		userIndexParamsWithoutConfigableKey = append(userIndexParamsWithoutConfigableKey, param)
	}

	if len(userIndexParamsWithoutConfigableKey) != len(req.GetUserIndexParams()) {
		return false
	}
	for _, param1 := range userIndexParamsWithoutConfigableKey {
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

// CanCreateIndex currently is used in Unittest
func (m *indexMeta) CanCreateIndex(req *indexpb.CreateIndexRequest, isJson bool) (UniqueID, error) {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()
	return m.canCreateIndex(req, isJson)
}

func (m *indexMeta) canCreateIndex(req *indexpb.CreateIndexRequest, isJson bool) (UniqueID, error) {
	indexes, ok := m.indexes[req.CollectionID]
	if !ok {
		return 0, nil
	}
	for _, index := range indexes {
		if index.IsDeleted {
			continue
		}
		if req.IndexName == index.IndexName {
			if req.FieldID == index.FieldID && checkParams(index, req) && (!isJson || checkJsonParams(index, req)) {
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
			if isJson {
				// if it is json index, check if json paths are same
				jsonPath1, err := getIndexParam(index.IndexParams, common.JSONPathKey)
				if err != nil {
					return 0, err
				}
				jsonPath2, err := getIndexParam(req.GetIndexParams(), common.JSONPathKey)
				if err != nil {
					return 0, err
				}
				if jsonPath1 != jsonPath2 {
					continue
				}
			}
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
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

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
		log.Ctx(context.TODO()).Debug("has same index", zap.Int64("collectionID", req.CollectionID),
			zap.Int64("fieldID", req.FieldID), zap.String("indexName", req.IndexName),
			zap.Int64("indexID", fieldIndex.IndexID))
		return true, fieldIndex.IndexID
	}

	return false, 0
}

func (m *indexMeta) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest, allocatedIndexID UniqueID, isJson bool) (UniqueID, error) {
	m.fieldIndexLock.Lock()
	defer m.fieldIndexLock.Unlock()

	indexID, err := m.canCreateIndex(req, isJson)
	if err != nil {
		return indexID, err
	}

	if indexID == 0 {
		indexID = allocatedIndexID
	} else {
		return indexID, nil
	}

	// exclude the mmap.enable param, because it will be conflicted with the index's mmap.enable param
	typeParams := DeleteParams(req.GetTypeParams(), []string{common.MmapEnabledKey})
	index := &model.Index{
		CollectionID:    req.GetCollectionID(),
		FieldID:         req.GetFieldID(),
		IndexID:         indexID,
		IndexName:       req.GetIndexName(),
		TypeParams:      typeParams,
		IndexParams:     req.GetIndexParams(),
		CreateTime:      req.GetTimestamp(),
		IsAutoIndex:     req.GetIsAutoIndex(),
		UserIndexParams: req.GetUserIndexParams(),
	}
	if err := ValidateIndexParams(index); err != nil {
		return indexID, err
	}
	log.Ctx(ctx).Info("meta update: CreateIndex", zap.Int64("collectionID", index.CollectionID),
		zap.Int64("fieldID", index.FieldID), zap.Int64("indexID", index.IndexID), zap.String("indexName", index.IndexName))

	if err := m.catalog.CreateIndex(ctx, index); err != nil {
		log.Ctx(ctx).Error("meta update: CreateIndex save meta fail", zap.Int64("collectionID", index.CollectionID),
			zap.Int64("fieldID", index.FieldID), zap.Int64("indexID", index.IndexID),
			zap.String("indexName", index.IndexName), zap.Error(err))
		return indexID, err
	}

	m.updateCollectionIndex(index)
	log.Ctx(ctx).Info("meta update: CreateIndex success", zap.Int64("collectionID", index.CollectionID),
		zap.Int64("fieldID", index.FieldID), zap.Int64("indexID", index.IndexID), zap.String("indexName", index.IndexName))
	return indexID, nil
}

func (m *indexMeta) AlterIndex(ctx context.Context, indexes ...*model.Index) error {
	m.fieldIndexLock.Lock()
	defer m.fieldIndexLock.Unlock()

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
func (m *indexMeta) AddSegmentIndex(ctx context.Context, segIndex *model.SegmentIndex) error {
	buildID := segIndex.BuildID

	m.keyLock.Lock(buildID)
	defer m.keyLock.Unlock(buildID)

	log.Ctx(ctx).Info("meta update: adding segment index", zap.Int64("collectionID", segIndex.CollectionID),
		zap.Int64("segmentID", segIndex.SegmentID), zap.Int64("indexID", segIndex.IndexID),
		zap.Int64("buildID", buildID))

	segIndex.IndexState = commonpb.IndexState_Unissued
	if err := m.catalog.CreateSegmentIndex(ctx, segIndex); err != nil {
		log.Ctx(ctx).Warn("meta update: adding segment index failed",
			zap.Int64("segmentID", segIndex.SegmentID), zap.Int64("indexID", segIndex.IndexID),
			zap.Int64("buildID", segIndex.BuildID), zap.Error(err))
		return err
	}
	m.updateSegmentIndex(segIndex)
	log.Ctx(ctx).Info("meta update: adding segment index success", zap.Int64("collectionID", segIndex.CollectionID),
		zap.Int64("segmentID", segIndex.SegmentID), zap.Int64("indexID", segIndex.IndexID),
		zap.Int64("buildID", buildID))
	return nil
}

func (m *indexMeta) GetIndexIDByName(collID int64, indexName string) map[int64]uint64 {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

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
	state := &indexpb.SegmentIndexState{
		SegmentID:  segmentID,
		State:      commonpb.IndexState_IndexStateNone,
		FailReason: "",
	}

	m.fieldIndexLock.RLock()
	fieldIndexes, ok := m.indexes[collID]
	if !ok {
		state.FailReason = fmt.Sprintf("collection not exist with ID: %d", collID)
		m.fieldIndexLock.RUnlock()
		return state
	}
	m.fieldIndexLock.RUnlock()

	indexes, ok := m.segmentIndexes.Get(segmentID)
	if !ok {
		state.State = commonpb.IndexState_Unissued
		state.FailReason = fmt.Sprintf("segment index not exist with ID: %d", segmentID)
		return state
	}

	if index, ok := fieldIndexes[indexID]; ok && !index.IsDeleted {
		if segIdx, ok := indexes.Get(indexID); ok {
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
	m.fieldIndexLock.RLock()
	fieldIndexes, ok := m.indexes[collectionID]
	if !ok {
		m.fieldIndexLock.RUnlock()
		return nil
	}
	m.fieldIndexLock.RUnlock()

	fieldIDSet := typeutil.NewUniqueSet(fieldIDs...)

	checkSegmentState := func(indexes *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]) bool {
		indexedFields := 0
		for indexID, index := range fieldIndexes {
			if !fieldIDSet.Contain(index.FieldID) || index.IsDeleted {
				continue
			}

			if segIdx, ok := indexes.Get(indexID); ok && segIdx.IndexState == commonpb.IndexState_Finished {
				indexedFields += 1
			}
		}

		return indexedFields == fieldIDSet.Len()
	}

	ret := make([]int64, 0)
	for _, sid := range segmentIDs {
		if indexes, ok := m.segmentIndexes.Get(sid); ok {
			if checkSegmentState(indexes) {
				ret = append(ret, sid)
			}
		}
	}

	return ret
}

// GetIndexesForCollection gets all indexes info with the specified collection.
func (m *indexMeta) GetIndexesForCollection(collID UniqueID, indexName string) []*model.Index {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

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

func (m *indexMeta) getFieldIndexes(collID, fieldID UniqueID, indexName string) []*model.Index {
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

func (m *indexMeta) GetFieldIndexes(collID, fieldID UniqueID, indexName string) []*model.Index {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

	return m.getFieldIndexes(collID, fieldID, indexName)
}

// MarkIndexAsDeleted will mark the corresponding index as deleted, and recycleUnusedIndexFiles will recycle these tasks.
func (m *indexMeta) MarkIndexAsDeleted(ctx context.Context, collID UniqueID, indexIDs []UniqueID) error {
	log.Ctx(ctx).Info("IndexCoord metaTable MarkIndexAsDeleted", zap.Int64("collectionID", collID),
		zap.Int64s("indexIDs", indexIDs))

	m.fieldIndexLock.Lock()
	defer m.fieldIndexLock.Unlock()

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
	err := m.catalog.AlterIndexes(ctx, indexes)
	if err != nil {
		log.Ctx(ctx).Error("failed to alter index meta in meta store", zap.Int("indexes num", len(indexes)), zap.Error(err))
		return err
	}
	for _, index := range indexes {
		m.indexes[index.CollectionID][index.IndexID] = index
	}

	log.Ctx(ctx).Info("IndexCoord metaTable MarkIndexAsDeleted success", zap.Int64("collectionID", collID), zap.Int64s("indexIDs", indexIDs))
	return nil
}

func (m *indexMeta) IsUnIndexedSegment(collectionID UniqueID, segID UniqueID) bool {
	m.fieldIndexLock.RLock()
	fieldIndexes, ok := m.indexes[collectionID]
	if !ok {
		m.fieldIndexLock.RUnlock()
		return false
	}
	m.fieldIndexLock.RUnlock()

	// the segment should be unindexed status if the fieldIndexes is not nil
	segIndexInfos, ok := m.segmentIndexes.Get(segID)
	if !ok || segIndexInfos.Len() == 0 {
		return true
	}

	for _, index := range fieldIndexes {
		if !index.IsDeleted {
			if _, ok := segIndexInfos.Get(index.IndexID); !ok {
				// the segment should be unindexed status if the segment index is not found within field indexes
				return true
			}
		}
	}

	return false
}

func (m *indexMeta) GetSegmentsIndexes(collectionID UniqueID, segIDs []UniqueID) map[int64]map[UniqueID]*model.SegmentIndex {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()
	segmentsIndexes := make(map[int64]map[UniqueID]*model.SegmentIndex)
	for _, segmentID := range segIDs {
		segmentsIndexes[segmentID] = m.getSegmentIndexes(collectionID, segmentID)
	}
	return segmentsIndexes
}

func (m *indexMeta) GetSegmentIndexes(collectionID UniqueID, segID UniqueID) map[UniqueID]*model.SegmentIndex {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()
	return m.getSegmentIndexes(collectionID, segID)
}

// Note: thread-unsafe, don't call it outside indexMeta
func (m *indexMeta) getSegmentIndexes(collectionID UniqueID, segID UniqueID) map[UniqueID]*model.SegmentIndex {
	ret := make(map[UniqueID]*model.SegmentIndex, 0)
	segIndexInfos, ok := m.segmentIndexes.Get(segID)
	if !ok || segIndexInfos.Len() == 0 {
		return ret
	}

	fieldIndexes, ok := m.indexes[collectionID]
	if !ok {
		return ret
	}

	for _, segIdx := range segIndexInfos.Values() {
		if index, ok := fieldIndexes[segIdx.IndexID]; ok && !index.IsDeleted {
			ret[segIdx.IndexID] = model.CloneSegmentIndex(segIdx)
		}
	}
	return ret
}

func (m *indexMeta) GetFieldIDByIndexID(collID, indexID UniqueID) UniqueID {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

	if fieldIndexes, ok := m.indexes[collID]; ok {
		if index, ok := fieldIndexes[indexID]; ok {
			return index.FieldID
		}
	}
	return 0
}

func (m *indexMeta) GetIndexNameByID(collID, indexID UniqueID) string {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

	if fieldIndexes, ok := m.indexes[collID]; ok {
		if index, ok := fieldIndexes[indexID]; ok {
			return index.IndexName
		}
	}
	return ""
}

func (m *indexMeta) GetIndexParams(collID, indexID UniqueID) []*commonpb.KeyValuePair {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

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
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

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
	segIdx, ok := m.segmentBuildInfo.Get(buildID)
	if ok {
		return model.CloneSegmentIndex(segIdx), true
	}

	return nil, false
}

func (m *indexMeta) IsIndexExist(collID, indexID UniqueID) bool {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

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
func (m *indexMeta) UpdateVersion(buildID, nodeID UniqueID) error {
	m.keyLock.Lock(buildID)
	defer m.keyLock.Unlock(buildID)

	log.Ctx(m.ctx).Info("IndexCoord metaTable UpdateVersion receive", zap.Int64("buildID", buildID), zap.Int64("nodeID", nodeID))
	segIdx, ok := m.segmentBuildInfo.Get(buildID)
	if !ok {
		return fmt.Errorf("there is no index with buildID: %d", buildID)
	}

	updateFunc := func(segIdx *model.SegmentIndex) error {
		segIdx.IndexVersion++
		segIdx.NodeID = nodeID
		return m.alterSegmentIndexes([]*model.SegmentIndex{segIdx})
	}

	return m.updateSegIndexMeta(segIdx, updateFunc)
}

func (m *indexMeta) FinishTask(taskInfo *workerpb.IndexTaskInfo) error {
	m.keyLock.Lock(taskInfo.GetBuildID())
	defer m.keyLock.Unlock(taskInfo.GetBuildID())

	segIdx, ok := m.segmentBuildInfo.Get(taskInfo.GetBuildID())
	if !ok {
		log.Ctx(m.ctx).Warn("there is no index with buildID", zap.Int64("buildID", taskInfo.GetBuildID()))
		return nil
	}
	updateFunc := func(segIdx *model.SegmentIndex) error {
		segIdx.IndexState = taskInfo.GetState()
		segIdx.IndexFileKeys = common.CloneStringList(taskInfo.GetIndexFileKeys())
		segIdx.FailReason = taskInfo.GetFailReason()
		segIdx.IndexSerializedSize = taskInfo.GetSerializedSize()
		segIdx.IndexMemSize = taskInfo.GetMemSize()
		segIdx.CurrentIndexVersion = taskInfo.GetCurrentIndexVersion()
		segIdx.FinishedUTCTime = uint64(time.Now().Unix())
		segIdx.CurrentScalarIndexVersion = taskInfo.GetCurrentScalarIndexVersion()
		return m.alterSegmentIndexes([]*model.SegmentIndex{segIdx})
	}

	if err := m.updateSegIndexMeta(segIdx, updateFunc); err != nil {
		return err
	}

	log.Ctx(m.ctx).Info("finish index task success", zap.Int64("buildID", taskInfo.GetBuildID()),
		zap.String("state", taskInfo.GetState().String()), zap.String("fail reason", taskInfo.GetFailReason()),
		zap.Int32("current_index_version", taskInfo.GetCurrentIndexVersion()),
	)
	metrics.FlushedSegmentFileNum.WithLabelValues(metrics.IndexFileLabel).Observe(float64(len(taskInfo.GetIndexFileKeys())))
	return nil
}

func (m *indexMeta) DeleteTask(buildID int64) error {
	m.keyLock.Lock(buildID)
	defer m.keyLock.Unlock(buildID)

	segIdx, ok := m.segmentBuildInfo.Get(buildID)
	if !ok {
		log.Ctx(m.ctx).Warn("there is no index with buildID", zap.Int64("buildID", buildID))
		return nil
	}

	updateFunc := func(segIdx *model.SegmentIndex) error {
		segIdx.IsDeleted = true
		return m.alterSegmentIndexes([]*model.SegmentIndex{segIdx})
	}

	if err := m.updateSegIndexMeta(segIdx, updateFunc); err != nil {
		return err
	}

	log.Ctx(m.ctx).Info("delete index task success", zap.Int64("buildID", buildID))
	return nil
}

// BuildIndex set the index state to be InProgress. It means IndexNode is building the index.
func (m *indexMeta) BuildIndex(buildID UniqueID) error {
	m.keyLock.Lock(buildID)
	defer m.keyLock.Unlock(buildID)

	segIdx, ok := m.segmentBuildInfo.Get(buildID)
	if !ok {
		return fmt.Errorf("there is no index with buildID: %d", buildID)
	}

	updateFunc := func(segIdx *model.SegmentIndex) error {
		segIdx.IndexState = commonpb.IndexState_InProgress

		err := m.alterSegmentIndexes([]*model.SegmentIndex{segIdx})
		if err != nil {
			log.Ctx(m.ctx).Error("meta Update: segment index in progress fail", zap.Int64("buildID", segIdx.BuildID), zap.Error(err))
			return err
		}
		return nil
	}
	if err := m.updateSegIndexMeta(segIdx, updateFunc); err != nil {
		return err
	}
	log.Ctx(m.ctx).Info("meta update: segment index in progress success", zap.Int64("buildID", segIdx.BuildID),
		zap.Int64("segmentID", segIdx.SegmentID))
	return nil
}

func (m *indexMeta) GetAllSegIndexes() map[int64]*model.SegmentIndex {
	tasks := m.segmentBuildInfo.List()
	segIndexes := make(map[int64]*model.SegmentIndex, len(tasks))
	for _, segIndex := range tasks {
		segIndexes[segIndex.BuildID] = segIndex
	}
	return segIndexes
}

// SetStoredIndexFileSizeMetric returns the total index files size of all segment for each collection.
func (m *indexMeta) SetStoredIndexFileSizeMetric(collections map[UniqueID]*collectionInfo) uint64 {
	m.fieldIndexLock.Lock()
	defer m.fieldIndexLock.Unlock()

	var total uint64
	metrics.DataCoordStoredIndexFilesSize.Reset()

	for _, segmentIdx := range m.segmentBuildInfo.List() {
		coll, ok := collections[segmentIdx.CollectionID]
		if ok {
			metrics.DataCoordStoredIndexFilesSize.WithLabelValues(coll.DatabaseName, coll.Schema.GetName(),
				fmt.Sprint(segmentIdx.CollectionID)).Add(float64(segmentIdx.IndexSerializedSize))
			total += segmentIdx.IndexSerializedSize
		}
	}
	return total
}

func (m *indexMeta) removeSegmentIndex(ctx context.Context, collID, partID, segID, indexID, buildID UniqueID) error {
	err := m.catalog.DropSegmentIndex(ctx, collID, partID, segID, buildID)
	if err != nil {
		return err
	}

	segIndexes, ok := m.segmentIndexes.Get(segID)
	if ok {
		segIndexes.Remove(indexID)
		m.segmentIndexes.Insert(segID, segIndexes)
	}
	if segIndexes.Len() == 0 {
		m.segmentIndexes.Remove(segID)
	}

	m.segmentBuildInfo.Remove(buildID)
	return nil
}

func (m *indexMeta) RemoveSegmentIndex(ctx context.Context, collID, partID, segID, indexID, buildID UniqueID) error {
	return m.removeSegmentIndex(ctx, collID, partID, segID, indexID, buildID)
}

func (m *indexMeta) RemoveSegmentIndexByID(ctx context.Context, buildID UniqueID) error {
	segIdx, ok := m.segmentBuildInfo.Get(buildID)
	if !ok {
		return nil
	}

	return m.removeSegmentIndex(ctx, segIdx.CollectionID, segIdx.PartitionID, segIdx.SegmentID, segIdx.IndexID, buildID)
}

func (m *indexMeta) GetDeletedIndexes() []*model.Index {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

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

func (m *indexMeta) RemoveIndex(ctx context.Context, collID, indexID UniqueID) error {
	m.fieldIndexLock.Lock()
	defer m.fieldIndexLock.Unlock()
	log.Ctx(ctx).Info("IndexCoord meta table remove index", zap.Int64("collectionID", collID), zap.Int64("indexID", indexID))
	err := m.catalog.DropIndex(ctx, collID, indexID)
	if err != nil {
		log.Ctx(ctx).Info("IndexCoord meta table remove index fail", zap.Int64("collectionID", collID),
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
	log.Ctx(ctx).Info("IndexCoord meta table remove index success", zap.Int64("collectionID", collID), zap.Int64("indexID", indexID))
	return nil
}

func (m *indexMeta) CheckCleanSegmentIndex(buildID UniqueID) (bool, *model.SegmentIndex) {
	if segIndex, ok := m.segmentBuildInfo.Get(buildID); ok {
		if segIndex.IndexState == commonpb.IndexState_Finished {
			return true, model.CloneSegmentIndex(segIndex)
		}
		return false, model.CloneSegmentIndex(segIndex)
	}
	return true, nil
}

func (m *indexMeta) getSegmentsIndexStates(collectionID UniqueID, segmentIDs []UniqueID) map[int64]map[int64]*indexpb.SegmentIndexState {
	ret := make(map[int64]map[int64]*indexpb.SegmentIndexState, 0)
	fieldIndexes, ok := m.indexes[collectionID]
	if !ok {
		return ret
	}

	for _, segID := range segmentIDs {
		ret[segID] = make(map[int64]*indexpb.SegmentIndexState)
		segIndexInfos, ok := m.segmentIndexes.Get(segID)
		if !ok || segIndexInfos.Len() == 0 {
			continue
		}

		for _, segIdx := range segIndexInfos.Values() {
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
			return vecindexmgr.GetVecIndexMgrInstance().IsDiskVecIndex(indexType)
		}
		return false
	})

	allDiskIndex := len(vectorFields) == len(vectorFieldsWithDiskIndex)
	return allDiskIndex
}

func (m *indexMeta) HasIndex(collectionID int64) bool {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

	indexes, ok := m.indexes[collectionID]
	if ok {
		for _, index := range indexes {
			if !index.IsDeleted {
				return true
			}
		}
	}
	return false
}

func (m *indexMeta) TaskStatsJSON() string {
	tasks := m.segmentBuildInfo.GetTaskStats()
	ret, err := json.Marshal(tasks)
	if err != nil {
		return ""
	}
	return string(ret)
}

func (m *indexMeta) GetIndexJSON(collectionID int64) string {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

	var indexMetrics []*metricsinfo.Index
	for collID, indexes := range m.indexes {
		for _, index := range indexes {
			if collectionID == 0 || collID == collectionID {
				im := &metricsinfo.Index{
					CollectionID:    collID,
					IndexID:         index.IndexID,
					FieldID:         index.FieldID,
					Name:            index.IndexName,
					IsDeleted:       index.IsDeleted,
					CreateTime:      tsoutil.PhysicalTimeFormat(index.CreateTime),
					IndexParams:     funcutil.KeyValuePair2Map(index.IndexParams),
					IsAutoIndex:     index.IsAutoIndex,
					UserIndexParams: funcutil.KeyValuePair2Map(index.UserIndexParams),
				}
				indexMetrics = append(indexMetrics, im)
			}
		}
	}

	ret, err := json.Marshal(indexMetrics)
	if err != nil {
		return ""
	}
	return string(ret)
}

func (m *indexMeta) GetSegmentIndexedFields(collectionID UniqueID, segmentID UniqueID) (bool, []*metricsinfo.IndexedField) {
	m.fieldIndexLock.RLock()
	fieldIndexes, ok := m.indexes[collectionID]
	if !ok {
		// the segment should be unindexed status if the collection has no indexes
		m.fieldIndexLock.RUnlock()
		return false, []*metricsinfo.IndexedField{}
	}
	m.fieldIndexLock.RUnlock()

	// the segment should be unindexed status if the segment indexes is not found
	segIndexInfos, ok := m.segmentIndexes.Get(segmentID)
	if !ok || segIndexInfos.Len() == 0 {
		return false, []*metricsinfo.IndexedField{}
	}

	isIndexed := true
	var segmentIndexes []*metricsinfo.IndexedField
	for _, index := range fieldIndexes {
		if si, ok := segIndexInfos.Get(index.IndexID); !index.IsDeleted {
			buildID := int64(-1)
			if !ok {
				// the segment should be unindexed status if the segment index is not found within field indexes
				isIndexed = false
			} else {
				buildID = si.BuildID
			}

			segmentIndexes = append(segmentIndexes, &metricsinfo.IndexedField{
				IndexFieldID: index.IndexID,
				IndexID:      index.IndexID,
				BuildID:      buildID,
				IndexSize:    int64(si.IndexSerializedSize),
			})
		}
	}
	return isIndexed, segmentIndexes
}
