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

	"github.com/cockroachdb/errors"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/indexparams"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var errIndexOperationIgnored = errors.New("index operation ignored")

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

const taskStatsLRUCapacity = 1024

func newSegmentIndexBuildInfo() *segmentBuildInfo {
	return &segmentBuildInfo{
		// build ID -> segment index
		buildID2SegmentIndex: typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex](),
		// build ID -> task stats
		taskStats: expirable.NewLRU[UniqueID, *metricsinfo.IndexTaskStats](taskStatsLRUCapacity, nil, time.Minute*30),
	}
}

func (m *segmentBuildInfo) Add(segIdx *model.SegmentIndex) {
	m.buildID2SegmentIndex.Insert(segIdx.BuildID, segIdx)
	m.taskStats.Add(segIdx.BuildID, newIndexTaskStats(segIdx))
}

// AddForRecovery inserts segment index during recovery. It skips the LRU write
// only when the LRU is already full and the index task is finished, since
// finished tasks are unlikely to be queried and the LRU write is expensive
// during bulk recovery.
func (m *segmentBuildInfo) AddForRecovery(segIdx *model.SegmentIndex) {
	m.buildID2SegmentIndex.Insert(segIdx.BuildID, segIdx)
	if m.taskStats.Len() >= taskStatsLRUCapacity && segIdx.IndexState == commonpb.IndexState_Finished {
		return
	}
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
func newIndexMeta(ctx context.Context, catalog metastore.DataCoordCatalog, collectionIDs []int64) (*indexMeta, error) {
	mt := &indexMeta{
		ctx:              ctx,
		catalog:          catalog,
		indexes:          make(map[UniqueID]map[UniqueID]*model.Index),
		keyLock:          lock.NewKeyLock[UniqueID](),
		segmentBuildInfo: newSegmentIndexBuildInfo(),
		segmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
	}
	err := mt.reloadFromKV(collectionIDs)
	if err != nil {
		return nil, err
	}
	return mt, nil
}

// reloadFromKV loads meta from KV storage
func (m *indexMeta) reloadFromKV(collectionIDs []int64) error {
	record := timerecord.NewTimeRecorder("indexMeta-reloadFromKV")

	// Parallel load and process: ListIndexes and ListSegmentIndexes have no dependency,
	// and they update completely separate data structures so memory updates can also run in parallel.
	// collectionSegIdxes is hoisted to function scope so the gauge goroutine
	// (launched after g.Wait) can access it when both indexes and segment indexes are loaded.
	collectionSegIdxes := make([][]*model.SegmentIndex, len(collectionIDs))
	g, _ := errgroup.WithContext(m.ctx)
	g.Go(func() error {
		fieldIndexes, err := m.catalog.ListIndexes(m.ctx)
		if err != nil {
			log.Error("indexMeta reloadFromKV load field indexes fail", zap.Error(err))
			return err
		}
		for _, fieldIndex := range fieldIndexes {
			m.updateCollectionIndex(fieldIndex)
		}
		return nil
	})
	g.Go(func() error {
		pool := conc.NewPool[any](paramtable.Get().MetaStoreCfg.ReadConcurrency.GetAsInt())
		defer pool.Release()
		futures := make([]*conc.Future[any], 0, len(collectionIDs))
		for i, collID := range collectionIDs {
			i, collID := i, collID
			futures = append(futures, pool.Submit(func() (any, error) {
				segIdxes, err := m.catalog.ListSegmentIndexes(m.ctx, collID)
				if err != nil {
					return nil, err
				}
				collectionSegIdxes[i] = segIdxes
				return nil, nil
			}))
		}
		if err := conc.AwaitAll(futures...); err != nil {
			return err
		}
		for _, segIdxes := range collectionSegIdxes {
			for _, segIdx := range segIdxes {
				if segIdx.IndexMemSize == 0 {
					segIdx.IndexMemSize = segIdx.IndexSerializedSize * paramtable.Get().DataCoordCfg.IndexMemSizeEstimateMultiplier.GetAsUint64()
				}
				indexes, ok := m.segmentIndexes.Get(segIdx.SegmentID)
				if ok {
					indexes.Insert(segIdx.IndexID, segIdx)
				} else {
					indexes = typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
					indexes.Insert(segIdx.IndexID, segIdx)
					m.segmentIndexes.Insert(segIdx.SegmentID, indexes)
				}
				m.segmentBuildInfo.AddForRecovery(segIdx)
			}
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}

	// Update Prometheus metrics asynchronously. Launched after g.Wait() so that
	// both m.indexes (from goroutine 1) and collectionSegIdxes (from goroutine 2)
	// are fully populated. Only count active indexes (index definition alive).
	go func() {
		storedSizeByCollection := make(map[int64]float64)
		for _, segIdxes := range collectionSegIdxes {
			for _, segIdx := range segIdxes {
				metrics.FlushedSegmentFileNum.WithLabelValues(metrics.IndexFileLabel).Observe(float64(len(segIdx.IndexFileKeys)))
				if m.IsIndexExist(segIdx.CollectionID, segIdx.IndexID) {
					storedSizeByCollection[segIdx.CollectionID] += float64(segIdx.IndexSerializedSize)
				}
			}
		}
		for collID, size := range storedSizeByCollection {
			metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "",
				fmt.Sprintf("%d", collID)).Add(size)
		}
	}()

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
		metrics.IndexStatsTaskNum.WithLabelValues(jobType, k.String()).Set(float64(v))
	}
	log.Ctx(m.ctx).Info("update index metric", zap.Int("collectionNum", len(taskMetrics)))
}

func checkIdenticalJSON(index *model.Index, req *indexpb.CreateIndexRequest) bool {
	// Skip error handling since json path existence is guaranteed in CreateIndex
	jsonPath1, _ := getIndexParam(index.IndexParams, common.JSONPathKey)
	jsonPath2, _ := getIndexParam(req.GetIndexParams(), common.JSONPathKey)

	if jsonPath1 != jsonPath2 {
		return false
	}
	castType1, _ := getIndexParam(index.IndexParams, common.JSONCastTypeKey)
	castType2, _ := getIndexParam(req.GetIndexParams(), common.JSONCastTypeKey)
	return castType1 == castType2
}

func checkParams(fieldIndex *model.Index, req *indexpb.CreateIndexRequest) bool {
	metaTypeParams := DeleteParams(fieldIndex.TypeParams, []string{common.MmapEnabledKey, common.WarmupKey})
	reqTypeParams := DeleteParams(req.TypeParams, []string{common.MmapEnabledKey, common.WarmupKey})
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
func (m *indexMeta) CanCreateIndex(req *indexpb.CreateIndexRequest, isJSON bool) (UniqueID, error) {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()
	indexID, err := m.canCreateIndex(req, isJSON)
	if err != nil {
		return 0, err
	}

	return indexID, nil
}

func (m *indexMeta) canCreateIndex(req *indexpb.CreateIndexRequest, isJSON bool) (UniqueID, error) {
	indexes, ok := m.indexes[req.CollectionID]
	if !ok {
		return 0, nil
	}
	for _, index := range indexes {
		if index.IsDeleted {
			continue
		}
		if req.IndexName == index.IndexName {
			if req.FieldID == index.FieldID && checkParams(index, req) &&
				/*only check json params when it is json index*/ (!isJSON || checkIdenticalJSON(index, req)) {
				return index.IndexID, errIndexOperationIgnored
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
			if isJSON {
				// Skip error handling since json path existence is guaranteed in CreateIndex
				jsonPath1, _ := getIndexParam(index.IndexParams, common.JSONPathKey)
				jsonPath2, _ := getIndexParam(req.GetIndexParams(), common.JSONPathKey)

				if jsonPath1 != jsonPath2 {
					// if json path is not same, create index is allowed
					continue
				}
			}
			// creating multiple indexes on same field is not supported
			errMsg := "CreateIndex failed: creating multiple indexes on same field is not supported"
			log.Warn(errMsg)
			return 0, errors.New(errMsg)
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

func (m *indexMeta) CreateIndex(ctx context.Context, index *model.Index) error {
	m.fieldIndexLock.Lock()
	defer m.fieldIndexLock.Unlock()

	log.Ctx(ctx).Info("meta update: CreateIndex", zap.Int64("collectionID", index.CollectionID),
		zap.Int64("fieldID", index.FieldID), zap.Int64("indexID", index.IndexID), zap.String("indexName", index.IndexName))

	if err := m.catalog.CreateIndex(ctx, index); err != nil {
		log.Ctx(ctx).Error("meta update: CreateIndex save meta fail", zap.Int64("collectionID", index.CollectionID),
			zap.Int64("fieldID", index.FieldID), zap.Int64("indexID", index.IndexID),
			zap.String("indexName", index.IndexName), zap.Error(err))
		return err
	}

	m.updateCollectionIndex(index)
	log.Ctx(ctx).Info("meta update: CreateIndex success", zap.Int64("collectionID", index.CollectionID),
		zap.Int64("fieldID", index.FieldID), zap.Int64("indexID", index.IndexID), zap.String("indexName", index.IndexName))
	return nil
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

// Precondition: caller holds fieldIndexLock — serializes the indexes map read with MarkIndexAsDeleted.
func (m *indexMeta) addStoredIndexSizeMetric(collID, indexID UniqueID, delta float64) {
	if delta == 0 {
		return
	}
	if idx, ok := m.indexes[collID][indexID]; ok && !idx.IsDeleted {
		metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "",
			fmt.Sprintf("%d", collID)).Add(delta)
	}
}

// AddSegmentIndex adds the index meta corresponding the indexBuildID to meta table.
func (m *indexMeta) AddSegmentIndex(ctx context.Context, segIndex *model.SegmentIndex) error {
	buildID := segIndex.BuildID

	m.keyLock.Lock(buildID)
	defer m.keyLock.Unlock(buildID)

	log.Ctx(ctx).Info("meta update: adding segment index", zap.Int64("collectionID", segIndex.CollectionID),
		zap.Int64("segmentID", segIndex.SegmentID), zap.Int64("indexID", segIndex.IndexID),
		zap.Int64("buildID", buildID), zap.String("indexType", segIndex.IndexType))

	// Only override to Unissued if the caller hasn't set a specific state.
	// Copy segment sets Finished because index files are already copied.
	if segIndex.IndexState == commonpb.IndexState_IndexStateNone {
		segIndex.IndexState = commonpb.IndexState_Unissued
	}
	if err := m.catalog.CreateSegmentIndex(ctx, segIndex); err != nil {
		log.Ctx(ctx).Warn("meta update: adding segment index failed",
			zap.Int64("segmentID", segIndex.SegmentID), zap.Int64("indexID", segIndex.IndexID),
			zap.Int64("buildID", segIndex.BuildID), zap.String("indexType", segIndex.IndexType), zap.Error(err))
		return err
	}

	// Insert + gauge Add must be serialized vs MarkIndexAsDeleted.
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

	m.updateSegmentIndex(segIndex)

	if segIndex.IndexState == commonpb.IndexState_Finished {
		m.addStoredIndexSizeMetric(segIndex.CollectionID, segIndex.IndexID,
			float64(segIndex.IndexSerializedSize))
	}

	log.Ctx(ctx).Info("meta update: adding segment index success", zap.Int64("collectionID", segIndex.CollectionID),
		zap.Int64("segmentID", segIndex.SegmentID), zap.Int64("indexID", segIndex.IndexID),
		zap.Int64("buildID", buildID), zap.String("indexType", segIndex.IndexType))
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
	fieldIndexesMap, ok := m.indexes[collID]
	if !ok {
		state.FailReason = fmt.Sprintf("collection not exist with ID: %d", collID)
		m.fieldIndexLock.RUnlock()
		return state
	}
	// Copy the map to avoid data race after releasing the lock
	fieldIndexes := make(map[UniqueID]*model.Index, len(fieldIndexesMap))
	for id, index := range fieldIndexesMap {
		fieldIndexes[id] = index
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
	fieldIndexesMap, ok := m.indexes[collectionID]
	if !ok {
		m.fieldIndexLock.RUnlock()
		return nil
	}
	// Copy the map to avoid data race after releasing the lock
	fieldIndexes := make(map[UniqueID]*model.Index, len(fieldIndexesMap))
	for id, index := range fieldIndexesMap {
		fieldIndexes[id] = index
	}
	m.fieldIndexLock.RUnlock()

	fieldIDSet := typeutil.NewUniqueSet(fieldIDs...)
	matchedFields := typeutil.NewUniqueSet()

	targetIndices := lo.Filter(lo.Values(fieldIndexes), func(index *model.Index, _ int) bool {
		return fieldIDSet.Contain(index.FieldID) && !index.IsDeleted
	})
	for _, index := range targetIndices {
		matchedFields.Insert(index.FieldID)
	}

	// some field has no index on it
	if len(matchedFields) != len(fieldIDSet) {
		return nil
	}

	checkSegmentState := func(indexes *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]) bool {
		indexedFields := 0
		for _, index := range targetIndices {
			if segIdx, ok := indexes.Get(index.IndexID); ok && segIdx.IndexState == commonpb.IndexState_Finished {
				indexedFields += 1
			}
		}

		return indexedFields == len(targetIndices)
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

func (m *indexMeta) GetFieldIndexes(collID, fieldID UniqueID, indexName string) []*model.Index {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

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
func (m *indexMeta) MarkIndexAsDeleted(ctx context.Context, collID UniqueID, indexIDs []UniqueID) error {
	log.Ctx(ctx).Info("IndexCoord metaTable MarkIndexAsDeleted", zap.Int64("collectionID", collID),
		zap.Int64s("indexIDs", indexIDs))

	m.fieldIndexLock.Lock()
	defer m.fieldIndexLock.Unlock()

	if len(indexIDs) == 0 {
		// drop all indexes if indexIDs is empty.
		indexIDs = make([]UniqueID, 0, len(m.indexes[collID]))
		for indexID, index := range m.indexes[collID] {
			if index.IsDeleted {
				continue
			}
			indexIDs = append(indexIDs, indexID)
		}
	}

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

	deletedSet := make(map[UniqueID]struct{}, len(indexes))
	for _, index := range indexes {
		m.indexes[index.CollectionID][index.IndexID] = index
		deletedSet[index.IndexID] = struct{}{}
	}

	// Subtract immediately — gauge tracks alive indexes only; deferred GC (RemoveSegmentIndex)
	// must not re-subtract. fieldIndexLock.Lock serializes with concurrent RLock gauge adders.
	var totalSize float64
	m.segmentIndexes.Range(func(_ UniqueID, idxMap *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]) bool {
		for indexID := range deletedSet {
			if segIdx, ok := idxMap.Get(indexID); ok && segIdx.CollectionID == collID {
				totalSize += float64(segIdx.IndexSerializedSize)
			}
		}
		return true
	})
	if totalSize > 0 {
		metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "",
			fmt.Sprintf("%d", collID)).Sub(totalSize)
	}

	log.Ctx(ctx).Info("IndexCoord metaTable MarkIndexAsDeleted success", zap.Int64("collectionID", collID), zap.Int64s("indexIDs", indexIDs))
	return nil
}

func (m *indexMeta) IsUnIndexedSegment(collectionID UniqueID, segID UniqueID) bool {
	m.fieldIndexLock.RLock()
	fieldIndexesMap, ok := m.indexes[collectionID]
	if !ok {
		m.fieldIndexLock.RUnlock()
		return false
	}
	// Copy the map to avoid data race after releasing the lock
	fieldIndexes := make(map[UniqueID]*model.Index, len(fieldIndexesMap))
	for id, index := range fieldIndexesMap {
		fieldIndexes[id] = index
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

func (m *indexMeta) UpdateIndexState(buildID UniqueID, state commonpb.IndexState, failReason string) error {
	m.keyLock.Lock(buildID)
	defer m.keyLock.Unlock(buildID)

	segIdx, ok := m.segmentBuildInfo.Get(buildID)
	if !ok {
		log.Ctx(m.ctx).Warn("there is no index with buildID", zap.Int64("buildID", buildID))
		return fmt.Errorf("there is no index with buildID: %d", buildID)
	}

	updateFunc := func(segIdx *model.SegmentIndex) error {
		segIdx.IndexState = state
		segIdx.FailReason = failReason
		return m.alterSegmentIndexes([]*model.SegmentIndex{segIdx})
	}

	if err := m.updateSegIndexMeta(segIdx, updateFunc); err != nil {
		log.Ctx(m.ctx).Warn("failed to update index meta", zap.Int64("buildID", buildID), zap.Error(err))
		return err
	}

	log.Ctx(m.ctx).Info("update index task state success", zap.Int64("buildID", buildID),
		zap.String("state", state.String()), zap.String("fail reason", failReason))
	return nil
}

func (m *indexMeta) FinishTask(taskInfo *workerpb.IndexTaskInfo) error {
	m.keyLock.Lock(taskInfo.GetBuildID())
	defer m.keyLock.Unlock(taskInfo.GetBuildID())

	segIdx, ok := m.segmentBuildInfo.Get(taskInfo.GetBuildID())
	if !ok {
		log.Ctx(m.ctx).Warn("there is no index with buildID", zap.Int64("buildID", taskInfo.GetBuildID()))
		return nil
	}

	oldSize := segIdx.IndexSerializedSize

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

	// gauge Add must be serialized vs MarkIndexAsDeleted.
	newSize := taskInfo.GetSerializedSize()
	m.fieldIndexLock.RLock()
	m.addStoredIndexSizeMetric(segIdx.CollectionID, segIdx.IndexID,
		float64(newSize)-float64(oldSize))
	m.fieldIndexLock.RUnlock()

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

func (m *indexMeta) RemoveSegmentIndex(ctx context.Context, buildID UniqueID) error {
	m.keyLock.Lock(buildID)
	defer m.keyLock.Unlock(buildID)

	segIdx, ok := m.segmentBuildInfo.Get(buildID)
	if !ok {
		return nil
	}

	err := m.catalog.DropSegmentIndex(ctx, segIdx.CollectionID, segIdx.PartitionID, segIdx.SegmentID, buildID)
	if err != nil {
		return err
	}

	// Reclaim size only when the index is still alive (segment-drop case);
	// MarkIndexAsDeleted already subtracted otherwise.
	m.fieldIndexLock.RLock()
	m.addStoredIndexSizeMetric(segIdx.CollectionID, segIdx.IndexID,
		-float64(segIdx.IndexSerializedSize))
	m.fieldIndexLock.RUnlock()

	segIndexes, ok := m.segmentIndexes.Get(segIdx.SegmentID)
	if ok {
		segIndexes.Remove(segIdx.IndexID)
		if segIndexes.Len() == 0 {
			m.segmentIndexes.Remove(segIdx.SegmentID)
		} else {
			m.segmentIndexes.Insert(segIdx.SegmentID, segIndexes)
		}
	}

	m.segmentBuildInfo.Remove(buildID)

	return nil
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

// HasCollectionWithPathVersion checks if any SegmentIndex with this collectionID uses pathVersion >= ver.
// Used by GC to distinguish collectionID dirs from orphan buildID dirs.
func (m *indexMeta) HasCollectionWithPathVersion(collectionID int64, pathVersion indexpb.IndexStorePathVersion) bool {
	if m.segmentBuildInfo == nil {
		return false
	}
	for _, segIdx := range m.segmentBuildInfo.List() {
		if segIdx.CollectionID == collectionID && segIdx.IndexStorePathVersion >= pathVersion {
			return true
		}
	}
	return false
}

// GetDeletedIndexesWithPathVersion returns SegmentIndex entries that are deleted and have pathVersion >= ver.
// Used by GC to find v1-format indexes that need file cleanup.
func (m *indexMeta) GetDeletedIndexesWithPathVersion(pathVersion indexpb.IndexStorePathVersion) []*model.SegmentIndex {
	if m.segmentBuildInfo == nil {
		return nil
	}
	var result []*model.SegmentIndex
	for _, segIdx := range m.segmentBuildInfo.List() {
		if segIdx.IsDeleted && segIdx.IndexStorePathVersion >= pathVersion {
			result = append(result, model.CloneSegmentIndex(segIdx))
		}
	}
	return result
}

func (m *indexMeta) getSegmentsIndexStates(collectionID UniqueID, segmentIDs []UniqueID) map[int64]map[int64]*indexpb.SegmentIndexState {
	ret := make(map[int64]map[int64]*indexpb.SegmentIndexState, 0)
	m.fieldIndexLock.RLock()
	fieldIndexesMap, ok := m.indexes[collectionID]
	if !ok {
		m.fieldIndexLock.RUnlock()
		return ret
	}
	fieldIndexes := make(map[UniqueID]*model.Index, len(fieldIndexesMap))
	for id, index := range fieldIndexesMap {
		fieldIndexes[id] = index
	}
	m.fieldIndexLock.RUnlock()

	for _, segID := range segmentIDs {
		ret[segID] = make(map[int64]*indexpb.SegmentIndexState)
		segIndexInfos, ok := m.segmentIndexes.Get(segID)
		if !ok || segIndexInfos.Len() == 0 {
			continue
		}

		for _, segIdx := range segIndexInfos.Values() {
			if index, ok := fieldIndexes[segIdx.IndexID]; ok && !index.IsDeleted {
				indexVersion := segIdx.CurrentIndexVersion
				if indexparamcheck.IsScalarIndexType(segIdx.IndexType) {
					indexVersion = segIdx.CurrentScalarIndexVersion
				}
				ret[segID][segIdx.IndexID] = &indexpb.SegmentIndexState{
					SegmentID:    segID,
					State:        segIdx.IndexState,
					FailReason:   segIdx.FailReason,
					IndexName:    index.IndexName,
					IndexVersion: indexVersion,
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

func (m *indexMeta) AllDenseWithDiskIndex(collectionID int64, schema *schemapb.CollectionSchema) bool {
	indexInfos := m.GetIndexesForCollection(collectionID, "")

	vectorFields := typeutil.GetDenseVectorFieldSchemas(schema)
	fieldIndexTypes := lo.SliceToMap(indexInfos, func(t *model.Index) (int64, indexparamcheck.IndexType) {
		return t.FieldID, GetIndexType(t.IndexParams)
	})
	vectorFieldsWithDiskIndex := lo.Filter(vectorFields, func(field *schemapb.FieldSchema, _ int) bool {
		if indexType, ok := fieldIndexTypes[field.FieldID]; ok {
			return vecindexmgr.GetVecIndexMgrInstance().IsDiskVecIndex(indexType)
		}
		return false
	})

	return len(vectorFields) == len(vectorFieldsWithDiskIndex)
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
	fieldIndexesMap, ok := m.indexes[collectionID]
	if !ok {
		// the segment should be unindexed status if the collection has no indexes
		m.fieldIndexLock.RUnlock()
		return false, []*metricsinfo.IndexedField{}
	}
	// Copy the map to avoid data race after releasing the lock
	fieldIndexes := make(map[UniqueID]*model.Index, len(fieldIndexesMap))
	for id, index := range fieldIndexesMap {
		fieldIndexes[id] = index
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
			serializedSize := int64(0)
			if !ok || si == nil {
				// the segment should be unindexed status if the segment index is not found within field indexes
				isIndexed = false
			} else {
				buildID = si.BuildID
				serializedSize = int64(si.IndexSerializedSize)
			}

			segmentIndexes = append(segmentIndexes, &metricsinfo.IndexedField{
				IndexFieldID: index.FieldID,
				IndexID:      index.IndexID,
				BuildID:      buildID,
				IndexSize:    serializedSize,
			})
		}
	}
	return isIndexed, segmentIndexes
}
