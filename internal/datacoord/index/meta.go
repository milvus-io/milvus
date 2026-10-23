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
package index

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
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/indexparams"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var ErrIndexOperationIgnored = errors.New("index operation ignored")

func validateIndexStorePathVersionForFinish(buildID int64, requested, actual indexpb.IndexStorePathVersion) error {
	if actual > requested {
		return merr.WrapErrParameterInvalidMsg(
			"index store path version returned by worker is newer than requested, buildID=%d, requested=%s, actual=%s",
			buildID,
			requested.String(),
			actual.String(),
		)
	}
	return nil
}

type IndexMeta struct {
	Ctx     context.Context
	Catalog metastore.DataCoordCatalog

	// collectionIndexes records which indexes are on the collection
	// collID -> indexID -> index
	fieldIndexLock sync.RWMutex
	Indexes        map[UniqueID]map[UniqueID]*model.Index
	taskCounts     indexTaskCounts

	// buildID2Meta records building index meta information of the segment
	SegmentBuildInfo *segmentBuildInfo

	// buildID -> lock
	KeyLock *lock.KeyLock[UniqueID]
	// segmentID -> indexID -> segmentIndex
	SegmentIndexes *typeutil.ConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]
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

func NewSegmentIndexBuildInfo() *segmentBuildInfo {
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
func NewIndexMeta(ctx context.Context, catalog metastore.DataCoordCatalog, collectionIDs []int64) (*IndexMeta, error) {
	mt := &IndexMeta{
		Ctx:              ctx,
		Catalog:          catalog,
		Indexes:          make(map[UniqueID]map[UniqueID]*model.Index),
		KeyLock:          lock.NewKeyLock[UniqueID](),
		SegmentBuildInfo: NewSegmentIndexBuildInfo(),
		SegmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
	}
	err := mt.reloadFromKV(collectionIDs)
	if err != nil {
		return nil, err
	}
	return mt, nil
}

// reloadFromKV loads meta from KV storage
func (m *IndexMeta) reloadFromKV(collectionIDs []int64) error {
	record := timerecord.NewTimeRecorder("indexMeta-reloadFromKV")

	// Parallel load and process: ListIndexes and ListSegmentIndexes have no dependency,
	// and they update completely separate data structures so memory updates can also run in parallel.
	// collectionSegIdxes is hoisted to function scope so the gauge goroutine
	// (launched after g.Wait) can access it when both indexes and segment indexes are loaded.
	collectionSegIdxes := make([][]*model.SegmentIndex, len(collectionIDs))
	g, _ := errgroup.WithContext(m.Ctx)
	g.Go(func() error {
		fieldIndexes, err := m.Catalog.ListIndexes(m.Ctx)
		if err != nil {
			mlog.Error(context.TODO(), "indexMeta reloadFromKV load field indexes fail", mlog.Err(err))
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
				segIdxes, err := m.Catalog.ListSegmentIndexes(m.Ctx, collID)
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
				indexes, ok := m.SegmentIndexes.Get(segIdx.SegmentID)
				if ok {
					indexes.Insert(segIdx.IndexID, segIdx)
				} else {
					indexes = typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
					indexes.Insert(segIdx.IndexID, segIdx)
					m.SegmentIndexes.Insert(segIdx.SegmentID, indexes)
				}
				old, _ := m.SegmentBuildInfo.Get(segIdx.BuildID)
				m.taskCounts.replaceTask(old, segIdx)
				m.SegmentBuildInfo.AddForRecovery(segIdx)
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

	mlog.Info(context.TODO(), "indexMeta reloadFromKV done", mlog.Duration("duration", record.ElapseSpan()))
	return nil
}

func (m *IndexMeta) updateCollectionIndex(index *model.Index) {
	m.taskCounts.replaceIndex(m.Indexes[index.CollectionID][index.IndexID], index)
	if _, ok := m.Indexes[index.CollectionID]; !ok {
		m.Indexes[index.CollectionID] = make(map[UniqueID]*model.Index)
	}
	m.Indexes[index.CollectionID][index.IndexID] = index
}

func (m *IndexMeta) updateSegmentIndex(segIdx *model.SegmentIndex) {
	old, _ := m.SegmentBuildInfo.Get(segIdx.BuildID)
	m.taskCounts.replaceTask(old, segIdx)
	indexes, ok := m.SegmentIndexes.Get(segIdx.SegmentID)
	if ok {
		indexes.Insert(segIdx.IndexID, segIdx)
	} else {
		indexes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		indexes.Insert(segIdx.IndexID, segIdx)
		m.SegmentIndexes.Insert(segIdx.SegmentID, indexes)
	}
	m.SegmentBuildInfo.Add(segIdx)
}

// writeSegmentIndexToManifest reports whether completed StorageV3 index
// artifacts are published through segment manifests. SegmentIndex remains the
// durable task record while a build is active or failed. Only a successful
// publication retires its Finished etcd row.
func WriteSegmentIndexToManifest() bool {
	return paramtable.Get().DataCoordCfg.WriteSegmentIndexToManifest.GetAsBool()
}

func (m *IndexMeta) alterSegmentIndexes(segIdxes []*model.SegmentIndex) error {
	err := m.Catalog.AlterSegmentIndexes(m.Ctx, segIdxes)
	if err != nil {
		mlog.Error(context.TODO(), "failed to alter segments index in meta store", mlog.Int("segment indexes num", len(segIdxes)),
			mlog.Err(err))
		return err
	}
	for _, segIdx := range segIdxes {
		m.updateSegmentIndex(segIdx)
	}
	return nil
}

func (m *IndexMeta) updateIndexMeta(index *model.Index, updateFunc func(clonedIndex *model.Index) error) error {
	return updateFunc(model.CloneIndex(index))
}

func (m *IndexMeta) updateSegIndexMeta(segIdx *model.SegmentIndex, updateFunc func(clonedSegIdx *model.SegmentIndex) error) error {
	return updateFunc(model.CloneSegmentIndex(segIdx))
}

// buildFinishedSegmentIndex projects a worker completion result onto a fresh
// copy of the task's metadata without persisting it or touching the in-memory
// record. Callers that must compose this update with another metadata write
// stage the returned value in the same catalog transaction; the second return
// value is the previously recorded serialized size, needed for the stored-size
// gauge delta.
//
// Precondition: the caller holds keyLock for taskInfo.BuildID.
func (m *IndexMeta) buildFinishedSegmentIndex(segIdx *model.SegmentIndex, taskInfo *workerpb.IndexTaskInfo) (*model.SegmentIndex, uint64, error) {
	actualPathVersion := taskInfo.GetIndexStorePathVersion()
	if err := validateIndexStorePathVersionForFinish(taskInfo.GetBuildID(), segIdx.IndexStorePathVersion, actualPathVersion); err != nil {
		mlog.Warn(m.Ctx, "invalid index store path version returned by worker",
			mlog.Int64("buildID", taskInfo.GetBuildID()),
			mlog.String("requested", segIdx.IndexStorePathVersion.String()),
			mlog.String("actual", actualPathVersion.String()),
			mlog.Err(err))
		return nil, 0, err
	}
	if actualPathVersion < segIdx.IndexStorePathVersion {
		mlog.Info(m.Ctx, "worker downgraded index store path version",
			mlog.Int64("buildID", taskInfo.GetBuildID()),
			mlog.String("requested", segIdx.IndexStorePathVersion.String()),
			mlog.String("actual", actualPathVersion.String()))
	}

	updated := model.CloneSegmentIndex(segIdx)
	updated.IndexState = taskInfo.GetState()
	updated.IndexFileKeys = common.CloneStringList(taskInfo.GetIndexFileKeys())
	updated.FailReason = taskInfo.GetFailReason()
	updated.IndexSerializedSize = taskInfo.GetSerializedSize()
	updated.IndexMemSize = taskInfo.GetMemSize()
	updated.CurrentIndexVersion = taskInfo.GetCurrentIndexVersion()
	updated.FinishedUTCTime = uint64(time.Now().Unix())
	updated.CurrentScalarIndexVersion = taskInfo.GetCurrentScalarIndexVersion()
	updated.IndexStorePathVersion = actualPathVersion
	return updated, segIdx.IndexSerializedSize, nil
}

// recordFinishedTask updates process-local observability only. The caller must
// have already persisted updated and installed it in the in-memory metadata.
func (m *IndexMeta) recordFinishedTask(old, updated *model.SegmentIndex, oldSize uint64, taskInfo *workerpb.IndexTaskInfo) {
	mlog.Info(m.Ctx, "finish index task success", mlog.Int64("buildID", taskInfo.GetBuildID()),
		mlog.String("state", taskInfo.GetState().String()), mlog.String("fail reason", taskInfo.GetFailReason()),
		mlog.Int32("current_index_version", taskInfo.GetCurrentIndexVersion()))

	// gauge Add must be serialized vs MarkIndexAsDeleted.
	m.fieldIndexLock.RLock()
	m.addStoredIndexSizeMetric(old.CollectionID, old.IndexID,
		float64(updated.IndexSerializedSize)-float64(oldSize))
	m.fieldIndexLock.RUnlock()

	metrics.FlushedSegmentFileNum.WithLabelValues(metrics.IndexFileLabel).Observe(float64(len(taskInfo.GetIndexFileKeys())))
}

func (m *IndexMeta) indexTaskCountsSnapshot() indexTaskStateCounts {
	return m.taskCounts.snapshot()
}

func (m *IndexMeta) UpdateIndexTasksMetrics() {
	counts := m.indexTaskCountsSnapshot()
	jobType := indexpb.JobType_JobTypeIndexJob.String()
	for state, count := range counts {
		metrics.IndexStatsTaskNum.WithLabelValues(jobType, indexTaskMetricStates[state].String()).Set(float64(count))
	}
}

func checkIdenticalJSON(index *model.Index, req *indexpb.CreateIndexRequest) bool {
	// Skip error handling since json path existence is guaranteed in CreateIndex
	jsonPath1, _ := GetIndexParam(index.IndexParams, common.JSONPathKey)
	jsonPath2, _ := GetIndexParam(req.GetIndexParams(), common.JSONPathKey)

	if jsonPath1 != jsonPath2 {
		return false
	}
	castType1, _ := GetIndexParam(index.IndexParams, common.JSONCastTypeKey)
	castType2, _ := GetIndexParam(req.GetIndexParams(), common.JSONCastTypeKey)
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
				mlog.Warn(context.TODO(), "user not specify autoindex metric type, autoindex config has changed, use old metric for compatibility",
					mlog.String("old metric type", param1.Value), mlog.String("new metric type", param2.Value))
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
	mlog.Info(context.TODO(), "final request", mlog.Any("create index request", req.String()))
	return !notEq
}

// CanCreateIndex currently is used in Unittest
func (m *IndexMeta) CanCreateIndex(req *indexpb.CreateIndexRequest, isJSON bool) (UniqueID, error) {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()
	indexID, err := m.canCreateIndex(req, isJSON)
	if err != nil {
		return 0, err
	}

	return indexID, nil
}

func (m *IndexMeta) canCreateIndex(req *indexpb.CreateIndexRequest, isJSON bool) (UniqueID, error) {
	indexes, ok := m.Indexes[req.CollectionID]
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
				return index.IndexID, ErrIndexOperationIgnored
			}
			errMsg := "at most one distinct index is allowed per field"
			mlog.Warn(context.TODO(), errMsg,
				mlog.String("source index", fmt.Sprintf("{index_name: %s, field_id: %d, index_params: %v, user_params: %v, type_params: %v}",
					index.IndexName, index.FieldID, index.IndexParams, index.UserIndexParams, index.TypeParams)),
				mlog.String("current index", fmt.Sprintf("{index_name: %s, field_id: %d, index_params: %v, user_params: %v, type_params: %v}",
					req.GetIndexName(), req.GetFieldID(), req.GetIndexParams(), req.GetUserIndexParams(), req.GetTypeParams())))
			return 0, merr.WrapErrParameterInvalidMsg("%s", errMsg)
		}
		if req.FieldID == index.FieldID {
			if isJSON {
				// Skip error handling since json path existence is guaranteed in CreateIndex
				jsonPath1, _ := GetIndexParam(index.IndexParams, common.JSONPathKey)
				jsonPath2, _ := GetIndexParam(req.GetIndexParams(), common.JSONPathKey)

				if jsonPath1 != jsonPath2 {
					// if json path is not same, create index is allowed
					continue
				}
			}
			// creating multiple indexes on same field is not supported
			errMsg := "CreateIndex failed: creating multiple indexes on same field is not supported"
			mlog.Warn(context.TODO(), errMsg)
			// Client-caused conflict, same family as the "one distinct index per
			// field" case above: return an input-class error rather than
			// ServiceInternal (code 5, "never return out of Milvus") so the
			// caller is not blamed with a system error / counted as a system-caused failure.
			return 0, merr.WrapErrParameterInvalidMsg("%s", errMsg)
		}
	}
	return 0, nil
}

// HasSameReq determine whether there are same indexing tasks.
func (m *IndexMeta) HasSameReq(req *indexpb.CreateIndexRequest) (bool, UniqueID) {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

	for _, fieldIndex := range m.Indexes[req.CollectionID] {
		if fieldIndex.IsDeleted {
			continue
		}
		if fieldIndex.FieldID != req.FieldID || fieldIndex.IndexName != req.IndexName {
			continue
		}
		if !checkParams(fieldIndex, req) {
			continue
		}
		mlog.Debug(context.TODO(), "has same index", mlog.Int64("collectionID", req.CollectionID),
			mlog.Int64("fieldID", req.FieldID), mlog.String("indexName", req.IndexName),
			mlog.Int64("indexID", fieldIndex.IndexID))
		return true, fieldIndex.IndexID
	}

	return false, 0
}

func (m *IndexMeta) CreateIndex(ctx context.Context, index *model.Index) error {
	m.fieldIndexLock.Lock()
	defer m.fieldIndexLock.Unlock()

	mlog.Info(ctx, "meta update: CreateIndex", mlog.Int64("collectionID", index.CollectionID),
		mlog.Int64("fieldID", index.FieldID), mlog.Int64("indexID", index.IndexID), mlog.String("indexName", index.IndexName))

	if err := m.Catalog.CreateIndex(ctx, index); err != nil {
		mlog.Error(ctx, "meta update: CreateIndex save meta fail", mlog.Int64("collectionID", index.CollectionID),
			mlog.Int64("fieldID", index.FieldID), mlog.Int64("indexID", index.IndexID),
			mlog.String("indexName", index.IndexName), mlog.Err(err))
		return err
	}

	m.updateCollectionIndex(index)
	mlog.Info(ctx, "meta update: CreateIndex success", mlog.Int64("collectionID", index.CollectionID),
		mlog.Int64("fieldID", index.FieldID), mlog.Int64("indexID", index.IndexID), mlog.String("indexName", index.IndexName))
	return nil
}

func (m *IndexMeta) AlterIndex(ctx context.Context, indexes ...*model.Index) error {
	m.fieldIndexLock.Lock()
	defer m.fieldIndexLock.Unlock()

	err := m.Catalog.AlterIndexes(ctx, indexes)
	if err != nil {
		return err
	}

	for _, index := range indexes {
		m.updateCollectionIndex(index)
	}

	return nil
}

// Precondition: caller holds fieldIndexLock — serializes the indexes map read with MarkIndexAsDeleted.
func (m *IndexMeta) addStoredIndexSizeMetric(collID, indexID UniqueID, delta float64) {
	if delta == 0 {
		return
	}
	if idx, ok := m.Indexes[collID][indexID]; ok && !idx.IsDeleted {
		metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "",
			fmt.Sprintf("%d", collID)).Add(delta)
	}
}

// AddSegmentIndex adds the index meta corresponding the indexBuildID to meta table.
func (m *IndexMeta) AddSegmentIndex(ctx context.Context, segIndex *model.SegmentIndex) error {
	return m.addSegmentIndex(ctx, segIndex, true)
}

// AddSegmentIndexFromManifest installs a completed index already made durable
// by a copied segment's published manifest. Copy targets use fresh build IDs,
// so there is no prior etcd task row to retire.
func (m *IndexMeta) AddSegmentIndexFromManifest(ctx context.Context, segIndex *model.SegmentIndex) error {
	if segIndex == nil {
		return merr.WrapErrServiceInternalMsg("manifest-resident segment index is nil")
	}
	if segIndex.IndexState != commonpb.IndexState_Finished || len(segIndex.IndexFileKeys) == 0 {
		return merr.WrapErrServiceInternalMsg(
			"manifest-resident segment index must be finished and carry artifact files, buildID=%d", segIndex.BuildID)
	}
	return m.addSegmentIndex(ctx, segIndex, false)
}

func (m *IndexMeta) addSegmentIndex(ctx context.Context, segIndex *model.SegmentIndex, persist bool) error {
	buildID := segIndex.BuildID

	m.KeyLock.Lock(buildID)
	defer m.KeyLock.Unlock(buildID)

	mlog.Info(ctx, "meta update: adding segment index", mlog.Int64("collectionID", segIndex.CollectionID),
		mlog.Int64("segmentID", segIndex.SegmentID), mlog.Int64("indexID", segIndex.IndexID),
		mlog.Int64("buildID", buildID), mlog.String("indexType", segIndex.IndexType))

	// Only override to Unissued if the caller hasn't set a specific state.
	// Copy segment sets Finished because index files are already copied.
	if segIndex.IndexState == commonpb.IndexState_IndexStateNone {
		segIndex.IndexState = commonpb.IndexState_Unissued
	}
	if persist {
		if err := m.Catalog.CreateSegmentIndex(ctx, segIndex); err != nil {
			mlog.Warn(ctx, "meta update: adding segment index failed",
				mlog.Int64("segmentID", segIndex.SegmentID), mlog.Int64("indexID", segIndex.IndexID),
				mlog.Int64("buildID", segIndex.BuildID), mlog.String("indexType", segIndex.IndexType), mlog.Err(err))
			return err
		}
	}

	// Insert + gauge Add must be serialized vs MarkIndexAsDeleted.
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

	m.updateSegmentIndex(segIndex)

	if segIndex.IndexState == commonpb.IndexState_Finished {
		m.addStoredIndexSizeMetric(segIndex.CollectionID, segIndex.IndexID,
			float64(segIndex.IndexSerializedSize))
	}

	mlog.Info(ctx, "meta update: adding segment index success", mlog.Int64("collectionID", segIndex.CollectionID),
		mlog.Int64("segmentID", segIndex.SegmentID), mlog.Int64("indexID", segIndex.IndexID),
		mlog.Int64("buildID", buildID), mlog.String("indexType", segIndex.IndexType))
	return nil
}

func (m *IndexMeta) GetIndexIDByName(collID int64, indexName string) map[int64]uint64 {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

	indexID2CreateTs := make(map[int64]uint64)

	fieldIndexes, ok := m.Indexes[collID]
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

func (m *IndexMeta) GetSegmentIndexState(collID, segmentID UniqueID, indexID UniqueID) *indexpb.SegmentIndexState {
	state := &indexpb.SegmentIndexState{
		SegmentID:  segmentID,
		State:      commonpb.IndexState_IndexStateNone,
		FailReason: "",
	}

	m.fieldIndexLock.RLock()
	fieldIndexesMap, ok := m.Indexes[collID]
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

	indexes, ok := m.SegmentIndexes.Get(segmentID)
	if !ok {
		state.State = commonpb.IndexState_Unissued
		state.FailReason = fmt.Sprintf("segment index not exist with ID: %d", segmentID)
		return state
	}

	if index, ok := fieldIndexes[indexID]; ok && !index.IsDeleted {
		if segIdx, ok := indexes.Get(indexID); ok && !segIdx.IsDeleted {
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

func (m *IndexMeta) GetIndexedSegments(collectionID int64, segmentIDs, fieldIDs []UniqueID) []int64 {
	m.fieldIndexLock.RLock()
	fieldIndexesMap, ok := m.Indexes[collectionID]
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
			if segIdx, ok := indexes.Get(index.IndexID); ok && !segIdx.IsDeleted && segIdx.IndexState == commonpb.IndexState_Finished {
				indexedFields += 1
			}
		}

		return indexedFields == len(targetIndices)
	}

	ret := make([]int64, 0)
	for _, sid := range segmentIDs {
		if indexes, ok := m.SegmentIndexes.Get(sid); ok {
			if checkSegmentState(indexes) {
				ret = append(ret, sid)
			}
		}
	}

	return ret
}

// GetIndexesForCollection gets all indexes info with the specified collection.
func (m *IndexMeta) GetIndexesForCollection(collID UniqueID, indexName string) []*model.Index {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

	indexInfos := make([]*model.Index, 0)
	for _, index := range m.Indexes[collID] {
		if index.IsDeleted {
			continue
		}
		if indexName == "" || indexName == index.IndexName {
			indexInfos = append(indexInfos, model.CloneIndex(index))
		}
	}
	return indexInfos
}

// ListAllIndexes returns a clone of every index that is not deleted, across all
// collections. Used by the feature usage report.
func (m *IndexMeta) ListAllIndexes() []*model.Index {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

	out := make([]*model.Index, 0)
	for _, indexes := range m.Indexes {
		for _, index := range indexes {
			if index.IsDeleted {
				continue
			}
			out = append(out, model.CloneIndex(index))
		}
	}
	return out
}

func (m *IndexMeta) GetFieldIndexes(collID, fieldID UniqueID, indexName string) []*model.Index {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

	indexInfos := make([]*model.Index, 0)
	for _, index := range m.Indexes[collID] {
		if index.IsDeleted || index.FieldID != fieldID {
			continue
		}
		if indexName == "" || indexName == index.IndexName {
			indexInfos = append(indexInfos, model.CloneIndex(index))
		}
	}
	return indexInfos
}

// MarkIndexAsDeleted marks indexes as deleted. File cleanup is split by path layout:
// recycleUnusedIndexFilesV0 handles legacy v0 index_files objects, and
// recycleUnusedIndexFilesV1 handles v1 index_v1 objects.
func (m *IndexMeta) MarkIndexAsDeleted(ctx context.Context, collID UniqueID, indexIDs []UniqueID) error {
	mlog.Info(ctx, "IndexCoord metaTable MarkIndexAsDeleted", mlog.Int64("collectionID", collID),
		mlog.Int64s("indexIDs", indexIDs))

	m.fieldIndexLock.Lock()
	defer m.fieldIndexLock.Unlock()

	if len(indexIDs) == 0 {
		// drop all indexes if indexIDs is empty.
		indexIDs = make([]UniqueID, 0, len(m.Indexes[collID]))
		for indexID, index := range m.Indexes[collID] {
			if index.IsDeleted {
				continue
			}
			indexIDs = append(indexIDs, indexID)
		}
	}

	fieldIndexes, ok := m.Indexes[collID]
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
	err := m.Catalog.AlterIndexes(ctx, indexes)
	if err != nil {
		mlog.Error(ctx, "failed to alter index meta in meta store", mlog.Int("indexes num", len(indexes)), mlog.Err(err))
		return err
	}

	deletedSet := make(map[UniqueID]struct{}, len(indexes))
	for _, index := range indexes {
		m.updateCollectionIndex(index)
		deletedSet[index.IndexID] = struct{}{}
	}

	// Subtract immediately — gauge tracks alive indexes only; deferred GC (RemoveSegmentIndex)
	// must not re-subtract. fieldIndexLock.Lock serializes with concurrent RLock gauge adders.
	var totalSize float64
	m.SegmentIndexes.Range(func(_ UniqueID, idxMap *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]) bool {
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

	mlog.Info(ctx, "IndexCoord metaTable MarkIndexAsDeleted success", mlog.Int64("collectionID", collID), mlog.Int64s("indexIDs", indexIDs))
	return nil
}

func (m *IndexMeta) IsUnIndexedSegment(collectionID UniqueID, segID UniqueID) bool {
	m.fieldIndexLock.RLock()
	fieldIndexesMap, ok := m.Indexes[collectionID]
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
	segIndexInfos, ok := m.SegmentIndexes.Get(segID)
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

func (m *IndexMeta) GetSegmentsIndexes(collectionID UniqueID, segIDs []UniqueID) map[int64]map[UniqueID]*model.SegmentIndex {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()
	segmentsIndexes := make(map[int64]map[UniqueID]*model.SegmentIndex)
	for _, segmentID := range segIDs {
		segmentsIndexes[segmentID] = m.GetSegmentIndexesUnsafe(collectionID, segmentID)
	}
	return segmentsIndexes
}

func (m *IndexMeta) GetSegmentIndexes(collectionID UniqueID, segID UniqueID) map[UniqueID]*model.SegmentIndex {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()
	return m.GetSegmentIndexesUnsafe(collectionID, segID)
}

// GetAllSegmentIndexes returns a deep-clone of every SegmentIndex recorded
// for segID, including deleted tasks and records whose parent field index has
// been deleted. GC needs their file keys until cleanup completes.
//
// It only reads the self-synchronized segmentIndexes map and does not acquire
// fieldIndexLock or consult field-index definitions.
func (m *IndexMeta) GetAllSegmentIndexes(segID UniqueID) []*model.SegmentIndex {
	if m.SegmentIndexes == nil {
		return nil
	}
	segIndexInfos, ok := m.SegmentIndexes.Get(segID)
	if !ok || segIndexInfos.Len() == 0 {
		return nil
	}

	segIndexes := segIndexInfos.Values()
	ret := make([]*model.SegmentIndex, 0, len(segIndexes))
	for _, segIdx := range segIndexes {
		ret = append(ret, model.CloneSegmentIndex(segIdx))
	}
	return ret
}

// Note: thread-unsafe, don't call it outside indexMeta
func (m *IndexMeta) GetSegmentIndexesUnsafe(collectionID UniqueID, segID UniqueID) map[UniqueID]*model.SegmentIndex {
	ret := make(map[UniqueID]*model.SegmentIndex, 0)
	segIndexInfos, ok := m.SegmentIndexes.Get(segID)
	if !ok || segIndexInfos.Len() == 0 {
		return ret
	}

	fieldIndexes, ok := m.Indexes[collectionID]
	if !ok {
		return ret
	}

	for _, segIdx := range segIndexInfos.Values() {
		if index, ok := fieldIndexes[segIdx.IndexID]; ok && !index.IsDeleted && !segIdx.IsDeleted {
			ret[segIdx.IndexID] = model.CloneSegmentIndex(segIdx)
		}
	}
	return ret
}

func (m *IndexMeta) GetFieldIDByIndexID(collID, indexID UniqueID) UniqueID {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

	if fieldIndexes, ok := m.Indexes[collID]; ok {
		if index, ok := fieldIndexes[indexID]; ok {
			return index.FieldID
		}
	}
	return 0
}

func (m *IndexMeta) GetIndexNameByID(collID, indexID UniqueID) string {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

	if fieldIndexes, ok := m.Indexes[collID]; ok {
		if index, ok := fieldIndexes[indexID]; ok {
			return index.IndexName
		}
	}
	return ""
}

func (m *IndexMeta) GetIndexParams(collID, indexID UniqueID) []*commonpb.KeyValuePair {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

	fieldIndexes, ok := m.Indexes[collID]
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

func (m *IndexMeta) GetTypeParams(collID, indexID UniqueID) []*commonpb.KeyValuePair {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

	fieldIndexes, ok := m.Indexes[collID]
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

func (m *IndexMeta) GetIndexJob(buildID UniqueID) (*model.SegmentIndex, bool) {
	segIdx, ok := m.SegmentBuildInfo.Get(buildID)
	if ok {
		return model.CloneSegmentIndex(segIdx), true
	}

	return nil, false
}

func (m *IndexMeta) IsIndexExist(collID, indexID UniqueID) bool {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

	fieldIndexes, ok := m.Indexes[collID]
	if !ok {
		return false
	}
	if index, ok := fieldIndexes[indexID]; !ok || index.IsDeleted {
		return false
	}

	return true
}

// UpdateVersion updates the version and nodeID of the index meta, whenever the task is built once, the version will be updated once.
func (m *IndexMeta) UpdateVersion(buildID, nodeID UniqueID) error {
	m.KeyLock.Lock(buildID)
	defer m.KeyLock.Unlock(buildID)

	mlog.Info(m.Ctx, "IndexCoord metaTable UpdateVersion receive", mlog.Int64("buildID", buildID), mlog.Int64("nodeID", nodeID))
	segIdx, ok := m.SegmentBuildInfo.Get(buildID)
	if !ok {
		return merr.WrapErrServiceInternalMsg("there is no index with buildID: %d", buildID)
	}

	updateFunc := func(segIdx *model.SegmentIndex) error {
		segIdx.IndexVersion++
		segIdx.NodeID = nodeID
		return m.alterSegmentIndexes([]*model.SegmentIndex{segIdx})
	}

	return m.updateSegIndexMeta(segIdx, updateFunc)
}

func (m *IndexMeta) UpdateIndexState(buildID UniqueID, state commonpb.IndexState, failReason string) error {
	m.KeyLock.Lock(buildID)
	defer m.KeyLock.Unlock(buildID)

	segIdx, ok := m.SegmentBuildInfo.Get(buildID)
	if !ok {
		mlog.Warn(m.Ctx, "there is no index with buildID", mlog.Int64("buildID", buildID))
		return merr.WrapErrServiceInternalMsg("there is no index with buildID: %d", buildID)
	}

	updateFunc := func(segIdx *model.SegmentIndex) error {
		segIdx.IndexState = state
		segIdx.FailReason = failReason
		return m.alterSegmentIndexes([]*model.SegmentIndex{segIdx})
	}

	if err := m.updateSegIndexMeta(segIdx, updateFunc); err != nil {
		mlog.Warn(m.Ctx, "failed to update index meta", mlog.Int64("buildID", buildID), mlog.Err(err))
		return err
	}

	mlog.Info(m.Ctx, "update index task state success", mlog.Int64("buildID", buildID),
		mlog.String("state", state.String()), mlog.String("fail reason", failReason))
	return nil
}

func (m *IndexMeta) FinishTask(taskInfo *workerpb.IndexTaskInfo) error {
	m.KeyLock.Lock(taskInfo.GetBuildID())
	defer m.KeyLock.Unlock(taskInfo.GetBuildID())

	segIdx, ok := m.SegmentBuildInfo.Get(taskInfo.GetBuildID())
	if !ok {
		mlog.Warn(m.Ctx, "there is no index with buildID", mlog.Int64("buildID", taskInfo.GetBuildID()))
		return nil
	}

	updated, oldSize, err := m.buildFinishedSegmentIndex(segIdx, taskInfo)
	if err != nil {
		return err
	}
	if err := m.alterSegmentIndexes([]*model.SegmentIndex{updated}); err != nil {
		return err
	}
	m.recordFinishedTask(segIdx, updated, oldSize, taskInfo)
	return nil
}

func (m *IndexMeta) DeleteTask(buildID int64) error {
	m.KeyLock.Lock(buildID)
	defer m.KeyLock.Unlock(buildID)

	segIdx, ok := m.SegmentBuildInfo.Get(buildID)
	if !ok {
		mlog.Warn(m.Ctx, "there is no index with buildID", mlog.Int64("buildID", buildID))
		return nil
	}

	if segIdx.IsDeleted {
		return nil
	}

	updateFunc := func(segIdx *model.SegmentIndex) error {
		segIdx.IsDeleted = true
		return m.alterSegmentIndexes([]*model.SegmentIndex{segIdx})
	}

	if err := m.updateSegIndexMeta(segIdx, updateFunc); err != nil {
		return err
	}

	mlog.Info(m.Ctx, "delete index task success", mlog.Int64("buildID", buildID))
	return nil
}

// BuildIndex set the index state to be InProgress. It means IndexNode is building the index.
func (m *IndexMeta) BuildIndex(buildID UniqueID) error {
	m.KeyLock.Lock(buildID)
	defer m.KeyLock.Unlock(buildID)

	segIdx, ok := m.SegmentBuildInfo.Get(buildID)
	if !ok {
		return merr.WrapErrServiceInternalMsg("there is no index with buildID: %d", buildID)
	}

	updateFunc := func(segIdx *model.SegmentIndex) error {
		segIdx.IndexState = commonpb.IndexState_InProgress

		err := m.alterSegmentIndexes([]*model.SegmentIndex{segIdx})
		if err != nil {
			mlog.Error(m.Ctx, "meta Update: segment index in progress fail", mlog.Int64("buildID", segIdx.BuildID), mlog.Err(err))
			return err
		}
		return nil
	}
	if err := m.updateSegIndexMeta(segIdx, updateFunc); err != nil {
		return err
	}
	mlog.Info(m.Ctx, "meta update: segment index in progress success", mlog.Int64("buildID", segIdx.BuildID),
		mlog.Int64("segmentID", segIdx.SegmentID))
	return nil
}

func (m *IndexMeta) GetAllSegIndexes() map[int64]*model.SegmentIndex {
	tasks := m.SegmentBuildInfo.List()
	segIndexes := make(map[int64]*model.SegmentIndex, len(tasks))
	for _, segIndex := range tasks {
		segIndexes[segIndex.BuildID] = segIndex
	}
	return segIndexes
}

func (m *IndexMeta) RemoveSegmentIndex(ctx context.Context, buildID UniqueID) error {
	m.KeyLock.Lock(buildID)
	defer m.KeyLock.Unlock(buildID)

	segIdx, ok := m.SegmentBuildInfo.Get(buildID)
	if !ok {
		return nil
	}

	err := m.Catalog.DropSegmentIndex(ctx, segIdx.CollectionID, segIdx.PartitionID, segIdx.SegmentID, buildID)
	if err != nil {
		return err
	}

	m.removeSegmentIndexInMemory(segIdx)
	return nil
}

// removeSegmentIndexInMemory drops a SegmentIndex from the in-memory indexes
// and reconciles its metrics, after its catalog record has been removed. The
// caller must hold keyLock for segIdx.BuildID. RemoveSegmentIndex, which owns
// the catalog removal itself, uses this combined form; the manifest-commit
// staging below uses the same two halves but runs the record removal under
// segMu and defers the gauge until segMu is released, so the two paths cannot
// drift.
func (m *IndexMeta) removeSegmentIndexInMemory(segIdx *model.SegmentIndex) {
	m.subtractStoredIndexSizeOnRemoval(segIdx)
	m.removeSegmentIndexRecordInMemory(segIdx)
}

// subtractStoredIndexSizeOnRemoval is the fieldIndexLock-guarded metric half of
// removeSegmentIndexInMemory. It is split out because the manifest-commit
// install defers it until segMu is released (see stagedSegmentIndexMutation):
// every index DDL holds fieldIndexLock's write side across an etcd round trip,
// so blocking on it inside segMu would stall every segment reader behind that
// round trip.
func (m *IndexMeta) subtractStoredIndexSizeOnRemoval(segIdx *model.SegmentIndex) {
	// Reclaim size only when the index is still alive (segment-drop case);
	// MarkIndexAsDeleted already subtracted otherwise.
	m.fieldIndexLock.RLock()
	m.addStoredIndexSizeMetric(segIdx.CollectionID, segIdx.IndexID,
		-float64(segIdx.IndexSerializedSize))
	m.fieldIndexLock.RUnlock()
}

// removeSegmentIndexRecordInMemory is the record half: it mutates only the
// in-memory segment-index maps and the memory-only task counters. It never
// acquires fieldIndexLock, so it is safe to run inside a segMu critical section.
func (m *IndexMeta) removeSegmentIndexRecordInMemory(segIdx *model.SegmentIndex) {
	segIndexes, ok := m.SegmentIndexes.Get(segIdx.SegmentID)
	if ok {
		segIndexes.Remove(segIdx.IndexID)
		if segIndexes.Len() == 0 {
			m.SegmentIndexes.Remove(segIdx.SegmentID)
		} else {
			m.SegmentIndexes.Insert(segIdx.SegmentID, segIndexes)
		}
	}

	m.taskCounts.replaceTask(segIdx, nil)
	m.SegmentBuildInfo.Remove(segIdx.BuildID)
}

// stageSegmentIndexMutation projects mut against the SegmentIndex record
// current under its per-buildID key lock, and returns the catalog action to
// compose into the caller's transaction plus the in-memory install to run once
// that transaction commits. The lock is held from projection through install,
// so a persisted value can never be built from a read that predates it.
//
// A missing record is terminal for an upsert (errSegmentIndexRecordGone) and
// benign for a removal - the record already being gone is that mutation's
// intended end state, so the caller's manifest revision still publishes.
//
// Precondition: the caller holds keyLock(mut.BuildID) across this projection,
// the returned install, and the deferred metric closure install returns. It is
// acquired by the caller rather than here so that it is ordered before segMu,
// matching every other SegmentIndex writer; see CommitSegmentManifest's
// lock-order note.
func (m *IndexMeta) stageSegmentIndexMutation(mut meta.SegmentIndexMutation) (*meta.StagedSegmentIndexMutation, error) {
	if mut.BuildID == 0 {
		return nil, merr.WrapErrServiceInternalMsg("segment index mutation requires a build ID")
	}
	switch mut.Type {
	case meta.SegmentIndexUpsert:
		if mut.FinishedTask == nil {
			return nil, merr.WrapErrServiceInternalMsg("segment index upsert requires a finished task, buildID=%d", mut.BuildID)
		}
		if mut.FinishedTask.GetBuildID() != mut.BuildID {
			return nil, merr.WrapErrServiceInternalMsg("finished task buildID %d does not match segment index mutation buildID %d",
				mut.FinishedTask.GetBuildID(), mut.BuildID)
		}
	case meta.SegmentIndexRemove:
		if mut.FinishedTask != nil {
			return nil, merr.WrapErrServiceInternalMsg("segment index removal cannot carry a finished task, buildID=%d", mut.BuildID)
		}
	default:
		return nil, merr.WrapErrServiceInternalMsg("unknown segment index mutation type %d, buildID=%d", mut.Type, mut.BuildID)
	}

	staged := &meta.StagedSegmentIndexMutation{Install: func() func() { return func() {} }}

	previous, ok := m.SegmentBuildInfo.Get(mut.BuildID)
	if !ok {
		if mut.Type == meta.SegmentIndexUpsert {
			return staged, meta.ErrSegmentIndexRecordGone
		}
		return staged, nil
	}

	if mut.Type == meta.SegmentIndexRemove {
		segIdx := previous
		action := metastore.DropSegmentIndex(segIdx)
		staged.Action = &action
		staged.Record = segIdx
		staged.Install = func() func() {
			m.removeSegmentIndexRecordInMemory(segIdx)
			return func() { m.subtractStoredIndexSizeOnRemoval(segIdx) }
		}
		return staged, nil
	}

	finished, previousSize, err := m.buildFinishedSegmentIndex(previous, mut.FinishedTask)
	if err != nil {
		return staged, err
	}
	// The task row remains durable throughout Unissued/InProgress/Failed. A
	// successful manifest publication is the single transition that retires
	// it, in the same catalog transaction that advances manifest_path.
	action := metastore.DropSegmentIndex(finished)
	staged.Action = &action
	staged.Record = finished
	staged.Install = func() func() {
		m.updateSegmentIndex(finished)
		// recordFinishedTask is observability only, but its stored-size gauge
		// takes fieldIndexLock, so the whole step is deferred out of segMu.
		return func() { m.recordFinishedTask(previous, finished, previousSize, mut.FinishedTask) }
	}
	return staged, nil
}

func (m *IndexMeta) GetDeletedIndexes() []*model.Index {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

	deletedIndexes := make([]*model.Index, 0)
	for _, fieldIndexes := range m.Indexes {
		for _, index := range fieldIndexes {
			if index.IsDeleted {
				deletedIndexes = append(deletedIndexes, index)
			}
		}
	}
	return deletedIndexes
}

func (m *IndexMeta) RemoveIndex(ctx context.Context, collID, indexID UniqueID) error {
	m.fieldIndexLock.Lock()
	defer m.fieldIndexLock.Unlock()
	mlog.Info(ctx, "IndexCoord meta table remove index", mlog.Int64("collectionID", collID), mlog.Int64("indexID", indexID))
	err := m.Catalog.DropIndex(ctx, collID, indexID)
	if err != nil {
		mlog.Info(ctx, "IndexCoord meta table remove index fail", mlog.Int64("collectionID", collID),
			mlog.Int64("indexID", indexID), mlog.Err(err))
		return err
	}

	m.taskCounts.replaceIndex(m.Indexes[collID][indexID], nil)
	delete(m.Indexes[collID], indexID)
	if len(m.Indexes[collID]) == 0 {
		delete(m.Indexes, collID)
		metrics.IndexTaskNum.Delete(prometheus.Labels{"collection_id": strconv.FormatInt(collID, 10), "index_task_status": metrics.UnissuedIndexTaskLabel})
		metrics.IndexTaskNum.Delete(prometheus.Labels{"collection_id": strconv.FormatInt(collID, 10), "index_task_status": metrics.InProgressIndexTaskLabel})
		metrics.IndexTaskNum.Delete(prometheus.Labels{"collection_id": strconv.FormatInt(collID, 10), "index_task_status": metrics.FinishedIndexTaskLabel})
		metrics.IndexTaskNum.Delete(prometheus.Labels{"collection_id": strconv.FormatInt(collID, 10), "index_task_status": metrics.FailedIndexTaskLabel})
	}
	mlog.Info(ctx, "IndexCoord meta table remove index success", mlog.Int64("collectionID", collID), mlog.Int64("indexID", indexID))
	return nil
}

func (m *IndexMeta) CheckCleanSegmentIndex(buildID UniqueID) (bool, *model.SegmentIndex) {
	if segIndex, ok := m.SegmentBuildInfo.Get(buildID); ok {
		if segIndex.IndexState == commonpb.IndexState_Finished {
			return true, model.CloneSegmentIndex(segIndex)
		}
		return false, model.CloneSegmentIndex(segIndex)
	}
	return true, nil
}

func (m *IndexMeta) GetSegmentsIndexStates(collectionID UniqueID, segmentIDs []UniqueID) map[int64]map[int64]*indexpb.SegmentIndexState {
	ret := make(map[int64]map[int64]*indexpb.SegmentIndexState, 0)
	m.fieldIndexLock.RLock()
	fieldIndexesMap, ok := m.Indexes[collectionID]
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
		segIndexInfos, ok := m.SegmentIndexes.Get(segID)
		if !ok || segIndexInfos.Len() == 0 {
			// Leave the entry out rather than storing an empty map. Both
			// callers read it as `states[indexID]`, which behaves identically
			// on a missing key and on an empty map - and a caller that scans
			// every flushed segment of a large collection on a timer would
			// otherwise allocate one map per segment per pass.
			continue
		}
		ret[segID] = make(map[int64]*indexpb.SegmentIndexState)

		for _, segIdx := range segIndexInfos.Values() {
			if index, ok := fieldIndexes[segIdx.IndexID]; ok && !index.IsDeleted && !segIdx.IsDeleted {
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

func (m *IndexMeta) GetUnindexedSegments(collectionID int64, segmentIDs []int64) []int64 {
	indexes := m.GetIndexesForCollection(collectionID, "")
	if len(indexes) == 0 {
		// doesn't have index
		return nil
	}
	indexed := make([]int64, 0, len(segmentIDs))
	segIndexStates := m.GetSegmentsIndexStates(collectionID, segmentIDs)
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

func (m *IndexMeta) AllDenseWithDiskIndex(collectionID int64, schema *schemapb.CollectionSchema) bool {
	indexInfos := m.GetIndexesForCollection(collectionID, "")

	vectorFields := typeutil.GetDenseVectorFieldSchemas(schema)
	fieldIndexTypes := lo.SliceToMap(indexInfos, func(t *model.Index) (int64, indexparamcheck.IndexType) {
		return t.FieldID, meta.GetIndexType(t.IndexParams)
	})
	vectorFieldsWithDiskIndex := lo.Filter(vectorFields, func(field *schemapb.FieldSchema, _ int) bool {
		if indexType, ok := fieldIndexTypes[field.FieldID]; ok {
			return vecindexmgr.GetVecIndexMgrInstance().IsDiskVecIndex(indexType)
		}
		return false
	})

	return len(vectorFields) == len(vectorFieldsWithDiskIndex)
}

func (m *IndexMeta) HasIndex(collectionID int64) bool {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

	indexes, ok := m.Indexes[collectionID]
	if ok {
		for _, index := range indexes {
			if !index.IsDeleted {
				return true
			}
		}
	}
	return false
}

func (m *IndexMeta) TaskStatsJSON() string {
	tasks := m.SegmentBuildInfo.GetTaskStats()
	ret, err := json.Marshal(tasks)
	if err != nil {
		return ""
	}
	return string(ret)
}

func (m *IndexMeta) GetIndexJSON(collectionID int64) string {
	m.fieldIndexLock.RLock()
	defer m.fieldIndexLock.RUnlock()

	var indexMetrics []*metricsinfo.Index
	for collID, indexes := range m.Indexes {
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

func (m *IndexMeta) GetSegmentIndexedFields(collectionID UniqueID, segmentID UniqueID) (bool, []*metricsinfo.IndexedField) {
	m.fieldIndexLock.RLock()
	fieldIndexesMap, ok := m.Indexes[collectionID]
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
	segIndexInfos, ok := m.SegmentIndexes.Get(segmentID)
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

func (m *IndexMeta) HasBuildInfo(buildID int64) bool {
	_, ok := m.SegmentBuildInfo.Get(buildID)
	return ok
}

func (m *IndexMeta) LockBuildIDs(buildIDs []int64) { lock.LockManyOrdered(m.KeyLock, buildIDs) }

func (m *IndexMeta) UnlockBuildIDs(buildIDs []int64) { m.KeyLock.UnlockMany(buildIDs) }

func (m *IndexMeta) UpdateSegmentIndex(segIdx *model.SegmentIndex) { m.updateSegmentIndex(segIdx) }

func (m *IndexMeta) AddStoredIndexSizeMetric(collID, indexID UniqueID, delta float64) {
	m.addStoredIndexSizeMetric(collID, indexID, delta)
}

func (m *IndexMeta) StageSegmentIndexMutation(mut meta.SegmentIndexMutation) (*meta.StagedSegmentIndexMutation, error) {
	return m.stageSegmentIndexMutation(mut)
}

var _ meta.SegmentIndexCatalog = (*IndexMeta)(nil)
