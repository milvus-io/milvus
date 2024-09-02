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

package datacoord

import (
	"context"
	"fmt"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// serverID return the session serverID
func (s *Server) serverID() int64 {
	if s.session != nil {
		return s.session.GetServerID()
	}
	// return 0 if no session exist, only for UT
	return 0
}

func (s *Server) startIndexService(ctx context.Context) {
	s.serverLoopWg.Add(1)
	go s.createIndexForSegmentLoop(ctx)
}

func (s *Server) createIndexForSegment(segment *SegmentInfo, indexID UniqueID) error {
	if !segment.GetIsSorted() && !segment.GetIsImporting() && segment.Level != datapb.SegmentLevel_L0 {
		log.Info("segment not sorted, skip create index", zap.Int64("segmentID", segment.GetID()))
		return nil
	}
	log.Info("create index for segment", zap.Int64("segmentID", segment.ID), zap.Int64("indexID", indexID))
	buildID, err := s.allocator.AllocID(context.Background())
	if err != nil {
		return err
	}
	segIndex := &model.SegmentIndex{
		SegmentID:    segment.ID,
		CollectionID: segment.CollectionID,
		PartitionID:  segment.PartitionID,
		NumRows:      segment.NumOfRows,
		IndexID:      indexID,
		BuildID:      buildID,
		CreateTime:   uint64(segment.ID),
		WriteHandoff: false,
	}
	if err = s.meta.indexMeta.AddSegmentIndex(segIndex); err != nil {
		return err
	}
	s.taskScheduler.enqueue(newIndexBuildTask(buildID))
	return nil
}

func (s *Server) createIndexesForSegment(segment *SegmentInfo) error {
	if !segment.GetIsSorted() && !segment.GetIsImporting() && segment.GetLevel() != datapb.SegmentLevel_L0 {
		log.Debug("segment is not sorted by pk, skip create index", zap.Int64("segmentID", segment.ID))
		return nil
	}
	indexes := s.meta.indexMeta.GetIndexesForCollection(segment.CollectionID, "")
	indexIDToSegIndexes := s.meta.indexMeta.GetSegmentIndexes(segment.CollectionID, segment.ID)
	for _, index := range indexes {
		if _, ok := indexIDToSegIndexes[index.IndexID]; !ok {
			if err := s.createIndexForSegment(segment, index.IndexID); err != nil {
				log.Warn("create index for segment fail", zap.Int64("segmentID", segment.ID),
					zap.Int64("indexID", index.IndexID))
				return err
			}
		}
	}
	return nil
}

func (s *Server) getUnIndexTaskSegments() []*SegmentInfo {
	flushedSegments := s.meta.SelectSegments(SegmentFilterFunc(func(seg *SegmentInfo) bool {
		return isFlush(seg)
	}))

	unindexedSegments := make([]*SegmentInfo, 0)
	for _, segment := range flushedSegments {
		if s.meta.indexMeta.IsUnIndexedSegment(segment.CollectionID, segment.GetID()) {
			unindexedSegments = append(unindexedSegments, segment)
		}
	}
	return unindexedSegments
}

func (s *Server) createIndexForSegmentLoop(ctx context.Context) {
	log.Info("start create index for segment loop...")
	defer s.serverLoopWg.Done()

	ticker := time.NewTicker(Params.DataCoordCfg.TaskCheckInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Warn("DataCoord context done, exit...")
			return
		case <-ticker.C:
			segments := s.getUnIndexTaskSegments()
			for _, segment := range segments {
				if err := s.createIndexesForSegment(segment); err != nil {
					log.Warn("create index for segment fail, wait for retry", zap.Int64("segmentID", segment.ID))
					continue
				}
			}
		case collectionID := <-s.notifyIndexChan:
			log.Info("receive create index notify", zap.Int64("collectionID", collectionID))
			segments := s.meta.SelectSegments(WithCollection(collectionID), SegmentFilterFunc(func(info *SegmentInfo) bool {
				return isFlush(info) && info.GetIsSorted()
			}))
			for _, segment := range segments {
				if err := s.createIndexesForSegment(segment); err != nil {
					log.Warn("create index for segment fail, wait for retry", zap.Int64("segmentID", segment.ID))
					continue
				}
			}
		case segID := <-s.buildIndexCh:
			log.Info("receive new flushed segment", zap.Int64("segmentID", segID))
			segment := s.meta.GetSegment(segID)
			if segment == nil {
				log.Warn("segment is not exist, no need to build index", zap.Int64("segmentID", segID))
				continue
			}
			if err := s.createIndexesForSegment(segment); err != nil {
				log.Warn("create index for segment fail, wait for retry", zap.Int64("segmentID", segment.ID))
				continue
			}
		}
	}
}

func (s *Server) getFieldNameByID(ctx context.Context, collID, fieldID int64) (string, error) {
	resp, err := s.broker.DescribeCollectionInternal(ctx, collID)
	if err != nil {
		return "", err
	}

	for _, field := range resp.GetSchema().GetFields() {
		if field.FieldID == fieldID {
			return field.Name, nil
		}
	}
	return "", nil
}

// CreateIndex create an index on collection.
// Index building is asynchronous, so when an index building request comes, an IndexID is assigned to the task and
// will get all flushed segments from DataCoord and record tasks with these segments. The background process
// indexBuilder will find this task and assign it to IndexNode for execution.
func (s *Server) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)
	log.Info("receive CreateIndex request",
		zap.String("IndexName", req.GetIndexName()), zap.Int64("fieldID", req.GetFieldID()),
		zap.Any("TypeParams", req.GetTypeParams()),
		zap.Any("IndexParams", req.GetIndexParams()),
	)

	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		log.Warn(msgDataCoordIsUnhealthy(paramtable.GetNodeID()), zap.Error(err))
		return merr.Status(err), nil
	}
	metrics.IndexRequestCounter.WithLabelValues(metrics.TotalLabel).Inc()

	if req.GetIndexName() == "" {
		indexes := s.meta.indexMeta.GetFieldIndexes(req.GetCollectionID(), req.GetFieldID(), req.GetIndexName())
		if len(indexes) == 0 {
			fieldName, err := s.getFieldNameByID(ctx, req.GetCollectionID(), req.GetFieldID())
			if err != nil {
				log.Warn("get field name from schema failed", zap.Int64("fieldID", req.GetFieldID()))
				return merr.Status(err), nil
			}
			req.IndexName = fieldName
		} else if len(indexes) == 1 {
			req.IndexName = indexes[0].IndexName
		}
	}

	indexID, err := s.meta.indexMeta.CanCreateIndex(req)
	if err != nil {
		metrics.IndexRequestCounter.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	// merge with previous params because create index would not pass mmap params
	indexes := s.meta.indexMeta.GetFieldIndexes(req.GetCollectionID(), req.GetFieldID(), req.GetIndexName())
	if len(indexes) == 1 {
		req.UserIndexParams = UpdateParams(indexes[0], indexes[0].UserIndexParams, req.GetUserIndexParams())
		req.IndexParams = UpdateParams(indexes[0], indexes[0].IndexParams, req.GetIndexParams())
	}

	if indexID == 0 {
		indexID, err = s.allocator.AllocID(ctx)
		if err != nil {
			log.Warn("failed to alloc indexID", zap.Error(err))
			metrics.IndexRequestCounter.WithLabelValues(metrics.FailLabel).Inc()
			return merr.Status(err), nil
		}
		if GetIndexType(req.GetIndexParams()) == indexparamcheck.IndexDISKANN && !s.indexNodeManager.ClientSupportDisk() {
			errMsg := "all IndexNodes do not support disk indexes, please verify"
			log.Warn(errMsg)
			err = merr.WrapErrIndexNotSupported(indexparamcheck.IndexDISKANN)
			metrics.IndexRequestCounter.WithLabelValues(metrics.FailLabel).Inc()
			return merr.Status(err), nil
		}
	}

	index := &model.Index{
		CollectionID:    req.GetCollectionID(),
		FieldID:         req.GetFieldID(),
		IndexID:         indexID,
		IndexName:       req.GetIndexName(),
		TypeParams:      req.GetTypeParams(),
		IndexParams:     req.GetIndexParams(),
		CreateTime:      req.GetTimestamp(),
		IsAutoIndex:     req.GetIsAutoIndex(),
		UserIndexParams: req.GetUserIndexParams(),
	}

	if err := ValidateIndexParams(index); err != nil {
		metrics.IndexRequestCounter.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	// Get flushed segments and create index
	err = s.meta.indexMeta.CreateIndex(index)
	if err != nil {
		log.Error("CreateIndex fail",
			zap.Int64("fieldID", req.GetFieldID()), zap.String("indexName", req.GetIndexName()), zap.Error(err))
		metrics.IndexRequestCounter.WithLabelValues(metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	select {
	case s.notifyIndexChan <- req.GetCollectionID():
	default:
	}

	log.Info("CreateIndex successfully",
		zap.String("IndexName", req.GetIndexName()), zap.Int64("fieldID", req.GetFieldID()),
		zap.Int64("IndexID", indexID))
	metrics.IndexRequestCounter.WithLabelValues(metrics.SuccessLabel).Inc()
	return merr.Success(), nil
}

func ValidateIndexParams(index *model.Index) error {
	indexType := GetIndexType(index.IndexParams)
	indexParams := funcutil.KeyValuePair2Map(index.IndexParams)
	userIndexParams := funcutil.KeyValuePair2Map(index.UserIndexParams)
	if err := indexparamcheck.ValidateMmapIndexParams(indexType, indexParams); err != nil {
		return merr.WrapErrParameterInvalidMsg("invalid mmap index params", err.Error())
	}
	if err := indexparamcheck.ValidateMmapIndexParams(indexType, userIndexParams); err != nil {
		return merr.WrapErrParameterInvalidMsg("invalid mmap user index params", err.Error())
	}
	if err := indexparamcheck.ValidateOffsetCacheIndexParams(indexType, indexParams); err != nil {
		return merr.WrapErrParameterInvalidMsg("invalid offset cache index params", err.Error())
	}
	if err := indexparamcheck.ValidateOffsetCacheIndexParams(indexType, userIndexParams); err != nil {
		return merr.WrapErrParameterInvalidMsg("invalid offset cache index params", err.Error())
	}
	return nil
}

func UpdateParams(index *model.Index, from []*commonpb.KeyValuePair, updates []*commonpb.KeyValuePair) []*commonpb.KeyValuePair {
	params := make(map[string]string)
	for _, param := range from {
		params[param.GetKey()] = param.GetValue()
	}

	// update the params
	for _, param := range updates {
		params[param.GetKey()] = param.GetValue()
	}

	return lo.MapToSlice(params, func(k string, v string) *commonpb.KeyValuePair {
		return &commonpb.KeyValuePair{
			Key:   k,
			Value: v,
		}
	})
}

func (s *Server) AlterIndex(ctx context.Context, req *indexpb.AlterIndexRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("indexName", req.GetIndexName()),
	)
	log.Info("received AlterIndex request", zap.Any("params", req.GetParams()))

	if req.IndexName == "" {
		return merr.Status(merr.WrapErrParameterInvalidMsg("index name is empty")), nil
	}

	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		log.Warn(msgDataCoordIsUnhealthy(paramtable.GetNodeID()), zap.Error(err))
		return merr.Status(err), nil
	}

	indexes := s.meta.indexMeta.GetIndexesForCollection(req.GetCollectionID(), req.GetIndexName())
	if len(indexes) == 0 {
		err := merr.WrapErrIndexNotFound(req.GetIndexName())
		return merr.Status(err), nil
	}

	for _, index := range indexes {
		// update user index params
		newUserIndexParams := UpdateParams(index, index.UserIndexParams, req.GetParams())
		log.Info("alter index user index params",
			zap.String("indexName", index.IndexName),
			zap.Any("params", newUserIndexParams),
		)
		index.UserIndexParams = newUserIndexParams

		// update index params
		newIndexParams := UpdateParams(index, index.IndexParams, req.GetParams())
		log.Info("alter index user index params",
			zap.String("indexName", index.IndexName),
			zap.Any("params", newIndexParams),
		)
		index.IndexParams = newIndexParams

		if err := ValidateIndexParams(index); err != nil {
			return merr.Status(err), nil
		}
	}

	err := s.meta.indexMeta.AlterIndex(ctx, indexes...)
	if err != nil {
		log.Warn("failed to alter index", zap.Error(err))
		return merr.Status(err), nil
	}

	return merr.Success(), nil
}

// GetIndexState gets the index state of the index name in the request from Proxy.
// Deprecated
func (s *Server) GetIndexState(ctx context.Context, req *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("indexName", req.GetIndexName()),
	)
	log.Info("receive GetIndexState request")

	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		log.Warn(msgDataCoordIsUnhealthy(paramtable.GetNodeID()), zap.Error(err))
		return &indexpb.GetIndexStateResponse{
			Status: merr.Status(err),
		}, nil
	}

	indexes := s.meta.indexMeta.GetIndexesForCollection(req.GetCollectionID(), req.GetIndexName())
	if len(indexes) == 0 {
		err := merr.WrapErrIndexNotFound(req.GetIndexName())
		log.Warn("GetIndexState fail", zap.Error(err))
		return &indexpb.GetIndexStateResponse{
			Status: merr.Status(err),
		}, nil
	}
	if len(indexes) > 1 {
		log.Warn(msgAmbiguousIndexName())
		err := merr.WrapErrIndexDuplicate(req.GetIndexName())
		return &indexpb.GetIndexStateResponse{
			Status: merr.Status(err),
		}, nil
	}
	ret := &indexpb.GetIndexStateResponse{
		Status: merr.Success(),
		State:  commonpb.IndexState_Finished,
	}

	indexInfo := &indexpb.IndexInfo{}
	// The total rows of all indexes should be based on the current perspective
	segments := s.selectSegmentIndexesStats(WithCollection(req.GetCollectionID()), SegmentFilterFunc(func(info *SegmentInfo) bool {
		return info.GetLevel() != datapb.SegmentLevel_L0 && (isFlush(info) || info.GetState() == commonpb.SegmentState_Dropped)
	}))

	s.completeIndexInfo(indexInfo, indexes[0], segments, false, indexes[0].CreateTime)
	ret.State = indexInfo.State
	ret.FailReason = indexInfo.IndexStateFailReason

	log.Info("GetIndexState success",
		zap.String("state", ret.GetState().String()),
	)
	return ret, nil
}

func (s *Server) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)
	log.Info("receive GetSegmentIndexState",
		zap.String("IndexName", req.GetIndexName()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
	)

	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		log.Warn(msgDataCoordIsUnhealthy(paramtable.GetNodeID()), zap.Error(err))
		return &indexpb.GetSegmentIndexStateResponse{
			Status: merr.Status(err),
		}, nil
	}

	ret := &indexpb.GetSegmentIndexStateResponse{
		Status: merr.Success(),
		States: make([]*indexpb.SegmentIndexState, 0),
	}
	indexID2CreateTs := s.meta.indexMeta.GetIndexIDByName(req.GetCollectionID(), req.GetIndexName())
	if len(indexID2CreateTs) == 0 {
		err := merr.WrapErrIndexNotFound(req.GetIndexName())
		log.Warn("GetSegmentIndexState fail", zap.String("indexName", req.GetIndexName()), zap.Error(err))
		return &indexpb.GetSegmentIndexStateResponse{
			Status: merr.Status(err),
		}, nil
	}
	for _, segID := range req.GetSegmentIDs() {
		for indexID := range indexID2CreateTs {
			state := s.meta.indexMeta.GetSegmentIndexState(req.GetCollectionID(), segID, indexID)
			ret.States = append(ret.States, state)
		}
	}
	log.Info("GetSegmentIndexState successfully", zap.String("indexName", req.GetIndexName()))
	return ret, nil
}

func (s *Server) selectSegmentIndexesStats(filters ...SegmentFilter) map[int64]*indexStats {
	ret := make(map[int64]*indexStats)

	segments := s.meta.SelectSegments(filters...)
	segmentIDs := lo.Map(segments, func(info *SegmentInfo, i int) int64 {
		return info.GetID()
	})
	if len(segments) == 0 {
		return ret
	}
	segmentsIndexes := s.meta.indexMeta.getSegmentsIndexStates(segments[0].CollectionID, segmentIDs)
	for _, info := range segments {
		is := &indexStats{
			ID:             info.GetID(),
			numRows:        info.GetNumOfRows(),
			compactionFrom: info.GetCompactionFrom(),
			indexStates:    segmentsIndexes[info.GetID()],
			state:          info.GetState(),
			lastExpireTime: info.GetLastExpireTime(),
		}
		ret[info.GetID()] = is
	}
	return ret
}

func (s *Server) countIndexedRows(indexInfo *indexpb.IndexInfo, segments map[int64]*indexStats) int64 {
	unIndexed, indexed := typeutil.NewSet[int64](), typeutil.NewSet[int64]()
	for segID, seg := range segments {
		if seg.state != commonpb.SegmentState_Flushed && seg.state != commonpb.SegmentState_Flushing {
			continue
		}
		segIdx, ok := seg.indexStates[indexInfo.IndexID]
		if !ok {
			unIndexed.Insert(segID)
			continue
		}
		switch segIdx.GetState() {
		case commonpb.IndexState_Finished:
			indexed.Insert(segID)
		default:
			unIndexed.Insert(segID)
		}
	}
	retrieveContinue := len(unIndexed) != 0
	for retrieveContinue {
		for segID := range unIndexed {
			unIndexed.Remove(segID)
			segment := segments[segID]
			if segment == nil || len(segment.compactionFrom) == 0 {
				continue
			}
			for _, fromID := range segment.compactionFrom {
				fromSeg := segments[fromID]
				if fromSeg == nil {
					continue
				}
				if segIndex, ok := fromSeg.indexStates[indexInfo.IndexID]; ok && segIndex.GetState() == commonpb.IndexState_Finished {
					indexed.Insert(fromID)
					continue
				}
				unIndexed.Insert(fromID)
			}
		}
		retrieveContinue = len(unIndexed) != 0
	}
	indexedRows := int64(0)
	for segID := range indexed {
		segment := segments[segID]
		if segment != nil {
			indexedRows += segment.numRows
		}
	}
	return indexedRows
}

// completeIndexInfo get the index row count and index task state
// if realTime, calculate current statistics
// if not realTime, which means get info of the prior `CreateIndex` action, skip segments created after index's create time
func (s *Server) completeIndexInfo(indexInfo *indexpb.IndexInfo, index *model.Index, segments map[int64]*indexStats, realTime bool, ts Timestamp) {
	var (
		cntNone          = 0
		cntUnissued      = 0
		cntInProgress    = 0
		cntFinished      = 0
		cntFailed        = 0
		failReason       string
		totalRows        = int64(0)
		indexedRows      = int64(0)
		pendingIndexRows = int64(0)
	)

	for segID, seg := range segments {
		if seg.state != commonpb.SegmentState_Flushed && seg.state != commonpb.SegmentState_Flushing {
			continue
		}
		totalRows += seg.numRows
		segIdx, ok := seg.indexStates[index.IndexID]

		if !ok {
			if seg.lastExpireTime <= ts {
				cntUnissued++
			}
			pendingIndexRows += seg.numRows
			continue
		}
		if segIdx.GetState() != commonpb.IndexState_Finished {
			pendingIndexRows += seg.numRows
		}

		// if realTime, calculate current statistics
		// if not realTime, skip segments created after index create
		if !realTime && seg.lastExpireTime > ts {
			continue
		}

		switch segIdx.GetState() {
		case commonpb.IndexState_IndexStateNone:
			// can't to here
			log.Warn("receive unexpected index state: IndexStateNone", zap.Int64("segmentID", segID))
			cntNone++
		case commonpb.IndexState_Unissued:
			cntUnissued++
		case commonpb.IndexState_InProgress:
			cntInProgress++
		case commonpb.IndexState_Finished:
			cntFinished++
			indexedRows += seg.numRows
		case commonpb.IndexState_Failed:
			cntFailed++
			failReason += fmt.Sprintf("%d: %s;", segID, segIdx.FailReason)
		}
	}

	if realTime {
		indexInfo.IndexedRows = indexedRows
	} else {
		indexInfo.IndexedRows = s.countIndexedRows(indexInfo, segments)
	}
	indexInfo.TotalRows = totalRows
	indexInfo.PendingIndexRows = pendingIndexRows
	switch {
	case cntFailed > 0:
		indexInfo.State = commonpb.IndexState_Failed
		indexInfo.IndexStateFailReason = failReason
	case cntInProgress > 0 || cntUnissued > 0:
		indexInfo.State = commonpb.IndexState_InProgress
	case cntNone > 0:
		indexInfo.State = commonpb.IndexState_IndexStateNone
	default:
		indexInfo.State = commonpb.IndexState_Finished
	}

	log.RatedInfo(60, "completeIndexInfo success", zap.Int64("collectionID", index.CollectionID), zap.Int64("indexID", index.IndexID),
		zap.Int64("totalRows", indexInfo.TotalRows), zap.Int64("indexRows", indexInfo.IndexedRows),
		zap.Int64("pendingIndexRows", indexInfo.PendingIndexRows),
		zap.String("state", indexInfo.State.String()), zap.String("failReason", indexInfo.IndexStateFailReason))
}

// GetIndexBuildProgress get the index building progress by num rows.
// Deprecated
func (s *Server) GetIndexBuildProgress(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest) (*indexpb.GetIndexBuildProgressResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)
	log.Info("receive GetIndexBuildProgress request", zap.String("indexName", req.GetIndexName()))

	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		log.Warn(msgDataCoordIsUnhealthy(paramtable.GetNodeID()), zap.Error(err))
		return &indexpb.GetIndexBuildProgressResponse{
			Status: merr.Status(err),
		}, nil
	}

	indexes := s.meta.indexMeta.GetIndexesForCollection(req.GetCollectionID(), req.GetIndexName())
	if len(indexes) == 0 {
		err := merr.WrapErrIndexNotFound(req.GetIndexName())
		log.Warn("GetIndexBuildProgress fail", zap.String("indexName", req.IndexName), zap.Error(err))
		return &indexpb.GetIndexBuildProgressResponse{
			Status: merr.Status(err),
		}, nil
	}

	if len(indexes) > 1 {
		log.Warn(msgAmbiguousIndexName())
		err := merr.WrapErrIndexDuplicate(req.GetIndexName())
		return &indexpb.GetIndexBuildProgressResponse{
			Status: merr.Status(err),
		}, nil
	}
	indexInfo := &indexpb.IndexInfo{
		CollectionID:     req.GetCollectionID(),
		IndexID:          indexes[0].IndexID,
		IndexedRows:      0,
		TotalRows:        0,
		PendingIndexRows: 0,
		State:            0,
	}

	// The total rows of all indexes should be based on the current perspective
	segments := s.selectSegmentIndexesStats(WithCollection(req.GetCollectionID()), SegmentFilterFunc(func(info *SegmentInfo) bool {
		return info.GetLevel() != datapb.SegmentLevel_L0 && (isFlush(info) || info.GetState() == commonpb.SegmentState_Dropped)
	}))

	s.completeIndexInfo(indexInfo, indexes[0], segments, false, indexes[0].CreateTime)
	log.Info("GetIndexBuildProgress success", zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("indexName", req.GetIndexName()))
	return &indexpb.GetIndexBuildProgressResponse{
		Status:           merr.Success(),
		IndexedRows:      indexInfo.IndexedRows,
		TotalRows:        indexInfo.TotalRows,
		PendingIndexRows: indexInfo.PendingIndexRows,
	}, nil
}

// indexStats just for indexing statistics.
// Please use it judiciously.
type indexStats struct {
	ID             int64
	numRows        int64
	compactionFrom []int64
	indexStates    map[int64]*indexpb.SegmentIndexState
	state          commonpb.SegmentState
	lastExpireTime uint64
}

// DescribeIndex describe the index info of the collection.
func (s *Server) DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("indexName", req.GetIndexName()),
		zap.Uint64("timestamp", req.GetTimestamp()),
	)
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		log.Warn(msgDataCoordIsUnhealthy(paramtable.GetNodeID()), zap.Error(err))
		return &indexpb.DescribeIndexResponse{
			Status: merr.Status(err),
		}, nil
	}

	indexes := s.meta.indexMeta.GetIndexesForCollection(req.GetCollectionID(), req.GetIndexName())
	if len(indexes) == 0 {
		err := merr.WrapErrIndexNotFound(req.GetIndexName())
		log.RatedWarn(60, "DescribeIndex fail", zap.Error(err))
		return &indexpb.DescribeIndexResponse{
			Status: merr.Status(err),
		}, nil
	}

	// The total rows of all indexes should be based on the current perspective
	segments := s.selectSegmentIndexesStats(WithCollection(req.GetCollectionID()), SegmentFilterFunc(func(info *SegmentInfo) bool {
		return info.GetLevel() != datapb.SegmentLevel_L0 && (isFlush(info) || info.GetState() == commonpb.SegmentState_Dropped)
	}))

	indexInfos := make([]*indexpb.IndexInfo, 0)
	for _, index := range indexes {
		indexInfo := &indexpb.IndexInfo{
			CollectionID:         index.CollectionID,
			FieldID:              index.FieldID,
			IndexName:            index.IndexName,
			IndexID:              index.IndexID,
			TypeParams:           index.TypeParams,
			IndexParams:          index.IndexParams,
			IndexedRows:          0,
			TotalRows:            0,
			State:                0,
			IndexStateFailReason: "",
			IsAutoIndex:          index.IsAutoIndex,
			UserIndexParams:      index.UserIndexParams,
		}
		createTs := index.CreateTime
		if req.GetTimestamp() != 0 {
			createTs = req.GetTimestamp()
		}
		s.completeIndexInfo(indexInfo, index, segments, false, createTs)
		indexInfos = append(indexInfos, indexInfo)
	}
	return &indexpb.DescribeIndexResponse{
		Status:     merr.Success(),
		IndexInfos: indexInfos,
	}, nil
}

// GetIndexStatistics get the statistics of the index. DescribeIndex doesn't contain statistics.
func (s *Server) GetIndexStatistics(ctx context.Context, req *indexpb.GetIndexStatisticsRequest) (*indexpb.GetIndexStatisticsResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)
	log.Info("receive GetIndexStatistics request", zap.String("indexName", req.GetIndexName()))
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		log.Warn(msgDataCoordIsUnhealthy(paramtable.GetNodeID()), zap.Error(err))
		return &indexpb.GetIndexStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	indexes := s.meta.indexMeta.GetIndexesForCollection(req.GetCollectionID(), req.GetIndexName())
	if len(indexes) == 0 {
		err := merr.WrapErrIndexNotFound(req.GetIndexName())
		log.Warn("GetIndexStatistics fail",
			zap.String("indexName", req.GetIndexName()),
			zap.Error(err))
		return &indexpb.GetIndexStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	// The total rows of all indexes should be based on the current perspective
	segments := s.selectSegmentIndexesStats(WithCollection(req.GetCollectionID()), SegmentFilterFunc(func(info *SegmentInfo) bool {
		return info.GetLevel() != datapb.SegmentLevel_L0 && (isFlush(info) || info.GetState() == commonpb.SegmentState_Dropped)
	}))

	indexInfos := make([]*indexpb.IndexInfo, 0)
	for _, index := range indexes {
		indexInfo := &indexpb.IndexInfo{
			CollectionID:         index.CollectionID,
			FieldID:              index.FieldID,
			IndexName:            index.IndexName,
			IndexID:              index.IndexID,
			TypeParams:           index.TypeParams,
			IndexParams:          index.IndexParams,
			IndexedRows:          0,
			TotalRows:            0,
			State:                0,
			IndexStateFailReason: "",
			IsAutoIndex:          index.IsAutoIndex,
			UserIndexParams:      index.UserIndexParams,
		}
		s.completeIndexInfo(indexInfo, index, segments, true, index.CreateTime)
		indexInfos = append(indexInfos, indexInfo)
	}
	log.Debug("GetIndexStatisticsResponse success",
		zap.String("indexName", req.GetIndexName()))
	return &indexpb.GetIndexStatisticsResponse{
		Status:     merr.Success(),
		IndexInfos: indexInfos,
	}, nil
}

// DropIndex deletes indexes based on IndexName. One IndexName corresponds to the index of an entire column. A column is
// divided into many segments, and each segment corresponds to an IndexBuildID. DataCoord uses IndexBuildID to record
// index tasks.
func (s *Server) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)
	log.Info("receive DropIndex request",
		zap.Int64s("partitionIDs", req.GetPartitionIDs()), zap.String("indexName", req.GetIndexName()),
		zap.Bool("drop all indexes", req.GetDropAll()))

	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		log.Warn(msgDataCoordIsUnhealthy(paramtable.GetNodeID()), zap.Error(err))
		return merr.Status(err), nil
	}

	indexes := s.meta.indexMeta.GetIndexesForCollection(req.GetCollectionID(), req.GetIndexName())
	if len(indexes) == 0 {
		log.Info(fmt.Sprintf("there is no index on collection: %d with the index name: %s", req.CollectionID, req.IndexName))
		return merr.Success(), nil
	}

	if !req.GetDropAll() && len(indexes) > 1 {
		log.Warn(msgAmbiguousIndexName())
		err := merr.WrapErrIndexDuplicate(req.GetIndexName())
		return merr.Status(err), nil
	}
	indexIDs := make([]UniqueID, 0)
	for _, index := range indexes {
		indexIDs = append(indexIDs, index.IndexID)
	}
	// Compatibility logic. To prevent the index on the corresponding segments
	// from being dropped at the same time when dropping_partition in version 2.1
	if len(req.GetPartitionIDs()) == 0 {
		// drop collection index
		err := s.meta.indexMeta.MarkIndexAsDeleted(req.GetCollectionID(), indexIDs)
		if err != nil {
			log.Warn("DropIndex fail", zap.String("indexName", req.IndexName), zap.Error(err))
			return merr.Status(err), nil
		}
	}

	log.Debug("DropIndex success", zap.Int64s("partitionIDs", req.GetPartitionIDs()),
		zap.String("indexName", req.GetIndexName()), zap.Int64s("indexIDs", indexIDs))
	return merr.Success(), nil
}

// GetIndexInfos gets the index file paths for segment from DataCoord.
func (s *Server) GetIndexInfos(ctx context.Context, req *indexpb.GetIndexInfoRequest) (*indexpb.GetIndexInfoResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)

	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		log.Warn(msgDataCoordIsUnhealthy(paramtable.GetNodeID()), zap.Error(err))
		return &indexpb.GetIndexInfoResponse{
			Status: merr.Status(err),
		}, nil
	}
	ret := &indexpb.GetIndexInfoResponse{
		Status:      merr.Success(),
		SegmentInfo: map[int64]*indexpb.SegmentInfo{},
	}

	for _, segID := range req.GetSegmentIDs() {
		segIdxes := s.meta.indexMeta.GetSegmentIndexes(req.GetCollectionID(), segID)
		ret.SegmentInfo[segID] = &indexpb.SegmentInfo{
			CollectionID: req.GetCollectionID(),
			SegmentID:    segID,
			EnableIndex:  false,
			IndexInfos:   make([]*indexpb.IndexFilePathInfo, 0),
		}
		if len(segIdxes) != 0 {
			ret.SegmentInfo[segID].EnableIndex = true
			for _, segIdx := range segIdxes {
				if segIdx.IndexState == commonpb.IndexState_Finished {
					indexFilePaths := metautil.BuildSegmentIndexFilePaths(s.meta.chunkManager.RootPath(), segIdx.BuildID, segIdx.IndexVersion,
						segIdx.PartitionID, segIdx.SegmentID, segIdx.IndexFileKeys)
					indexParams := s.meta.indexMeta.GetIndexParams(segIdx.CollectionID, segIdx.IndexID)
					indexParams = append(indexParams, s.meta.indexMeta.GetTypeParams(segIdx.CollectionID, segIdx.IndexID)...)
					ret.SegmentInfo[segID].IndexInfos = append(ret.SegmentInfo[segID].IndexInfos,
						&indexpb.IndexFilePathInfo{
							SegmentID:           segID,
							FieldID:             s.meta.indexMeta.GetFieldIDByIndexID(segIdx.CollectionID, segIdx.IndexID),
							IndexID:             segIdx.IndexID,
							BuildID:             segIdx.BuildID,
							IndexName:           s.meta.indexMeta.GetIndexNameByID(segIdx.CollectionID, segIdx.IndexID),
							IndexParams:         indexParams,
							IndexFilePaths:      indexFilePaths,
							SerializedSize:      segIdx.IndexSize,
							IndexVersion:        segIdx.IndexVersion,
							NumRows:             segIdx.NumRows,
							CurrentIndexVersion: segIdx.CurrentIndexVersion,
						})
				}
			}
		}
	}

	log.Debug("GetIndexInfos successfully", zap.String("indexName", req.GetIndexName()))

	return ret, nil
}

// ListIndexes returns all indexes created on provided collection.
func (s *Server) ListIndexes(ctx context.Context, req *indexpb.ListIndexesRequest) (*indexpb.ListIndexesResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)

	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		log.Warn(msgDataCoordIsUnhealthy(paramtable.GetNodeID()), zap.Error(err))
		return &indexpb.ListIndexesResponse{
			Status: merr.Status(err),
		}, nil
	}

	indexes := s.meta.indexMeta.GetIndexesForCollection(req.GetCollectionID(), "")

	indexInfos := lo.Map(indexes, func(index *model.Index, _ int) *indexpb.IndexInfo {
		return &indexpb.IndexInfo{
			CollectionID:    index.CollectionID,
			FieldID:         index.FieldID,
			IndexName:       index.IndexName,
			IndexID:         index.IndexID,
			TypeParams:      index.TypeParams,
			IndexParams:     index.IndexParams,
			IsAutoIndex:     index.IsAutoIndex,
			UserIndexParams: index.UserIndexParams,
		}
	})
	log.Debug("List index success")
	return &indexpb.ListIndexesResponse{
		Status:     merr.Success(),
		IndexInfos: indexInfos,
	}, nil
}
