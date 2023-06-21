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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// serverID return the session serverID
func (s *Server) serverID() int64 {
	if s.session != nil {
		return s.session.ServerID
	}
	// return 0 if no session exist, only for UT
	return 0
}

func (s *Server) startIndexService(ctx context.Context) {
	s.indexBuilder.Start()

	s.serverLoopWg.Add(1)
	go s.createIndexForSegmentLoop(ctx)
}

func (s *Server) createIndexForSegment(segment *SegmentInfo, indexID UniqueID) error {
	log.Info("create index for segment", zap.Int64("segID", segment.ID), zap.Int64("indexID", indexID))
	buildID, err := s.allocator.allocID(context.Background())
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
	if err = s.meta.AddSegmentIndex(segIndex); err != nil {
		return err
	}
	s.indexBuilder.enqueue(buildID)
	return nil
}

func (s *Server) createIndexesForSegment(segment *SegmentInfo) error {
	indexes := s.meta.GetIndexesForCollection(segment.CollectionID, "")
	for _, index := range indexes {
		if _, ok := segment.segmentIndexes[index.IndexID]; !ok {
			if err := s.createIndexForSegment(segment, index.IndexID); err != nil {
				log.Warn("create index for segment fail", zap.Int64("segID", segment.ID),
					zap.Int64("indexID", index.IndexID))
				return err
			}
		}
	}
	return nil
}

func (s *Server) createIndexForSegmentLoop(ctx context.Context) {
	log.Info("start create index for segment loop...")
	defer s.serverLoopWg.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Warn("DataCoord context done, exit...")
			return
		case <-ticker.C:
			segments := s.meta.GetHasUnindexTaskSegments()
			for _, segment := range segments {
				if err := s.createIndexesForSegment(segment); err != nil {
					log.Warn("create index for segment fail, wait for retry", zap.Int64("segID", segment.ID))
					continue
				}
			}
		case collID := <-s.notifyIndexChan:
			log.Info("receive create index notify", zap.Int64("collID", collID))
			segments := s.meta.SelectSegments(func(info *SegmentInfo) bool {
				return isFlush(info) && collID == info.CollectionID
			})
			for _, segment := range segments {
				if err := s.createIndexesForSegment(segment); err != nil {
					log.Warn("create index for segment fail, wait for retry", zap.Int64("segID", segment.ID))
					continue
				}
			}
		case segID := <-s.buildIndexCh:
			log.Info("receive new flushed segment", zap.Int64("segID", segID))
			segment := s.meta.GetSegment(segID)
			if segment == nil {
				log.Warn("segment is not exist, no need to build index", zap.Int64("segID", segID))
				continue
			}
			if err := s.createIndexesForSegment(segment); err != nil {
				log.Warn("create index for segment fail, wait for retry", zap.Int64("segID", segment.ID))
				continue
			}
		}
	}
}

// CreateIndex create an index on collection.
// Index building is asynchronous, so when an index building request comes, an IndexID is assigned to the task and
// will get all flushed segments from DataCoord and record tasks with these segments. The background process
// indexBuilder will find this task and assign it to IndexNode for execution.
func (s *Server) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx)
	log.Info("receive CreateIndex request", zap.Int64("CollectionID", req.GetCollectionID()),
		zap.String("IndexName", req.GetIndexName()), zap.Int64("fieldID", req.GetFieldID()),
		zap.Any("TypeParams", req.GetTypeParams()),
		zap.Any("IndexParams", req.GetIndexParams()))
	errResp := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "",
	}
	if s.isClosed() {
		log.Warn(msgDataCoordIsUnhealthy(paramtable.GetNodeID()))
		errResp.ErrorCode = commonpb.ErrorCode_DataCoordNA
		errResp.Reason = msgDataCoordIsUnhealthy(paramtable.GetNodeID())
		return errResp, nil
	}
	metrics.IndexRequestCounter.WithLabelValues(metrics.TotalLabel).Inc()

	indexID, err := s.meta.CanCreateIndex(req)
	if err != nil {
		log.Error("CreateIndex failed", zap.Error(err))
		errResp.Reason = err.Error()
		metrics.IndexRequestCounter.WithLabelValues(metrics.FailLabel).Inc()
		return errResp, nil
	}

	if indexID == 0 {
		indexID, err = s.allocator.allocID(ctx)
		if err != nil {
			log.Warn("failed to alloc indexID", zap.Error(err))
			errResp.Reason = "failed to alloc indexID"
			metrics.IndexRequestCounter.WithLabelValues(metrics.FailLabel).Inc()
			return errResp, nil
		}
		if getIndexType(req.GetIndexParams()) == diskAnnIndex && !s.indexNodeManager.ClientSupportDisk() {
			errMsg := "all IndexNodes do not support disk indexes, please verify"
			log.Warn(errMsg)
			errResp.Reason = errMsg
			metrics.IndexRequestCounter.WithLabelValues(metrics.FailLabel).Inc()
			return errResp, nil
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

	// Get flushed segments and create index

	err = s.meta.CreateIndex(index)
	if err != nil {
		log.Error("CreateIndex fail", zap.Int64("collectionID", req.GetCollectionID()),
			zap.Int64("fieldID", req.GetFieldID()), zap.String("indexName", req.GetIndexName()), zap.Error(err))
		errResp.Reason = err.Error()
		metrics.IndexRequestCounter.WithLabelValues(metrics.FailLabel).Inc()
		return errResp, nil
	}

	select {
	case s.notifyIndexChan <- req.GetCollectionID():
	default:
	}

	log.Info("CreateIndex successfully", zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("IndexName", req.GetIndexName()), zap.Int64("fieldID", req.GetFieldID()),
		zap.Int64("IndexID", indexID))
	errResp.ErrorCode = commonpb.ErrorCode_Success
	metrics.IndexRequestCounter.WithLabelValues(metrics.SuccessLabel).Inc()
	return errResp, nil
}

// GetIndexState gets the index state of the index name in the request from Proxy.
// Deprecated
func (s *Server) GetIndexState(ctx context.Context, req *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error) {
	log := log.Ctx(ctx)
	log.Info("receive GetIndexState request", zap.Int64("collectionID", req.CollectionID),
		zap.String("indexName", req.IndexName))

	errResp := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "",
	}
	if s.isClosed() {
		log.Warn(msgDataCoordIsUnhealthy(paramtable.GetNodeID()))
		errResp.ErrorCode = commonpb.ErrorCode_DataCoordNA
		errResp.Reason = msgDataCoordIsUnhealthy(paramtable.GetNodeID())
		return &indexpb.GetIndexStateResponse{
			Status: errResp,
		}, nil
	}

	indexes := s.meta.GetIndexesForCollection(req.GetCollectionID(), req.GetIndexName())
	if len(indexes) == 0 {
		errResp.ErrorCode = commonpb.ErrorCode_IndexNotExist
		errResp.Reason = fmt.Sprintf("there is no index on collection: %d with the index name: %s", req.CollectionID, req.IndexName)
		log.Error("GetIndexState fail", zap.Int64("collectionID", req.CollectionID),
			zap.String("indexName", req.IndexName), zap.String("fail reason", errResp.Reason))
		return &indexpb.GetIndexStateResponse{
			Status: errResp,
		}, nil
	}
	if len(indexes) > 1 {
		log.Warn(msgAmbiguousIndexName())
		errResp.ErrorCode = commonpb.ErrorCode_UnexpectedError
		errResp.Reason = msgAmbiguousIndexName()
		return &indexpb.GetIndexStateResponse{
			Status: errResp,
		}, nil
	}
	ret := &indexpb.GetIndexStateResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		State: commonpb.IndexState_Finished,
	}

	indexInfo := &indexpb.IndexInfo{
		IndexedRows:          0,
		TotalRows:            0,
		State:                0,
		IndexStateFailReason: "",
	}
	s.completeIndexInfo(indexInfo, indexes[0], s.meta.SelectSegments(func(info *SegmentInfo) bool {
		return isFlush(info) && info.CollectionID == req.GetCollectionID()
	}), false)
	ret.State = indexInfo.State
	ret.FailReason = indexInfo.IndexStateFailReason

	log.Info("GetIndexState success", zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("IndexName", req.GetIndexName()), zap.String("state", ret.GetState().String()))
	return ret, nil
}

func (s *Server) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error) {
	log := log.Ctx(ctx)
	log.Info("receive GetSegmentIndexState", zap.Int64("CollectionID", req.GetCollectionID()),
		zap.String("IndexName", req.GetIndexName()), zap.Int64s("fieldID", req.GetSegmentIDs()))
	errResp := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "",
	}
	if s.isClosed() {
		log.Warn(msgDataCoordIsUnhealthy(paramtable.GetNodeID()))
		errResp.ErrorCode = commonpb.ErrorCode_DataCoordNA
		errResp.Reason = msgDataCoordIsUnhealthy(paramtable.GetNodeID())
		return &indexpb.GetSegmentIndexStateResponse{
			Status: errResp,
		}, nil
	}

	ret := &indexpb.GetSegmentIndexStateResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		States: make([]*indexpb.SegmentIndexState, 0),
	}
	indexID2CreateTs := s.meta.GetIndexIDByName(req.GetCollectionID(), req.GetIndexName())
	if len(indexID2CreateTs) == 0 {
		errMsg := fmt.Sprintf("there is no index on collection: %d with the index name: %s", req.CollectionID, req.GetIndexName())
		log.Error("GetSegmentIndexState fail", zap.Int64("collectionID", req.GetCollectionID()),
			zap.String("indexName", req.GetIndexName()), zap.String("fail reason", errMsg))
		return &indexpb.GetSegmentIndexStateResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_IndexNotExist,
				Reason:    errMsg,
			},
		}, nil
	}
	for _, segID := range req.SegmentIDs {
		state := s.meta.GetSegmentIndexState(req.GetCollectionID(), segID)
		ret.States = append(ret.States, &indexpb.SegmentIndexState{
			SegmentID:  segID,
			State:      state.state,
			FailReason: state.failReason,
		})
	}
	log.Info("GetSegmentIndexState successfully", zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("indexName", req.GetIndexName()))
	return ret, nil
}

func (s *Server) countIndexedRows(indexInfo *indexpb.IndexInfo, segments []*SegmentInfo) int64 {
	unIndexed, indexed := typeutil.NewSet[int64](), typeutil.NewSet[int64]()
	for _, seg := range segments {
		segIdx, ok := seg.segmentIndexes[indexInfo.IndexID]
		if !ok {
			unIndexed.Insert(seg.GetID())
			continue
		}
		switch segIdx.IndexState {
		case commonpb.IndexState_Finished:
			indexed.Insert(seg.GetID())
		default:
			unIndexed.Insert(seg.GetID())
		}
	}
	retrieveContinue := len(unIndexed) != 0
	for retrieveContinue {
		for segID := range unIndexed {
			unIndexed.Remove(segID)
			segment := s.meta.GetSegment(segID)
			if segment == nil || len(segment.CompactionFrom) == 0 {
				continue
			}
			for _, fromID := range segment.CompactionFrom {
				fromSeg := s.meta.GetSegment(fromID)
				if fromSeg == nil {
					continue
				}
				if segIndex, ok := fromSeg.segmentIndexes[indexInfo.IndexID]; ok && segIndex.IndexState == commonpb.IndexState_Finished {
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
		segment := s.meta.GetSegment(segID)
		if segment != nil {
			indexedRows += segment.GetNumOfRows()
		}
	}
	return indexedRows
}

// completeIndexInfo get the index row count and index task state
// if realTime, calculate current statistics
// if not realTime, which means get info of the prior `CreateIndex` action, skip segments created after index's create time
func (s *Server) completeIndexInfo(indexInfo *indexpb.IndexInfo, index *model.Index, segments []*SegmentInfo, realTime bool) {
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

	for _, seg := range segments {
		totalRows += seg.NumOfRows
		segIdx, ok := seg.segmentIndexes[index.IndexID]

		if !ok {
			if seg.GetStartPosition().GetTimestamp() <= index.CreateTime {
				cntUnissued++
			}
			pendingIndexRows += seg.GetNumOfRows()
			continue
		}
		if segIdx.IndexState != commonpb.IndexState_Finished {
			pendingIndexRows += seg.GetNumOfRows()
		}

		// if realTime, calculate current statistics
		// if not realTime, skip segments created after index create
		if !realTime && seg.GetStartPosition().GetTimestamp() > index.CreateTime {
			continue
		}

		switch segIdx.IndexState {
		case commonpb.IndexState_IndexStateNone:
			// can't to here
			log.Warn("receive unexpected index state: IndexStateNone", zap.Int64("segmentID", segIdx.SegmentID))
			cntNone++
		case commonpb.IndexState_Unissued:
			cntUnissued++
		case commonpb.IndexState_InProgress:
			cntInProgress++
		case commonpb.IndexState_Finished:
			cntFinished++
			indexedRows += seg.NumOfRows
		case commonpb.IndexState_Failed:
			cntFailed++
			failReason += fmt.Sprintf("%d: %s;", segIdx.SegmentID, segIdx.FailReason)
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

	log.Info("completeIndexInfo success", zap.Int64("collID", index.CollectionID), zap.Int64("indexID", index.IndexID),
		zap.Int64("totalRows", indexInfo.TotalRows), zap.Int64("indexRows", indexInfo.IndexedRows),
		zap.Int64("pendingIndexRows", indexInfo.PendingIndexRows),
		zap.String("state", indexInfo.State.String()), zap.String("failReason", indexInfo.IndexStateFailReason))
}

// GetIndexBuildProgress get the index building progress by num rows.
// Deprecated
func (s *Server) GetIndexBuildProgress(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest) (*indexpb.GetIndexBuildProgressResponse, error) {
	log := log.Ctx(ctx)
	log.Info("receive GetIndexBuildProgress request", zap.Int64("collID", req.GetCollectionID()),
		zap.String("indexName", req.GetIndexName()))
	errResp := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "",
	}
	if s.isClosed() {
		log.Warn(msgDataCoordIsUnhealthy(paramtable.GetNodeID()))
		errResp.ErrorCode = commonpb.ErrorCode_DataCoordNA
		errResp.Reason = msgDataCoordIsUnhealthy(paramtable.GetNodeID())
		return &indexpb.GetIndexBuildProgressResponse{
			Status: errResp,
		}, nil
	}

	indexes := s.meta.GetIndexesForCollection(req.GetCollectionID(), req.GetIndexName())
	if len(indexes) == 0 {
		errMsg := fmt.Sprintf("there is no index on collection: %d with the index name: %s", req.CollectionID, req.IndexName)
		log.Error("GetIndexBuildProgress fail", zap.Int64("collectionID", req.CollectionID),
			zap.String("indexName", req.IndexName), zap.String("fail reason", errMsg))
		return &indexpb.GetIndexBuildProgressResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_IndexNotExist,
				Reason:    errMsg,
			},
		}, nil
	}

	if len(indexes) > 1 {
		log.Warn(msgAmbiguousIndexName())
		errResp.ErrorCode = commonpb.ErrorCode_UnexpectedError
		errResp.Reason = msgAmbiguousIndexName()
		return &indexpb.GetIndexBuildProgressResponse{
			Status: errResp,
		}, nil
	}
	indexInfo := &indexpb.IndexInfo{
		CollectionID:     req.CollectionID,
		IndexID:          indexes[0].IndexID,
		IndexedRows:      0,
		TotalRows:        0,
		PendingIndexRows: 0,
		State:            0,
	}
	s.completeIndexInfo(indexInfo, indexes[0], s.meta.SelectSegments(func(info *SegmentInfo) bool {
		return isFlush(info) && info.CollectionID == req.GetCollectionID()
	}), false)
	log.Info("GetIndexBuildProgress success", zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("indexName", req.GetIndexName()))
	return &indexpb.GetIndexBuildProgressResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		IndexedRows:      indexInfo.IndexedRows,
		TotalRows:        indexInfo.TotalRows,
		PendingIndexRows: indexInfo.PendingIndexRows,
	}, nil
}

// DescribeIndex describe the index info of the collection.
func (s *Server) DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error) {
	log := log.Ctx(ctx)
	log.Info("receive DescribeIndex request", zap.Int64("collID", req.GetCollectionID()),
		zap.String("indexName", req.GetIndexName()))
	errResp := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "",
	}
	if s.isClosed() {
		log.Warn(msgDataCoordIsUnhealthy(paramtable.GetNodeID()))
		errResp.ErrorCode = commonpb.ErrorCode_DataCoordNA
		errResp.Reason = msgDataCoordIsUnhealthy(paramtable.GetNodeID())
		return &indexpb.DescribeIndexResponse{
			Status: errResp,
		}, nil
	}

	indexes := s.meta.GetIndexesForCollection(req.GetCollectionID(), req.GetIndexName())
	if len(indexes) == 0 {
		errMsg := fmt.Sprintf("there is no index on collection: %d with the index name: %s", req.CollectionID, req.IndexName)
		log.Error("DescribeIndex fail", zap.Int64("collectionID", req.CollectionID),
			zap.String("indexName", req.IndexName), zap.String("fail reason", errMsg))
		return &indexpb.DescribeIndexResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_IndexNotExist,
				Reason:    fmt.Sprint("index doesn't exist, collectionID ", req.CollectionID),
			},
		}, nil
	}

	// The total rows of all indexes should be based on the current perspective
	segments := s.meta.SelectSegments(func(info *SegmentInfo) bool {
		return isFlush(info) && info.CollectionID == req.GetCollectionID()
	})
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
		s.completeIndexInfo(indexInfo, index, segments, false)
		indexInfos = append(indexInfos, indexInfo)
	}
	log.Info("DescribeIndex success", zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("indexName", req.GetIndexName()))
	return &indexpb.DescribeIndexResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		IndexInfos: indexInfos,
	}, nil
}

// GetIndexStatistics get the statistics of the index. DescribeIndex doesn't contain statistics.
func (s *Server) GetIndexStatistics(ctx context.Context, req *indexpb.GetIndexStatisticsRequest) (*indexpb.GetIndexStatisticsResponse, error) {
	log := log.Ctx(ctx)
	log.Info("receive GetIndexStatistics request",
		zap.Int64("collID", req.GetCollectionID()),
		zap.String("indexName", req.GetIndexName()))
	if s.isClosed() {
		log.Warn(msgDataCoordIsUnhealthy(s.serverID()))
		return &indexpb.GetIndexStatisticsResponse{
			Status: s.UnhealthyStatus(),
		}, nil
	}

	indexes := s.meta.GetIndexesForCollection(req.GetCollectionID(), req.GetIndexName())
	if len(indexes) == 0 {
		errMsg := fmt.Sprintf("there is no index on collection: %d with the index name: %s", req.CollectionID, req.IndexName)
		log.Error("GetIndexStatistics fail",
			zap.Int64("collectionID", req.CollectionID),
			zap.String("indexName", req.IndexName),
			zap.String("fail reason", errMsg))
		return &indexpb.GetIndexStatisticsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_IndexNotExist,
				Reason:    fmt.Sprint("index doesn't exist, collectionID ", req.CollectionID),
			},
		}, nil
	}

	// The total rows of all indexes should be based on the current perspective
	segments := s.meta.SelectSegments(func(info *SegmentInfo) bool {
		return isFlush(info) && info.CollectionID == req.GetCollectionID()
	})
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
		s.completeIndexInfo(indexInfo, index, segments, true)
		indexInfos = append(indexInfos, indexInfo)
	}
	log.Info("GetIndexStatisticsResponse success",
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("indexName", req.GetIndexName()))
	return &indexpb.GetIndexStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		IndexInfos: indexInfos,
	}, nil
}

// DropIndex deletes indexes based on IndexName. One IndexName corresponds to the index of an entire column. A column is
// divided into many segments, and each segment corresponds to an IndexBuildID. DataCoord uses IndexBuildID to record
// index tasks.
func (s *Server) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx)
	log.Info("receive DropIndex request", zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64s("partitionIDs", req.GetPartitionIDs()), zap.String("indexName", req.GetIndexName()),
		zap.Bool("drop all indexes", req.GetDropAll()))
	errResp := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "",
	}
	if s.isClosed() {
		log.Warn(msgDataCoordIsUnhealthy(paramtable.GetNodeID()))
		errResp.ErrorCode = commonpb.ErrorCode_DataCoordNA
		errResp.Reason = msgDataCoordIsUnhealthy(paramtable.GetNodeID())
		return errResp, nil
	}

	ret := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}

	indexes := s.meta.GetIndexesForCollection(req.GetCollectionID(), req.GetIndexName())
	if len(indexes) == 0 {
		log.Info(fmt.Sprintf("there is no index on collection: %d with the index name: %s", req.CollectionID, req.IndexName))
		return ret, nil
	}

	if !req.GetDropAll() && len(indexes) > 1 {
		log.Warn(msgAmbiguousIndexName())
		ret.ErrorCode = commonpb.ErrorCode_UnexpectedError
		ret.Reason = msgAmbiguousIndexName()
		return ret, nil
	}
	indexIDs := make([]UniqueID, 0)
	for _, index := range indexes {
		indexIDs = append(indexIDs, index.IndexID)
	}
	if len(req.GetPartitionIDs()) == 0 {
		// drop collection index
		err := s.meta.MarkIndexAsDeleted(req.CollectionID, indexIDs)
		if err != nil {
			log.Error("DropIndex fail", zap.Int64("collectionID", req.CollectionID),
				zap.String("indexName", req.IndexName), zap.Error(err))
			ret.ErrorCode = commonpb.ErrorCode_UnexpectedError
			ret.Reason = err.Error()
			return ret, nil
		}
	}

	log.Info("DropIndex success", zap.Int64("collID", req.CollectionID),
		zap.Int64s("partitionIDs", req.PartitionIDs), zap.String("indexName", req.IndexName),
		zap.Int64s("indexIDs", indexIDs))
	return ret, nil
}

// GetIndexInfos gets the index file paths for segment from DataCoord.
func (s *Server) GetIndexInfos(ctx context.Context, req *indexpb.GetIndexInfoRequest) (*indexpb.GetIndexInfoResponse, error) {
	log := log.Ctx(ctx)
	log.Info("receive GetIndexInfos request", zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()), zap.String("indexName", req.GetIndexName()))
	errResp := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "",
	}
	if s.isClosed() {
		log.Warn(msgDataCoordIsUnhealthy(paramtable.GetNodeID()))
		errResp.ErrorCode = commonpb.ErrorCode_DataCoordNA
		errResp.Reason = msgDataCoordIsUnhealthy(paramtable.GetNodeID())
		return &indexpb.GetIndexInfoResponse{
			Status: errResp,
		}, nil
	}
	ret := &indexpb.GetIndexInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		SegmentInfo: map[int64]*indexpb.SegmentInfo{},
	}

	for _, segID := range req.SegmentIDs {
		segIdxes := s.meta.GetSegmentIndexes(segID)
		ret.SegmentInfo[segID] = &indexpb.SegmentInfo{
			CollectionID: req.CollectionID,
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
					indexParams := s.meta.GetIndexParams(segIdx.CollectionID, segIdx.IndexID)
					indexParams = append(indexParams, s.meta.GetTypeParams(segIdx.CollectionID, segIdx.IndexID)...)
					ret.SegmentInfo[segID].IndexInfos = append(ret.SegmentInfo[segID].IndexInfos,
						&indexpb.IndexFilePathInfo{
							SegmentID:      segID,
							FieldID:        s.meta.GetFieldIDByIndexID(segIdx.CollectionID, segIdx.IndexID),
							IndexID:        segIdx.IndexID,
							BuildID:        segIdx.BuildID,
							IndexName:      s.meta.GetIndexNameByID(segIdx.CollectionID, segIdx.IndexID),
							IndexParams:    indexParams,
							IndexFilePaths: indexFilePaths,
							SerializedSize: segIdx.IndexSize,
							IndexVersion:   segIdx.IndexVersion,
							NumRows:        segIdx.NumRows,
						})
				}
			}
		}
	}

	log.Info("GetIndexInfos successfully", zap.Int64("collectionID", req.CollectionID),
		zap.String("indexName", req.GetIndexName()))

	return ret, nil
}

func (s *Server) UnhealthyStatus() *commonpb.Status {
	return merr.Status(
		merr.WrapErrServiceNotReady(
			fmt.Sprintf("datacoord %d is unhealthy", s.serverID())))
}
