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

// Package datanode implements data persistence logic.
//
// Data node persists insert logs into persistent storage like minIO/S3.
package datanode

import (
	"context"
	"fmt"
	"path"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// WatchDmChannels is not in use
func (node *DataNode) WatchDmChannels(ctx context.Context, in *datapb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	log.Warn("DataNode WatchDmChannels is not in use")

	// TODO ERROR OF GRPC NOT IN USE
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "watchDmChannels do nothing",
	}, nil
}

// GetComponentStates will return current state of DataNode
func (node *DataNode) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	log.Debug("DataNode current state", zap.Any("State", node.stateCode.Load()))
	nodeID := common.NotRegisteredID
	if node.GetSession() != nil && node.session.Registered() {
		nodeID = node.GetSession().ServerID
	}
	states := &milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			// NodeID:    Params.NodeID, // will race with DataNode.Register()
			NodeID:    nodeID,
			Role:      node.Role,
			StateCode: node.stateCode.Load().(commonpb.StateCode),
		},
		SubcomponentStates: make([]*milvuspb.ComponentInfo, 0),
		Status:             merr.Status(nil),
	}
	return states, nil
}

// FlushSegments packs flush messages into flowGraph through flushChan.
//
//	 DataCoord calls FlushSegments if the segment is seal&flush only.
//		If DataNode receives a valid segment to flush, new flush message for the segment should be ignored.
//		So if receiving calls to flush segment A, DataNode should guarantee the segment to be flushed.
func (node *DataNode) FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest) (*commonpb.Status, error) {
	metrics.DataNodeFlushReqCounter.WithLabelValues(
		fmt.Sprint(paramtable.GetNodeID()),
		metrics.TotalLabel).Inc()

	if !node.isHealthy() {
		err := merr.WrapErrServiceNotReady(node.GetStateCode().String())
		log.Warn("DataNode.FlushSegments failed", zap.Int64("nodeId", paramtable.GetNodeID()), zap.Error(err))

		return merr.Status(err), nil
	}

	serverID := node.GetSession().ServerID
	if req.GetBase().GetTargetID() != serverID {
		log.Warn("flush segment target id not matched",
			zap.Int64("targetID", req.GetBase().GetTargetID()),
			zap.Int64("serverID", serverID),
		)

		return merr.Status(merr.WrapErrNodeNotMatch(req.GetBase().GetTargetID(), serverID)), nil
	}

	log.Info("receiving FlushSegments request",
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64s("sealedSegments", req.GetSegmentIDs()),
	)

	segmentIDs := req.GetSegmentIDs()
	var flushedSeg []UniqueID
	for _, segID := range segmentIDs {
		// if the segment in already being flushed, skip it.
		if node.segmentCache.checkIfCached(segID) {
			logDupFlush(req.GetCollectionID(), segID)
			continue
		}
		// Get the flush channel for the given segment ID.
		// If no flush channel is found, report an error.
		flushCh, err := node.flowgraphManager.getFlushCh(segID)
		if err != nil {
			log.Error("no flush channel found for the segment, unable to flush", zap.Int64("segmentID", segID), zap.Error(err))
			return merr.Status(err), nil
		}

		// Double check that the segment is still not cached.
		// Skip this flush if segment ID is cached, otherwise cache the segment ID and proceed.
		exist := node.segmentCache.checkOrCache(segID)
		if exist {
			logDupFlush(req.GetCollectionID(), segID)
			continue
		}
		// flushedSeg is only for logging purpose.
		flushedSeg = append(flushedSeg, segID)
		// Send the segment to its flush channel.
		flushCh <- flushMsg{
			msgID:        req.GetBase().GetMsgID(),
			timestamp:    req.GetBase().GetTimestamp(),
			segmentID:    segID,
			collectionID: req.GetCollectionID(),
		}
	}

	// Log success flushed segments.
	if len(flushedSeg) > 0 {
		log.Info("sending segments to flush channel",
			zap.Int64("collectionID", req.GetCollectionID()),
			zap.Int64s("sealedSegments", flushedSeg))
	}

	metrics.DataNodeFlushReqCounter.WithLabelValues(
		fmt.Sprint(paramtable.GetNodeID()),
		metrics.SuccessLabel).Inc()
	return merr.Status(nil), nil
}

// ResendSegmentStats resend un-flushed segment stats back upstream to DataCoord by resending DataNode time tick message.
// It returns a list of segments to be sent.
func (node *DataNode) ResendSegmentStats(ctx context.Context, req *datapb.ResendSegmentStatsRequest) (*datapb.ResendSegmentStatsResponse, error) {
	log.Info("start resending segment stats, if any",
		zap.Int64("DataNode ID", paramtable.GetNodeID()))
	segResent := node.flowgraphManager.resendTT()
	log.Info("found segment(s) with stats to resend",
		zap.Int64s("segment IDs", segResent))
	return &datapb.ResendSegmentStatsResponse{
		Status:    merr.Status(nil),
		SegResent: segResent,
	}, nil
}

// GetTimeTickChannel currently do nothing
func (node *DataNode) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Status(nil),
	}, nil
}

// GetStatisticsChannel currently do nothing
func (node *DataNode) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Status(nil),
	}, nil
}

// ShowConfigurations returns the configurations of DataNode matching req.Pattern
func (node *DataNode) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	log.Debug("DataNode.ShowConfigurations", zap.String("pattern", req.Pattern))
	if !node.isHealthy() {
		err := merr.WrapErrServiceNotReady(node.GetStateCode().String())
		log.Warn("DataNode.ShowConfigurations failed", zap.Int64("nodeId", paramtable.GetNodeID()), zap.Error(err))

		return &internalpb.ShowConfigurationsResponse{
			Status:        merr.Status(err),
			Configuations: nil,
		}, nil
	}
	configList := make([]*commonpb.KeyValuePair, 0)
	for key, value := range Params.GetComponentConfigurations("datanode", req.Pattern) {
		configList = append(configList,
			&commonpb.KeyValuePair{
				Key:   key,
				Value: value,
			})
	}

	return &internalpb.ShowConfigurationsResponse{
		Status:        merr.Status(nil),
		Configuations: configList,
	}, nil
}

// GetMetrics return datanode metrics
func (node *DataNode) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if !node.isHealthy() {
		err := merr.WrapErrServiceNotReady(node.GetStateCode().String())
		log.Warn("DataNode.GetMetrics failed", zap.Int64("nodeId", paramtable.GetNodeID()), zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: merr.Status(err),
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Warn("DataNode.GetMetrics failed to parse metric type",
			zap.Int64("nodeID", paramtable.GetNodeID()),
			zap.String("req", req.Request),
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: merr.Status(err),
		}, nil
	}

	if metricType == metricsinfo.SystemInfoMetrics {
		systemInfoMetrics, err := node.getSystemInfoMetrics(ctx, req)
		if err != nil {
			log.Warn("DataNode GetMetrics failed", zap.Int64("nodeID", paramtable.GetNodeID()), zap.Error(err))
			return &milvuspb.GetMetricsResponse{
				Status: merr.Status(err),
			}, nil
		}

		return systemInfoMetrics, nil
	}

	log.RatedWarn(60, "DataNode.GetMetrics failed, request metric type is not implemented yet",
		zap.Int64("nodeID", paramtable.GetNodeID()),
		zap.String("req", req.Request),
		zap.String("metric_type", metricType))

	return &milvuspb.GetMetricsResponse{
		Status: merr.Status(merr.WrapErrMetricNotFound(metricType)),
	}, nil
}

// Compaction handles compaction request from DataCoord
// returns status as long as compaction task enqueued or invalid
func (node *DataNode) Compaction(ctx context.Context, req *datapb.CompactionPlan) (*commonpb.Status, error) {
	if !node.isHealthy() {
		err := merr.WrapErrServiceNotReady(node.GetStateCode().String())
		log.Warn("DataNode.Compaction failed", zap.Int64("nodeId", paramtable.GetNodeID()), zap.Error(err))
		return merr.Status(err), nil
	}

	ds, ok := node.flowgraphManager.getFlowgraphService(req.GetChannel())
	if !ok {
		log.Warn("illegel compaction plan, channel not in this DataNode", zap.String("channelName", req.GetChannel()))
		return merr.Status(merr.WrapErrChannelNotFound(req.GetChannel(), "illegel compaction plan")), nil
	}

	if !node.compactionExecutor.channelValidateForCompaction(req.GetChannel()) {
		log.Warn("channel of compaction is marked invalid in compaction executor", zap.String("channelName", req.GetChannel()))
		return merr.Status(merr.WrapErrChannelNotFound(req.GetChannel(), "channel is dropping")), nil
	}

	binlogIO := &binlogIO{node.chunkManager, ds.idAllocator}
	task := newCompactionTask(
		node.ctx,
		binlogIO, binlogIO,
		ds.channel,
		ds.flushManager,
		ds.idAllocator,
		req,
		node.chunkManager,
	)

	node.compactionExecutor.execute(task)

	return merr.Status(nil), nil
}

// GetCompactionState called by DataCoord
// return status of all compaction plans
func (node *DataNode) GetCompactionState(ctx context.Context, req *datapb.CompactionStateRequest) (*datapb.CompactionStateResponse, error) {
	if !node.isHealthy() {
		err := merr.WrapErrServiceNotReady(node.GetStateCode().String())
		log.Warn("DataNode.GetCompactionState failed", zap.Int64("nodeId", paramtable.GetNodeID()), zap.Error(err))
		return &datapb.CompactionStateResponse{
			Status: merr.Status(err),
		}, nil
	}
	results := make([]*datapb.CompactionStateResult, 0)
	node.compactionExecutor.executing.Range(func(k, v any) bool {
		results = append(results, &datapb.CompactionStateResult{
			State:  commonpb.CompactionState_Executing,
			PlanID: k.(UniqueID),
		})
		return true
	})
	node.compactionExecutor.completed.Range(func(k, v any) bool {
		results = append(results, &datapb.CompactionStateResult{
			State:  commonpb.CompactionState_Completed,
			PlanID: k.(UniqueID),
			Result: v.(*datapb.CompactionResult),
		})
		return true
	})

	if len(results) > 0 {
		log.Info("Compaction results", zap.Any("results", results))
	}
	return &datapb.CompactionStateResponse{
		Status:  merr.Status(nil),
		Results: results,
	}, nil
}

// SyncSegments called by DataCoord, sync the compacted segments' meta between DC and DN
func (node *DataNode) SyncSegments(ctx context.Context, req *datapb.SyncSegmentsRequest) (*commonpb.Status, error) {
	log.Ctx(ctx).Info("DataNode receives SyncSegments",
		zap.Int64("planID", req.GetPlanID()),
		zap.Int64("target segmentID", req.GetCompactedTo()),
		zap.Int64s("compacted from", req.GetCompactedFrom()),
		zap.Int64("numOfRows", req.GetNumOfRows()),
	)

	if !node.isHealthy() {
		err := merr.WrapErrServiceNotReady(node.GetStateCode().String())
		log.Warn("DataNode.SyncSegments failed", zap.Int64("nodeId", paramtable.GetNodeID()), zap.Error(err))
		return merr.Status(err), nil
	}

	if len(req.GetCompactedFrom()) <= 0 {
		return merr.Status(merr.WrapErrParameterInvalid(">0", "0", "compacted from segments shouldn't be empty")), nil
	}

	var (
		oneSegment int64
		channel    Channel
		err        error
		ds         *dataSyncService
		ok         bool
	)

	for _, fromSegment := range req.GetCompactedFrom() {
		channel, err = node.flowgraphManager.getChannel(fromSegment)
		if err != nil {
			log.Ctx(ctx).Warn("fail to get the channel", zap.Int64("segment", fromSegment), zap.Error(err))
			continue
		}
		ds, ok = node.flowgraphManager.getFlowgraphService(channel.getChannelName(fromSegment))
		if !ok {
			log.Ctx(ctx).Warn("fail to find flow graph service", zap.Int64("segment", fromSegment))
			continue
		}
		oneSegment = fromSegment
		break
	}
	if oneSegment == 0 {
		log.Ctx(ctx).Warn("no valid segment, maybe the request is a retry")
		return merr.Status(nil), nil
	}

	// oneSegment is definitely in the channel, guaranteed by the check before.
	collID, partID, _ := channel.getCollectionAndPartitionID(oneSegment)
	targetSeg := &Segment{
		collectionID: collID,
		partitionID:  partID,
		segmentID:    req.GetCompactedTo(),
		numRows:      req.GetNumOfRows(),
		lastSyncTs:   tsoutil.GetCurrentTime(),
	}

	err = channel.InitPKstats(ctx, targetSeg, req.GetStatsLogs(), tsoutil.GetCurrentTime())
	if err != nil {
		return merr.Status(err), nil
	}

	// block all flow graph so it's safe to remove segment
	ds.fg.Blockall()
	defer ds.fg.Unblock()
	if err := channel.mergeFlushedSegments(ctx, targetSeg, req.GetPlanID(), req.GetCompactedFrom()); err != nil {
		node.compactionExecutor.injectDone(req.GetPlanID(), false)
		return merr.Status(err), nil
	}
	node.compactionExecutor.injectDone(req.GetPlanID(), true)
	return merr.Status(nil), nil
}

// Import data files(json, numpy, etc.) on MinIO/S3 storage, read and parse them into sealed segments
func (node *DataNode) Import(ctx context.Context, req *datapb.ImportTaskRequest) (*commonpb.Status, error) {
	logFields := []zap.Field{
		zap.Int64("task ID", req.GetImportTask().GetTaskId()),
		zap.Int64("collectionID", req.GetImportTask().GetCollectionId()),
		zap.Int64("partitionID", req.GetImportTask().GetPartitionId()),
		zap.String("database name", req.GetImportTask().GetDatabaseName()),
		zap.Strings("channel names", req.GetImportTask().GetChannelNames()),
		zap.Int64s("working dataNodes", req.WorkingNodes),
		zap.Int64("node ID", paramtable.GetNodeID()),
	}
	log.Info("DataNode receive import request", logFields...)
	defer func() {
		log.Info("DataNode finish import request", logFields...)
	}()

	importResult := &rootcoordpb.ImportResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		TaskId:     req.GetImportTask().TaskId,
		DatanodeId: paramtable.GetNodeID(),
		State:      commonpb.ImportState_ImportStarted,
		Segments:   make([]int64, 0),
		AutoIds:    make([]int64, 0),
		RowCount:   0,
	}
	importResult.Infos = append(importResult.Infos, &commonpb.KeyValuePair{Key: importutil.ProgressPercent, Value: "0"})

	// Spawn a new context to ignore cancellation from parental context.
	newCtx, cancel := context.WithTimeout(context.TODO(), ImportCallTimeout)
	defer cancel()

	// function to report import state to RootCoord.
	// retry 10 times, if the rootcoord is down, the report function will cost 20+ seconds
	reportFunc := reportImportFunc(node)
	returnFailFunc := func(msg string, inputErr error) (*commonpb.Status, error) {
		logFields = append(logFields, zap.Error(inputErr))
		log.Warn(msg, logFields...)
		importResult.State = commonpb.ImportState_ImportFailed
		importResult.Infos = append(importResult.Infos, &commonpb.KeyValuePair{Key: importutil.FailedReason, Value: inputErr.Error()})

		reportFunc(importResult)

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    inputErr.Error(),
		}, nil
	}

	if !node.isHealthy() {
		err := merr.WrapErrServiceNotReady(node.GetStateCode().String())
		logFields = append(logFields, zap.Error(err))
		log.Warn("DataNode import failed, node is not healthy", logFields...)
		return merr.Status(err), nil
	}

	// get a timestamp for all the rows
	// Ignore cancellation from parent context.
	rep, err := node.rootCoord.AllocTimestamp(newCtx, &rootcoordpb.AllocTimestampRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_RequestTSO),
			commonpbutil.WithMsgID(0),
			commonpbutil.WithTimeStamp(0),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		Count: 1,
	})

	if rep.Status.ErrorCode != commonpb.ErrorCode_Success || err != nil {
		return returnFailFunc("DataNode alloc ts failed", err)
	}

	ts := rep.GetTimestamp()

	// get collection schema and shard number
	metaService := newMetaService(node.rootCoord, req.GetImportTask().GetCollectionId())
	colInfo, err := metaService.getCollectionInfo(newCtx, req.GetImportTask().GetCollectionId(), 0)
	if err != nil {
		return returnFailFunc("failed to get collection info for collection ID", err)
	}

	// the colInfo doesn't have a collect database name(it is empty). use the database name passed from rootcoord.
	partitions, err := node.getPartitions(ctx, req.GetImportTask().GetDatabaseName(), colInfo.GetCollectionName())
	if err != nil {
		return returnFailFunc("failed to get partition id list", err)
	}

	partitionIDs, err := importutil.DeduceTargetPartitions(partitions, colInfo.GetSchema(), req.GetImportTask().GetPartitionId())
	if err != nil {
		return returnFailFunc("failed to decude target partitions", err)
	}

	collectionInfo, err := importutil.NewCollectionInfo(colInfo.GetSchema(), colInfo.GetShardsNum(), partitionIDs)
	if err != nil {
		return returnFailFunc("invalid collection info to import", err)
	}

	// parse files and generate segments
	segmentSize := Params.DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024
	importWrapper := importutil.NewImportWrapper(newCtx, collectionInfo, segmentSize, node.allocator.GetIDAlloactor(),
		node.chunkManager, importResult, reportFunc)
	importWrapper.SetCallbackFunctions(assignSegmentFunc(node, req),
		createBinLogsFunc(node, req, colInfo.GetSchema(), ts),
		saveSegmentFunc(node, req, importResult, ts))
	// todo: pass tsStart and tsStart after import_wrapper support
	tsStart, tsEnd, err := importutil.ParseTSFromOptions(req.GetImportTask().GetInfos())
	isBackup := importutil.IsBackup(req.GetImportTask().GetInfos())
	if err != nil {
		return returnFailFunc("failed to parse timestamp from import options", err)
	}
	logFields = append(logFields, zap.Uint64("start_ts", tsStart), zap.Uint64("end_ts", tsEnd))
	log.Info("import time range", logFields...)
	err = importWrapper.Import(req.GetImportTask().GetFiles(),
		importutil.ImportOptions{OnlyValidate: false, TsStartPoint: tsStart, TsEndPoint: tsEnd, IsBackup: isBackup})
	if err != nil {
		return returnFailFunc("failed to import files", err)
	}

	resp := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	return resp, nil
}

func (node *DataNode) getPartitions(ctx context.Context, dbName string, collectionName string) (map[string]int64, error) {
	req := &milvuspb.ShowPartitionsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ShowPartitions),
		),
		DbName:         dbName,
		CollectionName: collectionName,
	}

	logFields := []zap.Field{
		zap.String("dbName", dbName),
		zap.String("collectionName", collectionName),
	}
	resp, err := node.rootCoord.ShowPartitions(ctx, req)
	if err != nil {
		logFields = append(logFields, zap.Error(err))
		log.Warn("failed to get partitions of collection", logFields...)
		return nil, err
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		log.Warn("failed to get partitions of collection", logFields...)
		return nil, errors.New(resp.Status.Reason)
	}

	partitionNames := resp.GetPartitionNames()
	partitionIDs := resp.GetPartitionIDs()
	if len(partitionNames) != len(partitionIDs) {
		logFields = append(logFields, zap.Int("number of names", len(partitionNames)), zap.Int("number of ids", len(partitionIDs)))
		log.Warn("partition names and ids are unequal", logFields...)
		return nil, fmt.Errorf("partition names and ids are unequal, number of names: %d, number of ids: %d",
			len(partitionNames), len(partitionIDs))
	}

	partitions := make(map[string]int64)
	for i := 0; i < len(partitionNames); i++ {
		partitions[partitionNames[i]] = partitionIDs[i]
	}

	return partitions, nil
}

// AddImportSegment adds the import segment to the current DataNode.
func (node *DataNode) AddImportSegment(ctx context.Context, req *datapb.AddImportSegmentRequest) (*datapb.AddImportSegmentResponse, error) {
	logFields := []zap.Field{
		zap.Int64("segmentID", req.GetSegmentId()),
		zap.Int64("collectionID", req.GetCollectionId()),
		zap.Int64("partitionID", req.GetPartitionId()),
		zap.String("channelName", req.GetChannelName()),
		zap.Int64("# of rows", req.GetRowNum()),
	}
	log.Info("adding segment to DataNode flow graph", logFields...)
	// Fetch the flow graph on the given v-channel.
	var ds *dataSyncService
	// Retry in case the channel hasn't been watched yet.
	err := retry.Do(ctx, func() error {
		var ok bool
		ds, ok = node.flowgraphManager.getFlowgraphService(req.GetChannelName())
		if !ok {
			return errors.New("channel not found")
		}
		return nil
	}, retry.Attempts(getFlowGraphServiceAttempts))
	if err != nil {
		logFields = append(logFields, zap.Int64("node ID", paramtable.GetNodeID()))
		log.Error("channel not found in current DataNode", logFields...)
		return &datapb.AddImportSegmentResponse{
			Status: &commonpb.Status{
				// TODO: Add specific error code.
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "channel not found in current DataNode",
			},
		}, nil
	}
	// Get the current dml channel position ID, that will be used in segments start positions and end positions.
	var posID []byte
	err = retry.Do(ctx, func() error {
		id, innerError := ds.getChannelLatestMsgID(context.Background(), req.GetChannelName(), req.GetSegmentId())
		posID = id
		return innerError
	}, retry.Attempts(30))

	if err != nil {
		return &datapb.AddImportSegmentResponse{
			Status: &commonpb.Status{
				// TODO: Add specific error code.
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "failed to get channel position",
			},
		}, nil
	}
	// Add the new segment to the channel.
	if !ds.channel.hasSegment(req.GetSegmentId(), true) {
		log.Info("adding a new segment to channel", logFields...)
		// Add segment as a flushed segment, but set `importing` to true to add extra information of the segment.
		// By 'extra information' we mean segment info while adding a `SegmentType_Flushed` typed segment.
		if err := ds.channel.addSegment(
			addSegmentReq{
				segType:      datapb.SegmentType_Flushed,
				segID:        req.GetSegmentId(),
				collID:       req.GetCollectionId(),
				partitionID:  req.GetPartitionId(),
				numOfRows:    req.GetRowNum(),
				statsBinLogs: req.GetStatsLog(),
				startPos: &msgpb.MsgPosition{
					ChannelName: req.GetChannelName(),
					MsgID:       posID,
					Timestamp:   req.GetBase().GetTimestamp(),
				},
				endPos: &msgpb.MsgPosition{
					ChannelName: req.GetChannelName(),
					MsgID:       posID,
					Timestamp:   req.GetBase().GetTimestamp(),
				},
				recoverTs: req.GetBase().GetTimestamp(),
				importing: true,
			}); err != nil {
			logFields = append(logFields, zap.Error(err))
			log.Error("failed to add segment to flow graph", logFields...)
			return &datapb.AddImportSegmentResponse{
				Status: &commonpb.Status{
					// TODO: Add specific error code.
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}
	}
	ds.flushingSegCache.Remove(req.GetSegmentId())
	return &datapb.AddImportSegmentResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		ChannelPos: posID,
	}, nil
}

func assignSegmentFunc(node *DataNode, req *datapb.ImportTaskRequest) importutil.AssignSegmentFunc {
	return func(shardID int, partID int64) (int64, string, error) {
		chNames := req.GetImportTask().GetChannelNames()
		importTaskID := req.GetImportTask().GetTaskId()
		logFields := []zap.Field{
			zap.Int64("task ID", importTaskID),
			zap.Int("shard ID", shardID),
			zap.Int64("partitionID", partID),
			zap.Int("# of channels", len(chNames)),
			zap.Strings("channel names", chNames),
		}
		if shardID >= len(chNames) {
			log.Error("import task returns invalid shard ID", logFields...)
			return 0, "", fmt.Errorf("syncSegmentID Failed: invalid shard ID %d", shardID)
		}

		tr := timerecord.NewTimeRecorder("assign segment function")
		defer tr.Elapse("finished")

		colID := req.GetImportTask().GetCollectionId()
		segmentIDReq := composeAssignSegmentIDRequest(1, shardID, chNames, colID, partID)
		targetChName := segmentIDReq.GetSegmentIDRequests()[0].GetChannelName()
		logFields = append(logFields, zap.String("target channel name", targetChName))
		log.Info("assign segment for the import task", logFields...)
		resp, err := node.dataCoord.AssignSegmentID(context.Background(), segmentIDReq)
		if err != nil {
			return 0, "", fmt.Errorf("syncSegmentID Failed:%w", err)
		}
		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return 0, "", fmt.Errorf("syncSegmentID Failed:%s", resp.Status.Reason)
		}
		if len(resp.SegIDAssignments) == 0 || resp.SegIDAssignments[0] == nil {
			return 0, "", fmt.Errorf("syncSegmentID Failed: the collection was dropped")
		}

		segmentID := resp.SegIDAssignments[0].SegID
		logFields = append(logFields, zap.Int64("segmentID", segmentID))
		log.Info("new segment assigned", logFields...)

		// call report to notify the rootcoord update the segment id list for this task
		// ignore the returned error, since even report failed the segments still can be cleaned
		// retry 10 times, if the rootcoord is down, the report function will cost 20+ seconds
		importResult := &rootcoordpb.ImportResult{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			TaskId:     req.GetImportTask().TaskId,
			DatanodeId: paramtable.GetNodeID(),
			State:      commonpb.ImportState_ImportStarted,
			Segments:   []int64{segmentID},
			AutoIds:    make([]int64, 0),
			RowCount:   0,
		}
		reportFunc := reportImportFunc(node)
		reportFunc(importResult)

		return segmentID, targetChName, nil
	}
}

func createBinLogsFunc(node *DataNode, req *datapb.ImportTaskRequest, schema *schemapb.CollectionSchema, ts Timestamp) importutil.CreateBinlogsFunc {
	return func(fields importutil.BlockData, segmentID int64, partID int64) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
		var rowNum int
		for _, field := range fields {
			rowNum = field.RowNum()
			break
		}

		chNames := req.GetImportTask().GetChannelNames()
		importTaskID := req.GetImportTask().GetTaskId()
		logFields := []zap.Field{
			zap.Int64("task ID", importTaskID),
			zap.Int64("partitionID", partID),
			zap.Int64("segmentID", segmentID),
			zap.Int("# of channels", len(chNames)),
			zap.Strings("channel names", chNames),
		}

		if rowNum <= 0 {
			log.Info("fields data is empty, no need to generate binlog", logFields...)
			return nil, nil, nil
		}

		colID := req.GetImportTask().GetCollectionId()
		fieldInsert, fieldStats, err := createBinLogs(rowNum, schema, ts, fields, node, segmentID, colID, partID)
		if err != nil {
			logFields = append(logFields, zap.Any("err", err))
			log.Error("failed to create binlogs", logFields...)
			return nil, nil, err
		}

		logFields = append(logFields, zap.Int("insert log count", len(fieldInsert)), zap.Int("stats log count", len(fieldStats)))
		log.Info("new binlog created", logFields...)

		return fieldInsert, fieldStats, err
	}
}

func saveSegmentFunc(node *DataNode, req *datapb.ImportTaskRequest, res *rootcoordpb.ImportResult, ts Timestamp) importutil.SaveSegmentFunc {
	importTaskID := req.GetImportTask().GetTaskId()
	return func(fieldsInsert []*datapb.FieldBinlog, fieldsStats []*datapb.FieldBinlog, segmentID int64,
		targetChName string, rowCount int64, partID int64) error {
		logFields := []zap.Field{
			zap.Int64("task ID", importTaskID),
			zap.Int64("partitionID", partID),
			zap.Int64("segmentID", segmentID),
			zap.String("target channel name", targetChName),
			zap.Int64("row count", rowCount),
			zap.Uint64("ts", ts),
		}
		log.Info("adding segment to the correct DataNode flow graph and saving binlog paths", logFields...)

		err := retry.Do(context.Background(), func() error {
			// Ask DataCoord to save binlog path and add segment to the corresponding DataNode flow graph.
			resp, err := node.dataCoord.SaveImportSegment(context.Background(), &datapb.SaveImportSegmentRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithTimeStamp(ts), // Pass current timestamp downstream.
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
				),
				SegmentId:    segmentID,
				ChannelName:  targetChName,
				CollectionId: req.GetImportTask().GetCollectionId(),
				PartitionId:  partID,
				RowNum:       rowCount,
				SaveBinlogPathReq: &datapb.SaveBinlogPathsRequest{
					Base: commonpbutil.NewMsgBase(
						commonpbutil.WithMsgType(0),
						commonpbutil.WithMsgID(0),
						commonpbutil.WithTimeStamp(ts),
						commonpbutil.WithSourceID(paramtable.GetNodeID()),
					),
					SegmentID:           segmentID,
					CollectionID:        req.GetImportTask().GetCollectionId(),
					Field2BinlogPaths:   fieldsInsert,
					Field2StatslogPaths: fieldsStats,
					// Set start positions of a SaveBinlogPathRequest explicitly.
					StartPositions: []*datapb.SegmentStartPosition{
						{
							StartPosition: &msgpb.MsgPosition{
								ChannelName: targetChName,
								Timestamp:   ts,
							},
							SegmentID: segmentID,
						},
					},
					Importing: true,
				},
			})
			// Only retrying when DataCoord is unhealthy or err != nil, otherwise return immediately.
			if err != nil {
				return fmt.Errorf(err.Error())
			}
			if resp.ErrorCode != commonpb.ErrorCode_Success && resp.ErrorCode != commonpb.ErrorCode_NotReadyServe {
				return retry.Unrecoverable(fmt.Errorf("failed to save import segment, reason = %s", resp.Reason))
			} else if resp.ErrorCode == commonpb.ErrorCode_NotReadyServe {
				return fmt.Errorf("failed to save import segment: %s", resp.GetReason())
			}
			return nil
		})
		if err != nil {
			log.Warn("failed to save import segment", zap.Error(err))
			return err
		}
		log.Info("segment imported and persisted", logFields...)
		res.Segments = append(res.Segments, segmentID)
		res.RowCount += rowCount
		return nil
	}
}

func composeAssignSegmentIDRequest(rowNum int, shardID int, chNames []string,
	collID int64, partID int64) *datapb.AssignSegmentIDRequest {
	// use the first field's row count as segment row count
	// all the fields row count are same, checked by ImportWrapper
	// ask DataCoord to alloc a new segment
	log.Info("import task flush segment",
		zap.Int("rowCount", rowNum),
		zap.Any("channel names", chNames),
		zap.Int64("collectionID", collID),
		zap.Int64("partitionID", partID),
		zap.Int("shardID", shardID))
	segReqs := []*datapb.SegmentIDRequest{
		{
			ChannelName:  chNames[shardID],
			Count:        uint32(rowNum),
			CollectionID: collID,
			PartitionID:  partID,
			IsImport:     true,
		},
	}
	segmentIDReq := &datapb.AssignSegmentIDRequest{
		NodeID:            0,
		PeerRole:          typeutil.ProxyRole,
		SegmentIDRequests: segReqs,
	}
	return segmentIDReq
}

func createBinLogs(rowNum int, schema *schemapb.CollectionSchema, ts Timestamp,
	fields map[storage.FieldID]storage.FieldData, node *DataNode, segmentID, colID, partID UniqueID) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tsFieldData := make([]int64, rowNum)
	for i := range tsFieldData {
		tsFieldData[i] = int64(ts)
	}
	fields[common.TimeStampField] = &storage.Int64FieldData{
		Data: tsFieldData,
	}

	if status, _ := node.dataCoord.UpdateSegmentStatistics(context.TODO(), &datapb.UpdateSegmentStatisticsRequest{
		Stats: []*commonpb.SegmentStats{{
			SegmentID: segmentID,
			NumRows:   int64(rowNum),
		}},
	}); status.GetErrorCode() != commonpb.ErrorCode_Success {
		return nil, nil, fmt.Errorf(status.GetReason())
	}

	data := BufferData{buffer: &InsertData{
		Data: fields,
	}}
	data.updateSize(int64(rowNum))
	meta := &etcdpb.CollectionMeta{
		ID:     colID,
		Schema: schema,
	}
	iCodec := storage.NewInsertCodecWithSchema(meta)

	binLogs, err := iCodec.Serialize(partID, segmentID, data.buffer)
	if err != nil {
		return nil, nil, err
	}

	start, _, err := node.allocator.Alloc(uint32(len(binLogs)))
	if err != nil {
		return nil, nil, err
	}

	field2Insert := make(map[UniqueID]*datapb.Binlog, len(binLogs))
	kvs := make(map[string][]byte, len(binLogs))
	field2Logidx := make(map[UniqueID]UniqueID, len(binLogs))
	for idx, blob := range binLogs {
		fieldID, err := strconv.ParseInt(blob.GetKey(), 10, 64)
		if err != nil {
			log.Error("Flush failed ... cannot parse string to fieldID ..", zap.Error(err))
			return nil, nil, err
		}

		logidx := start + int64(idx)

		k := metautil.JoinIDPath(colID, partID, segmentID, fieldID, logidx)

		key := path.Join(node.chunkManager.RootPath(), common.SegmentInsertLogPath, k)
		kvs[key] = blob.Value[:]
		field2Insert[fieldID] = &datapb.Binlog{
			EntriesNum:    data.size,
			TimestampFrom: ts,
			TimestampTo:   ts,
			LogPath:       key,
			LogSize:       int64(len(blob.Value)),
		}
		field2Logidx[fieldID] = logidx
	}

	field2Stats := make(map[UniqueID]*datapb.Binlog)
	// write stats binlog
	statsBinLog, err := iCodec.SerializePkStatsByData(data.buffer)
	if err != nil {
		return nil, nil, err
	}

	fieldID, err := strconv.ParseInt(statsBinLog.GetKey(), 10, 64)
	if err != nil {
		log.Error("Flush failed ... cannot parse string to fieldID ..", zap.Error(err))
		return nil, nil, err
	}

	logidx := field2Logidx[fieldID]

	// no error raise if alloc=false
	k := metautil.JoinIDPath(colID, partID, segmentID, fieldID, logidx)

	key := path.Join(node.chunkManager.RootPath(), common.SegmentStatslogPath, k)
	kvs[key] = statsBinLog.Value
	field2Stats[fieldID] = &datapb.Binlog{
		EntriesNum:    data.size,
		TimestampFrom: ts,
		TimestampTo:   ts,
		LogPath:       key,
		LogSize:       int64(len(statsBinLog.Value)),
	}

	err = node.chunkManager.MultiWrite(ctx, kvs)
	if err != nil {
		return nil, nil, err
	}
	var (
		fieldInsert []*datapb.FieldBinlog
		fieldStats  []*datapb.FieldBinlog
	)
	for k, v := range field2Insert {
		fieldInsert = append(fieldInsert, &datapb.FieldBinlog{FieldID: k, Binlogs: []*datapb.Binlog{v}})
	}
	for k, v := range field2Stats {
		fieldStats = append(fieldStats, &datapb.FieldBinlog{FieldID: k, Binlogs: []*datapb.Binlog{v}})
	}
	return fieldInsert, fieldStats, nil
}

func reportImportFunc(node *DataNode) importutil.ReportFunc {
	return func(importResult *rootcoordpb.ImportResult) error {
		err := retry.Do(context.Background(), func() error {
			status, err := node.rootCoord.ReportImport(context.Background(), importResult)
			if err != nil {
				log.Error("fail to report import state to RootCoord", zap.Error(err))
				return err
			}
			if status != nil && status.ErrorCode != commonpb.ErrorCode_Success {
				return errors.New(status.GetReason())
			}
			return nil
		}, retry.Attempts(node.reportImportRetryTimes))

		return err
	}
}

func logDupFlush(cID, segID int64) {
	log.Info("segment is already being flushed, ignoring flush request",
		zap.Int64("collectionID", cID),
		zap.Int64("segmentID", segID))
}
