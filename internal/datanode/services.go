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
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/io"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// WatchDmChannels is not in use
func (node *DataNode) WatchDmChannels(ctx context.Context, in *datapb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	log.Warn("DataNode WatchDmChannels is not in use")

	// TODO ERROR OF GRPC NOT IN USE
	return merr.Success(), nil
}

// GetComponentStates will return current state of DataNode
func (node *DataNode) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	nodeID := common.NotRegisteredID
	state := node.stateCode.Load().(commonpb.StateCode)
	log.Debug("DataNode current state", zap.String("State", state.String()))
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
		Status:             merr.Success(),
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

	log := log.Ctx(ctx)

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
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

	segmentIDs := req.GetSegmentIDs()
	log = log.With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("channelName", req.GetChannelName()),
		zap.Int64s("segmentIDs", segmentIDs),
	)

	log.Info("receiving FlushSegments request")

	err := node.writeBufferManager.FlushSegments(ctx, req.GetChannelName(), segmentIDs)
	if err != nil {
		log.Warn("failed to flush segments", zap.Error(err))
		return merr.Status(err), nil
	}

	// Log success flushed segments.
	log.Info("sending segments to WriteBuffer Manager")

	metrics.DataNodeFlushReqCounter.WithLabelValues(
		fmt.Sprint(paramtable.GetNodeID()),
		metrics.SuccessLabel).Inc()
	return merr.Success(), nil
}

// ResendSegmentStats . ResendSegmentStats resend un-flushed segment stats back upstream to DataCoord by resending DataNode time tick message.
// It returns a list of segments to be sent.
// Deprecated in 2.3.2, reversed it just for compatibility during rolling back
func (node *DataNode) ResendSegmentStats(ctx context.Context, req *datapb.ResendSegmentStatsRequest) (*datapb.ResendSegmentStatsResponse, error) {
	return &datapb.ResendSegmentStatsResponse{
		Status:    merr.Success(),
		SegResent: make([]int64, 0),
	}, nil
}

// GetTimeTickChannel currently do nothing
func (node *DataNode) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Success(),
	}, nil
}

// GetStatisticsChannel currently do nothing
func (node *DataNode) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Success(),
	}, nil
}

// ShowConfigurations returns the configurations of DataNode matching req.Pattern
func (node *DataNode) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	log.Debug("DataNode.ShowConfigurations", zap.String("pattern", req.Pattern))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
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
		Status:        merr.Success(),
		Configuations: configList,
	}, nil
}

// GetMetrics return datanode metrics
func (node *DataNode) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
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
	log := log.Ctx(ctx).With(zap.Int64("planID", req.GetPlanID()))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		log.Warn("DataNode.Compaction failed", zap.Int64("nodeId", paramtable.GetNodeID()), zap.Error(err))
		return merr.Status(err), nil
	}

	ds, ok := node.flowgraphManager.GetFlowgraphService(req.GetChannel())
	if !ok {
		log.Warn("illegel compaction plan, channel not in this DataNode", zap.String("channelName", req.GetChannel()))
		return merr.Status(merr.WrapErrChannelNotFound(req.GetChannel(), "illegel compaction plan")), nil
	}

	if !node.compactionExecutor.isValidChannel(req.GetChannel()) {
		log.Warn("channel of compaction is marked invalid in compaction executor", zap.String("channelName", req.GetChannel()))
		return merr.Status(merr.WrapErrChannelNotFound(req.GetChannel(), "channel is dropping")), nil
	}

	meta := ds.metacache
	for _, segment := range req.GetSegmentBinlogs() {
		if segment.GetLevel() == datapb.SegmentLevel_L0 {
			continue
		}
		_, ok := meta.GetSegmentByID(segment.GetSegmentID(), metacache.WithSegmentState(commonpb.SegmentState_Flushed))
		if !ok {
			log.Warn("compaction plan contains segment which is not flushed",
				zap.Int64("segmentID", segment.GetSegmentID()),
			)
			return merr.Status(merr.WrapErrSegmentNotFound(segment.GetSegmentID(), "segment with flushed state not found")), nil
		}
	}

	var task compactor
	switch req.GetType() {
	case datapb.CompactionType_Level0DeleteCompaction:
		binlogIO := io.NewBinlogIO(node.chunkManager, getOrCreateIOPool())
		task = newLevelZeroCompactionTask(
			node.ctx,
			binlogIO,
			node.allocator,
			ds.metacache,
			node.syncMgr,
			req,
		)
	case datapb.CompactionType_MixCompaction, datapb.CompactionType_MinorCompaction:
		// TODO, replace this binlogIO with io.BinlogIO
		binlogIO := &binlogIO{node.chunkManager, ds.idAllocator}
		task = newCompactionTask(
			node.ctx,
			binlogIO, binlogIO,
			ds.metacache,
			node.syncMgr,
			node.allocator,
			req,
		)
	default:
		log.Warn("Unknown compaction type", zap.String("type", req.GetType().String()))
		return merr.Status(merr.WrapErrParameterInvalidMsg("Unknown compaction type: %v", req.GetType().String())), nil
	}

	node.compactionExecutor.execute(task)
	return merr.Success(), nil
}

// GetCompactionState called by DataCoord
// return status of all compaction plans
func (node *DataNode) GetCompactionState(ctx context.Context, req *datapb.CompactionStateRequest) (*datapb.CompactionStateResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		log.Warn("DataNode.GetCompactionState failed", zap.Int64("nodeId", paramtable.GetNodeID()), zap.Error(err))
		return &datapb.CompactionStateResponse{
			Status: merr.Status(err),
		}, nil
	}
	results := node.compactionExecutor.getAllCompactionResults()

	if len(results) > 0 {
		planIDs := lo.Map(results, func(result *datapb.CompactionPlanResult, i int) UniqueID {
			return result.GetPlanID()
		})
		log.Info("Compaction results", zap.Int64s("planIDs", planIDs))
	}
	return &datapb.CompactionStateResponse{
		Status:  merr.Success(),
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
		zap.String("channelName", req.GetChannelName()),
	)

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		log.Warn("DataNode.SyncSegments failed", zap.Int64("nodeId", paramtable.GetNodeID()), zap.Error(err))
		return merr.Status(err), nil
	}

	if len(req.GetCompactedFrom()) <= 0 {
		return merr.Status(merr.WrapErrParameterInvalid(">0", "0", "compacted from segments shouldn't be empty")), nil
	}

	ds, ok := node.flowgraphManager.GetFlowgraphService(req.GetChannelName())
	if !ok {
		node.compactionExecutor.clearTasksByChannel(req.GetChannelName())
		err := merr.WrapErrChannelNotFound(req.GetChannelName())
		log.Warn("failed to sync segments", zap.Error(err))
		return merr.Status(err), nil
	}

	pks, err := loadStats(ctx, node.chunkManager, ds.metacache.Schema(), req.GetCompactedTo(), req.GetCollectionId(), req.GetStatsLogs(), 0)
	if err != nil {
		log.Warn("failed to load segment statslog", zap.Error(err))
		return merr.Status(err), nil
	}
	bfs := metacache.NewBloomFilterSet(pks...)
	ds.metacache.CompactSegments(req.GetCompactedTo(), req.GetPartitionId(), req.GetNumOfRows(), bfs, req.GetCompactedFrom()...)
	node.compactionExecutor.injectDone(req.GetPlanID())
	return merr.Success(), nil
}

func (node *DataNode) NotifyChannelOperation(ctx context.Context, req *datapb.ChannelOperationsRequest) (*commonpb.Status, error) {
	log.Warn("DataNode NotifyChannelOperation is unimplemented")
	return merr.Status(merr.ErrServiceUnavailable), nil
}

func (node *DataNode) CheckChannelOperationProgress(ctx context.Context, req *datapb.ChannelWatchInfo) (*datapb.ChannelOperationProgressResponse, error) {
	log.Warn("DataNode CheckChannelOperationProgress is unimplemented")
	return &datapb.ChannelOperationProgressResponse{
		Status: merr.Status(merr.ErrServiceUnavailable),
	}, nil
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
		Status:     merr.Success(),
		TaskId:     req.GetImportTask().TaskId,
		DatanodeId: paramtable.GetNodeID(),
		State:      commonpb.ImportState_ImportStarted,
		Segments:   make([]int64, 0),
		AutoIds:    make([]int64, 0),
		RowCount:   0,
	}
	importResult.Infos = append(importResult.Infos, &commonpb.KeyValuePair{Key: importutil.ProgressPercent, Value: "0"})

	// Spawn a new context to ignore cancellation from parental context.
	newCtx, cancel := context.WithTimeout(context.TODO(), paramtable.Get().DataNodeCfg.BulkInsertTimeoutSeconds.GetAsDuration(time.Second))
	defer cancel()

	// function to report import state to RootCoord.
	// retry 10 times, if the rootcoord is down, the report function will cost 20+ seconds
	reportFunc := reportImportFunc(node)
	returnFailFunc := func(msg string, err error) (*commonpb.Status, error) {
		logFields = append(logFields, zap.Error(err))
		log.Warn(msg, logFields...)
		importResult.State = commonpb.ImportState_ImportFailed
		importResult.Infos = append(importResult.Infos, &commonpb.KeyValuePair{Key: importutil.FailedReason, Value: err.Error()})

		reportFunc(importResult)

		return merr.Status(err), nil
	}

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		logFields = append(logFields, zap.Error(err))
		log.Warn("DataNode import failed, node is not healthy", logFields...)
		return merr.Status(err), nil
	}

	// get a timestamp for all the rows
	// Ignore cancellation from parent context.
	ts, _, err := node.broker.AllocTimestamp(newCtx, 1)
	if err != nil {
		return returnFailFunc("DataNode alloc ts failed", err)
	}

	// get collection schema and shard number
	metaService := newMetaService(node.broker, req.GetImportTask().GetCollectionId())
	colInfo, err := metaService.getCollectionInfo(newCtx, req.GetImportTask().GetCollectionId(), 0)
	if err != nil {
		return returnFailFunc("failed to get collection info for collection ID", err)
	}

	var partitionIDs []int64
	if req.GetImportTask().GetPartitionId() == 0 {
		if !typeutil.HasPartitionKey(colInfo.GetSchema()) {
			err = errors.New("try auto-distribute data but the collection has no partition key")
			return returnFailFunc(err.Error(), err)
		}
		// TODO: prefer to set partitionIDs in coord instead of get here.
		// the colInfo doesn't have a correct database name(it is empty). use the database name passed from rootcoord.
		partitions, err := node.broker.ShowPartitions(ctx, req.GetImportTask().GetDatabaseName(), colInfo.GetCollectionName())
		if err != nil {
			return returnFailFunc("failed to get partition id list", err)
		}
		_, partitionIDs, err = typeutil.RearrangePartitionsForPartitionKey(partitions)
		if err != nil {
			return returnFailFunc("failed to rearrange target partitions", err)
		}
	} else {
		partitionIDs = []int64{req.GetImportTask().GetPartitionId()}
	}

	collectionInfo, err := importutil.NewCollectionInfo(colInfo.GetSchema(), colInfo.GetShardsNum(), partitionIDs)
	if err != nil {
		return returnFailFunc("invalid collection info to import", err)
	}

	// parse files and generate segments
	segmentSize := Params.DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024
	importWrapper := importutil.NewImportWrapper(newCtx, collectionInfo, segmentSize, Params.DataNodeCfg.BinLogMaxSize.GetAsInt64(),
		node.allocator.GetIDAlloactor(), node.chunkManager, importResult, reportFunc)
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

	resp := merr.Success()
	return resp, nil
}

func (node *DataNode) FlushChannels(ctx context.Context, req *datapb.FlushChannelsRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(zap.Int64("nodeId", paramtable.GetNodeID()),
		zap.Time("flushTs", tsoutil.PhysicalTime(req.GetFlushTs())),
		zap.Strings("channels", req.GetChannels()))

	log.Info("DataNode receives FlushChannels request")

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		log.Warn("DataNode.FlushChannels failed", zap.Error(err))
		return merr.Status(err), nil
	}

	for _, channel := range req.GetChannels() {
		err := node.writeBufferManager.FlushChannel(ctx, channel, req.GetFlushTs())
		if err != nil {
			log.Warn("failed to flush channel", zap.String("channel", channel), zap.Error(err))
			return merr.Status(err), nil
		}
	}

	return merr.Success(), nil
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
		ds, ok = node.flowgraphManager.GetFlowgraphService(req.GetChannelName())
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
		id, innerError := node.getChannelLatestMsgID(context.Background(), req.GetChannelName(), req.GetSegmentId())
		posID = id
		return innerError
	}, retry.Attempts(30))

	if err != nil {
		return &datapb.AddImportSegmentResponse{
			Status: merr.Status(err),
		}, nil
	}
	// Add the new segment to the channel.
	if len(ds.metacache.GetSegmentIDsBy(metacache.WithSegmentIDs(req.GetSegmentId()), metacache.WithSegmentState(commonpb.SegmentState_Flushed))) == 0 {
		log.Info("adding a new segment to channel", logFields...)
		pks, err := loadStats(ctx, node.chunkManager, ds.metacache.Schema(), req.GetSegmentId(), req.GetCollectionId(), req.GetStatsLog(), req.GetBase().GetTimestamp())
		if err != nil {
			log.Warn("failed to get segment pk stats", zap.Error(err))
			return &datapb.AddImportSegmentResponse{
				Status: merr.Status(err),
			}, nil
		}

		// Add segment as a flushed segment, but set `importing` to true to add extra information of the segment.
		// By 'extra information' we mean segment info while adding a `SegmentType_Flushed` typed segment.
		// ds.metacache.
		ds.metacache.AddSegment(&datapb.SegmentInfo{
			ID:            req.GetSegmentId(),
			State:         commonpb.SegmentState_Flushed,
			CollectionID:  req.GetCollectionId(),
			PartitionID:   req.GetPartitionId(),
			InsertChannel: req.GetChannelName(),
			NumOfRows:     req.GetRowNum(),
			Statslogs:     req.GetStatsLog(),
			StartPosition: &msgpb.MsgPosition{
				ChannelName: req.GetChannelName(),
				MsgID:       posID,
				Timestamp:   req.GetBase().GetTimestamp(),
			},
			DmlPosition: &msgpb.MsgPosition{
				ChannelName: req.GetChannelName(),
				MsgID:       posID,
				Timestamp:   req.GetBase().GetTimestamp(),
			},
		}, func(info *datapb.SegmentInfo) *metacache.BloomFilterSet {
			bfs := metacache.NewBloomFilterSet(pks...)
			return bfs
		}, metacache.UpdateImporting(true))
	}

	return &datapb.AddImportSegmentResponse{
		Status:     merr.Success(),
		ChannelPos: posID,
	}, nil
}

func (node *DataNode) getChannelLatestMsgID(ctx context.Context, channelName string, segmentID int64) ([]byte, error) {
	pChannelName := funcutil.ToPhysicalChannel(channelName)
	dmlStream, err := node.factory.NewMsgStream(ctx)
	if err != nil {
		return nil, err
	}
	defer dmlStream.Close()

	subName := fmt.Sprintf("datanode-%d-%s-%d", paramtable.GetNodeID(), channelName, segmentID)
	log.Debug("dataSyncService register consumer for getChannelLatestMsgID",
		zap.String("pChannelName", pChannelName),
		zap.String("subscription", subName),
	)
	dmlStream.AsConsumer(ctx, []string{pChannelName}, subName, mqwrapper.SubscriptionPositionUnknown)
	id, err := dmlStream.GetLatestMsgID(pChannelName)
	if err != nil {
		log.Error("fail to GetLatestMsgID", zap.String("pChannelName", pChannelName), zap.Error(err))
		return nil, err
	}
	return id.Serialize(), nil
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
		logFields = append(logFields, zap.Int64("collection ID", colID))
		logFields = append(logFields, zap.String("target channel name", targetChName))
		log.Info("assign segment for the import task", logFields...)
		ids, err := node.broker.AssignSegmentID(context.Background(), segmentIDReq.GetSegmentIDRequests()...)
		if err != nil {
			return 0, "", errors.Wrap(err, "failed to AssignSegmentID")
		}

		if len(ids) == 0 {
			return 0, "", merr.WrapErrSegmentNotFound(0, "failed to assign segment id")
		}

		segmentID := ids[0]
		logFields = append(logFields, zap.Int64("segmentID", segmentID))
		log.Info("new segment assigned", logFields...)

		// call report to notify the rootcoord update the segment id list for this task
		// ignore the returned error, since even report failed the segments still can be cleaned
		// retry 10 times, if the rootcoord is down, the report function will cost 20+ seconds
		importResult := &rootcoordpb.ImportResult{
			Status:     merr.Success(),
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
		logFields = append(logFields, zap.Int("row count", rowNum))

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
		targetChName string, rowCount int64, partID int64,
	) error {
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
			err := node.broker.SaveImportSegment(context.Background(), &datapb.SaveImportSegmentRequest{
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
				if errors.Is(err, merr.ErrServiceNotReady) {
					return retry.Unrecoverable(err)
				}
				return err
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
	collID int64, partID int64,
) *datapb.AssignSegmentIDRequest {
	// use the first field's row count as segment row count
	// all the fields row count are same, checked by ImportWrapper
	// ask DataCoord to alloc a new segment
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
	fields map[storage.FieldID]storage.FieldData, node *DataNode, segmentID, colID, partID UniqueID,
) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tsFieldData := make([]int64, rowNum)
	for i := range tsFieldData {
		tsFieldData[i] = int64(ts)
	}
	fields[common.TimeStampField] = &storage.Int64FieldData{
		Data: tsFieldData,
	}

	if err := node.broker.UpdateSegmentStatistics(context.TODO(), &datapb.UpdateSegmentStatisticsRequest{
		Stats: []*commonpb.SegmentStats{
			{
				SegmentID: segmentID,
				NumRows:   int64(rowNum),
			},
		},
	}); err != nil {
		return nil, nil, err
	}

	insertData := &InsertData{
		Data: fields,
	}
	// data.updateSize(int64(rowNum))
	meta := &etcdpb.CollectionMeta{
		ID:     colID,
		Schema: schema,
	}
	iCodec := storage.NewInsertCodecWithSchema(meta)

	binLogs, err := iCodec.Serialize(partID, segmentID, insertData)
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
			EntriesNum:    int64(rowNum),
			TimestampFrom: ts,
			TimestampTo:   ts,
			LogPath:       key,
			LogSize:       int64(len(blob.Value)),
		}
		field2Logidx[fieldID] = logidx
	}

	field2Stats := make(map[UniqueID]*datapb.Binlog)
	// write stats binlog
	statsBinLog, err := iCodec.SerializePkStatsByData(insertData)
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
		EntriesNum:    int64(rowNum),
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
			err := node.broker.ReportImport(context.Background(), importResult)
			if err != nil {
				log.Error("failed to report import state to RootCoord", zap.Error(err))
			}
			return err
		}, retry.Attempts(node.reportImportRetryTimes))

		return err
	}
}

func logDupFlush(cID, segID int64) {
	log.Info("segment is already being flushed, ignoring flush request",
		zap.Int64("collectionID", cID),
		zap.Int64("segmentID", segID))
}

func (node *DataNode) PreImport(ctx context.Context, req *datapb.PreImportRequest) (*commonpb.Status, error) {
	return nil, merr.ErrServiceUnimplemented
}

func (node *DataNode) ImportV2(ctx context.Context, req *datapb.ImportRequest) (*commonpb.Status, error) {
	return nil, merr.ErrServiceUnimplemented
}

func (node *DataNode) QueryPreImport(ctx context.Context, req *datapb.QueryPreImportRequest) (*datapb.QueryPreImportResponse, error) {
	return nil, merr.ErrServiceUnimplemented
}

func (node *DataNode) QueryImport(ctx context.Context, req *datapb.QueryImportRequest) (*datapb.QueryImportResponse, error) {
	return nil, merr.ErrServiceUnimplemented
}

func (node *DataNode) DropImport(ctx context.Context, req *datapb.DropImportRequest) (*commonpb.Status, error) {
	return nil, merr.ErrServiceUnimplemented
}
