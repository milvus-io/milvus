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

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/datanode/compaction"
	"github.com/milvus-io/milvus/internal/datanode/importv2"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
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

func (node *DataNode) FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest) (*commonpb.Status, error) {
	serverID := node.GetNodeID()
	log := log.Ctx(ctx).With(
		zap.Int64("nodeID", serverID),
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("channelName", req.GetChannelName()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
	)
	log.Info("receive FlushSegments request")
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		log.Warn("failed to FlushSegments", zap.Error(err))
		return merr.Status(err), nil
	}

	if req.GetBase().GetTargetID() != serverID {
		log.Warn("faled to FlushSegments, target node not match", zap.Int64("targetID", req.GetBase().GetTargetID()))
		return merr.Status(merr.WrapErrNodeNotMatch(req.GetBase().GetTargetID(), serverID)), nil
	}

	err := node.writeBufferManager.SealSegments(ctx, req.GetChannelName(), req.GetSegmentIDs())
	if err != nil {
		log.Warn("failed to FlushSegments", zap.Error(err))
		return merr.Status(err), nil
	}

	log.Info("success to FlushSegments")
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
		log.Warn("DataNode.ShowConfigurations failed", zap.Int64("nodeId", node.GetNodeID()), zap.Error(err))

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
		log.Warn("DataNode.GetMetrics failed", zap.Int64("nodeId", node.GetNodeID()), zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: merr.Status(err),
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Warn("DataNode.GetMetrics failed to parse metric type",
			zap.Int64("nodeID", node.GetNodeID()),
			zap.String("req", req.Request),
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: merr.Status(err),
		}, nil
	}

	if metricType == metricsinfo.SystemInfoMetrics {
		systemInfoMetrics, err := node.getSystemInfoMetrics(ctx, req)
		if err != nil {
			log.Warn("DataNode GetMetrics failed", zap.Int64("nodeID", node.GetNodeID()), zap.Error(err))
			return &milvuspb.GetMetricsResponse{
				Status: merr.Status(err),
			}, nil
		}

		return systemInfoMetrics, nil
	}

	log.RatedWarn(60, "DataNode.GetMetrics failed, request metric type is not implemented yet",
		zap.Int64("nodeID", node.GetNodeID()),
		zap.String("req", req.Request),
		zap.String("metric_type", metricType))

	return &milvuspb.GetMetricsResponse{
		Status: merr.Status(merr.WrapErrMetricNotFound(metricType)),
	}, nil
}

// CompactionV2 handles compaction request from DataCoord
// returns status as long as compaction task enqueued or invalid
func (node *DataNode) CompactionV2(ctx context.Context, req *datapb.CompactionPlan) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(zap.Int64("planID", req.GetPlanID()))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		log.Warn("DataNode.Compaction failed", zap.Int64("nodeId", node.GetNodeID()), zap.Error(err))
		return merr.Status(err), nil
	}

	if len(req.GetSegmentBinlogs()) == 0 {
		log.Info("no segments to compact")
		return merr.Success(), nil
	}

	if req.GetBeginLogID() == 0 {
		return merr.Status(merr.WrapErrParameterInvalidMsg("invalid beginLogID")), nil
	}

	/*
		spanCtx := trace.SpanContextFromContext(ctx)

		taskCtx := trace.ContextWithSpanContext(node.ctx, spanCtx)*/
	taskCtx := tracer.Propagate(ctx, node.ctx)

	var task compaction.Compactor
	binlogIO := io.NewBinlogIO(node.chunkManager)
	switch req.GetType() {
	case datapb.CompactionType_Level0DeleteCompaction:
		task = compaction.NewLevelZeroCompactionTask(
			taskCtx,
			binlogIO,
			node.chunkManager,
			req,
		)
	case datapb.CompactionType_MixCompaction:
		if req.GetPreAllocatedSegmentIDs() == nil || req.GetPreAllocatedSegmentIDs().GetBegin() == 0 {
			return merr.Status(merr.WrapErrParameterInvalidMsg("invalid pre-allocated segmentID range")), nil
		}
		task = compaction.NewMixCompactionTask(
			taskCtx,
			binlogIO,
			req,
		)
	case datapb.CompactionType_ClusteringCompaction:
		if req.GetPreAllocatedSegmentIDs() == nil || req.GetPreAllocatedSegmentIDs().GetBegin() == 0 {
			return merr.Status(merr.WrapErrParameterInvalidMsg("invalid pre-allocated segmentID range")), nil
		}
		task = compaction.NewClusteringCompactionTask(
			taskCtx,
			binlogIO,
			req,
		)
	default:
		log.Warn("Unknown compaction type", zap.String("type", req.GetType().String()))
		return merr.Status(merr.WrapErrParameterInvalidMsg("Unknown compaction type: %v", req.GetType().String())), nil
	}

	succeed, err := node.compactionExecutor.Execute(task)
	if succeed {
		return merr.Success(), nil
	} else {
		return merr.Status(err), nil
	}
}

// GetCompactionState called by DataCoord
// return status of all compaction plans
func (node *DataNode) GetCompactionState(ctx context.Context, req *datapb.CompactionStateRequest) (*datapb.CompactionStateResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		log.Warn("DataNode.GetCompactionState failed", zap.Int64("nodeId", node.GetNodeID()), zap.Error(err))
		return &datapb.CompactionStateResponse{
			Status: merr.Status(err),
		}, nil
	}

	results := node.compactionExecutor.GetResults(req.GetPlanID())
	return &datapb.CompactionStateResponse{
		Status:  merr.Success(),
		Results: results,
	}, nil
}

// SyncSegments called by DataCoord, sync the compacted segments' meta between DC and DN
func (node *DataNode) SyncSegments(ctx context.Context, req *datapb.SyncSegmentsRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("planID", req.GetPlanID()),
		zap.Int64("nodeID", node.GetNodeID()),
		zap.Int64("collectionID", req.GetCollectionId()),
		zap.Int64("partitionID", req.GetPartitionId()),
		zap.String("channel", req.GetChannelName()),
	)

	log.Info("DataNode receives SyncSegments")

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		log.Warn("DataNode.SyncSegments failed", zap.Error(err))
		return merr.Status(err), nil
	}

	if len(req.GetSegmentInfos()) <= 0 {
		log.Info("sync segments is empty, skip it")
		return merr.Success(), nil
	}

	ds, ok := node.flowgraphManager.GetFlowgraphService(req.GetChannelName())
	if !ok {
		node.compactionExecutor.DiscardPlan(req.GetChannelName())
		err := merr.WrapErrChannelNotFound(req.GetChannelName())
		log.Warn("failed to get flow graph service", zap.Error(err))
		return merr.Status(err), nil
	}

	allSegments := make(map[int64]struct{})
	for segID := range req.GetSegmentInfos() {
		allSegments[segID] = struct{}{}
	}

	missingSegments := ds.GetMetaCache().DetectMissingSegments(allSegments)

	newSegments := make([]*datapb.SyncSegmentInfo, 0, len(missingSegments))
	futures := make([]*conc.Future[any], 0, len(missingSegments))

	for _, segID := range missingSegments {
		newSeg := req.GetSegmentInfos()[segID]
		switch newSeg.GetLevel() {
		case datapb.SegmentLevel_L0:
			log.Warn("segment level is L0, may be the channel has not been successfully watched yet", zap.Int64("segmentID", segID))
		case datapb.SegmentLevel_Legacy:
			log.Warn("segment level is legacy, please check", zap.Int64("segmentID", segID))
		default:
			if newSeg.GetState() == commonpb.SegmentState_Flushed {
				log.Info("segment loading PKs", zap.Int64("segmentID", segID))
				newSegments = append(newSegments, newSeg)
				future := io.GetOrCreateStatsPool().Submit(func() (any, error) {
					var val *pkoracle.BloomFilterSet
					var err error
					err = binlog.DecompressBinLog(storage.StatsBinlog, req.GetCollectionId(), req.GetPartitionId(), newSeg.GetSegmentId(), []*datapb.FieldBinlog{newSeg.GetPkStatsLog()})
					if err != nil {
						log.Warn("failed to DecompressBinLog", zap.Error(err))
						return val, err
					}
					pks, err := compaction.LoadStats(ctx, node.chunkManager, ds.GetMetaCache().Schema(), newSeg.GetSegmentId(), []*datapb.FieldBinlog{newSeg.GetPkStatsLog()})
					if err != nil {
						log.Warn("failed to load segment stats log", zap.Error(err))
						return val, err
					}
					val = pkoracle.NewBloomFilterSet(pks...)
					return val, nil
				})
				futures = append(futures, future)
			}
		}
	}

	err := conc.AwaitAll(futures...)
	if err != nil {
		return merr.Status(err), nil
	}

	newSegmentsBF := lo.Map(futures, func(future *conc.Future[any], _ int) *pkoracle.BloomFilterSet {
		return future.Value().(*pkoracle.BloomFilterSet)
	})

	ds.GetMetaCache().UpdateSegmentView(req.GetPartitionId(), newSegments, newSegmentsBF, allSegments)
	return merr.Success(), nil
}

func (node *DataNode) NotifyChannelOperation(ctx context.Context, req *datapb.ChannelOperationsRequest) (*commonpb.Status, error) {
	log.Ctx(ctx).Info("DataNode receives NotifyChannelOperation",
		zap.Int("operation count", len(req.GetInfos())))

	if node.channelManager == nil {
		log.Warn("DataNode NotifyChannelOperation failed due to nil channelManager")
		return merr.Status(merr.WrapErrServiceInternal("channelManager is nil! Ignore if you are upgrading datanode/coord to rpc based watch")), nil
	}

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		log.Warn("DataNode.NotifyChannelOperation failed", zap.Int64("nodeId", node.GetNodeID()), zap.Error(err))
		return merr.Status(err), nil
	}

	for _, info := range req.GetInfos() {
		err := node.channelManager.Submit(info)
		if err != nil {
			log.Warn("Submit error", zap.Error(err))
			return merr.Status(err), nil
		}
	}

	return merr.Status(nil), nil
}

func (node *DataNode) CheckChannelOperationProgress(ctx context.Context, req *datapb.ChannelWatchInfo) (*datapb.ChannelOperationProgressResponse, error) {
	log := log.Ctx(ctx).With(
		zap.String("channel", req.GetVchan().GetChannelName()),
		zap.String("operation", req.GetState().String()),
	)

	log.Info("DataNode receives CheckChannelOperationProgress")

	if node.channelManager == nil {
		log.Warn("DataNode CheckChannelOperationProgress failed due to nil channelManager")
		return &datapb.ChannelOperationProgressResponse{
			Status: merr.Status(merr.WrapErrServiceInternal("channelManager is nil! Ignore if you are upgrading datanode/coord to rpc based watch")),
		}, nil
	}

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		log.Warn("DataNode.CheckChannelOperationProgress failed", zap.Int64("nodeId", node.GetNodeID()), zap.Error(err))
		return &datapb.ChannelOperationProgressResponse{
			Status: merr.Status(err),
		}, nil
	}
	return node.channelManager.GetProgress(req), nil
}

func (node *DataNode) FlushChannels(ctx context.Context, req *datapb.FlushChannelsRequest) (*commonpb.Status, error) {
	metrics.DataNodeFlushReqCounter.WithLabelValues(fmt.Sprint(node.GetNodeID()), metrics.TotalLabel).Inc()
	log := log.Ctx(ctx).With(zap.Int64("nodeId", node.GetNodeID()),
		zap.Uint64("flushTs", req.GetFlushTs()),
		zap.Time("flushTs in Time", tsoutil.PhysicalTime(req.GetFlushTs())),
		zap.Strings("channels", req.GetChannels()))

	log.Info("DataNode receives FlushChannels request")
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		log.Warn("DataNode.FlushChannels failed", zap.Error(err))
		return merr.Status(err), nil
	}

	for _, channel := range req.GetChannels() {
		err := node.writeBufferManager.FlushChannel(ctx, channel, req.GetFlushTs())
		if err != nil {
			log.Warn("WriteBufferManager failed to flush channel", zap.String("channel", channel), zap.Error(err))
			return merr.Status(err), nil
		}
	}

	metrics.DataNodeFlushReqCounter.WithLabelValues(fmt.Sprint(node.GetNodeID()), metrics.SuccessLabel).Inc()
	log.Info("success to FlushChannels")
	return merr.Success(), nil
}

func (node *DataNode) PreImport(ctx context.Context, req *datapb.PreImportRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(zap.Int64("taskID", req.GetTaskID()),
		zap.Int64("jobID", req.GetJobID()),
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64s("partitionIDs", req.GetPartitionIDs()),
		zap.Strings("vchannels", req.GetVchannels()),
		zap.Any("files", req.GetImportFiles()))

	log.Info("datanode receive preimport request")

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	var task importv2.Task
	if importutilv2.IsL0Import(req.GetOptions()) {
		task = importv2.NewL0PreImportTask(req, node.importTaskMgr, node.chunkManager)
	} else {
		task = importv2.NewPreImportTask(req, node.importTaskMgr, node.chunkManager)
	}
	node.importTaskMgr.Add(task)

	log.Info("datanode added preimport task")
	return merr.Success(), nil
}

func (node *DataNode) ImportV2(ctx context.Context, req *datapb.ImportRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(zap.Int64("taskID", req.GetTaskID()),
		zap.Int64("jobID", req.GetJobID()),
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Any("segments", req.GetRequestSegments()),
		zap.Any("files", req.GetFiles()))

	log.Info("datanode receive import request")

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	var task importv2.Task
	if importutilv2.IsL0Import(req.GetOptions()) {
		task = importv2.NewL0ImportTask(req, node.importTaskMgr, node.syncMgr, node.chunkManager)
	} else {
		task = importv2.NewImportTask(req, node.importTaskMgr, node.syncMgr, node.chunkManager)
	}
	node.importTaskMgr.Add(task)

	log.Info("datanode added import task")
	return merr.Success(), nil
}

func (node *DataNode) QueryPreImport(ctx context.Context, req *datapb.QueryPreImportRequest) (*datapb.QueryPreImportResponse, error) {
	log := log.Ctx(ctx).With(zap.Int64("taskID", req.GetTaskID()),
		zap.Int64("jobID", req.GetJobID()))

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &datapb.QueryPreImportResponse{Status: merr.Status(err)}, nil
	}
	status := merr.Success()
	task := node.importTaskMgr.Get(req.GetTaskID())
	if task == nil {
		status = merr.Status(importv2.WrapTaskNotFoundError(req.GetTaskID()))
	}
	log.RatedInfo(10, "datanode query preimport", zap.String("state", task.GetState().String()),
		zap.String("reason", task.GetReason()))
	return &datapb.QueryPreImportResponse{
		Status: status,
		TaskID: task.GetTaskID(),
		State:  task.GetState(),
		Reason: task.GetReason(),
		FileStats: task.(interface {
			GetFileStats() []*datapb.ImportFileStats
		}).GetFileStats(),
	}, nil
}

func (node *DataNode) QueryImport(ctx context.Context, req *datapb.QueryImportRequest) (*datapb.QueryImportResponse, error) {
	log := log.Ctx(ctx).With(zap.Int64("taskID", req.GetTaskID()),
		zap.Int64("jobID", req.GetJobID()))

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &datapb.QueryImportResponse{Status: merr.Status(err)}, nil
	}

	status := merr.Success()

	// query slot
	if req.GetQuerySlot() {
		return &datapb.QueryImportResponse{
			Status: status,
			Slots:  node.importScheduler.Slots(),
		}, nil
	}

	// query import
	task := node.importTaskMgr.Get(req.GetTaskID())
	if task == nil {
		status = merr.Status(importv2.WrapTaskNotFoundError(req.GetTaskID()))
	}
	log.RatedInfo(10, "datanode query import", zap.String("state", task.GetState().String()),
		zap.String("reason", task.GetReason()))
	return &datapb.QueryImportResponse{
		Status: status,
		TaskID: task.GetTaskID(),
		State:  task.GetState(),
		Reason: task.GetReason(),
		ImportSegmentsInfo: task.(interface {
			GetSegmentsInfo() []*datapb.ImportSegmentInfo
		}).GetSegmentsInfo(),
	}, nil
}

func (node *DataNode) DropImport(ctx context.Context, req *datapb.DropImportRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(zap.Int64("taskID", req.GetTaskID()),
		zap.Int64("jobID", req.GetJobID()))

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	node.importTaskMgr.Remove(req.GetTaskID())

	log.Info("datanode drop import done")

	return merr.Success(), nil
}

func (node *DataNode) QuerySlot(ctx context.Context, req *datapb.QuerySlotRequest) (*datapb.QuerySlotResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &datapb.QuerySlotResponse{
			Status: merr.Status(err),
		}, nil
	}

	return &datapb.QuerySlotResponse{
		Status:   merr.Success(),
		NumSlots: node.compactionExecutor.Slots(),
	}, nil
}

func (node *DataNode) DropCompactionPlan(ctx context.Context, req *datapb.DropCompactionPlanRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	node.compactionExecutor.RemoveTask(req.GetPlanID())
	log.Ctx(ctx).Info("DropCompactionPlans success", zap.Int64("planID", req.GetPlanID()))
	return merr.Success(), nil
}
