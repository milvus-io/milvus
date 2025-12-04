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

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/datanode/compactor"
	"github.com/milvus-io/milvus/internal/datanode/importv2"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/tracer"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// WatchDmChannels is not in use
func (node *DataNode) WatchDmChannels(ctx context.Context, in *datapb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	log.Ctx(ctx).Warn("DataNode WatchDmChannels is not in use")

	// TODO ERROR OF GRPC NOT IN USE
	return merr.Success(), nil
}

// GetComponentStates will return current state of DataNode
func (node *DataNode) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	nodeID := common.NotRegisteredID
	state := node.GetStateCode()
	log.Ctx(ctx).Debug("DataNode current state", zap.String("State", state.String()))
	if node.GetSession() != nil && node.session.Registered() {
		nodeID = node.GetSession().ServerID
	}
	states := &milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			// NodeID:    Params.NodeID, // will race with DataNode.Register()
			NodeID:    nodeID,
			Role:      node.Role,
			StateCode: state,
		},
		SubcomponentStates: make([]*milvuspb.ComponentInfo, 0),
		Status:             merr.Success(),
	}
	return states, nil
}

// Deprecated after v2.6.0
func (node *DataNode) FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest) (*commonpb.Status, error) {
	log.Ctx(ctx).Info("FlushSegments was deprecated after v2.6.0, return success")
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
	log.Ctx(ctx).Debug("DataNode.ShowConfigurations", zap.String("pattern", req.Pattern))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		log.Ctx(ctx).Warn("DataNode.ShowConfigurations failed", zap.Int64("nodeId", node.GetNodeID()), zap.Error(err))

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
		log.Ctx(ctx).Warn("DataNode.GetMetrics failed", zap.Int64("nodeId", node.GetNodeID()), zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: merr.Status(err),
		}, nil
	}

	resp := &milvuspb.GetMetricsResponse{
		Status: merr.Success(),
		ComponentName: metricsinfo.ConstructComponentName(typeutil.DataNodeRole,
			paramtable.GetNodeID()),
	}

	ret, err := node.metricsRequest.ExecuteMetricsRequest(ctx, req)
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	resp.Response = ret
	return resp, nil
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

	if req.GetPreAllocatedLogIDs().GetBegin() == 0 || req.GetPreAllocatedLogIDs().GetEnd() == 0 {
		return merr.Status(merr.WrapErrParameterInvalidMsg(fmt.Sprintf("invalid beginID %d or invalid endID %d", req.GetPreAllocatedLogIDs().GetBegin(), req.GetPreAllocatedLogIDs().GetEnd()))), nil
	}

	/*
		spanCtx := trace.SpanContextFromContext(ctx)

		taskCtx := trace.ContextWithSpanContext(node.ctx, spanCtx)*/
	taskCtx := tracer.Propagate(ctx, node.ctx)
	compactionParams, err := compaction.ParseParamsFromJSON(req.GetJsonParams())
	if err != nil {
		return merr.Status(err), err
	}
	cm, err := node.storageFactory.NewChunkManager(node.ctx, compactionParams.StorageConfig)
	if err != nil {
		log.Error("create chunk manager failed",
			zap.String("bucket", compactionParams.StorageConfig.GetBucketName()),
			zap.String("ROOTPATH", compactionParams.StorageConfig.GetRootPath()),
			zap.Error(err),
		)
		return merr.Status(err), err
	}
	var task compactor.Compactor
	binlogIO := io.NewBinlogIO(cm)
	switch req.GetType() {
	case datapb.CompactionType_Level0DeleteCompaction:
		task = compactor.NewLevelZeroCompactionTask(
			taskCtx,
			binlogIO,
			cm,
			req,
			compactionParams,
		)
	case datapb.CompactionType_MixCompaction:
		if req.GetPreAllocatedSegmentIDs() == nil || req.GetPreAllocatedSegmentIDs().GetBegin() == 0 {
			return merr.Status(merr.WrapErrParameterInvalidMsg("invalid pre-allocated segmentID range")), nil
		}
		pk, err := typeutil.GetPrimaryFieldSchema(req.GetSchema())
		if err != nil {
			return merr.Status(err), err
		}
		task = compactor.NewMixCompactionTask(
			taskCtx,
			binlogIO,
			req,
			compactionParams,
			[]int64{pk.GetFieldID()},
		)
	case datapb.CompactionType_ClusteringCompaction:
		if req.GetPreAllocatedSegmentIDs() == nil || req.GetPreAllocatedSegmentIDs().GetBegin() == 0 {
			return merr.Status(merr.WrapErrParameterInvalidMsg("invalid pre-allocated segmentID range")), nil
		}
		task = compactor.NewClusteringCompactionTask(
			taskCtx,
			binlogIO,
			req,
			compactionParams,
		)
	case datapb.CompactionType_SortCompaction:
		if req.GetPreAllocatedSegmentIDs() == nil || req.GetPreAllocatedSegmentIDs().GetBegin() == 0 {
			return merr.Status(merr.WrapErrParameterInvalidMsg("invalid pre-allocated segmentID range")), nil
		}
		pk, err := typeutil.GetPrimaryFieldSchema(req.GetSchema())
		if err != nil {
			return merr.Status(err), err
		}
		task = compactor.NewSortCompactionTask(
			taskCtx,
			binlogIO,
			req,
			compactionParams,
			[]int64{pk.GetFieldID()},
		)
	case datapb.CompactionType_PartitionKeySortCompaction:
		if req.GetPreAllocatedSegmentIDs() == nil || req.GetPreAllocatedSegmentIDs().GetBegin() == 0 {
			return merr.Status(merr.WrapErrParameterInvalidMsg("invalid pre-allocated segmentID range")), nil
		}
		pk, err := typeutil.GetPartitionKeyFieldSchema(req.GetSchema())
		partitionkey, err := typeutil.GetPartitionKeyFieldSchema(req.GetSchema())
		if err != nil {
			return merr.Status(err), err
		}
		task = compactor.NewSortCompactionTask(
			taskCtx,
			binlogIO,
			req,
			compactionParams,
			[]int64{partitionkey.GetFieldID(), pk.GetFieldID()},
		)
	case datapb.CompactionType_ClusteringPartitionKeySortCompaction:
		// TODO
	default:
		log.Warn("Unknown compaction type", zap.String("type", req.GetType().String()))
		return merr.Status(merr.WrapErrParameterInvalidMsg("Unknown compaction type: %v", req.GetType().String())), nil
	}

	succeed, err := node.compactionExecutor.Enqueue(task)
	if succeed {
		return merr.Success(), nil
	} else {
		return merr.Status(err), nil
	}
}

// GetCompactionState called by DataCoord return status of all compaction plans
// Deprecated after v2.6.0
func (node *DataNode) GetCompactionState(ctx context.Context, req *datapb.CompactionStateRequest) (*datapb.CompactionStateResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		log.Ctx(ctx).Warn("DataNode.GetCompactionState failed", zap.Int64("nodeId", node.GetNodeID()), zap.Error(err))
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
// Deprecated after v2.6.0
func (node *DataNode) SyncSegments(ctx context.Context, req *datapb.SyncSegmentsRequest) (*commonpb.Status, error) {
	log.Ctx(ctx).Info("DataNode deprecated SyncSegments after v2.6.0, return success")
	return merr.Success(), nil
}

// Deprecated after v2.6.0
func (node *DataNode) NotifyChannelOperation(ctx context.Context, req *datapb.ChannelOperationsRequest) (*commonpb.Status, error) {
	log.Ctx(ctx).Info("DataNode deprecated NotifyChannelOperation after v2.6.0, return success")
	return merr.Success(), nil
}

// Deprecated after v2.6.0
func (node *DataNode) CheckChannelOperationProgress(ctx context.Context, req *datapb.ChannelWatchInfo) (*datapb.ChannelOperationProgressResponse, error) {
	log.Ctx(ctx).Info("DataNode deprecated CheckChannelOperationProgress after v2.6.0, return success")
	return &datapb.ChannelOperationProgressResponse{
		Status: merr.Success(),
	}, nil
}

// Deprecated after v2.6.0
func (node *DataNode) FlushChannels(ctx context.Context, req *datapb.FlushChannelsRequest) (*commonpb.Status, error) {
	log.Ctx(ctx).Info("DataNode deprecated FlushChannels after v2.6.0, return success")
	return merr.Success(), nil
}

func (node *DataNode) PreImport(ctx context.Context, req *datapb.PreImportRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(zap.Int64("taskID", req.GetTaskID()),
		zap.Int64("jobID", req.GetJobID()),
		zap.Int64("taskSlot", req.GetTaskSlot()),
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64s("partitionIDs", req.GetPartitionIDs()),
		zap.Strings("vchannels", req.GetVchannels()),
		zap.Any("files", req.GetImportFiles()))

	log.Info("datanode receive preimport request")

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	cm, err := node.storageFactory.NewChunkManager(node.ctx, req.GetStorageConfig())
	if err != nil {
		log.Error("create chunk manager failed", zap.String("bucket", req.GetStorageConfig().GetBucketName()),
			zap.String("accessKey", req.GetStorageConfig().GetAccessKeyID()),
			zap.Error(err),
		)
		return merr.Status(err), nil
	}

	var task importv2.Task
	if importutilv2.IsL0Import(req.GetOptions()) {
		task = importv2.NewL0PreImportTask(req, node.importTaskMgr, cm)
	} else {
		task = importv2.NewPreImportTask(req, node.importTaskMgr, cm)
	}
	node.importTaskMgr.Add(task)

	log.Info("datanode added preimport task")
	return merr.Success(), nil
}

func (node *DataNode) ImportV2(ctx context.Context, req *datapb.ImportRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(zap.Int64("taskID", req.GetTaskID()),
		zap.Int64("jobID", req.GetJobID()),
		zap.Int64("taskSlot", req.GetTaskSlot()),
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64s("partitionIDs", req.GetPartitionIDs()),
		zap.Strings("vchannels", req.GetVchannels()),
		zap.Uint64("ts", req.GetTs()),
		zap.Int64("idBegin", req.GetIDRange().GetBegin()),
		zap.Int64("idEnd", req.GetIDRange().GetEnd()),
		zap.Any("segments", req.GetRequestSegments()),
		zap.Any("files", req.GetFiles()))

	log.Info("datanode receive import request")

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	cm, err := node.storageFactory.NewChunkManager(node.ctx, req.GetStorageConfig())
	if err != nil {
		log.Error("create chunk manager failed", zap.String("bucket", req.GetStorageConfig().GetBucketName()),
			zap.String("accessKey", req.GetStorageConfig().GetAccessKeyID()),
			zap.Error(err),
		)
		return merr.Status(err), nil
	}
	var task importv2.Task
	if importutilv2.IsL0Import(req.GetOptions()) {
		task = importv2.NewL0ImportTask(req, node.importTaskMgr, node.syncMgr, cm)
	} else {
		task = importv2.NewImportTask(req, node.importTaskMgr, node.syncMgr, cm)
	}
	node.importTaskMgr.Add(task)

	log.Info("datanode added import task")
	return merr.Success(), nil
}

func (node *DataNode) QueryPreImport(ctx context.Context, req *datapb.QueryPreImportRequest) (*datapb.QueryPreImportResponse, error) {
	log := log.Ctx(ctx).WithRateGroup("datanode.QueryPreImport", 1, 60)

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &datapb.QueryPreImportResponse{Status: merr.Status(err)}, nil
	}
	task := node.importTaskMgr.Get(req.GetTaskID())
	if task == nil {
		return &datapb.QueryPreImportResponse{
			Status: merr.Status(importv2.WrapTaskNotFoundError(req.GetTaskID())),
		}, nil
	}
	fileStats := task.(interface {
		GetFileStats() []*datapb.ImportFileStats
	}).GetFileStats()
	logFields := []zap.Field{
		zap.Int64("taskID", task.GetTaskID()),
		zap.Int64("jobID", task.GetJobID()),
		zap.String("state", task.GetState().String()),
		zap.String("reason", task.GetReason()),
		zap.Int64("nodeID", node.GetNodeID()),
		zap.Any("fileStats", fileStats),
	}
	if task.GetState() == datapb.ImportTaskStateV2_InProgress {
		log.RatedInfo(30, "datanode query preimport", logFields...)
	} else {
		log.Info("datanode query preimport", logFields...)
	}

	return &datapb.QueryPreImportResponse{
		Status:    merr.Success(),
		TaskID:    task.GetTaskID(),
		State:     task.GetState(),
		Reason:    task.GetReason(),
		FileStats: fileStats,
	}, nil
}

func (node *DataNode) QueryImport(ctx context.Context, req *datapb.QueryImportRequest) (*datapb.QueryImportResponse, error) {
	log := log.Ctx(ctx).WithRateGroup("datanode.QueryImport", 1, 60)

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &datapb.QueryImportResponse{Status: merr.Status(err)}, nil
	}

	// query slot
	if req.GetQuerySlot() {
		return &datapb.QueryImportResponse{
			Status: merr.Success(),
			Slots:  node.importScheduler.Slots(),
		}, nil
	}

	// query import
	task := node.importTaskMgr.Get(req.GetTaskID())
	if task == nil {
		return &datapb.QueryImportResponse{
			Status: merr.Status(importv2.WrapTaskNotFoundError(req.GetTaskID())),
		}, nil
	}
	segmentsInfo := task.(interface {
		GetSegmentsInfo() []*datapb.ImportSegmentInfo
	}).GetSegmentsInfo()
	logFields := []zap.Field{
		zap.Int64("taskID", task.GetTaskID()),
		zap.Int64("jobID", task.GetJobID()),
		zap.String("state", task.GetState().String()),
		zap.String("reason", task.GetReason()),
		zap.Int64("nodeID", node.GetNodeID()),
		zap.Any("segmentsInfo", segmentsInfo),
	}
	if task.GetState() == datapb.ImportTaskStateV2_InProgress {
		log.RatedInfo(30, "datanode query import", logFields...)
	} else {
		log.Info("datanode query import", logFields...)
	}
	return &datapb.QueryImportResponse{
		Status:             merr.Success(),
		TaskID:             task.GetTaskID(),
		State:              task.GetState(),
		Reason:             task.GetReason(),
		ImportSegmentsInfo: segmentsInfo,
	}, nil
}

func (node *DataNode) DropImport(ctx context.Context, req *datapb.DropImportRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(zap.Int64("taskID", req.GetTaskID()),
		zap.Int64("jobID", req.GetJobID()),
		zap.Int64("nodeID", node.GetNodeID()))

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

	var (
		totalSlots     = node.totalSlot
		indexStatsUsed = node.taskScheduler.TaskQueue.GetUsingSlot()
		compactionUsed = node.compactionExecutor.Slots()
		importUsed     = node.importScheduler.Slots()
	)

	availableSlots := totalSlots - indexStatsUsed - compactionUsed - importUsed
	if availableSlots < 0 {
		availableSlots = 0
	}

	log.Ctx(ctx).Info("query slots done",
		zap.Int64("totalSlots", totalSlots),
		zap.Int64("availableSlots", availableSlots),
		zap.Int64("indexStatsUsed", indexStatsUsed),
		zap.Int64("compactionUsed", compactionUsed),
		zap.Int64("importUsed", importUsed),
	)

	metrics.DataNodeSlot.WithLabelValues(fmt.Sprint(node.GetNodeID()), "available").Set(float64(availableSlots))
	metrics.DataNodeSlot.WithLabelValues(fmt.Sprint(node.GetNodeID()), "total").Set(float64(totalSlots))
	metrics.DataNodeSlot.WithLabelValues(fmt.Sprint(node.GetNodeID()), "indexStatsUsed").Set(float64(indexStatsUsed))
	metrics.DataNodeSlot.WithLabelValues(fmt.Sprint(node.GetNodeID()), "compactionUsed").Set(float64(compactionUsed))
	metrics.DataNodeSlot.WithLabelValues(fmt.Sprint(node.GetNodeID()), "importUsed").Set(float64(importUsed))

	return &datapb.QuerySlotResponse{
		Status:         merr.Success(),
		AvailableSlots: availableSlots,
	}, nil
}

// Not in used now
func (node *DataNode) DropCompactionPlan(ctx context.Context, req *datapb.DropCompactionPlanRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	node.compactionExecutor.RemoveTask(req.GetPlanID())
	log.Ctx(ctx).Info("DropCompactionPlans success", zap.Int64("planID", req.GetPlanID()))
	return merr.Success(), nil
}

// CreateTask creates different types of tasks based on task type
func (node *DataNode) CreateTask(ctx context.Context, request *workerpb.CreateTaskRequest) (*commonpb.Status, error) {
	log.Ctx(ctx).Info("CreateTask received", zap.Any("properties", request.GetProperties()))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	properties := taskcommon.NewProperties(request.GetProperties())
	taskType, err := properties.GetTaskType()
	if err != nil {
		return merr.Status(err), nil
	}
	switch taskType {
	case taskcommon.PreImport:
		req := &datapb.PreImportRequest{}
		if err := proto.Unmarshal(request.GetPayload(), req); err != nil {
			return merr.Status(err), nil
		}
		if _, err := hookutil.CreateLocalEZByPluginContext(req.GetPluginContext()); err != nil {
			return merr.Status(err), nil
		}
		return node.PreImport(ctx, req)
	case taskcommon.Import:
		req := &datapb.ImportRequest{}
		if err := proto.Unmarshal(request.GetPayload(), req); err != nil {
			return merr.Status(err), nil
		}
		if _, err := hookutil.CreateLocalEZByPluginContext(req.GetPluginContext()); err != nil {
			return merr.Status(err), nil
		}
		return node.ImportV2(ctx, req)
	case taskcommon.Compaction:
		req := &datapb.CompactionPlan{}
		if err := proto.Unmarshal(request.GetPayload(), req); err != nil {
			return merr.Status(err), nil
		}
		if _, err := hookutil.CreateLocalEZByPluginContext(req.GetPluginContext()); err != nil {
			return merr.Status(err), nil
		}
		return node.CompactionV2(ctx, req)
	case taskcommon.Index:
		req := &workerpb.CreateJobRequest{}
		if err := proto.Unmarshal(request.GetPayload(), req); err != nil {
			return merr.Status(err), nil
		}
		return node.createIndexTask(ctx, req)
	case taskcommon.Stats:
		req := &workerpb.CreateStatsRequest{}
		if err := proto.Unmarshal(request.GetPayload(), req); err != nil {
			return merr.Status(err), nil
		}
		if _, err := hookutil.CreateLocalEZByPluginContext(req.GetPluginContext()); err != nil {
			return merr.Status(err), nil
		}
		return node.createStatsTask(ctx, req)
	case taskcommon.Analyze:
		req := &workerpb.AnalyzeRequest{}
		if err := proto.Unmarshal(request.GetPayload(), req); err != nil {
			return merr.Status(err), nil
		}
		if _, err := hookutil.CreateLocalEZByPluginContext(req.GetPluginContext()); err != nil {
			return merr.Status(err), nil
		}
		return node.createAnalyzeTask(ctx, req)
	default:
		err := fmt.Errorf("unrecognized task type '%s', properties=%v", taskType, request.GetProperties())
		log.Ctx(ctx).Warn("CreateTask failed", zap.Error(err))
		return merr.Status(err), nil
	}
}

type ResponseWithStatus interface {
	GetStatus() *commonpb.Status
}

func wrapQueryTaskResult[Resp proto.Message](resp Resp, properties taskcommon.Properties) (*workerpb.QueryTaskResponse, error) {
	payload, err := proto.Marshal(resp)
	if err != nil {
		return &workerpb.QueryTaskResponse{Status: merr.Status(err)}, nil
	}
	statusResp, ok := any(resp).(ResponseWithStatus)
	if !ok {
		return &workerpb.QueryTaskResponse{Status: merr.Status(fmt.Errorf("response does not implement GetStatus"))}, nil
	}
	return &workerpb.QueryTaskResponse{
		Status:     statusResp.GetStatus(),
		Payload:    payload,
		Properties: properties,
	}, nil
}

// QueryTask queries task status
func (node *DataNode) QueryTask(ctx context.Context, request *workerpb.QueryTaskRequest) (*workerpb.QueryTaskResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &workerpb.QueryTaskResponse{Status: merr.Status(err)}, nil
	}
	reqProperties := taskcommon.NewProperties(request.GetProperties())
	clusterID, err := reqProperties.GetClusterID()
	if err != nil {
		return &workerpb.QueryTaskResponse{Status: merr.Status(err)}, nil
	}
	taskType, err := reqProperties.GetTaskType()
	if err != nil {
		return &workerpb.QueryTaskResponse{Status: merr.Status(err)}, nil
	}
	taskID, err := reqProperties.GetTaskID()
	if err != nil {
		return &workerpb.QueryTaskResponse{Status: merr.Status(err)}, nil
	}
	switch taskType {
	case taskcommon.PreImport:
		resp, err := node.QueryPreImport(ctx, &datapb.QueryPreImportRequest{ClusterID: clusterID, TaskID: taskID})
		if err != nil {
			return nil, err
		}
		resProperties := taskcommon.NewProperties(nil)
		resProperties.AppendTaskState(taskcommon.FromImportState(resp.GetState()))
		resProperties.AppendReason(resp.GetReason())
		return wrapQueryTaskResult(resp, resProperties)
	case taskcommon.Import:
		resp, err := node.QueryImport(ctx, &datapb.QueryImportRequest{ClusterID: clusterID, TaskID: taskID})
		if err != nil {
			return nil, err
		}
		resProperties := taskcommon.NewProperties(nil)
		resProperties.AppendTaskState(taskcommon.FromImportState(resp.GetState()))
		resProperties.AppendReason(resp.GetReason())
		return wrapQueryTaskResult(resp, resProperties)
	case taskcommon.Compaction:
		resp, err := node.GetCompactionState(ctx, &datapb.CompactionStateRequest{PlanID: taskID})
		if err != nil {
			return nil, err
		}
		resProperties := taskcommon.NewProperties(nil)
		if len(resp.GetResults()) > 0 {
			resProperties.AppendTaskState(taskcommon.FromCompactionState(resp.GetResults()[0].GetState()))
		}
		return wrapQueryTaskResult(resp, resProperties)
	case taskcommon.Index:
		resp, err := node.queryIndexTask(ctx, &workerpb.QueryJobsRequest{ClusterID: clusterID, TaskIDs: []int64{taskID}})
		if err != nil {
			return nil, err
		}
		resProperties := taskcommon.NewProperties(nil)
		results := resp.GetIndexJobResults().GetResults()
		if len(results) > 0 {
			resProperties.AppendTaskState(taskcommon.State(results[0].GetState()))
			resProperties.AppendReason(results[0].GetFailReason())
		}
		return wrapQueryTaskResult(resp, resProperties)
	case taskcommon.Stats:
		resp, err := node.queryStatsTask(ctx, &workerpb.QueryJobsRequest{ClusterID: clusterID, TaskIDs: []int64{taskID}})
		if err != nil {
			return nil, err
		}
		resProperties := taskcommon.NewProperties(nil)
		results := resp.GetStatsJobResults().GetResults()
		if len(results) > 0 {
			resProperties.AppendTaskState(results[0].GetState())
			resProperties.AppendReason(results[0].GetFailReason())
		}
		return wrapQueryTaskResult(resp, resProperties)
	case taskcommon.Analyze:
		resp, err := node.queryAnalyzeTask(ctx, &workerpb.QueryJobsRequest{ClusterID: clusterID, TaskIDs: []int64{taskID}})
		if err != nil {
			return nil, err
		}
		resProperties := taskcommon.NewProperties(nil)
		results := resp.GetAnalyzeJobResults().GetResults()
		if len(results) > 0 {
			resProperties.AppendTaskState(results[0].GetState())
			resProperties.AppendReason(results[0].GetFailReason())
		}
		return wrapQueryTaskResult(resp, resProperties)
	default:
		err := fmt.Errorf("unrecognized task type '%s', properties=%v", taskType, request.GetProperties())
		log.Ctx(ctx).Warn("QueryTask failed", zap.Error(err))
		return &workerpb.QueryTaskResponse{
			Status: merr.Status(err),
		}, nil
	}
}

// DropTask deletes specified type of task
func (node *DataNode) DropTask(ctx context.Context, request *workerpb.DropTaskRequest) (*commonpb.Status, error) {
	log.Ctx(ctx).Info("DropTask received", zap.Any("properties", request.GetProperties()))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	properties := taskcommon.NewProperties(request.GetProperties())
	taskType, err := properties.GetTaskType()
	if err != nil {
		return merr.Status(err), nil
	}
	taskID, err := properties.GetTaskID()
	if err != nil {
		return merr.Status(err), nil
	}
	switch taskType {
	case taskcommon.PreImport, taskcommon.Import:
		return node.DropImport(ctx, &datapb.DropImportRequest{TaskID: taskID})
	case taskcommon.Compaction:
		return node.DropCompactionPlan(ctx, &datapb.DropCompactionPlanRequest{PlanID: taskID})
	case taskcommon.Index, taskcommon.Stats, taskcommon.Analyze:
		jobType, err := properties.GetJobType()
		if err != nil {
			return merr.Status(err), nil
		}
		clusterID, err := properties.GetClusterID()
		if err != nil {
			return merr.Status(err), nil
		}
		return node.DropJobsV2(ctx, &workerpb.DropJobsV2Request{
			ClusterID: clusterID,
			TaskIDs:   []int64{taskID},
			JobType:   jobType,
		})
	default:
		err := fmt.Errorf("unrecognized task type '%s', properties=%v", taskType, request.GetProperties())
		log.Ctx(ctx).Warn("DropTask failed", zap.Error(err))
		return merr.Status(err), nil
	}
}

func (node *DataNode) SyncFileResource(ctx context.Context, req *internalpb.SyncFileResourceRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(zap.Uint64("version", req.GetVersion()))
	log.Info("sync file resource", zap.Any("resources", req.Resources))

	if !node.isHealthy() {
		log.Warn("failed to sync file resource, DataNode is not healthy")
		return merr.Status(merr.ErrServiceNotReady), nil
	}

	err := fileresource.Sync(req.GetResources())
	if err != nil {
		return merr.Status(err), nil
	}
	return merr.Success(), nil
}
