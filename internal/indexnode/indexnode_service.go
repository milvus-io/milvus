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

package indexnode

import (
	"context"
	"fmt"
	"strconv"

	"github.com/golang/protobuf/proto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func (i *IndexNode) CreateJob(ctx context.Context, req *indexpb.CreateJobRequest) (*commonpb.Status, error) {
	if !i.lifetime.Add(commonpbutil.IsHealthy) {
		stateCode := i.lifetime.GetState()
		log.Ctx(ctx).Warn("index node not ready", zap.String("state", stateCode.String()), zap.String("ClusterID", req.ClusterID), zap.Int64("IndexBuildID", req.BuildID))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "state code is not healthy",
		}, nil
	}
	defer i.lifetime.Done()
	log.Ctx(ctx).Info("IndexNode building index ...",
		zap.String("ClusterID", req.ClusterID),
		zap.Int64("IndexBuildID", req.BuildID),
		zap.Int64("IndexID", req.IndexID),
		zap.String("IndexName", req.IndexName),
		zap.String("IndexFilePrefix", req.IndexFilePrefix),
		zap.Int64("IndexVersion", req.IndexVersion),
		zap.Strings("DataPaths", req.DataPaths),
		zap.Any("TypeParams", req.TypeParams),
		zap.Any("IndexParams", req.IndexParams),
		zap.Int64("num_rows", req.GetNumRows()))
	ctx, sp := otel.Tracer(typeutil.IndexNodeRole).Start(ctx, "IndexNode-CreateIndex", trace.WithAttributes(
		attribute.Int64("IndexBuildID", req.BuildID),
		attribute.String("ClusterID", req.ClusterID),
	))
	defer sp.End()
	metrics.IndexNodeBuildIndexTaskCounter.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.TotalLabel).Inc()

	taskCtx, taskCancel := context.WithCancel(i.loopCtx)
	if oldInfo := i.loadOrStoreTask(req.ClusterID, req.BuildID, &taskInfo{
		cancel: taskCancel,
		state:  commonpb.IndexState_InProgress}); oldInfo != nil {
		log.Ctx(ctx).Warn("duplicated index build task", zap.String("ClusterID", req.ClusterID), zap.Int64("BuildID", req.BuildID))
		metrics.IndexNodeBuildIndexTaskCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.FailLabel).Inc()
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_BuildIndexError,
			Reason:    "duplicated index build task",
		}, nil
	}
	cm, err := i.storageFactory.NewChunkManager(i.loopCtx, req.StorageConfig)
	if err != nil {
		log.Ctx(ctx).Error("create chunk manager failed", zap.String("Bucket", req.StorageConfig.BucketName),
			zap.String("AccessKey", req.StorageConfig.AccessKeyID),
			zap.String("ClusterID", req.ClusterID), zap.Int64("IndexBuildID", req.BuildID))
		metrics.IndexNodeBuildIndexTaskCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.FailLabel).Inc()
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_BuildIndexError,
			Reason:    "create chunk manager failed",
		}, nil
	}
	task := &indexBuildTask{
		ident:          fmt.Sprintf("%s/%d", req.ClusterID, req.BuildID),
		ctx:            taskCtx,
		cancel:         taskCancel,
		BuildID:        req.BuildID,
		ClusterID:      req.ClusterID,
		node:           i,
		req:            req,
		cm:             cm,
		nodeID:         i.GetNodeID(),
		tr:             timerecord.NewTimeRecorder(fmt.Sprintf("IndexBuildID: %d, ClusterID: %s", req.BuildID, req.ClusterID)),
		serializedSize: 0,
	}
	ret := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}
	if err := i.sched.IndexBuildQueue.Enqueue(task); err != nil {
		log.Ctx(ctx).Warn("IndexNode failed to schedule", zap.Int64("IndexBuildID", req.BuildID), zap.String("ClusterID", req.ClusterID), zap.Error(err))
		ret.ErrorCode = commonpb.ErrorCode_UnexpectedError
		ret.Reason = err.Error()
		metrics.IndexNodeBuildIndexTaskCounter.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.FailLabel).Inc()
		return ret, nil
	}
	metrics.IndexNodeBuildIndexTaskCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SuccessLabel).Inc()
	log.Ctx(ctx).Info("IndexNode successfully scheduled", zap.Int64("IndexBuildID", req.BuildID), zap.String("ClusterID", req.ClusterID), zap.String("indexName", req.IndexName))
	return ret, nil
}

func (i *IndexNode) QueryJobs(ctx context.Context, req *indexpb.QueryJobsRequest) (*indexpb.QueryJobsResponse, error) {
	log := log.Ctx(ctx).With(
		zap.String("ClusterID", req.GetClusterID()),
	).WithRateGroup("in.queryJobs", 1, 60)
	if !i.lifetime.Add(commonpbutil.IsHealthyOrStopping) {
		stateCode := i.lifetime.GetState()
		log.Warn("index node not ready", zap.String("state", stateCode.String()))
		return &indexpb.QueryJobsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "state code is not healthy",
			},
		}, nil
	}
	defer i.lifetime.Done()
	infos := make(map[UniqueID]*taskInfo)
	i.foreachTaskInfo(func(ClusterID string, buildID UniqueID, info *taskInfo) {
		if ClusterID == req.ClusterID {
			infos[buildID] = &taskInfo{
				state:          info.state,
				fileKeys:       common.CloneStringList(info.fileKeys),
				serializedSize: info.serializedSize,
				failReason:     info.failReason,
			}
		}
	})
	ret := &indexpb.QueryJobsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		ClusterID:  req.ClusterID,
		IndexInfos: make([]*indexpb.IndexTaskInfo, 0, len(req.BuildIDs)),
	}
	for i, buildID := range req.BuildIDs {
		ret.IndexInfos = append(ret.IndexInfos, &indexpb.IndexTaskInfo{
			BuildID:        buildID,
			State:          commonpb.IndexState_IndexStateNone,
			IndexFileKeys:  nil,
			SerializedSize: 0,
		})
		if info, ok := infos[buildID]; ok {
			ret.IndexInfos[i].State = info.state
			ret.IndexInfos[i].IndexFileKeys = info.fileKeys
			ret.IndexInfos[i].SerializedSize = info.serializedSize
			ret.IndexInfos[i].FailReason = info.failReason
			log.RatedDebug(5, "querying index build task",
				zap.Int64("IndexBuildID", buildID), zap.String("state", info.state.String()),
				zap.String("fail reason", info.failReason))
		}
	}
	return ret, nil
}

func (i *IndexNode) DropJobs(ctx context.Context, req *indexpb.DropJobsRequest) (*commonpb.Status, error) {
	log.Ctx(ctx).Info("drop index build jobs", zap.String("ClusterID", req.ClusterID), zap.Int64s("IndexBuildIDs", req.BuildIDs))
	if !i.lifetime.Add(commonpbutil.IsHealthyOrStopping) {
		stateCode := i.lifetime.GetState()
		log.Ctx(ctx).Warn("index node not ready", zap.String("state", stateCode.String()), zap.String("ClusterID", req.ClusterID))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "state code is not healthy",
		}, nil
	}
	defer i.lifetime.Done()
	keys := make([]taskKey, 0, len(req.BuildIDs))
	for _, buildID := range req.BuildIDs {
		keys = append(keys, taskKey{ClusterID: req.ClusterID, BuildID: buildID})
	}
	infos := i.deleteTaskInfos(keys)
	for _, info := range infos {
		if info.cancel != nil {
			info.cancel()
		}
	}
	log.Ctx(ctx).Info("drop index build jobs success", zap.String("ClusterID", req.ClusterID),
		zap.Int64s("IndexBuildIDs", req.BuildIDs))
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (i *IndexNode) GetJobStats(ctx context.Context, req *indexpb.GetJobStatsRequest) (*indexpb.GetJobStatsResponse, error) {
	if !i.lifetime.Add(commonpbutil.IsHealthyOrStopping) {
		stateCode := i.lifetime.GetState()
		log.Ctx(ctx).Warn("index node not ready", zap.String("state", stateCode.String()))
		return &indexpb.GetJobStatsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "state code is not healthy",
			},
		}, nil
	}
	defer i.lifetime.Done()
	unissued, active := i.sched.IndexBuildQueue.GetTaskNum()
	jobInfos := make([]*indexpb.JobInfo, 0)
	i.foreachTaskInfo(func(ClusterID string, buildID UniqueID, info *taskInfo) {
		if info.statistic != nil {
			jobInfos = append(jobInfos, proto.Clone(info.statistic).(*indexpb.JobInfo))
		}
	})
	slots := 0
	if i.sched.buildParallel > unissued+active {
		slots = i.sched.buildParallel - unissued - active
	}
	log.Ctx(ctx).Info("Get Index Job Stats", zap.Int("Unissued", unissued), zap.Int("Active", active), zap.Int("Slot", slots))
	return &indexpb.GetJobStatsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		TotalJobNum:      int64(active) + int64(unissued),
		InProgressJobNum: int64(active),
		EnqueueJobNum:    int64(unissued),
		TaskSlots:        int64(slots),
		JobInfos:         jobInfos,
		EnableDisk:       Params.IndexNodeCfg.EnableDisk.GetAsBool(),
	}, nil
}

// GetMetrics gets the metrics info of IndexNode.
// TODO(dragondriver): cache the Metrics and set a retention to the cache
func (i *IndexNode) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if !i.lifetime.Add(commonpbutil.IsHealthyOrStopping) {
		log.Ctx(ctx).Warn("IndexNode.GetMetrics failed",
			zap.Int64("nodeID", paramtable.GetNodeID()),
			zap.String("req", req.Request),
			zap.Error(errIndexNodeIsUnhealthy(paramtable.GetNodeID())))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgIndexNodeIsUnhealthy(paramtable.GetNodeID()),
			},
			Response: "",
		}, nil
	}
	defer i.lifetime.Done()

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Ctx(ctx).Warn("IndexNode.GetMetrics failed to parse metric type",
			zap.Int64("nodeID", paramtable.GetNodeID()),
			zap.String("req", req.Request),
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response: "",
		}, nil
	}

	if metricType == metricsinfo.SystemInfoMetrics {
		metrics, err := getSystemInfoMetrics(ctx, req, i)

		log.Ctx(ctx).RatedDebug(60, "IndexNode.GetMetrics",
			zap.Int64("nodeID", paramtable.GetNodeID()),
			zap.String("req", req.Request),
			zap.String("metric_type", metricType),
			zap.Error(err))

		return metrics, nil
	}

	log.Ctx(ctx).RatedWarn(60, "IndexNode.GetMetrics failed, request metric type is not implemented yet",
		zap.Int64("nodeID", paramtable.GetNodeID()),
		zap.String("req", req.Request),
		zap.String("metric_type", metricType))

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    metricsinfo.MsgUnimplementedMetric,
		},
		Response: "",
	}, nil
}
