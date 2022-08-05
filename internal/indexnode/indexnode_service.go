package indexnode

import (
	"context"
	"fmt"
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexnodepb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/logutil"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/trace"
	"go.uber.org/zap"
)

func (i *IndexNode) CreateJob(ctx context.Context, req *indexnodepb.CreateJobRequest) (*commonpb.Status, error) {
	stateCode := i.stateCode.Load().(internalpb.StateCode)
	if stateCode != internalpb.StateCode_Healthy {
		log.Warn("index node not ready", zap.Int32("state", int32(stateCode)), zap.Int64("ClusterID", req.ClusterID), zap.Int64("IndexBuildID", req.BuildID))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "state code is not healthy",
		}, nil
	}
	log.Info("IndexNode building index ...",
		zap.Int64("ClusterID", req.ClusterID),
		zap.Int64("IndexBuildID", req.BuildID),
		zap.Int64("IndexID", req.IndexID),
		zap.String("IndexName", req.IndexName),
		zap.String("IndexFilePrefix", req.IndexFilePrefix),
		zap.Int64("IndexVersion", req.IndexVersion),
		zap.Strings("DataPaths", req.DataPaths),
		zap.Any("TypeParams", req.TypeParams),
		zap.Any("IndexParams", req.IndexParams))
	sp, _ := trace.StartSpanFromContextWithOperationName(ctx, "IndexNode-CreateIndex")
	defer sp.Finish()
	sp.SetTag("IndexBuildID", strconv.FormatInt(req.BuildID, 10))
	sp.SetTag("ClusterID", strconv.FormatInt(req.ClusterID, 10))
	metrics.IndexNodeBuildIndexTaskCounter.WithLabelValues(strconv.FormatInt(Params.IndexNodeCfg.GetNodeID(), 10), metrics.TotalLabel).Inc()
	taskCtx := logutil.WithFields(i.loopCtx, zap.Int64("ClusterID", req.ClusterID), zap.Int64("IndexBuildID", req.BuildID))

	taskCtx, taskCancel := context.WithCancel(taskCtx)
	if oldInfo := i.loadOrStoreTask(req.ClusterID, req.BuildID, &taskInfo{
		cancel: taskCancel,
		state:  commonpb.IndexState_InProgress}); oldInfo != nil {
		log.Warn("duplicated index build task", zap.Int64("ClusterID", req.ClusterID), zap.Int64("BuildID", req.BuildID))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_BuildIndexError,
			Reason:    "duplicated index build task",
		}, nil
	}
	cm, err := i.storageFactory.NewChunkManager(ctx, req.BucketName, req.StorageAccessKey)
	if err != nil {
		log.Error("create chunk manager failed", zap.String("Bucket", req.BucketName), zap.String("AccessKey", req.StorageAccessKey),
			zap.Int64("ClusterID", req.ClusterID), zap.Int64("IndexBuildID", req.BuildID))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_BuildIndexError,
			Reason:    "create chunk manager failed",
		}, nil
	}
	task := &indexBuildTask{
		ident:          fmt.Sprintf("%d/%d", req.ClusterID, req.BuildID),
		ctx:            taskCtx,
		cancel:         taskCancel,
		BuildID:        req.BuildID,
		ClusterID:      req.ClusterID,
		node:           i,
		req:            req,
		cm:             cm,
		nodeID:         i.GetNodeID(),
		tr:             timerecord.NewTimeRecorder(fmt.Sprintf("IndexBuildID: %d, ClusterID: %d", req.BuildID, req.ClusterID)),
		serializedSize: 0,
	}
	ret := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}
	if err := i.sched.Enqueue(task); err != nil {
		log.Warn("IndexNode failed to schedule", zap.Int64("IndexBuildID", req.BuildID), zap.Int64("ClusterID", req.ClusterID), zap.Error(err))
		ret.ErrorCode = commonpb.ErrorCode_UnexpectedError
		ret.Reason = err.Error()
		metrics.IndexNodeBuildIndexTaskCounter.WithLabelValues(strconv.FormatInt(Params.IndexNodeCfg.GetNodeID(), 10), metrics.FailLabel).Inc()
		return ret, nil
	}
	log.Info("IndexNode successfully scheduled", zap.Int64("IndexBuildID", req.BuildID), zap.Int64("ClusterID", req.ClusterID), zap.String("indexName", req.IndexName))
	return ret, nil
}

func (i *IndexNode) QueryJobs(ctx context.Context, req *indexnodepb.QueryJobsRequest) (*indexnodepb.QueryJobsRespond, error) {
	stateCode := i.stateCode.Load().(internalpb.StateCode)
	if stateCode != internalpb.StateCode_Healthy {
		log.Warn("index node not ready", zap.Int32("state", int32(stateCode)), zap.Int64("ClusterID", req.ClusterID))
		return &indexnodepb.QueryJobsRespond{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "state code is not healthy",
			},
		}, nil
	}
	log.Debug("querying index build task", zap.Int64("ClusterID", req.ClusterID), zap.Int64s("IndexBuildIDs", req.BuildIDs))
	infos := make(map[UniqueID]*taskInfo)
	i.foreachTaskInfo(func(clusterID, buildID UniqueID, info *taskInfo) {
		if clusterID == req.ClusterID {
			infos[buildID] = &taskInfo{
				state:      info.state,
				indexfiles: info.indexfiles[:],
			}
		}
	})
	ret := &indexnodepb.QueryJobsRespond{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		ClusterID:  req.ClusterID,
		IndexInfos: make([]*indexnodepb.IndexInfo, 0, len(req.BuildIDs)),
	}
	for i, buildID := range req.BuildIDs {
		ret.IndexInfos = append(ret.IndexInfos, &indexnodepb.IndexInfo{
			BuildID:    buildID,
			State:      commonpb.IndexState_IndexStateNone,
			IndexFiles: nil,
		})
		if info, ok := infos[buildID]; ok {
			ret.IndexInfos[i].State = info.state
			ret.IndexInfos[i].IndexFiles = info.indexfiles
		}
	}
	return ret, nil
}

func (i *IndexNode) DropJobs(ctx context.Context, req *indexnodepb.DropJobsRequest) (*commonpb.Status, error) {
	log.Debug("drop index build jobs", zap.Int64("ClusterID", req.ClusterID), zap.Int64s("IndexBuildIDs", req.BuildIDs))
	stateCode := i.stateCode.Load().(internalpb.StateCode)
	if stateCode != internalpb.StateCode_Healthy {
		log.Warn("index node not ready", zap.Int32("state", int32(stateCode)), zap.Int64("ClusterID", req.ClusterID))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "state code is not healthy",
		}, nil
	}
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
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (i *IndexNode) GetJobStats(ctx context.Context, req *indexnodepb.GetJobStatsRequest) (*indexnodepb.GetJobStatsRespond, error) {
	stateCode := i.stateCode.Load().(internalpb.StateCode)
	if stateCode != internalpb.StateCode_Healthy {
		log.Warn("index node not ready", zap.Int32("state", int32(stateCode)))
		return &indexnodepb.GetJobStatsRespond{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "state code is not healthy",
			},
		}, nil
	}
	pending := i.sched.GetPendingJob()
	jobInfos := make([]*indexnodepb.JobInfo, 0)
	i.foreachTaskInfo(func(clusterID, buildID UniqueID, info *taskInfo) {
		if info.statistic != nil {
			jobInfos = append(jobInfos, proto.Clone(info.statistic).(*indexnodepb.JobInfo))
		}
	})
	return &indexnodepb.GetJobStatsRespond{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		PendingJobs: int64(pending),
		JobInfos:    jobInfos,
	}, nil
}

// GetMetrics gets the metrics info of IndexNode.
// TODO(dragondriver): cache the Metrics and set a retention to the cache
func (i *IndexNode) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if !i.isHealthy() {
		log.Warn("IndexNode.GetMetrics failed",
			zap.Int64("node_id", Params.IndexNodeCfg.GetNodeID()),
			zap.String("req", req.Request),
			zap.Error(errIndexNodeIsUnhealthy(Params.IndexNodeCfg.GetNodeID())))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgIndexNodeIsUnhealthy(Params.IndexNodeCfg.GetNodeID()),
			},
			Response: "",
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Warn("IndexNode.GetMetrics failed to parse metric type",
			zap.Int64("node_id", Params.IndexNodeCfg.GetNodeID()),
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

		log.Debug("IndexNode.GetMetrics",
			zap.Int64("node_id", Params.IndexNodeCfg.GetNodeID()),
			zap.String("req", req.Request),
			zap.String("metric_type", metricType),
			zap.Error(err))

		return metrics, nil
	}

	log.Warn("IndexNode.GetMetrics failed, request metric type is not implemented yet",
		zap.Int64("node_id", Params.IndexNodeCfg.GetNodeID()),
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
