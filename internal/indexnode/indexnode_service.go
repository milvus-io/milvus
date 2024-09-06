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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func (i *IndexNode) CreateJob(ctx context.Context, req *workerpb.CreateJobRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.String("clusterID", req.GetClusterID()),
		zap.Int64("indexBuildID", req.GetBuildID()),
	)

	if err := i.lifetime.Add(merr.IsHealthy); err != nil {
		log.Warn("index node not ready",
			zap.Error(err),
		)
		return merr.Status(err), nil
	}
	defer i.lifetime.Done()
	log.Info("IndexNode building index ...",
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64("partitionID", req.GetPartitionID()),
		zap.Int64("segmentID", req.GetSegmentID()),
		zap.Int64("indexID", req.GetIndexID()),
		zap.String("indexName", req.GetIndexName()),
		zap.String("indexFilePrefix", req.GetIndexFilePrefix()),
		zap.Int64("indexVersion", req.GetIndexVersion()),
		zap.Strings("dataPaths", req.GetDataPaths()),
		zap.Any("typeParams", req.GetTypeParams()),
		zap.Any("indexParams", req.GetIndexParams()),
		zap.Int64("numRows", req.GetNumRows()),
		zap.Int32("current_index_version", req.GetCurrentIndexVersion()),
		zap.Any("storepath", req.GetStorePath()),
		zap.Any("storeversion", req.GetStoreVersion()),
		zap.Any("indexstorepath", req.GetIndexStorePath()),
		zap.Any("dim", req.GetDim()),
	)
	ctx, sp := otel.Tracer(typeutil.IndexNodeRole).Start(ctx, "IndexNode-CreateIndex", trace.WithAttributes(
		attribute.Int64("indexBuildID", req.GetBuildID()),
		attribute.String("clusterID", req.GetClusterID()),
	))
	defer sp.End()
	metrics.IndexNodeBuildIndexTaskCounter.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.TotalLabel).Inc()

	taskCtx, taskCancel := context.WithCancel(i.loopCtx)
	if oldInfo := i.loadOrStoreIndexTask(req.GetClusterID(), req.GetBuildID(), &indexTaskInfo{
		cancel: taskCancel,
		state:  commonpb.IndexState_InProgress,
	}); oldInfo != nil {
		err := merr.WrapErrIndexDuplicate(req.GetIndexName(), "building index task existed")
		log.Warn("duplicated index build task", zap.Error(err))
		metrics.IndexNodeBuildIndexTaskCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}
	cm, err := i.storageFactory.NewChunkManager(i.loopCtx, req.GetStorageConfig())
	if err != nil {
		log.Error("create chunk manager failed", zap.String("bucket", req.GetStorageConfig().GetBucketName()),
			zap.String("accessKey", req.GetStorageConfig().GetAccessKeyID()),
			zap.Error(err),
		)
		i.deleteIndexTaskInfos(ctx, []taskKey{{ClusterID: req.GetClusterID(), TaskID: req.GetBuildID()}})
		metrics.IndexNodeBuildIndexTaskCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}
	task := newIndexBuildTask(taskCtx, taskCancel, req, cm, i)
	ret := merr.Success()
	if err := i.sched.TaskQueue.Enqueue(task); err != nil {
		log.Warn("IndexNode failed to schedule",
			zap.Error(err))
		ret = merr.Status(err)
		metrics.IndexNodeBuildIndexTaskCounter.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.FailLabel).Inc()
		return ret, nil
	}
	metrics.IndexNodeBuildIndexTaskCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SuccessLabel).Inc()
	log.Info("IndexNode successfully scheduled",
		zap.String("indexName", req.GetIndexName()))
	return ret, nil
}

func (i *IndexNode) QueryJobs(ctx context.Context, req *workerpb.QueryJobsRequest) (*workerpb.QueryJobsResponse, error) {
	log := log.Ctx(ctx).With(
		zap.String("clusterID", req.GetClusterID()),
	).WithRateGroup("in.queryJobs", 1, 60)
	if err := i.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		log.Warn("index node not ready", zap.Error(err))
		return &workerpb.QueryJobsResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer i.lifetime.Done()
	infos := make(map[UniqueID]*indexTaskInfo)
	i.foreachIndexTaskInfo(func(ClusterID string, buildID UniqueID, info *indexTaskInfo) {
		if ClusterID == req.GetClusterID() {
			infos[buildID] = &indexTaskInfo{
				state:               info.state,
				fileKeys:            common.CloneStringList(info.fileKeys),
				serializedSize:      info.serializedSize,
				failReason:          info.failReason,
				currentIndexVersion: info.currentIndexVersion,
				indexStoreVersion:   info.indexStoreVersion,
			}
		}
	})
	ret := &workerpb.QueryJobsResponse{
		Status:     merr.Success(),
		ClusterID:  req.GetClusterID(),
		IndexInfos: make([]*workerpb.IndexTaskInfo, 0, len(req.GetBuildIDs())),
	}
	for i, buildID := range req.GetBuildIDs() {
		ret.IndexInfos = append(ret.IndexInfos, &workerpb.IndexTaskInfo{
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
			ret.IndexInfos[i].CurrentIndexVersion = info.currentIndexVersion
			ret.IndexInfos[i].IndexStoreVersion = info.indexStoreVersion
			log.RatedDebug(5, "querying index build task",
				zap.Int64("indexBuildID", buildID),
				zap.String("state", info.state.String()),
				zap.String("reason", info.failReason),
			)
		}
	}
	return ret, nil
}

func (i *IndexNode) DropJobs(ctx context.Context, req *workerpb.DropJobsRequest) (*commonpb.Status, error) {
	log.Ctx(ctx).Info("drop index build jobs",
		zap.String("clusterID", req.ClusterID),
		zap.Int64s("indexBuildIDs", req.BuildIDs),
	)
	if err := i.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		log.Ctx(ctx).Warn("index node not ready", zap.Error(err), zap.String("clusterID", req.ClusterID))
		return merr.Status(err), nil
	}
	defer i.lifetime.Done()
	keys := make([]taskKey, 0, len(req.GetBuildIDs()))
	for _, buildID := range req.GetBuildIDs() {
		keys = append(keys, taskKey{ClusterID: req.GetClusterID(), TaskID: buildID})
	}
	infos := i.deleteIndexTaskInfos(ctx, keys)
	for _, info := range infos {
		if info.cancel != nil {
			info.cancel()
		}
	}
	log.Ctx(ctx).Info("drop index build jobs success", zap.String("clusterID", req.GetClusterID()),
		zap.Int64s("indexBuildIDs", req.GetBuildIDs()))
	return merr.Success(), nil
}

// GetJobStats should be GetSlots
func (i *IndexNode) GetJobStats(ctx context.Context, req *workerpb.GetJobStatsRequest) (*workerpb.GetJobStatsResponse, error) {
	if err := i.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		log.Ctx(ctx).Warn("index node not ready", zap.Error(err))
		return &workerpb.GetJobStatsResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer i.lifetime.Done()
	unissued, active := i.sched.TaskQueue.GetTaskNum()

	slots := 0
	if i.sched.buildParallel > unissued+active {
		slots = i.sched.buildParallel - unissued - active
	}
	log.Ctx(ctx).Info("Get Index Job Stats",
		zap.Int("unissued", unissued),
		zap.Int("active", active),
		zap.Int("slot", slots),
	)
	return &workerpb.GetJobStatsResponse{
		Status:           merr.Success(),
		TotalJobNum:      int64(active) + int64(unissued),
		InProgressJobNum: int64(active),
		EnqueueJobNum:    int64(unissued),
		TaskSlots:        int64(slots),
		EnableDisk:       Params.IndexNodeCfg.EnableDisk.GetAsBool(),
	}, nil
}

// GetMetrics gets the metrics info of IndexNode.
// TODO(dragondriver): cache the Metrics and set a retention to the cache
func (i *IndexNode) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if err := i.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		log.Ctx(ctx).Warn("IndexNode.GetMetrics failed",
			zap.Int64("nodeID", paramtable.GetNodeID()),
			zap.String("req", req.GetRequest()),
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer i.lifetime.Done()

	metricType, err := metricsinfo.ParseMetricType(req.GetRequest())
	if err != nil {
		log.Ctx(ctx).Warn("IndexNode.GetMetrics failed to parse metric type",
			zap.Int64("nodeID", paramtable.GetNodeID()),
			zap.String("req", req.GetRequest()),
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: merr.Status(err),
		}, nil
	}

	if metricType == metricsinfo.SystemInfoMetrics {
		metrics, err := getSystemInfoMetrics(ctx, req, i)

		log.Ctx(ctx).RatedDebug(60, "IndexNode.GetMetrics",
			zap.Int64("nodeID", paramtable.GetNodeID()),
			zap.String("req", req.GetRequest()),
			zap.String("metricType", metricType),
			zap.Error(err))

		return metrics, nil
	}

	log.Ctx(ctx).RatedWarn(60, "IndexNode.GetMetrics failed, request metric type is not implemented yet",
		zap.Int64("nodeID", paramtable.GetNodeID()),
		zap.String("req", req.GetRequest()),
		zap.String("metricType", metricType))

	return &milvuspb.GetMetricsResponse{
		Status: merr.Status(merr.WrapErrMetricNotFound(metricType)),
	}, nil
}

func (i *IndexNode) CreateJobV2(ctx context.Context, req *workerpb.CreateJobV2Request) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.String("clusterID", req.GetClusterID()), zap.Int64("TaskID", req.GetTaskID()),
		zap.String("jobType", req.GetJobType().String()),
	)

	if err := i.lifetime.Add(merr.IsHealthy); err != nil {
		log.Warn("index node not ready",
			zap.Error(err),
		)
		return merr.Status(err), nil
	}
	defer i.lifetime.Done()

	log.Info("IndexNode receive CreateJob request...")

	switch req.GetJobType() {
	case indexpb.JobType_JobTypeIndexJob:
		indexRequest := req.GetIndexRequest()
		log.Info("IndexNode building index ...",
			zap.Int64("collectionID", indexRequest.CollectionID),
			zap.Int64("partitionID", indexRequest.PartitionID),
			zap.Int64("segmentID", indexRequest.SegmentID),
			zap.String("indexFilePrefix", indexRequest.GetIndexFilePrefix()),
			zap.Int64("indexVersion", indexRequest.GetIndexVersion()),
			zap.Strings("dataPaths", indexRequest.GetDataPaths()),
			zap.Any("typeParams", indexRequest.GetTypeParams()),
			zap.Any("indexParams", indexRequest.GetIndexParams()),
			zap.Int64("numRows", indexRequest.GetNumRows()),
			zap.Int32("current_index_version", indexRequest.GetCurrentIndexVersion()),
			zap.String("storePath", indexRequest.GetStorePath()),
			zap.Int64("storeVersion", indexRequest.GetStoreVersion()),
			zap.String("indexStorePath", indexRequest.GetIndexStorePath()),
			zap.Int64("dim", indexRequest.GetDim()),
			zap.Int64("fieldID", indexRequest.GetFieldID()),
			zap.String("fieldType", indexRequest.GetFieldType().String()),
			zap.Any("field", indexRequest.GetField()),
		)
		taskCtx, taskCancel := context.WithCancel(i.loopCtx)
		if oldInfo := i.loadOrStoreIndexTask(indexRequest.GetClusterID(), indexRequest.GetBuildID(), &indexTaskInfo{
			cancel: taskCancel,
			state:  commonpb.IndexState_InProgress,
		}); oldInfo != nil {
			err := merr.WrapErrTaskDuplicate(req.GetJobType().String(),
				fmt.Sprintf("building index task existed with %s-%d", req.GetClusterID(), req.GetTaskID()))
			log.Warn("duplicated index build task", zap.Error(err))
			metrics.IndexNodeBuildIndexTaskCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.FailLabel).Inc()
			return merr.Status(err), nil
		}
		cm, err := i.storageFactory.NewChunkManager(i.loopCtx, indexRequest.GetStorageConfig())
		if err != nil {
			log.Error("create chunk manager failed", zap.String("bucket", indexRequest.GetStorageConfig().GetBucketName()),
				zap.String("accessKey", indexRequest.GetStorageConfig().GetAccessKeyID()),
				zap.Error(err),
			)
			i.deleteIndexTaskInfos(ctx, []taskKey{{ClusterID: indexRequest.GetClusterID(), TaskID: indexRequest.GetBuildID()}})
			metrics.IndexNodeBuildIndexTaskCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.FailLabel).Inc()
			return merr.Status(err), nil
		}
		task := newIndexBuildTask(taskCtx, taskCancel, indexRequest, cm, i)
		ret := merr.Success()
		if err := i.sched.TaskQueue.Enqueue(task); err != nil {
			log.Warn("IndexNode failed to schedule",
				zap.Error(err))
			ret = merr.Status(err)
			metrics.IndexNodeBuildIndexTaskCounter.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.FailLabel).Inc()
			return ret, nil
		}
		metrics.IndexNodeBuildIndexTaskCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SuccessLabel).Inc()
		log.Info("IndexNode index job enqueued successfully",
			zap.String("indexName", indexRequest.GetIndexName()))
		return ret, nil
	case indexpb.JobType_JobTypeAnalyzeJob:
		analyzeRequest := req.GetAnalyzeRequest()
		log.Info("receive analyze job", zap.Int64("collectionID", analyzeRequest.GetCollectionID()),
			zap.Int64("partitionID", analyzeRequest.GetPartitionID()),
			zap.Int64("fieldID", analyzeRequest.GetFieldID()),
			zap.String("fieldName", analyzeRequest.GetFieldName()),
			zap.String("dataType", analyzeRequest.GetFieldType().String()),
			zap.Int64("version", analyzeRequest.GetVersion()),
			zap.Int64("dim", analyzeRequest.GetDim()),
			zap.Float64("trainSizeRatio", analyzeRequest.GetMaxTrainSizeRatio()),
			zap.Int64("numClusters", analyzeRequest.GetNumClusters()),
		)
		taskCtx, taskCancel := context.WithCancel(i.loopCtx)
		if oldInfo := i.loadOrStoreAnalyzeTask(analyzeRequest.GetClusterID(), analyzeRequest.GetTaskID(), &analyzeTaskInfo{
			cancel: taskCancel,
			state:  indexpb.JobState_JobStateInProgress,
		}); oldInfo != nil {
			err := merr.WrapErrTaskDuplicate(req.GetJobType().String(),
				fmt.Sprintf("analyze task already existed with %s-%d", req.GetClusterID(), req.GetTaskID()))
			log.Warn("duplicated analyze task", zap.Error(err))
			return merr.Status(err), nil
		}
		t := newAnalyzeTask(taskCtx, taskCancel, analyzeRequest, i)
		ret := merr.Success()
		if err := i.sched.TaskQueue.Enqueue(t); err != nil {
			log.Warn("IndexNode failed to schedule", zap.Error(err))
			ret = merr.Status(err)
			return ret, nil
		}
		log.Info("IndexNode analyze job enqueued successfully")
		return ret, nil
	case indexpb.JobType_JobTypeStatsJob:
		statsRequest := req.GetStatsRequest()
		log.Info("receive stats job", zap.Int64("collectionID", statsRequest.GetCollectionID()),
			zap.Int64("partitionID", statsRequest.GetPartitionID()),
			zap.Int64("segmentID", statsRequest.GetSegmentID()),
			zap.Int64("targetSegmentID", statsRequest.GetTargetSegmentID()),
			zap.Int64("startLogID", statsRequest.GetStartLogID()),
			zap.Int64("endLogID", statsRequest.GetEndLogID()),
		)

		taskCtx, taskCancel := context.WithCancel(i.loopCtx)
		if oldInfo := i.loadOrStoreStatsTask(statsRequest.GetClusterID(), statsRequest.GetTaskID(), &statsTaskInfo{
			cancel: taskCancel,
			state:  indexpb.JobState_JobStateInProgress,
		}); oldInfo != nil {
			err := merr.WrapErrTaskDuplicate(req.GetJobType().String(),
				fmt.Sprintf("stats task already existed with %s-%d", req.GetClusterID(), req.GetTaskID()))
			log.Warn("duplicated stats task", zap.Error(err))
			return merr.Status(err), nil
		}
		cm, err := i.storageFactory.NewChunkManager(i.loopCtx, statsRequest.GetStorageConfig())
		if err != nil {
			log.Error("create chunk manager failed", zap.String("bucket", statsRequest.GetStorageConfig().GetBucketName()),
				zap.String("accessKey", statsRequest.GetStorageConfig().GetAccessKeyID()),
				zap.Error(err),
			)
			i.deleteStatsTaskInfos(ctx, []taskKey{{ClusterID: req.GetClusterID(), TaskID: req.GetTaskID()}})
			return merr.Status(err), nil
		}

		t := newStatsTask(taskCtx, taskCancel, statsRequest, i, io.NewBinlogIO(cm))
		ret := merr.Success()
		if err := i.sched.TaskQueue.Enqueue(t); err != nil {
			log.Warn("IndexNode failed to schedule", zap.Error(err))
			ret = merr.Status(err)
			return ret, nil
		}
		log.Info("IndexNode stats job enqueued successfully")
		return ret, nil

	default:
		log.Warn("IndexNode receive unknown type job")
		return merr.Status(fmt.Errorf("IndexNode receive unknown type job with TaskID: %d", req.GetTaskID())), nil
	}
}

func (i *IndexNode) QueryJobsV2(ctx context.Context, req *workerpb.QueryJobsV2Request) (*workerpb.QueryJobsV2Response, error) {
	log := log.Ctx(ctx).With(
		zap.String("clusterID", req.GetClusterID()), zap.Int64s("taskIDs", req.GetTaskIDs()),
	).WithRateGroup("QueryResult", 1, 60)

	if err := i.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		log.Warn("IndexNode not ready", zap.Error(err))
		return &workerpb.QueryJobsV2Response{
			Status: merr.Status(err),
		}, nil
	}
	defer i.lifetime.Done()

	switch req.GetJobType() {
	case indexpb.JobType_JobTypeIndexJob:
		infos := make(map[UniqueID]*indexTaskInfo)
		i.foreachIndexTaskInfo(func(ClusterID string, buildID UniqueID, info *indexTaskInfo) {
			if ClusterID == req.GetClusterID() {
				infos[buildID] = &indexTaskInfo{
					state:               info.state,
					fileKeys:            common.CloneStringList(info.fileKeys),
					serializedSize:      info.serializedSize,
					failReason:          info.failReason,
					currentIndexVersion: info.currentIndexVersion,
					indexStoreVersion:   info.indexStoreVersion,
				}
			}
		})
		results := make([]*workerpb.IndexTaskInfo, 0, len(req.GetTaskIDs()))
		for i, buildID := range req.GetTaskIDs() {
			results = append(results, &workerpb.IndexTaskInfo{
				BuildID:        buildID,
				State:          commonpb.IndexState_IndexStateNone,
				IndexFileKeys:  nil,
				SerializedSize: 0,
			})
			if info, ok := infos[buildID]; ok {
				results[i].State = info.state
				results[i].IndexFileKeys = info.fileKeys
				results[i].SerializedSize = info.serializedSize
				results[i].FailReason = info.failReason
				results[i].CurrentIndexVersion = info.currentIndexVersion
				results[i].IndexStoreVersion = info.indexStoreVersion
			}
		}
		log.Debug("query index jobs result success", zap.Any("results", results))
		return &workerpb.QueryJobsV2Response{
			Status:    merr.Success(),
			ClusterID: req.GetClusterID(),
			Result: &workerpb.QueryJobsV2Response_IndexJobResults{
				IndexJobResults: &workerpb.IndexJobResults{
					Results: results,
				},
			},
		}, nil
	case indexpb.JobType_JobTypeAnalyzeJob:
		results := make([]*workerpb.AnalyzeResult, 0, len(req.GetTaskIDs()))
		for _, taskID := range req.GetTaskIDs() {
			info := i.getAnalyzeTaskInfo(req.GetClusterID(), taskID)
			if info != nil {
				results = append(results, &workerpb.AnalyzeResult{
					TaskID:        taskID,
					State:         info.state,
					FailReason:    info.failReason,
					CentroidsFile: info.centroidsFile,
				})
			}
		}
		log.Debug("query analyze jobs result success", zap.Any("results", results))
		return &workerpb.QueryJobsV2Response{
			Status:    merr.Success(),
			ClusterID: req.GetClusterID(),
			Result: &workerpb.QueryJobsV2Response_AnalyzeJobResults{
				AnalyzeJobResults: &workerpb.AnalyzeResults{
					Results: results,
				},
			},
		}, nil
	case indexpb.JobType_JobTypeStatsJob:
		results := make([]*workerpb.StatsResult, 0, len(req.GetTaskIDs()))
		for _, taskID := range req.GetTaskIDs() {
			info := i.getStatsTaskInfo(req.GetClusterID(), taskID)
			if info != nil {
				results = append(results, &workerpb.StatsResult{
					TaskID:        taskID,
					State:         info.state,
					FailReason:    info.failReason,
					CollectionID:  info.collID,
					PartitionID:   info.partID,
					SegmentID:     info.segID,
					Channel:       info.insertChannel,
					InsertLogs:    info.insertLogs,
					StatsLogs:     info.statsLogs,
					DeltaLogs:     nil,
					TextStatsLogs: info.textStatsLogs,
					NumRows:       info.numRows,
				})
			}
		}
		log.Debug("query stats job result success", zap.Any("results", results))
		return &workerpb.QueryJobsV2Response{
			Status:    merr.Success(),
			ClusterID: req.GetClusterID(),
			Result: &workerpb.QueryJobsV2Response_StatsJobResults{
				StatsJobResults: &workerpb.StatsResults{
					Results: results,
				},
			},
		}, nil
	default:
		log.Warn("IndexNode receive querying unknown type jobs")
		return &workerpb.QueryJobsV2Response{
			Status: merr.Status(fmt.Errorf("IndexNode receive querying unknown type jobs")),
		}, nil
	}
}

func (i *IndexNode) DropJobsV2(ctx context.Context, req *workerpb.DropJobsV2Request) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(zap.String("clusterID", req.GetClusterID()),
		zap.Int64s("taskIDs", req.GetTaskIDs()),
		zap.String("jobType", req.GetJobType().String()),
	)

	if err := i.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		log.Warn("IndexNode not ready", zap.Error(err))
		return merr.Status(err), nil
	}
	defer i.lifetime.Done()

	log.Info("IndexNode receive DropJobs request")

	switch req.GetJobType() {
	case indexpb.JobType_JobTypeIndexJob:
		keys := make([]taskKey, 0, len(req.GetTaskIDs()))
		for _, buildID := range req.GetTaskIDs() {
			keys = append(keys, taskKey{ClusterID: req.GetClusterID(), TaskID: buildID})
		}
		infos := i.deleteIndexTaskInfos(ctx, keys)
		for _, info := range infos {
			if info.cancel != nil {
				info.cancel()
			}
		}
		log.Info("drop index build jobs success")
		return merr.Success(), nil
	case indexpb.JobType_JobTypeAnalyzeJob:
		keys := make([]taskKey, 0, len(req.GetTaskIDs()))
		for _, taskID := range req.GetTaskIDs() {
			keys = append(keys, taskKey{ClusterID: req.GetClusterID(), TaskID: taskID})
		}
		infos := i.deleteAnalyzeTaskInfos(ctx, keys)
		for _, info := range infos {
			if info.cancel != nil {
				info.cancel()
			}
		}
		log.Info("drop analyze jobs success")
		return merr.Success(), nil
	case indexpb.JobType_JobTypeStatsJob:
		keys := make([]taskKey, 0, len(req.GetTaskIDs()))
		for _, taskID := range req.GetTaskIDs() {
			keys = append(keys, taskKey{ClusterID: req.GetClusterID(), TaskID: taskID})
		}
		infos := i.deleteStatsTaskInfos(ctx, keys)
		for _, info := range infos {
			if info.cancel != nil {
				info.cancel()
			}
		}
		log.Info("drop stats jobs success")
		return merr.Success(), nil
	default:
		log.Warn("IndexNode receive dropping unknown type jobs")
		return merr.Status(fmt.Errorf("IndexNode receive dropping unknown type jobs")), nil
	}
}
