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

package datanode

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datanode/index"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// CreateJob is CreateIndex
func (node *DataNode) CreateJob(ctx context.Context, req *workerpb.CreateJobRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.String("clusterID", req.GetClusterID()),
		zap.Int64("indexBuildID", req.GetBuildID()),
	)

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		log.Warn("index node not ready",
			zap.Error(err),
		)
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()
	log.Info("DataNode building index ...",
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
	ctx, sp := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "DataNode-CreateIndex", trace.WithAttributes(
		attribute.Int64("indexBuildID", req.GetBuildID()),
		attribute.String("clusterID", req.GetClusterID()),
	))
	defer sp.End()
	metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.TotalLabel).Inc()

	taskCtx, taskCancel := context.WithCancel(node.ctx)
	if oldInfo := node.taskManager.LoadOrStoreIndexTask(req.GetClusterID(), req.GetBuildID(), &index.IndexTaskInfo{
		Cancel: taskCancel,
		State:  commonpb.IndexState_InProgress,
	}); oldInfo != nil {
		err := merr.WrapErrIndexDuplicate(req.GetIndexName(), "building index task existed")
		log.Warn("duplicated index build task", zap.Error(err))
		metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}
	cm, err := node.storageFactory.NewChunkManager(node.ctx, req.GetStorageConfig())
	if err != nil {
		log.Error("create chunk manager failed", zap.String("bucket", req.GetStorageConfig().GetBucketName()),
			zap.String("accessKey", req.GetStorageConfig().GetAccessKeyID()),
			zap.Error(err),
		)
		node.taskManager.DeleteIndexTaskInfos(ctx, []index.Key{{ClusterID: req.GetClusterID(), TaskID: req.GetBuildID()}})
		metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}
	task := index.NewIndexBuildTask(taskCtx, taskCancel, req, cm, node.taskManager)
	ret := merr.Success()
	if err := node.taskScheduler.TaskQueue.Enqueue(task); err != nil {
		log.Warn("DataNode failed to schedule",
			zap.Error(err))
		ret = merr.Status(err)
		metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.FailLabel).Inc()
		return ret, nil
	}
	metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SuccessLabel).Inc()
	log.Info("DataNode successfully scheduled",
		zap.String("indexName", req.GetIndexName()))
	return ret, nil
}

func (node *DataNode) QueryJobs(ctx context.Context, req *workerpb.QueryJobsRequest) (*workerpb.QueryJobsResponse, error) {
	log := log.Ctx(ctx).With(
		zap.String("clusterID", req.GetClusterID()),
	).WithRateGroup("in.queryJobs", 1, 60)
	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		log.Warn("index node not ready", zap.Error(err))
		return &workerpb.QueryJobsResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()
	infos := make(map[typeutil.UniqueID]*index.IndexTaskInfo)
	node.taskManager.ForeachIndexTaskInfo(func(ClusterID string, buildID typeutil.UniqueID, info *index.IndexTaskInfo) {
		if ClusterID == req.GetClusterID() {
			infos[buildID] = info.Clone()
		}
	})
	ret := &workerpb.QueryJobsResponse{
		Status:     merr.Success(),
		ClusterID:  req.GetClusterID(),
		IndexInfos: make([]*workerpb.IndexTaskInfo, 0, len(req.GetTaskIDs())),
	}
	for i, buildID := range req.GetTaskIDs() {
		ret.IndexInfos = append(ret.IndexInfos, &workerpb.IndexTaskInfo{
			BuildID:        buildID,
			State:          commonpb.IndexState_IndexStateNone,
			IndexFileKeys:  nil,
			SerializedSize: 0,
		})
		if info, ok := infos[buildID]; ok {
			ret.IndexInfos[i].State = info.State
			ret.IndexInfos[i].IndexFileKeys = info.FileKeys
			ret.IndexInfos[i].SerializedSize = info.SerializedSize
			ret.IndexInfos[i].MemSize = info.MemSize
			ret.IndexInfos[i].FailReason = info.FailReason
			ret.IndexInfos[i].CurrentIndexVersion = info.CurrentIndexVersion
			ret.IndexInfos[i].IndexStoreVersion = info.IndexStoreVersion
			log.RatedDebug(5, "querying index build task",
				zap.Int64("indexBuildID", buildID),
				zap.String("state", info.State.String()),
				zap.String("reason", info.FailReason),
			)
		}
	}
	return ret, nil
}

func (node *DataNode) DropJobs(ctx context.Context, req *workerpb.DropJobsRequest) (*commonpb.Status, error) {
	log.Ctx(ctx).Info("drop index build jobs",
		zap.String("clusterID", req.ClusterID),
		zap.Int64s("indexBuildIDs", req.GetTaskIDs()),
	)
	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		log.Ctx(ctx).Warn("index node not ready", zap.Error(err), zap.String("clusterID", req.ClusterID))
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()
	keys := make([]index.Key, 0, len(req.GetTaskIDs()))
	for _, taskID := range req.GetTaskIDs() {
		keys = append(keys, index.Key{ClusterID: req.GetClusterID(), TaskID: taskID})
	}
	infos := node.taskManager.DeleteIndexTaskInfos(ctx, keys)
	for _, info := range infos {
		if info.Cancel != nil {
			info.Cancel()
		}
	}
	log.Ctx(ctx).Info("drop index build jobs success", zap.String("clusterID", req.GetClusterID()),
		zap.Int64s("indexBuildIDs", req.GetTaskIDs()))
	return merr.Success(), nil
}

// GetJobStats should be GetSlots
func (node *DataNode) GetJobStats(ctx context.Context, req *workerpb.GetJobStatsRequest) (*workerpb.GetJobStatsResponse, error) {
	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		log.Ctx(ctx).Warn("index node not ready", zap.Error(err))
		return &workerpb.GetJobStatsResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

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

	return &workerpb.GetJobStatsResponse{
		Status:         merr.Success(),
		TotalSlots:     node.totalSlot,
		AvailableSlots: availableSlots,
	}, nil
}

// Deprecated: use CreateTask instead, keep for compatibility
func (node *DataNode) CreateJobV2(ctx context.Context, req *workerpb.CreateJobV2Request) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.String("clusterID", req.GetClusterID()), zap.Int64("TaskID", req.GetTaskID()),
		zap.String("jobType", req.GetJobType().String()),
	)

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		log.Warn("index node not ready",
			zap.Error(err),
		)
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	log.Info("DataNode receive CreateJob request...")

	switch req.GetJobType() {
	case indexpb.JobType_JobTypeIndexJob:
		indexRequest := req.GetIndexRequest()
		return node.createIndexTask(ctx, indexRequest)
	case indexpb.JobType_JobTypeAnalyzeJob:
		analyzeRequest := req.GetAnalyzeRequest()
		return node.createAnalyzeTask(ctx, analyzeRequest)
	case indexpb.JobType_JobTypeStatsJob:
		statsRequest := req.GetStatsRequest()
		return node.createStatsTask(ctx, statsRequest)
	default:
		log.Warn("DataNode receive unknown type job")
		return merr.Status(fmt.Errorf("DataNode receive unknown type job with TaskID: %d", req.GetTaskID())), nil
	}
}

func (node *DataNode) createIndexTask(ctx context.Context, req *workerpb.CreateJobRequest) (*commonpb.Status, error) {
	log.Info("DataNode building index ...",
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64("partitionID", req.GetPartitionID()),
		zap.Int64("segmentID", req.GetSegmentID()),
		zap.String("indexFilePrefix", req.GetIndexFilePrefix()),
		zap.Int64("indexVersion", req.GetIndexVersion()),
		zap.Strings("dataPaths", req.GetDataPaths()),
		zap.Any("typeParams", req.GetTypeParams()),
		zap.Any("indexParams", req.GetIndexParams()),
		zap.Int64("numRows", req.GetNumRows()),
		zap.Int32("current_index_version", req.GetCurrentIndexVersion()),
		zap.String("storePath", req.GetStorePath()),
		zap.Int64("storeVersion", req.GetStoreVersion()),
		zap.String("indexStorePath", req.GetIndexStorePath()),
		zap.Int64("dim", req.GetDim()),
		zap.Int64("fieldID", req.GetFieldID()),
		zap.String("fieldType", req.GetFieldType().String()),
		zap.Any("field", req.GetField()),
		zap.Int64("taskSlot", req.GetTaskSlot()),
		zap.Int64("lackBinlogRows", req.GetLackBinlogRows()),
	)
	taskCtx, taskCancel := context.WithCancel(node.ctx)
	if oldInfo := node.taskManager.LoadOrStoreIndexTask(req.GetClusterID(), req.GetBuildID(), &index.IndexTaskInfo{
		Cancel: taskCancel,
		State:  commonpb.IndexState_InProgress,
	}); oldInfo != nil {
		err := merr.WrapErrTaskDuplicate(indexpb.JobType_JobTypeIndexJob.String(),
			fmt.Sprintf("building index task existed with %s-%d", req.GetClusterID(), req.GetBuildID()))
		log.Warn("duplicated index build task", zap.Error(err))
		metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}
	cm, err := node.storageFactory.NewChunkManager(node.ctx, req.GetStorageConfig())
	if err != nil {
		log.Error("create chunk manager failed", zap.String("bucket", req.GetStorageConfig().GetBucketName()),
			zap.String("accessKey", req.GetStorageConfig().GetAccessKeyID()),
			zap.Error(err),
		)
		node.taskManager.DeleteIndexTaskInfos(ctx, []index.Key{{ClusterID: req.GetClusterID(), TaskID: req.GetBuildID()}})
		metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}
	task := index.NewIndexBuildTask(taskCtx, taskCancel, req, cm, node.taskManager)
	ret := merr.Success()
	if err := node.taskScheduler.TaskQueue.Enqueue(task); err != nil {
		log.Warn("DataNode failed to schedule",
			zap.Error(err))
		ret = merr.Status(err)
		metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.FailLabel).Inc()
		return ret, nil
	}
	metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.SuccessLabel).Inc()
	log.Info("DataNode index job enqueued successfully",
		zap.String("indexName", req.GetIndexName()))
	return ret, nil
}

func (node *DataNode) createAnalyzeTask(ctx context.Context, req *workerpb.AnalyzeRequest) (*commonpb.Status, error) {
	log.Info("receive analyze job", zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64("partitionID", req.GetPartitionID()),
		zap.Int64("fieldID", req.GetFieldID()),
		zap.String("fieldName", req.GetFieldName()),
		zap.String("dataType", req.GetFieldType().String()),
		zap.Int64("version", req.GetVersion()),
		zap.Int64("dim", req.GetDim()),
		zap.Float64("trainSizeRatio", req.GetMaxTrainSizeRatio()),
		zap.Int64("numClusters", req.GetNumClusters()),
		zap.Int64("taskSlot", req.GetTaskSlot()),
	)

	taskCtx, taskCancel := context.WithCancel(node.ctx)
	if oldInfo := node.taskManager.LoadOrStoreAnalyzeTask(req.GetClusterID(), req.GetTaskID(), &index.AnalyzeTaskInfo{
		Cancel: taskCancel,
		State:  indexpb.JobState_JobStateInProgress,
	}); oldInfo != nil {
		err := merr.WrapErrTaskDuplicate(indexpb.JobType_JobTypeAnalyzeJob.String(),
			fmt.Sprintf("analyze task already existed with %s-%d", req.GetClusterID(), req.GetTaskID()))
		log.Warn("duplicated analyze task", zap.Error(err))
		return merr.Status(err), nil
	}
	t := index.NewAnalyzeTask(taskCtx, taskCancel, req, node.taskManager)
	ret := merr.Success()
	if err := node.taskScheduler.TaskQueue.Enqueue(t); err != nil {
		log.Warn("DataNode failed to schedule", zap.Error(err))
		ret = merr.Status(err)
		return ret, nil
	}
	log.Info("DataNode analyze job enqueued successfully")
	return ret, nil
}

func (node *DataNode) createStatsTask(ctx context.Context, req *workerpb.CreateStatsRequest) (*commonpb.Status, error) {
	log.Info("receive stats job", zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64("partitionID", req.GetPartitionID()),
		zap.Int64("segmentID", req.GetSegmentID()),
		zap.Int64("numRows", req.GetNumRows()),
		zap.Int64("targetSegmentID", req.GetTargetSegmentID()),
		zap.String("subJobType", req.GetSubJobType().String()),
		zap.Int64("startLogID", req.GetStartLogID()),
		zap.Int64("endLogID", req.GetEndLogID()),
		zap.Int64("taskSlot", req.GetTaskSlot()),
	)

	taskCtx, taskCancel := context.WithCancel(node.ctx)
	if oldInfo := node.taskManager.LoadOrStoreStatsTask(req.GetClusterID(), req.GetTaskID(), &index.StatsTaskInfo{
		Cancel: taskCancel,
		State:  indexpb.JobState_JobStateInProgress,
	}); oldInfo != nil {
		err := merr.WrapErrTaskDuplicate(indexpb.JobType_JobTypeStatsJob.String(),
			fmt.Sprintf("stats task already existed with %s-%d", req.GetClusterID(), req.GetTaskID()))
		log.Warn("duplicated stats task", zap.Error(err))
		return merr.Status(err), nil
	}
	cm, err := node.storageFactory.NewChunkManager(node.ctx, req.GetStorageConfig())
	if err != nil {
		log.Error("create chunk manager failed", zap.String("bucket", req.GetStorageConfig().GetBucketName()),
			zap.String("accessKey", req.GetStorageConfig().GetAccessKeyID()),
			zap.Error(err),
		)
		node.taskManager.DeleteStatsTaskInfos(ctx, []index.Key{{ClusterID: req.GetClusterID(), TaskID: req.GetTaskID()}})
		return merr.Status(err), nil
	}

	t := index.NewStatsTask(taskCtx, taskCancel, req, node.taskManager, io.NewBinlogIO(cm))
	ret := merr.Success()
	if err := node.taskScheduler.TaskQueue.Enqueue(t); err != nil {
		log.Warn("DataNode failed to schedule", zap.Error(err))
		ret = merr.Status(err)
		return ret, nil
	}
	log.Info("DataNode stats job enqueued successfully")
	return ret, nil
}

// Deprecated: use QueryTask instead, keep for compatibility
func (node *DataNode) QueryJobsV2(ctx context.Context, req *workerpb.QueryJobsV2Request) (*workerpb.QueryJobsV2Response, error) {
	log := log.Ctx(ctx).With(
		zap.String("clusterID", req.GetClusterID()), zap.Int64s("taskIDs", req.GetTaskIDs()),
	).WithRateGroup("QueryResult", 1, 60)

	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		log.Warn("DataNode not ready", zap.Error(err))
		return &workerpb.QueryJobsV2Response{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	switch req.GetJobType() {
	case indexpb.JobType_JobTypeIndexJob:
		return node.queryIndexTask(ctx, &workerpb.QueryJobsRequest{
			ClusterID: req.GetClusterID(),
			TaskIDs:   req.GetTaskIDs(),
		})
	case indexpb.JobType_JobTypeAnalyzeJob:
		return node.queryAnalyzeTask(ctx, &workerpb.QueryJobsRequest{
			ClusterID: req.GetClusterID(),
			TaskIDs:   req.GetTaskIDs(),
		})
	case indexpb.JobType_JobTypeStatsJob:
		return node.queryStatsTask(ctx, &workerpb.QueryJobsRequest{
			ClusterID: req.GetClusterID(),
			TaskIDs:   req.GetTaskIDs(),
		})
	default:
		log.Warn("DataNode receive querying unknown type jobs")
		return &workerpb.QueryJobsV2Response{
			Status: merr.Status(errors.New("DataNode receive querying unknown type jobs")),
		}, nil
	}
}

func (node *DataNode) queryIndexTask(ctx context.Context, req *workerpb.QueryJobsRequest) (*workerpb.QueryJobsV2Response, error) {
	log := log.Ctx(ctx).With(
		zap.String("clusterID", req.GetClusterID()), zap.Int64s("taskIDs", req.GetTaskIDs()),
	).WithRateGroup("QueryResult", 1, 60)

	infos := make(map[typeutil.UniqueID]*index.IndexTaskInfo)
	node.taskManager.ForeachIndexTaskInfo(func(ClusterID string, buildID typeutil.UniqueID, info *index.IndexTaskInfo) {
		if ClusterID == req.GetClusterID() {
			infos[buildID] = info.Clone()
		}
	})
	results := make([]*workerpb.IndexTaskInfo, 0, len(req.GetTaskIDs()))
	for _, buildID := range req.GetTaskIDs() {
		if info, ok := infos[buildID]; ok {
			results = append(results, info.ToIndexTaskInfo(buildID))
		}
	}
	log.Debug("query index jobs result success", zap.Any("results", results))
	if len(results) == 0 {
		return &workerpb.QueryJobsV2Response{
			Status: merr.Status(fmt.Errorf("tasks '%v' not found", req.GetTaskIDs())),
		}, nil
	}
	return &workerpb.QueryJobsV2Response{
		Status:    merr.Success(),
		ClusterID: req.GetClusterID(),
		Result: &workerpb.QueryJobsV2Response_IndexJobResults{
			IndexJobResults: &workerpb.IndexJobResults{
				Results: results,
			},
		},
	}, nil
}

func (node *DataNode) queryStatsTask(ctx context.Context, req *workerpb.QueryJobsRequest) (*workerpb.QueryJobsV2Response, error) {
	log := log.Ctx(ctx).With(
		zap.String("clusterID", req.GetClusterID()), zap.Int64s("taskIDs", req.GetTaskIDs()),
	).WithRateGroup("QueryResult", 1, 60)

	results := make([]*workerpb.StatsResult, 0, len(req.GetTaskIDs()))
	for _, taskID := range req.GetTaskIDs() {
		info := node.taskManager.GetStatsTaskInfo(req.GetClusterID(), taskID)
		if info != nil {
			results = append(results, info.ToStatsResult(taskID))
		}
	}
	log.Debug("query stats job result success", zap.Any("results", results))
	if len(results) == 0 {
		return &workerpb.QueryJobsV2Response{
			Status: merr.Status(fmt.Errorf("tasks '%v' not found", req.GetTaskIDs())),
		}, nil
	}
	return &workerpb.QueryJobsV2Response{
		Status:    merr.Success(),
		ClusterID: req.GetClusterID(),
		Result: &workerpb.QueryJobsV2Response_StatsJobResults{
			StatsJobResults: &workerpb.StatsResults{
				Results: results,
			},
		},
	}, nil
}

func (node *DataNode) queryAnalyzeTask(ctx context.Context, req *workerpb.QueryJobsRequest) (*workerpb.QueryJobsV2Response, error) {
	log := log.Ctx(ctx).With(
		zap.String("clusterID", req.GetClusterID()), zap.Int64s("taskIDs", req.GetTaskIDs()),
	).WithRateGroup("QueryResult", 1, 60)

	results := make([]*workerpb.AnalyzeResult, 0, len(req.GetTaskIDs()))
	for _, taskID := range req.GetTaskIDs() {
		info := node.taskManager.GetAnalyzeTaskInfo(req.GetClusterID(), taskID)
		if info != nil {
			results = append(results, &workerpb.AnalyzeResult{
				TaskID:        taskID,
				State:         info.State,
				FailReason:    info.FailReason,
				CentroidsFile: info.CentroidsFile,
			})
		}
	}
	log.Debug("query analyze jobs result success", zap.Any("results", results))
	if len(results) == 0 {
		return &workerpb.QueryJobsV2Response{
			Status: merr.Status(fmt.Errorf("tasks '%v' not found", req.GetTaskIDs())),
		}, nil
	}
	return &workerpb.QueryJobsV2Response{
		Status:    merr.Success(),
		ClusterID: req.GetClusterID(),
		Result: &workerpb.QueryJobsV2Response_AnalyzeJobResults{
			AnalyzeJobResults: &workerpb.AnalyzeResults{
				Results: results,
			},
		},
	}, nil
}

// Deprecated: use DropTask instead, keep for compatibility
func (node *DataNode) DropJobsV2(ctx context.Context, req *workerpb.DropJobsV2Request) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(zap.String("clusterID", req.GetClusterID()),
		zap.Int64s("taskIDs", req.GetTaskIDs()),
		zap.String("jobType", req.GetJobType().String()),
	)

	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		log.Warn("DataNode not ready", zap.Error(err))
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	log.Info("DataNode receive DropJobs request")

	switch req.GetJobType() {
	case indexpb.JobType_JobTypeIndexJob:
		keys := make([]index.Key, 0, len(req.GetTaskIDs()))
		for _, buildID := range req.GetTaskIDs() {
			keys = append(keys, index.Key{ClusterID: req.GetClusterID(), TaskID: buildID})
		}
		infos := node.taskManager.DeleteIndexTaskInfos(ctx, keys)
		for _, info := range infos {
			if info.Cancel != nil {
				info.Cancel()
			}
		}
		log.Info("drop index build jobs success")
		return merr.Success(), nil
	case indexpb.JobType_JobTypeAnalyzeJob:
		keys := make([]index.Key, 0, len(req.GetTaskIDs()))
		for _, taskID := range req.GetTaskIDs() {
			keys = append(keys, index.Key{ClusterID: req.GetClusterID(), TaskID: taskID})
		}
		infos := node.taskManager.DeleteAnalyzeTaskInfos(ctx, keys)
		for _, info := range infos {
			if info.Cancel != nil {
				info.Cancel()
			}
		}
		log.Info("drop analyze jobs success")
		return merr.Success(), nil
	case indexpb.JobType_JobTypeStatsJob:
		keys := make([]index.Key, 0, len(req.GetTaskIDs()))
		for _, taskID := range req.GetTaskIDs() {
			keys = append(keys, index.Key{ClusterID: req.GetClusterID(), TaskID: taskID})
		}
		infos := node.taskManager.DeleteStatsTaskInfos(ctx, keys)
		for _, info := range infos {
			if info.Cancel != nil {
				info.Cancel()
			}
		}
		log.Info("drop stats jobs success")
		return merr.Success(), nil
	default:
		log.Warn("DataNode receive dropping unknown type jobs")
		return merr.Status(errors.New("DataNode receive dropping unknown type jobs")), nil
	}
}
