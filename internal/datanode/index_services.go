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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datanode/index"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// CreateJob is CreateIndex
func (node *DataNode) CreateJob(ctx context.Context, req *workerpb.CreateJobRequest) (*commonpb.Status, error) {
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		mlog.Warn(context.TODO(), "index node not ready",
			mlog.Err(err),
		)
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()
	mlog.Info(context.TODO(), "DataNode building index ...",
		mlog.Int64("collectionID", req.GetCollectionID()),
		mlog.Int64("partitionID", req.GetPartitionID()),
		mlog.Int64("segmentID", req.GetSegmentID()),
		mlog.Int64("indexID", req.GetIndexID()),
		mlog.String("indexName", req.GetIndexName()),
		mlog.String("indexFilePrefix", req.GetIndexFilePrefix()),
		mlog.Int64("indexVersion", req.GetIndexVersion()),
		mlog.Strings("dataPaths", req.GetDataPaths()),
		mlog.Any("typeParams", req.GetTypeParams()),
		mlog.Any("indexParams", req.GetIndexParams()),
		mlog.Int64("numRows", req.GetNumRows()),
		mlog.Int32("current_index_version", req.GetCurrentIndexVersion()),
		mlog.Any("storepath", req.GetStorePath()),
		mlog.Any("storeversion", req.GetStoreVersion()),
		mlog.Any("dim", req.GetDim()),
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
		// Node-internal dedup of a coordinator-dispatched build task: a duplicate
		// is a scheduling race, not the user re-creating an index.
		err := merr.WrapErrServiceInternalMsg("building index task existed, index=%s, buildID=%d", req.GetIndexName(), req.GetBuildID())
		mlog.Warn(context.TODO(), "duplicated index build task", mlog.Err(err))
		metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(paramtable.GetStringNodeID(), metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}
	cm, err := node.storageFactory.NewChunkManager(node.ctx, req.GetStorageConfig())
	if err != nil {
		mlog.Error(context.TODO(), "create chunk manager failed", mlog.String("bucket", req.GetStorageConfig().GetBucketName()),
			mlog.String("accessKey", req.GetStorageConfig().GetAccessKeyID()),
			mlog.Err(err),
		)
		node.taskManager.DeleteIndexTaskInfos(ctx, []index.Key{{ClusterID: req.GetClusterID(), TaskID: req.GetBuildID()}})
		metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(paramtable.GetStringNodeID(), metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}
	pluginContext, err := hookutil.GetCPluginContext(req.GetPluginContext(), req.GetCollectionID())
	if err != nil {
		return merr.Status(err), nil
	}
	task := index.NewIndexBuildTask(taskCtx, taskCancel, req, cm, node.taskManager, pluginContext, node.localFiles)
	ret := merr.Success()
	if err := node.taskScheduler.TaskQueue.Enqueue(task); err != nil {
		mlog.Warn(context.TODO(), "DataNode failed to schedule",
			mlog.Err(err))
		ret = merr.Status(err)
		metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.FailLabel).Inc()
		return ret, nil
	}
	metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(paramtable.GetStringNodeID(), metrics.SuccessLabel).Inc()
	mlog.Info(context.TODO(), "DataNode successfully scheduled",
		mlog.String("indexName", req.GetIndexName()))
	return ret, nil
}

func (node *DataNode) QueryJobs(ctx context.Context, req *workerpb.QueryJobsRequest) (*workerpb.QueryJobsResponse, error) {
	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		mlog.Warn(context.TODO(), "index node not ready", mlog.Err(err))
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
			ret.IndexInfos[i].CurrentScalarIndexVersion = info.CurrentScalarIndexVersion
			mlog.RatedDebug(context.TODO(), rate.Limit(5), "querying index build task",
				mlog.Int64("indexBuildID", buildID),
				mlog.String("state", info.State.String()),
				mlog.String("reason", info.FailReason),
			)
		}
	}
	return ret, nil
}

func (node *DataNode) DropJobs(ctx context.Context, req *workerpb.DropJobsRequest) (*commonpb.Status, error) {
	mlog.Info(ctx, "drop index build jobs",
		mlog.String("clusterID", req.ClusterID),
		mlog.Int64s("indexBuildIDs", req.GetTaskIDs()),
	)
	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		mlog.Warn(ctx, "index node not ready", mlog.Err(err), mlog.String("clusterID", req.ClusterID))
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
	mlog.Info(ctx, "drop index build jobs success", mlog.String("clusterID", req.GetClusterID()),
		mlog.Int64s("indexBuildIDs", req.GetTaskIDs()))
	return merr.Success(), nil
}

// GetJobStats should be GetSlots
func (node *DataNode) GetJobStats(ctx context.Context, req *workerpb.GetJobStatsRequest) (*workerpb.GetJobStatsResponse, error) {
	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		mlog.Warn(ctx, "index node not ready", mlog.Err(err))
		return &workerpb.GetJobStatsResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	var (
		totalSlots     = index.CalculateNodeSlots()
		indexStatsUsed = node.taskScheduler.TaskQueue.GetUsingSlot()
		compactionUsed = node.compactionExecutor.Slots()
		importUsed     = node.importScheduler.Slots()
	)

	availableSlots := totalSlots - indexStatsUsed - compactionUsed - importUsed
	if availableSlots < 0 {
		availableSlots = 0
	}

	mlog.Info(ctx, "query slots done",
		mlog.Int64("totalSlots", totalSlots),
		mlog.Int64("availableSlots", availableSlots),
		mlog.Int64("indexStatsUsed", indexStatsUsed),
		mlog.Int64("compactionUsed", compactionUsed),
		mlog.Int64("importUsed", importUsed),
	)

	return &workerpb.GetJobStatsResponse{
		Status:         merr.Success(),
		TotalSlots:     totalSlots,
		AvailableSlots: availableSlots,
	}, nil
}

// Deprecated: use CreateTask instead, keep for compatibility
func (node *DataNode) CreateJobV2(ctx context.Context, req *workerpb.CreateJobV2Request) (*commonpb.Status, error) {
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		mlog.Warn(context.TODO(), "index node not ready",
			mlog.Err(err),
		)
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	mlog.Info(context.TODO(), "DataNode receive CreateJob request...")

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
		mlog.Warn(context.TODO(), "DataNode receive unknown type job")
		return merr.Status(merr.WrapErrServiceInternalMsg("DataNode receive unknown type job with TaskID: %d", req.GetTaskID())), nil
	}
}

func (node *DataNode) createIndexTask(ctx context.Context, req *workerpb.CreateJobRequest) (*commonpb.Status, error) {
	mlog.Info(ctx, "DataNode building index ...",
		mlog.String("clusterID", req.GetClusterID()),
		mlog.Int64("taskID", req.GetBuildID()),
		mlog.Int64("collectionID", req.GetCollectionID()),
		mlog.Int64("partitionID", req.GetPartitionID()),
		mlog.Int64("segmentID", req.GetSegmentID()),
		mlog.String("indexFilePrefix", req.GetIndexFilePrefix()),
		mlog.Int64("indexVersion", req.GetIndexVersion()),
		mlog.Strings("dataPaths", req.GetDataPaths()),
		mlog.Any("typeParams", req.GetTypeParams()),
		mlog.Any("indexParams", req.GetIndexParams()),
		mlog.Int64("numRows", req.GetNumRows()),
		mlog.Int32("current_index_version", req.GetCurrentIndexVersion()),
		mlog.String("storePath", req.GetStorePath()),
		mlog.Int64("storeVersion", req.GetStoreVersion()),
		mlog.Int64("dim", req.GetDim()),
		mlog.Int64("fieldID", req.GetFieldID()),
		mlog.String("fieldType", req.GetFieldType().String()),
		mlog.Any("field", req.GetField()),
		mlog.Int64("taskSlot", req.GetTaskSlot()),
		mlog.Int64("lackBinlogRows", req.GetLackBinlogRows()),
	)
	if req.GetTaskSlot() <= 0 {
		mlog.Warn(ctx, "receive index task with invalid slot, set to 64", mlog.Int64("taskSlot", req.GetTaskSlot()))
		req.TaskSlot = 64
	}
	taskCtx, taskCancel := context.WithCancel(node.ctx)
	if oldInfo := node.taskManager.LoadOrStoreIndexTask(req.GetClusterID(), req.GetBuildID(), &index.IndexTaskInfo{
		Cancel:                taskCancel,
		State:                 commonpb.IndexState_InProgress,
		IndexStorePathVersion: req.GetIndexStorePathVersion(),
	}); oldInfo != nil {
		err := merr.WrapErrTaskDuplicate(indexpb.JobType_JobTypeIndexJob.String(),
			fmt.Sprintf("building index task existed with %s-%d", req.GetClusterID(), req.GetBuildID()))
		mlog.Warn(context.TODO(), "duplicated index build task", mlog.Err(err))
		metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(paramtable.GetStringNodeID(), metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}
	cm, err := node.storageFactory.NewChunkManager(node.ctx, req.GetStorageConfig())
	if err != nil {
		mlog.Error(context.TODO(), "create chunk manager failed", mlog.String("bucket", req.GetStorageConfig().GetBucketName()),
			mlog.String("accessKey", req.GetStorageConfig().GetAccessKeyID()),
			mlog.Err(err),
		)
		node.taskManager.DeleteIndexTaskInfos(ctx, []index.Key{{ClusterID: req.GetClusterID(), TaskID: req.GetBuildID()}})
		metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(paramtable.GetStringNodeID(), metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	pluginContext, err := hookutil.GetCPluginContext(req.GetPluginContext(), req.GetCollectionID())
	if err != nil {
		return merr.Status(err), nil
	}

	task := index.NewIndexBuildTask(taskCtx, taskCancel, req, cm, node.taskManager, pluginContext, node.localFiles)
	ret := merr.Success()
	if err := node.taskScheduler.TaskQueue.Enqueue(task); err != nil {
		mlog.Warn(context.TODO(), "DataNode failed to schedule",
			mlog.Err(err))
		ret = merr.Status(err)
		metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.FailLabel).Inc()
		return ret, nil
	}
	metrics.DataNodeBuildIndexTaskCounter.WithLabelValues(paramtable.GetStringNodeID(), metrics.SuccessLabel).Inc()
	mlog.Info(context.TODO(), "DataNode index job enqueued successfully",
		mlog.String("indexName", req.GetIndexName()))
	return ret, nil
}

func (node *DataNode) createAnalyzeTask(ctx context.Context, req *workerpb.AnalyzeRequest) (*commonpb.Status, error) {
	mlog.Info(ctx, "receive analyze job",
		mlog.String("clusterID", req.GetClusterID()),
		mlog.Int64("taskID", req.GetTaskID()),
		mlog.Int64("collectionID", req.GetCollectionID()),
		mlog.Int64("partitionID", req.GetPartitionID()),
		mlog.Int64("fieldID", req.GetFieldID()),
		mlog.String("fieldName", req.GetFieldName()),
		mlog.String("dataType", req.GetFieldType().String()),
		mlog.Int64("version", req.GetVersion()),
		mlog.Int64("dim", req.GetDim()),
		mlog.Float64("trainSizeRatio", req.GetMaxTrainSizeRatio()),
		mlog.Int64("numClusters", req.GetNumClusters()),
		mlog.Int64("taskSlot", req.GetTaskSlot()),
	)

	if req.GetTaskSlot() <= 0 {
		mlog.Warn(ctx, "receive analyze task with invalid slot, set to 65535", mlog.Int64("taskSlot", req.GetTaskSlot()))
		req.TaskSlot = 65535
	}

	taskCtx, taskCancel := context.WithCancel(node.ctx)
	if oldInfo := node.taskManager.LoadOrStoreAnalyzeTask(req.GetClusterID(), req.GetTaskID(), &index.AnalyzeTaskInfo{
		Cancel: taskCancel,
		State:  indexpb.JobState_JobStateInProgress,
	}); oldInfo != nil {
		err := merr.WrapErrTaskDuplicate(indexpb.JobType_JobTypeAnalyzeJob.String(),
			fmt.Sprintf("analyze task already existed with %s-%d", req.GetClusterID(), req.GetTaskID()))
		mlog.Warn(context.TODO(), "duplicated analyze task", mlog.Err(err))
		return merr.Status(err), nil
	}
	t := index.NewAnalyzeTask(taskCtx, taskCancel, req, node.taskManager)
	ret := merr.Success()
	if err := node.taskScheduler.TaskQueue.Enqueue(t); err != nil {
		mlog.Warn(context.TODO(), "DataNode failed to schedule", mlog.Err(err))
		ret = merr.Status(err)
		return ret, nil
	}
	mlog.Info(context.TODO(), "DataNode analyze job enqueued successfully")
	return ret, nil
}

func (node *DataNode) createStatsTask(ctx context.Context, req *workerpb.CreateStatsRequest) (*commonpb.Status, error) {
	mlog.Info(ctx, "receive stats job",
		mlog.String("clusterID", req.GetClusterID()),
		mlog.Int64("taskID", req.GetTaskID()),
		mlog.Int64("collectionID", req.GetCollectionID()),
		mlog.Int64("partitionID", req.GetPartitionID()),
		mlog.Int64("segmentID", req.GetSegmentID()),
		mlog.Int64("numRows", req.GetNumRows()),
		mlog.Int64("targetSegmentID", req.GetTargetSegmentID()),
		mlog.String("subJobType", req.GetSubJobType().String()),
		mlog.Int64("startLogID", req.GetStartLogID()),
		mlog.Int64("endLogID", req.GetEndLogID()),
		mlog.Int64("taskSlot", req.GetTaskSlot()),
	)

	if req.GetTaskSlot() <= 0 {
		mlog.Warn(ctx, "receive stats task with invalid slot, set to 64", mlog.Int64("taskSlot", req.GetTaskSlot()))
		req.TaskSlot = 64
	}

	taskCtx, taskCancel := context.WithCancel(node.ctx)
	if oldInfo := node.taskManager.LoadOrStoreStatsTask(req.GetClusterID(), req.GetTaskID(), &index.StatsTaskInfo{
		Cancel: taskCancel,
		State:  indexpb.JobState_JobStateInProgress,
	}); oldInfo != nil {
		err := merr.WrapErrTaskDuplicate(indexpb.JobType_JobTypeStatsJob.String(),
			fmt.Sprintf("stats task already existed with %s-%d", req.GetClusterID(), req.GetTaskID()))
		mlog.Warn(context.TODO(), "duplicated stats task", mlog.Err(err))
		return merr.Status(err), nil
	}
	cm, err := node.storageFactory.NewChunkManager(node.ctx, req.GetStorageConfig())
	if err != nil {
		mlog.Error(context.TODO(), "create chunk manager failed", mlog.String("bucket", req.GetStorageConfig().GetBucketName()),
			mlog.String("accessKey", req.GetStorageConfig().GetAccessKeyID()),
			mlog.Err(err),
		)
		node.taskManager.DeleteStatsTaskInfos(ctx, []index.Key{{ClusterID: req.GetClusterID(), TaskID: req.GetTaskID()}})
		return merr.Status(err), nil
	}

	t := index.NewStatsTask(taskCtx, taskCancel, req, node.taskManager, cm, node.localFiles)
	ret := merr.Success()
	if err := node.taskScheduler.TaskQueue.Enqueue(t); err != nil {
		mlog.Warn(context.TODO(), "DataNode failed to schedule", mlog.Err(err))
		ret = merr.Status(err)
		return ret, nil
	}
	mlog.Info(context.TODO(), "DataNode stats job enqueued successfully")
	return ret, nil
}

// Deprecated: use QueryTask instead, keep for compatibility
func (node *DataNode) QueryJobsV2(ctx context.Context, req *workerpb.QueryJobsV2Request) (*workerpb.QueryJobsV2Response, error) {
	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		mlog.Warn(context.TODO(), "DataNode not ready", mlog.Err(err))
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
		mlog.Warn(context.TODO(), "DataNode receive querying unknown type jobs")
		return &workerpb.QueryJobsV2Response{
			Status: merr.Status(merr.WrapErrServiceInternalMsg("DataNode receive querying unknown type jobs")),
		}, nil
	}
}

func (node *DataNode) queryIndexTask(ctx context.Context, req *workerpb.QueryJobsRequest) (*workerpb.QueryJobsV2Response, error) {
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
	mlog.Debug(context.TODO(), "query index jobs result success", mlog.Any("results", results))
	if len(results) == 0 {
		return &workerpb.QueryJobsV2Response{
			Status: merr.Status(merr.WrapErrServiceInternalMsg("tasks '%v' not found", req.GetTaskIDs())),
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
	results := make([]*workerpb.StatsResult, 0, len(req.GetTaskIDs()))
	for _, taskID := range req.GetTaskIDs() {
		info := node.taskManager.GetStatsTaskInfo(req.GetClusterID(), taskID)
		if info != nil {
			results = append(results, info.ToStatsResult(taskID))
		}
	}
	mlog.Debug(context.TODO(), "query stats job result success", mlog.Any("results", results))
	if len(results) == 0 {
		return &workerpb.QueryJobsV2Response{
			Status: merr.Status(merr.WrapErrServiceInternalMsg("tasks '%v' not found", req.GetTaskIDs())),
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
	mlog.Debug(context.TODO(), "query analyze jobs result success", mlog.Any("results", results))
	if len(results) == 0 {
		return &workerpb.QueryJobsV2Response{
			Status: merr.Status(merr.WrapErrServiceInternalMsg("tasks '%v' not found", req.GetTaskIDs())),
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
	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		mlog.Warn(context.TODO(), "DataNode not ready", mlog.Err(err))
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	mlog.Info(context.TODO(), "DataNode receive DropJobs request")

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
		mlog.Info(context.TODO(), "drop index build jobs success")
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
		mlog.Info(context.TODO(), "drop analyze jobs success")
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
		mlog.Info(context.TODO(), "drop stats jobs success")
		return merr.Success(), nil
	default:
		mlog.Warn(context.TODO(), "DataNode receive dropping unknown type jobs")
		return merr.Status(merr.WrapErrServiceInternalMsg("DataNode receive dropping unknown type jobs")), nil
	}
}
