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
	"math"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ Task = (*analyzeTask)(nil)

type analyzeTask struct {
	taskID   int64
	nodeID   int64
	taskInfo *indexpb.AnalyzeResult

	queueTime time.Time
	startTime time.Time
	endTime   time.Time

	req *indexpb.AnalyzeRequest
}

func (at *analyzeTask) GetTaskID() int64 {
	return at.taskID
}

func (at *analyzeTask) GetNodeID() int64 {
	return at.nodeID
}

func (at *analyzeTask) ResetNodeID() {
	at.nodeID = 0
}

func (at *analyzeTask) SetQueueTime(t time.Time) {
	at.queueTime = t
}

func (at *analyzeTask) GetQueueTime() time.Time {
	return at.queueTime
}

func (at *analyzeTask) SetStartTime(t time.Time) {
	at.startTime = t
}

func (at *analyzeTask) GetStartTime() time.Time {
	return at.startTime
}

func (at *analyzeTask) SetEndTime(t time.Time) {
	at.endTime = t
}

func (at *analyzeTask) GetEndTime() time.Time {
	return at.endTime
}

func (at *analyzeTask) GetTaskType() string {
	return indexpb.JobType_JobTypeIndexJob.String()
}

func (at *analyzeTask) CheckTaskHealthy(mt *meta) bool {
	t := mt.analyzeMeta.GetTask(at.GetTaskID())
	return t != nil
}

func (at *analyzeTask) SetState(state indexpb.JobState, failReason string) {
	at.taskInfo.State = state
	at.taskInfo.FailReason = failReason
}

func (at *analyzeTask) GetState() indexpb.JobState {
	return at.taskInfo.GetState()
}

func (at *analyzeTask) GetFailReason() string {
	return at.taskInfo.GetFailReason()
}

func (at *analyzeTask) UpdateVersion(ctx context.Context, nodeID int64, meta *meta) error {
	if err := meta.analyzeMeta.UpdateVersion(at.GetTaskID(), nodeID); err != nil {
		return err
	}
	at.nodeID = nodeID
	return nil
}

func (at *analyzeTask) UpdateMetaBuildingState(meta *meta) error {
	if err := meta.analyzeMeta.BuildingTask(at.GetTaskID()); err != nil {
		return err
	}
	return nil
}

func (at *analyzeTask) PreCheck(ctx context.Context, dependency *taskScheduler) bool {
	t := dependency.meta.analyzeMeta.GetTask(at.GetTaskID())
	if t == nil {
		log.Ctx(ctx).Info("task is nil, delete it", zap.Int64("taskID", at.GetTaskID()))
		at.SetState(indexpb.JobState_JobStateNone, "analyze task is nil")
		return true
	}

	var storageConfig *indexpb.StorageConfig
	if Params.CommonCfg.StorageType.GetValue() == "local" {
		storageConfig = &indexpb.StorageConfig{
			RootPath:    Params.LocalStorageCfg.Path.GetValue(),
			StorageType: Params.CommonCfg.StorageType.GetValue(),
		}
	} else {
		storageConfig = &indexpb.StorageConfig{
			Address:          Params.MinioCfg.Address.GetValue(),
			AccessKeyID:      Params.MinioCfg.AccessKeyID.GetValue(),
			SecretAccessKey:  Params.MinioCfg.SecretAccessKey.GetValue(),
			UseSSL:           Params.MinioCfg.UseSSL.GetAsBool(),
			BucketName:       Params.MinioCfg.BucketName.GetValue(),
			RootPath:         Params.MinioCfg.RootPath.GetValue(),
			UseIAM:           Params.MinioCfg.UseIAM.GetAsBool(),
			IAMEndpoint:      Params.MinioCfg.IAMEndpoint.GetValue(),
			StorageType:      Params.CommonCfg.StorageType.GetValue(),
			Region:           Params.MinioCfg.Region.GetValue(),
			UseVirtualHost:   Params.MinioCfg.UseVirtualHost.GetAsBool(),
			CloudProvider:    Params.MinioCfg.CloudProvider.GetValue(),
			RequestTimeoutMs: Params.MinioCfg.RequestTimeoutMs.GetAsInt64(),
		}
	}
	at.req = &indexpb.AnalyzeRequest{
		ClusterID:     Params.CommonCfg.ClusterPrefix.GetValue(),
		TaskID:        at.GetTaskID(),
		CollectionID:  t.CollectionID,
		PartitionID:   t.PartitionID,
		FieldID:       t.FieldID,
		FieldName:     t.FieldName,
		FieldType:     t.FieldType,
		Dim:           t.Dim,
		SegmentStats:  make(map[int64]*indexpb.SegmentStats),
		Version:       t.Version + 1,
		StorageConfig: storageConfig,
	}

	// When data analyze occurs, segments must not be discarded. Such as compaction, GC, etc.
	segments := dependency.meta.SelectSegments(SegmentFilterFunc(func(info *SegmentInfo) bool {
		return isSegmentHealthy(info) && slices.Contains(t.SegmentIDs, info.ID)
	}))
	segmentsMap := lo.SliceToMap(segments, func(t *SegmentInfo) (int64, *SegmentInfo) {
		return t.ID, t
	})

	totalSegmentsRows := int64(0)
	for _, segID := range t.SegmentIDs {
		info := segmentsMap[segID]
		if info == nil {
			log.Ctx(ctx).Warn("analyze stats task is processing, but segment is nil, delete the task",
				zap.Int64("taskID", at.GetTaskID()), zap.Int64("segmentID", segID))
			at.SetState(indexpb.JobState_JobStateFailed, fmt.Sprintf("segmentInfo with ID: %d is nil", segID))
			return true
		}

		totalSegmentsRows += info.GetNumOfRows()
		// get binlogIDs
		binlogIDs := getBinLogIDs(info, t.FieldID)
		at.req.SegmentStats[segID] = &indexpb.SegmentStats{
			ID:      segID,
			NumRows: info.GetNumOfRows(),
			LogIDs:  binlogIDs,
		}
	}

	collInfo, err := dependency.handler.GetCollection(ctx, segments[0].GetCollectionID())
	if err != nil {
		log.Ctx(ctx).Info("analyze task get collection info failed", zap.Int64("collectionID",
			segments[0].GetCollectionID()), zap.Error(err))
		at.SetState(indexpb.JobState_JobStateInit, err.Error())
		return true
	}

	schema := collInfo.Schema
	var field *schemapb.FieldSchema

	for _, f := range schema.Fields {
		if f.FieldID == t.FieldID {
			field = f
			break
		}
	}
	dim, err := storage.GetDimFromParams(field.TypeParams)
	if err != nil {
		at.SetState(indexpb.JobState_JobStateInit, err.Error())
		return true
	}
	at.req.Dim = int64(dim)

	totalSegmentsRawDataSize := float64(totalSegmentsRows) * float64(dim) * typeutil.VectorTypeSize(t.FieldType) // Byte
	numClusters := int64(math.Ceil(totalSegmentsRawDataSize / (Params.DataCoordCfg.SegmentMaxSize.GetAsFloat() * 1024 * 1024 * Params.DataCoordCfg.ClusteringCompactionMaxSegmentSizeRatio.GetAsFloat())))
	if numClusters < Params.DataCoordCfg.ClusteringCompactionMinCentroidsNum.GetAsInt64() {
		log.Ctx(ctx).Info("data size is too small, skip analyze task", zap.Float64("raw data size", totalSegmentsRawDataSize), zap.Int64("num clusters", numClusters), zap.Int64("minimum num clusters required", Params.DataCoordCfg.ClusteringCompactionMinCentroidsNum.GetAsInt64()))
		at.SetState(indexpb.JobState_JobStateFinished, "")
		return true
	}
	if numClusters > Params.DataCoordCfg.ClusteringCompactionMaxCentroidsNum.GetAsInt64() {
		numClusters = Params.DataCoordCfg.ClusteringCompactionMaxCentroidsNum.GetAsInt64()
	}
	at.req.NumClusters = numClusters
	at.req.MaxTrainSizeRatio = Params.DataCoordCfg.ClusteringCompactionMaxTrainSizeRatio.GetAsFloat() // control clustering train data size
	// config to detect data skewness
	at.req.MinClusterSizeRatio = Params.DataCoordCfg.ClusteringCompactionMinClusterSizeRatio.GetAsFloat()
	at.req.MaxClusterSizeRatio = Params.DataCoordCfg.ClusteringCompactionMaxClusterSizeRatio.GetAsFloat()
	at.req.MaxClusterSize = Params.DataCoordCfg.ClusteringCompactionMaxClusterSize.GetAsSize()

	return false
}

func (at *analyzeTask) AssignTask(ctx context.Context, client types.IndexNodeClient) bool {
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()
	resp, err := client.CreateJobV2(ctx, &indexpb.CreateJobV2Request{
		ClusterID: at.req.GetClusterID(),
		TaskID:    at.req.GetTaskID(),
		JobType:   indexpb.JobType_JobTypeAnalyzeJob,
		Request: &indexpb.CreateJobV2Request_AnalyzeRequest{
			AnalyzeRequest: at.req,
		},
	})
	if err == nil {
		err = merr.Error(resp)
	}
	if err != nil {
		log.Ctx(ctx).Warn("assign analyze task to indexNode failed", zap.Int64("taskID", at.GetTaskID()), zap.Error(err))
		at.SetState(indexpb.JobState_JobStateRetry, err.Error())
		return false
	}

	log.Ctx(ctx).Info("analyze task assigned successfully", zap.Int64("taskID", at.GetTaskID()))
	at.SetState(indexpb.JobState_JobStateInProgress, "")
	return true
}

func (at *analyzeTask) setResult(result *indexpb.AnalyzeResult) {
	at.taskInfo = result
}

func (at *analyzeTask) QueryResult(ctx context.Context, client types.IndexNodeClient) {
	resp, err := client.QueryJobsV2(ctx, &indexpb.QueryJobsV2Request{
		ClusterID: Params.CommonCfg.ClusterPrefix.GetValue(),
		TaskIDs:   []int64{at.GetTaskID()},
		JobType:   indexpb.JobType_JobTypeAnalyzeJob,
	})
	if err == nil {
		err = merr.Error(resp.GetStatus())
	}
	if err != nil {
		log.Ctx(ctx).Warn("query analysis task result from IndexNode fail", zap.Int64("nodeID", at.GetNodeID()),
			zap.Error(err))
		at.SetState(indexpb.JobState_JobStateRetry, err.Error())
		return
	}

	// infos length is always one.
	for _, result := range resp.GetAnalyzeJobResults().GetResults() {
		if result.GetTaskID() == at.GetTaskID() {
			log.Ctx(ctx).Info("query analysis task info successfully",
				zap.Int64("taskID", at.GetTaskID()), zap.String("result state", result.GetState().String()),
				zap.String("failReason", result.GetFailReason()))
			if result.GetState() == indexpb.JobState_JobStateFinished || result.GetState() == indexpb.JobState_JobStateFailed ||
				result.GetState() == indexpb.JobState_JobStateRetry {
				// state is retry or finished or failed
				at.setResult(result)
			} else if result.GetState() == indexpb.JobState_JobStateNone {
				at.SetState(indexpb.JobState_JobStateRetry, "analyze task state is none in info response")
			}
			// inProgress or unissued/init, keep InProgress state
			return
		}
	}
	log.Ctx(ctx).Warn("query analyze task info failed, indexNode does not have task info",
		zap.Int64("taskID", at.GetTaskID()))
	at.SetState(indexpb.JobState_JobStateRetry, "analyze result is not in info response")
}

func (at *analyzeTask) DropTaskOnWorker(ctx context.Context, client types.IndexNodeClient) bool {
	resp, err := client.DropJobsV2(ctx, &indexpb.DropJobsV2Request{
		ClusterID: Params.CommonCfg.ClusterPrefix.GetValue(),
		TaskIDs:   []UniqueID{at.GetTaskID()},
		JobType:   indexpb.JobType_JobTypeAnalyzeJob,
	})
	if err == nil {
		err = merr.Error(resp)
	}
	if err != nil {
		log.Ctx(ctx).Warn("notify worker drop the analysis task fail", zap.Int64("taskID", at.GetTaskID()),
			zap.Int64("nodeID", at.GetNodeID()), zap.Error(err))
		return false
	}
	log.Ctx(ctx).Info("drop analyze on worker success",
		zap.Int64("taskID", at.GetTaskID()), zap.Int64("nodeID", at.GetNodeID()))
	return true
}

func (at *analyzeTask) SetJobInfo(meta *meta) error {
	return meta.analyzeMeta.FinishTask(at.GetTaskID(), at.taskInfo)
}
