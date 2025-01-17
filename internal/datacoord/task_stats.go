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
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type statsTask struct {
	taskID          int64
	segmentID       int64
	targetSegmentID int64
	nodeID          int64
	taskInfo        *workerpb.StatsResult

	queueTime time.Time
	startTime time.Time
	endTime   time.Time

	req *workerpb.CreateStatsRequest

	subJobType indexpb.StatsSubJob
}

var _ Task = (*statsTask)(nil)

func newStatsTask(taskID int64, segmentID, targetSegmentID int64, subJobType indexpb.StatsSubJob) *statsTask {
	return &statsTask{
		taskID:          taskID,
		segmentID:       segmentID,
		targetSegmentID: targetSegmentID,
		taskInfo: &workerpb.StatsResult{
			TaskID: taskID,
			State:  indexpb.JobState_JobStateInit,
		},
		subJobType: subJobType,
	}
}

func (st *statsTask) setResult(result *workerpb.StatsResult) {
	st.taskInfo = result
}

func (st *statsTask) GetTaskID() int64 {
	return st.taskID
}

func (st *statsTask) GetNodeID() int64 {
	return st.nodeID
}

func (st *statsTask) ResetTask(mt *meta) {
	st.nodeID = 0
	// reset isCompacting

	mt.SetSegmentsCompacting(context.TODO(), []UniqueID{st.segmentID}, false)
}

func (st *statsTask) SetQueueTime(t time.Time) {
	st.queueTime = t
}

func (st *statsTask) GetQueueTime() time.Time {
	return st.queueTime
}

func (st *statsTask) SetStartTime(t time.Time) {
	st.startTime = t
}

func (st *statsTask) GetStartTime() time.Time {
	return st.startTime
}

func (st *statsTask) SetEndTime(t time.Time) {
	st.endTime = t
}

func (st *statsTask) GetEndTime() time.Time {
	return st.endTime
}

func (st *statsTask) GetTaskType() string {
	return indexpb.JobType_JobTypeStatsJob.String()
}

func (st *statsTask) CheckTaskHealthy(mt *meta) bool {
	seg := mt.GetHealthySegment(context.TODO(), st.segmentID)
	return seg != nil
}

func (st *statsTask) SetState(state indexpb.JobState, failReason string) {
	st.taskInfo.State = state
	st.taskInfo.FailReason = failReason
}

func (st *statsTask) GetState() indexpb.JobState {
	return st.taskInfo.GetState()
}

func (st *statsTask) GetFailReason() string {
	return st.taskInfo.GetFailReason()
}

func (st *statsTask) UpdateVersion(ctx context.Context, nodeID int64, meta *meta) error {
	// mark compacting
	if exist, canDo := meta.CheckAndSetSegmentsCompacting(ctx, []UniqueID{st.segmentID}); !exist || !canDo {
		log.Warn("segment is not exist or is compacting, skip stats",
			zap.Bool("exist", exist), zap.Bool("canDo", canDo))
		st.SetState(indexpb.JobState_JobStateNone, "segment is not healthy")
		return fmt.Errorf("mark segment compacting failed, isCompacting: %v", !canDo)
	}

	if err := meta.statsTaskMeta.UpdateVersion(st.taskID, nodeID); err != nil {
		return err
	}
	st.nodeID = nodeID
	return nil
}

func (st *statsTask) UpdateMetaBuildingState(meta *meta) error {
	return meta.statsTaskMeta.UpdateBuildingTask(st.taskID)
}

func (st *statsTask) PreCheck(ctx context.Context, dependency *taskScheduler) bool {
	// set segment compacting
	log := log.Ctx(ctx).With(zap.Int64("taskID", st.taskID), zap.Int64("segmentID", st.segmentID))
	segment := dependency.meta.GetHealthySegment(ctx, st.segmentID)
	if segment == nil {
		log.Warn("segment is node healthy, skip stats")
		st.SetState(indexpb.JobState_JobStateNone, "segment is not healthy")
		return false
	}

	if segment.GetIsSorted() && st.subJobType == indexpb.StatsSubJob_Sort {
		log.Info("stats task is marked as sorted, skip stats")
		st.SetState(indexpb.JobState_JobStateNone, "segment is marked as sorted")
		return false
	}

	collInfo, err := dependency.handler.GetCollection(ctx, segment.GetCollectionID())
	if err != nil || collInfo == nil {
		log.Warn("stats task get collection info failed", zap.Int64("collectionID",
			segment.GetCollectionID()), zap.Error(err))
		st.SetState(indexpb.JobState_JobStateInit, err.Error())
		return false
	}

	collTtl, err := getCollectionTTL(collInfo.Properties)
	if err != nil {
		log.Warn("stats task get collection ttl failed", zap.Int64("collectionID", segment.GetCollectionID()), zap.Error(err))
		st.SetState(indexpb.JobState_JobStateInit, err.Error())
		return false
	}

	binlogNum := (segment.getSegmentSize()/Params.DataNodeCfg.BinLogMaxSize.GetAsInt64() + 1) * int64(len(collInfo.Schema.GetFields())) * 100
	// binlogNum + BM25logNum + statslogNum
	start, end, err := dependency.allocator.AllocN(binlogNum + int64(len(collInfo.Schema.GetFunctions())) + 1)
	if err != nil {
		log.Warn("stats task alloc logID failed", zap.Int64("collectionID", segment.GetCollectionID()), zap.Error(err))
		st.SetState(indexpb.JobState_JobStateInit, err.Error())
		return false
	}

	st.req = &workerpb.CreateStatsRequest{
		ClusterID:       Params.CommonCfg.ClusterPrefix.GetValue(),
		TaskID:          st.GetTaskID(),
		CollectionID:    segment.GetCollectionID(),
		PartitionID:     segment.GetPartitionID(),
		InsertChannel:   segment.GetInsertChannel(),
		SegmentID:       segment.GetID(),
		InsertLogs:      segment.GetBinlogs(),
		DeltaLogs:       segment.GetDeltalogs(),
		StorageConfig:   createStorageConfig(),
		Schema:          collInfo.Schema,
		SubJobType:      st.subJobType,
		TargetSegmentID: st.targetSegmentID,
		StartLogID:      start,
		EndLogID:        end,
		NumRows:         segment.GetNumOfRows(),
		CollectionTtl:   collTtl.Nanoseconds(),
		CurrentTs:       tsoutil.GetCurrentTime(),
		BinlogMaxSize:   Params.DataNodeCfg.BinLogMaxSize.GetAsUint64(),
	}

	return true
}

func (st *statsTask) AssignTask(ctx context.Context, client types.DataNodeClient) bool {
	ctx, cancel := context.WithTimeout(ctx, reqTimeoutInterval)
	defer cancel()
	resp, err := client.CreateJobV2(ctx, &workerpb.CreateJobV2Request{
		ClusterID: st.req.GetClusterID(),
		TaskID:    st.req.GetTaskID(),
		JobType:   indexpb.JobType_JobTypeStatsJob,
		Request: &workerpb.CreateJobV2Request_StatsRequest{
			StatsRequest: st.req,
		},
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Ctx(ctx).Warn("assign stats task failed", zap.Int64("taskID", st.taskID),
			zap.Int64("segmentID", st.segmentID), zap.Error(err))
		st.SetState(indexpb.JobState_JobStateRetry, err.Error())
		return false
	}

	log.Ctx(ctx).Info("assign stats task success", zap.Int64("taskID", st.taskID), zap.Int64("segmentID", st.segmentID))
	st.SetState(indexpb.JobState_JobStateInProgress, "")
	return true
}

func (st *statsTask) QueryResult(ctx context.Context, client types.DataNodeClient) {
	ctx, cancel := context.WithTimeout(ctx, reqTimeoutInterval)
	defer cancel()
	resp, err := client.QueryJobsV2(ctx, &workerpb.QueryJobsV2Request{
		ClusterID: st.req.GetClusterID(),
		TaskIDs:   []int64{st.GetTaskID()},
		JobType:   indexpb.JobType_JobTypeStatsJob,
	})

	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Ctx(ctx).Warn("query stats task result failed", zap.Int64("taskID", st.GetTaskID()),
			zap.Int64("segmentID", st.segmentID), zap.Error(err))
		st.SetState(indexpb.JobState_JobStateRetry, err.Error())
		return
	}

	for _, result := range resp.GetStatsJobResults().GetResults() {
		if result.GetTaskID() == st.GetTaskID() {
			log.Ctx(ctx).Info("query stats task result success", zap.Int64("taskID", st.GetTaskID()),
				zap.Int64("segmentID", st.segmentID), zap.String("result state", result.GetState().String()),
				zap.String("failReason", result.GetFailReason()))
			if result.GetState() == indexpb.JobState_JobStateFinished || result.GetState() == indexpb.JobState_JobStateRetry ||
				result.GetState() == indexpb.JobState_JobStateFailed {
				st.setResult(result)
			} else if result.GetState() == indexpb.JobState_JobStateNone {
				st.SetState(indexpb.JobState_JobStateRetry, "stats task state is none in info response")
			}
			// inProgress or unissued/init, keep InProgress state
			return
		}
	}
	log.Ctx(ctx).Warn("query stats task result failed, indexNode does not have task info",
		zap.Int64("taskID", st.GetTaskID()), zap.Int64("segmentID", st.segmentID))
	st.SetState(indexpb.JobState_JobStateRetry, "stats task is not in info response")
}

func (st *statsTask) DropTaskOnWorker(ctx context.Context, client types.DataNodeClient) bool {
	ctx, cancel := context.WithTimeout(ctx, reqTimeoutInterval)
	defer cancel()
	resp, err := client.DropJobsV2(ctx, &workerpb.DropJobsV2Request{
		ClusterID: st.req.GetClusterID(),
		TaskIDs:   []int64{st.GetTaskID()},
		JobType:   indexpb.JobType_JobTypeStatsJob,
	})

	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Ctx(ctx).Warn("notify worker drop the stats task failed", zap.Int64("taskID", st.GetTaskID()),
			zap.Int64("segmentID", st.segmentID), zap.Error(err))
		return false
	}
	log.Ctx(ctx).Info("drop stats task success", zap.Int64("taskID", st.GetTaskID()),
		zap.Int64("segmentID", st.segmentID))
	return true
}

func (st *statsTask) SetJobInfo(meta *meta) error {
	if st.GetState() == indexpb.JobState_JobStateFinished {
		switch st.subJobType {
		case indexpb.StatsSubJob_Sort:
			// first update segment, failed state cannot generate new segment
			metricMutation, err := meta.SaveStatsResultSegment(st.segmentID, st.taskInfo)
			if err != nil {
				log.Warn("save sort stats result failed", zap.Int64("taskID", st.taskID),
					zap.Int64("segmentID", st.segmentID), zap.Error(err))
				return err
			}
			metricMutation.commit()

			select {
			case getBuildIndexChSingleton() <- st.taskInfo.GetSegmentID():
			default:
			}
		case indexpb.StatsSubJob_TextIndexJob:
			err := meta.UpdateSegment(st.taskInfo.GetSegmentID(), SetTextIndexLogs(st.taskInfo.GetTextStatsLogs()))
			if err != nil {
				log.Warn("save text index stats result failed", zap.Int64("taskID", st.taskID),
					zap.Int64("segmentID", st.segmentID), zap.Error(err))
				return err
			}
		case indexpb.StatsSubJob_BM25Job:
			// TODO: support bm25 job
		}
	}

	// second update the task meta
	if err := meta.statsTaskMeta.FinishTask(st.taskID, st.taskInfo); err != nil {
		log.Warn("save stats result failed", zap.Int64("taskID", st.taskID), zap.Error(err))
		return err
	}

	log.Info("SetJobInfo for stats task success", zap.Int64("taskID", st.taskID),
		zap.Int64("oldSegmentID", st.segmentID), zap.Int64("targetSegmentID", st.taskInfo.GetSegmentID()),
		zap.String("subJobType", st.subJobType.String()), zap.String("state", st.taskInfo.GetState().String()))
	return nil
}
