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
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

type statsTask struct {
	taskID          int64
	segmentID       int64
	targetSegmentID int64
	nodeID          int64
	taskInfo        *workerpb.StatsResult

	taskSlot int64

	queueTime time.Time
	startTime time.Time
	endTime   time.Time

	req *workerpb.CreateStatsRequest

	subJobType indexpb.StatsSubJob
}

var _ Task = (*statsTask)(nil)

func newStatsTask(taskID int64, segmentID, targetSegmentID int64, subJobType indexpb.StatsSubJob, taskSlot int64) *statsTask {
	return &statsTask{
		taskID:          taskID,
		segmentID:       segmentID,
		targetSegmentID: targetSegmentID,
		taskInfo: &workerpb.StatsResult{
			TaskID: taskID,
			State:  indexpb.JobState_JobStateInit,
		},
		subJobType: subJobType,
		taskSlot:   taskSlot,
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
	// reset isCompacting
	mt.SetSegmentsCompacting(context.TODO(), []UniqueID{st.segmentID}, false)
	mt.SetSegmentStating(st.segmentID, false)
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

func (st *statsTask) GetTaskSlot() int64 {
	return st.taskSlot
}

func (st *statsTask) UpdateVersion(ctx context.Context, nodeID int64, meta *meta, compactionHandler compactionPlanContext) error {
	// mark compacting
	if exist, canDo := meta.CheckAndSetSegmentsCompacting(ctx, []UniqueID{st.segmentID}); !exist || !canDo {
		log.Warn("segment is not exist or is compacting, skip stats",
			zap.Bool("exist", exist), zap.Bool("canDo", canDo))
		st.SetState(indexpb.JobState_JobStateFailed, "segment is not healthy")
		st.SetStartTime(time.Now())
		return fmt.Errorf("mark segment compacting failed, isCompacting: %v", !canDo)
	}

	if !compactionHandler.checkAndSetSegmentStating(st.req.GetInsertChannel(), st.segmentID) {
		log.Warn("segment is contains by l0 compaction, skip stats", zap.Int64("taskID", st.taskID),
			zap.Int64("segmentID", st.segmentID))
		st.SetState(indexpb.JobState_JobStateFailed, "segment is contains by l0 compaction")
		// reset compacting
		meta.SetSegmentsCompacting(ctx, []UniqueID{st.segmentID}, false)
		st.SetStartTime(time.Now())
		return fmt.Errorf("segment is contains by l0 compaction")
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
	log := log.Ctx(ctx).With(zap.Int64("taskID", st.taskID), zap.Int64("segmentID", st.segmentID),
		zap.Int64("targetSegmentID", st.targetSegmentID))

	statsMeta := dependency.meta.statsTaskMeta.GetStatsTask(st.taskID)
	if statsMeta == nil {
		log.Warn("stats task meta is null, skip it")
		st.SetState(indexpb.JobState_JobStateNone, "stats task meta is null")
		return false
	}
	// set segment compacting
	segment := dependency.meta.GetHealthySegment(ctx, st.segmentID)
	if segment == nil {
		log.Warn("segment is node healthy, skip stats")
		st.SetState(indexpb.JobState_JobStateNone, "segment is not healthy")
		return false
	}

	if segment.GetNumOfRows() == 0 {
		st.setResult(&workerpb.StatsResult{
			TaskID:       st.taskID,
			State:        indexpb.JobState_JobStateFinished,
			FailReason:   "segment num row is zero",
			CollectionID: st.req.GetCollectionID(),
			PartitionID:  st.req.GetPartitionID(),
			SegmentID:    st.targetSegmentID,
			Channel:      st.req.GetInsertChannel(),
			NumRows:      0,
		})
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
		StorageConfig:   createStorageConfig(),
		Schema:          collInfo.Schema,
		SubJobType:      st.subJobType,
		TargetSegmentID: st.targetSegmentID,
		StartLogID:      start,
		EndLogID:        end,
		NumRows:         segment.GetNumOfRows(),
		CollectionTtl:   collTtl.Nanoseconds(),
		CurrentTs:       tsoutil.GetCurrentTime(),
		// update version after check
		TaskVersion:               statsMeta.GetVersion() + 1,
		BinlogMaxSize:             Params.DataNodeCfg.BinLogMaxSize.GetAsUint64(),
		EnableJsonKeyStats:        Params.CommonCfg.EnabledJSONKeyStats.GetAsBool(),
		JsonKeyStatsTantivyMemory: Params.DataCoordCfg.JSONKeyStatsMemoryBudgetInTantivy.GetAsInt64(),
		JsonKeyStatsDataFormat:    1,
		EnableJsonKeyStatsInSort:  Params.DataCoordCfg.EnabledJSONKeyStatsInSort.GetAsBool(),
		TaskSlot:                  st.taskSlot,
	}

	log.Info("stats task pre check successfully", zap.String("subJobType", st.subJobType.String()),
		zap.Int64("num rows", segment.GetNumOfRows()), zap.Int64("task version", st.req.GetTaskVersion()))

	return true
}

func (st *statsTask) AssignTask(ctx context.Context, client types.IndexNodeClient, meta *meta) bool {
	segment := meta.GetHealthySegment(ctx, st.segmentID)
	if segment == nil {
		log.Ctx(ctx).Warn("segment is node healthy, skip stats")
		// need to set retry and reset compacting
		st.SetState(indexpb.JobState_JobStateRetry, "segment is not healthy")
		return false
	}

	// Set InsertLogs and DeltaLogs before execution, and wait for the L0 compaction containing the segment to complete
	st.req.InsertLogs = segment.GetBinlogs()
	st.req.DeltaLogs = segment.GetDeltalogs()

	ctx, cancel := context.WithTimeout(ctx, Params.DataCoordCfg.RequestTimeoutSeconds.GetAsDuration(time.Second))
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

func (st *statsTask) QueryResult(ctx context.Context, client types.IndexNodeClient) {
	ctx, cancel := context.WithTimeout(ctx, Params.DataCoordCfg.RequestTimeoutSeconds.GetAsDuration(time.Second))
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
			if result.GetState() == indexpb.JobState_JobStateFinished || result.GetState() == indexpb.JobState_JobStateRetry ||
				result.GetState() == indexpb.JobState_JobStateFailed {
				log.Ctx(ctx).Info("query stats task result success", zap.Int64("taskID", st.GetTaskID()),
					zap.Int64("segmentID", st.segmentID), zap.String("result state", result.GetState().String()),
					zap.String("failReason", result.GetFailReason()))
				st.setResult(result)
			} else if result.GetState() == indexpb.JobState_JobStateNone {
				log.Ctx(ctx).Info("query stats task result success", zap.Int64("taskID", st.GetTaskID()),
					zap.Int64("segmentID", st.segmentID), zap.String("result state", result.GetState().String()),
					zap.String("failReason", result.GetFailReason()))
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

func (st *statsTask) DropTaskOnWorker(ctx context.Context, client types.IndexNodeClient) bool {
	ctx, cancel := context.WithTimeout(ctx, Params.DataCoordCfg.RequestTimeoutSeconds.GetAsDuration(time.Second))
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
		case indexpb.StatsSubJob_JsonKeyIndexJob:
			err := meta.UpdateSegment(st.taskInfo.GetSegmentID(), SetJsonKeyIndexLogs(st.taskInfo.GetJsonKeyStatsLogs()))
			if err != nil {
				log.Warn("save json key index stats result failed", zap.Int64("taskId", st.taskID),
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

func (st *statsTask) DropTaskMeta(ctx context.Context, meta *meta) error {
	if err := meta.statsTaskMeta.DropStatsTask(st.taskID); err != nil {
		log.Ctx(ctx).Warn("drop stats task failed", zap.Int64("taskID", st.taskID), zap.Error(err))
		return err
	}
	log.Ctx(ctx).Info("drop stats task success", zap.Int64("taskID", st.taskID))
	return nil
}
