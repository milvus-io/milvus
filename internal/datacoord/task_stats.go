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
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func (s *Server) checkStatsTaskLoop(ctx context.Context) {
	log.Info("start check stats task for segment loop...")
	defer s.serverLoopWg.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Warn("DataCoord context done, exit...")
			return
		case <-ticker.C:
			segments := s.meta.SelectSegments(SegmentFilterFunc(func(seg *SegmentInfo) bool {
				return isFlush(seg) && !seg.GetIsSorted() && !seg.isCompacting
			}))
			for _, segment := range segments {
				if err := s.createStatsSegmentTask(segment); err != nil {
					log.Warn("create stats task for segment fail, wait for retry", zap.Int64("segmentID", segment.GetID()))
					continue
				}
			}
		}
	}
}

func (s *Server) createStatsSegmentTask(segment *SegmentInfo) error {
	taskID, err := s.allocator.allocID(context.Background())
	if err != nil {
		return err
	}
	t := &indexpb.StatsTask{
		CollectionID:  segment.GetCollectionID(),
		PartitionID:   segment.GetPartitionID(),
		SegmentID:     segment.GetID(),
		InsertChannel: segment.GetInsertChannel(),
		TaskID:        taskID,
		Version:       0,
		NodeID:        0,
		State:         indexpb.JobState_JobStateInit,
		FailReason:    "",
	}
	if err = s.meta.statsTaskMeta.AddStatsTask(t); err != nil {
		return err
	}
	s.taskScheduler.enqueue(newStatsTask(taskID, segment.GetID()))
	return nil
}

type statsTask struct {
	taskID    int64
	segmentID int64
	nodeID    int64
	taskInfo  *workerpb.StatsResult

	queueTime time.Time
	startTime time.Time
	endTime   time.Time

	req *workerpb.CreateStatsRequest
}

var _ Task = (*statsTask)(nil)

func newStatsTask(taskID int64, segmentID int64) *statsTask {
	return &statsTask{
		taskID:    taskID,
		segmentID: segmentID,
		taskInfo: &workerpb.StatsResult{
			TaskID: taskID,
			State:  indexpb.JobState_JobStateInit,
		},
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

func (st *statsTask) ResetNodeID() {
	st.nodeID = 0
}

func (at *statsTask) SetQueueTime(t time.Time) {
	at.queueTime = t
}

func (at *statsTask) GetQueueTime() time.Time {
	return at.queueTime
}

func (at *statsTask) SetStartTime(t time.Time) {
	at.startTime = t
}

func (at *statsTask) GetStartTime() time.Time {
	return at.startTime
}

func (at *statsTask) SetEndTime(t time.Time) {
	at.endTime = t
}

func (at *statsTask) GetEndTime() time.Time {
	return at.endTime
}

func (it *statsTask) GetTaskType() string {
	return indexpb.JobType_JobTypeStatsJob.String()
}

func (st *statsTask) CheckTaskHealthy(mt *meta) bool {
	seg := mt.GetHealthySegment(st.req.GetSegmentID())
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

func (st *statsTask) UpdateVersion(ctx context.Context, meta *meta) error {
	return meta.statsTaskMeta.UpdateVersion(st.taskID)
}

func (st *statsTask) UpdateMetaBuildingState(nodeID int64, meta *meta) error {
	st.nodeID = nodeID
	return meta.statsTaskMeta.UpdateBuildingTask(st.taskID, nodeID)
}

func (st *statsTask) PreCheck(ctx context.Context, dependency *taskScheduler) bool {
	// set segment compacting
	segment := dependency.meta.GetHealthySegment(st.segmentID)
	if segment == nil {
		log.Warn("segment is node healthy, skip stats", zap.Int64("segmentID", st.segmentID))
		st.SetState(indexpb.JobState_JobStateNone, "segment is not healthy")
		return false
	}

	if segment.GetIsSorted() {
		log.Info("stats task is marked as sorted, skip stats", zap.Int64("segmentID", segment.GetID()))
		st.SetState(indexpb.JobState_JobStateNone, "segment is marked as sorted")
		return false
	}

	if exist, canDo := dependency.meta.CheckAndSetSegmentsCompacting([]UniqueID{st.segmentID}); !exist || !canDo {
		log.Warn("segment is not exist or is compacting, skip stats", zap.Int64("segmentID", st.segmentID),
			zap.Bool("exist", exist), zap.Bool("canDo", canDo))
		st.SetState(indexpb.JobState_JobStateNone, "segment is not healthy")
		return false
	}

	return true
}

func (st *statsTask) AssignTask(ctx context.Context, client types.IndexNodeClient) bool {
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
	if err == nil {
		err = merr.Error(resp)
	}
	if err != nil {
		log.Ctx(ctx).Warn("assign stats task failed", zap.Int64("taskID", st.taskID),
			zap.Int64("segmentID", st.segmentID), zap.Error(err))
		return false
	}

	log.Ctx(ctx).Info("assign stats task success", zap.Int64("taskID", st.taskID), zap.Int64("segmentID", st.segmentID))
	st.SetState(indexpb.JobState_JobStateInProgress, "")
	return true
}

func (st *statsTask) QueryResult(ctx context.Context, client types.IndexNodeClient) {
	resp, err := client.QueryJobsV2(ctx, &workerpb.QueryJobsV2Request{
		ClusterID: st.req.GetClusterID(),
		TaskIDs:   []int64{st.GetTaskID()},
		JobType:   indexpb.JobType_JobTypeStatsJob,
	})
	if err == nil {
		err = merr.Error(resp.GetStatus())
	}
	if err != nil {
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

func (st *statsTask) DropTaskOnWorker(ctx context.Context, client types.IndexNodeClient) bool {
	resp, err := client.DropJobsV2(ctx, &workerpb.DropJobsV2Request{
		ClusterID: st.req.GetClusterID(),
		TaskIDs:   []int64{st.GetTaskID()},
		JobType:   indexpb.JobType_JobTypeStatsJob,
	})

	if err == nil {
		err = merr.Error(resp)
	}
	if err != nil {
		log.Ctx(ctx).Warn("notify worker drop the analysis task failed", zap.Int64("taskID", st.GetTaskID()),
			zap.Int64("segmentID", st.segmentID), zap.Error(err))
		return false
	}
	log.Ctx(ctx).Info("drop stats task success", zap.Int64("taskID", st.GetTaskID()),
		zap.Int64("segmentID", st.segmentID))
	return true
}

func (st *statsTask) SetJobInfo(meta *meta) error {
	// first update segment
	metricMutation, err := meta.saveStatsResultSegment(st.segmentID, st.taskInfo)
	if err != nil {
		log.Warn("save stats result failed", zap.Int64("taskID", st.taskID),
			zap.Int64("segmentID", st.segmentID), zap.Error(err))
		return err
	}

	// second update the task meta
	if err = meta.statsTaskMeta.FinishTask(st.taskID, st.taskInfo); err != nil {
		log.Warn("save stats result failed", zap.Int64("taskID", st.taskID))
	}

	metricMutation.commit()
	return nil
}
