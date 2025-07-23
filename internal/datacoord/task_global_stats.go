package datacoord

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
)

type globalStatsTask struct {
	*datapb.GlobalStatsTask
	taskSlot  int64
	times     *taskcommon.Times
	meta      *meta
	handler   Handler
	allocator allocator.Allocator
}

var _ globalTask.Task = (*globalStatsTask)(nil)

func newGlobalStatsTask(t *datapb.GlobalStatsTask, taskSlot int64, meta *meta, handler Handler, allocator allocator.Allocator) *globalStatsTask {
	return &globalStatsTask{
		GlobalStatsTask: t,
		taskSlot:        taskSlot,
		times:           taskcommon.NewTimes(),
		meta:            meta,
		handler:         handler,
		allocator:       allocator,
	}
}

func (gt *globalStatsTask) GetTaskType() taskcommon.Type {
	return taskcommon.GlobalStats
}

func (gt *globalStatsTask) GetCollectionID() int64 {
	return gt.CollectionID
}

func (gt *globalStatsTask) GetPartitionID() int64 {
	return gt.PartitionID
}

func (gt *globalStatsTask) GetVChannel() string {
	return gt.VChannel
}

func (gt *globalStatsTask) GetTimes() *taskcommon.Times {
	return gt.times
}

func (gt *globalStatsTask) GetVersion() int64 {
	return gt.Version
}

func (gt *globalStatsTask) SetVersion(version int64) {
	gt.Version = version
}

func (gt *globalStatsTask) GetTaskID() int64 {
	return gt.TaskID
}

func (gt *globalStatsTask) GetTaskSlot() int64 {
	return gt.taskSlot
}

func (gt *globalStatsTask) GetTaskState() taskcommon.State {
	return gt.State
}

func (gt *globalStatsTask) GetTaskVersion() int64 {
	return gt.Version
}

func (gt *globalStatsTask) SetState(state indexpb.JobState, failReason string) {
	gt.State = state
	gt.FailReason = failReason
}

func (t *globalStatsTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	t.times.SetTaskTime(timeType, time)
}

func (t *globalStatsTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(t.times)
}

func (gt *globalStatsTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	log.Info("execute global stats task",
		zap.Int64("taskID", gt.TaskID),
		zap.Int64("collectionID", gt.CollectionID),
		zap.Int64("nodeID", nodeID),
		zap.String("vChannel", gt.VChannel),
		zap.Int("segmentCount", len(gt.SegmentInfos)),
	)
	if err := gt.UpdateVersion(nodeID); err != nil {
		log.Warn("failed to update task version", zap.Error(err))
		return
	}
	cluster.CreateGlobalStats(nodeID, gt.GlobalStatsTask, gt.taskSlot)
	if err := gt.UpdateStateWithMeta(indexpb.JobState_JobStateInProgress, ""); err != nil {
		log.Warn("failed to update task state to inProgress", zap.Error(err))
	}
	log.Info("update task state to inProgress successfully")
}

func (gt *globalStatsTask) UpdateStateWithMeta(state indexpb.JobState, failReason string) error {
	if err := gt.meta.globalStatsMeta.UpdateState(gt.GetTaskID(), state, failReason); err != nil {
		return err
	}
	gt.SetState(state, failReason)
	return nil
}

func (gt *globalStatsTask) QueryTaskOnWorker(cluster session.Cluster) {
	log := log.Ctx(context.TODO()).With(
		zap.Int64("taskID", gt.GetTaskID()),
		zap.Int64("nodeID", gt.NodeID),
	)

	resp, err := cluster.QueryGlobalStats(gt.NodeID, &workerpb.QueryJobsRequest{
		ClusterID: Params.CommonCfg.ClusterPrefix.GetValue(),
		TaskIDs:   []int64{gt.GetTaskID()},
	})
	log.Info("query global stats task result from worker", zap.Any("resp", resp), zap.Error(err))
	if err != nil {
		log.Warn("query global stats task result from worker failed", zap.Error(err))
		gt.dropAndResetTaskOnWorker(cluster, err.Error())
		return
	}

	for _, result := range resp.GetResults() {
		if result.GetTaskID() != gt.GetTaskID() {
			continue
		}

		state := result.GetState()
		switch state {
		case indexpb.JobState_JobStateFinished:
			gt.UpdateStateWithMeta(state, result.GetFailReason())
		case indexpb.JobState_JobStateRetry, indexpb.JobState_JobStateNone:
			gt.dropAndResetTaskOnWorker(cluster, result.GetFailReason())
		case indexpb.JobState_JobStateFailed:
			gt.UpdateStateWithMeta(state, result.GetFailReason())
		}
		return
	}

	log.Warn("query global stats task info failed, worker does not have task info")
	gt.UpdateStateWithMeta(indexpb.JobState_JobStateInit, "global stats result is not in info response")
}

func (gt *globalStatsTask) dropAndResetTaskOnWorker(cluster session.Cluster, reason string) {
	if err := gt.tryDropTaskOnWorker(cluster); err != nil {
		return
	}
	gt.resetTask(reason)
}

func (gt *globalStatsTask) UpdateVersion(nodeID int64) error {
	if err := gt.meta.globalStatsMeta.UpdateVersion(gt.GetTaskID(), nodeID); err != nil {
		return err
	}
	log.Info("update global stats task version",
		zap.Int64("version", gt.GetVersion()),
	)
	gt.Version++
	gt.NodeID = nodeID
	return nil
}

func (gt *globalStatsTask) tryDropTaskOnWorker(cluster session.Cluster) error {
	log := log.Ctx(context.TODO()).With(
		zap.Int64("taskID", gt.GetTaskID()),
		zap.Int64("nodeID", gt.NodeID),
	)

	if err := cluster.DropGlobalStats(gt.NodeID, gt.GetTaskID()); err != nil {
		log.Warn("failed to drop global stats task on worker", zap.Error(err))
		return err
	}

	log.Info("dropped global stats task on worker successfully")
	return nil
}

func (gt *globalStatsTask) resetTask(reason string) {
	gt.UpdateStateWithMeta(indexpb.JobState_JobStateInit, reason)
}

func (gt *globalStatsTask) DropTaskOnWorker(cluster session.Cluster) {
	gt.tryDropTaskOnWorker(cluster)
}
