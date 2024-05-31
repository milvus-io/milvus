package datacoord

import (
	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

var _ CompactionTask = (*mixCompactionTask)(nil)

type mixCompactionTask struct {
	*datapb.CompactionTask
	plan       *datapb.CompactionPlan
	dataNodeID int64
	result     *datapb.CompactionPlanResult
	span       trace.Span
}

func (task *mixCompactionTask) ProcessTask(handler *compactionPlanHandler) error {
	switch task.GetState() {
	case datapb.CompactionTaskState_failed:
		return nil
	case datapb.CompactionTaskState_completed:
		return nil
	case datapb.CompactionTaskState_pipelining:
		return task.processPipeliningTask(handler)
	case datapb.CompactionTaskState_executing:
		return task.processExecutingTask(handler)
	case datapb.CompactionTaskState_timeout:
		return task.processTimeoutTask(handler)
	default:
		return errors.New("not supported state")
	}
	return nil
}

func (task *mixCompactionTask) processPipeliningTask(handler *compactionPlanHandler) error {
	handler.scheduler.Submit(task)
	handler.plans[task.GetPlanID()] = task.ShadowClone(setState(datapb.CompactionTaskState_executing))
	log.Info("Compaction plan submited")
	return nil
}

func (task *mixCompactionTask) processExecutingTask(handler *compactionPlanHandler) error {
	nodePlan, exist := handler.compactionResults[task.GetPlanID()]
	if !exist {
		// compaction task in DC but not found in DN means the compaction plan has failed
		log.Info("compaction failed")
		handler.plans[task.GetPlanID()] = task.ShadowClone(setState(datapb.CompactionTaskState_failed), endSpan())
		handler.setSegmentsCompacting(task, false)
		handler.scheduler.Finish(task.GetPlanID(), task)
		return nil
	}
	planResult := nodePlan.B
	switch planResult.GetState() {
	case commonpb.CompactionState_Completed:
		// channels are balanced to other nodes, yet the old datanode still have the compaction results
		// task.dataNodeID == planState.A, but
		// task.dataNodeID not match with channel
		// Mark this compaction as failure and skip processing the meta
		if !handler.chManager.Match(task.GetNodeID(), task.GetChannel()) {
			// Sync segments without CompactionFrom segmentsIDs to make sure DN clear the task
			// without changing the meta
			log.Warn("compaction failed for channel nodeID not match")
			err := handler.sessions.SyncSegments(task.GetNodeID(), &datapb.SyncSegmentsRequest{PlanID: task.GetPlanID()})
			if err != nil {
				log.Warn("compaction failed to sync segments with node", zap.Error(err))
				return err
			}
			handler.setSegmentsCompacting(task, false)
			handler.plans[task.GetPlanID()] = task.ShadowClone(setState(datapb.CompactionTaskState_failed), cleanLogPath(), endSpan())
			handler.scheduler.Finish(task.GetNodeID(), task)
			return nil
		}

		if err := handler.handleMergeCompactionResult(task.GetPlan(), planResult); err != nil {
			return err
		}

		UpdateCompactionSegmentSizeMetrics(planResult.GetSegments())
		handler.plans[task.GetPlanID()] = task.ShadowClone(setState(datapb.CompactionTaskState_completed), setResult(planResult), cleanLogPath(), endSpan())
		handler.scheduler.Finish(task.GetNodeID(), task)
	case commonpb.CompactionState_Executing:
		ts := tsoutil.GetCurrentTime()
		if isTimeout(ts, task.GetStartTime(), task.GetTimeoutInSeconds()) {
			log.Warn("compaction timeout",
				zap.Int32("timeout in seconds", task.GetTimeoutInSeconds()),
				zap.Uint64("startTime", task.GetStartTime()),
				zap.Uint64("now", ts),
			)
			handler.plans[task.GetPlanID()] = task.ShadowClone(setState(datapb.CompactionTaskState_timeout), endSpan())
		}
	}
	return nil
}

func (task *mixCompactionTask) processTimeoutTask(handler *compactionPlanHandler) error {
	log := log.With(
		zap.Int64("planID", task.GetPlanID()),
		zap.Int64("nodeID", task.GetNodeID()),
		zap.String("channel", task.GetChannel()),
	)
	planID := task.GetPlanID()
	if nodePlan, ok := handler.compactionResults[task.GetPlanID()]; ok {
		if nodePlan.B.GetState() == commonpb.CompactionState_Executing {
			log.RatedInfo(1, "compaction timeout in DataCoord yet DataNode is still running")
		}
	} else {
		// compaction task in DC but not found in DN means the compaction plan has failed
		log.Info("compaction failed for timeout")
		handler.plans[planID] = task.ShadowClone(setState(datapb.CompactionTaskState_failed), endSpan())
		handler.setSegmentsCompacting(task, false)
		handler.scheduler.Finish(task.GetNodeID(), task)
	}
	return nil
}

func (task *mixCompactionTask) GetCollectionID() int64 {
	return task.CompactionTask.GetCollectionID()
}

func (task *mixCompactionTask) GetPartitionID() int64 {
	return task.CompactionTask.GetPartitionID()
}

func (task *mixCompactionTask) GetTriggerID() int64 {
	return task.CompactionTask.GetTriggerID()
}

func (task *mixCompactionTask) GetSpan() trace.Span {
	return task.span
}

func (task *mixCompactionTask) GetType() datapb.CompactionType {
	return task.CompactionTask.GetType()
}

func (task *mixCompactionTask) GetChannel() string {
	return task.CompactionTask.GetChannel()
}

func (task *mixCompactionTask) GetPlanID() int64 {
	return task.CompactionTask.GetPlanID()
}

func (task *mixCompactionTask) GetResult() *datapb.CompactionPlanResult {
	return task.result
}

func (task *mixCompactionTask) GetNodeID() int64 {
	return task.dataNodeID
}

func (task *mixCompactionTask) GetState() datapb.CompactionTaskState {
	return task.CompactionTask.GetState()
}

func (task *mixCompactionTask) GetPlan() *datapb.CompactionPlan {
	return task.plan
}

func (task *mixCompactionTask) ShadowClone(opts ...compactionTaskOpt) CompactionTask {
	taskClone := &mixCompactionTask{
		CompactionTask: task.CompactionTask,
		plan:           task.plan,
		dataNodeID:     task.dataNodeID,
		span:           task.span,
		result:         task.result,
	}
	for _, opt := range opts {
		opt(taskClone)
	}
	return taskClone
}

func (task *mixCompactionTask) EndSpan() {
	if task.span != nil {
		task.span.End()
	}
}

func (task *mixCompactionTask) SetNodeID(nodeID int64) {
	task.dataNodeID = nodeID
}

func (task *mixCompactionTask) SetState(state datapb.CompactionTaskState) {
	task.State = state
}

func (task *mixCompactionTask) SetTask(ct *datapb.CompactionTask) {
	task.CompactionTask = ct
}

func (task *mixCompactionTask) SetStartTime(startTime uint64) {
	task.StartTime = startTime
}

func (task *mixCompactionTask) SetResult(result *datapb.CompactionPlanResult) {
	task.result = result
}

func (task *mixCompactionTask) SetSpan(span trace.Span) {
	task.span = span
}

func (task *mixCompactionTask) SetPlan(plan *datapb.CompactionPlan) {
	task.plan = plan
}

func (task *mixCompactionTask) CleanLogPath() {
	if task.plan.GetSegmentBinlogs() != nil {
		for _, binlogs := range task.plan.GetSegmentBinlogs() {
			binlogs.FieldBinlogs = nil
			binlogs.Field2StatslogPaths = nil
			binlogs.Deltalogs = nil
		}
	}
	if task.result.GetSegments() != nil {
		for _, segment := range task.result.GetSegments() {
			segment.InsertLogs = nil
			segment.Deltalogs = nil
			segment.Field2StatslogPaths = nil
		}
	}
}

func (task *mixCompactionTask) BuildCompactionRequest(handler *compactionPlanHandler) (*datapb.CompactionPlan, error) {
	plan := &datapb.CompactionPlan{
		PlanID:           task.GetPlanID(),
		StartTime:        task.GetStartTime(),
		TimeoutInSeconds: task.GetTimeoutInSeconds(),
		Type:             task.GetType(),
		Channel:          task.GetChannel(),
		CollectionTtl:    task.GetCollectionTtl(),
		TotalRows:        task.GetTotalRows(),
	}
	log := log.With(zap.Int64("taskID", task.GetTriggerID()), zap.Int64("planID", plan.GetPlanID()))

	segIDMap := make(map[int64][]*datapb.FieldBinlog, len(plan.SegmentBinlogs))
	for _, segID := range task.GetInputSegments() {
		segInfo := handler.meta.GetHealthySegment(segID)
		if segInfo == nil {
			return nil, merr.WrapErrSegmentNotFound(segID)
		}
		plan.SegmentBinlogs = append(plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			SegmentID:           segID,
			CollectionID:        segInfo.GetCollectionID(),
			PartitionID:         segInfo.GetPartitionID(),
			Level:               segInfo.GetLevel(),
			InsertChannel:       segInfo.GetInsertChannel(),
			FieldBinlogs:        segInfo.GetBinlogs(),
			Field2StatslogPaths: segInfo.GetStatslogs(),
			Deltalogs:           segInfo.GetDeltalogs(),
		})
		segIDMap[segID] = segInfo.GetDeltalogs()
	}
	log.Info("Compaction handler refreshed mix compaction plan", zap.Any("segID2DeltaLogs", segIDMap))
	handler.plans[task.GetPlanID()] = task.ShadowClone(setPlan(plan))
	return plan, nil
}
