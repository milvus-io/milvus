package datacoord

import (
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

var _ CompactionTask = (*l0CompactionTask)(nil)

type l0CompactionTask struct {
	*datapb.CompactionTask
	plan       *datapb.CompactionPlan
	dataNodeID int64
	result     *datapb.CompactionPlanResult
	span       trace.Span
}

func (task *l0CompactionTask) ProcessTask(handler *compactionPlanHandler) error {
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
}

func (task *l0CompactionTask) processPipeliningTask(handler *compactionPlanHandler) error {
	handler.scheduler.Submit(task)
	handler.plans[task.GetPlanID()] = task.ShadowClone(setState(datapb.CompactionTaskState_executing))
	log.Info("Compaction plan submited")
	return nil
}

func (task *l0CompactionTask) processExecutingTask(handler *compactionPlanHandler) error {
	if !handler.scheduler.GetTaskExecuting(task.PlanID) {
		return nil
	}
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
		if err := handler.handleL0CompactionResult(task.GetPlan(), planResult); err != nil {
			return err
		}
		UpdateCompactionSegmentSizeMetrics(planResult.GetSegments())
		handler.plans[task.GetPlanID()] = task.ShadowClone(setState(datapb.CompactionTaskState_completed), setResult(planResult), cleanLogPath(), endSpan())
		handler.scheduler.Finish(task.GetNodeID(), task)
	case commonpb.CompactionState_Executing:
		ts, err := handler.GetCurrentTS()
		if err != nil {
			return err
		}
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

func (task *l0CompactionTask) processTimeoutTask(handler *compactionPlanHandler) error {
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

func (task *l0CompactionTask) GetCollectionID() int64 {
	return task.CompactionTask.GetCollectionID()
}

func (task *l0CompactionTask) GetPartitionID() int64 {
	return task.CompactionTask.GetPartitionID()
}

func (task *l0CompactionTask) GetTriggerID() int64 {
	return task.CompactionTask.GetTriggerID()
}

func (task *l0CompactionTask) GetSpan() trace.Span {
	return task.span
}

func (task *l0CompactionTask) GetType() datapb.CompactionType {
	return task.CompactionTask.GetType()
}

func (task *l0CompactionTask) GetChannel() string {
	return task.CompactionTask.GetChannel()
}

func (task *l0CompactionTask) GetPlanID() int64 {
	return task.CompactionTask.GetPlanID()
}

func (task *l0CompactionTask) GetResult() *datapb.CompactionPlanResult {
	return task.result
}

func (task *l0CompactionTask) GetNodeID() int64 {
	return task.dataNodeID
}

func (task *l0CompactionTask) GetState() datapb.CompactionTaskState {
	return task.CompactionTask.GetState()
}

func (task *l0CompactionTask) GetPlan() *datapb.CompactionPlan {
	return task.plan
}

func (task *l0CompactionTask) ShadowClone(opts ...compactionTaskOpt) CompactionTask {
	taskClone := &l0CompactionTask{
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

func (task *l0CompactionTask) EndSpan() {
	if task.span != nil {
		task.span.End()
	}
}

func (task *l0CompactionTask) SetNodeID(nodeID int64) {
	task.dataNodeID = nodeID
}

func (task *l0CompactionTask) SetState(state datapb.CompactionTaskState) {
	task.State = state
}

func (task *l0CompactionTask) SetTask(ct *datapb.CompactionTask) {
	task.CompactionTask = ct
}

func (task *l0CompactionTask) SetStartTime(startTime uint64) {
	task.StartTime = startTime
}

func (task *l0CompactionTask) SetResult(result *datapb.CompactionPlanResult) {
	task.result = result
}

func (task *l0CompactionTask) SetSpan(span trace.Span) {
	task.span = span
}

func (task *l0CompactionTask) SetPlan(plan *datapb.CompactionPlan) {
	task.plan = plan
}

func (task *l0CompactionTask) CleanLogPath() {
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

func (task *l0CompactionTask) BuildCompactionRequest(handler *compactionPlanHandler) (*datapb.CompactionPlan, error) {
	plan := &datapb.CompactionPlan{
		PlanID:           task.GetPlanID(),
		StartTime:        task.GetStartTime(),
		TimeoutInSeconds: task.GetTimeoutInSeconds(),
		Type:             task.GetType(),
		Channel:          task.GetChannel(),
		CollectionTtl:    task.GetCollectionTtl(),
		TotalRows:        task.GetTotalRows(),
		Schema:           task.GetSchema(),
	}

	log := log.With(zap.Int64("taskID", task.GetTriggerID()), zap.Int64("planID", plan.GetPlanID()))
	for _, segID := range task.GetInputSegments() {
		segInfo := handler.meta.GetHealthySegment(segID)
		if segInfo == nil {
			return nil, merr.WrapErrSegmentNotFound(segID)
		}
		plan.SegmentBinlogs = append(plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			SegmentID:     segID,
			CollectionID:  segInfo.GetCollectionID(),
			PartitionID:   segInfo.GetPartitionID(),
			Level:         segInfo.GetLevel(),
			InsertChannel: segInfo.GetInsertChannel(),
			Deltalogs:     segInfo.GetDeltalogs(),
		})
	}

	// Select sealed L1 segments for LevelZero compaction that meets the condition:
	// dmlPos < triggerInfo.pos
	sealedSegments := handler.meta.SelectSegments(WithCollection(task.GetCollectionID()), SegmentFilterFunc(func(info *SegmentInfo) bool {
		return (task.GetPartitionID() == -1 || info.GetPartitionID() == task.GetPartitionID()) &&
			info.GetInsertChannel() == plan.GetChannel() &&
			isFlushState(info.GetState()) &&
			!info.isCompacting &&
			!info.GetIsImporting() &&
			info.GetLevel() != datapb.SegmentLevel_L0 &&
			info.GetDmlPosition().GetTimestamp() < task.GetPos().GetTimestamp()
	}))
	if len(sealedSegments) == 0 {
		return nil, errors.Errorf("Selected zero L1/L2 segments for the position=%v", task.GetPos())
	}

	sealedSegBinlogs := lo.Map(sealedSegments, func(segInfo *SegmentInfo, _ int) *datapb.CompactionSegmentBinlogs {
		return &datapb.CompactionSegmentBinlogs{
			SegmentID:    segInfo.GetID(),
			Level:        segInfo.GetLevel(),
			CollectionID: segInfo.GetCollectionID(),
			PartitionID:  segInfo.GetPartitionID(),
			// no need for binlogs info, as L0Compaction only append deltalogs on existing segments
		}
	})

	plan.SegmentBinlogs = append(plan.SegmentBinlogs, sealedSegBinlogs...)
	log.Info("Compaction handler refreshed level zero compaction plan",
		zap.Any("target position", task.GetPos()),
		zap.Any("target segments count", len(sealedSegBinlogs)))
	handler.plans[task.GetPlanID()] = task.ShadowClone(setPlan(plan))
	return plan, nil
}
