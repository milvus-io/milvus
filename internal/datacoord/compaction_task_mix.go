package datacoord

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var _ CompactionTask = (*mixCompactionTask)(nil)

type mixCompactionTask struct {
	taskProto atomic.Value // *datapb.CompactionTask
	plan      *datapb.CompactionPlan
	result    *datapb.CompactionPlanResult

	span          trace.Span
	allocator     allocator.Allocator
	sessions      session.DataNodeManager
	meta          CompactionMeta
	newSegmentIDs []int64
	slotUsage     int64
}

func (t *mixCompactionTask) GetTaskProto() *datapb.CompactionTask {
	task := t.taskProto.Load()
	if task == nil {
		return nil
	}
	return task.(*datapb.CompactionTask)
}

func newMixCompactionTask(t *datapb.CompactionTask, allocator allocator.Allocator, meta CompactionMeta, session session.DataNodeManager) *mixCompactionTask {
	task := &mixCompactionTask{
		allocator: allocator,
		meta:      meta,
		sessions:  session,
		slotUsage: paramtable.Get().DataCoordCfg.MixCompactionSlotUsage.GetAsInt64(),
	}
	task.taskProto.Store(t)
	return task
}

func (t *mixCompactionTask) processPipelining() bool {
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()), zap.Int64("PlanID", t.GetTaskProto().GetPlanID()), zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()), zap.Int64("nodeID", t.GetTaskProto().GetNodeID()))
	if t.NeedReAssignNodeID() {
		log.Info("mixCompactionTask need assign nodeID")
		return false
	}

	var err error
	t.plan, err = t.BuildCompactionRequest()
	if err != nil {
		log.Warn("mixCompactionTask failed to build compaction request", zap.Error(err))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
		if err != nil {
			log.Warn("mixCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
			return false
		}
		return true
	}

	err = t.sessions.Compaction(context.TODO(), t.GetTaskProto().GetNodeID(), t.GetPlan())
	if err != nil {
		// Compaction tasks may be refused by DataNode because of slot limit. In this case, the node id is reset
		//  to enable a retry in compaction.checkCompaction().
		// This is tricky, we should remove the reassignment here.
		log.Warn("mixCompactionTask failed to notify compaction tasks to DataNode", zap.Int64("planID", t.GetTaskProto().GetPlanID()), zap.Error(err))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
		if err != nil {
			log.Warn("mixCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
		}
		return false
	}
	log.Info("mixCompactionTask notify compaction tasks to DataNode")

	err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_executing))
	if err != nil {
		log.Warn("mixCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
		return false
	}
	return false
}

func (t *mixCompactionTask) processMetaSaved() bool {
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()), zap.Int64("PlanID", t.GetTaskProto().GetPlanID()), zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
	ts := time.Now().Unix()
	updateOps := []compactionTaskOpt{setEndTime(ts), setState(datapb.CompactionTaskState_completed)}
	if err := t.updateAndSaveTaskMeta(updateOps...); err != nil {
		log.Warn("mixCompactionTask failed to proccessMetaSaved", zap.Error(err))
		return false
	}

	return t.processCompleted()
}

func (t *mixCompactionTask) processExecuting() bool {
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()), zap.Int64("PlanID", t.GetTaskProto().GetPlanID()), zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
	result, err := t.sessions.GetCompactionPlanResult(t.GetTaskProto().GetNodeID(), t.GetTaskProto().GetPlanID())
	if err != nil || result == nil {
		if errors.Is(err, merr.ErrNodeNotFound) {
			if err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID)); err != nil {
				log.Warn("mixCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
			}
		}
		log.Warn("mixCompactionTask failed to get compaction result", zap.Error(err))
		return false
	}

	ts := time.Now().Unix()
	failedUpdateOps := []compactionTaskOpt{setEndTime(ts), setState(datapb.CompactionTaskState_failed)}
	switch result.GetState() {
	case datapb.CompactionTaskState_completed:
		t.result = result
		if len(result.GetSegments()) == 0 {
			log.Info("illegal compaction results")
			err := t.updateAndSaveTaskMeta(failedUpdateOps...)
			if err != nil {
				log.Warn("mixCompactionTask failed to setState failed", zap.Error(err))
				return false
			}
			return true
		}
		if err := t.saveSegmentMeta(); err != nil {
			log.Warn("mixCompactionTask failed to save segment meta", zap.Error(err))
			if errors.Is(err, merr.ErrIllegalCompactionPlan) {
				err := t.updateAndSaveTaskMeta(failedUpdateOps...)
				if err != nil {
					log.Warn("mixCompactionTask failed to setState failed", zap.Error(err))
					return false
				}
				return true
			}
			return false
		}
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_meta_saved), setResultSegments(t.newSegmentIDs))
		if err != nil {
			log.Warn("mixCompaction failed to setState meta saved", zap.Error(err))
			return false
		}
		return t.processMetaSaved()
	case datapb.CompactionTaskState_failed:
		log.Info("mixCompactionTask fail in datanode")
		err := t.updateAndSaveTaskMeta(failedUpdateOps...)
		if err != nil {
			log.Warn("fail to updateAndSaveTaskMeta")
		}
		return true
	}
	return false
}

func (t *mixCompactionTask) saveTaskMeta(task *datapb.CompactionTask) error {
	return t.meta.SaveCompactionTask(context.TODO(), task)
}

func (t *mixCompactionTask) SaveTaskMeta() error {
	return t.saveTaskMeta(t.GetTaskProto())
}

func (t *mixCompactionTask) saveSegmentMeta() error {
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()), zap.Int64("PlanID", t.GetTaskProto().GetPlanID()), zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
	// Also prepare metric updates.
	newSegments, metricMutation, err := t.meta.CompleteCompactionMutation(context.TODO(), t.taskProto.Load().(*datapb.CompactionTask), t.result)
	if err != nil {
		return err
	}
	// Apply metrics after successful meta update.
	t.newSegmentIDs = lo.Map(newSegments, func(s *SegmentInfo, _ int) UniqueID { return s.GetID() })
	metricMutation.commit()
	log.Info("mixCompactionTask success to save segment meta")
	return nil
}

// Note: return True means exit this state machine.
// ONLY return True for Completed, Failed or Timeout
func (t *mixCompactionTask) Process() bool {
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()), zap.Int64("PlanID", t.GetTaskProto().GetPlanID()), zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
	lastState := t.GetTaskProto().GetState().String()
	processResult := true
	switch t.GetTaskProto().GetState() {
	case datapb.CompactionTaskState_pipelining:
		processResult = t.processPipelining()
	case datapb.CompactionTaskState_executing:
		processResult = t.processExecuting()
	case datapb.CompactionTaskState_meta_saved:
		processResult = t.processMetaSaved()
	case datapb.CompactionTaskState_completed:
		processResult = t.processCompleted()
	case datapb.CompactionTaskState_failed:
		processResult = t.processFailed()
	}
	currentState := t.GetTaskProto().GetState().String()
	if currentState != lastState {
		log.Info("mix compaction task state changed", zap.String("lastState", lastState), zap.String("currentState", currentState))
	}
	return processResult
}

func (t *mixCompactionTask) SetPlan(plan *datapb.CompactionPlan) {
	t.plan = plan
}

func (t *mixCompactionTask) GetResult() *datapb.CompactionPlanResult {
	return t.result
}

func (t *mixCompactionTask) GetPlan() *datapb.CompactionPlan {
	return t.plan
}

func (t *mixCompactionTask) GetLabel() string {
	return fmt.Sprintf("%d-%s", t.taskProto.Load().(*datapb.CompactionTask).PartitionID, t.GetTaskProto().GetChannel())
}

func (t *mixCompactionTask) NeedReAssignNodeID() bool {
	return t.GetTaskProto().GetState() == datapb.CompactionTaskState_pipelining && (t.GetTaskProto().GetNodeID() == 0 || t.GetTaskProto().GetNodeID() == NullNodeID)
}

func (t *mixCompactionTask) processCompleted() bool {
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()), zap.Int64("PlanID", t.GetTaskProto().GetPlanID()), zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
	if err := t.sessions.DropCompactionPlan(t.GetTaskProto().GetNodeID(), &datapb.DropCompactionPlanRequest{
		PlanID: t.GetTaskProto().GetPlanID(),
	}); err != nil {
		log.Warn("mixCompactionTask processCompleted unable to drop compaction plan")
	}

	t.resetSegmentCompacting()
	UpdateCompactionSegmentSizeMetrics(t.result.GetSegments())

	task := t.GetTaskProto()
	log.Info("mixCompactionTask processCompleted done",
		zap.Int64("planID", task.GetPlanID()), zap.Duration("costs", time.Duration(task.GetEndTime()-task.GetStartTime())*time.Second))
	return true
}

func (t *mixCompactionTask) resetSegmentCompacting() {
	t.meta.SetSegmentsCompacting(context.TODO(), t.taskProto.Load().(*datapb.CompactionTask).GetInputSegments(), false)
}

func (t *mixCompactionTask) ShadowClone(opts ...compactionTaskOpt) *datapb.CompactionTask {
	taskClone := proto.Clone(t.GetTaskProto()).(*datapb.CompactionTask)
	for _, opt := range opts {
		opt(taskClone)
	}
	return taskClone
}

func (t *mixCompactionTask) processFailed() bool {
	return true
}

func (t *mixCompactionTask) Clean() bool {
	return t.doClean() == nil
}

func (t *mixCompactionTask) doClean() error {
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()), zap.Int64("PlanID", t.GetTaskProto().GetPlanID()), zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
	if err := t.sessions.DropCompactionPlan(t.GetTaskProto().GetNodeID(), &datapb.DropCompactionPlanRequest{
		PlanID: t.GetTaskProto().GetPlanID(),
	}); err != nil {
		log.Warn("mixCompactionTask processFailed unable to drop compaction plan", zap.Error(err))
		return err
	}

	err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_cleaned))
	if err != nil {
		log.Warn("mixCompactionTask fail to updateAndSaveTaskMeta", zap.Error(err))
		return err
	}
	// resetSegmentCompacting must be the last step of Clean, to make sure resetSegmentCompacting only called once
	// otherwise, it may unlock segments locked by other compaction tasks
	t.resetSegmentCompacting()
	log.Info("mixCompactionTask clean done")
	return nil
}

func (t *mixCompactionTask) updateAndSaveTaskMeta(opts ...compactionTaskOpt) error {
	task := t.ShadowClone(opts...)
	err := t.saveTaskMeta(task)
	if err != nil {
		return err
	}
	t.SetTask(task)
	return nil
}

func (t *mixCompactionTask) SetNodeID(id UniqueID) error {
	return t.updateAndSaveTaskMeta(setNodeID(id))
}

func (t *mixCompactionTask) GetSpan() trace.Span {
	return t.span
}

func (t *mixCompactionTask) SetTask(task *datapb.CompactionTask) {
	t.taskProto.Store(task)
}

func (t *mixCompactionTask) SetSpan(span trace.Span) {
	t.span = span
}

func (t *mixCompactionTask) BuildCompactionRequest() (*datapb.CompactionPlan, error) {
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()), zap.Int64("PlanID", t.GetTaskProto().GetPlanID()), zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
	beginLogID, _, err := t.allocator.AllocN(1)
	if err != nil {
		return nil, err
	}
	taskProto := t.taskProto.Load().(*datapb.CompactionTask)
	plan := &datapb.CompactionPlan{
		PlanID:                 taskProto.GetPlanID(),
		StartTime:              taskProto.GetStartTime(),
		TimeoutInSeconds:       taskProto.GetTimeoutInSeconds(),
		Type:                   taskProto.GetType(),
		Channel:                taskProto.GetChannel(),
		CollectionTtl:          taskProto.GetCollectionTtl(),
		TotalRows:              taskProto.GetTotalRows(),
		Schema:                 taskProto.GetSchema(),
		BeginLogID:             beginLogID,
		PreAllocatedSegmentIDs: taskProto.GetPreAllocatedSegmentIDs(),
		SlotUsage:              t.GetSlotUsage(),
		MaxSize:                taskProto.GetMaxSize(),
	}

	segIDMap := make(map[int64][]*datapb.FieldBinlog, len(plan.SegmentBinlogs))
	for _, segID := range taskProto.GetInputSegments() {
		segInfo := t.meta.GetHealthySegment(context.TODO(), segID)
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
			IsSorted:            segInfo.GetIsSorted(),
		})
		segIDMap[segID] = segInfo.GetDeltalogs()
	}
	log.Info("Compaction handler refreshed mix compaction plan", zap.Int64("maxSize", plan.GetMaxSize()), zap.Any("segID2DeltaLogs", segIDMap))
	return plan, nil
}

func (t *mixCompactionTask) GetSlotUsage() int64 {
	return t.slotUsage
}
