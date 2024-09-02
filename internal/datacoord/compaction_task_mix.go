package datacoord

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

var _ CompactionTask = (*mixCompactionTask)(nil)

type mixCompactionTask struct {
	*datapb.CompactionTask
	plan   *datapb.CompactionPlan
	result *datapb.CompactionPlanResult

	span          trace.Span
	allocator     allocator.Allocator
	sessions      session.DataNodeManager
	meta          CompactionMeta
	newSegmentIDs []int64
}

func (t *mixCompactionTask) processPipelining() bool {
	if t.NeedReAssignNodeID() {
		return false
	}

	log := log.With(zap.Int64("triggerID", t.GetTriggerID()), zap.Int64("nodeID", t.GetNodeID()))
	var err error
	t.plan, err = t.BuildCompactionRequest()
	if err != nil {
		log.Warn("mixCompactionTask failed to build compaction request", zap.Error(err))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
		if err != nil {
			log.Warn("mixCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
			return false
		}
		return t.processFailed()
	}

	err = t.sessions.Compaction(context.TODO(), t.GetNodeID(), t.GetPlan())
	if err != nil {
		log.Warn("mixCompactionTask failed to notify compaction tasks to DataNode", zap.Int64("planID", t.GetPlanID()), zap.Error(err))
		t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
		return false
	}

	t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_executing))
	return false
}

func (t *mixCompactionTask) processMetaSaved() bool {
	if err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_completed)); err != nil {
		log.Warn("mixCompactionTask failed to proccessMetaSaved", zap.Error(err))
		return false
	}

	return t.processCompleted()
}

func (t *mixCompactionTask) processExecuting() bool {
	log := log.With(zap.Int64("planID", t.GetPlanID()), zap.String("type", t.GetType().String()))
	result, err := t.sessions.GetCompactionPlanResult(t.GetNodeID(), t.GetPlanID())
	if err != nil || result == nil {
		if errors.Is(err, merr.ErrNodeNotFound) {
			t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
		}
		log.Warn("mixCompactionTask failed to get compaction result", zap.Error(err))
		return false
	}
	switch result.GetState() {
	case datapb.CompactionTaskState_executing:
		if t.checkTimeout() {
			err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_timeout))
			if err != nil {
				log.Warn("mixCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
				return false
			}
			return t.processTimeout()
		}
	case datapb.CompactionTaskState_completed:
		t.result = result
		if len(result.GetSegments()) == 0 {
			log.Info("illegal compaction results")
			err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
			if err != nil {
				log.Warn("mixCompactionTask failed to setState failed", zap.Error(err))
				return false
			}
			return t.processFailed()
		}
		if err := t.saveSegmentMeta(); err != nil {
			log.Warn("mixCompactionTask failed to save segment meta", zap.Error(err))
			if errors.Is(err, merr.ErrIllegalCompactionPlan) {
				err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
				if err != nil {
					log.Warn("mixCompactionTask failed to setState failed", zap.Error(err))
					return false
				}
				return t.processFailed()
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
		err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
		if err != nil {
			log.Warn("fail to updateAndSaveTaskMeta")
		}
		return false
	}
	return false
}

func (t *mixCompactionTask) saveTaskMeta(task *datapb.CompactionTask) error {
	return t.meta.SaveCompactionTask(task)
}

func (t *mixCompactionTask) SaveTaskMeta() error {
	return t.saveTaskMeta(t.CompactionTask)
}

func (t *mixCompactionTask) saveSegmentMeta() error {
	log := log.With(zap.Int64("planID", t.GetPlanID()), zap.String("type", t.GetType().String()))
	// Also prepare metric updates.
	newSegments, metricMutation, err := t.meta.CompleteCompactionMutation(t.CompactionTask, t.result)
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
// ONLY return True for processCompleted or processFailed
func (t *mixCompactionTask) Process() bool {
	switch t.GetState() {
	case datapb.CompactionTaskState_pipelining:
		return t.processPipelining()
	case datapb.CompactionTaskState_executing:
		return t.processExecuting()
	case datapb.CompactionTaskState_timeout:
		return t.processTimeout()
	case datapb.CompactionTaskState_meta_saved:
		return t.processMetaSaved()
	case datapb.CompactionTaskState_completed:
		return t.processCompleted()
	case datapb.CompactionTaskState_failed:
		return t.processFailed()
	}
	return true
}

func (t *mixCompactionTask) GetResult() *datapb.CompactionPlanResult {
	return t.result
}

func (t *mixCompactionTask) GetPlan() *datapb.CompactionPlan {
	return t.plan
}

/*
func (t *mixCompactionTask) GetState() datapb.CompactionTaskState {
	return t.CompactionTask.GetState()
}
*/

func (t *mixCompactionTask) GetLabel() string {
	return fmt.Sprintf("%d-%s", t.PartitionID, t.GetChannel())
}

func (t *mixCompactionTask) NeedReAssignNodeID() bool {
	return t.GetState() == datapb.CompactionTaskState_pipelining && (t.GetNodeID() == 0 || t.GetNodeID() == NullNodeID)
}

func (t *mixCompactionTask) processCompleted() bool {
	if err := t.sessions.DropCompactionPlan(t.GetNodeID(), &datapb.DropCompactionPlanRequest{
		PlanID: t.GetPlanID(),
	}); err != nil {
		log.Warn("mixCompactionTask processCompleted unable to drop compaction plan", zap.Int64("planID", t.GetPlanID()))
	}

	t.resetSegmentCompacting()
	UpdateCompactionSegmentSizeMetrics(t.result.GetSegments())
	log.Info("mixCompactionTask processCompleted done", zap.Int64("planID", t.GetPlanID()))

	return true
}

func (t *mixCompactionTask) resetSegmentCompacting() {
	t.meta.SetSegmentsCompacting(t.GetInputSegments(), false)
}

func (t *mixCompactionTask) processTimeout() bool {
	t.resetSegmentCompacting()
	return true
}

func (t *mixCompactionTask) ShadowClone(opts ...compactionTaskOpt) *datapb.CompactionTask {
	taskClone := &datapb.CompactionTask{
		PlanID:           t.GetPlanID(),
		TriggerID:        t.GetTriggerID(),
		State:            t.GetState(),
		StartTime:        t.GetStartTime(),
		EndTime:          t.GetEndTime(),
		TimeoutInSeconds: t.GetTimeoutInSeconds(),
		Type:             t.GetType(),
		CollectionTtl:    t.CollectionTtl,
		CollectionID:     t.GetCollectionID(),
		PartitionID:      t.GetPartitionID(),
		Channel:          t.GetChannel(),
		InputSegments:    t.GetInputSegments(),
		ResultSegments:   t.GetResultSegments(),
		TotalRows:        t.TotalRows,
		Schema:           t.Schema,
		NodeID:           t.GetNodeID(),
		FailReason:       t.GetFailReason(),
		RetryTimes:       t.GetRetryTimes(),
		Pos:              t.GetPos(),
		MaxSize:          t.GetMaxSize(),
	}
	for _, opt := range opts {
		opt(taskClone)
	}
	return taskClone
}

func (t *mixCompactionTask) processFailed() bool {
	if err := t.sessions.DropCompactionPlan(t.GetNodeID(), &datapb.DropCompactionPlanRequest{
		PlanID: t.GetPlanID(),
	}); err != nil {
		log.Warn("mixCompactionTask processFailed unable to drop compaction plan", zap.Int64("planID", t.GetPlanID()), zap.Error(err))
	}

	log.Info("mixCompactionTask processFailed done", zap.Int64("planID", t.GetPlanID()))
	t.resetSegmentCompacting()
	return true
}

func (t *mixCompactionTask) checkTimeout() bool {
	if t.GetTimeoutInSeconds() > 0 {
		diff := time.Since(time.Unix(t.GetStartTime(), 0)).Seconds()
		if diff > float64(t.GetTimeoutInSeconds()) {
			log.Warn("compaction timeout",
				zap.Int32("timeout in seconds", t.GetTimeoutInSeconds()),
				zap.Int64("startTime", t.GetStartTime()),
			)
			return true
		}
	}
	return false
}

func (t *mixCompactionTask) updateAndSaveTaskMeta(opts ...compactionTaskOpt) error {
	task := t.ShadowClone(opts...)
	err := t.saveTaskMeta(task)
	if err != nil {
		return err
	}
	t.CompactionTask = task
	return nil
}

func (t *mixCompactionTask) SetNodeID(id UniqueID) error {
	return t.updateAndSaveTaskMeta(setNodeID(id))
}

func (t *mixCompactionTask) GetSpan() trace.Span {
	return t.span
}

func (t *mixCompactionTask) SetTask(task *datapb.CompactionTask) {
	t.CompactionTask = task
}

func (t *mixCompactionTask) SetSpan(span trace.Span) {
	t.span = span
}

/*
func (t *mixCompactionTask) SetPlan(plan *datapb.CompactionPlan) {
	t.plan = plan
}
*/

func (t *mixCompactionTask) SetResult(result *datapb.CompactionPlanResult) {
	t.result = result
}

func (t *mixCompactionTask) EndSpan() {
	if t.span != nil {
		t.span.End()
	}
}

func (t *mixCompactionTask) CleanLogPath() {
	if t.plan == nil {
		return
	}
	if t.plan.GetSegmentBinlogs() != nil {
		for _, binlogs := range t.plan.GetSegmentBinlogs() {
			binlogs.FieldBinlogs = nil
			binlogs.Field2StatslogPaths = nil
			binlogs.Deltalogs = nil
		}
	}
	if t.result.GetSegments() != nil {
		for _, segment := range t.result.GetSegments() {
			segment.InsertLogs = nil
			segment.Deltalogs = nil
			segment.Field2StatslogPaths = nil
		}
	}
}

func (t *mixCompactionTask) BuildCompactionRequest() (*datapb.CompactionPlan, error) {
	beginLogID, _, err := t.allocator.AllocN(1)
	if err != nil {
		return nil, err
	}
	plan := &datapb.CompactionPlan{
		PlanID:           t.GetPlanID(),
		StartTime:        t.GetStartTime(),
		TimeoutInSeconds: t.GetTimeoutInSeconds(),
		Type:             t.GetType(),
		Channel:          t.GetChannel(),
		CollectionTtl:    t.GetCollectionTtl(),
		TotalRows:        t.GetTotalRows(),
		Schema:           t.GetSchema(),
		BeginLogID:       beginLogID,
		PreAllocatedSegmentIDs: &datapb.IDRange{
			Begin: t.GetResultSegments()[0],
			End:   t.GetResultSegments()[1],
		},
		SlotUsage: Params.DataCoordCfg.MixCompactionSlotUsage.GetAsInt64(),
		MaxSize:   t.GetMaxSize(),
	}
	log := log.With(zap.Int64("taskID", t.GetTriggerID()), zap.Int64("planID", plan.GetPlanID()))

	segIDMap := make(map[int64][]*datapb.FieldBinlog, len(plan.SegmentBinlogs))
	for _, segID := range t.GetInputSegments() {
		segInfo := t.meta.GetHealthySegment(segID)
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
