package datacoord

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

var _ CompactionTask = (*mixCompactionTask)(nil)

type mixCompactionTask struct {
	*datapb.CompactionTask
	plan          *datapb.CompactionPlan
	result        *datapb.CompactionPlanResult
	span          trace.Span
	sessions      SessionManager
	meta          CompactionMeta
	newSegmentIDs []int64
}

func (t *mixCompactionTask) processPipelining() bool {
	log := log.With(zap.Int64("triggerID", t.GetTriggerID()), zap.Int64("PlanID", t.GetPlanID()), zap.Int64("collectionID", t.GetCollectionID()), zap.Int64("nodeID", t.GetNodeID()))
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

	err = t.sessions.Compaction(context.TODO(), t.GetNodeID(), t.GetPlan())
	if err != nil {
		log.Warn("mixCompactionTask failed to notify compaction tasks to DataNode", zap.Int64("planID", t.GetPlanID()), zap.Error(err))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
		if err != nil {
			log.Warn("mixCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
		}
		return false
	}
	log.Warn("mixCompactionTask notify compaction tasks to DataNode")

	err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_executing))
	if err != nil {
		log.Warn("mixCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
	}
	return false
}

func (t *mixCompactionTask) processMetaSaved() bool {
	log := log.With(zap.Int64("triggerID", t.GetTriggerID()), zap.Int64("PlanID", t.GetPlanID()), zap.Int64("collectionID", t.GetCollectionID()))
	if err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_completed)); err != nil {
		log.Warn("mixCompactionTask failed to proccessMetaSaved", zap.Error(err))
		return false
	}

	return t.processCompleted()
}

func (t *mixCompactionTask) processExecuting() bool {
	log := log.With(zap.Int64("triggerID", t.GetTriggerID()), zap.Int64("PlanID", t.GetPlanID()), zap.Int64("collectionID", t.GetCollectionID()))
	result, err := t.sessions.GetCompactionPlanResult(t.GetNodeID(), t.GetPlanID())
	if err != nil || result == nil {
		if errors.Is(err, merr.ErrNodeNotFound) {
			if err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID)); err != nil {
				log.Warn("mixCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
			}
		}
		log.Warn("mixCompactionTask failed to get compaction result", zap.Error(err))
		return false
	}
	switch result.GetState() {
	case datapb.CompactionTaskState_executing:
		if t.checkTimeout() {
			log.Info("mixCompactionTask timeout", zap.Int32("timeout in seconds", t.GetTimeoutInSeconds()), zap.Int64("startTime", t.GetStartTime()))
			err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_timeout))
			if err != nil {
				log.Warn("mixCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
				return false
			}
			return true
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
			return true
		}

		// update the tmp segmentIDs first, revert the segments if CompleteCompactionMutation fails
		resultSegmentIDs := lo.Map(result.Segments, func(segment *datapb.CompactionSegment, _ int) int64 {
			return segment.GetSegmentID()
		})
		err = t.updateAndSaveTaskMeta(setResultSegments(resultSegmentIDs))
		if err != nil {
			log.Warn("mixCompactionTask failed to setResultSegments failed", zap.Error(err))
			return false
		}

		if err := t.saveSegmentMeta(); err != nil {
			log.Warn("mixCompactionTask failed to save segment meta", zap.Error(err))
			if errors.Is(err, merr.ErrIllegalCompactionPlan) {
				err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
				if err != nil {
					log.Warn("mixCompactionTask failed to setState failed", zap.Error(err))
					return false
				}
				return true
			}
			return false
		}
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_meta_saved), setCompactionCommited(true))
		if err != nil {
			log.Warn("mixCompaction failed to setState meta saved", zap.Error(err))
			return false
		}
		return t.processMetaSaved()
	case datapb.CompactionTaskState_failed:
		log.Info("mixCompactionTask fail in datanode")
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
	log := log.With(zap.Int64("triggerID", t.GetTriggerID()), zap.Int64("PlanID", t.GetPlanID()), zap.Int64("collectionID", t.GetCollectionID()))
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
	log := log.With(zap.Int64("triggerID", t.GetTriggerID()), zap.Int64("PlanID", t.GetPlanID()), zap.Int64("collectionID", t.GetCollectionID()))
	lastState := t.GetState().String()
	processResult := true
	switch t.GetState() {
	case datapb.CompactionTaskState_pipelining:
		processResult = t.processPipelining()
	case datapb.CompactionTaskState_executing:
		return t.processExecuting()
	case datapb.CompactionTaskState_meta_saved:
		processResult = t.processMetaSaved()
	case datapb.CompactionTaskState_completed:
		return t.processCompleted()
	}
	currentState := t.GetState().String()
	if currentState != lastState {
		log.Info("mix compaction task state changed", zap.String("lastState", lastState), zap.String("currentState", currentState))
	}
	return processResult
}

func (t *mixCompactionTask) GetResult() *datapb.CompactionPlanResult {
	return t.result
}

func (t *mixCompactionTask) GetPlan() *datapb.CompactionPlan {
	return t.plan
}

func (t *mixCompactionTask) GetLabel() string {
	return fmt.Sprintf("%d-%s", t.PartitionID, t.GetChannel())
}

func (t *mixCompactionTask) NeedReAssignNodeID() bool {
	return t.GetState() == datapb.CompactionTaskState_pipelining && (t.GetNodeID() == 0 || t.GetNodeID() == NullNodeID)
}

func (t *mixCompactionTask) processCompleted() bool {
	log := log.With(zap.Int64("triggerID", t.GetTriggerID()), zap.Int64("PlanID", t.GetPlanID()), zap.Int64("collectionID", t.GetCollectionID()))
	if err := t.sessions.DropCompactionPlan(t.GetNodeID(), &datapb.DropCompactionPlanRequest{
		PlanID: t.GetPlanID(),
	}); err != nil {
		log.Warn("mixCompactionTask processCompleted unable to drop compaction plan")
	}

	t.resetSegmentCompacting()
	UpdateCompactionSegmentSizeMetrics(t.result.GetSegments())
	log.Info("mixCompactionTask processCompleted done")

	return true
}

func (t *mixCompactionTask) resetSegmentCompacting() {
	t.meta.SetSegmentsCompacting(t.GetInputSegments(), false)
}

func (t *mixCompactionTask) ShadowClone(opts ...compactionTaskOpt) *datapb.CompactionTask {
	pbClone := proto.Clone(t).(*datapb.CompactionTask)
	for _, opt := range opts {
		opt(pbClone)
	}
	return pbClone
}

func (t *mixCompactionTask) Clean() bool {
	err := t.doClean()
	return err == nil
}

func (t *mixCompactionTask) doClean() error {
	if err := t.sessions.DropCompactionPlan(t.GetNodeID(), &datapb.DropCompactionPlanRequest{
		PlanID: t.GetPlanID(),
	}); err != nil {
		log.Warn("mixCompactionTask processFailed unable to drop compaction plan", zap.Int64("planID", t.GetPlanID()), zap.Error(err))
		return err
	}

	log.Info("mixCompactionTask processFailed done")
	t.resetSegmentCompacting()
	err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_cleaned))
	if err != nil {
		log.Warn("mixCompactionTask processFailed unable to drop compaction plan", zap.Int64("planID", t.GetPlanID()), zap.Error(err))
		return err
	}
	return nil
}

func (t *mixCompactionTask) checkTimeout() bool {
	if t.GetTimeoutInSeconds() > 0 {
		diff := time.Since(time.Unix(t.GetStartTime(), 0)).Seconds()
		if diff > float64(t.GetTimeoutInSeconds()) {
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
	log := log.With(zap.Int64("triggerID", t.GetTriggerID()), zap.Int64("PlanID", t.GetPlanID()), zap.Int64("collectionID", t.GetCollectionID()))
	plan := &datapb.CompactionPlan{
		PlanID:           t.GetPlanID(),
		StartTime:        t.GetStartTime(),
		TimeoutInSeconds: t.GetTimeoutInSeconds(),
		Type:             t.GetType(),
		Channel:          t.GetChannel(),
		CollectionTtl:    t.GetCollectionTtl(),
		TotalRows:        t.GetTotalRows(),
		Schema:           t.GetSchema(),
		SlotUsage:        Params.DataCoordCfg.MixCompactionSlotUsage.GetAsInt64(),
		MaxSize:          t.GetMaxSize(),
	}

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
		})
		segIDMap[segID] = segInfo.GetDeltalogs()
	}
	log.Info("Compaction handler refreshed mix compaction plan", zap.Int64("maxSize", plan.GetMaxSize()), zap.Any("segID2DeltaLogs", segIDMap))
	return plan, nil
}
