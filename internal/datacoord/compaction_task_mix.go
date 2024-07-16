package datacoord

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

var _ CompactionTask = (*mixCompactionTask)(nil)

type mixCompactionTask struct {
	pbGuard    sync.RWMutex
	pb         *datapb.CompactionTask
	plan       *datapb.CompactionPlan
	result     *datapb.CompactionPlanResult
	span       trace.Span
	allocator  allocator
	sessions   SessionManager
	meta       CompactionMeta
	newSegment *SegmentInfo
}

func (t *mixCompactionTask) GetTriggerID() UniqueID {
	return t.GetTaskPB().GetTriggerID()
}

func (t *mixCompactionTask) GetPlanID() UniqueID {
	return t.GetTaskPB().GetPlanID()
}

func (t *mixCompactionTask) GetState() datapb.CompactionTaskState {
	return t.GetTaskPB().GetState()
}

func (t *mixCompactionTask) GetChannel() string {
	return t.GetTaskPB().GetChannel()
}

func (t *mixCompactionTask) GetType() datapb.CompactionType {
	return t.GetTaskPB().GetType()
}

func (t *mixCompactionTask) GetCollectionID() int64 {
	return t.GetTaskPB().GetCollectionID()
}

func (t *mixCompactionTask) GetPartitionID() int64 {
	return t.GetTaskPB().GetPartitionID()
}

func (t *mixCompactionTask) GetInputSegments() []int64 {
	return t.GetTaskPB().GetInputSegments()
}

func (t *mixCompactionTask) GetStartTime() int64 {
	return t.GetTaskPB().GetStartTime()
}

func (t *mixCompactionTask) GetTimeoutInSeconds() int32 {
	return t.GetTaskPB().GetTimeoutInSeconds()
}

func (t *mixCompactionTask) GetNodeID() UniqueID {
	return t.GetTaskPB().GetNodeID()
}

func (t *mixCompactionTask) GetPos() *msgpb.MsgPosition {
	return t.GetTaskPB().GetPos()
}

func (t *mixCompactionTask) GetTaskPB() *datapb.CompactionTask {
	t.pbGuard.RLock()
	defer t.pbGuard.RUnlock()
	return t.pb
}

func (t *mixCompactionTask) processPipelining() bool {
	if t.NeedReAssignNodeID() {
		return false
	}

	log := log.With(zap.Int64("triggerID", t.GetTriggerID()), zap.Int64("nodeID", t.GetNodeID()))
	var err error
	t.plan, err = t.BuildCompactionRequest()
	// Segment not found
	if err != nil {
		log.Warn("mixCompactionTask failed to build compaction request", zap.Error(err))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
		if err != nil {
			log.Warn("mixCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
			return false
		}
		return t.processFailed()
	}
	err = t.sessions.Compaction(context.Background(), t.pb.GetNodeID(), t.GetPlan())
	if err != nil {
		log.Warn("Failed to notify compaction tasks to DataNode", zap.Error(err))
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
	log := log.With(zap.Int64("planID", t.pb.GetPlanID()), zap.String("type", t.pb.GetType().String()))
	result, err := t.sessions.GetCompactionPlanResult(t.pb.GetNodeID(), t.pb.GetPlanID())
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
		if len(result.GetSegments()) == 0 || len(result.GetSegments()) > 1 {
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
		segments := []UniqueID{t.newSegment.GetID()}
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_meta_saved), setResultSegments(segments))
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
	return t.saveTaskMeta(t.GetTaskPB())
}

func (t *mixCompactionTask) saveSegmentMeta() error {
	log := log.With(zap.Int64("planID", t.pb.GetPlanID()), zap.String("type", t.pb.GetType().String()))
	// Also prepare metric updates.
	newSegments, metricMutation, err := t.meta.CompleteCompactionMutation(t.pb, t.result)
	if err != nil {
		return err
	}
	// Apply metrics after successful meta update.
	t.newSegment = newSegments[0]
	metricMutation.commit()
	log.Info("mixCompactionTask success to save segment meta")
	return nil
}

// Note: return True means exit this state machine.
// ONLY return True for processCompleted or processFailed
func (t *mixCompactionTask) Process() bool {
	switch t.pb.GetState() {
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

func (t *mixCompactionTask) GetLabel() string {
	pb := t.GetTaskPB()
	return fmt.Sprintf("%d-%s", pb.GetPartitionID(), pb.GetChannel())
}

func (t *mixCompactionTask) NeedReAssignNodeID() bool {
	pb := t.GetTaskPB()
	return pb.GetState() == datapb.CompactionTaskState_pipelining && (pb.GetNodeID() == 0 || pb.GetNodeID() == NullNodeID)
}

func (t *mixCompactionTask) processCompleted() bool {
	if err := t.sessions.DropCompactionPlan(t.pb.GetNodeID(), &datapb.DropCompactionPlanRequest{
		PlanID: t.pb.GetPlanID(),
	}); err != nil {
		log.Warn("mixCompactionTask processCompleted unable to drop compaction plan", zap.Int64("planID", t.pb.GetPlanID()))
	}

	t.resetSegmentCompacting()
	UpdateCompactionSegmentSizeMetrics(t.result.GetSegments())
	log.Info("mixCompactionTask processCompleted done", zap.Int64("planID", t.pb.GetPlanID()))

	return true
}

func (t *mixCompactionTask) resetSegmentCompacting() {
	t.meta.SetSegmentsCompacting(t.pb.GetInputSegments(), false)
}

func (t *mixCompactionTask) processTimeout() bool {
	t.resetSegmentCompacting()
	return true
}

func (t *mixCompactionTask) CloneTaskPB(opts ...compactionTaskOpt) *datapb.CompactionTask {
	pbClone := proto.Clone(t.GetTaskPB()).(*datapb.CompactionTask)
	for _, opt := range opts {
		opt(pbClone)
	}
	return pbClone
}

func (t *mixCompactionTask) processFailed() bool {
	if err := t.sessions.DropCompactionPlan(t.pb.GetNodeID(), &datapb.DropCompactionPlanRequest{
		PlanID: t.pb.GetPlanID(),
	}); err != nil {
		log.Warn("mixCompactionTask processFailed unable to drop compaction plan", zap.Int64("planID", t.pb.GetPlanID()), zap.Error(err))
	}

	log.Info("mixCompactionTask processFailed done", zap.Int64("planID", t.pb.GetPlanID()))
	t.resetSegmentCompacting()
	return true
}

func (t *mixCompactionTask) checkTimeout() bool {
	if t.pb.GetTimeoutInSeconds() > 0 {
		diff := time.Since(time.Unix(t.pb.GetStartTime(), 0)).Seconds()
		if diff > float64(t.pb.GetTimeoutInSeconds()) {
			log.Warn("compaction timeout",
				zap.Int32("timeout in seconds", t.pb.GetTimeoutInSeconds()),
				zap.Int64("startTime", t.pb.GetStartTime()),
			)
			return true
		}
	}
	return false
}

func (t *mixCompactionTask) updateAndSaveTaskMeta(opts ...compactionTaskOpt) error {
	t.pbGuard.Lock()
	defer t.pbGuard.Unlock()
	pbCloned := proto.Clone(t.pb).(*datapb.CompactionTask)
	for _, opt := range opts {
		opt(pbCloned)
	}
	err := t.saveTaskMeta(pbCloned)
	if err != nil {
		return err
	}
	t.pb = pbCloned
	return nil
}

func (t *mixCompactionTask) SetNodeID(id UniqueID) error {
	return t.updateAndSaveTaskMeta(setNodeID(id))
}

func (t *mixCompactionTask) GetSpan() trace.Span {
	return t.span
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

func (t *mixCompactionTask) BuildCompactionRequest() (*datapb.CompactionPlan, error) {
	beginLogID, _, err := t.allocator.allocN(1)
	if err != nil {
		return nil, err
	}
	plan := &datapb.CompactionPlan{
		PlanID:           t.pb.GetPlanID(),
		StartTime:        t.pb.GetStartTime(),
		TimeoutInSeconds: t.pb.GetTimeoutInSeconds(),
		Type:             t.pb.GetType(),
		Channel:          t.pb.GetChannel(),
		CollectionTtl:    t.pb.GetCollectionTtl(),
		TotalRows:        t.pb.GetTotalRows(),
		Schema:           t.pb.GetSchema(),
		BeginLogID:       beginLogID,
		PreAllocatedSegments: &datapb.IDRange{
			Begin: t.pb.GetResultSegments()[0],
		},
		SlotUsage: Params.DataCoordCfg.MixCompactionSlotUsage.GetAsInt64(),
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
		})
		segIDMap[segID] = segInfo.GetDeltalogs()
	}
	log.Info("Compaction handler refreshed mix compaction plan", zap.Any("segID2DeltaLogs", segIDMap))
	return plan, nil
}
