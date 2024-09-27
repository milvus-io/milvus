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

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var _ CompactionTask = (*vshardSplitCompactionTask)(nil)

type vshardSplitCompactionTask struct {
	*datapb.CompactionTask
	plan   *datapb.CompactionPlan
	result *datapb.CompactionPlanResult

	span          trace.Span
	allocator     allocator.Allocator
	sessions      session.DataNodeManager
	meta          CompactionMeta
	vshardManager VshardManager

	newSegmentIDs []int64

	slotUsage int64
}

func newVshardSplitCompactionTask(t *datapb.CompactionTask, allocator allocator.Allocator, meta CompactionMeta, session session.DataNodeManager, vshardManager VshardManager) *vshardSplitCompactionTask {
	return &vshardSplitCompactionTask{
		CompactionTask: t,
		allocator:      allocator,
		meta:           meta,
		sessions:       session,
		vshardManager:  vshardManager,
		slotUsage:      paramtable.Get().DataCoordCfg.MixCompactionSlotUsage.GetAsInt64(),
	}
}

func (t *vshardSplitCompactionTask) processPipelining() bool {
	if t.NeedReAssignNodeID() {
		return false
	}

	log := log.With(zap.Int64("triggerID", t.GetTriggerID()), zap.Int64("nodeID", t.GetNodeID()))
	var err error
	t.plan, err = t.BuildCompactionRequest()
	if err != nil {
		log.Warn("vshard split CompactionTask failed to build compaction request", zap.Error(err))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
		if err != nil {
			log.Warn("vshard split CompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
			return false
		}
		return t.processFailed()
	}

	err = t.sessions.Compaction(context.TODO(), t.GetNodeID(), t.GetPlan())
	if err != nil {
		log.Warn("vshard split CompactionTask failed to notify compaction tasks to DataNode", zap.Int64("planID", t.GetPlanID()), zap.Error(err))
		t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
		return false
	}

	t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_executing))
	return false
}

func (t *vshardSplitCompactionTask) processMetaSaved() bool {
	if err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_completed)); err != nil {
		log.Warn("vshard split CompactionTask failed to proccessMetaSaved", zap.Error(err))
		return false
	}

	return t.processCompleted()
}

func (t *vshardSplitCompactionTask) processExecuting() bool {
	log := log.With(zap.Int64("planID", t.GetPlanID()), zap.String("type", t.GetType().String()))
	result, err := t.sessions.GetCompactionPlanResult(t.GetNodeID(), t.GetPlanID())
	if err != nil || result == nil {
		if errors.Is(err, merr.ErrNodeNotFound) {
			t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
		}
		log.Warn("vshard split CompactionTask failed to get compaction result", zap.Error(err))
		return false
	}
	switch result.GetState() {
	case datapb.CompactionTaskState_executing:
		if t.checkTimeout() {
			err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_timeout))
			if err != nil {
				log.Warn("vshard split CompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
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
				log.Warn("vshard split CompactionTask failed to setState failed", zap.Error(err))
				return false
			}
			return t.processFailed()
		}
		if err := t.saveSegmentMeta(); err != nil {
			log.Warn("vshard split CompactionTask failed to save segment meta", zap.Error(err))
			if errors.Is(err, merr.ErrIllegalCompactionPlan) {
				err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
				if err != nil {
					log.Warn("vshard split CompactionTask failed to setState failed", zap.Error(err))
					return false
				}
				return t.processFailed()
			}
			return false
		}
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_meta_saved), setResultSegments(t.newSegmentIDs))
		if err != nil {
			log.Warn("vshard split Compaction failed to setState meta saved", zap.Error(err))
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

func (t *vshardSplitCompactionTask) saveTaskMeta(task *datapb.CompactionTask) error {
	return t.meta.SaveCompactionTask(task)
}

func (t *vshardSplitCompactionTask) SaveTaskMeta() error {
	return t.saveTaskMeta(t.CompactionTask)
}

func (t *vshardSplitCompactionTask) saveSegmentMeta() error {
	log := log.With(zap.Int64("planID", t.GetPlanID()), zap.String("type", t.GetType().String()), zap.Int64("vshardTaskId", t.GetVshardTaskId()))

	for _, seg := range t.result.GetSegments() {
		log.Debug("compaction result segment", zap.Int64("segmentID", seg.SegmentID), zap.String("vshard", seg.Vshard.String()))
	}

	if t.VshardTaskId != 0 {
		err := t.vshardManager.ReportVShardCompaction(t.PartitionID, t.VshardTaskId)
		if err != nil {
			log.Error("fail to report vshard compaction done", zap.Error(err))
			return err
		}
	}

	// Also prepare metric updates.
	newSegments, metricMutation, err := t.meta.CompleteCompactionMutation(t.CompactionTask, t.result)
	if err != nil {
		log.Error("fail to CompleteCompactionMutation", zap.Error(err))
		return err
	}
	// Apply metrics after successful meta update.
	t.newSegmentIDs = lo.Map(newSegments, func(s *SegmentInfo, _ int) UniqueID { return s.GetID() })
	metricMutation.commit()
	log.Info("vshard split CompactionTask success to save segment meta")

	return nil
}

// Note: return True means exit this state machine.
// ONLY return True for processCompleted or processFailed
func (t *vshardSplitCompactionTask) Process() bool {
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

func (t *vshardSplitCompactionTask) GetResult() *datapb.CompactionPlanResult {
	return t.result
}

func (t *vshardSplitCompactionTask) GetPlan() *datapb.CompactionPlan {
	return t.plan
}

func (t *vshardSplitCompactionTask) GetLabel() string {
	return fmt.Sprintf("%d-%s", t.PartitionID, t.GetChannel())
}

func (t *vshardSplitCompactionTask) NeedReAssignNodeID() bool {
	return t.GetState() == datapb.CompactionTaskState_pipelining && (t.GetNodeID() == 0 || t.GetNodeID() == NullNodeID)
}

func (t *vshardSplitCompactionTask) processCompleted() bool {
	if err := t.sessions.DropCompactionPlan(t.GetNodeID(), &datapb.DropCompactionPlanRequest{
		PlanID: t.GetPlanID(),
	}); err != nil {
		log.Warn("vshard split CompactionTask processCompleted unable to drop compaction plan", zap.Int64("planID", t.GetPlanID()))
	}

	t.resetSegmentCompacting()
	UpdateCompactionSegmentSizeMetrics(t.result.GetSegments())
	log.Info("vshard split CompactionTask processCompleted done", zap.Int64("planID", t.GetPlanID()))

	return true
}

func (t *vshardSplitCompactionTask) resetSegmentCompacting() {
	t.meta.SetSegmentsCompacting(t.GetInputSegments(), false)
}

func (t *vshardSplitCompactionTask) processTimeout() bool {
	t.resetSegmentCompacting()
	return true
}

func (t *vshardSplitCompactionTask) ShadowClone(opts ...compactionTaskOpt) *datapb.CompactionTask {
	pbClone := proto.Clone(t).(*datapb.CompactionTask)
	for _, opt := range opts {
		opt(pbClone)
	}
	return pbClone
}

func (t *vshardSplitCompactionTask) processFailed() bool {
	if err := t.sessions.DropCompactionPlan(t.GetNodeID(), &datapb.DropCompactionPlanRequest{
		PlanID: t.GetPlanID(),
	}); err != nil {
		log.Warn("vshard split CompactionTask processFailed unable to drop compaction plan", zap.Int64("planID", t.GetPlanID()), zap.Error(err))
	}

	log.Info("vshard split CompactionTask processFailed done", zap.Int64("planID", t.GetPlanID()))
	t.resetSegmentCompacting()
	return true
}

func (t *vshardSplitCompactionTask) checkTimeout() bool {
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

func (t *vshardSplitCompactionTask) updateAndSaveTaskMeta(opts ...compactionTaskOpt) error {
	task := t.ShadowClone(opts...)
	err := t.saveTaskMeta(task)
	if err != nil {
		return err
	}
	t.CompactionTask = task
	return nil
}

func (t *vshardSplitCompactionTask) SetNodeID(id UniqueID) error {
	return t.updateAndSaveTaskMeta(setNodeID(id))
}

func (t *vshardSplitCompactionTask) GetSpan() trace.Span {
	return t.span
}

func (t *vshardSplitCompactionTask) SetTask(task *datapb.CompactionTask) {
	t.CompactionTask = task
}

func (t *vshardSplitCompactionTask) SetSpan(span trace.Span) {
	t.span = span
}

func (t *vshardSplitCompactionTask) SetResult(result *datapb.CompactionPlanResult) {
	t.result = result
}

func (t *vshardSplitCompactionTask) EndSpan() {
	if t.span != nil {
		t.span.End()
	}
}

func (t *vshardSplitCompactionTask) BuildCompactionRequest() (*datapb.CompactionPlan, error) {
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
		MaxSegmentRows: t.GetMaxSegmentRows(),
		SlotUsage:      Params.DataCoordCfg.MixCompactionSlotUsage.GetAsInt64(),
		MaxSize:        t.GetMaxSize(),
		Vshards:        t.GetToVshards(),
	}
	vshardsStr := lo.Map(t.GetToVshards(), func(vshard *datapb.VShardDesc, _ int) string {
		return vshard.String()
	})
	log := log.With(zap.Int64("taskID", t.GetTriggerID()), zap.Int64("planID", plan.GetPlanID()), zap.Strings("vshard", vshardsStr))

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
	log.Info("Compaction handler refreshed vshard compaction plan", zap.Int64("maxSize", plan.GetMaxSize()), zap.Any("segID2DeltaLogs", segIDMap))
	return plan, nil
}

func (t *vshardSplitCompactionTask) CleanLogPath() {
	// TODO implement me
	panic("implement me")
}

func (t *vshardSplitCompactionTask) GetSlotUsage() int64 {
	return t.slotUsage
}
