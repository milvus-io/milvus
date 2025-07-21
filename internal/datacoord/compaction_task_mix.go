package datacoord

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var _ CompactionTask = (*mixCompactionTask)(nil)

type mixCompactionTask struct {
	taskProto atomic.Value // *datapb.CompactionTask

	allocator allocator.Allocator
	meta      CompactionMeta

	ievm IndexEngineVersionManager

	times *taskcommon.Times

	slotUsage atomic.Int64
}

func (t *mixCompactionTask) GetTaskID() int64 {
	return t.GetTaskProto().GetPlanID()
}

func (t *mixCompactionTask) GetTaskType() taskcommon.Type {
	return taskcommon.Compaction
}

func (t *mixCompactionTask) GetTaskState() taskcommon.State {
	return taskcommon.FromCompactionState(t.GetTaskProto().GetState())
}

func (t *mixCompactionTask) GetTaskSlot() int64 {
	slotUsage := t.slotUsage.Load()
	if slotUsage == 0 {
		slotUsage = paramtable.Get().DataCoordCfg.MixCompactionSlotUsage.GetAsInt64()
		if t.GetTaskProto().GetType() == datapb.CompactionType_SortCompaction {
			segment := t.meta.GetHealthySegment(context.Background(), t.GetTaskProto().GetInputSegments()[0])
			if segment != nil {
				slotUsage = calculateStatsTaskSlot(segment.getSegmentSize())
			}
		}
		t.slotUsage.Store(slotUsage)
	}
	return slotUsage
}

func (t *mixCompactionTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	t.times.SetTaskTime(timeType, time)
}

func (t *mixCompactionTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(t.times)
}

func (t *mixCompactionTask) GetTaskVersion() int64 {
	return int64(t.GetTaskProto().GetRetryTimes())
}

func (t *mixCompactionTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
		zap.Int64("PlanID", t.GetTaskProto().GetPlanID()),
		zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()),
		zap.Int64("nodeID", nodeID))

	plan, err := t.BuildCompactionRequest()
	if err != nil {
		log.Warn("mixCompactionTask failed to build compaction request", zap.Error(err))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
		if err != nil {
			log.Warn("mixCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
		}
		return
	}

	err = cluster.CreateCompaction(nodeID, plan)
	if err != nil {
		// Compaction tasks may be refused by DataNode because of slot limit. In this case, the node id is reset
		//  to enable a retry in compaction.checkCompaction().
		// This is tricky, we should remove the reassignment here.
		originNodeID := t.GetTaskProto().GetNodeID()
		log.Warn("mixCompactionTask failed to notify compaction tasks to DataNode",
			zap.Int64("planID", t.GetTaskProto().GetPlanID()),
			zap.Int64("nodeID", originNodeID),
			zap.Error(err))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
		if err != nil {
			log.Warn("mixCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
		}
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", originNodeID), t.GetTaskProto().GetType().String(), metrics.Executing).Dec()
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", NullNodeID), t.GetTaskProto().GetType().String(), metrics.Pending).Inc()
		return
	}
	log.Info("mixCompactionTask notify compaction tasks to DataNode")

	err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_executing), setNodeID(nodeID))
	if err != nil {
		log.Warn("mixCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
	}
}

func (t *mixCompactionTask) QueryTaskOnWorker(cluster session.Cluster) {
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
		zap.Int64("PlanID", t.GetTaskProto().GetPlanID()),
		zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
	result, err := cluster.QueryCompaction(t.GetTaskProto().GetNodeID(), &datapb.CompactionStateRequest{
		PlanID: t.GetTaskProto().GetPlanID(),
	})
	if err != nil || result == nil {
		if errors.Is(err, merr.ErrNodeNotFound) {
			if err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID)); err != nil {
				log.Warn("mixCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
			}
		}
		log.Warn("mixCompactionTask failed to get compaction result", zap.Error(err))
		return
	}
	switch result.GetState() {
	case datapb.CompactionTaskState_completed:
		if len(result.GetSegments()) == 0 {
			log.Info("illegal compaction results")
			err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
			if err != nil {
				log.Warn("mixCompactionTask failed to setState failed", zap.Error(err))
			}
			return
		}
		if err := t.saveSegmentMeta(result); err != nil {
			log.Warn("mixCompactionTask failed to save segment meta", zap.Error(err))
			if errors.Is(err, merr.ErrIllegalCompactionPlan) {
				err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
				if err != nil {
					log.Warn("mixCompactionTask failed to setState failed", zap.Error(err))
				}
			}
			return
		}
		UpdateCompactionSegmentSizeMetrics(result.GetSegments())
		t.processMetaSaved()
	case datapb.CompactionTaskState_failed:
		log.Info("mixCompactionTask fail in datanode")
		err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
		if err != nil {
			log.Warn("fail to updateAndSaveTaskMeta")
		}
	}
}

func (t *mixCompactionTask) DropTaskOnWorker(cluster session.Cluster) {
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
		zap.Int64("PlanID", t.GetTaskProto().GetPlanID()),
		zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
	if err := cluster.DropCompaction(t.GetTaskProto().GetNodeID(), t.GetTaskProto().GetPlanID()); err != nil {
		log.Warn("mixCompactionTask processCompleted unable to drop compaction plan")
	}
}

func (t *mixCompactionTask) GetTaskProto() *datapb.CompactionTask {
	task := t.taskProto.Load()
	if task == nil {
		return nil
	}
	return task.(*datapb.CompactionTask)
}

func newMixCompactionTask(t *datapb.CompactionTask,
	allocator allocator.Allocator,
	meta CompactionMeta,
	ievm IndexEngineVersionManager,
) *mixCompactionTask {
	task := &mixCompactionTask{
		allocator: allocator,
		meta:      meta,
		ievm:      ievm,
		times:     taskcommon.NewTimes(),
	}
	task.taskProto.Store(t)
	return task
}

func (t *mixCompactionTask) processMetaSaved() bool {
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()), zap.Int64("PlanID", t.GetTaskProto().GetPlanID()), zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
	if err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_completed)); err != nil {
		log.Warn("mixCompactionTask failed to proccessMetaSaved", zap.Error(err))
		return false
	}

	return t.processCompleted()
}

func (t *mixCompactionTask) saveTaskMeta(task *datapb.CompactionTask) error {
	return t.meta.SaveCompactionTask(context.TODO(), task)
}

func (t *mixCompactionTask) SaveTaskMeta() error {
	return t.saveTaskMeta(t.GetTaskProto())
}

func (t *mixCompactionTask) saveSegmentMeta(result *datapb.CompactionPlanResult) error {
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()), zap.Int64("PlanID", t.GetTaskProto().GetPlanID()), zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
	// Also prepare metric updates.
	newSegments, metricMutation, err := t.meta.CompleteCompactionMutation(context.TODO(), t.taskProto.Load().(*datapb.CompactionTask), result)
	if err != nil {
		return err
	}
	// Apply metrics after successful meta update.
	newSegmentIDs := lo.Map(newSegments, func(s *SegmentInfo, _ int) UniqueID { return s.GetID() })
	metricMutation.commit()
	for _, newSegID := range newSegmentIDs {
		select {
		case getBuildIndexChSingleton() <- newSegID:
		default:
		}
	}

	err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_meta_saved), setResultSegments(newSegmentIDs))
	if err != nil {
		log.Warn("mixCompaction failed to setState meta saved", zap.Error(err))
		return err
	}
	log.Info("mixCompactionTask success to save segment meta")
	return nil
}

// Note: return True means exit this state machine.
// ONLY return True for Completed, Failed or Timeout
func (t *mixCompactionTask) Process() bool {
	log := log.With(zap.Int64("triggerID",
		t.GetTaskProto().GetTriggerID()),
		zap.Int64("PlanID", t.GetTaskProto().GetPlanID()),
		zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
	lastState := t.GetTaskProto().GetState().String()
	processResult := false
	switch t.GetTaskProto().GetState() {
	case datapb.CompactionTaskState_meta_saved:
		processResult = t.processMetaSaved()
	case datapb.CompactionTaskState_completed:
		processResult = t.processCompleted()
	case datapb.CompactionTaskState_failed:
		processResult = t.processFailed()
	case datapb.CompactionTaskState_timeout:
		processResult = true
	}
	currentState := t.GetTaskProto().GetState().String()
	if currentState != lastState {
		log.Info("mix compaction task state changed", zap.String("lastState", lastState), zap.String("currentState", currentState))
	}
	return processResult
}

func (t *mixCompactionTask) GetLabel() string {
	return fmt.Sprintf("%d-%s", t.taskProto.Load().(*datapb.CompactionTask).PartitionID, t.GetTaskProto().GetChannel())
}

func (t *mixCompactionTask) NeedReAssignNodeID() bool {
	return t.GetTaskProto().GetState() == datapb.CompactionTaskState_pipelining && (t.GetTaskProto().GetNodeID() == 0 || t.GetTaskProto().GetNodeID() == NullNodeID)
}

func (t *mixCompactionTask) processCompleted() bool {
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
		zap.Int64("PlanID", t.GetTaskProto().GetPlanID()),
		zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
	t.resetSegmentCompacting()
	log.Info("mixCompactionTask processCompleted done")
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
	// if task state is completed, cleaned, failed, timeout, then do append end time and save
	if t.GetTaskProto().State == datapb.CompactionTaskState_completed ||
		t.GetTaskProto().State == datapb.CompactionTaskState_cleaned ||
		t.GetTaskProto().State == datapb.CompactionTaskState_failed ||
		t.GetTaskProto().State == datapb.CompactionTaskState_timeout {
		ts := time.Now().Unix()
		opts = append(opts, setEndTime(ts))
	}

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

func (t *mixCompactionTask) SetTask(task *datapb.CompactionTask) {
	t.taskProto.Store(task)
}

func (t *mixCompactionTask) CheckCompactionContainsSegment(segmentID int64) bool {
	return false
}

func (t *mixCompactionTask) BuildCompactionRequest() (*datapb.CompactionPlan, error) {
	compactionParams, err := compaction.GenerateJSONParams()
	if err != nil {
		return nil, err
	}
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()), zap.Int64("PlanID", t.GetTaskProto().GetPlanID()), zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
	taskProto := t.taskProto.Load().(*datapb.CompactionTask)
	plan := &datapb.CompactionPlan{
		PlanID:                    taskProto.GetPlanID(),
		StartTime:                 taskProto.GetStartTime(),
		TimeoutInSeconds:          taskProto.GetTimeoutInSeconds(),
		Type:                      taskProto.GetType(),
		Channel:                   taskProto.GetChannel(),
		CollectionTtl:             taskProto.GetCollectionTtl(),
		TotalRows:                 taskProto.GetTotalRows(),
		Schema:                    taskProto.GetSchema(),
		PreAllocatedSegmentIDs:    taskProto.GetPreAllocatedSegmentIDs(),
		SlotUsage:                 t.GetSlotUsage(),
		MaxSize:                   taskProto.GetMaxSize(),
		JsonParams:                compactionParams,
		CurrentScalarIndexVersion: t.ievm.GetCurrentScalarIndexEngineVersion(),
	}

	segIDMap := make(map[int64][]*datapb.FieldBinlog, len(plan.SegmentBinlogs))
	segments := make([]*SegmentInfo, 0, len(taskProto.GetInputSegments()))
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
			StorageVersion:      segInfo.GetStorageVersion(),
		})
		segIDMap[segID] = segInfo.GetDeltalogs()
		segments = append(segments, segInfo)
	}

	logIDRange, err := PreAllocateBinlogIDs(t.allocator, segments)
	if err != nil {
		return nil, err
	}
	plan.PreAllocatedLogIDs = logIDRange
	// BeginLogID is deprecated, but still assign it for compatibility.
	plan.BeginLogID = logIDRange.Begin

	log.Info("Compaction handler refreshed mix compaction plan", zap.Int64("maxSize", plan.GetMaxSize()),
		zap.Any("PreAllocatedLogIDs", logIDRange), zap.Any("segID2DeltaLogs", segIDMap))
	return plan, nil
}

func (t *mixCompactionTask) GetSlotUsage() int64 {
	return t.GetTaskSlot()
}
