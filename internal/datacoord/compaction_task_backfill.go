package datacoord

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/samber/lo"
)

var _ CompactionTask = (*backfillCompactionTask)(nil)

type backfillCompactionTask struct {
	taskProto atomic.Value // *datapb.CompactionTask
	allocator allocator.Allocator
	meta      CompactionMeta
	handler   Handler
	ievm      IndexEngineVersionManager
	functions []*schemapb.FunctionSchema
}

func newBackfillCompactionTask(t *datapb.CompactionTask, allocator allocator.Allocator, meta CompactionMeta, handler Handler, ievm IndexEngineVersionManager, functions []*schemapb.FunctionSchema) *backfillCompactionTask {
	task := &backfillCompactionTask{
		allocator: allocator,
		meta:      meta,
		handler:   handler,
		ievm:      ievm,
		functions: functions,
	}
	task.taskProto.Store(t)
	return task
}

func (t *backfillCompactionTask) GetTaskID() int64 {
	return t.GetTaskProto().GetPlanID()
}

func (t *backfillCompactionTask) GetTaskType() taskcommon.Type {
	return taskcommon.Compaction
}

func (t *backfillCompactionTask) GetTaskState() taskcommon.State {
	return taskcommon.FromCompactionState(t.GetTaskProto().GetState())
}

func (t *backfillCompactionTask) GetTaskProto() *datapb.CompactionTask {
	return t.taskProto.Load().(*datapb.CompactionTask)
}

func (t *backfillCompactionTask) GetTaskSlot() int64 {
	return 1
}

func (t *backfillCompactionTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	return
}

func (t *backfillCompactionTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return time.Time{}
}

func (t *backfillCompactionTask) GetTaskVersion() int64 {
	return int64(t.GetTaskProto().GetRetryTimes())
}

func (t *backfillCompactionTask) BuildCompactionRequest() (*datapb.CompactionPlan, error) {
	compactionParams, err := compaction.GenerateJSONParams()
	if err != nil {
		return nil, err
	}
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()), zap.Int64("PlanID", t.GetTaskProto().GetPlanID()), zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
	taskProto := t.taskProto.Load().(*datapb.CompactionTask)
	plan := &datapb.CompactionPlan{
		PlanID:                    taskProto.GetPlanID(),
		StartTime:                 taskProto.GetStartTime(),
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
		Functions:                 t.functions,
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
	plan.BeginLogID = logIDRange.Begin
	WrapPluginContext(taskProto.GetCollectionID(), taskProto.GetSchema().GetProperties(), plan)
	log.Info("Compaction handler refreshed backfill compaction plan", zap.Int64("maxSize", plan.GetMaxSize()),
		zap.Any("PreAllocatedLogIDs", logIDRange), zap.Any("segID2DeltaLogs", segIDMap))
	return plan, nil
}

func (t *backfillCompactionTask) GetSlotUsage() int64 {
	return 1
}

func (t *backfillCompactionTask) GetLabel() string {
	return fmt.Sprintf("%d-%s", t.GetTaskProto().GetPartitionID(), t.GetTaskProto().GetChannel())
}

func (t *backfillCompactionTask) SetTask(task *datapb.CompactionTask) {
	t.taskProto.Store(task)
}

func (t *backfillCompactionTask) ShadowClone(opts ...compactionTaskOpt) *datapb.CompactionTask {
	taskProto := t.GetTaskProto()
	if taskProto == nil {
		return nil
	}

	// Create a copy of the task using protobuf Clone
	cloned := proto.Clone(taskProto).(*datapb.CompactionTask)

	// Apply options
	for _, opt := range opts {
		opt(cloned)
	}

	return cloned
}

func (t *backfillCompactionTask) SetNodeID(nodeID int64) error {
	return t.updateAndSaveTaskMeta(setNodeID(nodeID))
}

func (t *backfillCompactionTask) NeedReAssignNodeID() bool {
	return t.GetTaskProto().GetState() == datapb.CompactionTaskState_pipelining && (t.GetTaskProto().GetNodeID() == 0 || t.GetTaskProto().GetNodeID() == task.NullNodeID)
}

func (t *backfillCompactionTask) saveTaskMeta(task *datapb.CompactionTask) error {
	return t.meta.SaveCompactionTask(context.TODO(), task)
}

func (t *backfillCompactionTask) SaveTaskMeta() error {
	return t.saveTaskMeta(t.GetTaskProto())
}

func (t *backfillCompactionTask) Clean() bool {
	return t.doClean() == nil
}

func (t *backfillCompactionTask) doClean() error {
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
		zap.Int64("PlanID", t.GetTaskProto().GetPlanID()),
		zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
	err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_cleaned))
	if err != nil {
		log.Warn("backfillCompactionTask fail to updateAndSaveTaskMeta", zap.Error(err))
		return err
	}
	// resetSegmentCompacting must be the last step of Clean, to make sure resetSegmentCompacting only called once
	// otherwise, it may unlock segments locked by other compaction tasks
	t.resetSegmentCompacting()
	log.Info("backfillCompactionTask clean done")
	return nil
}

func (t *backfillCompactionTask) resetSegmentCompacting() {
	t.meta.SetSegmentsCompacting(context.TODO(), t.GetTaskProto().GetInputSegments(), false)
}

func (t *backfillCompactionTask) processFailed() bool {
	return true
}

func (t *backfillCompactionTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
		zap.Int64("PlanID", t.GetTaskProto().GetPlanID()),
		zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()),
		zap.Int64("nodeID", nodeID))

	plan, err := t.BuildCompactionRequest()
	if err != nil {
		log.Warn("backfillCompactionTask failed to build compaction request", zap.Error(err))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
		if err != nil {
			log.Warn("backfillCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
		}
		return
	}

	err = cluster.CreateCompaction(nodeID, plan)
	if err != nil {
		log.Warn("backfillCompactionTask failed to notify compaction tasks to DataNode",
			zap.Int64("planID", t.GetTaskProto().GetPlanID()),
			zap.Int64("nodeID", nodeID),
			zap.Error(err))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(task.NullNodeID))
		if err != nil {
			log.Warn("backfillCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
		}
		return
	}

	log.Info("backfillCompactionTask created task on worker", zap.Int64("planID", t.GetTaskProto().GetPlanID()),
		zap.Int64("nodeID", nodeID))

	err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_executing), setNodeID(nodeID))
	if err != nil {
		log.Warn("backfillCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
	}
}

func (t *backfillCompactionTask) QueryTaskOnWorker(cluster session.Cluster) {
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
		zap.Int64("PlanID", t.GetTaskProto().GetPlanID()),
		zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
	result, err := cluster.QueryCompaction(t.GetTaskProto().GetNodeID(), &datapb.CompactionStateRequest{
		PlanID: t.GetTaskProto().GetPlanID(),
	})
	if err != nil || result == nil {
		if errors.Is(err, merr.ErrNodeNotFound) {
			if err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(task.NullNodeID)); err != nil {
				log.Warn("backfillCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
			}
		}
		log.Warn("backfillCompactionTask failed to get compaction result", zap.Error(err))
		return
	}
	switch result.GetState() {
	case datapb.CompactionTaskState_completed:
		if len(result.GetSegments()) == 0 {
			log.Info("illegal compaction results")
			err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
			if err != nil {
				log.Warn("backfillCompactionTask failed to setState failed", zap.Error(err))
			}
			return
		}
		err = t.meta.ValidateSegmentStateBeforeCompleteCompactionMutation(t.GetTaskProto())
		if err != nil {
			t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
			return
		}
		if err := t.saveSegmentMeta(result); err != nil {
			log.Warn("backfillCompactionTask failed to save segment meta", zap.Error(err))
			if errors.Is(err, merr.ErrIllegalCompactionPlan) {
				err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
				if err != nil {
					log.Warn("backfillCompactionTask failed to setState failed", zap.Error(err))
				}
			}
			return
		}
		UpdateCompactionSegmentSizeMetrics(result.GetSegments())
		t.processMetaSaved()
	case datapb.CompactionTaskState_pipelining, datapb.CompactionTaskState_executing:
		return
	case datapb.CompactionTaskState_timeout:
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_timeout))
		if err != nil {
			log.Warn("backfillCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
			return
		}
	case datapb.CompactionTaskState_failed:
		log.Info("backfillCompactionTask fail in datanode")
		err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
		if err != nil {
			log.Warn("backfillCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
		}
	default:
		log.Error("not support compaction task state", zap.String("state", result.GetState().String()))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
		if err != nil {
			log.Warn("backfillCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
			return
		}
	}
}

func (t *backfillCompactionTask) DropTaskOnWorker(cluster session.Cluster) {
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
		zap.Int64("PlanID", t.GetTaskProto().GetPlanID()),
		zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
	if err := cluster.DropCompaction(t.GetTaskProto().GetNodeID(), t.GetTaskProto().GetPlanID()); err != nil {
		log.Warn("backfillCompactionTask unable to drop compaction plan", zap.Error(err))
	}
}

// Process performs the task's state machine
// Note: return True means exit this state machine.
// ONLY return True for Completed, Failed, Timeout
func (t *backfillCompactionTask) Process() bool {
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
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
		log.Info("backfill compaction task state changed", zap.String("lastState", lastState), zap.String("currentState", currentState))
	}
	return processResult
}

func (t *backfillCompactionTask) saveSegmentMeta(result *datapb.CompactionPlanResult) error {
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
		zap.Int64("PlanID", t.GetTaskProto().GetPlanID()),
		zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
	// Also prepare metric updates.
	newSegments, metricMutation, err := t.meta.CompleteCompactionMutation(context.TODO(), t.GetTaskProto(), result)
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
		log.Warn("backfillCompactionTask failed to setState meta saved", zap.Error(err))
		return err
	}
	log.Info("backfillCompactionTask success to save segment meta")
	return nil
}

func (t *backfillCompactionTask) processMetaSaved() bool {
	// For backfill compaction, we directly mark it as completed
	// since the actual work is done by updating segment metadata
	err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_completed))
	if err != nil {
		log.Warn("backfillCompactionTask unable to processMetaSaved",
			zap.Int64("planID", t.GetTaskProto().GetPlanID()),
			zap.Error(err))
		return false
	}
	return t.processCompleted()
}

func (t *backfillCompactionTask) processCompleted() bool {
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
		zap.Int64("PlanID", t.GetTaskProto().GetPlanID()),
		zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
	t.resetSegmentCompacting()
	log.Info("backfillCompactionTask processCompleted done")
	return true
}

func (t *backfillCompactionTask) updateAndSaveTaskMeta(opts ...compactionTaskOpt) error {
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
