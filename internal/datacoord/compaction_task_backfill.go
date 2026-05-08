// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var _ CompactionTask = (*backfillCompactionTask)(nil)

type backfillCompactionTask struct {
	taskProto atomic.Value // *datapb.CompactionTask
	allocator allocator.Allocator
	meta      CompactionMeta
	ievm      IndexEngineVersionManager
	functions []*schemapb.FunctionSchema
	times     *taskcommon.Times
}

func newBackfillCompactionTask(t *datapb.CompactionTask, allocator allocator.Allocator, meta CompactionMeta, ievm IndexEngineVersionManager, functions []*schemapb.FunctionSchema) *backfillCompactionTask {
	task := &backfillCompactionTask{
		allocator: allocator,
		meta:      meta,
		ievm:      ievm,
		functions: functions,
		times:     taskcommon.NewTimes(),
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
	return paramtable.Get().DataCoordCfg.BackfillCompactionSlotUsage.GetAsInt64()
}

func (t *backfillCompactionTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	t.times.SetTaskTime(timeType, time)
}

func (t *backfillCompactionTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(t.times)
}

func (t *backfillCompactionTask) GetTaskVersion() int64 {
	return int64(t.GetTaskProto().GetRetryTimes())
}

func (t *backfillCompactionTask) BuildCompactionRequest() (*datapb.CompactionPlan, error) {
	taskProto := t.GetTaskProto()
	compactionParams, err := compaction.GenerateJSONParams(taskProto.GetSchema())
	if err != nil {
		return nil, err
	}
	log := log.With(zap.Int64("triggerID", taskProto.GetTriggerID()), zap.Int64("PlanID", taskProto.GetPlanID()), zap.Int64("collectionID", taskProto.GetCollectionID()))
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
			IsSortedByNamespace: segInfo.GetIsSortedByNamespace(),
			StorageVersion:      segInfo.GetStorageVersion(),
			Manifest:            segInfo.GetManifestPath(),
		})
		segments = append(segments, segInfo)
	}

	logIDRange, err := PreAllocateBinlogIDs(t.allocator, segments, taskProto.GetSchema())
	if err != nil {
		return nil, err
	}
	plan.PreAllocatedLogIDs = logIDRange
	plan.BeginLogID = logIDRange.Begin
	WrapPluginContext(taskProto.GetCollectionID(), taskProto.GetSchema().GetProperties(), plan)
	log.Info("Compaction handler refreshed backfill compaction plan", zap.Int64("maxSize", plan.GetMaxSize()),
		zap.Any("PreAllocatedLogIDs", logIDRange), zap.Int64s("inputSegments", taskProto.GetInputSegments()))
	return plan, nil
}

func (t *backfillCompactionTask) GetSlotUsage() int64 {
	return t.GetTaskSlot()
}

func (t *backfillCompactionTask) GetLabel() string {
	return fmt.Sprintf("%d-%s", t.GetTaskProto().GetPartitionID(), t.GetTaskProto().GetChannel())
}

func (t *backfillCompactionTask) SetTask(task *datapb.CompactionTask) {
	t.taskProto.Store(task)
}

func (t *backfillCompactionTask) ShadowClone(opts ...compactionTaskOpt) *datapb.CompactionTask {
	cloned := proto.Clone(t.GetTaskProto()).(*datapb.CompactionTask)
	for _, opt := range opts {
		opt(cloned)
	}
	return cloned
}

func (t *backfillCompactionTask) SetNodeID(nodeID int64) error {
	return t.updateAndSaveTaskMeta(setNodeID(nodeID))
}

func (t *backfillCompactionTask) NeedReAssignNodeID() bool {
	return t.GetTaskProto().GetState() == datapb.CompactionTaskState_pipelining && (t.GetTaskProto().GetNodeID() == 0 || t.GetTaskProto().GetNodeID() == NullNodeID)
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
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
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
			if err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID)); err != nil {
				log.Warn("backfillCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
			}
		}
		log.Warn("backfillCompactionTask failed to get compaction result", zap.Error(err))
		return
	}
	switch result.GetState() {
	case datapb.CompactionTaskState_completed:
		if len(result.GetSegments()) == 0 {
			log.Warn("backfillCompactionTask illegal compaction results: no segments returned")
			if err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed),
				setFailReason("illegal compaction results: no segments returned")); err != nil {
				log.Warn("backfillCompactionTask failed to setState failed", zap.Error(err))
			}
			return
		}
		err = t.meta.ValidateSegmentStateBeforeCompleteCompactionMutation(t.GetTaskProto())
		if err != nil {
			if saveErr := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error())); saveErr != nil {
				log.Warn("backfillCompactionTask failed to setState failed", zap.Error(saveErr))
			}
			return
		}
		if err := t.saveSegmentMeta(result); err != nil {
			log.Warn("backfillCompactionTask failed to save segment meta", zap.Error(err))
			if errors.Is(err, merr.ErrIllegalCompactionPlan) {
				if saveErr := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed),
					setFailReason(err.Error())); saveErr != nil {
					log.Warn("backfillCompactionTask failed to setState failed", zap.Error(saveErr))
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
		log.Warn("backfillCompactionTask fail in datanode")
		if err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed),
			setFailReason("compaction failed in datanode")); err != nil {
			log.Warn("backfillCompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
		}
	default:
		log.Error("not support compaction task state", zap.String("state", result.GetState().String()))
		reason := fmt.Sprintf("unsupported compaction state: %s", result.GetState().String())
		if err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed),
			setFailReason(reason)); err != nil {
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
	newSegments, metricMutation, err := t.meta.CompleteCompactionMutation(context.TODO(), t.GetTaskProto(), result)
	if err != nil {
		return err
	}
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
