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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var _ CompactionTask = (*bumpSchemaVersionTask)(nil)

type bumpSchemaVersionTask struct {
	taskProto atomic.Value // *datapb.CompactionTask

	// ctx is the inspector's process context, threaded from buildCompactTask
	// so the scheduler callbacks (which receive none) can still log with it.
	ctx       context.Context
	allocator allocator.Allocator
	meta      CompactionMeta
	ievm      IndexEngineVersionManager
	times     *taskcommon.Times
}

func newBumpSchemaVersionTask(ctx context.Context, t *datapb.CompactionTask, allocator allocator.Allocator, meta CompactionMeta, ievm IndexEngineVersionManager) *bumpSchemaVersionTask {
	task := &bumpSchemaVersionTask{
		ctx:       ctx,
		allocator: allocator,
		meta:      meta,
		ievm:      ievm,
		times:     taskcommon.NewTimes(),
	}
	task.taskProto.Store(t)
	return task
}

func (t *bumpSchemaVersionTask) GetTaskID() int64 {
	return t.GetTask().GetPlanID()
}

func (t *bumpSchemaVersionTask) GetTaskType() taskcommon.Type {
	return taskcommon.Compaction
}

func (t *bumpSchemaVersionTask) GetTaskState() taskcommon.State {
	return taskcommon.FromCompactionState(t.GetTask().GetState())
}

func (t *bumpSchemaVersionTask) GetTask() *datapb.CompactionTask {
	return t.taskProto.Load().(*datapb.CompactionTask)
}

func (t *bumpSchemaVersionTask) GetTaskSlot() int64 {
	return paramtable.Get().DataCoordCfg.BumpSchemaVersionCompactionSlotUsage.GetAsInt64()
}

func (t *bumpSchemaVersionTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	t.times.SetTaskTime(timeType, time)
}

func (t *bumpSchemaVersionTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(t.times)
}

func (t *bumpSchemaVersionTask) GetTaskVersion() int64 {
	return int64(t.GetTask().GetRetryTimes())
}

func (t *bumpSchemaVersionTask) BuildCompactionRequest() (*datapb.CompactionPlan, error) {
	taskProto := t.GetTask()
	if taskProto.GetSchema() == nil {
		return nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction task schema is nil")
	}
	compactionParams, err := compaction.GenerateJSONParams(taskProto.GetSchema())
	if err != nil {
		return nil, err
	}
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
		CurrentScalarIndexVersion: t.ievm.ResolveScalarIndexVersion(),
	}
	segments := make([]*SegmentInfo, 0, len(taskProto.GetInputSegments()))
	for _, segID := range taskProto.GetInputSegments() {
		segInfo := t.meta.GetHealthySegment(t.ctx, segID)
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
			CommitTimestamp:     segInfo.GetCommitTimestamp(),
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
	return plan, nil
}

func (t *bumpSchemaVersionTask) GetSlotUsage() int64 {
	return t.GetTaskSlot()
}

func (t *bumpSchemaVersionTask) GetLabel() string {
	return fmt.Sprintf("%d-%s", t.GetTask().GetPartitionID(), t.GetTask().GetChannel())
}

func (t *bumpSchemaVersionTask) SetTask(task *datapb.CompactionTask) {
	t.taskProto.Store(task)
}

func (t *bumpSchemaVersionTask) ShadowClone(opts ...compactionTaskOpt) *datapb.CompactionTask {
	cloned := proto.Clone(t.GetTask()).(*datapb.CompactionTask)
	for _, opt := range opts {
		opt(cloned)
	}
	return cloned
}

func (t *bumpSchemaVersionTask) NeedReAssignNodeID() bool {
	return t.GetTask().GetState() == datapb.CompactionTaskState_pipelining && hasNoWorker(t)
}

func (t *bumpSchemaVersionTask) saveTaskMeta(task *datapb.CompactionTask) error {
	return t.meta.SaveCompactionTask(t.ctx, task)
}

func (t *bumpSchemaVersionTask) SaveTaskMeta() error {
	return t.saveTaskMeta(t.GetTask())
}

func (t *bumpSchemaVersionTask) Clean() bool {
	// Runs under the scheduler's per-task lock, handed over by Finalize.
	if alreadyCleaned(t) {
		return true
	}
	return t.doClean() == nil
}

func (t *bumpSchemaVersionTask) getCtx() context.Context {
	return t.ctx
}

func (t *bumpSchemaVersionTask) doClean() error {
	// See finishClean: the input release must stay the last step of Clean.
	return finishClean(t, "bumpSchemaVersionTask")
}

func (t *bumpSchemaVersionTask) resetSegmentCompacting() {
	t.meta.SetSegmentsCompacting(t.ctx, t.GetTask().GetInputSegments(), false)
}

func (t *bumpSchemaVersionTask) processFailed() bool {
	return true
}

func (t *bumpSchemaVersionTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	if callbackPreempted(t) {
		return
	}
	log := mlog.With(mlog.Int64("triggerID", t.GetTask().GetTriggerID()),
		mlog.Int64("planID", t.GetTask().GetPlanID()),
		mlog.FieldCollectionID(t.GetTask().GetCollectionID()),
		mlog.FieldNodeID(nodeID))

	plan, err := t.BuildCompactionRequest()
	if err != nil {
		log.Warn(t.ctx, "bumpSchemaVersionTask failed to build compaction request", mlog.Err(err))
		err = t.updateAndSaveTaskMeta(setAttemptEnded(), setFailReason(err.Error()))
		if err != nil {
			log.Warn(t.ctx, "bumpSchemaVersionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
		}
		return
	}

	// Persist the assignment, send the plan, and classify the outcome. See
	// dispatchCompactionPlan for the ordering and the outcome rules.
	if err := dispatchCompactionPlan(t.ctx, t, nodeID, cluster, plan, "bumpSchemaVersionTask"); err != nil {
		log.Warn(t.ctx, "bumpSchemaVersionTask failed to persist assignment, not sending plan",
			mlog.Err(err))
	}
}

func (t *bumpSchemaVersionTask) QueryTaskOnWorker(cluster session.Cluster) {
	if callbackPreempted(t) {
		return
	}
	if hasNoWorker(t) {
		return
	}

	log := mlog.With(mlog.Int64("triggerID", t.GetTask().GetTriggerID()),
		mlog.Int64("planID", t.GetTask().GetPlanID()),
		mlog.FieldCollectionID(t.GetTask().GetCollectionID()))
	result, err := cluster.QueryCompaction(t.GetTask().GetNodeID(), &datapb.CompactionStateRequest{
		PlanID: t.GetTask().GetPlanID(),
	})
	if err != nil || result == nil {
		// See mixCompactionTask.QueryTaskOnWorker: an unanswered query never
		// re-dispatches this plan; it waits, or abandons the attempt for a replan.
		if errors.Is(err, merr.ErrNodeNotFound) {
			log.Warn(t.ctx, "bumpSchemaVersionTask worker left the cluster, abandoning attempt for replan",
				mlog.FieldNodeID(t.GetTask().GetNodeID()), mlog.Err(err))
			abandonAttempt(t.ctx, t, "assigned worker left the cluster")
			return
		}
		// Same rule as the create path: an RPC round that ends without an
		// answer ends the attempt; see mixCompactionTask.QueryTaskOnWorker.
		log.Warn(t.ctx, "bumpSchemaVersionTask query unanswered, abandoning attempt for replan",
			mlog.FieldNodeID(t.GetTask().GetNodeID()), mlog.Err(err))
		abandonAttempt(t.ctx, t, "worker left the query unanswered")
		return
	}
	switch result.GetState() {
	case datapb.CompactionTaskState_completed:
		if len(result.GetSegments()) == 0 {
			log.Warn(t.ctx, "bumpSchemaVersionTask illegal compaction results: no segments returned")
			if err := t.updateAndSaveTaskMeta(setAttemptEnded(),
				setFailReason("illegal compaction results: no segments returned")); err != nil {
				log.Warn(t.ctx, "bumpSchemaVersionTask failed to setState failed", mlog.Err(err))
			}
			return
		}
		err = t.meta.ValidateSegmentStateBeforeCompleteCompactionMutation(t.GetTask())
		if err != nil {
			// See mixCompactionTask: log why the attempt ended, not only a
			// failure to write that down.
			log.Warn(t.ctx, "bumpSchemaVersionTask rejected a completed result, ending the attempt", mlog.Err(err))
			if saveErr := t.updateAndSaveTaskMeta(setAttemptEnded(), setFailReason(err.Error())); saveErr != nil {
				log.Warn(t.ctx, "bumpSchemaVersionTask failed to setState failed", mlog.Err(saveErr))
			}
			return
		}
		if err := t.saveSegmentMeta(result); err != nil {
			log.Warn(t.ctx, "bumpSchemaVersionTask failed to save segment meta", mlog.Err(err))
			if errors.Is(err, merr.ErrIllegalCompactionPlan) {
				if saveErr := t.updateAndSaveTaskMeta(setAttemptEnded(),
					setFailReason(err.Error())); saveErr != nil {
					log.Warn(t.ctx, "bumpSchemaVersionTask failed to setState failed", mlog.Err(saveErr))
				}
			}
			return
		}
		UpdateCompactionSegmentSizeMetrics(result.GetSegments())
		t.processMetaSaved()
	case datapb.CompactionTaskState_pipelining, datapb.CompactionTaskState_executing:
		return
	case datapb.CompactionTaskState_timeout:
		err = t.updateAndSaveTaskMeta(setAttemptEnded())
		if err != nil {
			log.Warn(t.ctx, "bumpSchemaVersionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
			return
		}
	case datapb.CompactionTaskState_failed:
		reason := workerCompactionFailReason(result.GetReason())
		log.Warn(t.ctx, "bumpSchemaVersionTask fail in datanode", mlog.String("reason", reason))
		if err := t.updateAndSaveTaskMeta(setAttemptEnded(),
			setFailReason(reason)); err != nil {
			log.Warn(t.ctx, "bumpSchemaVersionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
		}
	default:
		log.Error(t.ctx, "not support compaction task state", mlog.String("state", result.GetState().String()))
		reason := fmt.Sprintf("unsupported compaction state: %s", result.GetState().String())
		if err = t.updateAndSaveTaskMeta(setAttemptEnded(),
			setFailReason(reason)); err != nil {
			log.Warn(t.ctx, "bumpSchemaVersionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
			return
		}
	}
}

func (t *bumpSchemaVersionTask) DropTaskOnWorker(cluster session.Cluster) {
	log := mlog.With(mlog.Int64("triggerID", t.GetTask().GetTriggerID()),
		mlog.Int64("planID", t.GetTask().GetPlanID()),
		mlog.FieldCollectionID(t.GetTask().GetCollectionID()))
	if err := cluster.DropCompaction(t.GetTask().GetNodeID(), t.GetTask().GetPlanID()); err != nil {
		log.Warn(t.ctx, "bumpSchemaVersionTask unable to drop compaction plan", mlog.Err(err))
	}
}

// Process performs the task's state machine
// Note: return True means exit this state machine.
// ONLY return True for Completed, Failed, Timeout
func (t *bumpSchemaVersionTask) Process() bool {
	log := mlog.With(mlog.Int64("triggerID", t.GetTask().GetTriggerID()),
		mlog.Int64("planID", t.GetTask().GetPlanID()),
		mlog.FieldCollectionID(t.GetTask().GetCollectionID()))
	lastState := t.GetTask().GetState().String()
	processResult := false
	switch t.GetTask().GetState() {
	case datapb.CompactionTaskState_meta_saved:
		processResult = t.processMetaSaved()
	case datapb.CompactionTaskState_completed:
		processResult = t.processCompleted()
	case datapb.CompactionTaskState_failed:
		processResult = t.processFailed()
	case datapb.CompactionTaskState_timeout, datapb.CompactionTaskState_retrying:
		processResult = true
	}
	currentState := t.GetTask().GetState().String()
	if currentState != lastState {
		log.Info(t.ctx, "schema bump compaction task state changed", mlog.String("lastState", lastState), mlog.String("currentState", currentState))
	}
	return processResult
}

func (t *bumpSchemaVersionTask) saveSegmentMeta(result *datapb.CompactionPlanResult) error {
	log := mlog.With(mlog.Int64("triggerID", t.GetTask().GetTriggerID()),
		mlog.Int64("planID", t.GetTask().GetPlanID()),
		mlog.FieldCollectionID(t.GetTask().GetCollectionID()))
	if err := binlog.CompressCompactionBinlogs(result.GetSegments()); err != nil {
		return err
	}
	newSegments, metricMutation, err := t.meta.CompleteCompactionMutation(t.ctx, t.GetTask(), result)
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
		log.Warn(t.ctx, "bumpSchemaVersionTask failed to setState meta saved", mlog.Err(err))
		return err
	}
	log.Info(t.ctx, "bumpSchemaVersionTask success to save segment meta")
	return nil
}

func (t *bumpSchemaVersionTask) processMetaSaved() bool {
	err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_completed))
	if err != nil {
		mlog.Warn(t.ctx, "bumpSchemaVersionTask unable to processMetaSaved",
			mlog.Int64("planID", t.GetTask().GetPlanID()),
			mlog.Err(err))
		return false
	}
	return t.processCompleted()
}

func (t *bumpSchemaVersionTask) processCompleted() bool {
	log := mlog.With(mlog.Int64("triggerID", t.GetTask().GetTriggerID()),
		mlog.Int64("planID", t.GetTask().GetPlanID()),
		mlog.FieldCollectionID(t.GetTask().GetCollectionID()))
	log.Info(t.ctx, "bumpSchemaVersionTask processCompleted done")
	return true
}

func (t *bumpSchemaVersionTask) updateAndSaveTaskMeta(opts ...compactionTaskOpt) error {
	return updateAndSaveCompactionTaskMeta(t, opts...)
}
