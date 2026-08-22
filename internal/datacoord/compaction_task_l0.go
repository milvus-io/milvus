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
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var _ CompactionTask = (*l0CompactionTask)(nil)

type l0CompactionTask struct {
	taskProto atomic.Value // *datapb.CompactionTask

	// ctx is the inspector's process context, threaded from buildCompactTask
	// so the scheduler callbacks (which receive none) can still log with it.
	ctx context.Context

	allocator allocator.Allocator
	meta      CompactionMeta

	times                *taskcommon.Times
	committedV3Manifests map[int64]string
}

func (t *l0CompactionTask) GetTaskID() int64 {
	return t.GetTask().GetPlanID()
}

func (t *l0CompactionTask) GetTaskType() taskcommon.Type {
	return taskcommon.Compaction
}

func (t *l0CompactionTask) GetTaskState() taskcommon.State {
	return taskcommon.FromCompactionState(t.GetTask().GetState())
}

func (t *l0CompactionTask) GetTaskSlot() int64 {
	batchSize := paramtable.Get().CommonCfg.BloomFilterApplyBatchSize.GetAsInt()
	factor := paramtable.Get().DataCoordCfg.L0DeleteCompactionSlotUsage.GetAsInt64()
	slot := factor * t.GetTask().GetTotalRows() / int64(batchSize)
	if slot < 1 {
		return 1
	}
	return slot
}

func (t *l0CompactionTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	t.times.SetTaskTime(timeType, time)
}

func (t *l0CompactionTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(t.times)
}

func (t *l0CompactionTask) GetTaskVersion() int64 {
	return int64(t.GetTask().GetRetryTimes())
}

func (t *l0CompactionTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	if callbackPreempted(t) {
		return
	}
	// planID identifies the attempt in every line below; nodeID is NOT bound
	// here because it is still unassigned at this point -- branches that know
	// the node add it themselves, and binding a stale -1 would put two
	// conflicting nodeID keys on those lines.
	log := mlog.With(mlog.Int64("triggerID", t.GetTask().GetTriggerID()), mlog.Int64("planID", t.GetTask().GetPlanID()))
	plan, err := t.BuildCompactionRequest()
	if err != nil {
		log.Warn(t.ctx, "l0CompactionTask failed to build compaction request", mlog.Err(err))
		err = t.updateAndSaveTaskMeta(setAttemptEnded(), setFailReason(err.Error()))
		if err != nil {
			log.Warn(t.ctx, "l0CompactionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
		}
		return
	}

	// Check if this is a fast finish case (no target segments to compact with)
	// Fast finish plan only contains L0 input segments, no target L1/L2 segments
	if len(plan.SegmentBinlogs) == len(t.GetTask().GetInputSegments()) {
		log.Info(t.ctx, "l0CompactionTask fast finish: no target segments, directly marking L0 segments as dropped",
			mlog.Int64("planID", t.GetTask().GetPlanID()))

		// Save segment meta with empty output segments (marks L0 input segments as dropped)
		if err = t.saveSegmentMeta([]*datapb.CompactionSegment{}); err != nil {
			log.Warn(t.ctx, "l0CompactionTask fast finish failed to save segment meta", mlog.Err(err))
			err = t.updateAndSaveTaskMeta(setAttemptEnded(), setFailReason(err.Error()))
			if err != nil {
				log.Warn(t.ctx, "l0CompactionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
			}
			return
		}

		// Transition to meta_saved state
		if err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_meta_saved)); err != nil {
			log.Warn(t.ctx, "l0CompactionTask fast finish failed to save task meta_saved state", mlog.Err(err))
			return
		}

		log.Info(t.ctx, "l0CompactionTask fast finish completed", mlog.Int64("planID", t.GetTask().GetPlanID()))
		return
	}

	// Persist the assignment, send the plan, and classify the outcome. See
	// dispatchCompactionPlan for the ordering and the outcome rules.
	if err := dispatchCompactionPlan(t.ctx, t, nodeID, cluster, plan, "l0CompactionTask"); err != nil {
		log.Warn(t.ctx, "l0CompactionTask failed to persist assignment, not sending plan",
			mlog.FieldNodeID(nodeID), mlog.Err(err))
	}
}

func (t *l0CompactionTask) QueryTaskOnWorker(cluster session.Cluster) {
	if callbackPreempted(t) {
		return
	}
	if hasNoWorker(t) {
		return
	}

	log := mlog.With(mlog.Int64("planID", t.GetTask().GetPlanID()), mlog.FieldNodeID(t.GetTask().GetNodeID()))
	result, err := cluster.QueryCompaction(t.GetTask().GetNodeID(), &datapb.CompactionStateRequest{
		PlanID: t.GetTask().GetPlanID(),
	})
	if err != nil || result == nil {
		// See mixCompactionTask.QueryTaskOnWorker for the reasoning: a query
		// never carries a refusal, so an unanswered one never re-dispatches this
		// plan -- it either waits or abandons the attempt for a replan.
		if errors.Is(err, merr.ErrNodeNotFound) {
			log.Warn(t.ctx, "l0CompactionTask worker left the cluster, abandoning attempt for replan", mlog.Err(err))
			abandonAttempt(t.ctx, t, "assigned worker left the cluster")
			return
		}
		// Same rule as the create path: an RPC round that ends without an
		// answer ends the attempt; see mixCompactionTask.QueryTaskOnWorker.
		log.Warn(t.ctx, "l0CompactionTask query unanswered, abandoning attempt for replan", mlog.Err(err))
		abandonAttempt(t.ctx, t, "worker left the query unanswered")
		return
	}
	switch result.GetState() {
	case datapb.CompactionTaskState_completed:
		err = t.meta.ValidateSegmentStateBeforeCompleteCompactionMutation(t.GetTask())
		if err != nil {
			// See mixCompactionTask: a rejected completed result ends the
			// attempt and must not do so silently.
			log.Warn(t.ctx, "l0CompactionTask rejected a completed result, ending the attempt", mlog.Err(err))
			if saveErr := t.updateAndSaveTaskMeta(setAttemptEnded(), setFailReason(err.Error())); saveErr != nil {
				log.Warn(t.ctx, "l0CompactionTask failed to persist the rejected result", mlog.Err(saveErr))
			}
			return
		}

		if err = t.saveSegmentMeta(result.GetSegments()); err != nil {
			log.Warn(t.ctx, "l0CompactionTask failed to save segment meta", mlog.Err(err))
			return
		}

		if err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_meta_saved)); err != nil {
			log.Warn(t.ctx, "l0CompactionTask failed to save task meta_saved state", mlog.Err(err))
			return
		}
		UpdateCompactionSegmentSizeMetrics(result.GetSegments())
		t.processMetaSaved()
	case datapb.CompactionTaskState_pipelining, datapb.CompactionTaskState_executing:
		return
	case datapb.CompactionTaskState_timeout:
		err = t.updateAndSaveTaskMeta(setAttemptEnded(), setFailReason("worker reported timeout"))
		if err != nil {
			log.Warn(t.ctx, "update l0 compaction task meta failed", mlog.Err(err))
			return
		}
	case datapb.CompactionTaskState_failed:
		// Keep the worker's reason; see mixCompactionTask for why.
		reason := workerCompactionFailReason(result.GetReason())
		log.Warn(t.ctx, "l0CompactionTask fail in datanode", mlog.String("reason", reason))
		if err = t.updateAndSaveTaskMeta(setAttemptEnded(), setFailReason(reason)); err != nil {
			log.Warn(t.ctx, "l0CompactionTask failed to set task failed state", mlog.Err(err))
			return
		}
	default:
		log.Error(t.ctx, "not support compaction task state", mlog.String("state", result.GetState().String()))
		err = t.updateAndSaveTaskMeta(setAttemptEnded(), setFailReason("unsupported worker state "+result.GetState().String()))
		if err != nil {
			log.Warn(t.ctx, "update l0 compaction task meta failed", mlog.Err(err))
			return
		}
	}
}

func (t *l0CompactionTask) DropTaskOnWorker(cluster session.Cluster) {
	if t.hasAssignedWorker() {
		err := cluster.DropCompaction(t.GetTask().GetNodeID(), t.GetTask().GetPlanID())
		if err != nil {
			mlog.Warn(t.ctx, "l0CompactionTask unable to drop compaction plan", mlog.Int64("planID", t.GetTask().GetPlanID()), mlog.Err(err))
		}
	}
}

func (t *l0CompactionTask) GetTask() *datapb.CompactionTask {
	task := t.taskProto.Load()
	if task == nil {
		return nil
	}
	return task.(*datapb.CompactionTask)
}

func newL0CompactionTask(ctx context.Context, t *datapb.CompactionTask, allocator allocator.Allocator, meta CompactionMeta) *l0CompactionTask {
	task := &l0CompactionTask{
		ctx:                  ctx,
		allocator:            allocator,
		meta:                 meta,
		times:                taskcommon.NewTimes(),
		committedV3Manifests: make(map[int64]string),
	}
	task.taskProto.Store(t)
	return task
}

// Note: return True means exit this state machine.
// ONLY return True for Completed, Failed
func (t *l0CompactionTask) Process() bool {
	switch t.GetTask().GetState() {
	case datapb.CompactionTaskState_meta_saved:
		return t.processMetaSaved()
	case datapb.CompactionTaskState_completed:
		return t.processCompleted()
	case datapb.CompactionTaskState_failed:
		return true
	case datapb.CompactionTaskState_timeout, datapb.CompactionTaskState_retrying:
		return true
	default:
		return false
	}
}

func (t *l0CompactionTask) processMetaSaved() bool {
	err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_completed))
	if err != nil {
		mlog.Warn(t.ctx, "l0CompactionTask unable to processMetaSaved", mlog.Int64("planID", t.GetTask().GetPlanID()), mlog.Err(err))
		return false
	}
	return t.processCompleted()
}

func (t *l0CompactionTask) processCompleted() bool {
	// See mixCompactionTask.processCompleted: doClean is the only place that
	// releases the input segments.
	task := t.taskProto.Load().(*datapb.CompactionTask)
	mlog.Info(t.ctx, "l0CompactionTask processCompleted done", mlog.Int64("planID", task.GetPlanID()),
		mlog.Duration("costs", time.Duration(task.GetEndTime()-task.GetStartTime())*time.Second))
	return true
}

func (t *l0CompactionTask) getCtx() context.Context {
	return t.ctx
}

func (t *l0CompactionTask) doClean() error {
	// See finishClean: the input release must stay the last step of Clean.
	return finishClean(t, "l0CompactionTask")
}

func (t *l0CompactionTask) Clean() bool {
	// Runs under the scheduler's per-task lock, handed over by Finalize.
	if alreadyCleaned(t) {
		return true
	}
	return t.doClean() == nil
}

func (t *l0CompactionTask) SetTask(task *datapb.CompactionTask) {
	t.taskProto.Store(task)
}

func (t *l0CompactionTask) GetLabel() string {
	return fmt.Sprintf("%d-%s", t.GetTask().PartitionID, t.GetTask().GetChannel())
}

func (t *l0CompactionTask) NeedReAssignNodeID() bool {
	return t.GetTask().GetState() == datapb.CompactionTaskState_pipelining && hasNoWorker(t)
}

func (t *l0CompactionTask) ShadowClone(opts ...compactionTaskOpt) *datapb.CompactionTask {
	taskClone := proto.Clone(t.GetTask()).(*datapb.CompactionTask)
	for _, opt := range opts {
		opt(taskClone)
	}
	return taskClone
}

func (t *l0CompactionTask) selectFlushedSegment() ([]*SegmentInfo, []*datapb.CompactionSegmentBinlogs, error) {
	taskProto := t.taskProto.Load().(*datapb.CompactionTask)
	// Select flushed L1/L2 segments for LevelZero compaction that meets the condition:
	// dmlPos < triggerInfo.pos
	flushedSegments := t.meta.SelectSegments(t.ctx, WithCollection(taskProto.GetCollectionID()), SegmentFilterFunc(func(info *SegmentInfo) bool {
		return (taskProto.GetPartitionID() == common.AllPartitionsID || info.GetPartitionID() == taskProto.GetPartitionID()) &&
			info.GetInsertChannel() == taskProto.GetChannel() &&
			(info.GetState() == commonpb.SegmentState_Sealed || isFlushState(info.GetState())) &&
			!info.GetIsImporting() &&
			info.GetLevel() != datapb.SegmentLevel_L0 &&
			segmentEffectiveTs(info.SegmentInfo) < taskProto.GetPos().GetTimestamp()
	}))

	sealedSegBinlogs := []*datapb.CompactionSegmentBinlogs{}
	for _, info := range flushedSegments {
		// Sealed is unexpected, fail fast
		if info.GetState() == commonpb.SegmentState_Sealed {
			return nil, nil, merr.WrapErrServiceInternalMsg("L0 compaction selected invalid sealed segment %d", info.GetID())
		}

		sealedSegBinlogs = append(sealedSegBinlogs, &datapb.CompactionSegmentBinlogs{
			SegmentID:           info.GetID(),
			Field2StatslogPaths: info.GetStatslogs(),
			InsertChannel:       info.GetInsertChannel(),
			Level:               info.GetLevel(),
			CollectionID:        info.GetCollectionID(),
			PartitionID:         info.GetPartitionID(),
			IsSorted:            info.GetIsSorted(),
			IsSortedByNamespace: info.GetIsSortedByNamespace(),
			Manifest:            info.GetManifestPath(),
			CommitTimestamp:     info.GetCommitTimestamp(),
		})
	}

	return flushedSegments, sealedSegBinlogs, nil
}

func (t *l0CompactionTask) BuildCompactionRequest() (*datapb.CompactionPlan, error) {
	taskProto := t.taskProto.Load().(*datapb.CompactionTask)
	compactionParams, err := compaction.GenerateJSONParams(taskProto.GetSchema())
	if err != nil {
		return nil, err
	}
	plan := &datapb.CompactionPlan{
		PlanID:        taskProto.GetPlanID(),
		StartTime:     taskProto.GetStartTime(),
		Type:          taskProto.GetType(),
		Channel:       taskProto.GetChannel(),
		CollectionTtl: taskProto.GetCollectionTtl(),
		TotalRows:     taskProto.GetTotalRows(),
		Schema:        taskProto.GetSchema(),
		SlotUsage:     t.GetSlotUsage(),
		JsonParams:    compactionParams,
	}

	log := mlog.With(mlog.FieldTaskID(taskProto.GetTriggerID()), mlog.Int64("planID", plan.GetPlanID()))
	segments := make([]*SegmentInfo, 0)
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
			Deltalogs:           segInfo.GetDeltalogs(),
			IsSorted:            segInfo.GetIsSorted(),
			IsSortedByNamespace: segInfo.GetIsSortedByNamespace(),
			Manifest:            segInfo.GetManifestPath(),
			CommitTimestamp:     segInfo.GetCommitTimestamp(),
		})
		segments = append(segments, segInfo)
	}

	flushedSegments, flushedSegBinlogs, err := t.selectFlushedSegment()
	if err != nil {
		log.Warn(t.ctx, "invalid L0 compaction plan, unable to select flushed segments", mlog.Err(err))
		return nil, err
	}
	if len(flushedSegments) == 0 {
		// Fast finish: no target segments to compact with, return plan with only L0 segments
		log.Info(t.ctx, "l0Compaction available non-L0 Segments is empty, will fast finish",
			mlog.Any("target position", taskProto.GetPos()))
		return plan, nil
	}

	segments = append(segments, flushedSegments...)
	logIDRange, err := PreAllocateBinlogIDs(t.allocator, segments, nil)
	if err != nil {
		return nil, err
	}
	plan.PreAllocatedLogIDs = logIDRange
	// BeginLogID is deprecated, but still assign it for compatibility.
	plan.BeginLogID = logIDRange.Begin

	plan.SegmentBinlogs = append(plan.SegmentBinlogs, flushedSegBinlogs...)
	log.Info(t.ctx, "l0CompactionTask refreshed level zero compaction plan",
		mlog.Any("target position", taskProto.GetPos()),
		mlog.Any("target segments count", len(flushedSegBinlogs)),
		mlog.Any("PreAllocatedLogIDs", logIDRange))

	WrapPluginContext(taskProto.GetCollectionID(), taskProto.GetSchema().GetProperties(), plan)
	return plan, nil
}

func (t *l0CompactionTask) resetSegmentCompacting() {
	t.meta.SetSegmentsCompacting(t.ctx, t.GetTask().GetInputSegments(), false)
}

func (t *l0CompactionTask) hasAssignedWorker() bool {
	return t.GetTask().GetNodeID() != 0 && t.GetTask().GetNodeID() != NullNodeID
}

func (t *l0CompactionTask) SaveTaskMeta() error {
	return t.saveTaskMeta(t.GetTask())
}

func (t *l0CompactionTask) updateAndSaveTaskMeta(opts ...compactionTaskOpt) error {
	return updateAndSaveCompactionTaskMeta(t, opts...)
}

func (t *l0CompactionTask) saveTaskMeta(task *datapb.CompactionTask) error {
	return t.meta.SaveCompactionTask(t.ctx, task)
}

func buildL0V3DeltaLogEntries(segmentID int64, deltalogs []*datapb.FieldBinlog) ([]packed.DeltaLogEntry, error) {
	entries := make([]packed.DeltaLogEntry, 0)
	for _, fieldBinlog := range deltalogs {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			path := binlog.GetLogPath()
			if path == "" {
				return nil, merr.WrapErrServiceInternalMsg("L0 V3 compaction result missing deltalog path for segment %d, logID %d", segmentID, binlog.GetLogID())
			}
			entries = append(entries, packed.DeltaLogEntry{
				Path:       path,
				NumEntries: binlog.GetEntriesNum(),
			})
		}
	}
	return entries, nil
}

func (t *l0CompactionTask) saveSegmentMeta(outputSegs []*datapb.CompactionSegment) error {
	var operators []UpdateOperator
	storageConfig := compaction.CreateStorageConfig()
	for _, seg := range outputSegs {
		if len(seg.GetDeltalogs()) > 0 {
			operators = append(operators, AddL0DeltalogsAndUpdateManifestOperator(
				seg.GetSegmentID(),
				seg.GetDeltalogs(),
				storageConfig,
				t.committedV3Manifests,
			))
		}
	}

	for _, segID := range t.GetTask().InputSegments {
		operators = append(operators, UpdateStatusOperator(segID, commonpb.SegmentState_Dropped), UpdateCompactedOperator(segID))
	}

	mlog.Info(t.ctx, "meta update: update segments info for level zero compaction",
		mlog.Int64("planID", t.GetTask().GetPlanID()),
	)

	return t.meta.UpdateSegmentsInfo(t.ctx, operators...)
}

func (t *l0CompactionTask) GetSlotUsage() int64 {
	return t.GetTaskSlot()
}
