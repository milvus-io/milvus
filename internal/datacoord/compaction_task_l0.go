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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var _ CompactionTask = (*l0CompactionTask)(nil)

type l0CompactionTask struct {
	taskProto atomic.Value // *datapb.CompactionTask

	allocator allocator.Allocator
	meta      CompactionMeta

	times *taskcommon.Times
}

func (t *l0CompactionTask) GetTaskID() int64 {
	return t.GetTaskProto().GetPlanID()
}

func (t *l0CompactionTask) GetTaskType() taskcommon.Type {
	return taskcommon.Compaction
}

func (t *l0CompactionTask) GetTaskState() taskcommon.State {
	return taskcommon.FromCompactionState(t.GetTaskProto().GetState())
}

func (t *l0CompactionTask) GetTaskSlot() int64 {
	batchSize := paramtable.Get().CommonCfg.BloomFilterApplyBatchSize.GetAsInt()
	factor := paramtable.Get().DataCoordCfg.L0DeleteCompactionSlotUsage.GetAsInt64()
	slot := factor * t.GetTaskProto().GetTotalRows() / int64(batchSize)
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
	return int64(t.GetTaskProto().GetRetryTimes())
}

func (t *l0CompactionTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	log := log.With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()), zap.Int64("nodeID", t.GetTaskProto().GetNodeID()))
	plan, err := t.BuildCompactionRequest()
	if err != nil {
		log.Warn("l0CompactionTask failed to build compaction request", zap.Error(err))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
		if err != nil {
			log.Warn("l0CompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
		}
		return
	}

	err = cluster.CreateCompaction(nodeID, plan)
	if err != nil {
		originNodeID := t.GetTaskProto().GetNodeID()
		log.Warn("l0CompactionTask failed to notify compaction tasks to DataNode",
			zap.Int64("planID", t.GetTaskProto().GetPlanID()),
			zap.Int64("nodeID", originNodeID),
			zap.Error(err))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
		if err != nil {
			log.Warn("l0CompactionTask failed to updateAndSaveTaskMeta", zap.Int64("planID", t.GetTaskProto().GetPlanID()), zap.Error(err))
			return
		}
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", originNodeID), t.GetTaskProto().GetType().String(), metrics.Executing).Dec()
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", NullNodeID), t.GetTaskProto().GetType().String(), metrics.Pending).Inc()
		return
	}

	err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_executing), setNodeID(nodeID))
	if err != nil {
		log.Warn("l0CompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
	}
}

func (t *l0CompactionTask) QueryTaskOnWorker(cluster session.Cluster) {
	log := log.With(zap.Int64("planID", t.GetTaskProto().GetPlanID()), zap.Int64("nodeID", t.GetTaskProto().GetNodeID()))
	result, err := cluster.QueryCompaction(t.GetTaskProto().GetNodeID(), &datapb.CompactionStateRequest{
		PlanID: t.GetTaskProto().GetPlanID(),
	})
	if err != nil || result == nil {
		if errors.Is(err, merr.ErrNodeNotFound) {
			t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
		}
		log.Warn("l0CompactionTask failed to get compaction result", zap.Error(err))
		return
	}
	switch result.GetState() {
	case datapb.CompactionTaskState_completed:
		if err = t.saveSegmentMeta(result); err != nil {
			log.Warn("l0CompactionTask failed to save segment meta", zap.Error(err))
			return
		}

		if err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_meta_saved)); err != nil {
			log.Warn("l0CompactionTask failed to save task meta_saved state", zap.Error(err))
			return
		}
		UpdateCompactionSegmentSizeMetrics(result.GetSegments())
		t.processMetaSaved()
	case datapb.CompactionTaskState_failed:
		if err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed)); err != nil {
			log.Warn("l0CompactionTask failed to set task failed state", zap.Error(err))
			return
		}
	}
}

func (t *l0CompactionTask) DropTaskOnWorker(cluster session.Cluster) {
	if t.hasAssignedWorker() {
		err := cluster.DropCompaction(t.GetTaskProto().GetNodeID(), t.GetTaskProto().GetPlanID())
		if err != nil {
			log.Warn("l0CompactionTask unable to drop compaction plan", zap.Int64("planID", t.GetTaskProto().GetPlanID()), zap.Error(err))
		}
	}
}

func (t *l0CompactionTask) GetTaskProto() *datapb.CompactionTask {
	task := t.taskProto.Load()
	if task == nil {
		return nil
	}
	return task.(*datapb.CompactionTask)
}

func newL0CompactionTask(t *datapb.CompactionTask, allocator allocator.Allocator, meta CompactionMeta) *l0CompactionTask {
	task := &l0CompactionTask{
		allocator: allocator,
		meta:      meta,
		times:     taskcommon.NewTimes(),
	}
	task.taskProto.Store(t)
	return task
}

// Note: return True means exit this state machine.
// ONLY return True for Completed, Failed
func (t *l0CompactionTask) Process() bool {
	switch t.GetTaskProto().GetState() {
	case datapb.CompactionTaskState_meta_saved:
		return t.processMetaSaved()
	case datapb.CompactionTaskState_completed:
		return t.processCompleted()
	case datapb.CompactionTaskState_failed:
		return true
	case datapb.CompactionTaskState_timeout:
		return true
	default:
		return false
	}
}

func (t *l0CompactionTask) processMetaSaved() bool {
	err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_completed))
	if err != nil {
		log.Warn("l0CompactionTask unable to processMetaSaved", zap.Int64("planID", t.GetTaskProto().GetPlanID()), zap.Error(err))
		return false
	}
	return t.processCompleted()
}

func (t *l0CompactionTask) processCompleted() bool {
	t.resetSegmentCompacting()
	task := t.taskProto.Load().(*datapb.CompactionTask)
	log.Info("l0CompactionTask processCompleted done", zap.Int64("planID", task.GetPlanID()),
		zap.Duration("costs", time.Duration(task.GetEndTime()-task.GetStartTime())*time.Second))
	return true
}

func (t *l0CompactionTask) doClean() error {
	log := log.With(zap.Int64("planID", t.GetTaskProto().GetPlanID()))
	err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_cleaned))
	if err != nil {
		log.Warn("l0CompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
		return err
	}

	// resetSegmentCompacting must be the last step of Clean, to make sure resetSegmentCompacting only called once
	// otherwise, it may unlock segments locked by other compaction tasks
	t.resetSegmentCompacting()
	log.Info("l0CompactionTask clean done")
	return nil
}

func (t *l0CompactionTask) Clean() bool {
	return t.doClean() == nil
}

func (t *l0CompactionTask) SetTask(task *datapb.CompactionTask) {
	t.taskProto.Store(task)
}

func (t *l0CompactionTask) GetLabel() string {
	return fmt.Sprintf("%d-%s", t.GetTaskProto().PartitionID, t.GetTaskProto().GetChannel())
}

func (t *l0CompactionTask) NeedReAssignNodeID() bool {
	return t.GetTaskProto().GetState() == datapb.CompactionTaskState_pipelining && (!t.hasAssignedWorker())
}

func (t *l0CompactionTask) ShadowClone(opts ...compactionTaskOpt) *datapb.CompactionTask {
	taskClone := proto.Clone(t.GetTaskProto()).(*datapb.CompactionTask)
	for _, opt := range opts {
		opt(taskClone)
	}
	return taskClone
}

func (t *l0CompactionTask) selectSealedSegment() ([]*SegmentInfo, []*datapb.CompactionSegmentBinlogs) {
	taskProto := t.taskProto.Load().(*datapb.CompactionTask)
	// Select sealed L1 segments for LevelZero compaction that meets the condition:
	// dmlPos < triggerInfo.pos
	sealedSegments := t.meta.SelectSegments(context.TODO(), WithCollection(taskProto.GetCollectionID()), SegmentFilterFunc(func(info *SegmentInfo) bool {
		return (taskProto.GetPartitionID() == common.AllPartitionsID || info.GetPartitionID() == taskProto.GetPartitionID()) &&
			info.GetInsertChannel() == taskProto.GetChannel() &&
			isFlushState(info.GetState()) &&
			!info.GetIsImporting() &&
			info.GetLevel() != datapb.SegmentLevel_L0 &&
			info.GetStartPosition().GetTimestamp() < taskProto.GetPos().GetTimestamp()
	}))

	sealedSegBinlogs := lo.Map(sealedSegments, func(info *SegmentInfo, _ int) *datapb.CompactionSegmentBinlogs {
		return &datapb.CompactionSegmentBinlogs{
			SegmentID:           info.GetID(),
			Field2StatslogPaths: info.GetStatslogs(),
			InsertChannel:       info.GetInsertChannel(),
			Level:               info.GetLevel(),
			CollectionID:        info.GetCollectionID(),
			PartitionID:         info.GetPartitionID(),
			IsSorted:            info.GetIsSorted(),
		}
	})

	return sealedSegments, sealedSegBinlogs
}

func (t *l0CompactionTask) CheckCompactionContainsSegment(segmentID int64) bool {
	sealedSegmentIDs, _ := t.selectSealedSegment()
	for _, sealedSegment := range sealedSegmentIDs {
		if sealedSegment.GetID() == segmentID {
			return true
		}
	}
	return false
}

func (t *l0CompactionTask) PreparePlan() bool {
	sealedSegments, _ := t.selectSealedSegment()
	sealedSegmentIDs := lo.Map(sealedSegments, func(info *SegmentInfo, _ int) int64 {
		return info.GetID()
	})
	exist, hasStating := t.meta.CheckSegmentsStating(context.TODO(), sealedSegmentIDs)
	return exist && !hasStating
}

func (t *l0CompactionTask) BuildCompactionRequest() (*datapb.CompactionPlan, error) {
	compactionParams, err := compaction.GenerateJSONParams()
	if err != nil {
		return nil, err
	}
	taskProto := t.taskProto.Load().(*datapb.CompactionTask)
	plan := &datapb.CompactionPlan{
		PlanID:           taskProto.GetPlanID(),
		StartTime:        taskProto.GetStartTime(),
		TimeoutInSeconds: taskProto.GetTimeoutInSeconds(),
		Type:             taskProto.GetType(),
		Channel:          taskProto.GetChannel(),
		CollectionTtl:    taskProto.GetCollectionTtl(),
		TotalRows:        taskProto.GetTotalRows(),
		Schema:           taskProto.GetSchema(),
		SlotUsage:        t.GetSlotUsage(),
		JsonParams:       compactionParams,
	}

	log := log.With(zap.Int64("taskID", taskProto.GetTriggerID()), zap.Int64("planID", plan.GetPlanID()))
	segments := make([]*SegmentInfo, 0)
	for _, segID := range taskProto.GetInputSegments() {
		segInfo := t.meta.GetHealthySegment(context.TODO(), segID)
		if segInfo == nil {
			return nil, merr.WrapErrSegmentNotFound(segID)
		}
		plan.SegmentBinlogs = append(plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			SegmentID:     segID,
			CollectionID:  segInfo.GetCollectionID(),
			PartitionID:   segInfo.GetPartitionID(),
			Level:         segInfo.GetLevel(),
			InsertChannel: segInfo.GetInsertChannel(),
			Deltalogs:     segInfo.GetDeltalogs(),
			IsSorted:      segInfo.GetIsSorted(),
		})
		segments = append(segments, segInfo)
	}

	sealedSegments, sealedSegBinlogs := t.selectSealedSegment()
	if len(sealedSegments) == 0 {
		// TODO fast finish l0 segment, just drop l0 segment
		log.Info("l0Compaction available non-L0 Segments is empty ")
		return nil, errors.Errorf("Selected zero L1/L2 segments for the position=%v", taskProto.GetPos())
	}

	segments = append(segments, sealedSegments...)
	logIDRange, err := PreAllocateBinlogIDs(t.allocator, segments)
	if err != nil {
		return nil, err
	}
	plan.PreAllocatedLogIDs = logIDRange
	// BeginLogID is deprecated, but still assign it for compatibility.
	plan.BeginLogID = logIDRange.Begin

	plan.SegmentBinlogs = append(plan.SegmentBinlogs, sealedSegBinlogs...)
	log.Info("l0CompactionTask refreshed level zero compaction plan",
		zap.Any("target position", taskProto.GetPos()),
		zap.Any("target segments count", len(sealedSegBinlogs)),
		zap.Any("PreAllocatedLogIDs", logIDRange))
	return plan, nil
}

func (t *l0CompactionTask) resetSegmentCompacting() {
	t.meta.SetSegmentsCompacting(context.TODO(), t.GetTaskProto().GetInputSegments(), false)
}

func (t *l0CompactionTask) hasAssignedWorker() bool {
	return t.GetTaskProto().GetNodeID() != 0 && t.GetTaskProto().GetNodeID() != NullNodeID
}

func (t *l0CompactionTask) SetNodeID(id UniqueID) error {
	return t.updateAndSaveTaskMeta(setNodeID(id))
}

func (t *l0CompactionTask) SaveTaskMeta() error {
	return t.saveTaskMeta(t.GetTaskProto())
}

func (t *l0CompactionTask) updateAndSaveTaskMeta(opts ...compactionTaskOpt) error {
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

func (t *l0CompactionTask) saveTaskMeta(task *datapb.CompactionTask) error {
	return t.meta.SaveCompactionTask(context.TODO(), task)
}

func (t *l0CompactionTask) saveSegmentMeta(result *datapb.CompactionPlanResult) error {
	var operators []UpdateOperator
	for _, seg := range result.GetSegments() {
		operators = append(operators, AddBinlogsOperator(seg.GetSegmentID(), nil, nil, seg.GetDeltalogs(), nil))
	}

	for _, segID := range t.GetTaskProto().InputSegments {
		operators = append(operators, UpdateStatusOperator(segID, commonpb.SegmentState_Dropped), UpdateCompactedOperator(segID))
	}

	log.Info("meta update: update segments info for level zero compaction",
		zap.Int64("planID", t.GetTaskProto().GetPlanID()),
	)

	return t.meta.UpdateSegmentsInfo(context.TODO(), operators...)
}

func (t *l0CompactionTask) GetSlotUsage() int64 {
	return t.GetTaskSlot()
}
