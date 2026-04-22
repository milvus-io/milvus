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

	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var _ CompactionTask = (*l0CompactionTask)(nil)

type l0CompactionTask struct {
	taskProto atomic.Value // *datapb.CompactionTask

	allocator allocator.Allocator
	meta      CompactionMeta

	times                *taskcommon.Times
	committedV3Manifests map[int64]string
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
	log := mlog.With(mlog.Int64("triggerID", t.GetTaskProto().GetTriggerID()), mlog.FieldNodeID(t.GetTaskProto().GetNodeID()))
	plan, err := t.BuildCompactionRequest()
	if err != nil {
		log.Warn(context.TODO(), "l0CompactionTask failed to build compaction request", mlog.Err(err))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
		if err != nil {
			log.Warn(context.TODO(), "l0CompactionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
		}
		return
	}

	// Check if this is a fast finish case (no target segments to compact with)
	// Fast finish plan only contains L0 input segments, no target L1/L2 segments
	if len(plan.SegmentBinlogs) == len(t.GetTaskProto().GetInputSegments()) {
		log.Info(context.TODO(), "l0CompactionTask fast finish: no target segments, directly marking L0 segments as dropped",
			mlog.Int64("planID", t.GetTaskProto().GetPlanID()))

		// Save segment meta with empty output segments (marks L0 input segments as dropped)
		if err = t.saveSegmentMeta([]*datapb.CompactionSegment{}); err != nil {
			log.Warn(context.TODO(), "l0CompactionTask fast finish failed to save segment meta", mlog.Err(err))
			err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
			if err != nil {
				log.Warn(context.TODO(), "l0CompactionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
			}
			return
		}

		// Transition to meta_saved state
		if err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_meta_saved)); err != nil {
			log.Warn(context.TODO(), "l0CompactionTask fast finish failed to save task meta_saved state", mlog.Err(err))
			return
		}

		log.Info(context.TODO(), "l0CompactionTask fast finish completed", mlog.Int64("planID", t.GetTaskProto().GetPlanID()))
		return
	}

	err = cluster.CreateCompaction(nodeID, plan, t.GetTaskProto().GetCollectionID())
	if err != nil {
		originNodeID := t.GetTaskProto().GetNodeID()
		log.Warn(context.TODO(), "l0CompactionTask failed to notify compaction tasks to DataNode",
			mlog.Int64("planID", t.GetTaskProto().GetPlanID()),
			mlog.FieldNodeID(originNodeID),
			mlog.Err(err))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
		if err != nil {
			log.Warn(context.TODO(), "l0CompactionTask failed to updateAndSaveTaskMeta", mlog.Int64("planID", t.GetTaskProto().GetPlanID()), mlog.Err(err))
			return
		}
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", originNodeID), t.GetTaskProto().GetType().String(), metrics.Executing).Dec()
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", NullNodeID), t.GetTaskProto().GetType().String(), metrics.Pending).Inc()
		return
	}

	err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_executing), setNodeID(nodeID))
	if err != nil {
		log.Warn(context.TODO(), "l0CompactionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
	}
}

func (t *l0CompactionTask) QueryTaskOnWorker(cluster session.Cluster) {
	log := mlog.With(mlog.Int64("planID", t.GetTaskProto().GetPlanID()), mlog.FieldNodeID(t.GetTaskProto().GetNodeID()))
	result, err := cluster.QueryCompaction(t.GetTaskProto().GetNodeID(), &datapb.CompactionStateRequest{
		PlanID: t.GetTaskProto().GetPlanID(),
	})
	if err != nil || result == nil {
		log.Warn(context.TODO(), "l0CompactionTask failed to get compaction result", mlog.Err(err))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
		if err != nil {
			log.Warn(context.TODO(), "update l0 compaction task meta failed", mlog.Err(err))
		}
		return
	}
	switch result.GetState() {
	case datapb.CompactionTaskState_completed:
		err = t.meta.ValidateSegmentStateBeforeCompleteCompactionMutation(t.GetTaskProto())
		if err != nil {
			t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
			return
		}

		if err = t.saveSegmentMeta(result.GetSegments()); err != nil {
			log.Warn(context.TODO(), "l0CompactionTask failed to save segment meta", mlog.Err(err))
			return
		}

		if err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_meta_saved)); err != nil {
			log.Warn(context.TODO(), "l0CompactionTask failed to save task meta_saved state", mlog.Err(err))
			return
		}
		UpdateCompactionSegmentSizeMetrics(result.GetSegments())
		t.processMetaSaved()
	case datapb.CompactionTaskState_pipelining, datapb.CompactionTaskState_executing:
		return
	case datapb.CompactionTaskState_timeout:
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_timeout))
		if err != nil {
			log.Warn(context.TODO(), "update clustering compaction task meta failed", mlog.Err(err))
			return
		}
	case datapb.CompactionTaskState_failed:
		if err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed)); err != nil {
			log.Warn(context.TODO(), "l0CompactionTask failed to set task failed state", mlog.Err(err))
			return
		}
	default:
		log.Error(context.TODO(), "not support compaction task state", mlog.String("state", result.GetState().String()))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
		if err != nil {
			log.Warn(context.TODO(), "update clustering compaction task meta failed", mlog.Err(err))
			return
		}
	}
}

func (t *l0CompactionTask) DropTaskOnWorker(cluster session.Cluster) {
	if t.hasAssignedWorker() {
		err := cluster.DropCompaction(t.GetTaskProto().GetNodeID(), t.GetTaskProto().GetPlanID())
		if err != nil {
			mlog.Warn(context.TODO(), "l0CompactionTask unable to drop compaction plan", mlog.Int64("planID", t.GetTaskProto().GetPlanID()), mlog.Err(err))
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
		mlog.Warn(context.TODO(), "l0CompactionTask unable to processMetaSaved", mlog.Int64("planID", t.GetTaskProto().GetPlanID()), mlog.Err(err))
		return false
	}
	return t.processCompleted()
}

func (t *l0CompactionTask) processCompleted() bool {
	t.resetSegmentCompacting()
	task := t.taskProto.Load().(*datapb.CompactionTask)
	mlog.Info(context.TODO(), "l0CompactionTask processCompleted done", mlog.Int64("planID", task.GetPlanID()),
		mlog.Duration("costs", time.Duration(task.GetEndTime()-task.GetStartTime())*time.Second))
	return true
}

func (t *l0CompactionTask) doClean() error {
	log := mlog.With(mlog.Int64("planID", t.GetTaskProto().GetPlanID()))
	err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_cleaned))
	if err != nil {
		log.Warn(context.TODO(), "l0CompactionTask failed to updateAndSaveTaskMeta", mlog.Err(err))
		return err
	}

	// resetSegmentCompacting must be the last step of Clean, to make sure resetSegmentCompacting only called once
	// otherwise, it may unlock segments locked by other compaction tasks
	t.resetSegmentCompacting()
	log.Info(context.TODO(), "l0CompactionTask clean done")
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

func (t *l0CompactionTask) selectFlushedSegment() ([]*SegmentInfo, []*datapb.CompactionSegmentBinlogs, error) {
	taskProto := t.taskProto.Load().(*datapb.CompactionTask)
	// Select flushed L1/L2 segments for LevelZero compaction that meets the condition:
	// dmlPos < triggerInfo.pos
	flushedSegments := t.meta.SelectSegments(context.TODO(), WithCollection(taskProto.GetCollectionID()), SegmentFilterFunc(func(info *SegmentInfo) bool {
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
		log.Warn(context.TODO(), "invalid L0 compaction plan, unable to select flushed segments", mlog.Err(err))
		return nil, err
	}
	if len(flushedSegments) == 0 {
		// Fast finish: no target segments to compact with, return plan with only L0 segments
		log.Info(context.TODO(), "l0Compaction available non-L0 Segments is empty, will fast finish",
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
	log.Info(context.TODO(), "l0CompactionTask refreshed level zero compaction plan",
		mlog.Any("target position", taskProto.GetPos()),
		mlog.Any("target segments count", len(flushedSegBinlogs)),
		mlog.Any("PreAllocatedLogIDs", logIDRange))

	WrapPluginContext(taskProto.GetCollectionID(), taskProto.GetSchema().GetProperties(), plan)
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
	mutations := map[int64][]SegmentOperator{}
	for _, seg := range outputSegs {
		deltalogs := seg.GetDeltalogs() // capture
		manifest := seg.GetManifest()   // capture
		mutations[seg.GetSegmentID()] = []SegmentOperator{func(s *SegmentInfo) (BinlogIncrement, bool) {
			if manifest != "" {
				s.ManifestPath = manifest
				return BinlogIncrement{}, true
			}
			s.Deltalogs = mergeFieldBinlogs(s.GetDeltalogs(), deltalogs)
			return BinlogIncrement{Deltalogs: s.Deltalogs}, true
		}}
	}

	for _, segID := range t.GetTaskProto().InputSegments {
		mutations[segID] = []SegmentOperator{func(s *SegmentInfo) (BinlogIncrement, bool) {
			s.State = commonpb.SegmentState_Dropped
			s.DroppedAt = uint64(time.Now().UnixNano())
			s.Compacted = true
			return BinlogIncrement{}, true
		}}
	}

	mlog.Info(context.TODO(), "meta update: update segments info for level zero compaction",
		mlog.Int64("planID", t.GetTaskProto().GetPlanID()),
	)

	return t.meta.UpdateSegmentsInfo(context.TODO(), mutations)
}

func (t *l0CompactionTask) GetSlotUsage() int64 {
	return t.GetTaskSlot()
}
