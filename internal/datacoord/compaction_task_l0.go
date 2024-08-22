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
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

var _ CompactionTask = (*l0CompactionTask)(nil)

type l0CompactionTask struct {
	*datapb.CompactionTask
	plan   *datapb.CompactionPlan
	result *datapb.CompactionPlanResult

	span      trace.Span
	allocator allocator.Allocator
	sessions  session.DataNodeManager
	meta      CompactionMeta
}

// Note: return True means exit this state machine.
// ONLY return True for processCompleted or processFailed
func (t *l0CompactionTask) Process() bool {
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

func (t *l0CompactionTask) processPipelining() bool {
	if t.NeedReAssignNodeID() {
		return false
	}

	log := log.With(zap.Int64("triggerID", t.GetTriggerID()), zap.Int64("nodeID", t.GetNodeID()))
	var err error
	t.plan, err = t.BuildCompactionRequest()
	if err != nil {
		log.Warn("l0CompactionTask failed to build compaction request", zap.Error(err))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
		if err != nil {
			log.Warn("l0CompactionTask failed to updateAndSaveTaskMeta", zap.Error(err))
			return false
		}

		return t.processFailed()
	}

	err = t.sessions.Compaction(context.TODO(), t.GetNodeID(), t.GetPlan())
	if err != nil {
		log.Warn("l0CompactionTask failed to notify compaction tasks to DataNode", zap.Int64("planID", t.GetPlanID()), zap.Error(err))
		t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
		return false
	}

	t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_executing))
	return false
}

func (t *l0CompactionTask) processExecuting() bool {
	log := log.With(zap.Int64("planID", t.GetPlanID()), zap.Int64("nodeID", t.GetNodeID()))
	result, err := t.sessions.GetCompactionPlanResult(t.GetNodeID(), t.GetPlanID())
	if err != nil || result == nil {
		if errors.Is(err, merr.ErrNodeNotFound) {
			t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
		}
		log.Warn("l0CompactionTask failed to get compaction result", zap.Error(err))
		return false
	}
	switch result.GetState() {
	case datapb.CompactionTaskState_executing:
		// will L0Compaction be timeouted?
		if t.checkTimeout() {
			err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_timeout))
			if err != nil {
				log.Warn("l0CompactionTask failed to set task timeout state", zap.Error(err))
				return false
			}
			return t.processTimeout()
		}
	case datapb.CompactionTaskState_completed:
		t.result = result
		if err := t.saveSegmentMeta(); err != nil {
			log.Warn("l0CompactionTask failed to save segment meta", zap.Error(err))
			return false
		}

		if err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_meta_saved)); err != nil {
			log.Warn("l0CompactionTask failed to save task meta_saved state", zap.Error(err))
			return false
		}
		return t.processMetaSaved()
	case datapb.CompactionTaskState_failed:
		if err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed)); err != nil {
			log.Warn("l0CompactionTask failed to set task failed state", zap.Error(err))
			return false
		}
		return t.processFailed()
	}
	return false
}

func (t *l0CompactionTask) processMetaSaved() bool {
	err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_completed))
	if err != nil {
		log.Warn("l0CompactionTask unable to processMetaSaved", zap.Int64("planID", t.GetPlanID()), zap.Error(err))
		return false
	}
	return t.processCompleted()
}

func (t *l0CompactionTask) processCompleted() bool {
	if t.hasAssignedWorker() {
		err := t.sessions.DropCompactionPlan(t.GetNodeID(), &datapb.DropCompactionPlanRequest{
			PlanID: t.GetPlanID(),
		})
		if err != nil {
			log.Warn("l0CompactionTask unable to drop compaction plan", zap.Int64("planID", t.GetPlanID()), zap.Error(err))
		}
	}

	t.resetSegmentCompacting()
	UpdateCompactionSegmentSizeMetrics(t.result.GetSegments())
	log.Info("l0CompactionTask processCompleted done", zap.Int64("planID", t.GetPlanID()))
	return true
}

func (t *l0CompactionTask) processTimeout() bool {
	t.resetSegmentCompacting()
	return true
}

func (t *l0CompactionTask) processFailed() bool {
	if t.hasAssignedWorker() {
		err := t.sessions.DropCompactionPlan(t.GetNodeID(), &datapb.DropCompactionPlanRequest{
			PlanID: t.GetPlanID(),
		})
		if err != nil {
			log.Warn("l0CompactionTask processFailed unable to drop compaction plan", zap.Int64("planID", t.GetPlanID()), zap.Error(err))
		}
	}

	t.resetSegmentCompacting()
	log.Info("l0CompactionTask processFailed done", zap.Int64("taskID", t.GetTriggerID()), zap.Int64("planID", t.GetPlanID()))
	return true
}

func (t *l0CompactionTask) GetResult() *datapb.CompactionPlanResult {
	return t.result
}

func (t *l0CompactionTask) SetResult(result *datapb.CompactionPlanResult) {
	t.result = result
}

func (t *l0CompactionTask) SetTask(task *datapb.CompactionTask) {
	t.CompactionTask = task
}

func (t *l0CompactionTask) GetSpan() trace.Span {
	return t.span
}

func (t *l0CompactionTask) SetSpan(span trace.Span) {
	t.span = span
}

func (t *l0CompactionTask) EndSpan() {
	if t.span != nil {
		t.span.End()
	}
}

func (t *l0CompactionTask) SetPlan(plan *datapb.CompactionPlan) {
	t.plan = plan
}

func (t *l0CompactionTask) GetPlan() *datapb.CompactionPlan {
	return t.plan
}

func (t *l0CompactionTask) SetStartTime(startTime int64) {
	t.StartTime = startTime
}

func (t *l0CompactionTask) GetLabel() string {
	return fmt.Sprintf("%d-%s", t.PartitionID, t.GetChannel())
}

func (t *l0CompactionTask) NeedReAssignNodeID() bool {
	return t.GetState() == datapb.CompactionTaskState_pipelining && (!t.hasAssignedWorker())
}

func (t *l0CompactionTask) CleanLogPath() {
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

func (t *l0CompactionTask) ShadowClone(opts ...compactionTaskOpt) *datapb.CompactionTask {
	taskClone := &datapb.CompactionTask{
		PlanID:           t.GetPlanID(),
		TriggerID:        t.GetTriggerID(),
		State:            t.GetState(),
		StartTime:        t.GetStartTime(),
		EndTime:          t.GetEndTime(),
		TimeoutInSeconds: t.GetTimeoutInSeconds(),
		Type:             t.GetType(),
		CollectionTtl:    t.CollectionTtl,
		CollectionID:     t.GetCollectionID(),
		PartitionID:      t.GetPartitionID(),
		Channel:          t.GetChannel(),
		InputSegments:    t.GetInputSegments(),
		ResultSegments:   t.GetResultSegments(),
		TotalRows:        t.TotalRows,
		Schema:           t.Schema,
		NodeID:           t.GetNodeID(),
		FailReason:       t.GetFailReason(),
		RetryTimes:       t.GetRetryTimes(),
		Pos:              t.GetPos(),
	}
	for _, opt := range opts {
		opt(taskClone)
	}
	return taskClone
}

func (t *l0CompactionTask) BuildCompactionRequest() (*datapb.CompactionPlan, error) {
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
		SlotUsage:        Params.DataCoordCfg.L0DeleteCompactionSlotUsage.GetAsInt64(),
	}

	log := log.With(zap.Int64("taskID", t.GetTriggerID()), zap.Int64("planID", plan.GetPlanID()))
	for _, segID := range t.GetInputSegments() {
		segInfo := t.meta.GetHealthySegment(segID)
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
	}

	// Select sealed L1 segments for LevelZero compaction that meets the condition:
	// dmlPos < triggerInfo.pos
	sealedSegments := t.meta.SelectSegments(WithCollection(t.GetCollectionID()), SegmentFilterFunc(func(info *SegmentInfo) bool {
		return (t.GetPartitionID() == common.AllPartitionsID || info.GetPartitionID() == t.GetPartitionID()) &&
			info.GetInsertChannel() == plan.GetChannel() &&
			isFlushState(info.GetState()) &&
			!info.GetIsImporting() &&
			info.GetLevel() != datapb.SegmentLevel_L0 &&
			info.GetStartPosition().GetTimestamp() < t.GetPos().GetTimestamp()
	}))

	if len(sealedSegments) == 0 {
		// TO-DO fast finish l0 segment, just drop l0 segment
		log.Info("l0Compaction available non-L0 Segments is empty ")
		return nil, errors.Errorf("Selected zero L1/L2 segments for the position=%v", t.GetPos())
	}

	for _, segInfo := range sealedSegments {
		// TODO should allow parallel executing of l0 compaction
		if segInfo.isCompacting {
			log.Warn("l0CompactionTask candidate segment is compacting", zap.Int64("segmentID", segInfo.GetID()))
			return nil, merr.WrapErrCompactionPlanConflict(fmt.Sprintf("segment %d is compacting", segInfo.GetID()))
		}
	}

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

	plan.SegmentBinlogs = append(plan.SegmentBinlogs, sealedSegBinlogs...)
	log.Info("l0CompactionTask refreshed level zero compaction plan",
		zap.Any("target position", t.GetPos()),
		zap.Any("target segments count", len(sealedSegBinlogs)))
	return plan, nil
}

func (t *l0CompactionTask) resetSegmentCompacting() {
	t.meta.SetSegmentsCompacting(t.GetInputSegments(), false)
}

func (t *l0CompactionTask) hasAssignedWorker() bool {
	return t.GetNodeID() != 0 && t.GetNodeID() != NullNodeID
}

func (t *l0CompactionTask) checkTimeout() bool {
	if t.GetTimeoutInSeconds() > 0 {
		start := time.Unix(t.GetStartTime(), 0)
		diff := time.Since(start).Seconds()
		if diff > float64(t.GetTimeoutInSeconds()) {
			log.Warn("compaction timeout",
				zap.Int64("taskID", t.GetTriggerID()),
				zap.Int64("planID", t.GetPlanID()),
				zap.Int64("nodeID", t.GetNodeID()),
				zap.Int32("timeout in seconds", t.GetTimeoutInSeconds()),
				zap.Time("startTime", start),
			)
			return true
		}
	}
	return false
}

func (t *l0CompactionTask) SetNodeID(id UniqueID) error {
	return t.updateAndSaveTaskMeta(setNodeID(id))
}

func (t *l0CompactionTask) SaveTaskMeta() error {
	return t.saveTaskMeta(t.CompactionTask)
}

func (t *l0CompactionTask) updateAndSaveTaskMeta(opts ...compactionTaskOpt) error {
	task := t.ShadowClone(opts...)
	err := t.saveTaskMeta(task)
	if err != nil {
		return err
	}
	t.CompactionTask = task
	return nil
}

func (t *l0CompactionTask) saveTaskMeta(task *datapb.CompactionTask) error {
	return t.meta.SaveCompactionTask(task)
}

func (t *l0CompactionTask) saveSegmentMeta() error {
	result := t.result
	var operators []UpdateOperator
	for _, seg := range result.GetSegments() {
		operators = append(operators, AddBinlogsOperator(seg.GetSegmentID(), nil, nil, seg.GetDeltalogs()))
	}

	for _, segID := range t.InputSegments {
		operators = append(operators, UpdateStatusOperator(segID, commonpb.SegmentState_Dropped), UpdateCompactedOperator(segID))
	}

	log.Info("meta update: update segments info for level zero compaction",
		zap.Int64("planID", t.GetPlanID()),
	)

	return t.meta.UpdateSegmentsInfo(operators...)
}
