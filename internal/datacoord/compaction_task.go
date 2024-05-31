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
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type CompactionTask interface {
	ProcessTask(*compactionPlanHandler) error
	BuildCompactionRequest(*compactionPlanHandler) (*datapb.CompactionPlan, error)

	GetTriggerID() int64
	GetPlanID() int64
	GetState() datapb.CompactionTaskState
	GetChannel() string
	GetType() datapb.CompactionType
	GetCollectionID() int64
	GetPartitionID() int64
	GetInputSegments() []int64
	GetStartTime() uint64
	GetTimeoutInSeconds() int32
	GetPos() *msgpb.MsgPosition

	GetPlan() *datapb.CompactionPlan
	GetResult() *datapb.CompactionPlanResult
	GetNodeID() int64
	GetSpan() trace.Span

	ShadowClone(opts ...compactionTaskOpt) CompactionTask
	SetNodeID(int64)
	SetState(datapb.CompactionTaskState)
	SetTask(*datapb.CompactionTask)
	SetSpan(trace.Span)
	SetPlan(*datapb.CompactionPlan)
	SetStartTime(startTime uint64)
	SetResult(*datapb.CompactionPlanResult)
	EndSpan()
	CleanLogPath()
}

type compactionTaskOpt func(task CompactionTask)

func setNodeID(nodeID int64) compactionTaskOpt {
	return func(task CompactionTask) {
		task.SetNodeID(nodeID)
	}
}

func setPlan(plan *datapb.CompactionPlan) compactionTaskOpt {
	return func(task CompactionTask) {
		task.SetPlan(plan)
	}
}

func setState(state datapb.CompactionTaskState) compactionTaskOpt {
	return func(task CompactionTask) {
		task.SetState(state)
	}
}

func setTask(ctask *datapb.CompactionTask) compactionTaskOpt {
	return func(task CompactionTask) {
		task.SetTask(ctask)
	}
}

func endSpan() compactionTaskOpt {
	return func(task CompactionTask) {
		task.EndSpan()
	}
}

func setStartTime(startTime uint64) compactionTaskOpt {
	return func(task CompactionTask) {
		task.SetStartTime(startTime)
	}
}

func setResult(result *datapb.CompactionPlanResult) compactionTaskOpt {
	return func(task CompactionTask) {
		task.SetResult(result)
	}
}

// cleanLogPath clean the log info in the defaultCompactionTask object for avoiding the memory leak
func cleanLogPath() compactionTaskOpt {
	return func(task CompactionTask) {
		task.CleanLogPath()
	}
}

var _ CompactionTask = (*defaultCompactionTask)(nil)

type defaultCompactionTask struct {
	*datapb.CompactionTask
	// deprecated
	triggerInfo *compactionSignal
	plan        *datapb.CompactionPlan
	dataNodeID  int64
	result      *datapb.CompactionPlanResult
	span        trace.Span
}

func (task *defaultCompactionTask) ProcessTask(handler *compactionPlanHandler) error {
	switch task.GetState() {
	case datapb.CompactionTaskState_failed:
		return nil
	case datapb.CompactionTaskState_completed:
		return nil
	case datapb.CompactionTaskState_pipelining:
		return task.processPipeliningTask(handler)
	case datapb.CompactionTaskState_executing:
		return task.processExecutingTask(handler)
	case datapb.CompactionTaskState_timeout:
		return task.processTimeoutTask(handler)
	default:
		return errors.New("not supported state")
	}
	return nil
}

func (task *defaultCompactionTask) processPipeliningTask(handler *compactionPlanHandler) error {
	nodeID, err := handler.findNode(task.GetChannel())
	if err != nil {
		return err
	}
	task.dataNodeID = nodeID
	handler.scheduler.Submit(task)
	handler.plans[task.GetPlanID()] = task.ShadowClone(setNodeID(nodeID), setState(datapb.CompactionTaskState_executing))
	log.Info("Compaction plan submited")
	return nil
}

func (task *defaultCompactionTask) processExecutingTask(handler *compactionPlanHandler) error {
	nodePlan, exist := handler.compactionResults[task.GetPlanID()]
	if !exist {
		// compaction task in DC but not found in DN means the compaction plan has failed
		log.Info("compaction failed")
		handler.plans[task.GetPlanID()] = task.ShadowClone(setState(datapb.CompactionTaskState_failed), endSpan())
		handler.setSegmentsCompacting(task, false)
		handler.scheduler.Finish(task.GetPlanID(), task)
		return nil
	}
	planResult := nodePlan.B
	switch planResult.GetState() {
	case commonpb.CompactionState_Completed:
		// channels are balanced to other nodes, yet the old datanode still have the compaction results
		// task.dataNodeID == planState.A, but
		// task.dataNodeID not match with channel
		// Mark this compaction as failure and skip processing the meta
		if !handler.chManager.Match(task.GetNodeID(), task.GetChannel()) {
			// Sync segments without CompactionFrom segmentsIDs to make sure DN clear the task
			// without changing the meta
			log.Warn("compaction failed for channel nodeID not match")
			err := handler.sessions.SyncSegments(task.GetNodeID(), &datapb.SyncSegmentsRequest{PlanID: task.GetPlanID()})
			if err != nil {
				log.Warn("compaction failed to sync segments with node", zap.Error(err))
				return err
			}
			handler.setSegmentsCompacting(task, false)
			handler.plans[task.GetPlanID()] = task.ShadowClone(setState(datapb.CompactionTaskState_failed), cleanLogPath(), endSpan())
			handler.scheduler.Finish(task.GetNodeID(), task)
			return nil
		}
		switch task.GetType() {
		case datapb.CompactionType_MergeCompaction, datapb.CompactionType_MixCompaction:
			if err := handler.handleMergeCompactionResult(task.GetPlan(), planResult); err != nil {
				return err
			}
		case datapb.CompactionType_Level0DeleteCompaction:
			if err := handler.handleL0CompactionResult(task.GetPlan(), planResult); err != nil {
				return err
			}
		}
		UpdateCompactionSegmentSizeMetrics(planResult.GetSegments())
		handler.plans[task.GetPlanID()] = task.ShadowClone(setState(datapb.CompactionTaskState_completed), setResult(planResult), cleanLogPath(), endSpan())
		handler.scheduler.Finish(task.GetNodeID(), task)
	case commonpb.CompactionState_Executing:
		ts := tsoutil.GetCurrentTime()
		if isTimeout(ts, task.GetStartTime(), task.GetTimeoutInSeconds()) {
			log.Warn("compaction timeout",
				zap.Int32("timeout in seconds", task.GetTimeoutInSeconds()),
				zap.Uint64("startTime", task.GetStartTime()),
				zap.Uint64("now", ts),
			)
			handler.plans[task.GetPlanID()] = task.ShadowClone(setState(datapb.CompactionTaskState_timeout), endSpan())
		}
	}
	return nil
}

func (task *defaultCompactionTask) processTimeoutTask(handler *compactionPlanHandler) error {
	log := log.With(
		zap.Int64("planID", task.GetPlanID()),
		zap.Int64("nodeID", task.GetNodeID()),
		zap.String("channel", task.GetChannel()),
	)
	planID := task.GetPlanID()
	if nodePlan, ok := handler.compactionResults[task.GetPlanID()]; ok {
		if nodePlan.B.GetState() == commonpb.CompactionState_Executing {
			log.RatedInfo(1, "compaction timeout in DataCoord yet DataNode is still running")
		}
	} else {
		// compaction task in DC but not found in DN means the compaction plan has failed
		log.Info("compaction failed for timeout")
		handler.plans[planID] = task.ShadowClone(setState(datapb.CompactionTaskState_failed), endSpan())
		handler.setSegmentsCompacting(task, false)
		handler.scheduler.Finish(task.GetNodeID(), task)
	}
	return nil
}

func (task *defaultCompactionTask) GetCollectionID() int64 {
	return task.CompactionTask.GetCollectionID()
}

func (task *defaultCompactionTask) GetPartitionID() int64 {
	return task.CompactionTask.GetPartitionID()
}

func (task *defaultCompactionTask) GetTriggerID() int64 {
	return task.CompactionTask.GetTriggerID()
}

func (task *defaultCompactionTask) GetSpan() trace.Span {
	return task.span
}

func (task *defaultCompactionTask) GetType() datapb.CompactionType {
	return task.CompactionTask.GetType()
}

func (task *defaultCompactionTask) GetChannel() string {
	return task.CompactionTask.GetChannel()
}

func (task *defaultCompactionTask) GetPlanID() int64 {
	return task.CompactionTask.GetPlanID()
}

func (task *defaultCompactionTask) GetResult() *datapb.CompactionPlanResult {
	return task.result
}

func (task *defaultCompactionTask) GetNodeID() int64 {
	return task.dataNodeID
}

func (task *defaultCompactionTask) GetState() datapb.CompactionTaskState {
	return task.CompactionTask.GetState()
}

func (task *defaultCompactionTask) GetPlan() *datapb.CompactionPlan {
	return task.plan
}

func (task *defaultCompactionTask) ShadowClone(opts ...compactionTaskOpt) CompactionTask {
	taskClone := &defaultCompactionTask{
		CompactionTask: task.CompactionTask,
		plan:           task.plan,
		dataNodeID:     task.dataNodeID,
		span:           task.span,
		result:         task.result,
	}
	for _, opt := range opts {
		opt(taskClone)
	}
	return taskClone
}

func (task *defaultCompactionTask) EndSpan() {
	if task.span != nil {
		task.span.End()
	}
}

func (task *defaultCompactionTask) SetNodeID(nodeID int64) {
	task.dataNodeID = nodeID
}

func (task *defaultCompactionTask) SetState(state datapb.CompactionTaskState) {
	task.State = state
}

func (task *defaultCompactionTask) SetTask(ct *datapb.CompactionTask) {
	task.CompactionTask = ct
}

func (task *defaultCompactionTask) SetStartTime(startTime uint64) {
	task.StartTime = startTime
}

func (task *defaultCompactionTask) SetResult(result *datapb.CompactionPlanResult) {
	task.result = result
}

func (task *defaultCompactionTask) SetSpan(span trace.Span) {
	task.span = span
}

func (task *defaultCompactionTask) SetPlan(plan *datapb.CompactionPlan) {
	task.plan = plan
}

func (task *defaultCompactionTask) CleanLogPath() {
	if task.plan.GetSegmentBinlogs() != nil {
		for _, binlogs := range task.plan.GetSegmentBinlogs() {
			binlogs.FieldBinlogs = nil
			binlogs.Field2StatslogPaths = nil
			binlogs.Deltalogs = nil
		}
	}
	if task.result.GetSegments() != nil {
		for _, segment := range task.result.GetSegments() {
			segment.InsertLogs = nil
			segment.Deltalogs = nil
			segment.Field2StatslogPaths = nil
		}
	}
}

func (task *defaultCompactionTask) BuildCompactionRequest(handler *compactionPlanHandler) (*datapb.CompactionPlan, error) {
	plan := &datapb.CompactionPlan{
		PlanID:           task.GetPlanID(),
		StartTime:        task.GetStartTime(),
		TimeoutInSeconds: task.GetTimeoutInSeconds(),
		Type:             task.GetType(),
		Channel:          task.GetChannel(),
		CollectionTtl:    task.GetCollectionTtl(),
		TotalRows:        task.GetTotalRows(),
	}
	log := log.With(zap.Int64("taskID", task.GetTriggerID()), zap.Int64("planID", plan.GetPlanID()))
	if task.GetType() == datapb.CompactionType_Level0DeleteCompaction {
		for _, segID := range task.GetInputSegments() {
			segInfo := handler.meta.GetHealthySegment(segID)
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
			})
		}

		// Select sealed L1 segments for LevelZero compaction that meets the condition:
		// dmlPos < triggerInfo.pos
		sealedSegments := handler.meta.SelectSegments(WithCollection(task.GetCollectionID()), SegmentFilterFunc(func(info *SegmentInfo) bool {
			return (task.GetPartitionID() == -1 || info.GetPartitionID() == task.GetPartitionID()) &&
				info.GetInsertChannel() == plan.GetChannel() &&
				isFlushState(info.GetState()) &&
				!info.isCompacting &&
				!info.GetIsImporting() &&
				info.GetLevel() != datapb.SegmentLevel_L0 &&
				info.GetDmlPosition().GetTimestamp() < task.GetPos().GetTimestamp()
		}))
		if len(sealedSegments) == 0 {
			return nil, errors.Errorf("Selected zero L1/L2 segments for the position=%v", task.GetPos())
		}

		sealedSegBinlogs := lo.Map(sealedSegments, func(segInfo *SegmentInfo, _ int) *datapb.CompactionSegmentBinlogs {
			return &datapb.CompactionSegmentBinlogs{
				SegmentID:    segInfo.GetID(),
				Level:        segInfo.GetLevel(),
				CollectionID: segInfo.GetCollectionID(),
				PartitionID:  segInfo.GetPartitionID(),
				// no need for binlogs info, as L0Compaction only append deltalogs on existing segments
			}
		})

		plan.SegmentBinlogs = append(plan.SegmentBinlogs, sealedSegBinlogs...)
		log.Info("Compaction handler refreshed level zero compaction plan",
			zap.Any("target position", task.GetPos()),
			zap.Any("target segments count", len(sealedSegBinlogs)))
		return nil, nil
	}

	if task.GetType() == datapb.CompactionType_MixCompaction {
		segIDMap := make(map[int64][]*datapb.FieldBinlog, len(plan.SegmentBinlogs))
		for _, segID := range task.GetInputSegments() {
			segInfo := handler.meta.GetHealthySegment(segID)
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
	}
	handler.plans[task.GetPlanID()] = task.ShadowClone(setPlan(plan))
	return plan, nil
}
