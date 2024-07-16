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
	"path"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ CompactionTask = (*clusteringCompactionTask)(nil)

const (
	taskMaxRetryTimes = int32(3)
)

type clusteringCompactionTask struct {
	pbGuard sync.RWMutex
	pb      *datapb.CompactionTask
	plan    *datapb.CompactionPlan
	result  *datapb.CompactionPlanResult

	span             trace.Span
	allocator        allocator
	meta             CompactionMeta
	sessions         SessionManager
	handler          Handler
	analyzeScheduler *taskScheduler
}

func (t *clusteringCompactionTask) GetTriggerID() UniqueID {
	return t.GetTaskPB().GetTriggerID()
}

func (t *clusteringCompactionTask) GetPlanID() UniqueID {
	return t.GetTaskPB().GetPlanID()
}

func (t *clusteringCompactionTask) GetState() datapb.CompactionTaskState {
	return t.GetTaskPB().GetState()
}

func (t *clusteringCompactionTask) GetChannel() string {
	return t.GetTaskPB().GetChannel()
}

func (t *clusteringCompactionTask) GetType() datapb.CompactionType {
	return t.GetTaskPB().GetType()
}

func (t *clusteringCompactionTask) GetCollectionID() int64 {
	return t.GetTaskPB().GetCollectionID()
}

func (t *clusteringCompactionTask) GetPartitionID() int64 {
	return t.GetTaskPB().GetPartitionID()
}

func (t *clusteringCompactionTask) GetInputSegments() []int64 {
	return t.GetTaskPB().GetInputSegments()
}

func (t *clusteringCompactionTask) GetStartTime() int64 {
	return t.GetTaskPB().GetStartTime()
}

func (t *clusteringCompactionTask) GetTimeoutInSeconds() int32 {
	return t.GetTaskPB().GetTimeoutInSeconds()
}

func (t *clusteringCompactionTask) GetPos() *msgpb.MsgPosition {
	return t.GetTaskPB().GetPos()
}

func (t *clusteringCompactionTask) GetNodeID() UniqueID {
	return t.GetTaskPB().GetNodeID()
}

func (t *clusteringCompactionTask) GetTaskPB() *datapb.CompactionTask {
	t.pbGuard.RLock()
	defer t.pbGuard.RUnlock()
	return t.pb
}

func (t *clusteringCompactionTask) Process() bool {
	log := log.With(zap.Int64("triggerID", t.pb.GetTriggerID()), zap.Int64("PlanID", t.pb.GetPlanID()), zap.Int64("collectionID", t.pb.GetCollectionID()))
	lastState := t.pb.GetState().String()
	err := t.retryableProcess()
	if err != nil {
		log.Warn("fail in process task", zap.Error(err))
		if merr.IsRetryableErr(err) && t.pb.GetRetryTimes() < taskMaxRetryTimes {
			// retry in next Process
			t.updateAndSaveTaskMeta(setRetryTimes(t.pb.GetRetryTimes() + 1))
		} else {
			log.Error("task fail with unretryable reason or meet max retry times", zap.Error(err))
			t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
		}
	}
	// task state update, refresh retry times count
	currentState := t.pb.GetState().String()
	if currentState != lastState {
		ts := time.Now().UnixMilli()
		lastStateDuration := ts - t.pb.GetLastStateStartTime()
		log.Info("clustering compaction task state changed", zap.String("lastState", lastState), zap.String("currentState", currentState), zap.Int64("elapse", lastStateDuration))
		metrics.DataCoordCompactionLatency.
			WithLabelValues(fmt.Sprint(typeutil.IsVectorType(t.pb.GetClusteringKeyField().DataType)), fmt.Sprint(t.pb.GetCollectionID()), t.pb.GetChannel(), datapb.CompactionType_ClusteringCompaction.String(), lastState).
			Observe(float64(lastStateDuration))
		t.updateAndSaveTaskMeta(setRetryTimes(0), setLastStateStartTime(ts))

		if t.pb.GetState() == datapb.CompactionTaskState_completed {
			t.updateAndSaveTaskMeta(setEndTime(ts))
			elapse := ts - t.pb.GetStartTime()
			log.Info("clustering compaction task total elapse", zap.Int64("elapse", elapse))
			metrics.DataCoordCompactionLatency.
				WithLabelValues(fmt.Sprint(typeutil.IsVectorType(t.pb.GetClusteringKeyField().DataType)), fmt.Sprint(t.pb.GetCollectionID()), t.pb.GetChannel(), datapb.CompactionType_ClusteringCompaction.String(), "total").
				Observe(float64(elapse))
		}
	}
	log.Debug("process clustering task", zap.String("lastState", lastState), zap.String("currentState", currentState))
	return t.pb.GetState() == datapb.CompactionTaskState_completed || t.pb.GetState() == datapb.CompactionTaskState_cleaned
}

// retryableProcess process task's state transfer, return error if not work as expected
// the outer Process will set state and retry times according to the error type(retryable or not-retryable)
func (t *clusteringCompactionTask) retryableProcess() error {
	if t.pb.GetState() == datapb.CompactionTaskState_completed || t.pb.GetState() == datapb.CompactionTaskState_cleaned {
		return nil
	}

	coll, err := t.handler.GetCollection(context.Background(), t.pb.GetCollectionID())
	if err != nil {
		// retryable
		log.Warn("fail to get collection", zap.Int64("collectionID", t.pb.GetCollectionID()), zap.Error(err))
		return merr.WrapErrClusteringCompactionGetCollectionFail(t.pb.GetCollectionID(), err)
	}
	if coll == nil {
		// not-retryable fail fast if collection is dropped
		log.Warn("collection not found, it may be dropped, stop clustering compaction task", zap.Int64("collectionID", t.pb.GetCollectionID()))
		return merr.WrapErrCollectionNotFound(t.pb.GetCollectionID())
	}

	switch t.pb.GetState() {
	case datapb.CompactionTaskState_pipelining:
		return t.processPipelining()
	case datapb.CompactionTaskState_executing:
		return t.processExecuting()
	case datapb.CompactionTaskState_analyzing:
		return t.processAnalyzing()
	case datapb.CompactionTaskState_meta_saved:
		return t.processMetaSaved()
	case datapb.CompactionTaskState_indexing:
		return t.processIndexing()
	case datapb.CompactionTaskState_timeout:
		return t.processFailedOrTimeout()
	case datapb.CompactionTaskState_failed:
		return t.processFailedOrTimeout()
	}
	return nil
}

func (t *clusteringCompactionTask) BuildCompactionRequest() (*datapb.CompactionPlan, error) {
	beginLogID, _, err := t.allocator.allocN(1)
	if err != nil {
		return nil, err
	}
	plan := &datapb.CompactionPlan{
		PlanID:             t.pb.GetPlanID(),
		StartTime:          t.pb.GetStartTime(),
		TimeoutInSeconds:   t.pb.GetTimeoutInSeconds(),
		Type:               t.pb.GetType(),
		Channel:            t.pb.GetChannel(),
		CollectionTtl:      t.pb.GetCollectionTtl(),
		TotalRows:          t.pb.GetTotalRows(),
		Schema:             t.pb.GetSchema(),
		ClusteringKeyField: t.pb.GetClusteringKeyField().GetFieldID(),
		MaxSegmentRows:     t.pb.GetMaxSegmentRows(),
		PreferSegmentRows:  t.pb.GetPreferSegmentRows(),
		AnalyzeResultPath:  path.Join(t.meta.(*meta).chunkManager.RootPath(), common.AnalyzeStatsPath, metautil.JoinIDPath(t.pb.GetAnalyzeTaskID(), t.pb.GetAnalyzeVersion())),
		AnalyzeSegmentIds:  t.pb.GetInputSegments(),
		BeginLogID:         beginLogID,
		PreAllocatedSegments: &datapb.IDRange{
			Begin: t.pb.GetResultSegments()[0],
			End:   t.pb.GetResultSegments()[1],
		},
		SlotUsage: Params.DataCoordCfg.ClusteringCompactionSlotUsage.GetAsInt64(),
	}
	log := log.With(zap.Int64("taskID", t.pb.GetTriggerID()), zap.Int64("planID", plan.GetPlanID()))

	for _, segID := range t.pb.GetInputSegments() {
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
	}
	log.Info("Compaction handler build clustering compaction plan")
	return plan, nil
}

func (t *clusteringCompactionTask) processPipelining() error {
	log := log.With(zap.Int64("triggerID", t.pb.GetTriggerID()), zap.Int64("collectionID", t.pb.GetCollectionID()), zap.Int64("planID", t.pb.GetPlanID()))
	ts := time.Now().UnixMilli()
	t.updateAndSaveTaskMeta(setStartTime(ts))
	var operators []UpdateOperator
	for _, segID := range t.pb.GetInputSegments() {
		operators = append(operators, UpdateSegmentLevelOperator(segID, datapb.SegmentLevel_L2))
	}
	err := t.meta.UpdateSegmentsInfo(operators...)
	if err != nil {
		log.Warn("fail to set segment level to L2", zap.Error(err))
		return merr.WrapErrClusteringCompactionMetaError("UpdateSegmentsInfo before compaction executing", err)
	}

	if typeutil.IsVectorType(t.pb.GetClusteringKeyField().DataType) {
		err := t.doAnalyze()
		if err != nil {
			log.Warn("fail to submit analyze task", zap.Error(err))
			return merr.WrapErrClusteringCompactionSubmitTaskFail("analyze", err)
		}
	} else {
		err := t.doCompact()
		if err != nil {
			log.Warn("fail to submit compaction task", zap.Error(err))
			return merr.WrapErrClusteringCompactionSubmitTaskFail("compact", err)
		}
	}
	return nil
}

func (t *clusteringCompactionTask) processExecuting() error {
	log := log.With(zap.Int64("planID", t.pb.GetPlanID()), zap.String("type", t.pb.GetType().String()))
	result, err := t.sessions.GetCompactionPlanResult(t.pb.GetNodeID(), t.pb.GetPlanID())
	if err != nil || result == nil {
		if errors.Is(err, merr.ErrNodeNotFound) {
			log.Warn("GetCompactionPlanResult fail", zap.Error(err))
			// setNodeID(NullNodeID) to trigger reassign node ID
			t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
			return nil
		}
		return err
	}
	log.Info("compaction result", zap.Any("result", result.String()))
	switch result.GetState() {
	case datapb.CompactionTaskState_completed:
		t.result = result
		result := t.result
		if len(result.GetSegments()) == 0 {
			log.Warn("illegal compaction results, this should not happen")
			t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
			return merr.WrapErrCompactionResult("compaction result is empty")
		}

		resultSegmentIDs := lo.Map(result.Segments, func(segment *datapb.CompactionSegment, _ int) int64 {
			return segment.GetSegmentID()
		})

		_, metricMutation, err := t.meta.CompleteCompactionMutation(t.pb, t.result)
		if err != nil {
			return err
		}
		metricMutation.commit()
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_meta_saved), setResultSegments(resultSegmentIDs))
		if err != nil {
			return err
		}
		return t.processMetaSaved()
	case datapb.CompactionTaskState_executing:
		if t.checkTimeout() {
			err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_timeout))
			if err == nil {
				return t.processFailedOrTimeout()
			} else {
				return err
			}
		}
		return nil
	case datapb.CompactionTaskState_failed:
		return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
	}
	return nil
}

func (t *clusteringCompactionTask) processMetaSaved() error {
	return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_indexing))
}

func (t *clusteringCompactionTask) processIndexing() error {
	// wait for segment indexed
	collectionIndexes := t.meta.GetIndexMeta().GetIndexesForCollection(t.pb.GetCollectionID(), "")
	indexed := func() bool {
		for _, collectionIndex := range collectionIndexes {
			for _, segmentID := range t.pb.GetResultSegments() {
				segmentIndexState := t.meta.GetIndexMeta().GetSegmentIndexState(t.pb.GetCollectionID(), segmentID, collectionIndex.IndexID)
				if segmentIndexState.GetState() != commonpb.IndexState_Finished {
					return false
				}
			}
		}
		return true
	}()
	log.Debug("check compaction result segments index states", zap.Bool("indexed", indexed), zap.Int64("planID", t.pb.GetPlanID()), zap.Int64s("segments", t.pb.GetResultSegments()))
	if indexed {
		t.completeTask()
	}
	return nil
}

// indexed is the final state of a clustering compaction task
// one task should only run this once
func (t *clusteringCompactionTask) completeTask() error {
	err := t.meta.GetPartitionStatsMeta().SavePartitionStatsInfo(&datapb.PartitionStatsInfo{
		CollectionID: t.pb.GetCollectionID(),
		PartitionID:  t.pb.GetPartitionID(),
		VChannel:     t.pb.GetChannel(),
		Version:      t.pb.GetPlanID(),
		SegmentIDs:   t.pb.GetResultSegments(),
	})
	if err != nil {
		return merr.WrapErrClusteringCompactionMetaError("SavePartitionStatsInfo", err)
	}

	var operators []UpdateOperator
	for _, segID := range t.pb.GetResultSegments() {
		operators = append(operators, UpdateSegmentPartitionStatsVersionOperator(segID, t.pb.GetPlanID()))
	}
	err = t.meta.UpdateSegmentsInfo(operators...)
	if err != nil {
		return merr.WrapErrClusteringCompactionMetaError("UpdateSegmentPartitionStatsVersion", err)
	}

	err = t.meta.GetPartitionStatsMeta().SaveCurrentPartitionStatsVersion(t.pb.GetCollectionID(), t.pb.GetPartitionID(), t.pb.GetChannel(), t.pb.GetPlanID())
	if err != nil {
		return merr.WrapErrClusteringCompactionMetaError("SaveCurrentPartitionStatsVersion", err)
	}
	return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_completed))
}

func (t *clusteringCompactionTask) processAnalyzing() error {
	analyzeTask := t.meta.GetAnalyzeMeta().GetTask(t.pb.GetAnalyzeTaskID())
	if analyzeTask == nil {
		log.Warn("analyzeTask not found", zap.Int64("id", t.pb.GetAnalyzeTaskID()))
		return merr.WrapErrAnalyzeTaskNotFound(t.pb.GetAnalyzeTaskID()) // retryable
	}
	log := log.With(zap.Int64("planID", t.pb.GetPlanID()), zap.Int64("analyzeTaskID", t.pb.GetAnalyzeTaskID()),
		zap.Int64("version", analyzeTask.GetVersion()), zap.String("state", analyzeTask.State.String()))
	log.Info("check analyze task state")
	switch analyzeTask.State {
	case indexpb.JobState_JobStateFinished:
		if analyzeTask.GetCentroidsFile() == "" {
			// not retryable, fake finished vector clustering is not supported in opensource
			return merr.WrapErrClusteringCompactionNotSupportVector()
		} else {
			err := t.updateAndSaveTaskMeta(setAnalyzeVersion(analyzeTask.GetVersion()))
			if err == nil {
				return t.doCompact()
			}
			log.Warn("updateAndSaveTaskMeta failed")
			return err
		}
	case indexpb.JobState_JobStateFailed:
		log.Warn("analyze task fail")
		return errors.New(analyzeTask.FailReason)
	default:
	}
	return nil
}

func (t *clusteringCompactionTask) resetSegmentCompacting() {
	t.meta.SetSegmentsCompacting(t.pb.GetInputSegments(), false)
}

func (t *clusteringCompactionTask) processFailedOrTimeout() error {
	log.Info("clean task", zap.Int64("triggerID", t.pb.GetTriggerID()), zap.Int64("planID", t.pb.GetPlanID()), zap.String("state", t.pb.GetState().String()))
	// revert segments meta
	var operators []UpdateOperator
	// revert level of input segments
	// L1 : L1 ->(processPipelining)-> L2 ->(processFailedOrTimeout)-> L1
	// L2 : L2 ->(processPipelining)-> L2 ->(processFailedOrTimeout)-> L2
	for _, segID := range t.pb.GetInputSegments() {
		operators = append(operators, RevertSegmentLevelOperator(segID))
	}
	// if result segments are generated but task fail in the other steps, mark them as L1 segments without partitions stats
	for _, segID := range t.pb.GetResultSegments() {
		operators = append(operators, UpdateSegmentLevelOperator(segID, datapb.SegmentLevel_L1))
		operators = append(operators, UpdateSegmentPartitionStatsVersionOperator(segID, 0))
	}
	err := t.meta.UpdateSegmentsInfo(operators...)
	if err != nil {
		log.Warn("UpdateSegmentsInfo fail", zap.Error(err))
		return merr.WrapErrClusteringCompactionMetaError("UpdateSegmentsInfo", err)
	}
	t.resetSegmentCompacting()

	// drop partition stats if uploaded
	partitionStatsInfo := &datapb.PartitionStatsInfo{
		CollectionID: t.pb.GetCollectionID(),
		PartitionID:  t.pb.GetPartitionID(),
		VChannel:     t.pb.GetChannel(),
		Version:      t.pb.GetPlanID(),
		SegmentIDs:   t.pb.GetResultSegments(),
	}
	err = t.meta.CleanPartitionStatsInfo(partitionStatsInfo)
	if err != nil {
		log.Warn("gcPartitionStatsInfo fail", zap.Error(err))
	}

	t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_cleaned))
	return nil
}

func (t *clusteringCompactionTask) doAnalyze() error {
	newAnalyzeTask := &indexpb.AnalyzeTask{
		CollectionID: t.pb.GetCollectionID(),
		PartitionID:  t.pb.GetPartitionID(),
		FieldID:      t.pb.GetClusteringKeyField().FieldID,
		FieldName:    t.pb.GetClusteringKeyField().Name,
		FieldType:    t.pb.GetClusteringKeyField().DataType,
		SegmentIDs:   t.pb.GetInputSegments(),
		TaskID:       t.pb.GetAnalyzeTaskID(),
		State:        indexpb.JobState_JobStateInit,
	}
	err := t.meta.GetAnalyzeMeta().AddAnalyzeTask(newAnalyzeTask)
	if err != nil {
		log.Warn("failed to create analyze task", zap.Int64("planID", t.pb.GetPlanID()), zap.Error(err))
		return err
	}
	t.analyzeScheduler.enqueue(&analyzeTask{
		taskID: t.pb.GetAnalyzeTaskID(),
		taskInfo: &indexpb.AnalyzeResult{
			TaskID: t.pb.GetAnalyzeTaskID(),
			State:  indexpb.JobState_JobStateInit,
		},
	})
	t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_analyzing))
	log.Info("submit analyze task", zap.Int64("planID", t.pb.GetPlanID()), zap.Int64("triggerID", t.pb.GetTriggerID()), zap.Int64("collectionID", t.pb.GetCollectionID()), zap.Int64("id", t.pb.GetAnalyzeTaskID()))
	return nil
}

func (t *clusteringCompactionTask) doCompact() error {
	log := log.With(zap.Int64("planID", t.pb.GetPlanID()), zap.String("type", t.pb.GetType().String()))
	if t.NeedReAssignNodeID() {
		log.RatedWarn(10, "not assign nodeID")
		return nil
	}

	// todo refine this logic: GetCompactionPlanResult return a fail result when this is no compaction in datanode which is weird
	// check whether the compaction plan is already submitted considering
	// datacoord may crash between call sessions.Compaction and updateTaskState to executing
	// result, err := t.sessions.GetCompactionPlanResult(t.GetNodeID(), t.GetPlanID())
	// if err != nil {
	//	if errors.Is(err, merr.ErrNodeNotFound) {
	//		log.Warn("GetCompactionPlanResult fail", zap.Error(err))
	//		// setNodeID(NullNodeID) to trigger reassign node ID
	//		t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
	//		return nil
	//	}
	//	return merr.WrapErrGetCompactionPlanResultFail(err)
	// }
	// if result != nil {
	//	log.Info("compaction already submitted")
	//	t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_executing))
	//	return nil
	// }

	var err error
	t.plan, err = t.BuildCompactionRequest()
	if err != nil {
		log.Warn("Failed to BuildCompactionRequest", zap.Error(err))
		return merr.WrapErrBuildCompactionRequestFail(err) // retryable
	}
	err = t.sessions.Compaction(context.Background(), t.pb.GetNodeID(), t.GetPlan())
	if err != nil {
		if errors.Is(err, merr.ErrDataNodeSlotExhausted) {
			log.Warn("fail to notify compaction tasks to DataNode because the node slots exhausted")
			t.updateAndSaveTaskMeta(setNodeID(NullNodeID))
			return nil
		}
		log.Warn("Failed to notify compaction tasks to DataNode", zap.Error(err))
		t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
		return err
	}
	t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_executing))
	return nil
}

func (t *clusteringCompactionTask) CloneTaskPB(opts ...compactionTaskOpt) *datapb.CompactionTask {
	pbClone := proto.Clone(t.GetTaskPB()).(*datapb.CompactionTask)
	for _, opt := range opts {
		opt(pbClone)
	}
	return pbClone
}

func (t *clusteringCompactionTask) updateAndSaveTaskMeta(opts ...compactionTaskOpt) error {
	t.pbGuard.Lock()
	defer t.pbGuard.Unlock()
	pbCloned := proto.Clone(t.pb).(*datapb.CompactionTask)
	for _, opt := range opts {
		opt(pbCloned)
	}
	err := t.saveTaskMeta(pbCloned)
	if err != nil {
		log.Warn("Failed to saveTaskMeta", zap.Error(err))
		return merr.WrapErrClusteringCompactionMetaError("updateAndSaveTaskMeta", err) // retryable
	}
	t.pb = pbCloned
	return nil
}

func (t *clusteringCompactionTask) checkTimeout() bool {
	if t.pb.GetTimeoutInSeconds() > 0 {
		diff := time.Since(time.Unix(t.pb.GetStartTime(), 0)).Seconds()
		if diff > float64(t.pb.GetTimeoutInSeconds()) {
			log.Warn("compaction timeout",
				zap.Int32("timeout in seconds", t.pb.GetTimeoutInSeconds()),
				zap.Int64("startTime", t.pb.GetStartTime()),
			)
			return true
		}
	}
	return false
}

func (t *clusteringCompactionTask) saveTaskMeta(task *datapb.CompactionTask) error {
	return t.meta.SaveCompactionTask(task)
}

func (t *clusteringCompactionTask) SaveTaskMeta() error {
	return t.saveTaskMeta(t.GetTaskPB())
}

func (t *clusteringCompactionTask) GetPlan() *datapb.CompactionPlan {
	return t.plan
}

func (t *clusteringCompactionTask) GetResult() *datapb.CompactionPlanResult {
	return t.result
}

func (t *clusteringCompactionTask) GetSpan() trace.Span {
	return t.span
}

func (t *clusteringCompactionTask) EndSpan() {
	if t.span != nil {
		t.span.End()
	}
}

func (t *clusteringCompactionTask) SetResult(result *datapb.CompactionPlanResult) {
	t.result = result
}

func (t *clusteringCompactionTask) SetSpan(span trace.Span) {
	t.span = span
}

func (t *clusteringCompactionTask) SetPlan(plan *datapb.CompactionPlan) {
	t.plan = plan
}

func (t *clusteringCompactionTask) SetNodeID(id UniqueID) error {
	return t.updateAndSaveTaskMeta(setNodeID(id))
}

func (t *clusteringCompactionTask) GetLabel() string {
	pb := t.GetTaskPB()
	return fmt.Sprintf("%d-%s", pb.GetPartitionID(), pb.GetChannel())
}

func (t *clusteringCompactionTask) NeedReAssignNodeID() bool {
	pb := t.GetTaskPB()
	return pb.GetState() == datapb.CompactionTaskState_pipelining && (pb.GetNodeID() == 0 || pb.GetNodeID() == NullNodeID)
}

func (t *clusteringCompactionTask) CleanLogPath() {
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
