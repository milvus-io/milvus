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
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
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

type clusteringCompactionTask struct {
	*datapb.CompactionTask
	plan   *datapb.CompactionPlan
	result *datapb.CompactionPlanResult

	span             trace.Span
	allocator        allocator.Allocator
	meta             CompactionMeta
	sessions         session.DataNodeManager
	handler          Handler
	analyzeScheduler *taskScheduler

	maxRetryTimes int32
}

func newClusteringCompactionTask(t *datapb.CompactionTask, allocator allocator.Allocator, meta CompactionMeta, session session.DataNodeManager, handler Handler, analyzeScheduler *taskScheduler) *clusteringCompactionTask {
	return &clusteringCompactionTask{
		CompactionTask:   t,
		allocator:        allocator,
		meta:             meta,
		sessions:         session,
		handler:          handler,
		analyzeScheduler: analyzeScheduler,
		maxRetryTimes:    3,
	}
}

func (t *clusteringCompactionTask) Process() bool {
	log := log.With(zap.Int64("triggerID", t.GetTriggerID()), zap.Int64("PlanID", t.GetPlanID()), zap.Int64("collectionID", t.GetCollectionID()))
	lastState := t.GetState().String()
	err := t.retryableProcess()
	if err != nil {
		log.Warn("fail in process task", zap.Error(err))
		if merr.IsRetryableErr(err) && t.RetryTimes < t.maxRetryTimes {
			// retry in next Process
			err = t.updateAndSaveTaskMeta(setRetryTimes(t.RetryTimes + 1))
		} else {
			log.Error("task fail with unretryable reason or meet max retry times", zap.Error(err))
			err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
		}
		if err != nil {
			log.Warn("Failed to updateAndSaveTaskMeta", zap.Error(err))
		}
	}
	// task state update, refresh retry times count
	currentState := t.State.String()
	if currentState != lastState {
		ts := time.Now().Unix()
		lastStateDuration := ts - t.GetLastStateStartTime()
		log.Info("clustering compaction task state changed", zap.String("lastState", lastState), zap.String("currentState", currentState), zap.Int64("elapse seconds", lastStateDuration))
		metrics.DataCoordCompactionLatency.
			WithLabelValues(fmt.Sprint(typeutil.IsVectorType(t.GetClusteringKeyField().DataType)), fmt.Sprint(t.CollectionID), t.Channel, datapb.CompactionType_ClusteringCompaction.String(), lastState).
			Observe(float64(lastStateDuration * 1000))
		updateOps := []compactionTaskOpt{setRetryTimes(0), setLastStateStartTime(ts)}

		if t.State == datapb.CompactionTaskState_completed || t.State == datapb.CompactionTaskState_cleaned {
			updateOps = append(updateOps, setEndTime(ts))
			elapse := ts - t.StartTime
			log.Info("clustering compaction task total elapse", zap.Int64("elapse seconds", elapse))
			metrics.DataCoordCompactionLatency.
				WithLabelValues(fmt.Sprint(typeutil.IsVectorType(t.GetClusteringKeyField().DataType)), fmt.Sprint(t.CollectionID), t.Channel, datapb.CompactionType_ClusteringCompaction.String(), "total").
				Observe(float64(elapse * 1000))
		}
		err = t.updateAndSaveTaskMeta(updateOps...)
		if err != nil {
			log.Warn("Failed to updateAndSaveTaskMeta", zap.Error(err))
		}
	}
	log.Debug("process clustering task", zap.String("lastState", lastState), zap.String("currentState", currentState))
	return t.State == datapb.CompactionTaskState_completed || t.State == datapb.CompactionTaskState_cleaned
}

// retryableProcess process task's state transfer, return error if not work as expected
// the outer Process will set state and retry times according to the error type(retryable or not-retryable)
func (t *clusteringCompactionTask) retryableProcess() error {
	if t.State == datapb.CompactionTaskState_completed || t.State == datapb.CompactionTaskState_cleaned {
		return nil
	}

	coll, err := t.handler.GetCollection(context.Background(), t.GetCollectionID())
	if err != nil {
		// retryable
		log.Warn("fail to get collection", zap.Int64("collectionID", t.GetCollectionID()), zap.Error(err))
		return merr.WrapErrClusteringCompactionGetCollectionFail(t.GetCollectionID(), err)
	}
	if coll == nil {
		// not-retryable fail fast if collection is dropped
		log.Warn("collection not found, it may be dropped, stop clustering compaction task", zap.Int64("collectionID", t.GetCollectionID()))
		return merr.WrapErrCollectionNotFound(t.GetCollectionID())
	}

	switch t.State {
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
	case datapb.CompactionTaskState_statistic:
		return t.processStats()

	case datapb.CompactionTaskState_timeout:
		return t.processFailedOrTimeout()
	case datapb.CompactionTaskState_failed:
		return t.processFailedOrTimeout()
	}
	return nil
}

func (t *clusteringCompactionTask) BuildCompactionRequest() (*datapb.CompactionPlan, error) {
	beginLogID, _, err := t.allocator.AllocN(1)
	if err != nil {
		return nil, err
	}
	plan := &datapb.CompactionPlan{
		PlanID:             t.GetPlanID(),
		StartTime:          t.GetStartTime(),
		TimeoutInSeconds:   t.GetTimeoutInSeconds(),
		Type:               t.GetType(),
		Channel:            t.GetChannel(),
		CollectionTtl:      t.GetCollectionTtl(),
		TotalRows:          t.GetTotalRows(),
		Schema:             t.GetSchema(),
		ClusteringKeyField: t.GetClusteringKeyField().GetFieldID(),
		MaxSegmentRows:     t.GetMaxSegmentRows(),
		PreferSegmentRows:  t.GetPreferSegmentRows(),
		AnalyzeResultPath:  path.Join(t.meta.(*meta).chunkManager.RootPath(), common.AnalyzeStatsPath, metautil.JoinIDPath(t.AnalyzeTaskID, t.AnalyzeVersion)),
		AnalyzeSegmentIds:  t.GetInputSegments(),
		BeginLogID:         beginLogID,
		PreAllocatedSegmentIDs: &datapb.IDRange{
			Begin: t.GetResultSegments()[0],
			End:   t.GetResultSegments()[1],
		},
		SlotUsage: Params.DataCoordCfg.ClusteringCompactionSlotUsage.GetAsInt64(),
	}
	log := log.With(zap.Int64("taskID", t.GetTriggerID()), zap.Int64("planID", plan.GetPlanID()))

	for _, segID := range t.GetInputSegments() {
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
			IsSorted:            segInfo.GetIsSorted(),
		})
	}
	log.Info("Compaction handler build clustering compaction plan")
	return plan, nil
}

func (t *clusteringCompactionTask) processPipelining() error {
	log := log.With(zap.Int64("triggerID", t.TriggerID), zap.Int64("collectionID", t.GetCollectionID()), zap.Int64("planID", t.GetPlanID()))
	if t.NeedReAssignNodeID() {
		log.Debug("wait for the node to be assigned before proceeding with the subsequent steps")
		return nil
	}
	var operators []UpdateOperator
	for _, segID := range t.InputSegments {
		operators = append(operators, UpdateSegmentLevelOperator(segID, datapb.SegmentLevel_L2))
	}
	err := t.meta.UpdateSegmentsInfo(operators...)
	if err != nil {
		log.Warn("fail to set segment level to L2", zap.Error(err))
		return merr.WrapErrClusteringCompactionMetaError("UpdateSegmentsInfo before compaction executing", err)
	}

	if typeutil.IsVectorType(t.GetClusteringKeyField().DataType) {
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
	log := log.With(zap.Int64("planID", t.GetPlanID()), zap.String("type", t.GetType().String()))
	result, err := t.sessions.GetCompactionPlanResult(t.GetNodeID(), t.GetPlanID())
	if err != nil || result == nil {
		if errors.Is(err, merr.ErrNodeNotFound) {
			log.Warn("GetCompactionPlanResult fail", zap.Error(err))
			// setNodeID(NullNodeID) to trigger reassign node ID
			return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
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
			return merr.WrapErrCompactionResult("compaction result is empty")
		}

		resultSegmentIDs := lo.Map(result.Segments, func(segment *datapb.CompactionSegment, _ int) int64 {
			return segment.GetSegmentID()
		})

		_, metricMutation, err := t.meta.CompleteCompactionMutation(t.CompactionTask, t.result)
		if err != nil {
			return err
		}
		metricMutation.commit()
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_meta_saved), setTmpSegments(resultSegmentIDs))
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
	default:
		log.Error("not support compaction task state", zap.String("state", result.GetState().String()))
		return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
	}
}

func (t *clusteringCompactionTask) processMetaSaved() error {
	return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_statistic))
}

func (t *clusteringCompactionTask) processStats() error {
	// just the memory step, if it crashes at this step, the state after recovery is CompactionTaskState_statistic.
	resultSegments := make([]int64, 0, len(t.GetTmpSegments()))
	for _, segmentID := range t.GetTmpSegments() {
		to, ok := t.meta.(*meta).GetCompactionTo(segmentID)
		if !ok {
			return nil
		}
		resultSegments = append(resultSegments, to.GetID())
	}

	log.Info("clustering compaction stats task finished",
		zap.Int64s("tmp segments", t.GetTmpSegments()),
		zap.Int64s("result segments", resultSegments))

	return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_indexing), setResultSegments(resultSegments))
}

func (t *clusteringCompactionTask) processIndexing() error {
	// wait for segment indexed
	collectionIndexes := t.meta.GetIndexMeta().GetIndexesForCollection(t.GetCollectionID(), "")
	if len(collectionIndexes) == 0 {
		log.Debug("the collection has no index, no need to do indexing")
		return t.completeTask()
	}
	indexed := func() bool {
		for _, collectionIndex := range collectionIndexes {
			for _, segmentID := range t.GetResultSegments() {
				segmentIndexState := t.meta.GetIndexMeta().GetSegmentIndexState(t.GetCollectionID(), segmentID, collectionIndex.IndexID)
				log.Debug("segment index state", zap.String("segment", segmentIndexState.String()))
				if segmentIndexState.GetState() != commonpb.IndexState_Finished {
					return false
				}
			}
		}
		return true
	}()
	log.Debug("check compaction result segments index states", zap.Bool("indexed", indexed), zap.Int64("planID", t.GetPlanID()), zap.Int64s("segments", t.ResultSegments))
	if indexed {
		return t.completeTask()
	}
	return nil
}

// indexed is the final state of a clustering compaction task
// one task should only run this once
func (t *clusteringCompactionTask) completeTask() error {
	err := t.meta.GetPartitionStatsMeta().SavePartitionStatsInfo(&datapb.PartitionStatsInfo{
		CollectionID: t.GetCollectionID(),
		PartitionID:  t.GetPartitionID(),
		VChannel:     t.GetChannel(),
		Version:      t.GetPlanID(),
		SegmentIDs:   t.GetResultSegments(),
		CommitTime:   time.Now().Unix(),
	})
	if err != nil {
		return merr.WrapErrClusteringCompactionMetaError("SavePartitionStatsInfo", err)
	}

	var operators []UpdateOperator
	for _, segID := range t.GetResultSegments() {
		operators = append(operators, UpdateSegmentPartitionStatsVersionOperator(segID, t.GetPlanID()))
	}
	err = t.meta.UpdateSegmentsInfo(operators...)
	if err != nil {
		return merr.WrapErrClusteringCompactionMetaError("UpdateSegmentPartitionStatsVersion", err)
	}

	err = t.meta.GetPartitionStatsMeta().SaveCurrentPartitionStatsVersion(t.GetCollectionID(), t.GetPartitionID(), t.GetChannel(), t.GetPlanID())
	if err != nil {
		return merr.WrapErrClusteringCompactionMetaError("SaveCurrentPartitionStatsVersion", err)
	}
	return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_completed))
}

func (t *clusteringCompactionTask) processAnalyzing() error {
	analyzeTask := t.meta.GetAnalyzeMeta().GetTask(t.GetAnalyzeTaskID())
	if analyzeTask == nil {
		log.Warn("analyzeTask not found", zap.Int64("id", t.GetAnalyzeTaskID()))
		return merr.WrapErrAnalyzeTaskNotFound(t.GetAnalyzeTaskID()) // retryable
	}
	log.Info("check analyze task state", zap.Int64("id", t.GetAnalyzeTaskID()), zap.Int64("version", analyzeTask.GetVersion()), zap.String("state", analyzeTask.State.String()))
	switch analyzeTask.State {
	case indexpb.JobState_JobStateFinished:
		if analyzeTask.GetCentroidsFile() == "" {
			// not retryable, fake finished vector clustering is not supported in opensource
			return merr.WrapErrClusteringCompactionNotSupportVector()
		} else {
			t.AnalyzeVersion = analyzeTask.GetVersion()
			return t.doCompact()
		}
	case indexpb.JobState_JobStateFailed:
		log.Warn("analyze task fail", zap.Int64("analyzeID", t.GetAnalyzeTaskID()))
		return errors.New(analyzeTask.FailReason)
	default:
	}
	return nil
}

func (t *clusteringCompactionTask) resetSegmentCompacting() {
	t.meta.SetSegmentsCompacting(t.GetInputSegments(), false)
}

func (t *clusteringCompactionTask) processFailedOrTimeout() error {
	log.Info("clean task", zap.Int64("triggerID", t.GetTriggerID()), zap.Int64("planID", t.GetPlanID()), zap.String("state", t.GetState().String()))
	// revert segments meta
	var operators []UpdateOperator
	// revert level of input segments
	// L1 : L1 ->(processPipelining)-> L2 ->(processFailedOrTimeout)-> L1
	// L2 : L2 ->(processPipelining)-> L2 ->(processFailedOrTimeout)-> L2
	for _, segID := range t.InputSegments {
		operators = append(operators, RevertSegmentLevelOperator(segID))
	}
	// if result segments are generated but task fail in the other steps, mark them as L1 segments without partitions stats
	for _, segID := range t.ResultSegments {
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
		CollectionID: t.GetCollectionID(),
		PartitionID:  t.GetPartitionID(),
		VChannel:     t.GetChannel(),
		Version:      t.GetPlanID(),
		SegmentIDs:   t.GetResultSegments(),
	}
	err = t.meta.CleanPartitionStatsInfo(partitionStatsInfo)
	if err != nil {
		log.Warn("gcPartitionStatsInfo fail", zap.Error(err))
	}

	return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_cleaned))
}

func (t *clusteringCompactionTask) doAnalyze() error {
	analyzeTask := &indexpb.AnalyzeTask{
		CollectionID: t.GetCollectionID(),
		PartitionID:  t.GetPartitionID(),
		FieldID:      t.GetClusteringKeyField().FieldID,
		FieldName:    t.GetClusteringKeyField().Name,
		FieldType:    t.GetClusteringKeyField().DataType,
		SegmentIDs:   t.GetInputSegments(),
		TaskID:       t.GetAnalyzeTaskID(),
		State:        indexpb.JobState_JobStateInit,
	}
	err := t.meta.GetAnalyzeMeta().AddAnalyzeTask(analyzeTask)
	if err != nil {
		log.Warn("failed to create analyze task", zap.Int64("planID", t.GetPlanID()), zap.Error(err))
		return err
	}

	t.analyzeScheduler.enqueue(newAnalyzeTask(t.GetAnalyzeTaskID()))

	log.Info("submit analyze task", zap.Int64("planID", t.GetPlanID()), zap.Int64("triggerID", t.GetTriggerID()), zap.Int64("collectionID", t.GetCollectionID()), zap.Int64("id", t.GetAnalyzeTaskID()))
	return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_analyzing))
}

func (t *clusteringCompactionTask) doCompact() error {
	log := log.With(zap.Int64("planID", t.GetPlanID()), zap.String("type", t.GetType().String()))
	if t.NeedReAssignNodeID() {
		log.RatedWarn(10, "not assign nodeID")
		return nil
	}
	log = log.With(zap.Int64("nodeID", t.GetNodeID()))

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
		return err
	}
	err = t.sessions.Compaction(context.Background(), t.GetNodeID(), t.GetPlan())
	if err != nil {
		if errors.Is(err, merr.ErrDataNodeSlotExhausted) {
			log.Warn("fail to notify compaction tasks to DataNode because the node slots exhausted")
			return t.updateAndSaveTaskMeta(setNodeID(NullNodeID))
		}
		log.Warn("Failed to notify compaction tasks to DataNode", zap.Error(err))
		return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
	}
	return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_executing))
}

func (t *clusteringCompactionTask) ShadowClone(opts ...compactionTaskOpt) *datapb.CompactionTask {
	taskClone := &datapb.CompactionTask{
		PlanID:             t.GetPlanID(),
		TriggerID:          t.GetTriggerID(),
		State:              t.GetState(),
		StartTime:          t.GetStartTime(),
		EndTime:            t.GetEndTime(),
		TimeoutInSeconds:   t.GetTimeoutInSeconds(),
		Type:               t.GetType(),
		CollectionTtl:      t.CollectionTtl,
		CollectionID:       t.GetCollectionID(),
		PartitionID:        t.GetPartitionID(),
		Channel:            t.GetChannel(),
		InputSegments:      t.GetInputSegments(),
		ResultSegments:     t.GetResultSegments(),
		TotalRows:          t.TotalRows,
		Schema:             t.Schema,
		NodeID:             t.GetNodeID(),
		FailReason:         t.GetFailReason(),
		RetryTimes:         t.GetRetryTimes(),
		Pos:                t.GetPos(),
		ClusteringKeyField: t.GetClusteringKeyField(),
		MaxSegmentRows:     t.GetMaxSegmentRows(),
		PreferSegmentRows:  t.GetPreferSegmentRows(),
		AnalyzeTaskID:      t.GetAnalyzeTaskID(),
		AnalyzeVersion:     t.GetAnalyzeVersion(),
		LastStateStartTime: t.GetLastStateStartTime(),
	}
	for _, opt := range opts {
		opt(taskClone)
	}
	return taskClone
}

func (t *clusteringCompactionTask) updateAndSaveTaskMeta(opts ...compactionTaskOpt) error {
	task := t.ShadowClone(opts...)
	err := t.saveTaskMeta(task)
	if err != nil {
		log.Warn("Failed to saveTaskMeta", zap.Error(err))
		return merr.WrapErrClusteringCompactionMetaError("updateAndSaveTaskMeta", err) // retryable
	}
	t.CompactionTask = task
	return nil
}

func (t *clusteringCompactionTask) checkTimeout() bool {
	if t.GetTimeoutInSeconds() > 0 {
		diff := time.Since(time.Unix(t.GetStartTime(), 0)).Seconds()
		if diff > float64(t.GetTimeoutInSeconds()) {
			log.Warn("compaction timeout",
				zap.Int32("timeout in seconds", t.GetTimeoutInSeconds()),
				zap.Int64("startTime", t.GetStartTime()),
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
	return t.saveTaskMeta(t.CompactionTask)
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

func (t *clusteringCompactionTask) SetTask(ct *datapb.CompactionTask) {
	t.CompactionTask = ct
}

func (t *clusteringCompactionTask) SetNodeID(id UniqueID) error {
	return t.updateAndSaveTaskMeta(setNodeID(id))
}

func (t *clusteringCompactionTask) GetLabel() string {
	return fmt.Sprintf("%d-%s", t.PartitionID, t.GetChannel())
}

func (t *clusteringCompactionTask) NeedReAssignNodeID() bool {
	return t.GetState() == datapb.CompactionTaskState_pipelining && (t.GetNodeID() == 0 || t.GetNodeID() == NullNodeID)
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
