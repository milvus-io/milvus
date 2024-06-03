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
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ CompactionTask = (*clusteringCompactionTask)(nil)

type clusteringCompactionTask struct {
	*datapb.CompactionTask
	dataNodeID          int64
	plan                *datapb.CompactionPlan
	result              *datapb.CompactionPlanResult
	span                trace.Span
	lastUpdateStateTime int64
}

func (task *clusteringCompactionTask) processInitTask(handler *compactionPlanHandler) error {
	log := log.With(zap.Int64("triggerID", task.TriggerID), zap.Int64("collectionID", task.GetCollectionID()), zap.Int64("planID", task.GetPlanID()))
	var operators []UpdateOperator
	for _, segID := range task.InputSegments {
		operators = append(operators, UpdateSegmentLevelOperator(segID, datapb.SegmentLevel_L2))
	}
	err := handler.meta.UpdateSegmentsInfo(operators...)
	if err != nil {
		log.Warn("fail to set segment level to L2", zap.Error(err))
		return err
	}

	if typeutil.IsVectorType(task.GetClusteringKeyField().DataType) {
		err := task.submitToAnalyze(handler)
		if err != nil {
			log.Warn("fail to submit analyze task", zap.Error(err))
			return merr.WrapErrClusteringCompactionSubmitTaskFail("analyze", err)
		}
	} else {
		err := task.submitToCompact(handler)
		if err != nil {
			log.Warn("fail to submit compaction task", zap.Error(err))
			return merr.WrapErrClusteringCompactionSubmitTaskFail("compact", err)
		}
	}
	return nil
}

func (task *clusteringCompactionTask) processExecutingTask(handler *compactionPlanHandler) error {
	if !handler.scheduler.GetTaskExecuting(task.PlanID) {
		return nil
	}
	nodePlan, exist := handler.compactionResults[task.GetPlanID()]
	if !exist {
		return merr.WrapErrCompactionResultNotFound()
	}
	planResult := nodePlan.B
	switch planResult.GetState() {
	case commonpb.CompactionState_Completed:
		resultSegmentIDs := lo.Map(planResult.Segments, func(segment *datapb.CompactionSegment, _ int) int64 {
			return segment.GetSegmentID()
		})
		task.CompactionTask.ResultSegments = resultSegmentIDs
		err := handler.handleMergeCompactionResult(task.GetPlan(), planResult)
		if err != nil {
			return err
		}
		UpdateCompactionSegmentSizeMetrics(planResult.GetSegments())
		ts := time.Now().UnixMilli()
		compactionStageTime := ts - task.lastUpdateStateTime
		log.Debug("clustering compaction compact task elapse", zap.Int64("triggerID", task.GetTriggerID()), zap.Int64("collectionID", task.GetCollectionID()), zap.Int64("planID", task.GetPlanID()), zap.Int64("elapse", compactionStageTime))
		metrics.DataCoordCompactionLatency.
			WithLabelValues(fmt.Sprint(typeutil.IsVectorType(task.GetClusteringKeyField().DataType)), datapb.CompactionType_ClusteringCompaction.String(), "compacting").
			Observe(float64(compactionStageTime))
		task.result = planResult
		task.lastUpdateStateTime = ts
		handler.plans[task.GetPlanID()] = task.ShadowClone(setState(datapb.CompactionTaskState_indexing), endSpan())
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

func (task *clusteringCompactionTask) processIndexingTask(handler *compactionPlanHandler) error {
	// wait for segment indexed
	collectionIndexes := handler.meta.(*meta).indexMeta.GetIndexesForCollection(task.GetCollectionID(), "")
	indexed := func() bool {
		for _, collectionIndex := range collectionIndexes {
			for _, segmentID := range task.ResultSegments {
				segmentIndexState := handler.meta.(*meta).indexMeta.GetSegmentIndexState(task.GetCollectionID(), segmentID, collectionIndex.IndexID)
				if segmentIndexState.GetState() != commonpb.IndexState_Finished {
					return false
				}
			}
		}
		return true
	}()
	log.Debug("check compaction result segments index states", zap.Bool("indexed", indexed), zap.Int64("planID", task.GetPlanID()), zap.Int64s("segments", task.ResultSegments))
	if indexed {
		task.processIndexedTask(handler)
	}
	return nil
}

// indexed is the final state of a clustering compaction task
// one task should only run this once
func (task *clusteringCompactionTask) processIndexedTask(handler *compactionPlanHandler) error {
	err := handler.meta.(*meta).partitionStatsMeta.SavePartitionStatsInfo(&datapb.PartitionStatsInfo{
		CollectionID: task.GetCollectionID(),
		PartitionID:  task.GetPartitionID(),
		VChannel:     task.GetChannel(),
		Version:      task.GetPlanID(),
		SegmentIDs:   task.GetResultSegments(),
	})
	if err != nil {
		return merr.WrapErrClusteringCompactionMetaError("SavePartitionStatsInfo", err)
	}
	var operators []UpdateOperator
	for _, segID := range task.GetResultSegments() {
		operators = append(operators, UpdateSegmentPartitionStatsVersionOperator(segID, task.GetPlanID()))
	}
	err = handler.meta.UpdateSegmentsInfo(operators...)
	if err != nil {
		return merr.WrapErrClusteringCompactionMetaError("UpdateSegmentPartitionStatsVersion", err)
	}

	ts := time.Now().UnixMilli()
	indexStageTime := ts - task.lastUpdateStateTime
	log.Debug("clustering compaction index task elapse", zap.Int64("triggerID", task.GetTriggerID()), zap.Int64("collectionID", task.GetCollectionID()), zap.Int64("planID", task.GetPlanID()), zap.Int64("elapse", indexStageTime))
	metrics.DataCoordCompactionLatency.
		WithLabelValues(fmt.Sprint(typeutil.IsVectorType(task.GetClusteringKeyField().DataType)), datapb.CompactionType_ClusteringCompaction.String(), "indexing").
		Observe(float64(indexStageTime))
	task.lastUpdateStateTime = ts
	task.State = datapb.CompactionTaskState_completed
	globalTs, err := handler.allocator.allocTimestamp(context.Background())
	if err != nil {
		return err
	}
	task.EndTime = globalTs
	elapse := tsoutil.PhysicalTime(globalTs).UnixMilli() - tsoutil.PhysicalTime(task.StartTime).UnixMilli()
	log.Info("clustering compaction task elapse", zap.Int64("triggerID", task.GetTriggerID()), zap.Int64("collectionID", task.GetCollectionID()), zap.Int64("planID", task.GetPlanID()), zap.Int64("elapse", elapse))
	metrics.DataCoordCompactionLatency.
		WithLabelValues(fmt.Sprint(typeutil.IsVectorType(task.GetClusteringKeyField().DataType)), datapb.CompactionType_ClusteringCompaction.String(), "total").
		Observe(float64(elapse))
	return nil
}

func (task *clusteringCompactionTask) processAnalyzingTask(handler *compactionPlanHandler) error {
	analyzeTask := handler.meta.(*meta).analyzeMeta.GetTask(task.GetAnalyzeTaskID())
	if analyzeTask == nil {
		log.Warn("analyzeTask not found", zap.Int64("id", task.GetAnalyzeTaskID()))
		return errors.New("analyzeTask not found")
	}
	log.Info("check analyze task state", zap.Int64("id", task.GetAnalyzeTaskID()), zap.Int64("version", analyzeTask.GetVersion()), zap.String("state", analyzeTask.State.String()))
	switch analyzeTask.State {
	case indexpb.JobState_JobStateFinished:
		if analyzeTask.GetCentroidsFile() == "" {
			// fake finished vector clustering is not supported in opensource
			return merr.WrapErrClusteringCompactionNotSupportVector()
		} else {
			task.AnalyzeVersion = analyzeTask.GetVersion()
			task.submitToCompact(handler)
			ts := time.Now().UnixMilli()
			analyzeStageTime := ts - task.lastUpdateStateTime
			log.Info("clustering compaction analyze task elapse", zap.Int64("triggerID", task.GetTriggerID()), zap.Int64("collectionID", task.GetCollectionID()), zap.Int64("planID", task.GetPlanID()), zap.Int64("elapse", analyzeStageTime))
			metrics.DataCoordCompactionLatency.
				WithLabelValues(fmt.Sprint(typeutil.IsVectorType(task.GetClusteringKeyField().DataType)), datapb.CompactionType_ClusteringCompaction.String(), "analyzing").
				Observe(float64(analyzeStageTime))
			task.State = datapb.CompactionTaskState_executing
			task.lastUpdateStateTime = ts
		}
	case indexpb.JobState_JobStateFailed:
		log.Warn("analyze task fail", zap.Int64("analyzeID", task.GetAnalyzeTaskID()))
		// todo rethinking all the error flow
		return errors.New(analyzeTask.FailReason)
	default:
	}
	return nil
}

func (task *clusteringCompactionTask) processFailedOrTimeoutTask(handler *compactionPlanHandler) error {
	log.Info("clean fail or timeout task", zap.Int64("triggerID", task.GetTriggerID()), zap.Int64("planID", task.GetPlanID()))
	// revert segment level
	var operators []UpdateOperator
	for _, segID := range task.InputSegments {
		operators = append(operators, RevertSegmentLevelOperator(segID))
		operators = append(operators, RevertSegmentPartitionStatsVersionOperator(segID))
	}
	err := handler.meta.UpdateSegmentsInfo(operators...)
	if err != nil {
		log.Warn("UpdateSegmentsInfo fail", zap.Error(err))
	}

	// drop partition stats if uploaded
	partitionStatsInfo := &datapb.PartitionStatsInfo{
		CollectionID: task.GetCollectionID(),
		PartitionID:  task.GetPartitionID(),
		VChannel:     task.GetChannel(),
		Version:      task.GetPlanID(),
		SegmentIDs:   task.GetResultSegments(),
	}
	err = handler.gcPartitionStatsInfo(partitionStatsInfo)
	if err != nil {
		log.Warn("gcPartitionStatsInfo fail", zap.Error(err))
	}

	task.State = datapb.CompactionTaskState_cleaned
	return nil
}

func (task *clusteringCompactionTask) submitToAnalyze(handler *compactionPlanHandler) error {
	newAnalyzeTask := &indexpb.AnalyzeTask{
		CollectionID: task.GetCollectionID(),
		PartitionID:  task.GetPartitionID(),
		FieldID:      task.GetClusteringKeyField().FieldID,
		FieldName:    task.GetClusteringKeyField().Name,
		FieldType:    task.GetClusteringKeyField().DataType,
		SegmentIDs:   task.GetInputSegments(),
		TaskID:       task.GetAnalyzeTaskID(),
		State:        indexpb.JobState_JobStateInit,
	}
	err := handler.meta.(*meta).analyzeMeta.AddAnalyzeTask(newAnalyzeTask)
	if err != nil {
		log.Warn("failed to create analyze task", zap.Int64("planID", task.GetPlanID()), zap.Error(err))
		return err
	}
	handler.analyzeScheduler.enqueue(&analyzeTask{
		taskID: task.GetAnalyzeTaskID(),
		taskInfo: &indexpb.AnalyzeResult{
			TaskID: task.GetAnalyzeTaskID(),
			State:  indexpb.JobState_JobStateInit,
		},
	})
	task.State = datapb.CompactionTaskState_analyzing
	log.Info("submit analyze task", zap.Int64("planID", task.GetPlanID()), zap.Int64("triggerID", task.GetTriggerID()), zap.Int64("collectionID", task.GetCollectionID()), zap.Int64("id", task.GetAnalyzeTaskID()))
	return nil
}

func (task *clusteringCompactionTask) submitToCompact(handler *compactionPlanHandler) error {
	handler.scheduler.Submit(task)
	log.Info("send compaction task to execute", zap.Int64("triggerID", task.GetTriggerID()),
		zap.Int64("planID", task.GetPlanID()),
		zap.Int64("collectionID", task.GetCollectionID()),
		zap.Int64("partitionID", task.GetPartitionID()),
		zap.Int64s("inputSegments", task.InputSegments))
	return nil
}

func (task *clusteringCompactionTask) ProcessTask(handler *compactionPlanHandler) error {
	log := log.With(zap.Int64("PlanID", task.GetPlanID()))
	stateBefore := task.GetState().String()
	err := task.innerProcessTask(handler)
	if err != nil {
		log.Warn("fail in process task", zap.Error(err))
		if merr.IsRetryableErr(err) && task.RetryTimes < taskMaxRetryTimes {
			// retry in next loop
			task.RetryTimes = task.RetryTimes + 1
		} else {
			log.Error("task fail with unretryable reason or meet max retry times", zap.Error(err))
			task.State = datapb.CompactionTaskState_failed
			task.FailReason = err.Error()
		}
	}
	// task state update, refresh retry times count
	if task.State.String() != stateBefore {
		task.RetryTimes = 0
	}
	log.Debug("process task", zap.String("stateBefore", stateBefore), zap.String("stateAfter", task.State.String()))
	handler.meta.(*meta).compactionTaskMeta.SaveCompactionTask(task.CompactionTask)
	return err
}

func (task *clusteringCompactionTask) innerProcessTask(compactionHandler *compactionPlanHandler) error {
	if task.State == datapb.CompactionTaskState_completed || task.State == datapb.CompactionTaskState_cleaned {
		return nil
	}

	coll, err := compactionHandler.handler.GetCollection(context.Background(), task.GetCollectionID())
	if err != nil {
		log.Warn("fail to get collection", zap.Int64("collectionID", task.GetCollectionID()), zap.Error(err))
		return merr.WrapErrClusteringCompactionGetCollectionFail(task.GetCollectionID(), err)
	}
	if coll == nil {
		log.Warn("collection not found, it may be dropped, stop clustering compaction task", zap.Int64("collectionID", task.GetCollectionID()))
		return merr.WrapErrCollectionNotFound(task.GetCollectionID())
	}

	switch task.State {
	case datapb.CompactionTaskState_pipelining:
		return task.processInitTask(compactionHandler)
	case datapb.CompactionTaskState_executing:
		return task.processExecutingTask(compactionHandler)
	case datapb.CompactionTaskState_analyzing:
		return task.processAnalyzingTask(compactionHandler)
	case datapb.CompactionTaskState_indexing:
		return task.processIndexingTask(compactionHandler)
	case datapb.CompactionTaskState_timeout:
		return task.processFailedOrTimeoutTask(compactionHandler)
	case datapb.CompactionTaskState_failed:
		return task.processFailedOrTimeoutTask(compactionHandler)
	}
	return nil
}

func (task *clusteringCompactionTask) BuildCompactionRequest(handler *compactionPlanHandler) (*datapb.CompactionPlan, error) {
	plan := &datapb.CompactionPlan{
		PlanID:             task.GetPlanID(),
		StartTime:          task.GetStartTime(),
		TimeoutInSeconds:   task.GetTimeoutInSeconds(),
		Type:               task.GetType(),
		Channel:            task.GetChannel(),
		CollectionTtl:      task.GetCollectionTtl(),
		TotalRows:          task.GetTotalRows(),
		Schema:             task.GetSchema(),
		ClusteringKeyField: task.GetClusteringKeyField().GetFieldID(),
		MaxSegmentRows:     task.GetMaxSegmentRows(),
		PreferSegmentRows:  task.GetPreferSegmentRows(),
		AnalyzeResultPath:  path.Join(common.AnalyzeStatsPath, metautil.JoinIDPath(task.AnalyzeTaskID, task.AnalyzeVersion)),
		AnalyzeSegmentIds:  task.GetInputSegments(), // todo: if need
	}
	log := log.With(zap.Int64("taskID", task.GetTriggerID()), zap.Int64("planID", plan.GetPlanID()))

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
	}
	log.Info("Compaction handler build clustering compaction plan")
	handler.plans[task.GetPlanID()] = task.ShadowClone(setPlan(plan))
	return plan, nil
}

func (task *clusteringCompactionTask) GetPlan() *datapb.CompactionPlan {
	return task.plan
}

func (task *clusteringCompactionTask) GetNodeID() int64 {
	return task.dataNodeID
}

func (task *clusteringCompactionTask) GetResult() *datapb.CompactionPlanResult {
	return task.result
}

func (task *clusteringCompactionTask) GetSpan() trace.Span {
	return task.span
}

func (task *clusteringCompactionTask) ShadowClone(opts ...compactionTaskOpt) CompactionTask {
	ctask := &clusteringCompactionTask{
		CompactionTask:      task.CompactionTask,
		plan:                task.plan,
		dataNodeID:          task.dataNodeID,
		span:                task.span,
		result:              task.result,
		lastUpdateStateTime: task.lastUpdateStateTime,
	}
	for _, opt := range opts {
		opt(ctask)
	}
	return ctask
}

func (task *clusteringCompactionTask) EndSpan() {
	if task.span != nil {
		task.span.End()
	}
}

func (task *clusteringCompactionTask) SetState(state datapb.CompactionTaskState) {
	task.State = state
}

func (task *clusteringCompactionTask) SetStartTime(startTime uint64) {
	task.StartTime = startTime
}

func (task *clusteringCompactionTask) SetResult(result *datapb.CompactionPlanResult) {
	task.result = result
}

func (task *clusteringCompactionTask) SetSpan(span trace.Span) {
	task.span = span
}

func (task *clusteringCompactionTask) SetNodeID(nodeID int64) {
	task.dataNodeID = nodeID
}

func (task *clusteringCompactionTask) SetPlan(plan *datapb.CompactionPlan) {
	task.plan = plan
}

func (task *clusteringCompactionTask) SetTask(ct *datapb.CompactionTask) {
	task.CompactionTask = ct
}

func (task *clusteringCompactionTask) SetLastUpdateStateTime(ts int64) {
	task.lastUpdateStateTime = ts
}

func (task *clusteringCompactionTask) CleanLogPath() {
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
