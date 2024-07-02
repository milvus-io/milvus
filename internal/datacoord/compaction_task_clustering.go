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

const (
	taskMaxRetryTimes = int32(3)
)

type clusteringCompactionTask struct {
	*datapb.CompactionTask
	plan                *datapb.CompactionPlan
	result              *datapb.CompactionPlanResult
	span                trace.Span
	lastUpdateStateTime int64

	meta             CompactionMeta
	sessions         SessionManager
	handler          Handler
	analyzeScheduler *taskScheduler
}

func (t *clusteringCompactionTask) Process() bool {
	log := log.With(zap.Int64("triggerID", t.GetTriggerID()), zap.Int64("PlanID", t.GetPlanID()), zap.Int64("collectionID", t.GetCollectionID()))
	lastState := t.GetState().String()
	err := t.retryableProcess()
	if err != nil {
		log.Warn("fail in process task", zap.Error(err))
		if merr.IsRetryableErr(err) && t.RetryTimes < taskMaxRetryTimes {
			// retry in next Process
			t.RetryTimes = t.RetryTimes + 1
		} else {
			log.Error("task fail with unretryable reason or meet max retry times", zap.Error(err))
			t.State = datapb.CompactionTaskState_failed
			t.FailReason = err.Error()
		}
	}
	// task state update, refresh retry times count
	currentState := t.State.String()
	if currentState != lastState {
		t.RetryTimes = 0
		ts := time.Now().UnixMilli()
		lastStateDuration := ts - t.lastUpdateStateTime
		log.Info("clustering compaction task state changed", zap.String("lastState", lastState), zap.String("currentState", currentState), zap.Int64("elapse", lastStateDuration))
		metrics.DataCoordCompactionLatency.
			WithLabelValues(fmt.Sprint(typeutil.IsVectorType(t.GetClusteringKeyField().DataType)), datapb.CompactionType_ClusteringCompaction.String(), lastState).
			Observe(float64(lastStateDuration))
		t.lastUpdateStateTime = ts

		if t.State == datapb.CompactionTaskState_completed {
			t.updateAndSaveTaskMeta(setEndTime(ts))
			elapse := ts - tsoutil.PhysicalTime(uint64(t.StartTime)).UnixMilli()
			log.Info("clustering compaction task total elapse", zap.Int64("elapse", elapse))
			metrics.DataCoordCompactionLatency.
				WithLabelValues(fmt.Sprint(typeutil.IsVectorType(t.GetClusteringKeyField().DataType)), datapb.CompactionType_ClusteringCompaction.String(), "total").
				Observe(float64(elapse))
		}
	}
	// todo debug
	log.Info("process clustering task", zap.String("lastState", lastState), zap.String("currentState", currentState))
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
	case datapb.CompactionTaskState_timeout:
		return t.processFailedOrTimeout()
	case datapb.CompactionTaskState_failed:
		return t.processFailedOrTimeout()
	}
	return nil
}

func (t *clusteringCompactionTask) BuildCompactionRequest() (*datapb.CompactionPlan, error) {
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
		AnalyzeSegmentIds:  t.GetInputSegments(), // todo: if need
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
		})
	}
	log.Info("Compaction handler build clustering compaction plan")
	return plan, nil
}

func (t *clusteringCompactionTask) processPipelining() error {
	log := log.With(zap.Int64("triggerID", t.TriggerID), zap.Int64("collectionID", t.GetCollectionID()), zap.Int64("planID", t.GetPlanID()))
	var operators []UpdateOperator
	for _, segID := range t.InputSegments {
		operators = append(operators, UpdateSegmentLevelOperator(segID, datapb.SegmentLevel_L2))
	}
	err := t.meta.UpdateSegmentsInfo(operators...)
	if err != nil {
		log.Warn("fail to set segment level to L2", zap.Error(err))
		return err
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
			// todo reassign node ID
			t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(0))
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
			log.Info("illegal compaction results")
			err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
			return err
		}

		resultSegmentIDs := lo.Map(result.Segments, func(segment *datapb.CompactionSegment, _ int) int64 {
			return segment.GetSegmentID()
		})

		_, metricMutation, err := t.meta.CompleteCompactionMutation(t.GetPlan(), t.result)
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
	collectionIndexes := t.meta.GetIndexMeta().GetIndexesForCollection(t.GetCollectionID(), "")
	indexed := func() bool {
		for _, collectionIndex := range collectionIndexes {
			for _, segmentID := range t.ResultSegments {
				segmentIndexState := t.meta.GetIndexMeta().GetSegmentIndexState(t.GetCollectionID(), segmentID, collectionIndex.IndexID)
				if segmentIndexState.GetState() != commonpb.IndexState_Finished {
					return false
				}
			}
		}
		return true
	}()
	log.Debug("check compaction result segments index states", zap.Bool("indexed", indexed), zap.Int64("planID", t.GetPlanID()), zap.Int64s("segments", t.ResultSegments))
	if indexed {
		t.completeTask()
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
		return err
	}
	return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_completed))
}

func (t *clusteringCompactionTask) processAnalyzing() error {
	analyzeTask := t.meta.GetAnalyzeMeta().GetTask(t.GetAnalyzeTaskID())
	if analyzeTask == nil {
		log.Warn("analyzeTask not found", zap.Int64("id", t.GetAnalyzeTaskID()))
		return errors.New("analyzeTask not found")
	}
	log.Info("check analyze task state", zap.Int64("id", t.GetAnalyzeTaskID()), zap.Int64("version", analyzeTask.GetVersion()), zap.String("state", analyzeTask.State.String()))
	switch analyzeTask.State {
	case indexpb.JobState_JobStateFinished:
		if analyzeTask.GetCentroidsFile() == "" {
			// fake finished vector clustering is not supported in opensource
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
	for _, segmentBinlogs := range t.GetPlan().GetSegmentBinlogs() {
		t.meta.SetSegmentCompacting(segmentBinlogs.GetSegmentID(), false)
	}
}

func (t *clusteringCompactionTask) processFailedOrTimeout() error {
	log.Info("clean task", zap.Int64("triggerID", t.GetTriggerID()), zap.Int64("planID", t.GetPlanID()), zap.String("state", t.GetState().String()))
	// revert segment level
	var operators []UpdateOperator
	for _, segID := range t.InputSegments {
		operators = append(operators, RevertSegmentLevelOperator(segID))
		operators = append(operators, RevertSegmentPartitionStatsVersionOperator(segID))
	}
	err := t.meta.UpdateSegmentsInfo(operators...)
	if err != nil {
		log.Warn("UpdateSegmentsInfo fail", zap.Error(err))
		return err
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

	t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_cleaned))
	return nil
}

func (t *clusteringCompactionTask) doAnalyze() error {
	newAnalyzeTask := &indexpb.AnalyzeTask{
		CollectionID: t.GetCollectionID(),
		PartitionID:  t.GetPartitionID(),
		FieldID:      t.GetClusteringKeyField().FieldID,
		FieldName:    t.GetClusteringKeyField().Name,
		FieldType:    t.GetClusteringKeyField().DataType,
		SegmentIDs:   t.GetInputSegments(),
		TaskID:       t.GetAnalyzeTaskID(),
		State:        indexpb.JobState_JobStateInit,
	}
	err := t.meta.GetAnalyzeMeta().AddAnalyzeTask(newAnalyzeTask)
	if err != nil {
		log.Warn("failed to create analyze task", zap.Int64("planID", t.GetPlanID()), zap.Error(err))
		return err
	}
	t.analyzeScheduler.enqueue(&analyzeTask{
		taskID: t.GetAnalyzeTaskID(),
		taskInfo: &indexpb.AnalyzeResult{
			TaskID: t.GetAnalyzeTaskID(),
			State:  indexpb.JobState_JobStateInit,
		},
	})
	t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_analyzing))
	log.Info("submit analyze task", zap.Int64("planID", t.GetPlanID()), zap.Int64("triggerID", t.GetTriggerID()), zap.Int64("collectionID", t.GetCollectionID()), zap.Int64("id", t.GetAnalyzeTaskID()))
	return nil
}

func (t *clusteringCompactionTask) doCompact() error {
	if t.NeedReAssignNodeID() {
		return errors.New("not assign nodeID")
	}
	var err error
	t.plan, err = t.BuildCompactionRequest()
	if err != nil {
		err2 := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
		return err2
	}
	err = t.sessions.Compaction(context.Background(), t.GetNodeID(), t.GetPlan())
	if err != nil {
		log.Warn("Failed to notify compaction tasks to DataNode", zap.Error(err))
		t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(0))
		return err
	}
	t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_executing))
	return nil
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
		return err
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

func (t *clusteringCompactionTask) SetStartTime(startTime int64) {
	t.StartTime = startTime
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
	return t.GetState() == datapb.CompactionTaskState_pipelining && t.GetNodeID() == 0
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
