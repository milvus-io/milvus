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
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ CompactionTask = (*clusteringCompactionTask)(nil)

type clusteringCompactionTask struct {
	taskProto atomic.Value // *datapb.CompactionTask
	plan      *datapb.CompactionPlan
	result    *datapb.CompactionPlanResult

	span             trace.Span
	allocator        allocator.Allocator
	meta             CompactionMeta
	sessions         session.DataNodeManager
	handler          Handler
	analyzeScheduler *taskScheduler

	maxRetryTimes int32
	slotUsage     int64
}

func (t *clusteringCompactionTask) GetTaskProto() *datapb.CompactionTask {
	task := t.taskProto.Load()
	if task == nil {
		return nil
	}
	return task.(*datapb.CompactionTask)
}

func newClusteringCompactionTask(t *datapb.CompactionTask, allocator allocator.Allocator, meta CompactionMeta, session session.DataNodeManager, handler Handler, analyzeScheduler *taskScheduler) *clusteringCompactionTask {
	task := &clusteringCompactionTask{
		allocator:        allocator,
		meta:             meta,
		sessions:         session,
		handler:          handler,
		analyzeScheduler: analyzeScheduler,
		maxRetryTimes:    3,
		slotUsage:        paramtable.Get().DataCoordCfg.ClusteringCompactionSlotUsage.GetAsInt64(),
	}
	task.taskProto.Store(t)
	return task
}

// Note: return True means exit this state machine.
// ONLY return True for Completed, Failed or Timeout
func (t *clusteringCompactionTask) Process() bool {
	ctx := context.TODO()
	log := log.Ctx(ctx).With(zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()), zap.Int64("PlanID", t.GetTaskProto().GetPlanID()), zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
	lastState := t.GetTaskProto().GetState().String()
	err := t.retryableProcess(ctx)
	if err != nil {
		log.Warn("fail in process task", zap.Error(err))
		if merr.IsRetryableErr(err) && t.GetTaskProto().RetryTimes < t.maxRetryTimes {
			// retry in next Process
			err = t.updateAndSaveTaskMeta(setRetryTimes(t.GetTaskProto().RetryTimes + 1))
		} else {
			log.Error("task fail with unretryable reason or meet max retry times", zap.Error(err))
			err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
		}
		if err != nil {
			log.Warn("Failed to updateAndSaveTaskMeta", zap.Error(err))
		}
	}
	// task state update, refresh retry times count
	currentState := t.GetTaskProto().State.String()
	if currentState != lastState {
		ts := time.Now().Unix()
		lastStateDuration := ts - t.GetTaskProto().GetLastStateStartTime()
		log.Info("clustering compaction task state changed", zap.String("lastState", lastState), zap.String("currentState", currentState), zap.Int64("elapse seconds", lastStateDuration))
		metrics.DataCoordCompactionLatency.
			WithLabelValues(fmt.Sprint(typeutil.IsVectorType(t.GetTaskProto().GetClusteringKeyField().DataType)), fmt.Sprint(t.GetTaskProto().CollectionID), t.GetTaskProto().Channel, datapb.CompactionType_ClusteringCompaction.String(), lastState).
			Observe(float64(lastStateDuration * 1000))
		updateOps := []compactionTaskOpt{setRetryTimes(0), setLastStateStartTime(ts)}

		if t.GetTaskProto().State == datapb.CompactionTaskState_completed || t.GetTaskProto().State == datapb.CompactionTaskState_cleaned {
			updateOps = append(updateOps, setEndTime(ts))
			elapse := ts - t.GetTaskProto().StartTime
			log.Info("clustering compaction task total elapse", zap.Duration("costs", time.Duration(elapse)*time.Second))
			metrics.DataCoordCompactionLatency.
				WithLabelValues(fmt.Sprint(typeutil.IsVectorType(t.GetTaskProto().GetClusteringKeyField().DataType)), fmt.Sprint(t.GetTaskProto().CollectionID), t.GetTaskProto().Channel, datapb.CompactionType_ClusteringCompaction.String(), "total").
				Observe(float64(elapse * 1000))
		}
		err = t.updateAndSaveTaskMeta(updateOps...)
		if err != nil {
			log.Warn("Failed to updateAndSaveTaskMeta", zap.Error(err))
		}
		log.Info("clustering compaction task state changed", zap.String("lastState", lastState), zap.String("currentState", currentState), zap.Int64("elapse seconds", lastStateDuration))
	}
	log.Debug("process clustering task", zap.String("lastState", lastState), zap.String("currentState", currentState))
	return t.GetTaskProto().State == datapb.CompactionTaskState_completed ||
		t.GetTaskProto().State == datapb.CompactionTaskState_cleaned ||
		t.GetTaskProto().State == datapb.CompactionTaskState_failed ||
		t.GetTaskProto().State == datapb.CompactionTaskState_timeout
}

// retryableProcess process task's state transfer, return error if not work as expected
// the outer Process will set state and retry times according to the error type(retryable or not-retryable)
func (t *clusteringCompactionTask) retryableProcess(ctx context.Context) error {
	if t.GetTaskProto().State == datapb.CompactionTaskState_completed ||
		t.GetTaskProto().State == datapb.CompactionTaskState_cleaned ||
		t.GetTaskProto().State == datapb.CompactionTaskState_failed ||
		t.GetTaskProto().State == datapb.CompactionTaskState_timeout {
		return nil
	}

	coll, err := t.handler.GetCollection(ctx, t.GetTaskProto().GetCollectionID())
	if err != nil {
		// retryable
		log.Warn("fail to get collection", zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()), zap.Error(err))
		return merr.WrapErrClusteringCompactionGetCollectionFail(t.GetTaskProto().GetCollectionID(), err)
	}
	if coll == nil {
		// not-retryable fail fast if collection is dropped
		log.Warn("collection not found, it may be dropped, stop clustering compaction task", zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
		return merr.WrapErrCollectionNotFound(t.GetTaskProto().GetCollectionID())
	}

	switch t.GetTaskProto().State {
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
	}
	return nil
}

func (t *clusteringCompactionTask) Clean() bool {
	log.Ctx(context.TODO()).Info("clean task", zap.Int64("planID", t.GetTaskProto().GetPlanID()), zap.String("type", t.GetTaskProto().GetType().String()))
	return t.doClean() == nil
}

func (t *clusteringCompactionTask) PreparePlan() bool {
	return true
}

func (t *clusteringCompactionTask) CheckCompactionContainsSegment(segmentID int64) bool {
	return false
}

func (t *clusteringCompactionTask) BuildCompactionRequest() (*datapb.CompactionPlan, error) {
	beginLogID, _, err := t.allocator.AllocN(1)
	if err != nil {
		return nil, err
	}
	taskProto := t.taskProto.Load().(*datapb.CompactionTask)
	plan := &datapb.CompactionPlan{
		PlanID:                 taskProto.GetPlanID(),
		StartTime:              taskProto.GetStartTime(),
		TimeoutInSeconds:       taskProto.GetTimeoutInSeconds(),
		Type:                   taskProto.GetType(),
		Channel:                taskProto.GetChannel(),
		CollectionTtl:          taskProto.GetCollectionTtl(),
		TotalRows:              taskProto.GetTotalRows(),
		Schema:                 taskProto.GetSchema(),
		ClusteringKeyField:     taskProto.GetClusteringKeyField().GetFieldID(),
		MaxSegmentRows:         taskProto.GetMaxSegmentRows(),
		PreferSegmentRows:      taskProto.GetPreferSegmentRows(),
		AnalyzeResultPath:      path.Join(t.meta.(*meta).chunkManager.RootPath(), common.AnalyzeStatsPath, metautil.JoinIDPath(taskProto.AnalyzeTaskID, taskProto.AnalyzeVersion)),
		AnalyzeSegmentIds:      taskProto.GetInputSegments(),
		BeginLogID:             beginLogID,
		PreAllocatedSegmentIDs: taskProto.GetPreAllocatedSegmentIDs(),
		SlotUsage:              t.GetSlotUsage(),
	}
	log := log.With(zap.Int64("taskID", taskProto.GetTriggerID()), zap.Int64("planID", plan.GetPlanID()))

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
	log := log.Ctx(context.TODO()).With(zap.Int64("triggerID", t.GetTaskProto().TriggerID),
		zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()),
		zap.Int64("planID", t.GetTaskProto().GetPlanID()))
	if t.NeedReAssignNodeID() {
		log.Debug("wait for the node to be assigned before proceeding with the subsequent steps")
		return nil
	}

	// don't mark segment level to L2 before clustering compaction after v2.5.0

	if typeutil.IsVectorType(t.GetTaskProto().GetClusteringKeyField().DataType) {
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
	log := log.Ctx(context.TODO()).With(zap.Int64("planID", t.GetTaskProto().GetPlanID()), zap.String("type", t.GetTaskProto().GetType().String()))
	result, err := t.sessions.GetCompactionPlanResult(t.GetTaskProto().GetNodeID(), t.GetTaskProto().GetPlanID())
	if err != nil || result == nil {
		log.Warn("processExecuting clustering compaction", zap.Error(err))
		if errors.Is(err, merr.ErrNodeNotFound) {
			log.Warn("GetCompactionPlanResult fail", zap.Error(err))
			// setNodeID(NullNodeID) to trigger reassign node ID
			return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
		}
		return err
	}
	log.Debug("compaction result", zap.String("result state", result.GetState().String()),
		zap.Int("result segments num", len(result.GetSegments())), zap.Int("result string length", len(result.String())))
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

		_, metricMutation, err := t.meta.CompleteCompactionMutation(context.TODO(), t.GetTaskProto(), t.result)
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
			return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_timeout))
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
	log := log.Ctx(context.TODO()).With(zap.Int64("planID", t.GetTaskProto().GetPlanID()))
	if err := t.sessions.DropCompactionPlan(t.GetTaskProto().GetNodeID(), &datapb.DropCompactionPlanRequest{
		PlanID: t.GetTaskProto().GetPlanID(),
	}); err != nil {
		log.Warn("clusteringCompactionTask processFailedOrTimeout unable to drop compaction plan", zap.Error(err))
	}
	// to ensure compatibility, if a task upgraded from version 2.4 has a status of MetaSave,
	// its TmpSegments will be empty, so skip the stats task, to build index.
	if len(t.GetTaskProto().GetTmpSegments()) == 0 {
		log.Info("tmp segments is nil, skip stats task")
		return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_indexing))
	}
	return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_statistic))
}

func (t *clusteringCompactionTask) processStats() error {
	log := log.Ctx(context.TODO()).With(zap.Int64("planID", t.GetTaskProto().GetPlanID()))
	// just the memory step, if it crashes at this step, the state after recovery is CompactionTaskState_statistic.
	resultSegments := make([]int64, 0, len(t.GetTaskProto().GetTmpSegments()))
	if Params.DataCoordCfg.EnableStatsTask.GetAsBool() {
		existNonStats := false
		tmpToResultSegments := make(map[int64][]int64, len(t.GetTaskProto().GetTmpSegments()))
		for _, segmentID := range t.GetTaskProto().GetTmpSegments() {
			to, ok := t.meta.(*meta).GetCompactionTo(segmentID)
			if !ok || to == nil {
				select {
				case getStatsTaskChSingleton() <- segmentID:
				default:
				}
				existNonStats = true
				continue
			}
			tmpToResultSegments[segmentID] = lo.Map(to, func(segment *SegmentInfo, _ int) int64 { return segment.GetID() })
			resultSegments = append(resultSegments, lo.Map(to, func(segment *SegmentInfo, _ int) int64 { return segment.GetID() })...)
		}

		if existNonStats {
			return nil
		}

		if err := t.regeneratePartitionStats(tmpToResultSegments); err != nil {
			log.Warn("regenerate partition stats failed, wait for retry", zap.Error(err))
			return merr.WrapErrClusteringCompactionMetaError("regeneratePartitionStats", err)
		}
	} else {
		log.Info("stats task is not enable, set tmp segments to result segments")
		resultSegments = t.GetTaskProto().GetTmpSegments()
	}

	log.Info("clustering compaction stats task finished",
		zap.Int64s("tmp segments", t.GetTaskProto().GetTmpSegments()),
		zap.Int64s("result segments", resultSegments))

	return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_indexing), setResultSegments(resultSegments))
}

// this is just a temporary solution. A more long-term solution should be for the indexnode
// to regenerate the clustering information corresponding to each segment and merge them at the vshard level.
func (t *clusteringCompactionTask) regeneratePartitionStats(tmpToResultSegments map[int64][]int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	chunkManagerFactory := storage.NewChunkManagerFactoryWithParam(Params)
	cli, err := chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
	if err != nil {
		log.Error("chunk manager init failed", zap.Error(err))
		return err
	}
	partitionStatsFile := path.Join(cli.RootPath(), common.PartitionStatsPath,
		metautil.JoinIDPath(t.GetTaskProto().GetCollectionID(), t.GetTaskProto().GetPartitionID()), t.plan.GetChannel(),
		strconv.FormatInt(t.GetTaskProto().GetPlanID(), 10))

	value, err := cli.Read(ctx, partitionStatsFile)
	if err != nil {
		log.Warn("read partition stats file failed", zap.Int64("planID", t.GetTaskProto().GetPlanID()), zap.Error(err))
		return err
	}

	partitionStats, err := storage.DeserializePartitionsStatsSnapshot(value)
	if err != nil {
		log.Warn("deserialize partition stats failed", zap.Int64("planID", t.GetTaskProto().GetPlanID()), zap.Error(err))
		return err
	}

	for from, to := range tmpToResultSegments {
		stats := partitionStats.SegmentStats[from]
		// stats task only one to
		for _, toID := range to {
			partitionStats.SegmentStats[toID] = stats
		}
		delete(partitionStats.SegmentStats, from)
	}

	partitionStatsBytes, err := storage.SerializePartitionStatsSnapshot(partitionStats)
	if err != nil {
		log.Warn("serialize partition stats failed", zap.Int64("planID", t.GetTaskProto().GetPlanID()), zap.Error(err))
		return err
	}

	err = cli.Write(ctx, partitionStatsFile, partitionStatsBytes)
	if err != nil {
		log.Warn("save partition stats file failed", zap.Int64("planID", t.GetTaskProto().GetPlanID()),
			zap.String("path", partitionStatsFile), zap.Error(err))
		return err
	}
	return nil
}

func (t *clusteringCompactionTask) processIndexing() error {
	log := log.Ctx(context.TODO()).With(zap.Int64("planID", t.GetTaskProto().GetPlanID()))
	// wait for segment indexed
	collectionIndexes := t.meta.GetIndexMeta().GetIndexesForCollection(t.GetTaskProto().GetCollectionID(), "")
	if len(collectionIndexes) == 0 {
		log.Debug("the collection has no index, no need to do indexing")
		return t.completeTask()
	}
	indexed := func() bool {
		for _, collectionIndex := range collectionIndexes {
			for _, segmentID := range t.GetTaskProto().GetResultSegments() {
				segmentIndexState := t.meta.GetIndexMeta().GetSegmentIndexState(t.GetTaskProto().GetCollectionID(), segmentID, collectionIndex.IndexID)
				log.Debug("segment index state", zap.String("segment", segmentIndexState.String()))
				if segmentIndexState.GetState() != commonpb.IndexState_Finished {
					return false
				}
			}
		}
		return true
	}()
	log.Debug("check compaction result segments index states",
		zap.Bool("indexed", indexed), zap.Int64s("segments", t.GetTaskProto().ResultSegments))
	if indexed {
		return t.completeTask()
	}
	return nil
}

func (t *clusteringCompactionTask) markResultSegmentsVisible() error {
	var operators []UpdateOperator
	for _, segID := range t.GetTaskProto().GetResultSegments() {
		operators = append(operators, SetSegmentIsInvisible(segID, false))
		operators = append(operators, UpdateSegmentPartitionStatsVersionOperator(segID, t.GetTaskProto().GetPlanID()))
	}

	err := t.meta.UpdateSegmentsInfo(context.TODO(), operators...)
	if err != nil {
		log.Ctx(context.TODO()).Warn("markResultSegmentVisible UpdateSegmentsInfo fail", zap.Error(err))
		return merr.WrapErrClusteringCompactionMetaError("markResultSegmentVisible UpdateSegmentsInfo", err)
	}
	return nil
}

func (t *clusteringCompactionTask) markInputSegmentsDropped() error {
	var operators []UpdateOperator
	// mark
	for _, segID := range t.GetTaskProto().GetInputSegments() {
		operators = append(operators, UpdateStatusOperator(segID, commonpb.SegmentState_Dropped))
	}
	err := t.meta.UpdateSegmentsInfo(context.TODO(), operators...)
	if err != nil {
		log.Ctx(context.TODO()).Warn("markInputSegmentsDropped UpdateSegmentsInfo fail", zap.Error(err))
		return merr.WrapErrClusteringCompactionMetaError("markInputSegmentsDropped UpdateSegmentsInfo", err)
	}
	return nil
}

// indexed is the final state of a clustering compaction task
// one task should only run this once
func (t *clusteringCompactionTask) completeTask() error {
	log := log.Ctx(context.TODO()).With(zap.Int64("planID", t.GetTaskProto().GetPlanID()))
	var err error
	// first mark result segments visible
	if err = t.markResultSegmentsVisible(); err != nil {
		return err
	}

	// update current partition stats version
	// at this point, the segment view includes both the input segments and the result segments.
	if err = t.meta.GetPartitionStatsMeta().SavePartitionStatsInfo(&datapb.PartitionStatsInfo{
		CollectionID: t.GetTaskProto().GetCollectionID(),
		PartitionID:  t.GetTaskProto().GetPartitionID(),
		VChannel:     t.GetTaskProto().GetChannel(),
		Version:      t.GetTaskProto().GetPlanID(),
		SegmentIDs:   t.GetTaskProto().GetResultSegments(),
		CommitTime:   time.Now().Unix(),
	}); err != nil {
		return merr.WrapErrClusteringCompactionMetaError("SavePartitionStatsInfo", err)
	}

	err = t.meta.GetPartitionStatsMeta().SaveCurrentPartitionStatsVersion(t.GetTaskProto().GetCollectionID(),
		t.GetTaskProto().GetPartitionID(), t.GetTaskProto().GetChannel(), t.GetTaskProto().GetPlanID())
	if err != nil {
		return merr.WrapErrClusteringCompactionMetaError("SaveCurrentPartitionStatsVersion", err)
	}

	if err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_completed)); err != nil {
		log.Warn("completeTask update task state to completed failed", zap.Error(err))
		return err
	}
	// mark input segments as dropped
	// now, the segment view only includes the result segments.
	if err = t.markInputSegmentsDropped(); err != nil {
		log.Warn("mark input segments as Dropped failed, skip it and wait retry")
	}

	return nil
}

func (t *clusteringCompactionTask) processAnalyzing() error {
	log := log.Ctx(context.TODO()).With(zap.Int64("planID", t.GetTaskProto().GetPlanID()))
	analyzeTask := t.meta.GetAnalyzeMeta().GetTask(t.GetTaskProto().GetAnalyzeTaskID())
	if analyzeTask == nil {
		log.Warn("analyzeTask not found", zap.Int64("id", t.GetTaskProto().GetAnalyzeTaskID()))
		return merr.WrapErrAnalyzeTaskNotFound(t.GetTaskProto().GetAnalyzeTaskID()) // retryable
	}
	log.Info("check analyze task state", zap.Int64("id", t.GetTaskProto().GetAnalyzeTaskID()),
		zap.Int64("version", analyzeTask.GetVersion()), zap.String("state", analyzeTask.State.String()))
	switch analyzeTask.State {
	case indexpb.JobState_JobStateFinished:
		if analyzeTask.GetCentroidsFile() == "" {
			// not retryable, fake finished vector clustering is not supported in opensource
			return merr.WrapErrClusteringCompactionNotSupportVector()
		} else {
			t.GetTaskProto().AnalyzeVersion = analyzeTask.GetVersion()
			return t.doCompact()
		}
	case indexpb.JobState_JobStateFailed:
		log.Warn("analyze task fail", zap.Int64("analyzeID", t.GetTaskProto().GetAnalyzeTaskID()))
		return errors.New(analyzeTask.FailReason)
	default:
	}
	return nil
}

func (t *clusteringCompactionTask) resetSegmentCompacting() {
	t.meta.SetSegmentsCompacting(context.TODO(), t.GetTaskProto().GetInputSegments(), false)
}

func (t *clusteringCompactionTask) doClean() error {
	log := log.Ctx(context.TODO()).With(zap.Int64("planID", t.GetTaskProto().GetPlanID()))
	log.Info("clean task", zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
		zap.String("state", t.GetTaskProto().GetState().String()))

	if err := t.sessions.DropCompactionPlan(t.GetTaskProto().GetNodeID(), &datapb.DropCompactionPlanRequest{
		PlanID: t.GetTaskProto().GetPlanID(),
	}); err != nil {
		log.Warn("clusteringCompactionTask unable to drop compaction plan", zap.Error(err))
	}
	if t.GetTaskProto().GetState() == datapb.CompactionTaskState_completed {
		if err := t.markInputSegmentsDropped(); err != nil {
			return err
		}
	} else {
		isInputDropped := false
		for _, segID := range t.GetTaskProto().GetInputSegments() {
			if t.meta.GetHealthySegment(context.TODO(), segID) == nil {
				isInputDropped = true
				break
			}
		}
		if isInputDropped {
			log.Info("input segments dropped, doing for compatibility",
				zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()), zap.Int64("planID", t.GetTaskProto().GetPlanID()))
			// this task must be generated by v2.4, just for compatibility
			// revert segments meta
			var operators []UpdateOperator
			// revert level of input segments
			// L1 : L1 ->(process)-> L2 ->(clean)-> L1
			// L2 : L2 ->(process)-> L2 ->(clean)-> L2
			for _, segID := range t.GetTaskProto().GetInputSegments() {
				operators = append(operators, RevertSegmentLevelOperator(segID))
			}
			// if result segments are generated but task fail in the other steps, mark them as L1 segments without partitions stats
			for _, segID := range t.GetTaskProto().GetResultSegments() {
				operators = append(operators, UpdateSegmentLevelOperator(segID, datapb.SegmentLevel_L1))
				operators = append(operators, UpdateSegmentPartitionStatsVersionOperator(segID, 0))
			}
			for _, segID := range t.GetTaskProto().GetTmpSegments() {
				// maybe no necessary, there will be no `TmpSegments` that task was generated by v2.4
				operators = append(operators, UpdateSegmentLevelOperator(segID, datapb.SegmentLevel_L1))
				operators = append(operators, UpdateSegmentPartitionStatsVersionOperator(segID, 0))
			}
			err := t.meta.UpdateSegmentsInfo(context.TODO(), operators...)
			if err != nil {
				log.Warn("UpdateSegmentsInfo fail", zap.Error(err))
				return merr.WrapErrClusteringCompactionMetaError("UpdateSegmentsInfo", err)
			}
		} else {
			// after v2.5.0, mark the results segment as dropped
			var operators []UpdateOperator
			for _, segID := range t.GetTaskProto().GetResultSegments() {
				// Don't worry about them being loaded; they are all invisible.
				operators = append(operators, UpdateStatusOperator(segID, commonpb.SegmentState_Dropped))
			}
			for _, segID := range t.GetTaskProto().GetTmpSegments() {
				// Don't worry about them being loaded; they are all invisible.
				// tmpSegment is always invisible
				operators = append(operators, UpdateStatusOperator(segID, commonpb.SegmentState_Dropped))
			}
			err := t.meta.UpdateSegmentsInfo(context.TODO(), operators...)
			if err != nil {
				log.Warn("UpdateSegmentsInfo fail", zap.Error(err))
				return merr.WrapErrClusteringCompactionMetaError("UpdateSegmentsInfo", err)
			}
		}

		// drop partition stats if uploaded
		partitionStatsInfo := &datapb.PartitionStatsInfo{
			CollectionID: t.GetTaskProto().GetCollectionID(),
			PartitionID:  t.GetTaskProto().GetPartitionID(),
			VChannel:     t.GetTaskProto().GetChannel(),
			Version:      t.GetTaskProto().GetPlanID(),
			SegmentIDs:   t.GetTaskProto().GetResultSegments(),
		}
		err := t.meta.CleanPartitionStatsInfo(context.TODO(), partitionStatsInfo)
		if err != nil {
			log.Warn("gcPartitionStatsInfo fail", zap.Error(err))
			return merr.WrapErrCleanPartitionStatsFail(fmt.Sprintf("%d-%d-%s-%d", t.GetTaskProto().GetCollectionID(), t.GetTaskProto().GetPartitionID(), t.GetTaskProto().GetChannel(), t.GetTaskProto().GetPlanID()))
		}
	}

	err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_cleaned))
	if err != nil {
		log.Warn("clusteringCompactionTask fail to updateAndSaveTaskMeta", zap.Error(err))
		return err
	}

	// resetSegmentCompacting must be the last step of Clean, to make sure resetSegmentCompacting only called once
	// otherwise, it may unlock segments locked by other compaction tasks
	t.resetSegmentCompacting()
	log.Info("clusteringCompactionTask clean done")
	return nil
}

func (t *clusteringCompactionTask) doAnalyze() error {
	log := log.Ctx(context.TODO())
	analyzeTask := &indexpb.AnalyzeTask{
		CollectionID: t.GetTaskProto().GetCollectionID(),
		PartitionID:  t.GetTaskProto().GetPartitionID(),
		FieldID:      t.GetTaskProto().GetClusteringKeyField().FieldID,
		FieldName:    t.GetTaskProto().GetClusteringKeyField().Name,
		FieldType:    t.GetTaskProto().GetClusteringKeyField().DataType,
		SegmentIDs:   t.GetTaskProto().GetInputSegments(),
		TaskID:       t.GetTaskProto().GetAnalyzeTaskID(),
		State:        indexpb.JobState_JobStateInit,
	}
	err := t.meta.GetAnalyzeMeta().AddAnalyzeTask(analyzeTask)
	if err != nil {
		log.Warn("failed to create analyze task", zap.Int64("planID", t.GetTaskProto().GetPlanID()), zap.Error(err))
		return err
	}

	t.analyzeScheduler.enqueue(newAnalyzeTask(t.GetTaskProto().GetAnalyzeTaskID()))

	log.Info("submit analyze task", zap.Int64("planID", t.GetTaskProto().GetPlanID()), zap.Int64("triggerID", t.GetTaskProto().GetTriggerID()), zap.Int64("collectionID", t.GetTaskProto().GetCollectionID()), zap.Int64("id", t.GetTaskProto().GetAnalyzeTaskID()))
	return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_analyzing))
}

func (t *clusteringCompactionTask) doCompact() error {
	log := log.Ctx(context.TODO()).With(zap.Int64("planID", t.GetTaskProto().GetPlanID()), zap.String("type", t.GetTaskProto().GetType().String()))
	if t.NeedReAssignNodeID() {
		log.RatedWarn(10, "not assign nodeID")
		return nil
	}
	log = log.With(zap.Int64("nodeID", t.GetTaskProto().GetNodeID()))

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
	err = t.sessions.Compaction(context.Background(), t.GetTaskProto().GetNodeID(), t.GetPlan())
	if err != nil {
		originNodeID := t.GetTaskProto().GetNodeID()
		log.Warn("Failed to notify compaction tasks to DataNode",
			zap.Int64("planID", t.GetTaskProto().GetPlanID()),
			zap.Int64("nodeID", originNodeID),
			zap.Error(err))
		err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
		if err != nil {
			log.Warn("updateAndSaveTaskMeta fail", zap.Int64("planID", t.GetTaskProto().GetPlanID()), zap.Error(err))
			return err
		}
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", originNodeID), t.GetTaskProto().GetType().String(), metrics.Executing).Dec()
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", NullNodeID), t.GetTaskProto().GetType().String(), metrics.Pending).Inc()
	}
	return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_executing))
}

func (t *clusteringCompactionTask) ShadowClone(opts ...compactionTaskOpt) *datapb.CompactionTask {
	taskClone := proto.Clone(t.GetTaskProto()).(*datapb.CompactionTask)
	for _, opt := range opts {
		opt(taskClone)
	}
	return taskClone
}

func (t *clusteringCompactionTask) updateAndSaveTaskMeta(opts ...compactionTaskOpt) error {
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
		log.Ctx(context.TODO()).Warn("Failed to saveTaskMeta", zap.Error(err))
		return merr.WrapErrClusteringCompactionMetaError("updateAndSaveTaskMeta", err) // retryable
	}
	t.SetTask(task)
	log.Ctx(context.TODO()).Info("updateAndSaveTaskMeta success", zap.String("task state", t.GetTaskProto().GetState().String()))
	return nil
}

func (t *clusteringCompactionTask) checkTimeout() bool {
	if t.GetTaskProto().GetTimeoutInSeconds() > 0 {
		diff := time.Since(time.Unix(t.GetTaskProto().GetStartTime(), 0)).Seconds()
		if diff > float64(t.GetTaskProto().GetTimeoutInSeconds()) {
			log.Ctx(context.TODO()).Warn("compaction timeout",
				zap.Int32("timeout in seconds", t.GetTaskProto().GetTimeoutInSeconds()),
				zap.Int64("startTime", t.GetTaskProto().GetStartTime()),
			)
			return true
		}
	}
	return false
}

func (t *clusteringCompactionTask) saveTaskMeta(task *datapb.CompactionTask) error {
	return t.meta.SaveCompactionTask(context.TODO(), task)
}

func (t *clusteringCompactionTask) SaveTaskMeta() error {
	return t.saveTaskMeta(t.GetTaskProto())
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

func (t *clusteringCompactionTask) SetTask(task *datapb.CompactionTask) {
	t.taskProto.Store(task)
}

func (t *clusteringCompactionTask) SetNodeID(id UniqueID) error {
	return t.updateAndSaveTaskMeta(setNodeID(id))
}

func (t *clusteringCompactionTask) GetLabel() string {
	return fmt.Sprintf("%d-%s", t.GetTaskProto().PartitionID, t.GetTaskProto().GetChannel())
}

func (t *clusteringCompactionTask) NeedReAssignNodeID() bool {
	return t.GetTaskProto().GetState() == datapb.CompactionTaskState_pipelining && (t.GetTaskProto().GetNodeID() == 0 || t.GetTaskProto().GetNodeID() == NullNodeID)
}

func (t *clusteringCompactionTask) GetSlotUsage() int64 {
	return t.slotUsage
}
