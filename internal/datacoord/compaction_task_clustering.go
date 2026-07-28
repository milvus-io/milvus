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

	"github.com/samber/lo"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ CompactionTask = (*clusteringCompactionTask)(nil)

type clusteringCompactionTask struct {
	taskProto atomic.Value // *datapb.CompactionTask
	plan      *datapb.CompactionPlan
	result    *datapb.CompactionPlanResult

	allocator        allocator.Allocator
	meta             CompactionMeta
	handler          Handler
	analyzeScheduler globalTask.GlobalScheduler
	ievm             IndexEngineVersionManager

	maxRetryTimes int32

	times *taskcommon.Times
}

func (t *clusteringCompactionTask) GetTaskID() int64 {
	return t.GetTaskProto().GetPlanID()
}

func (t *clusteringCompactionTask) GetTaskType() taskcommon.Type {
	return taskcommon.Compaction
}

func (t *clusteringCompactionTask) GetTaskState() taskcommon.State {
	return taskcommon.FromCompactionState(t.GetTaskProto().GetState())
}

func (t *clusteringCompactionTask) GetTaskSlot() int64 {
	return paramtable.Get().DataCoordCfg.ClusteringCompactionSlotUsage.GetAsInt64()
}

func (t *clusteringCompactionTask) SetTaskTime(timeType taskcommon.TimeType, time time.Time) {
	t.times.SetTaskTime(timeType, time)
}

func (t *clusteringCompactionTask) GetTaskTime(timeType taskcommon.TimeType) time.Time {
	return timeType.GetTaskTime(t.times)
}

func (t *clusteringCompactionTask) GetTaskVersion() int64 {
	return int64(t.GetTaskProto().GetRetryTimes())
}

func (t *clusteringCompactionTask) retryOnError(err error) {
	if err != nil {
		mlog.Warn(context.TODO(), "clustering compaction task failed", mlog.Err(err))
		if merr.IsRetryableErr(err) && t.GetTaskProto().RetryTimes < t.maxRetryTimes {
			// retry in next Process
			err = t.updateAndSaveTaskMeta(setRetryTimes(t.GetTaskProto().RetryTimes + 1))
		} else {
			mlog.Error(context.TODO(), "task fail with unretryable reason or meet max retry times", mlog.Err(err))
			err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
		}
		if err != nil {
			mlog.Warn(context.TODO(), "Failed to updateAndSaveTaskMeta", mlog.Err(err))
		}
	}
}

func (t *clusteringCompactionTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	var err error
	defer func() {
		t.retryOnError(err)
	}()

	// don't mark segment level to L2 before clustering compaction after v2.5.0

	if typeutil.IsVectorType(t.GetTaskProto().GetClusteringKeyField().DataType) &&
		t.GetTaskProto().GetAnalyzeVersion() == 0 { // analyze not finished
		err = t.doAnalyze()
		if err != nil {
			mlog.Warn(context.TODO(), "fail to submit analyze task", mlog.Err(err))
			err = merr.WrapErrClusteringCompactionSubmitTaskFail("analyze", err)
		}
	} else {
		err = t.doCompact(nodeID, cluster)
		if err != nil {
			mlog.Warn(context.TODO(), "fail to submit compaction task", mlog.Err(err))
			err = merr.WrapErrClusteringCompactionSubmitTaskFail("compact", err)
		}
	}
}

func (t *clusteringCompactionTask) QueryTaskOnWorker(cluster session.Cluster) {
	// If task is in analyzing state, skip querying the DataNode — the compaction has not been
	// submitted yet. The state transition (analyzing → pipelining) is driven by Process() /
	// processAnalyzing(). Once the state becomes pipelining, the scheduler will move the task
	// back to pendingTasks and CreateTaskOnWorker will call doCompact.
	if t.GetTaskProto().GetState() == datapb.CompactionTaskState_analyzing {
		return
	}

	var err error
	defer func() {
		t.retryOnError(err)
	}()

	var result *datapb.CompactionPlanResult
	result, err = cluster.QueryCompaction(t.GetTaskProto().GetNodeID(), &datapb.CompactionStateRequest{
		PlanID: t.GetTaskProto().GetPlanID(),
	})
	if err != nil || result == nil {
		mlog.Warn(context.TODO(), "clusteringCompactionTask failed to get compaction result", mlog.Err(err))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
		if err != nil {
			mlog.Warn(context.TODO(), "update clustering compaction task meta failed", mlog.Err(err))
		}
		return
	}
	mlog.Debug(context.TODO(), "compaction result", mlog.String("result state", result.GetState().String()),
		mlog.Int("result segments num", len(result.GetSegments())), mlog.Int("result string length", len(result.String())))
	switch result.GetState() {
	case datapb.CompactionTaskState_completed:
		t.result = result
		if len(result.GetSegments()) == 0 {
			mlog.Warn(context.TODO(), "illegal compaction results, this should not happen")
			err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason("compaction result is empty"))
			if err != nil {
				mlog.Warn(context.TODO(), "update clustering compaction task meta failed", mlog.Err(err))
			}
			return
		}

		resultSegmentIDs := lo.Map(result.Segments, func(segment *datapb.CompactionSegment, _ int) int64 {
			return segment.GetSegmentID()
		})

		err = t.meta.ValidateSegmentStateBeforeCompleteCompactionMutation(t.GetTaskProto())
		if err != nil {
			t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error()))
			return
		}

		if err = binlog.CompressCompactionBinlogs(result.GetSegments()); err != nil {
			mlog.Warn(context.TODO(), "compress compaction result binlogs failed", mlog.Err(err))
			return
		}

		var metricMutation *segMetricMutation
		_, metricMutation, err = t.meta.CompleteCompactionMutation(context.TODO(), t.GetTaskProto(), t.result)
		if err != nil {
			mlog.Warn(context.TODO(), "CompleteCompactionMutation for clustering compaction task failed", mlog.Err(err))
			return
		}
		metricMutation.commit()
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_meta_saved), setTmpSegments(resultSegmentIDs))
		if err != nil {
			mlog.Warn(context.TODO(), "update clustering compaction task meta failed", mlog.Err(err))
			return
		}
		err = t.processMetaSaved()
		if err != nil {
			mlog.Warn(context.TODO(), "processMetaSaved failed", mlog.Err(err))
		}
	case datapb.CompactionTaskState_pipelining, datapb.CompactionTaskState_executing:
		return
	case datapb.CompactionTaskState_failed:
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
		if err != nil {
			mlog.Warn(context.TODO(), "update clustering compaction task meta failed", mlog.Err(err))
			return
		}
	case datapb.CompactionTaskState_timeout:
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_timeout))
		if err != nil {
			mlog.Warn(context.TODO(), "update clustering compaction task meta failed", mlog.Err(err))
			return
		}
	default:
		mlog.Error(context.TODO(), "not support compaction task state", mlog.String("state", result.GetState().String()))
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed))
		if err != nil {
			mlog.Warn(context.TODO(), "update clustering compaction task meta failed", mlog.Err(err))
			return
		}
	}
}

func (t *clusteringCompactionTask) DropTaskOnWorker(cluster session.Cluster) {
	if err := cluster.DropCompaction(t.GetTaskProto().GetNodeID(), t.GetTaskProto().GetPlanID()); err != nil {
		mlog.Warn(context.TODO(), "clusteringCompactionTask unable to drop compaction plan", mlog.Err(err))
	}
}

func (t *clusteringCompactionTask) GetTaskProto() *datapb.CompactionTask {
	task := t.taskProto.Load()
	if task == nil {
		return nil
	}
	return task.(*datapb.CompactionTask)
}

func newClusteringCompactionTask(t *datapb.CompactionTask, allocator allocator.Allocator, meta CompactionMeta, handler Handler, analyzeScheduler globalTask.GlobalScheduler, ievm IndexEngineVersionManager) *clusteringCompactionTask {
	task := &clusteringCompactionTask{
		allocator:        allocator,
		meta:             meta,
		handler:          handler,
		analyzeScheduler: analyzeScheduler,
		ievm:             ievm,
		maxRetryTimes:    3,
		times:            taskcommon.NewTimes(),
	}
	task.taskProto.Store(t)
	return task
}

// Note: return True means exit this state machine.
// ONLY return True for Completed, Failed or Timeout
func (t *clusteringCompactionTask) Process() bool {
	ctx := context.TODO()
	lastState := t.GetTaskProto().GetState().String()
	err := t.retryableProcess(ctx)
	if err != nil {
		t.retryOnError(err)
	}
	// task state update, refresh retry times count
	currentState := t.GetTaskProto().State.String()
	if currentState != lastState {
		ts := time.Now().Unix()
		lastStateDuration := ts - t.GetTaskProto().GetLastStateStartTime()
		metrics.DataCoordCompactionLatency.
			WithLabelValues(fmt.Sprint(typeutil.IsVectorType(t.GetTaskProto().GetClusteringKeyField().DataType)), t.GetTaskProto().Channel, datapb.CompactionType_ClusteringCompaction.String(), lastState).
			Observe(float64(lastStateDuration * 1000))
		updateOps := []compactionTaskOpt{setRetryTimes(0), setLastStateStartTime(ts)}

		if t.GetTaskProto().State == datapb.CompactionTaskState_completed || t.GetTaskProto().State == datapb.CompactionTaskState_cleaned {
			updateOps = append(updateOps, setEndTime(ts))
			elapse := ts - t.GetTaskProto().StartTime
			mlog.Info(context.TODO(), "clustering compaction task total elapse", mlog.Duration("costs", time.Duration(elapse)*time.Second))
			metrics.DataCoordCompactionLatency.
				WithLabelValues(fmt.Sprint(typeutil.IsVectorType(t.GetTaskProto().GetClusteringKeyField().DataType)), t.GetTaskProto().Channel, datapb.CompactionType_ClusteringCompaction.String(), "total").
				Observe(float64(elapse * 1000))
		}
		err = t.updateAndSaveTaskMeta(updateOps...)
		if err != nil {
			mlog.Warn(context.TODO(), "Failed to updateAndSaveTaskMeta", mlog.Err(err))
		}
		mlog.Info(context.TODO(), "clustering compaction task state changed", mlog.String("lastState", lastState), mlog.String("currentState", currentState), mlog.Int64("elapse seconds", lastStateDuration))
	}
	mlog.Debug(context.TODO(), "process clustering task", mlog.String("lastState", lastState), mlog.String("currentState", currentState))
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
		mlog.Warn(context.TODO(), "fail to get collection", mlog.Int64("collectionID", t.GetTaskProto().GetCollectionID()), mlog.Err(err))
		return merr.WrapErrClusteringCompactionGetCollectionFail(t.GetTaskProto().GetCollectionID(), err)
	}
	if coll == nil {
		// not-retryable fail fast if collection is dropped
		mlog.Warn(context.TODO(), "collection not found, it may be dropped, stop clustering compaction task", mlog.Int64("collectionID", t.GetTaskProto().GetCollectionID()))
		return merr.WrapErrCollectionNotFound(t.GetTaskProto().GetCollectionID())
	}

	switch t.GetTaskProto().State {
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
	mlog.Info(context.TODO(), "clean task", mlog.Int64("planID", t.GetTaskProto().GetPlanID()), mlog.String("type", t.GetTaskProto().GetType().String()))
	return t.doClean() == nil
}

func (t *clusteringCompactionTask) BuildCompactionRequest() (*datapb.CompactionPlan, error) {
	taskProto := t.taskProto.Load().(*datapb.CompactionTask)
	logIDRange, err := PreAllocateBinlogIDs(t.allocator, t.meta.GetSegmentInfos(taskProto.GetInputSegments()), taskProto.GetSchema())
	if err != nil {
		return nil, err
	}
	compactionParams, err := compaction.GenerateJSONParams(taskProto.GetSchema())
	if err != nil {
		return nil, err
	}
	plan := &datapb.CompactionPlan{
		PlanID:                    taskProto.GetPlanID(),
		StartTime:                 taskProto.GetStartTime(),
		Type:                      taskProto.GetType(),
		Channel:                   taskProto.GetChannel(),
		CollectionTtl:             taskProto.GetCollectionTtl(),
		TotalRows:                 taskProto.GetTotalRows(),
		Schema:                    taskProto.GetSchema(),
		ClusteringKeyField:        taskProto.GetClusteringKeyField().GetFieldID(),
		MaxSegmentRows:            taskProto.GetMaxSegmentRows(),
		PreferSegmentRows:         taskProto.GetPreferSegmentRows(),
		AnalyzeResultPath:         path.Join(t.meta.(*meta).chunkManager.RootPath(), common.AnalyzeStatsPath, metautil.JoinIDPath(taskProto.AnalyzeTaskID, taskProto.AnalyzeVersion)),
		AnalyzeSegmentIds:         taskProto.GetInputSegments(),
		BeginLogID:                logIDRange.Begin, // BeginLogID is deprecated, but still assign it for compatibility.
		PreAllocatedSegmentIDs:    taskProto.GetPreAllocatedSegmentIDs(),
		PreAllocatedLogIDs:        logIDRange,
		SlotUsage:                 t.GetSlotUsage(),
		MaxSize:                   taskProto.GetMaxSize(),
		JsonParams:                compactionParams,
		CurrentScalarIndexVersion: t.ievm.ResolveScalarIndexVersion(),
	}

	// set analyzer resource for text match index if use ref mode.
	// Namespace-enabled clustering compaction is routed to the namespace compactor on the
	// DataNode, which builds the text index inline and needs the analyzer resources.
	taskSchema := taskProto.GetSchema()
	if fileresource.IsRefMode(paramtable.Get().CommonCfg.DNFileResourceMode.GetValue()) &&
		taskSchema.GetEnableNamespace() &&
		len(taskSchema.GetFileResourceIds()) > 0 {
		resources, err := t.meta.GetFileResources(context.Background(), taskSchema.GetFileResourceIds()...)
		if err != nil {
			mlog.Warn(context.TODO(), "get file resources for clustering compaction failed", mlog.Int64("collectionID", taskProto.GetCollectionID()), mlog.Err(err))
			return nil, merr.Wrap(err, "get file resources for compaction failed")
		}
		plan.FileResources = resources
	}

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
			IsSortedByNamespace: segInfo.GetIsSortedByNamespace(),
			StorageVersion:      segInfo.GetStorageVersion(),
			Manifest:            segInfo.GetManifestPath(),
			CommitTimestamp:     segInfo.GetCommitTimestamp(),
		})
	}
	WrapPluginContext(taskProto.GetCollectionID(), taskProto.GetSchema().GetProperties(), plan)
	mlog.Info(context.TODO(), "Compaction handler build clustering compaction plan", mlog.Any("PreAllocatedLogIDs", logIDRange))
	return plan, nil
}

func (t *clusteringCompactionTask) processMetaSaved() error {
	// to ensure compatibility, if a task upgraded from version 2.4 has a status of MetaSave,
	// its TmpSegments will be empty, so skip the stats task, to build index.
	if len(t.GetTaskProto().GetTmpSegments()) == 0 {
		mlog.Info(context.TODO(), "tmp segments is nil, skip stats task")
		return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_indexing))
	}
	return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_statistic))
}

func (t *clusteringCompactionTask) processStats() error {
	// just the memory step, if it crashes at this step, the state after recovery is CompactionTaskState_statistic.
	resultSegments := make([]int64, 0, len(t.GetTaskProto().GetTmpSegments()))
	if Params.DataCoordCfg.EnableSortCompaction.GetAsBool() {
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

		task := t.ShadowClone(setResultSegments(resultSegments))
		err := t.saveTaskMeta(task)
		if err != nil {
			return merr.WrapErrClusteringCompactionMetaError("setResultSegments", err)
		}

		if err := t.regeneratePartitionStats(tmpToResultSegments); err != nil {
			mlog.Warn(context.TODO(), "regenerate partition stats failed, wait for retry", mlog.Err(err))
			return merr.WrapErrClusteringCompactionMetaError("regeneratePartitionStats", err)
		}
	} else {
		mlog.Info(context.TODO(), "stats task is not enable, set tmp segments to result segments")
		resultSegments = t.GetTaskProto().GetTmpSegments()
	}

	mlog.Info(context.TODO(), "clustering compaction stats task finished",
		mlog.Int64s("tmp segments", t.GetTaskProto().GetTmpSegments()),
		mlog.Int64s("result segments", resultSegments))

	return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_indexing), setResultSegments(resultSegments))
}

// this is just a temporary solution. A more long-term solution should be for the datanode
// to regenerate the clustering information corresponding to each segment and merge them at the vshard level.
func (t *clusteringCompactionTask) regeneratePartitionStats(tmpToResultSegments map[int64][]int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	chunkManagerFactory := storage.NewChunkManagerFactoryWithParam(Params)
	cli, err := chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
	if err != nil {
		mlog.Error(context.TODO(), "chunk manager init failed", mlog.Err(err))
		return err
	}
	partitionStatsFile := path.Join(cli.RootPath(), common.PartitionStatsPath,
		metautil.JoinIDPath(t.GetTaskProto().GetCollectionID(), t.GetTaskProto().GetPartitionID()), t.GetTaskProto().GetChannel(),
		strconv.FormatInt(t.GetTaskProto().GetPlanID(), 10))

	value, err := cli.Read(ctx, partitionStatsFile)
	if err != nil {
		mlog.Warn(context.TODO(), "read partition stats file failed", mlog.Int64("planID", t.GetTaskProto().GetPlanID()), mlog.Err(err))
		return err
	}

	partitionStats, err := storage.DeserializePartitionsStatsSnapshot(value)
	if err != nil {
		mlog.Warn(context.TODO(), "deserialize partition stats failed", mlog.Int64("planID", t.GetTaskProto().GetPlanID()), mlog.Err(err))
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
		mlog.Warn(context.TODO(), "serialize partition stats failed", mlog.Int64("planID", t.GetTaskProto().GetPlanID()), mlog.Err(err))
		return err
	}

	err = cli.Write(ctx, partitionStatsFile, partitionStatsBytes)
	if err != nil {
		mlog.Warn(context.TODO(), "save partition stats file failed", mlog.Int64("planID", t.GetTaskProto().GetPlanID()),
			mlog.String("path", partitionStatsFile), mlog.Err(err))
		return err
	}
	return nil
}

func (t *clusteringCompactionTask) processIndexing() error {
	// wait for segment indexed
	collectionIndexes := t.meta.GetIndexMeta().GetIndexesForCollection(t.GetTaskProto().GetCollectionID(), "")
	if len(collectionIndexes) == 0 {
		mlog.Debug(context.TODO(), "the collection has no index, no need to do indexing")
		return t.completeTask()
	}
	indexed := func() bool {
		for _, collectionIndex := range collectionIndexes {
			for _, segmentID := range t.GetTaskProto().GetResultSegments() {
				segmentIndexState := t.meta.GetIndexMeta().GetSegmentIndexState(t.GetTaskProto().GetCollectionID(), segmentID, collectionIndex.IndexID)
				mlog.Debug(context.TODO(), "segment index state", mlog.String("segment", segmentIndexState.String()))
				if segmentIndexState.GetState() != commonpb.IndexState_Finished {
					return false
				}
			}
		}
		return true
	}()
	mlog.Debug(context.TODO(), "check compaction result segments index states",
		mlog.Bool("indexed", indexed), mlog.Int64s("segments", t.GetTaskProto().ResultSegments))
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
		mlog.Warn(context.TODO(), "markResultSegmentVisible UpdateSegmentsInfo fail", mlog.Err(err))
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
		mlog.Warn(context.TODO(), "markInputSegmentsDropped UpdateSegmentsInfo fail", mlog.Err(err))
		return merr.WrapErrClusteringCompactionMetaError("markInputSegmentsDropped UpdateSegmentsInfo", err)
	}
	return nil
}

// indexed is the final state of a clustering compaction task
// one task should only run this once
func (t *clusteringCompactionTask) completeTask() error {
	var err error
	// first mark result segments visible
	if err = t.markResultSegmentsVisible(); err != nil {
		return err
	}

	// update current partition stats version
	// at this point, the segment view includes both the input segments and the result segments.
	// Persist the stats info and the current-version pointer bump as a single
	// composite catalog write (info first, version pointer last as the commit
	// marker), so a crash cannot leave the current version pointing at a stats
	// set that was never persisted.
	if err = t.meta.GetPartitionStatsMeta().SavePartitionStatsAndVersion(&datapb.PartitionStatsInfo{
		CollectionID: t.GetTaskProto().GetCollectionID(),
		PartitionID:  t.GetTaskProto().GetPartitionID(),
		VChannel:     t.GetTaskProto().GetChannel(),
		Version:      t.GetTaskProto().GetPlanID(),
		SegmentIDs:   t.GetTaskProto().GetResultSegments(),
		CommitTime:   time.Now().Unix(),
	}, t.GetTaskProto().GetPlanID()); err != nil {
		return merr.WrapErrClusteringCompactionMetaError("SavePartitionStatsAndVersion", err)
	}

	if err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_completed)); err != nil {
		mlog.Warn(context.TODO(), "completeTask update task state to completed failed", mlog.Err(err))
		return err
	}
	// mark input segments as dropped
	// now, the segment view only includes the result segments.
	if err = t.markInputSegmentsDropped(); err != nil {
		mlog.Warn(context.TODO(), "mark input segments as Dropped failed, skip it and wait retry")
	}

	return nil
}

func (t *clusteringCompactionTask) processAnalyzing() error {
	analyzeTask := t.meta.GetAnalyzeMeta().GetTask(t.GetTaskProto().GetAnalyzeTaskID())
	if analyzeTask == nil {
		mlog.Warn(context.TODO(), "analyzeTask not found", mlog.Int64("id", t.GetTaskProto().GetAnalyzeTaskID()))
		return merr.WrapErrAnalyzeTaskNotFound(t.GetTaskProto().GetAnalyzeTaskID()) // retryable
	}
	mlog.Info(context.TODO(), "check analyze task state", mlog.Int64("id", t.GetTaskProto().GetAnalyzeTaskID()),
		mlog.Int64("version", analyzeTask.GetVersion()), mlog.String("state", analyzeTask.State.String()))
	switch analyzeTask.State {
	case indexpb.JobState_JobStateFinished:
		if analyzeTask.GetCentroidsFile() == "" {
			// not retryable, fake finished vector clustering is not supported in opensource
			return merr.WrapErrClusteringCompactionNotSupportVector()
		} else {
			t.GetTaskProto().AnalyzeVersion = analyzeTask.GetVersion()
			return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining))
		}
	case indexpb.JobState_JobStateFailed:
		mlog.Warn(context.TODO(), "analyze task fail", mlog.Int64("analyzeID", t.GetTaskProto().GetAnalyzeTaskID()))
		return merr.WrapErrServiceInternalMsg(analyzeTask.FailReason)
	default:
	}
	return nil
}

func (t *clusteringCompactionTask) resetSegmentCompacting() {
	t.meta.SetSegmentsCompacting(context.TODO(), t.GetTaskProto().GetInputSegments(), false)
}

func (t *clusteringCompactionTask) doClean() error {
	mlog.Info(context.TODO(), "clean task", mlog.Int64("triggerID", t.GetTaskProto().GetTriggerID()),
		mlog.String("state", t.GetTaskProto().GetState().String()))

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
			mlog.Info(context.TODO(), "input segments dropped, doing for compatibility",
				mlog.Int64("triggerID", t.GetTaskProto().GetTriggerID()), mlog.Int64("planID", t.GetTaskProto().GetPlanID()))
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
				mlog.Warn(context.TODO(), "UpdateSegmentsInfo fail", mlog.Err(err))
				return merr.WrapErrClusteringCompactionMetaError("UpdateSegmentsInfo", err)
			}
		} else {
			// after v2.5.0, mark the results segment as dropped
			var operators []UpdateOperator
			hasResultSegments := len(t.GetTaskProto().GetResultSegments()) != 0
			if hasResultSegments {
				for _, segID := range t.GetTaskProto().GetResultSegments() {
					// Don't worry about them being loaded; they are all invisible.
					operators = append(operators, UpdateStatusOperator(segID, commonpb.SegmentState_Dropped))
				}
			}

			for _, segID := range t.GetTaskProto().GetTmpSegments() {
				// Don't worry about them being loaded; they are all invisible.
				// tmpSegment is always invisible
				operators = append(operators, UpdateStatusOperator(segID, commonpb.SegmentState_Dropped))
				if !hasResultSegments {
					toSegments, _ := t.meta.(*meta).GetCompactionTo(segID)
					for _, toSeg := range toSegments {
						operators = append(operators, UpdateStatusOperator(toSeg.GetID(), commonpb.SegmentState_Dropped))
					}
				}
			}
			err := t.meta.UpdateSegmentsInfo(context.TODO(), operators...)
			if err != nil {
				mlog.Warn(context.TODO(), "UpdateSegmentsInfo fail", mlog.Err(err))
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
			mlog.Warn(context.TODO(), "gcPartitionStatsInfo fail", mlog.Err(err))
			return merr.WrapErrCleanPartitionStatsFail(fmt.Sprintf("%d-%d-%s-%d", t.GetTaskProto().GetCollectionID(), t.GetTaskProto().GetPartitionID(), t.GetTaskProto().GetChannel(), t.GetTaskProto().GetPlanID()))
		}
	}

	err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_cleaned))
	if err != nil {
		mlog.Warn(context.TODO(), "clusteringCompactionTask fail to updateAndSaveTaskMeta", mlog.Err(err))
		return err
	}

	// resetSegmentCompacting must be the last step of Clean, to make sure resetSegmentCompacting only called once
	// otherwise, it may unlock segments locked by other compaction tasks
	t.resetSegmentCompacting()
	mlog.Info(context.TODO(), "clusteringCompactionTask clean done")
	return nil
}

func (t *clusteringCompactionTask) doAnalyze() error {
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
		mlog.Warn(context.TODO(), "failed to create analyze task", mlog.Int64("planID", t.GetTaskProto().GetPlanID()), mlog.Err(err))
		return err
	}

	t.analyzeScheduler.Enqueue(newAnalyzeTask(proto.Clone(analyzeTask).(*indexpb.AnalyzeTask), t.meta.(*meta)))

	mlog.Info(context.TODO(), "submit analyze task", mlog.Int64("planID", t.GetTaskProto().GetPlanID()), mlog.Int64("triggerID", t.GetTaskProto().GetTriggerID()), mlog.Int64("collectionID", t.GetTaskProto().GetCollectionID()), mlog.Int64("id", t.GetTaskProto().GetAnalyzeTaskID()))
	return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_analyzing))
}

func (t *clusteringCompactionTask) doCompact(nodeID int64, cluster session.Cluster) error {
	var err error
	t.plan, err = t.BuildCompactionRequest()
	if err != nil {
		mlog.Warn(context.TODO(), "Failed to BuildCompactionRequest", mlog.Err(err))
		return err
	}
	err = cluster.CreateCompaction(nodeID, t.GetPlan(), t.GetTaskProto().GetCollectionID())
	if err != nil {
		originNodeID := t.GetTaskProto().GetNodeID()
		mlog.Warn(context.TODO(), "Failed to notify compaction tasks to DataNode",
			mlog.Int64("planID", t.GetTaskProto().GetPlanID()),
			mlog.Int64("nodeID", originNodeID),
			mlog.Err(err))
		err := t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining), setNodeID(NullNodeID))
		if err != nil {
			mlog.Warn(context.TODO(), "updateAndSaveTaskMeta fail", mlog.Int64("planID", t.GetTaskProto().GetPlanID()), mlog.Err(err))
			return err
		}
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", originNodeID), t.GetTaskProto().GetType().String(), metrics.Executing).Dec()
		metrics.DataCoordCompactionTaskNum.WithLabelValues(fmt.Sprintf("%d", NullNodeID), t.GetTaskProto().GetType().String(), metrics.Pending).Inc()
	}
	return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_executing), setNodeID(nodeID))
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
		mlog.Warn(context.TODO(), "Failed to saveTaskMeta", mlog.Err(err))
		return merr.WrapErrClusteringCompactionMetaError("updateAndSaveTaskMeta", err) // retryable
	}
	t.SetTask(task)
	mlog.Info(context.TODO(), "updateAndSaveTaskMeta success", mlog.String("task state", t.GetTaskProto().GetState().String()))
	return nil
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

func (t *clusteringCompactionTask) SetResult(result *datapb.CompactionPlanResult) {
	t.result = result
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
	return t.GetTaskSlot()
}
