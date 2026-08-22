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

	plan   *datapb.CompactionPlan
	result *datapb.CompactionPlanResult

	// ctx is the inspector's process context, threaded from buildCompactTask
	// so the scheduler callbacks (which receive none) can still log with it.
	ctx context.Context

	allocator        allocator.Allocator
	meta             CompactionMeta
	handler          Handler
	analyzeScheduler globalTask.GlobalScheduler
	ievm             IndexEngineVersionManager

	times *taskcommon.Times
}

func (t *clusteringCompactionTask) GetTaskID() int64 {
	return t.GetTask().GetPlanID()
}

func (t *clusteringCompactionTask) GetTaskType() taskcommon.Type {
	return taskcommon.Compaction
}

func (t *clusteringCompactionTask) GetTaskState() taskcommon.State {
	return taskcommon.FromCompactionState(t.GetTask().GetState())
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
	return int64(t.GetTask().GetRetryTimes())
}

// failOnError persists failed for any error the state machine surfaces. There
// is deliberately no in-place retry and no retryable-vs-permanent
// classification here: the one retry mechanism is the inspector's replan at
// cleanup -- a failed task is rebuilt under a fresh planID, bounded by
// dataCoord.compaction.maxAttempts -- so a transient error costs one replan
// instead of maintaining a second, counter-based retry loop whose RetryTimes
// accounting the replan cap could not trust.
func (t *clusteringCompactionTask) failOnError(err error) {
	if err != nil {
		mlog.Warn(t.ctx, "clustering compaction task failed",
			mlog.Int64("planID", t.GetTask().GetPlanID()), mlog.Err(err))
		if saveErr := t.updateAndSaveTaskMeta(setAttemptEnded(), setFailReason(err.Error())); saveErr != nil {
			// The state stays where it was and the next round surfaces the
			// error again, which arrives at this same save.
			mlog.Warn(t.ctx, "Failed to updateAndSaveTaskMeta",
				mlog.Int64("planID", t.GetTask().GetPlanID()), mlog.Err(saveErr))
		}
	}
}

func (t *clusteringCompactionTask) CreateTaskOnWorker(nodeID int64, cluster session.Cluster) {
	if callbackPreempted(t) {
		return
	}
	var err error
	defer func() {
		t.failOnError(err)
	}()

	// don't mark segment level to L2 before clustering compaction after v2.5.0

	if typeutil.IsVectorType(t.GetTask().GetClusteringKeyField().DataType) &&
		t.GetTask().GetAnalyzeVersion() == 0 { // analyze not finished
		err = t.doAnalyze()
		if err != nil {
			mlog.Warn(t.ctx, "fail to submit analyze task", mlog.Err(err))
			err = merr.WrapErrClusteringCompactionSubmitTaskFail("analyze", err)
		}
	} else {
		err = t.doCompact(nodeID, cluster)
		if err != nil {
			mlog.Warn(t.ctx, "fail to submit compaction task", mlog.Err(err))
			err = merr.WrapErrClusteringCompactionSubmitTaskFail("compact", err)
		}
	}
}

func (t *clusteringCompactionTask) QueryTaskOnWorker(cluster session.Cluster) {
	if callbackPreempted(t) {
		return
	}
	// If task is in analyzing state, skip querying the DataNode — the compaction has not been
	// submitted yet. The state transition (analyzing → pipelining) is driven by Process() /
	// processAnalyzing(). Once the state becomes pipelining, the scheduler will move the task
	// back to pendingTasks and CreateTaskOnWorker will call doCompact.
	if t.GetTask().GetState() == datapb.CompactionTaskState_analyzing {
		return
	}

	if hasNoWorker(t) {
		return
	}

	var err error
	defer func() {
		t.failOnError(err)
	}()

	var result *datapb.CompactionPlanResult
	result, err = cluster.QueryCompaction(t.GetTask().GetNodeID(), &datapb.CompactionStateRequest{
		PlanID: t.GetTask().GetPlanID(),
	})
	if err != nil || result == nil {
		log := mlog.With(mlog.Int64("planID", t.GetTask().GetPlanID()),
			mlog.FieldNodeID(t.GetTask().GetNodeID()))
		queryErr := err
		// Clear the named err before returning: this function's deferred
		// failOnError(err) would otherwise persist failed -- the exact opposite
		// of what every branch below decides, which is to keep or abandon the
		// assignment without ending the attempt as a plain failure.
		err = nil
		// See mixCompactionTask.QueryTaskOnWorker. Clustering has the sharpest
		// version of the hazard: its partition-stats object is keyed by planID
		// alone, so two executions of the same plan overwrite one another.
		if errors.Is(queryErr, merr.ErrNodeNotFound) {
			log.Warn(t.ctx, "clusteringCompactionTask worker left the cluster, abandoning attempt for replan", mlog.Err(queryErr))
			abandonAttempt(t.ctx, t, "assigned worker left the cluster")
			return
		}
		// Same rule as the create path: an RPC round that ends without an
		// answer ends the attempt; see mixCompactionTask.QueryTaskOnWorker.
		log.Warn(t.ctx, "clusteringCompactionTask query unanswered, abandoning attempt for replan", mlog.Err(queryErr))
		abandonAttempt(t.ctx, t, "worker left the query unanswered")
		return
	}
	mlog.Debug(t.ctx, "compaction result", mlog.String("result state", result.GetState().String()),
		mlog.Int("result segments num", len(result.GetSegments())), mlog.Int("result string length", len(result.String())))
	switch result.GetState() {
	case datapb.CompactionTaskState_completed:
		t.result = result
		if len(result.GetSegments()) == 0 {
			mlog.Warn(t.ctx, "illegal compaction results, this should not happen")
			err = t.updateAndSaveTaskMeta(setAttemptEnded(), setFailReason("compaction result is empty"))
			if err != nil {
				mlog.Warn(t.ctx, "update clustering compaction task meta failed", mlog.Err(err))
			}
			return
		}

		resultSegmentIDs := lo.Map(result.Segments, func(segment *datapb.CompactionSegment, _ int) int64 {
			return segment.GetSegmentID()
		})

		err = t.meta.ValidateSegmentStateBeforeCompleteCompactionMutation(t.GetTask())
		if err != nil {
			// See mixCompactionTask: a rejected completed result ends the
			// attempt and must not do so silently.
			mlog.Warn(t.ctx, "clusteringCompactionTask rejected a completed result, ending the attempt",
				mlog.Int64("planID", t.GetTask().GetPlanID()), mlog.Err(err))
			if saveErr := t.updateAndSaveTaskMeta(setAttemptEnded(), setFailReason(err.Error())); saveErr != nil {
				mlog.Warn(t.ctx, "clusteringCompactionTask failed to persist the rejected result",
					mlog.Int64("planID", t.GetTask().GetPlanID()), mlog.Err(saveErr))
			}
			return
		}

		if err = binlog.CompressCompactionBinlogs(result.GetSegments()); err != nil {
			mlog.Warn(t.ctx, "compress compaction result binlogs failed", mlog.Err(err))
			return
		}

		// Persist ownership of every output ID before CompleteCompactionMutation
		// can publish any of those segments. TmpSegments is the durable cleanup
		// inventory for the direct compaction outputs: if the mutation partially
		// succeeds, its following task-state save fails, or DataCoord restarts in
		// either window, cleanup can still find and drop everything this attempt
		// may have created. A failed ownership write must stop before the segment
		// mutation; otherwise the old task record would have no way to name its
		// output.
		if err = t.updateAndSaveTaskMeta(setTmpSegments(resultSegmentIDs)); err != nil {
			mlog.Warn(t.ctx, "persist clustering compaction output ownership failed",
				mlog.Int64("planID", t.GetTask().GetPlanID()), mlog.Err(err))
			return
		}

		var metricMutation *segMetricMutation
		_, metricMutation, err = t.meta.CompleteCompactionMutation(t.ctx, t.GetTask(), t.result)
		if err != nil {
			mlog.Warn(t.ctx, "CompleteCompactionMutation for clustering compaction task failed", mlog.Err(err))
			return
		}
		metricMutation.commit()
		err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_meta_saved))
		if err != nil {
			mlog.Warn(t.ctx, "update clustering compaction task meta failed", mlog.Err(err))
			return
		}
		err = t.processMetaSaved()
		if err != nil {
			mlog.Warn(t.ctx, "processMetaSaved failed", mlog.Err(err))
		}
	case datapb.CompactionTaskState_pipelining, datapb.CompactionTaskState_executing:
		return
	case datapb.CompactionTaskState_failed:
		// Keep the worker's reason; see mixCompactionTask for why.
		reason := workerCompactionFailReason(result.GetReason())
		mlog.Warn(t.ctx, "clusteringCompactionTask fail in datanode",
			mlog.Int64("planID", t.GetTask().GetPlanID()), mlog.String("reason", reason))
		err = t.updateAndSaveTaskMeta(setAttemptEnded(), setFailReason(reason))
		if err != nil {
			mlog.Warn(t.ctx, "update clustering compaction task meta failed", mlog.Err(err))
			return
		}
	case datapb.CompactionTaskState_timeout:
		err = t.updateAndSaveTaskMeta(setAttemptEnded())
		if err != nil {
			mlog.Warn(t.ctx, "update clustering compaction task meta failed", mlog.Err(err))
			return
		}
	default:
		mlog.Error(t.ctx, "not support compaction task state", mlog.String("state", result.GetState().String()))
		err = t.updateAndSaveTaskMeta(setAttemptEnded())
		if err != nil {
			mlog.Warn(t.ctx, "update clustering compaction task meta failed", mlog.Err(err))
			return
		}
	}
}

func (t *clusteringCompactionTask) DropTaskOnWorker(cluster session.Cluster) {
	if err := cluster.DropCompaction(t.GetTask().GetNodeID(), t.GetTask().GetPlanID()); err != nil {
		mlog.Warn(t.ctx, "clusteringCompactionTask unable to drop compaction plan", mlog.Err(err))
	}
}

func (t *clusteringCompactionTask) GetTask() *datapb.CompactionTask {
	task := t.taskProto.Load()
	if task == nil {
		return nil
	}
	return task.(*datapb.CompactionTask)
}

func newClusteringCompactionTask(ctx context.Context, t *datapb.CompactionTask, allocator allocator.Allocator, meta CompactionMeta, handler Handler, analyzeScheduler globalTask.GlobalScheduler, ievm IndexEngineVersionManager) *clusteringCompactionTask {
	task := &clusteringCompactionTask{
		ctx:              ctx,
		allocator:        allocator,
		meta:             meta,
		handler:          handler,
		analyzeScheduler: analyzeScheduler,
		ievm:             ievm,
		times:            taskcommon.NewTimes(),
	}
	task.taskProto.Store(t)
	return task
}

// Note: return True means exit this state machine.
// ONLY return True for Completed, Failed or Timeout
func (t *clusteringCompactionTask) Process() bool {
	ctx := t.ctx
	lastState := t.GetTask().GetState().String()
	err := t.processState(ctx)
	if err != nil {
		t.failOnError(err)
	}
	// task state update, refresh retry times count
	currentState := t.GetTask().State.String()
	if currentState != lastState {
		ts := time.Now().Unix()
		lastStateDuration := ts - t.GetTask().GetLastStateStartTime()
		metrics.DataCoordCompactionLatency.
			WithLabelValues(fmt.Sprint(typeutil.IsVectorType(t.GetTask().GetClusteringKeyField().DataType)), t.GetTask().Channel, datapb.CompactionType_ClusteringCompaction.String(), lastState).
			Observe(float64(lastStateDuration * 1000))
		// RetryTimes is deliberately left alone: only the replan writes it,
		// so it counts replans durably and the attempt cap can trust it.
		updateOps := []compactionTaskOpt{setLastStateStartTime(ts)}

		if t.GetTask().State == datapb.CompactionTaskState_completed || t.GetTask().State == datapb.CompactionTaskState_cleaned {
			// EndTime needs no opt here: updateAndSaveCompactionTaskMeta stamps
			// it on the first terminal transition.
			elapse := ts - t.GetTask().StartTime
			mlog.Info(t.ctx, "clustering compaction task total elapse", mlog.Duration("costs", time.Duration(elapse)*time.Second))
			metrics.DataCoordCompactionLatency.
				WithLabelValues(fmt.Sprint(typeutil.IsVectorType(t.GetTask().GetClusteringKeyField().DataType)), t.GetTask().Channel, datapb.CompactionType_ClusteringCompaction.String(), "total").
				Observe(float64(elapse * 1000))
		}
		err = t.updateAndSaveTaskMeta(updateOps...)
		if err != nil {
			mlog.Warn(t.ctx, "Failed to updateAndSaveTaskMeta", mlog.Err(err))
		}
		mlog.Info(t.ctx, "clustering compaction task state changed", mlog.String("lastState", lastState), mlog.String("currentState", currentState), mlog.Int64("elapse seconds", lastStateDuration))
	}
	mlog.Debug(t.ctx, "process clustering task", mlog.String("lastState", lastState), mlog.String("currentState", currentState))
	return isTerminalState(t.GetTask().GetState())
}

// processState advances the task's state machine one step, returning any error
// it hits. The outer Process hands that error to failOnError: the task fails
// and the inspector's replan rebuilds it under a fresh planID -- there is no
// in-place retry and no retryable-vs-permanent classification anymore.
func (t *clusteringCompactionTask) processState(ctx context.Context) error {
	if isTerminalState(t.GetTask().GetState()) {
		return nil
	}

	coll, err := t.handler.GetCollection(ctx, t.GetTask().GetCollectionID())
	if err != nil {
		mlog.Warn(t.ctx, "fail to get collection", mlog.Int64("collectionID", t.GetTask().GetCollectionID()), mlog.Err(err))
		return merr.WrapErrClusteringCompactionGetCollectionFail(t.GetTask().GetCollectionID(), err)
	}
	if coll == nil {
		// collection dropped: fail fast, the replan's admission will reject too
		mlog.Warn(t.ctx, "collection not found, it may be dropped, stop clustering compaction task", mlog.Int64("collectionID", t.GetTask().GetCollectionID()))
		return merr.WrapErrCollectionNotFound(t.GetTask().GetCollectionID())
	}

	switch t.GetTask().State {
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
	// Runs under the scheduler's per-task lock, handed over by Finalize.
	if alreadyCleaned(t) {
		return true
	}
	mlog.Info(t.ctx, "clean task", mlog.Int64("planID", t.GetTask().GetPlanID()), mlog.String("type", t.GetTask().GetType().String()))
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

	// Namespace-enabled clustering compaction is routed to the namespace compactor on the
	// DataNode, which builds the text index inline and needs the analyzer resources in ref mode.
	taskSchema := taskProto.GetSchema()
	if fileresource.IsRefMode(paramtable.Get().CommonCfg.DNFileResourceMode.GetValue()) &&
		taskSchema.GetEnableNamespace() &&
		len(taskSchema.GetFileResourceIds()) > 0 {
		resources, err := t.meta.GetFileResources(context.Background(), taskSchema.GetFileResourceIds()...)
		if err != nil {
			mlog.Warn(t.ctx, "get file resources for clustering compaction failed", mlog.Int64("collectionID", taskProto.GetCollectionID()), mlog.Err(err))
			return nil, merr.Wrap(err, "get file resources for compaction failed")
		}
		plan.FileResources = resources
	}

	for _, segID := range taskProto.GetInputSegments() {
		segInfo := t.meta.GetHealthySegment(t.ctx, segID)
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
	mlog.Info(t.ctx, "Compaction handler build clustering compaction plan", mlog.Any("PreAllocatedLogIDs", logIDRange))
	return plan, nil
}

func (t *clusteringCompactionTask) processMetaSaved() error {
	// to ensure compatibility, if a task upgraded from version 2.4 has a status of MetaSave,
	// its TmpSegments will be empty, so skip the stats task, to build index.
	if len(t.GetTask().GetTmpSegments()) == 0 {
		mlog.Info(t.ctx, "tmp segments is nil, skip stats task")
		return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_indexing))
	}
	return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_statistic))
}

func (t *clusteringCompactionTask) processStats() error {
	// just the memory step, if it crashes at this step, the state after recovery is CompactionTaskState_statistic.
	resultSegments := make([]int64, 0, len(t.GetTask().GetTmpSegments()))
	existNonStats := false
	tmpToResultSegments := make(map[int64][]int64, len(t.GetTask().GetTmpSegments()))
	for _, segmentID := range t.GetTask().GetTmpSegments() {
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

	// Stats/sort output segments already exist by the time GetCompactionTo
	// returns them. Make them part of this attempt's in-memory and durable
	// cleanup inventory before touching the partition-stats object. Using the
	// normal update path matters: a later regeneration failure is converted to
	// retrying by failOnError, which clones the in-memory task; a raw catalog
	// save here would be overwritten by that failure save and lose the IDs.
	err := t.updateAndSaveTaskMeta(setResultSegments(resultSegments))
	if err != nil {
		return merr.WrapErrClusteringCompactionMetaError("setResultSegments", err)
	}

	if err := t.regeneratePartitionStats(tmpToResultSegments); err != nil {
		mlog.Warn(t.ctx, "regenerate partition stats failed, wait for retry", mlog.Err(err))
		return merr.WrapErrClusteringCompactionMetaError("regeneratePartitionStats", err)
	}

	mlog.Info(t.ctx, "clustering compaction stats task finished",
		mlog.Int64s("tmp segments", t.GetTask().GetTmpSegments()),
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
		mlog.Error(t.ctx, "chunk manager init failed", mlog.Err(err))
		return err
	}
	partitionStatsFile := path.Join(cli.RootPath(), common.PartitionStatsPath,
		metautil.JoinIDPath(t.GetTask().GetCollectionID(), t.GetTask().GetPartitionID()), t.GetTask().GetChannel(),
		strconv.FormatInt(t.GetTask().GetPlanID(), 10))

	value, err := cli.Read(ctx, partitionStatsFile)
	if err != nil {
		mlog.Warn(t.ctx, "read partition stats file failed", mlog.Int64("planID", t.GetTask().GetPlanID()), mlog.Err(err))
		return err
	}

	partitionStats, err := storage.DeserializePartitionsStatsSnapshot(value)
	if err != nil {
		mlog.Warn(t.ctx, "deserialize partition stats failed", mlog.Int64("planID", t.GetTask().GetPlanID()), mlog.Err(err))
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
		mlog.Warn(t.ctx, "serialize partition stats failed", mlog.Int64("planID", t.GetTask().GetPlanID()), mlog.Err(err))
		return err
	}

	err = cli.Write(ctx, partitionStatsFile, partitionStatsBytes)
	if err != nil {
		mlog.Warn(t.ctx, "save partition stats file failed", mlog.Int64("planID", t.GetTask().GetPlanID()),
			mlog.String("path", partitionStatsFile), mlog.Err(err))
		return err
	}
	return nil
}

func (t *clusteringCompactionTask) processIndexing() error {
	// wait for segment indexed
	collectionIndexes := t.meta.GetIndexMeta().GetIndexesForCollection(t.GetTask().GetCollectionID(), "")
	if len(collectionIndexes) == 0 {
		mlog.Debug(t.ctx, "the collection has no index, no need to do indexing")
		return t.completeTask()
	}
	indexed := func() bool {
		for _, collectionIndex := range collectionIndexes {
			for _, segmentID := range t.GetTask().GetResultSegments() {
				segmentIndexState := t.meta.GetIndexMeta().GetSegmentIndexState(t.GetTask().GetCollectionID(), segmentID, collectionIndex.IndexID)
				mlog.Debug(t.ctx, "segment index state", mlog.String("segment", segmentIndexState.String()))
				if segmentIndexState.GetState() != commonpb.IndexState_Finished {
					return false
				}
			}
		}
		return true
	}()
	mlog.Debug(t.ctx, "check compaction result segments index states",
		mlog.Bool("indexed", indexed), mlog.Int64s("segments", t.GetTask().ResultSegments))
	if indexed {
		return t.completeTask()
	}
	return nil
}

func (t *clusteringCompactionTask) markResultSegmentsVisible() error {
	var operators []UpdateOperator
	for _, segID := range t.GetTask().GetResultSegments() {
		operators = append(operators, SetSegmentIsInvisible(segID, false))
		operators = append(operators, UpdateSegmentPartitionStatsVersionOperator(segID, t.GetTask().GetPlanID()))
	}

	err := t.meta.UpdateSegmentsInfo(t.ctx, operators...)
	if err != nil {
		mlog.Warn(t.ctx, "markResultSegmentVisible UpdateSegmentsInfo fail", mlog.Err(err))
		return merr.WrapErrClusteringCompactionMetaError("markResultSegmentVisible UpdateSegmentsInfo", err)
	}
	return nil
}

func (t *clusteringCompactionTask) markInputSegmentsDropped() error {
	var operators []UpdateOperator
	// mark
	for _, segID := range t.GetTask().GetInputSegments() {
		operators = append(operators, UpdateStatusOperator(segID, commonpb.SegmentState_Dropped))
	}
	err := t.meta.UpdateSegmentsInfo(t.ctx, operators...)
	if err != nil {
		mlog.Warn(t.ctx, "markInputSegmentsDropped UpdateSegmentsInfo fail", mlog.Err(err))
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
		CollectionID: t.GetTask().GetCollectionID(),
		PartitionID:  t.GetTask().GetPartitionID(),
		VChannel:     t.GetTask().GetChannel(),
		Version:      t.GetTask().GetPlanID(),
		SegmentIDs:   t.GetTask().GetResultSegments(),
		CommitTime:   time.Now().Unix(),
		// Persisted so the eventual cleanup can find the analyze task. This
		// record outlives the compaction task, and CleanPartitionStatsInfo --
		// which runs much later, when this version ages out or the collection
		// is dropped -- resolves the analyze record from this field alone.
		// Leaving it zero makes that lookup resolve task 0, so the real analyze
		// record survives and recycleUnusedAnalyzeFiles keeps its files.
		AnalyzeTaskID: t.GetTask().GetAnalyzeTaskID(),
	}, t.GetTask().GetPlanID()); err != nil {
		return merr.WrapErrClusteringCompactionMetaError("SavePartitionStatsAndVersion", err)
	}

	if err = t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_completed)); err != nil {
		mlog.Warn(t.ctx, "completeTask update task state to completed failed", mlog.Err(err))
		return err
	}
	// mark input segments as dropped
	// now, the segment view only includes the result segments.
	if err = t.markInputSegmentsDropped(); err != nil {
		mlog.Warn(t.ctx, "mark input segments as Dropped failed, skip it and wait retry", mlog.Err(err))
	}

	return nil
}

func (t *clusteringCompactionTask) processAnalyzing() error {
	analyzeTask := t.meta.GetAnalyzeMeta().GetTask(t.GetTask().GetAnalyzeTaskID())
	if analyzeTask == nil {
		mlog.Warn(t.ctx, "analyzeTask not found", mlog.Int64("id", t.GetTask().GetAnalyzeTaskID()))
		return merr.WrapErrAnalyzeTaskNotFound(t.GetTask().GetAnalyzeTaskID())
	}
	mlog.Info(t.ctx, "check analyze task state", mlog.Int64("id", t.GetTask().GetAnalyzeTaskID()),
		mlog.Int64("version", analyzeTask.GetVersion()), mlog.String("state", analyzeTask.State.String()))
	switch analyzeTask.State {
	case indexpb.JobState_JobStateFinished:
		if analyzeTask.GetCentroidsFile() == "" {
			// fake finished vector clustering is not supported in opensource
			return merr.WrapErrClusteringCompactionNotSupportVector()
		} else {
			t.GetTask().AnalyzeVersion = analyzeTask.GetVersion()
			return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining))
		}
	case indexpb.JobState_JobStateFailed:
		mlog.Warn(t.ctx, "analyze task fail", mlog.Int64("analyzeID", t.GetTask().GetAnalyzeTaskID()))
		return merr.WrapErrServiceInternalMsg(analyzeTask.FailReason)
	default:
	}
	return nil
}

func (t *clusteringCompactionTask) resetSegmentCompacting() {
	t.meta.SetSegmentsCompacting(t.ctx, t.GetTask().GetInputSegments(), false)
}

func (t *clusteringCompactionTask) getCtx() context.Context {
	return t.ctx
}

func (t *clusteringCompactionTask) doClean() error {
	mlog.Info(t.ctx, "clean task", mlog.Int64("triggerID", t.GetTask().GetTriggerID()),
		mlog.String("state", t.GetTask().GetState().String()))

	if t.GetTask().GetState() == datapb.CompactionTaskState_completed {
		if err := t.markInputSegmentsDropped(); err != nil {
			return err
		}
	} else {
		isInputDropped := false
		for _, segID := range t.GetTask().GetInputSegments() {
			if t.meta.GetHealthySegment(t.ctx, segID) == nil {
				isInputDropped = true
				break
			}
		}
		if isInputDropped {
			mlog.Info(t.ctx, "input segments dropped, doing for compatibility",
				mlog.Int64("triggerID", t.GetTask().GetTriggerID()), mlog.Int64("planID", t.GetTask().GetPlanID()))
			// this task must be generated by v2.4, just for compatibility
			// revert segments meta
			var operators []UpdateOperator
			// revert level of input segments
			// L1 : L1 ->(process)-> L2 ->(clean)-> L1
			// L2 : L2 ->(process)-> L2 ->(clean)-> L2
			for _, segID := range t.GetTask().GetInputSegments() {
				operators = append(operators, RevertSegmentLevelOperator(segID))
			}
			// if result segments are generated but task fail in the other steps, mark them as L1 segments without partitions stats
			for _, segID := range t.GetTask().GetResultSegments() {
				operators = append(operators, UpdateSegmentLevelOperator(segID, datapb.SegmentLevel_L1))
				operators = append(operators, UpdateSegmentPartitionStatsVersionOperator(segID, 0))
			}
			for _, segID := range t.GetTask().GetTmpSegments() {
				// maybe no necessary, there will be no `TmpSegments` that task was generated by v2.4
				operators = append(operators, UpdateSegmentLevelOperator(segID, datapb.SegmentLevel_L1))
				operators = append(operators, UpdateSegmentPartitionStatsVersionOperator(segID, 0))
			}
			err := t.meta.UpdateSegmentsInfo(t.ctx, operators...)
			if err != nil {
				mlog.Warn(t.ctx, "UpdateSegmentsInfo fail", mlog.Err(err))
				return merr.WrapErrClusteringCompactionMetaError("UpdateSegmentsInfo", err)
			}
		} else {
			// after v2.5.0, mark the results segment as dropped
			var operators []UpdateOperator
			hasResultSegments := len(t.GetTask().GetResultSegments()) != 0
			if hasResultSegments {
				for _, segID := range t.GetTask().GetResultSegments() {
					// Don't worry about them being loaded; they are all invisible.
					operators = append(operators, UpdateStatusOperator(segID, commonpb.SegmentState_Dropped))
				}
			}

			for _, segID := range t.GetTask().GetTmpSegments() {
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
			err := t.meta.UpdateSegmentsInfo(t.ctx, operators...)
			if err != nil {
				mlog.Warn(t.ctx, "UpdateSegmentsInfo fail", mlog.Err(err))
				return merr.WrapErrClusteringCompactionMetaError("UpdateSegmentsInfo", err)
			}
		}

		// Drop the partition-stats and analyze metadata this attempt produced.
		// The objects themselves stay for garbage collection to reclaim once
		// nothing references them -- see meta.CleanPartitionStatsInfo.
		//
		// AnalyzeTaskID must be carried here. Without it the lookup inside
		// CleanPartitionStatsInfo resolves task 0, so DropAnalyzeTask removed
		// nothing and every failed clustering attempt leaked its analyze record
		// -- and with the record still in meta, recycleUnusedAnalyzeFiles keeps
		// the analyze files alive too.
		partitionStatsInfo := &datapb.PartitionStatsInfo{
			CollectionID:  t.GetTask().GetCollectionID(),
			PartitionID:   t.GetTask().GetPartitionID(),
			VChannel:      t.GetTask().GetChannel(),
			Version:       t.GetTask().GetPlanID(),
			SegmentIDs:    t.GetTask().GetResultSegments(),
			AnalyzeTaskID: t.GetTask().GetAnalyzeTaskID(),
		}
		err := t.meta.CleanPartitionStatsInfo(t.ctx, partitionStatsInfo)
		if err != nil {
			mlog.Warn(t.ctx, "gcPartitionStatsInfo fail", mlog.Err(err))
			return merr.WrapErrCleanPartitionStatsFail(fmt.Sprintf("%d-%d-%s-%d", t.GetTask().GetCollectionID(), t.GetTask().GetPartitionID(), t.GetTask().GetChannel(), t.GetTask().GetPlanID()))
		}
	}

	// finishClean writes cleaned and releases the inputs, with the release
	// deliberately last (see finishClean).
	return finishClean(t, "clusteringCompactionTask")
}

func (t *clusteringCompactionTask) doAnalyze() error {
	analyzeTask := &indexpb.AnalyzeTask{
		CollectionID: t.GetTask().GetCollectionID(),
		PartitionID:  t.GetTask().GetPartitionID(),
		FieldID:      t.GetTask().GetClusteringKeyField().FieldID,
		FieldName:    t.GetTask().GetClusteringKeyField().Name,
		FieldType:    t.GetTask().GetClusteringKeyField().DataType,
		SegmentIDs:   t.GetTask().GetInputSegments(),
		TaskID:       t.GetTask().GetAnalyzeTaskID(),
		State:        indexpb.JobState_JobStateInit,
	}
	err := t.meta.GetAnalyzeMeta().AddAnalyzeTask(analyzeTask)
	if err != nil {
		mlog.Warn(t.ctx, "failed to create analyze task", mlog.Int64("planID", t.GetTask().GetPlanID()), mlog.Err(err))
		return err
	}

	t.analyzeScheduler.Enqueue(newAnalyzeTask(proto.Clone(analyzeTask).(*indexpb.AnalyzeTask), t.meta.(*meta)))

	mlog.Info(t.ctx, "submit analyze task", mlog.Int64("planID", t.GetTask().GetPlanID()), mlog.Int64("triggerID", t.GetTask().GetTriggerID()), mlog.Int64("collectionID", t.GetTask().GetCollectionID()), mlog.Int64("id", t.GetTask().GetAnalyzeTaskID()))
	return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_analyzing))
}

func (t *clusteringCompactionTask) doCompact(nodeID int64, cluster session.Cluster) error {
	var err error
	t.plan, err = t.BuildCompactionRequest()
	if err != nil {
		mlog.Warn(t.ctx, "Failed to BuildCompactionRequest", mlog.Err(err))
		return err
	}
	// Persist the assignment, send the plan, and classify the outcome. See
	// dispatchCompactionPlan for the ordering and the outcome rules. Two
	// clustering executions overwrite the same partition-stats object, which
	// is keyed by planID alone, so the fresh-planID abandonment below matters
	// doubly here.
	return dispatchCompactionPlan(t.ctx, t, nodeID, cluster, t.GetPlan(), "clusteringCompactionTask")
}

func (t *clusteringCompactionTask) ShadowClone(opts ...compactionTaskOpt) *datapb.CompactionTask {
	taskClone := proto.Clone(t.GetTask()).(*datapb.CompactionTask)
	for _, opt := range opts {
		opt(taskClone)
	}
	return taskClone
}

func (t *clusteringCompactionTask) updateAndSaveTaskMeta(opts ...compactionTaskOpt) error {
	if err := updateAndSaveCompactionTaskMeta(t, opts...); err != nil {
		mlog.Warn(t.ctx, "Failed to saveTaskMeta", mlog.Err(err))
		return merr.WrapErrClusteringCompactionMetaError("updateAndSaveTaskMeta", err)
	}
	mlog.Info(t.ctx, "updateAndSaveTaskMeta success", mlog.String("task state", t.GetTask().GetState().String()))
	return nil
}

func (t *clusteringCompactionTask) saveTaskMeta(task *datapb.CompactionTask) error {
	return t.meta.SaveCompactionTask(t.ctx, task)
}

func (t *clusteringCompactionTask) SaveTaskMeta() error {
	return t.saveTaskMeta(t.GetTask())
}

func (t *clusteringCompactionTask) GetPlan() *datapb.CompactionPlan {
	return t.plan
}

func (t *clusteringCompactionTask) SetTask(task *datapb.CompactionTask) {
	t.taskProto.Store(task)
}

func (t *clusteringCompactionTask) GetLabel() string {
	return fmt.Sprintf("%d-%s", t.GetTask().PartitionID, t.GetTask().GetChannel())
}

func (t *clusteringCompactionTask) NeedReAssignNodeID() bool {
	return t.GetTask().GetState() == datapb.CompactionTaskState_pipelining && hasNoWorker(t)
}

func (t *clusteringCompactionTask) GetSlotUsage() int64 {
	return t.GetTaskSlot()
}
