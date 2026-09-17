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
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type StatsInspector interface {
	Start()
	Stop()
	SubmitStatsTask(originSegmentID, targetSegmentID int64, subJobType indexpb.StatsSubJob, canRecycle bool, resources []*internalpb.FileResourceInfo) error
	GetStatsTask(originSegmentID int64, subJobType indexpb.StatsSubJob) *indexpb.StatsTask
	DropStatsTask(originSegmentID int64, subJobType indexpb.StatsSubJob) error
}

var _ StatsInspector = (*statsInspector)(nil)

type statsInspector struct {
	ctx    context.Context
	cancel context.CancelFunc

	loopWg sync.WaitGroup

	mt *meta

	scheduler           task.GlobalScheduler
	cluster             session.Cluster
	allocator           allocator.Allocator
	handler             Handler
	compactionInspector CompactionInspector
	ievm                IndexEngineVersionManager
}

func newStatsInspector(ctx context.Context,
	mt *meta,
	scheduler task.GlobalScheduler,
	cluster session.Cluster,
	allocator allocator.Allocator,
	handler Handler,
	compactionInspector CompactionInspector,
	ievm IndexEngineVersionManager,
) *statsInspector {
	ctx, cancel := context.WithCancel(ctx)
	return &statsInspector{
		ctx:                 ctx,
		cancel:              cancel,
		loopWg:              sync.WaitGroup{},
		mt:                  mt,
		scheduler:           scheduler,
		cluster:             cluster,
		allocator:           allocator,
		handler:             handler,
		compactionInspector: compactionInspector,
		ievm:                ievm,
	}
}

func (si *statsInspector) Start() {
	si.warnDeprecatedThrottleConfigs()
	si.reloadFromMeta()
	si.loopWg.Add(2)
	go si.triggerStatsTaskLoop()
	go si.cleanupStatsTasksLoop()
}

// warnDeprecatedThrottleConfigs tells operators whose config still carries the
// old JSON throttle that it no longer has any effect, instead of letting the
// setting disappear silently on upgrade.
func (si *statsInspector) warnDeprecatedThrottleConfigs() {
	for _, item := range []*paramtable.ParamItem{
		&Params.DataCoordCfg.JSONStatsTriggerCount,
		&Params.DataCoordCfg.JSONStatsTriggerInterval,
	} {
		if item.GetValue() == item.DefaultValue {
			continue
		}
		mlog.Warn(si.ctx, "deprecated config is set and no longer throttles stats tasks, use dataCoord.statsTaskPendingLimit instead",
			mlog.String("key", item.Key),
			mlog.String("value", item.GetValue()))
	}
	if jsonShreddingDisabledByDeprecatedConfig() {
		mlog.Warn(si.ctx, "dataCoord.jsonShreddingTriggerCount is 0, keeping JSON key index submission disabled for compatibility",
			mlog.String("suggestion", "set common.enabledJSONShredding to false instead"))
	}
}

// jsonShreddingDisabledByDeprecatedConfig reports whether the deprecated
// jsonShreddingTriggerCount is still being used as a kill switch. The removed
// limiter broke out of the loop once the submitted count reached the configured
// value, so 0 disabled JSON key-index submission on the very first segment.
// Silently re-enabling shredding for an operator who had set 0 would undo a
// deliberate decision, so that one value keeps its meaning.
func jsonShreddingDisabledByDeprecatedConfig() bool {
	return Params.DataCoordCfg.JSONStatsTriggerCount.GetAsInt() == 0
}

func (si *statsInspector) Stop() {
	si.cancel()
	si.loopWg.Wait()
}

func (si *statsInspector) reloadFromMeta() {
	// Startup resumes dispatchable/assigned work. Retry waits for the first
	// TaskCheckInterval tick so the business owner, not scheduler startup, owns
	// the retry cadence.
	si.enqueueActiveTasks(false)
}

// enqueueActiveTasks is the stats owner's scheduling and retry loop. A Retry
// record is replaced under a fresh task ID only here; offering Init/InProgress
// every round also recovers wrappers released after a local persistence/RPC
// failure. Scheduler Enqueue is idempotent while it still owns the current
// wrapper.
func (si *statsInspector) enqueueActiveTasks(retry bool) {
	tasks := si.mt.statsTaskMeta.GetAllTasks()
	for _, st := range tasks {
		if st.GetState() == indexpb.JobState_JobStateRetry {
			if !retry {
				continue
			}
			newTaskID, err := si.allocator.AllocID(si.ctx)
			if err != nil {
				mlog.Warn(si.ctx, "failed to allocate replacement stats task ID",
					mlog.FieldTaskID(st.GetTaskID()), mlog.Err(err))
				continue
			}
			replacement, replaced, err := si.mt.statsTaskMeta.ReplaceRetryTask(si.ctx, st.GetTaskID(), newTaskID)
			if err != nil {
				// A failed transaction response is ambiguous: it may already have
				// replaced the catalog record. Restart and reload catalog before
				// attempting another replacement.
				if si.ctx.Err() == nil {
					mlog.Fatal(si.ctx, "stats retry task replacement failed; terminating process", mlog.Err(err))
				}
				continue
			}
			if !replaced {
				continue
			}
			st = replacement
		}
		if st == nil || (st.GetState() != indexpb.JobState_JobStateInit &&
			st.GetState() != indexpb.JobState_JobStateInProgress) {
			continue
		}
		taskSlot := int64(0)
		segment := si.mt.GetHealthySegment(si.ctx, st.GetSegmentID())
		if segment != nil {
			taskSlot = calculateStatsTaskSlot(segment.getSegmentSize())
		}
		si.scheduler.Enqueue(newStatsTask(
			proto.Clone(st).(*indexpb.StatsTask),
			taskSlot,
			si.mt,
			si.handler,
			si.allocator,
			si.ievm,
		))
	}
}

func (si *statsInspector) triggerStatsTaskLoop() {
	mlog.Info(si.ctx, "start checkStatsTaskLoop...")
	defer si.loopWg.Done()

	ticker := time.NewTicker(Params.DataCoordCfg.TaskCheckInterval.GetAsDuration(time.Second))
	defer ticker.Stop()

	round := 0
	for {
		select {
		case <-si.ctx.Done():
			mlog.Warn(si.ctx, "DataCoord context done, exit checkStatsTaskLoop...")
			return
		case <-ticker.C:
			si.enqueueActiveTasks(true)
			si.triggerStatsTasks(round)
			round++
		}
	}
}

// triggerStatsTasks runs one discovery round. The sub-jobs share a single
// admission budget and each trigger returns as soon as it is refused, so
// whichever runs first claims the capacity. Alternate text and JSON per round,
// otherwise a long text-index backlog starves JSON shredding for as long as it
// takes to drain - days on a large collection.
func (si *statsInspector) triggerStatsTasks(round int) {
	collections := si.collectionsWithSegments()
	if round%2 == 0 {
		si.triggerTextStatsTaskForCollections(collections)
		si.triggerJSONKeyIndexStatsTaskForCollections(collections)
	} else {
		si.triggerJSONKeyIndexStatsTaskForCollections(collections)
		si.triggerTextStatsTaskForCollections(collections)
	}
	si.triggerBM25StatsTaskForCollections(collections)
}

// collectionsWithSegments resolves collection metadata only for collection IDs
// present in DataCoord's authoritative segment metadata. The returned slice is
// a per-discovery-round snapshot; it is not retained as a cache.
func (si *statsInspector) collectionsWithSegments() []*collectionInfo {
	collections := make([]*collectionInfo, 0)
	for collectionID := range si.mt.GetAllCollectionNumRows() {
		collection, err := si.handler.GetCollection(si.ctx, collectionID)
		if err != nil || collection == nil {
			mlog.Warn(si.ctx, "failed to get collection while discovering stats tasks",
				mlog.FieldCollectionID(collectionID), mlog.Err(err))
			continue
		}
		collections = append(collections, collection)
	}
	return collections
}

func (si *statsInspector) enableBM25() bool {
	return false
}

func needDoTextIndex(segment *SegmentInfo, fieldIDs []UniqueID, allowUnsorted bool) bool {
	if !isFlush(segment) || segment.GetLevel() == datapb.SegmentLevel_L0 {
		return false
	}
	if !allowUnsorted && !segment.GetIsSorted() && !segment.GetIsSortedByNamespace() {
		return false
	}

	for _, fieldID := range fieldIDs {
		if segment.GetTextStatsLogs() == nil {
			return true
		}
		if segment.GetTextStatsLogs()[fieldID] == nil {
			return true
		}
	}
	return false
}

func needDoJSONKeyIndex(segment *SegmentInfo, fieldIDs []UniqueID, allowUnsorted bool) bool {
	if !isFlush(segment) || segment.GetLevel() == datapb.SegmentLevel_L0 {
		return false
	}
	if !allowUnsorted && !segment.GetIsSorted() && !segment.GetIsSortedByNamespace() {
		return false
	}

	for _, fieldID := range fieldIDs {
		if segment.GetJsonKeyStats() == nil {
			return true
		}
		if segment.GetJsonKeyStats()[fieldID] == nil {
			return true
		}
		// if the data format version is less than the current version, we need to do the stats task again
		// because the data format is updated, the old data format need to be converted to the new data format
		if segment.GetJsonKeyStats()[fieldID].GetJsonKeyStatsDataFormat() < common.JSONStatsDataFormatVersion {
			return true
		}
	}
	return false
}

func canBuildExternalJSONKeyIndex(segment *SegmentInfo) bool {
	return segment.GetStorageVersion() == storage.StorageV3 && segment.GetManifestPath() != ""
}

func needDoBM25(segment *SegmentInfo, fieldIDs []UniqueID) bool {
	// TODO: docking bm25 stats task
	return false
}

// canSubmitStatsTask reports whether the persisted stats-task backlog still has
// room for a new task. Init and Retry records are durable work debt even when the
// scheduler no longer owns a wrapper for them. Discovery re-runs on every
// TaskCheckInterval tick, so a segment skipped here is picked up again once the
// stats backlog drains.
func (si *statsInspector) canSubmitStatsTask(subJobType indexpb.StatsSubJob) bool {
	pendingTaskCount := si.mt.statsTaskMeta.GetPendingTaskCount()
	pendingTaskLimit := Params.DataCoordCfg.StatsTaskPendingLimit.GetAsInt()
	if pendingTaskCount >= pendingTaskLimit {
		mlog.RatedInfo(si.ctx, rate.Limit(10), "skip submitting stats task because stats meta reached the pending task limit",
			mlog.Int("pendingTaskCount", pendingTaskCount),
			mlog.Int("pendingTaskLimit", pendingTaskLimit),
			mlog.String("subJobType", subJobType.String()))
		return false
	}
	return true
}

func (si *statsInspector) triggerTextStatsTask() {
	si.triggerTextStatsTaskForCollections(si.collectionsWithSegments())
}

func (si *statsInspector) triggerTextStatsTaskForCollections(collections []*collectionInfo) {
	for _, collection := range collections {
		if collection == nil || collection.Schema == nil {
			continue
		}
		if !si.canSubmitStatsTask(indexpb.StatsSubJob_TextIndexJob) {
			return
		}
		needTriggerFieldIDs := make([]UniqueID, 0)
		for _, field := range collection.Schema.GetFields() {
			// TODO @longjiquan: please replace it to fieldSchemaHelper.EnableMath
			h := typeutil.CreateFieldSchemaHelper(field)
			if !h.EnableMatch() {
				continue
			}
			needTriggerFieldIDs = append(needTriggerFieldIDs, field.GetFieldID())
		}
		// needDoTextIndex is false for every segment once there is no field to
		// index, so skip the collection before scanning all of its segments.
		if len(needTriggerFieldIDs) == 0 {
			continue
		}
		allowUnsorted := collection.IsExternal()
		segments := si.mt.SelectSegments(si.ctx, WithCollection(collection.ID), SegmentFilterFunc(func(seg *SegmentInfo) bool {
			if !needDoTextIndex(seg, needTriggerFieldIDs, allowUnsorted) {
				return false
			}
			// A segment whose task is already in meta must not be re-submitted;
			// filtering it out here keeps the per-tick work proportional to the
			// segments that still need a task instead of to all of them.
			// Note this runs under meta.segMu.RLock, so keep it to a map read.
			return !si.mt.statsTaskMeta.HasStatsTask(seg.GetID(), indexpb.StatsSubJob_TextIndexJob)
		}))

		resources := []*internalpb.FileResourceInfo{}
		var err error
		if fileresource.IsRefMode(paramtable.Get().CommonCfg.DNFileResourceMode.GetValue()) &&
			len(collection.Schema.GetFileResourceIds()) > 0 {
			resources, err = si.mt.GetFileResources(si.ctx, collection.Schema.GetFileResourceIds()...)
			if err != nil {
				mlog.Warn(si.ctx, "get file resources for collection failed, wait for retry", mlog.FieldCollectionID(collection.ID), mlog.Err(err))
				continue
			}
		}

		for _, segment := range segments {
			if !si.canSubmitStatsTask(indexpb.StatsSubJob_TextIndexJob) {
				return
			}
			if err := si.SubmitStatsTask(segment.GetID(), segment.GetID(), indexpb.StatsSubJob_TextIndexJob, true, resources); err != nil {
				mlog.Warn(si.ctx, "create stats task with text index for segment failed, wait for retry",
					mlog.FieldSegmentID(segment.GetID()), mlog.Err(err))
				continue
			}
		}
	}
}

func (si *statsInspector) triggerJSONKeyIndexStatsTask() {
	si.triggerJSONKeyIndexStatsTaskForCollections(si.collectionsWithSegments())
}

func (si *statsInspector) triggerJSONKeyIndexStatsTaskForCollections(collections []*collectionInfo) {
	if jsonShreddingDisabledByDeprecatedConfig() {
		mlog.RatedWarn(si.ctx, rate.Limit(0.1), "skip JSON key index stats task, dataCoord.jsonShreddingTriggerCount is set to 0",
			mlog.String("suggestion", "set common.enabledJSONShredding to false instead"))
		return
	}
	for _, collection := range collections {
		if collection == nil || collection.Schema == nil {
			continue
		}
		if !si.canSubmitStatsTask(indexpb.StatsSubJob_JsonKeyIndexJob) {
			return
		}
		needTriggerFieldIDs := make([]UniqueID, 0)
		for _, field := range collection.Schema.GetFields() {
			h := typeutil.CreateFieldSchemaHelper(field)
			if h.EnableJSONKeyStatsIndex() && Params.CommonCfg.EnabledJSONKeyStats.GetAsBool() {
				needTriggerFieldIDs = append(needTriggerFieldIDs, field.GetFieldID())
			}
		}
		// Same as the text loop: no field to shred means no candidate segment,
		// which also short-circuits every collection once JSON shredding is off.
		if len(needTriggerFieldIDs) == 0 {
			continue
		}
		allowUnsorted := collection.IsExternal()
		segments := si.mt.SelectSegments(si.ctx, WithCollection(collection.ID), SegmentFilterFunc(func(seg *SegmentInfo) bool {
			if collection.IsExternal() && !canBuildExternalJSONKeyIndex(seg) {
				return false
			}
			if !needDoJSONKeyIndex(seg, needTriggerFieldIDs, allowUnsorted) {
				return false
			}
			return !si.mt.statsTaskMeta.HasStatsTask(seg.GetID(), indexpb.StatsSubJob_JsonKeyIndexJob)
		}))
		for _, segment := range segments {
			if !si.canSubmitStatsTask(indexpb.StatsSubJob_JsonKeyIndexJob) {
				return
			}
			if err := si.SubmitStatsTask(segment.GetID(), segment.GetID(), indexpb.StatsSubJob_JsonKeyIndexJob, true, nil); err != nil {
				mlog.Warn(si.ctx, "create stats task with json key index for segment failed, wait for retry:",
					mlog.FieldSegmentID(segment.GetID()), mlog.Err(err))
				continue
			}
		}
	}
}

func (si *statsInspector) triggerBM25StatsTask() {
	si.triggerBM25StatsTaskForCollections(si.collectionsWithSegments())
}

func (si *statsInspector) triggerBM25StatsTaskForCollections(collections []*collectionInfo) {
	// BM25 stats tasks are not docked yet, so every collection would be scanned
	// for nothing. Drop out before touching the segment meta at all.
	if !si.enableBM25() {
		return
	}
	for _, collection := range collections {
		if collection == nil || collection.Schema == nil || collection.IsExternal() {
			continue
		}
		if !si.canSubmitStatsTask(indexpb.StatsSubJob_BM25Job) {
			return
		}
		needTriggerFieldIDs := make([]UniqueID, 0)
		for _, field := range collection.Schema.GetFields() {
			// TODO: docking bm25 stats task
			if si.enableBM25() {
				needTriggerFieldIDs = append(needTriggerFieldIDs, field.GetFieldID())
			}
		}
		segments := si.mt.SelectSegments(si.ctx, WithCollection(collection.ID), SegmentFilterFunc(func(seg *SegmentInfo) bool {
			if !seg.GetIsSorted() && !seg.GetIsSortedByNamespace() {
				return false
			}
			if !needDoBM25(seg, needTriggerFieldIDs) {
				return false
			}
			return !si.mt.statsTaskMeta.HasStatsTask(seg.GetID(), indexpb.StatsSubJob_BM25Job)
		}))

		for _, segment := range segments {
			if !si.canSubmitStatsTask(indexpb.StatsSubJob_BM25Job) {
				return
			}
			if err := si.SubmitStatsTask(segment.GetID(), segment.GetID(), indexpb.StatsSubJob_BM25Job, true, nil); err != nil {
				mlog.Warn(si.ctx, "create stats task with bm25 for segment failed, wait for retry",
					mlog.FieldSegmentID(segment.GetID()), mlog.Err(err))
				continue
			}
		}
	}
}

// cleanupStatsTasks clean up the finished/failed stats tasks
func (si *statsInspector) cleanupStatsTasksLoop() {
	mlog.Info(si.ctx, "start cleanupStatsTasksLoop...")
	defer si.loopWg.Done()

	ticker := time.NewTicker(Params.DataCoordCfg.GCInterval.GetAsDuration(time.Second))
	defer ticker.Stop()

	for {
		select {
		case <-si.ctx.Done():
			mlog.Warn(si.ctx, "DataCoord context done, exit cleanupStatsTasksLoop...")
			return
		case <-ticker.C:
			start := time.Now()
			mlog.Info(si.ctx, "start cleanupUnusedStatsTasks...", mlog.Time("startAt", start))

			taskIDs := si.mt.statsTaskMeta.CanCleanedTasks()
			for _, taskID := range taskIDs {
				if err := si.cleanupStatsTask(taskID); err != nil {
					// ignore err, if remove failed, wait next GC
					mlog.Warn(si.ctx, "clean up stats task failed", mlog.FieldTaskID(taskID), mlog.Err(err))
				}
			}
			mlog.Info(si.ctx, "cleanupUnusedStatsTasks done", mlog.Duration("timeCost", time.Since(start)))
		}
	}
}

func (si *statsInspector) cleanupStatsTask(taskID int64) error {
	var retErr error
	cleanup := func() {
		// Resolve worker ownership only after the scheduler callback drains.
		// Without this fence, an in-flight QueryTaskOnWorker for the same task
		// can commit or resurrect state after the meta row below is gone
		// (e.g. pushing a Finished task back to Retry on a query error).
		latest := si.mt.statsTaskMeta.GetStatsTask(taskID)
		if latest == nil {
			return
		}
		if latest.GetNodeID() > 0 {
			if si.cluster == nil {
				retErr = merr.WrapErrServiceNotReadyMsg("stats worker cluster is unavailable")
				return
			}
			if err := si.cluster.DropStats(latest.GetNodeID(), taskID); err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
				retErr = err
				return
			}
		}
		retErr = si.mt.statsTaskMeta.DropStatsTask(si.ctx, taskID)
	}
	if si.scheduler == nil {
		// Tests that do not exercise scheduler concurrency use the direct path.
		cleanup()
	} else {
		// Release scheduler ownership before terminal cleanup so a late worker
		// callback finds the task gone and gives up instead of racing this
		// drop and meta removal. This mirrors copy/import/external GC.
		si.scheduler.Finalize(taskID, cleanup)
	}
	return retErr
}

func (si *statsInspector) SubmitStatsTask(originSegmentID, targetSegmentID int64,
	subJobType indexpb.StatsSubJob, canRecycle bool,
	resources []*internalpb.FileResourceInfo,
) error {
	originSegment := si.mt.GetHealthySegment(si.ctx, originSegmentID)
	if originSegment == nil {
		return merr.WrapErrSegmentNotFound(originSegmentID)
	}
	isExternal, err := si.isExternalCollection(si.ctx, originSegment.GetCollectionID())
	if err != nil {
		return err
	}
	if isExternal {
		if subJobType == indexpb.StatsSubJob_JsonKeyIndexJob && !canBuildExternalJSONKeyIndex(originSegment) {
			mlog.Info(si.ctx,
				"skip submit external json stats task without v3 manifest",
				mlog.FieldCollectionID(originSegment.GetCollectionID()),
				mlog.FieldSegmentID(originSegmentID))
			return nil
		}
		if subJobType != indexpb.StatsSubJob_TextIndexJob &&
			subJobType != indexpb.StatsSubJob_JsonKeyIndexJob {
			mlog.Info(si.ctx,
				"skip submit stats task for external collection",
				mlog.FieldCollectionID(originSegment.GetCollectionID()),
				mlog.FieldSegmentID(originSegmentID),
				mlog.String("subJobType", subJobType.String()))
			return nil
		}
	}
	if si.mt.statsTaskMeta.HasStatsTask(originSegmentID, subJobType) {
		mlog.RatedInfo(si.ctx, rate.Limit(10), "stats task already exists",
			mlog.FieldCollectionID(originSegment.GetCollectionID()),
			mlog.FieldSegmentID(originSegmentID),
			mlog.String("subJobType", subJobType.String()))
		return nil
	}
	// The trigger loops check admission before getting here; this guard covers
	// callers that reach the StatsInspector interface directly.
	if !si.canSubmitStatsTask(subJobType) {
		return nil
	}
	taskID, err := si.allocator.AllocID(context.Background())
	if err != nil {
		return err
	}
	originSegmentSize := originSegment.getSegmentSize()
	if subJobType == indexpb.StatsSubJob_JsonKeyIndexJob {
		originSegmentSize = originSegment.getSegmentSize() * 2
	}

	taskSlot := calculateStatsTaskSlot(originSegmentSize)
	t := &indexpb.StatsTask{
		CollectionID:    originSegment.GetCollectionID(),
		PartitionID:     originSegment.GetPartitionID(),
		SegmentID:       originSegmentID,
		InsertChannel:   originSegment.GetInsertChannel(),
		TaskID:          taskID,
		Version:         0,
		NodeID:          0,
		State:           indexpb.JobState_JobStateInit,
		FailReason:      "",
		TargetSegmentID: targetSegmentID,
		SubJobType:      subJobType,
		CanRecycle:      canRecycle,
		FileResources:   resources,
	}
	if err = si.mt.statsTaskMeta.AddStatsTask(t); err != nil {
		if errors.Is(err, merr.ErrTaskDuplicate) {
			mlog.RatedInfo(si.ctx, rate.Limit(10), "stats task already exists", mlog.FieldTaskID(taskID),
				mlog.FieldCollectionID(originSegment.GetCollectionID()),
				mlog.FieldSegmentID(originSegment.GetID()))
			return nil
		}
		return err
	}
	si.scheduler.Enqueue(newStatsTask(proto.Clone(t).(*indexpb.StatsTask), taskSlot, si.mt, si.handler, si.allocator, si.ievm))
	mlog.Info(si.ctx,
		"submit stats task success", mlog.FieldTaskID(taskID),
		mlog.String("subJobType", subJobType.String()),
		mlog.FieldCollectionID(originSegment.GetCollectionID()),
		mlog.Int64("originSegmentID", originSegmentID),
		mlog.Int64("targetSegmentID", targetSegmentID), mlog.Int64("taskSlot", taskSlot))
	return nil
}

func (si *statsInspector) GetStatsTask(originSegmentID int64, subJobType indexpb.StatsSubJob) *indexpb.StatsTask {
	task := si.mt.statsTaskMeta.GetStatsTaskBySegmentID(originSegmentID, subJobType)
	mlog.Info(si.ctx, "statsJobManager get stats task state", mlog.FieldSegmentID(originSegmentID),
		mlog.String("subJobType", subJobType.String()), mlog.String("state", task.GetState().String()),
		mlog.String("failReason", task.GetFailReason()))
	return task
}

func (si *statsInspector) DropStatsTask(originSegmentID int64, subJobType indexpb.StatsSubJob) error {
	task := si.mt.statsTaskMeta.GetStatsTaskBySegmentID(originSegmentID, subJobType)
	if task == nil {
		return nil
	}
	si.scheduler.AbortAndRemoveTask(task.GetTaskID())
	if err := si.mt.statsTaskMeta.MarkTaskCanRecycle(task.GetTaskID()); err != nil {
		return err
	}

	mlog.Info(si.ctx, "statsJobManager drop stats task success", mlog.FieldSegmentID(originSegmentID),
		mlog.FieldTaskID(task.GetTaskID()), mlog.String("subJobType", subJobType.String()))
	return nil
}

func (si *statsInspector) isExternalCollection(ctx context.Context, collectionID int64) (bool, error) {
	coll, err := si.handler.GetCollection(ctx, collectionID)
	if err != nil {
		return false, err
	}
	return coll != nil && coll.IsExternal(), nil
}
