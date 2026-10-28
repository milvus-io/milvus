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
	"github.com/samber/lo"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	taskcommon "github.com/milvus-io/milvus/pkg/v3/taskcommon"
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

	notifyStatsChan     chan int64
	notifiedStatsSegsCh chan struct{}
	pendingStatsMu      sync.Mutex
	// notifiedStatsSegs holds the segments notified since the last round; a
	// notification round processes exactly these.
	notifiedStatsSegs typeutil.UniqueSet
	// pendingStatsSegs holds the segments to retry (startup scan, deferred or
	// failed attempts); only the ticker processes them.
	pendingStatsSegs typeutil.UniqueSet

	mt *meta

	scheduler           task.GlobalScheduler
	allocator           allocator.Allocator
	handler             Handler
	compactionInspector CompactionInspector
	ievm                IndexEngineVersionManager
}

func newStatsInspector(ctx context.Context,
	notifyStatsChan chan int64,
	mt *meta,
	scheduler task.GlobalScheduler,
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
		notifyStatsChan:     notifyStatsChan,
		notifiedStatsSegsCh: make(chan struct{}, 1),
		notifiedStatsSegs:   typeutil.NewUniqueSet(),
		pendingStatsSegs:    typeutil.NewUniqueSet(),
		mt:                  mt,
		scheduler:           scheduler,
		allocator:           allocator,
		handler:             handler,
		compactionInspector: compactionInspector,
		ievm:                ievm,
	}
}

// notifyStatsSegments queues segments for the next notification round.
func (si *statsInspector) notifyStatsSegments(segIDs ...UniqueID) {
	if len(segIDs) == 0 {
		return
	}

	si.pendingStatsMu.Lock()
	defer si.pendingStatsMu.Unlock()
	si.notifiedStatsSegs.Insert(segIDs...)
	select {
	case si.notifiedStatsSegsCh <- struct{}{}:
	default:
	}
}

// requeuePendingStatsSegments keeps segments for the ticker to retry.
func (si *statsInspector) requeuePendingStatsSegments(segIDs ...UniqueID) {
	si.pendingStatsMu.Lock()
	defer si.pendingStatsMu.Unlock()
	si.pendingStatsSegs.Insert(segIDs...)
}

// popStatsSegments takes the notified segments, plus the pending ones when
// withPending is set. A notified segment supersedes its pending retry.
func (si *statsInspector) popStatsSegments(withPending bool) []UniqueID {
	si.pendingStatsMu.Lock()
	segIDs := si.notifiedStatsSegs.Collect()
	si.notifiedStatsSegs = typeutil.NewUniqueSet()
	if !withPending {
		si.pendingStatsSegs.Remove(segIDs...)
		si.pendingStatsMu.Unlock()
		return segIDs
	}
	pending := si.pendingStatsSegs
	si.pendingStatsSegs = typeutil.NewUniqueSet()
	si.pendingStatsMu.Unlock()

	// The swapped-out backlog is owned here, so merge it outside the lock.
	pending.Insert(segIDs...)
	return pending.Collect()
}

func (si *statsInspector) Start() {
	// Become the statsBuildCh consumer before any producer starts.
	setStatsBuildConsumer(si.ctx.Done())
	si.loopWg.Add(1)
	go si.forwardStatsBuildCh(si.ctx)
	si.warnDeprecatedThrottleConfigs()
	si.reloadFromMeta()
	si.requeuePendingStatsSegments(si.getStatsCandidateSegments(si.ctx, si.mt.GetCollections()...)...)
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
	tasks := si.mt.statsTaskMeta.GetAllTasks()
	for _, st := range tasks {
		if st.GetState() != indexpb.JobState_JobStateInit &&
			st.GetState() != indexpb.JobState_JobStateRetry &&
			st.GetState() != indexpb.JobState_JobStateInProgress {
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
	reconcileTicker := time.NewTicker(getStatsReconcileInterval())
	defer reconcileTicker.Stop()

	for {
		select {
		case <-si.ctx.Done():
			mlog.Warn(si.ctx, "DataCoord context done, exit checkStatsTaskLoop...")
			return
		case <-ticker.C:
			si.processStatsSegments(si.ctx, true)
		case <-si.notifiedStatsSegsCh:
			si.processStatsSegments(si.ctx, false)
		case <-reconcileTicker.C:
			si.notifyStatsSegments(si.getStatsCandidateSegments(si.ctx, si.mt.GetCollections()...)...)
		case collectionID := <-si.notifyStatsChan:
			mlog.Info(si.ctx, "receive stats notify", mlog.FieldCollectionID(collectionID))
			if collection := si.mt.GetCollection(collectionID); collection != nil {
				si.notifyStatsSegments(si.getStatsCandidateSegments(si.ctx, collection)...)
			}
		}
	}
}

// forwardStatsBuildCh moves segments from statsBuildCh into the notified set.
// It runs apart from triggerStatsTaskLoop so that a sender waiting on a full
// channel never waits for a stats round to finish.
func (si *statsInspector) forwardStatsBuildCh(ctx context.Context) {
	defer si.loopWg.Done()
	ch := getStatsBuildChSingleton()
	for {
		select {
		case <-ctx.Done():
			return
		case segID := <-ch:
			// Take what is already buffered too, so a burst costs one log line
			// and one wake-up of the trigger loop.
			segIDs := []UniqueID{segID}
		drain:
			for len(segIDs) < cap(ch) {
				select {
				case segID = <-ch:
					segIDs = append(segIDs, segID)
				default:
					break drain
				}
			}
			mlog.Info(ctx, "receive segments to check stats", mlog.Int64s("segmentIDs", segIDs))
			si.notifyStatsSegments(segIDs...)
		}
	}
}

func getStatsReconcileInterval() time.Duration {
	checkInterval := Params.DataCoordCfg.TaskCheckInterval.GetAsDuration(time.Second)
	return checkInterval * 10
}

var statsSubJobs = []indexpb.StatsSubJob{indexpb.StatsSubJob_TextIndexJob, indexpb.StatsSubJob_JsonKeyIndexJob}

// statsFields lists the fields of a collection that need text or JSON key stats.
type statsFields struct {
	text, json []UniqueID
	external   bool
	staged     typeutil.UniqueSet
}

func getStatsFields(collection *collectionInfo) statsFields {
	fields := statsFields{external: collection.IsExternal()}
	jsonEnabled := Params.CommonCfg.EnabledJSONKeyStats.GetAsBool() && !jsonShreddingDisabledByDeprecatedConfig()
	for _, field := range collection.Schema.GetFields() {
		if !typeutil.IsStringType(field.GetDataType()) && !typeutil.IsJSONType(field.GetDataType()) {
			continue
		}
		h := typeutil.CreateFieldSchemaHelper(field)
		if h.EnableMatch() {
			fields.text = append(fields.text, field.GetFieldID())
		}
		if jsonEnabled && h.EnableJSONKeyStatsIndex() {
			fields.json = append(fields.json, field.GetFieldID())
		}
	}
	return fields
}

// segmentNeedsStats reports whether the segment itself lacks the stats of
// subJobType. It reads only the segment, so it may run under meta.segMu.
func segmentNeedsStats(segment *SegmentInfo, fields statsFields, subJobType indexpb.StatsSubJob) bool {
	allowUnsorted := fields.external || (!enableSortCompaction() && fields.staged.Contain(segment.GetID()))
	switch subJobType {
	case indexpb.StatsSubJob_TextIndexJob:
		return needDoTextIndex(segment, fields.text, allowUnsorted)
	case indexpb.StatsSubJob_JsonKeyIndexJob:
		return (!fields.external || canBuildExternalJSONKeyIndex(segment)) &&
			needDoJSONKeyIndex(segment, fields.json, allowUnsorted)
	default:
		return false
	}
}

// needStatsTask reports whether the segment needs a stats task of subJobType
// that is not in meta yet.
func (si *statsInspector) needStatsTask(segment *SegmentInfo, fields statsFields, subJobType indexpb.StatsSubJob) bool {
	return segmentNeedsStats(segment, fields, subJobType) &&
		!si.mt.statsTaskMeta.HasStatsTask(segment.GetID(), subJobType)
}

// getStatsCandidateSegments returns the IDs of the segments of the given
// collections that still miss a text or JSON key stats task.
func (si *statsInspector) getStatsCandidateSegments(ctx context.Context, collections ...*collectionInfo) []UniqueID {
	var segIDs []UniqueID
	for _, collection := range collections {
		fields := getStatsFields(collection)
		fields.staged = si.mt.stagedSegmentMembers(collection.ID)
		// No field to build stats for means no candidate segment, so skip the
		// collection before scanning all of its segments.
		if len(fields.text) == 0 && len(fields.json) == 0 {
			continue
		}
		// Only segment-local checks run under segMu; the task lookups follow.
		segments := si.mt.SelectSegments(ctx, WithCollection(collection.ID), SegmentFilterFunc(func(segment *SegmentInfo) bool {
			return lo.SomeBy(statsSubJobs, func(subJobType indexpb.StatsSubJob) bool {
				return segmentNeedsStats(segment, fields, subJobType)
			})
		}))
		for _, segment := range segments {
			if lo.SomeBy(statsSubJobs, func(subJobType indexpb.StatsSubJob) bool {
				return si.needStatsTask(segment, fields, subJobType)
			}) {
				segIDs = append(segIDs, segment.GetID())
			}
		}
	}
	return segIDs
}

func (si *statsInspector) processStatsSegments(ctx context.Context, withPending bool) {
	segIDs := si.popStatsSegments(withPending)
	fieldsByCollection := make(map[UniqueID]statsFields)
	resourcesByCollection := make(map[UniqueID]fileResourceLookup)
	for i, segID := range segIDs {
		segment := si.mt.GetHealthySegment(ctx, segID)
		if segment == nil {
			continue
		}
		collection := si.mt.GetCollection(segment.GetCollectionID())
		if collection == nil {
			continue
		}
		fields, ok := fieldsByCollection[collection.ID]
		if !ok {
			fields = getStatsFields(collection)
			fields.staged = si.mt.stagedSegmentMembers(collection.ID)
			fieldsByCollection[collection.ID] = fields
		}
		for _, subJobType := range statsSubJobs {
			if !si.needStatsTask(segment, fields, subJobType) {
				continue
			}
			// A backlogged scheduler ends the round instead of checking every
			// remaining segment; the next tick retries them.
			if !si.canSubmitStatsTask(subJobType) {
				si.requeuePendingStatsSegments(segIDs[i:]...)
				return
			}
			resources, err := si.getStatsTaskResources(ctx, collection, subJobType, resourcesByCollection)
			if err == nil {
				err = si.submitStatsTask(segment, segID, subJobType, true, resources)
			}
			if err != nil {
				mlog.RatedWarn(ctx, rate.Limit(1), "create stats task for segment failed, wait for retry",
					mlog.FieldSegmentID(segID), mlog.String("subJobType", subJobType.String()), mlog.Err(err))
				si.requeuePendingStatsSegments(segID)
			}
		}
	}
}

// fileResourceLookup is one collection's file resource lookup in a round.
type fileResourceLookup struct {
	resources []*internalpb.FileResourceInfo
	err       error
}

// getStatsTaskResources returns the file resources a stats task needs: only a
// text index task in ref mode needs any. A collection's lookup runs at most once
// per round, so a failing one is not repeated for each of its segments.
func (si *statsInspector) getStatsTaskResources(ctx context.Context, collection *collectionInfo,
	subJobType indexpb.StatsSubJob, cache map[UniqueID]fileResourceLookup,
) ([]*internalpb.FileResourceInfo, error) {
	if subJobType != indexpb.StatsSubJob_TextIndexJob ||
		!fileresource.IsRefMode(paramtable.Get().CommonCfg.DNFileResourceMode.GetValue()) ||
		len(collection.Schema.GetFileResourceIds()) == 0 {
		return nil, nil
	}
	lookup, ok := cache[collection.ID]
	if !ok {
		lookup.resources, lookup.err = si.mt.GetFileResources(ctx, collection.Schema.GetFileResourceIds()...)
		cache[collection.ID] = lookup
	}
	return lookup.resources, lookup.err
}

func needDoTextIndex(segment *SegmentInfo, fieldIDs []UniqueID, allowUnsorted bool) bool {
	if !isFlush(segment) || segment.GetLevel() == datapb.SegmentLevel_L0 {
		return false
	}
	if !allowUnsorted && !segment.GetIsSorted() && !segment.GetIsSortedByNamespace() {
		return false
	}

	return !hasRequiredTextStats(segment, fieldIDs)
}

func hasRequiredTextStats(segment *SegmentInfo, fieldIDs []UniqueID) bool {
	for _, fieldID := range fieldIDs {
		if segment.GetTextStatsLogs()[fieldID] == nil {
			return false
		}
	}
	return true
}

func needDoJSONKeyIndex(segment *SegmentInfo, fieldIDs []UniqueID, allowUnsorted bool) bool {
	if !isFlush(segment) || segment.GetLevel() == datapb.SegmentLevel_L0 {
		return false
	}
	if !allowUnsorted && !segment.GetIsSorted() && !segment.GetIsSortedByNamespace() {
		return false
	}

	return !hasRequiredJSONKeyStats(segment, fieldIDs)
}

func hasRequiredJSONKeyStats(segment *SegmentInfo, fieldIDs []UniqueID) bool {
	for _, fieldID := range fieldIDs {
		if segment.GetJsonKeyStats()[fieldID] == nil {
			return false
		}
		// if the data format version is less than the current version, we need to do the stats task again
		// because the data format is updated, the old data format need to be converted to the new data format
		if segment.GetJsonKeyStats()[fieldID].GetJsonKeyStatsDataFormat() < common.JSONStatsDataFormatVersion {
			return false
		}
	}
	return true
}

func canBuildExternalJSONKeyIndex(segment *SegmentInfo) bool {
	return segment.GetStorageVersion() == storage.StorageV3 && segment.GetManifestPath() != ""
}

// canSubmitStatsTask reports whether the global scheduler still has room for a
// new stats task. The pending queue is shared by every task type, so the count is
// scoped to stats work: an index or compaction backlog must not starve text-index
// and JSON-shredding submission. Stats tasks waiting on a retry backoff are
// counted, because they still occupy queue depth. A segment skipped here stays
// pending and is retried on the next TaskCheckInterval tick.
func (si *statsInspector) canSubmitStatsTask(subJobType indexpb.StatsSubJob) bool {
	pendingTaskCount := si.scheduler.GetPendingTaskCount(taskcommon.Stats)
	pendingTaskLimit := Params.DataCoordCfg.StatsTaskPendingLimit.GetAsInt()
	if pendingTaskCount > pendingTaskLimit {
		mlog.RatedInfo(si.ctx, rate.Limit(10), "skip submitting stats task because global scheduler has too many pending tasks",
			mlog.Int("pendingTaskCount", pendingTaskCount),
			mlog.Int("pendingTaskLimit", pendingTaskLimit),
			mlog.String("subJobType", subJobType.String()))
		return false
	}
	return true
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
				if err := si.mt.statsTaskMeta.DropStatsTask(si.ctx, taskID); err != nil {
					// ignore err, if remove failed, wait next GC
					mlog.Warn(si.ctx, "clean up stats task failed", mlog.FieldTaskID(taskID), mlog.Err(err))
				}
			}
			mlog.Info(si.ctx, "cleanupUnusedStatsTasks done", mlog.Duration("timeCost", time.Since(start)))
		}
	}
}

func (si *statsInspector) SubmitStatsTask(originSegmentID, targetSegmentID int64,
	subJobType indexpb.StatsSubJob, canRecycle bool,
	resources []*internalpb.FileResourceInfo,
) error {
	originSegment := si.mt.GetHealthySegment(si.ctx, originSegmentID)
	if originSegment == nil {
		return merr.WrapErrSegmentNotFound(originSegmentID)
	}
	if si.isExternalCollection(originSegment.GetCollectionID()) {
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
	if !si.canSubmitStatsTask(subJobType) {
		return nil
	}
	return si.submitStatsTask(originSegment, targetSegmentID, subJobType, canRecycle, resources)
}

// submitStatsTask allocates, persists and enqueues a stats task. The caller has
// already checked that the segment needs it and that the scheduler has room.
func (si *statsInspector) submitStatsTask(originSegment *SegmentInfo, targetSegmentID int64,
	subJobType indexpb.StatsSubJob, canRecycle bool,
	resources []*internalpb.FileResourceInfo,
) error {
	originSegmentID := originSegment.GetID()
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

func (si *statsInspector) isExternalCollection(collectionID int64) bool {
	if si.mt == nil {
		return false
	}
	coll := si.mt.GetCollection(collectionID)
	return coll != nil && coll.IsExternal()
}
