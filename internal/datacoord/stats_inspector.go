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
	allocator           allocator.Allocator
	handler             Handler
	compactionInspector CompactionInspector
	ievm                IndexEngineVersionManager
}

func newStatsInspector(ctx context.Context,
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
		mt:                  mt,
		scheduler:           scheduler,
		allocator:           allocator,
		handler:             handler,
		compactionInspector: compactionInspector,
		ievm:                ievm,
	}
}

func (si *statsInspector) Start() {
	si.reloadFromMeta()
	si.loopWg.Add(2)
	go si.triggerStatsTaskLoop()
	go si.cleanupStatsTasksLoop()
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

	for {
		select {
		case <-si.ctx.Done():
			mlog.Warn(si.ctx, "DataCoord context done, exit checkStatsTaskLoop...")
			return
		case <-ticker.C:
			si.triggerTextStatsTask()
			si.triggerBM25StatsTask()
			si.triggerJSONKeyIndexStatsTask()
		}
	}
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

func (si *statsInspector) triggerTextStatsTask() {
	collections := si.mt.GetCollections()
	for _, collection := range collections {
		if collection == nil {
			continue
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
		allowUnsorted := collection.IsExternal()
		segments := si.mt.SelectSegments(si.ctx, WithCollection(collection.ID), SegmentFilterFunc(func(seg *SegmentInfo) bool {
			return needDoTextIndex(seg, needTriggerFieldIDs, allowUnsorted)
		}))

		resources := []*internalpb.FileResourceInfo{}
		var err error
		if fileresource.IsRefMode(paramtable.Get().CommonCfg.DNFileResourceMode.GetValue()) && len(collection.Schema.GetFileResourceIds()) > 0 {
			resources, err = si.mt.GetFileResources(si.ctx, collection.Schema.GetFileResourceIds()...)
			if err != nil {
				mlog.Warn(si.ctx, "get file resources for collection failed, wait for retry", mlog.FieldCollectionID(collection.ID), mlog.Err(err))
				continue
			}
		}

		for _, segment := range segments {
			if err := si.SubmitStatsTask(segment.GetID(), segment.GetID(), indexpb.StatsSubJob_TextIndexJob, true, resources); err != nil {
				mlog.Warn(si.ctx, "create stats task with text index for segment failed, wait for retry",
					mlog.FieldSegmentID(segment.GetID()), mlog.Err(err))
				continue
			}
		}
	}
}

func (si *statsInspector) triggerJSONKeyIndexStatsTask() {
	collections := si.mt.GetCollections()
	for _, collection := range collections {
		if collection == nil {
			continue
		}
		needTriggerFieldIDs := make([]UniqueID, 0)
		for _, field := range collection.Schema.GetFields() {
			h := typeutil.CreateFieldSchemaHelper(field)
			if h.EnableJSONKeyStatsIndex() && Params.CommonCfg.EnabledJSONKeyStats.GetAsBool() {
				needTriggerFieldIDs = append(needTriggerFieldIDs, field.GetFieldID())
			}
		}
		allowUnsorted := collection.IsExternal()
		segments := si.mt.SelectSegments(si.ctx, WithCollection(collection.ID), SegmentFilterFunc(func(seg *SegmentInfo) bool {
			if collection.IsExternal() && !canBuildExternalJSONKeyIndex(seg) {
				return false
			}
			return needDoJSONKeyIndex(seg, needTriggerFieldIDs, allowUnsorted)
		}))
		for _, segment := range segments {
			if err := si.SubmitStatsTask(segment.GetID(), segment.GetID(), indexpb.StatsSubJob_JsonKeyIndexJob, true, nil); err != nil {
				mlog.Warn(si.ctx, "create stats task with json key index for segment failed, wait for retry:",
					mlog.FieldSegmentID(segment.GetID()), mlog.Err(err))
				continue
			}
		}
	}
}

func (si *statsInspector) triggerBM25StatsTask() {
	collections := si.mt.GetCollections()
	for _, collection := range collections {
		if collection == nil || collection.IsExternal() {
			continue
		}
		needTriggerFieldIDs := make([]UniqueID, 0)
		for _, field := range collection.Schema.GetFields() {
			// TODO: docking bm25 stats task
			if si.enableBM25() {
				needTriggerFieldIDs = append(needTriggerFieldIDs, field.GetFieldID())
			}
		}
		segments := si.mt.SelectSegments(si.ctx, WithCollection(collection.ID), SegmentFilterFunc(func(seg *SegmentInfo) bool {
			return (seg.GetIsSorted() || seg.GetIsSortedByNamespace()) && needDoBM25(seg, needTriggerFieldIDs)
		}))

		for _, segment := range segments {
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
	pendingTaskCount := si.scheduler.GetPendingTaskCount()
	pendingTaskLimit := Params.DataCoordCfg.SortCompactionTriggerCount.GetAsInt()
	if pendingTaskCount > pendingTaskLimit {
		mlog.RatedInfo(si.ctx, rate.Limit(10), "skip submitting stats task because global scheduler has too many pending tasks",
			mlog.Int("pendingTaskCount", pendingTaskCount),
			mlog.Int("pendingTaskLimit", pendingTaskLimit),
			mlog.FieldSegmentID(originSegmentID),
			mlog.String("subJobType", subJobType.String()))
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

func (si *statsInspector) isExternalCollection(collectionID int64) bool {
	if si.mt == nil {
		return false
	}
	coll := si.mt.GetCollection(collectionID)
	return coll != nil && coll.IsExternal()
}
