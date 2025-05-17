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
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type StatsInspector interface {
	Start()
	Stop()
	SubmitStatsTask(originSegmentID, targetSegmentID int64, subJobType indexpb.StatsSubJob, canRecycle bool) error
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
	if Params.DataCoordCfg.EnableStatsTask.GetAsBool() {
		si.reloadFromMeta()
		si.loopWg.Add(2)
		go si.triggerStatsTaskLoop()
		go si.cleanupStatsTasksLoop()
	}
}

func (si *statsInspector) Stop() {
	si.cancel()
	si.loopWg.Wait()
}

func (si *statsInspector) reloadFromMeta() {
	tasks := si.mt.statsTaskMeta.GetAllTasks()
	for _, st := range tasks {
		if st.GetState() == indexpb.JobState_JobStateFinished ||
			st.GetState() == indexpb.JobState_JobStateFailed {
			continue
		}
		segment := si.mt.GetHealthySegment(si.ctx, st.GetSegmentID())
		taskSlot := int64(0)
		if segment != nil {
			taskSlot = calculateStatsTaskSlot(segment.getSegmentSize())
		}
		si.scheduler.Enqueue(newStatsTask(
			proto.Clone(st).(*indexpb.StatsTask),
			taskSlot,
			si.mt,
			si.compactionInspector,
			si.handler,
			si.allocator,
			si.ievm,
		))
	}
}

func (si *statsInspector) triggerStatsTaskLoop() {
	log.Info("start checkStatsTaskLoop...")
	defer si.loopWg.Done()

	ticker := time.NewTicker(Params.DataCoordCfg.TaskCheckInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-si.ctx.Done():
			log.Warn("DataCoord context done, exit checkStatsTaskLoop...")
			return
		case <-ticker.C:
			si.triggerSortStatsTask()
			si.triggerTextStatsTask()
			si.triggerBM25StatsTask()

		case segID := <-getStatsTaskChSingleton():
			log.Info("receive new segment to trigger stats task", zap.Int64("segmentID", segID))
			segment := si.mt.GetSegment(si.ctx, segID)
			if segment == nil {
				log.Warn("segment is not exist, no need to do stats task", zap.Int64("segmentID", segID))
				continue
			}
			si.createSortStatsTaskForSegment(segment)
		}
	}
}

func (si *statsInspector) triggerSortStatsTask() {
	invisibleSegments := si.mt.SelectSegments(si.ctx, SegmentFilterFunc(func(seg *SegmentInfo) bool {
		return isFlush(seg) && seg.GetLevel() != datapb.SegmentLevel_L0 && !seg.GetIsSorted() && !seg.GetIsImporting() && seg.GetIsInvisible()
	}))

	for _, seg := range invisibleSegments {
		si.createSortStatsTaskForSegment(seg)
	}

	visibleSegments := si.mt.SelectSegments(si.ctx, SegmentFilterFunc(func(seg *SegmentInfo) bool {
		return isFlush(seg) && seg.GetLevel() != datapb.SegmentLevel_L0 && !seg.GetIsSorted() && !seg.GetIsImporting() && !seg.GetIsInvisible()
	}))

	for _, segment := range visibleSegments {
		// TODO @xiaocai2333: add trigger count limit
		// if jm.scheduler.pendingTasks.TaskCount() > Params.DataCoordCfg.StatsTaskTriggerCount.GetAsInt() {
		// 	break
		// }
		si.createSortStatsTaskForSegment(segment)
	}
}

func (si *statsInspector) createSortStatsTaskForSegment(segment *SegmentInfo) {
	targetSegmentID, err := si.allocator.AllocID(si.ctx)
	if err != nil {
		log.Warn("allocID for segment stats task failed",
			zap.Int64("segmentID", segment.GetID()), zap.Error(err))
		return
	}
	if err := si.SubmitStatsTask(segment.GetID(), targetSegmentID, indexpb.StatsSubJob_Sort, true); err != nil {
		log.Warn("create stats task with sort for segment failed, wait for retry",
			zap.Int64("segmentID", segment.GetID()), zap.Error(err))
		return
	}
}

func (si *statsInspector) enableBM25() bool {
	return false
}

func needDoTextIndex(segment *SegmentInfo, fieldIDs []UniqueID) bool {
	if !(isFlush(segment) && segment.GetLevel() != datapb.SegmentLevel_L0 &&
		segment.GetIsSorted()) {
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

func needDoBM25(segment *SegmentInfo, fieldIDs []UniqueID) bool {
	// TODO: docking bm25 stats task
	return false
}

func (si *statsInspector) triggerTextStatsTask() {
	collections := si.mt.GetCollections()
	for _, collection := range collections {
		needTriggerFieldIDs := make([]UniqueID, 0)
		for _, field := range collection.Schema.GetFields() {
			// TODO @longjiquan: please replace it to fieldSchemaHelper.EnableMath
			h := typeutil.CreateFieldSchemaHelper(field)
			if !h.EnableMatch() {
				continue
			}
			needTriggerFieldIDs = append(needTriggerFieldIDs, field.GetFieldID())
		}
		segments := si.mt.SelectSegments(si.ctx, WithCollection(collection.ID), SegmentFilterFunc(func(seg *SegmentInfo) bool {
			return seg.GetIsSorted() && needDoTextIndex(seg, needTriggerFieldIDs)
		}))

		for _, segment := range segments {
			if err := si.SubmitStatsTask(segment.GetID(), segment.GetID(), indexpb.StatsSubJob_TextIndexJob, true); err != nil {
				log.Warn("create stats task with text index for segment failed, wait for retry",
					zap.Int64("segmentID", segment.GetID()), zap.Error(err))
				continue
			}
		}
	}
}

func (si *statsInspector) triggerBM25StatsTask() {
	collections := si.mt.GetCollections()
	for _, collection := range collections {
		needTriggerFieldIDs := make([]UniqueID, 0)
		for _, field := range collection.Schema.GetFields() {
			// TODO: docking bm25 stats task
			if si.enableBM25() {
				needTriggerFieldIDs = append(needTriggerFieldIDs, field.GetFieldID())
			}
		}
		segments := si.mt.SelectSegments(si.ctx, WithCollection(collection.ID), SegmentFilterFunc(func(seg *SegmentInfo) bool {
			return seg.GetIsSorted() && needDoBM25(seg, needTriggerFieldIDs)
		}))

		for _, segment := range segments {
			if err := si.SubmitStatsTask(segment.GetID(), segment.GetID(), indexpb.StatsSubJob_BM25Job, true); err != nil {
				log.Warn("create stats task with bm25 for segment failed, wait for retry",
					zap.Int64("segmentID", segment.GetID()), zap.Error(err))
				continue
			}
		}
	}
}

// cleanupStatsTasks clean up the finished/failed stats tasks
func (si *statsInspector) cleanupStatsTasksLoop() {
	log.Info("start cleanupStatsTasksLoop...")
	defer si.loopWg.Done()

	ticker := time.NewTicker(Params.DataCoordCfg.GCInterval.GetAsDuration(time.Second))
	defer ticker.Stop()

	for {
		select {
		case <-si.ctx.Done():
			log.Warn("DataCoord context done, exit cleanupStatsTasksLoop...")
			return
		case <-ticker.C:
			start := time.Now()
			log.Info("start cleanupUnusedStatsTasks...", zap.Time("startAt", start))

			taskIDs := si.mt.statsTaskMeta.CanCleanedTasks()
			for _, taskID := range taskIDs {
				if err := si.mt.statsTaskMeta.DropStatsTask(si.ctx, taskID); err != nil {
					// ignore err, if remove failed, wait next GC
					log.Warn("clean up stats task failed", zap.Int64("taskID", taskID), zap.Error(err))
				}
			}
			log.Info("cleanupUnusedStatsTasks done", zap.Duration("timeCost", time.Since(start)))
		}
	}
}

func (si *statsInspector) SubmitStatsTask(originSegmentID, targetSegmentID int64,
	subJobType indexpb.StatsSubJob, canRecycle bool,
) error {
	originSegment := si.mt.GetHealthySegment(si.ctx, originSegmentID)
	if originSegment == nil {
		return merr.WrapErrSegmentNotFound(originSegmentID)
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
	}
	if err = si.mt.statsTaskMeta.AddStatsTask(t); err != nil {
		if errors.Is(err, merr.ErrTaskDuplicate) {
			log.RatedInfo(10, "stats task already exists", zap.Int64("taskID", taskID),
				zap.Int64("collectionID", originSegment.GetCollectionID()),
				zap.Int64("segmentID", originSegment.GetID()))
			return nil
		}
		return err
	}
	si.scheduler.Enqueue(newStatsTask(proto.Clone(t).(*indexpb.StatsTask), taskSlot, si.mt, si.compactionInspector, si.handler, si.allocator, si.ievm))
	log.Ctx(si.ctx).Info("submit stats task success", zap.Int64("taskID", taskID),
		zap.String("subJobType", subJobType.String()),
		zap.Int64("collectionID", originSegment.GetCollectionID()),
		zap.Int64("originSegmentID", originSegmentID),
		zap.Int64("targetSegmentID", targetSegmentID), zap.Int64("taskSlot", taskSlot))
	return nil
}

func (si *statsInspector) GetStatsTask(originSegmentID int64, subJobType indexpb.StatsSubJob) *indexpb.StatsTask {
	task := si.mt.statsTaskMeta.GetStatsTaskBySegmentID(originSegmentID, subJobType)
	log.Info("statsJobManager get stats task state", zap.Int64("segmentID", originSegmentID),
		zap.String("subJobType", subJobType.String()), zap.String("state", task.GetState().String()),
		zap.String("failReason", task.GetFailReason()))
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

	log.Info("statsJobManager drop stats task success", zap.Int64("segmentID", originSegmentID),
		zap.Int64("taskID", task.GetTaskID()), zap.String("subJobType", subJobType.String()))
	return nil
}
