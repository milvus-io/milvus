package datacoord

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type StatsJobManager interface {
	Start()
	Stop()
	SubmitStatsTask(originSegmentID, targetSegmentID int64, subJobType indexpb.StatsSubJob, canRecycle bool) error
	GetStatsTask(originSegmentID int64, subJobType indexpb.StatsSubJob) *indexpb.StatsTask
	DropStatsTask(originSegmentID int64, subJobType indexpb.StatsSubJob) error
}

var _ StatsJobManager = (*statsJobManager)(nil)

type statsJobManager struct {
	ctx    context.Context
	cancel context.CancelFunc

	loopWg sync.WaitGroup

	mt *meta

	scheduler *taskScheduler
	allocator allocator.Allocator
}

func newJobManager(ctx context.Context,
	mt *meta,
	scheduler *taskScheduler,
	allocator allocator.Allocator,
) *statsJobManager {
	ctx, cancel := context.WithCancel(ctx)
	return &statsJobManager{
		ctx:       ctx,
		cancel:    cancel,
		loopWg:    sync.WaitGroup{},
		mt:        mt,
		scheduler: scheduler,
		allocator: allocator,
	}
}

func (jm *statsJobManager) Start() {
	if Params.DataCoordCfg.EnableStatsTask.GetAsBool() {
		jm.loopWg.Add(2)
		go jm.triggerStatsTaskLoop()
		go jm.cleanupStatsTasksLoop()
	}
}

func (jm *statsJobManager) Stop() {
	jm.cancel()
	jm.loopWg.Wait()
}

func (jm *statsJobManager) triggerStatsTaskLoop() {
	log.Info("start checkStatsTaskLoop...")
	defer jm.loopWg.Done()

	ticker := time.NewTicker(Params.DataCoordCfg.TaskCheckInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-jm.ctx.Done():
			log.Warn("DataCoord context done, exit checkStatsTaskLoop...")
			return
		case <-ticker.C:
			jm.triggerSortStatsTask()
			jm.triggerTextStatsTask()
			jm.triggerBM25StatsTask()
			jm.triggerJsonKeyIndexStatsTask()

		case segID := <-getStatsTaskChSingleton():
			log.Info("receive new segment to trigger stats task", zap.Int64("segmentID", segID))
			segment := jm.mt.GetSegment(jm.ctx, segID)
			if segment == nil {
				log.Warn("segment is not exist, no need to do stats task", zap.Int64("segmentID", segID))
				continue
			}
			jm.createSortStatsTaskForSegment(segment)
		}
	}
}

func (jm *statsJobManager) triggerSortStatsTask() {
	invisibleSegments := jm.mt.SelectSegments(jm.ctx, SegmentFilterFunc(func(seg *SegmentInfo) bool {
		return isFlush(seg) && seg.GetLevel() != datapb.SegmentLevel_L0 && !seg.GetIsSorted() && !seg.GetIsImporting() && seg.GetIsInvisible()
	}))

	for _, seg := range invisibleSegments {
		jm.createSortStatsTaskForSegment(seg)
	}

	visibleSegments := jm.mt.SelectSegments(jm.ctx, SegmentFilterFunc(func(seg *SegmentInfo) bool {
		return isFlush(seg) && seg.GetLevel() != datapb.SegmentLevel_L0 && !seg.GetIsSorted() && !seg.GetIsImporting() && !seg.GetIsInvisible()
	}))

	for _, segment := range visibleSegments {
		if jm.scheduler.pendingTasks.TaskCount() > Params.DataCoordCfg.StatsTaskTriggerCount.GetAsInt() {
			break
		}
		jm.createSortStatsTaskForSegment(segment)
	}
}

func (jm *statsJobManager) createSortStatsTaskForSegment(segment *SegmentInfo) {
	targetSegmentID, err := jm.allocator.AllocID(jm.ctx)
	if err != nil {
		log.Warn("allocID for segment stats task failed",
			zap.Int64("segmentID", segment.GetID()), zap.Error(err))
		return
	}
	if err := jm.SubmitStatsTask(segment.GetID(), targetSegmentID, indexpb.StatsSubJob_Sort, true); err != nil {
		log.Warn("create stats task with sort for segment failed, wait for retry",
			zap.Int64("segmentID", segment.GetID()), zap.Error(err))
		return
	}
}

func (jm *statsJobManager) enableBM25() bool {
	return false
}

func needDoTextIndex(segment *SegmentInfo, fieldIDs []UniqueID) bool {
	if !(isFlush(segment) && segment.GetLevel() != datapb.SegmentLevel_L0 &&
		segment.GetIsSorted()) {
		return false
	}

	for _, fieldID := range fieldIDs {
		if segment.GetTextStatsLogs()[fieldID] == nil {
			return true
		}
	}
	return false
}

func needDoJsonKeyIndex(segment *SegmentInfo, fieldIDs []UniqueID) bool {
	if !(isFlush(segment) && segment.GetLevel() != datapb.SegmentLevel_L0 &&
		segment.GetIsSorted()) {
		return false
	}

	for _, fieldID := range fieldIDs {
		if segment.GetJsonKeyStats()[fieldID] == nil {
			return true
		}
	}
	return false
}

func needDoBM25(segment *SegmentInfo, fieldIDs []UniqueID) bool {
	// TODO: docking bm25 stats task
	return false
}

func (jm *statsJobManager) triggerTextStatsTask() {
	collections := jm.mt.GetCollections()
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
		segments := jm.mt.SelectSegments(jm.ctx, WithCollection(collection.ID), SegmentFilterFunc(func(seg *SegmentInfo) bool {
			return needDoTextIndex(seg, needTriggerFieldIDs)
		}))

		for _, segment := range segments {
			if err := jm.SubmitStatsTask(segment.GetID(), segment.GetID(), indexpb.StatsSubJob_TextIndexJob, true); err != nil {
				log.Warn("create stats task with text index for segment failed, wait for retry",
					zap.Int64("segmentID", segment.GetID()), zap.Error(err))
				continue
			}
		}
	}
}

func (jm *statsJobManager) triggerJsonKeyIndexStatsTask() {
	collections := jm.mt.GetCollections()
	for _, collection := range collections {
		needTriggerFieldIDs := make([]UniqueID, 0)
		for _, field := range collection.Schema.GetFields() {
			h := typeutil.CreateFieldSchemaHelper(field)
			if h.EnableJSONKeyIndex() && Params.CommonCfg.EnabledJSONKeyStats.GetAsBool() {
				needTriggerFieldIDs = append(needTriggerFieldIDs, field.GetFieldID())
			}
		}
		segments := jm.mt.SelectSegments(jm.ctx, WithCollection(collection.ID), SegmentFilterFunc(func(seg *SegmentInfo) bool {
			return needDoJsonKeyIndex(seg, needTriggerFieldIDs)
		}))
		for _, segment := range segments {
			if err := jm.SubmitStatsTask(segment.GetID(), segment.GetID(), indexpb.StatsSubJob_JsonKeyIndexJob, true); err != nil {
				log.Warn("create stats task with json key index for segment failed, wait for retry:",
					zap.Int64("segmentID", segment.GetID()), zap.Error(err))
				continue
			}
		}
	}
}

func (jm *statsJobManager) triggerBM25StatsTask() {
	collections := jm.mt.GetCollections()
	for _, collection := range collections {
		needTriggerFieldIDs := make([]UniqueID, 0)
		for _, field := range collection.Schema.GetFields() {
			// TODO: docking bm25 stats task
			if jm.enableBM25() {
				needTriggerFieldIDs = append(needTriggerFieldIDs, field.GetFieldID())
			}
		}
		segments := jm.mt.SelectSegments(jm.ctx, WithCollection(collection.ID), SegmentFilterFunc(func(seg *SegmentInfo) bool {
			return needDoBM25(seg, needTriggerFieldIDs)
		}))

		for _, segment := range segments {
			if err := jm.SubmitStatsTask(segment.GetID(), segment.GetID(), indexpb.StatsSubJob_BM25Job, true); err != nil {
				log.Warn("create stats task with bm25 for segment failed, wait for retry",
					zap.Int64("segmentID", segment.GetID()), zap.Error(err))
				continue
			}
		}
	}
}

// cleanupStatsTasks clean up the finished/failed stats tasks
func (jm *statsJobManager) cleanupStatsTasksLoop() {
	log.Info("start cleanupStatsTasksLoop...")
	defer jm.loopWg.Done()

	ticker := time.NewTicker(Params.DataCoordCfg.GCInterval.GetAsDuration(time.Second))
	defer ticker.Stop()

	for {
		select {
		case <-jm.ctx.Done():
			log.Warn("DataCoord context done, exit cleanupStatsTasksLoop...")
			return
		case <-ticker.C:
			start := time.Now()
			log.Info("start cleanupUnusedStatsTasks...", zap.Time("startAt", start))

			taskIDs := jm.mt.statsTaskMeta.CanCleanedTasks()
			for _, taskID := range taskIDs {
				// waiting for queue processing tasks to complete
				if !jm.scheduler.exist(taskID) {
					if err := jm.mt.statsTaskMeta.DropStatsTask(taskID); err != nil {
						// ignore err, if remove failed, wait next GC
						log.Warn("clean up stats task failed", zap.Int64("taskID", taskID), zap.Error(err))
					}
				}
			}
			log.Info("cleanupUnusedStatsTasks done", zap.Duration("timeCost", time.Since(start)))
		}
	}
}

func (jm *statsJobManager) SubmitStatsTask(originSegmentID, targetSegmentID int64,
	subJobType indexpb.StatsSubJob, canRecycle bool,
) error {
	originSegment := jm.mt.GetHealthySegment(jm.ctx, originSegmentID)
	if originSegment == nil {
		return merr.WrapErrSegmentNotFound(originSegmentID)
	}
	taskID, err := jm.allocator.AllocID(context.Background())
	if err != nil {
		return err
	}
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
	if err = jm.mt.statsTaskMeta.AddStatsTask(t); err != nil {
		if errors.Is(err, merr.ErrTaskDuplicate) {
			log.RatedInfo(10, "stats task already exists", zap.Int64("taskID", taskID),
				zap.Int64("collectionID", originSegment.GetCollectionID()),
				zap.Int64("segmentID", originSegment.GetID()))
			return nil
		}
		return err
	}
	jm.scheduler.enqueue(newStatsTask(t.GetTaskID(), t.GetSegmentID(), t.GetTargetSegmentID(), subJobType))
	return nil
}

func (jm *statsJobManager) GetStatsTask(originSegmentID int64, subJobType indexpb.StatsSubJob) *indexpb.StatsTask {
	task := jm.mt.statsTaskMeta.GetStatsTaskBySegmentID(originSegmentID, subJobType)
	log.Info("statsJobManager get stats task state", zap.Int64("segmentID", originSegmentID),
		zap.String("subJobType", subJobType.String()), zap.String("state", task.GetState().String()),
		zap.String("failReason", task.GetFailReason()))
	return task
}

func (jm *statsJobManager) DropStatsTask(originSegmentID int64, subJobType indexpb.StatsSubJob) error {
	task := jm.mt.statsTaskMeta.GetStatsTaskBySegmentID(originSegmentID, subJobType)
	if task == nil {
		return nil
	}
	jm.scheduler.AbortTask(task.GetTaskID())
	if err := jm.mt.statsTaskMeta.MarkTaskCanRecycle(task.GetTaskID()); err != nil {
		return err
	}

	log.Info("statsJobManager drop stats task success", zap.Int64("segmentID", originSegmentID),
		zap.Int64("taskID", task.GetTaskID()), zap.String("subJobType", subJobType.String()))
	return nil
}
