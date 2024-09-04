package datacoord

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type StatsTaskManager interface {
	Start()
	Stop()
	SubmitStatsTask(originSegmentID, targetSegmentID int64, subJobType indexpb.StatsSubJob, canRecycle bool) error
	GetStatsTaskState(originSegmentID int64, subJobType indexpb.StatsSubJob) indexpb.JobState
	DropStatsTask(originSegmentID int64, subJobType indexpb.StatsSubJob) error
}

var _ StatsTaskManager = (*statsJobManager)(nil)

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
	allocator allocator.Allocator) *statsJobManager {
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
	jm.loopWg.Add(2)
	go jm.triggerStatsTaskLoop()
	go jm.cleanupStatsTasksLoop()
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
			jm.triggerTextIndexStatsTask()
			jm.triggerBM25StatsTask()

		case segID := <-getStatsTaskChSingleton().getChannel():
			log.Info("receive new segment to trigger stats task", zap.Int64("segmentID", segID))
			segment := jm.mt.GetSegment(segID)
			if segment == nil {
				log.Warn("segment is not exist, no need to do stats task", zap.Int64("segmentID", segID))
				continue
			}
			// TODO @xiaocai2333 @bigsheeper: remove code after allow create stats task for importing segment
			if segment.GetIsImporting() {
				log.Info("segment is importing, skip stats task", zap.Int64("segmentID", segID))
				select {
				case getBuildIndexChSingleton().getChannel() <- segID:
				default:
				}
				continue
			}
			jm.createSortStatsTaskForSegment(segment)
		}
	}
}

func (jm *statsJobManager) triggerSortStatsTask() {
	segments := jm.mt.SelectSegments(SegmentFilterFunc(func(seg *SegmentInfo) bool {
		return isFlush(seg) && seg.GetLevel() != datapb.SegmentLevel_L0 && !seg.GetIsSorted()
	}))
	for _, segment := range segments {
		if !segment.GetIsSorted() {
			// TODO @xiaocai2333, @bigsheeper:
			if segment.GetIsImporting() {
				log.Warn("segment is importing, skip stats task, wait @bigsheeper support it")
				continue
			}
			jm.createSortStatsTaskForSegment(segment)
		}
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

func (jm *statsJobManager) enableMatch(field *schemapb.FieldSchema) bool {
	if field.GetDataType() != schemapb.DataType_VarChar {
		return false
	}
	for _, pair := range field.GetTypeParams() {
		if pair.Key == "match" && pair.Value == "true" {
			return true
		}
	}
	return false
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

func (jm *statsJobManager) triggerTextIndexStatsTask() {
	collections := jm.mt.GetCollections()
	for _, collection := range collections {
		needTriggerFieldIDs := make([]UniqueID, 0)
		for _, field := range collection.Schema.GetFields() {
			// TODO @longjiquan: please replace it to fieldSchemaHelper.EnableMath
			if jm.enableMatch(field) {
				needTriggerFieldIDs = append(needTriggerFieldIDs, field.GetFieldID())
			}
		}
		segments := jm.mt.SelectSegments(WithCollection(collection.ID), SegmentFilterFunc(func(seg *SegmentInfo) bool {
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
		segments := jm.mt.SelectSegments(WithCollection(collection.ID), SegmentFilterFunc(func(seg *SegmentInfo) bool {
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
				if jm.scheduler.getTask(taskID) == nil {
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
	subJobType indexpb.StatsSubJob, canRecycle bool) error {
	originSegment := jm.mt.GetHealthySegment(originSegmentID)
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
			return nil
		}
		return err
	}
	jm.scheduler.enqueue(newStatsTask(t.GetTaskID(), t.GetSegmentID(), t.GetTargetSegmentID(), subJobType))
	return nil
}

func (jm *statsJobManager) GetStatsTaskState(originSegmentID int64, subJobType indexpb.StatsSubJob) indexpb.JobState {
	return jm.mt.statsTaskMeta.GetStatsTaskStateBySegmentID(originSegmentID, subJobType)
}

func (jm *statsJobManager) DropStatsTask(originSegmentID int64, subJobType indexpb.StatsSubJob) error {
	task := jm.mt.statsTaskMeta.GetStatsTaskBySegmentID(originSegmentID, subJobType)
	if task == nil {
		return nil
	}
	jm.scheduler.AbortTask(task.GetTaskID())
	return jm.mt.statsTaskMeta.MarkTaskCanRecycle(task.GetTaskID())
}
