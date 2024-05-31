package datacoord

import (
	"context"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
)

type CompactionTriggerType int8

const (
	TriggerTypeLevelZeroViewChange CompactionTriggerType = iota + 1
	TriggerTypeLevelZeroViewIDLE
	TriggerTypeSegmentSizeViewChange
)

type TriggerManager interface {
	Notify(UniqueID, CompactionTriggerType, []CompactionView)
}

// CompactionTriggerManager registers Triggers to TriggerType
// so that when the certain TriggerType happens, the corresponding triggers can
// trigger the correct compaction plans.
// Trigger types:
// 1. Change of Views
//   - LevelZeroViewTrigger
//   - SegmentSizeViewTrigger
//
// 2. SystemIDLE & schedulerIDLE
// 3. Manual Compaction
type CompactionTriggerManager struct {
	scheduler         Scheduler
	handler           Handler
	compactionHandler compactionPlanContext // TODO replace with scheduler

	allocator allocator
}

func NewCompactionTriggerManager(alloc allocator, handler Handler, compactionHandler compactionPlanContext) *CompactionTriggerManager {
	m := &CompactionTriggerManager{
		allocator:         alloc,
		handler:           handler,
		compactionHandler: compactionHandler,
	}

	return m
}

func (m *CompactionTriggerManager) Notify(taskID UniqueID, eventType CompactionTriggerType, views []CompactionView) {
	log := log.With(zap.Int64("taskID", taskID))
	for _, view := range views {
		if m.compactionHandler.isFull() {
			log.RatedInfo(1.0, "Skip trigger compaction for scheduler is full")
			return
		}

		switch eventType {
		case TriggerTypeLevelZeroViewChange:
			log.Debug("Start to trigger a level zero compaction by TriggerTypeLevelZeroViewChange")
			outView, reason := view.Trigger()
			if outView != nil {
				log.Info("Success to trigger a LevelZeroCompaction output view, try to sumit",
					zap.String("reason", reason),
					zap.String("output view", outView.String()))
				m.SubmitL0ViewToScheduler(taskID, outView)
			}

		case TriggerTypeLevelZeroViewIDLE:
			log.Debug("Start to trigger a level zero compaction by TriggerTypLevelZeroViewIDLE")
			outView, reason := view.Trigger()
			if outView == nil {
				log.Info("Start to force trigger a level zero compaction by TriggerTypLevelZeroViewIDLE")
				outView, reason = view.ForceTrigger()
			}

			if outView != nil {
				log.Info("Success to trigger a LevelZeroCompaction output view, try to submit",
					zap.String("reason", reason),
					zap.String("output view", outView.String()))
				m.SubmitL0ViewToScheduler(taskID, outView)
			}
		}
	}
}

func (m *CompactionTriggerManager) SubmitL0ViewToScheduler(taskID int64, outView CompactionView) {
	task := m.buildL0CompactionTask(taskID, outView)
	if task == nil {
		return
	}

	m.handler.enqueueCompaction(&defaultCompactionTask{
		CompactionTask: task,
	})
	log.Info("Finish to submit a LevelZeroCompaction plan",
		zap.Int64("taskID", taskID),
		zap.Int64("planID", task.GetPlanID()),
		zap.String("type", task.GetType().String()),
	)
}

func (m *CompactionTriggerManager) buildL0CompactionTask(taskID int64, view CompactionView) *datapb.CompactionTask {
	levelZeroSegs := lo.Map(view.GetSegmentsView(), func(segView *SegmentView, _ int) int64 {
		return segView.ID
	})

	// todo optimize allocate
	id, err := m.allocator.allocID(context.TODO())
	if err != nil {
		return nil
	}

	task := &datapb.CompactionTask{
		PlanID:           id,
		Type:             datapb.CompactionType_Level0DeleteCompaction,
		InputSegments:    levelZeroSegs,
		Channel:          view.GetGroupLabel().Channel,
		TriggerID:        taskID,
		CollectionID:     view.GetGroupLabel().CollectionID,
		PartitionID:      view.GetGroupLabel().PartitionID,
		Pos:              view.(*LevelZeroSegmentsView).earliestGrowingSegmentPos,
		TimeoutInSeconds: Params.DataCoordCfg.CompactionTimeoutInSeconds.GetAsInt32(),
		// collectionSchema todo wayblink
	}

	return task
}

// chanPartSegments is an internal result struct, which is aggregates of SegmentInfos with same collectionID, partitionID and channelName
type chanPartSegments struct {
	collectionID UniqueID
	partitionID  UniqueID
	channelName  string
	segments     []*SegmentInfo
}

func fillOriginPlan(schema *schemapb.CollectionSchema, alloc allocator, plan *datapb.CompactionPlan) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id, err := alloc.allocID(ctx)
	if err != nil {
		return err
	}

	plan.PlanID = id
	plan.TimeoutInSeconds = Params.DataCoordCfg.CompactionTimeoutInSeconds.GetAsInt32()
	plan.Schema = schema
	return nil
}
