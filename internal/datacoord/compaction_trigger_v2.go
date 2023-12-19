package datacoord

import (
	"context"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
)

type CompactionTriggerType int8

const (
	TriggerTypeLevelZeroView CompactionTriggerType = iota + 1
	TriggerTypeSegmentSizeView
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
	meta      *meta
	scheduler Scheduler
	handler   compactionPlanContext // TODO replace with scheduler

	allocator allocator
}

func NewCompactionTriggerManager(meta *meta, alloc allocator, handler compactionPlanContext) *CompactionTriggerManager {
	m := &CompactionTriggerManager{
		meta:      meta,
		allocator: alloc,
		handler:   handler,
	}

	return m
}

func (m *CompactionTriggerManager) Notify(taskID UniqueID, eventType CompactionTriggerType, views []CompactionView) {
	log := log.With(zap.Int64("taskID", taskID))
	for _, view := range views {
		switch eventType {
		case TriggerTypeLevelZeroView:
			log.Debug("Start to trigger a level zero compaction")
			outView, reason := view.Trigger()
			if outView == nil {
				continue
			}

			plan := m.BuildLevelZeroCompactionPlan(outView)
			if plan == nil {
				continue
			}

			label := outView.GetGroupLabel()
			signal := &compactionSignal{
				id:           taskID,
				isForce:      false,
				isGlobal:     true,
				collectionID: label.CollectionID,
				partitionID:  label.PartitionID,
				pos:          outView.(*LevelZeroSegmentsView).earliestGrowingSegmentPos,
			}

			// TODO, remove handler, use scheduler
			// m.scheduler.Submit(plan)
			m.handler.execCompactionPlan(signal, plan)
			log.Info("Finish to trigger a LevelZeroCompaction plan",
				zap.Int64("planID", plan.GetPlanID()),
				zap.String("type", plan.GetType().String()),
				zap.String("reason", reason),
				zap.String("output view", outView.String()))
		}
	}
}

func (m *CompactionTriggerManager) BuildLevelZeroCompactionPlan(view CompactionView) *datapb.CompactionPlan {
	var segmentBinlogs []*datapb.CompactionSegmentBinlogs
	levelZeroSegs := lo.Map(view.GetSegmentsView(), func(v *SegmentView, _ int) *datapb.CompactionSegmentBinlogs {
		s := m.meta.GetSegment(v.ID)
		return &datapb.CompactionSegmentBinlogs{
			SegmentID: s.GetID(),
			Deltalogs: s.GetDeltalogs(),
			Level:     datapb.SegmentLevel_L0,
		}
	})
	segmentBinlogs = append(segmentBinlogs, levelZeroSegs...)

	plan := &datapb.CompactionPlan{
		Type:           datapb.CompactionType_Level0DeleteCompaction,
		SegmentBinlogs: segmentBinlogs,
		Channel:        view.GetGroupLabel().Channel,
	}

	if err := fillOriginPlan(m.allocator, plan); err != nil {
		return nil
	}

	return plan
}

// chanPartSegments is an internal result struct, which is aggregates of SegmentInfos with same collectionID, partitionID and channelName
type chanPartSegments struct {
	collectionID UniqueID
	partitionID  UniqueID
	channelName  string
	segments     []*SegmentInfo
}

func fillOriginPlan(alloc allocator, plan *datapb.CompactionPlan) error {
	// TODO context
	id, err := alloc.allocID(context.TODO())
	if err != nil {
		return err
	}

	plan.PlanID = id
	plan.TimeoutInSeconds = Params.DataCoordCfg.CompactionTimeoutInSeconds.GetAsInt32()
	return nil
}
