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

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type CompactionTriggerType int8

const (
	TriggerTypeLevelZeroViewChange CompactionTriggerType = iota + 1
	TriggerTypeLevelZeroViewIDLE
	TriggerTypeSegmentSizeViewChange
	TriggerTypeClustering
	TriggerTypeDeltaTooMuch
	TriggerTypeOverSize
)

type TriggerManager interface {
	Start()
	Stop()
	ManualTrigger(ctx context.Context, collectionID int64, clusteringCompaction bool) (UniqueID, error)
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
	compactionHandler compactionPlanContext
	handler           Handler
	allocator         allocator

	view *FullViews
	// todo handle this lock
	viewGuard lock.RWMutex

	meta             *meta
	l0Policy         *l0CompactionPolicy
	clusteringPolicy *clusteringCompactionPolicy
	scoPolicy        *sizeOptimizationCompactionPolicy

	closeSig chan struct{}
	closeWg  sync.WaitGroup
}

func NewCompactionTriggerManager(alloc allocator, handler Handler, compactionHandler compactionPlanContext, meta *meta) *CompactionTriggerManager {
	m := &CompactionTriggerManager{
		allocator:         alloc,
		handler:           handler,
		compactionHandler: compactionHandler,
		view: &FullViews{
			collections: make(map[int64][]*SegmentView),
		},
		meta:     meta,
		closeSig: make(chan struct{}),
	}
	m.l0Policy = newL0CompactionPolicy(meta)
	m.clusteringPolicy = newClusteringCompactionPolicy(meta, m.allocator, m.handler)
	m.scoPolicy = newSOCPolicy(meta, m.allocator, m.handler)
	return m
}

func (m *CompactionTriggerManager) Start() {
	m.closeWg.Add(1)
	go m.startLoop()
}

func (m *CompactionTriggerManager) Close() {
	close(m.closeSig)
	m.closeWg.Wait()
}

func (m *CompactionTriggerManager) startLoop() {
	defer logutil.LogPanic()
	defer m.closeWg.Done()

	l0Ticker := time.NewTicker(Params.DataCoordCfg.GlobalCompactionInterval.GetAsDuration(time.Second))
	defer l0Ticker.Stop()
	clusteringTicker := time.NewTicker(Params.DataCoordCfg.ClusteringCompactionTriggerInterval.GetAsDuration(time.Second))
	defer clusteringTicker.Stop()
	singleTicker := time.NewTicker(Params.DataCoordCfg.GlobalCompactionInterval.GetAsDuration(time.Second))
	defer singleTicker.Stop()
	log.Info("Compaction trigger manager start")
	for {
		select {
		case <-m.closeSig:
			log.Info("Compaction trigger manager checkLoop quit")
			return
		case <-l0Ticker.C:
			if !m.l0Policy.Enable() {
				continue
			}
			if m.compactionHandler.isFull() {
				log.RatedInfo(10, "Skip trigger l0 compaction since compactionHandler is full")
				continue
			}
			events, err := m.l0Policy.Trigger()
			if err != nil {
				log.Warn("Fail to trigger L0 policy", zap.Error(err))
				continue
			}
			ctx := context.Background()
			if len(events) > 0 {
				for triggerType, views := range events {
					m.notify(ctx, triggerType, views)
				}
			}
		case <-clusteringTicker.C:
			if !m.clusteringPolicy.Enable() {
				continue
			}
			if m.compactionHandler.isFull() {
				log.RatedInfo(10, "Skip trigger clustering compaction since compactionHandler is full")
				continue
			}
			events, err := m.clusteringPolicy.Trigger()
			if err != nil {
				log.Warn("Fail to trigger clustering policy", zap.Error(err))
				continue
			}
			ctx := context.Background()
			if len(events) > 0 {
				for triggerType, views := range events {
					m.notify(ctx, triggerType, views)
				}
			}
		case <-singleTicker.C:
			if !m.scoPolicy.Enable() {
				continue
			}
			if m.compactionHandler.isFull() {
				log.RatedInfo(10, "Skip trigger single compaction since compactionHandler is full")
				continue
			}
			events, err := m.scoPolicy.Trigger()
			if err != nil {
				log.Warn("Fail to trigger single policy", zap.Error(err))
				continue
			}
			ctx := context.Background()
			if len(events) > 0 {
				for triggerType, views := range events {
					m.notify(ctx, triggerType, views)
				}
			}
		}
	}
}

func (m *CompactionTriggerManager) ManualTrigger(ctx context.Context, collectionID int64, clusteringCompaction bool) (UniqueID, error) {
	log.Info("receive manual trigger", zap.Int64("collectionID", collectionID))
	views, triggerID, err := m.clusteringPolicy.triggerOneCollection(context.Background(), collectionID, true)
	if err != nil {
		return 0, err
	}
	events := make(map[CompactionTriggerType][]CompactionView, 0)
	events[TriggerTypeClustering] = views
	if len(events) > 0 {
		for triggerType, views := range events {
			m.notify(ctx, triggerType, views)
		}
	}
	return triggerID, nil
}

func (m *CompactionTriggerManager) notify(ctx context.Context, eventType CompactionTriggerType, views []CompactionView) {
	if eventType == TriggerTypeLevelZeroViewIDLE {
		for _, view := range views {
			log.Debug("Try to force trigger a LevelZeroCompaction by ViewIDLE", zap.String("label", view.GetGroupLabel().String()))
			outView, reason := view.ForceTrigger()
			if outView != nil {
				log.Info("Success to trigger a LevelZeroCompaction output view, try to submit",
					zap.String("reason", reason),
					zap.String("output view", outView.String()))
				m.SubmitL0ViewToScheduler(ctx, outView)
			}
		}
		return
	}

	for _, view := range views {
		log.Debug("Try to trigger a Compaction", zap.String("view", view.String()), zap.String("label", view.GetGroupLabel().String()))
		outView, reason := view.Trigger()
		if outView == nil {
			continue
		}

		log := log.With(zap.String("label", view.GetGroupLabel().String()), zap.String("reason", reason), zap.String("output view", outView.String()))
		switch eventType {
		case TriggerTypeLevelZeroViewChange:
			log.Info("Success to trigger a LevelZeroCompaction output view, try to submit")
			m.SubmitL0ViewToScheduler(ctx, outView)
		case TriggerTypeClustering:
			log.Info("Success to trigger a ClusteringCompaction output view, try to submit")
			m.SubmitClusteringViewToScheduler(ctx, outView)
		case TriggerTypeDeltaTooMuch:
			log.Info("Success to trigger a SingleCompaction output view, try to submit")
			m.SubmitSingleViewToScheduler(ctx, outView)
		case TriggerTypeOverSize:
			log.Info("Success to trigger an OverSizeCompaction output view, try to submit")
			m.SubmitOversizeViewToScheduler(ctx, outView)
		}
	}
}

func (m *CompactionTriggerManager) SubmitL0ViewToScheduler(ctx context.Context, view CompactionView) {
	log := log.With(zap.String("view", view.String()))
	taskID, err := m.allocator.allocID(ctx)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because allocate id fail", zap.Error(err))
		return
	}

	levelZeroSegs := lo.Map(view.GetSegmentsView(), func(segView *SegmentView, _ int) int64 {
		return segView.ID
	})

	collection, err := m.handler.GetCollection(ctx, view.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because get collection fail", zap.Error(err))
		return
	}

	task := &datapb.CompactionTask{
		TriggerID:        taskID, // inner trigger, use task id as trigger id
		PlanID:           taskID,
		Type:             datapb.CompactionType_Level0DeleteCompaction,
		StartTime:        time.Now().Unix(),
		InputSegments:    levelZeroSegs,
		State:            datapb.CompactionTaskState_pipelining,
		Channel:          view.GetGroupLabel().Channel,
		CollectionID:     view.GetGroupLabel().CollectionID,
		PartitionID:      view.GetGroupLabel().PartitionID,
		Pos:              view.(*LevelZeroSegmentsView).earliestGrowingSegmentPos,
		TimeoutInSeconds: Params.DataCoordCfg.CompactionTimeoutInSeconds.GetAsInt32(),
		Schema:           collection.Schema,
	}

	err = m.compactionHandler.enqueueCompaction(task)
	if err != nil {
		log.Warn("Failed to execute compaction task",
			zap.Int64("triggerID", task.GetTriggerID()),
			zap.Int64("planID", task.GetPlanID()),
			zap.Int64s("segmentIDs", task.GetInputSegments()),
			zap.Error(err))
	}
	log.Info("Finish to submit a LevelZeroCompaction plan",
		zap.Int64("triggerID", task.GetTriggerID()),
		zap.Int64("planID", task.GetPlanID()),
		zap.String("type", task.GetType().String()),
		zap.Int64s("L0 segments", levelZeroSegs),
	)
}

func (m *CompactionTriggerManager) SubmitClusteringViewToScheduler(ctx context.Context, view CompactionView) {
	log := log.With(zap.String("view", view.String()))
	taskID, _, err := m.allocator.allocN(2)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because allocate id fail", zap.Error(err))
		return
	}
	collection, err := m.handler.GetCollection(ctx, view.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because get collection fail", zap.Error(err))
		return
	}

	expectedSegmentSize := getExpectedSegmentSize(m.meta, collection)
	totalRows, maxSegmentRows, preferSegmentRows, err := calculateClusteringCompactionConfig(collection, view, expectedSegmentSize)
	if err != nil {
		log.Warn("Failed to calculate cluster compaction config fail", zap.Error(err))
		return
	}

	task := &datapb.CompactionTask{
		PlanID:             taskID,
		TriggerID:          view.(*ClusteringSegmentsView).triggerID,
		State:              datapb.CompactionTaskState_pipelining,
		StartTime:          time.Now().Unix(),
		CollectionTtl:      view.(*ClusteringSegmentsView).collectionTTL.Nanoseconds(),
		TimeoutInSeconds:   Params.DataCoordCfg.ClusteringCompactionTimeoutInSeconds.GetAsInt32(),
		Type:               datapb.CompactionType_ClusteringCompaction,
		CollectionID:       view.GetGroupLabel().CollectionID,
		PartitionID:        view.GetGroupLabel().PartitionID,
		Channel:            view.GetGroupLabel().Channel,
		Schema:             collection.Schema,
		ClusteringKeyField: view.(*ClusteringSegmentsView).clusteringKeyField,
		InputSegments:      lo.Map(view.GetSegmentsView(), func(segmentView *SegmentView, _ int) int64 { return segmentView.ID }),
		MaxSegmentRows:     maxSegmentRows,
		PreferSegmentRows:  preferSegmentRows,
		TotalRows:          totalRows,
		AnalyzeTaskID:      taskID + 1,
		LastStateStartTime: time.Now().Unix(),
	}
	err = m.compactionHandler.enqueueCompaction(task)
	if err != nil {
		log.Warn("Failed to execute compaction task",
			zap.Int64("planID", task.GetPlanID()),
			zap.Error(err))
	}
	log.Info("Finish to submit a clustering compaction task",
		zap.Int64("triggerID", task.GetTriggerID()),
		zap.Int64("planID", task.GetPlanID()),
		zap.Int64("MaxSegmentRows", task.MaxSegmentRows),
		zap.Int64("PreferSegmentRows", task.PreferSegmentRows),
	)
}

func (m *CompactionTriggerManager) SubmitOversizeViewToScheduler(ctx context.Context, view CompactionView) {
	log := log.With(zap.String("view", view.String()))
	taskID, _, err := m.allocator.allocN(1)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because allocate id fail", zap.Error(err))
		return
	}
	collection, err := m.handler.GetCollection(ctx, view.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because get collection fail", zap.Error(err))
		return
	}

	task := &datapb.CompactionTask{
		PlanID:             taskID,
		TriggerID:          view.(*OversizedSegmentView).triggerID,
		State:              datapb.CompactionTaskState_pipelining,
		StartTime:          time.Now().Unix(),
		CollectionTtl:      view.(*OversizedSegmentView).collectionTTL.Nanoseconds(),
		TimeoutInSeconds:   Params.DataCoordCfg.CompactionTimeoutInSeconds.GetAsInt32(),
		Type:               datapb.CompactionType_SplitCompaction,
		CollectionID:       view.GetGroupLabel().CollectionID,
		PartitionID:        view.GetGroupLabel().PartitionID,
		Channel:            view.GetGroupLabel().Channel,
		Schema:             collection.Schema,
		InputSegments:      lo.Map(view.GetSegmentsView(), func(segmentView *SegmentView, _ int) int64 { return segmentView.ID }),
		TotalRows:          lo.SumBy(view.GetSegmentsView(), func(segmentView *SegmentView) int64 { return segmentView.NumOfRows }),
		LastStateStartTime: time.Now().Unix(),
		MaxSize:            view.(*OversizedSegmentView).maxSize,
	}
	err = m.compactionHandler.enqueueCompaction(task)
	if err != nil {
		log.Warn("Failed to execute compaction task",
			zap.Int64("triggerID", task.GetTriggerID()),
			zap.Int64("planID", task.GetPlanID()),
			zap.Int64s("segmentIDs", task.GetInputSegments()),
			zap.Error(err))
	}
	log.Info("Finish to submit an oversize compaction task",
		zap.Int64("triggerID", task.GetTriggerID()),
		zap.Int64("planID", task.GetPlanID()),
		zap.String("type", task.GetType().String()),
		zap.Int64("maxSize", task.GetMaxSize()),
	)
}

func (m *CompactionTriggerManager) SubmitSingleViewToScheduler(ctx context.Context, view CompactionView) {
	log := log.With(zap.String("view", view.String()))
	taskID, _, err := m.allocator.allocN(1)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because allocate id fail", zap.Error(err))
		return
	}
	collection, err := m.handler.GetCollection(ctx, view.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because get collection fail", zap.Error(err))
		return
	}

	task := &datapb.CompactionTask{
		PlanID:             taskID,
		TriggerID:          view.(*SegmentDeltaView).triggerID,
		State:              datapb.CompactionTaskState_pipelining,
		StartTime:          time.Now().Unix(),
		CollectionTtl:      view.(*SegmentDeltaView).collectionTTL.Nanoseconds(),
		TimeoutInSeconds:   Params.DataCoordCfg.CompactionTimeoutInSeconds.GetAsInt32(),
		Type:               datapb.CompactionType_MixCompaction, // todo: use SingleCompaction
		CollectionID:       view.GetGroupLabel().CollectionID,
		PartitionID:        view.GetGroupLabel().PartitionID,
		Channel:            view.GetGroupLabel().Channel,
		Schema:             collection.Schema,
		InputSegments:      lo.Map(view.GetSegmentsView(), func(segmentView *SegmentView, _ int) int64 { return segmentView.ID }),
		TotalRows:          lo.SumBy(view.GetSegmentsView(), func(segmentView *SegmentView) int64 { return segmentView.NumOfRows }),
		LastStateStartTime: time.Now().Unix(),
	}
	err = m.compactionHandler.enqueueCompaction(task)
	if err != nil {
		log.Warn("Failed to execute compaction task",
			zap.Int64("triggerID", task.GetTriggerID()),
			zap.Int64("planID", task.GetPlanID()),
			zap.Int64s("segmentIDs", task.GetInputSegments()),
			zap.Error(err))
	}
	log.Info("Finish to submit a single compaction task",
		zap.Int64("triggerID", task.GetTriggerID()),
		zap.Int64("planID", task.GetPlanID()),
		zap.String("type", task.GetType().String()),
	)
}

func getExpectedSegmentSize(meta *meta, collection *collectionInfo) int64 {
	allDiskIndex := meta.indexMeta.AreAllDiskIndex(collection.ID, collection.Schema)
	if allDiskIndex {
		// Only if all vector fields index type are DiskANN, recalc segment max size here.
		return paramtable.Get().DataCoordCfg.DiskSegmentMaxSize.GetAsInt64() * 1024 * 1024
	}
	// If some vector fields index type are not DiskANN, recalc segment max size using default policy.
	return paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024
}

// chanPartSegments is an internal result struct, which is aggregates of SegmentInfos with same collectionID, partitionID and channelName
type chanPartSegments struct {
	collectionID UniqueID
	partitionID  UniqueID
	channelName  string
	segments     []*SegmentInfo
}
