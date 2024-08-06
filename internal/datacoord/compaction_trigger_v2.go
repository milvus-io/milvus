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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type CompactionTriggerType int8

const (
	TriggerTypeLevelZeroViewChange CompactionTriggerType = iota + 1
	TriggerTypeLevelZeroViewIDLE
	TriggerTypeSegmentSizeViewChange
	TriggerTypeClustering
	TriggerTypeSingle
)

type TriggerManager interface {
	Start()
	Stop()
	ManualTrigger(ctx context.Context, collectionID int64, clusteringCompaction bool) (UniqueID, error)
}

var _ TriggerManager = (*CompactionTriggerManager)(nil)

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
	singlePolicy     *singleCompactionPolicy

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
	m.singlePolicy = newSingleCompactionPolicy(meta, m.allocator, m.handler)
	return m
}

func (m *CompactionTriggerManager) Start() {
	m.closeWg.Add(1)
	go m.startLoop()
}

func (m *CompactionTriggerManager) Stop() {
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
			if !m.singlePolicy.Enable() {
				continue
			}
			if m.compactionHandler.isFull() {
				log.RatedInfo(10, "Skip trigger single compaction since compactionHandler is full")
				continue
			}
			events, err := m.singlePolicy.Trigger()
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
	for _, view := range views {
		switch eventType {
		case TriggerTypeLevelZeroViewChange:
			log.Debug("Start to trigger a level zero compaction by TriggerTypeLevelZeroViewChange")
			outView, reason := view.Trigger()
			if outView != nil {
				log.Info("Success to trigger a LevelZeroCompaction output view, try to submit",
					zap.String("reason", reason),
					zap.String("output view", outView.String()))
				m.SubmitL0ViewToScheduler(ctx, outView)
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
				m.SubmitL0ViewToScheduler(ctx, outView)
			}
		case TriggerTypeClustering:
			log.Debug("Start to trigger a clustering compaction by TriggerTypeClustering")
			outView, reason := view.Trigger()
			if outView != nil {
				log.Info("Success to trigger a ClusteringCompaction output view, try to submit",
					zap.String("reason", reason),
					zap.String("output view", outView.String()))
				m.SubmitClusteringViewToScheduler(ctx, outView)
			}
		case TriggerTypeSingle:
			log.Debug("Start to trigger a single compaction by TriggerTypeSingle")
			outView, reason := view.Trigger()
			if outView != nil {
				log.Info("Success to trigger a MixCompaction output view, try to submit",
					zap.String("reason", reason),
					zap.String("output view", outView.String()))
				m.SubmitSingleViewToScheduler(ctx, outView)
			}
		}
	}
}

func (m *CompactionTriggerManager) SubmitL0ViewToScheduler(ctx context.Context, view CompactionView) {
	taskID, err := m.allocator.allocID(ctx)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because allocate id fail", zap.String("view", view.String()))
		return
	}

	levelZeroSegs := lo.Map(view.GetSegmentsView(), func(segView *SegmentView, _ int) int64 {
		return segView.ID
	})

	collection, err := m.handler.GetCollection(ctx, view.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because get collection fail", zap.String("view", view.String()))
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
			zap.Int64("collection", task.CollectionID),
			zap.Int64("planID", task.GetPlanID()),
			zap.Int64s("segmentIDs", task.GetInputSegments()),
			zap.Error(err))
		return
	}
	log.Info("Finish to submit a LevelZeroCompaction plan",
		zap.Int64("taskID", taskID),
		zap.Int64("planID", task.GetPlanID()),
		zap.String("type", task.GetType().String()),
		zap.Int64s("L0 segments", levelZeroSegs),
	)
}

func (m *CompactionTriggerManager) SubmitClusteringViewToScheduler(ctx context.Context, view CompactionView) {
	taskID, _, err := m.allocator.allocN(2)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because allocate id fail", zap.String("view", view.String()),
			zap.Error(err))
		return
	}
	collection, err := m.handler.GetCollection(ctx, view.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because get collection fail", zap.String("view", view.String()),
			zap.Error(err))
		return
	}

	expectedSegmentSize := m.getExpectedSegmentSize(collection)

	_, totalRows, maxSegmentRows, preferSegmentRows, err := calculateClusteringCompactionConfig(collection, view, expectedSegmentSize)
	if err != nil {
		log.Warn("Failed to calculate cluster compaction config fail", zap.String("view", view.String()), zap.Error(err))
		return
	}

	resultSegmentNum := totalRows / preferSegmentRows * 2
	start, end, err := m.allocator.allocN(resultSegmentNum)
	if err != nil {
		log.Warn("pre-allocate result segments failed", zap.String("view", view.String()), zap.Error(err))
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
		ResultSegments:     []int64{start, end}, // pre-allocated result segments range
		MaxSegmentRows:     maxSegmentRows,
		PreferSegmentRows:  preferSegmentRows,
		TotalRows:          totalRows,
		AnalyzeTaskID:      taskID + 1,
		LastStateStartTime: time.Now().Unix(),
	}
	err = m.compactionHandler.enqueueCompaction(task)
	if err != nil {
		log.Warn("Failed to execute compaction task",
			zap.Int64("collection", task.CollectionID),
			zap.Int64("planID", task.GetPlanID()),
			zap.Int64s("segmentIDs", task.GetInputSegments()),
			zap.Error(err))
		return
	}
	log.Info("Finish to submit a clustering compaction task",
		zap.Int64("taskID", taskID),
		zap.Int64("planID", task.GetPlanID()),
		zap.String("type", task.GetType().String()),
		zap.Int64("MaxSegmentRows", task.MaxSegmentRows),
		zap.Int64("PreferSegmentRows", task.PreferSegmentRows),
	)
}

func (m *CompactionTriggerManager) SubmitSingleViewToScheduler(ctx context.Context, view CompactionView) {
	taskID, _, err := m.allocator.allocN(2)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because allocate id fail", zap.String("view", view.String()))
		return
	}
	collection, err := m.handler.GetCollection(ctx, view.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because get collection fail", zap.String("view", view.String()))
		return
	}
	var totalRows int64 = 0
	for _, s := range view.GetSegmentsView() {
		totalRows += s.NumOfRows
	}
	task := &datapb.CompactionTask{
		PlanID:             taskID,
		TriggerID:          view.(*MixSegmentView).triggerID,
		State:              datapb.CompactionTaskState_pipelining,
		StartTime:          time.Now().Unix(),
		CollectionTtl:      view.(*MixSegmentView).collectionTTL.Nanoseconds(),
		TimeoutInSeconds:   Params.DataCoordCfg.ClusteringCompactionTimeoutInSeconds.GetAsInt32(),
		Type:               datapb.CompactionType_MixCompaction, // todo: use SingleCompaction
		CollectionID:       view.GetGroupLabel().CollectionID,
		PartitionID:        view.GetGroupLabel().PartitionID,
		Channel:            view.GetGroupLabel().Channel,
		Schema:             collection.Schema,
		InputSegments:      lo.Map(view.GetSegmentsView(), func(segmentView *SegmentView, _ int) int64 { return segmentView.ID }),
		ResultSegments:     []int64{taskID + 1},
		TotalRows:          totalRows,
		LastStateStartTime: time.Now().Unix(),
	}
	err = m.compactionHandler.enqueueCompaction(task)
	if err != nil {
		log.Warn("Failed to execute compaction task",
			zap.Int64("collection", task.CollectionID),
			zap.Int64("planID", task.GetPlanID()),
			zap.Int64s("segmentIDs", task.GetInputSegments()),
			zap.Error(err))
	}
	log.Info("Finish to submit a single compaction task",
		zap.Int64("taskID", taskID),
		zap.Int64("planID", task.GetPlanID()),
		zap.String("type", task.GetType().String()),
	)
}

func (m *CompactionTriggerManager) getExpectedSegmentSize(collection *collectionInfo) int64 {
	indexInfos := m.meta.indexMeta.GetIndexesForCollection(collection.ID, "")

	vectorFields := typeutil.GetVectorFieldSchemas(collection.Schema)
	fieldIndexTypes := lo.SliceToMap(indexInfos, func(t *model.Index) (int64, indexparamcheck.IndexType) {
		return t.FieldID, GetIndexType(t.IndexParams)
	})
	vectorFieldsWithDiskIndex := lo.Filter(vectorFields, func(field *schemapb.FieldSchema, _ int) bool {
		if indexType, ok := fieldIndexTypes[field.FieldID]; ok {
			return indexparamcheck.IsDiskIndex(indexType)
		}
		return false
	})

	allDiskIndex := len(vectorFields) == len(vectorFieldsWithDiskIndex)
	if allDiskIndex {
		// Only if all vector fields index type are DiskANN, recalc segment max size here.
		return Params.DataCoordCfg.DiskSegmentMaxSize.GetAsInt64() * 1024 * 1024
	}
	// If some vector fields index type are not DiskANN, recalc segment max size using default policy.
	return Params.DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024
}

// chanPartSegments is an internal result struct, which is aggregates of SegmentInfos with same collectionID, partitionID and channelName
type chanPartSegments struct {
	collectionID UniqueID
	partitionID  UniqueID
	channelName  string
	segments     []*SegmentInfo
}
