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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/logutil"
)

type CompactionTriggerType int8

const (
	TriggerTypeLevelZeroViewChange CompactionTriggerType = iota + 1
	TriggerTypeClustering
	TriggerTypeDeltaTooMuch
)

const (
	Force         = true
	NotForce      = false
	AllCollection = -1
)

type TriggerManager interface {
	Start()
	Stop()
	IDLETrigger(ctx context.Context, collectionID int64)
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

	meta *meta

	clusteringPolicy *clusteringCompactionPolicy // TODO
	policies         map[CompactionTriggerType]CompactionPolicy

	closeCh     lifetime.SafeChan
	closeWaiter sync.WaitGroup
}

func NewCompactionTriggerManager(alloc allocator, handler Handler, compactionHandler compactionPlanContext, meta *meta) *CompactionTriggerManager {
	m := &CompactionTriggerManager{
		allocator:         alloc,
		handler:           handler,
		compactionHandler: compactionHandler,
		meta:              meta,
		policies:          make(map[CompactionTriggerType]CompactionPolicy),
		closeCh:           lifetime.NewSafeChan(),
	}

	m.registerPolicy(TriggerTypeLevelZeroViewChange, &L0CompactionPolicy{})
	m.registerPolicy(TriggerTypeDeltaTooMuch, &DeltaTooMuchCompactionPolicy{})
	m.clusteringPolicy = newClusteringCompactionPolicy(meta, m.allocator, m.handler)
	return m
}

func (m *CompactionTriggerManager) Start() {
	m.closeWaiter.Add(1)
	go m.startLoop()
}

func (m *CompactionTriggerManager) Close() {
	m.closeCh.Close()
	m.closeWaiter.Wait()
}

// TODO: register clustering policy here
func (m *CompactionTriggerManager) registerPolicy(triggerType CompactionTriggerType, policy CompactionPolicy) {
	m.policies[triggerType] = policy
}

func (m *CompactionTriggerManager) startLoop() {
	defer logutil.LogPanic()
	defer m.closeWaiter.Done()

	clusteringTicker := time.NewTicker(Params.DataCoordCfg.ClusteringCompactionTriggerInterval.GetAsDuration(time.Second))
	defer clusteringTicker.Stop()
	normalTicker := time.NewTicker(Params.DataCoordCfg.GlobalCompactionInterval.GetAsDuration(time.Second))
	defer normalTicker.Stop()
	idleTicker := time.NewTicker(Params.DataCoordCfg.GlobalCompactionInterval.GetAsDuration(3 * time.Second))
	defer idleTicker.Stop()
	log.Info("Compaction trigger manager start")
	for {
		select {
		case <-m.closeCh.CloseCh():
			log.Info("Compaction trigger manager checkLoop quit")
			return
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
		case <-normalTicker.C:
			m.TimelyTrigger()
		case <-idleTicker.C:
			if m.compactionHandler.isEmpty() {
				m.IDLETrigger(AllCollection)
			}
		}
	}
}

func (m *CompactionTriggerManager) TimelyTrigger() {
	ctx := context.Background()

	collections := m.meta.GetCollections()
	for _, collection := range collections {
		collID := collection.ID
		collInfo, err := m.handler.GetCollection(ctx, collID)
		if err != nil || collInfo == nil {
			log.Warn("compaction trigger fail to get collection from handler")
			continue
		}

		if !isCollectionAutoCompactionEnabled(collInfo) {
			log.RatedInfo(20, "collection auto compaction disabled")
			continue
		}

		views := m.getSeperateViews(ctx, collInfo, NotForce)
		for tType, views := range views {
			m.trigger(ctx, tType, NotForce, views...)
		}
	}
}

func (m *CompactionTriggerManager) IDLETrigger(collectionID int64) {
	ctx := context.Background()
	collIDs := []int64{}
	if collectionID == AllCollection {
		collIDs = lo.Map(m.meta.GetCollections(), func(coll *collectionInfo, _ int) int64 {
			return coll.ID
		})
	} else {
		collIDs = []int64{collectionID}
	}

	for _, collectionID := range collIDs {
		collInfo, err := m.handler.GetCollection(ctx, collectionID)
		if err != nil || collInfo == nil {
			log.Warn("compaction trigger fail to get collection from handler", zap.Int64("collectionID", collectionID))
			return
		}

		if !isCollectionAutoCompactionEnabled(collInfo) {
			log.RatedInfo(20, "collection auto compaction disabled", zap.Int64("collectionID", collectionID))
			return
		}

		views := m.getSeperateViews(ctx, collInfo, Force)
		for tType, views := range views {
			m.trigger(ctx, tType, Force, views...)
		}
	}
}

func (m *CompactionTriggerManager) ManualTrigger(ctx context.Context, collectionID int64, triggerType CompactionTriggerType) (UniqueID, error) {
	log.Info("receive manual trigger",
		zap.Int64("collectionID", collectionID),
		zap.Any("triggerType", triggerType))

	if triggerType == TriggerTypeClustering {
		views, triggerID, err := m.clusteringPolicy.triggerOneCollection(context.Background(), collectionID, true)
		if err != nil {
			return 0, err
		}

		if len(views) > 0 {
			m.notify(ctx, triggerType, views)
		}
		return triggerID, nil
	}

	return 0, errors.Newf("Trigger type unsupported: %d", triggerType)
}

func (m *CompactionTriggerManager) trigger(ctx context.Context, triggerType CompactionTriggerType, isForce bool, views ...CompactionView) {
	log := log.Ctx(ctx).With(zap.Bool("isForce", isForce), zap.Any("triggerType", triggerType))
	for _, view := range views {
		t, ok := m.policies[triggerType]
		if !ok {
			log.Warn("policy not found for trigger type")
			continue
		}

		if !t.Enabled() {
			log.Warn("policy not enabled for trigger type")
		}

		if m.compactionHandler.isFull() {
			log.RatedInfo(10, "Skip trigger compaction since compactionHandler is full")
			return
		}

		var output CompactionView
		var reason string
		if isForce {
			output, reason = t.ForceTrigger(view)
		} else {
			output, reason = t.Trigger(view)
		}

		if output != nil {
			log.Info("Success to trigger compaction",
				zap.Any("reason", reason),
				zap.String("ouput", output.String()))

			task, err := m.buildTaskFromView(ctx, triggerType, output)
			if err != nil {
				log.Warn("build compaction task failed", zap.Error(err))
				continue
			}

			if err = m.compactionHandler.enqueueCompaction(task); err != nil {
				log.Warn("enqueue compaction failed", zap.Error(err))
				continue
			}
		}
	}
}

func (m *CompactionTriggerManager) getSeperateViews(ctx context.Context, collInfo *collectionInfo, isForce bool) map[CompactionTriggerType][]CompactionView {
	result := make(map[CompactionTriggerType][]CompactionView)
	partSegments := m.meta.GetSegmentsChanPart(func(segment *SegmentInfo) bool {
		return segment.CollectionID == collInfo.ID &&
			isSegmentHealthy(segment) &&
			isFlush(segment) &&
			!segment.isCompacting && // not compacting now
			!segment.GetIsImporting() // not importing now
	})

	collectionTTL, err := getCollectionTTL(collInfo.Properties)
	if err != nil {
		log.Warn("get collection ttl failed, skip to handle compaction", zap.Int64("collectionID", collInfo.ID), zap.Error(err))
		return nil
	}

	newTriggerID, err := m.allocator.allocID(ctx)
	if err != nil {
		log.Warn("fail to allocate triggerID", zap.Error(err))
		return nil
	}

	for _, group := range partSegments {
		label := &CompactionGroupLabel{group.collectionID, group.partitionID, group.channelName}

		// process L0 segments
		l0Segments := lo.Filter(group.segments, func(seg *SegmentInfo, _ int) bool {
			return seg.GetLevel() == datapb.SegmentLevel_L0
		})
		l0Views := m.getLevelZeroViews(label, l0Segments)
		result = lo.Assign(l0Views, result)

		// TODO: Only L0 Compaction can be force triggered now
		if isForce {
			continue
		}

		// process L2 segments
		if Params.DataCoordCfg.IndexBasedCompaction.GetAsBool() {
			group.segments = FilterInIndexedSegments(m.handler, m.meta, group.segments...)
		}

		l2Segments := lo.Filter(group.segments, func(seg *SegmentInfo, _ int) bool {
			return seg.GetLevel() == datapb.SegmentLevel_L2
		})
		if len(l2Segments) > 0 {
			for _, segment := range l2Segments {
				view := &MixSegmentView{
					label:         label,
					segments:      GetViewsByInfo(segment),
					collectionTTL: collectionTTL,
					triggerID:     newTriggerID,
				}

				if _, ok := result[TriggerTypeDeltaTooMuch]; !ok {
					result[TriggerTypeDeltaTooMuch] = make([]CompactionView, 0)
				}
				result[TriggerTypeDeltaTooMuch] = append(result[TriggerTypeDeltaTooMuch], view)
			}
		}

		// TODO L1 segment and mix comapction, L1 segment and clustering compaction
	}

	return result
}

func (m *CompactionTriggerManager) getLevelZeroViews(label *CompactionGroupLabel, l0Segments []*SegmentInfo) map[CompactionTriggerType][]CompactionView {
	result := make(map[CompactionTriggerType][]CompactionView)
	if len(l0Segments) > 0 {
		earliestGrowingSegmentPos := m.meta.GetEarliestStartPositionOfGrowingSegments(label)

		view := &LevelZeroSegmentsView{
			label:                     label,
			segments:                  GetViewsByInfo(l0Segments...),
			earliestGrowingSegmentPos: earliestGrowingSegmentPos,
		}

		if _, ok := result[TriggerTypeLevelZeroViewChange]; !ok {
			result[TriggerTypeLevelZeroViewChange] = make([]CompactionView, 0)
		}
		result[TriggerTypeLevelZeroViewChange] = append(result[TriggerTypeLevelZeroViewChange], view)
	}
	return result
}

func (m *CompactionTriggerManager) buildTaskFromView(ctx context.Context, triggerType CompactionTriggerType, view CompactionView) (*datapb.CompactionTask, error) {
	log := log.With(zap.Any("triggerType", triggerType), zap.String("view", view.String()))
	taskID, err := m.allocator.allocID(ctx)
	if err != nil {
		log.Warn("Failed to build compaction view to scheduler because allocate id fail", zap.Error(err))
		return nil, err
	}

	collection, err := m.handler.GetCollection(ctx, view.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because get collection fail", zap.Error(err))
		return nil, err
	}
	switch triggerType {
	case TriggerTypeLevelZeroViewChange:
		inputSegments := lo.Map(view.GetSegmentsView(), func(segView *SegmentView, _ int) int64 {
			return segView.ID
		})
		task := &datapb.CompactionTask{
			TriggerID:        taskID, // inner trigger, use task id as trigger id
			PlanID:           taskID,
			Type:             datapb.CompactionType_Level0DeleteCompaction,
			StartTime:        time.Now().Unix(),
			InputSegments:    inputSegments,
			State:            datapb.CompactionTaskState_pipelining,
			Channel:          view.GetGroupLabel().Channel,
			CollectionID:     view.GetGroupLabel().CollectionID,
			PartitionID:      view.GetGroupLabel().PartitionID,
			Pos:              view.(*LevelZeroSegmentsView).earliestGrowingSegmentPos, // Choose the smaller of earliest growing segment and choosing l0 segments msgPos
			TimeoutInSeconds: Params.DataCoordCfg.CompactionTimeoutInSeconds.GetAsInt32(),
			Schema:           collection.Schema,
		}
		return task, nil

	case TriggerTypeDeltaTooMuch:
		expectedSize := getExpectedSegmentSize(m.meta, collection)
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
			TotalRows:          lo.SumBy(view.GetSegmentsView(), func(segmentView *SegmentView) int64 { return segmentView.NumOfRows }),
			LastStateStartTime: time.Now().Unix(),
			MaxSize:            getExpandedSize(expectedSize),
		}

		return task, nil
	}

	return nil, errors.Newf("invalid trigger type: %v", triggerType)
}

func (m *CompactionTriggerManager) notify(ctx context.Context, eventType CompactionTriggerType, views []CompactionView) {
	if eventType != TriggerTypeClustering {
		log.Warn("Only ClusteringCompaction is supported in notify now")
		return
	}

	for _, view := range views {
		log.Debug("Try to trigger a compaction", zap.String("view", view.String()), zap.String("label", view.GetGroupLabel().String()))
		if view == nil {
			continue
		}

		log.Info("Success to trigger a ClusteringCompaction output view, try to submit",
			zap.String("label", view.GetGroupLabel().String()),
			zap.String("output view", view.String()))
		m.SubmitClusteringViewToScheduler(ctx, view)
	}
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

func getExpectedSegmentSize(meta *meta, collInfo *collectionInfo) int64 {
	allDiskIndex := meta.indexMeta.AreAllDiskIndex(collInfo.ID, collInfo.Schema)
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
