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
	"fmt"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/logutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type CompactionTriggerType int8

const (
	TriggerTypeLevelZeroViewChange CompactionTriggerType = iota + 1
	TriggerTypeLevelZeroViewIDLE
	TriggerTypeLevelZeroViewManual
	TriggerTypeSegmentSizeViewChange
	TriggerTypeClustering
	TriggerTypeSingle
	TriggerTypeSort
	TriggerTypeForceMerge
	TriggerTypeStorageVersionUpgrade
	TriggerTypeBackfill
)

type TickerType int8

const (
	L0Ticker TickerType = iota + 1
	ClusteringTicker
	SingleTicker
	BackfillTicker
	StorageVersionTicker
)

func (t CompactionTriggerType) GetCompactionType() datapb.CompactionType {
	switch t {
	case TriggerTypeLevelZeroViewChange, TriggerTypeLevelZeroViewIDLE, TriggerTypeLevelZeroViewManual:
		return datapb.CompactionType_Level0DeleteCompaction
	case TriggerTypeSegmentSizeViewChange, TriggerTypeSingle, TriggerTypeForceMerge:
		return datapb.CompactionType_MixCompaction
	case TriggerTypeClustering:
		return datapb.CompactionType_ClusteringCompaction
	case TriggerTypeSort:
		return datapb.CompactionType_SortCompaction
	case TriggerTypeStorageVersionUpgrade:
		return datapb.CompactionType_MixCompaction
	case TriggerTypeBackfill:
		return datapb.CompactionType_BackfillCompaction
	default:
		return datapb.CompactionType_MixCompaction
	}
}

func (t CompactionTriggerType) String() string {
	switch t {
	case TriggerTypeLevelZeroViewChange:
		return "LevelZeroViewChange"
	case TriggerTypeLevelZeroViewIDLE:
		return "LevelZeroViewIDLE"
	case TriggerTypeLevelZeroViewManual:
		return "LevelZeroViewManual"
	case TriggerTypeSegmentSizeViewChange:
		return "SegmentSizeViewChange"
	case TriggerTypeClustering:
		return "Clustering"
	case TriggerTypeSingle:
		return "Single"
	case TriggerTypeSort:
		return "Sort"
	case TriggerTypeForceMerge:
		return "ForceMerge"
	case TriggerTypeStorageVersionUpgrade:
		return "StorageVersionUpgrade"
	case TriggerTypeBackfill:
		return "Backfill"
	default:
		return ""
	}
}

// CompactionPolicy defines the interface for different compaction policies
type CompactionPolicy interface {
	// Enable returns whether this compaction policy is enabled
	Enable() bool
	// TriggerInline returns views that can be applied inline (without inspector slots).
	// Called unconditionally before the isFull() check so metadata-only updates always
	// proceed regardless of inspector capacity. Non-backfill policies return an empty map.
	TriggerInline(ctx context.Context) (map[CompactionTriggerType][]CompactionView, error)
	// Trigger returns views that require inspector slots (actual compaction tasks).
	// Only called when the inspector is not full.
	Trigger(ctx context.Context) (map[CompactionTriggerType][]CompactionView, error)
	// Name returns the name of this compaction policy
	Name() string
}

type TriggerManager interface {
	Start()
	Stop()
	OnCollectionUpdate(collectionID int64)
	ManualTrigger(ctx context.Context, collectionID int64, clusteringCompaction bool, l0Compaction bool, targetSize int64) (UniqueID, error)
	InitForceMergeMemoryQuerier(nodeManager session.NodeManager, mixCoord types.MixCoord, session sessionutil.SessionInterface)
}

var _ TriggerManager = (*CompactionTriggerManager)(nil)

// CompactionTriggerManager registers Triggers to TriggerType
// so that when the certain TriggerType happens, the corresponding triggers can
// trigger the correct compaction plans.
type CompactionTriggerManager struct {
	inspector CompactionInspector
	handler   Handler
	allocator allocator.Allocator

	meta     *meta
	policies map[TickerType]CompactionPolicy

	l0Policy                    *l0CompactionPolicy
	clusteringPolicy            *clusteringCompactionPolicy
	singlePolicy                *singleCompactionPolicy
	forceMergePolicy            *forceMergeCompactionPolicy
	upgradeStorageVersionPolicy *storageVersionUpgradePolicy
	backfillPolicy              *backfillCompactionPolicy

	cancel  context.CancelFunc
	closeWg sync.WaitGroup
}

func NewCompactionTriggerManager(alloc allocator.Allocator, handler Handler, inspector CompactionInspector, meta *meta,
	versionManager IndexEngineVersionManager,
) *CompactionTriggerManager {
	m := &CompactionTriggerManager{
		allocator: alloc,
		handler:   handler,
		inspector: inspector,
		meta:      meta,
		policies:  make(map[TickerType]CompactionPolicy),
	}
	// Initialize policies and keep separate pointers for frequently accessed ones

	m.l0Policy = newL0CompactionPolicy(meta, alloc)
	m.clusteringPolicy = newClusteringCompactionPolicy(meta, m.allocator, m.handler)
	m.singlePolicy = newSingleCompactionPolicy(meta, m.allocator, m.handler)

	m.forceMergePolicy = newForceMergeCompactionPolicy(meta, m.allocator, m.handler)
	m.upgradeStorageVersionPolicy = newStorageVersionUpgradePolicy(meta, m.allocator, m.handler, versionManager)
	m.backfillPolicy = newBackfillCompactionPolicy(meta, m.allocator, m.handler)

	// Initialize policies map for ticker handling
	m.policies[L0Ticker] = m.l0Policy
	m.policies[ClusteringTicker] = m.clusteringPolicy
	m.policies[SingleTicker] = m.singlePolicy
	m.policies[BackfillTicker] = m.backfillPolicy
	m.policies[StorageVersionTicker] = m.upgradeStorageVersionPolicy
	return m
}

// InitForceMergeMemoryQuerier initializes the topology querier for force merge auto calculation
func (m *CompactionTriggerManager) InitForceMergeMemoryQuerier(nodeManager session.NodeManager, mixCoord types.MixCoord, session sessionutil.SessionInterface) {
	if m.forceMergePolicy != nil {
		querier := newMetricsNodeMemoryQuerier(nodeManager, mixCoord, session)
		m.forceMergePolicy.SetTopologyQuerier(querier)
	}
}

// OnCollectionUpdate notifies L0Policy about latest collection's L0 segment changes
// This tells the l0 triggers about which collections are active
func (m *CompactionTriggerManager) OnCollectionUpdate(collectionID int64) {
	m.l0Policy.OnCollectionUpdate(collectionID)
}

func (m *CompactionTriggerManager) Start() {
	m.closeWg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	go func() {
		defer m.closeWg.Done()
		m.loop(ctx)
	}()
}

func (m *CompactionTriggerManager) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	m.closeWg.Wait()
}

func (m *CompactionTriggerManager) loop(ctx context.Context) {
	defer logutil.LogPanic()

	log := log.Ctx(ctx)
	l0Ticker := time.NewTicker(Params.DataCoordCfg.L0CompactionTriggerInterval.GetAsDuration(time.Second))
	defer l0Ticker.Stop()
	clusteringTicker := time.NewTicker(Params.DataCoordCfg.ClusteringCompactionTriggerInterval.GetAsDuration(time.Second))
	defer clusteringTicker.Stop()
	singleTicker := time.NewTicker(Params.DataCoordCfg.MixCompactionTriggerInterval.GetAsDuration(time.Second))
	defer singleTicker.Stop()
	storageVersionTicker := time.NewTicker(Params.DataCoordCfg.MixCompactionTriggerInterval.GetAsDuration(time.Second))
	defer storageVersionTicker.Stop()
	backfillTicker := time.NewTicker(Params.DataCoordCfg.BackfillCompactionTriggerInterval.GetAsDuration(time.Second))
	defer backfillTicker.Stop()
	log.Info("Compaction trigger manager start")
	for {
		select {
		case <-ctx.Done():
			log.Info("Compaction trigger manager checkLoop quit")
			return
		case <-l0Ticker.C:
			m.handleTicker(ctx, L0Ticker)
		case <-clusteringTicker.C:
			m.handleTicker(ctx, ClusteringTicker)
		case <-singleTicker.C:
			m.handleTicker(ctx, SingleTicker)
		case <-storageVersionTicker.C:
			m.handleTicker(ctx, StorageVersionTicker)
		case <-backfillTicker.C:
			m.handleTicker(ctx, BackfillTicker)
		case segID := <-getStatsTaskChSingleton():
			log.Info("receive new segment to trigger sort compaction", zap.Int64("segmentID", segID))
			view := m.singlePolicy.triggerSegmentSortCompaction(ctx, segID)
			if view == nil {
				log.Warn("segment no need to do sort compaction", zap.Int64("segmentID", segID))
				continue
			}
			if m.meta.GetSegment(ctx, segID) == nil {
				log.Warn("segment not found", zap.Int64("segmentID", segID))
				continue
			}
			m.notify(ctx, TriggerTypeSort, []CompactionView{view})
		}
	}
}

func (m *CompactionTriggerManager) handleTicker(ctx context.Context, tickerType TickerType) {
	policy, exists := m.policies[tickerType]
	if !exists {
		log.Warn("Policy not found for ticker type", zap.Any("tickerType", tickerType))
		return
	}

	if !policy.Enable() {
		return
	}

	// Step 1: apply inline views unconditionally — these never need inspector slots
	// (e.g. backfill metadata-only schema-version bumps) and must proceed even when
	// the inspector queue is full.
	inlineEvents, err := policy.TriggerInline(ctx)
	if err != nil {
		log.Warn("Fail to trigger inline policy", zap.String("policy", policy.Name()), zap.Error(err))
		return
	}
	m.executeInline(ctx, inlineEvents)

	// Step 2: gate normal compaction dispatch on inspector capacity.
	if m.inspector.isFull() {
		log.RatedInfo(10, "Skip dispatching compaction events since inspector is full",
			zap.String("policy", policy.Name()))
		return
	}

	// Step 3: trigger and dispatch views that require inspector slots.
	events, err := policy.Trigger(ctx)
	if err != nil {
		log.Warn("Fail to trigger policy", zap.String("policy", policy.Name()), zap.Error(err))
		return
	}
	for triggerType, views := range events {
		if len(views) == 0 {
			continue
		}
		m.notify(ctx, triggerType, views)
	}
}

// executeInline applies all views returned by TriggerInline directly inside datacoord
// without consuming inspector slots or notifying the scheduler.
func (m *CompactionTriggerManager) executeInline(ctx context.Context, events map[CompactionTriggerType][]CompactionView) {
	for _, views := range events {
		for _, view := range views {
			m.applyInlineView(ctx, view)
		}
	}
}

// applyInlineView applies a CompactionView whose IsInlineExecutable() == true
// directly inside datacoord. Today this is only used by backfillCompactionPolicy
// for the metadata-only path: bump segment SchemaVersion via meta.UpdateSegment
// without producing any compaction task.
func (m *CompactionTriggerManager) applyInlineView(ctx context.Context, view CompactionView) {
	bv, ok := view.(*BackfillSegmentsView)
	if !ok {
		log.Ctx(ctx).Warn("unexpected inline-executable view type, skip",
			zap.String("actualType", fmt.Sprintf("%T", view)))
		return
	}
	for _, sv := range bv.GetSegmentsView() {
		if err := m.meta.UpdateSegment(sv.ID, SetSchemaVersion(bv.targetSchemaVersion)); err != nil {
			log.Ctx(ctx).Error("failed to apply inline backfill schema version update",
				zap.Int64("segmentID", sv.ID),
				zap.Int32("newSchemaVersion", bv.targetSchemaVersion),
				zap.Error(err))
			continue
		}
		log.Ctx(ctx).Info("applied inline backfill schema version update",
			zap.Int64("segmentID", sv.ID),
			zap.Int32("newSchemaVersion", bv.targetSchemaVersion))
	}
}

func (m *CompactionTriggerManager) ManualTrigger(ctx context.Context, collectionID int64, isClustering bool, isL0 bool, targetSize int64) (UniqueID, error) {
	log.Ctx(ctx).Info("receive manual trigger",
		zap.Int64("collectionID", collectionID),
		zap.Bool("is clustering", isClustering),
		zap.Bool("is l0", isL0),
		zap.Int64("targetSize", targetSize))

	collection, err := m.handler.GetCollection(ctx, collectionID)
	if err != nil {
		return 0, err
	}
	if collection == nil {
		return 0, merr.WrapErrCollectionNotFound(collectionID)
	}
	if collection.IsExternal() {
		return 0, merr.WrapErrParameterInvalidMsg(
			"compaction is not supported for external collection")
	}

	var triggerID UniqueID
	var views []CompactionView

	events := make(map[CompactionTriggerType][]CompactionView, 0)
	if targetSize != 0 {
		views, triggerID, err = m.forceMergePolicy.triggerOneCollection(ctx, collectionID, targetSize)
		events[TriggerTypeForceMerge] = views
	} else if isL0 {
		views, triggerID, err = m.l0Policy.triggerOneCollection(ctx, collectionID)
		events[TriggerTypeLevelZeroViewManual] = views
	} else if isClustering {
		views, triggerID, err = m.clusteringPolicy.triggerOneCollection(ctx, collectionID, true)
		events[TriggerTypeClustering] = views
	}
	if err != nil {
		return 0, err
	}
	if len(events) > 0 {
		for triggerType, views := range events {
			m.notify(ctx, triggerType, views)
		}
	}
	return triggerID, nil
}

func (m *CompactionTriggerManager) triggerViewForCompaction(ctx context.Context, eventType CompactionTriggerType,
	view CompactionView,
) ([]CompactionView, string) {
	switch eventType {
	case TriggerTypeLevelZeroViewIDLE:
		view, reason := view.ForceTrigger()
		return []CompactionView{view}, reason

	case TriggerTypeLevelZeroViewManual, TriggerTypeForceMerge:
		return view.ForceTriggerAll()

	default:
		outView, reason := view.Trigger()
		return []CompactionView{outView}, reason
	}
}

func (m *CompactionTriggerManager) notify(ctx context.Context, eventType CompactionTriggerType, views []CompactionView) {
	log := log.Ctx(ctx)
	log.Debug("Start to trigger compactions", zap.String("eventType", eventType.String()))
	for _, view := range views {
		outViews, reason := m.triggerViewForCompaction(ctx, eventType, view)
		for _, outView := range outViews {
			if outView != nil {
				log.Info("Success to trigger a compaction, try to submit",
					zap.String("eventType", eventType.String()),
					zap.String("reason", reason),
					zap.String("output view", outView.String()),
					zap.Int64("triggerID", outView.GetTriggerID()))

				switch eventType {
				case TriggerTypeLevelZeroViewChange, TriggerTypeLevelZeroViewIDLE, TriggerTypeLevelZeroViewManual:
					m.SubmitL0ViewToScheduler(ctx, outView)
				case TriggerTypeClustering:
					m.SubmitClusteringViewToScheduler(ctx, outView)
				case TriggerTypeSingle, TriggerTypeSort, TriggerTypeStorageVersionUpgrade:
					m.SubmitSingleViewToScheduler(ctx, outView, eventType)
				case TriggerTypeForceMerge:
					m.SubmitForceMergeViewToScheduler(ctx, outView)
				case TriggerTypeBackfill:
					m.SubmitBackfillViewToScheduler(ctx, outView)
				}
			}
		}
	}
}

func (m *CompactionTriggerManager) SubmitL0ViewToScheduler(ctx context.Context, view CompactionView) {
	log := log.Ctx(ctx).With(zap.String("view", view.String()))
	taskID, err := m.allocator.AllocID(ctx)
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
	if collection == nil {
		log.Warn("collection not found when submitting l0 compaction view", zap.Int64("collectionID", view.GetGroupLabel().CollectionID))
		return
	}
	if collection.IsExternal() {
		log.Info("skip submitting l0 compaction for external collection", zap.Int64("collectionID", collection.ID))
		return
	}

	totalRows := lo.SumBy(view.GetSegmentsView(), func(segView *SegmentView) int64 {
		return segView.NumOfRows
	})

	task := &datapb.CompactionTask{
		TriggerID:     view.GetTriggerID(),
		PlanID:        taskID,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		StartTime:     time.Now().Unix(),
		TotalRows:     totalRows,
		InputSegments: levelZeroSegs,
		State:         datapb.CompactionTaskState_pipelining,
		Channel:       view.GetGroupLabel().Channel,
		CollectionID:  view.GetGroupLabel().CollectionID,
		PartitionID:   view.GetGroupLabel().PartitionID,
		Pos:           view.(*LevelZeroCompactionView).latestDeletePos,
		Schema:        collection.Schema,
	}

	err = m.inspector.enqueueCompaction(task)
	if err != nil {
		log.Warn("Failed to execute compaction task",
			zap.Int64("triggerID", task.GetTriggerID()),
			zap.Int64("planID", task.GetPlanID()),
			zap.Int64s("segmentIDs", task.GetInputSegments()),
			zap.Error(err))
		return
	}
	log.Info("Finish to submit a LevelZeroCompaction plan",
		zap.Int64("triggerID", task.GetTriggerID()),
		zap.Int64("planID", task.GetPlanID()),
		zap.String("type", task.GetType().String()),
		zap.Int64s("L0 segments", levelZeroSegs),
	)
}

func (m *CompactionTriggerManager) SubmitClusteringViewToScheduler(ctx context.Context, view CompactionView) {
	log := log.Ctx(ctx).With(zap.String("view", view.String()))
	taskID, _, err := m.allocator.AllocN(2)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because allocate id fail", zap.Error(err))
		return
	}
	collection, err := m.handler.GetCollection(ctx, view.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because get collection fail", zap.Error(err))
		return
	}
	if collection == nil {
		log.Warn("collection not found when submitting clustering compaction", zap.Int64("collectionID", view.GetGroupLabel().CollectionID))
		return
	}
	if collection.IsExternal() {
		log.Info("skip submitting clustering compaction for external collection", zap.Int64("collectionID", collection.ID))
		return
	}

	expectedSegmentSize := getExpectedSegmentSize(m.meta, collection.ID, collection.Schema)
	totalRows, maxSegmentRows, preferSegmentRows, err := calculateClusteringCompactionConfig(collection, view, expectedSegmentSize)
	if err != nil {
		log.Warn("Failed to calculate cluster compaction config fail", zap.Error(err))
		return
	}

	resultSegmentNum := (totalRows/preferSegmentRows + 1) * 2
	n := resultSegmentNum * paramtable.Get().DataCoordCfg.CompactionPreAllocateIDExpansionFactor.GetAsInt64()
	start, end, err := m.allocator.AllocN(n)
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
		Type:               datapb.CompactionType_ClusteringCompaction,
		CollectionID:       view.GetGroupLabel().CollectionID,
		PartitionID:        view.GetGroupLabel().PartitionID,
		Channel:            view.GetGroupLabel().Channel,
		Schema:             collection.Schema,
		ClusteringKeyField: view.(*ClusteringSegmentsView).clusteringKeyField,
		InputSegments:      lo.Map(view.GetSegmentsView(), func(segmentView *SegmentView, _ int) int64 { return segmentView.ID }),
		ResultSegments:     []int64{},
		MaxSegmentRows:     maxSegmentRows,
		PreferSegmentRows:  preferSegmentRows,
		TotalRows:          totalRows,
		AnalyzeTaskID:      taskID + 1,
		LastStateStartTime: time.Now().Unix(),
		PreAllocatedSegmentIDs: &datapb.IDRange{
			Begin: start,
			End:   end,
		},
		MaxSize: expectedSegmentSize,
	}
	err = m.inspector.enqueueCompaction(task)
	if err != nil {
		log.Warn("Failed to execute compaction task",
			zap.Int64("planID", task.GetPlanID()),
			zap.Error(err))
		return
	}
	log.Info("Finish to submit a clustering compaction task",
		zap.Int64("triggerID", task.GetTriggerID()),
		zap.Int64("planID", task.GetPlanID()),
		zap.Int64("MaxSegmentRows", task.MaxSegmentRows),
		zap.Int64("PreferSegmentRows", task.PreferSegmentRows),
	)
}

func (m *CompactionTriggerManager) SubmitSingleViewToScheduler(ctx context.Context, view CompactionView, triggerType CompactionTriggerType) {
	// single view is definitely one-one mapping
	log := log.Ctx(ctx).With(zap.String("trigger type", triggerType.String()), zap.String("view", view.String()))
	// TODO[GOOSE], 11 = 1 planID + 10 segmentID, this is a hack need to be removed.
	// Any plan that output segment number greater than 10 will be marked as invalid plan for now.
	n := 11 * paramtable.Get().DataCoordCfg.CompactionPreAllocateIDExpansionFactor.GetAsInt64()
	startID, endID, err := m.allocator.AllocN(n)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because allocate id fail", zap.Error(err))
		return
	}

	collection, err := m.handler.GetCollection(ctx, view.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because get collection fail", zap.Error(err))
		return
	}
	if collection == nil {
		log.Warn("collection not found when submitting single compaction", zap.Int64("collectionID", view.GetGroupLabel().CollectionID))
		return
	}
	if collection.IsExternal() {
		log.Info("skip submitting single compaction for external collection", zap.Int64("collectionID", collection.ID))
		return
	}
	var totalRows int64 = 0
	for _, s := range view.GetSegmentsView() {
		totalRows += s.NumOfRows
	}

	expectedSize := getExpectedSegmentSize(m.meta, collection.ID, collection.Schema)
	task := &datapb.CompactionTask{
		PlanID:             startID,
		TriggerID:          view.(*MixSegmentView).triggerID,
		State:              datapb.CompactionTaskState_pipelining,
		StartTime:          time.Now().Unix(),
		CollectionTtl:      view.(*MixSegmentView).collectionTTL.Nanoseconds(),
		Type:               triggerType.GetCompactionType(),
		CollectionID:       view.GetGroupLabel().CollectionID,
		PartitionID:        view.GetGroupLabel().PartitionID,
		Channel:            view.GetGroupLabel().Channel,
		Schema:             collection.Schema,
		InputSegments:      lo.Map(view.GetSegmentsView(), func(segmentView *SegmentView, _ int) int64 { return segmentView.ID }),
		ResultSegments:     []int64{},
		TotalRows:          totalRows,
		LastStateStartTime: time.Now().Unix(),
		MaxSize:            expectedSize,
		PreAllocatedSegmentIDs: &datapb.IDRange{
			Begin: startID + 1,
			End:   endID,
		},
	}
	err = m.inspector.enqueueCompaction(task)
	if err != nil {
		log.Warn("Failed to execute compaction task",
			zap.Int64("triggerID", task.GetTriggerID()),
			zap.Int64("planID", task.GetPlanID()),
			zap.Int64s("segmentIDs", task.GetInputSegments()),
			zap.Error(err))
		return
	}
	log.Info("Finish to submit a single compaction task",
		zap.Int64("triggerID", task.GetTriggerID()),
		zap.Int64("planID", task.GetPlanID()),
		zap.String("type", task.GetType().String()),
		zap.Int64("targetSize", task.GetMaxSize()),
	)
}

func (m *CompactionTriggerManager) SubmitForceMergeViewToScheduler(ctx context.Context, view CompactionView) {
	log := log.Ctx(ctx).With(zap.String("view", view.String()))

	taskID, err := m.allocator.AllocID(ctx)
	if err != nil {
		log.Warn("Failed to allocate task ID", zap.Error(err))
		return
	}

	collection, err := m.handler.GetCollection(ctx, view.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn("Failed to get collection", zap.Error(err))
		return
	}

	totalRows := lo.SumBy(view.GetSegmentsView(), func(v *SegmentView) int64 { return v.NumOfRows })

	targetCount := view.(*ForceMergeSegmentView).targetSegmentCount
	n := targetCount * paramtable.Get().DataCoordCfg.CompactionPreAllocateIDExpansionFactor.GetAsInt64()
	startID, endID, err := m.allocator.AllocN(n)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because allocate id fail", zap.Error(err))
		return
	}

	task := &datapb.CompactionTask{
		PlanID:             taskID,
		TriggerID:          view.GetTriggerID(),
		State:              datapb.CompactionTaskState_pipelining,
		StartTime:          time.Now().Unix(),
		CollectionTtl:      view.(*ForceMergeSegmentView).collectionTTL.Nanoseconds(),
		Type:               datapb.CompactionType_MixCompaction,
		CollectionID:       view.GetGroupLabel().CollectionID,
		PartitionID:        view.GetGroupLabel().PartitionID,
		Channel:            view.GetGroupLabel().Channel,
		Schema:             collection.Schema,
		InputSegments:      lo.Map(view.GetSegmentsView(), func(segmentView *SegmentView, _ int) int64 { return segmentView.ID }),
		ResultSegments:     []int64{},
		TotalRows:          totalRows,
		LastStateStartTime: time.Now().Unix(),
		MaxSize:            int64(view.(*ForceMergeSegmentView).targetSegmentSize),
		PreAllocatedSegmentIDs: &datapb.IDRange{
			Begin: startID + 1,
			End:   endID,
		},
	}

	err = m.inspector.enqueueCompaction(task)
	if err != nil {
		log.Warn("Failed to enqueue task", zap.Error(err))
		return
	}

	log.Info("Finish to submit force merge task",
		zap.Int64("planID", taskID),
		zap.Int64("triggerID", task.GetTriggerID()),
		zap.Int64("collectionID", task.GetCollectionID()),
		zap.Int64("targetSize", task.GetMaxSize()),
	)
}

func (m *CompactionTriggerManager) SubmitBackfillViewToScheduler(ctx context.Context, view CompactionView) {
	log := log.Ctx(ctx).With(zap.String("view", view.String()))
	planID, _, err := m.allocator.AllocN(1)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because allocate id fail", zap.Error(err))
		return
	}
	collection, err := m.handler.GetCollection(ctx, view.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn("Failed to submit compaction view to scheduler because get collection fail", zap.Error(err))
		return
	}
	if collection == nil {
		log.Warn("Failed to submit compaction view to scheduler because collection is nil")
		return
	}
	if collection.IsExternal() {
		log.Info("skip submitting backfill compaction for external collection", zap.Int64("collectionID", collection.ID))
		return
	}
	var totalRows int64 = 0
	for _, s := range view.GetSegmentsView() {
		totalRows += s.NumOfRows
	}
	expectedSize := getExpectedSegmentSize(m.meta, collection.ID, collection.Schema)
	bfView, ok := view.(*BackfillSegmentsView)
	if !ok {
		log.Warn("unexpected view type for backfill trigger, expected *BackfillSegmentsView",
			zap.String("actualType", fmt.Sprintf("%T", view)))
		return
	}
	if bfView.funcDiff == nil || len(bfView.funcDiff.Added) != 1 {
		funcCount := 0
		if bfView.funcDiff != nil {
			funcCount = len(bfView.funcDiff.Added)
		}
		log.Warn("backfill view must have exactly one function to backfill",
			zap.Int("funcCount", funcCount))
		return
	}
	task := &datapb.CompactionTask{
		PlanID:       planID,
		TriggerID:    bfView.triggerID,
		State:        datapb.CompactionTaskState_pipelining,
		StartTime:    time.Now().Unix(),
		Type:         datapb.CompactionType_BackfillCompaction,
		CollectionID: view.GetGroupLabel().CollectionID,
		PartitionID:  view.GetGroupLabel().PartitionID,
		Channel:      view.GetGroupLabel().Channel,
		// Use the schema frozen at scan time so completeBackfillCompactionMutation
		// only advances the segment to the version that was actually backfilled.
		// Re-using the live collection.Schema here risks advancing the segment's
		// SchemaVersion beyond what this task backfills if the collection raced
		// ahead between scan and submission (prevented in practice by PR #48989).
		Schema:             bfView.schema,
		InputSegments:      lo.Map(view.GetSegmentsView(), func(segmentView *SegmentView, _ int) int64 { return segmentView.ID }),
		ResultSegments:     []int64{},
		TotalRows:          totalRows,
		LastStateStartTime: time.Now().Unix(),
		MaxSize:            expectedSize,
		DiffFunctions:      bfView.funcDiff.Added,
	}
	err = m.inspector.enqueueCompaction(task)
	if err != nil {
		log.Warn("Failed to execute compaction task",
			zap.Int64("triggerID", task.GetTriggerID()),
			zap.Int64("planID", task.GetPlanID()),
			zap.Int64s("segmentIDs", task.GetInputSegments()),
			zap.Error(err))
		return
	}
	log.Info("Finish to submit a backfill compaction task",
		zap.Int64("triggerID", task.GetTriggerID()),
		zap.Int64("planID", task.GetPlanID()),
		zap.String("type", task.GetType().String()),
	)
}

func getExpectedSegmentSize(meta *meta, collectionID int64, schema *schemapb.CollectionSchema) int64 {
	allDiskIndex := meta.indexMeta.AllDenseWithDiskIndex(collectionID, schema)
	if allDiskIndex {
		// Only if all dense vector fields index type are DiskANN, recalc segment max size here.
		return Params.DataCoordCfg.DiskSegmentMaxSize.GetAsInt64() * 1024 * 1024
	}
	// If some dense vector fields index type are not DiskANN, recalc segment max size using default policy.
	return Params.DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024
}

// chanPartSegments is an internal result struct, which is aggregates of SegmentInfos with same collectionID, partitionID and channelName
type chanPartSegments struct {
	collectionID UniqueID
	partitionID  UniqueID
	channelName  string
	segments     []*SegmentInfo
}
