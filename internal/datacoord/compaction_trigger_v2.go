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
	"math"
	"sync"
	"time"

	"github.com/samber/lo"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
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
	TriggerTypeBumpSchemaVersion
)

type TickerType int8

const (
	L0Ticker TickerType = iota + 1
	ClusteringTicker
	SingleTicker
	BumpSchemaVersionTicker
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
	case TriggerTypeBumpSchemaVersion:
		return datapb.CompactionType_BumpSchemaVersionCompaction
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
	case TriggerTypeBumpSchemaVersion:
		return "BumpSchemaVersion"
	default:
		return ""
	}
}

// CompactionPolicy defines the interface for different compaction policies
type CompactionPolicy interface {
	// Enable returns whether this compaction policy is enabled
	Enable() bool
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
	bumpSchemaVersionPolicy     *bumpSchemaVersionPolicy

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
	m.bumpSchemaVersionPolicy = newBumpSchemaVersionPolicy(meta, m.allocator, m.handler)

	// Initialize policies map for ticker handling
	m.policies[L0Ticker] = m.l0Policy
	m.policies[ClusteringTicker] = m.clusteringPolicy
	m.policies[SingleTicker] = m.singlePolicy
	m.policies[BumpSchemaVersionTicker] = m.bumpSchemaVersionPolicy
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

	log := mlog.With()
	l0Ticker := time.NewTicker(Params.DataCoordCfg.L0CompactionTriggerInterval.GetAsDuration(time.Second))
	defer l0Ticker.Stop()
	clusteringTicker := time.NewTicker(Params.DataCoordCfg.ClusteringCompactionTriggerInterval.GetAsDuration(time.Second))
	defer clusteringTicker.Stop()
	singleTicker := time.NewTicker(Params.DataCoordCfg.MixCompactionTriggerInterval.GetAsDuration(time.Second))
	defer singleTicker.Stop()
	storageVersionTicker := time.NewTicker(Params.DataCoordCfg.MixCompactionTriggerInterval.GetAsDuration(time.Second))
	defer storageVersionTicker.Stop()
	bumpSchemaVersionTicker := time.NewTicker(Params.DataCoordCfg.BumpSchemaVersionCompactionTriggerInterval.GetAsDuration(time.Second))
	defer bumpSchemaVersionTicker.Stop()
	mlog.Info(ctx, "Compaction trigger manager start")
	for {
		select {
		case <-ctx.Done():
			log.Info(ctx, "Compaction trigger manager checkLoop quit")
			return
		case <-l0Ticker.C:
			m.handleTicker(ctx, L0Ticker)
		case <-clusteringTicker.C:
			m.handleTicker(ctx, ClusteringTicker)
		case <-singleTicker.C:
			m.handleTicker(ctx, SingleTicker)
		case <-storageVersionTicker.C:
			m.handleTicker(ctx, StorageVersionTicker)
		case <-bumpSchemaVersionTicker.C:
			m.handleTicker(ctx, BumpSchemaVersionTicker)
		case segID := <-getStatsTaskChSingleton():
			log.Info(ctx, "receive new segment to trigger sort compaction", mlog.Int64("segmentID", segID))
			view := m.singlePolicy.triggerSegmentSortCompaction(ctx, segID)
			if view == nil {
				log.Warn(ctx, "segment no need to do sort compaction", mlog.Int64("segmentID", segID))
				continue
			}
			if m.meta.GetSegment(ctx, segID) == nil {
				log.Warn(ctx, "segment not found", mlog.Int64("segmentID", segID))
				continue
			}
			m.notify(ctx, TriggerTypeSort, []CompactionView{view})
		}
	}
}

func (m *CompactionTriggerManager) handleTicker(ctx context.Context, tickerType TickerType) {
	policy, exists := m.policies[tickerType]
	if !exists {
		mlog.Warn(ctx, "Policy not found for ticker type", mlog.Any("tickerType", tickerType))
		return
	}

	if !policy.Enable() {
		return
	}

	if m.inspector.isFull() {
		mlog.RatedInfo(ctx, rate.Limit(10), "Skip dispatching compaction events since inspector is full",
			mlog.String("policy", policy.Name()))
		return
	}

	events, err := policy.Trigger(ctx)
	if err != nil {
		mlog.Warn(ctx, "Fail to trigger policy", mlog.String("policy", policy.Name()), mlog.Err(err))
		return
	}
	for triggerType, views := range events {
		if len(views) == 0 {
			continue
		}
		m.notify(ctx, triggerType, views)
	}
}

func (m *CompactionTriggerManager) ManualTrigger(ctx context.Context, collectionID int64, isClustering bool, isL0 bool, targetSize int64) (UniqueID, error) {
	mlog.Info(ctx, "receive manual trigger",
		mlog.Int64("collectionID", collectionID),
		mlog.Bool("is clustering", isClustering),
		mlog.Bool("is l0", isL0),
		mlog.Int64("targetSize", targetSize))

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
	log := mlog.With()
	log.Debug(ctx, "Start to trigger compactions", mlog.String("eventType", eventType.String()))
	for _, view := range views {
		outViews, reason := m.triggerViewForCompaction(ctx, eventType, view)
		for _, outView := range outViews {
			if outView != nil {
				log.Info(ctx, "Success to trigger a compaction, try to submit",
					mlog.String("eventType", eventType.String()),
					mlog.String("reason", reason),
					mlog.String("output view", outView.String()),
					mlog.Int64("triggerID", outView.GetTriggerID()))

				switch eventType {
				case TriggerTypeLevelZeroViewChange, TriggerTypeLevelZeroViewIDLE, TriggerTypeLevelZeroViewManual:
					m.SubmitL0ViewToScheduler(ctx, outView)
				case TriggerTypeClustering:
					m.SubmitClusteringViewToScheduler(ctx, outView)
				case TriggerTypeSingle, TriggerTypeSort, TriggerTypeStorageVersionUpgrade:
					m.SubmitSingleViewToScheduler(ctx, outView, eventType)
				case TriggerTypeForceMerge:
					m.SubmitForceMergeViewToScheduler(ctx, outView)
				case TriggerTypeBumpSchemaVersion:
					m.SubmitBumpSchemaVersionViewToScheduler(ctx, outView)
				}
			}
		}
	}
}

func (m *CompactionTriggerManager) SubmitL0ViewToScheduler(ctx context.Context, view CompactionView) {
	log := mlog.With(mlog.String("view", view.String()))
	taskID, err := m.allocator.AllocID(ctx)
	if err != nil {
		log.Warn(ctx, "Failed to submit compaction view to scheduler because allocate id fail", mlog.Err(err))
		return
	}

	levelZeroSegs := lo.Map(view.GetSegmentsView(), func(segView *SegmentView, _ int) int64 {
		return segView.ID
	})

	collection, err := m.handler.GetCollection(ctx, view.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn(ctx, "Failed to submit compaction view to scheduler because get collection fail", mlog.Err(err))
		return
	}
	if collection == nil {
		log.Warn(ctx, "collection not found when submitting l0 compaction view", mlog.Int64("collectionID", view.GetGroupLabel().CollectionID))
		return
	}
	if collection.IsExternal() {
		log.Info(ctx, "skip submitting l0 compaction for external collection", mlog.Int64("collectionID", collection.ID))
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
		log.Warn(ctx, "Failed to execute compaction task",
			mlog.Int64("triggerID", task.GetTriggerID()),
			mlog.Int64("planID", task.GetPlanID()),
			mlog.Int64s("segmentIDs", task.GetInputSegments()),
			mlog.Err(err))
		return
	}
	log.Info(ctx, "Finish to submit a LevelZeroCompaction plan",
		mlog.Int64("triggerID", task.GetTriggerID()),
		mlog.Int64("planID", task.GetPlanID()),
		mlog.String("type", task.GetType().String()),
		mlog.Int64s("L0 segments", levelZeroSegs),
	)
}

func (m *CompactionTriggerManager) SubmitClusteringViewToScheduler(ctx context.Context, view CompactionView) {
	log := mlog.With(mlog.String("view", view.String()))
	collection, err := m.handler.GetCollection(ctx, view.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn(ctx, "Failed to submit compaction view to scheduler because get collection fail", mlog.Err(err))
		return
	}
	if collection == nil {
		log.Warn(ctx, "collection not found when submitting clustering compaction", mlog.Int64("collectionID", view.GetGroupLabel().CollectionID))
		return
	}
	if collection.IsExternal() {
		log.Info(ctx, "skip submitting clustering compaction for external collection", mlog.Int64("collectionID", collection.ID))
		return
	}

	expectedSegmentSize := getExpectedSegmentSize(m.meta, collection.ID, collection.Schema)
	totalRows, maxSegmentRows, preferSegmentRows, err := calculateClusteringCompactionConfig(collection, view, expectedSegmentSize)
	if err != nil {
		log.Warn(ctx, "Failed to calculate cluster compaction config fail", mlog.Err(err))
		return
	}

	totalSize := view.GetTotalSize()
	preferSegmentSize := float64(expectedSegmentSize) * paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.GetAsFloat()
	planID, analyzeTaskID, preAllocatedSegmentIDs, err := allocClusteringCompactionPlanIDs(m.allocator, totalSize, preferSegmentSize)
	if err != nil {
		log.Warn(ctx, "failed to pre-allocate result segment IDs", mlog.Err(err))
		return
	}
	now := time.Now().Unix()
	task := &datapb.CompactionTask{
		PlanID:                 planID,
		TriggerID:              view.(*ClusteringSegmentsView).triggerID,
		State:                  datapb.CompactionTaskState_pipelining,
		StartTime:              now,
		CollectionTtl:          view.GetCollectionTTL().Nanoseconds(),
		Type:                   datapb.CompactionType_ClusteringCompaction,
		CollectionID:           view.GetGroupLabel().CollectionID,
		PartitionID:            view.GetGroupLabel().PartitionID,
		Channel:                view.GetGroupLabel().Channel,
		Schema:                 collection.Schema,
		ClusteringKeyField:     view.(*ClusteringSegmentsView).clusteringKeyField,
		InputSegments:          lo.Map(view.GetSegmentsView(), func(segmentView *SegmentView, _ int) int64 { return segmentView.ID }),
		ResultSegments:         []int64{},
		MaxSegmentRows:         maxSegmentRows,
		PreferSegmentRows:      preferSegmentRows,
		TotalRows:              totalRows,
		AnalyzeTaskID:          analyzeTaskID,
		LastStateStartTime:     now,
		PreAllocatedSegmentIDs: preAllocatedSegmentIDs,
		MaxSize:                expectedSegmentSize,
	}
	err = m.inspector.enqueueCompaction(task)
	if err != nil {
		log.Warn(ctx, "Failed to execute compaction task",
			mlog.Int64("planID", task.GetPlanID()),
			mlog.Err(err))
		return
	}
	log.Info(ctx, "Finish to submit a clustering compaction task",
		mlog.Int64("triggerID", task.GetTriggerID()),
		mlog.Int64("planID", task.GetPlanID()),
		mlog.Int64("MaxSegmentRows", task.MaxSegmentRows),
		mlog.Int64("PreferSegmentRows", task.PreferSegmentRows),
	)
}

func (m *CompactionTriggerManager) SubmitSingleViewToScheduler(ctx context.Context, view CompactionView, triggerType CompactionTriggerType) {
	log := mlog.With(mlog.String("trigger type", triggerType.String()), mlog.String("view", view.String()))

	collection, err := m.handler.GetCollection(ctx, view.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn(ctx, "Failed to submit compaction view to scheduler because get collection fail", mlog.Err(err))
		return
	}
	if collection == nil {
		log.Warn(ctx, "collection not found when submitting single compaction", mlog.Int64("collectionID", view.GetGroupLabel().CollectionID))
		return
	}
	if collection.IsExternal() {
		log.Info(ctx, "skip submitting single compaction for external collection", mlog.Int64("collectionID", collection.ID))
		return
	}

	expectedSize := getExpectedSegmentSize(m.meta, collection.ID, collection.Schema)
	totalSize := view.GetTotalSize()
	planID, preAllocatedSegmentIDs, err := allocCompactionPlanIDs(m.allocator, totalSize, float64(expectedSize))
	if err != nil {
		log.Warn(ctx, "Failed to submit compaction view to scheduler because allocate id fail", mlog.Err(err))
		return
	}

	var totalRows int64 = 0
	for _, s := range view.GetSegmentsView() {
		totalRows += s.NumOfRows
	}

	now := time.Now().Unix()
	task := &datapb.CompactionTask{
		PlanID:                 planID,
		TriggerID:              view.(*MixSegmentView).triggerID,
		State:                  datapb.CompactionTaskState_pipelining,
		StartTime:              now,
		CollectionTtl:          view.GetCollectionTTL().Nanoseconds(),
		Type:                   triggerType.GetCompactionType(),
		CollectionID:           view.GetGroupLabel().CollectionID,
		PartitionID:            view.GetGroupLabel().PartitionID,
		Channel:                view.GetGroupLabel().Channel,
		Schema:                 collection.Schema,
		InputSegments:          lo.Map(view.GetSegmentsView(), func(segmentView *SegmentView, _ int) int64 { return segmentView.ID }),
		ResultSegments:         []int64{},
		TotalRows:              totalRows,
		LastStateStartTime:     now,
		MaxSize:                expectedSize,
		PreAllocatedSegmentIDs: preAllocatedSegmentIDs,
	}
	err = m.inspector.enqueueCompaction(task)
	if err != nil {
		log.Warn(ctx, "Failed to execute compaction task",
			mlog.Int64("triggerID", task.GetTriggerID()),
			mlog.Int64("planID", task.GetPlanID()),
			mlog.Int64s("segmentIDs", task.GetInputSegments()),
			mlog.Err(err))
		return
	}
	log.Info(ctx, "Finish to submit a single compaction task",
		mlog.Int64("triggerID", task.GetTriggerID()),
		mlog.Int64("planID", task.GetPlanID()),
		mlog.String("type", task.GetType().String()),
		mlog.Int64("targetSize", task.GetMaxSize()),
	)
}

func allocCompactionPlanIDs(allocator allocator.Allocator, totalSize float64, preferredSize float64) (int64, *datapb.IDRange, error) {
	segmentIDCount := estimateResultSegmentCount(totalSize, preferredSize)
	// Reserve one metadata ID because this helper returns one task ID: PlanID.
	block, err := createCompactionIDBlock(allocator, segmentIDCount, 1)
	if err != nil {
		return 0, nil, err
	}
	planID, err := block.take()
	if err != nil {
		return 0, nil, err
	}
	return planID, block.segmentIDRange(), nil
}

func allocClusteringCompactionPlanIDs(allocator allocator.Allocator, totalSize float64, preferredSize float64) (int64, int64, *datapb.IDRange, error) {
	segmentIDCount := estimateResultSegmentCount(totalSize, preferredSize)
	// Reserve two metadata IDs because this helper returns PlanID and AnalyzeTaskID.
	block, err := createCompactionIDBlock(allocator, segmentIDCount, 2)
	if err != nil {
		return 0, 0, nil, err
	}
	planID, err := block.take()
	if err != nil {
		return 0, 0, nil, err
	}
	analyzeTaskID, err := block.take()
	if err != nil {
		return 0, 0, nil, err
	}
	return planID, analyzeTaskID, block.segmentIDRange(), nil
}

// createCompactionIDBlock allocates one contiguous block where the
// first segmentIDCount IDs are result segment IDs and the rest are task metadata IDs.
func createCompactionIDBlock(allocator allocator.Allocator, segmentIDCount int64, metaIDCount int64) (*compactionIDBlock, error) {
	segmentIDCount = max(segmentIDCount, 1)
	expansionFactor := paramtable.Get().DataCoordCfg.CompactionPreAllocateIDExpansionFactor.GetAsInt64()
	if expansionFactor <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("compaction pre-allocate ID expansion factor must be positive: %d", expansionFactor)
	}
	maxBatchSize := int64(math.MaxUint32)
	if metaIDCount < 0 || metaIDCount > maxBatchSize || segmentIDCount > (maxBatchSize-metaIDCount)/expansionFactor {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf(
				"compaction too large to allocate IDs in a single batch: segment ID count %d with expansion factor %d and %d metadata IDs exceeds max batch size %d",
				segmentIDCount, expansionFactor, metaIDCount, maxBatchSize,
			),
		)
	}
	segmentIDCount = segmentIDCount * expansionFactor
	n := metaIDCount + segmentIDCount
	startID, endID, err := allocator.AllocN(n)
	if err != nil {
		return nil, err
	}
	if endID-startID != n {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf("compaction ID allocation range mismatch: expected %d IDs, got %d IDs", n, endID-startID),
		)
	}
	segmentIDEnd := startID + segmentIDCount
	return &compactionIDBlock{
		segments: &datapb.IDRange{Begin: startID, End: segmentIDEnd},
		next:     segmentIDEnd,
		end:      endID,
	}, nil
}

// compactionIDBlock splits one contiguous RootCoord ID allocation into a fixed
// result segment ID range followed by metadata IDs consumed by take().
type compactionIDBlock struct {
	segments *datapb.IDRange
	next     int64
	end      int64
}

func (b *compactionIDBlock) take() (int64, error) {
	if b.next >= b.end {
		return 0, merr.WrapErrServiceInternal("compaction metadata ID range exhausted")
	}
	id := b.next
	b.next++
	return id, nil
}

func (b *compactionIDBlock) segmentIDRange() *datapb.IDRange {
	return b.segments
}

func estimateResultSegmentCount(totalSize float64, targetSize float64) int64 {
	if targetSize <= 0 {
		return 1
	}
	if totalSize <= 0 {
		return 1
	}
	return max(int64(math.Ceil(totalSize/targetSize)), 1)
}

func (m *CompactionTriggerManager) SubmitForceMergeViewToScheduler(ctx context.Context, view CompactionView) {
	log := mlog.With(mlog.String("view", view.String()))

	collection, err := m.handler.GetCollection(ctx, view.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn(ctx, "Failed to get collection", mlog.Err(err))
		return
	}
	if collection == nil {
		log.Warn(ctx, "collection not found when submitting force merge compaction", mlog.Int64("collectionID", view.GetGroupLabel().CollectionID))
		return
	}
	if collection.IsExternal() {
		log.Info(ctx, "skip submitting force merge compaction for external collection", mlog.Int64("collectionID", collection.ID))
		return
	}

	totalRows := lo.SumBy(view.GetSegmentsView(), func(v *SegmentView) int64 { return v.NumOfRows })

	totalSize := view.GetTotalSize()
	planID, preAllocatedSegmentIDs, err := allocCompactionPlanIDs(m.allocator, totalSize, view.(*ForceMergeSegmentView).GetTargetSegmentSize())
	if err != nil {
		log.Warn(ctx, "Failed to submit compaction view to scheduler because allocate id fail", mlog.Err(err))
		return
	}

	now := time.Now().Unix()
	task := &datapb.CompactionTask{
		PlanID:                 planID,
		TriggerID:              view.GetTriggerID(),
		State:                  datapb.CompactionTaskState_pipelining,
		StartTime:              now,
		CollectionTtl:          view.GetCollectionTTL().Nanoseconds(),
		Type:                   datapb.CompactionType_MixCompaction,
		CollectionID:           view.GetGroupLabel().CollectionID,
		PartitionID:            view.GetGroupLabel().PartitionID,
		Channel:                view.GetGroupLabel().Channel,
		Schema:                 collection.Schema,
		InputSegments:          lo.Map(view.GetSegmentsView(), func(segmentView *SegmentView, _ int) int64 { return segmentView.ID }),
		ResultSegments:         []int64{},
		TotalRows:              totalRows,
		LastStateStartTime:     now,
		MaxSize:                int64(view.(*ForceMergeSegmentView).GetTargetSegmentSize()),
		PreAllocatedSegmentIDs: preAllocatedSegmentIDs,
	}

	err = m.inspector.enqueueCompaction(task)
	if err != nil {
		log.Warn(ctx, "Failed to enqueue task", mlog.Err(err))
		return
	}

	log.Info(ctx, "Finish to submit force merge task",
		mlog.Int64("planID", task.GetPlanID()),
		mlog.Int64("triggerID", task.GetTriggerID()),
		mlog.Int64("collectionID", task.GetCollectionID()),
		mlog.Int64("targetSize", task.GetMaxSize()),
	)
}

func (m *CompactionTriggerManager) SubmitBumpSchemaVersionViewToScheduler(ctx context.Context, view CompactionView) {
	log := mlog.With(mlog.String("view", view.String()))
	bumpView, ok := view.(*BumpSchemaVersionView)
	if !ok {
		log.Warn(ctx, "unexpected view type for schema bump trigger, expected *BumpSchemaVersionView",
			mlog.String("actualType", fmt.Sprintf("%T", view)))
		return
	}
	collection, err := m.handler.GetCollection(ctx, view.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn(ctx, "Failed to submit compaction view to scheduler because get collection fail", mlog.Err(err))
		return
	}
	if collection == nil {
		log.Warn(ctx, "Failed to submit compaction view to scheduler because collection is nil")
		return
	}
	if collection.IsExternal() {
		log.Info(ctx, "skip submitting schema bump compaction for external collection", mlog.Int64("collectionID", collection.ID))
		return
	}
	collectionTTL, err := common.GetCollectionTTLFromMap(collection.Properties)
	if err != nil {
		log.Warn(ctx, "Failed to submit schema bump compaction because get collection ttl failed", mlog.Err(err))
		return
	}
	var totalRows int64 = 0
	for _, s := range view.GetSegmentsView() {
		totalRows += s.NumOfRows
	}
	expectedSize := getExpectedSegmentSize(m.meta, collection.ID, collection.Schema)
	planID, preAllocatedSegmentIDs, err := allocCompactionPlanIDs(m.allocator, view.GetTotalSize(), float64(expectedSize))
	if err != nil {
		log.Warn(ctx, "Failed to submit compaction view to scheduler because allocate id fail", mlog.Err(err))
		return
	}
	now := time.Now().Unix()
	task := &datapb.CompactionTask{
		PlanID:                 planID,
		TriggerID:              bumpView.triggerID,
		State:                  datapb.CompactionTaskState_pipelining,
		StartTime:              now,
		CollectionTtl:          collectionTTL.Nanoseconds(),
		Type:                   datapb.CompactionType_BumpSchemaVersionCompaction,
		CollectionID:           view.GetGroupLabel().CollectionID,
		PartitionID:            view.GetGroupLabel().PartitionID,
		Channel:                view.GetGroupLabel().Channel,
		Schema:                 bumpView.schema,
		InputSegments:          lo.Map(view.GetSegmentsView(), func(segmentView *SegmentView, _ int) int64 { return segmentView.ID }),
		ResultSegments:         []int64{},
		TotalRows:              totalRows,
		LastStateStartTime:     now,
		MaxSize:                expectedSize,
		PreAllocatedSegmentIDs: preAllocatedSegmentIDs,
	}
	err = m.inspector.enqueueCompaction(task)
	if err != nil {
		log.Warn(ctx, "Failed to execute compaction task",
			mlog.Int64("triggerID", task.GetTriggerID()),
			mlog.Int64("planID", task.GetPlanID()),
			mlog.Int64s("segmentIDs", task.GetInputSegments()),
			mlog.Err(err))
		return
	}
	log.Info(ctx, "Finish to submit a schema bump compaction task",
		mlog.Int64("triggerID", task.GetTriggerID()),
		mlog.Int64("planID", task.GetPlanID()),
		mlog.String("type", task.GetType().String()),
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
