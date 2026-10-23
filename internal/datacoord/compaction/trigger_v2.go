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

package compaction

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/samber/lo"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/targets"
	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/partitionstats"
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

type TickerType int8

const (
	L0Ticker TickerType = iota + 1
	ClusteringTicker
	SingleTicker
	BumpSchemaVersionTicker
	StorageVersionTicker
	TargetTicker
)

type TriggerManager interface {
	Start()
	Stop()
	OnCollectionUpdate(collectionID int64)
	ManualTrigger(ctx context.Context, req *milvuspb.ManualCompactionRequest) (UniqueID, error)
	InitForceMergeMemoryQuerier(nodeManager session.NodeManager, mixCoord types.MixCoord, session sessionutil.SessionInterface)
}

var _ TriggerManager = (*CompactionTriggerManager)(nil)

// CompactionTriggerManager registers Triggers to TriggerType
// so that when the certain TriggerType happens, the corresponding triggers can
// trigger the correct compaction plans.
type CompactionTriggerManager struct {
	inspector CompactionInspector
	handler   meta.Handler
	allocator allocator.Allocator

	meta                 *meta.Meta
	indexMeta            *index.IndexMeta
	compactionTargetMeta *targets.CompactionTargetMeta
	policies             map[TickerType]policy.CompactionPolicy

	l0Policy                    *policy.L0CompactionPolicy
	clusteringPolicy            *policy.ClusteringCompactionPolicy
	singlePolicy                *policy.SingleCompactionPolicy
	forceMergePolicy            *policy.ForceMergeCompactionPolicy
	upgradeStorageVersionPolicy *policy.StorageVersionUpgradePolicy
	bumpSchemaVersionPolicy     *policy.BumpSchemaVersionPolicy
	targetReconciler            *targets.CompactionTargetReconciler

	cancel  context.CancelFunc
	closeWg sync.WaitGroup
}

func NewCompactionTriggerManager(alloc allocator.Allocator, handler meta.Handler, inspector CompactionInspector, mt *meta.Meta,
	indexMeta *index.IndexMeta, compactionTaskMeta *ctask.CompactionTaskMeta, partitionStatsMeta *partitionstats.PartitionStatsMeta, compactionTargetMeta *targets.CompactionTargetMeta,
	versionManager index.IndexEngineVersionManager,
) *CompactionTriggerManager {
	m := &CompactionTriggerManager{
		allocator:            alloc,
		handler:              handler,
		inspector:            inspector,
		meta:                 mt,
		indexMeta:            indexMeta,
		compactionTargetMeta: compactionTargetMeta,
		policies:             make(map[TickerType]policy.CompactionPolicy),
	}
	// Initialize policies and keep separate pointers for frequently accessed ones

	m.l0Policy = policy.NewL0CompactionPolicy(mt, alloc)
	m.clusteringPolicy = policy.NewClusteringCompactionPolicy(mt, compactionTaskMeta, partitionStatsMeta, m.allocator, m.handler)
	m.singlePolicy = policy.NewSingleCompactionPolicy(mt, indexMeta, m.allocator, m.handler)

	m.forceMergePolicy = policy.NewForceMergeCompactionPolicy(mt, indexMeta, m.allocator, m.handler)
	m.upgradeStorageVersionPolicy = policy.NewStorageVersionUpgradePolicy(mt, m.allocator, m.handler, versionManager)
	m.bumpSchemaVersionPolicy = policy.NewBumpSchemaVersionPolicy(mt, m.allocator, m.handler)
	if Params.DataCoordCfg.EnableTargetBasedCompaction.GetAsBool() {
		m.targetReconciler = targets.NewCompactionTargetReconciler(mt, indexMeta, compactionTargetMeta, handler)
	}

	// Initialize policies map for ticker handling
	m.policies[L0Ticker] = m.l0Policy
	m.policies[ClusteringTicker] = m.clusteringPolicy
	m.policies[SingleTicker] = m.singlePolicy
	m.policies[BumpSchemaVersionTicker] = m.bumpSchemaVersionPolicy
	m.policies[StorageVersionTicker] = m.upgradeStorageVersionPolicy
	if m.targetReconciler != nil {
		m.policies[TargetTicker] = m.targetReconciler
	}
	return m
}

// InitForceMergeMemoryQuerier initializes the topology querier for force merge auto calculation
func (m *CompactionTriggerManager) InitForceMergeMemoryQuerier(nodeManager session.NodeManager, mixCoord types.MixCoord, session sessionutil.SessionInterface) {
	if m.forceMergePolicy != nil {
		querier := view.NewMetricsNodeMemoryQuerier(nodeManager, mixCoord, session)
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
	targetTicker := time.NewTicker(Params.DataCoordCfg.MixCompactionTriggerInterval.GetAsDuration(time.Second))
	defer targetTicker.Stop()
	log.Info(ctx, "Compaction trigger manager start")
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
		case <-targetTicker.C:
			if _, exists := m.policies[TargetTicker]; exists {
				m.handleTicker(ctx, TargetTicker)
			}
		case segID := <-meta.GetStatsTaskChSingleton():
			log.Info(ctx, "receive new segment to trigger sort compaction", mlog.Int64("segmentID", segID))
			cv := m.singlePolicy.TriggerSegmentSortCompaction(ctx, segID)
			if cv == nil {
				log.Warn(ctx, "segment no need to do sort compaction", mlog.Int64("segmentID", segID))
				continue
			}
			if m.meta.GetSegment(ctx, segID) == nil {
				log.Warn(ctx, "segment not found", mlog.Int64("segmentID", segID))
				continue
			}
			m.notify(ctx, policy.TriggerTypeSort, []view.CompactionView{cv})
		}
	}
}

func (m *CompactionTriggerManager) handleTicker(ctx context.Context, tickerType TickerType) {
	pol, exists := m.policies[tickerType]
	if !exists {
		mlog.Warn(ctx, "Policy not found for ticker type", mlog.Any("tickerType", tickerType))
		return
	}

	if !pol.Enable() {
		return
	}

	if m.inspector.isFull() {
		mlog.RatedInfo(ctx, rate.Limit(10), "Skip dispatching compaction events since inspector is full",
			mlog.String("policy", pol.Name()))
		return
	}

	events, err := pol.Trigger(ctx)
	if err != nil {
		mlog.Warn(ctx, "Fail to trigger policy", mlog.String("policy", pol.Name()), mlog.Err(err))
		return
	}
	for triggerType, views := range events {
		if len(views) == 0 {
			continue
		}
		m.notify(ctx, triggerType, views)
	}
}

func (m *CompactionTriggerManager) ManualTrigger(ctx context.Context, req *milvuspb.ManualCompactionRequest) (UniqueID, error) {
	collectionID := req.GetCollectionID()
	isClustering := req.GetMajorCompaction()
	isL0 := req.GetL0Compaction()
	targetSize := req.GetTargetSize()
	mlog.Info(ctx, "receive manual trigger",
		mlog.FieldCollectionID(collectionID),
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
	if IsTargetBasedManualRewriteCompactionRequest(req) {
		return m.saveManualRewriteCompactionTarget(ctx, req)
	}

	var triggerID UniqueID
	var views []view.CompactionView

	events := make(map[policy.CompactionTriggerType][]view.CompactionView, 0)
	if targetSize != 0 {
		views, triggerID, err = m.forceMergePolicy.TriggerOneCollection(ctx, collectionID, targetSize)
		events[policy.TriggerTypeForceMerge] = views
	} else if isL0 {
		views, triggerID, err = m.l0Policy.TriggerOneCollection(ctx, collectionID)
		events[policy.TriggerTypeLevelZeroViewManual] = views
	} else if isClustering {
		views, triggerID, err = m.clusteringPolicy.TriggerOneCollection(ctx, collectionID, true)
		events[policy.TriggerTypeClustering] = views
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

func isManualRewriteCompactionRequest(req *milvuspb.ManualCompactionRequest) bool {
	return req != nil &&
		!req.GetMajorCompaction() &&
		!req.GetL0Compaction() &&
		req.GetTargetSize() == 0
}

func IsTargetBasedManualRewriteCompactionRequest(req *milvuspb.ManualCompactionRequest) bool {
	return Params.DataCoordCfg.EnableTargetBasedCompaction.GetAsBool() &&
		isManualRewriteCompactionRequest(req)
}

func (m *CompactionTriggerManager) saveManualRewriteCompactionTarget(ctx context.Context, req *milvuspb.ManualCompactionRequest) (UniqueID, error) {
	if req.GetPartitionId() > 0 || req.GetChannel() != "" {
		return 0, merr.WrapErrParameterInvalidMsg(
			"target-based manual rewrite compaction does not support partition or channel filters")
	}
	if m.meta == nil {
		return 0, merr.WrapErrServiceInternal("data coord meta is not initialized")
	}
	if m.meta.IsCollectionCompactionBlocked(req.GetCollectionID()) {
		return 0, merr.WrapErrCompactionBlocked(fmt.Sprintf(
			"collection %d has pending snapshot or unloaded snapshot RefIndex",
			req.GetCollectionID()))
	}
	targetMeta := m.compactionTargetMeta
	if targetMeta == nil {
		return 0, merr.WrapErrServiceInternal("compaction target meta is not initialized")
	}
	record, err := targets.NewManualRewriteCompactionTarget(req.GetCollectionID(), req.GetSegmentIds()).Create(ctx, m.allocator)
	if err != nil {
		return 0, err
	}
	if err := targetMeta.SaveCompactionTarget(ctx, record); err != nil {
		return 0, err
	}
	return record.GetTargetID(), nil
}

func (m *CompactionTriggerManager) triggerViewForCompaction(ctx context.Context, eventType policy.CompactionTriggerType,
	cv view.CompactionView,
) ([]view.CompactionView, string) {
	switch eventType {
	case policy.TriggerTypeLevelZeroViewIDLE:
		cv, reason := cv.ForceTrigger()
		return []view.CompactionView{cv}, reason

	case policy.TriggerTypeLevelZeroViewManual, policy.TriggerTypeForceMerge:
		return cv.ForceTriggerAll()

	default:
		outView, reason := cv.Trigger()
		return []view.CompactionView{outView}, reason
	}
}

func (m *CompactionTriggerManager) notify(ctx context.Context, eventType policy.CompactionTriggerType, views []view.CompactionView) {
	log := mlog.With()
	log.Debug(ctx, "Start to trigger compactions", mlog.String("eventType", eventType.String()))
	for _, cv := range views {
		outViews, reason := m.triggerViewForCompaction(ctx, eventType, cv)
		for _, outView := range outViews {
			if outView != nil {
				log.Info(ctx, "Success to trigger a compaction, try to submit",
					mlog.String("eventType", eventType.String()),
					mlog.String("reason", reason),
					mlog.String("output view", outView.String()),
					mlog.Int64("triggerID", outView.GetTriggerID()))

				switch eventType {
				case policy.TriggerTypeLevelZeroViewChange, policy.TriggerTypeLevelZeroViewIDLE, policy.TriggerTypeLevelZeroViewManual:
					m.SubmitL0ViewToScheduler(ctx, outView)
				case policy.TriggerTypeClustering:
					m.SubmitClusteringViewToScheduler(ctx, outView)
				case policy.TriggerTypeSingle, policy.TriggerTypeSort, policy.TriggerTypeStorageVersionUpgrade:
					m.SubmitSingleViewToScheduler(ctx, outView, eventType)
				case policy.TriggerTypeForceMerge:
					m.SubmitForceMergeViewToScheduler(ctx, outView)
				case policy.TriggerTypeBumpSchemaVersion:
					m.SubmitBumpSchemaVersionViewToScheduler(ctx, outView)
				}
			}
		}
	}
}

func (m *CompactionTriggerManager) SubmitL0ViewToScheduler(ctx context.Context, cv view.CompactionView) {
	log := mlog.With(mlog.String("view", cv.String()))
	taskID, err := m.allocator.AllocID(ctx)
	if err != nil {
		log.Warn(ctx, "Failed to submit compaction view to scheduler because allocate id fail", mlog.Err(err))
		return
	}

	levelZeroSegs := lo.Map(cv.GetSegmentsView(), func(segView *view.SegmentView, _ int) int64 {
		return segView.ID
	})

	collection, err := m.handler.GetCollection(ctx, cv.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn(ctx, "Failed to submit compaction view to scheduler because get collection fail", mlog.Err(err))
		return
	}
	if collection == nil {
		log.Warn(ctx, "collection not found when submitting l0 compaction view", mlog.Int64("collectionID", cv.GetGroupLabel().CollectionID))
		return
	}
	if collection.IsExternal() {
		log.Info(ctx, "skip submitting l0 compaction for external collection", mlog.Int64("collectionID", collection.ID))
		return
	}

	totalRows := lo.SumBy(cv.GetSegmentsView(), func(segView *view.SegmentView) int64 {
		return segView.NumOfRows
	})

	task := &datapb.CompactionTask{
		TriggerID:     cv.GetTriggerID(),
		PlanID:        taskID,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		StartTime:     time.Now().Unix(),
		TotalRows:     totalRows,
		InputSegments: levelZeroSegs,
		State:         datapb.CompactionTaskState_pipelining,
		Channel:       cv.GetGroupLabel().Channel,
		CollectionID:  cv.GetGroupLabel().CollectionID,
		PartitionID:   cv.GetGroupLabel().PartitionID,
		Pos:           cv.(*view.LevelZeroCompactionView).LatestDeletePos,
		Schema:        collection.Schema,
	}

	err = m.inspector.EnqueueCompaction(task)
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

func (m *CompactionTriggerManager) SubmitClusteringViewToScheduler(ctx context.Context, cv view.CompactionView) {
	log := mlog.With(mlog.String("view", cv.String()))
	collection, err := m.handler.GetCollection(ctx, cv.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn(ctx, "Failed to submit compaction view to scheduler because get collection fail", mlog.Err(err))
		return
	}
	if collection == nil {
		log.Warn(ctx, "collection not found when submitting clustering compaction", mlog.Int64("collectionID", cv.GetGroupLabel().CollectionID))
		return
	}
	if collection.IsExternal() {
		log.Info(ctx, "skip submitting clustering compaction for external collection", mlog.Int64("collectionID", collection.ID))
		return
	}

	expectedSegmentSize := policy.GetExpectedSegmentSize(m.indexMeta, collection.ID, collection.Schema)
	totalRows, maxSegmentRows, preferSegmentRows, err := policy.CalculateClusteringCompactionConfig(collection, cv, expectedSegmentSize)
	if err != nil {
		log.Warn(ctx, "Failed to calculate cluster compaction config fail", mlog.Err(err))
		return
	}

	totalSize := cv.GetTotalSize()
	preferSegmentSize := float64(expectedSegmentSize) * paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.GetAsFloat()
	planID, analyzeTaskID, preAllocatedSegmentIDs, err := allocClusteringCompactionPlanIDs(m.allocator, totalSize, preferSegmentSize)
	if err != nil {
		log.Warn(ctx, "failed to pre-allocate result segment IDs", mlog.Err(err))
		return
	}
	now := time.Now().Unix()
	task := &datapb.CompactionTask{
		PlanID:                 planID,
		TriggerID:              cv.(*policy.ClusteringSegmentsView).TriggerID,
		State:                  datapb.CompactionTaskState_pipelining,
		StartTime:              now,
		CollectionTtl:          cv.GetCollectionTTL().Nanoseconds(),
		Type:                   datapb.CompactionType_ClusteringCompaction,
		CollectionID:           cv.GetGroupLabel().CollectionID,
		PartitionID:            cv.GetGroupLabel().PartitionID,
		Channel:                cv.GetGroupLabel().Channel,
		Schema:                 collection.Schema,
		ClusteringKeyField:     cv.(*policy.ClusteringSegmentsView).ClusteringKeyField,
		InputSegments:          lo.Map(cv.GetSegmentsView(), func(segmentView *view.SegmentView, _ int) int64 { return segmentView.ID }),
		ResultSegments:         []int64{},
		MaxSegmentRows:         maxSegmentRows,
		PreferSegmentRows:      preferSegmentRows,
		TotalRows:              totalRows,
		AnalyzeTaskID:          analyzeTaskID,
		LastStateStartTime:     now,
		PreAllocatedSegmentIDs: preAllocatedSegmentIDs,
		MaxSize:                expectedSegmentSize,
	}
	err = m.inspector.EnqueueCompaction(task)
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

func (m *CompactionTriggerManager) SubmitSingleViewToScheduler(ctx context.Context, cv view.CompactionView, triggerType policy.CompactionTriggerType) {
	log := mlog.With(mlog.String("trigger type", triggerType.String()), mlog.String("view", cv.String()))

	collection, err := m.handler.GetCollection(ctx, cv.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn(ctx, "Failed to submit compaction view to scheduler because get collection fail", mlog.Err(err))
		return
	}
	if collection == nil {
		log.Warn(ctx, "collection not found when submitting single compaction", mlog.Int64("collectionID", cv.GetGroupLabel().CollectionID))
		return
	}
	if collection.IsExternal() {
		log.Info(ctx, "skip submitting single compaction for external collection", mlog.Int64("collectionID", collection.ID))
		return
	}

	expectedSize := policy.GetExpectedSegmentSize(m.indexMeta, collection.ID, collection.Schema)
	totalSize := cv.GetTotalSize()
	planID, preAllocatedSegmentIDs, err := allocCompactionPlanIDs(m.allocator, totalSize, float64(expectedSize))
	if err != nil {
		log.Warn(ctx, "Failed to submit compaction view to scheduler because allocate id fail", mlog.Err(err))
		return
	}

	var totalRows int64 = 0
	for _, s := range cv.GetSegmentsView() {
		totalRows += s.NumOfRows
	}

	now := time.Now().Unix()
	task := &datapb.CompactionTask{
		PlanID:                 planID,
		TriggerID:              cv.(*policy.MixSegmentView).TriggerID,
		State:                  datapb.CompactionTaskState_pipelining,
		StartTime:              now,
		CollectionTtl:          cv.GetCollectionTTL().Nanoseconds(),
		Type:                   triggerType.GetCompactionType(),
		CollectionID:           cv.GetGroupLabel().CollectionID,
		PartitionID:            cv.GetGroupLabel().PartitionID,
		Channel:                cv.GetGroupLabel().Channel,
		Schema:                 collection.Schema,
		InputSegments:          lo.Map(cv.GetSegmentsView(), func(segmentView *view.SegmentView, _ int) int64 { return segmentView.ID }),
		ResultSegments:         []int64{},
		TotalRows:              totalRows,
		LastStateStartTime:     now,
		MaxSize:                expectedSize,
		PreAllocatedSegmentIDs: preAllocatedSegmentIDs,
	}
	err = m.inspector.EnqueueCompaction(task)
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

func (m *CompactionTriggerManager) SubmitForceMergeViewToScheduler(ctx context.Context, cv view.CompactionView) {
	log := mlog.With(mlog.String("view", cv.String()))

	collection, err := m.handler.GetCollection(ctx, cv.GetGroupLabel().CollectionID)
	if err != nil {
		log.Warn(ctx, "Failed to get collection", mlog.Err(err))
		return
	}
	if collection == nil {
		log.Warn(ctx, "collection not found when submitting force merge compaction", mlog.Int64("collectionID", cv.GetGroupLabel().CollectionID))
		return
	}
	if collection.IsExternal() {
		log.Info(ctx, "skip submitting force merge compaction for external collection", mlog.Int64("collectionID", collection.ID))
		return
	}

	segmentViews := cv.GetSegmentsView()
	totalRows := lo.SumBy(segmentViews, func(v *view.SegmentView) int64 { return v.NumOfRows })

	forceMergeView := cv.(*view.ForceMergeSegmentView)
	totalSize := view.SumSegmentSize(segmentViews)
	planID, preAllocatedSegmentIDs, err := allocCompactionPlanIDs(m.allocator, totalSize, float64(forceMergeView.GetTargetSegmentSize()))
	if err != nil {
		log.Warn(ctx, "Failed to submit compaction view to scheduler because allocate id fail", mlog.Err(err))
		return
	}

	now := time.Now().Unix()
	task := &datapb.CompactionTask{
		PlanID:                 planID,
		TriggerID:              cv.GetTriggerID(),
		State:                  datapb.CompactionTaskState_pipelining,
		StartTime:              now,
		CollectionTtl:          cv.GetCollectionTTL().Nanoseconds(),
		Type:                   datapb.CompactionType_MixCompaction,
		CollectionID:           cv.GetGroupLabel().CollectionID,
		PartitionID:            cv.GetGroupLabel().PartitionID,
		Channel:                cv.GetGroupLabel().Channel,
		Schema:                 collection.Schema,
		InputSegments:          lo.Map(segmentViews, func(segmentView *view.SegmentView, _ int) int64 { return segmentView.ID }),
		ResultSegments:         []int64{},
		TotalRows:              totalRows,
		LastStateStartTime:     now,
		MaxSize:                forceMergeView.GetTargetSegmentSize(),
		PreAllocatedSegmentIDs: preAllocatedSegmentIDs,
	}

	err = m.inspector.EnqueueCompaction(task)
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

func (m *CompactionTriggerManager) SubmitBumpSchemaVersionViewToScheduler(ctx context.Context, cv view.CompactionView) {
	log := mlog.With(mlog.String("view", cv.String()))
	bumpView, ok := cv.(*policy.BumpSchemaVersionView)
	if !ok {
		log.Warn(ctx, "unexpected view type for schema bump trigger, expected *BumpSchemaVersionView",
			mlog.String("actualType", fmt.Sprintf("%T", cv)))
		return
	}
	collection, err := m.handler.GetCollection(ctx, cv.GetGroupLabel().CollectionID)
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
	for _, s := range cv.GetSegmentsView() {
		totalRows += s.NumOfRows
	}
	expectedSize := policy.GetExpectedSegmentSize(m.indexMeta, collection.ID, collection.Schema)
	planID, preAllocatedSegmentIDs, err := allocCompactionPlanIDs(m.allocator, cv.GetTotalSize(), float64(expectedSize))
	if err != nil {
		log.Warn(ctx, "Failed to submit compaction view to scheduler because allocate id fail", mlog.Err(err))
		return
	}
	now := time.Now().Unix()
	task := &datapb.CompactionTask{
		PlanID:                 planID,
		TriggerID:              bumpView.TriggerID,
		State:                  datapb.CompactionTaskState_pipelining,
		StartTime:              now,
		CollectionTtl:          collectionTTL.Nanoseconds(),
		Type:                   datapb.CompactionType_BumpSchemaVersionCompaction,
		CollectionID:           cv.GetGroupLabel().CollectionID,
		PartitionID:            cv.GetGroupLabel().PartitionID,
		Channel:                cv.GetGroupLabel().Channel,
		Schema:                 bumpView.Schema,
		InputSegments:          lo.Map(cv.GetSegmentsView(), func(segmentView *view.SegmentView, _ int) int64 { return segmentView.ID }),
		ResultSegments:         []int64{},
		TotalRows:              totalRows,
		LastStateStartTime:     now,
		MaxSize:                expectedSize,
		PreAllocatedSegmentIDs: preAllocatedSegmentIDs,
	}
	err = m.inspector.EnqueueCompaction(task)
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
