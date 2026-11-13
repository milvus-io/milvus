package targets

import (
	"cmp"
	"context"
	"slices"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// compactionTargetReconciler converges segments toward declared compaction
// targets: each tick it compares active CompactionTargets (the desired state of
// the data) against live segment facts (the actual state) and emits compaction
// views for segments that still miss their target. It stores no progress - a
// target is satisfied when no in-scope segment matches its predicate anymore.
type CompactionTargetReconciler struct {
	meta                 *meta.Meta
	IndexMeta            *index.IndexMeta
	CompactionTargetMeta *CompactionTargetMeta
	handler              meta.Handler
	allocator            allocator.Allocator
	versionManager       index.IndexEngineVersionManager
	taskMeta             *task.CompactionTaskMeta
}

func (reconciler *CompactionTargetReconciler) SetIndexVersionManager(manager index.IndexEngineVersionManager) {
	reconciler.versionManager = manager
}

func (reconciler *CompactionTargetReconciler) SetTaskMeta(tasks *task.CompactionTaskMeta) {
	reconciler.taskMeta = tasks
}

func NewCompactionTargetReconciler(mt *meta.Meta, indexMeta *index.IndexMeta, compactionTargetMeta *CompactionTargetMeta, handler meta.Handler, alloc allocator.Allocator) *CompactionTargetReconciler {
	return &CompactionTargetReconciler{
		meta:                 mt,
		IndexMeta:            indexMeta,
		CompactionTargetMeta: compactionTargetMeta,
		handler:              handler,
		allocator:            alloc,
	}
}

func (reconciler *CompactionTargetReconciler) Enable() bool {
	return reconciler != nil &&
		reconciler.meta != nil &&
		reconciler.CompactionTargetMeta != nil
}

func (reconciler *CompactionTargetReconciler) Reconcile(ctx context.Context) (map[policy.CompactionTriggerType][]view.TargetView, error) {
	events := map[policy.CompactionTriggerType][]view.TargetView{}
	if !reconciler.Enable() {
		return events, nil
	}

	sortMatches := reconciler.observeStandingSort(ctx)
	manager := reconciler.CompactionTargetMeta
	manager.refreshUnconfirmedCreates(ctx)
	var size SizeCondition
	var sizeErr error
	if _, err := manager.SyncSize(ctx, reconciler.allocator); err != nil {
		sizeErr = err
	} else {
		size, sizeErr = ReadSizeCondition()
	}
	if sizeErr != nil {
		mlog.RatedWarn(ctx, rate.Limit(1), "could not synchronize Standing SIZE compaction target", mlog.Err(sizeErr))
		size.Enabled = false
	}
	records := manager.GetActiveCompactionTargets()
	matches := make(map[int64]*meta.SegmentInfo)
	optimizeInputs := make(map[int64]optimizeInput)
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !reconciler.meta.GetCollection(record.GetCollectionID()).IsCreated() {
			continue
		}
		if IsOptimize(record) {
			reconciler.observeOptimize(ctx, record, optimizeInputs)
			continue
		}
		observation, err := ObserveRewrite(ctx, reconciler.meta, record, false)
		if err == nil && len(observation.Match) == 0 {
			err = reconciler.finishRewrite(ctx, record, &observation)
		}
		if err != nil {
			mlog.RatedWarn(ctx, rate.Limit(1), "reconcile compaction target failed",
				mlog.Int64("targetID", record.GetTargetID()),
				mlog.FieldCollectionID(record.GetCollectionID()), mlog.Err(err))
			continue
		}
		for _, segment := range observation.Match {
			matches[segment.GetID()] = segment
		}
	}

	// Pick the physical path for each segment before constructing any View.
	// An independently selectable SORT takes precedence over a matching REWRITE.
	eligible := make(map[int64]collectionPlanningFacts)
	sortSelectable := make([]*meta.SegmentInfo, 0, len(sortMatches))
	allocated := make(map[int64]struct{}, len(sortMatches))
	for _, segment := range sortMatches {
		if reconciler.selectable(ctx, segment, eligible) {
			allocated[segment.GetID()] = struct{}{}
			sortSelectable = append(sortSelectable, segment)
		}
	}
	sharedSelectable := make([]*meta.SegmentInfo, 0, len(matches))
	for id, segment := range matches {
		if _, assigned := allocated[id]; !assigned && reconciler.selectable(ctx, segment, eligible) {
			sharedSelectable = append(sharedSelectable, segment)
		}
	}
	rewriteSelectable := reconciler.filterMixCompactionSelectable(ctx, sharedSelectable)
	if len(sortMatches) > len(sortSelectable) || len(matches) > len(rewriteSelectable) {
		mlog.RatedInfo(ctx, rate.Limit(1), "compaction target inputs awaiting selection",
			mlog.Int("sortMatches", len(sortMatches)), mlog.Int("sortSelectable", len(sortSelectable)),
			mlog.Int("rewriteMatches", len(matches)), mlog.Int("rewriteSelectable", len(rewriteSelectable)))
	}
	slices.SortFunc(sortSelectable, func(a, b *meta.SegmentInfo) int {
		if a.GetIsInvisible() != b.GetIsInvisible() {
			if a.GetIsInvisible() {
				return -1
			}
			return 1
		}
		return compareSegment(a, b)
	})
	slices.SortFunc(rewriteSelectable, compareSegment)
	events[policy.TriggerTypeSort] = targetViews(sortSelectable, eligible, true)
	events[policy.TriggerTypeSingle] = reconciler.mixViews(ctx, rewriteSelectable, optimizeInputs, allocated, eligible, size)
	return events, ctx.Err()
}

func compareSegment(a, b *meta.SegmentInfo) int {
	if a.GetCollectionID() != b.GetCollectionID() {
		return cmp.Compare(a.GetCollectionID(), b.GetCollectionID())
	}
	return cmp.Compare(a.GetID(), b.GetID())
}

type collectionPlanningFacts struct {
	blocked      bool
	ttl          time.Duration
	schema       *schemapb.CollectionSchema
	expectedSize int64
}

func (reconciler *CompactionTargetReconciler) selectable(
	ctx context.Context,
	segment *meta.SegmentInfo,
	collections map[int64]collectionPlanningFacts,
) bool {
	id := segment.GetCollectionID()
	facts, checked := collections[id]
	if !checked {
		collection := reconciler.meta.GetCollection(id)
		facts.blocked = !collection.IsCreated() || collection.IsExternal() || reconciler.meta.IsCollectionCompactionBlocked(id)
		if !facts.blocked {
			ttl, err := common.GetCollectionTTLFromMap(collection.Properties)
			if err != nil {
				mlog.RatedWarn(ctx, rate.Limit(1), "could not determine collection TTL for compaction target",
					mlog.FieldCollectionID(id), mlog.Err(err))
				facts.blocked = true
			} else {
				facts.ttl = ttl
				facts.schema = proto.CloneOf(collection.Schema)
				facts.expectedSize = policy.GetExpectedSegmentSize(reconciler.IndexMeta, id, facts.schema)
			}
		}
		collections[id] = facts
	}
	return !facts.blocked && segment.GetNumOfRows() >= 0 && policy.IsSharedCompactionSelectable(reconciler.meta, segment)
}

func targetViews(segments []*meta.SegmentInfo, collections map[int64]collectionPlanningFacts, sorting bool) []view.TargetView {
	groups := make(map[view.CompactionGroupLabel]*view.RewriteInputs)
	var order []view.CompactionGroupLabel
	for _, segment := range segments {
		input := view.GetTargetSegmentView(segment)
		label := *input.Label
		pool := groups[label]
		if pool == nil {
			facts := collections[label.CollectionID]
			pool = &view.RewriteInputs{
				Group: input.Label, TTL: facts.ttl, Schema: facts.schema, ExpectedSize: facts.expectedSize,
			}
			groups[label] = pool
			order = append(order, label)
		}
		pool.Segments = append(pool.Segments, input)
	}
	views := make([]view.TargetView, 0, len(order))
	for _, label := range order {
		pool := groups[label]
		if sorting {
			views = append(views, &view.SortView{RewriteInputs: *pool})
		} else {
			views = append(views, &view.RewriteView{RewriteInputs: *pool})
		}
	}
	return views
}

func (reconciler *CompactionTargetReconciler) observeStandingSort(ctx context.Context) []*meta.SegmentInfo {
	record, err := reconciler.SyncSort(ctx)
	if err != nil {
		mlog.RatedWarn(ctx, rate.Limit(1), "could not synchronize Standing SORT compaction target", mlog.Err(err))
		return nil
	}
	if !record.GetEnabled() {
		return nil
	}
	observation, err := ObserveSort(ctx, reconciler.meta)
	if err != nil {
		mlog.RatedWarn(ctx, rate.Limit(1), "could not fully observe Standing SORT compaction target", mlog.Err(err))
	}
	return observation.Match
}

func (reconciler *CompactionTargetReconciler) SyncSort(ctx context.Context) (*datapb.CompactionTarget, error) {
	return reconciler.CompactionTargetMeta.SyncSort(ctx, reconciler.allocator, Params.DataCoordCfg.EnableSortCompaction.GetAsBool())
}

// SelectSort handles the ordinary post-flush notification using the same
// confirmed source condition and input rules as the periodic sweep.
func (reconciler *CompactionTargetReconciler) SelectSort(ctx context.Context, segmentID int64) (view.TargetView, error) {
	record, err := reconciler.SyncSort(ctx)
	if err != nil || !record.GetEnabled() {
		return nil, err
	}
	segment := reconciler.meta.GetSegment(ctx, segmentID)
	collections := make(map[int64]collectionPlanningFacts)
	if !policy.MatchesSortCompaction(segment) || !reconciler.selectable(ctx, segment, collections) {
		return nil, nil
	}
	return targetViews([]*meta.SegmentInfo{segment}, collections, true)[0], nil
}

func (reconciler *CompactionTargetReconciler) finishRewrite(ctx context.Context, record *datapb.CompactionTarget, observation *RewriteObservation) error {
	manager := reconciler.CompactionTargetMeta
	defer manager.LockCollectionAdmission(record.GetCollectionID())()
	return manager.Stop(ctx, record.GetTargetID(), StopSatisfied, func(current *datapb.CompactionTarget) (bool, error) {
		if current.GetState() != datapb.TargetState_TARGET_STATE_ACTIVE ||
			!reconciler.meta.GetCollection(current.GetCollectionID()).IsCreated() {
			return false, nil
		}
		// The collection snapshot is the stop decision point. It observes
		// replacement atomically and includes data published since the first
		// observation. The admission lock keeps new tasks from taking inputs
		// between this check and the terminal save.
		currentObservation, err := ObserveRewrite(ctx, reconciler.meta, current, false)
		if err != nil {
			return false, err
		}
		*observation = currentObservation
		if len(currentObservation.Match) != 0 {
			return false, nil
		}
		for _, segment := range currentObservation.Scope {
			if reconciler.meta.IsSegmentCompacting(segment.GetID()) {
				return false, nil
			}
		}
		return true, nil
	})
}

func (reconciler *CompactionTargetReconciler) filterMixCompactionSelectable(
	ctx context.Context,
	segments []*meta.SegmentInfo,
) []*meta.SegmentInfo {
	selectable := make([]*meta.SegmentInfo, 0, len(segments))
	for _, segment := range segments {
		if policy.IsMixCompactionSelectable(segment) {
			selectable = append(selectable, segment)
		}
	}
	if paramtable.Get().DataCoordCfg.IndexBasedCompaction.GetAsBool() {
		return index.FilterInIndexedSegments(ctx, reconciler.handler, reconciler.IndexMeta, true, selectable...)
	}
	return selectable
}
