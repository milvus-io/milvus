package targets

import (
	"cmp"
	"context"
	"slices"
	"time"

	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
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
	collectionAvailable  func(context.Context, int64) (bool, error)
}

var _ policy.CompactionPolicy = (*CompactionTargetReconciler)(nil)

func NewCompactionTargetReconciler(mt *meta.Meta, indexMeta *index.IndexMeta, compactionTargetMeta *CompactionTargetMeta, handler meta.Handler, collectionAvailable func(context.Context, int64) (bool, error)) *CompactionTargetReconciler {
	return &CompactionTargetReconciler{
		meta:                 mt,
		IndexMeta:            indexMeta,
		CompactionTargetMeta: compactionTargetMeta,
		handler:              handler,
		collectionAvailable:  collectionAvailable,
	}
}

func (reconciler *CompactionTargetReconciler) Enable() bool {
	return reconciler != nil &&
		reconciler.meta != nil &&
		reconciler.CompactionTargetMeta != nil &&
		reconciler.collectionAvailable != nil
}

func (reconciler *CompactionTargetReconciler) Name() string {
	return "CompactionTargetReconciler"
}

func (reconciler *CompactionTargetReconciler) Trigger(ctx context.Context) (map[policy.CompactionTriggerType][]view.CompactionView, error) {
	return reconciler.Reconcile(ctx)
}

func (reconciler *CompactionTargetReconciler) Reconcile(ctx context.Context) (map[policy.CompactionTriggerType][]view.CompactionView, error) {
	events := map[policy.CompactionTriggerType][]view.CompactionView{}
	if !reconciler.Enable() {
		return events, nil
	}

	manager := reconciler.CompactionTargetMeta
	manager.refreshUnconfirmedCreates(ctx)
	records := manager.GetActiveCompactionTargets()
	matches := make(map[int64]*meta.SegmentInfo)
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		available, err := reconciler.collectionAvailable(ctx, record.GetCollectionID())
		if err != nil {
			mlog.RatedWarn(ctx, rate.Limit(1), "could not check compaction target collection",
				mlog.FieldCollectionID(record.GetCollectionID()), mlog.Err(err))
		}
		if !available || err != nil {
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

	// Duplicate targets contribute one physical input per round. Keep the
	// Target out of the Task's TriggerID so the Task survives independently.
	sharedSelectable := make([]*meta.SegmentInfo, 0, len(matches))
	blocked := make(map[int64]bool)
	ttls := make(map[int64]time.Duration)
	for _, segment := range matches {
		collectionID := segment.GetCollectionID()
		collectionBlocked, checked := blocked[collectionID]
		if !checked {
			collection, err := reconciler.handler.GetCollection(ctx, collectionID)
			if err != nil {
				mlog.RatedWarn(ctx, rate.Limit(1), "could not load collection for compaction target planning",
					mlog.FieldCollectionID(collectionID), mlog.Err(err))
			}
			collectionBlocked = err != nil || collection == nil || collection.IsExternal() || reconciler.meta.IsCollectionCompactionBlocked(collectionID)
			if !collectionBlocked {
				ttl, err := common.GetCollectionTTLFromMap(collection.Properties)
				if err != nil {
					mlog.RatedWarn(ctx, rate.Limit(1), "could not determine collection TTL for compaction target",
						mlog.FieldCollectionID(collectionID), mlog.Err(err))
					collectionBlocked = true
				}
				ttls[collectionID] = ttl
			}
			blocked[collectionID] = collectionBlocked
		}
		if !collectionBlocked && policy.IsSharedCompactionSelectable(reconciler.meta, segment) {
			// Reoffer eligible Sort work for a matched segment if its
			// reactive notification was missed, including with auto off.
			if paramtable.Get().DataCoordCfg.EnableSortCompaction.GetAsBool() && policy.CanTriggerSortCompaction(segment) {
				views := view.GetViewsByInfo(segment)
				events[policy.TriggerTypeSort] = append(events[policy.TriggerTypeSort], &policy.MixSegmentView{
					Label: views[0].Label, Segments: views, CollectionTTL: ttls[collectionID],
				})
				continue
			}
			sharedSelectable = append(sharedSelectable, segment)
		}
	}
	selectable := reconciler.filterMixCompactionSelectable(ctx, sharedSelectable)
	slices.SortFunc(selectable, func(a, b *meta.SegmentInfo) int {
		if a.GetCollectionID() != b.GetCollectionID() {
			return cmp.Compare(a.GetCollectionID(), b.GetCollectionID())
		}
		return cmp.Compare(a.GetID(), b.GetID())
	})
	for _, segment := range selectable {
		views := view.GetViewsByInfo(segment)
		events[policy.TriggerTypeSingle] = append(events[policy.TriggerTypeSingle], &policy.MixSegmentView{
			Label:         views[0].Label,
			Segments:      views,
			CollectionTTL: ttls[segment.GetCollectionID()],
		})
	}
	return events, ctx.Err()
}

func (reconciler *CompactionTargetReconciler) finishRewrite(ctx context.Context, record *datapb.CompactionTarget, observation *RewriteObservation) error {
	manager := reconciler.CompactionTargetMeta
	defer manager.LockCollectionAdmission(record.GetCollectionID())()
	return manager.Stop(ctx, record.GetTargetID(), StopSatisfied, func(current *datapb.CompactionTarget) (bool, error) {
		if current.GetState() != datapb.TargetState_TARGET_STATE_ACTIVE {
			return false, nil
		}
		available, err := reconciler.collectionAvailable(ctx, current.GetCollectionID())
		if err != nil || !available {
			return false, err
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
