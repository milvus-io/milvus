package targets

import (
	"context"

	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
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
}

var _ policy.CompactionPolicy = (*CompactionTargetReconciler)(nil)

func NewCompactionTargetReconciler(mt *meta.Meta, indexMeta *index.IndexMeta, compactionTargetMeta *CompactionTargetMeta, handler meta.Handler) *CompactionTargetReconciler {
	return &CompactionTargetReconciler{
		meta:                 mt,
		IndexMeta:            indexMeta,
		CompactionTargetMeta: compactionTargetMeta,
		handler:              handler,
	}
}

func (reconciler *CompactionTargetReconciler) Enable() bool {
	return paramtable.Get().DataCoordCfg.EnableTargetBasedCompaction.GetAsBool() &&
		reconciler != nil &&
		reconciler.meta != nil &&
		reconciler.CompactionTargetMeta != nil
}

func (reconciler *CompactionTargetReconciler) Name() string {
	return "CompactionTargetReconciler"
}

func (reconciler *CompactionTargetReconciler) Trigger(ctx context.Context) (map[policy.CompactionTriggerType][]view.CompactionView, error) {
	return reconciler.Reconcile(ctx)
}

func (reconciler *CompactionTargetReconciler) Reconcile(ctx context.Context) (map[policy.CompactionTriggerType][]view.CompactionView, error) {
	events := map[policy.CompactionTriggerType][]view.CompactionView{
		policy.TriggerTypeSingle: nil,
	}
	if !reconciler.Enable() {
		return events, nil
	}

	targetMeta := reconciler.CompactionTargetMeta
	targets := targetMeta.GetActiveCompactionTargets()
	if len(targets) == 0 {
		return events, nil
	}
	maxEvents := paramtable.Get().DataCoordCfg.TargetCompactionMaxEvents.GetAsInt()

	satisfiedTargets := make([]*datapb.CompactionTarget, 0)
	for _, target := range targets {
		record := target.Clone()
		matches := reconciler.meta.SelectSegments(ctx, target.MatchFilters()...)
		// Satisfaction uses semantic matches before temporary execution
		// blockers. A snapshot-protected segment must keep the target active
		// until the snapshot releases it.
		if target.Satisfied(matches) {
			satisfiedTargets = append(satisfiedTargets, record)
			continue
		}
		remaining := maxEvents - len(events[policy.TriggerTypeSingle])
		if remaining <= 0 {
			continue
		}
		events[policy.TriggerTypeSingle] = append(
			events[policy.TriggerTypeSingle],
			reconciler.compactionViews(ctx, record, target.CompactionType(), matches, remaining)...,
		)
	}

	for _, record := range satisfiedTargets {
		if err := targetMeta.UpdateCompactionTargetState(ctx, record.GetTargetID(), datapb.TargetState_TARGET_STATE_INACTIVE); err != nil {
			return events, err
		}
		mlog.Info(ctx, "compaction target satisfied",
			mlog.Int64("targetID", record.GetTargetID()),
			mlog.FieldCollectionID(record.GetCollectionID()))
	}
	return events, nil
}

func (reconciler *CompactionTargetReconciler) compactionViews(
	ctx context.Context,
	record *datapb.CompactionTarget,
	compactionType datapb.CompactionType,
	matches []*meta.SegmentInfo,
	limit int,
) []view.CompactionView {
	blockedCollections := make(map[int64]bool)
	sharedSelectable := make([]*meta.SegmentInfo, 0, len(matches))
	for _, segment := range matches {
		collectionID := segment.GetCollectionID()
		blocked, checked := blockedCollections[collectionID]
		if !checked {
			blocked = reconciler.meta.IsCollectionCompactionBlocked(collectionID)
			blockedCollections[collectionID] = blocked
		}
		if blocked || !policy.IsSharedCompactionSelectable(reconciler.meta, segment) {
			continue
		}
		sharedSelectable = append(sharedSelectable, segment)
	}

	switch compactionType {
	case datapb.CompactionType_MixCompaction:
		selectable := reconciler.filterMixCompactionSelectable(ctx, sharedSelectable)
		views := make([]view.CompactionView, 0, min(len(selectable), limit))
		for _, segment := range selectable {
			if len(views) >= limit {
				break
			}
			segmentViews := view.GetViewsByInfo(segment)
			views = append(views, &policy.MixSegmentView{
				Label:     segmentViews[0].Label,
				Segments:  segmentViews,
				TriggerID: record.GetTargetID(),
			})
		}
		return views
	default:
		return nil
	}
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
