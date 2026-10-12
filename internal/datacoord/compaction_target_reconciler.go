package datacoord

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// compactionTargetReconciler converges segments toward declared compaction
// targets: each tick it compares active CompactionTargets (the desired state of
// the data) against live segment facts (the actual state) and emits compaction
// views for segments that still miss their target. It stores no progress - a
// target is satisfied when no in-scope segment matches its predicate anymore.
type compactionTargetReconciler struct {
	meta    *meta
	handler Handler
}

var _ CompactionPolicy = (*compactionTargetReconciler)(nil)

func newCompactionTargetReconciler(meta *meta, handler Handler) *compactionTargetReconciler {
	return &compactionTargetReconciler{
		meta:    meta,
		handler: handler,
	}
}

func (reconciler *compactionTargetReconciler) Enable() bool {
	return paramtable.Get().DataCoordCfg.EnableTargetBasedCompaction.GetAsBool() &&
		reconciler != nil &&
		reconciler.meta != nil &&
		reconciler.meta.GetCompactionTargetMeta() != nil
}

func (reconciler *compactionTargetReconciler) Name() string {
	return "CompactionTargetReconciler"
}

func (reconciler *compactionTargetReconciler) Trigger(ctx context.Context) (map[CompactionTriggerType][]CompactionView, error) {
	return reconciler.Reconcile(ctx)
}

func (reconciler *compactionTargetReconciler) Reconcile(ctx context.Context) (map[CompactionTriggerType][]CompactionView, error) {
	events := map[CompactionTriggerType][]CompactionView{
		TriggerTypeSingle: nil,
	}
	if !reconciler.Enable() {
		return events, nil
	}

	targetMeta := reconciler.meta.GetCompactionTargetMeta()
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
		remaining := maxEvents - len(events[TriggerTypeSingle])
		if remaining <= 0 {
			continue
		}
		events[TriggerTypeSingle] = append(
			events[TriggerTypeSingle],
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

func (reconciler *compactionTargetReconciler) compactionViews(
	ctx context.Context,
	record *datapb.CompactionTarget,
	compactionType datapb.CompactionType,
	matches []*SegmentInfo,
	limit int,
) []CompactionView {
	blockedCollections := make(map[int64]bool)
	sharedSelectable := make([]*SegmentInfo, 0, len(matches))
	for _, segment := range matches {
		collectionID := segment.GetCollectionID()
		blocked, checked := blockedCollections[collectionID]
		if !checked {
			blocked = reconciler.meta.isCollectionCompactionBlocked(collectionID)
			blockedCollections[collectionID] = blocked
		}
		if blocked || !isSharedCompactionSelectable(reconciler.meta, segment) {
			continue
		}
		sharedSelectable = append(sharedSelectable, segment)
	}

	switch compactionType {
	case datapb.CompactionType_MixCompaction:
		selectable := reconciler.filterMixCompactionSelectable(ctx, sharedSelectable)
		views := make([]CompactionView, 0, min(len(selectable), limit))
		for _, segment := range selectable {
			if len(views) >= limit {
				break
			}
			segmentViews := GetViewsByInfo(segment)
			views = append(views, &MixSegmentView{
				label:     segmentViews[0].label,
				segments:  segmentViews,
				triggerID: record.GetTargetID(),
			})
		}
		return views
	default:
		return nil
	}
}

func (reconciler *compactionTargetReconciler) filterMixCompactionSelectable(
	ctx context.Context,
	segments []*SegmentInfo,
) []*SegmentInfo {
	selectable := make([]*SegmentInfo, 0, len(segments))
	for _, segment := range segments {
		if isMixCompactionSelectable(segment) {
			selectable = append(selectable, segment)
		}
	}
	if paramtable.Get().DataCoordCfg.IndexBasedCompaction.GetAsBool() {
		return FilterInIndexedSegments(ctx, reconciler.handler, reconciler.meta, true, selectable...)
	}
	return selectable
}
