package datacoord

import (
	"context"
	"sort"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/log"
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
		TriggerTypeTarget: nil,
	}
	if !reconciler.Enable() {
		return events, nil
	}

	targetMeta := reconciler.meta.GetCompactionTargetMeta()
	targets := targetMeta.GetActiveCompactionTargets()
	if len(targets) == 0 {
		return events, nil
	}

	matchCandidateFilter := SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return isNormalManualCompactionMatchCandidate(reconciler.meta, segment)
	})
	satisfiedTargets := make([]*datapb.CompactionTarget, 0)
	for _, target := range targets {
		record := target.Clone()
		filters := append([]SegmentFilter{matchCandidateFilter}, target.SegmentFilters()...)
		matches := reconciler.meta.SelectSegments(ctx, filters...)
		if target.Satisfied(matches) {
			satisfiedTargets = append(satisfiedTargets, record)
			continue
		}
		for _, segment := range reconciler.filterExecutable(ctx, matches) {
			events[TriggerTypeTarget] = append(events[TriggerTypeTarget], compactionTargetView(record, segment))
		}
	}

	for _, record := range satisfiedTargets {
		if err := targetMeta.UpdateCompactionTargetState(ctx, record.GetTargetID(), datapb.TargetState_TARGET_STATE_INACTIVE); err != nil {
			return events, err
		}
		log.Ctx(ctx).Info("compaction target satisfied",
			zap.Int64("targetID", record.GetTargetID()),
			zap.Int64("collectionID", record.GetCollectionID()))
	}
	sortCompactionTargetViews(events[TriggerTypeTarget])
	return events, nil
}

func (reconciler *compactionTargetReconciler) filterExecutable(ctx context.Context, matches []*SegmentInfo) []*SegmentInfo {
	blockedCollections := make(map[int64]bool)
	executable := make([]*SegmentInfo, 0, len(matches))
	for _, segment := range matches {
		collectionID := segment.GetCollectionID()
		blocked, checked := blockedCollections[collectionID]
		if !checked {
			blocked = reconciler.meta.isCollectionCompactionBlocked(collectionID)
			blockedCollections[collectionID] = blocked
		}
		if blocked || !isNormalManualCompactionExecutionCandidate(reconciler.meta, segment) {
			continue
		}
		executable = append(executable, segment)
	}
	if paramtable.Get().DataCoordCfg.IndexBasedCompaction.GetAsBool() {
		return FilterInIndexedSegments(ctx, reconciler.handler, reconciler.meta, true, executable...)
	}
	return executable
}

func compactionTargetView(record *datapb.CompactionTarget, segment *SegmentInfo) CompactionView {
	segmentViews := GetViewsByInfo(segment)
	return &MixSegmentView{
		label:     segmentViews[0].label,
		segments:  segmentViews,
		triggerID: record.GetTargetID(),
	}
}

func sortCompactionTargetViews(views []CompactionView) {
	sort.Slice(views, func(i, j int) bool {
		left := views[i].GetGroupLabel()
		right := views[j].GetGroupLabel()
		if left.CollectionID != right.CollectionID {
			return left.CollectionID < right.CollectionID
		}
		if left.PartitionID != right.PartitionID {
			return left.PartitionID < right.PartitionID
		}
		if left.Channel != right.Channel {
			return left.Channel < right.Channel
		}
		if views[i].GetSegmentsView()[0].ID != views[j].GetSegmentsView()[0].ID {
			return views[i].GetSegmentsView()[0].ID < views[j].GetSegmentsView()[0].ID
		}
		return views[i].GetTriggerID() < views[j].GetTriggerID()
	})
}
