package datacoord

import (
	"context"
	"sort"

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
	meta *meta
}

var _ CompactionPolicy = (*compactionTargetReconciler)(nil)

func newCompactionTargetReconciler(meta *meta) *compactionTargetReconciler {
	return &compactionTargetReconciler{meta: meta}
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

	candidateSegments := reconciler.selectCompactionTargetCandidates(ctx, targets)
	segmentsByLabel := groupCompactionTargetSegmentsByLabel(candidateSegments)
	segmentsInScope := make(map[int64]map[CompactionGroupLabel][]*SegmentInfo, len(targets))
	for _, label := range sortedCompactionTargetLabels(segmentsByLabel) {
		groupSegments := segmentsByLabel[label]
		for _, target := range targets {
			record := target.Clone()
			scopedSegments := target.SegmentsInScope(groupSegments)
			if len(scopedSegments) == 0 {
				continue
			}
			if _, ok := segmentsInScope[record.GetTargetID()]; !ok {
				segmentsInScope[record.GetTargetID()] = make(map[CompactionGroupLabel][]*SegmentInfo)
			}
			segmentsInScope[record.GetTargetID()][label] = append(segmentsInScope[record.GetTargetID()][label], scopedSegments...)
			for _, segment := range scopedSegments {
				if !target.Match(segment) || !isNormalManualCompactionCandidate(reconciler.meta, segment) {
					continue
				}
				events[TriggerTypeTarget] = append(events[TriggerTypeTarget], compactionTargetView(record, segment))
			}
		}
	}

	for _, target := range targets {
		record := target.Clone()
		if !target.Satisfied(segmentsInScope[record.GetTargetID()]) {
			continue
		}
		if err := targetMeta.UpdateCompactionTargetState(ctx, record.GetTargetID(), datapb.TargetState_TARGET_STATE_INACTIVE); err != nil {
			return events, err
		}
		mlog.Info(ctx, "compaction target satisfied",
			mlog.Int64("targetID", record.GetTargetID()),
			mlog.FieldCollectionID(record.GetCollectionID()))
	}
	sortCompactionTargetViews(events[TriggerTypeTarget])
	return events, nil
}

func (reconciler *compactionTargetReconciler) selectCompactionTargetCandidates(ctx context.Context, targets []*compactionTarget) []*SegmentInfo {
	return reconciler.meta.SelectSegments(ctx, SegmentFilterFunc(func(segment *SegmentInfo) bool {
		if !isSegmentHealthy(segment) {
			return false
		}
		for _, target := range targets {
			if target.ScopeIn(segment) {
				return true
			}
		}
		return false
	}))
}

func groupCompactionTargetSegmentsByLabel(segments []*SegmentInfo) map[CompactionGroupLabel][]*SegmentInfo {
	segmentsByLabel := make(map[CompactionGroupLabel][]*SegmentInfo)
	for _, segment := range segments {
		label := CompactionGroupLabel{
			CollectionID: segment.GetCollectionID(),
			PartitionID:  segment.GetPartitionID(),
			Channel:      segment.GetInsertChannel(),
		}
		segmentsByLabel[label] = append(segmentsByLabel[label], segment)
	}
	return segmentsByLabel
}

func sortedCompactionTargetLabels(segmentsByLabel map[CompactionGroupLabel][]*SegmentInfo) []CompactionGroupLabel {
	labels := make([]CompactionGroupLabel, 0, len(segmentsByLabel))
	for label := range segmentsByLabel {
		labels = append(labels, label)
	}
	sort.Slice(labels, func(i, j int) bool {
		if labels[i].CollectionID != labels[j].CollectionID {
			return labels[i].CollectionID < labels[j].CollectionID
		}
		if labels[i].PartitionID != labels[j].PartitionID {
			return labels[i].PartitionID < labels[j].PartitionID
		}
		return labels[i].Channel < labels[j].Channel
	})
	return labels
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
