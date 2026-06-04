package datacoord

import (
	"context"
	"sort"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type compactionReasonSelector struct {
	meta *meta
}

var _ CompactionPolicy = (*compactionReasonSelector)(nil)

func newCompactionReasonSelector(meta *meta) *compactionReasonSelector {
	return &compactionReasonSelector{meta: meta}
}

func (selector *compactionReasonSelector) Enable() bool {
	return paramtable.Get().DataCoordCfg.EnableCompactionReasonRecord.GetAsBool() &&
		selector != nil &&
		selector.meta != nil &&
		selector.meta.GetCompactionReasonMeta() != nil
}

func (selector *compactionReasonSelector) Name() string {
	return "CompactionReasonSelector"
}

func (selector *compactionReasonSelector) Trigger(ctx context.Context) (map[CompactionTriggerType][]CompactionView, error) {
	events := map[CompactionTriggerType][]CompactionView{
		TriggerTypeReason: nil,
	}
	if !selector.Enable() {
		return events, nil
	}

	reasonMeta := selector.meta.GetCompactionReasonMeta()
	reasons := reasonMeta.GetActiveCompactionReasons()
	if len(reasons) == 0 {
		return events, nil
	}

	candidateSegments := selector.selectCompactionReasonCandidates(ctx, reasons)
	segmentsByLabel := groupCompactionReasonSegmentsByLabel(candidateSegments)
	segmentsInScope := make(map[int64][]*SegmentInfo, len(reasons))
	for _, label := range sortedCompactionReasonLabels(segmentsByLabel) {
		groupSegments := segmentsByLabel[label]
		for _, reason := range reasons {
			record := reason.Record()
			scopedSegments := reason.SegmentsInScope(groupSegments)
			if len(scopedSegments) == 0 {
				continue
			}
			segmentsInScope[record.GetReasonID()] = append(segmentsInScope[record.GetReasonID()], scopedSegments...)
			for _, segment := range scopedSegments {
				if !reason.Match(segment) || !isNormalManualCompactionCandidate(selector.meta, segment) {
					continue
				}
				events[TriggerTypeReason] = append(events[TriggerTypeReason], compactionReasonView(record, segment))
			}
		}
	}

	for _, reason := range reasons {
		record := reason.Record()
		if !reason.ReasonSatisfied(segmentsInScope[record.GetReasonID()]) {
			continue
		}
		if err := reasonMeta.UpdateCompactionReasonRecordState(ctx, record.GetReasonID(), datapb.CompactionReasonState_REASON_STATE_DONE, 0); err != nil {
			return events, err
		}
		log.Ctx(ctx).Info("compaction reason satisfied",
			zap.Int64("reasonID", record.GetReasonID()),
			zap.Int64("collectionID", record.GetScope().GetCollectionID()))
	}
	sortCompactionReasonViews(events[TriggerTypeReason])
	return events, nil
}

func (selector *compactionReasonSelector) selectCompactionReasonCandidates(ctx context.Context, reasons []compactionReason) []*SegmentInfo {
	return selector.meta.SelectSegments(ctx, SegmentFilterFunc(func(segment *SegmentInfo) bool {
		for _, reason := range reasons {
			if reason.ScopeIn(segment) {
				return true
			}
		}
		return false
	}))
}

func groupCompactionReasonSegmentsByLabel(segments []*SegmentInfo) map[CompactionGroupLabel][]*SegmentInfo {
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

func sortedCompactionReasonLabels(segmentsByLabel map[CompactionGroupLabel][]*SegmentInfo) []CompactionGroupLabel {
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

func compactionReasonView(record *datapb.CompactionReasonRecord, segment *SegmentInfo) CompactionView {
	segmentViews := GetViewsByInfo(segment)
	return &MixSegmentView{
		label:     segmentViews[0].label,
		segments:  segmentViews,
		triggerID: record.GetReasonID(),
	}
}

func sortCompactionReasonViews(views []CompactionView) {
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
