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
	for _, record := range activeCompactionReasonRecords(reasonMeta.GetCompactionReasonRecords()) {
		switch record.GetReasonType() {
		case datapb.CompactionReasonType_REASON_INTENT_REWRITE:
		default:
			continue
		}

		reason := newCompactionReason(record)
		matches := selector.meta.SelectSegments(ctx, SegmentFilterFunc(func(segment *SegmentInfo) bool {
			return reason.Match(segment)
		}))
		if reason.ReasonSatisfied(matches) {
			if err := reasonMeta.UpdateCompactionReasonRecordState(ctx, record.GetReasonID(), datapb.CompactionReasonState_REASON_STATE_DONE, 0); err != nil {
				return events, err
			}
			log.Ctx(ctx).Info("compaction reason satisfied",
				zap.Int64("reasonID", record.GetReasonID()),
				zap.Int64("collectionID", record.GetScope().GetCollectionID()))
			continue
		}

		for _, segment := range matches {
			if !isNormalManualCompactionCandidate(selector.meta, segment) {
				continue
			}
			events[TriggerTypeReason] = append(events[TriggerTypeReason], compactionReasonView(record, segment))
		}
	}
	sortCompactionReasonViews(events[TriggerTypeReason])
	return events, nil
}

func activeCompactionReasonRecords(records map[int64]*datapb.CompactionReasonRecord) []*datapb.CompactionReasonRecord {
	active := make([]*datapb.CompactionReasonRecord, 0, len(records))
	for _, record := range records {
		if record.GetState() == datapb.CompactionReasonState_REASON_STATE_ACTIVE {
			active = append(active, record)
		}
	}
	sort.Slice(active, func(i, j int) bool {
		return active[i].GetReasonID() < active[j].GetReasonID()
	})
	return active
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
		return views[i].GetSegmentsView()[0].ID < views[j].GetSegmentsView()[0].ID
	})
}
