package datacoord

import (
	"context"
	"sort"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type compactionReasonPolicy struct {
	meta *meta
}

var _ CompactionPolicy = (*compactionReasonPolicy)(nil)

func newCompactionReasonPolicy(meta *meta) *compactionReasonPolicy {
	return &compactionReasonPolicy{meta: meta}
}

func (policy *compactionReasonPolicy) Enable() bool {
	return paramtable.Get().DataCoordCfg.EnableCompactionReasonRecord.GetAsBool() &&
		policy != nil &&
		policy.meta != nil &&
		policy.meta.GetCompactionReasonMeta() != nil
}

func (policy *compactionReasonPolicy) Name() string {
	return "CompactionReasonPolicy"
}

func (policy *compactionReasonPolicy) Trigger(ctx context.Context) (map[CompactionTriggerType][]CompactionView, error) {
	events := map[CompactionTriggerType][]CompactionView{
		TriggerTypeReasonRewrite: nil,
	}
	if !policy.Enable() {
		return events, nil
	}

	reasonMeta := policy.meta.GetCompactionReasonMeta()
	for _, record := range activeRewriteCompactionReasonRecords(reasonMeta.GetCompactionReasonRecords()) {
		reason := newCompactionReason(record)
		matches := policy.meta.SelectSegments(ctx, SegmentFilterFunc(func(segment *SegmentInfo) bool {
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

		eligible := lo.Filter(matches, func(segment *SegmentInfo, _ int) bool {
			return isNormalManualCompactionCandidate(policy.meta, segment)
		})
		if len(eligible) == 0 {
			continue
		}
		events[TriggerTypeReasonRewrite] = append(events[TriggerTypeReasonRewrite], compactionReasonViews(record, eligible)...)
	}
	return events, nil
}

func activeRewriteCompactionReasonRecords(records map[int64]*datapb.CompactionReasonRecord) []*datapb.CompactionReasonRecord {
	active := make([]*datapb.CompactionReasonRecord, 0, len(records))
	for _, record := range records {
		if record.GetState() == datapb.CompactionReasonState_REASON_STATE_ACTIVE &&
			record.GetReasonType() == datapb.CompactionReasonType_REASON_INTENT_REWRITE &&
			record.GetTailLimit() == 0 {
			active = append(active, record)
		}
	}
	sort.Slice(active, func(i, j int) bool {
		return active[i].GetReasonID() < active[j].GetReasonID()
	})
	return active
}

func compactionReasonViews(record *datapb.CompactionReasonRecord, segments []*SegmentInfo) []CompactionView {
	grouped := groupByPartitionChannel(GetViewsByInfo(segments...))
	views := make([]CompactionView, 0, len(grouped))
	for label, segmentViews := range grouped {
		views = append(views, &MixSegmentView{
			label:     label,
			segments:  segmentViews,
			triggerID: record.GetReasonID(),
		})
	}
	sort.Slice(views, func(i, j int) bool {
		left := views[i].GetGroupLabel()
		right := views[j].GetGroupLabel()
		if left.PartitionID != right.PartitionID {
			return left.PartitionID < right.PartitionID
		}
		return left.Channel < right.Channel
	})
	return views
}
