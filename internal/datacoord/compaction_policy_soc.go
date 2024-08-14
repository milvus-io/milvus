package datacoord

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

type sizeOptimizationCompactionPolicy struct {
	meta      *meta
	allocator allocator
	handler   Handler
}

func newSOCPolicy(meta *meta, allocator allocator, handler Handler) *sizeOptimizationCompactionPolicy {
	return &sizeOptimizationCompactionPolicy{meta: meta, allocator: allocator, handler: handler}
}

func (policy *sizeOptimizationCompactionPolicy) Enable() bool {
	return Params.DataCoordCfg.EnableAutoCompaction.GetAsBool()
}

func (policy *sizeOptimizationCompactionPolicy) Trigger() (map[CompactionTriggerType][]CompactionView, error) {
	ctx := context.Background()
	collections := policy.meta.GetCollections()

	events := make(map[CompactionTriggerType][]CompactionView, 0)
	for _, collection := range collections {
		collectionViews, err := policy.triggerOneCollection(ctx, collection.ID)
		if err != nil {
			// not throw this error because no need to fail because of one collection
			log.Warn("fail to trigger collection soc", zap.Int64("collectionID", collection.ID), zap.Error(err))
			continue
		}

		for triggerType, views := range collectionViews {
			events[triggerType] = append(events[triggerType], views...)
		}
	}
	return events, nil
}

func (policy *sizeOptimizationCompactionPolicy) triggerOneCollection(ctx context.Context, collectionID int64) (map[CompactionTriggerType][]CompactionView, error) {
	log := log.With(zap.Int64("collectionID", collectionID))
	collection, err := policy.handler.GetCollection(ctx, collectionID)
	if err != nil {
		log.Warn("fail to get collection from handler")
		return nil, err
	}
	if collection == nil {
		log.Warn("collection not exist")
		return nil, nil
	}
	if !isCollectionAutoCompactionEnabled(collection) {
		log.RatedInfo(20, "collection auto compaction disabled")
		return nil, nil
	}

	newTriggerID, err := policy.allocator.allocID(ctx)
	if err != nil {
		log.Warn("fail to allocate triggerID", zap.Error(err))
		return nil, err
	}

	collectionTTL, err := getCollectionTTL(collection.Properties)
	if err != nil {
		log.Warn("get collection ttl failed, skip to handle compaction")
		return nil, err
	}

	partSegments := policy.meta.GetSegmentsChanPart(func(segment *SegmentInfo) bool {
		return segment.CollectionID == collectionID &&
			isSegmentHealthy(segment) &&
			isFlush(segment) &&
			!segment.isCompacting && // not compacting now
			!segment.GetIsImporting() // not importing now
	})

	targetSize := getExpectedSegmentSize(policy.meta, collection)
	views := make(map[CompactionTriggerType][]CompactionView)
	for _, group := range partSegments {
		if Params.DataCoordCfg.IndexBasedCompaction.GetAsBool() {
			group.segments = FilterInIndexedSegments(policy.handler, policy.meta, group.segments...)
		}

		// Target on L2 segment only
		segDeltaViews := lo.FilterMap(group.segments, func(info *SegmentInfo, _ int) (CompactionView, bool) {
			if info.GetLevel() == datapb.SegmentLevel_L2 {
				segmentView := GetViewsByInfo(info)[0]
				if segmentView.DeltaSize > 0 || segmentView.DeltalogCount > 0 {
					view := &SegmentDeltaView{
						label:         segmentView.label,
						segments:      []*SegmentView{segmentView},
						collectionTTL: collectionTTL,
						triggerID:     newTriggerID,
					}
					return view, true
				}
			}
			return nil, false
		})

		// Target on L1 segment only
		segOversizeViews := lo.FilterMap(group.segments, func(info *SegmentInfo, _ int) (CompactionView, bool) {
			if info.GetLevel() == datapb.SegmentLevel_L1 {
				segmentView := GetViewsByInfo(info)[0]
				if segmentView.Size > paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsFloat() {
					view := NewOversizedSegmentView(
						segmentView.label,
						segmentView,
						newTriggerID,
						collectionTTL,
						targetSize,
					)
					return view, true
				}
			}
			return nil, false
		})

		views[TriggerTypeDeltaTooMuch] = append(views[TriggerTypeDeltaTooMuch], segDeltaViews...)
		views[TriggerTypeOverSize] = append(views[TriggerTypeOverSize], segOversizeViews...)
	}
	return views, nil
}

type OversizedSegmentView struct {
	label         *CompactionGroupLabel
	segments      []*SegmentView
	collectionTTL time.Duration
	triggerID     int64

	maxSize int64
}

func NewOversizedSegmentView(
	label *CompactionGroupLabel,
	segment *SegmentView,
	triggetID int64,
	collectionTTL time.Duration,
	expectedSize int64,
) *OversizedSegmentView {
	return &OversizedSegmentView{
		label:         label,
		segments:      []*SegmentView{segment},
		collectionTTL: collectionTTL,
		triggerID:     triggetID,

		maxSize: expectedSize,
	}
}

func (v *OversizedSegmentView) GetGroupLabel() *CompactionGroupLabel {
	if v == nil {
		return &CompactionGroupLabel{}
	}
	return v.label
}

func (v *OversizedSegmentView) GetSegmentsView() []*SegmentView {
	if v == nil {
		return nil
	}

	return v.segments
}

func (v *OversizedSegmentView) Append(segments ...*SegmentView) {
	if v.segments == nil {
		v.segments = segments
		return
	}

	v.segments = append(v.segments, segments...)
}

func (v *OversizedSegmentView) String() string {
	strs := lo.Map(v.segments, func(segView *SegmentView, _ int) string {
		return segView.String()
	})
	return fmt.Sprintf("maxSize=<%d>, label=<%s>,  segments=%v", v.maxSize, v.label.String(), strs)
}

func (v *OversizedSegmentView) Trigger() (CompactionView, string) {
	if len(v.segments) == 0 || len(v.segments) > 1 {
		return nil, ""
	}

	oneSegment := v.segments[0]
	if oneSegment.Size > float64(v.maxSize)*paramtable.Get().DataCoordCfg.SegmentExpansionRate.GetAsFloat() {
		return v, "segment oversize"
	}

	return nil, ""
}

func (v *OversizedSegmentView) ForceTrigger() (CompactionView, string) {
	panic("implement me")
}

var (
	_ CompactionView = (*SegmentDeltaView)(nil)
	_ CompactionView = (*OversizedSegmentView)(nil)
)

// L2 segment only
// TODO, add L1 segments
type SegmentDeltaView struct {
	label         *CompactionGroupLabel
	segments      []*SegmentView
	collectionTTL time.Duration
	triggerID     int64
}

func (v *SegmentDeltaView) GetGroupLabel() *CompactionGroupLabel {
	if v == nil {
		return &CompactionGroupLabel{}
	}
	return v.label
}

func (v *SegmentDeltaView) GetSegmentsView() []*SegmentView {
	if v == nil {
		return nil
	}

	return v.segments
}

func (v *SegmentDeltaView) Append(segments ...*SegmentView) {
	if v.segments == nil {
		v.segments = segments
		return
	}

	v.segments = append(v.segments, segments...)
}

func (v *SegmentDeltaView) String() string {
	strs := lo.Map(v.segments, func(segView *SegmentView, _ int) string {
		return segView.String()
	})
	return fmt.Sprintf("label=<%s>,  segments=%v", v.label.String(), strs)
}

func (v *SegmentDeltaView) Trigger() (CompactionView, string) {
	if len(v.segments) == 0 || len(v.segments) > 1 {
		return nil, ""
	}

	oneSegment := v.segments[0]
	if deltaLogCountReachLimit(int64(oneSegment.DeltalogCount)) {
		return v, "deltalog count reaches limit"
	}

	if deltaRowCountRationReachLimit(oneSegment.DeltaRowCount, oneSegment.NumOfRows) {
		return v, "delta rowcount ratio reaches limit"
	}
	if deltaLogSizeReachLimit(int64(oneSegment.DeltaSize)) {
		return v, "deltalog size reaches limit"
	}

	return nil, ""
}

func (v *SegmentDeltaView) ForceTrigger() (CompactionView, string) {
	panic("implement me")
}

func deltaLogCountReachLimit(deltalogCount int64) bool {
	return deltalogCount > paramtable.Get().DataCoordCfg.SingleCompactionDeltalogMaxNum.GetAsInt64()
}

func deltaRowCountRationReachLimit(deltaRowCount int64, numRows int64) bool {
	return float64(deltaRowCount)/float64(numRows) >= paramtable.Get().DataCoordCfg.SingleCompactionRatioThreshold.GetAsFloat()
}

func deltaLogSizeReachLimit(deltaSize int64) bool {
	return deltaSize > paramtable.Get().DataCoordCfg.SingleCompactionDeltaLogMaxSize.GetAsInt64()
}
