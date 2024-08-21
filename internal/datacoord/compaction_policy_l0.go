package datacoord

import (
	"fmt"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type L0CompactionPolicy struct{}

func (policy *L0CompactionPolicy) Enabled() bool {
	return paramtable.Get().DataCoordCfg.EnableAutoCompaction.GetAsBool() && paramtable.Get().DataCoordCfg.EnableLevelZeroSegment.GetAsBool()
}

func (policy *L0CompactionPolicy) Trigger(input CompactionView) (CompactionView, string) {
	v, ok := input.(*LevelZeroSegmentsView)
	if !ok {
		log.Warn("Failed to trigger compaction because input is not LevelZeroSegmentsView")
		return nil, ""
	}

	// Only choose segments with position less than the earliest growing segment position
	validSegments := lo.Filter(v.segments, func(view *SegmentView, _ int) bool {
		return view.dmlPos.GetTimestamp() < v.earliestGrowingSegmentPos.GetTimestamp()
	})

	targetViews, reason := minCountSizeTrigger(validSegments)
	if len(targetViews) > 0 {
		return &LevelZeroSegmentsView{
			label:                     v.label,
			segments:                  targetViews,
			earliestGrowingSegmentPos: v.earliestGrowingSegmentPos,
		}, reason
	}

	return nil, ""
}

func (policy *L0CompactionPolicy) ForceTrigger(input CompactionView) (CompactionView, string) {
	v, ok := input.(*LevelZeroSegmentsView)
	if !ok {
		log.Warn("Failed to force trigger compaction because input is not LevelZeroSegmentsView")
		return nil, ""
	}

	// Only choose segments with position less than the earliest growing segment position
	validSegments := lo.Filter(v.segments, func(view *SegmentView, _ int) bool {
		return view.dmlPos.GetTimestamp() < v.earliestGrowingSegmentPos.GetTimestamp()
	})

	targetViews, reason := forceTrigger(validSegments)
	if len(targetViews) > 0 {
		return &LevelZeroSegmentsView{
			label:                     v.label,
			segments:                  targetViews,
			earliestGrowingSegmentPos: v.earliestGrowingSegmentPos,
		}, reason
	}

	return nil, ""
}

// minCountSizeTrigger tries to trigger LevelZeroCompaction when segmentViews reaches minimum trigger conditions:
// 1. count >= minDeltaCount, OR
// 2. size >= minDeltaSize
func minCountSizeTrigger(segments []*SegmentView) (picked []*SegmentView, reason string) {
	var (
		minDeltaSize  = paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerMinSize.GetAsFloat()
		maxDeltaSize  = paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerMaxSize.GetAsFloat()
		minDeltaCount = paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerDeltalogMinNum.GetAsInt64()
		maxDeltaCount = paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerDeltalogMaxNum.GetAsInt64()
	)

	pickedSize := float64(0)
	pickedCount := int64(0)

	// count >= minDeltaCount
	if lo.SumBy(segments, func(view *SegmentView) int64 { return view.DeltalogCount }) >= minDeltaCount {
		picked, pickedSize, pickedCount = pickByMaxCountSize(segments, maxDeltaSize, maxDeltaCount)
		reason = fmt.Sprintf("level zero segments count reaches minForceTriggerCountLimit=%d, pickedSize=%.2fB, pickedCount=%d", minDeltaCount, pickedSize, pickedCount)
		return
	}

	// size >= minDeltaSize
	if lo.SumBy(segments, func(view *SegmentView) float64 { return view.DeltaSize }) >= minDeltaSize {
		picked, pickedSize, pickedCount = pickByMaxCountSize(segments, maxDeltaSize, maxDeltaCount)
		reason = fmt.Sprintf("level zero segments size reaches minForceTriggerSizeLimit=%.2fB, pickedSize=%.2fB, pickedCount=%d", minDeltaSize, pickedSize, pickedCount)
		return
	}

	return
}

// forceTrigger tries to trigger LevelZeroCompaction even when segmentsViews don't meet the minimum condition,
// the picked plan is still satisfied with the maximum condition
func forceTrigger(segments []*SegmentView) (picked []*SegmentView, reason string) {
	var (
		maxDeltaSize  = paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerMaxSize.GetAsFloat()
		maxDeltaCount = paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerDeltalogMaxNum.GetAsInt64()
	)

	picked, pickedSize, pickedCount := pickByMaxCountSize(segments, maxDeltaSize, maxDeltaCount)
	reason = fmt.Sprintf("level zero views force to trigger, pickedSize=%.2fB, pickedCount=%d", pickedSize, pickedCount)
	return picked, reason
}

// pickByMaxCountSize picks segments that count <= maxCount or size <= maxSize
func pickByMaxCountSize(segments []*SegmentView, maxSize float64, maxCount int64) (picked []*SegmentView, pickedSize float64, pickedCount int64) {
	idx := 0
	for _, view := range segments {
		targetCount := view.DeltalogCount + pickedCount
		targetSize := view.DeltaSize + pickedSize

		if (pickedCount != 0 && pickedSize != float64(0)) && (targetSize > maxSize || targetCount > maxCount) {
			break
		}

		pickedCount = targetCount
		pickedSize = targetSize
		idx += 1
	}
	return segments[:idx], pickedSize, pickedCount
}

// The LevelZeroSegments keeps the min group
type LevelZeroSegmentsView struct {
	label                     *CompactionGroupLabel
	segments                  []*SegmentView
	earliestGrowingSegmentPos *msgpb.MsgPosition
}

var _ CompactionView = (*LevelZeroSegmentsView)(nil)

func (v *LevelZeroSegmentsView) String() string {
	l0strings := lo.Map(v.segments, func(v *SegmentView, _ int) string {
		return v.LevelZeroString()
	})

	count := lo.SumBy(v.segments, func(v *SegmentView) int64 {
		return v.DeltaRowCount
	})
	return fmt.Sprintf("L0SegCount=%d, DeltaRowCount=%d, label=<%s>, posT=<%v>, L0 segments=%v",
		len(v.segments),
		count,
		v.label.String(),
		v.earliestGrowingSegmentPos.GetTimestamp(),
		l0strings)
}

func (v *LevelZeroSegmentsView) Append(segments ...*SegmentView) {
	if v.segments != nil {
		v.segments = append(v.segments, segments...)
		return
	}

	v.segments = segments
}

func (v *LevelZeroSegmentsView) GetGroupLabel() *CompactionGroupLabel {
	return v.label
}

func (v *LevelZeroSegmentsView) GetSegmentsView() []*SegmentView {
	return v.segments
}
