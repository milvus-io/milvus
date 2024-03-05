package datacoord

import (
	"fmt"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

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
	return fmt.Sprintf("label=<%s>, posT=<%v>, l0 segments=%v",
		v.label.String(),
		v.earliestGrowingSegmentPos.GetTimestamp(),
		l0strings)
}

func (v *LevelZeroSegmentsView) Append(segments ...*SegmentView) {
	if v.segments == nil {
		v.segments = segments
		return
	}

	v.segments = append(v.segments, segments...)
}

func (v *LevelZeroSegmentsView) GetGroupLabel() *CompactionGroupLabel {
	if v == nil {
		return &CompactionGroupLabel{}
	}
	return v.label
}

func (v *LevelZeroSegmentsView) GetSegmentsView() []*SegmentView {
	if v == nil {
		return nil
	}

	return v.segments
}

func (v *LevelZeroSegmentsView) Equal(others []*SegmentView) bool {
	if len(v.segments) != len(others) {
		return false
	}

	IDSelector := func(v *SegmentView, _ int) int64 {
		return v.ID
	}

	diffLeft, diffRight := lo.Difference(lo.Map(others, IDSelector), lo.Map(v.segments, IDSelector))

	diffCount := len(diffLeft) + len(diffRight)
	return diffCount == 0
}

// ForceTrigger triggers all qualified LevelZeroSegments according to views
func (v *LevelZeroSegmentsView) ForceTrigger() (CompactionView, string) {
	// Only choose segments with position less than the earliest growing segment position
	validSegments := lo.Filter(v.segments, func(view *SegmentView, _ int) bool {
		return view.dmlPos.GetTimestamp() < v.earliestGrowingSegmentPos.GetTimestamp()
	})

	targetViews, reason := v.forceTrigger(validSegments)
	if len(targetViews) > 0 {
		return &LevelZeroSegmentsView{
			label:                     v.label,
			segments:                  targetViews,
			earliestGrowingSegmentPos: v.earliestGrowingSegmentPos,
		}, reason
	}

	return nil, ""
}

// Trigger triggers all qualified LevelZeroSegments according to views
func (v *LevelZeroSegmentsView) Trigger() (CompactionView, string) {
	// Only choose segments with position less than the earliest growing segment position
	validSegments := lo.Filter(v.segments, func(view *SegmentView, _ int) bool {
		return view.dmlPos.GetTimestamp() < v.earliestGrowingSegmentPos.GetTimestamp()
	})

	targetViews, reason := v.minCountSizeTrigger(validSegments)
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
func (v *LevelZeroSegmentsView) minCountSizeTrigger(segments []*SegmentView) (picked []*SegmentView, reason string) {
	var (
		minDeltaSize  = paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerMinSize.GetAsFloat()
		maxDeltaSize  = paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerMaxSize.GetAsFloat()
		minDeltaCount = paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerDeltalogMinNum.GetAsInt()
		maxDeltaCount = paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerDeltalogMaxNum.GetAsInt()
	)

	curSize := float64(0)

	// count >= minDeltaCount
	if lo.SumBy(segments, func(view *SegmentView) int { return view.DeltalogCount }) >= minDeltaCount {
		picked, curSize = pickByMaxCountSize(segments, maxDeltaSize, maxDeltaCount)
		reason = fmt.Sprintf("level zero segments count reaches minForceTriggerCountLimit=%d, curDeltaSize=%.2f, curDeltaCount=%d", minDeltaCount, curSize, len(segments))
		return
	}

	// size >= minDeltaSize
	if lo.SumBy(segments, func(view *SegmentView) float64 { return view.DeltaSize }) >= minDeltaSize {
		picked, curSize = pickByMaxCountSize(segments, maxDeltaSize, maxDeltaCount)
		reason = fmt.Sprintf("level zero segments size reaches minForceTriggerSizeLimit=%.2f, curDeltaSize=%.2f, curDeltaCount=%d", minDeltaSize, curSize, len(segments))
		return
	}

	return
}

// forceTrigger tries to trigger LevelZeroCompaction even when segmentsViews don't meet the minimum condition,
// the picked plan is still satisfied with the maximum condition
func (v *LevelZeroSegmentsView) forceTrigger(segments []*SegmentView) (picked []*SegmentView, reason string) {
	var (
		maxDeltaSize  = paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerMaxSize.GetAsFloat()
		maxDeltaCount = paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerDeltalogMaxNum.GetAsInt()
	)

	curSize := float64(0)
	picked, curSize = pickByMaxCountSize(segments, maxDeltaSize, maxDeltaCount)
	reason = fmt.Sprintf("level zero views force to trigger, curDeltaSize=%.2f, curDeltaCount=%d", curSize, len(segments))
	return
}

// pickByMaxCountSize picks segments that count <= maxCount or size <= maxSize
func pickByMaxCountSize(segments []*SegmentView, maxSize float64, maxCount int) ([]*SegmentView, float64) {
	var (
		curDeltaCount = 0
		curDeltaSize  = float64(0)
	)
	idx := 0
	for _, view := range segments {
		targetCount := view.DeltalogCount + curDeltaCount
		targetSize := view.DeltaSize + curDeltaSize

		if (curDeltaCount != 0 && curDeltaSize != float64(0)) && (targetSize > maxSize || targetCount > maxCount) {
			break
		}

		curDeltaCount = targetCount
		curDeltaSize = targetSize
		idx += 1
	}
	return segments[:idx], curDeltaSize
}
