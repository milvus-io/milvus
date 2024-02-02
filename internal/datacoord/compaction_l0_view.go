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

// Trigger triggers all qualified LevelZeroSegments according to views
func (v *LevelZeroSegmentsView) Trigger() (CompactionView, string) {
	// Only choose segments with position less than the earliest growing segment position
	validSegments := lo.Filter(v.segments, func(view *SegmentView, _ int) bool {
		return view.dmlPos.GetTimestamp() < v.earliestGrowingSegmentPos.GetTimestamp()
	})

	var (
		minDeltaSize  = paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerMinSize.GetAsFloat()
		maxDeltaSize  = paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerMaxSize.GetAsFloat()
		minDeltaCount = paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerDeltalogMinNum.GetAsInt()
		maxDeltaCount = paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerDeltalogMaxNum.GetAsInt()
	)

	targetViews, targetSize := v.filterViewsBySizeRange(validSegments, minDeltaSize, maxDeltaSize)
	if targetViews != nil {
		reason := fmt.Sprintf("level zero segments size reaches compaction limit, curDeltaSize=%.2f, limitSizeRange=[%.2f, %.2f]",
			targetSize, minDeltaSize, maxDeltaSize)
		return &LevelZeroSegmentsView{
			label:                     v.label,
			segments:                  targetViews,
			earliestGrowingSegmentPos: v.earliestGrowingSegmentPos,
		}, reason
	}

	targetViews, targetCount := v.filterViewsByCountRange(validSegments, minDeltaCount, maxDeltaCount)
	if targetViews != nil {
		reason := fmt.Sprintf("level zero segments count reaches compaction limit, curDeltaCount=%d, limitCountRange=[%d, %d]",
			targetCount, minDeltaCount, maxDeltaCount)
		return &LevelZeroSegmentsView{
			label:                     v.label,
			segments:                  targetViews,
			earliestGrowingSegmentPos: v.earliestGrowingSegmentPos,
		}, reason
	}

	return nil, ""
}

// filterViewByCountRange picks segment views that total sizes in range [minCount, maxCount]
func (v *LevelZeroSegmentsView) filterViewsByCountRange(segments []*SegmentView, minCount, maxCount int) ([]*SegmentView, int) {
	curDeltaCount := 0
	idx := 0
	for _, view := range segments {
		targetCount := view.DeltalogCount + curDeltaCount
		if idx != 0 && targetCount > maxCount {
			break
		}

		idx += 1
		curDeltaCount = targetCount
	}

	if curDeltaCount < minCount {
		return nil, 0
	}

	return segments[:idx], curDeltaCount
}

// filterViewBySizeRange picks segment views that total count in range [minSize, maxSize]
func (v *LevelZeroSegmentsView) filterViewsBySizeRange(segments []*SegmentView, minSize, maxSize float64) ([]*SegmentView, float64) {
	var curDeltaSize float64
	idx := 0
	for _, view := range segments {
		targetSize := view.DeltaSize + curDeltaSize
		if idx != 0 && targetSize > maxSize {
			break
		}

		idx += 1
		curDeltaSize = targetSize
	}

	if curDeltaSize < minSize {
		return nil, 0
	}

	return segments[:idx], curDeltaSize
}
