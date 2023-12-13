package datacoord

import (
	"fmt"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
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
		minDeltaSize  = Params.DataCoordCfg.LevelZeroCompactionTriggerMinSize.GetAsFloat()
		minDeltaCount = Params.DataCoordCfg.LevelZeroCompactionTriggerDeltalogMinNum.GetAsInt()

		curDeltaSize  float64
		curDeltaCount int
		reason        string
	)

	for _, segView := range validSegments {
		curDeltaSize += segView.DeltaSize
		curDeltaCount += segView.DeltalogCount
	}

	if curDeltaSize > minDeltaSize {
		reason = "level zero segments size reaches compaction limit"
	}

	if curDeltaCount > minDeltaCount {
		reason = "level zero segments number reaches compaction limit"
	}

	if curDeltaSize < minDeltaSize && curDeltaCount < minDeltaCount {
		return nil, ""
	}

	return &LevelZeroSegmentsView{
		label:                     v.label,
		segments:                  validSegments,
		earliestGrowingSegmentPos: v.earliestGrowingSegmentPos,
	}, reason
}
