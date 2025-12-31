package datacoord

import (
	"fmt"
	"sort"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// LevelZeroCompactionView holds all compactable L0 segments in a compaction group
// Trigger use static algorithm, it will selects l0Segments according to the min and max threshold
//
//	to limit the memory and io usages per L0 Compaction.
//
// Given the same l0Segments, Trigger is idempotent, it'll give consist result.
type LevelZeroCompactionView struct {
	triggerID       int64
	label           *CompactionGroupLabel
	l0Segments      []*SegmentView
	latestDeletePos *msgpb.MsgPosition
}

var _ CompactionView = (*LevelZeroCompactionView)(nil)

func (v *LevelZeroCompactionView) String() string {
	l0strings := lo.Map(v.l0Segments, func(v *SegmentView, _ int) string {
		return v.LevelZeroString()
	})

	count := lo.SumBy(v.l0Segments, func(v *SegmentView) int {
		return v.DeltaRowCount
	})
	return fmt.Sprintf("L0SegCount=%d, DeltaRowCount=%d, label=<%s>, posT=<%v>, L0 segments=%v",
		len(v.l0Segments),
		count,
		v.label.String(),
		v.latestDeletePos.GetTimestamp(),
		l0strings)
}

func (v *LevelZeroCompactionView) Append(segments ...*SegmentView) {
	if v.l0Segments == nil {
		v.l0Segments = segments
		return
	}

	v.l0Segments = append(v.l0Segments, segments...)
}

func (v *LevelZeroCompactionView) GetGroupLabel() *CompactionGroupLabel {
	if v == nil {
		return &CompactionGroupLabel{}
	}
	return v.label
}

func (v *LevelZeroCompactionView) GetSegmentsView() []*SegmentView {
	if v == nil {
		return nil
	}

	return v.l0Segments
}

// ForceTrigger triggers all qualified LevelZeroSegments according to views
func (v *LevelZeroCompactionView) ForceTrigger() (CompactionView, string) {
	sort.Slice(v.l0Segments, func(i, j int) bool {
		return v.l0Segments[i].dmlPos.GetTimestamp() < v.l0Segments[j].dmlPos.GetTimestamp()
	})

	targetViews, reason := v.forceTrigger(v.l0Segments)

	// Use the max dmlPos timestamp as the latestDeletePos
	latestL0 := lo.MaxBy(targetViews, func(view1, view2 *SegmentView) bool {
		return view1.dmlPos.GetTimestamp() > view2.dmlPos.GetTimestamp()
	})
	if len(targetViews) > 0 {
		return &LevelZeroCompactionView{
			label:           v.label,
			l0Segments:      targetViews,
			latestDeletePos: latestL0.dmlPos,
			triggerID:       v.triggerID,
		}, reason
	}

	return nil, ""
}

func (v *LevelZeroCompactionView) ForceTriggerAll() ([]CompactionView, string) {
	sort.Slice(v.l0Segments, func(i, j int) bool {
		return v.l0Segments[i].dmlPos.GetTimestamp() < v.l0Segments[j].dmlPos.GetTimestamp()
	})

	var resultViews []CompactionView
	remainingSegments := v.l0Segments

	// Multi-round force trigger loop
	for len(remainingSegments) > 0 {
		targetViews, _ := v.forceTrigger(remainingSegments)
		if len(targetViews) == 0 {
			// No more segments can be force triggered, break the loop
			break
		}

		// Create a new LevelZeroSegmentsView for this round's target views
		latestL0 := lo.MaxBy(targetViews, func(view1, view2 *SegmentView) bool {
			return view1.dmlPos.GetTimestamp() > view2.dmlPos.GetTimestamp()
		})
		roundView := &LevelZeroCompactionView{
			label:           v.label,
			l0Segments:      targetViews,
			latestDeletePos: latestL0.dmlPos,
			triggerID:       v.triggerID,
		}
		resultViews = append(resultViews, roundView)

		// Remove the target segments from remaining segments for next round
		targetSegmentIDs := lo.Map(targetViews, func(view *SegmentView, _ int) int64 {
			return view.ID
		})
		remainingSegments = lo.Filter(remainingSegments, func(view *SegmentView, _ int) bool {
			return !lo.Contains(targetSegmentIDs, view.ID)
		})
	}

	return resultViews, "force trigger all"
}

func (v *LevelZeroCompactionView) GetTriggerID() int64 {
	return v.triggerID
}

// Trigger triggers all qualified LevelZeroSegments according to views
func (v *LevelZeroCompactionView) Trigger() (CompactionView, string) {
	latestL0 := lo.MaxBy(v.l0Segments, func(view1, view2 *SegmentView) bool {
		return view1.dmlPos.GetTimestamp() > view2.dmlPos.GetTimestamp()
	})

	targetViews, reason := v.minCountSizeTrigger(v.l0Segments)
	if len(targetViews) > 0 {
		return &LevelZeroCompactionView{
			label:           v.label,
			l0Segments:      targetViews,
			latestDeletePos: latestL0.dmlPos,
			triggerID:       v.triggerID,
		}, reason
	}

	return nil, ""
}

// minCountSizeTrigger tries to trigger LevelZeroCompaction when segmentViews reaches minimum trigger conditions:
// 1. count >= minDeltaCount, OR
// 2. size >= minDeltaSize
func (v *LevelZeroCompactionView) minCountSizeTrigger(segments []*SegmentView) (picked []*SegmentView, reason string) {
	var (
		minDeltaSize  = paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerMinSize.GetAsFloat()
		maxDeltaSize  = paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerMaxSize.GetAsFloat()
		minDeltaCount = paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerDeltalogMinNum.GetAsInt()
		maxDeltaCount = paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerDeltalogMaxNum.GetAsInt()
	)

	pickedSize := float64(0)
	pickedCount := 0

	// count >= minDeltaCount
	if lo.SumBy(segments, func(view *SegmentView) int { return view.DeltalogCount }) >= minDeltaCount {
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
func (v *LevelZeroCompactionView) forceTrigger(segments []*SegmentView) (picked []*SegmentView, reason string) {
	var (
		maxDeltaSize  = paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerMaxSize.GetAsFloat()
		maxDeltaCount = paramtable.Get().DataCoordCfg.LevelZeroCompactionTriggerDeltalogMaxNum.GetAsInt()
	)

	picked, pickedSize, pickedCount := pickByMaxCountSize(segments, maxDeltaSize, maxDeltaCount)
	reason = fmt.Sprintf("level zero views force to trigger, pickedSize=%.2fB, pickedCount=%d", pickedSize, pickedCount)
	return picked, reason
}

// pickByMaxCountSize picks segments that count <= maxCount or size <= maxSize
func pickByMaxCountSize(segments []*SegmentView, maxSize float64, maxCount int) (picked []*SegmentView, pickedSize float64, pickedCount int) {
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
