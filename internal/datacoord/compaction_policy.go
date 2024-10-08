package datacoord

import (
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type CompactionPolicy interface {
	Enabled() bool
	Trigger(input CompactionView) (ouput CompactionView, reason string)
	ForceTrigger(input CompactionView) (ouput CompactionView, reason string)
}

type DeltaTooMuchCompactionPolicy struct{}

func (policy *DeltaTooMuchCompactionPolicy) Enabled() bool {
	return Params.DataCoordCfg.EnableAutoCompaction.GetAsBool()
}

func (policy *DeltaTooMuchCompactionPolicy) Trigger(input CompactionView) (CompactionView, string) {
	segView, ok := input.(*MixSegmentView)
	if !ok {
		log.Warn("Failed to trigger compaction because input is not MixSegmentView")
		return nil, ""
	}

	if len(segView.GetSegmentsView()) > 1 || len(segView.GetSegmentsView()) == 0 {
		log.Warn("Failed to trigger DeltaTooMuchPolicy for unsupported too many/0 input segments")
		return nil, ""
	}

	singleSeg := segView.GetSegmentsView()[0]
	if picked, reason := isSegmentDeltaTooMuch(singleSeg); picked {
		return input, reason
	}

	return nil, ""
}

func (policy *DeltaTooMuchCompactionPolicy) ForceTrigger(input CompactionView) (CompactionView, string) {
	return nil, ""
}

func isSegmentDeltaTooMuch(view *SegmentView) (bool, string) {
	if deltaLogCountReachLimit(view.DeltalogCount) {
		return true, "daltalog count reaches limit"
	}

	if deltaRowCountRatioReachLimit(view.DeltaRowCount, view.NumOfRows) {
		return true, "delta rowcount ratio reaches limit"
	}

	if deltaLogSizeReachLimit(int64(view.DeltaSize)) {
		return true, "deltalog size reaches limit"
	}

	return false, ""
}

func deltaLogCountReachLimit(deltalogCount int64) bool {
	return deltalogCount > paramtable.Get().DataCoordCfg.SingleCompactionDeltalogMaxNum.GetAsInt64()
}

func deltaRowCountRatioReachLimit(deltaRowCount int64, numRows int64) bool {
	return float64(deltaRowCount)/float64(numRows) >= paramtable.Get().DataCoordCfg.SingleCompactionRatioThreshold.GetAsFloat()
}

func deltaLogSizeReachLimit(deltaSize int64) bool {
	return deltaSize > paramtable.Get().DataCoordCfg.SingleCompactionDeltaLogMaxSize.GetAsInt64()
}
