package segmentutil

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
)

// ReCalcRowCount re-calculates number of rows of `oldSeg` based on its bin log count, and correct its value in its
// cloned copy, which is `newSeg`.
// Note that `segCloned` should be a copied version of `seg`.
func ReCalcRowCount(seg, segCloned *datapb.SegmentInfo) {
	// `segment` is not mutated but only cloned above and is safe to be referred here.
	if newCount := CalcRowCountFromBinLog(seg); newCount != seg.GetNumOfRows() && newCount > 0 {
		log.Warn("segment row number meta inconsistent with bin log row count and will be corrected",
			zap.Int64("segmentID", seg.GetID()),
			zap.Int64("segment meta row count (wrong)", seg.GetNumOfRows()),
			zap.Int64("segment bin log row count (correct)", newCount))
		// Update the corrected row count.
		segCloned.NumOfRows = newCount
	}
}

// CalcRowCountFromBinLog calculates # of rows of a segment from bin logs
func CalcRowCountFromBinLog(seg *datapb.SegmentInfo) int64 {
	var rowCt int64
	if len(seg.GetBinlogs()) > 0 {
		for _, ct := range seg.GetBinlogs()[0].GetBinlogs() {
			rowCt += ct.GetEntriesNum()
			// This segment contains stale log with incorrect entries num,
			if ct.GetEntriesNum() <= 0 {
				return -1
			}
		}
	}
	return rowCt
}

// MergeRequestCost merge the costs of request, the cost may came from different worker in same channel
// or different channel in same collection, for now we just choose the part with the highest response time
func MergeRequestCost(requestCosts []*internalpb.CostAggregation) *internalpb.CostAggregation {
	var result *internalpb.CostAggregation
	for _, cost := range requestCosts {
		if result == nil || result.ResponseTime < cost.ResponseTime {
			result = cost
		}
	}

	return result
}
