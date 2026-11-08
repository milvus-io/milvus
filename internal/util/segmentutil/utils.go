package segmentutil

import (
	"context"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// ReCalcRowCount re-calculates number of rows of `oldSeg` based on its bin log count, and correct its value in its
// cloned copy, which is `newSeg`.
// Note that `segCloned` should be a copied version of `seg`.
func ReCalcRowCount(seg, segCloned *datapb.SegmentInfo) {
	// V3 segments resolve row counts via SegmentInfo.NumOfRows (advanced from
	// the writer checkpoint) and Statistics; their binlog arrays are a
	// pass-through cache that may be empty (recovery) or delta-only (growing
	// source flush), so array-derived counts are never authoritative for them.
	if seg.GetStorageVersion() == storage.StorageV3 {
		return
	}
	// `segment` is not mutated but only cloned above and is safe to be referred here.
	if newCount := CalcRowCountFromBinLog(seg); newCount != seg.GetNumOfRows() && newCount > 0 {
		mlog.Warn(context.TODO(), "segment row number meta inconsistent with bin log row count and will be corrected",
			mlog.FieldSegmentID(seg.GetID()),
			mlog.Int64("segment meta row count (wrong)", seg.GetNumOfRows()),
			mlog.Int64("segment bin log row count (correct)", newCount))
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

// CalcDelRowCountFromDeltaLog calculates deleted rows of a L0 segment from delta logs
func CalcDelRowCountFromDeltaLog(seg *datapb.SegmentInfo) int64 {
	var rowCt int64
	if len(seg.GetDeltalogs()) > 0 {
		for _, dls := range seg.GetDeltalogs() {
			for _, dl := range dls.GetBinlogs() {
				rowCt += dl.GetEntriesNum()
			}
		}
	}
	return rowCt
}

// MergeRequestCost merges the costs of request; the cost may come from different worker in same channel
// or different channel in same collection, for now we just choose the part with the highest response time
func MergeRequestCost(requestCosts []*internalpb.CostAggregation) *internalpb.CostAggregation {
	var result *internalpb.CostAggregation
	for _, cost := range requestCosts {
		if cost == nil {
			continue
		}
		if result == nil || result.ResponseTime < cost.ResponseTime {
			result = cost
		}
	}

	return result
}

// AggregateWorkerCosts is the cost a delegator reports for its channel from
// its workers' replies, each read with cost (the worker it came from and its
// cost): MergeRequestCost of every worker's cost when allWorkers, else of
// localNodeID's only, with TotalRelatedDataSize summed over every worker. It
// never returns nil.
func AggregateWorkerCosts[R any](replies []R, cost func(R) (int64, *internalpb.CostAggregation), localNodeID int64, allWorkers bool) *internalpb.CostAggregation {
	costs := make([]*internalpb.CostAggregation, 0, len(replies))
	var relatedDataSize int64
	for _, r := range replies {
		source, c := cost(r)
		relatedDataSize += c.GetTotalRelatedDataSize()
		if allWorkers || source == localNodeID {
			costs = append(costs, c)
		}
	}
	out := MergeRequestCost(costs)
	if out == nil {
		out = &internalpb.CostAggregation{}
	}
	out.TotalRelatedDataSize = relatedDataSize
	return out
}
