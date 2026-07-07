// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"sort"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// deltalogQuantiles are the summary quantiles published for the per-segment
// deltalog distributions. The 1.0 quantile (max) tracks the dirtiest segment
// against the single-compaction thresholds; the lower quantiles rising toward
// a threshold reveal a same-batch cohort accumulating toward a synchronized
// compaction wave, which the max alone cannot distinguish from a lone dirty
// segment.
var deltalogQuantiles = []struct {
	label string
	q     float64
}{
	{"0.5", 0.5},
	{"0.9", 0.9},
	{"0.99", 0.99},
	{"1.0", 1.0},
}

// deltalogAggregate collects per-collection deltalog observability stats,
// mirroring the accumulation dimensions checked by hasTooManyDeletions.
type deltalogAggregate struct {
	fileCounts    []float64 // per non-L0 segment deltalog file count
	deletedRatios []float64 // per non-L0 segment deleted-rows/total-rows
	l0FileCount   int64
}

func (a *deltalogAggregate) observe(segment *SegmentInfo) {
	fileCount := 0
	deletedRows := int64(0)
	for _, deltaLogs := range segment.GetDeltalogs() {
		for _, l := range deltaLogs.GetBinlogs() {
			deletedRows += l.GetEntriesNum()
		}
		fileCount += len(deltaLogs.GetBinlogs())
	}

	if segment.GetLevel() == datapb.SegmentLevel_L0 {
		// L0 segments hold the un-applied delete stream; their deltalogs never
		// count toward the single-compaction trigger, so track them separately.
		a.l0FileCount += int64(fileCount)
		return
	}

	a.fileCounts = append(a.fileCounts, float64(fileCount))
	if segment.GetNumOfRows() > 0 {
		a.deletedRatios = append(a.deletedRatios, float64(deletedRows)/float64(segment.GetNumOfRows()))
	}
}

// quantile returns the exact nearest-rank quantile of values, sorting in
// place; 0 for an empty slice.
func quantile(values []float64, q float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	idx := int(q*float64(len(values))+0.5) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

// reportDeltalogMetrics resets and republishes the per-collection deltalog
// gauges; the reset removes series of dropped collections (they are also
// eagerly deleted in CleanupDataCoordWithCollectionID on DropCollection).
func reportDeltalogMetrics(aggregates map[string]*deltalogAggregate) {
	metrics.DataCoordSegmentDeltalogFileCount.Reset()
	metrics.DataCoordSegmentDeletedRowsRatio.Reset()
	metrics.DataCoordL0DeltalogFileNum.Reset()
	for collectionID, agg := range aggregates {
		metrics.DataCoordL0DeltalogFileNum.WithLabelValues(collectionID).Set(float64(agg.l0FileCount))
		for _, p := range deltalogQuantiles {
			metrics.DataCoordSegmentDeltalogFileCount.WithLabelValues(collectionID, p.label).Set(quantile(agg.fileCounts, p.q))
			metrics.DataCoordSegmentDeletedRowsRatio.WithLabelValues(collectionID, p.label).Set(quantile(agg.deletedRatios, p.q))
		}
	}
}
