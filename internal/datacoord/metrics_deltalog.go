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
	"math"
	"sort"
	"sync"

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

// levelDist holds one segment level's mirrored deltalog distributions:
// deltalog file count, accumulated deltalog size, and deleted-rows ratio.
type levelDist struct {
	fileCounts    []float64
	sizes         []float64
	deletedRatios []float64
}

// deltalogAggregate collects per-collection deltalog observability stats over
// healthy FLUSHED non-L0 segments — the population hasTooManyDeletions is
// approximately evaluated against (growing/sealed segments route deletes to L0
// and would pad the distributions with zeros). It only gates on L0 + isFlushed
// (the caller already guarantees healthy && !importing); the finer candidacy
// predicates (isCompacting, IsSorted, IsInvisible, compaction-protected) are
// not applied, so the distributions approximate rather than exactly equal the
// candidate set. Distributions are kept per segment level (byLevel): the two
// single-compaction paths that reach hasTooManyDeletions each cover one level
// (auto-compaction filters to L2, manual to L1), so blending them into one
// quantile would dilute p50/p90. Zero-row segments are excluded from the ratio
// distribution (the trigger's own uncapped division would fire on them, but a
// +Inf sample would poison the gauge). Deltalog files of L0 segments are
// counted separately over all healthy L0 segments, matching
// l0_delete_entries_num.
type deltalogAggregate struct {
	dbName      string
	byLevel     map[datapb.SegmentLevel]*levelDist
	l0FileCount int64
}

func (a *deltalogAggregate) observe(segment *SegmentInfo) {
	// L0 uses only the deltalog file count (l0_delete_entries_num companion);
	// skip the size/deleted-row accumulation the non-L0 path needs.
	if segment.GetLevel() == datapb.SegmentLevel_L0 {
		fileCount := 0
		for _, deltaLogs := range segment.GetDeltalogs() {
			fileCount += len(deltaLogs.GetBinlogs())
		}
		a.l0FileCount += int64(fileCount)
		return
	}
	if !isFlushed(segment) {
		return
	}

	fileCount := 0
	size := int64(0)
	deletedRows := int64(0)
	for _, deltaLogs := range segment.GetDeltalogs() {
		for _, l := range deltaLogs.GetBinlogs() {
			deletedRows += l.GetEntriesNum()
			size += l.GetMemorySize()
		}
		fileCount += len(deltaLogs.GetBinlogs())
	}

	if a.byLevel == nil {
		a.byLevel = make(map[datapb.SegmentLevel]*levelDist)
	}
	d := a.byLevel[segment.GetLevel()]
	if d == nil {
		d = &levelDist{}
		a.byLevel[segment.GetLevel()] = d
	}
	d.fileCounts = append(d.fileCounts, float64(fileCount))
	d.sizes = append(d.sizes, float64(size))
	if segment.GetNumOfRows() > 0 {
		d.deletedRatios = append(d.deletedRatios, float64(deletedRows)/float64(segment.GetNumOfRows()))
	}
}

// quantilesInPlace sorts values once and returns the exact nearest-rank
// (rank = ceil(q*n)) value for each requested quantile; all-zeros for an empty
// slice. Sorting once (vs once per quantile) shortens the read-lock hold in
// reportDeltalogMetrics.
func quantilesInPlace(values []float64, qs []float64) []float64 {
	out := make([]float64, len(qs))
	if len(values) == 0 {
		return out
	}
	sort.Float64s(values)
	for i, q := range qs {
		idx := int(math.Ceil(q*float64(len(values)))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(values) {
			idx = len(values) - 1
		}
		out[i] = values[idx]
	}
	return out
}

var (
	deltalogPublishMu sync.Mutex
	// collections whose deltalog series are currently published; lets a
	// refresh round prune collections that disappeared since the last one
	publishedDeltalogCollections = make(map[string]struct{})
)

// reportDeltalogMetrics publishes the per-collection deltalog gauges, then
// prunes series of collections absent from this round (dropped collections are
// also eagerly cleaned in CleanupDataCoordWithCollectionID on DropCollection).
// Publish-then-prune instead of Reset-then-Set so a concurrent scrape never
// observes an empty window; the mutex serializes concurrent GetQuotaInfo
// callers.
func reportDeltalogMetrics(aggregates map[string]*deltalogAggregate) {
	deltalogPublishMu.Lock()
	defer deltalogPublishMu.Unlock()

	qs := make([]float64, len(deltalogQuantiles))
	for i, p := range deltalogQuantiles {
		qs[i] = p.q
	}

	for collectionID, agg := range aggregates {
		metrics.DataCoordL0DeltalogFileNum.WithLabelValues(agg.dbName, collectionID).Set(float64(agg.l0FileCount))
		for level, d := range agg.byLevel {
			levelStr := level.String()
			fcQ := quantilesInPlace(d.fileCounts, qs)
			szQ := quantilesInPlace(d.sizes, qs)
			drQ := quantilesInPlace(d.deletedRatios, qs)
			for i, p := range deltalogQuantiles {
				metrics.DataCoordSegmentDeltalogFileCount.WithLabelValues(agg.dbName, collectionID, levelStr, p.label).Set(fcQ[i])
				metrics.DataCoordSegmentDeltalogSize.WithLabelValues(agg.dbName, collectionID, levelStr, p.label).Set(szQ[i])
				metrics.DataCoordSegmentDeletedRowsRatio.WithLabelValues(agg.dbName, collectionID, levelStr, p.label).Set(drQ[i])
			}
		}
	}
	for collectionID := range publishedDeltalogCollections {
		if _, ok := aggregates[collectionID]; !ok {
			metrics.CleanupDataCoordDeltalogMetrics(collectionID)
			delete(publishedDeltalogCollections, collectionID)
		}
	}
	for collectionID := range aggregates {
		publishedDeltalogCollections[collectionID] = struct{}{}
	}
}
