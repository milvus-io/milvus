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

package meta

import (
	"math"
	"sort"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
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
	// Read the same source hasTooManyDeletions reads: EnsureStats() is the
	// persisted Statistics (an O(1) read when populated) and only falls back
	// to rebuilding from the log metadata when Stats is nil. The in-memory
	// Deltalogs array is not a valid source: for StorageV3 segments the
	// catalog does not persist the per-FieldBinlog KVs, so after a DataCoord
	// restart the array is empty while the cumulative values live in Stats.
	stats := segment.EnsureStats()
	if segment.GetLevel() == datapb.SegmentLevel_L0 {
		// L0 contributes only its deltalog file count (l0_delete_entries_num
		// companion).
		a.l0FileCount += stats.GetDeltaBinlogCount()
		return
	}
	if !IsFlushed(segment) {
		return
	}

	fileCount := stats.GetDeltaBinlogCount()
	size := stats.GetDeltaBinlogSize()
	deletedRows := stats.GetDeleteNumRows()

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
	// (collection, segment level) pairs whose per-level quantile series are
	// currently published; lets a refresh round prune a level that stopped
	// contributing while its collection survived (e.g. clustering compaction
	// consumed every L1 segment), as well as collections that disappeared
	publishedDeltalogLevels = make(map[string]map[string]struct{})
)

// reportDeltalogMetrics publishes the per-collection deltalog gauges, then
// prunes the series absent from this round: a whole collection (also eagerly
// cleaned in CleanupDataCoordWithCollectionID on DropCollection), or one
// segment level of a surviving collection, whose 12 quantile series would
// otherwise freeze at their last published values. Publish-then-prune instead
// of Reset-then-Set so a concurrent scrape never observes an empty window; the
// mutex serializes concurrent GetQuotaInfo callers.
func reportDeltalogMetrics(aggregates map[string]*deltalogAggregate) {
	deltalogPublishMu.Lock()
	defer deltalogPublishMu.Unlock()

	for collectionID, agg := range aggregates {
		publishCollectionDeltalogMetrics(agg.dbName, collectionID, agg.l0FileCount, agg.quantiles())
	}
	for collectionID := range publishedDeltalogLevels {
		if _, present := aggregates[collectionID]; !present {
			removeCollectionDeltalogMetrics(collectionID)
		}
	}
}

// The caller holds deltalogPublishMu.
func publishCollectionDeltalogMetrics(dbName, collectionID string, l0FileCount int64, quantiles map[string]metricsinfo.QuotaDeltalogQuantiles) {
	metrics.DataCoordL0DeltalogFileNum.WithLabelValues(dbName, collectionID).Set(float64(l0FileCount))
	levels := make(map[string]struct{}, len(quantiles))
	for level, values := range quantiles {
		levels[level] = struct{}{}
		publishDeltalogQuantiles(dbName, collectionID, level, values)
	}
	for level := range publishedDeltalogLevels[collectionID] {
		if _, present := levels[level]; !present {
			metrics.CleanupDataCoordDeltalogLevelMetrics(collectionID, level)
		}
	}
	publishedDeltalogLevels[collectionID] = levels
}

func removeCollectionDeltalogMetrics(collectionID string) {
	metrics.CleanupDataCoordDeltalogMetrics(collectionID)
	delete(publishedDeltalogLevels, collectionID)
}

func (a *deltalogAggregate) quantiles() map[string]metricsinfo.QuotaDeltalogQuantiles {
	if len(a.byLevel) == 0 {
		return nil
	}
	qs := make([]float64, len(deltalogQuantiles))
	for i, p := range deltalogQuantiles {
		qs[i] = p.q
	}
	result := make(map[string]metricsinfo.QuotaDeltalogQuantiles, len(a.byLevel))
	for level, d := range a.byLevel {
		result[level.String()] = metricsinfo.QuotaDeltalogQuantiles{
			FileCount:    quantilesInPlace(d.fileCounts, qs),
			Size:         quantilesInPlace(d.sizes, qs),
			DeletedRatio: quantilesInPlace(d.deletedRatios, qs),
		}
	}
	return result
}

func publishDeltalogQuantiles(dbName, collectionID, level string, values metricsinfo.QuotaDeltalogQuantiles) {
	for i, p := range deltalogQuantiles {
		metrics.DataCoordSegmentDeltalogFileCount.WithLabelValues(dbName, collectionID, level, p.label).Set(values.FileCount[i])
		metrics.DataCoordSegmentDeltalogSize.WithLabelValues(dbName, collectionID, level, p.label).Set(values.Size[i])
		metrics.DataCoordSegmentDeletedRowsRatio.WithLabelValues(dbName, collectionID, level, p.label).Set(values.DeletedRatio[i])
	}
}
