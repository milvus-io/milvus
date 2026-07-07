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

// deltalogAggregate collects per-collection deltalog observability stats over
// healthy FLUSHED non-L0 segments — the population hasTooManyDeletions is
// evaluated against (growing/sealed segments route deletes to L0 and would
// pad the distributions with zeros). Three of its accumulation dimensions are
// mirrored: deltalog file count, accumulated deltalog size, and deleted-rows
// ratio; zero-row segments are excluded from the ratio distribution (the
// trigger's own uncapped division would fire on them, but a +Inf sample would
// poison the gauge). Deltalog files of L0 segments are counted separately
// over all healthy L0 segments, matching l0_delete_entries_num.
type deltalogAggregate struct {
	dbName        string
	fileCounts    []float64
	sizes         []float64
	deletedRatios []float64
	l0FileCount   int64
}

func (a *deltalogAggregate) observe(segment *SegmentInfo) {
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

	if segment.GetLevel() == datapb.SegmentLevel_L0 {
		a.l0FileCount += int64(fileCount)
		return
	}
	if !isFlushed(segment) {
		return
	}

	a.fileCounts = append(a.fileCounts, float64(fileCount))
	a.sizes = append(a.sizes, float64(size))
	if segment.GetNumOfRows() > 0 {
		a.deletedRatios = append(a.deletedRatios, float64(deletedRows)/float64(segment.GetNumOfRows()))
	}
}

// quantile returns the exact nearest-rank quantile (rank = ceil(q*n)) of
// values, sorting in place; 0 for an empty slice.
func quantile(values []float64, q float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	idx := int(math.Ceil(q*float64(len(values)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
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

	for collectionID, agg := range aggregates {
		metrics.DataCoordL0DeltalogFileNum.WithLabelValues(agg.dbName, collectionID).Set(float64(agg.l0FileCount))
		for _, p := range deltalogQuantiles {
			metrics.DataCoordSegmentDeltalogFileCount.WithLabelValues(agg.dbName, collectionID, p.label).Set(quantile(agg.fileCounts, p.q))
			metrics.DataCoordSegmentDeltalogSize.WithLabelValues(agg.dbName, collectionID, p.label).Set(quantile(agg.sizes, p.q))
			metrics.DataCoordSegmentDeletedRowsRatio.WithLabelValues(agg.dbName, collectionID, p.label).Set(quantile(agg.deletedRatios, p.q))
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
