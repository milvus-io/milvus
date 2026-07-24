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
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func makeDeltaMetricSegment(level datapb.SegmentLevel, state commonpb.SegmentState, numRows int64, deltalogCount int, rowsPerLog int64, sizePerLog int64) *SegmentInfo {
	binlogs := make([]*datapb.Binlog, 0, deltalogCount)
	for i := 0; i < deltalogCount; i++ {
		binlogs = append(binlogs, &datapb.Binlog{EntriesNum: rowsPerLog, MemorySize: sizePerLog})
	}
	return &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			Level:     level,
			State:     state,
			NumOfRows: numRows,
			Deltalogs: []*datapb.FieldBinlog{{Binlogs: binlogs}},
		},
	}
}

func TestDeltalogQuantile(t *testing.T) {
	assert.Equal(t, []float64{0, 0}, quantilesInPlace(nil, []float64{0.5, 1.0}))

	values := []float64{40, 10, 30, 20} // sorted: 10 20 30 40
	got := quantilesInPlace(values, []float64{0.5, 0.99, 1.0})
	assert.Equal(t, []float64{20, 40, 40}, got)

	// nearest-rank: rank = ceil(0.9*9) = 9 -> the 9th value
	nine := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9}
	assert.Equal(t, []float64{9}, quantilesInPlace(nine, []float64{0.9}))

	single := []float64{7}
	assert.Equal(t, []float64{7, 7}, quantilesInPlace(single, []float64{0.5, 1.0}))
}

func TestDeltalogAggregateObserve(t *testing.T) {
	agg := &deltalogAggregate{}

	// L0 segments only feed the L0 file counter
	agg.observe(makeDeltaMetricSegment(datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 0, 7, 100, 1024))
	assert.Equal(t, int64(7), agg.l0FileCount)
	assert.Empty(t, agg.byLevel)

	// non-flushed segments are outside the trigger's candidate population
	agg.observe(makeDeltaMetricSegment(datapb.SegmentLevel_L1, commonpb.SegmentState_Growing, 1000, 3, 10, 512))
	agg.observe(makeDeltaMetricSegment(datapb.SegmentLevel_L1, commonpb.SegmentState_Sealed, 1000, 3, 10, 512))
	assert.Empty(t, agg.byLevel)

	// flushed non-L0 segments feed the distributions, kept per level
	agg.observe(makeDeltaMetricSegment(datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed, 1000, 30, 10, 2048)) // ratio 0.3
	agg.observe(makeDeltaMetricSegment(datapb.SegmentLevel_L2, commonpb.SegmentState_Flushed, 1000, 150, 1, 4096)) // ratio 0.15
	agg.observe(makeDeltaMetricSegment(datapb.SegmentLevel_L2, commonpb.SegmentState_Flushed, 0, 900, 1, 512))     // zero rows: ratio skipped

	l1 := agg.byLevel[datapb.SegmentLevel_L1]
	l2 := agg.byLevel[datapb.SegmentLevel_L2]
	assert.Equal(t, []float64{30}, l1.fileCounts)
	assert.Equal(t, []float64{30 * 2048}, l1.sizes)
	assert.Len(t, l1.deletedRatios, 1)
	assert.InDelta(t, 0.3, l1.deletedRatios[0], 1e-9)
	assert.Equal(t, []float64{150, 900}, l2.fileCounts)
	assert.Equal(t, []float64{150 * 4096, 900 * 512}, l2.sizes)
	assert.Len(t, l2.deletedRatios, 1) // zero-row L2 segment excluded
	assert.InDelta(t, 0.15, l2.deletedRatios[0], 1e-9)
}

func TestReportDeltalogMetrics(t *testing.T) {
	agg := &deltalogAggregate{dbName: "db1"}
	agg.observe(makeDeltaMetricSegment(datapb.SegmentLevel_L0, commonpb.SegmentState_Flushed, 0, 5, 100, 1024))
	// L1 (manual-compaction candidates): a cohort at ~180 files
	agg.observe(makeDeltaMetricSegment(datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed, 1000, 180, 1, 1024))
	agg.observe(makeDeltaMetricSegment(datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed, 1000, 181, 1, 1024))
	// L2 (auto-compaction candidates): a cohort plus one lone dirty segment
	agg.observe(makeDeltaMetricSegment(datapb.SegmentLevel_L2, commonpb.SegmentState_Flushed, 1000, 182, 1, 1024))
	agg.observe(makeDeltaMetricSegment(datapb.SegmentLevel_L2, commonpb.SegmentState_Flushed, 1000, 600, 1, 1024))

	reportDeltalogMetrics(map[string]*deltalogAggregate{"100": agg})

	assert.Equal(t, 5.0, testutil.ToFloat64(metrics.DataCoordL0DeltalogFileNum.WithLabelValues("db1", "100")))
	// L1 and L2 stay separate quantiles (not blended): p50 = nearest-rank of each level's 2 samples
	assert.Equal(t, 180.0, testutil.ToFloat64(metrics.DataCoordSegmentDeltalogFileCount.WithLabelValues("db1", "100", "L1", "0.5")))
	assert.Equal(t, 181.0, testutil.ToFloat64(metrics.DataCoordSegmentDeltalogFileCount.WithLabelValues("db1", "100", "L1", "1.0")))
	assert.Equal(t, 182.0, testutil.ToFloat64(metrics.DataCoordSegmentDeltalogFileCount.WithLabelValues("db1", "100", "L2", "0.5")))
	assert.Equal(t, 600.0, testutil.ToFloat64(metrics.DataCoordSegmentDeltalogFileCount.WithLabelValues("db1", "100", "L2", "1.0")))
	assert.Equal(t, float64(180*1024), testutil.ToFloat64(metrics.DataCoordSegmentDeltalogSize.WithLabelValues("db1", "100", "L1", "0.5")))
	assert.InDelta(t, 0.180, testutil.ToFloat64(metrics.DataCoordSegmentDeletedRowsRatio.WithLabelValues("db1", "100", "L1", "0.5")), 1e-9)

	// a refresh round without the collection prunes its series
	reportDeltalogMetrics(map[string]*deltalogAggregate{})
	assert.Equal(t, 0, testutil.CollectAndCount(metrics.DataCoordSegmentDeltalogFileCount))
	assert.Equal(t, 0, testutil.CollectAndCount(metrics.DataCoordSegmentDeltalogSize))
	assert.Equal(t, 0, testutil.CollectAndCount(metrics.DataCoordSegmentDeletedRowsRatio))
	assert.Equal(t, 0, testutil.CollectAndCount(metrics.DataCoordL0DeltalogFileNum))
}
