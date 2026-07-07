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

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func makeDeltaMetricSegment(level datapb.SegmentLevel, numRows int64, deltalogCount int, rowsPerLog int64) *SegmentInfo {
	binlogs := make([]*datapb.Binlog, 0, deltalogCount)
	for i := 0; i < deltalogCount; i++ {
		binlogs = append(binlogs, &datapb.Binlog{EntriesNum: rowsPerLog, MemorySize: 1024})
	}
	return &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			Level:     level,
			NumOfRows: numRows,
			Deltalogs: []*datapb.FieldBinlog{{Binlogs: binlogs}},
		},
	}
}

func TestDeltalogQuantile(t *testing.T) {
	assert.Equal(t, 0.0, quantile(nil, 0.5))

	values := []float64{40, 10, 30, 20} // sorted: 10 20 30 40
	assert.Equal(t, 20.0, quantile(values, 0.5))
	assert.Equal(t, 40.0, quantile(values, 0.99))
	assert.Equal(t, 40.0, quantile(values, 1.0))

	single := []float64{7}
	assert.Equal(t, 7.0, quantile(single, 0.5))
	assert.Equal(t, 7.0, quantile(single, 1.0))
}

func TestDeltalogAggregateObserve(t *testing.T) {
	agg := &deltalogAggregate{}

	// L0 segments only feed the L0 file counter
	agg.observe(makeDeltaMetricSegment(datapb.SegmentLevel_L0, 0, 7, 100))
	assert.Equal(t, int64(7), agg.l0FileCount)
	assert.Empty(t, agg.fileCounts)

	// non-L0 segments feed the distributions
	agg.observe(makeDeltaMetricSegment(datapb.SegmentLevel_L1, 1000, 30, 10)) // 300 deleted -> ratio 0.3
	agg.observe(makeDeltaMetricSegment(datapb.SegmentLevel_L2, 1000, 150, 1)) // 150 deleted -> ratio 0.15
	agg.observe(makeDeltaMetricSegment(datapb.SegmentLevel_L2, 0, 900, 1))    // zero rows: ratio skipped

	assert.Equal(t, []float64{30, 150, 900}, agg.fileCounts)
	assert.Len(t, agg.deletedRatios, 2)
	assert.InDelta(t, 0.3, agg.deletedRatios[0], 1e-9)
	assert.InDelta(t, 0.15, agg.deletedRatios[1], 1e-9)
}

func TestReportDeltalogMetrics(t *testing.T) {
	agg := &deltalogAggregate{}
	agg.observe(makeDeltaMetricSegment(datapb.SegmentLevel_L0, 0, 5, 100))
	// a synchronized cohort at ~180 files plus one lone dirty segment
	agg.observe(makeDeltaMetricSegment(datapb.SegmentLevel_L1, 1000, 180, 1))
	agg.observe(makeDeltaMetricSegment(datapb.SegmentLevel_L1, 1000, 181, 1))
	agg.observe(makeDeltaMetricSegment(datapb.SegmentLevel_L2, 1000, 182, 1))
	agg.observe(makeDeltaMetricSegment(datapb.SegmentLevel_L2, 1000, 600, 1))

	reportDeltalogMetrics(map[string]*deltalogAggregate{"100": agg})

	assert.Equal(t, 5.0, testutil.ToFloat64(metrics.DataCoordL0DeltalogFileNum.WithLabelValues("100")))
	// the cohort shows up in the median while max sees only the outlier
	assert.Equal(t, 181.0, testutil.ToFloat64(metrics.DataCoordSegmentDeltalogFileCount.WithLabelValues("100", "0.5")))
	assert.Equal(t, 600.0, testutil.ToFloat64(metrics.DataCoordSegmentDeltalogFileCount.WithLabelValues("100", "1.0")))
	assert.InDelta(t, 0.181, testutil.ToFloat64(metrics.DataCoordSegmentDeletedRowsRatio.WithLabelValues("100", "0.5")), 1e-9)

	// a refresh round without the collection removes its series
	reportDeltalogMetrics(map[string]*deltalogAggregate{})
	assert.Equal(t, 0, testutil.CollectAndCount(metrics.DataCoordSegmentDeltalogFileCount))
	assert.Equal(t, 0, testutil.CollectAndCount(metrics.DataCoordSegmentDeletedRowsRatio))
	assert.Equal(t, 0, testutil.CollectAndCount(metrics.DataCoordL0DeltalogFileNum))
}
