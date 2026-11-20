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
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
)

func resetQuotaDisplayMetrics() {
	metrics.DataCoordStoredBinlogSize.Reset()
	metrics.DataCoordSegmentBinLogFileCount.Reset()
	metrics.DataCoordNumStoredRows.Reset()
	metrics.DataCoordL0DeleteEntriesNum.Reset()
}

func TestQuotaInfoAggregatePreservesControlData(t *testing.T) {
	previous := metrics.CollectionLevelMetricsMode()
	t.Cleanup(func() {
		metrics.SetCollectionLevelMetricsMode(previous)
		resetQuotaDisplayMetrics()
	})
	store := metacache.NewMetaStore(nil)
	m := &Meta{
		MetaStore: store,
		Segments:  NewSegmentsInfo(store),
	}
	for id, db := range map[int64]string{1: "db-a", 2: "db-a", 3: "db-b"} {
		m.MetaStore.PutCollection(&CollectionInfo{
			ID: id, DatabaseName: db,
			Schema: &schemapb.CollectionSchema{Name: strconv.FormatInt(id, 10)},
		})
	}
	for _, fixture := range []struct {
		id, collection, partition, rows, size, files, deletes int64
		state                                                 commonpb.SegmentState
		level                                                 datapb.SegmentLevel
		importing                                             bool
	}{
		{10, 1, 11, 10, 100, 1, 3, commonpb.SegmentState_Growing, datapb.SegmentLevel_L0, false},
		{11, 1, 12, 5, 50, 2, 0, commonpb.SegmentState_Flushed, datapb.SegmentLevel_L1, false},
		{20, 2, 21, 20, 200, 2, 4, commonpb.SegmentState_Growing, datapb.SegmentLevel_L0, false},
		{30, 3, 31, 7, 70, 1, 0, commonpb.SegmentState_Growing, datapb.SegmentLevel_L1, false},
		// Quota must still count healthy segments whose collection is not cached.
		{40, 999, 41, 9, 90, 1, 5, commonpb.SegmentState_Growing, datapb.SegmentLevel_L0, false},
		{50, 1, 11, 100, 1000, 10, 0, commonpb.SegmentState_Growing, datapb.SegmentLevel_L1, true},
		{60, 1, 11, 200, 2000, 20, 0, commonpb.SegmentState_Dropped, datapb.SegmentLevel_L1, false},
	} {
		segment := BuildSegment(fixture.collection, fixture.partition, fixture.id, "channel")
		segment.NumOfRows = fixture.rows
		segment.State = fixture.state
		segment.Level = fixture.level
		segment.IsImporting = fixture.importing
		segment.Stats = &datapb.Statistics{
			InsertBinlogSize:  fixture.size,
			InsertBinlogCount: fixture.files, DeleteNumRows: fixture.deletes,
		}
		m.Segments.SetSegment(fixture.id, segment)
	}
	expected := &metricsinfo.DataCoordQuotaMetrics{
		TotalBinlogSize:      510,
		CollectionBinlogSize: map[int64]int64{1: 150, 2: 200, 3: 70, 999: 90},
		PartitionsBinlogSize: map[int64]map[int64]int64{
			1: {11: 100, 12: 50}, 2: {21: 200}, 3: {31: 70}, 999: {41: 90},
		},
		CollectionL0RowCount: map[int64]int64{1: 3, 2: 4, 999: 5},
	}
	// Repeat mode changes: stale collection-level series must not leak into all.
	for _, mode := range []string{
		metrics.CollectionLevelMetricsModeFull,
		metrics.CollectionLevelMetricsModeAggregate, metrics.CollectionLevelMetricsModeFull,
		metrics.CollectionLevelMetricsModeAggregate,
	} {
		metrics.SetCollectionLevelMetricsMode(mode)
		require.Equal(t, expected, m.GetQuotaInfo())
		if mode == metrics.CollectionLevelMetricsModeAggregate {
			require.Equal(t, 3, testutil.CollectAndCount(metrics.DataCoordStoredBinlogSize))
			require.Equal(t, 1, testutil.CollectAndCount(metrics.DataCoordSegmentBinLogFileCount))
			require.Equal(t, 3, testutil.CollectAndCount(metrics.DataCoordNumStoredRows))
			require.Equal(t, 1, testutil.CollectAndCount(metrics.DataCoordL0DeleteEntriesNum))
			require.Equal(t, float64(300), testutil.ToFloat64(metrics.DataCoordStoredBinlogSize.WithLabelValues(
				"db-a", metrics.AllLabel, commonpb.SegmentState_Growing.String())))
			require.Equal(t, float64(6), testutil.ToFloat64(metrics.DataCoordSegmentBinLogFileCount.WithLabelValues(metrics.AllLabel)))
			require.Equal(t, float64(30), testutil.ToFloat64(metrics.DataCoordNumStoredRows.WithLabelValues(
				"db-a", metrics.AllLabel, metrics.AllLabel, commonpb.SegmentState_Growing.String())))
			require.Equal(t, float64(7), testutil.ToFloat64(metrics.DataCoordL0DeleteEntriesNum.WithLabelValues("db-a", metrics.AllLabel)))
		} else {
			require.Equal(t, 4, testutil.CollectAndCount(metrics.DataCoordStoredBinlogSize))
			require.Equal(t, 3, testutil.CollectAndCount(metrics.DataCoordSegmentBinLogFileCount))
			require.Equal(t, 4, testutil.CollectAndCount(metrics.DataCoordNumStoredRows))
			require.Equal(t, 2, testutil.CollectAndCount(metrics.DataCoordL0DeleteEntriesNum))
			require.Equal(t, float64(100), testutil.ToFloat64(metrics.DataCoordStoredBinlogSize.WithLabelValues(
				"db-a", "1", commonpb.SegmentState_Growing.String())))
		}
	}

	// Metadata removal clears display series, but never changes disk admission data.
	for _, id := range []int64{1, 2, 3} {
		m.MetaStore.RemoveCollection(id)
	}
	require.Equal(t, expected, m.GetQuotaInfo())
	require.Zero(t, testutil.CollectAndCount(metrics.DataCoordStoredBinlogSize))
	require.Zero(t, testutil.CollectAndCount(metrics.DataCoordNumStoredRows))
	require.Zero(t, testutil.CollectAndCount(metrics.DataCoordL0DeleteEntriesNum))
	require.Equal(t, float64(0), testutil.ToFloat64(metrics.DataCoordSegmentBinLogFileCount.WithLabelValues(metrics.AllLabel)))
	for _, segment := range m.Segments.GetSegments() {
		m.Segments.DropSegment(segment.GetID())
	}
	empty := m.GetQuotaInfo()
	require.Zero(t, empty.TotalBinlogSize)
	require.Empty(t, empty.CollectionBinlogSize)
	require.Empty(t, empty.PartitionsBinlogSize)
	require.Empty(t, empty.CollectionL0RowCount)
}

func BenchmarkQuotaInfoAggregate(b *testing.B) {
	previous := metrics.CollectionLevelMetricsMode()
	metrics.SetCollectionLevelMetricsMode(metrics.CollectionLevelMetricsModeAggregate)
	b.Cleanup(func() {
		metrics.SetCollectionLevelMetricsMode(previous)
		resetQuotaDisplayMetrics()
	})
	for _, collections := range []int{10000, 100000} {
		store := metacache.NewMetaStore(nil)
		m := &Meta{
			MetaStore: store,
			Segments:  NewSegmentsInfo(store),
		}
		for i := range collections {
			id := int64(i + 1)
			m.MetaStore.PutCollection(&CollectionInfo{ID: id, DatabaseName: "quota-db"})
			segment := BuildSegment(id, id, id, "channel")
			segment.NumOfRows = 100
			segment.Stats = &datapb.Statistics{InsertBinlogSize: 1024, InsertBinlogCount: 1}
			m.Segments.SetSegment(id, segment)
		}
		b.Run(strconv.Itoa(collections), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				m.GetQuotaInfo()
			}
		})
	}
}
