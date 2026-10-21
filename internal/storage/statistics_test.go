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

package storage

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestBuildStatsFromFieldBinlogs_Empty(t *testing.T) {
	s := BuildStatsFromFieldBinlogs(nil, nil, nil, nil)
	require.NotNil(t, s)
	assert.Zero(t, s.GetInsertBinlogSize())
	assert.Zero(t, s.GetInsertBinlogCount())
	assert.Zero(t, s.GetStatsBinlogSize())
	assert.Zero(t, s.GetDeltaBinlogSize())
	assert.Zero(t, s.GetDeleteNumRows())
	assert.Zero(t, s.GetDeltaBinlogCount())
	assert.Zero(t, s.GetTimestampFrom())
	assert.Zero(t, s.GetTimestampTo())
	assert.Zero(t, s.GetDeltaTimestampFrom())
	assert.Zero(t, s.GetDeltaTimestampTo())
	assert.Nil(t, s.GetTimestampQuantiles())
	assert.Nil(t, s.GetNullCounts())
	assert.Nil(t, s.GetLoadResource())
}

// fieldBinlog is a small constructor helper to keep table rows readable.
func fieldBinlog(fieldID int64, logs ...*datapb.Binlog) *datapb.FieldBinlog {
	return &datapb.FieldBinlog{FieldID: fieldID, Binlogs: logs}
}

func TestBuildStatsFromFieldBinlogs_InsertAggregates(t *testing.T) {
	binlogs := []*datapb.FieldBinlog{
		fieldBinlog(
			100,
			&datapb.Binlog{
				MemorySize: 100, EntriesNum: 4, TimestampFrom: 10, TimestampTo: 20,
				FieldNullCounts: map[int64]int64{100: 1, 101: 2},
			},
			&datapb.Binlog{
				MemorySize: 200, EntriesNum: 6, TimestampFrom: 30, TimestampTo: 40,
				FieldNullCounts: map[int64]int64{100: 3},
			},
		),
		// Second field's binlogs also contribute to InsertBinlogSize / Count —
		// the receiver does not deduplicate by row, it sums per-file bytes.
		fieldBinlog(
			101,
			&datapb.Binlog{MemorySize: 50, EntriesNum: 4, TimestampFrom: 10, TimestampTo: 20},
			&datapb.Binlog{MemorySize: 75, EntriesNum: 6, TimestampFrom: 30, TimestampTo: 40},
		),
	}
	s := BuildStatsFromFieldBinlogs(binlogs, nil, nil, nil)
	assert.EqualValues(t, 100+200+50+75, s.GetInsertBinlogSize())
	assert.EqualValues(t, 4, s.GetInsertBinlogCount())
	assert.EqualValues(t, 10, s.GetTimestampFrom())
	assert.EqualValues(t, 40, s.GetTimestampTo())
	assert.Equal(t, map[int64]int64{100: 4, 101: 2}, s.GetNullCounts())
	assert.Equal(t, []*datapb.ColumnGroupStatistics{
		{GroupId: 100, FieldIds: []int64{100}, MemorySize: 300},
		{GroupId: 101, FieldIds: []int64{101}, MemorySize: 125},
	}, s.GetLoadResource().GetColumnGroups())
}

func TestBuildStatsFromFieldBinlogs_TimestampFromIgnoresZero(t *testing.T) {
	// A binlog with TimestampFrom=0 must NOT drag the segment-wide
	// TimestampFrom down to 0 — it's the sentinel for "no boundary
	// recorded," same convention as the live collector.
	binlogs := []*datapb.FieldBinlog{
		fieldBinlog(
			100,
			&datapb.Binlog{MemorySize: 10, EntriesNum: 1, TimestampFrom: 0, TimestampTo: 5},
			&datapb.Binlog{MemorySize: 10, EntriesNum: 1, TimestampFrom: 50, TimestampTo: 60},
		),
	}
	s := BuildStatsFromFieldBinlogs(binlogs, nil, nil, nil)
	assert.EqualValues(t, 50, s.GetTimestampFrom())
	assert.EqualValues(t, 60, s.GetTimestampTo())
}

func TestBuildStatsFromFieldBinlogs_StatsAndDelta(t *testing.T) {
	statslogs := []*datapb.FieldBinlog{
		fieldBinlog(
			100,
			&datapb.Binlog{MemorySize: 1024},
			&datapb.Binlog{MemorySize: 2048},
		),
	}
	deltalogs := []*datapb.FieldBinlog{
		fieldBinlog(
			0,
			&datapb.Binlog{MemorySize: 128, EntriesNum: 3, TimestampFrom: 100, TimestampTo: 200},
			&datapb.Binlog{MemorySize: 256, EntriesNum: 5, TimestampFrom: 50, TimestampTo: 250},
		),
	}
	s := BuildStatsFromFieldBinlogs(nil, statslogs, nil, deltalogs)
	assert.EqualValues(t, 3072, s.GetStatsBinlogSize())
	assert.EqualValues(t, 384, s.GetDeltaBinlogSize())
	assert.EqualValues(t, 8, s.GetDeleteNumRows())
	assert.EqualValues(t, 2, s.GetDeltaBinlogCount())
	assert.EqualValues(t, 50, s.GetDeltaTimestampFrom())
	assert.EqualValues(t, 250, s.GetDeltaTimestampTo())
}

func TestBuildStatsFromFieldBinlogs_StatsIncludesBM25(t *testing.T) {
	statslogs := []*datapb.FieldBinlog{
		fieldBinlog(100, &datapb.Binlog{MemorySize: 1024}),
	}
	bm25logs := []*datapb.FieldBinlog{
		fieldBinlog(101, &datapb.Binlog{MemorySize: 512}, &datapb.Binlog{MemorySize: 256}),
	}
	// StatsBinlogSize is the bloom-filter + BM25 footprint: both arrays sum in.
	s := BuildStatsFromFieldBinlogs(nil, statslogs, bm25logs, nil)
	assert.EqualValues(t, 1024+512+256, s.GetStatsBinlogSize())
}

func TestBuildStatsFromFieldBinlogs_TimestampQuantiles_SingleBinlog(t *testing.T) {
	// One binlog covers all rows; every percentile lands on this file's
	// TimestampTo.
	binlogs := []*datapb.FieldBinlog{
		fieldBinlog(
			100,
			&datapb.Binlog{EntriesNum: 100, TimestampTo: 500, TimestampFrom: 100},
		),
	}
	s := BuildStatsFromFieldBinlogs(binlogs, nil, nil, nil)
	assert.Equal(t, []int64{500, 500, 500, 500, 500}, s.GetTimestampQuantiles())
}

func TestBuildStatsFromFieldBinlogs_TimestampQuantiles_MultipleBinlogs(t *testing.T) {
	// Five equal-sized binlogs at TimestampTo {10, 20, 30, 40, 50}.
	// Cumulative-rowcount marks for 20/40/60/80/100% are 1/2/3/4/5
	// binlogs respectively → quantiles = {10, 20, 30, 40, 50}.
	binlogs := []*datapb.FieldBinlog{
		fieldBinlog(
			100,
			&datapb.Binlog{EntriesNum: 10, TimestampTo: 10},
			&datapb.Binlog{EntriesNum: 10, TimestampTo: 20},
			&datapb.Binlog{EntriesNum: 10, TimestampTo: 30},
			&datapb.Binlog{EntriesNum: 10, TimestampTo: 40},
			&datapb.Binlog{EntriesNum: 10, TimestampTo: 50},
		),
	}
	s := BuildStatsFromFieldBinlogs(binlogs, nil, nil, nil)
	assert.Equal(t, []int64{10, 20, 30, 40, 50}, s.GetTimestampQuantiles())
}

func TestBuildStatsFromFieldBinlogs_TimestampQuantiles_FirstFieldOnly(t *testing.T) {
	// Only the first field's binlogs are walked for quantile derivation —
	// every field shares per-file row counts and timestamps, matching
	// segmentutil.CalcRowCountFromBinLog's convention. Other fields'
	// entries must NOT inflate `totalEntries`.
	binlogs := []*datapb.FieldBinlog{
		fieldBinlog(
			100,
			&datapb.Binlog{EntriesNum: 10, TimestampTo: 100},
		),
		fieldBinlog(
			101,
			&datapb.Binlog{EntriesNum: 10, TimestampTo: 999},
		),
	}
	s := BuildStatsFromFieldBinlogs(binlogs, nil, nil, nil)
	// Only field 100's binlog is used → all percentiles → 100.
	assert.Equal(t, []int64{100, 100, 100, 100, 100}, s.GetTimestampQuantiles())
}

func TestBuildStatsFromFieldBinlogs_TimestampQuantiles_ZeroEntriesSkipped(t *testing.T) {
	// Binlogs with EntriesNum<=0 are skipped from quantile computation
	// (they contribute no rows). The result should match a fixture
	// without them.
	binlogs := []*datapb.FieldBinlog{
		fieldBinlog(
			100,
			&datapb.Binlog{EntriesNum: 0, TimestampTo: 999},
			&datapb.Binlog{EntriesNum: 10, TimestampTo: 100},
		),
	}
	s := BuildStatsFromFieldBinlogs(binlogs, nil, nil, nil)
	assert.Equal(t, []int64{100, 100, 100, 100, 100}, s.GetTimestampQuantiles())
}

func TestBuildStatsFromFieldBinlogs_TimestampQuantiles_UnsortedBinlogs(t *testing.T) {
	// Inputs arrive in non-monotone TimestampTo order; the helper sorts
	// internally before the cumulative scan.
	binlogs := []*datapb.FieldBinlog{
		fieldBinlog(
			100,
			&datapb.Binlog{EntriesNum: 10, TimestampTo: 50},
			&datapb.Binlog{EntriesNum: 10, TimestampTo: 10},
			&datapb.Binlog{EntriesNum: 10, TimestampTo: 30},
			&datapb.Binlog{EntriesNum: 10, TimestampTo: 40},
			&datapb.Binlog{EntriesNum: 10, TimestampTo: 20},
		),
	}
	s := BuildStatsFromFieldBinlogs(binlogs, nil, nil, nil)
	assert.Equal(t, []int64{10, 20, 30, 40, 50}, s.GetTimestampQuantiles())
}

func TestBuildStatsFromFieldBinlogs_TimestampQuantiles_UnevenSizes(t *testing.T) {
	// 100 entries split as 90/5/5. The 20/40/60/80% marks all fall inside
	// the first (large) binlog and pick its TimestampTo (10). The 100%
	// mark picks the last binlog's TimestampTo (30).
	binlogs := []*datapb.FieldBinlog{
		fieldBinlog(
			100,
			&datapb.Binlog{EntriesNum: 90, TimestampTo: 10},
			&datapb.Binlog{EntriesNum: 5, TimestampTo: 20},
			&datapb.Binlog{EntriesNum: 5, TimestampTo: 30},
		),
	}
	s := BuildStatsFromFieldBinlogs(binlogs, nil, nil, nil)
	assert.Equal(t, []int64{10, 10, 10, 10, 30}, s.GetTimestampQuantiles())
}

// insertFieldBinlog is a small constructor for a single-column-group insert
// FieldBinlog with one binlog, used by the Digest tests.
func insertFieldBinlog(groupID int64, members []int64, memSize, entries int64, tsFrom, tsTo uint64, nullCounts map[int64]int64) map[int64]*datapb.FieldBinlog {
	return map[int64]*datapb.FieldBinlog{
		groupID: {
			FieldID:     groupID,
			ChildFields: members,
			Binlogs: []*datapb.Binlog{
				{
					MemorySize:      memSize,
					EntriesNum:      entries,
					TimestampFrom:   tsFrom,
					TimestampTo:     tsTo,
					FieldNullCounts: nullCounts,
				},
			},
		},
	}
}

func TestStatisticsCollector_DigestAndPublish_Cumulative(t *testing.T) {
	c := NewStatisticsCollector()
	// two syncs; Publish returns the cumulative sum (no scaling).
	c.Digest(
		insertFieldBinlog(0, []int64{100, 101}, 1000, 100, 1, 10, map[int64]int64{100: 5}),
		nil, 50, 100, 1, 10,
	)
	c.Digest(
		insertFieldBinlog(0, []int64{100, 101}, 1000, 100, 11, 20, map[int64]int64{100: 3}),
		nil, 50, 100, 11, 20,
	)
	s := c.Publish()
	assert.Equal(t, int64(2000), s.GetInsertBinlogSize())
	assert.Equal(t, int64(2), s.GetInsertBinlogCount())
	assert.Equal(t, int64(100), s.GetStatsBinlogSize())
	// field 100 sums to 8 nulls; field 101 present with zero nulls (presence).
	assert.Equal(t, map[int64]int64{100: 8, 101: 0}, s.GetNullCounts())
	_, ok := s.GetNullCounts()[101]
	assert.True(t, ok)
	assert.Equal(t, uint64(1), s.GetTimestampFrom())
	assert.Equal(t, uint64(20), s.GetTimestampTo())
	// quantiles: 5 marks over cumulative rows by tsTo; both syncs present.
	assert.Len(t, s.GetTimestampQuantiles(), 5)
	assert.Equal(t, []*datapb.ColumnGroupStatistics{
		{GroupId: 0, FieldIds: []int64{100, 101}, MemorySize: 2000},
	}, s.GetLoadResource().GetColumnGroups())
}

func TestStatisticsCollector_Publish_EmptyIsNil(t *testing.T) {
	assert.Nil(t, NewStatisticsCollector().Publish())
}

func TestBuildLoadResourceStatistics_Deterministic(t *testing.T) {
	loadResource := BuildLoadResourceStatistics([]*datapb.FieldBinlog{
		{FieldID: 20, ChildFields: []int64{102, 101, 101}, Binlogs: []*datapb.Binlog{{MemorySize: 30}}},
		{FieldID: 10, Binlogs: []*datapb.Binlog{{MemorySize: 10}, {MemorySize: 20}}},
		{FieldID: 20, ChildFields: []int64{103}, Binlogs: []*datapb.Binlog{{MemorySize: 40}}},
	})
	require.Equal(t, []*datapb.ColumnGroupStatistics{
		{GroupId: 10, FieldIds: []int64{10}, MemorySize: 30},
		{GroupId: 20, FieldIds: []int64{101, 102, 103}, MemorySize: 70},
	}, loadResource.GetColumnGroups())
}

func TestStatisticsLoadResourceProtoRoundTrip(t *testing.T) {
	original := &datapb.Statistics{
		LoadResource: &datapb.LoadResourceStatistics{
			ColumnGroups: []*datapb.ColumnGroupStatistics{{GroupId: 0, FieldIds: []int64{100}, MemorySize: 0}},
		},
	}

	data, err := proto.Marshal(original)
	require.NoError(t, err)
	var restored datapb.Statistics
	require.NoError(t, proto.Unmarshal(data, &restored))
	require.True(t, proto.Equal(original, &restored))
	require.NotNil(t, restored.GetLoadResource())

	data, err = proto.Marshal(&datapb.Statistics{})
	require.NoError(t, err)
	restored.Reset()
	require.NoError(t, proto.Unmarshal(data, &restored))
	require.Nil(t, restored.GetLoadResource())
}

func TestStatisticsCollector_LoadResourceAccumulatesAndRestores(t *testing.T) {
	c := NewStatisticsCollector()
	c.Digest(insertFieldBinlog(20, []int64{102, 101}, 30, 10, 1, 10, nil), nil, 0, 10, 1, 10)
	c.Digest(insertFieldBinlog(10, nil, 20, 10, 11, 20, nil), nil, 0, 10, 11, 20)

	persisted := c.Publish()
	require.Equal(t, []*datapb.ColumnGroupStatistics{
		{GroupId: 10, FieldIds: []int64{10}, MemorySize: 20},
		{GroupId: 20, FieldIds: []int64{101, 102}, MemorySize: 30},
	}, persisted.GetLoadResource().GetColumnGroups())

	restored := NewStatisticsCollectorFromStats(persisted, 20)
	restored.Digest(insertFieldBinlog(20, []int64{101, 102, 103}, 40, 10, 21, 30, nil), nil, 0, 10, 21, 30)
	got := restored.Publish()
	require.Equal(t, []*datapb.ColumnGroupStatistics{
		{GroupId: 10, FieldIds: []int64{10}, MemorySize: 20},
		{GroupId: 20, FieldIds: []int64{101, 102, 103}, MemorySize: 70},
	}, got.GetLoadResource().GetColumnGroups())
	require.EqualValues(t, 90, got.GetInsertBinlogSize())
}

func TestStatisticsCollector_LoadResourceNilAndEmptyRoundTrip(t *testing.T) {
	require.Nil(t, NewStatisticsCollectorFromStats(&datapb.Statistics{}, 0).Publish())

	c := NewStatisticsCollectorFromStats(&datapb.Statistics{
		LoadResource: &datapb.LoadResourceStatistics{},
	}, 0)
	got := c.Publish()
	require.NotNil(t, got)
	require.NotNil(t, got.GetLoadResource())
	require.Empty(t, got.GetLoadResource().GetColumnGroups())
	require.NotNil(t, c.Clone().Publish().GetLoadResource())
}

func TestStatisticsCollector_LoadResourceZeroValuesAreAvailable(t *testing.T) {
	c := NewStatisticsCollector()
	inserts := insertFieldBinlog(0, []int64{100}, 0, 1, 1, 1, nil)
	c.Digest(inserts, nil, 0, 1, 1, 1)

	loadResource := c.Publish().GetLoadResource()
	require.NotNil(t, loadResource)
	require.Equal(t, []*datapb.ColumnGroupStatistics{{GroupId: 0, FieldIds: []int64{100}}}, loadResource.GetColumnGroups())
}

func TestStatisticsCollector_CloneLoadResourceIsIndependent(t *testing.T) {
	c := NewStatisticsCollector()
	c.Digest(insertFieldBinlog(0, []int64{100}, 10, 1, 1, 1, nil), nil, 0, 1, 1, 1)

	clone := c.Clone()
	clone.Digest(insertFieldBinlog(0, []int64{100}, 20, 1, 2, 2, nil), nil, 0, 1, 2, 2)

	require.EqualValues(t, 10, c.Publish().GetLoadResource().GetColumnGroups()[0].GetMemorySize())
	require.EqualValues(t, 30, clone.Publish().GetLoadResource().GetColumnGroups()[0].GetMemorySize())
}

// TestStatisticsCollector_StatsBlobSizeAccumulates pins the caller contract for
// the sync path: each Digest receives THIS SYNC's newly-written stats-blob
// bytes (a per-sync delta), and Digest accumulates them. Two syncs each passing
// statsBlobSize=100 must sum to 200 — NOT 300. If a caller mistakenly passed the
// cumulative footprint per sync, this would over-count.
func TestStatisticsCollector_StatsBlobSizeAccumulates(t *testing.T) {
	c := NewStatisticsCollector()
	// sync1: 100 new stats-blob bytes written this sync.
	c.Digest(
		insertFieldBinlog(0, []int64{100}, 1000, 100, 1, 10, nil),
		nil, 100, 100, 1, 10,
	)
	// sync2: another 100 new stats-blob bytes written this sync.
	c.Digest(
		insertFieldBinlog(0, []int64{100}, 1000, 100, 11, 20, nil),
		nil, 100, 100, 11, 20,
	)
	s := c.Publish()
	assert.Equal(t, int64(200), s.GetStatsBinlogSize())
}

func TestBuildStatsFromFieldBinlogs_NullCountsCompletion(t *testing.T) {
	cases := []struct {
		name    string
		binlogs []*datapb.FieldBinlog
		want    map[int64]int64
	}{
		{
			// V1 / pre-#46903 shape: one FieldBinlog per real field, no
			// per-binlog FieldNullCounts metadata. Presence of the
			// FieldBinlog itself must yield a zero entry.
			name: "v1 per-field binlogs without null counts get zero entries",
			binlogs: []*datapb.FieldBinlog{
				{FieldID: 100, Binlogs: []*datapb.Binlog{{EntriesNum: 10}}},
				{FieldID: 101, Binlogs: []*datapb.Binlog{{EntriesNum: 10}}},
			},
			want: map[int64]int64{100: 0, 101: 0},
		},
		{
			// Packed shape: FieldID is a column-group ID, ChildFields are
			// the real members. Counts sum across binlogs; members without
			// counts still get a zero entry.
			name: "packed column group sums counts and completes members",
			binlogs: []*datapb.FieldBinlog{
				{
					FieldID:     0,
					ChildFields: []int64{100, 101},
					Binlogs: []*datapb.Binlog{
						{EntriesNum: 10, FieldNullCounts: map[int64]int64{100: 3}},
						{EntriesNum: 10, FieldNullCounts: map[int64]int64{100: 2}},
					},
				},
			},
			want: map[int64]int64{100: 5, 101: 0},
		},
		{
			// A FieldBinlog with no binlogs carries no data: no entry.
			name: "empty field binlog adds no entry",
			binlogs: []*datapb.FieldBinlog{
				{FieldID: 100},
			},
			want: nil,
		},
		{
			// Pre-ChildFields packed shape: a vector column group whose
			// GroupID equals the vector field id, with no ChildFields and
			// no FieldNullCounts metadata. The FieldID fallback must seed
			// the vector field's zero entry.
			name: "legacy packed vector group seeds entry via FieldID fallback",
			binlogs: []*datapb.FieldBinlog{
				{FieldID: 102, Binlogs: []*datapb.Binlog{{EntriesNum: 10}}},
			},
			want: map[int64]int64{102: 0},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := BuildStatsFromFieldBinlogs(tc.binlogs, nil, nil, nil)
			assert.Equal(t, tc.want, s.GetNullCounts())
		})
	}
}

func TestStatisticsCollector_RestoreFromStats_RoundTrips(t *testing.T) {
	persisted := &datapb.Statistics{
		InsertBinlogSize:   1000,
		InsertBinlogCount:  4,
		StatsBinlogSize:    50,
		DeltaBinlogSize:    30,
		DeltaBinlogCount:   2,
		DeleteNumRows:      7,
		TimestampFrom:      10,
		TimestampTo:        50,
		NullCounts:         map[int64]int64{100: 5, 101: 0},
		TimestampQuantiles: []int64{20, 30, 40, 45, 50},
	}
	got := NewStatisticsCollectorFromStats(persisted, 1000).Publish()
	require.NotNil(t, got)
	assert.Equal(t, int64(1000), got.GetInsertBinlogSize())
	assert.Equal(t, int64(4), got.GetInsertBinlogCount())
	assert.Equal(t, int64(50), got.GetStatsBinlogSize())
	assert.Equal(t, int64(30), got.GetDeltaBinlogSize())
	assert.Equal(t, int64(2), got.GetDeltaBinlogCount())
	assert.Equal(t, int64(7), got.GetDeleteNumRows())
	assert.Equal(t, uint64(10), got.GetTimestampFrom())
	assert.Equal(t, uint64(50), got.GetTimestampTo())
	assert.Equal(t, map[int64]int64{100: 5, 101: 0}, got.GetNullCounts())
	// quantiles round-trip exactly from the reconstructed buckets
	assert.Equal(t, []int64{20, 30, 40, 45, 50}, got.GetTimestampQuantiles())
}

func TestStatisticsCollector_RestoreFromStats_RoundTrips_NonDivisibleNumRows(t *testing.T) {
	// numRows not divisible by len(quantiles): the reconstructed buckets must
	// still reproduce the persisted marks exactly (idempotent restore).
	persisted := &datapb.Statistics{
		InsertBinlogSize:   1000,
		InsertBinlogCount:  4,
		TimestampFrom:      10,
		TimestampTo:        50,
		TimestampQuantiles: []int64{20, 30, 40, 45, 50},
	}
	got := NewStatisticsCollectorFromStats(persisted, 103).Publish()
	require.NotNil(t, got)
	assert.Equal(t, []int64{20, 30, 40, 45, 50}, got.GetTimestampQuantiles())
}

func TestStatisticsCollector_RestoreThenDigest_Accumulates(t *testing.T) {
	c := NewStatisticsCollectorFromStats(&datapb.Statistics{
		InsertBinlogSize:  1000,
		InsertBinlogCount: 4,
		NullCounts:        map[int64]int64{100: 5},
	}, 1000)
	// a post-restore sync: one insert binlog of 200 bytes, field 100 +2 nulls
	c.Digest(map[int64]*datapb.FieldBinlog{
		0: {FieldID: 0, ChildFields: []int64{100}, Binlogs: []*datapb.Binlog{
			{MemorySize: 200, FieldNullCounts: map[int64]int64{100: 2}},
		}},
	}, nil, 0, 100, 60, 70)
	got := c.Publish()
	assert.Equal(t, int64(1200), got.GetInsertBinlogSize())
	assert.Equal(t, int64(5), got.GetInsertBinlogCount())
	assert.Equal(t, int64(7), got.GetNullCounts()[100])
}

func TestStatisticsCollector_RestoreFromNil_Empty(t *testing.T) {
	assert.Nil(t, NewStatisticsCollectorFromStats(nil, 0).Publish())
}

func TestBuildStatsFromFieldBinlogs_Formats(t *testing.T) {
	t.Run("single format", func(t *testing.T) {
		s := BuildStatsFromFieldBinlogs([]*datapb.FieldBinlog{
			{FieldID: 0, Format: "parquet", Binlogs: []*datapb.Binlog{{MemorySize: 100}}},
			{FieldID: 1, Format: "parquet", Binlogs: []*datapb.Binlog{{MemorySize: 200}}},
		}, nil, nil, nil)
		assert.Equal(t, []string{"parquet"}, s.GetFormats())
	})
	t.Run("mixed formats sorted", func(t *testing.T) {
		s := BuildStatsFromFieldBinlogs([]*datapb.FieldBinlog{
			{FieldID: 0, Format: "parquet", Binlogs: []*datapb.Binlog{{MemorySize: 100}}},
			{FieldID: 1, Format: "lance", Binlogs: []*datapb.Binlog{{MemorySize: 200}}},
		}, nil, nil, nil)
		assert.Equal(t, []string{"lance", "parquet"}, s.GetFormats())
	})
	t.Run("empty format ignored", func(t *testing.T) {
		s := BuildStatsFromFieldBinlogs([]*datapb.FieldBinlog{
			{FieldID: 0, Format: "", Binlogs: []*datapb.Binlog{{MemorySize: 100}}},
			{FieldID: 1, Format: "parquet", Binlogs: []*datapb.Binlog{{MemorySize: 200}}},
		}, nil, nil, nil)
		assert.Equal(t, []string{"parquet"}, s.GetFormats())
	})
	t.Run("no format at all", func(t *testing.T) {
		s := BuildStatsFromFieldBinlogs([]*datapb.FieldBinlog{
			{FieldID: 0, Binlogs: []*datapb.Binlog{{MemorySize: 100}}},
		}, nil, nil, nil)
		assert.Empty(t, s.GetFormats())
	})
	t.Run("format collected even with empty binlogs", func(t *testing.T) {
		s := BuildStatsFromFieldBinlogs([]*datapb.FieldBinlog{
			{FieldID: 0, Format: "parquet"},
		}, nil, nil, nil)
		assert.Equal(t, []string{"parquet"}, s.GetFormats())
	})
}

func TestStatisticsCollector_FormatsDigestPublishCloneRestore(t *testing.T) {
	c := NewStatisticsCollector()

	fb1 := map[int64]*datapb.FieldBinlog{
		0: {FieldID: 0, Format: "parquet", Binlogs: []*datapb.Binlog{{MemorySize: 100, EntriesNum: 10, TimestampFrom: 1, TimestampTo: 10}}},
	}
	fb2 := map[int64]*datapb.FieldBinlog{
		1: {FieldID: 1, Format: "lance", Binlogs: []*datapb.Binlog{{MemorySize: 200, EntriesNum: 20, TimestampFrom: 11, TimestampTo: 20}}},
	}

	c.Digest(fb1, nil, 0, 10, 1, 10)
	c.Digest(fb2, nil, 0, 20, 11, 20)

	s := c.Publish()
	assert.Equal(t, []string{"lance", "parquet"}, s.GetFormats())

	// Clone is independent
	cloned := c.Clone()
	s2 := cloned.Publish()
	assert.Equal(t, []string{"lance", "parquet"}, s2.GetFormats())

	// Mutating clone doesn't affect original
	fb3 := map[int64]*datapb.FieldBinlog{
		2: {FieldID: 2, Format: "orc", Binlogs: []*datapb.Binlog{{MemorySize: 300, EntriesNum: 30, TimestampFrom: 21, TimestampTo: 30}}},
	}
	cloned.Digest(fb3, nil, 0, 30, 21, 30)
	assert.Equal(t, []string{"lance", "orc", "parquet"}, cloned.Publish().GetFormats())
	assert.Equal(t, []string{"lance", "parquet"}, c.Publish().GetFormats())

	// Restore from published stats
	restored := NewStatisticsCollectorFromStats(s, 30)
	sr := restored.Publish()
	assert.Equal(t, []string{"lance", "parquet"}, sr.GetFormats())

	// Restore accumulates new formats
	restored.Digest(fb3, nil, 0, 30, 21, 30)
	assert.Equal(t, []string{"lance", "orc", "parquet"}, restored.Publish().GetFormats())
}

func validElementBinlog(groupID int64, members []int64, counts ...map[int64]int64) map[int64]*datapb.FieldBinlog {
	fb := &datapb.FieldBinlog{FieldID: groupID, ChildFields: members}
	for _, c := range counts {
		fb.Binlogs = append(fb.Binlogs, &datapb.Binlog{EntriesNum: 10, MemorySize: 100, FieldValidElementCounts: c})
	}
	return map[int64]*datapb.FieldBinlog{groupID: fb}
}

func TestBuildStatsFromFieldBinlogs_ValidElementCounts(t *testing.T) {
	fieldBinlogs := func(groups ...map[int64]*datapb.FieldBinlog) []*datapb.FieldBinlog {
		var out []*datapb.FieldBinlog
		for _, g := range groups {
			for _, fb := range g {
				out = append(out, fb)
			}
		}
		return out
	}
	// Summed across binlogs, zero kept.
	s := BuildStatsFromFieldBinlogs(fieldBinlogs(
		validElementBinlog(0, []int64{100, 101}, nil, nil),
		validElementBinlog(102, []int64{102}, map[int64]int64{102: 3}, map[int64]int64{102: 4}),
		validElementBinlog(103, []int64{103}, map[int64]int64{103: 0}),
	), nil, nil, nil)
	assert.Equal(t, map[int64]int64{102: 7, 103: 0}, s.GetValidElementCounts())

	// One binlog of the field without a count (written before counts were
	// recorded): unknown, so no entry.
	s = BuildStatsFromFieldBinlogs(fieldBinlogs(
		validElementBinlog(102, []int64{102}, nil, map[int64]int64{102: 4}),
		validElementBinlog(103, []int64{103}, map[int64]int64{103: 2}),
	), nil, nil, nil)
	assert.Equal(t, map[int64]int64{103: 2}, s.GetValidElementCounts())

	// The same field in another FieldBinlog without counts is unknown too.
	s = BuildStatsFromFieldBinlogs(fieldBinlogs(
		validElementBinlog(102, []int64{102}, map[int64]int64{102: 4}),
		validElementBinlog(0, []int64{100, 102}, nil),
	), nil, nil, nil)
	assert.Empty(t, s.GetValidElementCounts())
}

func TestStatisticsCollector_ValidElementCounts(t *testing.T) {
	t.Run("counted syncs accumulate and survive restore", func(t *testing.T) {
		c := NewStatisticsCollector()
		c.Digest(validElementBinlog(102, []int64{102}, map[int64]int64{102: 3}), nil, 0, 10, 1, 2)
		c.Digest(validElementBinlog(102, []int64{102}, map[int64]int64{102: 0}), nil, 0, 10, 3, 4)
		s := c.Publish()
		assert.Equal(t, map[int64]int64{102: 3}, s.GetValidElementCounts())

		restored := NewStatisticsCollectorFromStats(s, 20)
		restored.Digest(validElementBinlog(102, []int64{102}, map[int64]int64{102: 5}), nil, 0, 10, 5, 6)
		assert.Equal(t, map[int64]int64{102: 8}, restored.Publish().GetValidElementCounts())

		cloned := restored.Clone()
		cloned.Digest(validElementBinlog(102, []int64{102}, map[int64]int64{102: 1}), nil, 0, 10, 7, 8)
		assert.Equal(t, map[int64]int64{102: 8}, restored.Publish().GetValidElementCounts())
		assert.Equal(t, map[int64]int64{102: 9}, cloned.Publish().GetValidElementCounts())
	})

	t.Run("a sync without the count makes it unknown for good", func(t *testing.T) {
		c := NewStatisticsCollector()
		c.Digest(validElementBinlog(102, []int64{102}, map[int64]int64{102: 3}), nil, 0, 10, 1, 2)
		c.Digest(validElementBinlog(102, []int64{102}, nil), nil, 0, 10, 3, 4)
		assert.Empty(t, c.Publish().GetValidElementCounts())
		c.Digest(validElementBinlog(102, []int64{102}, map[int64]int64{102: 5}), nil, 0, 10, 5, 6)
		assert.Empty(t, c.Publish().GetValidElementCounts())
		// After a restore the field has data (null_counts) but no count.
		restored := NewStatisticsCollectorFromStats(c.Publish(), 30)
		restored.Digest(validElementBinlog(102, []int64{102}, map[int64]int64{102: 5}), nil, 0, 10, 7, 8)
		assert.Empty(t, restored.Publish().GetValidElementCounts())
	})

	t.Run("data written before counts were recorded", func(t *testing.T) {
		old := NewStatisticsCollector()
		old.Digest(validElementBinlog(102, []int64{102}, nil), nil, 0, 10, 1, 2)
		restored := NewStatisticsCollectorFromStats(old.Publish(), 10)
		restored.Digest(validElementBinlog(102, []int64{102}, map[int64]int64{102: 5}), nil, 0, 10, 3, 4)
		assert.Empty(t, restored.Publish().GetValidElementCounts())
	})

	t.Run("one binlog of a sync without the count", func(t *testing.T) {
		c := NewStatisticsCollector()
		c.Digest(validElementBinlog(102, []int64{102}, map[int64]int64{102: 3}, nil), nil, 0, 20, 1, 2)
		assert.Empty(t, c.Publish().GetValidElementCounts())
	})
}

func TestVectorArrayValidElementCount(t *testing.T) {
	const fieldID = int64(102)
	field := &schemapb.FieldSchema{
		FieldID: fieldID, Name: "s[vec]", DataType: schemapb.DataType_ArrayOfVector,
		ElementType: schemapb.DataType_FloatVector, Nullable: true, ElementNullable: true,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
	}
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: 101, Name: "s", Nullable: true, Fields: []*schemapb.FieldSchema{field},
		}},
	}
	vec := func(valid []bool, data ...float32) *schemapb.VectorField {
		return &schemapb.VectorField{
			Dim: 2, ValidData: valid,
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: data}},
		}
	}
	rows := []any{
		vec([]bool{true, false, true}, 1, 2, 3, 4), // 2 valid of 3
		nil,                       // null struct row
		vec(nil),                  // empty array
		vec([]bool{false, false}), // all elements null
		vec([]bool{true}, 5, 6),   // 1 valid
	}
	values := make([]*Value, 0, len(rows))
	for i, row := range rows {
		values = append(values, &Value{Value: map[FieldID]any{100: int64(i), fieldID: row}})
	}
	record, err := ValueSerializer(values, schema)
	require.NoError(t, err)
	defer record.Release()

	n, ok := VectorArrayValidElementCount(record.Column(fieldID))
	require.True(t, ok)
	assert.EqualValues(t, 3, n)

	// A sliced column counts only its own rows.
	sliced := array.NewSlice(record.Column(fieldID), 2, 5)
	defer sliced.Release()
	n, ok = VectorArrayValidElementCount(sliced)
	require.True(t, ok)
	assert.EqualValues(t, 1, n)

	_, ok = VectorArrayValidElementCount(record.Column(100))
	assert.False(t, ok)

	counts, unknown := CollectVectorArrayValidElements(schema, record, nil, nil)
	assert.Equal(t, map[FieldID]int64{fieldID: 3}, counts)
	assert.Empty(t, unknown)
	counts, _ = CollectVectorArrayValidElements(schema, record, counts, unknown)
	assert.Equal(t, map[FieldID]int64{fieldID: 6}, counts)
	assert.Equal(t, map[int64]int64{fieldID: 6},
		ColumnGroupValidElementCounts(storagecommon.ColumnGroup{GroupID: fieldID, Fields: []int64{fieldID}}, counts))
	assert.Nil(t, ColumnGroupValidElementCounts(storagecommon.ColumnGroup{GroupID: 0, Fields: []int64{100}}, counts))
}

// The packed writers record the valid element count of every VectorArray
// field on the binlog of its column group (StorageV2 and V3).
func TestPackedWritersRecordValidElementCounts(t *testing.T) {
	const (
		pkID    = int64(100)
		vecID   = int64(102)
		plainID = int64(103)
	)
	dim := []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}
	schema := &schemapb.CollectionSchema{
		Name: "valid_element_counts",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: pkID, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: 101, Name: "s", Nullable: true, Fields: []*schemapb.FieldSchema{
				{
					FieldID: vecID, Name: "s[vec]", DataType: schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector, Nullable: true, ElementNullable: true, TypeParams: dim,
				},
				{
					FieldID: plainID, Name: "s[plain]", DataType: schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector, Nullable: true, TypeParams: dim,
				},
			},
		}},
	}
	vec := func(valid []bool, data ...float32) *schemapb.VectorField {
		return &schemapb.VectorField{
			Dim: 2, ValidData: valid,
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: data}},
		}
	}
	type row struct{ en, plain any }
	rows := []row{
		{vec([]bool{true, false, true}, 1, 2, 3, 4), vec(nil, 1, 2, 3, 4)},
		{nil, nil},           // null struct row
		{vec(nil), vec(nil)}, // empty arrays
		{vec([]bool{false, false}), vec(nil, 5, 6)},
	}
	values := make([]*Value, 0, len(rows))
	for i, r := range rows {
		values = append(values, &Value{Value: map[FieldID]any{
			common.RowIDField: int64(i), common.TimeStampField: int64(1000), pkID: int64(i), vecID: r.en, plainID: r.plain,
		}})
	}
	want := map[int64]int64{vecID: 2, plainID: 3}

	for _, version := range []int64{StorageV2, StorageV3} {
		t.Run(fmt.Sprintf("storage v%d", version), func(t *testing.T) {
			dir := t.TempDir()
			paramtable.Get().Save(paramtable.Get().CommonCfg.StorageType.Key, "local")
			paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, dir)
			defer func() {
				paramtable.Get().Reset(paramtable.Get().CommonCfg.StorageType.Key)
				paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key)
			}()
			config := &indexpb.StorageConfig{RootPath: dir, StorageType: "local"}
			record, err := ValueSerializer(values, schema)
			require.NoError(t, err)
			defer record.Release()
			opts := []RwOption{
				WithVersion(version), WithStorageConfig(config),
				WithUploader(func(context.Context, map[string][]byte) error { return nil }),
			}
			if version == StorageV3 {
				opts = append(opts, WithWriterFormat("parquet"))
			}
			w, err := NewBinlogRecordWriter(context.Background(), 1, 2, 3, schema,
				allocator.NewLocalAllocator(1, math.MaxInt64), 1<<20, int64(len(values)), opts...)
			require.NoError(t, err)
			require.NoError(t, w.Write(record))
			require.NoError(t, w.Close())
			fieldBinlogs, _, _, _, _ := w.GetLogs()

			got := make(map[int64]int64)
			for _, fb := range fieldBinlogs {
				for _, l := range fb.GetBinlogs() {
					for f, n := range l.GetFieldValidElementCounts() {
						got[f] += n
					}
					for f := range l.GetFieldValidElementCounts() {
						assert.Contains(t, fb.GetChildFields(), f)
					}
				}
			}
			assert.Equal(t, want, got)
			stats := BuildStatsFromFieldBinlogs(lo.Values(fieldBinlogs), nil, nil, nil)
			assert.Equal(t, want, stats.GetValidElementCounts())
		})
	}
}
