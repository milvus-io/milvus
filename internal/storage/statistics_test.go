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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
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
}

func TestStatisticsCollector_Publish_EmptyIsNil(t *testing.T) {
	assert.Nil(t, NewStatisticsCollector().Publish())
}

// TestStatisticsCollector_StatsBlobSizeAccumulates pins the caller contract for
// the V3 sync path: each Digest receives THIS SYNC's newly-written stats-blob
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
