package datacoord

import (
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestCompactionTo(t *testing.T) {
	t.Run("mix_2_to_1", func(t *testing.T) {
		segments := NewSegmentsInfo()
		segment := NewSegmentInfo(&datapb.SegmentInfo{
			ID: 1,
		})
		segments.SetSegment(segment.GetID(), segment)

		compactTos, ok := segments.GetCompactionTo(1)
		assert.True(t, ok)
		assert.Nil(t, compactTos)

		segment = NewSegmentInfo(&datapb.SegmentInfo{
			ID: 2,
		})
		segments.SetSegment(segment.GetID(), segment)
		segment = NewSegmentInfo(&datapb.SegmentInfo{
			ID:             3,
			CompactionFrom: []int64{1, 2},
		})
		segments.SetSegment(segment.GetID(), segment)

		getCompactToIDs := func(segments []*SegmentInfo) []int64 {
			return lo.Map(segments, func(segment *SegmentInfo, _ int) int64 { return segment.GetID() })
		}

		compactTos, ok = segments.GetCompactionTo(3)
		assert.Nil(t, compactTos)
		assert.True(t, ok)
		compactTos, ok = segments.GetCompactionTo(1)
		assert.True(t, ok)
		assert.NotNil(t, compactTos)
		assert.ElementsMatch(t, []int64{3}, getCompactToIDs(compactTos))
		compactTos, ok = segments.GetCompactionTo(2)
		assert.True(t, ok)
		assert.NotNil(t, compactTos)
		assert.ElementsMatch(t, []int64{3}, getCompactToIDs(compactTos))

		// should be droped.
		segments.DropSegment(1)
		compactTos, ok = segments.GetCompactionTo(1)
		assert.False(t, ok)
		assert.NotNil(t, compactTos)
		compactTos, ok = segments.GetCompactionTo(2)
		assert.True(t, ok)
		assert.NotNil(t, compactTos)
		assert.ElementsMatch(t, []int64{3}, getCompactToIDs(compactTos))
		compactTos, ok = segments.GetCompactionTo(3)
		assert.Nil(t, compactTos)
		assert.True(t, ok)

		segments.DropSegment(3)
		compactTos, ok = segments.GetCompactionTo(2)
		assert.True(t, ok)
		assert.Nil(t, compactTos)
	})

	t.Run("split_1_to_2", func(t *testing.T) {
		segments := NewSegmentsInfo()
		segment := NewSegmentInfo(&datapb.SegmentInfo{
			ID: 1,
		})
		segments.SetSegment(segment.GetID(), segment)

		compactTos, ok := segments.GetCompactionTo(1)
		assert.True(t, ok)
		assert.Nil(t, compactTos)

		segment = NewSegmentInfo(&datapb.SegmentInfo{
			ID:             2,
			CompactionFrom: []int64{1},
		})
		segments.SetSegment(segment.GetID(), segment)
		segment = NewSegmentInfo(&datapb.SegmentInfo{
			ID:             3,
			CompactionFrom: []int64{1},
		})
		segments.SetSegment(segment.GetID(), segment)

		getCompactToIDs := func(segments []*SegmentInfo) []int64 {
			return lo.Map(segments, func(segment *SegmentInfo, _ int) int64 { return segment.GetID() })
		}

		compactTos, ok = segments.GetCompactionTo(2)
		assert.Nil(t, compactTos)
		assert.True(t, ok)
		compactTos, ok = segments.GetCompactionTo(3)
		assert.Nil(t, compactTos)
		assert.True(t, ok)
		compactTos, ok = segments.GetCompactionTo(1)
		assert.True(t, ok)
		assert.NotNil(t, compactTos)
		assert.ElementsMatch(t, []int64{2, 3}, getCompactToIDs(compactTos))
	})
}

func TestGetSegmentSize(t *testing.T) {
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{
						{
							LogID:      1,
							MemorySize: 1,
						},
					},
				},
			},
			Statslogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{
							LogID:      1,
							MemorySize: 1,
						},
					},
				},
			},
			Deltalogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{
							LogID:      1,
							MemorySize: 1,
						},
					},
				},
			},
		},
	}

	assert.Equal(t, int64(3), segment.getSegmentSize())
	assert.Equal(t, int64(3), segment.getSegmentSize())
	assert.Equal(t, int64(1), segment.getFieldBinlogSize(1))
	// field 2 has no binlogs, fallback to getSegmentSize
	assert.Equal(t, int64(3), segment.getFieldBinlogSize(2))
}

func TestIsDeltaLogExists(t *testing.T) {
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			Deltalogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{
							LogID: 1,
						},
						{
							LogID: 2,
						},
					},
				},
			},
		},
	}
	assert.True(t, segment.IsDeltaLogExists(1))
	assert.True(t, segment.IsDeltaLogExists(2))
	assert.False(t, segment.IsDeltaLogExists(3))
	assert.False(t, segment.IsDeltaLogExists(0))
}

func TestIsStatsLogExists(t *testing.T) {
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			Statslogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{
							LogID: 1,
						},
						{
							LogID: 2,
						},
					},
				},
			},
		},
	}
	assert.True(t, segment.IsStatsLogExists(1))
	assert.True(t, segment.IsStatsLogExists(2))
	assert.False(t, segment.IsStatsLogExists(3))
	assert.False(t, segment.IsStatsLogExists(0))
}

func TestValidateManifestSegment(t *testing.T) {
	t.Run("no manifest is always valid", func(t *testing.T) {
		info := NewSegmentInfo(&datapb.SegmentInfo{
			ID: 1,
			Statslogs: []*datapb.FieldBinlog{
				{FieldID: 100},
			},
		})
		assert.Empty(t, ValidateManifestSegment(info))
	})

	t.Run("manifest with empty legacy fields is valid", func(t *testing.T) {
		info := NewSegmentInfo(&datapb.SegmentInfo{
			ID:           2,
			ManifestPath: "base/path@1",
		})
		assert.Empty(t, ValidateManifestSegment(info))
	})

	t.Run("manifest with statslogs is invalid", func(t *testing.T) {
		info := NewSegmentInfo(&datapb.SegmentInfo{
			ID:           3,
			ManifestPath: "base/path@1",
			Statslogs: []*datapb.FieldBinlog{
				{FieldID: 100},
			},
		})
		msg := ValidateManifestSegment(info)
		assert.Contains(t, msg, "statslogs")
		assert.Contains(t, msg, "segment 3")
	})

	t.Run("manifest with bm25statslogs is invalid", func(t *testing.T) {
		info := NewSegmentInfo(&datapb.SegmentInfo{
			ID:           4,
			ManifestPath: "base/path@1",
			Bm25Statslogs: []*datapb.FieldBinlog{
				{FieldID: 200},
			},
		})
		msg := ValidateManifestSegment(info)
		assert.Contains(t, msg, "bm25statslogs")
	})

	t.Run("manifest with text stats is invalid", func(t *testing.T) {
		info := NewSegmentInfo(&datapb.SegmentInfo{
			ID:           5,
			ManifestPath: "base/path@1",
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				10: {FieldID: 10},
			},
		})
		msg := ValidateManifestSegment(info)
		assert.Contains(t, msg, "textStatsLogs")
	})

	t.Run("manifest with json key stats is invalid", func(t *testing.T) {
		info := NewSegmentInfo(&datapb.SegmentInfo{
			ID:           6,
			ManifestPath: "base/path@1",
			JsonKeyStats: map[int64]*datapb.JsonKeyStats{
				20: {FieldID: 20},
			},
		})
		msg := ValidateManifestSegment(info)
		assert.Contains(t, msg, "jsonKeyStats")
	})

	t.Run("manifest with multiple non-empty fields", func(t *testing.T) {
		info := NewSegmentInfo(&datapb.SegmentInfo{
			ID:           7,
			ManifestPath: "base/path@1",
			Statslogs:    []*datapb.FieldBinlog{{FieldID: 100}},
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				10: {FieldID: 10},
			},
		})
		msg := ValidateManifestSegment(info)
		assert.Contains(t, msg, "statslogs")
		assert.Contains(t, msg, "textStatsLogs")
	})
}

func TestSegmentEffectiveTs(t *testing.T) {
	t.Run("returns commit_timestamp when non-zero", func(t *testing.T) {
		seg := &datapb.SegmentInfo{
			StartPosition:   &msgpb.MsgPosition{Timestamp: 1000},
			CommitTimestamp: 5000,
		}
		assert.Equal(t, uint64(5000), segmentEffectiveTs(seg))
	})
	t.Run("returns start_position.Timestamp when commit_timestamp is zero", func(t *testing.T) {
		seg := &datapb.SegmentInfo{
			StartPosition: &msgpb.MsgPosition{Timestamp: 1000},
		}
		assert.Equal(t, uint64(1000), segmentEffectiveTs(seg))
	})
}

func TestSegmentEffectiveDmlTs(t *testing.T) {
	t.Run("returns commit_timestamp when non-zero", func(t *testing.T) {
		seg := &datapb.SegmentInfo{
			DmlPosition:     &msgpb.MsgPosition{Timestamp: 2000},
			CommitTimestamp: 5000,
		}
		assert.Equal(t, uint64(5000), segmentEffectiveDmlTs(seg))
	})
	t.Run("returns dml_position.Timestamp when commit_timestamp is zero", func(t *testing.T) {
		seg := &datapb.SegmentInfo{
			DmlPosition: &msgpb.MsgPosition{Timestamp: 2000},
		}
		assert.Equal(t, uint64(2000), segmentEffectiveDmlTs(seg))
	})
}

func TestGetEarliestTs_CommitTimestamp(t *testing.T) {
	t.Run("returns commit_timestamp when non-zero, ignoring stale binlog timestamps", func(t *testing.T) {
		seg := NewSegmentInfo(&datapb.SegmentInfo{
			Binlogs: []*datapb.FieldBinlog{
				{Binlogs: []*datapb.Binlog{{TimestampFrom: 100, TimestampTo: 200}}},
			},
			CommitTimestamp: 9999,
		})
		assert.Equal(t, uint64(9999), seg.GetEarliestTs())
	})

	t.Run("falls back to binlog TimestampFrom when commit_timestamp is zero", func(t *testing.T) {
		seg := NewSegmentInfo(&datapb.SegmentInfo{
			Binlogs: []*datapb.FieldBinlog{
				{Binlogs: []*datapb.Binlog{{TimestampFrom: 100, TimestampTo: 200}}},
				{Binlogs: []*datapb.Binlog{{TimestampFrom: 50, TimestampTo: 150}}},
			},
		})
		assert.Equal(t, uint64(50), seg.GetEarliestTs())
	})
}

// TestGetEarliestTs_AfterCloneWithReplacedBinlogs exercises the path that
// compaction completion takes: an import segment with commit_ts != 0 is
// Clone()d with an option that sets CommitTimestamp=0 and replaces the
// binlogs. The cloned segment must recompute earliestTs from the new
// binlogs (cold cache), not return 0 and not return a carried-over value
// from the original's cache.
func TestGetEarliestTs_AfterCloneWithReplacedBinlogs(t *testing.T) {
	orig := NewSegmentInfo(&datapb.SegmentInfo{
		ID:              1,
		CommitTimestamp: 9999,
		Binlogs: []*datapb.FieldBinlog{
			{Binlogs: []*datapb.Binlog{{TimestampFrom: 100, TimestampTo: 200}}},
		},
	})
	// Populate earliestTs cache on the original via the commit_ts short-circuit
	// path — not strictly required, just ensuring we don't accidentally depend
	// on the original's unset cache.
	assert.Equal(t, uint64(9999), orig.GetEarliestTs())

	// Simulate compaction completion: replace binlogs + clear commit_timestamp.
	cloned := orig.Clone(func(s *SegmentInfo) {
		s.CommitTimestamp = 0
		s.Binlogs = []*datapb.FieldBinlog{
			{Binlogs: []*datapb.Binlog{{TimestampFrom: 3000, TimestampTo: 4000}}},
			{Binlogs: []*datapb.Binlog{{TimestampFrom: 2500, TimestampTo: 3500}}},
		}
	})

	// Must recompute from the NEW binlogs. Returning 0 would mean the cache
	// was miscomputed; returning 100 would mean a carried-over stale cache.
	assert.Equal(t, uint64(2500), cloned.GetEarliestTs(),
		"Clone with replaced binlogs must recompute earliestTs from new binlogs")
}

// TestGetEarliestTs_AfterShadowClone verifies that ShadowClone (which shares
// the underlying proto) still returns the correct value via the commit_ts
// short-circuit path. This is the non-compaction clone path — binlogs are
// shared, so a rescan would also be correct but unnecessary.
func TestGetEarliestTs_AfterShadowClone(t *testing.T) {
	orig := NewSegmentInfo(&datapb.SegmentInfo{
		ID:              1,
		CommitTimestamp: 7777,
		Binlogs: []*datapb.FieldBinlog{
			{Binlogs: []*datapb.Binlog{{TimestampFrom: 100, TimestampTo: 200}}},
		},
	})

	// ShadowClone shares the proto, so commit_timestamp is still non-zero
	// on the clone and the short-circuit branch returns it directly.
	cloned := orig.ShadowClone()
	assert.Equal(t, uint64(7777), cloned.GetEarliestTs())
}
