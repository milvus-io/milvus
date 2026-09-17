package datacoord

import (
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestSegmentsInfo_MetaStore_GetSegment(t *testing.T) {
	store := metacache.NewMetaStore(nil)
	info := NewSegmentsInfo(store)

	seg := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID: 1, CollectionID: 100, InsertChannel: "ch-0",
			State: commonpb.SegmentState_Flushed, NumOfRows: 1000,
		},
		isCompacting:  true,
		lastFlushTime: time.Now(),
	}
	info.SetSegment(1, seg)

	got := info.GetSegment(1)
	assert.NotNil(t, got)
	assert.Equal(t, int64(1), got.GetID())
	assert.Equal(t, int64(1000), got.GetNumOfRows())
	assert.True(t, got.isCompacting)

	// Verify proto lives in MetaStore, not in a local map
	storeSeg, ok := store.GetSegment(1)
	assert.True(t, ok)
	assert.Equal(t, int64(1), storeSeg.GetID())

	// Verify DC-only field mutations don't touch the store
	info.SetIsCompacting(1, false)
	got2 := info.GetSegment(1)
	assert.False(t, got2.isCompacting)

	// Verify proto field mutations go through the store
	info.SetRowCount(1, 2000)
	got3 := info.GetSegment(1)
	assert.Equal(t, int64(2000), got3.GetNumOfRows())
	storeSeg3, _ := store.GetSegment(1)
	assert.Equal(t, int64(2000), storeSeg3.GetNumOfRows())
}

func TestSegmentsInfo_MetaStore_GetCandidates(t *testing.T) {
	store := metacache.NewMetaStore(nil)
	info := NewSegmentsInfo(store)

	info.SetSegment(1, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 1, CollectionID: 100, InsertChannel: "ch-0",
		State: commonpb.SegmentState_Flushed,
	}})
	info.SetSegment(2, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 2, CollectionID: 100, InsertChannel: "ch-1",
		State: commonpb.SegmentState_Growing,
	}})
	info.SetSegment(3, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 3, CollectionID: 200, InsertChannel: "ch-0",
		State: commonpb.SegmentState_Flushed,
	}})

	// By collection
	result := info.GetSegmentsBySelector(CollectionFilter(100))
	assert.Len(t, result, 2)

	// By channel
	result = info.GetSegmentsBySelector(ChannelFilter("ch-0"))
	assert.Len(t, result, 2)

	// By collection + channel
	result = info.GetSegmentsBySelector(CollectionFilter(100), ChannelFilter("ch-0"))
	assert.Len(t, result, 1)
}

func TestCompactionTo(t *testing.T) {
	t.Run("mix_2_to_1", func(t *testing.T) {
		segments := NewSegmentsInfo(metacache.NewMetaStore(nil))
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
		segments := NewSegmentsInfo(metacache.NewMetaStore(nil))
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
	// NewSegmentInfo populates Stats from the binlog arrays so getSegmentSize
	// reads it without falling back to iteration.
	segment := NewSegmentInfo(&datapb.SegmentInfo{
		Binlogs: []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{LogID: 1, MemorySize: 1},
				},
			},
		},
		Statslogs: []*datapb.FieldBinlog{
			{
				Binlogs: []*datapb.Binlog{
					{LogID: 1, MemorySize: 1},
				},
			},
		},
		Deltalogs: []*datapb.FieldBinlog{
			{
				Binlogs: []*datapb.Binlog{
					{LogID: 1, MemorySize: 1},
				},
			},
		},
	})

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
// compaction completion conceptually takes: a segment with commit_ts != 0 is
// Clone()d with an option that sets CommitTimestamp=0 and replaces the
// binlogs. The cloned segment must recompute earliestTs from the new
// binlogs, not return 0 and not return a carried-over value from the
// original.
//
// Stats is array-derived; the opt that replaces Binlogs must also refresh
// Stats so reads see the new arrays. Production paths (AddBinlogsOperator,
// UpdateBinlogsFromSaveBinlogPathsOperator) reset Stats inside the
// write-locked operator chain; ad-hoc Clone callers are on the hook to do
// the same eagerly, since EnsureStats no longer writes back lazily (that
// would race with concurrent RLock readers).
func TestGetEarliestTs_AfterCloneWithReplacedBinlogs(t *testing.T) {
	orig := NewSegmentInfo(&datapb.SegmentInfo{
		ID:              1,
		CommitTimestamp: 9999,
		Binlogs: []*datapb.FieldBinlog{
			{Binlogs: []*datapb.Binlog{{TimestampFrom: 100, TimestampTo: 200}}},
		},
	})
	// commit_ts short-circuit returns 9999 regardless of binlogs.
	assert.Equal(t, uint64(9999), orig.GetEarliestTs())

	// Simulate compaction completion: replace binlogs + clear commit_timestamp.
	cloned := orig.Clone(func(s *SegmentInfo) {
		s.CommitTimestamp = 0
		s.Binlogs = []*datapb.FieldBinlog{
			{Binlogs: []*datapb.Binlog{{TimestampFrom: 3000, TimestampTo: 4000}}},
			{Binlogs: []*datapb.Binlog{{TimestampFrom: 2500, TimestampTo: 3500}}},
		}
		s.Stats = storage.BuildStatsFromFieldBinlogs(s.GetBinlogs(), s.GetStatslogs(), s.GetBm25Statslogs(), s.GetDeltalogs())
	})

	// Stats now reflects the new arrays: min(TimestampFrom) → 2500.
	assert.Equal(t, uint64(2500), cloned.GetEarliestTs(),
		"Clone with replaced binlogs + Stats refresh must recompute earliestTs from new binlogs")
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

// NewSegmentInfo backfills Stats from the binlog arrays so legacy segments
// persisted before Statistics existed don't return nil from GetStats(). This
// is what reloadFromKV depends on for V2 segments whose etcd record predates
// the proto change.
func TestNewSegmentInfo_BackfillsStatsForLegacySegment(t *testing.T) {
	mkBinlog := func(logID, entries, mem int64, tsFrom, tsTo uint64) *datapb.FieldBinlog {
		return &datapb.FieldBinlog{Binlogs: []*datapb.Binlog{{LogID: logID, EntriesNum: entries, MemorySize: mem, TimestampFrom: tsFrom, TimestampTo: tsTo}}}
	}
	insertBinlog := mkBinlog(1, 50, 2048, 100, 500)
	insertBinlog.FieldID = 10
	insertBinlog.ChildFields = []int64{100, 101}

	// Persisted segment record has no Stats field, mirroring a V2 segment
	// loaded after upgrade.
	seg := NewSegmentInfo(&datapb.SegmentInfo{
		ID:        21,
		State:     commonpb.SegmentState_Flushed,
		NumOfRows: 50,
		Binlogs:   []*datapb.FieldBinlog{insertBinlog},
		Statslogs: []*datapb.FieldBinlog{mkBinlog(2, 0, 64, 0, 0)},
		Deltalogs: []*datapb.FieldBinlog{mkBinlog(3, 7, 128, 200, 400)},
	})

	stats := seg.GetStats()
	require.NotNil(t, stats, "NewSegmentInfo must backfill Stats from arrays")
	assert.EqualValues(t, 2048, stats.GetInsertBinlogSize())
	assert.EqualValues(t, 64, stats.GetStatsBinlogSize())
	assert.EqualValues(t, 128, stats.GetDeltaBinlogSize())
	assert.EqualValues(t, 7, stats.GetDeleteNumRows())
	assert.EqualValues(t, 1, stats.GetInsertBinlogCount())
	assert.EqualValues(t, 1, stats.GetDeltaBinlogCount())
	assert.EqualValues(t, 100, stats.GetTimestampFrom())
	assert.EqualValues(t, 500, stats.GetTimestampTo())
	assert.EqualValues(t, 200, stats.GetDeltaTimestampFrom())
	assert.EqualValues(t, 400, stats.GetDeltaTimestampTo())
	assert.Equal(t, []*datapb.ColumnGroupStatistics{{
		GroupId: 10, FieldIds: []int64{100, 101}, MemorySize: 2048,
	}}, stats.GetLoadResource().GetColumnGroups())
}

// NewSegmentInfo must respect an explicitly-supplied Stats field — used by
// the V3 flush path where the writer ships Statistics that the arrays alone
// cannot reconstruct (stats_binlog_size lives in the manifest).
func TestNewSegmentInfo_PreservesExplicitStats(t *testing.T) {
	explicit := &datapb.Statistics{
		InsertBinlogSize: 9999,
		StatsBinlogSize:  4444,
	}
	seg := NewSegmentInfo(&datapb.SegmentInfo{
		ID:           22,
		ManifestPath: "manifest://foo",
		Stats:        explicit,
	})
	assert.Same(t, explicit, seg.GetStats(), "supplied Stats must not be replaced")
}

// newSegmentsInfoWithSegments builds a store-backed SegmentsInfo pre-populated
// with the given segments, for use in test fixtures that used to build a
// SegmentsInfo struct literal directly (e.g. `&SegmentsInfo{segments: ...}`)
// back when the standalone in-memory map existed.
// newEmptyTestMeta returns a meta whose segments and metaStore share one
// empty MetaStore.
func newEmptyTestMeta() *meta {
	ms := metacache.NewMetaStore(nil)
	return &meta{metaStore: ms, segments: NewSegmentsInfo(ms)}
}

func newSegmentsInfoWithSegments(segs map[int64]*SegmentInfo) *SegmentsInfo {
	si := NewSegmentsInfo(metacache.NewMetaStore(nil))
	for id, seg := range segs {
		si.SetSegment(id, seg)
	}
	return si
}

// TestRebuildFromStore covers the recovery path: segments loaded straight into
// the shared store bypass SetSegment, so the compaction relations and the
// per-segment transient state have to be seeded explicitly. Without it
// SetIsCompacting silently does nothing and GetCompactionTo reports no
// children, both of which only show up after a restart.
func TestRebuildFromStore(t *testing.T) {
	store := metacache.NewMetaStore(nil)
	store.LoadSegments([]*datapb.SegmentInfo{
		{ID: 1, CollectionID: 100, InsertChannel: "ch-0", State: commonpb.SegmentState_Dropped},
		{ID: 2, CollectionID: 100, InsertChannel: "ch-0", State: commonpb.SegmentState_Flushed, CompactionFrom: []int64{1}},
		{ID: 3, CollectionID: 100, InsertChannel: "ch-0", State: commonpb.SegmentState_Growing},
	})
	segments := NewSegmentsInfo(store)

	// Before the rebuild the DC-side state is empty.
	compactTos, ok := segments.GetCompactionTo(1)
	assert.True(t, ok)
	assert.Empty(t, compactTos)

	segments.RebuildFromStore()

	compactTos, ok = segments.GetCompactionTo(1)
	assert.True(t, ok)
	require.Len(t, compactTos, 1)
	assert.EqualValues(t, 2, compactTos[0].GetID())

	segments.SetIsCompacting(2, true)
	assert.True(t, segments.GetSegment(2).isCompacting)

	// A recovered growing segment starts out flushable and idle, matching
	// NewSegmentInfo.
	growing := segments.GetSegment(3)
	assert.False(t, growing.lastFlushTime.IsZero())
	assert.True(t, growing.lastWrittenTime.IsZero())
}

// TestSetIsCompactingWithoutRebuild asserts the setters themselves recover
// from a missing entry, so a segment that reaches memory by some other path
// than SetSegment still tracks its compaction state.
func TestSetIsCompactingWithoutRebuild(t *testing.T) {
	store := metacache.NewMetaStore(nil)
	store.LoadSegments([]*datapb.SegmentInfo{{ID: 1, CollectionID: 100, State: commonpb.SegmentState_Flushed}})
	segments := NewSegmentsInfo(store)

	segments.SetIsCompacting(1, true)
	assert.True(t, segments.GetSegment(1).isCompacting)

	// An unknown segment is still a no-op rather than a panic.
	segments.SetIsCompacting(42, true)
	assert.Nil(t, segments.GetSegment(42))
}

// TestSetSegmentNormalizesRowCount pins the invariant that makes the shared
// store trustworthy for row counts: the catalog reconciles a V1/V2 segment's
// NumOfRows against its insert binlogs only on the copy it persists, so the
// in-memory segment has to be corrected on its way in. QueryCoord reads that
// in-memory copy through the MetaView.
func TestSetSegmentNormalizesRowCount(t *testing.T) {
	tests := []struct {
		name    string
		segment *datapb.SegmentInfo
		want    int64
	}{
		{
			name: "v2 stale row count is corrected from binlogs",
			segment: &datapb.SegmentInfo{
				ID: 1, NumOfRows: 0, StorageVersion: storage.StorageV2,
				Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{
					{EntriesNum: 20}, {EntriesNum: 10},
				}}},
			},
			want: 30,
		},
		{
			name: "v2 consistent row count is left alone",
			segment: &datapb.SegmentInfo{
				ID: 2, NumOfRows: 30, StorageVersion: storage.StorageV2,
				Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{EntriesNum: 30}}}},
			},
			want: 30,
		},
		{
			name: "no insert binlogs cannot answer, so the count stands",
			segment: &datapb.SegmentInfo{
				ID: 3, NumOfRows: 42, StorageVersion: storage.StorageV2,
			},
			want: 42,
		},
		{
			name: "v3 keeps the writer-reported count",
			segment: &datapb.SegmentInfo{
				ID: 4, NumOfRows: 70, StorageVersion: storage.StorageV3,
				Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{EntriesNum: 5}}}},
			},
			want: 70,
		},
		{
			name: "manifest-backed segment keeps its count",
			segment: &datapb.SegmentInfo{
				ID: 5, NumOfRows: 70, ManifestPath: "files/insert_log/1/2/5",
				Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{EntriesNum: 5}}}},
			},
			want: 70,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := metacache.NewMetaStore(nil)
			segments := NewSegmentsInfo(store)
			segments.SetSegment(test.segment.GetID(), &SegmentInfo{SegmentInfo: test.segment})

			stored, ok := store.GetSegment(test.segment.GetID())
			require.True(t, ok)
			assert.Equal(t, test.want, stored.GetNumOfRows())
		})
	}
}
