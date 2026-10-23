package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// ── helpers ──────────────────────────────────────────────────────────────

func TestSegmentEffectiveTs_NormalSegment(t *testing.T) {
	seg := &datapb.SegmentInfo{
		StartPosition: &msgpb.MsgPosition{Timestamp: 1000},
	}
	assert.Equal(t, uint64(1000), meta.SegmentEffectiveTs(seg))
}

func TestSegmentEffectiveTs_ImportSegment(t *testing.T) {
	seg := &datapb.SegmentInfo{
		StartPosition:   &msgpb.MsgPosition{Timestamp: 1000},
		CommitTimestamp: 5000,
	}
	assert.Equal(t, uint64(5000), meta.SegmentEffectiveTs(seg))
}

func TestSegmentEffectiveTs_NilStartPosition(t *testing.T) {
	seg := &datapb.SegmentInfo{CommitTimestamp: 5000}
	assert.Equal(t, uint64(5000), meta.SegmentEffectiveTs(seg))
}

func TestSegmentEffectiveTs_BothZero(t *testing.T) {
	seg := &datapb.SegmentInfo{}
	assert.Equal(t, uint64(0), meta.SegmentEffectiveTs(seg))
}

func TestSegmentEffectiveDmlTs_NormalSegment(t *testing.T) {
	seg := &datapb.SegmentInfo{
		DmlPosition: &msgpb.MsgPosition{Timestamp: 2000},
	}
	assert.Equal(t, uint64(2000), meta.SegmentEffectiveDmlTs(seg))
}

func TestSegmentEffectiveDmlTs_ImportSegment(t *testing.T) {
	seg := &datapb.SegmentInfo{
		DmlPosition:     &msgpb.MsgPosition{Timestamp: 2000},
		CommitTimestamp: 5000,
	}
	assert.Equal(t, uint64(5000), meta.SegmentEffectiveDmlTs(seg))
}

// ── UpdateCommitTimestamp operator ──────────────────────────────────────

func TestUpdateCommitTimestamp_SetsField(t *testing.T) {
	mt, err := meta.NewMemoryMeta(t)
	assert.NoError(t, err)
	err = mt.AddSegment(context.Background(), &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Flushed}})
	assert.NoError(t, err)

	err = mt.UpdateSegmentsInfo(context.Background(), meta.UpdateCommitTimestamp(1, 9999))
	assert.NoError(t, err)
	assert.Equal(t, uint64(9999), mt.GetSegment(context.Background(), 1).GetCommitTimestamp())
}

func TestUpdateCommitTimestamp_SegmentNotFound(t *testing.T) {
	mt, err := meta.NewMemoryMeta(t)
	assert.NoError(t, err)
	err = mt.UpdateSegmentsInfo(context.Background(), meta.UpdateCommitTimestamp(999, 9999))
	assert.NoError(t, err)
}

// ── UpdateCommitTimestamp input validation ──────────────────────────────
// Invariant: commit_ts (when non-zero) must be >= the accepted segment
// timestamp bound (Statistics.TimestampTo for V3, binlog fallback for V2).
// Reject violations at the entry point so C++ segcore never sees an invalid
// commit_ts that would silently lower row timestamps during load-time overwrite.

// segWithBinlogs builds a Flushed SegmentInfo carrying a single binlog whose
// TimestampTo == maxTsTo, used to exercise the max(binlog.TimestampTo) check.
func segWithBinlogs(id int64, maxTsTo uint64) *meta.SegmentInfo {
	return &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:    id,
		State: commonpb.SegmentState_Flushed,
		Binlogs: []*datapb.FieldBinlog{{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{{LogID: 1, TimestampFrom: 100, TimestampTo: maxTsTo}},
		}},
	}}
}

func TestUpdateCommitTimestamp_AcceptAboveMaxTimestampTo(t *testing.T) {
	mt, err := meta.NewMemoryMeta(t)
	assert.NoError(t, err)
	assert.NoError(t, mt.AddSegment(context.Background(), segWithBinlogs(1, 5000)))

	err = mt.UpdateSegmentsInfo(context.Background(), meta.UpdateCommitTimestamp(1, 9999))
	assert.NoError(t, err)
	assert.Equal(t, uint64(9999), mt.GetSegment(context.Background(), 1).GetCommitTimestamp())
}

func TestUpdateCommitTimestamp_AcceptEqualToMaxTimestampTo(t *testing.T) {
	// Boundary: commit_ts == max(binlog.TimestampTo) is valid.
	mt, err := meta.NewMemoryMeta(t)
	assert.NoError(t, err)
	assert.NoError(t, mt.AddSegment(context.Background(), segWithBinlogs(1, 5000)))

	err = mt.UpdateSegmentsInfo(context.Background(), meta.UpdateCommitTimestamp(1, 5000))
	assert.NoError(t, err)
	assert.Equal(t, uint64(5000), mt.GetSegment(context.Background(), 1).GetCommitTimestamp())
}

func TestUpdateCommitTimestamp_RejectBelowMaxTimestampTo(t *testing.T) {
	// Violation: commit_ts < max(binlog.TimestampTo). The operator must leave
	// commit_timestamp unchanged so downstream code never observes a
	// commit_ts that would lower row timestamps.
	mt, err := meta.NewMemoryMeta(t)
	assert.NoError(t, err)
	assert.NoError(t, mt.AddSegment(context.Background(), segWithBinlogs(1, 5000)))

	err = mt.UpdateSegmentsInfo(context.Background(), meta.UpdateCommitTimestamp(1, 3000))
	assert.ErrorIs(t, err, merr.ErrImportSysFailed)
	assert.Equal(t, uint64(0), mt.GetSegment(context.Background(), 1).GetCommitTimestamp(),
		"rejected update must leave commit_timestamp unchanged")
}

func TestUpdateCommitTimestamp_RejectBelowPersistedStatsTimestampTo(t *testing.T) {
	mt, err := meta.NewMemoryMeta(t)
	assert.NoError(t, err)
	assert.NoError(t, mt.AddSegment(context.Background(), &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:           1,
		State:        commonpb.SegmentState_Flushed,
		ManifestPath: "files/insert_log/1/2/1/manifest",
		Stats:        &datapb.Statistics{TimestampTo: 5000},
	}}))

	err = mt.UpdateSegmentsInfo(context.Background(), meta.UpdateCommitTimestamp(1, 3000))
	assert.ErrorIs(t, err, merr.ErrImportSysFailed)
	assert.Equal(t, uint64(0), mt.GetSegment(context.Background(), 1).GetCommitTimestamp())
}

func TestUpdateCommitTimestamp_UsesLargestPersistedBound(t *testing.T) {
	mt, err := meta.NewMemoryMeta(t)
	assert.NoError(t, err)
	seg := segWithBinlogs(1, 4000)
	seg.Stats = &datapb.Statistics{TimestampTo: 5000}
	assert.NoError(t, mt.AddSegment(context.Background(), seg))

	err = mt.UpdateSegmentsInfo(context.Background(), meta.UpdateCommitTimestamp(1, 4500))
	assert.ErrorIs(t, err, merr.ErrImportSysFailed)
	assert.Equal(t, uint64(0), mt.GetSegment(context.Background(), 1).GetCommitTimestamp())
}

func TestUpdateCommitTimestamp_AcceptZeroReset(t *testing.T) {
	// The ts == 0 reset path is used by compaction completion and must always
	// pass, regardless of the binlog TimestampTo values on the segment.
	mt, err := meta.NewMemoryMeta(t)
	assert.NoError(t, err)
	seg := segWithBinlogs(1, 5000)
	seg.CommitTimestamp = 9999
	assert.NoError(t, mt.AddSegment(context.Background(), seg))

	err = mt.UpdateSegmentsInfo(context.Background(), meta.UpdateCommitTimestamp(1, 0))
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), mt.GetSegment(context.Background(), 1).GetCommitTimestamp())
}

func TestUpdateCommitTimestamp_AcceptWhenNoBinlogs(t *testing.T) {
	// Pre-binlog state (e.g. just before first flush): max(binlog.TimestampTo)
	// is 0, so any commit_ts passes the >= check.
	mt, err := meta.NewMemoryMeta(t)
	assert.NoError(t, err)
	assert.NoError(t, mt.AddSegment(context.Background(), &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 1, State: commonpb.SegmentState_Flushed,
	}}))

	err = mt.UpdateSegmentsInfo(context.Background(), meta.UpdateCommitTimestamp(1, 7777))
	assert.NoError(t, err)
	assert.Equal(t, uint64(7777), mt.GetSegment(context.Background(), 1).GetCommitTimestamp())
}

// ── EffectiveTimestamp / helper boundary values ─────────────────────────
// Documents the expected behavior at the commit_ts == raw_ts boundary.

func TestEffectiveTimestamp_CommitEqualsRawTs(t *testing.T) {
	// At the boundary, either value is correct; the helper returns rawTs
	// because the `commitTs > rawTs` branch does not trigger.
	assert.Equal(t, uint64(5000), tsoutil.EffectiveTimestamp(5000, 5000))
}

func TestSegmentEffectiveTs_CommitEqualsStartPosition(t *testing.T) {
	// Boundary: commit_ts == start_position.Timestamp. Helper returns
	// commit_ts (non-zero branch wins) — behavior-wise identical to rawTs.
	seg := &datapb.SegmentInfo{
		StartPosition:   &msgpb.MsgPosition{Timestamp: 5000},
		CommitTimestamp: 5000,
	}
	assert.Equal(t, uint64(5000), meta.SegmentEffectiveTs(seg))
}

// ── GenSnapshot (handler.go) ─────────────────────────────────────────────

func TestGenSnapshot_ImportSegment_ExcludedBeforeCommitTs(t *testing.T) {
	seg := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:              1,
		State:           commonpb.SegmentState_Flushed,
		StartPosition:   &msgpb.MsgPosition{Timestamp: 1000},
		CommitTimestamp: 5000,
		Binlogs:         []*datapb.FieldBinlog{{FieldID: 0, Binlogs: []*datapb.Binlog{{LogID: 1}}}},
		IsImporting:     false,
	}}
	assert.False(t, meta.SegmentEffectiveTs(seg.SegmentInfo) < 3000,
		"import segment with commit_ts=5000 must NOT pass snapshot filter at snapshotTs=3000")
}

func TestGenSnapshot_ImportSegment_IncludedAfterCommitTs(t *testing.T) {
	seg := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:              1,
		State:           commonpb.SegmentState_Flushed,
		StartPosition:   &msgpb.MsgPosition{Timestamp: 1000},
		CommitTimestamp: 5000,
		Binlogs:         []*datapb.FieldBinlog{{FieldID: 0, Binlogs: []*datapb.Binlog{{LogID: 1}}}},
	}}
	assert.True(t, meta.SegmentEffectiveTs(seg.SegmentInfo) < 6000,
		"import segment with commit_ts=5000 MUST pass snapshot filter at snapshotTs=6000")
}

func TestGenSnapshot_NormalSegment_UnchangedBehavior(t *testing.T) {
	seg := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:            2,
		State:         commonpb.SegmentState_Flushed,
		StartPosition: &msgpb.MsgPosition{Timestamp: 3000},
	}}
	assert.True(t, meta.SegmentEffectiveTs(seg.SegmentInfo) < 4000)
	assert.False(t, meta.SegmentEffectiveTs(seg.SegmentInfo) < 2000)
}

// ── GC eligibility (garbage_collector.go) ───────────────────────────────

func TestSegmentEffectiveDmlTs_GCProtection(t *testing.T) {
	seg := &datapb.SegmentInfo{
		DmlPosition:     &msgpb.MsgPosition{Timestamp: 1000},
		CommitTimestamp: 5000,
	}
	cpTs := uint64(3000)
	assert.True(t, meta.SegmentEffectiveDmlTs(seg) > cpTs,
		"import segment with commit_ts=5000 must NOT be GC'd at checkpoint=3000")
}

func TestSegmentEffectiveDmlTs_GCAllowed_AfterCommitTs(t *testing.T) {
	seg := &datapb.SegmentInfo{
		DmlPosition:     &msgpb.MsgPosition{Timestamp: 1000},
		CommitTimestamp: 5000,
	}
	cpTs := uint64(6000)
	assert.False(t, meta.SegmentEffectiveDmlTs(seg) > cpTs,
		"import segment with commit_ts=5000 CAN be GC'd at checkpoint=6000")
}
