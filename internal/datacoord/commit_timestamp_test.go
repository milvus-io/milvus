package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

// ── helpers ──────────────────────────────────────────────────────────────

func TestSegmentEffectiveTs_NormalSegment(t *testing.T) {
	seg := &datapb.SegmentInfo{
		StartPosition: &msgpb.MsgPosition{Timestamp: 1000},
	}
	assert.Equal(t, uint64(1000), segmentEffectiveTs(seg))
}

func TestSegmentEffectiveTs_ImportSegment(t *testing.T) {
	seg := &datapb.SegmentInfo{
		StartPosition:   &msgpb.MsgPosition{Timestamp: 1000},
		CommitTimestamp: 5000,
	}
	assert.Equal(t, uint64(5000), segmentEffectiveTs(seg))
}

func TestSegmentEffectiveTs_NilStartPosition(t *testing.T) {
	seg := &datapb.SegmentInfo{CommitTimestamp: 5000}
	assert.Equal(t, uint64(5000), segmentEffectiveTs(seg))
}

func TestSegmentEffectiveTs_BothZero(t *testing.T) {
	seg := &datapb.SegmentInfo{}
	assert.Equal(t, uint64(0), segmentEffectiveTs(seg))
}

func TestSegmentEffectiveDmlTs_NormalSegment(t *testing.T) {
	seg := &datapb.SegmentInfo{
		DmlPosition: &msgpb.MsgPosition{Timestamp: 2000},
	}
	assert.Equal(t, uint64(2000), segmentEffectiveDmlTs(seg))
}

func TestSegmentEffectiveDmlTs_ImportSegment(t *testing.T) {
	seg := &datapb.SegmentInfo{
		DmlPosition:     &msgpb.MsgPosition{Timestamp: 2000},
		CommitTimestamp: 5000,
	}
	assert.Equal(t, uint64(5000), segmentEffectiveDmlTs(seg))
}

// ── UpdateCommitTimestamp operator ──────────────────────────────────────

func TestUpdateCommitTimestamp_SetsField(t *testing.T) {
	meta, err := newMemoryMeta(t)
	assert.NoError(t, err)
	err = meta.AddSegment(context.Background(), &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Flushed}})
	assert.NoError(t, err)

	err = meta.UpdateSegmentsInfo(context.Background(), UpdateCommitTimestamp(1, 9999))
	assert.NoError(t, err)
	assert.Equal(t, uint64(9999), meta.GetSegment(context.Background(), 1).GetCommitTimestamp())
}

func TestUpdateCommitTimestamp_SegmentNotFound(t *testing.T) {
	meta, err := newMemoryMeta(t)
	assert.NoError(t, err)
	err = meta.UpdateSegmentsInfo(context.Background(), UpdateCommitTimestamp(999, 9999))
	assert.NoError(t, err)
}

// ── GenSnapshot (handler.go) ─────────────────────────────────────────────

func TestGenSnapshot_ImportSegment_ExcludedBeforeCommitTs(t *testing.T) {
	seg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:              1,
		State:           commonpb.SegmentState_Flushed,
		StartPosition:   &msgpb.MsgPosition{Timestamp: 1000},
		CommitTimestamp: 5000,
		Binlogs:         []*datapb.FieldBinlog{{FieldID: 0, Binlogs: []*datapb.Binlog{{LogID: 1}}}},
		IsImporting:     false,
	}}
	assert.False(t, segmentEffectiveTs(seg.SegmentInfo) < 3000,
		"import segment with commit_ts=5000 must NOT pass snapshot filter at snapshotTs=3000")
}

func TestGenSnapshot_ImportSegment_IncludedAfterCommitTs(t *testing.T) {
	seg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:              1,
		State:           commonpb.SegmentState_Flushed,
		StartPosition:   &msgpb.MsgPosition{Timestamp: 1000},
		CommitTimestamp: 5000,
		Binlogs:         []*datapb.FieldBinlog{{FieldID: 0, Binlogs: []*datapb.Binlog{{LogID: 1}}}},
	}}
	assert.True(t, segmentEffectiveTs(seg.SegmentInfo) < 6000,
		"import segment with commit_ts=5000 MUST pass snapshot filter at snapshotTs=6000")
}

func TestGenSnapshot_NormalSegment_UnchangedBehavior(t *testing.T) {
	seg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:            2,
		State:         commonpb.SegmentState_Flushed,
		StartPosition: &msgpb.MsgPosition{Timestamp: 3000},
	}}
	assert.True(t, segmentEffectiveTs(seg.SegmentInfo) < 4000)
	assert.False(t, segmentEffectiveTs(seg.SegmentInfo) < 2000)
}

// ── GC eligibility (garbage_collector.go) ───────────────────────────────

func TestSegmentEffectiveDmlTs_GCProtection(t *testing.T) {
	seg := &datapb.SegmentInfo{
		DmlPosition:     &msgpb.MsgPosition{Timestamp: 1000},
		CommitTimestamp: 5000,
	}
	cpTs := uint64(3000)
	assert.True(t, segmentEffectiveDmlTs(seg) > cpTs,
		"import segment with commit_ts=5000 must NOT be GC'd at checkpoint=3000")
}

func TestSegmentEffectiveDmlTs_GCAllowed_AfterCommitTs(t *testing.T) {
	seg := &datapb.SegmentInfo{
		DmlPosition:     &msgpb.MsgPosition{Timestamp: 1000},
		CommitTimestamp: 5000,
	}
	cpTs := uint64(6000)
	assert.False(t, segmentEffectiveDmlTs(seg) > cpTs,
		"import segment with commit_ts=5000 CAN be GC'd at checkpoint=6000")
}
