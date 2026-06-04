package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestCompactionReasonSelectorTriggersEligibleRewriteSegments(t *testing.T) {
	enableCompactionReasonSelector(t)
	ctx := context.Background()
	record := &datapb.CompactionReasonRecord{
		ReasonID:   100,
		Scope:      compactionReasonScope(1, 0, ""),
		ReasonType: datapb.CompactionReasonType_REASON_INTENT_REWRITE,
		ExpectedTS: 200,
		TailLimit:  0,
		State:      datapb.CompactionReasonState_REASON_STATE_ACTIVE,
	}
	reasonMeta := newLoadedCompactionReasonMeta(t, ctx, record)
	meta := newCompactionReasonSelectorTestMeta(reasonMeta,
		sortedReasonSegment(1, 1, 10, "ch-1", 0, 199, false),
		sortedReasonSegment(4, 1, 10, "ch-1", 0, 198, false),
		reasonSegmentWithDataTS(2, 1, 10, "ch-1", 0, 199, false),
		sortedReasonSegment(3, 1, 10, "ch-1", 0, 200, false),
	)

	events, err := newCompactionReasonSelector(meta).Trigger(ctx)

	require.NoError(t, err)
	views := events[TriggerTypeReason]
	require.Len(t, views, 2)
	require.Equal(t, int64(100), views[0].GetTriggerID())
	require.Equal(t, int64(10), views[0].GetGroupLabel().PartitionID)
	require.Equal(t, "ch-1", views[0].GetGroupLabel().Channel)
	require.Equal(t, []int64{1}, segmentIDsFromViews(views[0].GetSegmentsView()))
	require.Equal(t, int64(100), views[1].GetTriggerID())
	require.Equal(t, int64(10), views[1].GetGroupLabel().PartitionID)
	require.Equal(t, "ch-1", views[1].GetGroupLabel().Channel)
	require.Equal(t, []int64{4}, segmentIDsFromViews(views[1].GetSegmentsView()))
	require.Equal(t, datapb.CompactionReasonState_REASON_STATE_ACTIVE, reasonMeta.GetCompactionReasonRecord(100).GetState())
}

func TestCompactionReasonSelectorScansCandidatesOnceForMultipleReasons(t *testing.T) {
	enableCompactionReasonSelector(t)
	ctx := context.Background()
	record1 := &datapb.CompactionReasonRecord{
		ReasonID:   100,
		Scope:      compactionReasonScope(1, 10, "ch-1"),
		ReasonType: datapb.CompactionReasonType_REASON_INTENT_REWRITE,
		ExpectedTS: 200,
		TailLimit:  0,
		State:      datapb.CompactionReasonState_REASON_STATE_ACTIVE,
	}
	record2 := &datapb.CompactionReasonRecord{
		ReasonID:   200,
		Scope:      compactionReasonScope(1, 20, "ch-2"),
		ReasonType: datapb.CompactionReasonType_REASON_INTENT_REWRITE,
		ExpectedTS: 200,
		TailLimit:  0,
		State:      datapb.CompactionReasonState_REASON_STATE_ACTIVE,
	}
	reasonMeta := newLoadedCompactionReasonMeta(t, ctx, record1, record2)
	dcMeta := newCompactionReasonSelectorTestMeta(reasonMeta,
		sortedReasonSegment(1, 1, 10, "ch-1", 0, 199, false),
		sortedReasonSegment(2, 1, 20, "ch-2", 0, 199, false),
		sortedReasonSegment(3, 2, 10, "ch-1", 0, 199, false),
	)
	scans := 0
	patch := mockey.Mock((*meta).SelectSegments).To(func(m *meta, ctx context.Context, filters ...SegmentFilter) []*SegmentInfo {
		scans++
		return m.segments.GetSegmentsBySelector(filters...)
	}).Build()
	defer patch.UnPatch()

	events, err := newCompactionReasonSelector(dcMeta).Trigger(ctx)

	require.NoError(t, err)
	require.Equal(t, 1, scans)
	views := events[TriggerTypeReason]
	require.Len(t, views, 2)
	require.Equal(t, int64(100), views[0].GetTriggerID())
	require.Equal(t, []int64{1}, segmentIDsFromViews(views[0].GetSegmentsView()))
	require.Equal(t, int64(200), views[1].GetTriggerID())
	require.Equal(t, []int64{2}, segmentIDsFromViews(views[1].GetSegmentsView()))
}

func TestCompactionReasonSelectorCompletesRewriteReasonWhenNoMatchRemains(t *testing.T) {
	enableCompactionReasonSelector(t)
	ctx := context.Background()
	record := &datapb.CompactionReasonRecord{
		ReasonID:   100,
		Scope:      compactionReasonScope(1, 0, ""),
		ReasonType: datapb.CompactionReasonType_REASON_INTENT_REWRITE,
		ExpectedTS: 200,
		TailLimit:  0,
		State:      datapb.CompactionReasonState_REASON_STATE_ACTIVE,
	}
	reasonMeta := newLoadedCompactionReasonMeta(t, ctx, record)
	meta := newCompactionReasonSelectorTestMeta(reasonMeta,
		sortedReasonSegment(1, 1, 10, "ch-1", 201, 199, false),
		sortedReasonSegment(2, 1, 10, "ch-1", 0, 200, false),
	)

	events, err := newCompactionReasonSelector(meta).Trigger(ctx)

	require.NoError(t, err)
	require.Empty(t, events[TriggerTypeReason])
	require.Equal(t, datapb.CompactionReasonState_REASON_STATE_DONE, reasonMeta.GetCompactionReasonRecord(100).GetState())
}

func TestCompactionReasonSelectorKeepsMatchedButIneligibleRewriteActive(t *testing.T) {
	enableCompactionReasonSelector(t)
	ctx := context.Background()
	record := &datapb.CompactionReasonRecord{
		ReasonID:   100,
		Scope:      compactionReasonScope(1, 0, ""),
		ReasonType: datapb.CompactionReasonType_REASON_INTENT_REWRITE,
		ExpectedTS: 200,
		TailLimit:  0,
		State:      datapb.CompactionReasonState_REASON_STATE_ACTIVE,
	}
	reasonMeta := newLoadedCompactionReasonMeta(t, ctx, record)
	meta := newCompactionReasonSelectorTestMeta(reasonMeta,
		reasonSegmentWithDataTS(1, 1, 10, "ch-1", 0, 199, false),
	)

	events, err := newCompactionReasonSelector(meta).Trigger(ctx)

	require.NoError(t, err)
	require.Empty(t, events[TriggerTypeReason])
	require.Equal(t, datapb.CompactionReasonState_REASON_STATE_ACTIVE, reasonMeta.GetCompactionReasonRecord(100).GetState())
}

func newLoadedCompactionReasonMeta(t *testing.T, ctx context.Context, records ...*datapb.CompactionReasonRecord) *compactionReasonMeta {
	t.Helper()
	catalog, _, _, _ := newCompactionReasonTestCatalog(t, records...)
	reasonMeta, err := newCompactionReasonMeta(ctx, catalog)
	require.NoError(t, err)
	return reasonMeta
}

func enableCompactionReasonSelector(t *testing.T) {
	t.Helper()
	paramtable.Get().Save(Params.DataCoordCfg.EnableCompactionReasonRecord.Key, "true")
	t.Cleanup(func() {
		paramtable.Get().Reset(Params.DataCoordCfg.EnableCompactionReasonRecord.Key)
	})
}

func newCompactionReasonSelectorTestMeta(reasonMeta *compactionReasonMeta, segments ...*SegmentInfo) *meta {
	meta := &meta{
		segments:             NewSegmentsInfo(),
		collections:          typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		compactionReasonMeta: reasonMeta,
	}
	for _, segment := range segments {
		meta.segments.SetSegment(segment.GetID(), segment)
	}
	return meta
}

func sortedReasonSegment(id, collectionID, partitionID int64, channel string, createTS uint64, dataTS uint64, compacting bool) *SegmentInfo {
	segment := reasonSegmentWithDataTS(id, collectionID, partitionID, channel, createTS, dataTS, compacting)
	segment.IsSorted = true
	return segment
}

func segmentIDsFromViews(views []*SegmentView) []int64 {
	segmentIDs := make([]int64, 0, len(views))
	for _, view := range views {
		segmentIDs = append(segmentIDs, view.ID)
	}
	return segmentIDs
}
