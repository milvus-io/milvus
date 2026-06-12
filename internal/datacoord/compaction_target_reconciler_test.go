package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestCompactionTargetReconcilerTriggersEligibleRewriteSegments(t *testing.T) {
	enableCompactionTargetReconciler(t)
	ctx := context.Background()
	record := &datapb.CompactionTarget{
		TargetID:     100,
		CollectionID: 1,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		ExpectedTS:   200,
		TailLimit:    0,
		State:        datapb.TargetState_TARGET_STATE_ACTIVE,
	}
	targetMeta := newLoadedCompactionTargetMeta(t, ctx, record)
	meta := newCompactionTargetReconcilerTestMeta(targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false),
		sortedTargetSegment(4, 1, 10, "ch-1", 0, 198, false),
		targetSegmentWithDataTS(2, 1, 10, "ch-1", 0, 199, false),
		sortedTargetSegment(3, 1, 10, "ch-1", 0, 200, false),
	)

	events, err := newCompactionTargetReconciler(meta).Trigger(ctx)

	require.NoError(t, err)
	views := events[TriggerTypeTarget]
	require.Len(t, views, 3)
	require.Equal(t, int64(100), views[0].GetTriggerID())
	require.Equal(t, int64(10), views[0].GetGroupLabel().PartitionID)
	require.Equal(t, "ch-1", views[0].GetGroupLabel().Channel)
	require.Equal(t, []int64{1}, segmentIDsFromViews(views[0].GetSegmentsView()))
	require.Equal(t, int64(100), views[1].GetTriggerID())
	require.Equal(t, int64(10), views[1].GetGroupLabel().PartitionID)
	require.Equal(t, "ch-1", views[1].GetGroupLabel().Channel)
	require.Equal(t, []int64{3}, segmentIDsFromViews(views[1].GetSegmentsView()))
	require.Equal(t, int64(100), views[2].GetTriggerID())
	require.Equal(t, int64(10), views[2].GetGroupLabel().PartitionID)
	require.Equal(t, "ch-1", views[2].GetGroupLabel().Channel)
	require.Equal(t, []int64{4}, segmentIDsFromViews(views[2].GetSegmentsView()))
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())
}

func TestCompactionTargetReconcilerScansCandidatesOnceForMultipleTargets(t *testing.T) {
	enableCompactionTargetReconciler(t)
	ctx := context.Background()
	record1 := &datapb.CompactionTarget{
		TargetID:     100,
		CollectionID: 1,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		Properties:   compactionTargetSegmentIDProperties([]int64{1}),
		ExpectedTS:   200,
		TailLimit:    0,
		State:        datapb.TargetState_TARGET_STATE_ACTIVE,
	}
	record2 := &datapb.CompactionTarget{
		TargetID:     200,
		CollectionID: 1,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		Properties:   compactionTargetSegmentIDProperties([]int64{2}),
		ExpectedTS:   200,
		TailLimit:    0,
		State:        datapb.TargetState_TARGET_STATE_ACTIVE,
	}
	targetMeta := newLoadedCompactionTargetMeta(t, ctx, record1, record2)
	dcMeta := newCompactionTargetReconcilerTestMeta(targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false),
		sortedTargetSegment(2, 1, 20, "ch-2", 0, 199, false),
		sortedTargetSegment(3, 2, 10, "ch-1", 0, 199, false),
	)
	scans := 0
	patch := mockey.Mock((*meta).SelectSegments).To(func(m *meta, ctx context.Context, filters ...SegmentFilter) []*SegmentInfo {
		scans++
		return m.segments.GetSegmentsBySelector(filters...)
	}).Build()
	defer patch.UnPatch()

	events, err := newCompactionTargetReconciler(dcMeta).Trigger(ctx)

	require.NoError(t, err)
	require.Equal(t, 1, scans)
	views := events[TriggerTypeTarget]
	require.Len(t, views, 2)
	require.Equal(t, int64(100), views[0].GetTriggerID())
	require.Equal(t, []int64{1}, segmentIDsFromViews(views[0].GetSegmentsView()))
	require.Equal(t, int64(200), views[1].GetTriggerID())
	require.Equal(t, []int64{2}, segmentIDsFromViews(views[1].GetSegmentsView()))
}

func TestCompactionTargetReconcilerInactivatesRewriteTargetWhenNoMatchRemains(t *testing.T) {
	enableCompactionTargetReconciler(t)
	ctx := context.Background()
	record := &datapb.CompactionTarget{
		TargetID:     100,
		CollectionID: 1,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		ExpectedTS:   200,
		TailLimit:    0,
		State:        datapb.TargetState_TARGET_STATE_ACTIVE,
	}
	targetMeta := newLoadedCompactionTargetMeta(t, ctx, record)
	meta := newCompactionTargetReconcilerTestMeta(targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 201, 199, false),
		sortedTargetSegment(2, 1, 10, "ch-1", 0, 201, false),
	)

	events, err := newCompactionTargetReconciler(meta).Trigger(ctx)

	require.NoError(t, err)
	require.Empty(t, events[TriggerTypeTarget])
	require.Equal(t, datapb.TargetState_TARGET_STATE_INACTIVE, targetMeta.GetCompactionTarget(100).GetState())
}

func TestCompactionTargetReconcilerIgnoresDroppedSegmentsForSatisfaction(t *testing.T) {
	enableCompactionTargetReconciler(t)
	ctx := context.Background()
	record := &datapb.CompactionTarget{
		TargetID:     100,
		CollectionID: 1,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		ExpectedTS:   200,
		TailLimit:    0,
		State:        datapb.TargetState_TARGET_STATE_ACTIVE,
	}
	targetMeta := newLoadedCompactionTargetMeta(t, ctx, record)
	droppedSource := sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false)
	droppedSource.State = commonpb.SegmentState_Dropped
	meta := newCompactionTargetReconcilerTestMeta(targetMeta,
		droppedSource,
		sortedTargetSegment(2, 1, 10, "ch-1", 201, 199, false, 1),
	)

	events, err := newCompactionTargetReconciler(meta).Trigger(ctx)

	require.NoError(t, err)
	require.Empty(t, events[TriggerTypeTarget])
	require.Equal(t, datapb.TargetState_TARGET_STATE_INACTIVE, targetMeta.GetCompactionTarget(100).GetState())
}

func TestCompactionTargetReconcilerKeepsMatchedButIneligibleRewriteActive(t *testing.T) {
	enableCompactionTargetReconciler(t)
	ctx := context.Background()
	record := &datapb.CompactionTarget{
		TargetID:     100,
		CollectionID: 1,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		ExpectedTS:   200,
		TailLimit:    0,
		State:        datapb.TargetState_TARGET_STATE_ACTIVE,
	}
	targetMeta := newLoadedCompactionTargetMeta(t, ctx, record)
	meta := newCompactionTargetReconcilerTestMeta(targetMeta,
		targetSegmentWithDataTS(1, 1, 10, "ch-1", 0, 199, false),
	)

	events, err := newCompactionTargetReconciler(meta).Trigger(ctx)

	require.NoError(t, err)
	require.Empty(t, events[TriggerTypeTarget])
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())
}

func newLoadedCompactionTargetMeta(t *testing.T, ctx context.Context, records ...*datapb.CompactionTarget) *compactionTargetMeta {
	t.Helper()
	catalog, _, _, _ := newCompactionTargetTestCatalog(t, records...)
	targetMeta, err := newCompactionTargetMeta(ctx, catalog)
	require.NoError(t, err)
	return targetMeta
}

func enableCompactionTargetReconciler(t *testing.T) {
	t.Helper()
	paramtable.Get().Save(Params.DataCoordCfg.EnableTargetBasedCompaction.Key, "true")
	t.Cleanup(func() {
		paramtable.Get().Reset(Params.DataCoordCfg.EnableTargetBasedCompaction.Key)
	})
}

func newCompactionTargetReconcilerTestMeta(targetMeta *compactionTargetMeta, segments ...*SegmentInfo) *meta {
	meta := &meta{
		segments:             NewSegmentsInfo(),
		collections:          typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		compactionTargetMeta: targetMeta,
	}
	for _, segment := range segments {
		meta.segments.SetSegment(segment.GetID(), segment)
	}
	return meta
}

func sortedTargetSegment(id, collectionID, partitionID int64, channel string, createTS uint64, dataTS uint64, compacting bool, compactionFrom ...int64) *SegmentInfo {
	segment := targetSegmentWithDataTS(id, collectionID, partitionID, channel, createTS, dataTS, compacting, compactionFrom...)
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
