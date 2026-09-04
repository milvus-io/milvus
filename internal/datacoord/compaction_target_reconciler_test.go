package datacoord

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
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

	events, err := newCompactionTargetReconcilerForTest(meta).Trigger(ctx)

	require.NoError(t, err)
	views := events[TriggerTypeSingle]
	require.Len(t, views, 3)
	segmentIDs := make([]int64, 0, len(views))
	for _, view := range views {
		require.Equal(t, int64(100), view.GetTriggerID())
		require.Equal(t, int64(10), view.GetGroupLabel().PartitionID)
		require.Equal(t, "ch-1", view.GetGroupLabel().Channel)
		segmentIDs = append(segmentIDs, segmentIDsFromViews(view.GetSegmentsView())...)
	}
	require.ElementsMatch(t, []int64{1, 3, 4}, segmentIDs)
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())
}

func TestCompactionTargetReconcilerSatisfiedTargetEmitsNoWork(t *testing.T) {
	enableCompactionTargetReconciler(t)
	ctx := context.Background()
	record := &datapb.CompactionTarget{
		TargetID:     100,
		CollectionID: 1,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		ExpectedTS:   200,
		TailLimit:    1,
		State:        datapb.TargetState_TARGET_STATE_ACTIVE,
	}
	targetMeta := newLoadedCompactionTargetMeta(t, ctx, record)
	meta := newCompactionTargetReconcilerTestMeta(targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false),
	)

	events, err := newCompactionTargetReconcilerForTest(meta).Trigger(ctx)

	require.NoError(t, err)
	require.Empty(t, events[TriggerTypeSingle])
	require.Equal(t, datapb.TargetState_TARGET_STATE_INACTIVE, targetMeta.GetCompactionTarget(100).GetState())
}

func TestCompactionTargetReconcilerReconcilesMultipleTargetsIndependently(t *testing.T) {
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
	events, err := newCompactionTargetReconcilerForTest(dcMeta).Trigger(ctx)

	require.NoError(t, err)
	views := events[TriggerTypeSingle]
	require.Len(t, views, 2)
	segmentsByTarget := make(map[int64][]int64, len(views))
	for _, view := range views {
		segmentsByTarget[view.GetTriggerID()] = segmentIDsFromViews(view.GetSegmentsView())
	}
	require.Equal(t, map[int64][]int64{
		100: {1},
		200: {2},
	}, segmentsByTarget)
}

func TestCompactionTargetReconcilerLimitsEventsWithoutSkippingSatisfaction(t *testing.T) {
	enableCompactionTargetReconciler(t)
	paramtable.Get().Save(Params.DataCoordCfg.TargetCompactionMaxEvents.Key, "1")
	t.Cleanup(func() {
		paramtable.Get().Reset(Params.DataCoordCfg.TargetCompactionMaxEvents.Key)
	})

	ctx := context.Background()
	activeRecord := &datapb.CompactionTarget{
		TargetID:     100,
		CollectionID: 1,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		Properties:   compactionTargetSegmentIDProperties([]int64{1, 2}),
		ExpectedTS:   200,
		TailLimit:    0,
		State:        datapb.TargetState_TARGET_STATE_ACTIVE,
	}
	satisfiedRecord := &datapb.CompactionTarget{
		TargetID:     200,
		CollectionID: 1,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		Properties:   compactionTargetSegmentIDProperties([]int64{3}),
		ExpectedTS:   200,
		TailLimit:    0,
		State:        datapb.TargetState_TARGET_STATE_ACTIVE,
	}
	targetMeta := newLoadedCompactionTargetMeta(t, ctx, activeRecord, satisfiedRecord)
	meta := newCompactionTargetReconcilerTestMeta(targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false),
		sortedTargetSegment(2, 1, 10, "ch-1", 0, 199, false),
	)
	reconciler := newCompactionTargetReconcilerForTest(meta)

	events, err := reconciler.Reconcile(ctx)

	require.NoError(t, err)
	require.Len(t, events[TriggerTypeSingle], 1)
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())
	require.Equal(t, datapb.TargetState_TARGET_STATE_INACTIVE, targetMeta.GetCompactionTarget(200).GetState())

	paramtable.Get().Save(Params.DataCoordCfg.TargetCompactionMaxEvents.Key, "2")
	events, err = reconciler.Reconcile(ctx)

	require.NoError(t, err)
	require.Len(t, events[TriggerTypeSingle], 2)
}

func TestCompactionTargetReconcilerAppliesTargetCollectionScope(t *testing.T) {
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
		sortedTargetSegment(2, 2, 10, "ch-1", 0, 199, false),
	)

	events, err := newCompactionTargetReconcilerForTest(meta).Trigger(ctx)

	require.NoError(t, err)
	require.Len(t, events[TriggerTypeSingle], 1)
	require.Equal(t, []int64{1}, segmentIDsFromViews(events[TriggerTypeSingle][0].GetSegmentsView()))
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())
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

	events, err := newCompactionTargetReconcilerForTest(meta).Trigger(ctx)

	require.NoError(t, err)
	require.Empty(t, events[TriggerTypeSingle])
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

	events, err := newCompactionTargetReconcilerForTest(meta).Trigger(ctx)

	require.NoError(t, err)
	require.Empty(t, events[TriggerTypeSingle])
	require.Equal(t, datapb.TargetState_TARGET_STATE_INACTIVE, targetMeta.GetCompactionTarget(100).GetState())
}

func TestCompactionTargetReconcilerOutsideManualMatchDomainDoesNotHoldTargetActive(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*meta, *SegmentInfo)
	}{
		{
			name: "flushing",
			mutate: func(_ *meta, segment *SegmentInfo) {
				segment.State = commonpb.SegmentState_Flushing
			},
		},
		{
			name: "importing",
			mutate: func(_ *meta, segment *SegmentInfo) {
				segment.IsImporting = true
			},
		},
		{
			name: "L0",
			mutate: func(_ *meta, segment *SegmentInfo) {
				segment.Level = datapb.SegmentLevel_L0
			},
		},
		{
			name: "L2",
			mutate: func(_ *meta, segment *SegmentInfo) {
				segment.Level = datapb.SegmentLevel_L2
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
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
			segment := sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false)
			meta := newCompactionTargetReconcilerTestMeta(targetMeta, segment)
			test.mutate(meta, segment)

			events, err := newCompactionTargetReconcilerForTest(meta).Trigger(ctx)

			require.NoError(t, err)
			require.Empty(t, events[TriggerTypeSingle])
			require.Equal(t, datapb.TargetState_TARGET_STATE_INACTIVE, targetMeta.GetCompactionTarget(100).GetState())
		})
	}
}

func TestCompactionTargetReconcilerWaitsForSnapshotCreatedAfterTarget(t *testing.T) {
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
	segment := sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false)
	meta := newCompactionTargetReconcilerTestMeta(targetMeta, segment)
	meta.snapshotMeta = &snapshotMeta{
		segmentProtectionUntil: map[int64]uint64{
			segment.GetID(): uint64(time.Now().Add(time.Hour).Unix()),
		},
	}

	events, err := newCompactionTargetReconcilerForTest(meta).Trigger(ctx)

	require.NoError(t, err)
	require.Empty(t, events[TriggerTypeSingle])
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())

	delete(meta.snapshotMeta.segmentProtectionUntil, segment.GetID())
	events, err = newCompactionTargetReconcilerForTest(meta).Trigger(ctx)

	require.NoError(t, err)
	require.Len(t, events[TriggerTypeSingle], 1)
	require.Equal(t, []int64{1}, segmentIDsFromViews(events[TriggerTypeSingle][0].GetSegmentsView()))
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())
}

func TestCompactionTargetReconcilerKeepsTemporarilyBlockedMatchActive(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*SegmentInfo)
	}{
		{
			name: "compacting",
			mutate: func(segment *SegmentInfo) {
				segment.isCompacting = true
			},
		},
		{
			name: "invisible",
			mutate: func(segment *SegmentInfo) {
				segment.IsInvisible = true
			},
		},
		{
			name: "unsorted",
			mutate: func(segment *SegmentInfo) {
				segment.IsSorted = false
			},
		},
		{
			name: "invisible and unsorted",
			mutate: func(segment *SegmentInfo) {
				segment.IsInvisible = true
				segment.IsSorted = false
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
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
			segment := sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false)
			test.mutate(segment)
			meta := newCompactionTargetReconcilerTestMeta(targetMeta, segment)

			events, err := newCompactionTargetReconcilerForTest(meta).Trigger(ctx)

			require.NoError(t, err)
			require.Empty(t, events[TriggerTypeSingle])
			require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())
		})
	}
}

func TestCompactionTargetReconcilerPausesAndResumesSnapshotBlockedCollection(t *testing.T) {
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
	)
	meta.snapshotMeta = createTestSnapshotMetaLoaded(t)
	meta.snapshotMeta.SetSnapshotPending(1)

	events, err := newCompactionTargetReconcilerForTest(meta).Trigger(ctx)

	require.NoError(t, err)
	require.Empty(t, events[TriggerTypeSingle])
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())

	meta.snapshotMeta.ClearSnapshotPending(1)
	events, err = newCompactionTargetReconcilerForTest(meta).Trigger(ctx)

	require.NoError(t, err)
	require.Len(t, events[TriggerTypeSingle], 1)
	require.Equal(t, []int64{1}, segmentIDsFromViews(events[TriggerTypeSingle][0].GetSegmentsView()))
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())
}

func TestCompactionTargetReconcilerUsesManualIndexReadinessFilter(t *testing.T) {
	enableCompactionTargetReconciler(t)
	paramtable.Get().Save(Params.DataCoordCfg.IndexBasedCompaction.Key, "true")
	t.Cleanup(func() {
		paramtable.Get().Reset(Params.DataCoordCfg.IndexBasedCompaction.Key)
	})

	const (
		collectionID  = int64(1)
		vectorFieldID = int64(100)
		indexID       = int64(1000)
	)
	tests := []struct {
		name               string
		finishedSegmentIDs []int64
		wantSegmentIDs     []int64
	}{
		{
			name:               "emits only index-ready matches",
			finishedSegmentIDs: []int64{1},
			wantSegmentIDs:     []int64{1},
		},
		{
			name: "keeps target active when every match is index-rejected",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			record := &datapb.CompactionTarget{
				TargetID:     100,
				CollectionID: collectionID,
				Intent:       datapb.TargetIntent_INTENT_REWRITE,
				ExpectedTS:   200,
				TailLimit:    0,
				State:        datapb.TargetState_TARGET_STATE_ACTIVE,
			}
			targetMeta := newLoadedCompactionTargetMeta(t, ctx, record)
			meta := newCompactionTargetReconcilerTestMeta(targetMeta,
				sortedTargetSegment(1, collectionID, 10, "ch-1", 0, 199, false),
				sortedTargetSegment(2, collectionID, 10, "ch-1", 0, 199, false),
			)
			meta.indexMeta = &indexMeta{
				indexes: map[UniqueID]map[UniqueID]*model.Index{
					collectionID: {
						indexID: {
							CollectionID: collectionID,
							FieldID:      vectorFieldID,
							IndexID:      indexID,
						},
					},
				},
				segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
			}
			for _, segmentID := range test.finishedSegmentIDs {
				finishedIndexes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
				finishedIndexes.Insert(indexID, &model.SegmentIndex{
					CollectionID: collectionID,
					SegmentID:    segmentID,
					IndexID:      indexID,
					IndexState:   commonpb.IndexState_Finished,
				})
				meta.indexMeta.segmentIndexes.Insert(segmentID, finishedIndexes)
			}

			events, err := newCompactionTargetReconcilerForTest(meta).Trigger(ctx)

			require.NoError(t, err)
			views := events[TriggerTypeSingle]
			require.Len(t, views, len(test.wantSegmentIDs))
			for i, segmentID := range test.wantSegmentIDs {
				require.Equal(t, []int64{segmentID}, segmentIDsFromViews(views[i].GetSegmentsView()))
			}
			require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())
		})
	}
}

func TestCompactionTargetReconcilerSkipsManualIndexFilterWhenDisabled(t *testing.T) {
	enableCompactionTargetReconciler(t)
	paramtable.Get().Save(Params.DataCoordCfg.IndexBasedCompaction.Key, "false")
	t.Cleanup(func() {
		paramtable.Get().Reset(Params.DataCoordCfg.IndexBasedCompaction.Key)
	})

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
	)
	meta.indexMeta.indexes[1] = map[UniqueID]*model.Index{
		1000: {CollectionID: 1, FieldID: 100, IndexID: 1000},
	}

	events, err := newCompactionTargetReconcilerForTest(meta).Trigger(ctx)

	require.NoError(t, err)
	require.Len(t, events[TriggerTypeSingle], 1)
	require.Equal(t, []int64{1}, segmentIDsFromViews(events[TriggerTypeSingle][0].GetSegmentsView()))
}

func TestCompactionTargetReconcilerManualIndexFilterKeepsNoIndexCollection(t *testing.T) {
	enableCompactionTargetReconciler(t)
	paramtable.Get().Save(Params.DataCoordCfg.IndexBasedCompaction.Key, "true")
	t.Cleanup(func() {
		paramtable.Get().Reset(Params.DataCoordCfg.IndexBasedCompaction.Key)
	})

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
	)

	events, err := newCompactionTargetReconcilerForTest(meta).Trigger(ctx)

	require.NoError(t, err)
	require.Len(t, events[TriggerTypeSingle], 1)
	require.Equal(t, []int64{1}, segmentIDsFromViews(events[TriggerTypeSingle][0].GetSegmentsView()))
}

func newLoadedCompactionTargetMeta(t *testing.T, ctx context.Context, records ...*datapb.CompactionTarget) *compactionTargetMeta {
	t.Helper()
	catalog, _, _, _ := newCompactionTargetTestCatalog(t, records...)
	targetMeta, err := newCompactionTargetMeta(ctx, catalog)
	require.NoError(t, err)
	return targetMeta
}

func newCompactionTargetReconcilerForTest(meta *meta) *compactionTargetReconciler {
	return newCompactionTargetReconciler(meta, newMockHandler())
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
		compactionTargetMeta: targetMeta,
		indexMeta: &indexMeta{
			indexes: make(map[UniqueID]map[UniqueID]*model.Index),
		},
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
