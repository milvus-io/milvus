package targets

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/snapshot"
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
	targetMeta := NewLoadedCompactionTargetMeta(t, ctx, record)
	mt := newCompactionTargetReconcilerTestMeta(targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false),
		sortedTargetSegment(4, 1, 10, "ch-1", 0, 198, false),
		targetSegmentWithDataTS(2, 1, 10, "ch-1", 0, 199, false),
		sortedTargetSegment(3, 1, 10, "ch-1", 0, 200, false),
	)

	events, err := newCompactionTargetReconcilerForTest(mt).Trigger(ctx)

	require.NoError(t, err)
	views := events[policy.TriggerTypeSingle]
	require.Len(t, views, 2)
	segmentIDs := make([]int64, 0, len(views))
	for _, cv := range views {
		require.Zero(t, cv.GetTriggerID())
		require.Equal(t, int64(10), cv.GetGroupLabel().PartitionID)
		require.Equal(t, "ch-1", cv.GetGroupLabel().Channel)
		segmentIDs = append(segmentIDs, segmentIDsFromViews(cv.GetSegmentsView())...)
	}
	require.ElementsMatch(t, []int64{1, 4}, segmentIDs)
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
		TailLimit:    0,
		State:        datapb.TargetState_TARGET_STATE_ACTIVE,
	}
	targetMeta := NewLoadedCompactionTargetMeta(t, ctx, record)
	mt := newCompactionTargetReconcilerTestMeta(targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 200, 199, false),
	)

	events, err := newCompactionTargetReconcilerForTest(mt).Trigger(ctx)

	require.NoError(t, err)
	require.Empty(t, events[policy.TriggerTypeSingle])
	require.Equal(t, datapb.TargetState_TARGET_STATE_INACTIVE, targetMeta.GetCompactionTarget(100).GetState())
}

func TestCompactionTargetReconcilerReconcilesMultipleTargetsIndependently(t *testing.T) {
	enableCompactionTargetReconciler(t)
	ctx := context.Background()
	record1 := &datapb.CompactionTarget{
		TargetID:     100,
		CollectionID: 1,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		Rewrite:      &datapb.CompactionTargetRewrite{RootSegmentIds: []int64{1}},
		ExpectedTS:   200,
		TailLimit:    0,
		State:        datapb.TargetState_TARGET_STATE_ACTIVE,
	}
	record2 := &datapb.CompactionTarget{
		TargetID:     200,
		CollectionID: 1,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		Rewrite:      &datapb.CompactionTargetRewrite{RootSegmentIds: []int64{2}},
		ExpectedTS:   200,
		TailLimit:    0,
		State:        datapb.TargetState_TARGET_STATE_ACTIVE,
	}
	targetMeta := NewLoadedCompactionTargetMeta(t, ctx, record1, record2)
	dcMeta := newCompactionTargetReconcilerTestMeta(targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false),
		sortedTargetSegment(2, 1, 20, "ch-2", 0, 199, false),
		sortedTargetSegment(3, 2, 10, "ch-1", 0, 199, false),
	)
	events, err := newCompactionTargetReconcilerForTest(dcMeta).Trigger(ctx)

	require.NoError(t, err)
	views := events[policy.TriggerTypeSingle]
	require.Len(t, views, 2)
	var segmentIDs []int64
	for _, cv := range views {
		segmentIDs = append(segmentIDs, segmentIDsFromViews(cv.GetSegmentsView())...)
	}
	require.ElementsMatch(t, []int64{1, 2}, segmentIDs)
}

func TestCompactionTargetReconcilerPlansCompleteMatchAndChecksSatisfaction(t *testing.T) {
	enableCompactionTargetReconciler(t)

	ctx := context.Background()
	activeRecord := &datapb.CompactionTarget{
		TargetID:     100,
		CollectionID: 1,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		Rewrite:      &datapb.CompactionTargetRewrite{RootSegmentIds: []int64{1, 2}},
		ExpectedTS:   200,
		TailLimit:    0,
		State:        datapb.TargetState_TARGET_STATE_ACTIVE,
	}
	satisfiedRecord := &datapb.CompactionTarget{
		TargetID:     200,
		CollectionID: 1,
		Intent:       datapb.TargetIntent_INTENT_REWRITE,
		Rewrite:      &datapb.CompactionTargetRewrite{RootSegmentIds: []int64{3}},
		ExpectedTS:   200,
		TailLimit:    0,
		State:        datapb.TargetState_TARGET_STATE_ACTIVE,
	}
	targetMeta := NewLoadedCompactionTargetMeta(t, ctx, activeRecord, satisfiedRecord)
	mt := newCompactionTargetReconcilerTestMeta(targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false),
		sortedTargetSegment(2, 1, 10, "ch-1", 0, 199, false),
	)
	reconciler := newCompactionTargetReconcilerForTest(mt)

	events, err := reconciler.Reconcile(ctx)

	require.NoError(t, err)
	require.Len(t, events[policy.TriggerTypeSingle], 2)
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())
	require.Equal(t, datapb.TargetState_TARGET_STATE_INACTIVE, targetMeta.GetCompactionTarget(200).GetState())
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
	targetMeta := NewLoadedCompactionTargetMeta(t, ctx, record)
	mt := newCompactionTargetReconcilerTestMeta(targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false),
		sortedTargetSegment(2, 2, 10, "ch-1", 0, 199, false),
	)

	events, err := newCompactionTargetReconcilerForTest(mt).Trigger(ctx)

	require.NoError(t, err)
	require.Len(t, events[policy.TriggerTypeSingle], 1)
	require.Equal(t, []int64{1}, segmentIDsFromViews(events[policy.TriggerTypeSingle][0].GetSegmentsView()))
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
	targetMeta := NewLoadedCompactionTargetMeta(t, ctx, record)
	mt := newCompactionTargetReconcilerTestMeta(targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 201, 199, false),
		sortedTargetSegment(2, 1, 10, "ch-1", 0, 201, false),
	)

	events, err := newCompactionTargetReconcilerForTest(mt).Trigger(ctx)

	require.NoError(t, err)
	require.Empty(t, events[policy.TriggerTypeSingle])
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
	targetMeta := NewLoadedCompactionTargetMeta(t, ctx, record)
	droppedSource := sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false)
	droppedSource.State = commonpb.SegmentState_Dropped
	mt := newCompactionTargetReconcilerTestMeta(targetMeta,
		droppedSource,
		sortedTargetSegment(2, 1, 10, "ch-1", 201, 199, false, 1),
	)

	events, err := newCompactionTargetReconcilerForTest(mt).Trigger(ctx)

	require.NoError(t, err)
	require.Empty(t, events[policy.TriggerTypeSingle])
	require.Equal(t, datapb.TargetState_TARGET_STATE_INACTIVE, targetMeta.GetCompactionTarget(100).GetState())
}

func TestCompactionTargetReconcilerOutsideManualMatchDomainDoesNotHoldTargetActive(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*meta.Meta, *meta.SegmentInfo)
	}{
		{
			name: "flushing",
			mutate: func(_ *meta.Meta, segment *meta.SegmentInfo) {
				segment.State = commonpb.SegmentState_Flushing
			},
		},
		{
			name: "importing",
			mutate: func(_ *meta.Meta, segment *meta.SegmentInfo) {
				segment.IsImporting = true
			},
		},
		{
			name: "L0",
			mutate: func(_ *meta.Meta, segment *meta.SegmentInfo) {
				segment.Level = datapb.SegmentLevel_L0
			},
		},
		{
			name: "L2",
			mutate: func(_ *meta.Meta, segment *meta.SegmentInfo) {
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
			targetMeta := NewLoadedCompactionTargetMeta(t, ctx, record)
			segment := sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false)
			mt := newCompactionTargetReconcilerTestMeta(targetMeta, segment)
			test.mutate(mt.Meta, segment)

			events, err := newCompactionTargetReconcilerForTest(mt).Trigger(ctx)

			require.NoError(t, err)
			require.Empty(t, events[policy.TriggerTypeSingle])
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
	targetMeta := NewLoadedCompactionTargetMeta(t, ctx, record)
	segment := sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false)
	mt := newCompactionTargetReconcilerTestMeta(targetMeta, segment)
	snapMeta := &snapshot.SnapshotMeta{
		SegmentProtectionUntil: map[int64]uint64{
			segment.GetID(): uint64(time.Now().Add(time.Hour).Unix()),
		},
	}
	mt.CompactionProtection = snapMeta

	events, err := newCompactionTargetReconcilerForTest(mt).Trigger(ctx)

	require.NoError(t, err)
	require.Empty(t, events[policy.TriggerTypeSingle])
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())

	delete(snapMeta.SegmentProtectionUntil, segment.GetID())
	events, err = newCompactionTargetReconcilerForTest(mt).Trigger(ctx)

	require.NoError(t, err)
	require.Len(t, events[policy.TriggerTypeSingle], 1)
	require.Equal(t, []int64{1}, segmentIDsFromViews(events[policy.TriggerTypeSingle][0].GetSegmentsView()))
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())
}

func TestCompactionTargetReconcilerKeepsTemporarilyBlockedMatchActive(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*meta.SegmentInfo)
	}{
		{
			name: "compacting",
			mutate: func(segment *meta.SegmentInfo) {
				segment.IsCompacting = true
			},
		},
		{
			name: "invisible",
			mutate: func(segment *meta.SegmentInfo) {
				segment.IsInvisible = true
			},
		},
		{
			name: "unsorted",
			mutate: func(segment *meta.SegmentInfo) {
				segment.IsSorted = false
			},
		},
		{
			name: "invisible and unsorted",
			mutate: func(segment *meta.SegmentInfo) {
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
			targetMeta := NewLoadedCompactionTargetMeta(t, ctx, record)
			segment := sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false)
			test.mutate(segment)
			mt := newCompactionTargetReconcilerTestMeta(targetMeta, segment)

			events, err := newCompactionTargetReconcilerForTest(mt).Trigger(ctx)

			require.NoError(t, err)
			require.Empty(t, events[policy.TriggerTypeSingle])
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
	targetMeta := NewLoadedCompactionTargetMeta(t, ctx, record)
	mt := newCompactionTargetReconcilerTestMeta(targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false),
	)
	snapMeta := snapshot.CreateTestSnapshotMetaLoaded(t)
	mt.CompactionProtection = snapMeta
	snapMeta.SetSnapshotPending(1)

	events, err := newCompactionTargetReconcilerForTest(mt).Trigger(ctx)

	require.NoError(t, err)
	require.Empty(t, events[policy.TriggerTypeSingle])
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())

	snapMeta.ClearSnapshotPending(1)
	events, err = newCompactionTargetReconcilerForTest(mt).Trigger(ctx)

	require.NoError(t, err)
	require.Len(t, events[policy.TriggerTypeSingle], 1)
	require.Equal(t, []int64{1}, segmentIDsFromViews(events[policy.TriggerTypeSingle][0].GetSegmentsView()))
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())
}

func TestCompactionTargetReconcilerUsesManualIndexReadinessFilter(t *testing.T) {
	enableCompactionTargetReconciler(t)
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key, "true")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key)
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
			targetMeta := NewLoadedCompactionTargetMeta(t, ctx, record)
			mt := newCompactionTargetReconcilerTestMeta(targetMeta,
				sortedTargetSegment(1, collectionID, 10, "ch-1", 0, 199, false),
				sortedTargetSegment(2, collectionID, 10, "ch-1", 0, 199, false),
			)
			mt.AddCollection(&meta.CollectionInfo{
				ID: collectionID,
				Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
					{FieldID: vectorFieldID, DataType: schemapb.DataType_FloatVector},
				}},
			})
			mt.indexMeta = &index.IndexMeta{
				Indexes: map[int64]map[int64]*model.Index{
					collectionID: {
						indexID: {
							CollectionID: collectionID,
							FieldID:      vectorFieldID,
							IndexID:      indexID,
						},
					},
				},
				SegmentIndexes: typeutil.NewConcurrentMap[int64, *typeutil.ConcurrentMap[int64, *model.SegmentIndex]](),
			}
			for _, segmentID := range test.finishedSegmentIDs {
				finishedIndexes := typeutil.NewConcurrentMap[int64, *model.SegmentIndex]()
				finishedIndexes.Insert(indexID, &model.SegmentIndex{
					CollectionID: collectionID,
					SegmentID:    segmentID,
					IndexID:      indexID,
					IndexState:   commonpb.IndexState_Finished,
				})
				mt.indexMeta.SegmentIndexes.Insert(segmentID, finishedIndexes)
			}

			events, err := newCompactionTargetReconcilerForTest(mt).Trigger(ctx)

			require.NoError(t, err)
			views := events[policy.TriggerTypeSingle]
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
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key, "false")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key)
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
	targetMeta := NewLoadedCompactionTargetMeta(t, ctx, record)
	mt := newCompactionTargetReconcilerTestMeta(targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false),
	)
	mt.indexMeta.Indexes[1] = map[int64]*model.Index{
		1000: {CollectionID: 1, FieldID: 100, IndexID: 1000},
	}

	events, err := newCompactionTargetReconcilerForTest(mt).Trigger(ctx)

	require.NoError(t, err)
	require.Len(t, events[policy.TriggerTypeSingle], 1)
	require.Equal(t, []int64{1}, segmentIDsFromViews(events[policy.TriggerTypeSingle][0].GetSegmentsView()))
}

func TestCompactionTargetReconcilerManualIndexFilterKeepsNoIndexCollection(t *testing.T) {
	enableCompactionTargetReconciler(t)
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key, "true")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key)
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
	targetMeta := NewLoadedCompactionTargetMeta(t, ctx, record)
	mt := newCompactionTargetReconcilerTestMeta(targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false),
	)

	events, err := newCompactionTargetReconcilerForTest(mt).Trigger(ctx)

	require.NoError(t, err)
	require.Len(t, events[policy.TriggerTypeSingle], 1)
	require.Equal(t, []int64{1}, segmentIDsFromViews(events[policy.TriggerTypeSingle][0].GetSegmentsView()))
}

func newCompactionTargetReconcilerForTest(fixture *compactionTargetReconcilerFixture) *CompactionTargetReconciler {
	return NewCompactionTargetReconciler(fixture.Meta, fixture.indexMeta, fixture.targetMeta, meta.NewMockHandlerWithMeta(fixture.Meta),
		func(_ context.Context, id int64) (bool, error) {
			return fixture.Meta.GetCollection(id) != nil, nil
		})
}

func enableCompactionTargetReconciler(t *testing.T) {
	t.Helper()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableTargetBasedCompaction.Key, "true")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableTargetBasedCompaction.Key)
	})
}

// compactionTargetReconcilerFixture bundles the meta and the domain metas that a reconciler under test reads.
type compactionTargetReconcilerFixture struct {
	*meta.Meta
	targetMeta *CompactionTargetMeta
	indexMeta  *index.IndexMeta
}

func newCompactionTargetReconcilerTestMeta(targetMeta *CompactionTargetMeta, segments ...*meta.SegmentInfo) *compactionTargetReconcilerFixture {
	mt := meta.NewTestMetaForCompactionTargets()
	for _, record := range targetMeta.GetCompactionTargets() {
		mt.AddCollection(&meta.CollectionInfo{ID: record.GetCollectionID(), Schema: &schemapb.CollectionSchema{}})
	}
	for _, segment := range segments {
		mt.Segments.SetSegment(segment.GetID(), segment)
	}
	return &compactionTargetReconcilerFixture{
		Meta:       mt,
		targetMeta: targetMeta,
		indexMeta: &index.IndexMeta{
			Indexes: make(map[int64]map[int64]*model.Index),
		},
	}
}

func sortedTargetSegment(id, collectionID, partitionID int64, channel string, createTS uint64, dataTS uint64, compacting bool, compactionFrom ...int64) *meta.SegmentInfo {
	segment := targetSegmentWithDataTS(id, collectionID, partitionID, channel, createTS, dataTS, compacting, compactionFrom...)
	segment.IsSorted = true
	return segment
}

func segmentIDsFromViews(views []*view.SegmentView) []int64 {
	segmentIDs := make([]int64, 0, len(views))
	for _, cv := range views {
		segmentIDs = append(segmentIDs, cv.ID)
	}
	return segmentIDs
}

func TestCompactionTargetReconcilerReoffersSortWithAutoCompactionOff(t *testing.T) {
	enableCompactionTargetReconciler(t)
	Params.Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "false")
	Params.Save(paramtable.Get().DataCoordCfg.EnableSortCompaction.Key, "true")
	t.Cleanup(func() {
		Params.Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)
		Params.Reset(paramtable.Get().DataCoordCfg.EnableSortCompaction.Key)
	})

	record := &datapb.CompactionTarget{
		TargetID: 100, CollectionID: 1, Intent: datapb.TargetIntent_INTENT_REWRITE, ExpectedTS: 200,
	}
	targetMeta := NewLoadedCompactionTargetMeta(t, t.Context(), record)
	unsorted := targetSegmentWithDataTS(1, 1, 10, "ch-1", 0, 199, false)
	mt := newCompactionTargetReconcilerTestMeta(targetMeta, unsorted)
	events, err := newCompactionTargetReconcilerForTest(mt).Reconcile(t.Context())
	require.NoError(t, err)
	require.Len(t, events[policy.TriggerTypeSort], 1)
	require.Equal(t, []int64{1}, segmentIDsFromViews(events[policy.TriggerTypeSort][0].GetSegmentsView()))
	require.Empty(t, events[policy.TriggerTypeSingle])
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())
}

func TestCompactionTargetReconcilerSatisfiesEmptyCollectionWithoutLocalCache(t *testing.T) {
	enableCompactionTargetReconciler(t)
	record := &datapb.CompactionTarget{
		TargetID: 100, CollectionID: 1, Intent: datapb.TargetIntent_INTENT_REWRITE, ExpectedTS: 200,
	}
	targetMeta := NewLoadedCompactionTargetMeta(t, t.Context(), record)
	mt := meta.NewTestMetaForCompactionTargets()
	reconciler := NewCompactionTargetReconciler(mt, nil, targetMeta, meta.NewMockHandlerWithMeta(mt),
		func(_ context.Context, id int64) (bool, error) {
			return id == 1, nil
		})
	events, err := reconciler.Reconcile(t.Context())
	require.NoError(t, err)
	require.Empty(t, events)
	require.Equal(t, StopSatisfied, targetMeta.GetCompactionTarget(100).GetStopReason())
}
