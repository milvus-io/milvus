package targets

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/snapshot"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestCompactionTargetReconcilerResolvesLostStopBeforePlanning(t *testing.T) {
	for _, committed := range []bool{true, false} {
		name := "uncommitted stop"
		if committed {
			name = "committed stop"
		}
		t.Run(name, func(t *testing.T) {
			enableCompactionTargetReconciler(t)
			ctx := t.Context()
			catalog := catalogmocks.NewDataCoordCatalog(t)
			record := &datapb.CompactionTarget{
				TargetID: 100, CollectionID: 1, ExpectedTS: 200, ActivatedAtTS: 200,
				Intent: datapb.TargetIntent_INTENT_REWRITE, Rewrite: &datapb.CompactionTargetRewrite{},
			}
			catalog.EXPECT().ListCompactionTargets(mock.Anything).Return([]*datapb.CompactionTarget{record}, nil)
			catalog.EXPECT().CreateCompactionTarget(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Maybe()
			catalog.EXPECT().LoadCompactionTarget(mock.Anything, int64(1000000)).Return(nil, nil).Maybe()
			persisted := proto.CloneOf(record)
			catalog.EXPECT().StopCompactionTarget(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
				func(_ context.Context, next *datapb.CompactionTarget, _ uint64) (*datapb.CompactionTarget, bool, error) {
					if committed {
						persisted = proto.CloneOf(next)
					}
					return nil, false, context.DeadlineExceeded
				}).Once()
			// All subsequent reads reflect the state actually persisted.
			catalog.EXPECT().LoadCompactionTarget(mock.Anything, record.GetTargetID()).RunAndReturn(
				func(context.Context, int64) (*datapb.CompactionTarget, error) {
					return proto.CloneOf(persisted), nil
				}).Maybe()
			manager, err := NewCompactionTargetMetaForTest(ctx, catalog)
			require.NoError(t, err)
			mt := newCompactionTargetReconcilerTestMeta(manager)
			reconciler := newCompactionTargetReconcilerForTest(t, mt)

			events, err := reconciler.Reconcile(ctx)
			require.NoError(t, err)
			require.Empty(t, plannedTaskDescriptors(t, events[policy.TriggerTypeSingle]))
			late := sortedTargetSegment(1, 1, 10, "ch", 0, 199, false)
			mt.Segments.SetSegment(late.GetID(), late)

			events, err = reconciler.Reconcile(ctx)
			require.NoError(t, err)
			plans := plannedTaskDescriptors(t, events[policy.TriggerTypeSingle])
			if committed {
				require.Empty(t, plans)
				require.Equal(t, StopSatisfied, manager.GetCompactionTarget(record.GetTargetID()).GetStopReason())
				return
			}
			require.Len(t, plans, 1)
			require.Equal(t, []int64{late.GetID()}, plans[0].GetInputSegments())
			require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, persisted.GetState())
		})
	}
}

func TestCompactionTargetGCRefreshesUnconfirmedStopsIndependently(t *testing.T) {
	ctx := t.Context()
	const retention = "dataCoord.compaction.target.retentionSeconds"
	require.NoError(t, paramtable.Get().Save(retention, "60"))
	t.Cleanup(func() { paramtable.Get().Reset(retention) })

	expired := &datapb.CompactionTarget{
		TargetID: 100, CollectionID: 1, ExpectedTS: 200, ActivatedAtTS: 200,
		Intent: datapb.TargetIntent_INTENT_REWRITE, Rewrite: &datapb.CompactionTargetRewrite{},
	}
	unreadable := proto.CloneOf(expired)
	unreadable.TargetID++
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListCompactionTargets(mock.Anything).Return([]*datapb.CompactionTarget{expired, unreadable}, nil)
	now := time.Now()
	terminal := proto.CloneOf(expired)
	terminal.State = datapb.TargetState_TARGET_STATE_INACTIVE
	terminal.StopReason = StopOperatorDropped
	terminal.InactivatedAtTS = tsoutil.ComposeTSByTime(now.Add(-time.Hour))
	for _, record := range []*datapb.CompactionTarget{expired, unreadable} {
		catalog.EXPECT().StopCompactionTarget(mock.Anything, mock.MatchedBy(func(next *datapb.CompactionTarget) bool {
			return next.GetTargetID() == record.GetTargetID()
		}), mock.Anything).Return(nil, false, context.DeadlineExceeded).Once()
	}
	catalog.EXPECT().LoadCompactionTarget(mock.Anything, expired.GetTargetID()).Return(terminal, nil).Once()
	catalog.EXPECT().LoadCompactionTarget(mock.Anything, unreadable.GetTargetID()).Return(nil, merr.ErrIoFailed).Once()
	catalog.EXPECT().DropCompactionTarget(mock.Anything, terminal, mock.Anything).Return(nil).Once()

	manager, err := NewCompactionTargetMetaForTest(ctx, catalog)
	require.NoError(t, err)
	for _, record := range []*datapb.CompactionTarget{expired, unreadable} {
		err = manager.Stop(ctx, record.GetTargetID(), StopOperatorDropped, func(*datapb.CompactionTarget) (bool, error) {
			return true, nil
		})
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
	manager.CollectExpired(ctx, now)
	got, err := manager.Read(ctx, expired.GetTargetID())
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestCompactionTargetReconcilerKeepsOptimizeScopeUntilTerminalSave(t *testing.T) {
	enableCompactionTargetReconciler(t)
	require.NoError(t, paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "true"))
	require.NoError(t, paramtable.Get().Save(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key, "false"))
	require.NoError(t, paramtable.Get().Save(paramtable.Get().DataCoordCfg.SegmentMaxSize.Key, "1"))
	require.NoError(t, paramtable.Get().Save(paramtable.Get().DataCoordCfg.DiskSegmentMaxSize.Key, "1"))
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.IndexBasedCompaction.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.SegmentMaxSize.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.DiskSegmentMaxSize.Key)
	})
	ctx := t.Context()
	record := &datapb.CompactionTarget{
		TargetID: 100, CollectionID: 1, Intent: datapb.TargetIntent_INTENT_OPTIMIZE,
		ExpectedTS: 200, ActivatedAtTS: 200, TailLimit: 1,
		Optimize: &datapb.CompactionTargetOptimize{InputTargetSizeBytes: 1 << 20, ActualTargetSizeBytes: 1 << 20},
	}
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListCompactionTargets(mock.Anything).Return([]*datapb.CompactionTarget{record}, nil)
	catalog.EXPECT().CreateCompactionTarget(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Maybe()
	catalog.EXPECT().LoadCompactionTarget(mock.Anything, int64(1000000)).Return(nil, nil).Maybe()
	catalog.EXPECT().StopCompactionTarget(mock.Anything, mock.Anything, mock.Anything).Return(nil, false, merr.ErrIoFailed).Once()
	catalog.EXPECT().LoadCompactionTarget(mock.Anything, record.GetTargetID()).Return(record, nil).Maybe()
	catalog.EXPECT().StopCompactionTarget(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, next *datapb.CompactionTarget, _ uint64) (*datapb.CompactionTarget, bool, error) {
			return proto.CloneOf(next), true, nil
		}).Once()
	manager, err := NewCompactionTargetMetaForTest(ctx, catalog)
	require.NoError(t, err)
	inside := sortedTargetSegment(1, 1, 10, "ch", 0, 199, false)
	outside := sortedTargetSegment(2, 1, 10, "ch", 0, 201, false)
	for _, segment := range []*meta.SegmentInfo{inside, outside} {
		segment.NumOfRows = 10
		segment.Stats = &datapb.Statistics{InsertBinlogSize: 500 << 10}
	}
	mt := newCompactionTargetReconcilerTestMeta(manager, inside, outside)
	reconciler := newCompactionTargetReconcilerForTest(t, mt)

	events, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	require.Empty(t, plannedTaskDescriptors(t, events[policy.TriggerTypeSingle]))
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, manager.GetCompactionTarget(record.GetTargetID()).GetState())

	events, err = reconciler.Reconcile(ctx)
	require.NoError(t, err)
	plans := plannedTaskDescriptors(t, events[policy.TriggerTypeSingle])
	require.Len(t, plans, 1)
	require.ElementsMatch(t, []int64{inside.GetID(), outside.GetID()}, plans[0].GetInputSegments())
	require.Equal(t, StopSatisfied, manager.GetCompactionTarget(record.GetTargetID()).GetStopReason())
}

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

	events, err := newCompactionTargetReconcilerForTest(t, mt).Reconcile(ctx)

	require.NoError(t, err)
	views := events[policy.TriggerTypeSingle]
	require.Len(t, plannedTaskDescriptors(t, views), 2)
	segmentIDs := make([]int64, 0, len(views))
	for _, cv := range views {
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

	events, err := newCompactionTargetReconcilerForTest(t, mt).Reconcile(ctx)

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
	events, err := newCompactionTargetReconcilerForTest(t, dcMeta).Reconcile(ctx)

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
	reconciler := newCompactionTargetReconcilerForTest(t, mt)

	events, err := reconciler.Reconcile(ctx)

	require.NoError(t, err)
	require.Len(t, plannedTaskDescriptors(t, events[policy.TriggerTypeSingle]), 2)
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

	events, err := newCompactionTargetReconcilerForTest(t, mt).Reconcile(ctx)

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

	events, err := newCompactionTargetReconcilerForTest(t, mt).Reconcile(ctx)

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

	events, err := newCompactionTargetReconcilerForTest(t, mt).Reconcile(ctx)

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

			events, err := newCompactionTargetReconcilerForTest(t, mt).Reconcile(ctx)

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

	events, err := newCompactionTargetReconcilerForTest(t, mt).Reconcile(ctx)

	require.NoError(t, err)
	require.Empty(t, events[policy.TriggerTypeSingle])
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())

	delete(snapMeta.SegmentProtectionUntil, segment.GetID())
	events, err = newCompactionTargetReconcilerForTest(t, mt).Reconcile(ctx)

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

			events, err := newCompactionTargetReconcilerForTest(t, mt).Reconcile(ctx)

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

	events, err := newCompactionTargetReconcilerForTest(t, mt).Reconcile(ctx)

	require.NoError(t, err)
	require.Empty(t, events[policy.TriggerTypeSingle])
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())

	snapMeta.ClearSnapshotPending(1)
	events, err = newCompactionTargetReconcilerForTest(t, mt).Reconcile(ctx)

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

			events, err := newCompactionTargetReconcilerForTest(t, mt).Reconcile(ctx)

			require.NoError(t, err)
			views := events[policy.TriggerTypeSingle]
			require.Len(t, plannedTaskDescriptors(t, views), len(test.wantSegmentIDs))
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

	events, err := newCompactionTargetReconcilerForTest(t, mt).Reconcile(ctx)

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

	events, err := newCompactionTargetReconcilerForTest(t, mt).Reconcile(ctx)

	require.NoError(t, err)
	require.Len(t, events[policy.TriggerTypeSingle], 1)
	require.Equal(t, []int64{1}, segmentIDsFromViews(events[policy.TriggerTypeSingle][0].GetSegmentsView()))
}

func newCompactionTargetReconcilerForTest(t *testing.T, fixture *compactionTargetReconcilerFixture) *CompactionTargetReconciler {
	return NewCompactionTargetReconciler(fixture.Meta, fixture.indexMeta, fixture.targetMeta, meta.NewMockHandlerWithMeta(fixture.Meta), newStandingSortAllocator(t),
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
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key, "false")
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableSortCompaction.Key, "true")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableAutoCompaction.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableSortCompaction.Key)
	})

	record := &datapb.CompactionTarget{
		TargetID: 100, CollectionID: 1, Intent: datapb.TargetIntent_INTENT_REWRITE, ExpectedTS: 200,
	}
	targetMeta := NewLoadedCompactionTargetMeta(t, t.Context(), record)
	unsorted := targetSegmentWithDataTS(1, 1, 10, "ch-1", 0, 199, false)
	mt := newCompactionTargetReconcilerTestMeta(targetMeta, unsorted)
	events, err := newCompactionTargetReconcilerForTest(t, mt).Reconcile(t.Context())
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
	reconciler := NewCompactionTargetReconciler(mt, nil, targetMeta, meta.NewMockHandlerWithMeta(mt), newStandingSortAllocator(t),
		func(_ context.Context, id int64) (bool, error) {
			return id == 1, nil
		})
	events, err := reconciler.Reconcile(t.Context())
	require.NoError(t, err)
	require.Empty(t, events[policy.TriggerTypeSingle])
	require.Empty(t, events[policy.TriggerTypeSort])
	require.Equal(t, StopSatisfied, targetMeta.GetCompactionTarget(100).GetStopReason())
}

func TestCompactionTargetReconcilerObservesFlushBeforeSatisfaction(t *testing.T) {
	enableCompactionTargetReconciler(t)
	record := &datapb.CompactionTarget{
		TargetID: 100, CollectionID: 1, Intent: datapb.TargetIntent_INTENT_REWRITE,
		ExpectedTS: 200, TotalRows: proto.Int64(0),
	}
	targetMeta := NewLoadedCompactionTargetMeta(t, t.Context(), record)
	segment := sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false)
	segment.State = commonpb.SegmentState_Growing
	fixture := newCompactionTargetReconcilerTestMeta(targetMeta, segment)
	observations := 0
	reconciler := NewCompactionTargetReconciler(fixture.Meta, fixture.indexMeta, targetMeta, meta.NewMockHandlerWithMeta(fixture.Meta), newStandingSortAllocator(t),
		func(_ context.Context, id int64) (bool, error) {
			observations++
			if observations == 2 {
				require.NoError(t, fixture.Meta.UpdateSegmentsInfo(t.Context(),
					meta.UpdateStatusOperator(1, commonpb.SegmentState_Flushed)))
			}
			return id == 1, nil
		})
	fixture.Catalog = &datacoord.Catalog{MetaKv: meta.NewMetaMemoryKV()}

	events, err := reconciler.Reconcile(t.Context())
	require.NoError(t, err)
	require.Len(t, events[policy.TriggerTypeSingle], 1)
	require.Equal(t, []int64{1}, segmentIDsFromViews(events[policy.TriggerTypeSingle][0].GetSegmentsView()))
	stored := targetMeta.GetCompactionTarget(100)
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, stored.GetState())
	require.Zero(t, stored.GetTotalRows())
}

func TestOptimizeWaitsForConcurrentL0Publication(t *testing.T) {
	enableCompactionTargetReconciler(t)
	ctx := t.Context()
	const expectedSize = 1 << 20
	target := &datapb.CompactionTarget{
		TargetID: 100, CollectionID: 1, Intent: datapb.TargetIntent_INTENT_OPTIMIZE,
		ExpectedTS: 200, ActivatedAtTS: 200, TailLimit: 1,
		Optimize: &datapb.CompactionTargetOptimize{InputTargetSizeBytes: expectedSize, ActualTargetSizeBytes: expectedSize},
	}
	targetMeta := NewLoadedCompactionTargetMeta(t, ctx, target)
	first := sortedTargetSegment(1, 1, 10, "ch", 0, 100, false)
	first.Stats = &datapb.Statistics{InsertBinlogSize: expectedSize, TimestampFrom: 100}
	second := sortedTargetSegment(2, 1, 10, "ch", 0, 100, false)
	second.Stats = &datapb.Statistics{InsertBinlogSize: 400_000, TimestampFrom: 100}
	fixture := newCompactionTargetReconcilerTestMeta(targetMeta, first, second)
	tasks := task.NewTestCompactionTaskMeta(t)
	l0 := &datapb.CompactionTask{
		PlanID: 300, TriggerID: 301, CollectionID: 1, PartitionID: 10, Channel: "ch",
		Type: datapb.CompactionType_Level0DeleteCompaction, State: datapb.CompactionTaskState_executing,
		Pos: &msgpb.MsgPosition{Timestamp: 150},
	}
	require.NoError(t, tasks.SaveCompactionTask(ctx, l0))
	reconciler := newCompactionTargetReconcilerForTest(t, fixture)
	reconciler.SetTaskMeta(tasks)

	// Publish between the final scope snapshot and its use. The L0 task
	// transitions only after its updated delta statistics become visible.
	var selectSegments func(*meta.Meta, context.Context, ...meta.SegmentFilter) []*meta.SegmentInfo
	observations := 0
	patch := mockey.Mock((*meta.Meta).SelectSegments).Origin(&selectSegments).To(
		func(mt *meta.Meta, ctx context.Context, filters ...meta.SegmentFilter) []*meta.SegmentInfo {
			segments := selectSegments(mt, ctx, filters...)
			if len(filters) == 2 {
				observations++
				if observations == 2 {
					updated := first.Clone()
					updated.Stats = proto.CloneOf(first.Stats)
					updated.Stats.DeleteNumRows = first.GetNumOfRows() / 2
					mt.Segments.SetSegment(first.GetID(), updated)
					published := proto.CloneOf(l0)
					published.State = datapb.CompactionTaskState_meta_saved
					require.NoError(t, tasks.SaveCompactionTask(ctx, published))
				}
			}
			return segments
		}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	_, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, observations)
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(target.GetTargetID()).GetState())
	patch.UnPatch()
	views, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	plans := plannedTaskDescriptors(t, views[policy.TriggerTypeSingle])
	require.Len(t, plans, 1)
	require.ElementsMatch(t, []int64{1, 2}, plans[0].GetInputSegments())
}

func newStandingSortAllocator(t *testing.T) allocator.Allocator {
	t.Helper()
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocID(mock.Anything).Return(1_000_000, nil).Maybe()
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(100), nil).Maybe()
	return alloc
}

func plannedTaskDescriptors(t *testing.T, views []view.TargetView) []*datapb.CompactionTask {
	t.Helper()
	var result []*datapb.CompactionTask
	for _, targetView := range views {
		plans, err := targetView.Plan()
		require.NoError(t, err)
		result = append(result, plans...)
	}
	return result
}
