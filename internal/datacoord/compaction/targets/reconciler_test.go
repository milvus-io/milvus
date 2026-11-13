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
	compactiontask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/snapshot"
	"github.com/milvus-io/milvus/internal/metacache"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
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
			catalog.EXPECT().CreateCompactionTarget(mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Maybe()
			persisted := proto.CloneOf(record)
			catalog.EXPECT().StopCompactionTarget(mock.Anything, mock.Anything).RunAndReturn(
				func(_ context.Context, next *datapb.CompactionTarget) (*datapb.CompactionTarget, bool, error) {
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
			mt := newCompactionTargetReconcilerTestMeta(t, manager)
			reconciler := newCompactionTargetReconcilerForTest(mt)

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
	require.NoError(t, Params.Save(retention, "60"))
	t.Cleanup(func() { Params.Reset(retention) })

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
		})).Return(nil, false, context.DeadlineExceeded).Once()
	}
	catalog.EXPECT().LoadCompactionTarget(mock.Anything, expired.GetTargetID()).Return(terminal, nil).Once()
	catalog.EXPECT().LoadCompactionTarget(mock.Anything, unreadable.GetTargetID()).Return(nil, merr.ErrIoFailed).Once()
	catalog.EXPECT().DropCompactionTarget(mock.Anything, terminal).Return(nil).Once()

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
	require.NoError(t, Params.Save(Params.DataCoordCfg.EnableAutoCompaction.Key, "true"))
	require.NoError(t, Params.Save(Params.DataCoordCfg.IndexBasedCompaction.Key, "false"))
	require.NoError(t, Params.Save(Params.DataCoordCfg.SegmentMaxSize.Key, "1"))
	require.NoError(t, Params.Save(Params.DataCoordCfg.DiskSegmentMaxSize.Key, "1"))
	t.Cleanup(func() {
		Params.Reset(Params.DataCoordCfg.EnableAutoCompaction.Key)
		Params.Reset(Params.DataCoordCfg.IndexBasedCompaction.Key)
		Params.Reset(Params.DataCoordCfg.SegmentMaxSize.Key)
		Params.Reset(Params.DataCoordCfg.DiskSegmentMaxSize.Key)
	})
	ctx := t.Context()
	record := &datapb.CompactionTarget{
		TargetID: 100, CollectionID: 1, Intent: datapb.TargetIntent_INTENT_OPTIMIZE,
		ExpectedTS: 200, ActivatedAtTS: 200, TailLimit: 1,
		Optimize: &datapb.CompactionTargetOptimize{InputTargetSizeBytes: 1 << 20, ActualTargetSizeBytes: 1 << 20},
	}
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListCompactionTargets(mock.Anything).Return([]*datapb.CompactionTarget{record}, nil)
	catalog.EXPECT().CreateCompactionTarget(mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Maybe()
	catalog.EXPECT().StopCompactionTarget(mock.Anything, mock.Anything).Return(nil, false, merr.ErrIoFailed).Once()
	catalog.EXPECT().LoadCompactionTarget(mock.Anything, record.GetTargetID()).Return(record, nil).Maybe()
	catalog.EXPECT().StopCompactionTarget(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, next *datapb.CompactionTarget) (*datapb.CompactionTarget, bool, error) {
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
	mt := newCompactionTargetReconcilerTestMeta(t, manager, inside, outside)
	reconciler := newCompactionTargetReconcilerForTest(mt)

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

func TestCompactionTargetReconcilerDoesNotMissL0PublicationDuringOptimizeFinish(t *testing.T) {
	enableCompactionTargetReconciler(t)
	ctx := t.Context()
	const targetSize = 1 << 20
	record := &datapb.CompactionTarget{
		TargetID: 100, CollectionID: 1, Intent: datapb.TargetIntent_INTENT_OPTIMIZE,
		ExpectedTS: 200, ActivatedAtTS: 200, TailLimit: 1,
		Optimize: &datapb.CompactionTargetOptimize{InputTargetSizeBytes: targetSize, ActualTargetSizeBytes: targetSize},
	}
	manager := NewLoadedCompactionTargetMeta(t, ctx, record)
	segment := sortedTargetSegment(1, 1, 10, "ch", 0, 199, false)
	segment.Stats = &datapb.Statistics{InsertBinlogSize: targetSize}
	mt := newCompactionTargetReconcilerTestMeta(t, manager, segment)
	reconciler := newCompactionTargetReconcilerForTest(mt)

	l0 := &datapb.CompactionTask{
		PlanID: 20, TriggerID: 19, CollectionID: 1, PartitionID: 10, Channel: "ch",
		Type: datapb.CompactionType_Level0DeleteCompaction, State: datapb.CompactionTaskState_executing,
		Pos: &msgpb.MsgPosition{Timestamp: 200},
	}
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return([]*datapb.CompactionTask{l0}, nil).Once()
	catalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
	tasks, err := compactiontask.NewCompactionTaskMeta(ctx, catalog)
	require.NoError(t, err)
	reconciler.SetTaskMeta(tasks)

	// Inject the L0 publication between the final segment snapshot and
	// consumption of that snapshot, then publish its persisted task state.
	var selectSegments func(*meta.Meta, context.Context, ...meta.SegmentFilter) []*meta.SegmentInfo
	var observations int
	patch := mockey.Mock((*meta.Meta).SelectSegments).Origin(&selectSegments).To(
		func(m *meta.Meta, ctx context.Context, filters ...meta.SegmentFilter) []*meta.SegmentInfo {
			segments := selectSegments(m, ctx, filters...)
			if len(filters) == 2 {
				observations++
				if observations == 2 {
					output := proto.CloneOf(segment.SegmentInfo)
					output.Stats.DeltaBinlogSize = targetSize / 2
					m.Segments.SetSegment(output.GetID(), meta.NewSegmentInfo(output))
					updated := proto.CloneOf(l0)
					updated.State = datapb.CompactionTaskState_meta_saved
					require.NoError(t, tasks.SaveCompactionTask(ctx, updated))
				}
			}
			return segments
		}).Build()
	t.Cleanup(func() { patch.UnPatch() })

	_, err = reconciler.Reconcile(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, observations, 2)
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, manager.GetCompactionTarget(record.GetTargetID()).GetState())

	events, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	plans := plannedTaskDescriptors(t, events[policy.TriggerTypeSingle])
	require.Len(t, plans, 1)
	require.Equal(t, []int64{segment.GetID()}, plans[0].GetInputSegments())
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
	mt := newCompactionTargetReconcilerTestMeta(t, targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false),
		sortedTargetSegment(4, 1, 10, "ch-1", 0, 198, false),
		targetSegmentWithDataTS(2, 1, 10, "ch-1", 0, 199, false),
		sortedTargetSegment(3, 1, 10, "ch-1", 0, 200, false),
	)

	events, err := newCompactionTargetReconcilerForTest(mt).Reconcile(ctx)

	require.NoError(t, err)
	views := events[policy.TriggerTypeSingle]
	plans := plannedTaskDescriptors(t, views)
	require.Len(t, plans, 2)
	var segmentIDs []int64
	for _, plan := range plans {
		require.Zero(t, plan.GetTriggerID())
		require.Zero(t, plan.GetPlanID())
		require.Equal(t, int64(10), plan.GetPartitionID())
		require.Equal(t, "ch-1", plan.GetChannel())
		segmentIDs = append(segmentIDs, plan.GetInputSegments()...)
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
	mt := newCompactionTargetReconcilerTestMeta(t, targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 200, 199, false),
	)

	events, err := newCompactionTargetReconcilerForTest(mt).Reconcile(ctx)

	require.NoError(t, err)
	require.Empty(t, plannedTaskDescriptors(t, events[policy.TriggerTypeSingle]))
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
	dcMeta := newCompactionTargetReconcilerTestMeta(t, targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false),
		sortedTargetSegment(2, 1, 20, "ch-2", 0, 199, false),
		sortedTargetSegment(3, 2, 10, "ch-1", 0, 199, false),
	)
	events, err := newCompactionTargetReconcilerForTest(dcMeta).Reconcile(ctx)

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
	paramtable.Get().Save(Params.DataCoordCfg.TargetCompactionMaxEvents.Key, "1")
	t.Cleanup(func() {
		paramtable.Get().Reset(Params.DataCoordCfg.TargetCompactionMaxEvents.Key)
	})

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
	mt := newCompactionTargetReconcilerTestMeta(t, targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false),
		sortedTargetSegment(2, 1, 10, "ch-1", 0, 199, false),
	)
	reconciler := newCompactionTargetReconcilerForTest(mt)

	events, err := reconciler.Reconcile(ctx)

	require.NoError(t, err)
	require.Len(t, plannedTaskDescriptors(t, events[policy.TriggerTypeSingle]), 2)
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())
	require.Equal(t, datapb.TargetState_TARGET_STATE_INACTIVE, targetMeta.GetCompactionTarget(200).GetState())

	paramtable.Get().Save(Params.DataCoordCfg.TargetCompactionMaxEvents.Key, "2")
	events, err = reconciler.Reconcile(ctx)

	require.NoError(t, err)
	require.Len(t, plannedTaskDescriptors(t, events[policy.TriggerTypeSingle]), 2)
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
	mt := newCompactionTargetReconcilerTestMeta(t, targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false),
		sortedTargetSegment(2, 2, 10, "ch-1", 0, 199, false),
	)

	events, err := newCompactionTargetReconcilerForTest(mt).Reconcile(ctx)

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
	mt := newCompactionTargetReconcilerTestMeta(t, targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 201, 199, false),
		sortedTargetSegment(2, 1, 10, "ch-1", 0, 201, false),
	)

	events, err := newCompactionTargetReconcilerForTest(mt).Reconcile(ctx)

	require.NoError(t, err)
	require.Empty(t, plannedTaskDescriptors(t, events[policy.TriggerTypeSingle]))
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
	mt := newCompactionTargetReconcilerTestMeta(t, targetMeta,
		droppedSource,
		sortedTargetSegment(2, 1, 10, "ch-1", 201, 199, false, 1),
	)

	events, err := newCompactionTargetReconcilerForTest(mt).Reconcile(ctx)

	require.NoError(t, err)
	require.Empty(t, plannedTaskDescriptors(t, events[policy.TriggerTypeSingle]))
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
			mt := newCompactionTargetReconcilerTestMeta(t, targetMeta, segment)
			test.mutate(mt.Meta, segment)

			events, err := newCompactionTargetReconcilerForTest(mt).Reconcile(ctx)

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
	mt := newCompactionTargetReconcilerTestMeta(t, targetMeta, segment)
	snapMeta := &snapshot.SnapshotMeta{
		SegmentProtectionUntil: map[int64]uint64{
			segment.GetID(): uint64(time.Now().Add(time.Hour).Unix()),
		},
	}
	mt.CompactionProtection = snapMeta

	events, err := newCompactionTargetReconcilerForTest(mt).Reconcile(ctx)

	require.NoError(t, err)
	require.Empty(t, events[policy.TriggerTypeSingle])
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())

	delete(snapMeta.SegmentProtectionUntil, segment.GetID())
	events, err = newCompactionTargetReconcilerForTest(mt).Reconcile(ctx)

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
			mt := newCompactionTargetReconcilerTestMeta(t, targetMeta, segment)

			events, err := newCompactionTargetReconcilerForTest(mt).Reconcile(ctx)

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
	mt := newCompactionTargetReconcilerTestMeta(t, targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false),
	)
	snapMeta := snapshot.CreateTestSnapshotMetaLoaded(t)
	mt.CompactionProtection = snapMeta
	snapMeta.SetSnapshotPending(1)

	events, err := newCompactionTargetReconcilerForTest(mt).Reconcile(ctx)

	require.NoError(t, err)
	require.Empty(t, events[policy.TriggerTypeSingle])
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())

	snapMeta.ClearSnapshotPending(1)
	events, err = newCompactionTargetReconcilerForTest(mt).Reconcile(ctx)

	require.NoError(t, err)
	require.Len(t, events[policy.TriggerTypeSingle], 1)
	require.Equal(t, []int64{1}, segmentIDsFromViews(events[policy.TriggerTypeSingle][0].GetSegmentsView()))
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
			targetMeta := NewLoadedCompactionTargetMeta(t, ctx, record)
			mt := newCompactionTargetReconcilerTestMeta(t, targetMeta,
				sortedTargetSegment(1, collectionID, 10, "ch-1", 0, 199, false),
				sortedTargetSegment(2, collectionID, 10, "ch-1", 0, 199, false),
			)
			mt.MetaStore.PutCollection(&meta.CollectionInfo{
				ID: collectionID,
				Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
					{FieldID: vectorFieldID, DataType: schemapb.DataType_FloatVector},
				}},
			})
			mt.indexMeta = &index.IndexMeta{
				Indexes: map[UniqueID]map[UniqueID]*model.Index{
					collectionID: {
						indexID: {
							CollectionID: collectionID,
							FieldID:      vectorFieldID,
							IndexID:      indexID,
						},
					},
				},
				SegmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
			}
			for _, segmentID := range test.finishedSegmentIDs {
				finishedIndexes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
				finishedIndexes.Insert(indexID, &model.SegmentIndex{
					CollectionID: collectionID,
					SegmentID:    segmentID,
					IndexID:      indexID,
					IndexState:   commonpb.IndexState_Finished,
				})
				mt.indexMeta.SegmentIndexes.Insert(segmentID, finishedIndexes)
			}

			events, err := newCompactionTargetReconcilerForTest(mt).Reconcile(ctx)

			require.NoError(t, err)
			views := events[policy.TriggerTypeSingle]
			var ids []int64
			for _, plan := range plannedTaskDescriptors(t, views) {
				ids = append(ids, plan.GetInputSegments()...)
			}
			require.ElementsMatch(t, test.wantSegmentIDs, ids)
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
	targetMeta := NewLoadedCompactionTargetMeta(t, ctx, record)
	mt := newCompactionTargetReconcilerTestMeta(t, targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false),
	)
	mt.indexMeta.Indexes[1] = map[UniqueID]*model.Index{
		1000: {CollectionID: 1, FieldID: 100, IndexID: 1000},
	}

	events, err := newCompactionTargetReconcilerForTest(mt).Reconcile(ctx)

	require.NoError(t, err)
	require.Len(t, events[policy.TriggerTypeSingle], 1)
	require.Equal(t, []int64{1}, segmentIDsFromViews(events[policy.TriggerTypeSingle][0].GetSegmentsView()))
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
	targetMeta := NewLoadedCompactionTargetMeta(t, ctx, record)
	mt := newCompactionTargetReconcilerTestMeta(t, targetMeta,
		sortedTargetSegment(1, 1, 10, "ch-1", 0, 199, false),
	)

	events, err := newCompactionTargetReconcilerForTest(mt).Reconcile(ctx)

	require.NoError(t, err)
	require.Len(t, events[policy.TriggerTypeSingle], 1)
	require.Equal(t, []int64{1}, segmentIDsFromViews(events[policy.TriggerTypeSingle][0].GetSegmentsView()))
}

func newCompactionTargetReconcilerForTest(fixture *compactionTargetReconcilerFixture) *CompactionTargetReconciler {
	return NewCompactionTargetReconciler(fixture.Meta, fixture.indexMeta, fixture.targetMeta, meta.NewMockHandlerWithMeta(fixture.Meta), fixture.allocator)
}

func enableCompactionTargetReconciler(t *testing.T) {
	t.Helper()
	paramtable.Get().Save(Params.DataCoordCfg.EnableTargetBasedCompaction.Key, "true")
	t.Cleanup(func() {
		paramtable.Get().Reset(Params.DataCoordCfg.EnableTargetBasedCompaction.Key)
	})
}

// compactionTargetReconcilerFixture bundles the meta and the domain metas that a reconciler under test reads.
type compactionTargetReconcilerFixture struct {
	*meta.Meta
	targetMeta *CompactionTargetMeta
	allocator  allocator.Allocator
	indexMeta  *index.IndexMeta
}

func newCompactionTargetReconcilerTestMeta(t *testing.T, targetMeta *CompactionTargetMeta, segments ...*meta.SegmentInfo) *compactionTargetReconcilerFixture {
	store := metacache.NewMetaStore(nil)
	mt := &meta.Meta{
		Segments:  meta.NewSegmentsInfo(store),
		MetaStore: store,
	}
	for _, record := range targetMeta.GetCompactionTargets() {
		mt.AddCollection(&meta.CollectionInfo{ID: record.GetCollectionID(), Schema: &schemapb.CollectionSchema{}})
	}
	for _, segment := range segments {
		mt.Segments.SetSegment(segment.GetID(), segment)
	}
	return &compactionTargetReconcilerFixture{
		Meta:       mt,
		targetMeta: targetMeta,
		allocator:  meta.NewMockAllocator(t),
		indexMeta: &index.IndexMeta{
			Indexes: make(map[UniqueID]map[UniqueID]*model.Index),
		},
	}
}

func sortedTargetSegment(id, collectionID, partitionID int64, channel string, createTS uint64, dataTS uint64, compacting bool, compactionFrom ...int64) *meta.SegmentInfo {
	segment := targetSegmentWithDataTS(id, collectionID, partitionID, channel, createTS, dataTS, compacting, compactionFrom...)
	segment.IsSorted = true
	return segment
}

func plannedTaskDescriptors(t *testing.T, views []view.TargetView) []*datapb.CompactionTask {
	t.Helper()
	var plans []*datapb.CompactionTask
	for _, v := range views {
		tasks, err := v.Plan()
		require.NoError(t, err)
		plans = append(plans, tasks...)
	}
	return plans
}

func segmentIDsFromViews(views []*view.SegmentView) []int64 {
	segmentIDs := make([]int64, 0, len(views))
	for _, cv := range views {
		segmentIDs = append(segmentIDs, cv.ID)
	}
	return segmentIDs
}

func TestCompactionTargetReconcilerStandingSortWithAutoCompactionOff(t *testing.T) {
	enableCompactionTargetReconciler(t)
	Params.Save(Params.DataCoordCfg.EnableAutoCompaction.Key, "false")
	Params.Save(Params.DataCoordCfg.EnableSortCompaction.Key, "true")
	t.Cleanup(func() {
		Params.Reset(Params.DataCoordCfg.EnableAutoCompaction.Key)
		Params.Reset(Params.DataCoordCfg.EnableSortCompaction.Key)
	})

	record := &datapb.CompactionTarget{
		TargetID: 100, CollectionID: 1, Intent: datapb.TargetIntent_INTENT_REWRITE, ExpectedTS: 200,
	}
	targetMeta := NewLoadedCompactionTargetMeta(t, t.Context(), record)
	unsorted := targetSegmentWithDataTS(1, 1, 10, "ch-1", 0, 199, false)
	l2 := targetSegmentWithDataTS(2, 1, 10, "ch-1", 0, 199, false)
	l2.Level = datapb.SegmentLevel_L2
	l2.IsInvisible = true
	namespaceSorted := targetSegmentWithDataTS(3, 1, 10, "ch-1", 0, 199, false)
	namespaceSorted.IsSortedByNamespace = true
	importing := targetSegmentWithDataTS(4, 1, 10, "ch-1", 0, 199, false)
	importing.IsImporting = true
	busy := targetSegmentWithDataTS(5, 1, 10, "ch-1", 0, 199, true)

	mt := newCompactionTargetReconcilerTestMeta(t, targetMeta, unsorted, l2, namespaceSorted, importing, busy)
	events, err := newCompactionTargetReconcilerForTest(mt).Reconcile(t.Context())
	require.NoError(t, err)
	var sorting, rewriting []int64
	for _, v := range events[policy.TriggerTypeSort] {
		sorting = append(sorting, segmentIDsFromViews(v.GetSegmentsView())...)
	}
	for _, v := range events[policy.TriggerTypeSingle] {
		rewriting = append(rewriting, segmentIDsFromViews(v.GetSegmentsView())...)
	}
	require.ElementsMatch(t, []int64{1, 2}, sorting)
	require.Equal(t, []int64{3}, rewriting)
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, targetMeta.GetCompactionTarget(100).GetState())
}

func TestCompactionTargetReconcilerAllocatesSchemaBeforeSize(t *testing.T) {
	ctx := t.Context()
	cfg := &Params.DataCoordCfg
	for key, value := range map[string]string{
		cfg.EnableAutoCompaction.Key:               "true",
		cfg.EnableSortCompaction.Key:               "true",
		cfg.BumpSchemaVersionCompactionEnabled.Key: "true",
		cfg.IndexBasedCompaction.Key:               "false",
		cfg.StorageVersionCompactionEnabled.Key:    "false",
		cfg.StorageFormatCompactionEnabled.Key:     "false",
		cfg.SegmentMaxSize.Key:                     "16",
		cfg.DiskSegmentMaxSize.Key:                 "16",
	} {
		require.NoError(t, Params.Save(key, value))
		t.Cleanup(func() { Params.Reset(key) })
	}

	const collectionID = int64(1)
	rewrite := &datapb.CompactionTarget{
		TargetID: 100, CollectionID: collectionID, Intent: datapb.TargetIntent_INTENT_REWRITE,
		ExpectedTS: 200, Rewrite: &datapb.CompactionTargetRewrite{RootSegmentIds: []int64{1, 2}},
	}
	optimize := &datapb.CompactionTarget{
		TargetID: 200, CollectionID: collectionID, Intent: datapb.TargetIntent_INTENT_OPTIMIZE,
		ExpectedTS: 200, ActivatedAtTS: 200, TailLimit: 1,
		Optimize: &datapb.CompactionTargetOptimize{InputTargetSizeBytes: 16 << 20, ActualTargetSizeBytes: 16 << 20},
	}
	manager := NewLoadedCompactionTargetMeta(t, ctx, rewrite, optimize)
	segment := func(id int64, sizeMiB int64, version int32, dataTS uint64, sorted bool) *meta.SegmentInfo {
		t.Helper()
		s := targetSegmentWithDataTS(id, collectionID, 10, "ch", 0, dataTS, false)
		s.IsSorted = sorted
		s.SchemaVersion = version
		s.StorageVersion = storage.StorageV3
		s.ManifestPath = "v3-manifest"
		s.Stats = &datapb.Statistics{InsertBinlogSize: sizeMiB << 20}
		return s
	}
	mt := newCompactionTargetReconcilerTestMeta(t, manager,
		segment(1, 3, 1, 199, false), // SORT outranks independent REWRITE, Schema and OPTIMIZE.
		segment(2, 6, 1, 199, true),  // REWRITE outranks Schema and OPTIMIZE.
		segment(3, 7, 1, 199, true),  // Only Schema and OPTIMIZE; cannot support segment 2.
		segment(4, 3, 2, 199, true),  // Remains eligible to support segment 2 in OPTIMIZE.
		segment(5, 7, 1, 201, true),  // Schema outranks SIZE outside OPTIMIZE scope.
		segment(6, 7, 2, 201, true),  // Pure SIZE inputs form their own full-size group.
		segment(7, 7, 2, 201, true),
	)
	mt.AddCollection(&meta.CollectionInfo{ID: collectionID, Schema: &schemapb.CollectionSchema{Version: 2}})
	reconciler := newCompactionTargetReconcilerForTest(mt)
	events, err := reconciler.Reconcile(ctx)
	require.NoError(t, err)
	inputs := make(map[int64]datapb.CompactionType)
	for _, pools := range events {
		for _, plan := range plannedTaskDescriptors(t, pools) {
			for _, id := range plan.GetInputSegments() {
				require.NotContains(t, inputs, id)
				inputs[id] = plan.GetType()
			}
		}
	}
	require.Equal(t, map[int64]datapb.CompactionType{
		1: datapb.CompactionType_SortCompaction,
		2: datapb.CompactionType_MixCompaction,
		3: datapb.CompactionType_BumpSchemaVersionCompaction,
		4: datapb.CompactionType_MixCompaction,
		5: datapb.CompactionType_BumpSchemaVersionCompaction,
		6: datapb.CompactionType_MixCompaction,
		7: datapb.CompactionType_MixCompaction,
	}, inputs)
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, manager.GetCompactionTarget(rewrite.GetTargetID()).GetState())
	require.Equal(t, datapb.TargetState_TARGET_STATE_ACTIVE, manager.GetCompactionTarget(optimize.GetTargetID()).GetState())
	rows, err := ObserveSchemaVersion(ctx, mt.Meta)
	require.NoError(t, err)
	require.EqualValues(t, 400, rows)
}
