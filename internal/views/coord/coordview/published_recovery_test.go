package coordview

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestPublishedRecoveryCannotReplaceNewerDataView(t *testing.T) {
	old := testBuilder(1, 1, 1).SetLoadInfoVersion(99).Build()
	old.Meta.State = viewpb.QueryViewState_QueryViewStateUp
	old.Meta.TransformStartAfterTimetick = 10
	catalog, syncer := newMockCatalog(), newMockSyncer()
	manager := newTestManager(t, catalog, syncer, old)
	manager.PublishSchema(0)
	oldVersion := testVersion(1, 1, 1)

	target := qviews.NewQueryViewAtCoordBuilder(testReplicaID, &viewpb.DataViewOfCollection{
		CollectionId: testCollectionID,
		DataVersion:  testVersion(2, 1, 1).DataVersion.IntoProto(),
		Shards: []*viewpb.DataViewOfShard{{
			Vchannel:                           testVChannel,
			GrowingTransformStartAfterTimetick: 50, CompactedTransformStartAfterTimetick: 50,
		}},
	}, testVChannel).SetSchemaVersion(1).SetLoadInfoVersion(100).
		SetAssignments(map[int64]map[int64][]int64{2: {10: {2001}}})
	require.NoError(t, manager.AddPreparing(t.Context(), target))
	targetVersion := testVersion(2, 1, 1)
	require.EqualValues(t, 50, manager.views[targetVersion].View().GetMeta().GetTransformStartAfterTimetick(),
		"target retains its own frontier; older views retain their own resources")
	lost := manager.makeOnQueryNodeLost(oldVersion)
	require.NotNil(t, lost)
	lost(testQN1)
	// The serving SN reports the failed view after its QN disappears.
	manager.makeOnSyncResponse(oldVersion, snReport(old, qviews.QueryViewStateUp))(snReport(old, qviews.QueryViewStateUnrecoverable))
	manager.waitFlush()
	require.Equal(t, qviews.QueryViewStateUnrecoverable, manager.views[oldVersion].State())

	repair := testBuilder(1, 1, 1).SetLoadInfoVersion(99).
		SetAssignments(map[int64]map[int64][]int64{2: {10: {1001}}})
	require.ErrorIs(t, manager.AddPreparing(t.Context(), repair), errDataVersionRollback,
		"ordinary DataView planning still cannot move backwards")
	require.ErrorIs(t, manager.AddPublishedRecovery(t.Context(), repair, oldVersion), errDataVersionRollback)
	require.Nil(t, manager.PublishedRecovery(1), "recovery must not select the old schema behind target preparation")
	require.Equal(t, qviews.QueryViewStatePreparing, manager.views[targetVersion].State())

	// A failed target still owns the newest DataVersion. It must be replaced
	// at that version or later, rather than falling back to the old schema.
	simulateNodeResponse(t, syncer, qviews.NewQueryNode(2), targetVersion, qviews.QueryViewStateUnrecoverable)
	require.Equal(t, qviews.QueryViewStateUnrecoverable, manager.views[targetVersion].State())
	require.Nil(t, manager.PublishedRecovery(1))
	require.ErrorIs(t, manager.AddPublishedRecovery(t.Context(), repair, oldVersion), errDataVersionRollback)
	require.NoError(t, manager.AddPreparing(t.Context(), target))
	manager.waitFlush()
	require.Equal(t, testVersion(2, 1, 2), *manager.Stats().PreparingVersion)
	require.Equal(t, qviews.QueryViewStateDropping, manager.views[targetVersion].State())
}

func TestTargetPreemptsPublishedRecovery(t *testing.T) {
	for _, ready := range []bool{false, true} {
		t.Run(map[bool]string{false: "preparing", true: "ready"}[ready], func(t *testing.T) {
			old := testBuilder(1, 1, 1).SetLoadInfoVersion(99).Build()
			old.Meta.State = viewpb.QueryViewState_QueryViewStateUnrecoverable
			syncer := newMockSyncer()
			manager := newTestManager(t, newMockCatalog(), syncer, old)
			manager.PublishSchema(0)
			source := testVersion(1, 1, 1)
			require.Equal(t, source.IntoProto(), manager.PublishedRecovery(1).View.Meta.Version)
			repair := testBuilder(1, 1, 1).SetLoadInfoVersion(99)
			require.NoError(t, manager.AddPublishedRecovery(t.Context(), repair, source))
			manager.waitFlush()
			repaired := testVersion(1, 1, 2)
			require.Equal(t, qviews.QueryViewStateDropping, manager.views[source].State())
			require.Equal(t, repaired.IntoProto(), manager.PublishedRecovery(1).View.Meta.Version)
			require.Error(t, manager.AddPublishedRecovery(t.Context(), repair, source), "a stale source cannot replace the latest view even at the same DataVersion")
			if ready {
				simulateNodeResponse(t, syncer, testQN1, repaired, qviews.QueryViewStateReady)
				simulateNodeResponse(t, syncer, testSN, repaired, qviews.QueryViewStateReady)
				require.Equal(t, qviews.QueryViewStateReady, manager.views[repaired].State())
			}
			require.NoError(t, manager.AddPreparing(t.Context(), testBuilder(2, 1, 1).SetSchemaVersion(1)))
			manager.waitFlush()
			require.Equal(t, qviews.QueryViewStateDropping, manager.views[repaired].State())
			require.Equal(t, testVersion(2, 1, 1), *manager.Stats().PreparingVersion)
			require.Nil(t, manager.PublishedRecovery(1))
		})
	}
}

func TestOlderUpReportRetainsLatestUnrecoverable(t *testing.T) {
	old := testBuilder(1, 1, 1).Build()
	old.Meta.State = viewpb.QueryViewState_QueryViewStatePreparing
	latest := testBuilder(2, 1, 1).SetSchemaVersion(1).Build()
	latest.Meta.State = viewpb.QueryViewState_QueryViewStateUnrecoverable
	syncer := newMockSyncer()
	manager := newTestManager(t, newMockCatalog(), syncer, old, latest)
	manager.PublishSchema(0)
	version := testVersion(1, 1, 1)
	simulateNodeResponse(t, syncer, testQN1, version, qviews.QueryViewStateReady)
	simulateNodeResponse(t, syncer, testSN, version, qviews.QueryViewStateReady)
	simulateNodeResponse(t, syncer, testSN, version, qviews.QueryViewStateUp)
	manager.waitFlush()
	require.Equal(t, qviews.QueryViewStateUnrecoverable, manager.views[testVersion(2, 1, 1)].State())
	require.Nil(t, manager.PublishedRecovery(1), "a late old Up cannot revive recovery of its base")
}

func TestPublishedRecoveryRechecksAfterReferenceAcquisition(t *testing.T) {
	started, unblock := make(chan struct{}), make(chan struct{})
	unblockOnce := sync.OnceFunc(func() { close(unblock) })
	defer unblockOnce()
	var refs atomic.Int32
	patch := mockey.Mock((*stubLoadInfoRefs).AcquireLoadInfo).To(func(_ context.Context, _ int64, version uint64) (func(), error) {
		if version != 99 {
			return func() {}, nil
		}
		refs.Add(1)
		close(started)
		<-unblock
		return sync.OnceFunc(func() { refs.Add(-1) }), nil
	}).Build()
	defer patch.UnPatch()
	old := testBuilder(1, 1, 1).SetLoadInfoVersion(99).Build()
	old.Meta.State = viewpb.QueryViewState_QueryViewStateUp
	manager := newTestManager(t, newMockCatalog(), newMockSyncer(), old)
	manager.PublishSchema(0)
	manager.loadInfoRefs = &stubLoadInfoRefs{}
	done := make(chan error, 1)
	go func() {
		done <- manager.AddPublishedRecovery(t.Context(), testBuilder(1, 1, 1).SetLoadInfoVersion(99), testVersion(1, 1, 1))
	}()
	awaitReferenceOperation(t, started)
	err := manager.AddPreparing(t.Context(), testBuilder(2, 1, 1).SetSchemaVersion(1).SetLoadInfoVersion(100))
	unblockOnce()
	require.NoError(t, err)
	require.ErrorIs(t, awaitReferenceOperation(t, done), errDataVersionRollback)
	require.Zero(t, refs.Load(), "rejected recovery must release its acquired metadata")
	require.Equal(t, testVersion(2, 1, 1), *manager.Stats().PreparingVersion)
}

func TestPublishAndReleaseCancelRetainedPreparation(t *testing.T) {
	for _, release := range []bool{false, true} {
		t.Run(map[bool]string{false: "publish", true: "release"}[release], func(t *testing.T) {
			old := testBuilder(1, 1, 1).SetLoadInfoVersion(99).Build()
			old.Meta.State = viewpb.QueryViewState_QueryViewStateUp
			syncer := newMockSyncer()
			manager := newTestManager(t, newMockCatalog(), syncer, old)
			manager.PublishSchema(0)
			require.NoError(t, manager.AddPublishedRecovery(t.Context(), testBuilder(1, 1, 1).SetLoadInfoVersion(99), testVersion(1, 1, 1)))
			manager.waitFlush()
			if release {
				require.NoError(t, manager.RequestRelease(t.Context()))
			} else {
				// Publication follows target Up; preparing the target supersedes
				// recovery rather than keeping a second preparation slot.
				require.NoError(t, manager.AddPreparing(t.Context(), testBuilder(2, 1, 1).SetSchemaVersion(1)))
				target := testVersion(2, 1, 1)
				simulateNodeResponse(t, syncer, testQN1, target, qviews.QueryViewStateReady)
				simulateNodeResponse(t, syncer, testSN, target, qviews.QueryViewStateReady)
				simulateNodeResponse(t, syncer, testSN, target, qviews.QueryViewStateUp)
				manager.PublishSchema(1)
				manager.waitFlush()
			}
			require.Equal(t, qviews.QueryViewStateDropping, manager.views[testVersion(1, 1, 2)].State())
			require.Nil(t, manager.PublishedRecovery(1))
		})
	}
}
