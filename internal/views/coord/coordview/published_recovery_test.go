package coordview

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestPublishedSchemaRecoversWithoutCancellingTarget(t *testing.T) {
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
	require.EqualValues(t, 10, manager.views[targetVersion].View().GetMeta().GetTransformStartAfterTimetick(),
		"first target subscription must preserve the old serving buffer range")
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
	require.NoError(t, manager.AddPublishedRecovery(t.Context(), repair, oldVersion))
	manager.waitFlush()
	repairedVersion := testVersion(1, 1, 2)
	require.Equal(t, qviews.QueryViewStatePreparing, manager.views[targetVersion].State())
	require.Equal(t, qviews.QueryViewStatePreparing, manager.views[repairedVersion].State())
	require.NotNil(t, manager.PublishedRecovery(1).Preparing)

	simulateNodeResponse(t, syncer, qviews.NewQueryNode(2), repairedVersion, qviews.QueryViewStateReady)
	simulateNodeResponse(t, syncer, testSN, repairedVersion, qviews.QueryViewStateReady)
	simulateNodeResponse(t, syncer, testSN, repairedVersion, qviews.QueryViewStateUp)
	require.Equal(t, repairedVersion, manager.Stats().UpBySchema[0].Version)
	require.Equal(t, qviews.QueryViewStatePreparing, manager.views[targetVersion].State())
	require.Equal(t, qviews.QueryViewStateDropping, manager.views[oldVersion].State())

	manager.PublishSchema(1)
	manager.waitFlush()
	require.Nil(t, manager.PublishedRecovery(1))
	require.Error(t, manager.AddPublishedRecovery(t.Context(), repair, repairedVersion), "publication invalidates stale repair work")
}

func TestPublishAndReleaseCancelRetainedPreparation(t *testing.T) {
	for _, release := range []bool{false, true} {
		t.Run(map[bool]string{false: "publish", true: "release"}[release], func(t *testing.T) {
			old := testBuilder(1, 1, 1).SetLoadInfoVersion(99).Build()
			old.Meta.State = viewpb.QueryViewState_QueryViewStateUp
			manager := newTestManager(t, newMockCatalog(), newMockSyncer(), old)
			manager.PublishSchema(0)
			require.NoError(t, manager.AddPreparing(t.Context(), testBuilder(2, 1, 1).SetSchemaVersion(1)))
			require.NoError(t, manager.AddPublishedRecovery(t.Context(), testBuilder(1, 1, 1).SetLoadInfoVersion(99), testVersion(1, 1, 1)))
			manager.waitFlush()
			if release {
				require.NoError(t, manager.RequestRelease(t.Context()))
			} else {
				manager.PublishSchema(1)
				manager.waitFlush()
			}
			require.Equal(t, qviews.QueryViewStateDropping, manager.views[testVersion(1, 1, 2)].State())
			require.Nil(t, manager.PublishedRecovery(1))
		})
	}
}
