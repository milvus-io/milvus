package snview

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func publishReplicaView(t *testing.T, h *SNQueryViewHandler, resources *mockResourceManager, replica int64, vc string, version int64, schema int32, nodes ...int64) qviews.QueryViewKey {
	t.Helper()
	meta := buildHandlerTestMeta(version)
	meta.ReplicaId, meta.Vchannel, meta.SchemaVersion = replica, vc, schema
	var queryNodes []*viewpb.QueryViewOfQueryNode
	for _, id := range nodes {
		queryNodes = append(queryNodes, &viewpb.QueryViewOfQueryNode{NodeId: id})
	}
	view := qviews.NewFullQueryViewAtStreamingNode(meta, &viewpb.QueryViewOfStreamingNode{}, queryNodes)
	h.ApplyViews([]handler.ApplyView{{View: view}})
	acquired, ok := resources.getAcquired(view.QueryViewKey())
	require.True(t, ok)
	acquired.OnReady()
	up := proto.Clone(meta).(*viewpb.QueryViewMeta)
	up.State = viewpb.QueryViewState_QueryViewStateUp
	h.ApplyViews([]handler.ApplyView{{View: qviews.NewFullQueryViewAtStreamingNode(up, &viewpb.QueryViewOfStreamingNode{}, queryNodes)}})
	return view.QueryViewKey()
}

func TestReplicaSelectionRotatesWithinVChannelAndPinsActualView(t *testing.T) {
	useImmediateHandoff(t)
	resources := newMockResourceManager()
	h := recoverSNQueryViewHandler(t.Context(), testPChannel, newMockCatalog(), resources, nil)
	for _, replica := range []int64{11, 22} {
		publishReplicaView(t, h, resources, replica, testVChannel, 1, 0)
		publishReplicaView(t, h, resources, replica, testVChannel, 2, 0)
		publishReplicaView(t, h, resources, replica, testVChannel, 3, 1)
	}
	publishReplicaView(t, h, resources, 33, "another-vchannel", 100, 0)
	unknown := qviews.ShardID{ReplicaID: qviews.UnknownReplicaID, VChannel: testVChannel}
	var selected []int64
	for range 4 {
		lease, err := h.AcquireUpViewAtSchema(t.Context(), unknown, 0)
		require.NoError(t, err)
		selected = append(selected, lease.Meta.ReplicaId)
		require.EqualValues(t, 2, lease.Version.QueryVersion)
		require.Equal(t, testVChannel, lease.Meta.Vchannel)
		lease.Release()
	}
	require.Equal(t, []int64{11, 22, 11, 22}, selected)
	lease, err := h.AcquireLatestUpView(t.Context(), unknown)
	require.NoError(t, err)
	require.EqualValues(t, 3, lease.Version.QueryVersion)
	lease.Release()
	_, err = h.AcquireUpViewAtSchema(t.Context(), unknown, 2)
	require.True(t, viewerror.AsViewError(err).IsViewNotFound())
	lease, err = h.AcquireUpViewAtSchema(t.Context(), qviews.ShardID{ReplicaID: 22, VChannel: testVChannel}, 0)
	require.NoError(t, err)
	require.EqualValues(t, 22, lease.Meta.ReplicaId)
	lease.Release()
	_, err = h.AcquireLatestUpView(t.Context(), qviews.ShardID{ReplicaID: 99, VChannel: testVChannel})
	require.True(t, viewerror.AsViewError(err).IsViewNotFound(), "an explicit replica must not silently fall back")
}

func TestReplicaSelectionWakesWhenHealthyReplicaJoinsDuringRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		resources := newMockResourceManager()
		meta := buildHandlerTestMeta(1)
		meta.State = viewpb.QueryViewState_QueryViewStateUp
		h := recoverSNQueryViewHandler(t.Context(), testPChannel, newMockCatalog(), resources,
			[]*viewpb.QueryViewOfShard{{Meta: meta, StreamingNode: &viewpb.QueryViewOfStreamingNode{}}})
		unknown := qviews.ShardID{ReplicaID: qviews.UnknownReplicaID, VChannel: testVChannel}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan *QueryViewLease, 1)
		go func() {
			lease, err := h.AcquireUpViewAtSchema(ctx, unknown, 0)
			if err == nil {
				done <- lease
			}
		}()
		synctest.Wait()
		require.Empty(t, done)
		// Let the waiter observe the new replica before it is ready. The
		// membership event alone cannot announce its later transition to Up.
		joining := buildHandlerTestMeta(2)
		joining.ReplicaId = testReplicaID + 1
		view := qviews.NewQueryViewAtStreamingNode(joining, &viewpb.QueryViewOfStreamingNode{})
		h.ApplyViews([]handler.ApplyView{{View: view}})
		synctest.Wait()
		require.Empty(t, done)
		acquired, ok := resources.getAcquired(view.QueryViewKey())
		require.True(t, ok)
		acquired.OnReady()
		synctest.Wait()
		require.Empty(t, done)
		up := proto.Clone(joining).(*viewpb.QueryViewMeta)
		up.State = viewpb.QueryViewState_QueryViewStateUp
		h.ApplyViews([]handler.ApplyView{{View: qviews.NewQueryViewAtStreamingNode(up, &viewpb.QueryViewOfStreamingNode{})}})
		synctest.Wait()
		select {
		case lease := <-done:
			require.EqualValues(t, testReplicaID+1, lease.Meta.ReplicaId)
			lease.Release()
		default:
			t.Fatal("healthy replica must wake a request waiting on another replica's recovery")
		}
	})
}

func TestReplicaSelectionRecoveryHonorsCancellation(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	meta.State = viewpb.QueryViewState_QueryViewStateUp
	h := recoverSNQueryViewHandler(t.Context(), testPChannel, newMockCatalog(), newMockResourceManager(),
		[]*viewpb.QueryViewOfShard{{Meta: meta, StreamingNode: &viewpb.QueryViewOfStreamingNode{}}})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	_, err := h.AcquireLatestUpView(ctx, qviews.ShardID{ReplicaID: qviews.UnknownReplicaID, VChannel: testVChannel})
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestReplicaIndexRemovesOnlyMatchingShardIncarnation(t *testing.T) {
	h := recoverSNQueryViewHandler(t.Context(), testPChannel, newMockCatalog(), newMockResourceManager(), nil)
	id := qviews.ShardID{ReplicaID: 11, VChannel: testVChannel}
	old := h.getOrCreateShard(id)
	require.Same(t, old, h.getOrCreateShard(id))
	changed := h.vchannels[testVChannel].changed
	h.makeOnEmpty(id)(old)
	select {
	case <-changed:
	default:
		t.Fatal("removal must wake recovery waiters")
	}
	require.Empty(t, h.vchannels)
	current := h.getOrCreateShard(id)
	h.makeOnEmpty(id)(old)
	require.Same(t, current, h.shards[id])
	require.Equal(t, []*snShardView{current}, h.vchannels[testVChannel].shards)
	h.makeOnEmpty(id)(current)
	h.CloseForHandoff()
	require.Nil(t, h.getOrCreateShard(id))
}

func TestReplicaRoutingExcludesBeforeLeaseAndNeverFallsBackToOldVersion(t *testing.T) {
	resources := newMockResourceManager()
	h := recoverSNQueryViewHandler(t.Context(), testPChannel, newMockCatalog(), resources, nil)
	publishReplicaView(t, h, resources, 11, testVChannel, 1, 0, 101)
	latest := publishReplicaView(t, h, resources, 11, testVChannel, 2, 0, 101)
	publishReplicaView(t, h, resources, 22, testVChannel, 3, 0, 102)
	constraints := &viewpb.QueryRoutingConstraints{BypassHandoff: true, ExcludedQueryViews: []*viewpb.QueryViewIdentity{{ReplicaId: 11, Version: latest.QueryViewVersion.IntoProto()}}}
	schema := int32(0)
	for range 3 {
		lease, err := h.AcquireUpViewForRouting(t.Context(), testVChannel, &schema, constraints)
		require.NoError(t, err)
		require.EqualValues(t, 22, lease.Meta.ReplicaId)
		lease.Release()
	}
	entry := h.shards[latest.ShardID].views[latest.QueryViewVersion]
	require.Zero(t, entry.queryRefs)
	require.True(t, entry.leaseExpireAt.IsZero(), "excluded View must not receive a serving lease")
	constraints.ExcludedQueryNodeIds = []int64{102}
	_, err := h.AcquireUpViewForRouting(t.Context(), testVChannel, &schema, constraints)
	require.True(t, viewerror.AsViewError(err).IsAllCandidatesExcluded())
	require.False(t, viewerror.AsViewError(err).IsViewNotFound())
	// A newer exact-schema View in the excluded replica is a separate identity.
	publishReplicaView(t, h, resources, 11, testVChannel, 4, 0, 103)
	lease, err := h.AcquireUpViewForRouting(t.Context(), testVChannel, &schema, constraints)
	require.NoError(t, err)
	require.EqualValues(t, 11, lease.Meta.ReplicaId)
	require.EqualValues(t, 4, lease.Version.QueryVersion)
	lease.Release()
}
