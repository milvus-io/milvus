package querycoordv2

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newReleaseTestServer(t *testing.T) *Server {
	t.Helper()
	registry, err := coordview.RecoverShardViewRegistry(t.Context(), &fakeQueryViewCatalog{}, nil)
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	return &Server{qviewsRuntime: &qviewsRuntime{
		shardViewRegistry: registry, balancer: &fakeRuntimeBalancer{}, loadManager: &loadmgr.CollectionLoadManager{},
	}}
}

func TestQueryViewReleaseWaitsForFinalRemoval(t *testing.T) {
	s := newReleaseTestServer(t)
	registry := s.qviewsRuntime.shardViewRegistry
	first := registry.Ensure(qviews.ShardID{ReplicaID: 1, VChannel: "p_100v0"})
	second := registry.Ensure(qviews.ShardID{ReplicaID: 2, VChannel: "p_100v0"})
	other := qviews.ShardID{ReplicaID: 3, VChannel: "p_200v0"}
	registry.Ensure(other)
	// Empty stats are not completion: resident managers must be removed.
	require.Nil(t, first.Stats().UpVersion)
	require.Empty(t, first.Stats().Segments)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.waitQueryViewsReleased(ctx, 100) }()
	select {
	case err := <-done:
		t.Fatalf("release completed before cleanup: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	require.NoError(t, first.RequestRelease(ctx))
	select {
	case err := <-done:
		t.Fatalf("release completed with a resident replica: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	require.NoError(t, second.RequestRelease(ctx))
	require.NoError(t, <-done)
	require.NotNil(t, registry.Get(other), "other collections do not block release")
	require.NoError(t, s.waitQueryViewsReleased(ctx, 100), "repeated release is idempotent")
}

func TestQueryViewReleaseRemovalSubscriptionLifecycle(t *testing.T) {
	for _, scenario := range []string{"before subscription", "after subscription", "during initial check", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			s := newReleaseTestServer(t)
			registry := s.qviewsRuntime.shardViewRegistry
			manager := registry.Ensure(qviews.ShardID{ReplicaID: 1, VChannel: "p_100v0"})
			other := qviews.ShardID{ReplicaID: 2, VChannel: "p_200v0"}
			registry.Ensure(other)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			registered, unregistered := 0, 0
			var register func(*coordview.ShardViewRegistry, int64, func()) func()
			var collectionShards func(*coordview.ShardViewRegistry, int64) []qviews.ShardID
			patches := []*mockey.Mocker{
				mockey.Mock((*coordview.ShardViewRegistry).RegisterPublicationListener).To(func(*coordview.ShardViewRegistry, coordview.ShardPublicationListener) func() {
					t.Error("release must not subscribe to global publications or replay")
					return func() {}
				}).Build(),
				mockey.Mock((*fakeRuntimeBalancer).Reconcile).To(func(_ *fakeRuntimeBalancer, ctx context.Context) error {
					if scenario == "before subscription" {
						return manager.RequestRelease(ctx)
					}
					return nil
				}).Build(),
				mockey.Mock((*coordview.ShardViewRegistry).RegisterShardRemovalListener).To(func(reg *coordview.ShardViewRegistry, collectionID int64, listener func()) func() {
					require.Equal(t, int64(100), collectionID)
					stop := register(reg, collectionID, listener)
					registered++
					switch scenario {
					case "after subscription":
						require.NoError(t, manager.RequestRelease(ctx))
					case "cancel":
						cancel()
					}
					return func() {
						stop()
						unregistered++
					}
				}).Origin(&register).Build(),
				mockey.Mock((*coordview.ShardViewRegistry).CollectionShards).To(func(reg *coordview.ShardViewRegistry, collectionID int64) []qviews.ShardID {
					shards := collectionShards(reg, collectionID)
					if scenario == "during initial check" && len(shards) > 0 {
						// Return the stale nonempty result after final removal. The
						// queued notification must wake the subsequent wait.
						require.NoError(t, manager.RequestRelease(ctx))
					}
					return shards
				}).Origin(&collectionShards).Build(),
			}
			defer func() {
				for _, patch := range patches {
					patch.UnPatch()
				}
			}()

			err := s.waitQueryViewsReleased(ctx, 100)
			if scenario == "cancel" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, registered)
			require.Equal(t, 1, unregistered, "success and cancellation must both unsubscribe")
			require.NotNil(t, registry.Get(other), "other collections must remain untouched")
		})
	}
}

func TestQueryViewReleaseConcurrentWaiters(t *testing.T) {
	s := newReleaseTestServer(t)
	registry := s.qviewsRuntime.shardViewRegistry
	first := registry.Ensure(qviews.ShardID{ReplicaID: 1, VChannel: "p_100v0"})
	second := registry.Ensure(qviews.ShardID{ReplicaID: 2, VChannel: "p_200v0"})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	firstDone, repeatedDone, secondDone := make(chan error, 1), make(chan error, 1), make(chan error, 1)
	go func() { firstDone <- s.waitQueryViewsReleased(ctx, 100) }()
	go func() { repeatedDone <- s.waitQueryViewsReleased(ctx, 100) }()
	go func() { secondDone <- s.waitQueryViewsReleased(ctx, 200) }()
	assertReadinessBlocked(t, firstDone)
	assertReadinessBlocked(t, repeatedDone)
	assertReadinessBlocked(t, secondDone)
	require.NoError(t, first.RequestRelease(ctx))
	require.NoError(t, <-firstDone)
	require.NoError(t, <-repeatedDone, "all waiters of the same collection must complete")
	assertReadinessBlocked(t, secondDone)
	require.NoError(t, second.RequestRelease(ctx))
	require.NoError(t, <-secondDone)
}

func TestQueryViewReleaseDrainsInFlightPreparation(t *testing.T) {
	s := newReleaseTestServer(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	applied := make(chan *coordview.ShardViewManager, 1)
	patch := mockey.Mock((*fakeRuntimeBalancer).Reconcile).To(func(_ *fakeRuntimeBalancer, _ context.Context) error {
		// Simulate a prepare that started before desired-state removal, but was
		// not applied until the release barrier drained the in-flight pass.
		applied <- s.qviewsRuntime.shardViewRegistry.Ensure(qviews.ShardID{ReplicaID: 1, VChannel: "p_100v0"})
		return nil
	}).Build()
	defer patch.UnPatch()
	done := make(chan error, 1)
	go func() { done <- s.waitQueryViewsReleased(ctx, 100) }()
	manager := <-applied
	select {
	case err := <-done:
		t.Fatalf("missed in-flight preparation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	require.NoError(t, manager.RequestRelease(ctx))
	require.NoError(t, <-done)
}

func TestQueryViewReleaseCancellationAndReconcileError(t *testing.T) {
	s := newReleaseTestServer(t)
	s.qviewsRuntime.shardViewRegistry.Ensure(qviews.ShardID{ReplicaID: 1, VChannel: "p_100v0"})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- s.waitQueryViewsReleased(ctx, 100) }()
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	patch := mockey.Mock((*fakeRuntimeBalancer).Reconcile).Return(merr.ErrServiceUnavailable).Build()
	defer patch.UnPatch()
	require.ErrorIs(t, s.waitQueryViewsReleased(t.Context(), 100), merr.ErrServiceUnavailable)
}

func TestDropLoadConfigAckWaitsAfterDesiredStateRemoval(t *testing.T) {
	s := newReleaseTestServer(t)
	manager := s.qviewsRuntime.shardViewRegistry.Ensure(qviews.ShardID{ReplicaID: 1, VChannel: "p_100v0"})
	removed := make(chan struct{}, 1)
	patch := mockey.Mock((*loadmgr.CollectionLoadManager).ReleaseCollection).To(func(_ *loadmgr.CollectionLoadManager, _ context.Context, _ *messagespb.DropLoadConfigMessageHeader) error {
		removed <- struct{}{}
		return nil
	}).Build()
	defer patch.UnPatch()
	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.dropLoadConfigV2AckCallback(ctx, buildDropLoadConfigBroadcastResult(100)) }()
	<-removed
	select {
	case err := <-done:
		t.Fatalf("ack completed before node cleanup: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	require.NoError(t, manager.RequestRelease(ctx))
	require.NoError(t, <-done)
}

func TestQueryViewRepeatedReleaseWaitsWithoutLoadConfig(t *testing.T) {
	s := newReleaseTestServer(t)
	s.broker = &meta.CoordinatorBroker{}
	s.qviewsRuntime.loadConfigStore = &loadmgr.LoadConfigStore{}
	manager := s.qviewsRuntime.shardViewRegistry.Ensure(qviews.ShardID{ReplicaID: 1, VChannel: "p_100v0"})
	for _, patch := range []*mockey.Mocker{
		mockey.Mock((*Server).startBroadcastWithCollectionIDLock).Return(stubBroadcaster{}, nil).Build(),
		mockey.Mock((*meta.CoordinatorBroker).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{CollectionID: 100}, nil).Build(),
		mockey.Mock((*loadmgr.LoadConfigStore).Contains).Return(false).Build(),
	} {
		t.Cleanup(func() { patch.UnPatch() })
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- s.broadcastDropLoadConfigCollectionV2ForReleaseCollection(ctx, &querypb.ReleaseCollectionRequest{CollectionID: 100})
	}()
	select {
	case err := <-done:
		t.Fatalf("repeated release completed before cleanup: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	require.NoError(t, manager.RequestRelease(ctx))
	require.ErrorIs(t, <-done, errReleaseCollectionNotLoaded)
}
