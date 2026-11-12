package querycoordv2

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr/loadmgrtest"
	"github.com/milvus-io/milvus/internal/views/coord/readiness"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func newReleaseTestServer(t *testing.T) *Server {
	t.Helper()
	registry, err := coordview.RecoverShardViewRegistry(t.Context(), &fakeQueryViewCatalog{}, nil)
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	changes := readiness.NewNotifications()
	t.Cleanup(changes.Close)
	return &Server{qviewsRuntime: &qviewsRuntime{
		shardViewRegistry: registry, balancer: &fakeRuntimeBalancer{}, loadManager: &loadmgr.CollectionLoadManager{}, readyChanges: changes,
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
				mockey.Mock((*fakeRuntimeBalancer).TriggerWithCompletion).To(func(_ *fakeRuntimeBalancer, scopes ...balancer.TriggerScope) *syncutil.Future[error] {
					require.Equal(t, []balancer.TriggerScope{{DirtyCollections: []int64{100}}}, scopes)
					completion := syncutil.NewFuture[error]()
					if scenario == "before subscription" {
						completion.Set(manager.RequestRelease(ctx))
					} else {
						completion.Set(nil)
					}
					return completion
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
	completion := syncutil.NewFuture[error]()
	s.qviewsRuntime.balancer = &fakeRuntimeBalancer{completion: completion}
	registry := s.qviewsRuntime.shardViewRegistry
	first := registry.Ensure(qviews.ShardID{ReplicaID: 1, VChannel: "p_100v0"})
	second := registry.Ensure(qviews.ShardID{ReplicaID: 2, VChannel: "p_200v0"})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	firstDone, repeatedDone, secondDone := make(chan error, 1), make(chan error, 1), make(chan error, 1)
	go func() { firstDone <- s.waitQueryViewsReleased(ctx, 100) }()
	go func() { repeatedDone <- s.waitQueryViewsReleased(ctx, 100) }()
	go func() { secondDone <- s.waitQueryViewsReleased(ctx, 200) }()
	canceledCtx, cancelWaiter := context.WithCancel(ctx)
	defer cancelWaiter()
	canceledDone := make(chan error, 1)
	go func() { canceledDone <- s.waitQueryViewsReleased(canceledCtx, 100) }()
	assertReadinessBlocked(t, canceledDone)
	cancelWaiter()
	require.ErrorIs(t, <-canceledDone, context.Canceled)
	require.False(t, completion.Ready())
	assertReadinessBlocked(t, firstDone)
	assertReadinessBlocked(t, repeatedDone)
	assertReadinessBlocked(t, secondDone)
	completion.Set(nil)
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
	completion := syncutil.NewFuture[error]()
	s.qviewsRuntime.balancer = &fakeRuntimeBalancer{completion: completion}
	done := make(chan error, 1)
	go func() { done <- s.waitQueryViewsReleased(ctx, 100) }()
	assertReadinessBlocked(t, done)
	// Simulate an old prepare applied after withdrawal but before the release
	// batch finishes. An initially empty registry must not complete the wait.
	manager := s.qviewsRuntime.shardViewRegistry.Ensure(qviews.ShardID{ReplicaID: 1, VChannel: "p_100v0"})
	completion.Set(nil)
	select {
	case err := <-done:
		t.Fatalf("missed in-flight preparation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	require.NoError(t, manager.RequestRelease(ctx))
	require.NoError(t, <-done)
}

func TestQueryViewReleaseCancellationAndReconcileError(t *testing.T) {
	for _, phase := range []string{"batch", "final removal"} {
		t.Run(phase, func(t *testing.T) {
			s := newReleaseTestServer(t)
			s.qviewsRuntime.shardViewRegistry.Ensure(qviews.ShardID{ReplicaID: 1, VChannel: "p_100v0"})
			completion := syncutil.NewFuture[error]()
			s.qviewsRuntime.balancer = &fakeRuntimeBalancer{completion: completion}
			if phase == "final removal" {
				completion.Set(nil)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- s.waitQueryViewsReleased(ctx, 100) }()
			assertReadinessBlocked(t, done)
			cancel()
			require.ErrorIs(t, <-done, context.Canceled)
			if phase == "batch" {
				require.False(t, completion.Ready(), "canceling a waiter must not finish the shared batch")
				completion.Set(merr.ErrServiceUnavailable)
				err := s.waitQueryViewsReleased(t.Context(), 100)
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
				require.Equal(t, merr.Code(merr.ErrServiceUnavailable), merr.Code(err))
			}
		})
	}
}

func TestQueryViewReleaseRuntimeShutdown(t *testing.T) {
	for _, phase := range []string{"batch", "already stopped"} {
		t.Run(phase, func(t *testing.T) {
			s := newReleaseTestServer(t)
			runtime := s.qviewsRuntime
			runtime.shardViewRegistry.Ensure(qviews.ShardID{ReplicaID: 1, VChannel: "p_100v0"})
			completion := syncutil.NewFuture[error]()
			runtime.balancer = &fakeRuntimeBalancer{completion: completion}
			runtime.syncer = &readinessSyncer{}
			if phase == "already stopped" {
				runtime.stop()
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- s.waitQueryViewsReleased(ctx, 100) }()
			if phase != "already stopped" {
				assertReadinessBlocked(t, done)
				runtime.stop()
			}
			require.ErrorIs(t, <-done, merr.ErrServiceUnavailable)
			require.NoError(t, ctx.Err(), "shutdown must wake the waiter without canceling its request")
		})
	}
}

func TestDropLoadConfigAckWaitsAfterDesiredStateRemoval(t *testing.T) {
	s := newReleaseTestServer(t)
	deps := loadmgrtest.Dependencies(t)
	store, err := loadmgr.RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	require.NoError(t, store.Put(t.Context(), &loadmgr.LoadConfig{CollectionID: 100, PartitionIDs: []int64{10}, Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 1}}}))
	version := store.GetConfigVersion(100)
	s.qviewsRuntime.loadConfigStore = store
	removed := make(chan struct{}, 1)
	s.qviewsRuntime.loadManager = loadmgr.NewCollectionLoadManager(store, func(int64) { removed <- struct{}{} }, nil)
	manager := s.qviewsRuntime.shardViewRegistry.Ensure(qviews.ShardID{ReplicaID: 1, VChannel: "p_100v0"})
	previousCache := meta.GlobalFailedLoadCache
	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()
	t.Cleanup(func() { meta.GlobalFailedLoadCache = previousCache })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.dropLoadConfigV2AckCallback(ctx, buildDropLoadConfigBroadcastResult(100)) }()
	<-removed
	require.False(t, store.Contains(100), "release must stop planning before waiting for cleanup")
	_, err = store.GetLoadInfo(ctx, 100, version)
	require.NoError(t, err, "node cleanup still needs the historical load requirements")
	records, err := deps.Catalog.List(ctx)
	require.NoError(t, err)
	require.Len(t, records, 1)
	select {
	case err := <-done:
		t.Fatalf("ack completed before node cleanup: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	require.NoError(t, manager.RequestRelease(ctx))
	require.NoError(t, <-done)
	records, err = deps.Catalog.List(ctx)
	require.NoError(t, err)
	require.Empty(t, records, "ACK must follow durable removal of all load versions")
	_, err = store.GetLoadInfo(ctx, 100, version)
	require.Error(t, err)
}

func TestQueryViewRepeatedReleaseWaitsWithoutLoadConfig(t *testing.T) {
	s := newReleaseTestServer(t)
	s.meta = newViewMeta(&metacache.CollectionInfo{ID: 100})
	s.qviewsRuntime.loadConfigStore = &loadmgr.LoadConfigStore{}
	manager := s.qviewsRuntime.shardViewRegistry.Ensure(qviews.ShardID{ReplicaID: 1, VChannel: "p_100v0"})
	for _, patch := range []*mockey.Mocker{
		mockey.Mock((*Server).startBroadcastWithCollectionIDLock).Return(stubBroadcaster{}, nil).Build(),
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
