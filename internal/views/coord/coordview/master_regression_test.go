package coordview

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRegistryRejectsPrepareThroughDetachedManager(t *testing.T) {
	catalog, syncer := newMockCatalog(), newMockSyncer()
	registry := newTestRegistry(t, catalog, syncer)
	old := registry.Ensure(testShardID) // A caller pauses between Ensure and AddPreparing.
	require.NoError(t, old.RequestRelease(t.Context()))
	require.Nil(t, registry.Get(testShardID))
	require.ErrorIs(t, old.AddPreparing(t.Context(), testBuilder(1, 1, 1)), merr.ErrServiceUnavailable)
	require.Zero(t, catalog.numSaveCalls())
	require.Zero(t, syncer.syncViewCount())
	replacement := registry.Ensure(testShardID)
	require.NotSame(t, old, replacement)
	require.NoError(t, replacement.AddPreparing(t.Context(), testBuilder(1, 1, 1)))
	require.NoError(t, registry.flushScheduler.Flush(t.Context()))
	require.Equal(t, 1, catalog.numSaveCalls())
}

func TestRegistryFailedFirstPrepareRetiresEmptyManager(t *testing.T) {
	registry := newTestRegistry(t, newMockCatalog(), newMockSyncer())
	id := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_100v0"}
	manager := registry.Ensure(id)
	var last *ShardStats
	stop := registry.RegisterPublicationListener(func(_ qviews.ShardID, stats *ShardStats) { last = stats })
	defer stop()
	require.NotNil(t, last)
	patch := mockey.Mock((*stubDataViewRefProvider).Get).Return(nil, nil).Build()
	defer patch.UnPatch()
	require.Error(t, manager.AddPreparing(t.Context(), testBuilderForShard(100, id)))
	require.Nil(t, registry.Get(id))
	require.Nil(t, last)
	require.Empty(t, registry.CollectionShards(100))
	require.Empty(t, registry.Snapshot().StatsMap())
	require.ErrorIs(t, manager.AddPreparing(t.Context(), testBuilderForShard(100, id)), merr.ErrServiceUnavailable)
}

func TestRegistryRecoveryRetainsValidFailedViewUntilReplacement(t *testing.T) {
	catalog, syncer := newMockCatalog(), newMockSyncer()
	old := buildTestViewWithVersion(1, 1, 1, 1)
	old.Meta.State = viewpb.QueryViewState_QueryViewStateUnrecoverable
	catalog.listed = []*viewpb.QueryViewOfShard{old}
	registry, err := RecoverShardViewRegistryWithDataViews(t.Context(), catalog, syncer, stubDataViewRefProvider{})
	require.NoError(t, err)
	defer registry.Close()
	manager := registry.Get(qviews.NewShardIDFromQVMeta(old.Meta))
	version := testVersion(1, 1, 1)
	manager.mu.Lock()
	require.Equal(t, qviews.QueryViewStateUnrecoverable, manager.views[version].State())
	require.NotNil(t, manager.views[version].Ref())
	manager.mu.Unlock()
	require.Zero(t, syncer.syncViewCount(), "recovery must not release protected resources before a successor exists")
	require.NoError(t, manager.AddPreparing(t.Context(), testBuilder(1, 1, 1)))
	require.NoError(t, registry.flushScheduler.Flush(t.Context()))
	manager.mu.Lock()
	defer manager.mu.Unlock()
	require.Equal(t, qviews.QueryViewStateDropping, manager.views[version].State())
	require.Equal(t, qviews.QueryViewStatePreparing, manager.views[testVersion(1, 1, 2)].State())
}

func TestRecoveryAbortSerializesWithLateSyncCallbacks(t *testing.T) {
	var derefs atomic.Int32
	patch := mockey.Mock((stubDataViewRef).Deref).To(func(stubDataViewRef) { derefs.Add(1) }).Build()
	defer patch.UnPatch()
	submitter := &capturedDirtyViewEventSubmitter{}
	manager := newShardViewManager(t.Context(), testShardID, submitter, nil, stubDataViewRefProvider{})
	require.NoError(t, manager.AddPreparing(t.Context(), testBuilder(1, 1, 1)))
	view := buildTestViewWithVersion(1, 1, 1, 1)
	callback := manager.makeOnSyncResponse(testVersion(1, 1, 1), qnReport(view, 1, qviews.QueryViewStatePreparing))
	report := qnReport(view, 1, qviews.QueryViewStateReady)
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 100 {
			callback(report)
		}
	})
	manager.abortRecovery()
	wg.Wait()
	require.Equal(t, int32(1), derefs.Load())
	count := len(submitter.snapshot())
	require.True(t, callback(report))
	require.Len(t, submitter.snapshot(), count)
	require.ErrorIs(t, manager.AddPreparing(context.Background(), testBuilder(1, 1, 1)), merr.ErrServiceUnavailable)
	manager.abortRecovery()
	require.Equal(t, int32(1), derefs.Load())
}

func TestOriginalRegistryAPIAndExplicitDataViewProvider(t *testing.T) {
	catalog, syncer := newMockCatalog(), newMockSyncer()
	registry, err := RecoverShardViewRegistry(t.Context(), catalog, syncer)
	require.NoError(t, err)
	defer registry.Close()
	require.NoError(t, registry.Ensure(testShardID).AddPreparing(t.Context(), testBuilder(1, 1, 1)))
	require.NoError(t, registry.flushScheduler.Flush(t.Context()))
	require.Equal(t, 1, catalog.numSaveCalls())
	_, err = RecoverShardViewRegistryWithDataViews(t.Context(), catalog, syncer, nil)
	require.Error(t, err)
}

func TestRecoveryCancellationStopsFlushBeforeReleasingReferences(t *testing.T) {
	var derefs atomic.Int32
	patch := mockey.Mock((stubDataViewRef).Deref).To(func(stubDataViewRef) { derefs.Add(1) }).Build()
	defer patch.UnPatch()
	started, canceled, finish := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(finish) })
	syncPatch := mockey.Mock((*mockSyncer).SyncViews).To(func(_ *mockSyncer, ctx context.Context, _ syncer.SyncGroup) error {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-finish // Model a flush that has not quiesced just because it was canceled.
		return ctx.Err()
	}).Build()
	defer syncPatch.UnPatch()
	catalog := newMockCatalog()
	catalog.listed = []*viewpb.QueryViewOfShard{buildTestViewWithVersion(1, 1, 1, 1)}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := RecoverShardViewRegistryWithDataViews(ctx, catalog, newMockSyncer(), stubDataViewRefProvider{})
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery flush did not start")
	}
	cancel()
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery did not stop flush tasks")
	}
	require.Zero(t, derefs.Load(), "references must outlive the running flush")
	once.Do(func() { close(finish) })
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("recovery did not finish cleanup")
	}
	require.Equal(t, int32(1), derefs.Load())
}
