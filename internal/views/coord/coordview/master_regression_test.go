package coordview

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

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
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "missing reference"},
		{name: "provider error", err: merr.WrapErrServiceUnavailableMsg("injected reference provider failure")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog, syncer := newMockCatalog(), newMockSyncer()
			registry := newTestRegistry(t, catalog, syncer)
			id := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_100v0"}
			manager := registry.Ensure(id)
			var last *ShardStats
			stop := registry.RegisterPublicationListener(func(_ qviews.ShardID, stats *ShardStats) { last = stats })
			defer stop()
			require.NotNil(t, last)
			patch := mockey.Mock((*stubDataViewRefProvider).Get).Return(nil, tc.err).Build()
			defer patch.UnPatch()
			err := manager.AddPreparing(t.Context(), testBuilderForShard(100, id))
			require.Error(t, err)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
			}
			require.Nil(t, registry.Get(id))
			require.Nil(t, last)
			require.Empty(t, registry.CollectionShards(100))
			require.Empty(t, registry.Snapshot().StatsMap())
			require.ErrorIs(t, manager.AddPreparing(t.Context(), testBuilderForShard(100, id)), merr.ErrServiceUnavailable)
			require.Zero(t, catalog.numSaveCalls())
			require.Zero(t, syncer.syncViewCount())
		})
	}
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

func TestRegistryRecoversAfterPartialChunkedHandoff(t *testing.T) {
	// The catalog contains the serving view and its Preparing successor.
	// Promotion must persist old Down before new Up, even across transactions.
	old := buildTestViewWithVersion(1, 1, 1, 1)
	old.Meta.State = viewpb.QueryViewState_QueryViewStateUp
	next := buildTestViewWithVersion(1, 1, 1, 2)
	next.Meta.State = viewpb.QueryViewState_QueryViewStatePreparing
	var mu sync.Mutex
	durable := map[int64]*viewpb.QueryViewOfShard{1: old, 2: next}
	readDurable := func() map[int64]*viewpb.QueryViewOfShard {
		mu.Lock()
		defer mu.Unlock()
		result := make(map[int64]*viewpb.QueryViewOfShard, len(durable))
		for version, view := range durable {
			result[version] = proto.Clone(view).(*viewpb.QueryViewOfShard)
		}
		return result
	}
	catalog := newMockCatalog()
	var writes [][]int64
	writeErr := merr.WrapErrServiceInternalMsg("injected second transaction failure")
	savePatch := mockey.Mock((*mockCatalog).SaveQueryViews).To(func(_ *mockCatalog, _ context.Context, views []*viewpb.QueryViewOfShard) error {
		mu.Lock()
		defer mu.Unlock()
		versions := make([]int64, 0, len(views))
		for _, view := range views {
			versions = append(versions, view.Meta.Version.QueryVersion)
		}
		writes = append(writes, versions)
		if len(writes) == 2 {
			return writeErr // First transaction remains durable; second commits nothing.
		}
		for _, view := range views {
			version := view.Meta.Version.QueryVersion
			if view.Meta.State == viewpb.QueryViewState_QueryViewStateDropped {
				delete(durable, version)
			} else {
				durable[version] = proto.Clone(view).(*viewpb.QueryViewOfShard)
			}
		}
		return nil
	}).Build()
	t.Cleanup(func() { savePatch.UnPatch() })
	listPatch := mockey.Mock((*mockCatalog).ListQueryViews).To(func(*mockCatalog, context.Context) ([]*viewpb.QueryViewOfShard, error) {
		var views []*viewpb.QueryViewOfShard
		for _, view := range readDurable() {
			views = append(views, view)
		}
		return views, nil
	}).Build()
	t.Cleanup(func() { listPatch.UnPatch() })

	oldDown := proto.Clone(old).(*viewpb.QueryViewOfShard)
	oldDown.Meta.State = viewpb.QueryViewState_QueryViewStateDown
	nextUp := proto.Clone(next).(*viewpb.QueryViewOfShard)
	nextUp.Meta.State = viewpb.QueryViewState_QueryViewStateUp
	tasks := &capturedDirtyViewTaskScheduler{}
	beforeSyncer := newMockSyncer()
	scheduler := newDirtyViewFlushScheduler(catalog, beforeSyncer, 1, tasks)
	t.Cleanup(scheduler.Close)
	afterPersist := false
	scheduler.Submit(dirtyViewEvent{
		shardID: testShardID,
		// Deliberately reversed: without sorting, the first commit would
		// leave both versions Up while the second transaction fails.
		persists: []*viewpb.QueryViewOfShard{nextUp, oldDown},
		syncs: []syncer.SyncView{{
			View: qviews.NewFullQueryViewAtStreamingNode(nextUp.Meta, nextUp.StreamingNode, nextUp.QueryNode),
		}},
		afterPersist: []func(){func() { afterPersist = true }},
	})
	require.Len(t, tasks.snapshot(), 1)
	require.Panics(t, func() { _ = tasks.snapshot()[0].Execute(t.Context()) })
	scheduler.Close() // Model the failed coordinator stopping before recovery.
	require.Equal(t, [][]int64{{1}, {2}}, writes)
	require.False(t, afterPersist, "a partially persisted batch must not finalize removals")
	require.Zero(t, beforeSyncer.syncViewCount(), "no sync may escape a failed batch")
	prefix := readDurable()
	require.Equal(t, viewpb.QueryViewState_QueryViewStateDown, prefix[1].Meta.State)
	require.Equal(t, viewpb.QueryViewState_QueryViewStatePreparing, prefix[2].Meta.State)

	// Recover through the real registry, then let workers report completion.
	afterSyncer := newMockSyncer()
	registry, err := RecoverShardViewRegistryWithDataViews(t.Context(), catalog, afterSyncer, stubDataViewRefProvider{})
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	manager := registry.Get(testShardID)
	require.NotNil(t, manager)
	oldVersion, nextVersion := testVersion(1, 1, 1), testVersion(1, 1, 2)
	require.Nil(t, manager.Stats().UpVersion)
	require.Equal(t, nextVersion, *manager.Stats().PreparingVersion)
	report := func(node qviews.WorkNode, version qviews.QueryViewVersion, state qviews.QueryViewState) {
		t.Helper()
		simulateNodeResponse(t, afterSyncer, node, version, state)
		require.NoError(t, registry.flushScheduler.Flush(t.Context()))
	}
	report(testQN1, nextVersion, qviews.QueryViewStateReady)
	report(testSN, nextVersion, qviews.QueryViewStateReady)
	report(testSN, nextVersion, qviews.QueryViewStateUp)
	report(testSN, oldVersion, qviews.QueryViewStateDown)
	report(testSN, oldVersion, qviews.QueryViewStateDropped)
	report(testQN1, oldVersion, qviews.QueryViewStateDropped)

	require.Equal(t, nextVersion, *manager.Stats().UpVersion)
	require.Nil(t, manager.Stats().PreparingVersion)
	manager.mu.Lock()
	remaining := len(manager.views)
	manager.mu.Unlock()
	require.Equal(t, 1, remaining)
	recovered := readDurable()
	require.Len(t, recovered, 1)
	require.Equal(t, viewpb.QueryViewState_QueryViewStateUp, recovered[2].Meta.State)
}
