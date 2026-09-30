package coordview

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func awaitReferenceOperation[T any](t *testing.T, done <-chan T) T {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("operation blocked on a DataView reference while holding the manager lock")
		var zero T
		return zero
	}
}

func TestAddPreparingDoesNotHoldManagerLockDuringDataViewMutation(t *testing.T) {
	dataViews := newInterfaceDataViews(t)
	started := make(chan struct{})
	var calls atomic.Int32
	patch := mockey.Mock((*stubDataViewRefProvider).Get).To(func(_ *stubDataViewRefProvider, ctx context.Context, collectionID int64, version *viewpb.DataVersion) (qviews.DataViewRef, error) {
		if calls.Add(1) == 2 {
			close(started)
		}
		return dataViews.Get(ctx, collectionID, version)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	manager := newShardViewManager(t.Context(), testShardID, &capturedDirtyViewEventSubmitter{}, nil, stubDataViewRefProvider{})
	require.NoError(t, manager.AddPreparing(t.Context(), testBuilder(1, 0, 1)))
	_, _, abort, err := dataViews.PrepareFlush(t.Context(), dataview.FlushDataViewEvent{
		CollectionID: testCollectionID,
		Segments:     []dataview.LoadableSegment{interfaceSegment(1002, 7)},
	})
	require.NoError(t, err)
	defer abort()
	done := make(chan error, 1)
	var workers sync.WaitGroup
	workers.Go(func() { done <- manager.AddPreparing(t.Context(), testBuilder(1, 0, 1)) })
	defer func() {
		abort()
		workers.Wait()
		manager.abortRecovery()
	}()
	awaitReferenceOperation(t, started)
	progress := make(chan error, 1)
	workers.Go(func() {
		_ = manager.Stats()
		view := testBuilder(1, 0, 1).Build()
		version := testVersion(1, 0, 1)
		ready := snReport(view, qviews.QueryViewStateReady)
		manager.makeOnSyncResponse(version, ready)(ready)
		manager.makeOnQueryNodeLost(version)(testQN1)
		progress <- manager.RequestRelease(t.Context())
	})
	require.NoError(t, awaitReferenceOperation(t, progress))
	select {
	case err := <-done:
		t.Fatalf("Get bypassed the held collection mutation: %v", err)
	default:
	}
	abort()
	require.ErrorIs(t, awaitReferenceOperation(t, done), merr.ErrServiceUnavailable)
	manager.abortRecovery()
	// Both the original view's ref and the rejected acquisition must be gone.
	publishInterfaceDataView(t, dataViews, 1003, 9)
	require.NoError(t, dataViews.GarbageCollect(t.Context(), testCollectionID, 1))
	ref, err := dataViews.Get(t.Context(), testCollectionID, &viewpb.DataVersion{StreamingVersion: 1})
	require.NoError(t, err)
	require.Nil(t, ref)
}

func TestAddPreparingRevalidatesAfterReferenceAcquisition(t *testing.T) {
	for _, mode := range []string{"release", "abort", "newer version", "same version", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			started, unblock := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(unblock) }) }
			defer release()
			var gets, derefs atomic.Int32
			getPatch := mockey.Mock((*stubDataViewRefProvider).Get).To(func(_ *stubDataViewRefProvider, _ context.Context, _ int64, version *viewpb.DataVersion) (qviews.DataViewRef, error) {
				// Copy before waiting: callers may pass a non-escaping temporary.
				value := qviews.FromProtoDataVersion(version)
				if gets.Add(1) == 1 {
					close(started)
					<-unblock
				}
				return stubDataViewRef{version: value}, nil
			}).Build()
			t.Cleanup(func() { getPatch.UnPatch() })
			derefPatch := mockey.Mock((stubDataViewRef).Deref).To(func(stubDataViewRef) { derefs.Add(1) }).Build()
			t.Cleanup(func() { derefPatch.UnPatch() })
			manager := newShardViewManager(t.Context(), testShardID, &capturedDirtyViewEventSubmitter{}, nil, stubDataViewRefProvider{})
			var workers sync.WaitGroup
			defer func() {
				release()
				workers.Wait()
				manager.abortRecovery()
			}()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			workers.Go(func() { done <- manager.AddPreparing(ctx, testBuilder(1, 1, 1)) })
			awaitReferenceOperation(t, started)
			changed := make(chan error, 1)
			workers.Go(func() {
				var err error
				switch mode {
				case "release":
					err = manager.RequestRelease(t.Context())
				case "abort":
					manager.abortRecovery()
				case "newer version":
					err = manager.AddPreparing(t.Context(), testBuilder(2, 1, 1))
				case "same version":
					err = manager.AddPreparing(t.Context(), testBuilder(1, 1, 1))
				case "cancel":
					cancel()
				}
				changed <- err
			})
			require.NoError(t, awaitReferenceOperation(t, changed))
			release()
			err := awaitReferenceOperation(t, done)
			switch mode {
			case "release", "abort":
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			case "newer version":
				require.ErrorIs(t, err, errDataVersionRollback)
				require.Equal(t, testVersion(2, 1, 1), *manager.Stats().PreparingVersion)
			case "same version":
				require.NoError(t, err)
				require.Equal(t, testVersion(1, 1, 2), *manager.Stats().PreparingVersion)
				require.Zero(t, derefs.Load())
			case "cancel":
				require.ErrorIs(t, err, context.Canceled)
			}
			if err != nil {
				require.Equal(t, int32(1), derefs.Load(), "rejected acquisition must be released once")
			}
			manager.abortRecovery()
			manager.abortRecovery()
			require.Equal(t, gets.Load(), derefs.Load(), "all acquired refs must be released exactly once")
		})
	}
}

func TestManagerReleasesReferencesOutsideLock(t *testing.T) {
	for _, mode := range []string{"durable removal", "recovery abort"} {
		t.Run(mode, func(t *testing.T) {
			catalog, transport := newMockCatalog(), newMockSyncer()
			registry := newTestRegistry(t, catalog, transport)
			manager := registry.Ensure(testShardID)
			require.NoError(t, manager.AddPreparing(t.Context(), testBuilder(1, 1, 1)))
			require.NoError(t, registry.flushScheduler.Flush(t.Context()))
			version := testVersion(1, 1, 1)
			started, unblock := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(unblock) }) }
			defer release()
			var derefs atomic.Int32
			patch := mockey.Mock((stubDataViewRef).Deref).To(func(stubDataViewRef) {
				if derefs.Add(1) == 1 {
					close(started)
				}
				<-unblock
			}).Build()
			t.Cleanup(func() { patch.UnPatch() })
			done := make(chan error, 1)
			var workers sync.WaitGroup
			defer func() {
				release()
				workers.Wait()
			}()
			if mode == "durable removal" {
				require.NoError(t, manager.RequestRelease(t.Context()))
				require.NoError(t, registry.flushScheduler.Flush(t.Context()))
				simulateNodeResponse(t, transport, testSN, version, qviews.QueryViewStateDropped)
				simulateNodeResponse(t, transport, testQN1, version, qviews.QueryViewStateDropped)
				workers.Go(func() { done <- registry.flushScheduler.Flush(t.Context()) })
			} else {
				workers.Go(func() {
					manager.abortRecovery()
					done <- nil
				})
			}
			awaitReferenceOperation(t, started)
			progress := make(chan error, 1)
			workers.Go(func() {
				_ = manager.Stats()
				manager.makeOnQueryNodeLost(version)(testQN1)
				manager.abortRecovery() // Must not release the detached ref twice.
				progress <- nil
			})
			require.NoError(t, awaitReferenceOperation(t, progress))
			if mode == "durable removal" {
				require.Contains(t, catalog.savedStates(), viewpb.QueryViewState_QueryViewStateDropped)
				require.Nil(t, registry.Get(testShardID))
			}
			release()
			require.NoError(t, awaitReferenceOperation(t, done))
			require.Equal(t, int32(1), derefs.Load())
		})
	}
}
