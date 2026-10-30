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

type stubLoadInfoRefs struct{}

func (*stubLoadInfoRefs) AcquireLoadInfo(context.Context, int64, uint64) (func(), error) {
	panic("patch with mockey")
}

func TestLoadInfoReferenceUntilDurableRemoval(t *testing.T) {
	var refs atomic.Int32
	patch := mockey.Mock((*stubLoadInfoRefs).AcquireLoadInfo).To(func(_ context.Context, id int64, version uint64) (func(), error) {
		require.EqualValues(t, testCollectionID, id)
		require.EqualValues(t, 55, version)
		refs.Add(1)
		return sync.OnceFunc(func() { refs.Add(-1) }), nil
	}).Build()
	defer patch.UnPatch()
	catalog, transport := newMockCatalog(), newMockSyncer()
	registry := newTestRegistry(t, catalog, transport)
	registry.loadInfoRefs = &stubLoadInfoRefs{}
	manager := registry.Ensure(testShardID)
	builder := testBuilder(1, 1, 1).SetLoadInfoVersion(55)
	require.NoError(t, manager.AddPreparing(t.Context(), builder))
	require.EqualValues(t, 1, refs.Load())
	version := testVersion(1, 1, 1)
	require.NoError(t, registry.flushScheduler.Flush(t.Context()))
	simulateNodeResponse(t, transport, testQN1, version, qviews.QueryViewStateReady)
	simulateNodeResponse(t, transport, testSN, version, qviews.QueryViewStateReady)
	require.NoError(t, registry.flushScheduler.Flush(t.Context()))
	simulateNodeResponse(t, transport, testSN, version, qviews.QueryViewStateUp)
	require.NoError(t, registry.flushScheduler.Flush(t.Context()))
	require.NoError(t, manager.RequestRelease(t.Context()))
	require.NoError(t, registry.flushScheduler.Flush(t.Context()))
	require.EqualValues(t, 1, refs.Load(), "Down still owns loading metadata")
	simulateNodeResponse(t, transport, testSN, version, qviews.QueryViewStateDown)
	require.NoError(t, registry.flushScheduler.Flush(t.Context()))
	simulateNodeResponse(t, transport, testSN, version, qviews.QueryViewStateDropped)
	require.NoError(t, registry.flushScheduler.Flush(t.Context()))
	require.EqualValues(t, 1, refs.Load(), "the QN has not cleaned up yet")
	batch := registry.Begin()
	defer batch.Commit()
	simulateNodeResponse(t, transport, testQN1, version, qviews.QueryViewStateDropped)
	require.EqualValues(t, 1, refs.Load(), "node ACKs are not durable View removal")
	batch.Commit()
	require.NoError(t, registry.flushScheduler.Flush(t.Context()))
	require.Zero(t, refs.Load())
	manager.abortRecovery()
	require.Zero(t, refs.Load(), "shutdown cannot release twice")
}

func TestLoadInfoReferenceRollbackAfterReleaseDuringAcquire(t *testing.T) {
	started, unblock := make(chan struct{}), make(chan struct{})
	var refs atomic.Int32
	patch := mockey.Mock((*stubLoadInfoRefs).AcquireLoadInfo).To(func(context.Context, int64, uint64) (func(), error) {
		refs.Add(1)
		close(started)
		<-unblock
		return sync.OnceFunc(func() { refs.Add(-1) }), nil
	}).Build()
	defer patch.UnPatch()
	manager := newShardViewManager(t.Context(), testShardID, &capturedDirtyViewEventSubmitter{}, nil, nil, &stubLoadInfoRefs{})
	done := make(chan error, 1)
	go func() { done <- manager.AddPreparing(t.Context(), testBuilder(1, 1, 1).SetLoadInfoVersion(55)) }()
	awaitReferenceOperation(t, started)
	require.NoError(t, manager.RequestRelease(t.Context()), "acquisition must not hold the manager lock")
	close(unblock)
	require.Error(t, awaitReferenceOperation(t, done))
	require.Zero(t, refs.Load())
}

func TestLoadInfoReferenceRecoveredBeforeNodeSync(t *testing.T) {
	var refs atomic.Int32
	patch := mockey.Mock((*stubLoadInfoRefs).AcquireLoadInfo).To(func(context.Context, int64, uint64) (func(), error) {
		refs.Add(1)
		return sync.OnceFunc(func() { refs.Add(-1) }), nil
	}).Build()
	defer patch.UnPatch()
	// The existing DataView provider is patched by the registry fixture; use
	// that same production recovery path with an additional LoadInfo owner.
	catalog, transport := newMockCatalog(), newMockSyncer()
	registry := newTestRegistry(t, catalog, transport)
	registry.Close()
	view := testBuilder(1, 1, 1).SetLoadInfoVersion(55).Build()
	view.Meta.State = viewpb.QueryViewState_QueryViewStateUnrecoverable
	manager, err := RecoverShardViewManager(t.Context(), testShardID, &capturedDirtyViewEventSubmitter{}, stubDataViewRefProvider{}, []*viewpb.QueryViewOfShard{view}, &stubLoadInfoRefs{})
	require.NoError(t, err)
	require.EqualValues(t, 1, refs.Load(), "Unrecoverable is still a live metadata owner")
	manager.abortRecovery()
	require.Zero(t, refs.Load())
}
