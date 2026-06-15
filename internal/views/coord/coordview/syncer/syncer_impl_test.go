//go:build test && dynamic

package syncer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// reliableTestSetup creates a ReliableSyncer backed by a mockViewSyncClient.
// Streams are created with the syncer's ctx and exposed via streamReady channel.
type reliableTestSetup struct {
	syncer      ReliableSyncer
	client      *mockViewSyncClient
	streamReady chan *mockStream
}

func newReliableTestSetup(t *testing.T, nodes ...qviews.WorkNode) *reliableTestSetup {
	t.Helper()
	client := newMockViewSyncClient()
	for _, n := range nodes {
		client.addNode(n)
	}

	streamReady := make(chan *mockStream, 10)
	client.openStreamFn = func(ctx context.Context, _ qviews.WorkNode) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
		s := newMockStream(ctx)
		streamReady <- s
		return s, nil
	}

	syncer := NewReliableSyncer(client)
	return &reliableTestSetup{
		syncer:      syncer,
		client:      client,
		streamReady: streamReady,
	}
}

func (s *reliableTestSetup) waitStream(t *testing.T) *mockStream {
	t.Helper()
	select {
	case stream := <-s.streamReady:
		return stream
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for stream")
		return nil
	}
}

func TestReliable_NormalFlow(t *testing.T) {
	node := qviews.NewQueryNode(1)
	setup := newReliableTestSetup(t, node)
	defer setup.syncer.Close()

	var respCalled atomic.Bool
	sv := newTestSyncView(1, 1, func(resp qviews.QueryViewAtWorkNode) bool {
		respCalled.Store(true)
		return true
	}, nil)

	err := setup.syncer.SyncViews(context.Background(), newTestSyncGroup(sv))
	require.NoError(t, err)

	stream := setup.waitStream(t)

	// Wait for send.
	req, ok := stream.waitSend(time.Second)
	require.True(t, ok)
	require.NotNil(t, req.GetViews())

	// Inject response.
	stream.injectResponse(sv.View.IntoProto())

	// Wait for callback.
	assert.True(t, waitForCond(respCalled.Load, time.Second))
}

func TestReliable_NodeNotFound_DrainImmediately(t *testing.T) {
	// Node NOT added to client — IsNodeAlive returns false.
	setup := newReliableTestSetup(t)
	defer setup.syncer.Close()

	var lostCalled atomic.Bool
	sv := newTestSyncView(1, 1, nil, func(qviews.QueryNode) { lostCalled.Store(true) })

	err := setup.syncer.SyncViews(context.Background(), newTestSyncGroup(sv))
	require.NoError(t, err)

	assert.True(t, lostCalled.Load(), "OnQueryNodeLost should be called immediately for unknown node")
}

func TestReliable_StreamingNodeNotFoundDoesNotCallQueryNodeLost(t *testing.T) {
	setup := newReliableTestSetup(t)
	defer setup.syncer.Close()

	var lostCalled atomic.Bool
	sv := SyncView{
		View:            newTestSNView(1),
		OnQueryNodeLost: func(qviews.QueryNode) { lostCalled.Store(true) },
	}

	err := setup.syncer.SyncViews(context.Background(), newTestSyncGroup(sv))
	require.NoError(t, err)

	assert.False(t, lostCalled.Load(), "StreamingNode loss should not invoke OnQueryNodeLost")
}

func TestReliable_LazyCreation(t *testing.T) {
	node := qviews.NewQueryNode(1)
	client := newMockViewSyncClient()
	client.addNode(node)

	var streamOpened atomic.Bool
	client.openStreamFn = func(ctx context.Context, _ qviews.WorkNode) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
		streamOpened.Store(true)
		return newMockStream(ctx), nil
	}

	syncer := NewReliableSyncer(client)
	defer syncer.Close()

	// No stream opened initially.
	time.Sleep(20 * testTimeUnit)
	assert.False(t, streamOpened.Load(), "no stream should be opened before SyncViews")

	// Trigger lazy creation.
	sv := newTestSyncView(1, 1, nil, nil)
	err := syncer.SyncViews(context.Background(), newTestSyncGroup(sv))
	require.NoError(t, err)

	assert.True(t, waitForCond(streamOpened.Load, time.Second),
		"stream should be opened on first SyncViews")
}

func TestReliable_QueryNodeLostViaWatch(t *testing.T) {
	node := qviews.NewQueryNode(1)
	setup := newReliableTestSetup(t, node)
	defer setup.syncer.Close()

	var lostCalled atomic.Bool
	sv := newTestSyncView(1, 1,
		func(qviews.QueryViewAtWorkNode) bool { return false },
		func(qviews.QueryNode) { lostCalled.Store(true) },
	)

	err := setup.syncer.SyncViews(context.Background(), newTestSyncGroup(sv))
	require.NoError(t, err)

	// Wait for stream and the view to be sent.
	stream := setup.waitStream(t)
	stream.waitSend(time.Second)

	// Remove node from service discovery.
	setup.client.removeNode(node)
	setup.client.notifyNodeChanged()

	assert.True(t, waitForCond(lostCalled.Load, 2*time.Second),
		"OnQueryNodeLost should be called after node removal")
}

func TestReliable_StreamBreakRecovery(t *testing.T) {
	node := qviews.NewQueryNode(1)
	setup := newReliableTestSetup(t, node)
	defer setup.syncer.Close()

	sv := newTestSyncView(1, 1,
		func(qviews.QueryViewAtWorkNode) bool { return false },
		nil,
	)

	err := setup.syncer.SyncViews(context.Background(), newTestSyncGroup(sv))
	require.NoError(t, err)

	// Wait for first stream and the view to be sent.
	s1 := setup.waitStream(t)
	s1.waitSend(time.Second)

	// Break stream.
	close(s1.recvCh)

	// Wait for reconnect.
	s2 := setup.waitStream(t)

	// Pending view should be re-pushed.
	req, ok := s2.waitSend(time.Second)
	require.True(t, ok, "view should be re-pushed on reconnect")
	assert.Len(t, req.GetViews().QueryViews, 1)
}

func TestReliable_EntryReplacement(t *testing.T) {
	node := qviews.NewQueryNode(1)
	setup := newReliableTestSetup(t, node)
	defer setup.syncer.Close()

	var oldCalled, newCalled atomic.Bool
	sv1 := newTestSyncView(1, 1, func(qviews.QueryViewAtWorkNode) bool {
		oldCalled.Store(true)
		return true
	}, nil)
	sv2 := newTestSyncView(1, 1, func(qviews.QueryViewAtWorkNode) bool {
		newCalled.Store(true)
		return true
	}, nil)

	// First SyncViews triggers lazy syncer creation which opens the stream.
	err := setup.syncer.SyncViews(context.Background(), newTestSyncGroup(sv1))
	require.NoError(t, err)

	stream := setup.waitStream(t)
	stream.waitSend(time.Second)

	err = setup.syncer.SyncViews(context.Background(), newTestSyncGroup(sv2))
	require.NoError(t, err)
	stream.waitSend(time.Second)

	// Inject response — should invoke the new callback.
	stream.injectResponse(sv2.View.IntoProto())

	assert.True(t, waitForCond(newCalled.Load, time.Second))
	assert.False(t, oldCalled.Load(), "old callback should be silently replaced")
}

func TestReliable_OnSyncResponseFalse_ContinueMonitoring(t *testing.T) {
	node := qviews.NewQueryNode(1)
	setup := newReliableTestSetup(t, node)
	defer setup.syncer.Close()

	var callCount atomic.Int32
	sv := newTestSyncView(1, 1, func(qviews.QueryViewAtWorkNode) bool {
		n := callCount.Add(1)
		return n >= 2 // return false on first call, true on second
	}, nil)

	err := setup.syncer.SyncViews(context.Background(), newTestSyncGroup(sv))
	require.NoError(t, err)

	stream := setup.waitStream(t)
	stream.waitSend(time.Second)

	// First response — callback returns false → entry stays.
	stream.injectResponse(sv.View.IntoProto())
	assert.True(t, waitForCond(func() bool { return callCount.Load() >= 1 }, time.Second))

	// Second response — callback returns true → entry removed.
	stream.injectResponse(sv.View.IntoProto())
	assert.True(t, waitForCond(func() bool { return callCount.Load() >= 2 }, time.Second))

	// Third response — entry already removed, callback should NOT be called.
	stream.injectResponse(sv.View.IntoProto())
	time.Sleep(20 * testTimeUnit)
	assert.Equal(t, int32(2), callCount.Load(), "callback should not be called after entry removal")
}

func TestReliable_ClosedReturnsError(t *testing.T) {
	setup := newReliableTestSetup(t)

	err := setup.syncer.Close()
	require.NoError(t, err)

	sv := newTestSyncView(1, 1, nil, nil)
	err = setup.syncer.SyncViews(context.Background(), newTestSyncGroup(sv))
	assert.ErrorIs(t, err, ErrSyncerClosed)
}

func TestReliable_ConcurrentSyncViews(t *testing.T) {
	node := qviews.NewQueryNode(1)
	setup := newReliableTestSetup(t, node)
	defer setup.syncer.Close()

	const numGoroutines = 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			sv := newTestSyncView(1, int64(idx+1), func(qviews.QueryViewAtWorkNode) bool { return true }, nil)
			err := setup.syncer.SyncViews(context.Background(), newTestSyncGroup(sv))
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	stream := setup.waitStream(t)

	// All views should eventually be sent (no panics, no deadlocks).
	var totalViews int
	deadline := time.After(2 * time.Second)
	for totalViews < numGoroutines {
		select {
		case req := <-stream.sendCh:
			if v := req.GetViews(); v != nil {
				totalViews += len(v.QueryViews)
			}
		case <-deadline:
			t.Fatalf("timeout: received %d/%d views", totalViews, numGoroutines)
		}
	}
}

func TestReliable_IdempotentResponse(t *testing.T) {
	node := qviews.NewQueryNode(1)
	setup := newReliableTestSetup(t, node)
	defer setup.syncer.Close()

	var callCount atomic.Int32
	sv := newTestSyncView(1, 1, func(qviews.QueryViewAtWorkNode) bool {
		callCount.Add(1)
		return true // remove on first response
	}, nil)

	err := setup.syncer.SyncViews(context.Background(), newTestSyncGroup(sv))
	require.NoError(t, err)

	stream := setup.waitStream(t)
	stream.waitSend(time.Second)

	// First response removes the entry.
	stream.injectResponse(sv.View.IntoProto())
	assert.True(t, waitForCond(func() bool { return callCount.Load() >= 1 }, time.Second))

	// Second response — entry already gone → no-op.
	stream.injectResponse(sv.View.IntoProto())
	time.Sleep(20 * testTimeUnit)
	assert.Equal(t, int32(1), callCount.Load(), "second response should be no-op")
}

func TestReliable_NodeRemovedDuringSync(t *testing.T) {
	node := qviews.NewQueryNode(1)
	setup := newReliableTestSetup(t, node)
	defer setup.syncer.Close()

	var lostCalled atomic.Bool
	sv := newTestSyncView(1, 1,
		func(qviews.QueryViewAtWorkNode) bool { return false },
		func(qviews.QueryNode) { lostCalled.Store(true) },
	)

	err := setup.syncer.SyncViews(context.Background(), newTestSyncGroup(sv))
	require.NoError(t, err)

	// Wait for the stream and view to propagate.
	stream := setup.waitStream(t)
	stream.waitSend(time.Second)

	// Remove node.
	setup.client.removeNode(node)
	setup.client.notifyNodeChanged()

	// The view should be drained via OnQueryNodeLost.
	assert.True(t, waitForCond(lostCalled.Load, 2*time.Second))
}

func TestReliable_SyncViewsContextCanceled(t *testing.T) {
	setup := newReliableTestSetup(t)
	defer setup.syncer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := setup.syncer.SyncViews(ctx, newTestSyncGroup())
	require.ErrorIs(t, err, context.Canceled)
}

func TestReliable_EmptyViewListDoesNotCreateSyncer(t *testing.T) {
	node := qviews.NewQueryNode(1)
	setup := newReliableTestSetup(t, node)
	defer setup.syncer.Close()

	err := setup.syncer.SyncViews(context.Background(), SyncGroup{
		ViewsByNode: map[qviews.WorkNodeKey][]SyncView{
			node.Key(): nil,
		},
	})
	require.NoError(t, err)

	inner := setup.syncer.(*reliableSyncer)
	inner.mu.Lock()
	defer inner.mu.Unlock()
	assert.Empty(t, inner.resumableSyncers)
}

func TestReliable_CloseIsIdempotent(t *testing.T) {
	setup := newReliableTestSetup(t)
	require.NoError(t, setup.syncer.Close())
	require.NoError(t, setup.syncer.Close())
}
