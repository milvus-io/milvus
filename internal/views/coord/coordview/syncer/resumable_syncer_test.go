//go:build test && dynamic

package syncer

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// newResumableTestSetup creates a resumableSyncer with a mockViewSyncClient
// that exposes streams via a channel. The openStreamFn creates mock streams
// using the ctx passed by the syncer (critical for proper cancellation).
func newResumableTestSetup(t *testing.T) (
	rs *resumableSyncer,
	streamReady chan *mockStream,
	cancel context.CancelFunc,
) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	node := qviews.NewQueryNode(1)
	client := newMockViewSyncClient()
	streamReady = make(chan *mockStream, 5)

	client.openStreamFn = func(ctx context.Context, _ qviews.WorkNode) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
		s := newMockStream(ctx)
		streamReady <- s
		return s, nil
	}

	rs = newResumableSyncer(ctx, node, client)
	return rs, streamReady, cancel
}

// waitStream waits for a mockStream to appear on the channel.
func waitStream(t *testing.T, ch chan *mockStream) *mockStream {
	t.Helper()
	select {
	case s := <-ch:
		return s
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for stream")
		return nil
	}
}

func TestResumable_SyncAndReceiveResponse(t *testing.T) {
	rs, streamReady, cancel := newResumableTestSetup(t)
	defer cancel()
	defer rs.Close()

	stream := waitStream(t, streamReady)

	var respCalled atomic.Bool
	sv := newTestSyncView(1, 1, func(resp qviews.QueryViewAtWorkNode) bool {
		respCalled.Store(true)
		return true
	}, nil)

	rs.Sync([]SyncView{sv})

	// Wait for the stream to receive the sent request.
	req, ok := stream.waitSend(time.Second)
	require.True(t, ok, "should receive a send")
	require.NotNil(t, req.GetViews())
	assert.Len(t, req.GetViews().QueryViews, 1)

	// Inject response — same view proto (simulating node response).
	stream.injectResponse(sv.View.IntoProto())

	// Wait for callback.
	assert.True(t, waitForCond(respCalled.Load, time.Second),
		"OnSyncResponse should be called")
}

func TestResumable_StreamBreakAndReconnect(t *testing.T) {
	rs, streamReady, cancel := newResumableTestSetup(t)
	defer cancel()
	defer rs.Close()

	s1 := waitStream(t, streamReady)

	// Sync a view.
	sv := newTestSyncView(1, 1, func(qviews.QueryViewAtWorkNode) bool { return false }, nil)
	rs.Sync([]SyncView{sv})

	// Wait for the view to be sent on stream 1.
	_, ok := s1.waitSend(time.Second)
	require.True(t, ok)

	// Break the first stream by closing its recvCh (causes Recv to return EOF).
	close(s1.recvCh)

	// Wait for reconnection — second stream should be opened.
	s2 := waitStream(t, streamReady)

	// After reconnect, rePush should resend pending entries.
	req, ok := s2.waitSend(time.Second)
	require.True(t, ok, "pending entry should be re-pushed on reconnect")
	require.NotNil(t, req.GetViews())
	assert.Len(t, req.GetViews().QueryViews, 1)
}

func TestResumable_CloseDoesNotDrain(t *testing.T) {
	rs, streamReady, cancel := newResumableTestSetup(t)
	defer cancel()

	stream := waitStream(t, streamReady)

	var lostCalled atomic.Bool
	sv := newTestSyncView(1, 1, nil, func(qviews.QueryNode) { lostCalled.Store(true) })
	rs.Sync([]SyncView{sv})

	// Wait for send to confirm it's in pending.
	stream.waitSend(time.Second)

	// Close — should NOT call OnQueryNodeLost.
	rs.Close()
	assert.False(t, lostCalled.Load(), "Close should not drain pending entries")

	// Pending entries should still be collectible.
	protos := rs.pending.CollectProtos()
	assert.Len(t, protos, 1)
}

func TestResumable_DrainPendingIfQueryNodeLost(t *testing.T) {
	rs, streamReady, cancel := newResumableTestSetup(t)
	defer cancel()

	_ = waitStream(t, streamReady)

	var lostCount atomic.Int32
	for i := int64(1); i <= 3; i++ {
		sv := newTestSyncView(1, i, nil, func(qviews.QueryNode) { lostCount.Add(1) })
		rs.Sync([]SyncView{sv})
	}

	// Give time for views to be sent (they may be batched into fewer sends).
	time.Sleep(50 * testTimeUnit)

	rs.Close()
	rs.DrainPendingIfNodeLost()

	assert.Equal(t, int32(3), lostCount.Load())
}

func TestResumable_BatchSending(t *testing.T) {
	rs, streamReady, cancel := newResumableTestSetup(t)
	defer cancel()
	defer rs.Close()

	stream := waitStream(t, streamReady)

	// Sync more than sendBatchSize (16) views.
	const numViews = 20
	for i := int64(1); i <= numViews; i++ {
		sv := newTestSyncView(1, i, nil, nil)
		rs.Sync([]SyncView{sv})
	}

	// Collect all sent requests within a reasonable time.
	var totalViews int
	deadline := time.After(2 * time.Second)
	for totalViews < numViews {
		select {
		case req := <-stream.sendCh:
			if views := req.GetViews(); views != nil {
				batch := len(views.QueryViews)
				assert.LessOrEqual(t, batch, sendBatchSize, "batch should not exceed sendBatchSize")
				totalViews += batch
			}
		case <-deadline:
			t.Fatalf("timeout: only received %d/%d views", totalViews, numViews)
		}
	}
	assert.Equal(t, numViews, totalViews)
}

func TestResumable_RePushClearsUnsent(t *testing.T) {
	rs, streamReady, cancel := newResumableTestSetup(t)
	defer cancel()
	defer rs.Close()

	s1 := waitStream(t, streamReady)

	// Sync a view.
	sv := newTestSyncView(1, 1, func(qviews.QueryViewAtWorkNode) bool { return false }, nil)
	rs.Sync([]SyncView{sv})
	s1.waitSend(time.Second)

	// Upsert another view (same key) while stream is still alive.
	sv2 := newTestSyncView(1, 1, func(qviews.QueryViewAtWorkNode) bool { return false }, nil)
	rs.Sync([]SyncView{sv2})
	s1.waitSend(time.Second)

	// Break stream.
	close(s1.recvCh)

	// Wait for reconnect.
	s2 := waitStream(t, streamReady)

	// rePush should send exactly 1 entry (the current pending entry),
	// NOT 2 (which would happen if stale unsent wasn't cleared).
	req, ok := s2.waitSend(time.Second)
	require.True(t, ok)
	assert.Len(t, req.GetViews().QueryViews, 1, "rePush should send exactly the current entries")
}

// TestResumable_OpenStreamError verifies the syncer retries with backoff on stream open failure.
func TestResumable_OpenStreamError(t *testing.T) {
	node := qviews.NewQueryNode(1)
	client := newMockViewSyncClient()

	var attempts atomic.Int32
	streamReady := make(chan *mockStream, 5)

	client.openStreamFn = func(ctx context.Context, _ qviews.WorkNode) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
		n := attempts.Add(1)
		if n <= 2 {
			return nil, io.ErrUnexpectedEOF
		}
		s := newMockStream(ctx)
		streamReady <- s
		return s, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rs := newResumableSyncer(ctx, node, client)
	defer rs.Close()

	// Sync a view — it should eventually be sent after retries.
	sv := newTestSyncView(1, 1, nil, nil)
	rs.Sync([]SyncView{sv})

	stream := waitStream(t, streamReady)
	_, ok := stream.waitSend(3 * time.Second)
	assert.True(t, ok, "view should be sent after retries")
	assert.GreaterOrEqual(t, int(attempts.Load()), 3, "should have retried at least twice")
}

// TestResumable_ConcurrentSyncAndRecv verifies no races when sync and recv happen concurrently.
func TestResumable_ConcurrentSyncAndRecv(t *testing.T) {
	rs, streamReady, cancel := newResumableTestSetup(t)
	defer cancel()
	defer rs.Close()

	stream := waitStream(t, streamReady)

	const numViews = 50
	var wg sync.WaitGroup
	var respCount atomic.Int32

	for i := int64(1); i <= numViews; i++ {
		wg.Add(1)
		go func(ver int64) {
			defer wg.Done()
			sv := newTestSyncView(1, ver, func(qviews.QueryViewAtWorkNode) bool {
				respCount.Add(1)
				return true
			}, nil)
			rs.Sync([]SyncView{sv})
		}(i)
	}
	wg.Wait()

	// Drain all sent requests and respond to each.
	deadline := time.After(3 * time.Second)
	responded := 0
	for responded < numViews {
		select {
		case req := <-stream.sendCh:
			if v := req.GetViews(); v != nil {
				for _, qv := range v.QueryViews {
					stream.injectResponse(qv)
					responded++
				}
			}
		case <-deadline:
			t.Fatalf("timeout: responded to %d/%d", responded, numViews)
		}
	}

	assert.True(t, waitForCond(func() bool { return respCount.Load() >= numViews }, 2*time.Second))
}

func TestResumable_SendBatchedEmptyAndSendError(t *testing.T) {
	rs := &resumableSyncer{node: qviews.NewQueryNode(1)}
	stream := newMockStream(context.Background())

	require.NoError(t, rs.sendBatched(stream, nil))
	assert.Empty(t, stream.collectSent())

	stream.setSendErr(io.ErrUnexpectedEOF)
	err := rs.sendBatched(stream, []*viewpb.QueryViewOfShard{newTestQNView(1, 1).IntoProto()})
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	assert.Empty(t, stream.collectSent())
}

func TestResumable_RecvLoopIgnoresNonViewResponse(t *testing.T) {
	rs := &resumableSyncer{node: qviews.NewQueryNode(1), pending: newPendingSyncQueryViews()}
	stream := newMockStream(context.Background())
	stream.recvCh <- &viewpb.SyncResponse{}
	close(stream.recvCh)

	rs.recvLoop(stream)
}

func TestResumable_SendLoopReturnsWhenContextDone(t *testing.T) {
	rs := &resumableSyncer{node: qviews.NewQueryNode(1), pending: newPendingSyncQueryViews()}
	stream := newMockStream(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	rs.sendLoop(ctx, stream)
	assert.Empty(t, stream.collectSent())
}
