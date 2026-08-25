//go:build test && dynamic

package handler

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// ---------------------------------------------------------------------------
// Test constants and helpers
// ---------------------------------------------------------------------------

const (
	testCollectionID int64 = 100
	testReplicaID    int64 = 1
	testVChannel           = "v0_c0"
	testTimeUnit           = 10 * time.Millisecond
)

func newTestQNView(nodeID int64, version int64, state viewpb.QueryViewState) qviews.QueryViewAtWorkNode {
	meta := &viewpb.QueryViewMeta{
		CollectionId: testCollectionID,
		ReplicaId:    testReplicaID,
		Vchannel:     testVChannel,
		Version: &viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: version, CompactVersion: 1},
			QueryVersion: version,
		},
		State: state,
	}
	qnView := &viewpb.QueryViewOfQueryNode{
		NodeId: nodeID,
		Partitions: []*viewpb.QueryViewOfPartition{
			{PartitionId: 10, SegmentIds: []int64{1000 + nodeID}},
		},
	}
	return qviews.NewQueryViewAtQueryNode(meta, qnView)
}

func newTestSNView(version int64, state viewpb.QueryViewState) qviews.QueryViewAtWorkNode {
	meta := &viewpb.QueryViewMeta{
		CollectionId: testCollectionID,
		ReplicaId:    testReplicaID,
		Vchannel:     testVChannel,
		Version: &viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: version, CompactVersion: 1},
			QueryVersion: version,
		},
		State: state,
	}
	return qviews.NewQueryViewAtStreamingNode(meta, &viewpb.QueryViewOfStreamingNode{})
}

// ---------------------------------------------------------------------------
// mockServerStream — implements viewpb.ViewSyncService_SyncQueryViewServer
// ---------------------------------------------------------------------------

type mockServerStream struct {
	ctx    context.Context
	sendCh chan *viewpb.SyncResponse // captures what server sends
	recvCh chan *viewpb.SyncRequest  // test injects requests
}

func newMockServerStream(ctx context.Context) *mockServerStream {
	return &mockServerStream{
		ctx:    ctx,
		sendCh: make(chan *viewpb.SyncResponse, 100),
		recvCh: make(chan *viewpb.SyncRequest, 100),
	}
}

func (s *mockServerStream) Send(resp *viewpb.SyncResponse) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.sendCh <- resp:
		return nil
	}
}

func (s *mockServerStream) Recv() (*viewpb.SyncRequest, error) {
	select {
	case <-s.ctx.Done():
		return nil, io.EOF
	case req, ok := <-s.recvCh:
		if !ok {
			return nil, io.EOF
		}
		return req, nil
	}
}

func (s *mockServerStream) SetHeader(metadata.MD) error  { return nil }
func (s *mockServerStream) SendHeader(metadata.MD) error { return nil }
func (s *mockServerStream) SetTrailer(metadata.MD)       {}
func (s *mockServerStream) Context() context.Context     { return s.ctx }
func (s *mockServerStream) SendMsg(m interface{}) error  { return nil }
func (s *mockServerStream) RecvMsg(m interface{}) error  { return nil }

// injectViewsRequest sends a views request into the stream.
func (s *mockServerStream) injectViewsRequest(views ...*viewpb.QueryViewOfShard) {
	s.recvCh <- &viewpb.SyncRequest{
		Request: &viewpb.SyncRequest_Views{
			Views: &viewpb.SyncQueryViewsRequest{
				QueryViews: views,
			},
		},
	}
}

// injectCloseRequest sends a close request into the stream.
func (s *mockServerStream) injectCloseRequest() {
	s.recvCh <- &viewpb.SyncRequest{
		Request: &viewpb.SyncRequest_Close{
			Close: &viewpb.SyncCloseRequest{},
		},
	}
}

// waitSend waits for a response to appear on sendCh within timeout.
func (s *mockServerStream) waitSend(timeout time.Duration) (*viewpb.SyncResponse, bool) {
	select {
	case resp := <-s.sendCh:
		return resp, true
	case <-time.After(timeout):
		return nil, false
	}
}

// collectSent drains all currently buffered responses.
func (s *mockServerStream) collectSent() []*viewpb.SyncResponse {
	var resps []*viewpb.SyncResponse
	for {
		select {
		case resp := <-s.sendCh:
			resps = append(resps, resp)
		default:
			return resps
		}
	}
}

// ---------------------------------------------------------------------------
// mockHandler — implements QueryViewHandler
// ---------------------------------------------------------------------------

type mockHandler struct {
	mu         sync.Mutex
	applyFn    func(views []ApplyView)
	applyCalls int
}

func newMockHandler() *mockHandler {
	return &mockHandler{}
}

func (h *mockHandler) ApplyViews(views []ApplyView) {
	h.mu.Lock()
	h.applyCalls++
	fn := h.applyFn
	h.mu.Unlock()
	if fn != nil {
		fn(views)
	}
}

func (h *mockHandler) getApplyCalls() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.applyCalls
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestViewSyncServer_NormalFlow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := newMockServerStream(ctx)
	handler := newMockHandler()

	// Handler immediately reports Ready state via OnReport callback.
	handler.applyFn = func(views []ApplyView) {
		for _, av := range views {
			av.OnReport(newTestQNView(
				av.View.(*qviews.QueryViewAtQueryNode).NodeID(),
				av.View.Version().QueryVersion,
				viewpb.QueryViewState_QueryViewStateReady,
			))
		}
	}

	server := NewViewSyncServer(handler)

	// Run SyncQueryView in background.
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.SyncQueryView(stream)
	}()

	// Send a views request.
	view := newTestQNView(1, 1, viewpb.QueryViewState_QueryViewStatePreparing)
	stream.injectViewsRequest(view.IntoProto())

	// Expect a report back.
	resp, ok := stream.waitSend(5 * testTimeUnit)
	require.True(t, ok, "expected report")
	require.NotNil(t, resp.GetViews())
	assert.Len(t, resp.GetViews().QueryViews, 1)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateReady, resp.GetViews().QueryViews[0].Meta.State)

	// Close stream.
	cancel()
	err := <-errCh
	assert.Nil(t, err)
}

func TestViewSyncServer_AsyncReport(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := newMockServerStream(ctx)
	handler := newMockHandler()

	// Store OnReport callbacks.
	var callbacks []func(qviews.QueryViewAtWorkNode)
	var cbMu sync.Mutex

	handler.applyFn = func(views []ApplyView) {
		cbMu.Lock()
		for _, av := range views {
			callbacks = append(callbacks, av.OnReport)
		}
		cbMu.Unlock()
	}

	server := NewViewSyncServer(handler)

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.SyncQueryView(stream)
	}()

	// Send a views request.
	view := newTestQNView(1, 1, viewpb.QueryViewState_QueryViewStatePreparing)
	stream.injectViewsRequest(view.IntoProto())

	// Wait for handler to be called.
	require.Eventually(t, func() bool {
		cbMu.Lock()
		defer cbMu.Unlock()
		return len(callbacks) > 0
	}, 5*testTimeUnit, time.Millisecond)

	// Invoke async report callback.
	cbMu.Lock()
	cb := callbacks[0]
	cbMu.Unlock()

	readyView := newTestQNView(1, 1, viewpb.QueryViewState_QueryViewStateReady)
	cb(readyView)

	// Expect async report to appear on the stream.
	resp, ok := stream.waitSend(5 * testTimeUnit)
	require.True(t, ok, "expected async report")
	require.NotNil(t, resp.GetViews())
	assert.Len(t, resp.GetViews().QueryViews, 1)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateReady, resp.GetViews().QueryViews[0].Meta.State)

	cancel()
	<-errCh
}

func TestViewSyncServer_CloseRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := newMockServerStream(ctx)
	handler := newMockHandler()
	server := NewViewSyncServer(handler)

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.SyncQueryView(stream)
	}()

	// Send close request.
	stream.injectCloseRequest()

	// Expect close response.
	resp, ok := stream.waitSend(5 * testTimeUnit)
	require.True(t, ok, "expected close response")
	assert.NotNil(t, resp.GetClose())

	// SyncQueryView should return nil.
	err := <-errCh
	assert.Nil(t, err)
}

func TestViewSyncServer_StreamEOF(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := newMockServerStream(ctx)
	handler := newMockHandler()
	server := NewViewSyncServer(handler)

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.SyncQueryView(stream)
	}()

	// Close the recv channel to simulate EOF.
	close(stream.recvCh)

	err := <-errCh
	assert.Nil(t, err)
}

func TestViewSyncServer_MultipleViews(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := newMockServerStream(ctx)
	handler := newMockHandler()

	handler.applyFn = func(views []ApplyView) {
		for _, av := range views {
			av.OnReport(newTestQNView(
				av.View.(*qviews.QueryViewAtQueryNode).NodeID(),
				av.View.Version().QueryVersion,
				viewpb.QueryViewState_QueryViewStateReady,
			))
		}
	}

	server := NewViewSyncServer(handler)

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.SyncQueryView(stream)
	}()

	// Send batch of 3 views (different view keys).
	v1 := newTestQNView(1, 1, viewpb.QueryViewState_QueryViewStatePreparing)
	v2 := newTestQNView(2, 2, viewpb.QueryViewState_QueryViewStatePreparing)
	v3 := newTestQNView(3, 3, viewpb.QueryViewState_QueryViewStatePreparing)
	stream.injectViewsRequest(v1.IntoProto(), v2.IntoProto(), v3.IntoProto())

	// Expect response(s) containing all 3 reports (may be batched or split).
	var allViews []*viewpb.QueryViewOfShard
	require.Eventually(t, func() bool {
		resps := stream.collectSent()
		for _, r := range resps {
			if v := r.GetViews(); v != nil {
				allViews = append(allViews, v.QueryViews...)
			}
		}
		return len(allViews) >= 3
	}, 10*testTimeUnit, time.Millisecond)
	assert.Len(t, allViews, 3)

	cancel()
	<-errCh
}

func TestViewSyncServer_StreamingNodeView(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := newMockServerStream(ctx)
	handler := newMockHandler()

	handler.applyFn = func(views []ApplyView) {
		for _, av := range views {
			av.OnReport(newTestSNView(
				av.View.Version().QueryVersion,
				viewpb.QueryViewState_QueryViewStateReady,
			))
		}
	}

	server := NewViewSyncServer(handler)

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.SyncQueryView(stream)
	}()

	// Send a streaming node view.
	view := newTestSNView(1, viewpb.QueryViewState_QueryViewStatePreparing)
	stream.injectViewsRequest(view.IntoProto())

	resp, ok := stream.waitSend(5 * testTimeUnit)
	require.True(t, ok, "expected sync report")
	assert.Len(t, resp.GetViews().QueryViews, 1)
	assert.NotNil(t, resp.GetViews().QueryViews[0].StreamingNode)

	cancel()
	<-errCh
}

func TestViewSyncServer_AsyncReportAfterContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	stream := newMockServerStream(ctx)
	handler := newMockHandler()

	var callbacks []func(qviews.QueryViewAtWorkNode)
	var cbMu sync.Mutex

	handler.applyFn = func(views []ApplyView) {
		cbMu.Lock()
		for _, av := range views {
			callbacks = append(callbacks, av.OnReport)
		}
		cbMu.Unlock()
	}

	server := NewViewSyncServer(handler)

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.SyncQueryView(stream)
	}()

	view := newTestQNView(1, 1, viewpb.QueryViewState_QueryViewStatePreparing)
	stream.injectViewsRequest(view.IntoProto())

	require.Eventually(t, func() bool {
		cbMu.Lock()
		defer cbMu.Unlock()
		return len(callbacks) > 0
	}, 5*testTimeUnit, time.Millisecond)

	// Cancel context first.
	cancel()
	<-errCh

	// Invoking OnReport after cancel should not panic (non-blocking skip).
	cbMu.Lock()
	cb := callbacks[0]
	cbMu.Unlock()

	readyView := newTestQNView(1, 1, viewpb.QueryViewState_QueryViewStateReady)
	assert.NotPanics(t, func() {
		cb(readyView)
	})
}

func TestPendingReports_Deduplication(t *testing.T) {
	pending := newPendingReports()

	// Update the same view key twice — only the latest state should be kept.
	pending.Update(newTestQNView(1, 1, viewpb.QueryViewState_QueryViewStatePreparing))
	pending.Update(newTestQNView(1, 1, viewpb.QueryViewState_QueryViewStateReady))

	protos, closing := pending.Drain()
	assert.False(t, closing)
	require.Len(t, protos, 1)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateReady, protos[0].Meta.State)
}

func TestPendingReports_CloseResponse(t *testing.T) {
	pending := newPendingReports()

	pending.Update(newTestQNView(1, 1, viewpb.QueryViewState_QueryViewStateReady))
	pending.SetCloseResponse()

	protos, closing := pending.Drain()
	assert.True(t, closing)
	assert.Len(t, protos, 1)
}

func TestViewSyncServer_EmptyApplyResult(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := newMockServerStream(ctx)
	handler := newMockHandler()

	// Handler does not call OnReport — no reports.
	handler.applyFn = func(views []ApplyView) {}

	server := NewViewSyncServer(handler)

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.SyncQueryView(stream)
	}()

	view := newTestQNView(1, 1, viewpb.QueryViewState_QueryViewStatePreparing)
	stream.injectViewsRequest(view.IntoProto())

	// Give some time for processing.
	time.Sleep(3 * testTimeUnit)

	// No responses should have been sent.
	resps := stream.collectSent()
	assert.Empty(t, resps)

	assert.Equal(t, 1, handler.getApplyCalls())

	cancel()
	<-errCh
}
