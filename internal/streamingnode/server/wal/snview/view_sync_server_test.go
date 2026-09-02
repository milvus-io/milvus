package snview

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	streamingstatus "github.com/milvus-io/milvus/internal/util/streamingutil/status"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestPChannelViewSyncServerRejectsUnavailableWAL(t *testing.T) {
	server := NewPChannelViewSyncServer(&fakeViewSyncWALManager{
		err: streamingstatus.NewChannelNotExist("p0"),
	})
	stream := newTestSyncQueryViewServerStream(newIncomingViewSyncContext(types.PChannelInfo{
		Name:       "p0",
		Term:       1,
		AccessMode: types.AccessModeRW,
	}))

	err := server.SyncQueryView(stream)

	require.Error(t, err)
	streamingErr := streamingstatus.AsStreamingError(streamingstatus.ConvertStreamingError("ViewSyncService.SyncQueryView", err))
	require.True(t, streamingErr.IsWrongStreamingNode())
}

func TestPChannelViewSyncServerClosesStreamWhenWALCloses(t *testing.T) {
	walDone := make(chan struct{})
	server := NewPChannelViewSyncServer(&fakeViewSyncWALManager{
		wal: fakeViewSyncWAL{available: walDone},
	})
	stream := newTestSyncQueryViewServerStream(newIncomingViewSyncContext(types.PChannelInfo{
		Name:       "p0",
		Term:       1,
		AccessMode: types.AccessModeRW,
	}))

	done := make(chan error, 1)
	go func() {
		done <- server.SyncQueryView(stream)
	}()
	<-stream.recvStarted

	close(walDone)

	select {
	case resp := <-stream.sendCh:
		require.NotNil(t, resp.GetClose())
	case <-time.After(time.Second):
		t.Fatal("expected sync stream close response after WAL closed")
	}
	close(stream.recvCh)
	require.NoError(t, <-done)
}

func TestPChannelViewSyncServerUsesWrappedWALProvider(t *testing.T) {
	walDone := make(chan struct{})
	raw := fakeViewSyncWAL{available: walDone}
	server := NewPChannelViewSyncServer(&fakeViewSyncWALManager{
		wal: wrappedTestWAL{WAL: raw, raw: raw},
	})
	stream := newTestSyncQueryViewServerStream(newIncomingViewSyncContext(types.PChannelInfo{
		Name:       "p0",
		Term:       1,
		AccessMode: types.AccessModeRW,
	}))

	done := make(chan error, 1)
	go func() {
		done <- server.SyncQueryView(stream)
	}()
	select {
	case <-stream.recvStarted:
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("expected sync stream to start")
	}

	close(walDone)

	select {
	case resp := <-stream.sendCh:
		require.NotNil(t, resp.GetClose())
	case <-time.After(time.Second):
		t.Fatal("expected sync stream close response after WAL closed")
	}
	close(stream.recvCh)
	require.NoError(t, <-done)
}

func newIncomingViewSyncContext(pchannel types.PChannelInfo) context.Context {
	outgoingCtx := worknodehandler.EncodeQueryViewPChannelToOutgoingContext(context.Background(), pchannel)
	md, _ := metadata.FromOutgoingContext(outgoingCtx)
	return metadata.NewIncomingContext(context.Background(), md)
}

type fakeViewSyncWALManager struct {
	wal wal.WAL
	err error
}

func (m *fakeViewSyncWALManager) Open(context.Context, types.PChannelInfo) error {
	return nil
}

func (m *fakeViewSyncWALManager) GetAvailableWAL(types.PChannelInfo) (wal.WAL, error) {
	return m.wal, m.err
}

func (m *fakeViewSyncWALManager) GetAvailableRawWALByPChannel(string) (wal.WAL, error) {
	return nil, nil
}

func (m *fakeViewSyncWALManager) Metrics() (*types.StreamingNodeMetrics, error) {
	return &types.StreamingNodeMetrics{}, nil
}

func (m *fakeViewSyncWALManager) Remove(context.Context, types.PChannelInfo) error {
	return nil
}

func (m *fakeViewSyncWALManager) Close() {}

type fakeViewSyncWAL struct {
	wal.WAL
	available <-chan struct{}
}

func (w fakeViewSyncWAL) Available() <-chan struct{} {
	return w.available
}

func (w fakeViewSyncWAL) QueryViewHandler() worknodehandler.QueryViewHandler {
	return fakeViewSyncQueryViewHandler{}
}

type wrappedTestWAL struct {
	wal.WAL
	raw wal.WAL
}

func (w wrappedTestWAL) UnwrapWAL() wal.WAL {
	return w.raw
}

type fakeViewSyncQueryViewHandler struct{}

func (fakeViewSyncQueryViewHandler) ApplyViews([]worknodehandler.ApplyView) {}

type testSyncQueryViewServerStream struct {
	ctx         context.Context
	sendCh      chan *viewpb.SyncResponse
	recvCh      chan *viewpb.SyncRequest
	recvStarted chan struct{}
	recvOnce    sync.Once
}

func newTestSyncQueryViewServerStream(ctx context.Context) *testSyncQueryViewServerStream {
	return &testSyncQueryViewServerStream{
		ctx:         ctx,
		sendCh:      make(chan *viewpb.SyncResponse, 8),
		recvCh:      make(chan *viewpb.SyncRequest, 8),
		recvStarted: make(chan struct{}),
	}
}

func (s *testSyncQueryViewServerStream) Send(resp *viewpb.SyncResponse) error {
	s.sendCh <- resp
	return nil
}

func (s *testSyncQueryViewServerStream) Recv() (*viewpb.SyncRequest, error) {
	s.recvOnce.Do(func() {
		close(s.recvStarted)
	})
	req, ok := <-s.recvCh
	if !ok {
		return nil, io.EOF
	}
	return req, nil
}

func (s *testSyncQueryViewServerStream) SetHeader(metadata.MD) error  { return nil }
func (s *testSyncQueryViewServerStream) SendHeader(metadata.MD) error { return nil }
func (s *testSyncQueryViewServerStream) SetTrailer(metadata.MD)       {}
func (s *testSyncQueryViewServerStream) Context() context.Context     { return s.ctx }
func (s *testSyncQueryViewServerStream) SendMsg(interface{}) error    { return nil }
func (s *testSyncQueryViewServerStream) RecvMsg(interface{}) error    { return nil }
