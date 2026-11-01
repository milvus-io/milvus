package snview

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	serviceinterceptor "github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	streamingstatus "github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	streamingErr := streamingstatus.AsStreamingError(err)
	require.True(t, streamingErr.IsWrongStreamingNode())
	require.Same(t, server.walManager.(*fakeViewSyncWALManager).err, err) // wire encoding belongs to the interceptor
}

func TestPChannelViewSyncServerClosesStreamWhenWALCloses(t *testing.T) {
	walDone := make(chan struct{})
	server := NewPChannelViewSyncServer(&fakeViewSyncWALManager{
		wal: fakeViewSyncWAL{unavailable: walDone},
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
	raw := fakeViewSyncWAL{unavailable: walDone}
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
	unavailable <-chan struct{}
}

func (w fakeViewSyncWAL) Unavailable() <-chan struct{} {
	return w.unavailable
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

func TestViewSyncTransportUsesViewErrorProtocol(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failure error
		code    viewpb.ViewCode
	}{
		{"missing WAL", streamingstatus.NewChannelNotExist("p0"), viewpb.ViewCode_VIEW_CODE_NOT_PRIMARY},
		{"old term", streamingstatus.NewUnmatchedChannelTerm("p0", 1, 2), viewpb.ViewCode_VIEW_CODE_NOT_PRIMARY},
		{"closed manager", streamingstatus.NewOnShutdownError("closed"), viewpb.ViewCode_VIEW_CODE_ON_SHUTDOWN},
	} {
		mockey.PatchConvey(tc.name, t, func() {
			mockey.Mock((*fakeViewSyncWALManager).GetAvailableWAL).Return(nil, tc.failure).Build()
			listener := bufconn.Listen(1024 * 1024)
			server := grpc.NewServer(grpc.StreamInterceptor(serviceinterceptor.NewServiceStreamServerInterceptor()))
			viewpb.RegisterViewSyncServiceServer(server, NewPChannelViewSyncServer(&fakeViewSyncWALManager{}))
			go func() { _ = server.Serve(listener) }()
			defer server.Stop()
			defer listener.Close()
			conn, err := grpc.NewClient("passthrough:///view-sync", grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
				grpc.WithStreamInterceptor(serviceinterceptor.NewServiceStreamClientInterceptor(typeutil.StreamingNodeRole)))
			require.NoError(t, err)
			defer conn.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			ctx = worknodehandler.EncodeQueryViewPChannelToOutgoingContext(ctx, types.PChannelInfo{Name: "p0", Term: 1, AccessMode: types.AccessModeRW})
			stream, err := viewpb.NewViewSyncServiceClient(conn).SyncQueryView(ctx)
			require.NoError(t, err)
			_, err = stream.Recv()
			require.Error(t, err)
			require.Equal(t, tc.code, viewerror.AsViewError(err).Code)
			for _, detail := range grpcstatus.Convert(err).Details() {
				_, isStreaming := detail.(*streamingpb.StreamingError)
				require.False(t, isStreaming)
			}
		})
	}
}
