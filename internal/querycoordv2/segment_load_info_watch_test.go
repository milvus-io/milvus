package querycoordv2

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	componenttypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestQueryViewSegmentLoadInfoWatchSession_ClearsSubscriptionsOnStreamClose(t *testing.T) {
	watcher := newQueryViewSegmentLoadInfoWatcher()
	session := &queryViewSegmentLoadInfoWatchSession{
		stream:  &eofSegmentLoadInfoWatchServer{ctx: context.Background()},
		watcher: watcher,
		subscriptions: map[int64]queryViewSegmentLoadInfoSubscription{
			1000: {collectionID: 100, segmentID: 1000},
		},
		notifyCh: make(chan struct{}, 1),
		dirty:    make(map[int64]struct{}),
	}
	watcher.register(session)
	require.NotEmpty(t, watcher.sessions)

	err := session.run()
	require.NoError(t, err)
	assert.Empty(t, session.subscriptions)
	assert.Empty(t, watcher.sessions)
	assert.Empty(t, watcher.bySegment)
}

func TestQueryViewSegmentLoadInfoWatchSession_PushesSnapshotOnNotify(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	segmentID := int64(1000)
	collectionID := int64(100)
	loadInfo := &querypb.SegmentLoadInfo{
		CollectionID: collectionID,
		SegmentID:    segmentID,
		NumOfRows:    10,
	}
	mixCoord := &fakeSegmentLoadInfoWatchMixCoord{
		loadInfo: loadInfo,
		indexes:  []*indexpb.IndexInfo{{CollectionID: collectionID, IndexID: 10}},
	}
	server := &Server{
		ctx:                    ctx,
		mixCoord:               mixCoord,
		segmentLoadInfoWatcher: newQueryViewSegmentLoadInfoWatcher(),
	}
	server.UpdateStateCode(commonpb.StateCode_Healthy)
	stream := newChannelSegmentLoadInfoWatchServer(ctx)

	done := make(chan error, 1)
	go func() {
		done <- server.WatchQueryViewSegmentLoadInfo(stream)
	}()

	stream.recv <- &querypb.WatchQueryViewSegmentLoadInfoRequest{
		Subscribe: []*querypb.WatchQueryViewSegmentLoadInfoSubscription{{
			CollectionID: collectionID,
			SegmentID:    segmentID,
			Revision:     calculateQueryViewSegmentLoadInfoRevision(loadInfo, mixCoord.indexes),
		}},
	}
	assertNoWatchResponse(t, stream.send)

	mixCoord.setLoadInfo(&querypb.SegmentLoadInfo{
		CollectionID: collectionID,
		SegmentID:    segmentID,
		NumOfRows:    20,
	})
	server.NotifyQueryViewSegmentLoadInfoChanged(collectionID, segmentID)

	resp := requireWatchResponse(t, stream.send)
	require.True(t, merr.Ok(resp.GetStatus()))
	require.Len(t, resp.GetSnapshots(), 1)
	assert.Equal(t, segmentID, resp.GetSnapshots()[0].GetSegmentID())
	assert.Equal(t, int64(20), resp.GetSnapshots()[0].GetLoadInfo().GetNumOfRows())

	stream.closeRecv()
	require.NoError(t, <-done)

	mixCoord.setLoadInfo(&querypb.SegmentLoadInfo{
		CollectionID: collectionID,
		SegmentID:    segmentID,
		NumOfRows:    30,
	})
	server.NotifyQueryViewSegmentLoadInfoChanged(collectionID, segmentID)
	assertNoWatchResponse(t, stream.send)
}

type eofSegmentLoadInfoWatchServer struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *eofSegmentLoadInfoWatchServer) Send(*querypb.WatchQueryViewSegmentLoadInfoResponse) error {
	return nil
}

func (s *eofSegmentLoadInfoWatchServer) Recv() (*querypb.WatchQueryViewSegmentLoadInfoRequest, error) {
	return nil, io.EOF
}

func (s *eofSegmentLoadInfoWatchServer) Context() context.Context {
	return s.ctx
}

type channelSegmentLoadInfoWatchServer struct {
	grpc.ServerStream
	ctx    context.Context
	recv   chan *querypb.WatchQueryViewSegmentLoadInfoRequest
	send   chan *querypb.WatchQueryViewSegmentLoadInfoResponse
	closed chan struct{}
	once   sync.Once
}

func newChannelSegmentLoadInfoWatchServer(ctx context.Context) *channelSegmentLoadInfoWatchServer {
	return &channelSegmentLoadInfoWatchServer{
		ctx:    ctx,
		recv:   make(chan *querypb.WatchQueryViewSegmentLoadInfoRequest, 8),
		send:   make(chan *querypb.WatchQueryViewSegmentLoadInfoResponse, 8),
		closed: make(chan struct{}),
	}
}

func (s *channelSegmentLoadInfoWatchServer) Send(resp *querypb.WatchQueryViewSegmentLoadInfoResponse) error {
	select {
	case s.send <- resp:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *channelSegmentLoadInfoWatchServer) Recv() (*querypb.WatchQueryViewSegmentLoadInfoRequest, error) {
	select {
	case req := <-s.recv:
		return req, nil
	case <-s.closed:
		return nil, io.EOF
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *channelSegmentLoadInfoWatchServer) Context() context.Context {
	return s.ctx
}

func (s *channelSegmentLoadInfoWatchServer) closeRecv() {
	s.once.Do(func() {
		close(s.closed)
	})
}

func requireWatchResponse(t *testing.T, ch <-chan *querypb.WatchQueryViewSegmentLoadInfoResponse) *querypb.WatchQueryViewSegmentLoadInfoResponse {
	t.Helper()
	select {
	case resp := <-ch:
		return resp
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for watch response")
		return nil
	}
}

func assertNoWatchResponse(t *testing.T, ch <-chan *querypb.WatchQueryViewSegmentLoadInfoResponse) {
	t.Helper()
	select {
	case resp := <-ch:
		t.Fatalf("unexpected watch response: %v", resp)
	case <-time.After(100 * time.Millisecond):
	}
}

type fakeSegmentLoadInfoWatchMixCoord struct {
	componenttypes.MixCoord
	mu       sync.Mutex
	loadInfo *querypb.SegmentLoadInfo
	indexes  []*indexpb.IndexInfo
}

func (m *fakeSegmentLoadInfoWatchMixCoord) setLoadInfo(loadInfo *querypb.SegmentLoadInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.loadInfo = loadInfo
}

func (m *fakeSegmentLoadInfoWatchMixCoord) GetQueryViewSegmentLoadInfos(ctx context.Context, collectionID int64, segmentIDs []int64) ([]*querypb.SegmentLoadInfo, []*indexpb.IndexInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return []*querypb.SegmentLoadInfo{m.loadInfo}, m.indexes, nil
}
