//go:build test && dynamic

package grpcmixcoordclient

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestSegmentLoadInfoWatcher_ReopensStreamAndResubscribes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opener := &fakeSegmentLoadInfoWatchStreamOpener{streams: make(chan *fakeSegmentLoadInfoWatchStream, 2)}
	snapshots := make(chan qnview.SegmentLoadInfoSnapshot, 1)
	watcher := newSegmentLoadInfoWatcher(ctx, opener.watch, func(_ context.Context, snapshot qnview.SegmentLoadInfoSnapshot) {
		snapshots <- snapshot
	})
	defer watcher.Close()

	watcher.Subscribe(qnview.SegmentLoadInfoSubscription{
		CollectionID: 100,
		SegmentID:    1000,
		Revision:     qnview.SegmentLoadInfoRevision{Revision: 1},
	})

	stream1 := opener.waitStream(t)
	req1 := stream1.waitSend(t)
	require.Len(t, req1.GetSubscribe(), 1)
	assert.Equal(t, int64(1000), req1.GetSubscribe()[0].GetSegmentID())
	assert.Equal(t, uint64(1), req1.GetSubscribe()[0].GetRevision().GetLoadInfoRevision())

	close(stream1.recv)

	stream2 := opener.waitStream(t)
	defer close(stream2.recv)
	req2 := stream2.waitSend(t)
	require.Len(t, req2.GetSubscribe(), 1)
	assert.Equal(t, int64(1000), req2.GetSubscribe()[0].GetSegmentID())
	assert.Equal(t, uint64(1), req2.GetSubscribe()[0].GetRevision().GetLoadInfoRevision())

	stream2.recv <- &querypb.WatchQueryViewSegmentLoadInfoResponse{
		Snapshots: []*querypb.QueryViewSegmentLoadInfoSnapshot{{
			CollectionID: 100,
			SegmentID:    1000,
			Revision:     &querypb.QueryViewSegmentLoadInfoRevision{LoadInfoRevision: 2},
			LoadInfo:     &querypb.SegmentLoadInfo{SegmentID: 1000, CollectionID: 100},
		}},
	}

	select {
	case snapshot := <-snapshots:
		assert.Equal(t, int64(1000), snapshot.SegmentID)
		assert.Equal(t, qnview.SegmentLoadInfoRevision{Revision: 2}, snapshot.Revision)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for snapshot")
	}
}

type fakeSegmentLoadInfoWatchStreamOpener struct {
	streams chan *fakeSegmentLoadInfoWatchStream
}

func (o *fakeSegmentLoadInfoWatchStreamOpener) watch(context.Context) (querypb.QueryCoord_WatchQueryViewSegmentLoadInfoClient, error) {
	stream := &fakeSegmentLoadInfoWatchStream{
		sent: make(chan *querypb.WatchQueryViewSegmentLoadInfoRequest, 8),
		recv: make(chan *querypb.WatchQueryViewSegmentLoadInfoResponse, 8),
	}
	o.streams <- stream
	return stream, nil
}

func (o *fakeSegmentLoadInfoWatchStreamOpener) waitStream(t *testing.T) *fakeSegmentLoadInfoWatchStream {
	t.Helper()
	select {
	case stream := <-o.streams:
		return stream
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watch stream")
		return nil
	}
}

type fakeSegmentLoadInfoWatchStream struct {
	grpc.ClientStream
	sent chan *querypb.WatchQueryViewSegmentLoadInfoRequest
	recv chan *querypb.WatchQueryViewSegmentLoadInfoResponse
}

func (s *fakeSegmentLoadInfoWatchStream) Send(req *querypb.WatchQueryViewSegmentLoadInfoRequest) error {
	s.sent <- req
	return nil
}

func (s *fakeSegmentLoadInfoWatchStream) Recv() (*querypb.WatchQueryViewSegmentLoadInfoResponse, error) {
	resp, ok := <-s.recv
	if !ok {
		return nil, io.EOF
	}
	return resp, nil
}

func (s *fakeSegmentLoadInfoWatchStream) CloseSend() error {
	return nil
}

func (s *fakeSegmentLoadInfoWatchStream) waitSend(t *testing.T) *querypb.WatchQueryViewSegmentLoadInfoRequest {
	t.Helper()
	select {
	case req := <-s.sent:
		return req
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watch request")
		return nil
	}
}
