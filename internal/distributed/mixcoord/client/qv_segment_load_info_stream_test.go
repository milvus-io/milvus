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

func TestSegmentLoadInfoStream_ReopensAndResubscribesFromDeliveredRevision(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opener := &fakeSegmentLoadInfoWatchStreamOpener{
		streams:   make(chan *fakeSegmentLoadInfoWatchStream, 2),
		allowOpen: make(chan struct{}),
	}
	snapshots := make(chan qnview.SegmentLoadInfoSnapshot, 1)
	stream := newSegmentLoadInfoStream(ctx, opener.watch)
	defer stream.Close()
	sub := stream.Subscribe(qnview.SegmentLoadInfoSubscriptionOption{
		CollectionID: 100,
		SegmentID:    1000,
		Revision:     qnview.SegmentLoadInfoRevision{Revision: 1},
		Handler: segmentLoadInfoHandlerFunc(func(snapshot qnview.SegmentLoadInfoSnapshot) error {
			snapshots <- snapshot
			return nil
		}),
	})
	require.NotNil(t, sub)
	defer sub.Close()
	secondSub := stream.Subscribe(qnview.SegmentLoadInfoSubscriptionOption{
		CollectionID: 200,
		SegmentID:    2000,
		Revision:     qnview.SegmentLoadInfoRevision{Revision: 5},
		Handler:      segmentLoadInfoHandlerFunc(func(qnview.SegmentLoadInfoSnapshot) error { return nil }),
	})
	require.NotNil(t, secondSub)
	defer secondSub.Close()
	close(opener.allowOpen)

	stream1 := opener.waitStream(t)
	req1 := stream1.waitSend(t)
	require.Len(t, req1.GetSubscribe(), 2)
	assert.Equal(t, uint64(1), subscriptionRevision(t, req1, 1000))
	assert.Equal(t, uint64(5), subscriptionRevision(t, req1, 2000))
	stream1.recv <- &querypb.WatchQueryViewSegmentLoadInfoResponse{
		Snapshots: []*querypb.QueryViewSegmentLoadInfoSnapshot{{
			CollectionID: 100,
			SegmentID:    1000,
			Revision:     &querypb.QueryViewSegmentLoadInfoRevision{LoadInfoRevision: 2},
			LoadInfo:     &querypb.SegmentLoadInfo{SegmentID: 1000, CollectionID: 100},
		}},
	}
	select {
	case snapshot := <-snapshots:
		assert.Equal(t, qnview.SegmentLoadInfoRevision{Revision: 2}, snapshot.Revision)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for snapshot")
	}
	stream1.assertNoSend(t)

	close(stream1.recv)

	stream2 := opener.waitStream(t)
	defer close(stream2.recv)
	req2 := stream2.waitSend(t)
	require.Len(t, req2.GetSubscribe(), 2)
	assert.Equal(t, uint64(2), subscriptionRevision(t, req2, 1000))
	assert.Equal(t, uint64(5), subscriptionRevision(t, req2, 2000))
}

func TestSegmentLoadInfoStream_KeysSubscriptionsBySegmentID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opener := &fakeSegmentLoadInfoWatchStreamOpener{
		streams:   make(chan *fakeSegmentLoadInfoWatchStream, 1),
		allowOpen: make(chan struct{}),
	}
	stream := newSegmentLoadInfoStream(ctx, opener.watch)
	defer stream.Close()
	handler := segmentLoadInfoHandlerFunc(func(qnview.SegmentLoadInfoSnapshot) error { return nil })
	require.NotNil(t, stream.Subscribe(qnview.SegmentLoadInfoSubscriptionOption{
		CollectionID: 100,
		SegmentID:    1000,
		Revision:     qnview.SegmentLoadInfoRevision{Revision: 1},
		Handler:      handler,
	}))
	require.NotNil(t, stream.Subscribe(qnview.SegmentLoadInfoSubscriptionOption{
		CollectionID: 200,
		SegmentID:    1000,
		Revision:     qnview.SegmentLoadInfoRevision{Revision: 2},
		Handler:      handler,
	}))
	close(opener.allowOpen)

	underlying := opener.waitStream(t)
	defer close(underlying.recv)
	req := underlying.waitSend(t)
	require.Len(t, req.GetSubscribe(), 1)
	assert.Equal(t, int64(200), req.GetSubscribe()[0].GetCollectionID())
	assert.Equal(t, int64(1000), req.GetSubscribe()[0].GetSegmentID())
	assert.Equal(t, uint64(2), req.GetSubscribe()[0].GetRevision().GetLoadInfoRevision())
}

func subscriptionRevision(t *testing.T, req *querypb.WatchQueryViewSegmentLoadInfoRequest, segmentID int64) uint64 {
	t.Helper()
	for _, subscription := range req.GetSubscribe() {
		if subscription.GetSegmentID() == segmentID {
			return subscription.GetRevision().GetLoadInfoRevision()
		}
	}
	t.Fatalf("segment %d subscription not found", segmentID)
	return 0
}

type segmentLoadInfoHandlerFunc func(qnview.SegmentLoadInfoSnapshot) error

func (f segmentLoadInfoHandlerFunc) Handle(snapshot qnview.SegmentLoadInfoSnapshot) error {
	return f(snapshot)
}

func (segmentLoadInfoHandlerFunc) Close() {}

type fakeSegmentLoadInfoWatchStreamOpener struct {
	streams   chan *fakeSegmentLoadInfoWatchStream
	allowOpen chan struct{}
}

func (o *fakeSegmentLoadInfoWatchStreamOpener) watch(ctx context.Context) (querypb.QueryCoord_WatchQueryViewSegmentLoadInfoClient, error) {
	select {
	case <-o.allowOpen:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
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

func (s *fakeSegmentLoadInfoWatchStream) assertNoSend(t *testing.T) {
	t.Helper()
	select {
	case req := <-s.sent:
		t.Fatalf("unexpected watch request: %v", req)
	case <-time.After(100 * time.Millisecond):
	}
}
