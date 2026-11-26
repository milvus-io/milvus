package syncer

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	qnmanager "github.com/milvus-io/milvus/internal/querynodev2/client/manager"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/views/coord/resourcewatermark"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type watermarkDiscoveryClient struct{ *mockViewSyncClient }

func (*watermarkDiscoveryClient) QueryNodes(context.Context) ([]qviews.QueryNode, error) {
	panic("patch with mockey")
}

func TestResourceWatermarksIdleQNAndReconnect(t *testing.T) {
	mockey.PatchConvey("one stream per idle QN, periodic feedback, reconnect and membership", t, func() {
		listener := bufconn.Listen(1 << 20)
		server := grpc.NewServer()
		var memory atomic.Int64
		memory.Store(90)
		viewpb.RegisterViewSyncServiceServer(server, handler.NewViewSyncServer(qnview.NewQNQueryViewHandler(nil), handler.WithResourceWatermarks(func() *viewpb.NodeResourceWatermarks {
			return &viewpb.NodeResourceWatermarks{Memory: float64(memory.Load()), Disk: 20}
		})))
		go func() { _ = server.Serve(listener) }()
		defer server.Stop()
		conn, err := grpc.NewClient("passthrough:///watermarks", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
		require.NoError(t, err)
		defer conn.Close()
		client := &watermarkDiscoveryClient{newMockViewSyncClient()}
		qn := qviews.QueryNode{ID: 1}
		client.addNode(qn)
		var listingFails atomic.Bool
		mockey.Mock((*watermarkDiscoveryClient).QueryNodes).To(func(_ *watermarkDiscoveryClient, _ context.Context) ([]qviews.QueryNode, error) {
			if listingFails.Load() {
				return nil, merr.ErrServiceUnavailable
			}
			return []qviews.QueryNode{qn, qn}, nil
		}).Build()
		attempts := make(chan context.CancelFunc, 8)
		client.openStreamFn = func(ctx context.Context, node qviews.WorkNode) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
			child, cancel := context.WithCancel(ctx)
			t.Cleanup(cancel)
			attempts <- cancel
			return viewpb.NewViewSyncServiceClient(conn).SyncQueryView(child)
		}
		store := resourcewatermark.New()
		rs := NewReliableSyncer(client, nil, nil, WithResourceWatermarks(store)).(*reliableSyncer)
		defer rs.Close()
		await := func(want float64) {
			require.Eventually(t, func() bool { sample, ok := store.Read(1, time.Now()); return ok && sample.Memory == want }, 8*time.Second, 10*time.Millisecond)
		}
		await(90)
		cancel := <-attempts
		memory.Store(40)
		await(40)
		select {
		case <-attempts:
			t.Fatal("idle QN received duplicate streams")
		default:
		}
		listingFails.Store(true)
		rs.ensureQueryNodes()
		cancel()
		memory.Store(30)
		await(30)
		cancel = <-attempts
		defer cancel()
		client.removeNode(qn)
		client.notifyNodeChanged()
		require.Eventually(t, func() bool { _, ok := store.Read(1, time.Now()); return !ok }, time.Second, time.Millisecond)
		require.NoError(t, rs.Close())
		listingFails.Store(false)
		rs.ensureQueryNodes() // closed syncer cannot recreate an idle stream
	})
}

func TestResourceWatermarksOnlyAcceptQNStream(t *testing.T) {
	for _, node := range []qviews.WorkNode{qviews.QueryNode{ID: 1}, qviews.StreamingNode{PChannel: "p"}} {
		client := newMockViewSyncClient()
		streams := make(chan *mockStream, 1)
		client.openStreamFn = func(ctx context.Context, _ qviews.WorkNode) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
			stream := newMockStream(ctx)
			streams <- stream
			return stream, nil
		}
		store := resourcewatermark.New()
		rs := newResumableSyncer(t.Context(), node, client, nil, store)
		stream := <-streams
		stream.recvCh <- &viewpb.SyncResponse{Response: &viewpb.SyncResponse_ResourceWatermarks{ResourceWatermarks: &viewpb.NodeResourceWatermarks{Memory: 60}}}
		if _, qn := node.(qviews.QueryNode); qn {
			require.Eventually(t, func() bool { _, ok := store.Read(1, time.Now()); return ok }, time.Second, time.Millisecond)
		} else {
			require.Eventually(t, func() bool { return len(stream.recvCh) == 0 }, time.Second, time.Millisecond)
			_, ok := store.Read(1, time.Now())
			require.False(t, ok)
		}
		rs.Close()
		_, ok := store.Read(1, time.Now())
		require.False(t, ok, "close invalidates the last watermark")
	}
}

func TestResourceWatermarksQueryNodeDiscovery(t *testing.T) {
	mockey.PatchConvey("empty and failed discovery are not zero-capacity nodes", t, func() {
		client := NewDefaultViewSyncClient(nil, nil)
		nodes, err := client.QueryNodes(t.Context())
		require.NoError(t, err)
		require.Empty(t, nodes)
		manager := &fakeQueryNodeViewSyncClient{nodes: map[int64]*qnmanager.NodeInfo{1: {ServerID: 1}}}
		client = NewDefaultViewSyncClient(manager, nil)
		nodes, err = client.QueryNodes(t.Context())
		require.NoError(t, err)
		require.Equal(t, []qviews.QueryNode{{ID: 1}}, nodes)
		mockey.Mock((*fakeQueryNodeViewSyncClient).GetAllQueryNodes).Return(nil, context.Canceled).Build()
		nodes, err = client.QueryNodes(t.Context())
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, nodes)
	})
}
