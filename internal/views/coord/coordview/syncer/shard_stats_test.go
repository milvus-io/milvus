package syncer

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/views/coord/shardstats"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestShardStatsPersistentStreamWithoutViews(t *testing.T) {
	mockey.PatchConvey("discovery, idle connection, changes and reconnect over gRPC", t, func() {
		listener := bufconn.Listen(1 << 20)
		server := grpc.NewServer()
		h := &snview.SNQueryViewHandler{}
		id := qviews.ShardID{ReplicaID: 1, VChannel: "p_1v0"}
		h.RecordShardAccess(id)
		viewpb.RegisterViewSyncServiceServer(server, handler.NewViewSyncServer(h))
		go func() { _ = server.Serve(listener) }()
		defer server.Stop()
		conn, err := grpc.NewClient("passthrough:///stats", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
		require.NoError(t, err)
		defer conn.Close()
		reports := make(chan []qviews.ShardStats, 16)
		sink := &shardstats.ReplicaStatsStore{}
		mockey.Mock((*shardstats.ReplicaStatsStore).OnShardStats).To(func(_ *shardstats.ReplicaStatsStore, samples []qviews.ShardStats) { reports <- samples }).Build()
		mockey.Mock((*mockViewSyncClient).WatchPChannels).To(func(_ *mockViewSyncClient, ctx context.Context, cb func([]string)) error {
			cb([]string{"p", "p"})
			cb(nil)
			<-ctx.Done()
			return ctx.Err()
		}).Build()
		attempts := make(chan context.CancelFunc, 8)
		client := newMockViewSyncClient()
		client.openStreamFn = func(ctx context.Context, node qviews.WorkNode) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
			child, cancel := context.WithCancel(ctx)
			t.Cleanup(cancel)
			attempts <- cancel
			return viewpb.NewViewSyncServiceClient(conn).SyncQueryView(child)
		}
		rs := NewReliableSyncer(client, sink)
		defer rs.Close()
		next := func() []qviews.ShardStats {
			select {
			case batch := <-reports:
				return batch
			case <-time.After(8 * time.Second):
				t.Fatal("missing stats report")
				return nil
			}
		}
		first := next()
		require.Len(t, first, 1)
		require.EqualValues(t, 1, first[0].RequestCount)
		cancel := <-attempts
		select {
		case <-reports:
			t.Fatal("idle counter was reported again")
		case <-time.After(5500 * time.Millisecond):
		}
		h.RecordShardAccess(id)
		require.EqualValues(t, 2, next()[0].RequestCount)
		cancel()
		require.EqualValues(t, 2, next()[0].RequestCount, "reconnect sends unchanged baseline")
		cancel = <-attempts
		defer cancel()
		h.RecordShardAccess(id)
		require.EqualValues(t, 3, next()[0].RequestCount)
		require.NoError(t, rs.Close())
		select {
		case <-attempts:
			t.Fatal("duplicate discovery created another stream")
		default:
		}
	})
}

func TestShardStatsRoutingAndDiscoveryRetry(t *testing.T) {
	mockey.PatchConvey("stats accepts only the owning SN, discovery retries", t, func() {
		var received []qviews.ShardStats
		sink := &shardstats.ReplicaStatsStore{}
		mockey.Mock((*shardstats.ReplicaStatsStore).OnShardStats).To(func(_ *shardstats.ReplicaStatsStore, s []qviews.ShardStats) { received = append(received, s...) }).Build()
		report := &viewpb.ShardStatsReport{Shards: []*viewpb.ShardStats{nil, {ShardId: &viewpb.ShardID{Vchannel: "other_1v0", ReplicaId: 1}}, {ShardId: &viewpb.ShardID{Vchannel: "p_1v0", ReplicaId: 1}, RequestCount: 3, LastAccessUnixNanos: 5}}}
		rs := &resumableSyncer{node: qviews.QueryNode{}, statsHandler: sink}
		rs.receiveShardStats(report)
		require.Empty(t, received)
		rs.node = qviews.StreamingNode{PChannel: "p"}
		rs.receiveShardStats(report)
		require.Len(t, received, 1)
		require.EqualValues(t, 3, received[0].RequestCount)
		rs.statsHandler = nil
		rs.receiveShardStats(report)
		require.Len(t, received, 1)
		var calls atomic.Int32
		mockey.Mock((*mockViewSyncClient).WatchPChannels).To(func(_ *mockViewSyncClient, ctx context.Context, cb func([]string)) error {
			if calls.Add(1) == 1 {
				return io.ErrUnexpectedEOF
			}
			cb([]string{"p"})
			<-ctx.Done()
			cb([]string{"late"})
			return ctx.Err()
		}).Build()
		client := newMockViewSyncClient()
		s := NewReliableSyncer(client, nil)
		require.Eventually(t, func() bool { return calls.Load() >= 2 }, 3*time.Second, 10*time.Millisecond)
		require.NoError(t, s.Close())
	})
}

func TestShardStatsDefaultClientDiscovery(t *testing.T) {
	mockey.PatchConvey("forward discovery and allow QN-only clients to close", t, func() {
		mockey.Mock((*fakeStreamingNodeViewSyncClient).WatchPChannels).To(func(_ *fakeStreamingNodeViewSyncClient, _ context.Context, cb func([]string)) error {
			cb([]string{"p"})
			return io.EOF
		}).Build()
		client := NewDefaultViewSyncClient(nil, &fakeStreamingNodeViewSyncClient{})
		var channels []string
		require.ErrorIs(t, client.WatchPChannels(t.Context(), func(c []string) { channels = c }), io.EOF)
		require.Equal(t, []string{"p"}, channels)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.ErrorIs(t, NewDefaultViewSyncClient(nil, nil).WatchPChannels(ctx, func([]string) { t.Fatal("unexpected snapshot") }), context.Canceled)
	})
}
