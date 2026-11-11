package syncer

import (
	"context"
	"net"
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
	mockey.PatchConvey("initial PChannels, idle connection, changes and reconnect over gRPC", t, func() {
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
		reports := make(chan qviews.ShardStatsWindow, 16)
		sink := &shardstats.ReplicaStatsStore{}
		mockey.Mock((*shardstats.ReplicaStatsStore).OnShardStats).To(func(_ *shardstats.ReplicaStatsStore, samples qviews.ShardStatsWindow) { reports <- samples }).Build()
		attempts := make(chan context.CancelFunc, 8)
		client := newMockViewSyncClient()
		client.openStreamFn = func(ctx context.Context, node qviews.WorkNode) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
			child, cancel := context.WithCancel(ctx)
			t.Cleanup(cancel)
			attempts <- cancel
			return viewpb.NewViewSyncServiceClient(conn).SyncQueryView(child)
		}
		rs := NewReliableSyncer(client, sink, []string{"p", "p"})
		defer rs.Close()
		next := func() qviews.ShardStatsWindow {
			select {
			case batch := <-reports:
				return batch
			case <-time.After(8 * time.Second):
				t.Fatal("missing stats report")
				return qviews.ShardStatsWindow{}
			}
		}
		first := next()
		require.Len(t, first.Shards, 1)
		require.EqualValues(t, 1, first.Shards[id].RequestCount)
		cancel := <-attempts
		select {
		case <-reports:
			t.Fatal("idle counter was reported again")
		case <-time.After(5500 * time.Millisecond):
		}
		h.RecordShardAccess(id)
		require.EqualValues(t, 1, next().Shards[id].RequestCount)
		cancel()
		select {
		case <-reports:
			t.Fatal("reconnect replayed a consumed window")
		case <-time.After(time.Second):
		}
		cancel = <-attempts
		defer cancel()
		h.RecordShardAccess(id)
		previous := next()
		require.EqualValues(t, 1, previous.Shards[id].RequestCount)
		for range 2 {
			for i := range qviews.ShardStatsMaxEntries {
				h.RecordShardAccess(qviews.ShardID{ReplicaID: int64(i + 100), VChannel: "p_1v0"})
			}
			var end int64
			for range qviews.ShardStatsMaxEntries / 256 {
				select {
				case batch := <-reports:
					require.Len(t, batch.Shards, 256)
					require.GreaterOrEqual(t, batch.EndUnixNanos-previous.EndUnixNanos, int64(qviews.ShardStatsMinInterval))
					end = batch.EndUnixNanos
				case <-time.After(2 * time.Second):
					t.Fatal("size threshold did not wake sender")
				}
			}
			previous.EndUnixNanos = end
		}
		require.NoError(t, rs.Close())
		select {
		case <-attempts:
			t.Fatal("duplicate initial PChannel created another stream")
		default:
		}
	})
}

func TestShardStatsRouting(t *testing.T) {
	mockey.PatchConvey("stats accepts only the owning SN", t, func() {
		var received []qviews.ShardStats
		sink := &shardstats.ReplicaStatsStore{}
		mockey.Mock((*shardstats.ReplicaStatsStore).OnShardStats).To(func(_ *shardstats.ReplicaStatsStore, s qviews.ShardStatsWindow) {
			for _, sample := range s.Shards {
				received = append(received, sample)
			}
		}).Build()
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
	})
}
