package service

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

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type replicaManager struct{ walmanager.Manager }

func (*replicaManager) GetAvailableWAL(types.PChannelInfo) (wal.WAL, error) { panic("mockey") }

type replicaWAL struct{ wal.WAL }

func (*replicaWAL) AcquireReplicaWatchStream(context.Context) (wal.ReplicaWatchStream, error) {
	panic("mockey")
}

func TestReplicaWatchServiceOwnerFence(t *testing.T) {
	mockey.PatchConvey("owner term checked before stream acknowledgement, unavailability ends stream", t, func() {
		d := snview.NewReplicaDirectoryForTest()
		defer d.Close()
		d.Set("p_1v0", 17, &qviews.QueryViewVersion{}, false)
		unavailable := make(chan struct{})
		mockey.Mock((*replicaManager).GetAvailableWAL).To(func(_ *replicaManager, pc types.PChannelInfo) (wal.WAL, error) {
			if pc.Term != 7 {
				return nil, status.NewChannelNotExist(pc.Name)
			}
			require.Equal(t, "p", pc.Name)
			return &replicaWAL{}, nil
		}).Build()
		mockey.Mock((*replicaWAL).AcquireReplicaWatchStream).To(func(_ *replicaWAL, ctx context.Context) (wal.ReplicaWatchStream, error) {
			return d.AcquireStream(ctx, "p", unavailable), nil
		}).Build()
		listener := bufconn.Listen(65536)
		server := grpc.NewServer()
		streamingpb.RegisterStreamingNodeHandlerServiceServer(server, NewHandlerService(&replicaManager{}))
		go func() { _ = server.Serve(listener) }()
		defer server.Stop()
		defer listener.Close()
		conn, err := grpc.NewClient("passthrough:///replica-service", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
		require.NoError(t, err)
		defer conn.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		client := streamingpb.NewStreamingNodeHandlerServiceClient(conn)
		missing, err := client.SubscribeReplicas(ctx)
		require.NoError(t, err)
		_, err = missing.Recv()
		require.Error(t, err)
		metadata := func(term int64) context.Context {
			return contextutil.WithCreateReplicaWatchStream(ctx, &streamingpb.CreateReplicaWatchStreamRequest{Pchannel: &streamingpb.PChannelInfo{Name: "p", Term: term}})
		}
		wrong, err := client.SubscribeReplicas(metadata(6))
		require.NoError(t, err)
		_, err = wrong.Recv()
		require.True(t, status.AsStreamingError(status.ConvertStreamingError("SubscribeReplicas", err)).IsWrongStreamingNode())
		rpc, err := client.SubscribeReplicas(metadata(7))
		require.NoError(t, err)
		ready, err := rpc.Recv()
		require.NoError(t, err)
		require.NotNil(t, ready.GetCreateStream())
		require.NoError(t, rpc.Send(&streamingpb.ReplicaWatchRequest{Request: &streamingpb.ReplicaWatchRequest_Create{Create: &streamingpb.CreateReplicaWatchSubscriptionRequest{SubscriptionId: 1, Vchannel: "p_1v0"}}}))
		created, err := rpc.Recv()
		require.NoError(t, err)
		require.EqualValues(t, 1, created.GetCreate().GetSubscriptionId())
		snapshot, err := rpc.Recv()
		require.NoError(t, err)
		require.EqualValues(t, 17, snapshot.GetSnapshot().GetSnapshot().GetReplicas()[0].GetReplicaId())
		close(unavailable)
		_, err = rpc.Recv()
		require.True(t, status.AsStreamingError(status.ConvertStreamingError("SubscribeReplicas", err)).IsOnShutdown())
		watch := d.Watch()
		defer watch.Close()
		watch.Subscribe("p_1v0")
		require.Len(t, watch.Pop().GetReplicas(), 1)
	})
}
