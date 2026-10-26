package snview

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

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/replicadiscovery"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// Only the WAL provider is substituted; metadata routing, service, protobuf
// transport and the actual directory run unchanged.
type replicaTestWAL struct{ fakeViewSyncWAL }

func (*replicaTestWAL) WatchReplicas() (*replicadiscovery.Watch, error) { panic("mockey") }

func TestReplicaWatchServerWALUnavailable(t *testing.T) {
	directory := replicadiscovery.NewDirectory()
	defer directory.Close()
	directory.Set("p0_1v0", 17, &qviews.QueryViewVersion{})
	patch := mockey.Mock((*replicaTestWAL).WatchReplicas).To(func(*replicaTestWAL) (*replicadiscovery.Watch, error) { return directory.Watch(), nil }).Build()
	defer patch.UnPatch()
	unavailable := make(chan struct{})
	endpoint := NewPChannelViewQueryServer(&fakeViewSyncWALManager{wal: &replicaTestWAL{fakeViewSyncWAL{unavailable: unavailable}}}, nil)
	listener := bufconn.Listen(64 * 1024)
	server := grpc.NewServer()
	viewpb.RegisterViewQueryServiceServer(server, endpoint)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	defer listener.Close()
	conn, err := grpc.NewClient("passthrough:///replica-owner", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	client := viewpb.NewViewQueryServiceClient(conn)
	stream, err := client.WatchVChannelReplicas(worknodehandler.EncodeQueryViewPChannelToOutgoingContext(ctx, types.PChannelInfo{Name: "p0", Term: 7}))
	require.NoError(t, err)
	response, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, response.GetReady())
	require.NoError(t, stream.Send(&viewpb.WatchVChannelReplicasRequest{Vchannel: "p0_1v0"}))
	response, err = stream.Recv()
	require.NoError(t, err)
	require.EqualValues(t, 17, response.GetSnapshot().GetReplicas()[0].GetReplicaId())
	close(unavailable)
	_, err = stream.Recv()
	require.Error(t, err)
	require.True(t, status.AsStreamingError(status.ConvertStreamingError("WatchVChannelReplicas", err)).IsOnShutdown())
	// The WAL directory has not yet closed: unavailable must independently fence RPCs.
	watch := directory.Watch()
	defer watch.Close()
	watch.Subscribe("p0_1v0")
	require.EqualValues(t, 17, watch.Pop().GetReplicas()[0].GetReplicaId())
}
