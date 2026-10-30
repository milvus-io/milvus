package grpcquerynode

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestQueryViewShutdownClosesSyncStream(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	h := registerQueryViewServers(server, noopQNSegmentManager{}, 100)
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///queryview", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	stream, err := viewpb.NewViewSyncServiceClient(conn).SyncQueryView(ctx)
	require.NoError(t, err)
	require.NoError(t, h.Close(ctx))
	response, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, response.GetClose())
	require.NoError(t, stream.CloseSend())
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)
	done := make(chan struct{})
	go func() { server.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("ViewSync blocked gRPC graceful shutdown")
	}
}
