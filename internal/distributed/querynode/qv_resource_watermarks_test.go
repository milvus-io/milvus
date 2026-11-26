package grpcquerynode

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

	qn "github.com/milvus-io/milvus/internal/querynodev2"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestServerRegisterQueryViewWatermarks(t *testing.T) {
	mockey.PatchConvey("production QN service registration wires loader feedback", t, func() {
		paramtable.Init()
		node := qn.NewQueryNode(t.Context(), nil)
		mockey.Mock((*qn.QueryNode).QueryViewResourceWatermarks).Return(&viewpb.NodeResourceWatermarks{Memory: 65, Disk: 35}).Build()
		grpcServer := grpc.NewServer()
		server := &Server{grpcServer: grpcServer, querynode: node, ctx: t.Context()}
		server.registerQueryViewServers()
		listener := bufconn.Listen(1 << 20)
		go func() { _ = grpcServer.Serve(listener) }()
		defer grpcServer.Stop()
		conn, err := grpc.NewClient("passthrough:///qn-watermarks", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
		require.NoError(t, err)
		defer conn.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		stream, err := viewpb.NewViewSyncServiceClient(conn).SyncQueryView(ctx)
		require.NoError(t, err)
		report, err := stream.Recv()
		require.NoError(t, err)
		require.Equal(t, 65.0, report.GetResourceWatermarks().GetMemory())
		require.Equal(t, 35.0, report.GetResourceWatermarks().GetDisk())
	})
}
