package grpcmixcoordclient

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type prewarmAdmissionServer struct {
	querypb.UnimplementedQueryCoordServer
	calls          atomic.Int32
	transportError bool
}

func (s *prewarmAdmissionServer) PrewarmCollection(context.Context, *querypb.PrewarmCollectionRequest) (*commonpb.Status, error) {
	s.calls.Add(1)
	if s.transportError {
		return nil, status.Error(codes.Unavailable, "response lost after admission")
	}
	return merr.Status(merr.ErrServiceNotReady), nil
}

func TestPrewarmAdmissionIsNotRetried(t *testing.T) {
	paramtable.Init()
	for _, transportError := range []bool{true, false} {
		t.Run(map[bool]string{true: "transport", false: "status"}[transportError], func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			t.Cleanup(server.Stop)
			service := &prewarmAdmissionServer{transportError: transportError}
			querypb.RegisterQueryCoordServer(server, service)
			go func() { _ = server.Serve(listener) }()
			base := grpcclient.NewClientBase[MixCoordClient](&paramtable.Get().RootCoordGrpcClientCfg, "milvus.proto.query.QueryCoord")
			base.SetRole(typeutil.MixCoordRole)
			base.SetGetAddrFunc(func() (string, error) { return listener.Addr().String(), nil })
			base.SetNewGrpcClientFunc(func(conn *grpc.ClientConn) MixCoordClient {
				return MixCoordClient{QueryCoordClient: querypb.NewQueryCoordClient(conn)}
			})
			client := &Client{grpcClient: base}
			t.Cleanup(func() { _ = client.Close() })
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			result, err := client.PrewarmCollection(ctx, &querypb.PrewarmCollectionRequest{CollectionID: 100})
			require.Error(t, merr.CheckRPCCall(result, err))
			require.EqualValues(t, 1, service.calls.Load(), "retry would create another background request")
		})
	}
}
