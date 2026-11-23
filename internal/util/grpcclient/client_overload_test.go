package grpcclient

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// A server that refuses a request as overloaded is asked again by default;
// with WithFailFastOnOverload the caller gets its response at once.
func TestClientBase_FailFastOnOverload(t *testing.T) {
	lis, err := net.Listen("tcp", "localhost:")
	require.NoError(t, err)
	// The calls below answer by themselves: the server is only there to
	// connect to.
	s := grpc.NewServer()
	go func() { s.Serve(lis) }()
	defer s.Stop()

	clientBase := ClientBase[rootcoordpb.RootCoordClient]{
		ClientMaxRecvSize:      1 * 1024 * 1024,
		ClientMaxSendSize:      1 * 1024 * 1024,
		DialTimeout:            60 * time.Second,
		KeepAliveTime:          60 * time.Second,
		KeepAliveTimeout:       60 * time.Second,
		RetryServiceNameConfig: "milvus.proto.rootcoord.RootCoord",
		MaxAttempts:            3,
		InitialBackoff:         0.01,
		MaxBackoff:             0.01,
	}
	clientBase.SetRole(typeutil.DataCoordRole)
	clientBase.SetGetAddrFunc(func() (string, error) { return lis.Addr().String(), nil })
	clientBase.SetNewGrpcClientFunc(func(cc *grpc.ClientConn) rootcoordpb.RootCoordClient {
		return rootcoordpb.NewRootCoordClient(cc)
	})
	defer clientBase.Close()

	overloaded := merr.Status(merr.WrapErrTooManyRequests(200, "read task queue"))
	calls := 0
	caller := func(rootcoordpb.RootCoordClient) (any, error) {
		calls++
		return overloaded, nil
	}

	_, err = clientBase.Call(context.Background(), caller)
	assert.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	assert.Equal(t, 3, calls)

	calls = 0
	res, err := clientBase.Call(WithFailFastOnOverload(context.Background()), caller)
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
	assert.ErrorIs(t, merr.Error(res.(*commonpb.Status)), merr.ErrServiceTooManyRequests)
}
