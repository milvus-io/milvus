package dql

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type overloadTaskClient struct {
	types.QueryNodeClient
	base *grpcclient.ClientBase[querypb.QueryNodeClient]
}

func (c *overloadTaskClient) Search(ctx context.Context, _ *querypb.SearchRequest, _ ...grpc.CallOption) (*internalpb.SearchResults, error) {
	res, err := c.base.Call(ctx, func(querypb.QueryNodeClient) (any, error) {
		return &internalpb.SearchResults{Status: merr.Status(merr.WrapErrTooManyRequests(1))}, nil
	})
	if err != nil {
		return nil, err
	}
	return res.(*internalpb.SearchResults), nil
}

func (c *overloadTaskClient) Query(ctx context.Context, _ *querypb.QueryRequest, _ ...grpc.CallOption) (*internalpb.RetrieveResults, error) {
	res, err := c.base.Call(ctx, func(querypb.QueryNodeClient) (any, error) {
		return &internalpb.RetrieveResults{Status: merr.Status(merr.WrapErrTooManyRequests(1))}, nil
	})
	if err != nil {
		return nil, err
	}
	return res.(*internalpb.RetrieveResults), nil
}

func TestLegacyQueueOverloadKeepsShardLeaderCache(t *testing.T) {
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	base := &grpcclient.ClientBase[querypb.QueryNodeClient]{
		ClientMaxRecvSize: 1024 * 1024, ClientMaxSendSize: 1024 * 1024,
		DialTimeout: time.Second, KeepAliveTime: time.Minute, KeepAliveTimeout: time.Minute,
		RetryServiceNameConfig: "milvus.proto.query.QueryNode", MaxAttempts: 3,
		InitialBackoff: 0.01, MaxBackoff: 0.01,
	}
	base.SetRole(typeutil.QueryNodeRole)
	base.SetGetAddrFunc(func() (string, error) { return listener.Addr().String(), nil })
	base.SetNewGrpcClientFunc(func(cc *grpc.ClientConn) querypb.QueryNodeClient { return querypb.NewQueryNodeClient(cc) })
	defer base.Close()
	client := &overloadTaskClient{base: base}
	for _, kind := range []string{"search", "query"} {
		t.Run(kind, func(t *testing.T) {
			// No cache invalidation is expected for a wire-encoded queue refusal.
			manager := shardclient.NewMockShardClientManager(t)
			var err error
			if kind == "search" {
				task := &SearchTask{SearchRequest: &internalpb.SearchRequest{Base: &commonpb.MsgBase{}}, shardClientMgr: manager}
				err = task.searchShard(context.Background(), 1, client, "ch")
			} else {
				task := &QueryTask{RetrieveRequest: &internalpb.RetrieveRequest{Base: &commonpb.MsgBase{}}, shardclientMgr: manager}
				err = task.queryShard(context.Background(), 1, client, "ch")
			}
			require.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
		})
	}
}
