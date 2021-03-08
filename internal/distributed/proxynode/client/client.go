package grpcproxynodeclient

import (
	"context"
	"time"

	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/zilliztech/milvus-distributed/internal/util/retry"
	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
)

type Client struct {
	grpcClient proxypb.ProxyNodeServiceClient
	address    string
	ctx        context.Context
}

func (c *Client) GetComponentStates(ctx context.Context) (*internalpb2.ComponentStates, error) {
	//TODO
	panic("implement me")
}

func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	//TODO
	panic("implement me")
}

func (c *Client) Init() error {
	tracer := opentracing.GlobalTracer()
	connectGrpcFunc := func() error {
		conn, err := grpc.DialContext(c.ctx, c.address, grpc.WithInsecure(), grpc.WithBlock(),
			grpc.WithUnaryInterceptor(
				otgrpc.OpenTracingClientInterceptor(tracer)),
			grpc.WithStreamInterceptor(
				otgrpc.OpenTracingStreamClientInterceptor(tracer)))
		if err != nil {
			return err
		}
		c.grpcClient = proxypb.NewProxyNodeServiceClient(conn)
		return nil
	}
	err := retry.Retry(10, time.Millisecond*200, connectGrpcFunc)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Start() error {
	return nil
}

func (c *Client) Stop() error {
	return nil
}

func (c *Client) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	return c.grpcClient.InvalidateCollectionMetaCache(ctx, request)
}

func NewClient(ctx context.Context, address string) *Client {
	return &Client{
		address: address,
		ctx:     ctx,
	}
}
