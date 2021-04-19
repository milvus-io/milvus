package grpcproxyserviceclient

import (
	"context"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
	"github.com/zilliztech/milvus-distributed/internal/util/retry"
)

type Client struct {
	proxyServiceClient proxypb.ProxyServiceClient
	address            string
	ctx                context.Context
}

func (c *Client) Init() error {
	connectGrpcFunc := func() error {
		conn, err := grpc.DialContext(c.ctx, c.address, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			return err
		}
		c.proxyServiceClient = proxypb.NewProxyServiceClient(conn)
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

func (c *Client) RegisterNode(request *proxypb.RegisterNodeRequest) (*proxypb.RegisterNodeResponse, error) {
	return c.proxyServiceClient.RegisterNode(c.ctx, request)
}

func (c *Client) InvalidateCollectionMetaCache(request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	_, err := c.proxyServiceClient.InvalidateCollectionMetaCache(c.ctx, request)
	return nil, err
}

func (c *Client) GetTimeTickChannel() (*milvuspb.StringResponse, error) {
	return c.proxyServiceClient.GetTimeTickChannel(c.ctx, &commonpb.Empty{})
}

func (c *Client) GetComponentStates() (*internalpb2.ComponentStates, error) {
	return c.proxyServiceClient.GetComponentStates(c.ctx, &commonpb.Empty{})
}

func (c *Client) GetStatisticsChannel() (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
	}, nil
}

func NewClient(address string) *Client {
	return &Client{
		address: address,
		ctx:     context.Background(),
	}
}
