package grpcproxyservice

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
)

type Client struct {
	proxyServiceClient proxypb.ProxyServiceClient
	address            string
	ctx                context.Context
}

func (c *Client) tryConnect() error {
	if c.proxyServiceClient != nil {
		return nil
	}
	conn, err := grpc.DialContext(c.ctx, c.address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return err
	}
	c.proxyServiceClient = proxypb.NewProxyServiceClient(conn)
	return nil
}

func (c *Client) RegisterNode(request *proxypb.RegisterNodeRequest) (*proxypb.RegisterNodeResponse, error) {
	err := c.tryConnect()
	if err != nil {
		return nil, err
	}
	return c.proxyServiceClient.RegisterNode(c.ctx, request)
}

func (c *Client) InvalidateCollectionMetaCache(request *proxypb.InvalidateCollMetaCacheRequest) error {
	var err error
	err = c.tryConnect()
	if err != nil {
		return err
	}
	_, err = c.proxyServiceClient.InvalidateCollectionMetaCache(c.ctx, request)
	return err
}

func (c *Client) GetTimeTickChannel() (string, error) {
	response, err := c.proxyServiceClient.GetTimeTickChannel(c.ctx, &commonpb.Empty{})
	if err != nil {
		return "", err
	}
	return response.Value, nil
}

func (c *Client) GetComponentStates() (*internalpb2.ComponentStates, error) {
	return c.proxyServiceClient.GetComponentStates(c.ctx, &commonpb.Empty{})
}

func NewClient(address string) *Client {
	return &Client{
		address: address,
		ctx:     context.Background(),
	}
}
