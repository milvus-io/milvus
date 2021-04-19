package grpcproxyservice

import (
	"context"

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

func NewClient(address string) *Client {
	return &Client{
		address: address,
		ctx:     context.Background(),
	}
}
