package grpcproxynode

import (
	"context"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
)

type Client struct {
	client  proxypb.ProxyNodeServiceClient
	address string
	ctx     context.Context
}

func (c *Client) tryConnect() error {
	if c.client != nil {
		return nil
	}
	conn, err := grpc.DialContext(c.ctx, c.address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return err
	}
	c.client = proxypb.NewProxyNodeServiceClient(conn)
	return nil
}

func (c *Client) InvalidateCollectionMetaCache(request *proxypb.InvalidateCollMetaCacheRequest) error {
	var err error
	err = c.tryConnect()
	if err != nil {
		return err
	}
	_, err = c.client.InvalidateCollectionMetaCache(c.ctx, request)
	return err
}

func NewClient(ctx context.Context, address string) *Client {
	return &Client{
		address: address,
		ctx:     ctx,
	}
}
