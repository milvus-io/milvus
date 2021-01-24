package datanode

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"

	"google.golang.org/grpc"
)

type Client struct {
	ctx     context.Context
	grpc    datapb.DataNodeClient
	conn    *grpc.ClientConn
	address string
}

func NewClient(address string) *Client {
	return &Client{
		address: address,
	}
}

func (c *Client) Init() error {
	ctx, cancel := context.WithTimeout(context.Background(), RPCConnectionTimeout)
	defer cancel()
	var err error
	for i := 0; i < Retry; i++ {
		if c.conn, err = grpc.DialContext(ctx, c.address, grpc.WithInsecure(), grpc.WithBlock()); err == nil {
			break
		}
	}
	if err != nil {
		return err
	}
	c.grpc = datapb.NewDataNodeClient(c.conn)
	return nil
}

func (c *Client) Start() error {
	return nil
}

func (c *Client) Stop() error {
	return c.conn.Close()
}

func (c *Client) GetComponentStates(empty *commonpb.Empty) (*internalpb2.ComponentStates, error) {
	return c.grpc.GetComponentStates(context.Background(), empty)
}

func (c *Client) WatchDmChannels(in *datapb.WatchDmChannelRequest) (*commonpb.Status, error) {
	return c.grpc.WatchDmChannels(context.Background(), in)
}

func (c *Client) FlushSegments(in *datapb.FlushSegRequest) (*commonpb.Status, error) {
	return c.grpc.FlushSegments(context.Background(), in)
}
