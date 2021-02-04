package grpcindexnodeclient

import (
	"context"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"

	"github.com/zilliztech/milvus-distributed/internal/util/retry"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"google.golang.org/grpc"
)

type Client struct {
	grpcClient indexpb.IndexNodeClient
	address    string
	ctx        context.Context
}

func (c *Client) Init() error {
	connectGrpcFunc := func() error {
		conn, err := grpc.DialContext(c.ctx, c.address, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			return err
		}
		c.grpcClient = indexpb.NewIndexNodeClient(conn)
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

func (c *Client) GetComponentStates() (*internalpb2.ComponentStates, error) {
	return c.grpcClient.GetComponentStates(context.Background(), &commonpb.Empty{})
}

func (c *Client) GetTimeTickChannel() (*milvuspb.StringResponse, error) {
	return c.grpcClient.GetTimeTickChannel(context.Background(), &commonpb.Empty{})
}

func (c *Client) GetStatisticsChannel() (*milvuspb.StringResponse, error) {
	return c.grpcClient.GetStatisticsChannel(context.Background(), &commonpb.Empty{})
}

func (c *Client) BuildIndex(req *indexpb.BuildIndexCmd) (*commonpb.Status, error) {
	ctx := context.TODO()
	return c.grpcClient.BuildIndex(ctx, req)
}

func NewClient(nodeAddress string) (*Client, error) {

	return &Client{
		address: nodeAddress,
		ctx:     context.Background(),
	}, nil
}
