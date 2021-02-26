package grpcquerynodeclient

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

const (
	RPCConnectionTimeout = 30 * time.Second
	Retry                = 3
)

type Client struct {
	ctx        context.Context
	grpcClient querypb.QueryNodeClient
	conn       *grpc.ClientConn
	addr       string
}

func NewClient(address string) *Client {
	return &Client{
		addr: address,
	}
}

func (c *Client) Init() error {
	ctx, cancel := context.WithTimeout(context.Background(), RPCConnectionTimeout)
	defer cancel()
	var err error
	for i := 0; i < Retry; i++ {
		if c.conn, err = grpc.DialContext(ctx, c.addr, grpc.WithInsecure(), grpc.WithBlock()); err == nil {
			break
		}
	}
	if err != nil {
		return err
	}
	c.grpcClient = querypb.NewQueryNodeClient(c.conn)
	return nil
}

func (c *Client) Start() error {
	return nil
}

func (c *Client) Stop() error {
	return c.conn.Close()
}

func (c *Client) GetComponentStates() (*internalpb2.ComponentStates, error) {
	return c.grpcClient.GetComponentStates(context.TODO(), nil)
}

func (c *Client) GetTimeTickChannel() (string, error) {
	response, err := c.grpcClient.GetTimeTickChannel(context.TODO(), nil)
	if err != nil {
		return "", err
	}
	return response.Value, nil
}

func (c *Client) GetStatisticsChannel() (string, error) {
	response, err := c.grpcClient.GetStatsChannel(context.TODO(), nil)
	if err != nil {
		return "", err
	}
	return response.Value, nil
}

func (c *Client) AddQueryChannel(in *querypb.AddQueryChannelsRequest) (*commonpb.Status, error) {
	return c.grpcClient.AddQueryChannel(context.TODO(), in)
}

func (c *Client) RemoveQueryChannel(in *querypb.RemoveQueryChannelsRequest) (*commonpb.Status, error) {
	return c.grpcClient.RemoveQueryChannel(context.TODO(), in)
}

func (c *Client) WatchDmChannels(in *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	return c.grpcClient.WatchDmChannels(context.TODO(), in)
}

func (c *Client) LoadSegments(in *querypb.LoadSegmentRequest) (*commonpb.Status, error) {
	return c.grpcClient.LoadSegments(context.TODO(), in)
}

func (c *Client) ReleaseCollection(in *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return c.grpcClient.ReleaseCollection(context.TODO(), in)
}

func (c *Client) ReleasePartitions(in *querypb.ReleasePartitionRequest) (*commonpb.Status, error) {
	return c.grpcClient.ReleasePartitions(context.TODO(), in)
}

func (c *Client) ReleaseSegments(in *querypb.ReleaseSegmentRequest) (*commonpb.Status, error) {
	return c.grpcClient.ReleaseSegments(context.TODO(), in)
}

func (c *Client) GetSegmentInfo(in *querypb.SegmentInfoRequest) (*querypb.SegmentInfoResponse, error) {
	return c.grpcClient.GetSegmentInfo(context.TODO(), in)
}
