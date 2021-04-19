package grpcquerynodeclient

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

type Client struct {
	ctx        context.Context
	grpcClient querypb.QueryNodeClient
}

func (c *Client) GetComponentStates() (*internalpb2.ComponentStates, error) {
	states, err := c.grpcClient.GetComponentStates(context.TODO(), nil)
	if err != nil {
		return nil, err
	}
	return states.States, nil
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

func (c *Client) ReleaseSegments(in *querypb.ReleaseSegmentRequest) (*commonpb.Status, error) {
	return c.grpcClient.ReleaseSegments(context.TODO(), in)
}

func NewClient(address string) *Client {
	ctx1, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx1, address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Printf("connect to queryNode failed, error= %v", err)
	}
	log.Printf("connected to queryNode, queryNode=%s", address)

	return &Client{
		grpcClient: querypb.NewQueryNodeClient(conn),
	}
}
