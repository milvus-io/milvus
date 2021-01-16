package querynode

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

type Client struct {
	ctx        context.Context
	grpcClient querypb.QueryNodeClient
}

func (c *Client) Init() {
	panic("implement me")
}

func (c *Client) Start() {
	panic("implement me")
}

func (c *Client) Stop() {
	panic("implement me")
}

func (c *Client) GetServiceStates() (internalpb2.ServiceStates, error) {
	panic("implement me")
}

func (c *Client) GetTimeTickChannel() (string, error) {
	panic("implement me")
}

func (c *Client) GetStatisticsChannel() (string, error) {
	panic("implement me")
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

func (c *Client) GetPartitionState(in *querypb.PartitionStatesRequest) (*querypb.PartitionStatesResponse, error) {
	return c.grpcClient.GetPartitionState(context.TODO(), in)
}
