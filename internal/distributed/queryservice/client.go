package queryservice

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

type Client struct {
	grpcClient querypb.QueryServiceClient
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

func (c *Client) RegisterNode(req querypb.RegisterNodeRequest) (querypb.RegisterNodeResponse, error) {
	panic("implement me")
}

func (c *Client) ShowCollections(req querypb.ShowCollectionRequest) (querypb.ShowCollectionResponse, error) {
	panic("implement me")
}

func (c *Client) LoadCollection(req querypb.LoadCollectionRequest) error {
	panic("implement me")
}

func (c *Client) ReleaseCollection(req querypb.ReleaseCollectionRequest) error {
	panic("implement me")
}

func (c *Client) ShowPartitions(req querypb.ShowPartitionRequest) (querypb.ShowPartitionResponse, error) {
	panic("implement me")
}

func (c *Client) LoadPartitions(req querypb.LoadPartitionRequest) error {
	panic("implement me")
}

func (c *Client) ReleasePartitions(req querypb.ReleasePartitionRequest) error {
	panic("implement me")
}

func (c *Client) CreateQueryChannel() (querypb.CreateQueryChannelResponse, error) {
	panic("implement me")
}

func (c *Client) GetPartitionStates(req querypb.PartitionStatesRequest) (querypb.PartitionStatesResponse, error) {
	panic("implement me")
}
