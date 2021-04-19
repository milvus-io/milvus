package grpcindexnode

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type Client struct {
	grpcClient indexpb.IndexNodeClient
}

func (c Client) Init() {
	panic("implement me")
}

func (c Client) Start() {
	panic("implement me")
}

func (c Client) Stop() {
	panic("implement me")
}

func (c Client) GetServiceStates() (internalpb2.ServiceStates, error) {
	panic("implement me")
}

func (c Client) GetTimeTickChannel() (string, error) {
	panic("implement me")
}

func (c Client) GetStatisticsChannel() (string, error) {
	panic("implement me")
}

func (c Client) BuildIndex(req indexpb.BuildIndexRequest) (indexpb.BuildIndexResponse, error) {
	panic("implement me")
}

func NewClient() *Client {
	return &Client{}
}
