package grpcindexservice

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type Client struct {
	grpcClient indexpb.IndexServiceClient
}

func (g Client) Init() {
	panic("implement me")
}

func (g Client) Start() {
	panic("implement me")
}

func (g Client) Stop() {
	panic("implement me")
}

func (g Client) GetServiceStates() (internalpb2.ServiceStates, error) {
	panic("implement me")
}

func (g Client) GetTimeTickChannel() (string, error) {
	panic("implement me")
}

func (g Client) GetStatisticsChannel() (string, error) {
	panic("implement me")
}

func (g Client) RegisterNode(req indexpb.RegisterNodeRequest) (indexpb.RegisterNodeResponse, error) {
	panic("implement me")
}

func (g Client) BuildIndex(req indexpb.BuildIndexRequest) (indexpb.BuildIndexResponse, error) {
	panic("implement me")
}

func (g Client) GetIndexStates(req indexpb.IndexStatesRequest) (indexpb.IndexStatesResponse, error) {
	panic("implement me")
}

func (g Client) GetIndexFilePaths(req indexpb.IndexFilePathRequest) (indexpb.IndexFilePathsResponse, error) {
	panic("implement me")
}

func NewClient() *Client {
	return &Client{}
}
