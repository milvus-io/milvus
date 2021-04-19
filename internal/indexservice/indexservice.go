package indexservice

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type IndexService struct {
	// implement Service

	//nodeClients [] .Interface
	// factory method

}

func (i IndexService) Init() {
	panic("implement me")
}

func (i IndexService) Start() {
	panic("implement me")
}

func (i IndexService) Stop() {
	panic("implement me")
}

func (i IndexService) GetServiceStates() (internalpb2.ServiceStates, error) {
	panic("implement me")
}

func (i IndexService) GetTimeTickChannel() (string, error) {
	panic("implement me")
}

func (i IndexService) GetStatisticsChannel() (string, error) {
	panic("implement me")
}

func (i IndexService) RegisterNode(req indexpb.RegisterNodeRequest) (indexpb.RegisterNodeResponse, error) {
	panic("implement me")
}

func (i IndexService) BuildIndex(req indexpb.BuildIndexRequest) (indexpb.BuildIndexResponse, error) {
	panic("implement me")
}

func (i IndexService) GetIndexStates(req indexpb.IndexStatesRequest) (indexpb.IndexStatesResponse, error) {
	panic("implement me")
}

func (i IndexService) GetIndexFilePaths(req indexpb.IndexFilePathRequest) (indexpb.IndexFilePathsResponse, error) {
	panic("implement me")
}

func NewIndexServiceImpl() Interface {
	return &IndexService{}
}
