package queryserviceimpl

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

type QueryService struct {
}

//serverBase interface
func (qs *QueryService) Init() {
	panic("implement me")
}

func (qs *QueryService) Start() {
	panic("implement me")
}

func (qs *QueryService) Stop() {
	panic("implement me")
}

func (qs *QueryService) GetServiceStates() (internalpb2.ServiceStates, error) {
	panic("implement me")
}

func (qs *QueryService) GetTimeTickChannel() (string, error) {
	panic("implement me")
}

func (qs *QueryService) GetStatisticsChannel() (string, error) {
	panic("implement me")
}

//queryService interface
func (qs *QueryService) RegisterNode(req querypb.RegisterNodeRequest) (querypb.RegisterNodeResponse, error) {
	panic("implement me")
}

func (qs *QueryService) ShowCollections(req querypb.ShowCollectionRequest) (querypb.ShowCollectionResponse, error) {
	panic("implement me")
}

func (qs *QueryService) LoadCollection(req querypb.LoadCollectionRequest) error {
	panic("implement me")
}

func (qs *QueryService) ReleaseCollection(req querypb.ReleaseCollectionRequest) error {
	panic("implement me")
}

func (qs *QueryService) ShowPartitions(req querypb.ShowPartitionRequest) (querypb.ShowPartitionResponse, error) {
	panic("implement me")
}

func (qs *QueryService) LoadPartitions(req querypb.LoadPartitionRequest) error {
	panic("implement me")
}

func (qs *QueryService) ReleasePartitions(req querypb.ReleasePartitionRequest) error {
	panic("implement me")
}

func (qs *QueryService) CreateQueryChannel() (querypb.CreateQueryChannelResponse, error) {
	panic("implement me")
}

func (qs *QueryService) GetPartitionStates(req querypb.PartitionStatesRequest) (querypb.PartitionStatesResponse, error) {
	panic("implement me")
}
