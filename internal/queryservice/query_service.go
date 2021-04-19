package queryserviceimpl

import "github.com/zilliztech/milvus-distributed/internal/proto/querypb"

type QueryService struct {
}

func (qs *QueryService) RegisterNode(req querypb.RegisterNodeRequest) (querypb.RegisterNodeResponse, error) {
	return querypb.RegisterNodeResponse{}, nil
}

func (qs *QueryService) ShowCollections(req querypb.ShowCollectionRequest) (querypb.ShowCollectionResponse, error) {
	return querypb.ShowCollectionResponse{}, nil
}

func (qs *QueryService) LoadCollection(req querypb.LoadCollectionRequest) error {
	return nil
}

func (qs *QueryService) ReleaseCollection(req querypb.ReleaseCollectionRequest) error {
	return nil
}

func (qs *QueryService) ShowPartitions(req querypb.ShowPartitionRequest) (querypb.ShowPartitionResponse, error) {
	return querypb.ShowPartitionResponse{}, nil
}

func (qs *QueryService) GetPartitionStates(req querypb.PartitionStatesRequest) (querypb.PartitionStatesResponse, error) {
	return querypb.PartitionStatesResponse{}, nil
}

func (qs *QueryService) LoadPartitions(req querypb.LoadPartitionRequest) error {
	return nil
}

func (qs *QueryService) ReleasePartitions(req querypb.ReleasePartitionRequest) error {
	return nil
}

func (qs *QueryService) CreateQueryChannel() (querypb.CreateQueryChannelResponse, error) {
	return querypb.CreateQueryChannelResponse{}, nil
}
