package queryserviceimpl

import "github.com/zilliztech/milvus-distributed/internal/proto/querypb"

type Interface interface {
	RegisterNode(req querypb.RegisterNodeRequest) (querypb.RegisterNodeResponse, error)

	ShowCollections(req querypb.ShowCollectionRequest) (querypb.ShowCollectionResponse, error)
	LoadCollection(req querypb.LoadCollectionRequest) error
	ReleaseCollection(req querypb.ReleaseCollectionRequest) error

	ShowPartitions(req querypb.ShowPartitionRequest) (querypb.ShowPartitionResponse, error)
	GetPartitionStates(req querypb.PartitionStatesRequest) (querypb.PartitionStatesResponse, error)
	LoadPartitions(req querypb.LoadPartitionRequest) error
	ReleasePartitions(req querypb.ReleasePartitionRequest) error

	CreateQueryChannel() (querypb.CreateQueryChannelResponse, error)
}
