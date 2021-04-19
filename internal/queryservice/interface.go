package queryserviceimpl

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type ServiceBase = typeutil.Component

type Interface interface {
	ServiceBase

	RegisterNode(req querypb.RegisterNodeRequest) (querypb.RegisterNodeResponse, error)
	ShowCollections(req querypb.ShowCollectionRequest) (querypb.ShowCollectionResponse, error)
	LoadCollection(req querypb.LoadCollectionRequest) error
	ReleaseCollection(req querypb.ReleaseCollectionRequest) error
	ShowPartitions(req querypb.ShowPartitionRequest) (querypb.ShowPartitionResponse, error)
	LoadPartitions(req querypb.LoadPartitionRequest) error
	ReleasePartitions(req querypb.ReleasePartitionRequest) error
	CreateQueryChannel() (querypb.CreateQueryChannelResponse, error)
	GetPartitionStates(req querypb.PartitionStatesRequest) (querypb.PartitionStatesResponse, error)
}
