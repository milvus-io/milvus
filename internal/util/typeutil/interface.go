package typeutil

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

type Service interface {
	Init() error
	Start() error
	Stop() error
}

type Component interface {
	GetComponentStates() (*internalpb2.ComponentStates, error)
	GetTimeTickChannel() (string, error)
	GetStatisticsChannel() (string, error)
}

type IndexNodeInterface interface {
	Service
	Component
	BuildIndex(req *indexpb.BuildIndexCmd) (*commonpb.Status, error)
}

type IndexServiceInterface interface {
	Service
	Component
	RegisterNode(req *indexpb.RegisterNodeRequest) (*indexpb.RegisterNodeResponse, error)
	BuildIndex(req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error)
	GetIndexStates(req *indexpb.IndexStatesRequest) (*indexpb.IndexStatesResponse, error)
	GetIndexFilePaths(req *indexpb.IndexFilePathsRequest) (*indexpb.IndexFilePathsResponse, error)
	NotifyBuildIndex(nty *indexpb.BuildIndexNotification) (*commonpb.Status, error)
}

type QueryServiceInterface interface {
	Service
	Component

	RegisterNode(req *querypb.RegisterNodeRequest) (*querypb.RegisterNodeResponse, error)
	ShowCollections(req *querypb.ShowCollectionRequest) (*querypb.ShowCollectionResponse, error)
	LoadCollection(req *querypb.LoadCollectionRequest) (*commonpb.Status, error)
	ReleaseCollection(req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error)
	ShowPartitions(req *querypb.ShowPartitionRequest) (*querypb.ShowPartitionResponse, error)
	LoadPartitions(req *querypb.LoadPartitionRequest) (*commonpb.Status, error)
	ReleasePartitions(req *querypb.ReleasePartitionRequest) (*commonpb.Status, error)
	CreateQueryChannel() (*querypb.CreateQueryChannelResponse, error)
	GetPartitionStates(req *querypb.PartitionStatesRequest) (*querypb.PartitionStatesResponse, error)
}
