package typeutil

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
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

type QueryNodeInterface interface {
	Component

	AddQueryChannel(in *querypb.AddQueryChannelsRequest) (*commonpb.Status, error)
	RemoveQueryChannel(in *querypb.RemoveQueryChannelsRequest) (*commonpb.Status, error)
	WatchDmChannels(in *querypb.WatchDmChannelsRequest) (*commonpb.Status, error)
	LoadSegments(in *querypb.LoadSegmentRequest) (*commonpb.Status, error)
	ReleaseSegments(in *querypb.ReleaseSegmentRequest) (*commonpb.Status, error)
}
