package proxynode

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

type MasterClient interface {
	CreateCollection(in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error)
	DropCollection(in *milvuspb.DropCollectionRequest) (*commonpb.Status, error)
	HasCollection(in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error)
	DescribeCollection(in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)
	ShowCollections(in *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error)
	CreatePartition(in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error)
	DropPartition(in *milvuspb.DropPartitionRequest) (*commonpb.Status, error)
	HasPartition(in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error)
	ShowPartitions(in *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error)
	CreateIndex(in *milvuspb.CreateIndexRequest) (*commonpb.Status, error)
	DescribeIndex(in *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error)
}

type IndexServiceClient interface {
	GetIndexStates(req *indexpb.IndexStatesRequest) (*indexpb.IndexStatesResponse, error)
	GetComponentStates() (*internalpb2.ComponentStates, error)
}

type QueryServiceClient interface {
	ShowCollections(req *querypb.ShowCollectionRequest) (*querypb.ShowCollectionResponse, error)
	LoadCollection(req *querypb.LoadCollectionRequest) (*commonpb.Status, error)
	ReleaseCollection(req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error)
	ShowPartitions(req *querypb.ShowPartitionRequest) (*querypb.ShowPartitionResponse, error)
	LoadPartitions(req *querypb.LoadPartitionRequest) (*commonpb.Status, error)
	ReleasePartitions(req *querypb.ReleasePartitionRequest) (*commonpb.Status, error)
	CreateQueryChannel() (*querypb.CreateQueryChannelResponse, error)
	GetPartitionStates(req *querypb.PartitionStatesRequest) (*querypb.PartitionStatesResponse, error)

	//GetSearchChannelNames() ([]string, error)
	//GetSearchResultChannels() ([]string, error)
	GetComponentStates() (*internalpb2.ComponentStates, error)
}

type DataServiceClient interface {
	AssignSegmentID(req *datapb.AssignSegIDRequest) (*datapb.AssignSegIDResponse, error)
	GetInsertChannels(req *datapb.InsertChannelRequest) ([]string, error)
	Flush(req *datapb.FlushRequest) (*commonpb.Status, error)
	GetCollectionStatistics(req *datapb.CollectionStatsRequest) (*datapb.CollectionStatsResponse, error)

	GetComponentStates() (*internalpb2.ComponentStates, error)
}

type ProxyServiceClient interface {
	GetTimeTickChannel() (string, error)
	RegisterNode(request *proxypb.RegisterNodeRequest) (*proxypb.RegisterNodeResponse, error)
	GetComponentStates() (*internalpb2.ComponentStates, error)
}

type ProxyNode interface {
	Init() error
	Start() error
	Stop() error

	InvalidateCollectionMetaCache(request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error)

	CreateCollection(request *milvuspb.CreateCollectionRequest) (*commonpb.Status, error)
	DropCollection(request *milvuspb.DropCollectionRequest) (*commonpb.Status, error)
	HasCollection(request *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error)
	LoadCollection(request *milvuspb.LoadCollectionRequest) (*commonpb.Status, error)
	ReleaseCollection(request *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error)
	DescribeCollection(request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)
	GetCollectionStatistics(request *milvuspb.CollectionStatsRequest) (*milvuspb.CollectionStatsResponse, error)
	ShowCollections(request *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error)

	CreatePartition(request *milvuspb.CreatePartitionRequest) (*commonpb.Status, error)
	DropPartition(request *milvuspb.DropPartitionRequest) (*commonpb.Status, error)
	HasPartition(request *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error)
	LoadPartitions(request *milvuspb.LoadPartitonRequest) (*commonpb.Status, error)
	ReleasePartitions(request *milvuspb.ReleasePartitionRequest) (*commonpb.Status, error)
	GetPartitionStatistics(request *milvuspb.PartitionStatsRequest) (*milvuspb.PartitionStatsResponse, error)
	ShowPartitions(request *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error)

	CreateIndex(request *milvuspb.CreateIndexRequest) (*commonpb.Status, error)
	DescribeIndex(request *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error)
	GetIndexState(request *milvuspb.IndexStateRequest) (*milvuspb.IndexStateResponse, error)

	Insert(request *milvuspb.InsertRequest) (*milvuspb.InsertResponse, error)
	Search(request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error)
	Flush(request *milvuspb.FlushRequest) (*commonpb.Status, error)

	GetDdChannel(request *commonpb.Empty) (*milvuspb.StringResponse, error)
}
