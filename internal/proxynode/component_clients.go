package proxynode

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
)

type MasterClientInterface interface {
	Init() error
	Start() error
	Stop() error

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
	Init() error
	Start() error
	Stop() error

	GetIndexStates(req *indexpb.IndexStatesRequest) (*indexpb.IndexStatesResponse, error)
}

type QueryServiceClient interface {
	Init() error
	Start() error
	Stop() error

	GetSearchChannelNames() ([]string, error)
	GetSearchResultChannelNames() ([]string, error)
}

type DataServiceClient interface {
	Init() error
	Start() error
	Stop() error

	GetInsertChannelNames() ([]string, error)
}

func (node *NodeImpl) SetMasterClient(cli MasterClientInterface) {
	node.masterClient = cli
}

func (node *NodeImpl) SetIndexServiceClient(cli IndexServiceClient) {
	node.indexServiceClient = cli
}

func (node *NodeImpl) SetQueryServiceClient(cli QueryServiceClient) {
	node.queryServiceClient = cli
}

func (node *NodeImpl) SetDataServiceClient(cli DataServiceClient) {
	node.dataServiceClient = cli
}
