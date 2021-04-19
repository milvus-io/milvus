package typeutil

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

type Service interface {
	Init() error
	Start() error
	Stop() error
}

type Component interface {
	GetComponentStates(ctx context.Context) (*internalpb2.ComponentStates, error)
	GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error)
	GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error)
}

type IndexNodeInterface interface {
	Service
	Component
	BuildIndex(ctx context.Context, req *indexpb.BuildIndexCmd) (*commonpb.Status, error)
	DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error)
}

type IndexServiceInterface interface {
	Service
	Component
	RegisterNode(ctx context.Context, req *indexpb.RegisterNodeRequest) (*indexpb.RegisterNodeResponse, error)
	BuildIndex(ctx context.Context, req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error)
	GetIndexStates(ctx context.Context, req *indexpb.IndexStatesRequest) (*indexpb.IndexStatesResponse, error)
	GetIndexFilePaths(ctx context.Context, req *indexpb.IndexFilePathsRequest) (*indexpb.IndexFilePathsResponse, error)
	NotifyBuildIndex(ctx context.Context, nty *indexpb.BuildIndexNotification) (*commonpb.Status, error)
}

type QueryServiceInterface interface {
	Service
	Component

	RegisterNode(ctx context.Context, req *querypb.RegisterNodeRequest) (*querypb.RegisterNodeResponse, error)
	ShowCollections(ctx context.Context, req *querypb.ShowCollectionRequest) (*querypb.ShowCollectionResponse, error)
	LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error)
	ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error)
	ShowPartitions(ctx context.Context, req *querypb.ShowPartitionRequest) (*querypb.ShowPartitionResponse, error)
	LoadPartitions(ctx context.Context, req *querypb.LoadPartitionRequest) (*commonpb.Status, error)
	ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionRequest) (*commonpb.Status, error)
	CreateQueryChannel(ctx context.Context) (*querypb.CreateQueryChannelResponse, error)
	GetPartitionStates(ctx context.Context, req *querypb.PartitionStatesRequest) (*querypb.PartitionStatesResponse, error)
	GetSegmentInfo(ctx context.Context, req *querypb.SegmentInfoRequest) (*querypb.SegmentInfoResponse, error)
}
