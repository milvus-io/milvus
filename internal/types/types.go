package types

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

type TimeTickProvider interface {
	GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error)
}

type Component interface {
	Init() error
	Start() error
	Stop() error
	GetComponentStates(ctx context.Context) (*internalpb2.ComponentStates, error)
	GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error)
}

type DataNode interface {
	Component

	WatchDmChannels(ctx context.Context, in *datapb.WatchDmChannelRequest) (*commonpb.Status, error)
	FlushSegments(ctx context.Context, in *datapb.FlushSegRequest) (*commonpb.Status, error)
}

type DataService interface {
	Component
	TimeTickProvider

	RegisterNode(ctx context.Context, req *datapb.RegisterNodeRequest) (*datapb.RegisterNodeResponse, error)
	Flush(ctx context.Context, req *datapb.FlushRequest) (*commonpb.Status, error)

	AssignSegmentID(ctx context.Context, req *datapb.AssignSegIDRequest) (*datapb.AssignSegIDResponse, error)
	ShowSegments(ctx context.Context, req *datapb.ShowSegmentRequest) (*datapb.ShowSegmentResponse, error)
	GetSegmentStates(ctx context.Context, req *datapb.SegmentStatesRequest) (*datapb.SegmentStatesResponse, error)
	GetInsertBinlogPaths(ctx context.Context, req *datapb.InsertBinlogPathRequest) (*datapb.InsertBinlogPathsResponse, error)
	GetSegmentInfoChannel(ctx context.Context) (*milvuspb.StringResponse, error)
	GetInsertChannels(ctx context.Context, req *datapb.InsertChannelRequest) (*internalpb2.StringList, error)
	GetCollectionStatistics(ctx context.Context, req *datapb.CollectionStatsRequest) (*datapb.CollectionStatsResponse, error)
	GetPartitionStatistics(ctx context.Context, req *datapb.PartitionStatsRequest) (*datapb.PartitionStatsResponse, error)
	GetCount(ctx context.Context, req *datapb.CollectionCountRequest) (*datapb.CollectionCountResponse, error)
	GetSegmentInfo(ctx context.Context, req *datapb.SegmentInfoRequest) (*datapb.SegmentInfoResponse, error)
}

type IndexNode interface {
	Component
	TimeTickProvider

	BuildIndex(ctx context.Context, req *indexpb.BuildIndexCmd) (*commonpb.Status, error)
	DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error)
}

type IndexService interface {
	Component
	TimeTickProvider

	RegisterNode(ctx context.Context, req *indexpb.RegisterNodeRequest) (*indexpb.RegisterNodeResponse, error)
	BuildIndex(ctx context.Context, req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error)
	DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error)
	GetIndexStates(ctx context.Context, req *indexpb.IndexStatesRequest) (*indexpb.IndexStatesResponse, error)
	GetIndexFilePaths(ctx context.Context, req *indexpb.IndexFilePathsRequest) (*indexpb.IndexFilePathsResponse, error)
	NotifyBuildIndex(ctx context.Context, nty *indexpb.BuildIndexNotification) (*commonpb.Status, error)
}

type MasterService interface {
	Component

	//DDL request
	CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error)
	DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error)
	HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error)
	DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)
	ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error)
	CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error)
	DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error)
	HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error)
	ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error)

	//index builder service
	CreateIndex(ctx context.Context, in *milvuspb.CreateIndexRequest) (*commonpb.Status, error)
	DescribeIndex(ctx context.Context, in *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error)
	DropIndex(ctx context.Context, in *milvuspb.DropIndexRequest) (*commonpb.Status, error)

	//global timestamp allocator
	AllocTimestamp(ctx context.Context, in *masterpb.TsoRequest) (*masterpb.TsoResponse, error)
	AllocID(ctx context.Context, in *masterpb.IDRequest) (*masterpb.IDResponse, error)

	//TODO, master load these channel form config file ?

	//receiver time tick from proxy service, and put it into this channel
	GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error)

	//receive ddl from rpc and time tick from proxy service, and put them into this channel
	GetDdChannel(ctx context.Context) (*milvuspb.StringResponse, error)

	//just define a channel, not used currently
	GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error)

	//segment
	DescribeSegment(ctx context.Context, in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error)
	ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentRequest) (*milvuspb.ShowSegmentResponse, error)
}

type ProxyNode interface {
	Component

	InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error)

	//TODO: move to milvus service
	/*
		CreateCollection(ctx context.Context, request *milvuspb.CreateCollectionRequest) (*commonpb.Status, error)
		DropCollection(ctx context.Context, request *milvuspb.DropCollectionRequest) (*commonpb.Status, error)
		HasCollection(ctx context.Context, request *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error)
		LoadCollection(ctx context.Context, request *milvuspb.LoadCollectionRequest) (*commonpb.Status, error)
		ReleaseCollection(ctx context.Context, request *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error)
		DescribeCollection(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)
		GetCollectionStatistics(ctx context.Context, request *milvuspb.CollectionStatsRequest) (*milvuspb.CollectionStatsResponse, error)
		ShowCollections(ctx context.Context, request *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error)

		CreatePartition(ctx context.Context, request *milvuspb.CreatePartitionRequest) (*commonpb.Status, error)
		DropPartition(ctx context.Context, request *milvuspb.DropPartitionRequest) (*commonpb.Status, error)
		HasPartition(ctx context.Context, request *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error)
		LoadPartitions(ctx context.Context, request *milvuspb.LoadPartitonRequest) (*commonpb.Status, error)
		ReleasePartitions(ctx context.Context, request *milvuspb.ReleasePartitionRequest) (*commonpb.Status, error)
		GetPartitionStatistics(ctx context.Context, request *milvuspb.PartitionStatsRequest) (*milvuspb.PartitionStatsResponse, error)
		ShowPartitions(ctx context.Context, request *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error)

		CreateIndex(ctx context.Context, request *milvuspb.CreateIndexRequest) (*commonpb.Status, error)
		DescribeIndex(ctx context.Context, request *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error)
		GetIndexState(ctx context.Context, request *milvuspb.IndexStateRequest) (*milvuspb.IndexStateResponse, error)
		DropIndex(ctx context.Context, request *milvuspb.DropIndexRequest) (*commonpb.Status, error)

		Insert(ctx context.Context, request *milvuspb.InsertRequest) (*milvuspb.InsertResponse, error)
		Search(ctx context.Context, request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error)
		Flush(ctx context.Context, request *milvuspb.FlushRequest) (*commonpb.Status, error)

		GetDdChannel(ctx context.Context, request *commonpb.Empty) (*milvuspb.StringResponse, error)

		GetQuerySegmentInfo(ctx context.Context, req *milvuspb.QuerySegmentInfoRequest) (*milvuspb.QuerySegmentInfoResponse, error)
		GetPersistentSegmentInfo(ctx context.Context, req *milvuspb.PersistentSegmentInfoRequest) (*milvuspb.PersistentSegmentInfoResponse, error)
	*/
}

type ProxyService interface {
	Component
	TimeTickProvider

	RegisterNode(ctx context.Context, request *proxypb.RegisterNodeRequest) (*proxypb.RegisterNodeResponse, error)
	InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error)
}

type QueryNode interface {
	Component
	TimeTickProvider

	AddQueryChannel(ctx context.Context, in *querypb.AddQueryChannelsRequest) (*commonpb.Status, error)
	RemoveQueryChannel(ctx context.Context, in *querypb.RemoveQueryChannelsRequest) (*commonpb.Status, error)
	WatchDmChannels(ctx context.Context, in *querypb.WatchDmChannelsRequest) (*commonpb.Status, error)
	LoadSegments(ctx context.Context, in *querypb.LoadSegmentRequest) (*commonpb.Status, error)
	ReleaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest) (*commonpb.Status, error)
	ReleasePartitions(ctx context.Context, in *querypb.ReleasePartitionRequest) (*commonpb.Status, error)
	ReleaseSegments(ctx context.Context, in *querypb.ReleaseSegmentRequest) (*commonpb.Status, error)
	GetSegmentInfo(ctx context.Context, in *querypb.SegmentInfoRequest) (*querypb.SegmentInfoResponse, error)
}

type QueryService interface {
	Component
	TimeTickProvider

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
