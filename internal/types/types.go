// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package types

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
)

type TimeTickProvider interface {
	GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error)
}

type Component interface {
	Init() error
	Start() error
	Stop() error
	GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error)
	GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error)
	Register() error
}

type DataNode interface {
	Component

	WatchDmChannels(ctx context.Context, req *datapb.WatchDmChannelsRequest) (*commonpb.Status, error)
	FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest) (*commonpb.Status, error)

	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

type DataCoord interface {
	Component
	TimeTickProvider

	// Flush notifies DataCoord to flush all current growing segments of specified Collection
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, which are database name(not used for now) and collection id
	//
	// response struct `FlushResponse` contains related db & collection meta
	// and the affected segment ids
	// error is returned only when some communication issue occurs
	// if some error occurs in the process of `Flush`, it will be recorded and returned in `Status` field of response
	//
	// `Flush` returns when all growing segments of specified collection is "sealed"
	// the flush procedure will wait corresponding data node(s) proceeds to the safe timestamp
	// and the `Flush` operation will be truly invoked
	// If the Datacoord or Datanode crashes in the flush procedure, recovery process will replay the ts check until all requirement is met
	//
	// Flushed segments can be check via `GetFlushedSegments` API
	Flush(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error)

	AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error)
	GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error)
	GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest) (*datapb.GetInsertBinlogPathsResponse, error)
	GetSegmentInfoChannel(ctx context.Context) (*milvuspb.StringResponse, error)
	GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest) (*datapb.GetCollectionStatisticsResponse, error)
	GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest) (*datapb.GetPartitionStatisticsResponse, error)
	GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error)
	GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error)
	SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error)
	GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error)

	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

type IndexNode interface {
	Component
	TimeTickProvider

	// CreateIndex receives request from IndexCoordinator to build an index.
	// Index building is asynchronous, so when an index building request comes, IndexNode records the task and returns.
	CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error)
	// GetMetrics gets the metrics about IndexNode.
	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

type IndexCoord interface {
	Component
	TimeTickProvider

	// BuildIndex receives request from RootCoordinator to build an index.
	// Index building is asynchronous, so when an index building request comes, an IndexBuildID is assigned to the task and
	// the task is recorded in Meta. The background process assignTaskLoop will find this task and assign it to IndexNode for
	// execution.
	BuildIndex(ctx context.Context, req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error)
	// DropIndex deletes indexes based on IndexID. One IndexID corresponds to the index of an entire column. A column is
	// divided into many segments, and each segment corresponds to an IndexBuildID. IndexCoord uses IndexBuildID to record
	// index tasks. Therefore, when DropIndex is called, delete all tasks corresponding to IndexBuildID corresponding to IndexID.
	DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error)
	// GetIndexStates gets the index states of the IndexBuildIDs in the request from RootCoordinator.
	GetIndexStates(ctx context.Context, req *indexpb.GetIndexStatesRequest) (*indexpb.GetIndexStatesResponse, error)
	// GetIndexFilePaths gets the index files of the IndexBuildIDs in the request from RootCoordinator.
	GetIndexFilePaths(ctx context.Context, req *indexpb.GetIndexFilePathsRequest) (*indexpb.GetIndexFilePathsResponse, error)
	// GetMetrics gets the metrics about IndexCoord.
	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

type RootCoord interface {
	Component
	TimeTickProvider

	// CreateCollection notifies RootCoord to create a collection
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(not used), collection name, collection schema and
	//     physical channel num for inserting data
	//
	// The `ErrorCode` of `Status` is `Success` if create collection successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	CreateCollection(ctx context.Context, req *milvuspb.CreateCollectionRequest) (*commonpb.Status, error)

	// DropCollection notifies RootCoord to drop a collection
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(not used) and collection name
	//
	// The `ErrorCode` of `Status` is `Success` if drop collection successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	DropCollection(ctx context.Context, req *milvuspb.DropCollectionRequest) (*commonpb.Status, error)

	// HasCollection notifies RootCoord to check a collection's existence at specified timestamp
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(not used), collection name and timestamp
	//
	// The `Status` in response struct `BoolResponse` indicates if this operation is processed successfully or fail cause;
	// the `Value` in `BoolResponse` is `true` if the collection exists at the specified timestamp, `false` otherwise.
	// Timestamp is ignored if set to 0.
	// error is always nil
	HasCollection(ctx context.Context, req *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error)

	// DescribeCollection notifies RootCoord to get all information about this collection at specified timestamp
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(not used), collection name or collection id, and timestamp
	//
	// The `Status` in response struct `DescribeCollectionResponse` indicates if this operation is processed successfully or fail cause;
	// other fields in `DescribeCollectionResponse` are filled with this collection's schema, collection id,
	// physical channel names, virtual channel names, created time, alias names, and so on.
	//
	// If timestamp is set a non-zero value and collection does not exist at this timestamp,
	// the `ErrorCode` of `Status` in `DescribeCollectionResponse` will be set to `Error`.
	// Timestamp is ignored if set to 0.
	// error is always nil
	DescribeCollection(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)

	// ShowCollections notifies RootCoord to list all collection names and other info in database at specified timestamp
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(not used), collection name and timestamp
	//
	// The `Status` in response struct `ShowCollectionsResponse` indicates if this operation is processed successfully or fail cause;
	// other fields in `ShowCollectionsResponse` are filled with all collection names, collection ids,
	// created times, created UTC times, and so on.
	// error is always nil
	ShowCollections(ctx context.Context, req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error)

	// CreatePartition notifies RootCoord to create a partition
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(not used), collection name and partition name
	//
	// The `ErrorCode` of `Status` is `Success` if create partition successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	CreatePartition(ctx context.Context, req *milvuspb.CreatePartitionRequest) (*commonpb.Status, error)

	// DropPartition notifies RootCoord to drop a partition
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(not used), collection name and partition name
	//
	// The `ErrorCode` of `Status` is `Success` if drop partition successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	//
	// Default partition cannot be dropped
	DropPartition(ctx context.Context, req *milvuspb.DropPartitionRequest) (*commonpb.Status, error)

	// HasPartition notifies RootCoord to check if a partition with specified name exists in the collection
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(not used), collection name and partition name
	//
	// The `Status` in response struct `BoolResponse` indicates if this operation is processed successfully or fail cause;
	// the `Value` in `BoolResponse` is `true` if the partition exists in the collection, `false` otherwise.
	// error is always nil
	HasPartition(ctx context.Context, req *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error)

	// ShowPartitions notifies RootCoord to list all partition names and other info in the collection
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(not used), collection name or collection id, and partition names
	//
	// The `Status` in response struct `ShowPartitionsResponse` indicates if this operation is processed successfully or fail cause;
	// other fields in `ShowPartitionsResponse` are filled with all partition names, partition ids,
	// created times, created UTC times, and so on.
	// error is always nil
	ShowPartitions(ctx context.Context, req *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error)

	//index builder service
	CreateIndex(ctx context.Context, req *milvuspb.CreateIndexRequest) (*commonpb.Status, error)
	DescribeIndex(ctx context.Context, req *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error)
	DropIndex(ctx context.Context, req *milvuspb.DropIndexRequest) (*commonpb.Status, error)

	// collection alias
	CreateAlias(ctx context.Context, req *milvuspb.CreateAliasRequest) (*commonpb.Status, error)
	DropAlias(ctx context.Context, req *milvuspb.DropAliasRequest) (*commonpb.Status, error)
	AlterAlias(ctx context.Context, req *milvuspb.AlterAliasRequest) (*commonpb.Status, error)

	//global timestamp allocator
	AllocTimestamp(ctx context.Context, req *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error)
	AllocID(ctx context.Context, req *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error)
	UpdateChannelTimeTick(ctx context.Context, req *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error)

	//segment
	DescribeSegment(ctx context.Context, req *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error)
	ShowSegments(ctx context.Context, req *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error)
	ReleaseDQLMessageStream(ctx context.Context, in *proxypb.ReleaseDQLMessageStreamRequest) (*commonpb.Status, error)
	SegmentFlushCompleted(ctx context.Context, in *datapb.SegmentFlushCompletedMsg) (*commonpb.Status, error)

	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

// RootCoordComponent is used by grpc server of RootCoord
type RootCoordComponent interface {
	RootCoord

	UpdateStateCode(internalpb.StateCode)
	SetDataCoord(context.Context, DataCoord) error
	SetIndexCoord(IndexCoord) error
	SetQueryCoord(QueryCoord) error
	SetNewProxyClient(func(sess *sessionutil.Session) (Proxy, error))

	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

type Proxy interface {
	Component

	// InvalidateCollectionMetaCache notifies Proxy to clear all the meta cache of specific collection.
	//
	// InvalidateCollectionMetaCache should be called when there are any meta changes in specific collection.
	// Such as `DropCollection`, `CreatePartition`, `DropPartition`, etc.
	//
	// ctx is the request to control request deadline and cancellation.
	// request contains the request params, which are database name(not used now) and collection name.
	//
	// InvalidateCollectionMetaCache should always succeed even though the specific collection doesn't exist in Proxy.
	// So the code of response `Status` should be always `Success`.
	//
	// error is returned only when some communication issue occurs.
	InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error)

	// ReleaseDQLMessageStream notifies Proxy to release and close the search message stream of specific collection.
	//
	// ReleaseDQLMessageStream should be called when the specific collection was released.
	//
	// ctx is the request to control request deadline and cancellation.
	// request contains the request params, which are database id(not used now) and collection id.
	//
	// ReleaseDQLMessageStream should always succeed even though the specific collection doesn't exist in Proxy.
	// So the code of response `Status` should be always `Success`.
	//
	// error is returned only when some communication issue occurs.
	ReleaseDQLMessageStream(ctx context.Context, in *proxypb.ReleaseDQLMessageStreamRequest) (*commonpb.Status, error)
}

type QueryNode interface {
	Component
	TimeTickProvider

	AddQueryChannel(ctx context.Context, req *querypb.AddQueryChannelRequest) (*commonpb.Status, error)
	RemoveQueryChannel(ctx context.Context, req *querypb.RemoveQueryChannelRequest) (*commonpb.Status, error)
	WatchDmChannels(ctx context.Context, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error)
	LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error)
	ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error)
	ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error)
	ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error)
	GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error)

	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

type QueryCoord interface {
	Component
	TimeTickProvider

	ShowCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error)
	LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error)
	ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error)
	ShowPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error)
	LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error)
	ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error)
	CreateQueryChannel(ctx context.Context, req *querypb.CreateQueryChannelRequest) (*querypb.CreateQueryChannelResponse, error)
	GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error)
	GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error)

	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}
