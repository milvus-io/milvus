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

// TimeTickProvider is the interface all services implement
type TimeTickProvider interface {
	GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error)
}

// Component is the interface all services implement
type Component interface {
	Init() error
	Start() error
	Stop() error
	GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error)
	GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error)
	Register() error
}

// DataNode is the interface `datanode` package implements
type DataNode interface {
	Component

	WatchDmChannels(ctx context.Context, req *datapb.WatchDmChannelsRequest) (*commonpb.Status, error)

	// FlushSegments notifies DataNode to flush the segments req provids. The flush tasks are async to this
	//  rpc, DataNode will flush the segments in the background.
	//
	// Return UnexpectedError code in status:
	//     If DataNode isn't in HEALTHY: states not HEALTHY or dynamic checks not HEALTHY
	//     If DataNode doesn't find the correspounding segmentID in its memeory replica
	// Return Success code in status and trigers background flush:
	//     Log an info log if a segment is under flushing
	FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest) (*commonpb.Status, error)

	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

// DataCoord is the interface `datacoord` package implements
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

	// AssignSegmentID applies allocations for specified Coolection/Partition and related Channel Name(Virtial Channel)
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the requester's info(id and role) and the list of Assignment Request,
	// which coontains the specified collection, partitaion id, the related VChannel Name and row count it needs
	//
	// response struct `AssignSegmentIDResponse` contains the the assignment result for each request
	// error is returned only when some communication issue occurs
	// if some error occurs in the process of `AssignSegmentID`, it will be recorded and returned in `Status` field of response
	//
	// `AssignSegmentID` will applies current configured allocation policies for each request
	// if the VChannel is newly used, `WatchDmlChannels` will be invoked to notify a `DataNode`(selected by policy) to watch it
	// if there is anything make the allocation impossible, the response will not contain the corresponding result
	AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error)

	// GetSegmentStates requests segment state information
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the list of segment id to query
	//
	// response struct `GetSegmentStatesResponse` contains the list of each state query result
	// 	when the segment is not found, the state entry will has the field `Status`  to identify failure
	// 	otherwise the Segment State and Start position information will be returned
	// error is returned only when some communication issue occurs
	GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error)

	// GetInsertBinlogPaths requests binlog paths for specified segment
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the segment id to query
	//
	// response struct `GetInsertBinlogPathsResponse` contains the fields list
	// 	and corresponding binlog path list
	// error is returned only when some communication issue occurs
	GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest) (*datapb.GetInsertBinlogPathsResponse, error)

	// GetSegmentInfoChannel DEPRECATED
	// legacy api to get SegmentInfo Channel name
	GetSegmentInfoChannel(ctx context.Context) (*milvuspb.StringResponse, error)
	GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest) (*datapb.GetCollectionStatisticsResponse, error)
	GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest) (*datapb.GetPartitionStatisticsResponse, error)
	GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error)
	GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error)
	SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error)
	GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error)

	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

// IndexNode is the interface `indexnode` package implements
type IndexNode interface {
	Component
	TimeTickProvider

	// CreateIndex receives request from IndexCoordinator to build an index.
	// Index building is asynchronous, so when an index building request comes, IndexNode records the task and returns.
	CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error)
	// GetMetrics gets the metrics about IndexNode.
	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

// IndexCoord is the interface `indexcoord` package implements
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

// RootCoord is the interface `rootcoord` package implements
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

	// CreateIndex notifies RootCoord to create an index for the specified field in the collection
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(not used), collection name, field name and index parameters
	//
	// The `ErrorCode` of `Status` is `Success` if create index successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	//
	// RootCoord forwards this request to IndexCoord to create index
	CreateIndex(ctx context.Context, req *milvuspb.CreateIndexRequest) (*commonpb.Status, error)

	// DescribeIndex notifies RootCoord to get specified index information for specified field
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(not used), collection name, field name and index name
	//
	// The `Status` in response struct `DescribeIndexResponse` indicates if this operation is processed successfully or fail cause;
	// index information is filled in `IndexDescriptions`
	// error is always nil
	DescribeIndex(ctx context.Context, req *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error)

	// DropIndex notifies RootCoord to drop the specified index for the specified field
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(not used), collection name, field name and index name
	//
	// The `ErrorCode` of `Status` is `Success` if drop index successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	//
	// RootCoord forwards this request to IndexCoord to drop index
	DropIndex(ctx context.Context, req *milvuspb.DropIndexRequest) (*commonpb.Status, error)

	// CreateAlias notifies RootCoord to create an alias for the collection
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including collection name and alias
	//
	// The `ErrorCode` of `Status` is `Success` if create alias successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	CreateAlias(ctx context.Context, req *milvuspb.CreateAliasRequest) (*commonpb.Status, error)

	// DropAlias notifies RootCoord to drop an alias for the collection
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including alias
	//
	// The `ErrorCode` of `Status` is `Success` if drop alias successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	DropAlias(ctx context.Context, req *milvuspb.DropAliasRequest) (*commonpb.Status, error)

	// AlterAlias notifies RootCoord to alter alias for the collection
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including collection name and new alias
	//
	// The `ErrorCode` of `Status` is `Success` if alter alias successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	AlterAlias(ctx context.Context, req *milvuspb.AlterAliasRequest) (*commonpb.Status, error)

	// AllocTimestamp notifies RootCoord to alloc timestamps
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the count of timestamps need to be allocated
	//
	// The `Status` in response struct `AllocTimestampResponse` indicates if this operation is processed successfully or fail cause;
	// `Timestamp` is the first available timestamp allocated
	// error is always nil
	AllocTimestamp(ctx context.Context, req *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error)

	// AllocID notifies RootCoord to alloc IDs
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the count of IDs need to be allocated
	//
	// The `Status` in response struct `AllocTimestampResponse` indicates if this operation is processed successfully or fail cause;
	// `ID` is the first available id allocated
	// error is always nil
	AllocID(ctx context.Context, req *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error)

	// UpdateChannelTimeTick notifies RootCoord to update each Proxy's safe timestamp
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including physical channel names, channels' safe timestamps and default timestamp
	//
	// The `ErrorCode` of `Status` is `Success` if update channel timetick successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	UpdateChannelTimeTick(ctx context.Context, req *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error)

	// DescribeSegment notifies RootCoord to get specified segment information in the collection
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including collection id and segment id
	//
	// The `Status` in response struct `DescribeSegmentResponse` indicates if this operation is processed successfully or fail cause;
	// segment index information is filled in `IndexID`, `BuildID` and `EnableIndex`.
	// error is always nil
	DescribeSegment(ctx context.Context, req *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error)

	// ShowSegments notifies RootCoord to list all segment ids in the collection or partition
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including collection id and partition id
	//
	// The `Status` in response struct `ShowSegmentsResponse` indicates if this operation is processed successfully or fail cause;
	// `SegmentIDs` in `ShowSegmentsResponse` records all segment ids.
	// error is always nil
	ShowSegments(ctx context.Context, req *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error)

	// ReleaseDQLMessageStream notifies RootCoord to release and close the search message stream of specific collection.
	//
	// ctx is the request to control request deadline and cancellation.
	// request contains the request params, which are database id(not used) and collection id.
	//
	// The `ErrorCode` of `Status` is `Success` if drop index successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	//
	// RootCoord just forwards this request to Proxy client
	ReleaseDQLMessageStream(ctx context.Context, in *proxypb.ReleaseDQLMessageStreamRequest) (*commonpb.Status, error)

	// SegmentFlushCompleted notifies RootCoord that specified segment has been flushed
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including SegmentInfo
	//
	// The `ErrorCode` of `Status` is `Success` if process successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	//
	// This interface is only used by DataCoord, when RootCoord receives this request, RootCoord will notify IndexCoord
	// to build index for this segment.
	SegmentFlushCompleted(ctx context.Context, in *datapb.SegmentFlushCompletedMsg) (*commonpb.Status, error)

	// GetMetrics notifies RootCoord to collect metrics for specified component
	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

// RootCoordComponent is used by grpc server of RootCoord
type RootCoordComponent interface {
	RootCoord

	// UpdateStateCode updates state code for RootCoord
	// State includes: Initializing, Healthy and Abnormal
	UpdateStateCode(internalpb.StateCode)

	// SetDataCoord set DataCoord for RootCoord
	SetDataCoord(context.Context, DataCoord) error

	// SetIndexCoord set IndexCoord for RootCoord
	SetIndexCoord(IndexCoord) error

	// SetQueryCoord set QueryCoord for RootCoord
	SetQueryCoord(QueryCoord) error

	// SetNewProxyClient set Proxy client creator func for RootCoord
	SetNewProxyClient(func(sess *sessionutil.Session) (Proxy, error))

	// GetMetrics notifies RootCoordComponent to collect metrics for specified component
	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

// Proxy is the interface `proxy` package implements
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

// QueryNode is the interface `querynode` package implements
type QueryNode interface {
	Component
	TimeTickProvider

	// AddQueryChannel notifies QueryNode to subscribe a query channel and be a producer of a query result channel.
	// `ctx` is the context to control request deadline and cancellation.
	// `req` contains the request params, which are collection id, query channel and query result channel.
	//
	// Return UnexpectedError code in status:
	//     If QueryNode isn't in HEALTHY: states not HEALTHY or dynamic checks not HEALTHY.
	// Return Success code in status:
	//     Subscribe a query channel and be a producer of a query result channel.
	AddQueryChannel(ctx context.Context, req *querypb.AddQueryChannelRequest) (*commonpb.Status, error)
	RemoveQueryChannel(ctx context.Context, req *querypb.RemoveQueryChannelRequest) (*commonpb.Status, error)
	WatchDmChannels(ctx context.Context, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error)
	// LoadSegments notifies QueryNode to load the sealed segments from storage. The load tasks are sync to this
	// rpc, QueryNode will return after all the sealed segments are loaded.
	//
	// Return UnexpectedError code in status:
	//     If QueryNode isn't in HEALTHY: states not HEALTHY or dynamic checks not HEALTHY.
	//     If any segment is loaded failed in QueryNode.
	// Return Success code in status:
	//     All the sealed segments are loaded.
	LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error)
	ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error)
	ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error)
	ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error)
	GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error)

	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

// QueryCoord is the interface `querycoord` package implements
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
