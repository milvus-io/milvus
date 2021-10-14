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
	"github.com/milvus-io/milvus/internal/util/typeutil"
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

// DataNodeComponent is used by grpc server of DataNode
type DataNodeComponent interface {
	DataNode

	// UpdateStateCode updates state code for DataNode
	//  `stateCode` is current statement of this data node, indicating whether it's healthy.
	UpdateStateCode(stateCode internalpb.StateCode)

	// GetStateCode return state code of this data node
	GetStateCode() internalpb.StateCode

	// SetRootCoord set RootCoord for DataNode
	// `rootCoord` is a client of root coordinator.
	//
	// Return a generic error in status:
	//     If the rootCoord is nil or the rootCoord has been set before.
	// Return nil in status:
	//     The rootCoord is not nil.
	SetRootCoord(rootCoord RootCoord) error

	// SetDataCoord set DataCoord for DataNode
	// `dataCoord` is a client of data coordinator.
	//
	// Return a generic error in status:
	//     If the dataCoord is nil or the dataCoord has been set before.
	// Return nil in status:
	//     The dataCoord is not nil.
	SetDataCoord(dataCoord DataCoord) error

	// SetNodeID set node id for DataNode
	SetNodeID(typeutil.UniqueID)
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

	// GetCollectionStatistics requests collection statistics
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the collection id to query
	//
	// response struct `GetCollectionStatisticsResponse` contains the key-value list fields returning related data
	// 	only row count for now
	// error is returned only when some communication issue occurs
	GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest) (*datapb.GetCollectionStatisticsResponse, error)

	// GetParititonStatistics requests partition statistics
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the collection and partition id to query
	//
	// response struct `GetPartitionStatisticsResponse` contains the key-value list fields returning related data
	// 	only row count for now
	// error is returned only when some communication issue occurs
	GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest) (*datapb.GetPartitionStatisticsResponse, error)

	// GetSegmentInfo requests segment info
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the list of segment ids to query
	//
	// response struct `GetSegmentInfoResponse` contains the list of segment info
	// error is returned only when some communication issue occurs
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
	// `dataCoord` is a client of data coordinator.
	// `ctx` is the context pass to DataCoord api.
	//
	// Always return nil.
	SetDataCoord(ctx context.Context, dataCoord DataCoord) error

	// SetIndexCoord set IndexCoord for RootCoord
	//  `indexCoord` is a client of index coordinator.
	//
	// Always return nil.
	SetIndexCoord(indexCoord IndexCoord) error

	// SetQueryCoord set QueryCoord for RootCoord
	//  `queryCoord` is a client of query coordinator.
	//
	// Always return nil.
	SetQueryCoord(queryCoord QueryCoord) error

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

type ProxyComponent interface {
	Proxy

	// SetRootCoord set RootCoord for Proxy
	// `rootCoord` is a client of root coordinator.
	SetRootCoordClient(rootCoord RootCoord)

	// SetDataCoord set DataCoord for Proxy
	// `dataCoord` is a client of data coordinator.
	SetDataCoordClient(dataCoord DataCoord)

	// SetIndexCoord set IndexCoord for Proxy
	//  `indexCoord` is a client of index coordinator.
	SetIndexCoordClient(indexCoord IndexCoord)

	// SetQueryCoord set QueryCoord for Proxy
	//  `queryCoord` is a client of query coordinator.
	SetQueryCoordClient(queryCoord QueryCoord)

	// UpdateStateCode updates state code for Proxy
	//  `stateCode` is current statement of this proxy node, indicating whether it's healthy.
	UpdateStateCode(stateCode internalpb.StateCode)

	// CreateCollection notifies Proxy to create a collection
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, collection schema
	//
	// The `ErrorCode` of `Status` is `Success` if create collection successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	CreateCollection(ctx context.Context, request *milvuspb.CreateCollectionRequest) (*commonpb.Status, error)

	// DropCollection notifies Proxy to drop a collection
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved) and collection name
	//
	// The `ErrorCode` of `Status` is `Success` if drop collection successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	DropCollection(ctx context.Context, request *milvuspb.DropCollectionRequest) (*commonpb.Status, error)

	// HasCollection notifies Proxy to check a collection's existence at specified timestamp
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name and timestamp
	//
	// The `Status` in response struct `BoolResponse` indicates if this operation is processed successfully or fail cause;
	// the `Value` in `BoolResponse` is `true` if the collection exists at the specified timestamp, `false` otherwise.
	// Timestamp is ignored if set to 0.
	// error is always nil
	HasCollection(ctx context.Context, request *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error)

	// LoadCollection notifies Proxy to load a collection's data
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name
	//
	// The `ErrorCode` of `Status` is `Success` if load collection successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error
	// error is always nil
	LoadCollection(ctx context.Context, request *milvuspb.LoadCollectionRequest) (*commonpb.Status, error)

	// ReleaseCollection notifies Proxy to release a collection's data
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name
	//
	// The `ErrorCode` of `Status` is `Success` if release collection successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error
	// error is always nil
	ReleaseCollection(ctx context.Context, request *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error)

	// DescribeCollection notifies Proxy to return a collection's description
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name or collection id
	//
	// The `Status` in response struct `DescribeCollectionResponse` indicates if this operation is processed successfully or fail cause;
	// the `Schema` in `DescribeCollectionResponse` return collection's schema.
	// error is always nil
	DescribeCollection(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)

	// GetCollectionStatistics notifies Proxy to return a collection's statistics
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name
	//
	// The `Status` in response struct `GetCollectionStatisticsResponse` indicates if this operation is processed successfully or fail cause;
	// the `Stats` in `GetCollectionStatisticsResponse` return collection's statistics in key-value format.
	// error is always nil
	GetCollectionStatistics(ctx context.Context, request *milvuspb.GetCollectionStatisticsRequest) (*milvuspb.GetCollectionStatisticsResponse, error)

	// ShowCollections notifies Proxy to return collections list in current db at specified timestamp
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), timestamp
	//
	// The `Status` in response struct `ShowCollectionsResponse` indicates if this operation is processed successfully or fail cause;
	// the `CollectionNames` in `ShowCollectionsResponse` return collection names list.
	// the `CollectionIds` in `ShowCollectionsResponse` return collection ids list.
	// error is always nil
	ShowCollections(ctx context.Context, request *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error)

	// CreatePartition notifies Proxy to create a partition
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, partition name
	//
	// The `ErrorCode` of `Status` is `Success` if create partition successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	CreatePartition(ctx context.Context, request *milvuspb.CreatePartitionRequest) (*commonpb.Status, error)

	// DropPartition notifies Proxy to drop a partition
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, partition name
	//
	// The `ErrorCode` of `Status` is `Success` if drop partition successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	DropPartition(ctx context.Context, request *milvuspb.DropPartitionRequest) (*commonpb.Status, error)

	// HasPartition notifies Proxy to check a partition's existence
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, partition name
	//
	// The `ErrorCode` of `Status` is `Success` if check partition's existence successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	HasPartition(ctx context.Context, request *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error)

	// LoadPartitions notifies Proxy to load partition's data
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, partition names
	//
	// The `ErrorCode` of `Status` is `Success` if load partitions successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error
	// error is always nil
	LoadPartitions(ctx context.Context, request *milvuspb.LoadPartitionsRequest) (*commonpb.Status, error)

	// ReleasePartitions notifies Proxy to release collection's data
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, partition names
	//
	// The `ErrorCode` of `Status` is `Success` if release collection successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error
	// error is always nil
	ReleasePartitions(ctx context.Context, request *milvuspb.ReleasePartitionsRequest) (*commonpb.Status, error)

	// GetPartitionStatistics notifies Proxy to return a partiiton's statistics
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, partition name
	//
	// The `Status` in response struct `GetPartitionStatisticsResponse` indicates if this operation is processed successfully or fail cause;
	// the `Stats` in `GetPartitionStatisticsResponse` return collection's statistics in key-value format.
	// error is always nil
	GetPartitionStatistics(ctx context.Context, request *milvuspb.GetPartitionStatisticsRequest) (*milvuspb.GetPartitionStatisticsResponse, error)

	// ShowPartitions notifies Proxy to return collections list in current db at specified timestamp
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, partition names(optional)
	// When partition names is specified, will return these patitions's inMemory_percentages.
	//
	// The `Status` in response struct `ShowPartitionsResponse` indicates if this operation is processed successfully or fail cause;
	// the `PartitionNames` in `ShowPartitionsResponse` return partition names list.
	// the `PartitionIds` in `ShowPartitionsResponse` return partition ids list.
	// the `InMemoryPercentages` in `ShowPartitionsResponse` return partitions's inMemory_percentages if the partition names of req is specified.
	// error is always nil
	ShowPartitions(ctx context.Context, request *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error)

	// CreateIndex notifies Proxy to create index of a field
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, field name, index parameters
	//
	// The `ErrorCode` of `Status` is `Success` if create index successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	CreateIndex(ctx context.Context, request *milvuspb.CreateIndexRequest) (*commonpb.Status, error)

	// DropIndex notifies Proxy to drop an index
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, field name, index name
	//
	// The `ErrorCode` of `Status` is `Success` if drop index successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	DropIndex(ctx context.Context, request *milvuspb.DropIndexRequest) (*commonpb.Status, error)

	// DescribeIndex notifies Proxy to return index's description
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, field name, index name
	//
	// The `Status` in response struct `DescribeIndexResponse` indicates if this operation is processed successfully or fail cause;
	// the `IndexDescriptions` in `DescribeIndexResponse` return index's description.
	// error is always nil
	DescribeIndex(ctx context.Context, request *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error)

	// GetIndexBuildProgress notifies Proxy to return index build progress
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, field name, index name
	//
	// The `Status` in response struct `GetIndexBuildProgressResponse` indicates if this operation is processed successfully or fail cause;
	// the `IndexdRows` in `GetIndexBuildProgressResponse` return the num of indexed rows.
	// the `TotalRows` in `GetIndexBuildProgressResponse` return the total number of segment rows.
	// error is always nil
	GetIndexBuildProgress(ctx context.Context, request *milvuspb.GetIndexBuildProgressRequest) (*milvuspb.GetIndexBuildProgressResponse, error)

	// GetIndexState notifies Proxy to return index state
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, field name, index name
	//
	// The `Status` in response struct `GetIndexStateResponse` indicates if this operation is processed successfully or fail cause;
	// the `State` in `GetIndexStateResponse` return the state of index: Unissued/InProgress/Finished/Failed.
	// error is always nil
	GetIndexState(ctx context.Context, request *milvuspb.GetIndexStateRequest) (*milvuspb.GetIndexStateResponse, error)

	// Insert notifies Proxy to insert rows
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, partition name(optional), fields data
	//
	// The `Status` in response struct `MutationResult` indicates if this operation is processed successfully or fail cause;
	// the `IDs` in `MutationResult` return the id list of inserted rows.
	// the `SuccIndex` in `MutationResult` return the succeed number of inserted rows.
	// the `ErrIndex` in `MutationResult` return the failed number of insert rows.
	// error is always nil
	Insert(ctx context.Context, request *milvuspb.InsertRequest) (*milvuspb.MutationResult, error)

	// Delete notifies Proxy to delete rows
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, partition name(optional), filter expression
	//
	// The `Status` in response struct `MutationResult` indicates if this operation is processed successfully or fail cause;
	// the `IDs` in `MutationResult` return the id list of deleted rows.
	// the `SuccIndex` in `MutationResult` return the succeed number of deleted rows.
	// the `ErrIndex` in `MutationResult` return the failed number of delete rows.
	// error is always nil
	Delete(ctx context.Context, request *milvuspb.DeleteRequest) (*milvuspb.MutationResult, error)

	// Search notifies Proxy to do search
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, partition name(optional), filter expression
	//
	// The `Status` in response struct `SearchResults` indicates if this operation is processed successfully or fail cause;
	// the `Results` in `SearchResults` return search results.
	// error is always nil
	Search(ctx context.Context, request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error)

	// Flush notifies Proxy to flush buffer into storage
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name
	//
	// The `Status` in response struct `FlushResponse` indicates if this operation is processed successfully or fail cause;
	// error is always nil
	Flush(ctx context.Context, request *milvuspb.FlushRequest) (*milvuspb.FlushResponse, error)

	// Query notifies Proxy to query rows
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, partition names(optional), filter expression, output fields
	//
	// The `Status` in response struct `QueryResults` indicates if this operation is processed successfully or fail cause;
	// the `FieldsData` in `QueryResults` return query results.
	// error is always nil
	Query(ctx context.Context, request *milvuspb.QueryRequest) (*milvuspb.QueryResults, error)

	// CalcDistance notifies Proxy to calculate distance between specified vectors
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), vectors to calculate
	//
	// The `Status` in response struct `CalcDistanceResults` indicates if this operation is processed successfully or fail cause;
	// The `Array` in response struct `CalcDistanceResults` return distance result
	// Return generic error when specified vectors not found or float/binary vectors mismatch, otherwise return nil
	CalcDistance(ctx context.Context, request *milvuspb.CalcDistanceRequest) (*milvuspb.CalcDistanceResults, error)

	// Not yet implemented
	GetDdChannel(ctx context.Context, request *internalpb.GetDdChannelRequest) (*milvuspb.StringResponse, error)

	// GetPersistentSegmentInfo notifies Proxy to return sealed segments's information of a collection from data coord
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name
	//
	// The `Status` in response struct `GetPersistentSegmentInfoResponse` indicates if this operation is processed successfully or fail cause;
	// the `Infos` in `GetPersistentSegmentInfoResponse` return sealed segments's information of a collection.
	// error is always nil
	GetPersistentSegmentInfo(ctx context.Context, request *milvuspb.GetPersistentSegmentInfoRequest) (*milvuspb.GetPersistentSegmentInfoResponse, error)

	// GetQuerySegmentInfo notifies Proxy to return growing segments's information of a collection from query coord
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name
	//
	// The `Status` in response struct `GetQuerySegmentInfoResponse` indicates if this operation is processed successfully or fail cause;
	// the `Infos` in `GetQuerySegmentInfoResponse` return growing segments's information of a collection.
	// error is always nil
	GetQuerySegmentInfo(ctx context.Context, request *milvuspb.GetQuerySegmentInfoRequest) (*milvuspb.GetQuerySegmentInfoResponse, error)

	// For internal usage
	Dummy(ctx context.Context, request *milvuspb.DummyRequest) (*milvuspb.DummyResponse, error)

	// RegisterLink notifies Proxy to its state code
	//
	// ctx is the context to control request deadline and cancellation
	//
	// The `Status` in response struct `RegisterLinkResponse` indicates if this proxy is healthy or not
	// error is always nil
	RegisterLink(ctx context.Context, request *milvuspb.RegisterLinkRequest) (*milvuspb.RegisterLinkResponse, error)

	// GetMetrics gets the metrics of the proxy.
	GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)

	// CreateAlias notifies Proxy to create alias for a collection
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, alias
	//
	// The `ErrorCode` of `Status` is `Success` if create alias successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	CreateAlias(ctx context.Context, request *milvuspb.CreateAliasRequest) (*commonpb.Status, error)

	// DropAlias notifies Proxy to drop an alias
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, alias
	//
	// The `ErrorCode` of `Status` is `Success` if drop alias successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	DropAlias(ctx context.Context, request *milvuspb.DropAliasRequest) (*commonpb.Status, error)

	// AlterAlias notifies Proxy to alter an alias from a colection to another
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, alias
	//
	// The `ErrorCode` of `Status` is `Success` if alter alias successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	AlterAlias(ctx context.Context, request *milvuspb.AlterAliasRequest) (*commonpb.Status, error)
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

// QueryNodeComponent is used by grpc server of QueryNode
type QueryNodeComponent interface {
	QueryNode

	// UpdateStateCode updates state code for QueryNode
	//  `stateCode` is current statement of this query node, indicating whether it's healthy.
	UpdateStateCode(stateCode internalpb.StateCode)

	// SetRootCoord set RootCoord for QueryNode
	// `rootCoord` is a client of root coordinator. Pass to segmentLoader.
	//
	// Return a generic error in status:
	//     If the rootCoord is nil.
	// Return nil in status:
	//     The rootCoord is not nil.
	SetRootCoord(rootCoord RootCoord) error

	// SetIndexCoord set IndexCoord for QueryNode
	//  `indexCoord` is a client of index coordinator. Pass to segmentLoader.
	//
	// Return a generic error in status:
	//     If the indexCoord is nil.
	// Return nil in status:
	//     The indexCoord is not nil.
	SetIndexCoord(indexCoord IndexCoord) error
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

// QueryCoordComponent is used by grpc server of QueryCoord
type QueryCoordComponent interface {
	QueryCoord

	// UpdateStateCode updates state code for QueryCoord
	//  `stateCode` is current statement of this query coord, indicating whether it's healthy.
	UpdateStateCode(stateCode internalpb.StateCode)

	// SetDataCoord set DataCoord for QueryCoord
	// `dataCoord` is a client of data coordinator.
	//
	// Return a generic error in status:
	//     If the dataCoord is nil.
	// Return nil in status:
	//     The dataCoord is not nil.
	SetDataCoord(dataCoord DataCoord) error

	// SetRootCoord set RootCoord for QueryCoord
	// `rootCoord` is a client of root coordinator.
	//
	// Return a generic error in status:
	//     If the rootCoord is nil.
	// Return nil in status:
	//     The rootCoord is not nil.
	SetRootCoord(rootCoord RootCoord) error
}
