// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
)

// TimeTickProvider is the interface all services implement
type TimeTickProvider interface {
	GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error)
}

// Limiter defines the interface to perform request rate limiting.
// If Limit function return true, the request will be rejected.
// Otherwise, the request will pass. Limit also returns limit of limiter.
type Limiter interface {
	Check(collectionID int64, rt internalpb.RateType, n int) commonpb.ErrorCode
}

// Component is the interface all services implement
type Component interface {
	Init() error
	Start() error
	Stop() error
	GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error)
	GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error)
	Register() error
	//SetAddress(address string)
	//GetAddress() string
}

// DataNode is the interface `datanode` package implements
type DataNode interface {
	Component

	// Deprecated
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

	// ShowConfigurations gets specified configurations param of DataNode
	ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error)
	// GetMetrics gets the metrics about DataNode.
	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)

	// Compaction will add a compaction task according to the request plan
	Compaction(ctx context.Context, req *datapb.CompactionPlan) (*commonpb.Status, error)
	// GetCompactionState get states of all compation tasks
	GetCompactionState(ctx context.Context, req *datapb.CompactionStateRequest) (*datapb.CompactionStateResponse, error)
	// SyncSegments is called by DataCoord, to sync the segments meta when complete compaction
	SyncSegments(ctx context.Context, req *datapb.SyncSegmentsRequest) (*commonpb.Status, error)

	// Import data files(json, numpy, etc.) on MinIO/S3 storage, read and parse them into sealed segments
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including file path and options
	//
	// Return status indicates if this operation is processed successfully or fail cause;
	// error is always nil
	Import(ctx context.Context, req *datapb.ImportTaskRequest) (*commonpb.Status, error)

	// ResendSegmentStats resend un-flushed segment stats back upstream to DataCoord by resending DataNode time tick message.
	// It returns a list of segments to be sent.
	ResendSegmentStats(ctx context.Context, req *datapb.ResendSegmentStatsRequest) (*datapb.ResendSegmentStatsResponse, error)

	// AddImportSegment puts the given import segment to current DataNode's flow graph.
	AddImportSegment(ctx context.Context, req *datapb.AddImportSegmentRequest) (*datapb.AddImportSegmentResponse, error)
}

// DataNodeComponent is used by grpc server of DataNode
//
//go:generate mockery --name=DataNodeComponent --structname=MockDataNode --output=../mocks  --filename=mock_datanode.go --with-expecter
type DataNodeComponent interface {
	DataNode

	// UpdateStateCode updates state code for DataNode
	//  `stateCode` is current statement of this data node, indicating whether it's healthy.
	UpdateStateCode(stateCode commonpb.StateCode)

	// GetStateCode return state code of this data node
	GetStateCode() commonpb.StateCode

	SetAddress(address string)
	GetAddress() string

	// SetEtcdClient set etcd client for DataNode
	SetEtcdClient(etcdClient *clientv3.Client)

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
}

// DataCoord is the interface `datacoord` package implements
type DataCoord interface {
	Component
	TimeTickProvider

	// Flush notifies DataCoord to flush all current growing segments of specified Collection
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, which are database name(not used for now) and collection id
	//
	// response struct `FlushResponse` contains
	// 		1, related db id
	// 		2, related collection id
	// 		3, affected segment ids
	// 		4, already flush/flushing segment ids of related collection before this request
	// 		5, timeOfSeal, all data before timeOfSeal is guaranteed to be sealed or flushed
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
	// which contains the specified collection, partition id, the related VChannel Name and row count it needs
	//
	// response struct `AssignSegmentIDResponse` contains the assignment result for each request
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

	// GetPartitionStatistics requests partition statistics
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

	// GetRecoveryInfo request segment recovery info of collection/partition
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the collection/partition id to query
	//
	// response struct `GetRecoveryInfoResponse` contains the list of segments info and corresponding vchannel info
	// error is returned only when some communication issue occurs
	GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error)

	// GetRecoveryInfoV2 request segment recovery info of collection or batch partitions
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the collection/partitions id to query
	//
	// response struct `GetRecoveryInfoResponseV2` contains the list of segments info and corresponding vchannel info
	// error is returned only when some communication issue occurs
	GetRecoveryInfoV2(ctx context.Context, req *datapb.GetRecoveryInfoRequestV2) (*datapb.GetRecoveryInfoResponseV2, error)

	// SaveBinlogPaths updates segments binlogs(including insert binlogs, stats logs and delta logs)
	//  and related message stream positions
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the segment binlogs and checkpoint informations.
	//
	// response status contains the status/error code and failing reason if any
	// error is returned only when some communication issue occurs
	//
	// there is a constraint that the `SaveBinlogPaths` requests of same segment shall be passed in sequence
	// 	the root reason is each `SaveBinlogPaths` will overwrite the checkpoint position
	//  if the constraint is broken, the checkpoint position will not be monotonically increasing and the integrity will be compromised
	SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error)

	// GetSegmentsByStates returns segment list of requested collection/partition in given states
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the collection/partition id and states to query
	// when partition is lesser or equal to 0, all flushed segments of collection will be returned
	//
	// response struct `GetSegmentsByStatesResponse` contains segment id list
	// error is returned only when some communication issue occurs
	GetSegmentsByStates(ctx context.Context, req *datapb.GetSegmentsByStatesRequest) (*datapb.GetSegmentsByStatesResponse, error)

	// GetFlushedSegments returns flushed segment list of requested collection/partition
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the collection/partition id to query
	// when partition is lesser or equal to 0, all flushed segments of collection will be returned
	//
	// response struct `GetFlushedSegmentsResponse` contains flushed segment id list
	// error is returned only when some communication issue occurs
	GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error)

	// ShowConfigurations gets specified configurations para of DataCoord
	ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error)
	// GetMetrics gets the metrics about DataCoord.
	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
	// ManualCompaction triggers a compaction for a collection
	ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error)
	// GetCompactionState gets the state of a compaction
	GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error)
	// GetCompactionStateWithPlans get the state of requested plan id
	GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest) (*milvuspb.GetCompactionPlansResponse, error)

	// WatchChannels notifies DataCoord to watch vchannels of a collection
	WatchChannels(ctx context.Context, req *datapb.WatchChannelsRequest) (*datapb.WatchChannelsResponse, error)
	// GetFlushState gets the flush state of multiple segments
	GetFlushState(ctx context.Context, req *milvuspb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error)
	// GetFlushAllState checks if all DML messages before `FlushAllTs` have been flushed.
	GetFlushAllState(ctx context.Context, req *milvuspb.GetFlushAllStateRequest) (*milvuspb.GetFlushAllStateResponse, error)
	// SetSegmentState updates a segment's state explicitly.
	SetSegmentState(ctx context.Context, req *datapb.SetSegmentStateRequest) (*datapb.SetSegmentStateResponse, error)

	// DropVirtualChannel notifies DataCoord a virtual channel is dropped and
	// updates related segments binlogs(including insert binlogs, stats logs and delta logs)
	//  and related message stream positions
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the dropped virtual channel name and related segment information
	//
	// response status contains the status/error code and failing reason if any
	// error is returned only when some communication issue occurs
	DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest) (*datapb.DropVirtualChannelResponse, error)

	// Import data files(json, numpy, etc.) on MinIO/S3 storage, read and parse them into sealed segments
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including file path and options
	//
	// The `Status` in response struct `ImportResponse` indicates if this operation is processed successfully or fail cause;
	// the `tasks` in `ImportResponse` return an id list of tasks.
	// error is always nil
	Import(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error)

	// UpdateSegmentStatistics updates a segment's stats.
	UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest) (*commonpb.Status, error)
	// UpdateChannelCheckpoint updates channel checkpoint in dataCoord.
	UpdateChannelCheckpoint(ctx context.Context, req *datapb.UpdateChannelCheckpointRequest) (*commonpb.Status, error)
	// ReportDataNodeTtMsgs report DataNodeTtMsgs to dataCoord, called by datanode.
	ReportDataNodeTtMsgs(ctx context.Context, req *datapb.ReportDataNodeTtMsgsRequest) (*commonpb.Status, error)

	// SaveImportSegment saves the import segment binlog paths data and then looks for the right DataNode to add the
	// segment to that DataNode.
	SaveImportSegment(ctx context.Context, req *datapb.SaveImportSegmentRequest) (*commonpb.Status, error)

	// UnsetIsImportingState unsets the `isImport` state of the given segments so that they can get compacted normally.
	UnsetIsImportingState(ctx context.Context, req *datapb.UnsetIsImportingStateRequest) (*commonpb.Status, error)

	// MarkSegmentsDropped marks the given segments as `dropped` state.
	MarkSegmentsDropped(ctx context.Context, req *datapb.MarkSegmentsDroppedRequest) (*commonpb.Status, error)

	BroadcastAlteredCollection(ctx context.Context, req *datapb.AlterCollectionRequest) (*commonpb.Status, error)

	CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error)

	GcConfirm(ctx context.Context, request *datapb.GcConfirmRequest) (*datapb.GcConfirmResponse, error)

	// CreateIndex create an index on collection.
	// Index building is asynchronous, so when an index building request comes, an IndexID is assigned to the task and
	// will get all flushed segments from DataCoord and record tasks with these segments. The background process
	// indexBuilder will find this task and assign it to IndexNode for execution.
	CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error)

	// GetIndexState gets the index state of the index name in the request from Proxy.
	// Deprecated: use DescribeIndex instead
	GetIndexState(ctx context.Context, req *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error)

	// GetSegmentIndexState gets the index state of the segments in the request from RootCoord.
	GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error)

	// GetIndexInfos gets the index files of the IndexBuildIDs in the request from RootCoordinator.
	GetIndexInfos(ctx context.Context, req *indexpb.GetIndexInfoRequest) (*indexpb.GetIndexInfoResponse, error)

	// DescribeIndex describe the index info of the collection.
	DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error)

	// GetIndexStatistics get the statistics of the index.
	GetIndexStatistics(ctx context.Context, req *indexpb.GetIndexStatisticsRequest) (*indexpb.GetIndexStatisticsResponse, error)

	// GetIndexBuildProgress get the index building progress by num rows.
	// Deprecated: use DescribeIndex instead
	GetIndexBuildProgress(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest) (*indexpb.GetIndexBuildProgressResponse, error)

	// DropIndex deletes indexes based on IndexID. One IndexID corresponds to the index of an entire column. A column is
	// divided into many segments, and each segment corresponds to an IndexBuildID. IndexCoord uses IndexBuildID to record
	// index tasks. Therefore, when DropIndex is called, delete all tasks corresponding to IndexBuildID corresponding to IndexID.
	DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error)
}

// DataCoordComponent defines the interface of DataCoord component.
//
//go:generate mockery --name=DataCoordComponent --structname=MockDataCoord --output=../mocks  --filename=mock_datacoord.go --with-expecter
type DataCoordComponent interface {
	DataCoord

	SetAddress(address string)
	// SetEtcdClient set EtcdClient for DataCoord
	// `etcdClient` is a client of etcd
	SetEtcdClient(etcdClient *clientv3.Client)

	SetRootCoord(rootCoord RootCoord)

	// SetDataNodeCreator set DataNode client creator func for DataCoord
	SetDataNodeCreator(func(context.Context, string) (DataNode, error))

	//SetIndexNodeCreator set Index client creator func for DataCoord
	SetIndexNodeCreator(func(context.Context, string) (IndexNode, error))
}

// IndexNode is the interface `indexnode` package implements
type IndexNode interface {
	Component
	//TimeTickProvider

	// BuildIndex receives request from IndexCoordinator to build an index.
	// Index building is asynchronous, so when an index building request comes, IndexNode records the task and returns.
	//BuildIndex(ctx context.Context, req *datapb.BuildIndexRequest) (*commonpb.Status, error)
	//GetTaskSlots(ctx context.Context, req *datapb.GetTaskSlotsRequest) (*datapb.GetTaskSlotsResponse, error)
	//
	//// GetMetrics gets the metrics about IndexNode.
	//GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)

	// CreateJob receive index building job from indexcoord. Notes that index building is asynchronous, task is recorded
	// in indexnode and then request is finished.
	CreateJob(context.Context, *indexpb.CreateJobRequest) (*commonpb.Status, error)
	// QueryJobs returns states of index building jobs specified by BuildIDs. There are four states of index building task
	// Unissued, InProgress, Finished, Failed
	QueryJobs(context.Context, *indexpb.QueryJobsRequest) (*indexpb.QueryJobsResponse, error)
	// DropJobs cancel index building jobs specified by BuildIDs. Notes that dropping task may have finished.
	DropJobs(context.Context, *indexpb.DropJobsRequest) (*commonpb.Status, error)
	// GetJobStats returns metrics of indexnode, including available job queue info, available task slots and finished job infos.
	GetJobStats(context.Context, *indexpb.GetJobStatsRequest) (*indexpb.GetJobStatsResponse, error)

	ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error)
	// GetMetrics gets the metrics about IndexNode.
	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

// IndexNodeComponent is used by grpc server of IndexNode
type IndexNodeComponent interface {
	IndexNode

	SetAddress(address string)
	GetAddress() string
	// SetEtcdClient set etcd client for IndexNodeComponent
	SetEtcdClient(etcdClient *clientv3.Client)

	// UpdateStateCode updates state code for IndexNodeComponent
	//  `stateCode` is current statement of this QueryCoord, indicating whether it's healthy.
	UpdateStateCode(stateCode commonpb.StateCode)
}

// RootCoord is the interface `rootcoord` package implements
//
//go:generate mockery --name=RootCoord  --output=../mocks --filename=mock_rootcoord.go --with-expecter
type RootCoord interface {
	Component
	TimeTickProvider

	// CreateDatabase notifies RootCoord to create a database
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including a database name
	//
	// The `ErrorCode` of `Status` is `Success` if create database successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	CreateDatabase(ctx context.Context, req *milvuspb.CreateDatabaseRequest) (*commonpb.Status, error)

	// DropDatabase notifies RootCoord to drop a database
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including a database name
	//
	// The `ErrorCode` of `Status` is `Success` if drop database successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	DropDatabase(ctx context.Context, req *milvuspb.DropDatabaseRequest) (*commonpb.Status, error)

	// ListDatabases notifies RootCoord to list all database names at specified timestamp
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(not used), collection name and timestamp
	//
	// The `Status` in response struct `ListDatabasesResponse` indicates if this operation is processed successfully or fail cause;
	// other fields in `ListDatabasesResponse` are filled with all database names, error is always nil
	ListDatabases(ctx context.Context, req *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error)

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

	// DescribeCollectionInternal same to DescribeCollection, only used in internal RPC.
	// Besides, it'll also return unavailable collection, for example, creating, dropping.
	DescribeCollectionInternal(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)

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

	// AlterCollection notifies Proxy to create a collection
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name and collection properties
	//
	// The `ErrorCode` of `Status` is `Success` if create collection successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	AlterCollection(ctx context.Context, request *milvuspb.AlterCollectionRequest) (*commonpb.Status, error)

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

	// ShowPartitionsInternal same to ShowPartitions, but will return unavailable resources and only used in internal.
	ShowPartitionsInternal(ctx context.Context, req *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error)

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
	//CreateIndex(ctx context.Context, req *milvuspb.CreateIndexRequest) (*commonpb.Status, error)

	// DescribeIndex notifies RootCoord to get specified index information for specified field
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(not used), collection name, field name and index name
	//
	// The `Status` in response struct `DescribeIndexResponse` indicates if this operation is processed successfully or fail cause;
	// index information is filled in `IndexDescriptions`
	// error is always nil
	//DescribeIndex(ctx context.Context, req *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error)

	//GetIndexState(ctx context.Context, req *milvuspb.GetIndexStateRequest) (*milvuspb.GetIndexStateResponse, error)

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
	//DropIndex(ctx context.Context, req *milvuspb.DropIndexRequest) (*commonpb.Status, error)

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
	//DescribeSegment(ctx context.Context, req *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error)

	// ShowSegments notifies RootCoord to list all segment ids in the collection or partition
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including collection id and partition id
	//
	// The `Status` in response struct `ShowSegmentsResponse` indicates if this operation is processed successfully or fail cause;
	// `SegmentIDs` in `ShowSegmentsResponse` records all segment ids.
	// error is always nil
	ShowSegments(ctx context.Context, req *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error)

	// InvalidateCollectionMetaCache notifies RootCoord to clear the meta cache of specific collection in Proxies.
	// If `CollectionID` is specified in request, all the collection meta cache with the specified collectionID will be
	// invalidated, if only the `CollectionName` is specified in request, only the collection meta cache with the
	// specified collectionName will be invalidated.
	//
	// ctx is the request to control request deadline and cancellation.
	// request contains the request params, which are database id(not used) and collection id.
	//
	// The `ErrorCode` of `Status` is `Success` if drop index successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	//
	// RootCoord just forwards this request to Proxy client
	InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error)

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
	//SegmentFlushCompleted(ctx context.Context, in *datapb.SegmentFlushCompletedMsg) (*commonpb.Status, error)

	// ShowConfigurations gets specified configurations para of RootCoord
	ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error)
	// GetMetrics notifies RootCoord to collect metrics for specified component
	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)

	// Import data files(json, numpy, etc.) on MinIO/S3 storage, read and parse them into sealed segments
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including file path and options
	//
	// Return status indicates if this operation is processed successfully or fail cause;
	// error is always nil
	Import(ctx context.Context, req *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error)

	// GetImportState checks import task state from datanode
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including a task id
	//
	// The `Status` in response struct `GetImportStateResponse` indicates if this operation is processed successfully or fail cause;
	// the `state` in `GetImportStateResponse` return the state of the import task.
	// error is always nil
	GetImportState(ctx context.Context, req *milvuspb.GetImportStateRequest) (*milvuspb.GetImportStateResponse, error)

	// List id array of all import tasks
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params
	//
	// The `Status` in response struct `ListImportTasksResponse` indicates if this operation is processed successfully or fail cause;
	// the `Tasks` in `ListImportTasksResponse` return the id array of all import tasks.
	// error is always nil
	ListImportTasks(ctx context.Context, req *milvuspb.ListImportTasksRequest) (*milvuspb.ListImportTasksResponse, error)

	// ReportImport reports import task state to rootCoord
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the import results, including imported row count and an id list of generated segments
	//
	// response status contains the status/error code and failing reason if any error is returned
	// error is always nil
	ReportImport(ctx context.Context, req *rootcoordpb.ImportResult) (*commonpb.Status, error)

	// CreateCredential create new user and password
	CreateCredential(ctx context.Context, req *internalpb.CredentialInfo) (*commonpb.Status, error)
	// UpdateCredential update password for a user
	UpdateCredential(ctx context.Context, req *internalpb.CredentialInfo) (*commonpb.Status, error)
	// DeleteCredential delete a user
	DeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error)
	// ListCredUsers list all usernames
	ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error)
	// GetCredential get credential by username
	GetCredential(ctx context.Context, req *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error)

	CreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest) (*commonpb.Status, error)
	DropRole(ctx context.Context, req *milvuspb.DropRoleRequest) (*commonpb.Status, error)
	OperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest) (*commonpb.Status, error)
	SelectRole(ctx context.Context, req *milvuspb.SelectRoleRequest) (*milvuspb.SelectRoleResponse, error)
	SelectUser(ctx context.Context, req *milvuspb.SelectUserRequest) (*milvuspb.SelectUserResponse, error)
	OperatePrivilege(ctx context.Context, req *milvuspb.OperatePrivilegeRequest) (*commonpb.Status, error)
	SelectGrant(ctx context.Context, req *milvuspb.SelectGrantRequest) (*milvuspb.SelectGrantResponse, error)
	ListPolicy(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error)

	CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error)

	RenameCollection(ctx context.Context, req *milvuspb.RenameCollectionRequest) (*commonpb.Status, error)
}

// RootCoordComponent is used by grpc server of RootCoord
type RootCoordComponent interface {
	RootCoord

	SetAddress(address string)
	// SetEtcdClient set EtcdClient for RootCoord
	// `etcdClient` is a client of etcd
	SetEtcdClient(etcdClient *clientv3.Client)

	// UpdateStateCode updates state code for RootCoord
	// State includes: Initializing, Healthy and Abnormal
	UpdateStateCode(commonpb.StateCode)

	// SetDataCoord set DataCoord for RootCoord
	// `dataCoord` is a client of data coordinator.
	//
	// Always return nil.
	SetDataCoord(dataCoord DataCoord) error

	// SetQueryCoord set QueryCoord for RootCoord
	//  `queryCoord` is a client of query coordinator.
	//
	// Always return nil.
	SetQueryCoord(queryCoord QueryCoord) error

	// SetProxyCreator set Proxy client creator func for RootCoord
	SetProxyCreator(func(ctx context.Context, addr string) (Proxy, error))

	// GetMetrics notifies RootCoordComponent to collect metrics for specified component
	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

// Proxy is the interface `proxy` package implements
type Proxy interface {
	Component

	// InvalidateCollectionMetaCache notifies Proxy to clear the meta cache of specific collection.
	// If `CollectionID` is specified in request, all the collection meta cache with the specified collectionID will be
	// invalidated, if only the `CollectionName` is specified in request, only the collection meta cache with the
	// specified collectionName will be invalidated.
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

	// InvalidateCredentialCache notifies Proxy to clear all the credential cache of specified username.
	//
	// InvalidateCredentialCache should be called when there are credential changes for specified username.
	// Such as `CreateCredential`, `UpdateCredential`, `DeleteCredential`, etc.
	//
	// InvalidateCredentialCache should always succeed even though the specified username doesn't exist in Proxy.
	// So the code of response `Status` should be always `Success`.
	//
	// error is returned only when some communication issue occurs.
	InvalidateCredentialCache(ctx context.Context, request *proxypb.InvalidateCredCacheRequest) (*commonpb.Status, error)

	UpdateCredentialCache(ctx context.Context, request *proxypb.UpdateCredCacheRequest) (*commonpb.Status, error)

	// SetRates notifies Proxy to limit rates of requests.
	SetRates(ctx context.Context, req *proxypb.SetRatesRequest) (*commonpb.Status, error)

	// GetProxyMetrics gets the metrics of proxy, it's an internal interface which is different from GetMetrics interface,
	// because it only obtains the metrics of Proxy, not including the topological metrics of Query cluster and Data cluster.
	GetProxyMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
	RefreshPolicyInfoCache(ctx context.Context, req *proxypb.RefreshPolicyInfoCacheRequest) (*commonpb.Status, error)

	ListClientInfos(ctx context.Context, req *proxypb.ListClientInfosRequest) (*proxypb.ListClientInfosResponse, error)
}

// ProxyComponent defines the interface of proxy component.
//
//go:generate mockery --name=ProxyComponent --structname=MockProxy --output=../mocks  --filename=mock_proxy.go --with-expecter
type ProxyComponent interface {
	Proxy

	SetAddress(address string)
	GetAddress() string
	// SetEtcdClient set EtcdClient for Proxy
	// `etcdClient` is a client of etcd
	SetEtcdClient(etcdClient *clientv3.Client)

	//SetRootCoordClient set RootCoord for Proxy
	// `rootCoord` is a client of root coordinator.
	SetRootCoordClient(rootCoord RootCoord)

	// SetDataCoordClient set DataCoord for Proxy
	// `dataCoord` is a client of data coordinator.
	SetDataCoordClient(dataCoord DataCoord)

	// SetIndexCoordClient set IndexCoord for Proxy
	//  `indexCoord` is a client of index coordinator.
	//SetIndexCoordClient(indexCoord IndexCoord)

	// SetQueryCoordClient set QueryCoord for Proxy
	//  `queryCoord` is a client of query coordinator.
	SetQueryCoordClient(queryCoord QueryCoord)

	// SetQueryNodeCreator set QueryNode client creator func for Proxy
	SetQueryNodeCreator(func(ctx context.Context, addr string) (QueryNode, error))

	// GetRateLimiter returns the rateLimiter in Proxy
	GetRateLimiter() (Limiter, error)

	// UpdateStateCode updates state code for Proxy
	//  `stateCode` is current statement of this proxy node, indicating whether it's healthy.
	UpdateStateCode(stateCode commonpb.StateCode)

	// CreateDatabase notifies Proxy to create a database
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including a database name
	//
	// The `ErrorCode` of `Status` is `Success` if create database successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	CreateDatabase(ctx context.Context, req *milvuspb.CreateDatabaseRequest) (*commonpb.Status, error)

	// DropDatabase notifies Proxy to drop a database
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including a database name
	//
	// The `ErrorCode` of `Status` is `Success` if drop database successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	DropDatabase(ctx context.Context, req *milvuspb.DropDatabaseRequest) (*commonpb.Status, error)

	// ListDatabases notifies Proxy to list all database names at specified timestamp
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(not used), collection name and timestamp
	//
	// The `Status` in response struct `ListDatabasesResponse` indicates if this operation is processed successfully or fail cause;
	// other fields in `ListDatabasesResponse` are filled with all database names, error is always nil
	ListDatabases(ctx context.Context, req *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error)

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

	// AlterCollection notifies Proxy to create a collection
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name and collection properties
	//
	// The `ErrorCode` of `Status` is `Success` if create collection successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	AlterCollection(ctx context.Context, request *milvuspb.AlterCollectionRequest) (*commonpb.Status, error)

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

	// GetLoadingProgress get the collection or partitions loading progress
	GetLoadingProgress(ctx context.Context, request *milvuspb.GetLoadingProgressRequest) (*milvuspb.GetLoadingProgressResponse, error)
	// GetLoadState get the collection or partitions load state
	GetLoadState(ctx context.Context, request *milvuspb.GetLoadStateRequest) (*milvuspb.GetLoadStateResponse, error)

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

	// GetIndexStatistics notifies Proxy to return index's statistics
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, field name, index name
	//
	// The `Status` in response struct `GetIndexStatisticsResponse` indicates if this operation is processed successfully or fail cause;
	// the `IndexDescriptions` in `GetIndexStatisticsResponse` return index's statistics.
	// error is always nil
	GetIndexStatistics(ctx context.Context, request *milvuspb.GetIndexStatisticsRequest) (*milvuspb.GetIndexStatisticsResponse, error)

	// GetIndexBuildProgress notifies Proxy to return index build progress
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, field name, index name
	//
	// The `Status` in response struct `GetIndexBuildProgressResponse` indicates if this operation is processed successfully or fail cause;
	// the `IndexdRows` in `GetIndexBuildProgressResponse` return the num of indexed rows.
	// the `TotalRows` in `GetIndexBuildProgressResponse` return the total number of segment rows.
	// error is always nil
	// Deprecated: use DescribeIndex instead
	GetIndexBuildProgress(ctx context.Context, request *milvuspb.GetIndexBuildProgressRequest) (*milvuspb.GetIndexBuildProgressResponse, error)

	// GetIndexState notifies Proxy to return index state
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, field name, index name
	//
	// The `Status` in response struct `GetIndexStateResponse` indicates if this operation is processed successfully or fail cause;
	// the `State` in `GetIndexStateResponse` return the state of index: Unissued/InProgress/Finished/Failed.
	// error is always nil
	// Deprecated: use DescribeIndex instead
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

	// Upsert notifies Proxy to upsert rows
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including database name(reserved), collection name, partition name(optional), fields data
	//
	// The `Status` in response struct `MutationResult` indicates if this operation is processed successfully or fail cause;
	// the `IDs` in `MutationResult` return the id list of upserted rows.
	// the `SuccIndex` in `MutationResult` return the succeed number of upserted rows.
	// the `ErrIndex` in `MutationResult` return the failed number of upsert rows.
	// error is always nil
	Upsert(ctx context.Context, request *milvuspb.UpsertRequest) (*milvuspb.MutationResult, error)

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

	// FlushAll notifies Proxy to flush all collection's DML messages, including those in message stream.
	//
	// ctx is the context to control request deadline and cancellation
	//
	// The `Status` in response struct `FlushAllResponse` indicates if this operation is processed successfully or fail cause;
	// The `FlushAllTs` field in the `FlushAllResponse` response struct is used to check the flushAll state at the
	// `GetFlushAllState` interface. `GetFlushAllState` would check if all DML messages before `FlushAllTs` have been flushed.
	// error is always nil
	FlushAll(ctx context.Context, request *milvuspb.FlushAllRequest) (*milvuspb.FlushAllResponse, error)

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

	// LoadBalance would do a load balancing operation between query nodes.
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including source query node ids and sealed segment ids to balance
	//
	// The `ErrorCode` of `Status` is `Success` if load balance successfully;
	// otherwise, the `ErrorCode` of `Status` will be `Error`, and the `Reason` of `Status` will record the fail cause.
	// error is always nil
	LoadBalance(ctx context.Context, request *milvuspb.LoadBalanceRequest) (*commonpb.Status, error)

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
	GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error)
	ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error)
	GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest) (*milvuspb.GetCompactionPlansResponse, error)
	// GetFlushState gets the flush state of multiple segments
	GetFlushState(ctx context.Context, req *milvuspb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error)
	// GetFlushAllState checks if all DML messages before `FlushAllTs` have been flushed.
	GetFlushAllState(ctx context.Context, req *milvuspb.GetFlushAllStateRequest) (*milvuspb.GetFlushAllStateResponse, error)

	// Import data files(json, numpy, etc.) on MinIO/S3 storage, read and parse them into sealed segments
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including file path and options
	//
	// The `Status` in response struct `ImportResponse` indicates if this operation is processed successfully or fail cause;
	// the `tasks` in `ImportResponse` return an id list of tasks.
	// error is always nil
	Import(ctx context.Context, req *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error)

	// Check import task state from datanode
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params, including a task id
	//
	// The `Status` in response struct `GetImportStateResponse` indicates if this operation is processed successfully or fail cause;
	// the `state` in `GetImportStateResponse` return the state of the import task.
	// error is always nil
	GetImportState(ctx context.Context, req *milvuspb.GetImportStateRequest) (*milvuspb.GetImportStateResponse, error)

	// List id array of all import tasks
	//
	// ctx is the context to control request deadline and cancellation
	// req contains the request params
	//
	// The `Status` in response struct `ListImportTasksResponse` indicates if this operation is processed successfully or fail cause;
	// the `Tasks` in `ListImportTasksResponse` return the id array of all import tasks.
	// error is always nil
	ListImportTasks(ctx context.Context, req *milvuspb.ListImportTasksRequest) (*milvuspb.ListImportTasksResponse, error)

	GetReplicas(ctx context.Context, req *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error)

	// CreateCredential create new user and password
	CreateCredential(ctx context.Context, req *milvuspb.CreateCredentialRequest) (*commonpb.Status, error)
	// UpdateCredential update password for a user
	UpdateCredential(ctx context.Context, req *milvuspb.UpdateCredentialRequest) (*commonpb.Status, error)
	// DeleteCredential delete a user
	DeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error)
	// ListCredUsers list all usernames
	ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error)

	CreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest) (*commonpb.Status, error)
	DropRole(ctx context.Context, req *milvuspb.DropRoleRequest) (*commonpb.Status, error)
	OperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest) (*commonpb.Status, error)
	SelectRole(ctx context.Context, req *milvuspb.SelectRoleRequest) (*milvuspb.SelectRoleResponse, error)
	SelectUser(ctx context.Context, req *milvuspb.SelectUserRequest) (*milvuspb.SelectUserResponse, error)
	OperatePrivilege(ctx context.Context, req *milvuspb.OperatePrivilegeRequest) (*commonpb.Status, error)
	SelectGrant(ctx context.Context, req *milvuspb.SelectGrantRequest) (*milvuspb.SelectGrantResponse, error)

	CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error)

	// RenameCollection rename collection from  old name to new name
	RenameCollection(ctx context.Context, req *milvuspb.RenameCollectionRequest) (*commonpb.Status, error)

	CreateResourceGroup(ctx context.Context, req *milvuspb.CreateResourceGroupRequest) (*commonpb.Status, error)
	DropResourceGroup(ctx context.Context, req *milvuspb.DropResourceGroupRequest) (*commonpb.Status, error)
	TransferNode(ctx context.Context, req *milvuspb.TransferNodeRequest) (*commonpb.Status, error)
	TransferReplica(ctx context.Context, req *milvuspb.TransferReplicaRequest) (*commonpb.Status, error)
	ListResourceGroups(ctx context.Context, req *milvuspb.ListResourceGroupsRequest) (*milvuspb.ListResourceGroupsResponse, error)
	DescribeResourceGroup(ctx context.Context, req *milvuspb.DescribeResourceGroupRequest) (*milvuspb.DescribeResourceGroupResponse, error)

	Connect(ctx context.Context, req *milvuspb.ConnectRequest) (*milvuspb.ConnectResponse, error)
}

// QueryNode is the interface `querynode` package implements
type QueryNode interface {
	Component
	TimeTickProvider

	WatchDmChannels(ctx context.Context, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error)
	UnsubDmChannel(ctx context.Context, req *querypb.UnsubDmChannelRequest) (*commonpb.Status, error)
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
	LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error)
	ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error)
	ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error)
	GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error)

	GetStatistics(ctx context.Context, req *querypb.GetStatisticsRequest) (*internalpb.GetStatisticsResponse, error)
	Search(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error)
	SearchSegments(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error)
	Query(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error)
	QuerySegments(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error)
	SyncReplicaSegments(ctx context.Context, req *querypb.SyncReplicaSegmentsRequest) (*commonpb.Status, error)

	ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error)
	// GetMetrics gets the metrics about QueryNode.
	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
	GetDataDistribution(context.Context, *querypb.GetDataDistributionRequest) (*querypb.GetDataDistributionResponse, error)
	SyncDistribution(context.Context, *querypb.SyncDistributionRequest) (*commonpb.Status, error)
	Delete(context.Context, *querypb.DeleteRequest) (*commonpb.Status, error)
}

// QueryNodeComponent is used by grpc server of QueryNode
//
//go:generate mockery --name=QueryNodeComponent --structname=MockQueryNode --output=../mocks  --filename=mock_querynode.go --with-expecter
type QueryNodeComponent interface {
	QueryNode

	// UpdateStateCode updates state code for QueryNode
	//  `stateCode` is current statement of this query node, indicating whether it's healthy.
	UpdateStateCode(stateCode commonpb.StateCode)

	SetAddress(address string)
	GetAddress() string

	// SetEtcdClient set etcd client for QueryNode
	SetEtcdClient(etcdClient *clientv3.Client)
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
	GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error)
	GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error)
	SyncNewCreatedPartition(ctx context.Context, req *querypb.SyncNewCreatedPartitionRequest) (*commonpb.Status, error)
	LoadBalance(ctx context.Context, req *querypb.LoadBalanceRequest) (*commonpb.Status, error)

	ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error)
	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)

	GetReplicas(ctx context.Context, req *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error)
	GetShardLeaders(ctx context.Context, req *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error)

	CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error)

	CreateResourceGroup(ctx context.Context, req *milvuspb.CreateResourceGroupRequest) (*commonpb.Status, error)
	DropResourceGroup(ctx context.Context, req *milvuspb.DropResourceGroupRequest) (*commonpb.Status, error)
	TransferNode(ctx context.Context, req *milvuspb.TransferNodeRequest) (*commonpb.Status, error)
	TransferReplica(ctx context.Context, req *querypb.TransferReplicaRequest) (*commonpb.Status, error)
	ListResourceGroups(ctx context.Context, req *milvuspb.ListResourceGroupsRequest) (*milvuspb.ListResourceGroupsResponse, error)
	DescribeResourceGroup(ctx context.Context, req *querypb.DescribeResourceGroupRequest) (*querypb.DescribeResourceGroupResponse, error)
}

// QueryCoordComponent is used by grpc server of QueryCoord
//
//go:generate mockery --name=QueryCoordComponent --structname=MockQueryCoord --output=../mocks  --filename=mock_querycoord.go --with-expecter
type QueryCoordComponent interface {
	QueryCoord

	SetAddress(address string)

	// SetEtcdClient set etcd client for QueryCoord
	SetEtcdClient(etcdClient *clientv3.Client)

	// UpdateStateCode updates state code for QueryCoord
	//  `stateCode` is current statement of this QueryCoord, indicating whether it's healthy.
	UpdateStateCode(stateCode commonpb.StateCode)

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

	// SetQueryNodeCreator set QueryNode client creator func for QueryCoord
	SetQueryNodeCreator(func(ctx context.Context, addr string) (QueryNode, error))
}
