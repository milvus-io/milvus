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

package grpcdatacoordclient

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var Params *paramtable.ComponentParam = paramtable.Get()

var _ types.DataCoord = (*Client)(nil)

// Client is the datacoord grpc client
type Client struct {
	grpcClient grpcclient.GrpcClient[datapb.DataCoordClient]
	sess       *sessionutil.Session
	sourceID   int64
}

// NewClient creates a new client instance
func NewClient(ctx context.Context, metaRoot string, etcdCli *clientv3.Client) (*Client, error) {
	sess := sessionutil.NewSession(ctx, metaRoot, etcdCli)
	if sess == nil {
		err := fmt.Errorf("new session error, maybe can not connect to etcd")
		log.Debug("DataCoordClient NewClient failed", zap.Error(err))
		return nil, err
	}

	clientParams := &Params.DataCoordGrpcClientCfg
	client := &Client{
		grpcClient: &grpcclient.ClientBase[datapb.DataCoordClient]{
			ClientMaxRecvSize:      clientParams.ClientMaxRecvSize.GetAsInt(),
			ClientMaxSendSize:      clientParams.ClientMaxSendSize.GetAsInt(),
			DialTimeout:            clientParams.DialTimeout.GetAsDuration(time.Millisecond),
			KeepAliveTime:          clientParams.KeepAliveTime.GetAsDuration(time.Millisecond),
			KeepAliveTimeout:       clientParams.KeepAliveTimeout.GetAsDuration(time.Millisecond),
			RetryServiceNameConfig: "milvus.proto.data.DataCoord",
			MaxAttempts:            clientParams.MaxAttempts.GetAsInt(),
			InitialBackoff:         float32(clientParams.InitialBackoff.GetAsFloat()),
			MaxBackoff:             float32(clientParams.MaxBackoff.GetAsFloat()),
			BackoffMultiplier:      float32(clientParams.BackoffMultiplier.GetAsFloat()),
			CompressionEnabled:     clientParams.CompressionEnabled.GetAsBool(),
		},
		sess: sess,
	}
	client.grpcClient.SetRole(typeutil.DataCoordRole)
	client.grpcClient.SetGetAddrFunc(client.getDataCoordAddr)
	client.grpcClient.SetNewGrpcClientFunc(client.newGrpcClient)

	return client, nil
}

func (c *Client) newGrpcClient(cc *grpc.ClientConn) datapb.DataCoordClient {
	return datapb.NewDataCoordClient(cc)
}

func (c *Client) getDataCoordAddr() (string, error) {
	key := c.grpcClient.GetRole()
	msess, _, err := c.sess.GetSessions(key)
	if err != nil {
		log.Debug("DataCoordClient, getSessions failed", zap.Any("key", key), zap.Error(err))
		return "", err
	}
	ms, ok := msess[key]
	if !ok {
		log.Debug("DataCoordClient, not existed in msess ", zap.Any("key", key), zap.Any("len of msess", len(msess)))
		return "", fmt.Errorf("find no available datacoord, check datacoord state")
	}

	c.grpcClient.SetNodeID(ms.ServerID)
	return ms.Address, nil
}

// Init initializes the client
func (c *Client) Init() error {
	return nil
}

// Start enables the client
func (c *Client) Start() error {
	return nil
}

// Stop stops the client
func (c *Client) Stop() error {
	return c.grpcClient.Close()
}

// Register dummy
func (c *Client) Register() error {
	return nil
}

func wrapGrpcCall[T any](ctx context.Context, c *Client, call func(coordClient datapb.DataCoordClient) (*T, error)) (*T, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client datapb.DataCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return call(client)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*T), err
}

// GetComponentStates calls DataCoord GetComponentStates services
func (c *Client) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*milvuspb.ComponentStates, error) {
		return client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	})
}

// GetTimeTickChannel return the name of time tick channel.
func (c *Client) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*milvuspb.StringResponse, error) {
		return client.GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
	})
}

// GetStatisticsChannel return the name of statistics channel.
func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*milvuspb.StringResponse, error) {
		return client.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
}

// Flush flushes a collection's data
func (c *Client) Flush(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.FlushResponse, error) {
		return client.Flush(ctx, req)
	})
}

// AssignSegmentID applies allocations for specified Coolection/Partition and related Channel Name(Virtial Channel)
//
// ctx is the context to control request deadline and cancellation
// req contains the requester's info(id and role) and the list of Assignment Request,
// which contains the specified collection, partitaion id, the related VChannel Name and row count it needs
//
// response struct `AssignSegmentIDResponse` contains the assignment result for each request
// error is returned only when some communication issue occurs
// if some error occurs in the process of `AssignSegmentID`, it will be recorded and returned in `Status` field of response
//
// `AssignSegmentID` will applies current configured allocation policies for each request
// if the VChannel is newly used, `WatchDmlChannels` will be invoked to notify a `DataNode`(selected by policy) to watch it
// if there is anything make the allocation impossible, the response will not contain the corresponding result
func (c *Client) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.AssignSegmentIDResponse, error) {
		return client.AssignSegmentID(ctx, req)
	})
}

// GetSegmentStates requests segment state information
//
// ctx is the context to control request deadline and cancellation
// req contains the list of segment id to query
//
// response struct `GetSegmentStatesResponse` contains the list of each state query result
//
//	when the segment is not found, the state entry will has the field `Status`  to identify failure
//	otherwise the Segment State and Start position information will be returned
//
// error is returned only when some communication issue occurs
func (c *Client) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.GetSegmentStatesResponse, error) {
		return client.GetSegmentStates(ctx, req)
	})
}

// GetInsertBinlogPaths requests binlog paths for specified segment
//
// ctx is the context to control request deadline and cancellation
// req contains the segment id to query
//
// response struct `GetInsertBinlogPathsResponse` contains the fields list
//
//	and corresponding binlog path list
//
// error is returned only when some communication issue occurs
func (c *Client) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest) (*datapb.GetInsertBinlogPathsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.GetInsertBinlogPathsResponse, error) {
		return client.GetInsertBinlogPaths(ctx, req)
	})
}

// GetCollectionStatistics requests collection statistics
//
// ctx is the context to control request deadline and cancellation
// req contains the collection id to query
//
// response struct `GetCollectionStatisticsResponse` contains the key-value list fields returning related data
//
//	only row count for now
//
// error is returned only when some communication issue occurs
func (c *Client) GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest) (*datapb.GetCollectionStatisticsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.GetCollectionStatisticsResponse, error) {
		return client.GetCollectionStatistics(ctx, req)
	})
}

// GetPartitionStatistics requests partition statistics
//
// ctx is the context to control request deadline and cancellation
// req contains the collection and partition id to query
//
// response struct `GetPartitionStatisticsResponse` contains the key-value list fields returning related data
//
//	only row count for now
//
// error is returned only when some communication issue occurs
func (c *Client) GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest) (*datapb.GetPartitionStatisticsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.GetPartitionStatisticsResponse, error) {
		return client.GetPartitionStatistics(ctx, req)
	})
}

// GetSegmentInfoChannel DEPRECATED
// legacy api to get SegmentInfo Channel name
func (c *Client) GetSegmentInfoChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*milvuspb.StringResponse, error) {
		return client.GetSegmentInfoChannel(ctx, &datapb.GetSegmentInfoChannelRequest{})
	})
}

// GetSegmentInfo requests segment info
//
// ctx is the context to control request deadline and cancellation
// req contains the list of segment ids to query
//
// response struct `GetSegmentInfoResponse` contains the list of segment info
// error is returned only when some communication issue occurs
func (c *Client) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.GetSegmentInfoResponse, error) {
		return client.GetSegmentInfo(ctx, req)
	})
}

// SaveBinlogPaths updates segments binlogs(including insert binlogs, stats logs and delta logs)
//
//	and related message stream positions
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partition id to query
//
// response status contains the status/error code and failing reason if any
// error is returned only when some communication issue occurs
//
// there is a constraint that the `SaveBinlogPaths` requests of same segment shall be passed in sequence
//
//		the root reason is each `SaveBinlogPaths` will overwrite the checkpoint position
//	 if the constraint is broken, the checkpoint position will not be monotonically increasing and the integrity will be compromised
func (c *Client) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error) {
	// use Call here on purpose
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*commonpb.Status, error) {
		return client.SaveBinlogPaths(ctx, req)
	})
}

// GetRecoveryInfo request segment recovery info of collection/partition
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partition id to query
//
// response struct `GetRecoveryInfoResponse` contains the list of segments info and corresponding vchannel info
// error is returned only when some communication issue occurs
func (c *Client) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.GetRecoveryInfoResponse, error) {
		return client.GetRecoveryInfo(ctx, req)
	})
}

// GetRecoveryInfoV2 request segment recovery info of collection/partitions
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partitions id to query
//
// response struct `GetRecoveryInfoResponseV2` contains the list of segments info and corresponding vchannel info
// error is returned only when some communication issue occurs
func (c *Client) GetRecoveryInfoV2(ctx context.Context, req *datapb.GetRecoveryInfoRequestV2) (*datapb.GetRecoveryInfoResponseV2, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.GetRecoveryInfoResponseV2, error) {
		return client.GetRecoveryInfoV2(ctx, req)
	})
}

// GetFlushedSegments returns flushed segment list of requested collection/parition
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partition id to query
//
//	when partition is lesser or equal to 0, all flushed segments of collection will be returned
//
// response struct `GetFlushedSegmentsResponse` contains flushed segment id list
// error is returned only when some communication issue occurs
func (c *Client) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.GetFlushedSegmentsResponse, error) {
		return client.GetFlushedSegments(ctx, req)
	})
}

// GetSegmentsByStates returns segment list of requested collection/partition and segment states
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partition id and segment states to query
// when partition is lesser or equal to 0, all segments of collection will be returned
//
// response struct `GetSegmentsByStatesResponse` contains segment id list
// error is returned only when some communication issue occurs
func (c *Client) GetSegmentsByStates(ctx context.Context, req *datapb.GetSegmentsByStatesRequest) (*datapb.GetSegmentsByStatesResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.GetSegmentsByStatesResponse, error) {
		return client.GetSegmentsByStates(ctx, req)
	})
}

// ShowConfigurations gets specified configurations para of DataCoord
func (c *Client) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*internalpb.ShowConfigurationsResponse, error) {
		return client.ShowConfigurations(ctx, req)
	})
}

// GetMetrics gets all metrics of datacoord
func (c *Client) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*milvuspb.GetMetricsResponse, error) {
		return client.GetMetrics(ctx, req)
	})
}

// ManualCompaction triggers a compaction for a collection
func (c *Client) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*milvuspb.ManualCompactionResponse, error) {
		return client.ManualCompaction(ctx, req)
	})
}

// GetCompactionState gets the state of a compaction
func (c *Client) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*milvuspb.GetCompactionStateResponse, error) {
		return client.GetCompactionState(ctx, req)
	})
}

// GetCompactionStateWithPlans gets the state of a compaction by plan
func (c *Client) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest) (*milvuspb.GetCompactionPlansResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*milvuspb.GetCompactionPlansResponse, error) {
		return client.GetCompactionStateWithPlans(ctx, req)
	})
}

// WatchChannels notifies DataCoord to watch vchannels of a collection
func (c *Client) WatchChannels(ctx context.Context, req *datapb.WatchChannelsRequest) (*datapb.WatchChannelsResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.WatchChannelsResponse, error) {
		return client.WatchChannels(ctx, req)
	})
}

// GetFlushState gets the flush state of multiple segments
func (c *Client) GetFlushState(ctx context.Context, req *milvuspb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*milvuspb.GetFlushStateResponse, error) {
		return client.GetFlushState(ctx, req)
	})
}

// GetFlushAllState checks if all DML messages before `FlushAllTs` have been flushed.
func (c *Client) GetFlushAllState(ctx context.Context, req *milvuspb.GetFlushAllStateRequest) (*milvuspb.GetFlushAllStateResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*milvuspb.GetFlushAllStateResponse, error) {
		return client.GetFlushAllState(ctx, req)
	})
}

// DropVirtualChannel drops virtual channel in datacoord.
func (c *Client) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest) (*datapb.DropVirtualChannelResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.DropVirtualChannelResponse, error) {
		return client.DropVirtualChannel(ctx, req)
	})
}

// SetSegmentState sets the state of a given segment.
func (c *Client) SetSegmentState(ctx context.Context, req *datapb.SetSegmentStateRequest) (*datapb.SetSegmentStateResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.SetSegmentStateResponse, error) {
		return client.SetSegmentState(ctx, req)
	})
}

// Import data files(json, numpy, etc.) on MinIO/S3 storage, read and parse them into sealed segments
func (c *Client) Import(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.ImportTaskResponse, error) {
		return client.Import(ctx, req)
	})
}

// UpdateSegmentStatistics is the client side caller of UpdateSegmentStatistics.
func (c *Client) UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*commonpb.Status, error) {
		return client.UpdateSegmentStatistics(ctx, req)
	})
}

// UpdateChannelCheckpoint updates channel checkpoint in dataCoord.
func (c *Client) UpdateChannelCheckpoint(ctx context.Context, req *datapb.UpdateChannelCheckpointRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*commonpb.Status, error) {
		return client.UpdateChannelCheckpoint(ctx, req)
	})
}

// SaveImportSegment is the DataCoord client side code for SaveImportSegment call.
func (c *Client) SaveImportSegment(ctx context.Context, req *datapb.SaveImportSegmentRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*commonpb.Status, error) {
		return client.SaveImportSegment(ctx, req)
	})
}

func (c *Client) UnsetIsImportingState(ctx context.Context, req *datapb.UnsetIsImportingStateRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*commonpb.Status, error) {
		return client.UnsetIsImportingState(ctx, req)
	})
}

func (c *Client) MarkSegmentsDropped(ctx context.Context, req *datapb.MarkSegmentsDroppedRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*commonpb.Status, error) {
		return client.MarkSegmentsDropped(ctx, req)
	})
}

// BroadcastAlteredCollection is the DataCoord client side code for BroadcastAlteredCollection call.
func (c *Client) BroadcastAlteredCollection(ctx context.Context, req *datapb.AlterCollectionRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*commonpb.Status, error) {
		return client.BroadcastAlteredCollection(ctx, req)
	})
}

func (c *Client) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*milvuspb.CheckHealthResponse, error) {
		return client.CheckHealth(ctx, req)
	})
}

func (c *Client) GcConfirm(ctx context.Context, req *datapb.GcConfirmRequest) (*datapb.GcConfirmResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.GcConfirmResponse, error) {
		return client.GcConfirm(ctx, req)
	})
}

// CreateIndex sends the build index request to IndexCoord.
func (c *Client) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*commonpb.Status, error) {
		return client.CreateIndex(ctx, req)
	})
}

// GetIndexState gets the index states from IndexCoord.
func (c *Client) GetIndexState(ctx context.Context, req *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*indexpb.GetIndexStateResponse, error) {
		return client.GetIndexState(ctx, req)
	})
}

// GetSegmentIndexState gets the index states from IndexCoord.
func (c *Client) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*indexpb.GetSegmentIndexStateResponse, error) {
		return client.GetSegmentIndexState(ctx, req)
	})
}

// GetIndexInfos gets the index file paths from IndexCoord.
func (c *Client) GetIndexInfos(ctx context.Context, req *indexpb.GetIndexInfoRequest) (*indexpb.GetIndexInfoResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*indexpb.GetIndexInfoResponse, error) {
		return client.GetIndexInfos(ctx, req)
	})
}

// DescribeIndex describe the index info of the collection.
func (c *Client) DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*indexpb.DescribeIndexResponse, error) {
		return client.DescribeIndex(ctx, req)
	})
}

// GetIndexStatistics get the statistics of the index.
func (c *Client) GetIndexStatistics(ctx context.Context, req *indexpb.GetIndexStatisticsRequest) (*indexpb.GetIndexStatisticsResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*indexpb.GetIndexStatisticsResponse, error) {
		return client.GetIndexStatistics(ctx, req)
	})
}

// GetIndexBuildProgress describe the progress of the index.
func (c *Client) GetIndexBuildProgress(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest) (*indexpb.GetIndexBuildProgressResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*indexpb.GetIndexBuildProgressResponse, error) {
		return client.GetIndexBuildProgress(ctx, req)
	})
}

// DropIndex sends the drop index request to IndexCoord.
func (c *Client) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*commonpb.Status, error) {
		return client.DropIndex(ctx, req)
	})
}

func (c *Client) ReportDataNodeTtMsgs(ctx context.Context, req *datapb.ReportDataNodeTtMsgsRequest) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*commonpb.Status, error) {
		return client.ReportDataNodeTtMsgs(ctx, req)
	})
}
