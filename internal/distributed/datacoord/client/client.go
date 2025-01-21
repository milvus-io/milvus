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

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/utils"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var Params *paramtable.ComponentParam = paramtable.Get()

var _ types.DataCoordClient = (*Client)(nil)

// Client is the datacoord grpc client
type Client struct {
	grpcClient grpcclient.GrpcClient[datapb.DataCoordClient]
	sess       *sessionutil.Session
	sourceID   int64
	ctx        context.Context
}

// NewClient creates a new client instance
func NewClient(ctx context.Context) (types.DataCoordClient, error) {
	sess := sessionutil.NewSession(ctx)
	if sess == nil {
		err := fmt.Errorf("new session error, maybe can not connect to etcd")
		log.Ctx(ctx).Debug("DataCoordClient NewClient failed", zap.Error(err))
		return nil, err
	}

	config := &Params.DataCoordGrpcClientCfg
	client := &Client{
		grpcClient: grpcclient.NewClientBase[datapb.DataCoordClient](config, "milvus.proto.data.DataCoord"),
		sess:       sess,
		ctx:        ctx,
	}
	client.grpcClient.SetRole(typeutil.DataCoordRole)
	client.grpcClient.SetGetAddrFunc(client.getDataCoordAddr)
	client.grpcClient.SetNewGrpcClientFunc(client.newGrpcClient)
	client.grpcClient.SetSession(sess)

	if Params.InternalTLSCfg.InternalTLSEnabled.GetAsBool() {
		client.grpcClient.EnableEncryption()
		cp, err := utils.CreateCertPoolforClient(Params.InternalTLSCfg.InternalTLSCaPemPath.GetValue(), "Datacoord")
		if err != nil {
			log.Ctx(ctx).Error("Failed to create cert pool for Datacoord client")
			return nil, err
		}
		client.grpcClient.SetInternalTLSCertPool(cp)
		client.grpcClient.SetInternalTLSServerName(Params.InternalTLSCfg.InternalTLSSNI.GetValue())
	}
	return client, nil
}

func (c *Client) newGrpcClient(cc *grpc.ClientConn) datapb.DataCoordClient {
	return datapb.NewDataCoordClient(cc)
}

func (c *Client) getDataCoordAddr() (string, error) {
	key := c.grpcClient.GetRole()
	log := log.Ctx(c.ctx)
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
	log.Debug("DataCoordClient GetSessions success",
		zap.String("address", ms.Address),
		zap.Int64("serverID", ms.ServerID),
	)
	c.grpcClient.SetNodeID(ms.ServerID)
	return ms.Address, nil
}

// Stop stops the client
func (c *Client) Close() error {
	return c.grpcClient.Close()
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
func (c *Client) GetComponentStates(ctx context.Context, _ *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*milvuspb.ComponentStates, error) {
		return client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	})
}

// GetTimeTickChannel return the name of time tick channel.
func (c *Client) GetTimeTickChannel(ctx context.Context, _ *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*milvuspb.StringResponse, error) {
		return client.GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
	})
}

// GetStatisticsChannel return the name of statistics channel.
func (c *Client) GetStatisticsChannel(ctx context.Context, _ *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*milvuspb.StringResponse, error) {
		return client.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
}

// Flush flushes a collection's data
func (c *Client) Flush(ctx context.Context, req *datapb.FlushRequest, opts ...grpc.CallOption) (*datapb.FlushResponse, error) {
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
func (c *Client) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest, opts ...grpc.CallOption) (*datapb.AssignSegmentIDResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.AssignSegmentIDResponse, error) {
		return client.AssignSegmentID(ctx, req)
	})
}

func (c *Client) AllocSegment(ctx context.Context, in *datapb.AllocSegmentRequest, opts ...grpc.CallOption) (*datapb.AllocSegmentResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.AllocSegmentResponse, error) {
		return client.AllocSegment(ctx, in)
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
func (c *Client) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest, opts ...grpc.CallOption) (*datapb.GetSegmentStatesResponse, error) {
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
func (c *Client) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest, opts ...grpc.CallOption) (*datapb.GetInsertBinlogPathsResponse, error) {
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
func (c *Client) GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest, opts ...grpc.CallOption) (*datapb.GetCollectionStatisticsResponse, error) {
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
func (c *Client) GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest, opts ...grpc.CallOption) (*datapb.GetPartitionStatisticsResponse, error) {
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
func (c *Client) GetSegmentInfoChannel(ctx context.Context, _ *datapb.GetSegmentInfoChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
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
func (c *Client) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*datapb.GetSegmentInfoResponse, error) {
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
func (c *Client) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
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
func (c *Client) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest, opts ...grpc.CallOption) (*datapb.GetRecoveryInfoResponse, error) {
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
func (c *Client) GetRecoveryInfoV2(ctx context.Context, req *datapb.GetRecoveryInfoRequestV2, opts ...grpc.CallOption) (*datapb.GetRecoveryInfoResponseV2, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.GetRecoveryInfoResponseV2, error) {
		return client.GetRecoveryInfoV2(ctx, req)
	})
}

// GetChannelRecoveryInfo returns the corresponding vchannel info.
func (c *Client) GetChannelRecoveryInfo(ctx context.Context, req *datapb.GetChannelRecoveryInfoRequest, opts ...grpc.CallOption) (*datapb.GetChannelRecoveryInfoResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.GetChannelRecoveryInfoResponse, error) {
		return client.GetChannelRecoveryInfo(ctx, req)
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
func (c *Client) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest, opts ...grpc.CallOption) (*datapb.GetFlushedSegmentsResponse, error) {
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
func (c *Client) GetSegmentsByStates(ctx context.Context, req *datapb.GetSegmentsByStatesRequest, opts ...grpc.CallOption) (*datapb.GetSegmentsByStatesResponse, error) {
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
func (c *Client) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
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
func (c *Client) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
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
func (c *Client) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest, opts ...grpc.CallOption) (*milvuspb.ManualCompactionResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*milvuspb.ManualCompactionResponse, error) {
		return client.ManualCompaction(ctx, req)
	})
}

// GetCompactionState gets the state of a compaction
func (c *Client) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest, opts ...grpc.CallOption) (*milvuspb.GetCompactionStateResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*milvuspb.GetCompactionStateResponse, error) {
		return client.GetCompactionState(ctx, req)
	})
}

// GetCompactionStateWithPlans gets the state of a compaction by plan
func (c *Client) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest, opts ...grpc.CallOption) (*milvuspb.GetCompactionPlansResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*milvuspb.GetCompactionPlansResponse, error) {
		return client.GetCompactionStateWithPlans(ctx, req)
	})
}

// WatchChannels notifies DataCoord to watch vchannels of a collection
func (c *Client) WatchChannels(ctx context.Context, req *datapb.WatchChannelsRequest, opts ...grpc.CallOption) (*datapb.WatchChannelsResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.WatchChannelsResponse, error) {
		return client.WatchChannels(ctx, req)
	})
}

// GetFlushState gets the flush state of the collection based on the provided flush ts and segment IDs.
func (c *Client) GetFlushState(ctx context.Context, req *datapb.GetFlushStateRequest, opts ...grpc.CallOption) (*milvuspb.GetFlushStateResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*milvuspb.GetFlushStateResponse, error) {
		return client.GetFlushState(ctx, req)
	})
}

// GetFlushAllState checks if all DML messages before `FlushAllTs` have been flushed.
func (c *Client) GetFlushAllState(ctx context.Context, req *milvuspb.GetFlushAllStateRequest, opts ...grpc.CallOption) (*milvuspb.GetFlushAllStateResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*milvuspb.GetFlushAllStateResponse, error) {
		return client.GetFlushAllState(ctx, req)
	})
}

// DropVirtualChannel drops virtual channel in datacoord.
func (c *Client) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest, opts ...grpc.CallOption) (*datapb.DropVirtualChannelResponse, error) {
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
func (c *Client) SetSegmentState(ctx context.Context, req *datapb.SetSegmentStateRequest, opts ...grpc.CallOption) (*datapb.SetSegmentStateResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.SetSegmentStateResponse, error) {
		return client.SetSegmentState(ctx, req)
	})
}

// UpdateSegmentStatistics is the client side caller of UpdateSegmentStatistics.
func (c *Client) UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
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
func (c *Client) UpdateChannelCheckpoint(ctx context.Context, req *datapb.UpdateChannelCheckpointRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*commonpb.Status, error) {
		return client.UpdateChannelCheckpoint(ctx, req)
	})
}

func (c *Client) MarkSegmentsDropped(ctx context.Context, req *datapb.MarkSegmentsDroppedRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
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
func (c *Client) BroadcastAlteredCollection(ctx context.Context, req *datapb.AlterCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*commonpb.Status, error) {
		return client.BroadcastAlteredCollection(ctx, req)
	})
}

// BroadcastAddedField is the DataCoord client side code for BroadcastAddedField call.
func (c *Client) BroadcastAddedField(ctx context.Context, req *datapb.AddFieldRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*commonpb.Status, error) {
		return client.BroadcastAddedField(ctx, req)
	})
}

func (c *Client) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest, opts ...grpc.CallOption) (*milvuspb.CheckHealthResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*milvuspb.CheckHealthResponse, error) {
		return client.CheckHealth(ctx, req)
	})
}

func (c *Client) GcConfirm(ctx context.Context, req *datapb.GcConfirmRequest, opts ...grpc.CallOption) (*datapb.GcConfirmResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*datapb.GcConfirmResponse, error) {
		return client.GcConfirm(ctx, req)
	})
}

// CreateIndex sends the build index request to IndexCoord.
func (c *Client) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	var resp *commonpb.Status
	var err error

	retryErr := retry.Do(ctx, func() error {
		resp, err = wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*commonpb.Status, error) {
			return client.CreateIndex(ctx, req)
		})

		// retry on un implemented, to be compatible with 2.2.x
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			return err
		}
		return nil
	})
	if retryErr != nil {
		return resp, retryErr
	}

	return resp, err
}

// AlterIndex sends the alter index request to IndexCoord.
func (c *Client) AlterIndex(ctx context.Context, req *indexpb.AlterIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*commonpb.Status, error) {
		return client.AlterIndex(ctx, req)
	})
}

// GetIndexState gets the index states from IndexCoord.
func (c *Client) GetIndexState(ctx context.Context, req *indexpb.GetIndexStateRequest, opts ...grpc.CallOption) (*indexpb.GetIndexStateResponse, error) {
	var resp *indexpb.GetIndexStateResponse
	var err error

	retryErr := retry.Do(ctx, func() error {
		resp, err = wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*indexpb.GetIndexStateResponse, error) {
			return client.GetIndexState(ctx, req)
		})

		// retry on un implemented, to be compatible with 2.2.x
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			return err
		}
		return nil
	})
	if retryErr != nil {
		return resp, retryErr
	}

	return resp, err
}

// GetSegmentIndexState gets the index states from IndexCoord.
func (c *Client) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest, opts ...grpc.CallOption) (*indexpb.GetSegmentIndexStateResponse, error) {
	var resp *indexpb.GetSegmentIndexStateResponse
	var err error

	retryErr := retry.Do(ctx, func() error {
		resp, err = wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*indexpb.GetSegmentIndexStateResponse, error) {
			return client.GetSegmentIndexState(ctx, req)
		})

		// retry on un implemented, to be compatible with 2.2.x
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			return err
		}
		return nil
	})
	if retryErr != nil {
		return resp, retryErr
	}

	return resp, err
}

// GetIndexInfos gets the index file paths from IndexCoord.
func (c *Client) GetIndexInfos(ctx context.Context, req *indexpb.GetIndexInfoRequest, opts ...grpc.CallOption) (*indexpb.GetIndexInfoResponse, error) {
	var resp *indexpb.GetIndexInfoResponse
	var err error

	retryErr := retry.Do(ctx, func() error {
		resp, err = wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*indexpb.GetIndexInfoResponse, error) {
			return client.GetIndexInfos(ctx, req)
		})

		// retry on un implemented, to be compatible with 2.2.x
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			return err
		}
		return nil
	})
	if retryErr != nil {
		return resp, retryErr
	}

	return resp, err
}

// DescribeIndex describe the index info of the collection.
func (c *Client) DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest, opts ...grpc.CallOption) (*indexpb.DescribeIndexResponse, error) {
	var resp *indexpb.DescribeIndexResponse
	var err error

	retryErr := retry.Do(ctx, func() error {
		resp, err = wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*indexpb.DescribeIndexResponse, error) {
			return client.DescribeIndex(ctx, req)
		})

		// retry on un implemented, to be compatible with 2.2.x
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			return err
		}
		return nil
	})
	if retryErr != nil {
		return resp, retryErr
	}

	return resp, err
}

// GetIndexStatistics get the statistics of the index.
func (c *Client) GetIndexStatistics(ctx context.Context, req *indexpb.GetIndexStatisticsRequest, opts ...grpc.CallOption) (*indexpb.GetIndexStatisticsResponse, error) {
	var resp *indexpb.GetIndexStatisticsResponse
	var err error

	retryErr := retry.Do(ctx, func() error {
		resp, err = wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*indexpb.GetIndexStatisticsResponse, error) {
			return client.GetIndexStatistics(ctx, req)
		})

		// retry on un implemented, to be compatible with 2.2.x
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			return err
		}
		return nil
	})
	if retryErr != nil {
		return resp, retryErr
	}

	return resp, err
}

// GetIndexBuildProgress describe the progress of the index.
func (c *Client) GetIndexBuildProgress(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest, opts ...grpc.CallOption) (*indexpb.GetIndexBuildProgressResponse, error) {
	var resp *indexpb.GetIndexBuildProgressResponse
	var err error
	retryErr := retry.Do(ctx, func() error {
		resp, err = wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*indexpb.GetIndexBuildProgressResponse, error) {
			return client.GetIndexBuildProgress(ctx, req)
		})

		// retry on un implemented, to be compatible with 2.2.x
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			return err
		}
		return nil
	})
	if retryErr != nil {
		return resp, retryErr
	}

	return resp, err
}

// DropIndex sends the drop index request to IndexCoord.
func (c *Client) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	var resp *commonpb.Status
	var err error

	retryErr := retry.Do(ctx, func() error {
		resp, err = wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*commonpb.Status, error) {
			return client.DropIndex(ctx, req)
		})

		// retry on un implemented, to be compatible with 2.2.x
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			return err
		}
		return nil
	})
	if retryErr != nil {
		return resp, retryErr
	}

	return resp, err
}

func (c *Client) ReportDataNodeTtMsgs(ctx context.Context, req *datapb.ReportDataNodeTtMsgsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*commonpb.Status, error) {
		return client.ReportDataNodeTtMsgs(ctx, req)
	})
}

func (c *Client) GcControl(ctx context.Context, req *datapb.GcControlRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*commonpb.Status, error) {
		return client.GcControl(ctx, req)
	})
}

func (c *Client) ImportV2(ctx context.Context, in *internalpb.ImportRequestInternal, opts ...grpc.CallOption) (*internalpb.ImportResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*internalpb.ImportResponse, error) {
		return client.ImportV2(ctx, in)
	})
}

func (c *Client) GetImportProgress(ctx context.Context, in *internalpb.GetImportProgressRequest, opts ...grpc.CallOption) (*internalpb.GetImportProgressResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*internalpb.GetImportProgressResponse, error) {
		return client.GetImportProgress(ctx, in)
	})
}

func (c *Client) ListImports(ctx context.Context, in *internalpb.ListImportsRequestInternal, opts ...grpc.CallOption) (*internalpb.ListImportsResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*internalpb.ListImportsResponse, error) {
		return client.ListImports(ctx, in)
	})
}

func (c *Client) ListIndexes(ctx context.Context, in *indexpb.ListIndexesRequest, opts ...grpc.CallOption) (*indexpb.ListIndexesResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataCoordClient) (*indexpb.ListIndexesResponse, error) {
		return client.ListIndexes(ctx, in)
	})
}
