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

package grpcdatanodeclient

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"google.golang.org/grpc"
)

var Params *paramtable.ComponentParam = paramtable.Get()

// Client is the grpc client for DataNode
type Client struct {
	grpcClient grpcclient.GrpcClient[datapb.DataNodeClient]
	addr       string
}

// NewClient creates a client for DataNode.
func NewClient(ctx context.Context, addr string, nodeID int64) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("address is empty")
	}
	config := &Params.DataNodeGrpcClientCfg
	client := &Client{
		addr:       addr,
		grpcClient: grpcclient.NewClientBase[datapb.DataNodeClient](config, "milvus.proto.data.DataNode"),
	}
	client.grpcClient.SetRole(typeutil.DataNodeRole)
	client.grpcClient.SetGetAddrFunc(client.getAddr)
	client.grpcClient.SetNewGrpcClientFunc(client.newGrpcClient)
	client.grpcClient.SetNodeID(nodeID)

	return client, nil
}

// Init initializes the client.
func (c *Client) Init() error {
	return nil
}

// Start starts the client.
// Currently, it does nothing.
func (c *Client) Start() error {
	return nil
}

// Stop stops the client.
// Currently, it closes the grpc connection with the DataNode.
func (c *Client) Stop() error {
	return c.grpcClient.Close()
}

// Register does nothing.
func (c *Client) Register() error {
	return nil
}

func (c *Client) newGrpcClient(cc *grpc.ClientConn) datapb.DataNodeClient {
	return datapb.NewDataNodeClient(cc)
}

func (c *Client) getAddr() (string, error) {
	return c.addr, nil
}

func wrapGrpcCall[T any](ctx context.Context, c *Client, call func(grpcClient datapb.DataNodeClient) (*T, error)) (*T, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client datapb.DataNodeClient) (any, error) {
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

// GetComponentStates returns ComponentStates
func (c *Client) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataNodeClient) (*milvuspb.ComponentStates, error) {
		return client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	})
}

// GetStatisticsChannel return the statistics channel in string
// Statistics channel contains statistics infos of query nodes, such as segment infos, memory infos
func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataNodeClient) (*milvuspb.StringResponse, error) {
		return client.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
}

// Deprecated
// WatchDmChannels create consumers on dmChannels to reveive Incremental data
func (c *Client) WatchDmChannels(ctx context.Context, req *datapb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client datapb.DataNodeClient) (*commonpb.Status, error) {
		return client.WatchDmChannels(ctx, req)
	})
}

// FlushSegments notifies DataNode to flush the segments req provids. The flush tasks are async to this
//
//	rpc, DataNode will flush the segments in the background.
//
// Return UnexpectedError code in status:
//
//	If DataNode isn't in HEALTHY: states not HEALTHY or dynamic checks not HEALTHY
//	If DataNode doesn't find the correspounding segmentID in its memeory replica
//
// Return Success code in status and trigers background flush:
//
//	Log an info log if a segment is under flushing
func (c *Client) FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client datapb.DataNodeClient) (*commonpb.Status, error) {
		return client.FlushSegments(ctx, req)
	})
}

// ShowConfigurations gets specified configurations para of DataNode
func (c *Client) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client datapb.DataNodeClient) (*internalpb.ShowConfigurationsResponse, error) {
		return client.ShowConfigurations(ctx, req)
	})
}

// GetMetrics returns metrics
func (c *Client) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client datapb.DataNodeClient) (*milvuspb.GetMetricsResponse, error) {
		return client.GetMetrics(ctx, req)
	})
}

// Compaction return compaction by given plan
func (c *Client) Compaction(ctx context.Context, req *datapb.CompactionPlan) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataNodeClient) (*commonpb.Status, error) {
		return client.Compaction(ctx, req)
	})
}

func (c *Client) GetCompactionState(ctx context.Context, req *datapb.CompactionStateRequest) (*datapb.CompactionStateResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client datapb.DataNodeClient) (*datapb.CompactionStateResponse, error) {
		return client.GetCompactionState(ctx, req)
	})
}

// Import data files(json, numpy, etc.) on MinIO/S3 storage, read and parse them into sealed segments
func (c *Client) Import(ctx context.Context, req *datapb.ImportTaskRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client datapb.DataNodeClient) (*commonpb.Status, error) {
		return client.Import(ctx, req)
	})
}

func (c *Client) ResendSegmentStats(ctx context.Context, req *datapb.ResendSegmentStatsRequest) (*datapb.ResendSegmentStatsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client datapb.DataNodeClient) (*datapb.ResendSegmentStatsResponse, error) {
		return client.ResendSegmentStats(ctx, req)
	})
}

// AddImportSegment is the DataNode client side code for AddImportSegment call.
func (c *Client) AddImportSegment(ctx context.Context, req *datapb.AddImportSegmentRequest) (*datapb.AddImportSegmentResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client datapb.DataNodeClient) (*datapb.AddImportSegmentResponse, error) {
		return client.AddImportSegment(ctx, req)
	})
}

// SyncSegments is the DataNode client side code for SyncSegments call.
func (c *Client) SyncSegments(ctx context.Context, req *datapb.SyncSegmentsRequest) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client datapb.DataNodeClient) (*commonpb.Status, error) {
		return client.SyncSegments(ctx, req)
	})
}
