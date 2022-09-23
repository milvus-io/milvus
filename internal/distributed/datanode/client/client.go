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

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"google.golang.org/grpc"
)

var ClientParams paramtable.GrpcClientConfig

// Client is the grpc client for DataNode
type Client struct {
	grpcClient grpcclient.GrpcClient
	addr       string
}

// NewClient creates a client for DataNode.
func NewClient(ctx context.Context, addr string) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("address is empty")
	}
	ClientParams.InitOnce(typeutil.DataNodeRole)
	client := &Client{
		addr: addr,
		grpcClient: &grpcclient.ClientBase{
			ClientMaxRecvSize:      ClientParams.ClientMaxRecvSize,
			ClientMaxSendSize:      ClientParams.ClientMaxSendSize,
			DialTimeout:            ClientParams.DialTimeout,
			KeepAliveTime:          ClientParams.KeepAliveTime,
			KeepAliveTimeout:       ClientParams.KeepAliveTimeout,
			RetryServiceNameConfig: "milvus.proto.data.DataNode",
			MaxAttempts:            ClientParams.MaxAttempts,
			InitialBackoff:         ClientParams.InitialBackoff,
			MaxBackoff:             ClientParams.MaxBackoff,
			BackoffMultiplier:      ClientParams.BackoffMultiplier,
		},
	}
	client.grpcClient.SetRole(typeutil.DataNodeRole)
	client.grpcClient.SetGetAddrFunc(client.getAddr)
	client.grpcClient.SetNewGrpcClientFunc(client.newGrpcClient)

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

func (c *Client) newGrpcClient(cc *grpc.ClientConn) interface{} {
	return datapb.NewDataNodeClient(cc)
}

func (c *Client) getAddr() (string, error) {
	return c.addr, nil
}

// GetComponentStates returns ComponentStates
func (c *Client) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataNodeClient).GetComponentStates(ctx, &internalpb.GetComponentStatesRequest{})
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*internalpb.ComponentStates), err
}

// GetStatisticsChannel return the statistics channel in string
// Statistics channel contains statistics infos of query nodes, such as segment infos, memory infos
func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataNodeClient).GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.StringResponse), err
}

// Deprecated
// WatchDmChannels create consumers on dmChannels to reveive Incremental data
func (c *Client) WatchDmChannels(ctx context.Context, req *datapb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataNodeClient).WatchDmChannels(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// FlushSegments notifies DataNode to flush the segments req provids. The flush tasks are async to this
//  rpc, DataNode will flush the segments in the background.
//
// Return UnexpectedError code in status:
//     If DataNode isn't in HEALTHY: states not HEALTHY or dynamic checks not HEALTHY
//     If DataNode doesn't find the correspounding segmentID in its memeory replica
// Return Success code in status and trigers background flush:
//     Log an info log if a segment is under flushing
func (c *Client) FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest) (*commonpb.Status, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataNodeClient).FlushSegments(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// ShowConfigurations gets specified configurations para of DataNode
func (c *Client) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataNodeClient).ShowConfigurations(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}

	return ret.(*internalpb.ShowConfigurationsResponse), err
}

// GetMetrics returns metrics
func (c *Client) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataNodeClient).GetMetrics(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.GetMetricsResponse), err
}

// Compaction return compaction by given plan
func (c *Client) Compaction(ctx context.Context, req *datapb.CompactionPlan) (*commonpb.Status, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataNodeClient).Compaction(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

func (c *Client) GetCompactionState(ctx context.Context, req *datapb.CompactionStateRequest) (*datapb.CompactionStateResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataNodeClient).GetCompactionState(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*datapb.CompactionStateResponse), err
}

// Import data files(json, numpy, etc.) on MinIO/S3 storage, read and parse them into sealed segments
func (c *Client) Import(ctx context.Context, req *datapb.ImportTaskRequest) (*commonpb.Status, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataNodeClient).Import(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

func (c *Client) ResendSegmentStats(ctx context.Context, req *datapb.ResendSegmentStatsRequest) (*datapb.ResendSegmentStatsResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataNodeClient).ResendSegmentStats(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*datapb.ResendSegmentStatsResponse), err
}

// AddSegment is the DataNode client side code for AddSegment call.
func (c *Client) AddSegment(ctx context.Context, req *datapb.AddSegmentRequest) (*commonpb.Status, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataNodeClient).AddSegment(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// SyncSegments is the DataNode client side code for SyncSegments call.
func (c *Client) SyncSegments(ctx context.Context, req *datapb.SyncSegmentsRequest) (*commonpb.Status, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataNodeClient).SyncSegments(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}
