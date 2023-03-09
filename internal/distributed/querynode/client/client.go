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

package grpcquerynodeclient

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

var Params *paramtable.ComponentParam = paramtable.Get()

// Client is the grpc client of QueryNode.
type Client struct {
	grpcClient grpcclient.GrpcClient[querypb.QueryNodeClient]
	addr       string
}

// NewClient creates a new QueryNode client.
func NewClient(ctx context.Context, addr string) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("addr is empty")
	}
	clientParams := &Params.QueryNodeGrpcClientCfg
	client := &Client{
		addr: addr,
		grpcClient: &grpcclient.ClientBase[querypb.QueryNodeClient]{
			ClientMaxRecvSize:      clientParams.ClientMaxRecvSize.GetAsInt(),
			ClientMaxSendSize:      clientParams.ClientMaxSendSize.GetAsInt(),
			DialTimeout:            clientParams.DialTimeout.GetAsDuration(time.Millisecond),
			KeepAliveTime:          clientParams.KeepAliveTime.GetAsDuration(time.Millisecond),
			KeepAliveTimeout:       clientParams.KeepAliveTimeout.GetAsDuration(time.Millisecond),
			RetryServiceNameConfig: "milvus.proto.query.QueryNode",
			MaxAttempts:            clientParams.MaxAttempts.GetAsInt(),
			InitialBackoff:         float32(clientParams.InitialBackoff.GetAsFloat()),
			MaxBackoff:             float32(clientParams.MaxBackoff.GetAsFloat()),
			BackoffMultiplier:      float32(clientParams.BackoffMultiplier.GetAsFloat()),
			CompressionEnabled:     clientParams.CompressionEnabled.GetAsBool(),
		},
	}
	client.grpcClient.SetRole(typeutil.QueryNodeRole)
	client.grpcClient.SetGetAddrFunc(client.getAddr)
	client.grpcClient.SetNewGrpcClientFunc(client.newGrpcClient)

	return client, nil
}

// Init initializes QueryNode's grpc client.
func (c *Client) Init() error {
	return nil
}

// Start starts QueryNode's client service. But it does nothing here.
func (c *Client) Start() error {
	return nil
}

// Stop stops QueryNode's grpc client server.
func (c *Client) Stop() error {
	return c.grpcClient.Close()
}

// Register dummy
func (c *Client) Register() error {
	return nil
}

func (c *Client) newGrpcClient(cc *grpc.ClientConn) querypb.QueryNodeClient {
	return querypb.NewQueryNodeClient(cc)
}

func (c *Client) getAddr() (string, error) {
	return c.addr, nil
}

// GetComponentStates gets the component states of QueryNode.
func (c *Client) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryNodeClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.ComponentStates), err
}

// GetTimeTickChannel gets the time tick channel of QueryNode.
func (c *Client) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryNodeClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.StringResponse), err
}

// GetStatisticsChannel gets the statistics channel of QueryNode.
func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryNodeClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.StringResponse), err
}

// WatchDmChannels watches the channels about data manipulation.
func (c *Client) WatchDmChannels(ctx context.Context, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryNodeClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.WatchDmChannels(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// UnsubDmChannel unsubscribes the channels about data manipulation.
func (c *Client) UnsubDmChannel(ctx context.Context, req *querypb.UnsubDmChannelRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryNodeClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.UnsubDmChannel(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// LoadSegments loads the segments to search.
func (c *Client) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryNodeClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.LoadSegments(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// ReleaseCollection releases the data of the specified collection in QueryNode.
func (c *Client) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryNodeClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.ReleaseCollection(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// LoadPartitions updates partitions meta info in QueryNode.
func (c *Client) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryNodeClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.LoadPartitions(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// ReleasePartitions releases the data of the specified partitions in QueryNode.
func (c *Client) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryNodeClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.ReleasePartitions(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// ReleaseSegments releases the data of the specified segments in QueryNode.
func (c *Client) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryNodeClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.ReleaseSegments(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// Search performs replica search tasks in QueryNode.
func (c *Client) Search(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	ret, err := c.grpcClient.Call(ctx, func(client querypb.QueryNodeClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.Search(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*internalpb.SearchResults), err
}

// Query performs replica query tasks in QueryNode.
func (c *Client) Query(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	ret, err := c.grpcClient.Call(ctx, func(client querypb.QueryNodeClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.Query(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*internalpb.RetrieveResults), err
}

// GetSegmentInfo gets the information of the specified segments in QueryNode.
func (c *Client) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryNodeClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.GetSegmentInfo(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*querypb.GetSegmentInfoResponse), err
}

// SyncReplicaSegments syncs replica node segments information to shard leaders.
func (c *Client) SyncReplicaSegments(ctx context.Context, req *querypb.SyncReplicaSegmentsRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryNodeClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.SyncReplicaSegments(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// ShowConfigurations gets specified configurations para of QueryNode
func (c *Client) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryNodeClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.ShowConfigurations(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}

	return ret.(*internalpb.ShowConfigurationsResponse), err
}

// GetMetrics gets the metrics information of QueryNode.
func (c *Client) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryNodeClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.GetMetrics(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.GetMetricsResponse), err
}

func (c *Client) GetStatistics(ctx context.Context, request *querypb.GetStatisticsRequest) (*internalpb.GetStatisticsResponse, error) {
	ret, err := c.grpcClient.Call(ctx, func(client querypb.QueryNodeClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.GetStatistics(ctx, request)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*internalpb.GetStatisticsResponse), err
}

func (c *Client) GetDataDistribution(ctx context.Context, req *querypb.GetDataDistributionRequest) (*querypb.GetDataDistributionResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	ret, err := c.grpcClient.Call(ctx, func(client querypb.QueryNodeClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.GetDataDistribution(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*querypb.GetDataDistributionResponse), err
}

func (c *Client) SyncDistribution(ctx context.Context, req *querypb.SyncDistributionRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	ret, err := c.grpcClient.Call(ctx, func(client querypb.QueryNodeClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.SyncDistribution(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}
