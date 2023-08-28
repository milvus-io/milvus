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

	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// Client is the grpc client of QueryNode.
type Client struct {
	grpcClient grpcclient.GrpcClient[querypb.QueryNodeClient]
	addr       string
}

// NewClient creates a new QueryNode client.
func NewClient(ctx context.Context, addr string, nodeID int64) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("addr is empty")
	}
	config := &paramtable.Get().QueryNodeGrpcClientCfg
	client := &Client{
		addr:       addr,
		grpcClient: grpcclient.NewClientBase[querypb.QueryNodeClient](config, "milvus.proto.query.QueryNode"),
	}
	client.grpcClient.SetRole(typeutil.QueryNodeRole)
	client.grpcClient.SetGetAddrFunc(client.getAddr)
	client.grpcClient.SetNewGrpcClientFunc(client.newGrpcClient)
	client.grpcClient.SetNodeID(nodeID)

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

func wrapGrpcCall[T any](ctx context.Context, c *Client, call func(grpcClient querypb.QueryNodeClient) (*T, error)) (*T, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryNodeClient) (any, error) {
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

// GetComponentStates gets the component states of QueryNode.
func (c *Client) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*milvuspb.ComponentStates, error) {
		return client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	})
}

// GetTimeTickChannel gets the time tick channel of QueryNode.
func (c *Client) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*milvuspb.StringResponse, error) {
		return client.GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
	})
}

// GetStatisticsChannel gets the statistics channel of QueryNode.
func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*milvuspb.StringResponse, error) {
		return client.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
}

// WatchDmChannels watches the channels about data manipulation.
func (c *Client) WatchDmChannels(ctx context.Context, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*commonpb.Status, error) {
		return client.WatchDmChannels(ctx, req)
	})
}

// UnsubDmChannel unsubscribes the channels about data manipulation.
func (c *Client) UnsubDmChannel(ctx context.Context, req *querypb.UnsubDmChannelRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*commonpb.Status, error) {
		return client.UnsubDmChannel(ctx, req)
	})
}

// LoadSegments loads the segments to search.
func (c *Client) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*commonpb.Status, error) {
		return client.LoadSegments(ctx, req)
	})
}

// ReleaseCollection releases the data of the specified collection in QueryNode.
func (c *Client) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*commonpb.Status, error) {
		return client.ReleaseCollection(ctx, req)
	})
}

// LoadPartitions updates partitions meta info in QueryNode.
func (c *Client) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*commonpb.Status, error) {
		return client.LoadPartitions(ctx, req)
	})
}

// ReleasePartitions releases the data of the specified partitions in QueryNode.
func (c *Client) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*commonpb.Status, error) {
		return client.ReleasePartitions(ctx, req)
	})
}

// ReleaseSegments releases the data of the specified segments in QueryNode.
func (c *Client) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*commonpb.Status, error) {
		return client.ReleaseSegments(ctx, req)
	})
}

// Search performs replica search tasks in QueryNode.
func (c *Client) Search(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*internalpb.SearchResults, error) {
		return client.Search(ctx, req)
	})
}

func (c *Client) SearchSegments(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*internalpb.SearchResults, error) {
		return client.SearchSegments(ctx, req)
	})
}

// Query performs replica query tasks in QueryNode.
func (c *Client) Query(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*internalpb.RetrieveResults, error) {
		return client.Query(ctx, req)
	})
}

func (c *Client) QuerySegments(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*internalpb.RetrieveResults, error) {
		return client.QuerySegments(ctx, req)
	})
}

// GetSegmentInfo gets the information of the specified segments in QueryNode.
func (c *Client) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*querypb.GetSegmentInfoResponse, error) {
		return client.GetSegmentInfo(ctx, req)
	})
}

// SyncReplicaSegments syncs replica node segments information to shard leaders.
func (c *Client) SyncReplicaSegments(ctx context.Context, req *querypb.SyncReplicaSegmentsRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*commonpb.Status, error) {
		return client.SyncReplicaSegments(ctx, req)
	})
}

// ShowConfigurations gets specified configurations para of QueryNode
func (c *Client) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*internalpb.ShowConfigurationsResponse, error) {
		return client.ShowConfigurations(ctx, req)
	})
}

// GetMetrics gets the metrics information of QueryNode.
func (c *Client) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*milvuspb.GetMetricsResponse, error) {
		return client.GetMetrics(ctx, req)
	})
}

func (c *Client) GetStatistics(ctx context.Context, request *querypb.GetStatisticsRequest) (*internalpb.GetStatisticsResponse, error) {
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*internalpb.GetStatisticsResponse, error) {
		return client.GetStatistics(ctx, request)
	})
}

func (c *Client) GetDataDistribution(ctx context.Context, req *querypb.GetDataDistributionRequest) (*querypb.GetDataDistributionResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*querypb.GetDataDistributionResponse, error) {
		return client.GetDataDistribution(ctx, req)
	})
}

func (c *Client) SyncDistribution(ctx context.Context, req *querypb.SyncDistributionRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*commonpb.Status, error) {
		return client.SyncDistribution(ctx, req)
	})
}

// Delete is used to forward delete message between delegator and workers.
func (c *Client) Delete(ctx context.Context, req *querypb.DeleteRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()),
	)
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*commonpb.Status, error) {
		return client.Delete(ctx, req)
	})
}
