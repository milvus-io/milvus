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

package grpcindexnodeclient

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var Params *paramtable.ComponentParam = paramtable.Get()

// Client is the grpc client of IndexNode.
type Client struct {
	grpcClient grpcclient.GrpcClient[indexpb.IndexNodeClient]
	addr       string
}

// NewClient creates a new IndexNode client.
func NewClient(ctx context.Context, addr string, nodeID int64, encryption bool) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("address is empty")
	}
	config := &Params.IndexNodeGrpcClientCfg
	client := &Client{
		addr:       addr,
		grpcClient: grpcclient.NewClientBase[indexpb.IndexNodeClient](config, "milvus.proto.index.IndexNode"),
	}
	client.grpcClient.SetRole(typeutil.IndexNodeRole)
	client.grpcClient.SetGetAddrFunc(client.getAddr)
	client.grpcClient.SetNewGrpcClientFunc(client.newGrpcClient)
	client.grpcClient.SetNodeID(nodeID)
	if encryption {
		client.grpcClient.EnableEncryption()
	}
	return client, nil
}

// Init initializes IndexNode's grpc client.
func (c *Client) Init() error {
	return nil
}

// Start starts IndexNode's client service. But it does nothing here.
func (c *Client) Start() error {
	return nil
}

// Stop stops IndexNode's grpc client.
func (c *Client) Stop() error {
	return c.grpcClient.Close()
}

// Register dummy
func (c *Client) Register() error {
	return nil
}

func (c *Client) newGrpcClient(cc *grpc.ClientConn) indexpb.IndexNodeClient {
	return indexpb.NewIndexNodeClient(cc)
}

func (c *Client) getAddr() (string, error) {
	return c.addr, nil
}

func wrapGrpcCall[T any](ctx context.Context, c *Client, call func(indexClient indexpb.IndexNodeClient) (*T, error)) (*T, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client indexpb.IndexNodeClient) (any, error) {
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

// GetComponentStates gets the component states of IndexNode.
func (c *Client) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	return wrapGrpcCall(ctx, c, func(client indexpb.IndexNodeClient) (*milvuspb.ComponentStates, error) {
		return client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	})
}

func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return wrapGrpcCall(ctx, c, func(client indexpb.IndexNodeClient) (*milvuspb.StringResponse, error) {
		return client.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
}

// CreateJob sends the build index request to IndexNode.
func (c *Client) CreateJob(ctx context.Context, req *indexpb.CreateJobRequest) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client indexpb.IndexNodeClient) (*commonpb.Status, error) {
		return client.CreateJob(ctx, req)
	})
}

// QueryJobs query the task info of the index task.
func (c *Client) QueryJobs(ctx context.Context, req *indexpb.QueryJobsRequest) (*indexpb.QueryJobsResponse, error) {
	return wrapGrpcCall(ctx, c, func(client indexpb.IndexNodeClient) (*indexpb.QueryJobsResponse, error) {
		return client.QueryJobs(ctx, req)
	})
}

// DropJobs query the task info of the index task.
func (c *Client) DropJobs(ctx context.Context, req *indexpb.DropJobsRequest) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client indexpb.IndexNodeClient) (*commonpb.Status, error) {
		return client.DropJobs(ctx, req)
	})
}

// GetJobStats query the task info of the index task.
func (c *Client) GetJobStats(ctx context.Context, req *indexpb.GetJobStatsRequest) (*indexpb.GetJobStatsResponse, error) {
	return wrapGrpcCall(ctx, c, func(client indexpb.IndexNodeClient) (*indexpb.GetJobStatsResponse, error) {
		return client.GetJobStats(ctx, req)
	})
}

// ShowConfigurations gets specified configurations para of IndexNode
func (c *Client) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client indexpb.IndexNodeClient) (*internalpb.ShowConfigurationsResponse, error) {
		return client.ShowConfigurations(ctx, req)
	})
}

// GetMetrics gets the metrics info of IndexNode.
func (c *Client) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client indexpb.IndexNodeClient) (*milvuspb.GetMetricsResponse, error) {
		return client.GetMetrics(ctx, req)
	})
}
