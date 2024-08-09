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

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var Params *paramtable.ComponentParam = paramtable.Get()

// Client is the grpc client of IndexNode.
type Client struct {
	grpcClient grpcclient.GrpcClient[workerpb.IndexNodeClient]
	addr       string
	sess       *sessionutil.Session
}

// NewClient creates a new IndexNode client.
func NewClient(ctx context.Context, addr string, nodeID int64, encryption bool) (types.IndexNodeClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("address is empty")
	}
	sess := sessionutil.NewSession(ctx)
	if sess == nil {
		err := fmt.Errorf("new session error, maybe can not connect to etcd")
		log.Debug("IndexNodeClient New Etcd Session failed", zap.Error(err))
		return nil, err
	}
	config := &Params.IndexNodeGrpcClientCfg
	client := &Client{
		addr:       addr,
		grpcClient: grpcclient.NewClientBase[workerpb.IndexNodeClient](config, "milvus.proto.index.IndexNode"),
		sess:       sess,
	}
	// node shall specify node id
	client.grpcClient.SetRole(fmt.Sprintf("%s-%d", typeutil.IndexNodeRole, nodeID))
	client.grpcClient.SetGetAddrFunc(client.getAddr)
	client.grpcClient.SetNewGrpcClientFunc(client.newGrpcClient)
	client.grpcClient.SetNodeID(nodeID)
	client.grpcClient.SetSession(sess)
	if encryption {
		client.grpcClient.EnableEncryption()
	}
	return client, nil
}

// Close stops IndexNode's grpc client.
func (c *Client) Close() error {
	return c.grpcClient.Close()
}

func (c *Client) newGrpcClient(cc *grpc.ClientConn) workerpb.IndexNodeClient {
	return workerpb.NewIndexNodeClient(cc)
}

func (c *Client) getAddr() (string, error) {
	return c.addr, nil
}

func wrapGrpcCall[T any](ctx context.Context, c *Client, call func(indexClient workerpb.IndexNodeClient) (*T, error)) (*T, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client workerpb.IndexNodeClient) (any, error) {
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
func (c *Client) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return wrapGrpcCall(ctx, c, func(client workerpb.IndexNodeClient) (*milvuspb.ComponentStates, error) {
		return client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	})
}

func (c *Client) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return wrapGrpcCall(ctx, c, func(client workerpb.IndexNodeClient) (*milvuspb.StringResponse, error) {
		return client.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
}

// CreateJob sends the build index request to IndexNode.
func (c *Client) CreateJob(ctx context.Context, req *workerpb.CreateJobRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client workerpb.IndexNodeClient) (*commonpb.Status, error) {
		return client.CreateJob(ctx, req)
	})
}

// QueryJobs query the task info of the index task.
func (c *Client) QueryJobs(ctx context.Context, req *workerpb.QueryJobsRequest, opts ...grpc.CallOption) (*workerpb.QueryJobsResponse, error) {
	return wrapGrpcCall(ctx, c, func(client workerpb.IndexNodeClient) (*workerpb.QueryJobsResponse, error) {
		return client.QueryJobs(ctx, req)
	})
}

// DropJobs query the task info of the index task.
func (c *Client) DropJobs(ctx context.Context, req *workerpb.DropJobsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client workerpb.IndexNodeClient) (*commonpb.Status, error) {
		return client.DropJobs(ctx, req)
	})
}

// GetJobStats query the task info of the index task.
func (c *Client) GetJobStats(ctx context.Context, req *workerpb.GetJobStatsRequest, opts ...grpc.CallOption) (*workerpb.GetJobStatsResponse, error) {
	return wrapGrpcCall(ctx, c, func(client workerpb.IndexNodeClient) (*workerpb.GetJobStatsResponse, error) {
		return client.GetJobStats(ctx, req)
	})
}

// ShowConfigurations gets specified configurations para of IndexNode
func (c *Client) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client workerpb.IndexNodeClient) (*internalpb.ShowConfigurationsResponse, error) {
		return client.ShowConfigurations(ctx, req)
	})
}

// GetMetrics gets the metrics info of IndexNode.
func (c *Client) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client workerpb.IndexNodeClient) (*milvuspb.GetMetricsResponse, error) {
		return client.GetMetrics(ctx, req)
	})
}

func (c *Client) CreateJobV2(ctx context.Context, req *workerpb.CreateJobV2Request, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client workerpb.IndexNodeClient) (*commonpb.Status, error) {
		return client.CreateJobV2(ctx, req)
	})
}

func (c *Client) QueryJobsV2(ctx context.Context, req *workerpb.QueryJobsV2Request, opts ...grpc.CallOption) (*workerpb.QueryJobsV2Response, error) {
	return wrapGrpcCall(ctx, c, func(client workerpb.IndexNodeClient) (*workerpb.QueryJobsV2Response, error) {
		return client.QueryJobsV2(ctx, req)
	})
}

func (c *Client) DropJobsV2(ctx context.Context, req *workerpb.DropJobsV2Request, opt ...grpc.CallOption) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client workerpb.IndexNodeClient) (*commonpb.Status, error) {
		return client.DropJobsV2(ctx, req)
	})
}
