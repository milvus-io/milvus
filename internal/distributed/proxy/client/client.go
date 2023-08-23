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

package grpcproxyclient

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"google.golang.org/grpc"
)

var Params *paramtable.ComponentParam = paramtable.Get()

// Client is the grpc client for Proxy
type Client struct {
	grpcClient grpcclient.GrpcClient[proxypb.ProxyClient]
	addr       string
}

// NewClient creates a new client instance
func NewClient(ctx context.Context, addr string, nodeID int64) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("address is empty")
	}
	config := &Params.ProxyGrpcClientCfg
	client := &Client{
		addr:       addr,
		grpcClient: grpcclient.NewClientBase[proxypb.ProxyClient](config, "milvus.proto.proxy.Proxy"),
	}
	client.grpcClient.SetRole(typeutil.ProxyRole)
	client.grpcClient.SetGetAddrFunc(client.getAddr)
	client.grpcClient.SetNewGrpcClientFunc(client.newGrpcClient)
	client.grpcClient.SetNodeID(nodeID)
	return client, nil
}

// Init initializes Proxy's grpc client.
func (c *Client) Init() error {
	return nil
}

func (c *Client) newGrpcClient(cc *grpc.ClientConn) proxypb.ProxyClient {
	return proxypb.NewProxyClient(cc)
}

func (c *Client) getAddr() (string, error) {
	return c.addr, nil
}

// Start dummy
func (c *Client) Start() error {
	return nil
}

// Stop stops the client, closes the connection
func (c *Client) Stop() error {
	return c.grpcClient.Close()
}

// Register dummy
func (c *Client) Register() error {
	return nil
}

func wrapGrpcCall[T any](ctx context.Context, c *Client, call func(proxyClient proxypb.ProxyClient) (*T, error)) (*T, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client proxypb.ProxyClient) (any, error) {
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

// GetComponentStates get the component state.
func (c *Client) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	return wrapGrpcCall(ctx, c, func(client proxypb.ProxyClient) (*milvuspb.ComponentStates, error) {
		return client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	})
}

// GetStatisticsChannel return the statistics channel in string
func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return wrapGrpcCall(ctx, c, func(client proxypb.ProxyClient) (*milvuspb.StringResponse, error) {
		return client.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
}

// InvalidateCollectionMetaCache invalidate collection meta cache
func (c *Client) InvalidateCollectionMetaCache(ctx context.Context, req *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client proxypb.ProxyClient) (*commonpb.Status, error) {
		return client.InvalidateCollectionMetaCache(ctx, req)
	})
}

func (c *Client) InvalidateCredentialCache(ctx context.Context, req *proxypb.InvalidateCredCacheRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client proxypb.ProxyClient) (*commonpb.Status, error) {
		return client.InvalidateCredentialCache(ctx, req)
	})
}

func (c *Client) UpdateCredentialCache(ctx context.Context, req *proxypb.UpdateCredCacheRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client proxypb.ProxyClient) (*commonpb.Status, error) {
		return client.UpdateCredentialCache(ctx, req)
	})
}

func (c *Client) RefreshPolicyInfoCache(ctx context.Context, req *proxypb.RefreshPolicyInfoCacheRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client proxypb.ProxyClient) (*commonpb.Status, error) {
		return client.RefreshPolicyInfoCache(ctx, req)
	})
}

// GetProxyMetrics gets the metrics of proxy, it's an internal interface which is different from GetMetrics interface,
// because it only obtains the metrics of Proxy, not including the topological metrics of Query cluster and Data cluster.
func (c *Client) GetProxyMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client proxypb.ProxyClient) (*milvuspb.GetMetricsResponse, error) {
		return client.GetProxyMetrics(ctx, req)
	})
}

// SetRates notifies Proxy to limit rates of requests.
func (c *Client) SetRates(ctx context.Context, req *proxypb.SetRatesRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client proxypb.ProxyClient) (*commonpb.Status, error) {
		return client.SetRates(ctx, req)
	})
}

func (c *Client) ListClientInfos(ctx context.Context, req *proxypb.ListClientInfosRequest) (*proxypb.ListClientInfosResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client proxypb.ProxyClient) (*proxypb.ListClientInfosResponse, error) {
		return client.ListClientInfos(ctx, req)
	})
}
