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

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"google.golang.org/grpc"
)

var ClientParams paramtable.GrpcClientConfig

// Client is the grpc client for Proxy
type Client struct {
	grpcClient grpcclient.GrpcClient
	addr       string
}

// NewClient creates a new client instance
func NewClient(ctx context.Context, addr string) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("address is empty")
	}
	ClientParams.InitOnce(typeutil.ProxyRole)
	client := &Client{
		addr: addr,
		grpcClient: &grpcclient.ClientBase{
			ClientMaxRecvSize: ClientParams.ClientMaxRecvSize,
			ClientMaxSendSize: ClientParams.ClientMaxSendSize,
			DialTimeout:       ClientParams.DialTimeout,
			KeepAliveTime:     ClientParams.KeepAliveTime,
			KeepAliveTimeout:  ClientParams.KeepAliveTimeout,
		},
	}
	client.grpcClient.SetRole(typeutil.ProxyRole)
	client.grpcClient.SetGetAddrFunc(client.getAddr)
	client.grpcClient.SetNewGrpcClientFunc(client.newGrpcClient)
	return client, nil
}

// Init initializes Proxy's grpc client.
func (c *Client) Init() error {
	return nil
}

func (c *Client) newGrpcClient(cc *grpc.ClientConn) interface{} {
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

// GetComponentStates get the component state.
func (c *Client) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(proxypb.ProxyClient).GetComponentStates(ctx, &internalpb.GetComponentStatesRequest{})
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*internalpb.ComponentStates), err
}

//GetStatisticsChannel return the statistics channel in string
func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(proxypb.ProxyClient).GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.StringResponse), err
}

// InvalidateCollectionMetaCache invalidate collection meta cache
func (c *Client) InvalidateCollectionMetaCache(ctx context.Context, req *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(proxypb.ProxyClient).InvalidateCollectionMetaCache(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// ReleaseDQLMessageStream release dql message stream by request
func (c *Client) ReleaseDQLMessageStream(ctx context.Context, req *proxypb.ReleaseDQLMessageStreamRequest) (*commonpb.Status, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(proxypb.ProxyClient).ReleaseDQLMessageStream(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

func (c *Client) SendSearchResult(ctx context.Context, results *internalpb.SearchResults) (*commonpb.Status, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(proxypb.ProxyClient).SendSearchResult(ctx, results)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

func (c *Client) SendRetrieveResult(ctx context.Context, results *internalpb.RetrieveResults) (*commonpb.Status, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(proxypb.ProxyClient).SendRetrieveResult(ctx, results)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

func (c *Client) InvalidateCredentialCache(ctx context.Context, req *proxypb.InvalidateCredCacheRequest) (*commonpb.Status, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(proxypb.ProxyClient).InvalidateCredentialCache(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

func (c *Client) UpdateCredentialCache(ctx context.Context, req *proxypb.UpdateCredCacheRequest) (*commonpb.Status, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(proxypb.ProxyClient).UpdateCredentialCache(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

func (c *Client) ClearCredUsersCache(ctx context.Context, req *internalpb.ClearCredUsersCacheRequest) (*commonpb.Status, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(proxypb.ProxyClient).ClearCredUsersCache(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}
