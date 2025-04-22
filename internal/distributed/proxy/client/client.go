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
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var Params *paramtable.ComponentParam = paramtable.Get()

// Client is the grpc client for Proxy
type Client struct {
	grpcClient grpcclient.GrpcClient[proxypb.ProxyClient]
	addr       string
	sess       *sessionutil.Session
}

// NewClient creates a new client instance
func NewClient(ctx context.Context, addr string, nodeID int64) (types.ProxyClient, error) {
	if addr == "" {
		return nil, errors.New("address is empty")
	}
	sess := sessionutil.NewSession(ctx)
	if sess == nil {
		err := errors.New("new session error, maybe can not connect to etcd")
		log.Ctx(ctx).Debug("Proxy client new session failed", zap.Error(err))
		return nil, err
	}
	config := &Params.ProxyGrpcClientCfg
	client := &Client{
		addr:       addr,
		grpcClient: grpcclient.NewClientBase[proxypb.ProxyClient](config, "milvus.proto.proxy.Proxy"),
		sess:       sess,
	}
	// node shall specify node id
	client.grpcClient.SetRole(fmt.Sprintf("%s-%d", typeutil.ProxyRole, nodeID))
	client.grpcClient.SetGetAddrFunc(client.getAddr)
	client.grpcClient.SetNewGrpcClientFunc(client.newGrpcClient)
	client.grpcClient.SetNodeID(nodeID)
	client.grpcClient.SetSession(sess)
	if Params.InternalTLSCfg.InternalTLSEnabled.GetAsBool() {
		client.grpcClient.EnableEncryption()
		cp, err := utils.CreateCertPoolforClient(Params.InternalTLSCfg.InternalTLSCaPemPath.GetValue(), "Proxy")
		if err != nil {
			log.Ctx(ctx).Error("Failed to create cert pool for Proxy client")
			return nil, err
		}
		client.grpcClient.SetInternalTLSCertPool(cp)
		client.grpcClient.SetInternalTLSServerName(Params.InternalTLSCfg.InternalTLSSNI.GetValue())
	}
	return client, nil
}

func (c *Client) newGrpcClient(cc *grpc.ClientConn) proxypb.ProxyClient {
	return proxypb.NewProxyClient(cc)
}

func (c *Client) getAddr() (string, error) {
	return c.addr, nil
}

// Close stops the client, closes the connection
func (c *Client) Close() error {
	return c.grpcClient.Close()
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
func (c *Client) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return wrapGrpcCall(ctx, c, func(client proxypb.ProxyClient) (*milvuspb.ComponentStates, error) {
		return client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	})
}

// GetStatisticsChannel return the statistics channel in string
func (c *Client) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return wrapGrpcCall(ctx, c, func(client proxypb.ProxyClient) (*milvuspb.StringResponse, error) {
		return client.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
}

// InvalidateCollectionMetaCache invalidate collection meta cache
func (c *Client) InvalidateCollectionMetaCache(ctx context.Context, req *proxypb.InvalidateCollMetaCacheRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client proxypb.ProxyClient) (*commonpb.Status, error) {
		return client.InvalidateCollectionMetaCache(ctx, req)
	})
}

func (c *Client) InvalidateCredentialCache(ctx context.Context, req *proxypb.InvalidateCredCacheRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client proxypb.ProxyClient) (*commonpb.Status, error) {
		return client.InvalidateCredentialCache(ctx, req)
	})
}

func (c *Client) UpdateCredentialCache(ctx context.Context, req *proxypb.UpdateCredCacheRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client proxypb.ProxyClient) (*commonpb.Status, error) {
		return client.UpdateCredentialCache(ctx, req)
	})
}

func (c *Client) RefreshPolicyInfoCache(ctx context.Context, req *proxypb.RefreshPolicyInfoCacheRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
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
func (c *Client) GetProxyMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
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
func (c *Client) SetRates(ctx context.Context, req *proxypb.SetRatesRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client proxypb.ProxyClient) (*commonpb.Status, error) {
		return client.SetRates(ctx, req)
	})
}

func (c *Client) ListClientInfos(ctx context.Context, req *proxypb.ListClientInfosRequest, opts ...grpc.CallOption) (*proxypb.ListClientInfosResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client proxypb.ProxyClient) (*proxypb.ListClientInfosResponse, error) {
		return client.ListClientInfos(ctx, req)
	})
}

func (c *Client) GetDdChannel(ctx context.Context, req *internalpb.GetDdChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return wrapGrpcCall(ctx, c, func(client proxypb.ProxyClient) (*milvuspb.StringResponse, error) {
		return client.GetDdChannel(ctx, req)
	})
}

func (c *Client) ImportV2(ctx context.Context, req *internalpb.ImportRequest, opts ...grpc.CallOption) (*internalpb.ImportResponse, error) {
	return wrapGrpcCall(ctx, c, func(client proxypb.ProxyClient) (*internalpb.ImportResponse, error) {
		return client.ImportV2(ctx, req)
	})
}

func (c *Client) GetImportProgress(ctx context.Context, req *internalpb.GetImportProgressRequest, opts ...grpc.CallOption) (*internalpb.GetImportProgressResponse, error) {
	return wrapGrpcCall(ctx, c, func(client proxypb.ProxyClient) (*internalpb.GetImportProgressResponse, error) {
		return client.GetImportProgress(ctx, req)
	})
}

func (c *Client) ListImports(ctx context.Context, req *internalpb.ListImportsRequest, opts ...grpc.CallOption) (*internalpb.ListImportsResponse, error) {
	return wrapGrpcCall(ctx, c, func(client proxypb.ProxyClient) (*internalpb.ListImportsResponse, error) {
		return client.ListImports(ctx, req)
	})
}

func (c *Client) InvalidateShardLeaderCache(ctx context.Context, req *proxypb.InvalidateShardLeaderCacheRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client proxypb.ProxyClient) (*commonpb.Status, error) {
		return client.InvalidateShardLeaderCache(ctx, req)
	})
}

func (c *Client) GetSegmentsInfo(ctx context.Context, req *internalpb.GetSegmentsInfoRequest, opts ...grpc.CallOption) (*internalpb.GetSegmentsInfoResponse, error) {
	return wrapGrpcCall(ctx, c, func(client proxypb.ProxyClient) (*internalpb.GetSegmentsInfoResponse, error) {
		return client.GetSegmentsInfo(ctx, req)
	})
}
