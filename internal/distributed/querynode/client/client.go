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
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var Params *paramtable.ComponentParam = paramtable.Get()

// Client is the grpc client of QueryNode.
type Client struct {
	grpcClient grpcclient.GrpcClient[querypb.QueryNodeClient]
	addr       string
	sess       *sessionutil.Session
	nodeID     int64
	ctx        context.Context
}

// NewClient creates a new QueryNode client.
func NewClient(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("addr is empty")
	}
	sess := sessionutil.NewSession(ctx)
	if sess == nil {
		err := fmt.Errorf("new session error, maybe can not connect to etcd")
		log.Ctx(ctx).Debug("QueryNodeClient NewClient failed", zap.Error(err))
		return nil, err
	}
	config := &paramtable.Get().QueryNodeGrpcClientCfg
	client := &Client{
		addr:       addr,
		grpcClient: grpcclient.NewClientBase[querypb.QueryNodeClient](config, "milvus.proto.query.QueryNode"),
		sess:       sess,
		nodeID:     nodeID,
		ctx:        ctx,
	}
	// node shall specify node id
	client.grpcClient.SetRole(fmt.Sprintf("%s-%d", typeutil.QueryNodeRole, nodeID))
	client.grpcClient.SetGetAddrFunc(client.getAddr)
	client.grpcClient.SetNewGrpcClientFunc(client.newGrpcClient)
	client.grpcClient.SetNodeID(nodeID)
	client.grpcClient.SetSession(sess)

	if Params.InternalTLSCfg.InternalTLSEnabled.GetAsBool() {
		client.grpcClient.EnableEncryption()
		cp, err := utils.CreateCertPoolforClient(Params.InternalTLSCfg.InternalTLSCaPemPath.GetValue(), "QueryNode")
		if err != nil {
			log.Ctx(ctx).Error("Failed to create cert pool for QueryNode client")
			return nil, err
		}
		client.grpcClient.SetInternalTLSCertPool(cp)
		client.grpcClient.SetInternalTLSServerName(Params.InternalTLSCfg.InternalTLSSNI.GetValue())
	}
	return client, nil
}

// Close close QueryNode's grpc client
func (c *Client) Close() error {
	return c.grpcClient.Close()
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
func (c *Client) GetComponentStates(ctx context.Context, _ *milvuspb.GetComponentStatesRequest, _ ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*milvuspb.ComponentStates, error) {
		return client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	})
}

// GetTimeTickChannel gets the time tick channel of QueryNode.
func (c *Client) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest, _ ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*milvuspb.StringResponse, error) {
		return client.GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
	})
}

// GetStatisticsChannel gets the statistics channel of QueryNode.
func (c *Client) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest, _ ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*milvuspb.StringResponse, error) {
		return client.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
}

// WatchDmChannels watches the channels about data manipulation.
func (c *Client) WatchDmChannels(ctx context.Context, req *querypb.WatchDmChannelsRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(c.nodeID))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*commonpb.Status, error) {
		return client.WatchDmChannels(ctx, req)
	})
}

// UnsubDmChannel unsubscribes the channels about data manipulation.
func (c *Client) UnsubDmChannel(ctx context.Context, req *querypb.UnsubDmChannelRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(c.nodeID))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*commonpb.Status, error) {
		return client.UnsubDmChannel(ctx, req)
	})
}

// LoadSegments loads the segments to search.
func (c *Client) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(c.nodeID))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*commonpb.Status, error) {
		return client.LoadSegments(ctx, req)
	})
}

// ReleaseCollection releases the data of the specified collection in QueryNode.
func (c *Client) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(c.nodeID))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*commonpb.Status, error) {
		return client.ReleaseCollection(ctx, req)
	})
}

// LoadPartitions updates partitions meta info in QueryNode.
func (c *Client) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(c.nodeID))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*commonpb.Status, error) {
		return client.LoadPartitions(ctx, req)
	})
}

// ReleasePartitions releases the data of the specified partitions in QueryNode.
func (c *Client) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(c.nodeID))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*commonpb.Status, error) {
		return client.ReleasePartitions(ctx, req)
	})
}

// ReleaseSegments releases the data of the specified segments in QueryNode.
func (c *Client) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(c.nodeID))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*commonpb.Status, error) {
		return client.ReleaseSegments(ctx, req)
	})
}

// Search performs replica search tasks in QueryNode.
func (c *Client) Search(ctx context.Context, req *querypb.SearchRequest, _ ...grpc.CallOption) (*internalpb.SearchResults, error) {
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*internalpb.SearchResults, error) {
		return client.Search(ctx, req)
	})
}

func (c *Client) SearchSegments(ctx context.Context, req *querypb.SearchRequest, _ ...grpc.CallOption) (*internalpb.SearchResults, error) {
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*internalpb.SearchResults, error) {
		return client.SearchSegments(ctx, req)
	})
}

// Query performs replica query tasks in QueryNode.
func (c *Client) Query(ctx context.Context, req *querypb.QueryRequest, _ ...grpc.CallOption) (*internalpb.RetrieveResults, error) {
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*internalpb.RetrieveResults, error) {
		return client.Query(ctx, req)
	})
}

func (c *Client) QueryStream(ctx context.Context, req *querypb.QueryRequest, _ ...grpc.CallOption) (querypb.QueryNode_QueryStreamClient, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryNodeClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}

		return client.QueryStream(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(querypb.QueryNode_QueryStreamClient), nil
}

func (c *Client) QuerySegments(ctx context.Context, req *querypb.QueryRequest, _ ...grpc.CallOption) (*internalpb.RetrieveResults, error) {
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*internalpb.RetrieveResults, error) {
		return client.QuerySegments(ctx, req)
	})
}

func (c *Client) QueryStreamSegments(ctx context.Context, req *querypb.QueryRequest, _ ...grpc.CallOption) (querypb.QueryNode_QueryStreamSegmentsClient, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryNodeClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}

		return client.QueryStreamSegments(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(querypb.QueryNode_QueryStreamSegmentsClient), nil
}

// GetSegmentInfo gets the information of the specified segments in QueryNode.
func (c *Client) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest, _ ...grpc.CallOption) (*querypb.GetSegmentInfoResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(c.nodeID))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*querypb.GetSegmentInfoResponse, error) {
		return client.GetSegmentInfo(ctx, req)
	})
}

// SyncReplicaSegments syncs replica node segments information to shard leaders.
func (c *Client) SyncReplicaSegments(ctx context.Context, req *querypb.SyncReplicaSegmentsRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(c.nodeID))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*commonpb.Status, error) {
		return client.SyncReplicaSegments(ctx, req)
	})
}

// ShowConfigurations gets specified configurations para of QueryNode
func (c *Client) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest, _ ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(c.nodeID))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*internalpb.ShowConfigurationsResponse, error) {
		return client.ShowConfigurations(ctx, req)
	})
}

// GetMetrics gets the metrics information of QueryNode.
func (c *Client) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, _ ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(c.nodeID))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*milvuspb.GetMetricsResponse, error) {
		return client.GetMetrics(ctx, req)
	})
}

func (c *Client) GetStatistics(ctx context.Context, request *querypb.GetStatisticsRequest, _ ...grpc.CallOption) (*internalpb.GetStatisticsResponse, error) {
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*internalpb.GetStatisticsResponse, error) {
		return client.GetStatistics(ctx, request)
	})
}

func (c *Client) GetDataDistribution(ctx context.Context, req *querypb.GetDataDistributionRequest, _ ...grpc.CallOption) (*querypb.GetDataDistributionResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(c.nodeID))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*querypb.GetDataDistributionResponse, error) {
		return client.GetDataDistribution(ctx, req)
	})
}

func (c *Client) SyncDistribution(ctx context.Context, req *querypb.SyncDistributionRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(c.nodeID))
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*commonpb.Status, error) {
		return client.SyncDistribution(ctx, req)
	})
}

// Delete is used to forward delete message between delegator and workers.
func (c *Client) Delete(ctx context.Context, req *querypb.DeleteRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(c.nodeID),
	)
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*commonpb.Status, error) {
		return client.Delete(ctx, req)
	})
}

// DeleteBatch is the API to apply same delete data into multiple segments.
// it's basically same as `Delete` but cost less memory pressure.
func (c *Client) DeleteBatch(ctx context.Context, req *querypb.DeleteBatchRequest, _ ...grpc.CallOption) (*querypb.DeleteBatchResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(c.nodeID),
	)
	return wrapGrpcCall(ctx, c, func(client querypb.QueryNodeClient) (*querypb.DeleteBatchResponse, error) {
		return client.DeleteBatch(ctx, req)
	})
}
