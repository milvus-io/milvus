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

package grpcquerycoordclient

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var Params *paramtable.ComponentParam = paramtable.Get()

// Client is the grpc client of QueryCoord.
type Client struct {
	grpcClient grpcclient.GrpcClient[querypb.QueryCoordClient]
	sess       *sessionutil.Session
}

// NewClient creates a client for QueryCoord grpc call.
func NewClient(ctx context.Context, metaRoot string, etcdCli *clientv3.Client) (*Client, error) {
	sess := sessionutil.NewSession(ctx, metaRoot, etcdCli)
	if sess == nil {
		err := fmt.Errorf("new session error, maybe can not connect to etcd")
		log.Debug("QueryCoordClient NewClient failed", zap.Error(err))
		return nil, err
	}
	clientParams := &Params.QueryCoordGrpcClientCfg
	client := &Client{
		grpcClient: &grpcclient.ClientBase[querypb.QueryCoordClient]{
			ClientMaxRecvSize:      clientParams.ClientMaxRecvSize.GetAsInt(),
			ClientMaxSendSize:      clientParams.ClientMaxSendSize.GetAsInt(),
			DialTimeout:            clientParams.DialTimeout.GetAsDuration(time.Millisecond),
			KeepAliveTime:          clientParams.KeepAliveTime.GetAsDuration(time.Millisecond),
			KeepAliveTimeout:       clientParams.KeepAliveTimeout.GetAsDuration(time.Millisecond),
			RetryServiceNameConfig: "milvus.proto.query.QueryCoord",
			MaxAttempts:            clientParams.MaxAttempts.GetAsInt(),
			InitialBackoff:         float32(clientParams.InitialBackoff.GetAsFloat()),
			MaxBackoff:             float32(clientParams.MaxBackoff.GetAsFloat()),
			BackoffMultiplier:      float32(clientParams.BackoffMultiplier.GetAsFloat()),
			CompressionEnabled:     clientParams.CompressionEnabled.GetAsBool(),
		},
		sess: sess,
	}
	client.grpcClient.SetRole(typeutil.QueryCoordRole)
	client.grpcClient.SetGetAddrFunc(client.getQueryCoordAddr)
	client.grpcClient.SetNewGrpcClientFunc(client.newGrpcClient)

	return client, nil
}

// Init initializes QueryCoord's grpc client.
func (c *Client) Init() error {
	return nil
}

func (c *Client) getQueryCoordAddr() (string, error) {
	key := c.grpcClient.GetRole()
	msess, _, err := c.sess.GetSessions(key)
	if err != nil {
		log.Debug("QueryCoordClient GetSessions failed", zap.Error(err))
		return "", err
	}
	ms, ok := msess[key]
	if !ok {
		log.Debug("QueryCoordClient msess key not existed", zap.Any("key", key))
		return "", fmt.Errorf("find no available querycoord, check querycoord state")
	}
	return ms.Address, nil
}

func (c *Client) newGrpcClient(cc *grpc.ClientConn) querypb.QueryCoordClient {
	return querypb.NewQueryCoordClient(cc)
}

// Start starts QueryCoordinator's client service. But it does nothing here.
func (c *Client) Start() error {
	return nil
}

// Stop stops QueryCoordinator's grpc client server.
func (c *Client) Stop() error {
	return c.grpcClient.Close()
}

// Register dummy
func (c *Client) Register() error {
	return nil
}

// GetComponentStates gets the component states of QueryCoord.
func (c *Client) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
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

// GetTimeTickChannel gets the time tick channel of QueryCoord.
func (c *Client) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
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

// GetStatisticsChannel gets the statistics channel of QueryCoord.
func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
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

// ShowCollections shows the collections in the QueryCoord.
func (c *Client) ShowCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.ShowCollections(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*querypb.ShowCollectionsResponse), err
}

// LoadCollection loads the data of the specified collections in the QueryCoord.
func (c *Client) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.LoadCollection(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// ReleaseCollection release the data of the specified collections in the QueryCoord.
func (c *Client) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
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

// ShowPartitions shows the partitions in the QueryCoord.
func (c *Client) ShowPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.ShowPartitions(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*querypb.ShowPartitionsResponse), err
}

// LoadPartitions loads the data of the specified partitions in the QueryCoord.
func (c *Client) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
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

// ReleasePartitions release the data of the specified partitions in the QueryCoord.
func (c *Client) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
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

// SyncNewCreatedPartition notifies QueryCoord to sync new created partition if collection is loaded.
func (c *Client) SyncNewCreatedPartition(ctx context.Context, req *querypb.SyncNewCreatedPartitionRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.SyncNewCreatedPartition(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// GetPartitionStates gets the states of the specified partition.
func (c *Client) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.GetPartitionStates(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*querypb.GetPartitionStatesResponse), err
}

// GetSegmentInfo gets the information of the specified segment from QueryCoord.
func (c *Client) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
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

// LoadBalance migrate the sealed segments on the source node to the dst nodes.
func (c *Client) LoadBalance(ctx context.Context, req *querypb.LoadBalanceRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.LoadBalance(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// ShowConfigurations gets specified configurations para of QueryCoord
func (c *Client) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
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

// GetMetrics gets the metrics information of QueryCoord.
func (c *Client) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
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

// GetReplicas gets the replicas of a certain collection.
func (c *Client) GetReplicas(ctx context.Context, req *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.GetReplicas(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.GetReplicasResponse), err
}

// GetShardLeaders gets the shard leaders of a certain collection.
func (c *Client) GetShardLeaders(ctx context.Context, req *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.GetShardLeaders(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*querypb.GetShardLeadersResponse), err
}

func (c *Client) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.CheckHealth(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.CheckHealthResponse), err
}

func (c *Client) CreateResourceGroup(ctx context.Context, req *milvuspb.CreateResourceGroupRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.CreateResourceGroup(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

func (c *Client) DropResourceGroup(ctx context.Context, req *milvuspb.DropResourceGroupRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.DropResourceGroup(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

func (c *Client) DescribeResourceGroup(ctx context.Context, req *querypb.DescribeResourceGroupRequest) (*querypb.DescribeResourceGroupResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.DescribeResourceGroup(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*querypb.DescribeResourceGroupResponse), err
}

func (c *Client) TransferNode(ctx context.Context, req *milvuspb.TransferNodeRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.TransferNode(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

func (c *Client) TransferReplica(ctx context.Context, req *querypb.TransferReplicaRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.TransferReplica(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

func (c *Client) ListResourceGroups(ctx context.Context, req *milvuspb.ListResourceGroupsRequest) (*milvuspb.ListResourceGroupsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client querypb.QueryCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.ListResourceGroups(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.ListResourceGroupsResponse), err
}
