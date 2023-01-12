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

package grpcrootcoordclient

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

var Params *paramtable.ComponentParam = paramtable.Get()

// Client grpc client
type Client struct {
	grpcClient grpcclient.GrpcClient[rootcoordpb.RootCoordClient]
	sess       *sessionutil.Session
}

// NewClient create root coordinator client with specified etcd info and timeout
// ctx execution control context
// metaRoot is the path in etcd for root coordinator registration
// etcdEndpoints are the address list for etcd end points
// timeout is default setting for each grpc call
func NewClient(ctx context.Context, metaRoot string, etcdCli *clientv3.Client) (*Client, error) {
	sess := sessionutil.NewSession(ctx, metaRoot, etcdCli)
	if sess == nil {
		err := fmt.Errorf("new session error, maybe can not connect to etcd")
		log.Debug("QueryCoordClient NewClient failed", zap.Error(err))
		return nil, err
	}
	clientParams := &Params.RootCoordGrpcClientCfg
	client := &Client{
		grpcClient: &grpcclient.ClientBase[rootcoordpb.RootCoordClient]{
			ClientMaxRecvSize:      clientParams.ClientMaxRecvSize.GetAsInt(),
			ClientMaxSendSize:      clientParams.ClientMaxSendSize.GetAsInt(),
			DialTimeout:            clientParams.DialTimeout.GetAsDuration(time.Millisecond),
			KeepAliveTime:          clientParams.KeepAliveTime.GetAsDuration(time.Millisecond),
			KeepAliveTimeout:       clientParams.KeepAliveTimeout.GetAsDuration(time.Millisecond),
			RetryServiceNameConfig: "milvus.proto.rootcoord.RootCoord",
			MaxAttempts:            clientParams.MaxAttempts.GetAsInt(),
			InitialBackoff:         float32(clientParams.InitialBackoff.GetAsFloat()),
			MaxBackoff:             float32(clientParams.MaxBackoff.GetAsFloat()),
			BackoffMultiplier:      float32(clientParams.BackoffMultiplier.GetAsFloat()),
			CompressionEnabled:     clientParams.CompressionEnabled.GetAsBool(),
		},
		sess: sess,
	}
	client.grpcClient.SetRole(typeutil.RootCoordRole)
	client.grpcClient.SetGetAddrFunc(client.getRootCoordAddr)
	client.grpcClient.SetNewGrpcClientFunc(client.newGrpcClient)

	return client, nil
}

// Init initialize grpc parameters
func (c *Client) Init() error {
	return nil
}

func (c *Client) newGrpcClient(cc *grpc.ClientConn) rootcoordpb.RootCoordClient {
	return rootcoordpb.NewRootCoordClient(cc)
}

func (c *Client) getRootCoordAddr() (string, error) {
	key := c.grpcClient.GetRole()
	msess, _, err := c.sess.GetSessions(key)
	if err != nil {
		log.Debug("RootCoordClient GetSessions failed", zap.Any("key", key))
		return "", err
	}
	ms, ok := msess[key]
	if !ok {
		log.Warn("RootCoordClient mess key not exist", zap.Any("key", key))
		return "", fmt.Errorf("find no available rootcoord, check rootcoord state")
	}
	log.Debug("RootCoordClient GetSessions success", zap.String("address", ms.Address))
	return ms.Address, nil
}

// Start dummy
func (c *Client) Start() error {
	return nil
}

// Stop terminate grpc connection
func (c *Client) Stop() error {
	return c.grpcClient.Close()
}

// Register dummy
func (c *Client) Register() error {
	return nil
}

// GetComponentStates TODO: timeout need to be propagated through ctx
func (c *Client) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
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

// GetTimeTickChannel get timetick channel name
func (c *Client) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
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

// GetStatisticsChannel just define a channel, not used currently
func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
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

// CreateCollection create collection
func (c *Client) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.CreateCollection(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// DropCollection drop collection
func (c *Client) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.DropCollection(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// HasCollection check collection existence
func (c *Client) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.HasCollection(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.BoolResponse), err
}

// DescribeCollection return collection info
func (c *Client) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.DescribeCollection(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.DescribeCollectionResponse), err
}

// describeCollectionInternal return collection info
func (c *Client) describeCollectionInternal(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.DescribeCollectionInternal(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.DescribeCollectionResponse), err
}

func (c *Client) DescribeCollectionInternal(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	resp, err := c.describeCollectionInternal(ctx, in)
	status, ok := grpcStatus.FromError(err)
	if ok && status.Code() == grpcCodes.Unimplemented {
		return c.DescribeCollection(ctx, in)
	}
	return resp, err
}

// ShowCollections list all collection names
func (c *Client) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.ShowCollections(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.ShowCollectionsResponse), err
}

func (c *Client) AlterCollection(ctx context.Context, request *milvuspb.AlterCollectionRequest) (*commonpb.Status, error) {
	request = typeutil.Clone(request)
	commonpbutil.UpdateMsgBase(
		request.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.AlterCollection(ctx, request)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// CreatePartition create partition
func (c *Client) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.CreatePartition(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// DropPartition drop partition
func (c *Client) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.DropPartition(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// HasPartition check partition existence
func (c *Client) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.HasPartition(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.BoolResponse), err
}

// ShowPartitions list all partitions in collection
func (c *Client) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.ShowPartitions(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.ShowPartitionsResponse), err
}

// showPartitionsInternal list all partitions in collection
func (c *Client) showPartitionsInternal(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.ShowPartitionsInternal(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.ShowPartitionsResponse), err
}

func (c *Client) ShowPartitionsInternal(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	resp, err := c.showPartitionsInternal(ctx, in)
	status, ok := grpcStatus.FromError(err)
	if ok && status.Code() == grpcCodes.Unimplemented {
		return c.ShowPartitions(ctx, in)
	}
	return resp, err
}

// AllocTimestamp global timestamp allocator
func (c *Client) AllocTimestamp(ctx context.Context, in *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.AllocTimestamp(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*rootcoordpb.AllocTimestampResponse), err
}

// AllocID global ID allocator
func (c *Client) AllocID(ctx context.Context, in *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.AllocID(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*rootcoordpb.AllocIDResponse), err
}

// UpdateChannelTimeTick used to handle ChannelTimeTickMsg
func (c *Client) UpdateChannelTimeTick(ctx context.Context, in *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.UpdateChannelTimeTick(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// ShowSegments list all segments
func (c *Client) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.ShowSegments(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.ShowSegmentsResponse), err
}

// InvalidateCollectionMetaCache notifies RootCoord to release the collection cache in Proxies.
func (c *Client) InvalidateCollectionMetaCache(ctx context.Context, in *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.InvalidateCollectionMetaCache(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// ShowConfigurations gets specified configurations para of RootCoord
func (c *Client) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
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

// GetMetrics get metrics
func (c *Client) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.GetMetrics(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.GetMetricsResponse), err
}

// CreateAlias create collection alias
func (c *Client) CreateAlias(ctx context.Context, req *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.CreateAlias(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// DropAlias drop collection alias
func (c *Client) DropAlias(ctx context.Context, req *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.DropAlias(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// AlterAlias alter collection alias
func (c *Client) AlterAlias(ctx context.Context, req *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.AlterAlias(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// Import data files(json, numpy, etc.) on MinIO/S3 storage, read and parse them into sealed segments
func (c *Client) Import(ctx context.Context, req *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.Import(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.ImportResponse), err
}

// Check import task state from datanode
func (c *Client) GetImportState(ctx context.Context, req *milvuspb.GetImportStateRequest) (*milvuspb.GetImportStateResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.GetImportState(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.GetImportStateResponse), err
}

// List id array of all import tasks
func (c *Client) ListImportTasks(ctx context.Context, req *milvuspb.ListImportTasksRequest) (*milvuspb.ListImportTasksResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.ListImportTasks(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.ListImportTasksResponse), err
}

// Report impot task state to rootcoord
func (c *Client) ReportImport(ctx context.Context, req *rootcoordpb.ImportResult) (*commonpb.Status, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.ReportImport(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

func (c *Client) CreateCredential(ctx context.Context, req *internalpb.CredentialInfo) (*commonpb.Status, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.CreateCredential(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

func (c *Client) GetCredential(ctx context.Context, req *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.GetCredential(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*rootcoordpb.GetCredentialResponse), err
}

func (c *Client) UpdateCredential(ctx context.Context, req *internalpb.CredentialInfo) (*commonpb.Status, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.UpdateCredential(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

func (c *Client) DeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.DeleteCredential(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

func (c *Client) ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.ListCredUsers(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.ListCredUsersResponse), err
}

func (c *Client) CreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.CreateRole(ctx, req)
	})
	if err != nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

func (c *Client) DropRole(ctx context.Context, req *milvuspb.DropRoleRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.DropRole(ctx, req)
	})
	if err != nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

func (c *Client) OperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.OperateUserRole(ctx, req)
	})
	if err != nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

func (c *Client) SelectRole(ctx context.Context, req *milvuspb.SelectRoleRequest) (*milvuspb.SelectRoleResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.SelectRole(ctx, req)
	})
	if err != nil {
		return nil, err
	}
	return ret.(*milvuspb.SelectRoleResponse), err
}

func (c *Client) SelectUser(ctx context.Context, req *milvuspb.SelectUserRequest) (*milvuspb.SelectUserResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.SelectUser(ctx, req)
	})
	if err != nil {
		return nil, err
	}
	return ret.(*milvuspb.SelectUserResponse), err
}

func (c *Client) OperatePrivilege(ctx context.Context, req *milvuspb.OperatePrivilegeRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.OperatePrivilege(ctx, req)
	})
	if err != nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

func (c *Client) SelectGrant(ctx context.Context, req *milvuspb.SelectGrantRequest) (*milvuspb.SelectGrantResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.SelectGrant(ctx, req)
	})
	if err != nil {
		return nil, err
	}
	return ret.(*milvuspb.SelectGrantResponse), err
}

func (c *Client) ListPolicy(ctx context.Context, req *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.ListPolicy(ctx, req)
	})
	if err != nil {
		return nil, err
	}
	return ret.(*internalpb.ListPolicyResponse), err
}

func (c *Client) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.CheckHealth(ctx, req)
	})
	if err != nil {
		return nil, err
	}
	return ret.(*milvuspb.CheckHealthResponse), err
}
