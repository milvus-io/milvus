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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
	log.Debug("RootCoordClient GetSessions success",
		zap.String("address", ms.Address),
		zap.Int64("serverID", ms.ServerID),
	)
	c.grpcClient.SetNodeID(ms.ServerID)
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

func wrapGrpcCall[T any](ctx context.Context, c *Client, call func(grpcClient rootcoordpb.RootCoordClient) (*T, error)) (*T, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
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

// GetComponentStates TODO: timeout need to be propagated through ctx
func (c *Client) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.ComponentStates, error) {
		return client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	})
}

// GetTimeTickChannel get timetick channel name
func (c *Client) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.StringResponse, error) {
		return client.GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
	})
}

// GetStatisticsChannel just define a channel, not used currently
func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.StringResponse, error) {
		return client.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
}

// CreateCollection create collection
func (c *Client) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.CreateCollection(ctx, in)
	})
}

// DropCollection drop collection
func (c *Client) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.DropCollection(ctx, in)
	})
}

// HasCollection check collection existence
func (c *Client) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.BoolResponse, error) {
		return client.HasCollection(ctx, in)
	})
}

// DescribeCollection return collection info
func (c *Client) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.DescribeCollectionResponse, error) {
		return client.DescribeCollection(ctx, in)
	})
}

// describeCollectionInternal return collection info
func (c *Client) describeCollectionInternal(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.DescribeCollectionResponse, error) {
		return client.DescribeCollectionInternal(ctx, in)
	})
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
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.ShowCollectionsResponse, error) {
		return client.ShowCollections(ctx, in)
	})
}

func (c *Client) AlterCollection(ctx context.Context, request *milvuspb.AlterCollectionRequest) (*commonpb.Status, error) {
	request = typeutil.Clone(request)
	commonpbutil.UpdateMsgBase(
		request.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.AlterCollection(ctx, request)
	})
}

// CreatePartition create partition
func (c *Client) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.CreatePartition(ctx, in)
	})
}

// DropPartition drop partition
func (c *Client) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.DropPartition(ctx, in)
	})
}

// HasPartition check partition existence
func (c *Client) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.BoolResponse, error) {
		return client.HasPartition(ctx, in)
	})
}

// ShowPartitions list all partitions in collection
func (c *Client) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.ShowPartitionsResponse, error) {
		return client.ShowPartitions(ctx, in)
	})
}

// showPartitionsInternal list all partitions in collection
func (c *Client) showPartitionsInternal(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.ShowPartitionsResponse, error) {
		return client.ShowPartitionsInternal(ctx, in)
	})
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
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*rootcoordpb.AllocTimestampResponse, error) {
		return client.AllocTimestamp(ctx, in)
	})
}

// AllocID global ID allocator
func (c *Client) AllocID(ctx context.Context, in *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*rootcoordpb.AllocIDResponse, error) {
		return client.AllocID(ctx, in)
	})
}

// UpdateChannelTimeTick used to handle ChannelTimeTickMsg
func (c *Client) UpdateChannelTimeTick(ctx context.Context, in *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.UpdateChannelTimeTick(ctx, in)
	})
}

// ShowSegments list all segments
func (c *Client) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.ShowSegmentsResponse, error) {
		return client.ShowSegments(ctx, in)
	})
}

// InvalidateCollectionMetaCache notifies RootCoord to release the collection cache in Proxies.
func (c *Client) InvalidateCollectionMetaCache(ctx context.Context, in *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.InvalidateCollectionMetaCache(ctx, in)
	})
}

// ShowConfigurations gets specified configurations para of RootCoord
func (c *Client) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*internalpb.ShowConfigurationsResponse, error) {
		return client.ShowConfigurations(ctx, req)
	})
}

// GetMetrics get metrics
func (c *Client) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.GetMetricsResponse, error) {
		return client.GetMetrics(ctx, in)
	})
}

// CreateAlias create collection alias
func (c *Client) CreateAlias(ctx context.Context, req *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.CreateAlias(ctx, req)
	})
}

// DropAlias drop collection alias
func (c *Client) DropAlias(ctx context.Context, req *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.DropAlias(ctx, req)
	})
}

// AlterAlias alter collection alias
func (c *Client) AlterAlias(ctx context.Context, req *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.AlterAlias(ctx, req)
	})
}

// Import data files(json, numpy, etc.) on MinIO/S3 storage, read and parse them into sealed segments
func (c *Client) Import(ctx context.Context, req *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error) {
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.ImportResponse, error) {
		return client.Import(ctx, req)
	})
}

// Check import task state from datanode
func (c *Client) GetImportState(ctx context.Context, req *milvuspb.GetImportStateRequest) (*milvuspb.GetImportStateResponse, error) {
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.GetImportStateResponse, error) {
		return client.GetImportState(ctx, req)
	})
}

// List id array of all import tasks
func (c *Client) ListImportTasks(ctx context.Context, req *milvuspb.ListImportTasksRequest) (*milvuspb.ListImportTasksResponse, error) {
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.ListImportTasksResponse, error) {
		return client.ListImportTasks(ctx, req)
	})
}

// Report impot task state to rootcoord
func (c *Client) ReportImport(ctx context.Context, req *rootcoordpb.ImportResult) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.ReportImport(ctx, req)
	})
}

func (c *Client) CreateCredential(ctx context.Context, req *internalpb.CredentialInfo) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.CreateCredential(ctx, req)
	})
}

func (c *Client) GetCredential(ctx context.Context, req *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*rootcoordpb.GetCredentialResponse, error) {
		return client.GetCredential(ctx, req)
	})
}

func (c *Client) UpdateCredential(ctx context.Context, req *internalpb.CredentialInfo) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.UpdateCredential(ctx, req)
	})
}

func (c *Client) DeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.DeleteCredential(ctx, req)
	})
}

func (c *Client) ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.ListCredUsersResponse, error) {
		return client.ListCredUsers(ctx, req)
	})
}

func (c *Client) CreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.CreateRole(ctx, req)
	})
}

func (c *Client) DropRole(ctx context.Context, req *milvuspb.DropRoleRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.DropRole(ctx, req)
	})
}

func (c *Client) OperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.OperateUserRole(ctx, req)
	})
}

func (c *Client) SelectRole(ctx context.Context, req *milvuspb.SelectRoleRequest) (*milvuspb.SelectRoleResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.SelectRoleResponse, error) {
		return client.SelectRole(ctx, req)
	})
}

func (c *Client) SelectUser(ctx context.Context, req *milvuspb.SelectUserRequest) (*milvuspb.SelectUserResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.SelectUserResponse, error) {
		return client.SelectUser(ctx, req)
	})
}

func (c *Client) OperatePrivilege(ctx context.Context, req *milvuspb.OperatePrivilegeRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.OperatePrivilege(ctx, req)
	})
}

func (c *Client) SelectGrant(ctx context.Context, req *milvuspb.SelectGrantRequest) (*milvuspb.SelectGrantResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.SelectGrantResponse, error) {
		return client.SelectGrant(ctx, req)
	})
}

func (c *Client) ListPolicy(ctx context.Context, req *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*internalpb.ListPolicyResponse, error) {
		return client.ListPolicy(ctx, req)
	})
}

func (c *Client) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.CheckHealthResponse, error) {
		return client.CheckHealth(ctx, req)
	})
}

func (c *Client) RenameCollection(ctx context.Context, req *milvuspb.RenameCollectionRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.RenameCollection(ctx, req)
	})
}

func (c *Client) CreateDatabase(ctx context.Context, in *milvuspb.CreateDatabaseRequest) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.CreateDatabase(ctx, in)
	})

	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

func (c *Client) DropDatabase(ctx context.Context, in *milvuspb.DropDatabaseRequest) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.DropDatabase(ctx, in)
	})

	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

func (c *Client) ListDatabases(ctx context.Context, in *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.ListDatabases(ctx, in)
	})

	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.ListDatabasesResponse), err
}
