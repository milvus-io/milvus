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

	"go.uber.org/zap"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/utils"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
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

// Client grpc client
type Client struct {
	grpcClient grpcclient.GrpcClient[rootcoordpb.RootCoordClient]
	sess       *sessionutil.Session
	ctx        context.Context
}

// NewClient create root coordinator client with specified etcd info and timeout
// ctx execution control context
// metaRoot is the path in etcd for root coordinator registration
// etcdEndpoints are the address list for etcd end points
// timeout is default setting for each grpc call
func NewClient(ctx context.Context) (types.RootCoordClient, error) {
	sess := sessionutil.NewSession(ctx)
	if sess == nil {
		err := fmt.Errorf("new session error, maybe can not connect to etcd")
		log.Ctx(ctx).Debug("New RootCoord Client failed", zap.Error(err))
		return nil, err
	}
	config := &Params.RootCoordGrpcClientCfg
	client := &Client{
		grpcClient: grpcclient.NewClientBase[rootcoordpb.RootCoordClient](config, "milvus.proto.rootcoord.RootCoord"),
		sess:       sess,
		ctx:        ctx,
	}
	client.grpcClient.SetRole(typeutil.RootCoordRole)
	client.grpcClient.SetGetAddrFunc(client.getRootCoordAddr)
	client.grpcClient.SetNewGrpcClientFunc(client.newGrpcClient)
	client.grpcClient.SetSession(sess)

	if Params.InternalTLSCfg.InternalTLSEnabled.GetAsBool() {
		client.grpcClient.EnableEncryption()
		cp, err := utils.CreateCertPoolforClient(Params.InternalTLSCfg.InternalTLSCaPemPath.GetValue(), "RootCoord")
		if err != nil {
			log.Ctx(ctx).Error("Failed to create cert pool for RootCoord client")
			return nil, err
		}
		client.grpcClient.SetInternalTLSCertPool(cp)
		client.grpcClient.SetInternalTLSServerName(Params.InternalTLSCfg.InternalTLSSNI.GetValue())
	}
	return client, nil
}

// Init initialize grpc parameters
func (c *Client) newGrpcClient(cc *grpc.ClientConn) rootcoordpb.RootCoordClient {
	return rootcoordpb.NewRootCoordClient(cc)
}

func (c *Client) getRootCoordAddr() (string, error) {
	log := log.Ctx(c.ctx)
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

// Close terminate grpc connection
func (c *Client) Close() error {
	return c.grpcClient.Close()
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
func (c *Client) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.ComponentStates, error) {
		return client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	})
}

// GetTimeTickChannel get timetick channel name
func (c *Client) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.StringResponse, error) {
		return client.GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
	})
}

// GetStatisticsChannel just define a channel, not used currently
func (c *Client) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.StringResponse, error) {
		return client.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
}

// CreateCollection create collection
func (c *Client) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
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
func (c *Client) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
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
func (c *Client) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest, opts ...grpc.CallOption) (*milvuspb.BoolResponse, error) {
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
func (c *Client) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
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
func (c *Client) describeCollectionInternal(ctx context.Context, in *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.DescribeCollectionResponse, error) {
		return client.DescribeCollectionInternal(ctx, in)
	})
}

func (c *Client) DescribeCollectionInternal(ctx context.Context, in *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	resp, err := c.describeCollectionInternal(ctx, in)
	status, ok := grpcStatus.FromError(err)
	if ok && status.Code() == grpcCodes.Unimplemented {
		return c.DescribeCollection(ctx, in)
	}
	return resp, err
}

// ShowCollections list all collection names
func (c *Client) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowCollectionsResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.ShowCollectionsResponse, error) {
		return client.ShowCollections(ctx, in)
	})
}

func (c *Client) AlterCollection(ctx context.Context, request *milvuspb.AlterCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	request = typeutil.Clone(request)
	commonpbutil.UpdateMsgBase(
		request.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.AlterCollection(ctx, request)
	})
}

func (c *Client) AlterCollectionField(ctx context.Context, request *milvuspb.AlterCollectionFieldRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	request = typeutil.Clone(request)
	commonpbutil.UpdateMsgBase(
		request.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.AlterCollectionField(ctx, request)
	})
}

// CreatePartition create partition
func (c *Client) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
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
func (c *Client) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
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
func (c *Client) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest, opts ...grpc.CallOption) (*milvuspb.BoolResponse, error) {
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
func (c *Client) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
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
func (c *Client) showPartitionsInternal(ctx context.Context, in *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.ShowPartitionsResponse, error) {
		return client.ShowPartitionsInternal(ctx, in)
	})
}

func (c *Client) ShowPartitionsInternal(ctx context.Context, in *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
	resp, err := c.showPartitionsInternal(ctx, in)
	status, ok := grpcStatus.FromError(err)
	if ok && status.Code() == grpcCodes.Unimplemented {
		return c.ShowPartitions(ctx, in)
	}
	return resp, err
}

// AllocTimestamp global timestamp allocator
func (c *Client) AllocTimestamp(ctx context.Context, in *rootcoordpb.AllocTimestampRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
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
func (c *Client) AllocID(ctx context.Context, in *rootcoordpb.AllocIDRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocIDResponse, error) {
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
func (c *Client) UpdateChannelTimeTick(ctx context.Context, in *internalpb.ChannelTimeTickMsg, opts ...grpc.CallOption) (*commonpb.Status, error) {
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
func (c *Client) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest, opts ...grpc.CallOption) (*milvuspb.ShowSegmentsResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.ShowSegmentsResponse, error) {
		return client.ShowSegments(ctx, in)
	})
}

// GetVChannels returns all vchannels belonging to the pchannel.
func (c *Client) GetPChannelInfo(ctx context.Context, in *rootcoordpb.GetPChannelInfoRequest, opts ...grpc.CallOption) (*rootcoordpb.GetPChannelInfoResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*rootcoordpb.GetPChannelInfoResponse, error) {
		return client.GetPChannelInfo(ctx, in)
	})
}

// InvalidateCollectionMetaCache notifies RootCoord to release the collection cache in Proxies.
func (c *Client) InvalidateCollectionMetaCache(ctx context.Context, in *proxypb.InvalidateCollMetaCacheRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
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
func (c *Client) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
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
func (c *Client) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
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
func (c *Client) CreateAlias(ctx context.Context, req *milvuspb.CreateAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
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
func (c *Client) DropAlias(ctx context.Context, req *milvuspb.DropAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
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
func (c *Client) AlterAlias(ctx context.Context, req *milvuspb.AlterAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.AlterAlias(ctx, req)
	})
}

// DescribeAlias describe alias
func (c *Client) DescribeAlias(ctx context.Context, req *milvuspb.DescribeAliasRequest, opts ...grpc.CallOption) (*milvuspb.DescribeAliasResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.DescribeAliasResponse, error) {
		return client.DescribeAlias(ctx, req)
	})
}

// ListAliases list all aliases of db or collection
func (c *Client) ListAliases(ctx context.Context, req *milvuspb.ListAliasesRequest, opts ...grpc.CallOption) (*milvuspb.ListAliasesResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.ListAliasesResponse, error) {
		return client.ListAliases(ctx, req)
	})
}

func (c *Client) CreateCredential(ctx context.Context, req *internalpb.CredentialInfo, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.CreateCredential(ctx, req)
	})
}

func (c *Client) GetCredential(ctx context.Context, req *rootcoordpb.GetCredentialRequest, opts ...grpc.CallOption) (*rootcoordpb.GetCredentialResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*rootcoordpb.GetCredentialResponse, error) {
		return client.GetCredential(ctx, req)
	})
}

func (c *Client) UpdateCredential(ctx context.Context, req *internalpb.CredentialInfo, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.UpdateCredential(ctx, req)
	})
}

func (c *Client) DeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.DeleteCredential(ctx, req)
	})
}

func (c *Client) ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest, opts ...grpc.CallOption) (*milvuspb.ListCredUsersResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.ListCredUsersResponse, error) {
		return client.ListCredUsers(ctx, req)
	})
}

func (c *Client) CreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.CreateRole(ctx, req)
	})
}

func (c *Client) DropRole(ctx context.Context, req *milvuspb.DropRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.DropRole(ctx, req)
	})
}

func (c *Client) OperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.OperateUserRole(ctx, req)
	})
}

func (c *Client) SelectRole(ctx context.Context, req *milvuspb.SelectRoleRequest, opts ...grpc.CallOption) (*milvuspb.SelectRoleResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.SelectRoleResponse, error) {
		return client.SelectRole(ctx, req)
	})
}

func (c *Client) SelectUser(ctx context.Context, req *milvuspb.SelectUserRequest, opts ...grpc.CallOption) (*milvuspb.SelectUserResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.SelectUserResponse, error) {
		return client.SelectUser(ctx, req)
	})
}

func (c *Client) OperatePrivilege(ctx context.Context, req *milvuspb.OperatePrivilegeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.OperatePrivilege(ctx, req)
	})
}

func (c *Client) SelectGrant(ctx context.Context, req *milvuspb.SelectGrantRequest, opts ...grpc.CallOption) (*milvuspb.SelectGrantResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.SelectGrantResponse, error) {
		return client.SelectGrant(ctx, req)
	})
}

func (c *Client) ListPolicy(ctx context.Context, req *internalpb.ListPolicyRequest, opts ...grpc.CallOption) (*internalpb.ListPolicyResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*internalpb.ListPolicyResponse, error) {
		return client.ListPolicy(ctx, req)
	})
}

func (c *Client) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest, opts ...grpc.CallOption) (*milvuspb.CheckHealthResponse, error) {
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.CheckHealthResponse, error) {
		return client.CheckHealth(ctx, req)
	})
}

func (c *Client) RenameCollection(ctx context.Context, req *milvuspb.RenameCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.RenameCollection(ctx, req)
	})
}

func (c *Client) CreateDatabase(ctx context.Context, in *milvuspb.CreateDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
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

func (c *Client) DropDatabase(ctx context.Context, in *milvuspb.DropDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
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

func (c *Client) ListDatabases(ctx context.Context, in *milvuspb.ListDatabasesRequest, opts ...grpc.CallOption) (*milvuspb.ListDatabasesResponse, error) {
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

func (c *Client) DescribeDatabase(ctx context.Context, req *rootcoordpb.DescribeDatabaseRequest, opts ...grpc.CallOption) (*rootcoordpb.DescribeDatabaseResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*rootcoordpb.DescribeDatabaseResponse, error) {
		return client.DescribeDatabase(ctx, req)
	})
}

func (c *Client) AlterDatabase(ctx context.Context, request *rootcoordpb.AlterDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	request = typeutil.Clone(request)
	commonpbutil.UpdateMsgBase(
		request.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.AlterDatabase(ctx, request)
	})
}

func (c *Client) BackupRBAC(ctx context.Context, in *milvuspb.BackupRBACMetaRequest, opts ...grpc.CallOption) (*milvuspb.BackupRBACMetaResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)

	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.BackupRBACMetaResponse, error) {
		return client.BackupRBAC(ctx, in)
	})
}

func (c *Client) RestoreRBAC(ctx context.Context, in *milvuspb.RestoreRBACMetaRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)

	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.RestoreRBAC(ctx, in)
	})
}

func (c *Client) CreatePrivilegeGroup(ctx context.Context, in *milvuspb.CreatePrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)

	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.CreatePrivilegeGroup(ctx, in)
	})
}

func (c *Client) DropPrivilegeGroup(ctx context.Context, in *milvuspb.DropPrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)

	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.DropPrivilegeGroup(ctx, in)
	})
}

func (c *Client) ListPrivilegeGroups(ctx context.Context, in *milvuspb.ListPrivilegeGroupsRequest, opts ...grpc.CallOption) (*milvuspb.ListPrivilegeGroupsResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)

	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*milvuspb.ListPrivilegeGroupsResponse, error) {
		return client.ListPrivilegeGroups(ctx, in)
	})
}

func (c *Client) OperatePrivilegeGroup(ctx context.Context, in *milvuspb.OperatePrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)

	return wrapGrpcCall(ctx, c, func(client rootcoordpb.RootCoordClient) (*commonpb.Status, error) {
		return client.OperatePrivilegeGroup(ctx, in)
	})
}
