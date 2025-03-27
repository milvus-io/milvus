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

package grpcmixcoordclient

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/utils"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var Params *paramtable.ComponentParam = paramtable.Get()

type MixCoordClient struct {
	rootcoordpb.RootCoordClient
	datapb.DataCoordClient
	querypb.QueryCoordClient
	indexpb.IndexCoordClient
}

// Client grpc client
type Client struct {
	grpcClient grpcclient.GrpcClient[MixCoordClient]
	sess       *sessionutil.Session
	ctx        context.Context
}

// NewClient create root coordinator client with specified etcd info and timeout
// ctx execution control context
// metaRoot is the path in etcd for root coordinator registration
// etcdEndpoints are the address list for etcd end points
// timeout is default setting for each grpc call
func NewClient(ctx context.Context) (types.MixCoordClient, error) {
	sess := sessionutil.NewSession(ctx)
	if sess == nil {
		err := fmt.Errorf("new session error, maybe can not connect to etcd")
		log.Ctx(ctx).Debug("New MixCoord Client failed", zap.Error(err))
		return nil, err
	}
	config := &Params.RootCoordGrpcClientCfg
	client := &Client{
		grpcClient: grpcclient.NewClientBase[MixCoordClient](config, "milvus.proto.rootcoord.RootCoord"),
		sess:       sess,
		ctx:        ctx,
	}
	client.grpcClient.SetRole(typeutil.MixCoordRole)
	client.grpcClient.SetGetAddrFunc(client.getMixCoordAddr)
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
func (c *Client) newGrpcClient(cc *grpc.ClientConn) MixCoordClient {
	return MixCoordClient{
		RootCoordClient:  rootcoordpb.NewRootCoordClient(cc),
		DataCoordClient:  datapb.NewDataCoordClient(cc),
		QueryCoordClient: querypb.NewQueryCoordClient(cc),
	}
}

func (c *Client) getMixCoordAddr() (string, error) {
	log := log.Ctx(c.ctx)
	key := c.grpcClient.GetRole()
	msess, _, err := c.sess.GetSessions(key)
	if err != nil {
		log.Debug("MixCoordClient GetSessions failed", zap.Any("key", key))
		return "", err
	}
	ms, ok := msess[key]
	if !ok {
		log.Warn("MixCoordClient mess key not exist", zap.Any("key", key))
		return "", fmt.Errorf("find no available mixcoord, check mixcoord state")
	}
	log.Debug("MixCoordClient GetSessions success",
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

func wrapGrpcCall[T any](ctx context.Context, c *Client, call func(grpcClient MixCoordClient) (*T, error)) (*T, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client MixCoordClient) (any, error) {
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
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.ComponentStates, error) {
		return client.RootCoordClient.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	})
}

// GetTimeTickChannel get timetick channel name
func (c *Client) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.StringResponse, error) {
		return client.RootCoordClient.GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
	})
}

// GetStatisticsChannel just define a channel, not used currently
func (c *Client) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.StringResponse, error) {
		return client.RootCoordClient.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
}

func (c *Client) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.GetMetricsResponse, error) {
		return client.RootCoordClient.GetMetrics(ctx, req)
	})
}

// ShowConfigurations gets specified configurations para of RootCoord
func (c *Client) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*internalpb.ShowConfigurationsResponse, error) {
		return client.RootCoordClient.ShowConfigurations(ctx, req)
	})
}

// CreateCollection create collection
func (c *Client) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
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
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
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
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.BoolResponse, error) {
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
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.DescribeCollectionResponse, error) {
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
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.DescribeCollectionResponse, error) {
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
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.ShowCollectionsResponse, error) {
		return client.RootCoordClient.ShowCollections(ctx, in)
	})
}

// ShowCollectionIDs returns all collection IDs.
func (c *Client) ShowCollectionIDs(ctx context.Context, in *rootcoordpb.ShowCollectionIDsRequest, opts ...grpc.CallOption) (*rootcoordpb.ShowCollectionIDsResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*rootcoordpb.ShowCollectionIDsResponse, error) {
		return client.ShowCollectionIDs(ctx, in)
	})
}

func (c *Client) AlterCollection(ctx context.Context, request *milvuspb.AlterCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	request = typeutil.Clone(request)
	commonpbutil.UpdateMsgBase(
		request.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.AlterCollection(ctx, request)
	})
}

func (c *Client) AlterCollectionField(ctx context.Context, request *milvuspb.AlterCollectionFieldRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	request = typeutil.Clone(request)
	commonpbutil.UpdateMsgBase(
		request.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
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
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
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
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
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
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.BoolResponse, error) {
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
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.ShowPartitionsResponse, error) {
		return client.RootCoordClient.ShowPartitions(ctx, in)
	})
}

// showPartitionsInternal list all partitions in collection
func (c *Client) showPartitionsInternal(ctx context.Context, in *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.ShowPartitionsResponse, error) {
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
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*rootcoordpb.AllocTimestampResponse, error) {
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
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*rootcoordpb.AllocIDResponse, error) {
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
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
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
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.ShowSegmentsResponse, error) {
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
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*rootcoordpb.GetPChannelInfoResponse, error) {
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
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.InvalidateCollectionMetaCache(ctx, in)
	})
}

// CreateAlias create collection alias
func (c *Client) CreateAlias(ctx context.Context, req *milvuspb.CreateAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
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
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
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
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
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
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.DescribeAliasResponse, error) {
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
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.ListAliasesResponse, error) {
		return client.ListAliases(ctx, req)
	})
}

func (c *Client) CreateCredential(ctx context.Context, req *internalpb.CredentialInfo, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.CreateCredential(ctx, req)
	})
}

func (c *Client) GetCredential(ctx context.Context, req *rootcoordpb.GetCredentialRequest, opts ...grpc.CallOption) (*rootcoordpb.GetCredentialResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*rootcoordpb.GetCredentialResponse, error) {
		return client.GetCredential(ctx, req)
	})
}

func (c *Client) UpdateCredential(ctx context.Context, req *internalpb.CredentialInfo, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.UpdateCredential(ctx, req)
	})
}

func (c *Client) DeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.DeleteCredential(ctx, req)
	})
}

func (c *Client) ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest, opts ...grpc.CallOption) (*milvuspb.ListCredUsersResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.ListCredUsersResponse, error) {
		return client.ListCredUsers(ctx, req)
	})
}

func (c *Client) CreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.CreateRole(ctx, req)
	})
}

func (c *Client) DropRole(ctx context.Context, req *milvuspb.DropRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.DropRole(ctx, req)
	})
}

func (c *Client) OperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.OperateUserRole(ctx, req)
	})
}

func (c *Client) SelectRole(ctx context.Context, req *milvuspb.SelectRoleRequest, opts ...grpc.CallOption) (*milvuspb.SelectRoleResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.SelectRoleResponse, error) {
		return client.SelectRole(ctx, req)
	})
}

func (c *Client) SelectUser(ctx context.Context, req *milvuspb.SelectUserRequest, opts ...grpc.CallOption) (*milvuspb.SelectUserResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.SelectUserResponse, error) {
		return client.SelectUser(ctx, req)
	})
}

func (c *Client) OperatePrivilege(ctx context.Context, req *milvuspb.OperatePrivilegeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.OperatePrivilege(ctx, req)
	})
}

func (c *Client) SelectGrant(ctx context.Context, req *milvuspb.SelectGrantRequest, opts ...grpc.CallOption) (*milvuspb.SelectGrantResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.SelectGrantResponse, error) {
		return client.SelectGrant(ctx, req)
	})
}

func (c *Client) ListPolicy(ctx context.Context, req *internalpb.ListPolicyRequest, opts ...grpc.CallOption) (*internalpb.ListPolicyResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*internalpb.ListPolicyResponse, error) {
		return client.ListPolicy(ctx, req)
	})
}

func (c *Client) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest, opts ...grpc.CallOption) (*milvuspb.CheckHealthResponse, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.CheckHealthResponse, error) {
		return client.RootCoordClient.CheckHealth(ctx, req)
	})
}

func (c *Client) RenameCollection(ctx context.Context, req *milvuspb.RenameCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.RenameCollection(ctx, req)
	})
}

func (c *Client) CreateDatabase(ctx context.Context, in *milvuspb.CreateDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	ret, err := c.grpcClient.ReCall(ctx, func(client MixCoordClient) (any, error) {
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
	ret, err := c.grpcClient.ReCall(ctx, func(client MixCoordClient) (any, error) {
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
	ret, err := c.grpcClient.ReCall(ctx, func(client MixCoordClient) (any, error) {
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
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*rootcoordpb.DescribeDatabaseResponse, error) {
		return client.DescribeDatabase(ctx, req)
	})
}

func (c *Client) AlterDatabase(ctx context.Context, request *rootcoordpb.AlterDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	request = typeutil.Clone(request)
	commonpbutil.UpdateMsgBase(
		request.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.AlterDatabase(ctx, request)
	})
}

func (c *Client) BackupRBAC(ctx context.Context, in *milvuspb.BackupRBACMetaRequest, opts ...grpc.CallOption) (*milvuspb.BackupRBACMetaResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)

	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.BackupRBACMetaResponse, error) {
		return client.BackupRBAC(ctx, in)
	})
}

func (c *Client) RestoreRBAC(ctx context.Context, in *milvuspb.RestoreRBACMetaRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)

	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.RestoreRBAC(ctx, in)
	})
}

func (c *Client) CreatePrivilegeGroup(ctx context.Context, in *milvuspb.CreatePrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)

	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.CreatePrivilegeGroup(ctx, in)
	})
}

func (c *Client) DropPrivilegeGroup(ctx context.Context, in *milvuspb.DropPrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)

	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.DropPrivilegeGroup(ctx, in)
	})
}

func (c *Client) ListPrivilegeGroups(ctx context.Context, in *milvuspb.ListPrivilegeGroupsRequest, opts ...grpc.CallOption) (*milvuspb.ListPrivilegeGroupsResponse, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)

	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.ListPrivilegeGroupsResponse, error) {
		return client.ListPrivilegeGroups(ctx, in)
	})
}

func (c *Client) OperatePrivilegeGroup(ctx context.Context, in *milvuspb.OperatePrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	in = typeutil.Clone(in)
	commonpbutil.UpdateMsgBase(
		in.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)

	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.OperatePrivilegeGroup(ctx, in)
	})
}

// Flush flushes a collection's data
func (c *Client) Flush(ctx context.Context, req *datapb.FlushRequest, opts ...grpc.CallOption) (*datapb.FlushResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*datapb.FlushResponse, error) {
		return client.Flush(ctx, req)
	})
}

// AssignSegmentID applies allocations for specified Coolection/Partition and related Channel Name(Virtial Channel)
//
// ctx is the context to control request deadline and cancellation
// req contains the requester's info(id and role) and the list of Assignment Request,
// which contains the specified collection, partitaion id, the related VChannel Name and row count it needs
//
// response struct `AssignSegmentIDResponse` contains the assignment result for each request
// error is returned only when some communication issue occurs
// if some error occurs in the process of `AssignSegmentID`, it will be recorded and returned in `Status` field of response
//
// `AssignSegmentID` will applies current configured allocation policies for each request
// if the VChannel is newly used, `WatchDmlChannels` will be invoked to notify a `DataNode`(selected by policy) to watch it
// if there is anything make the allocation impossible, the response will not contain the corresponding result
func (c *Client) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest, opts ...grpc.CallOption) (*datapb.AssignSegmentIDResponse, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*datapb.AssignSegmentIDResponse, error) {
		return client.AssignSegmentID(ctx, req)
	})
}

func (c *Client) AllocSegment(ctx context.Context, in *datapb.AllocSegmentRequest, opts ...grpc.CallOption) (*datapb.AllocSegmentResponse, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*datapb.AllocSegmentResponse, error) {
		return client.AllocSegment(ctx, in)
	})
}

// GetSegmentStates requests segment state information
//
// ctx is the context to control request deadline and cancellation
// req contains the list of segment id to query
//
// response struct `GetSegmentStatesResponse` contains the list of each state query result
//
//	when the segment is not found, the state entry will has the field `Status`  to identify failure
//	otherwise the Segment State and Start position information will be returned
//
// error is returned only when some communication issue occurs
func (c *Client) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest, opts ...grpc.CallOption) (*datapb.GetSegmentStatesResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*datapb.GetSegmentStatesResponse, error) {
		return client.GetSegmentStates(ctx, req)
	})
}

// GetInsertBinlogPaths requests binlog paths for specified segment
//
// ctx is the context to control request deadline and cancellation
// req contains the segment id to query
//
// response struct `GetInsertBinlogPathsResponse` contains the fields list
//
//	and corresponding binlog path list
//
// error is returned only when some communication issue occurs
func (c *Client) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest, opts ...grpc.CallOption) (*datapb.GetInsertBinlogPathsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*datapb.GetInsertBinlogPathsResponse, error) {
		return client.GetInsertBinlogPaths(ctx, req)
	})
}

// GetCollectionStatistics requests collection statistics
//
// ctx is the context to control request deadline and cancellation
// req contains the collection id to query
//
// response struct `GetCollectionStatisticsResponse` contains the key-value list fields returning related data
//
//	only row count for now
//
// error is returned only when some communication issue occurs
func (c *Client) GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest, opts ...grpc.CallOption) (*datapb.GetCollectionStatisticsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*datapb.GetCollectionStatisticsResponse, error) {
		return client.GetCollectionStatistics(ctx, req)
	})
}

// GetPartitionStatistics requests partition statistics
//
// ctx is the context to control request deadline and cancellation
// req contains the collection and partition id to query
//
// response struct `GetPartitionStatisticsResponse` contains the key-value list fields returning related data
//
//	only row count for now
//
// error is returned only when some communication issue occurs
func (c *Client) GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest, opts ...grpc.CallOption) (*datapb.GetPartitionStatisticsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*datapb.GetPartitionStatisticsResponse, error) {
		return client.GetPartitionStatistics(ctx, req)
	})
}

// GetSegmentInfoChannel DEPRECATED
// legacy api to get SegmentInfo Channel name
func (c *Client) GetSegmentInfoChannel(ctx context.Context, _ *datapb.GetSegmentInfoChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.StringResponse, error) {
		return client.GetSegmentInfoChannel(ctx, &datapb.GetSegmentInfoChannelRequest{})
	})
}

// GetSegmentInfo requests segment info
//
// ctx is the context to control request deadline and cancellation
// req contains the list of segment ids to query
//
// response struct `GetSegmentInfoResponse` contains the list of segment info
// error is returned only when some communication issue occurs
func (c *Client) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*datapb.GetSegmentInfoResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*datapb.GetSegmentInfoResponse, error) {
		return client.DataCoordClient.GetSegmentInfo(ctx, req)
	})
}

// SaveBinlogPaths updates segments binlogs(including insert binlogs, stats logs and delta logs)
//
//	and related message stream positions
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partition id to query
//
// response status contains the status/error code and failing reason if any
// error is returned only when some communication issue occurs
//
// there is a constraint that the `SaveBinlogPaths` requests of same segment shall be passed in sequence
//
//		the root reason is each `SaveBinlogPaths` will overwrite the checkpoint position
//	 if the constraint is broken, the checkpoint position will not be monotonically increasing and the integrity will be compromised
func (c *Client) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	// use Call here on purpose
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.SaveBinlogPaths(ctx, req)
	})
}

// GetRecoveryInfo request segment recovery info of collection/partition
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partition id to query
//
// response struct `GetRecoveryInfoResponse` contains the list of segments info and corresponding vchannel info
// error is returned only when some communication issue occurs
func (c *Client) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest, opts ...grpc.CallOption) (*datapb.GetRecoveryInfoResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*datapb.GetRecoveryInfoResponse, error) {
		return client.GetRecoveryInfo(ctx, req)
	})
}

// GetRecoveryInfoV2 request segment recovery info of collection/partitions
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partitions id to query
//
// response struct `GetRecoveryInfoResponseV2` contains the list of segments info and corresponding vchannel info
// error is returned only when some communication issue occurs
func (c *Client) GetRecoveryInfoV2(ctx context.Context, req *datapb.GetRecoveryInfoRequestV2, opts ...grpc.CallOption) (*datapb.GetRecoveryInfoResponseV2, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*datapb.GetRecoveryInfoResponseV2, error) {
		return client.GetRecoveryInfoV2(ctx, req)
	})
}

// GetChannelRecoveryInfo returns the corresponding vchannel info.
func (c *Client) GetChannelRecoveryInfo(ctx context.Context, req *datapb.GetChannelRecoveryInfoRequest, opts ...grpc.CallOption) (*datapb.GetChannelRecoveryInfoResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*datapb.GetChannelRecoveryInfoResponse, error) {
		return client.GetChannelRecoveryInfo(ctx, req)
	})
}

// GetFlushedSegments returns flushed segment list of requested collection/parition
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partition id to query
//
//	when partition is lesser or equal to 0, all flushed segments of collection will be returned
//
// response struct `GetFlushedSegmentsResponse` contains flushed segment id list
// error is returned only when some communication issue occurs
func (c *Client) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest, opts ...grpc.CallOption) (*datapb.GetFlushedSegmentsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*datapb.GetFlushedSegmentsResponse, error) {
		return client.GetFlushedSegments(ctx, req)
	})
}

// GetSegmentsByStates returns segment list of requested collection/partition and segment states
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partition id and segment states to query
// when partition is lesser or equal to 0, all segments of collection will be returned
//
// response struct `GetSegmentsByStatesResponse` contains segment id list
// error is returned only when some communication issue occurs
func (c *Client) GetSegmentsByStates(ctx context.Context, req *datapb.GetSegmentsByStatesRequest, opts ...grpc.CallOption) (*datapb.GetSegmentsByStatesResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*datapb.GetSegmentsByStatesResponse, error) {
		return client.GetSegmentsByStates(ctx, req)
	})
}

// ManualCompaction triggers a compaction for a collection
func (c *Client) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest, opts ...grpc.CallOption) (*milvuspb.ManualCompactionResponse, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.ManualCompactionResponse, error) {
		return client.ManualCompaction(ctx, req)
	})
}

// GetCompactionState gets the state of a compaction
func (c *Client) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest, opts ...grpc.CallOption) (*milvuspb.GetCompactionStateResponse, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.GetCompactionStateResponse, error) {
		return client.GetCompactionState(ctx, req)
	})
}

// GetCompactionStateWithPlans gets the state of a compaction by plan
func (c *Client) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest, opts ...grpc.CallOption) (*milvuspb.GetCompactionPlansResponse, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.GetCompactionPlansResponse, error) {
		return client.GetCompactionStateWithPlans(ctx, req)
	})
}

// WatchChannels notifies DataCoord to watch vchannels of a collection
func (c *Client) WatchChannels(ctx context.Context, req *datapb.WatchChannelsRequest, opts ...grpc.CallOption) (*datapb.WatchChannelsResponse, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*datapb.WatchChannelsResponse, error) {
		return client.WatchChannels(ctx, req)
	})
}

// GetFlushState gets the flush state of the collection based on the provided flush ts and segment IDs.
func (c *Client) GetFlushState(ctx context.Context, req *datapb.GetFlushStateRequest, opts ...grpc.CallOption) (*milvuspb.GetFlushStateResponse, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.GetFlushStateResponse, error) {
		return client.GetFlushState(ctx, req)
	})
}

// GetFlushAllState checks if all DML messages before `FlushAllTs` have been flushed.
func (c *Client) GetFlushAllState(ctx context.Context, req *milvuspb.GetFlushAllStateRequest, opts ...grpc.CallOption) (*milvuspb.GetFlushAllStateResponse, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.GetFlushAllStateResponse, error) {
		return client.GetFlushAllState(ctx, req)
	})
}

// DropVirtualChannel drops virtual channel in datacoord.
func (c *Client) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest, opts ...grpc.CallOption) (*datapb.DropVirtualChannelResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*datapb.DropVirtualChannelResponse, error) {
		return client.DropVirtualChannel(ctx, req)
	})
}

// SetSegmentState sets the state of a given segment.
func (c *Client) SetSegmentState(ctx context.Context, req *datapb.SetSegmentStateRequest, opts ...grpc.CallOption) (*datapb.SetSegmentStateResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*datapb.SetSegmentStateResponse, error) {
		return client.SetSegmentState(ctx, req)
	})
}

// UpdateSegmentStatistics is the client side caller of UpdateSegmentStatistics.
func (c *Client) UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.UpdateSegmentStatistics(ctx, req)
	})
}

// UpdateChannelCheckpoint updates channel checkpoint in dataCoord.
func (c *Client) UpdateChannelCheckpoint(ctx context.Context, req *datapb.UpdateChannelCheckpointRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.UpdateChannelCheckpoint(ctx, req)
	})
}

func (c *Client) MarkSegmentsDropped(ctx context.Context, req *datapb.MarkSegmentsDroppedRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.MarkSegmentsDropped(ctx, req)
	})
}

// BroadcastAlteredCollection is the DataCoord client side code for BroadcastAlteredCollection call.
func (c *Client) BroadcastAlteredCollection(ctx context.Context, req *datapb.AlterCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.BroadcastAlteredCollection(ctx, req)
	})
}

func (c *Client) GcConfirm(ctx context.Context, req *datapb.GcConfirmRequest, opts ...grpc.CallOption) (*datapb.GcConfirmResponse, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*datapb.GcConfirmResponse, error) {
		return client.GcConfirm(ctx, req)
	})
}

// CreateIndex sends the build index request to IndexCoord.
func (c *Client) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	var resp *commonpb.Status
	var err error

	retryErr := retry.Do(ctx, func() error {
		resp, err = wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
			return client.DataCoordClient.CreateIndex(ctx, req)
		})

		// retry on un implemented, to be compatible with 2.2.x
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			return err
		}
		return nil
	})
	if retryErr != nil {
		return resp, retryErr
	}

	return resp, err
}

// AlterIndex sends the alter index request to IndexCoord.
func (c *Client) AlterIndex(ctx context.Context, req *indexpb.AlterIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.DataCoordClient.AlterIndex(ctx, req)
	})
}

// GetIndexState gets the index states from IndexCoord.
func (c *Client) GetIndexState(ctx context.Context, req *indexpb.GetIndexStateRequest, opts ...grpc.CallOption) (*indexpb.GetIndexStateResponse, error) {
	var resp *indexpb.GetIndexStateResponse
	var err error

	retryErr := retry.Do(ctx, func() error {
		resp, err = wrapGrpcCall(ctx, c, func(client MixCoordClient) (*indexpb.GetIndexStateResponse, error) {
			return client.DataCoordClient.GetIndexState(ctx, req)
		})

		// retry on un implemented, to be compatible with 2.2.x
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			return err
		}
		return nil
	})
	if retryErr != nil {
		return resp, retryErr
	}

	return resp, err
}

// GetSegmentIndexState gets the index states from IndexCoord.
func (c *Client) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest, opts ...grpc.CallOption) (*indexpb.GetSegmentIndexStateResponse, error) {
	var resp *indexpb.GetSegmentIndexStateResponse
	var err error

	retryErr := retry.Do(ctx, func() error {
		resp, err = wrapGrpcCall(ctx, c, func(client MixCoordClient) (*indexpb.GetSegmentIndexStateResponse, error) {
			return client.DataCoordClient.GetSegmentIndexState(ctx, req)
		})

		// retry on un implemented, to be compatible with 2.2.x
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			return err
		}
		return nil
	})
	if retryErr != nil {
		return resp, retryErr
	}

	return resp, err
}

// GetIndexInfos gets the index file paths from IndexCoord.
func (c *Client) GetIndexInfos(ctx context.Context, req *indexpb.GetIndexInfoRequest, opts ...grpc.CallOption) (*indexpb.GetIndexInfoResponse, error) {
	var resp *indexpb.GetIndexInfoResponse
	var err error

	retryErr := retry.Do(ctx, func() error {
		resp, err = wrapGrpcCall(ctx, c, func(client MixCoordClient) (*indexpb.GetIndexInfoResponse, error) {
			return client.DataCoordClient.GetIndexInfos(ctx, req)
		})

		// retry on un implemented, to be compatible with 2.2.x
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			return err
		}
		return nil
	})
	if retryErr != nil {
		return resp, retryErr
	}

	return resp, err
}

// DescribeIndex describe the index info of the collection.
func (c *Client) DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest, opts ...grpc.CallOption) (*indexpb.DescribeIndexResponse, error) {
	var resp *indexpb.DescribeIndexResponse
	var err error

	retryErr := retry.Do(ctx, func() error {
		resp, err = wrapGrpcCall(ctx, c, func(client MixCoordClient) (*indexpb.DescribeIndexResponse, error) {
			return client.DataCoordClient.DescribeIndex(ctx, req)
		})

		// retry on un implemented, to be compatible with 2.2.x
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			return err
		}
		return nil
	})
	if retryErr != nil {
		return resp, retryErr
	}

	return resp, err
}

// GetIndexStatistics get the statistics of the index.
func (c *Client) GetIndexStatistics(ctx context.Context, req *indexpb.GetIndexStatisticsRequest, opts ...grpc.CallOption) (*indexpb.GetIndexStatisticsResponse, error) {
	var resp *indexpb.GetIndexStatisticsResponse
	var err error

	retryErr := retry.Do(ctx, func() error {
		resp, err = wrapGrpcCall(ctx, c, func(client MixCoordClient) (*indexpb.GetIndexStatisticsResponse, error) {
			return client.DataCoordClient.GetIndexStatistics(ctx, req)
		})

		// retry on un implemented, to be compatible with 2.2.x
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			return err
		}
		return nil
	})
	if retryErr != nil {
		return resp, retryErr
	}

	return resp, err
}

// GetIndexBuildProgress describe the progress of the index.
func (c *Client) GetIndexBuildProgress(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest, opts ...grpc.CallOption) (*indexpb.GetIndexBuildProgressResponse, error) {
	var resp *indexpb.GetIndexBuildProgressResponse
	var err error
	retryErr := retry.Do(ctx, func() error {
		resp, err = wrapGrpcCall(ctx, c, func(client MixCoordClient) (*indexpb.GetIndexBuildProgressResponse, error) {
			return client.DataCoordClient.GetIndexBuildProgress(ctx, req)
		})

		// retry on un implemented, to be compatible with 2.2.x
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			return err
		}
		return nil
	})
	if retryErr != nil {
		return resp, retryErr
	}

	return resp, err
}

// DropIndex sends the drop index request to IndexCoord.
func (c *Client) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	var resp *commonpb.Status
	var err error

	retryErr := retry.Do(ctx, func() error {
		resp, err = wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
			return client.DataCoordClient.DropIndex(ctx, req)
		})

		// retry on un implemented, to be compatible with 2.2.x
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			return err
		}
		return nil
	})
	if retryErr != nil {
		return resp, retryErr
	}

	return resp, err
}

func (c *Client) ReportDataNodeTtMsgs(ctx context.Context, req *datapb.ReportDataNodeTtMsgsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.ReportDataNodeTtMsgs(ctx, req)
	})
}

func (c *Client) GcControl(ctx context.Context, req *datapb.GcControlRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.GcControl(ctx, req)
	})
}

func (c *Client) ImportV2(ctx context.Context, in *internalpb.ImportRequestInternal, opts ...grpc.CallOption) (*internalpb.ImportResponse, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*internalpb.ImportResponse, error) {
		return client.ImportV2(ctx, in)
	})
}

func (c *Client) GetImportProgress(ctx context.Context, in *internalpb.GetImportProgressRequest, opts ...grpc.CallOption) (*internalpb.GetImportProgressResponse, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*internalpb.GetImportProgressResponse, error) {
		return client.GetImportProgress(ctx, in)
	})
}

func (c *Client) ListImports(ctx context.Context, in *internalpb.ListImportsRequestInternal, opts ...grpc.CallOption) (*internalpb.ListImportsResponse, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*internalpb.ListImportsResponse, error) {
		return client.ListImports(ctx, in)
	})
}

func (c *Client) ListIndexes(ctx context.Context, in *indexpb.ListIndexesRequest, opts ...grpc.CallOption) (*indexpb.ListIndexesResponse, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*indexpb.ListIndexesResponse, error) {
		return client.ListIndexes(ctx, in)
	})
}

func (c *Client) ShowLoadCollections(ctx context.Context, req *querypb.ShowCollectionsRequest, opts ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*querypb.ShowCollectionsResponse, error) {
		return client.ShowLoadCollections(ctx, req)
	})
}

// LoadCollection loads the data of the specified collections in the QueryCoord.
func (c *Client) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.LoadCollection(ctx, req)
	})
}

// ReleaseCollection release the data of the specified collections in the QueryCoord.
func (c *Client) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.ReleaseCollection(ctx, req)
	})
}

// ShowPartitions shows the partitions in the QueryCoord.
func (c *Client) ShowLoadPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest, opts ...grpc.CallOption) (*querypb.ShowPartitionsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*querypb.ShowPartitionsResponse, error) {
		return client.ShowLoadPartitions(ctx, req)
	})
}

// LoadPartitions loads the data of the specified partitions in the QueryCoord.
func (c *Client) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.LoadPartitions(ctx, req)
	})
}

// ReleasePartitions release the data of the specified partitions in the QueryCoord.
func (c *Client) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.ReleasePartitions(ctx, req)
	})
}

// SyncNewCreatedPartition notifies QueryCoord to sync new created partition if collection is loaded.
func (c *Client) SyncNewCreatedPartition(ctx context.Context, req *querypb.SyncNewCreatedPartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.sess.ServerID)),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.SyncNewCreatedPartition(ctx, req)
	})
}

// GetPartitionStates gets the states of the specified partition.
func (c *Client) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest, opts ...grpc.CallOption) (*querypb.GetPartitionStatesResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*querypb.GetPartitionStatesResponse, error) {
		return client.GetPartitionStates(ctx, req)
	})
}

// GetSegmentInfo gets the information of the specified segment from QueryCoord.
func (c *Client) GetLoadSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*querypb.GetSegmentInfoResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*querypb.GetSegmentInfoResponse, error) {
		return client.GetLoadSegmentInfo(ctx, req)
	})
}

// LoadBalance migrate the sealed segments on the source node to the dst nodes.
func (c *Client) LoadBalance(ctx context.Context, req *querypb.LoadBalanceRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.LoadBalance(ctx, req)
	})
}

// ShowConfigurations gets specified configurations para of QueryCoord
// func (c *Client) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
// 	req = typeutil.Clone(req)
// 	commonpbutil.UpdateMsgBase(
// 		req.GetBase(),
// 		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
// 	)
// 	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*internalpb.ShowConfigurationsResponse, error) {
// 		return client.ShowConfigurations(ctx, req)
// 	})
// }

// GetReplicas gets the replicas of a certain collection.
func (c *Client) GetReplicas(ctx context.Context, req *milvuspb.GetReplicasRequest, opts ...grpc.CallOption) (*milvuspb.GetReplicasResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.GetReplicasResponse, error) {
		return client.GetReplicas(ctx, req)
	})
}

// GetShardLeaders gets the shard leaders of a certain collection.
func (c *Client) GetShardLeaders(ctx context.Context, req *querypb.GetShardLeadersRequest, opts ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*querypb.GetShardLeadersResponse, error) {
		return client.GetShardLeaders(ctx, req)
	})
}

func (c *Client) CreateResourceGroup(ctx context.Context, req *milvuspb.CreateResourceGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.CreateResourceGroup(ctx, req)
	})
}

func (c *Client) UpdateResourceGroups(ctx context.Context, req *querypb.UpdateResourceGroupsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.UpdateResourceGroups(ctx, req)
	})
}

func (c *Client) DropResourceGroup(ctx context.Context, req *milvuspb.DropResourceGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.DropResourceGroup(ctx, req)
	})
}

func (c *Client) DescribeResourceGroup(ctx context.Context, req *querypb.DescribeResourceGroupRequest, opts ...grpc.CallOption) (*querypb.DescribeResourceGroupResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*querypb.DescribeResourceGroupResponse, error) {
		return client.DescribeResourceGroup(ctx, req)
	})
}

func (c *Client) TransferNode(ctx context.Context, req *milvuspb.TransferNodeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.TransferNode(ctx, req)
	})
}

func (c *Client) TransferReplica(ctx context.Context, req *querypb.TransferReplicaRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.TransferReplica(ctx, req)
	})
}

func (c *Client) ListResourceGroups(ctx context.Context, req *milvuspb.ListResourceGroupsRequest, opts ...grpc.CallOption) (*milvuspb.ListResourceGroupsResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*milvuspb.ListResourceGroupsResponse, error) {
		return client.ListResourceGroups(ctx, req)
	})
}

func (c *Client) ListCheckers(ctx context.Context, req *querypb.ListCheckersRequest, opts ...grpc.CallOption) (*querypb.ListCheckersResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*querypb.ListCheckersResponse, error) {
		return client.ListCheckers(ctx, req)
	})
}

func (c *Client) ActivateChecker(ctx context.Context, req *querypb.ActivateCheckerRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.ActivateChecker(ctx, req)
	})
}

func (c *Client) DeactivateChecker(ctx context.Context, req *querypb.DeactivateCheckerRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.DeactivateChecker(ctx, req)
	})
}

func (c *Client) ListQueryNode(ctx context.Context, req *querypb.ListQueryNodeRequest, opts ...grpc.CallOption) (*querypb.ListQueryNodeResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*querypb.ListQueryNodeResponse, error) {
		return client.ListQueryNode(ctx, req)
	})
}

func (c *Client) GetQueryNodeDistribution(ctx context.Context, req *querypb.GetQueryNodeDistributionRequest, opts ...grpc.CallOption) (*querypb.GetQueryNodeDistributionResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*querypb.GetQueryNodeDistributionResponse, error) {
		return client.GetQueryNodeDistribution(ctx, req)
	})
}

func (c *Client) SuspendBalance(ctx context.Context, req *querypb.SuspendBalanceRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.SuspendBalance(ctx, req)
	})
}

func (c *Client) ResumeBalance(ctx context.Context, req *querypb.ResumeBalanceRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.ResumeBalance(ctx, req)
	})
}

func (c *Client) CheckBalanceStatus(ctx context.Context, req *querypb.CheckBalanceStatusRequest, opts ...grpc.CallOption) (*querypb.CheckBalanceStatusResponse, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*querypb.CheckBalanceStatusResponse, error) {
		return client.CheckBalanceStatus(ctx, req)
	})
}

func (c *Client) SuspendNode(ctx context.Context, req *querypb.SuspendNodeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.SuspendNode(ctx, req)
	})
}

func (c *Client) ResumeNode(ctx context.Context, req *querypb.ResumeNodeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.ResumeNode(ctx, req)
	})
}

func (c *Client) TransferSegment(ctx context.Context, req *querypb.TransferSegmentRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.TransferSegment(ctx, req)
	})
}

func (c *Client) TransferChannel(ctx context.Context, req *querypb.TransferChannelRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.TransferChannel(ctx, req)
	})
}

func (c *Client) CheckQueryNodeDistribution(ctx context.Context, req *querypb.CheckQueryNodeDistributionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.CheckQueryNodeDistribution(ctx, req)
	})
}

func (c *Client) UpdateLoadConfig(ctx context.Context, req *querypb.UpdateLoadConfigRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
	)
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*commonpb.Status, error) {
		return client.UpdateLoadConfig(ctx, req)
	})
}
