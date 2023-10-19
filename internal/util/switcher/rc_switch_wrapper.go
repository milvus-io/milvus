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

package switchers

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	rcc "github.com/milvus-io/milvus/internal/distributed/rootcoord/client"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/registry"
	"github.com/milvus-io/milvus/internal/types"
)

type rcSwitchWrapper struct {
	*CoordWrapper[types.RootCoordClient]
}

func (rc *rcSwitchWrapper) Close() error {
	return rc.CoordWrapper.Close()
}

func (rc *rcSwitchWrapper) GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return rc.GetCurrent().GetComponentStates(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) GetTimeTickChannel(ctx context.Context, in *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return rc.GetCurrent().GetTimeTickChannel(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) GetStatisticsChannel(ctx context.Context, in *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return rc.GetCurrent().GetStatisticsChannel(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.GetCurrent().CreateCollection(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.GetCurrent().DropCollection(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest, opts ...grpc.CallOption) (*milvuspb.BoolResponse, error) {
	return rc.GetCurrent().HasCollection(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	return rc.GetCurrent().DescribeCollection(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) DescribeCollectionInternal(ctx context.Context, in *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	return rc.GetCurrent().DescribeCollectionInternal(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) CreateAlias(ctx context.Context, in *milvuspb.CreateAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.GetCurrent().CreateAlias(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) DropAlias(ctx context.Context, in *milvuspb.DropAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.GetCurrent().DropAlias(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) AlterAlias(ctx context.Context, in *milvuspb.AlterAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.GetCurrent().AlterAlias(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowCollectionsResponse, error) {
	return rc.GetCurrent().ShowCollections(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) AlterCollection(ctx context.Context, in *milvuspb.AlterCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.GetCurrent().AlterCollection(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.GetCurrent().CreatePartition(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.GetCurrent().DropPartition(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest, opts ...grpc.CallOption) (*milvuspb.BoolResponse, error) {
	return rc.GetCurrent().HasPartition(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
	return rc.GetCurrent().ShowPartitions(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) ShowPartitionsInternal(ctx context.Context, in *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
	return rc.GetCurrent().ShowPartitionsInternal(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest, opts ...grpc.CallOption) (*milvuspb.ShowSegmentsResponse, error) {
	return rc.GetCurrent().ShowSegments(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) AllocTimestamp(ctx context.Context, in *rootcoordpb.AllocTimestampRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
	return rc.GetCurrent().AllocTimestamp(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) AllocID(ctx context.Context, in *rootcoordpb.AllocIDRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocIDResponse, error) {
	return rc.GetCurrent().AllocID(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) UpdateChannelTimeTick(ctx context.Context, in *internalpb.ChannelTimeTickMsg, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.GetCurrent().UpdateChannelTimeTick(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) InvalidateCollectionMetaCache(ctx context.Context, in *proxypb.InvalidateCollMetaCacheRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.GetCurrent().InvalidateCollectionMetaCache(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) ShowConfigurations(ctx context.Context, in *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	return rc.GetCurrent().ShowConfigurations(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	return rc.GetCurrent().GetMetrics(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) Import(ctx context.Context, in *milvuspb.ImportRequest, opts ...grpc.CallOption) (*milvuspb.ImportResponse, error) {
	return rc.GetCurrent().Import(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) GetImportState(ctx context.Context, in *milvuspb.GetImportStateRequest, opts ...grpc.CallOption) (*milvuspb.GetImportStateResponse, error) {
	return rc.GetCurrent().GetImportState(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) ListImportTasks(ctx context.Context, in *milvuspb.ListImportTasksRequest, opts ...grpc.CallOption) (*milvuspb.ListImportTasksResponse, error) {
	return rc.GetCurrent().ListImportTasks(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) ReportImport(ctx context.Context, in *rootcoordpb.ImportResult, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.GetCurrent().ReportImport(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) CreateCredential(ctx context.Context, in *internalpb.CredentialInfo, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.GetCurrent().CreateCredential(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) UpdateCredential(ctx context.Context, in *internalpb.CredentialInfo, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.GetCurrent().UpdateCredential(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) DeleteCredential(ctx context.Context, in *milvuspb.DeleteCredentialRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.GetCurrent().DeleteCredential(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) ListCredUsers(ctx context.Context, in *milvuspb.ListCredUsersRequest, opts ...grpc.CallOption) (*milvuspb.ListCredUsersResponse, error) {
	return rc.GetCurrent().ListCredUsers(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) GetCredential(ctx context.Context, in *rootcoordpb.GetCredentialRequest, opts ...grpc.CallOption) (*rootcoordpb.GetCredentialResponse, error) {
	return rc.GetCurrent().GetCredential(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) CreateRole(ctx context.Context, in *milvuspb.CreateRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.GetCurrent().CreateRole(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) DropRole(ctx context.Context, in *milvuspb.DropRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.GetCurrent().DropRole(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) OperateUserRole(ctx context.Context, in *milvuspb.OperateUserRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.GetCurrent().OperateUserRole(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) SelectRole(ctx context.Context, in *milvuspb.SelectRoleRequest, opts ...grpc.CallOption) (*milvuspb.SelectRoleResponse, error) {
	return rc.GetCurrent().SelectRole(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) SelectUser(ctx context.Context, in *milvuspb.SelectUserRequest, opts ...grpc.CallOption) (*milvuspb.SelectUserResponse, error) {
	return rc.GetCurrent().SelectUser(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) OperatePrivilege(ctx context.Context, in *milvuspb.OperatePrivilegeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.GetCurrent().OperatePrivilege(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) SelectGrant(ctx context.Context, in *milvuspb.SelectGrantRequest, opts ...grpc.CallOption) (*milvuspb.SelectGrantResponse, error) {
	return rc.GetCurrent().SelectGrant(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) ListPolicy(ctx context.Context, in *internalpb.ListPolicyRequest, opts ...grpc.CallOption) (*internalpb.ListPolicyResponse, error) {
	return rc.GetCurrent().ListPolicy(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) CheckHealth(ctx context.Context, in *milvuspb.CheckHealthRequest, opts ...grpc.CallOption) (*milvuspb.CheckHealthResponse, error) {
	return rc.GetCurrent().CheckHealth(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) RenameCollection(ctx context.Context, in *milvuspb.RenameCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.GetCurrent().RenameCollection(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) CreateDatabase(ctx context.Context, in *milvuspb.CreateDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.GetCurrent().CreateDatabase(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) DropDatabase(ctx context.Context, in *milvuspb.DropDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.GetCurrent().DropDatabase(ctx, in, opts...)
}

func (rc *rcSwitchWrapper) ListDatabases(ctx context.Context, in *milvuspb.ListDatabasesRequest, opts ...grpc.CallOption) (*milvuspb.ListDatabasesResponse, error) {
	return rc.GetCurrent().ListDatabases(ctx, in, opts...)
}

func NewRootCoordSwitcher(ctx context.Context, metaRoot string, etcdCli *clientv3.Client) (types.RootCoordClient, error) {
	w, err := NewCoordWrapper[types.RootCoordClient](ctx, "rootcoord", &etcdResolver{cli: etcdCli, metaRoot: metaRoot}, []serviceFactory[types.RootCoordClient]{
		func(ctx context.Context, id int64, addr string) (types.RootCoordClient, error) {
			client, err := registry.GetInMemoryResolver().ResolveRootCoord(ctx, addr, id)
			return client, err
		},
		// fallback to grpcclient
		func(ctx context.Context, id int64, addr string) (types.RootCoordClient, error) {
			return rcc.NewClient(ctx, metaRoot, etcdCli)
		},
	})
	if err != nil {
		return nil, err
	}
	return &rcSwitchWrapper{CoordWrapper: w}, nil
}
