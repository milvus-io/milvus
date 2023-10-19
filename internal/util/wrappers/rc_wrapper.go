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

package wrappers

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/types"
)

var _ types.RootCoordClient = (*rcServerWrapper)(nil)

type rcServerWrapper struct {
	types.RootCoord
}

func (rc *rcServerWrapper) Close() error {
	return nil
}

func (rc *rcServerWrapper) GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return rc.RootCoord.GetComponentStates(ctx, in)
}

func (rc *rcServerWrapper) GetTimeTickChannel(ctx context.Context, in *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return rc.RootCoord.GetTimeTickChannel(ctx, in)
}

func (rc *rcServerWrapper) GetStatisticsChannel(ctx context.Context, in *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return rc.RootCoord.GetStatisticsChannel(ctx, in)
}

func (rc *rcServerWrapper) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.RootCoord.CreateCollection(ctx, in)
}

func (rc *rcServerWrapper) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.RootCoord.DropCollection(ctx, in)
}

func (rc *rcServerWrapper) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest, opts ...grpc.CallOption) (*milvuspb.BoolResponse, error) {
	return rc.RootCoord.HasCollection(ctx, in)
}

func (rc *rcServerWrapper) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	return rc.RootCoord.DescribeCollection(ctx, in)
}

func (rc *rcServerWrapper) DescribeCollectionInternal(ctx context.Context, in *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	return rc.RootCoord.DescribeCollectionInternal(ctx, in)
}

func (rc *rcServerWrapper) CreateAlias(ctx context.Context, in *milvuspb.CreateAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.RootCoord.CreateAlias(ctx, in)
}

func (rc *rcServerWrapper) DropAlias(ctx context.Context, in *milvuspb.DropAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.RootCoord.DropAlias(ctx, in)
}

func (rc *rcServerWrapper) AlterAlias(ctx context.Context, in *milvuspb.AlterAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.RootCoord.AlterAlias(ctx, in)
}

func (rc *rcServerWrapper) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowCollectionsResponse, error) {
	return rc.RootCoord.ShowCollections(ctx, in)
}

func (rc *rcServerWrapper) AlterCollection(ctx context.Context, in *milvuspb.AlterCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.RootCoord.AlterCollection(ctx, in)
}

func (rc *rcServerWrapper) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.RootCoord.CreatePartition(ctx, in)
}

func (rc *rcServerWrapper) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.RootCoord.DropPartition(ctx, in)
}

func (rc *rcServerWrapper) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest, opts ...grpc.CallOption) (*milvuspb.BoolResponse, error) {
	return rc.RootCoord.HasPartition(ctx, in)
}

func (rc *rcServerWrapper) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
	return rc.RootCoord.ShowPartitions(ctx, in)
}

func (rc *rcServerWrapper) ShowPartitionsInternal(ctx context.Context, in *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
	return rc.RootCoord.ShowPartitionsInternal(ctx, in)
}

func (rc *rcServerWrapper) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest, opts ...grpc.CallOption) (*milvuspb.ShowSegmentsResponse, error) {
	return rc.RootCoord.ShowSegments(ctx, in)
}

func (rc *rcServerWrapper) AllocTimestamp(ctx context.Context, in *rootcoordpb.AllocTimestampRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
	return rc.RootCoord.AllocTimestamp(ctx, in)
}

func (rc *rcServerWrapper) AllocID(ctx context.Context, in *rootcoordpb.AllocIDRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocIDResponse, error) {
	return rc.RootCoord.AllocID(ctx, in)
}

func (rc *rcServerWrapper) UpdateChannelTimeTick(ctx context.Context, in *internalpb.ChannelTimeTickMsg, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.RootCoord.UpdateChannelTimeTick(ctx, in)
}

func (rc *rcServerWrapper) InvalidateCollectionMetaCache(ctx context.Context, in *proxypb.InvalidateCollMetaCacheRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.RootCoord.InvalidateCollectionMetaCache(ctx, in)
}

func (rc *rcServerWrapper) ShowConfigurations(ctx context.Context, in *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	return rc.RootCoord.ShowConfigurations(ctx, in)
}

func (rc *rcServerWrapper) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	return rc.RootCoord.GetMetrics(ctx, in)
}

func (rc *rcServerWrapper) Import(ctx context.Context, in *milvuspb.ImportRequest, opts ...grpc.CallOption) (*milvuspb.ImportResponse, error) {
	return rc.RootCoord.Import(ctx, in)
}

func (rc *rcServerWrapper) GetImportState(ctx context.Context, in *milvuspb.GetImportStateRequest, opts ...grpc.CallOption) (*milvuspb.GetImportStateResponse, error) {
	return rc.RootCoord.GetImportState(ctx, in)
}

func (rc *rcServerWrapper) ListImportTasks(ctx context.Context, in *milvuspb.ListImportTasksRequest, opts ...grpc.CallOption) (*milvuspb.ListImportTasksResponse, error) {
	return rc.RootCoord.ListImportTasks(ctx, in)
}

func (rc *rcServerWrapper) ReportImport(ctx context.Context, in *rootcoordpb.ImportResult, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.RootCoord.ReportImport(ctx, in)
}

func (rc *rcServerWrapper) CreateCredential(ctx context.Context, in *internalpb.CredentialInfo, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.RootCoord.CreateCredential(ctx, in)
}

func (rc *rcServerWrapper) UpdateCredential(ctx context.Context, in *internalpb.CredentialInfo, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.RootCoord.UpdateCredential(ctx, in)
}

func (rc *rcServerWrapper) DeleteCredential(ctx context.Context, in *milvuspb.DeleteCredentialRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.RootCoord.DeleteCredential(ctx, in)
}

func (rc *rcServerWrapper) ListCredUsers(ctx context.Context, in *milvuspb.ListCredUsersRequest, opts ...grpc.CallOption) (*milvuspb.ListCredUsersResponse, error) {
	return rc.RootCoord.ListCredUsers(ctx, in)
}

func (rc *rcServerWrapper) GetCredential(ctx context.Context, in *rootcoordpb.GetCredentialRequest, opts ...grpc.CallOption) (*rootcoordpb.GetCredentialResponse, error) {
	return rc.RootCoord.GetCredential(ctx, in)
}

func (rc *rcServerWrapper) CreateRole(ctx context.Context, in *milvuspb.CreateRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.RootCoord.CreateRole(ctx, in)
}

func (rc *rcServerWrapper) DropRole(ctx context.Context, in *milvuspb.DropRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.RootCoord.DropRole(ctx, in)
}

func (rc *rcServerWrapper) OperateUserRole(ctx context.Context, in *milvuspb.OperateUserRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.RootCoord.OperateUserRole(ctx, in)
}

func (rc *rcServerWrapper) SelectRole(ctx context.Context, in *milvuspb.SelectRoleRequest, opts ...grpc.CallOption) (*milvuspb.SelectRoleResponse, error) {
	return rc.RootCoord.SelectRole(ctx, in)
}

func (rc *rcServerWrapper) SelectUser(ctx context.Context, in *milvuspb.SelectUserRequest, opts ...grpc.CallOption) (*milvuspb.SelectUserResponse, error) {
	return rc.RootCoord.SelectUser(ctx, in)
}

func (rc *rcServerWrapper) OperatePrivilege(ctx context.Context, in *milvuspb.OperatePrivilegeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.RootCoord.OperatePrivilege(ctx, in)
}

func (rc *rcServerWrapper) SelectGrant(ctx context.Context, in *milvuspb.SelectGrantRequest, opts ...grpc.CallOption) (*milvuspb.SelectGrantResponse, error) {
	return rc.RootCoord.SelectGrant(ctx, in)
}

func (rc *rcServerWrapper) ListPolicy(ctx context.Context, in *internalpb.ListPolicyRequest, opts ...grpc.CallOption) (*internalpb.ListPolicyResponse, error) {
	return rc.RootCoord.ListPolicy(ctx, in)
}

func (rc *rcServerWrapper) CheckHealth(ctx context.Context, in *milvuspb.CheckHealthRequest, opts ...grpc.CallOption) (*milvuspb.CheckHealthResponse, error) {
	return rc.RootCoord.CheckHealth(ctx, in)
}

func (rc *rcServerWrapper) RenameCollection(ctx context.Context, in *milvuspb.RenameCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.RootCoord.RenameCollection(ctx, in)
}

func (rc *rcServerWrapper) CreateDatabase(ctx context.Context, in *milvuspb.CreateDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.RootCoord.CreateDatabase(ctx, in)
}

func (rc *rcServerWrapper) DropDatabase(ctx context.Context, in *milvuspb.DropDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return rc.RootCoord.DropDatabase(ctx, in)
}

func (rc *rcServerWrapper) ListDatabases(ctx context.Context, in *milvuspb.ListDatabasesRequest, opts ...grpc.CallOption) (*milvuspb.ListDatabasesResponse, error) {
	return rc.RootCoord.ListDatabases(ctx, in)
}

func WrapRootCoordServerAsClient(rc types.RootCoord) types.RootCoordClient {
	return &rcServerWrapper{
		RootCoord: rc,
	}
}
