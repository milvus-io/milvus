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

package mock

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/uniquegenerator"
)

var _ rootcoordpb.RootCoordClient = &GrpcRootCoordClient{}

type GrpcRootCoordClient struct {
	Err error
}

func (m *GrpcRootCoordClient) DescribeDatabase(ctx context.Context, in *rootcoordpb.DescribeDatabaseRequest, opts ...grpc.CallOption) (*rootcoordpb.DescribeDatabaseResponse, error) {
	return &rootcoordpb.DescribeDatabaseResponse{}, m.Err
}

func (m *GrpcRootCoordClient) CreateDatabase(ctx context.Context, in *milvuspb.CreateDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) DropDatabase(ctx context.Context, in *milvuspb.DropDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) ListDatabases(ctx context.Context, in *milvuspb.ListDatabasesRequest, opts ...grpc.CallOption) (*milvuspb.ListDatabasesResponse, error) {
	return &milvuspb.ListDatabasesResponse{}, m.Err
}

func (m *GrpcRootCoordClient) RenameCollection(ctx context.Context, in *milvuspb.RenameCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (m *GrpcRootCoordClient) CheckHealth(ctx context.Context, in *milvuspb.CheckHealthRequest, opts ...grpc.CallOption) (*milvuspb.CheckHealthResponse, error) {
	return &milvuspb.CheckHealthResponse{}, m.Err
}

func (m *GrpcRootCoordClient) CreateRole(ctx context.Context, in *milvuspb.CreateRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) DropRole(ctx context.Context, in *milvuspb.DropRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) OperateUserRole(ctx context.Context, in *milvuspb.OperateUserRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) SelectRole(ctx context.Context, in *milvuspb.SelectRoleRequest, opts ...grpc.CallOption) (*milvuspb.SelectRoleResponse, error) {
	return &milvuspb.SelectRoleResponse{}, m.Err
}

func (m *GrpcRootCoordClient) SelectUser(ctx context.Context, in *milvuspb.SelectUserRequest, opts ...grpc.CallOption) (*milvuspb.SelectUserResponse, error) {
	return &milvuspb.SelectUserResponse{}, m.Err
}

func (m *GrpcRootCoordClient) OperatePrivilege(ctx context.Context, in *milvuspb.OperatePrivilegeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) SelectGrant(ctx context.Context, in *milvuspb.SelectGrantRequest, opts ...grpc.CallOption) (*milvuspb.SelectGrantResponse, error) {
	return &milvuspb.SelectGrantResponse{}, m.Err
}

func (m *GrpcRootCoordClient) ListPolicy(ctx context.Context, in *internalpb.ListPolicyRequest, opts ...grpc.CallOption) (*internalpb.ListPolicyResponse, error) {
	return &internalpb.ListPolicyResponse{}, m.Err
}

func (m *GrpcRootCoordClient) GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			NodeID:    int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
			Role:      "MockRootCoord",
			StateCode: commonpb.StateCode_Healthy,
			ExtraInfo: nil,
		},
		SubcomponentStates: nil,
		Status:             merr.Success(),
	}, m.Err
}

func (m *GrpcRootCoordClient) GetTimeTickChannel(ctx context.Context, in *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.Err
}

func (m *GrpcRootCoordClient) GetStatisticsChannel(ctx context.Context, in *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.Err
}

func (m *GrpcRootCoordClient) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest, opts ...grpc.CallOption) (*milvuspb.BoolResponse, error) {
	return &milvuspb.BoolResponse{}, m.Err
}

func (m *GrpcRootCoordClient) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	return &milvuspb.DescribeCollectionResponse{}, m.Err
}

func (m *GrpcRootCoordClient) DescribeCollectionInternal(ctx context.Context, in *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	return &milvuspb.DescribeCollectionResponse{}, m.Err
}

func (m *GrpcRootCoordClient) CreateAlias(ctx context.Context, in *milvuspb.CreateAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) DropAlias(ctx context.Context, in *milvuspb.DropAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) AlterAlias(ctx context.Context, in *milvuspb.AlterAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) DescribeAlias(ctx context.Context, in *milvuspb.DescribeAliasRequest, opts ...grpc.CallOption) (*milvuspb.DescribeAliasResponse, error) {
	return &milvuspb.DescribeAliasResponse{}, m.Err
}

func (m *GrpcRootCoordClient) ListAliases(ctx context.Context, in *milvuspb.ListAliasesRequest, opts ...grpc.CallOption) (*milvuspb.ListAliasesResponse, error) {
	return &milvuspb.ListAliasesResponse{}, m.Err
}

func (m *GrpcRootCoordClient) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowCollectionsResponse, error) {
	return &milvuspb.ShowCollectionsResponse{}, m.Err
}

func (m *GrpcRootCoordClient) ShowCollectionIDs(ctx context.Context, in *rootcoordpb.ShowCollectionIDsRequest, opts ...grpc.CallOption) (*rootcoordpb.ShowCollectionIDsResponse, error) {
	return &rootcoordpb.ShowCollectionIDsResponse{}, m.Err
}

func (m *GrpcRootCoordClient) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest, opts ...grpc.CallOption) (*milvuspb.BoolResponse, error) {
	return &milvuspb.BoolResponse{}, m.Err
}

func (m *GrpcRootCoordClient) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
	return &milvuspb.ShowPartitionsResponse{}, m.Err
}

func (m *GrpcRootCoordClient) ShowPartitionsInternal(ctx context.Context, in *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
	return &milvuspb.ShowPartitionsResponse{}, m.Err
}

func (m *GrpcRootCoordClient) DescribeSegment(ctx context.Context, in *milvuspb.DescribeSegmentRequest, opts ...grpc.CallOption) (*milvuspb.DescribeSegmentResponse, error) {
	return &milvuspb.DescribeSegmentResponse{}, m.Err
}

func (m *GrpcRootCoordClient) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest, opts ...grpc.CallOption) (*milvuspb.ShowSegmentsResponse, error) {
	return &milvuspb.ShowSegmentsResponse{}, m.Err
}

func (m *GrpcRootCoordClient) GetPChannelInfo(ctx context.Context, in *rootcoordpb.GetPChannelInfoRequest, opts ...grpc.CallOption) (*rootcoordpb.GetPChannelInfoResponse, error) {
	return &rootcoordpb.GetPChannelInfoResponse{}, m.Err
}

func (m *GrpcRootCoordClient) DescribeSegments(ctx context.Context, in *rootcoordpb.DescribeSegmentsRequest, opts ...grpc.CallOption) (*rootcoordpb.DescribeSegmentsResponse, error) {
	return &rootcoordpb.DescribeSegmentsResponse{}, m.Err
}

//func (m *GrpcRootCoordClient) CreateIndex(ctx context.Context, in *milvuspb.CreateIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
//	return &commonpb.Status{}, m.Err
//}
//
//func (m *GrpcRootCoordClient) DescribeIndex(ctx context.Context, in *milvuspb.DescribeIndexRequest, opts ...grpc.CallOption) (*milvuspb.DescribeIndexResponse, error) {
//	return &milvuspb.DescribeIndexResponse{}, m.Err
//}
//
//func (m *GrpcRootCoordClient) GetIndexState(ctx context.Context, in *milvuspb.GetIndexStateRequest, opt ...grpc.CallOption) (*indexpb.GetIndexStatesResponse, error) {
//	return &indexpb.GetIndexStatesResponse{}, m.Err
//}
//
//func (m *GrpcRootCoordClient) DropIndex(ctx context.Context, in *milvuspb.DropIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
//	return &commonpb.Status{}, m.Err
//}

func (m *GrpcRootCoordClient) AllocTimestamp(ctx context.Context, in *rootcoordpb.AllocTimestampRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
	return &rootcoordpb.AllocTimestampResponse{}, m.Err
}

func (m *GrpcRootCoordClient) AllocID(ctx context.Context, in *rootcoordpb.AllocIDRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocIDResponse, error) {
	return &rootcoordpb.AllocIDResponse{}, m.Err
}

func (m *GrpcRootCoordClient) UpdateChannelTimeTick(ctx context.Context, in *internalpb.ChannelTimeTickMsg, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) InvalidateCollectionMetaCache(ctx context.Context, in *proxypb.InvalidateCollMetaCacheRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) SegmentFlushCompleted(ctx context.Context, in *datapb.SegmentFlushCompletedMsg, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) ShowConfigurations(ctx context.Context, in *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	return &internalpb.ShowConfigurationsResponse{}, m.Err
}

func (m *GrpcRootCoordClient) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	return &milvuspb.GetMetricsResponse{}, m.Err
}

func (m *GrpcRootCoordClient) CreateCredential(ctx context.Context, in *internalpb.CredentialInfo, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) UpdateCredential(ctx context.Context, in *internalpb.CredentialInfo, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) DeleteCredential(ctx context.Context, in *milvuspb.DeleteCredentialRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) ListCredUsers(ctx context.Context, in *milvuspb.ListCredUsersRequest, opts ...grpc.CallOption) (*milvuspb.ListCredUsersResponse, error) {
	return &milvuspb.ListCredUsersResponse{}, m.Err
}

func (m *GrpcRootCoordClient) GetCredential(ctx context.Context, in *rootcoordpb.GetCredentialRequest, opts ...grpc.CallOption) (*rootcoordpb.GetCredentialResponse, error) {
	return &rootcoordpb.GetCredentialResponse{}, m.Err
}

func (m *GrpcRootCoordClient) AlterCollection(ctx context.Context, in *milvuspb.AlterCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) AlterCollectionField(ctx context.Context, in *milvuspb.AlterCollectionFieldRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) AlterDatabase(ctx context.Context, in *rootcoordpb.AlterDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) BackupRBAC(ctx context.Context, in *milvuspb.BackupRBACMetaRequest, opts ...grpc.CallOption) (*milvuspb.BackupRBACMetaResponse, error) {
	return &milvuspb.BackupRBACMetaResponse{}, m.Err
}

func (m *GrpcRootCoordClient) RestoreRBAC(ctx context.Context, in *milvuspb.RestoreRBACMetaRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) CreatePrivilegeGroup(ctx context.Context, in *milvuspb.CreatePrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) DropPrivilegeGroup(ctx context.Context, in *milvuspb.DropPrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) ListPrivilegeGroups(ctx context.Context, in *milvuspb.ListPrivilegeGroupsRequest, opts ...grpc.CallOption) (*milvuspb.ListPrivilegeGroupsResponse, error) {
	return &milvuspb.ListPrivilegeGroupsResponse{}, m.Err
}

func (m *GrpcRootCoordClient) OperatePrivilegeGroup(ctx context.Context, in *milvuspb.OperatePrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcRootCoordClient) Close() error {
	return nil
}
