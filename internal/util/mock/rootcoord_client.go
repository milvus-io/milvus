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
	"google.golang.org/grpc"

	"context"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
)

type RootCoordClient struct {
	Err error
}

func (m *RootCoordClient) GetComponentStates(ctx context.Context, in *internalpb.GetComponentStatesRequest, opts ...grpc.CallOption) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{}, m.Err
}
func (m *RootCoordClient) GetTimeTickChannel(ctx context.Context, in *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.Err
}
func (m *RootCoordClient) GetStatisticsChannel(ctx context.Context, in *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.Err
}

func (m *RootCoordClient) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *RootCoordClient) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *RootCoordClient) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest, opts ...grpc.CallOption) (*milvuspb.BoolResponse, error) {
	return &milvuspb.BoolResponse{}, m.Err
}

func (m *RootCoordClient) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	return &milvuspb.DescribeCollectionResponse{}, m.Err
}

func (m *RootCoordClient) CreateAlias(ctx context.Context, in *milvuspb.CreateAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *RootCoordClient) DropAlias(ctx context.Context, in *milvuspb.DropAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *RootCoordClient) AlterAlias(ctx context.Context, in *milvuspb.AlterAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *RootCoordClient) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowCollectionsResponse, error) {
	return &milvuspb.ShowCollectionsResponse{}, m.Err
}

func (m *RootCoordClient) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *RootCoordClient) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *RootCoordClient) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest, opts ...grpc.CallOption) (*milvuspb.BoolResponse, error) {
	return &milvuspb.BoolResponse{}, m.Err
}

func (m *RootCoordClient) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
	return &milvuspb.ShowPartitionsResponse{}, m.Err
}

func (m *RootCoordClient) DescribeSegment(ctx context.Context, in *milvuspb.DescribeSegmentRequest, opts ...grpc.CallOption) (*milvuspb.DescribeSegmentResponse, error) {
	return &milvuspb.DescribeSegmentResponse{}, m.Err
}

func (m *RootCoordClient) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest, opts ...grpc.CallOption) (*milvuspb.ShowSegmentsResponse, error) {
	return &milvuspb.ShowSegmentsResponse{}, m.Err
}

func (m *RootCoordClient) DescribeSegments(ctx context.Context, in *rootcoordpb.DescribeSegmentsRequest, opts ...grpc.CallOption) (*rootcoordpb.DescribeSegmentsResponse, error) {
	return &rootcoordpb.DescribeSegmentsResponse{}, m.Err
}

func (m *RootCoordClient) CreateIndex(ctx context.Context, in *milvuspb.CreateIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *RootCoordClient) DescribeIndex(ctx context.Context, in *milvuspb.DescribeIndexRequest, opts ...grpc.CallOption) (*milvuspb.DescribeIndexResponse, error) {
	return &milvuspb.DescribeIndexResponse{}, m.Err
}

func (m *RootCoordClient) DropIndex(ctx context.Context, in *milvuspb.DropIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *RootCoordClient) AllocTimestamp(ctx context.Context, in *rootcoordpb.AllocTimestampRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
	return &rootcoordpb.AllocTimestampResponse{}, m.Err
}

func (m *RootCoordClient) AllocID(ctx context.Context, in *rootcoordpb.AllocIDRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocIDResponse, error) {
	return &rootcoordpb.AllocIDResponse{}, m.Err
}

func (m *RootCoordClient) UpdateChannelTimeTick(ctx context.Context, in *internalpb.ChannelTimeTickMsg, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *RootCoordClient) ReleaseDQLMessageStream(ctx context.Context, in *proxypb.ReleaseDQLMessageStreamRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *RootCoordClient) InvalidateCollectionMetaCache(ctx context.Context, in *proxypb.InvalidateCollMetaCacheRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *RootCoordClient) SegmentFlushCompleted(ctx context.Context, in *datapb.SegmentFlushCompletedMsg, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *RootCoordClient) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	return &milvuspb.GetMetricsResponse{}, m.Err
}

func (m *RootCoordClient) Import(ctx context.Context, req *milvuspb.ImportRequest, opts ...grpc.CallOption) (*milvuspb.ImportResponse, error) {
	return &milvuspb.ImportResponse{}, m.Err
}

func (m *RootCoordClient) GetImportState(ctx context.Context, req *milvuspb.GetImportStateRequest, opts ...grpc.CallOption) (*milvuspb.GetImportStateResponse, error) {
	return &milvuspb.GetImportStateResponse{}, m.Err
}

func (m *RootCoordClient) ListImportTasks(ctx context.Context, req *milvuspb.ListImportTasksRequest, opts ...grpc.CallOption) (*milvuspb.ListImportTasksResponse, error) {
	return &milvuspb.ListImportTasksResponse{}, m.Err
}

func (m *RootCoordClient) ReportImport(ctx context.Context, req *rootcoordpb.ImportResult, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *RootCoordClient) CreateCredential(ctx context.Context, in *internalpb.CredentialInfo, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *RootCoordClient) UpdateCredential(ctx context.Context, in *internalpb.CredentialInfo, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *RootCoordClient) DeleteCredential(ctx context.Context, in *milvuspb.DeleteCredentialRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *RootCoordClient) ListCredUsers(ctx context.Context, in *milvuspb.ListCredUsersRequest, opts ...grpc.CallOption) (*milvuspb.ListCredUsersResponse, error) {
	return &milvuspb.ListCredUsersResponse{}, m.Err
}

func (m *RootCoordClient) GetCredential(ctx context.Context, in *rootcoordpb.GetCredentialRequest, opts ...grpc.CallOption) (*rootcoordpb.GetCredentialResponse, error) {
	return &rootcoordpb.GetCredentialResponse{}, m.Err
}

func (m *RootCoordClient) CreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *RootCoordClient) DropRole(ctx context.Context, req *milvuspb.DropRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *RootCoordClient) OperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *RootCoordClient) SelectRole(ctx context.Context, req *milvuspb.SelectRoleRequest, opts ...grpc.CallOption) (*milvuspb.SelectRoleResponse, error) {
	return &milvuspb.SelectRoleResponse{}, m.Err
}

func (m *RootCoordClient) SelectUser(ctx context.Context, req *milvuspb.SelectUserRequest, opts ...grpc.CallOption) (*milvuspb.SelectUserResponse, error) {
	return &milvuspb.SelectUserResponse{}, m.Err
}

func (m *RootCoordClient) SelectResource(ctx context.Context, req *milvuspb.SelectResourceRequest, opts ...grpc.CallOption) (*milvuspb.SelectResourceResponse, error) {
	return &milvuspb.SelectResourceResponse{}, m.Err
}

func (m *RootCoordClient) OperatePrivilege(ctx context.Context, req *milvuspb.OperatePrivilegeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *RootCoordClient) SelectGrant(ctx context.Context, req *milvuspb.SelectGrantRequest, opts ...grpc.CallOption) (*milvuspb.SelectGrantResponse, error) {
	return &milvuspb.SelectGrantResponse{}, m.Err
}

func (m *RootCoordClient) ListPolicy(ctx context.Context, req *internalpb.ListPolicyRequest, opts ...grpc.CallOption) (*internalpb.ListPolicyResponse, error) {
	return &internalpb.ListPolicyResponse{}, m.Err
}
