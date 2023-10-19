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
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type RcWrapperSuite struct {
	suite.Suite

	rc     *mocks.RootCoord
	client types.RootCoordClient
}

func (s *RcWrapperSuite) SetupTest() {
	s.rc = mocks.NewRootCoord(s.T())
	s.client = WrapRootCoordServerAsClient(s.rc)
}

func (s *RcWrapperSuite) TearDownTest() {
	s.client = nil
	s.rc = nil
}

func (s *RcWrapperSuite) TestGetComponentStates() {
	s.rc.EXPECT().GetComponentStates(mock.Anything, mock.Anything).
		Return(&milvuspb.ComponentStates{Status: merr.Status(nil)}, nil)

	resp, err := s.client.GetComponentStates(context.Background(), &milvuspb.GetComponentStatesRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestGetTimeTickChannel() {
	s.rc.EXPECT().GetTimeTickChannel(mock.Anything, mock.Anything).
		Return(&milvuspb.StringResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.GetTimeTickChannel(context.Background(), &internalpb.GetTimeTickChannelRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestGetStatisticsChannel() {
	s.rc.EXPECT().GetStatisticsChannel(mock.Anything, mock.Anything).
		Return(&milvuspb.StringResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.GetStatisticsChannel(context.Background(), &internalpb.GetStatisticsChannelRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestCreateCollection() {
	s.rc.EXPECT().CreateCollection(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.CreateCollection(context.Background(), &milvuspb.CreateCollectionRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestDropCollection() {
	s.rc.EXPECT().DropCollection(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.DropCollection(context.Background(), &milvuspb.DropCollectionRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestHasCollection() {
	s.rc.EXPECT().HasCollection(mock.Anything, mock.Anything).
		Return(&milvuspb.BoolResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.HasCollection(context.Background(), &milvuspb.HasCollectionRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestDescribeCollection() {
	s.rc.EXPECT().DescribeCollection(mock.Anything, mock.Anything).
		Return(&milvuspb.DescribeCollectionResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.DescribeCollection(context.Background(), &milvuspb.DescribeCollectionRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestDescribeCollectionInternal() {
	s.rc.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).
		Return(&milvuspb.DescribeCollectionResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.DescribeCollectionInternal(context.Background(), &milvuspb.DescribeCollectionRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestCreateAlias() {
	s.rc.EXPECT().CreateAlias(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.CreateAlias(context.Background(), &milvuspb.CreateAliasRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestDropAlias() {
	s.rc.EXPECT().DropAlias(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.DropAlias(context.Background(), &milvuspb.DropAliasRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestAlterAlias() {
	s.rc.EXPECT().AlterAlias(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.AlterAlias(context.Background(), &milvuspb.AlterAliasRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestShowCollections() {
	s.rc.EXPECT().ShowCollections(mock.Anything, mock.Anything).
		Return(&milvuspb.ShowCollectionsResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.ShowCollections(context.Background(), &milvuspb.ShowCollectionsRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestAlterCollection() {
	s.rc.EXPECT().AlterCollection(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.AlterCollection(context.Background(), &milvuspb.AlterCollectionRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestCreatePartition() {
	s.rc.EXPECT().CreatePartition(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.CreatePartition(context.Background(), &milvuspb.CreatePartitionRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestDropPartition() {
	s.rc.EXPECT().DropPartition(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.DropPartition(context.Background(), &milvuspb.DropPartitionRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestHasPartition() {
	s.rc.EXPECT().HasPartition(mock.Anything, mock.Anything).
		Return(&milvuspb.BoolResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.HasPartition(context.Background(), &milvuspb.HasPartitionRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestShowPartitions() {
	s.rc.EXPECT().ShowPartitions(mock.Anything, mock.Anything).
		Return(&milvuspb.ShowPartitionsResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.ShowPartitions(context.Background(), &milvuspb.ShowPartitionsRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestShowPartitionsInternal() {
	s.rc.EXPECT().ShowPartitionsInternal(mock.Anything, mock.Anything).
		Return(&milvuspb.ShowPartitionsResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.ShowPartitionsInternal(context.Background(), &milvuspb.ShowPartitionsRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestShowSegments() {
	s.rc.EXPECT().ShowSegments(mock.Anything, mock.Anything).
		Return(&milvuspb.ShowSegmentsResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.ShowSegments(context.Background(), &milvuspb.ShowSegmentsRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestAllocTimestamp() {
	s.rc.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).
		Return(&rootcoordpb.AllocTimestampResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.AllocTimestamp(context.Background(), &rootcoordpb.AllocTimestampRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestAllocID() {
	s.rc.EXPECT().AllocID(mock.Anything, mock.Anything).
		Return(&rootcoordpb.AllocIDResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.AllocID(context.Background(), &rootcoordpb.AllocIDRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestUpdateChannelTimeTick() {
	s.rc.EXPECT().UpdateChannelTimeTick(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.UpdateChannelTimeTick(context.Background(), &internalpb.ChannelTimeTickMsg{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestInvalidateCollectionMetaCache() {
	s.rc.EXPECT().InvalidateCollectionMetaCache(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.InvalidateCollectionMetaCache(context.Background(), &proxypb.InvalidateCollMetaCacheRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestShowConfigurations() {
	s.rc.EXPECT().ShowConfigurations(mock.Anything, mock.Anything).
		Return(&internalpb.ShowConfigurationsResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.ShowConfigurations(context.Background(), &internalpb.ShowConfigurationsRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestGetMetrics() {
	s.rc.EXPECT().GetMetrics(mock.Anything, mock.Anything).
		Return(&milvuspb.GetMetricsResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.GetMetrics(context.Background(), &milvuspb.GetMetricsRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestImport() {
	s.rc.EXPECT().Import(mock.Anything, mock.Anything).
		Return(&milvuspb.ImportResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.Import(context.Background(), &milvuspb.ImportRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestGetImportState() {
	s.rc.EXPECT().GetImportState(mock.Anything, mock.Anything).
		Return(&milvuspb.GetImportStateResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.GetImportState(context.Background(), &milvuspb.GetImportStateRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestListImportTasks() {
	s.rc.EXPECT().ListImportTasks(mock.Anything, mock.Anything).
		Return(&milvuspb.ListImportTasksResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.ListImportTasks(context.Background(), &milvuspb.ListImportTasksRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestReportImport() {
	s.rc.EXPECT().ReportImport(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.ReportImport(context.Background(), &rootcoordpb.ImportResult{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestCreateCredential() {
	s.rc.EXPECT().CreateCredential(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.CreateCredential(context.Background(), &internalpb.CredentialInfo{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestUpdateCredential() {
	s.rc.EXPECT().UpdateCredential(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.UpdateCredential(context.Background(), &internalpb.CredentialInfo{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestDeleteCredential() {
	s.rc.EXPECT().DeleteCredential(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.DeleteCredential(context.Background(), &milvuspb.DeleteCredentialRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestListCredUsers() {
	s.rc.EXPECT().ListCredUsers(mock.Anything, mock.Anything).
		Return(&milvuspb.ListCredUsersResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.ListCredUsers(context.Background(), &milvuspb.ListCredUsersRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestGetCredential() {
	s.rc.EXPECT().GetCredential(mock.Anything, mock.Anything).
		Return(&rootcoordpb.GetCredentialResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.GetCredential(context.Background(), &rootcoordpb.GetCredentialRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestCreateRole() {
	s.rc.EXPECT().CreateRole(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.CreateRole(context.Background(), &milvuspb.CreateRoleRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestDropRole() {
	s.rc.EXPECT().DropRole(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.DropRole(context.Background(), &milvuspb.DropRoleRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestOperateUserRole() {
	s.rc.EXPECT().OperateUserRole(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.OperateUserRole(context.Background(), &milvuspb.OperateUserRoleRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestSelectRole() {
	s.rc.EXPECT().SelectRole(mock.Anything, mock.Anything).
		Return(&milvuspb.SelectRoleResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.SelectRole(context.Background(), &milvuspb.SelectRoleRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestSelectUser() {
	s.rc.EXPECT().SelectUser(mock.Anything, mock.Anything).
		Return(&milvuspb.SelectUserResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.SelectUser(context.Background(), &milvuspb.SelectUserRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestOperatePrivilege() {
	s.rc.EXPECT().OperatePrivilege(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.OperatePrivilege(context.Background(), &milvuspb.OperatePrivilegeRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestSelectGrant() {
	s.rc.EXPECT().SelectGrant(mock.Anything, mock.Anything).
		Return(&milvuspb.SelectGrantResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.SelectGrant(context.Background(), &milvuspb.SelectGrantRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestListPolicy() {
	s.rc.EXPECT().ListPolicy(mock.Anything, mock.Anything).
		Return(&internalpb.ListPolicyResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.ListPolicy(context.Background(), &internalpb.ListPolicyRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestCheckHealth() {
	s.rc.EXPECT().CheckHealth(mock.Anything, mock.Anything).
		Return(&milvuspb.CheckHealthResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.CheckHealth(context.Background(), &milvuspb.CheckHealthRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestRenameCollection() {
	s.rc.EXPECT().RenameCollection(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.RenameCollection(context.Background(), &milvuspb.RenameCollectionRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestCreateDatabase() {
	s.rc.EXPECT().CreateDatabase(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.CreateDatabase(context.Background(), &milvuspb.CreateDatabaseRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestDropDatabase() {
	s.rc.EXPECT().DropDatabase(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.DropDatabase(context.Background(), &milvuspb.DropDatabaseRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *RcWrapperSuite) TestListDatabases() {
	s.rc.EXPECT().ListDatabases(mock.Anything, mock.Anything).
		Return(&milvuspb.ListDatabasesResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.ListDatabases(context.Background(), &milvuspb.ListDatabasesRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func TestRcServerWrapper(t *testing.T) {
	suite.Run(t, new(RcWrapperSuite))
}
