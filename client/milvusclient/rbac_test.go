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

package milvusclient

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type UserSuite struct {
	MockSuiteBase
}

func (s *UserSuite) TestListUsers() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		s.mock.EXPECT().ListCredUsers(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
			return &milvuspb.ListCredUsersResponse{
				Usernames: []string{"user1", "user2"},
			}, nil
		}).Once()

		users, err := s.client.ListUsers(ctx, NewListUserOption())
		s.NoError(err)
		s.Equal([]string{"user1", "user2"}, users)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().ListCredUsers(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.ListUsers(ctx, NewListUserOption())
		s.Error(err)
	})
}

func (s *UserSuite) TestDescribeUser() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	userName := fmt.Sprintf("user_%s", s.randString(5))

	s.Run("success", func() {
		s.mock.EXPECT().SelectUser(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *milvuspb.SelectUserRequest) (*milvuspb.SelectUserResponse, error) {
			s.Equal(userName, r.GetUser().GetName())
			return &milvuspb.SelectUserResponse{
				Results: []*milvuspb.UserResult{
					{
						User: &milvuspb.UserEntity{Name: userName},
						Roles: []*milvuspb.RoleEntity{
							{Name: "role1"},
							{Name: "role2"},
						},
					},
				},
			}, nil
		}).Once()

		user, err := s.client.DescribeUser(ctx, NewDescribeUserOption(userName))
		s.NoError(err)
		s.Equal(userName, user.UserName)
		s.Equal([]string{"role1", "role2"}, user.Roles)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().SelectUser(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.DescribeUser(ctx, NewDescribeUserOption(userName))
		s.Error(err)
	})
}

func (s *UserSuite) TestCreateUser() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		userName := fmt.Sprintf("user_%s", s.randString(5))
		password := s.randString(12)
		s.mock.EXPECT().CreateCredential(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, ccr *milvuspb.CreateCredentialRequest) (*commonpb.Status, error) {
			s.Equal(userName, ccr.GetUsername())
			s.Equal(crypto.Base64Encode(password), ccr.GetPassword())
			return merr.Success(), nil
		}).Once()

		err := s.client.CreateUser(ctx, NewCreateUserOption(userName, password))
		s.NoError(err)
	})
}

func (s *UserSuite) TestUpdatePassword() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		userName := fmt.Sprintf("user_%s", s.randString(5))
		oldPassword := s.randString(12)
		newPassword := s.randString(12)
		s.mock.EXPECT().UpdateCredential(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, ucr *milvuspb.UpdateCredentialRequest) (*commonpb.Status, error) {
			s.Equal(userName, ucr.GetUsername())
			s.Equal(crypto.Base64Encode(oldPassword), ucr.GetOldPassword())
			s.Equal(crypto.Base64Encode(newPassword), ucr.GetNewPassword())
			return merr.Success(), nil
		}).Once()

		err := s.client.UpdatePassword(ctx, NewUpdatePasswordOption(userName, oldPassword, newPassword))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().UpdateCredential(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.UpdatePassword(ctx, NewUpdatePasswordOption("user", "old", "new"))
		s.Error(err)
	})
}

func (s *UserSuite) TestDropUser() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		userName := fmt.Sprintf("user_%s", s.randString(5))
		s.mock.EXPECT().DeleteCredential(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, dcr *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
			s.Equal(userName, dcr.GetUsername())
			return merr.Success(), nil
		}).Once()

		err := s.client.DropUser(ctx, NewDropUserOption(userName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().DeleteCredential(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.DropUser(ctx, NewDropUserOption("user"))
		s.Error(err)
	})
}

func TestUserRBAC(t *testing.T) {
	suite.Run(t, new(UserSuite))
}

type RoleSuite struct {
	MockSuiteBase
}

func (s *RoleSuite) TestListRoles() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		s.mock.EXPECT().SelectRole(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *milvuspb.SelectRoleRequest) (*milvuspb.SelectRoleResponse, error) {
			return &milvuspb.SelectRoleResponse{
				Results: []*milvuspb.RoleResult{
					{Role: &milvuspb.RoleEntity{Name: "role1"}},
					{Role: &milvuspb.RoleEntity{Name: "role2"}},
				},
			}, nil
		}).Once()

		roles, err := s.client.ListRoles(ctx, NewListRoleOption())
		s.NoError(err)
		s.Equal([]string{"role1", "role2"}, roles)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().SelectRole(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.ListRoles(ctx, NewListRoleOption())
		s.Error(err)
	})
}

func (s *RoleSuite) TestCreateRole() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		roleName := fmt.Sprintf("role_%s", s.randString(5))
		s.mock.EXPECT().CreateRole(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *milvuspb.CreateRoleRequest) (*commonpb.Status, error) {
			s.Equal(roleName, r.GetEntity().GetName())
			return merr.Success(), nil
		}).Once()

		err := s.client.CreateRole(ctx, NewCreateRoleOption(roleName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().CreateRole(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.CreateRole(ctx, NewCreateRoleOption("role"))
		s.Error(err)
	})
}

func (s *RoleSuite) TestGrantRole() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		userName := fmt.Sprintf("user_%s", s.randString(5))
		roleName := fmt.Sprintf("role_%s", s.randString(5))
		s.mock.EXPECT().OperateUserRole(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *milvuspb.OperateUserRoleRequest) (*commonpb.Status, error) {
			s.Equal(userName, r.GetUsername())
			s.Equal(roleName, r.GetRoleName())
			return merr.Success(), nil
		}).Once()

		err := s.client.GrantRole(ctx, NewGrantRoleOption(userName, roleName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().OperateUserRole(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.GrantRole(ctx, NewGrantRoleOption("user", "role"))
		s.Error(err)
	})
}

func (s *RoleSuite) TestRevokeRole() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		userName := fmt.Sprintf("user_%s", s.randString(5))
		roleName := fmt.Sprintf("role_%s", s.randString(5))
		s.mock.EXPECT().OperateUserRole(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *milvuspb.OperateUserRoleRequest) (*commonpb.Status, error) {
			s.Equal(userName, r.GetUsername())
			s.Equal(roleName, r.GetRoleName())
			return merr.Success(), nil
		}).Once()

		err := s.client.RevokeRole(ctx, NewRevokeRoleOption(userName, roleName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().OperateUserRole(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.RevokeRole(ctx, NewRevokeRoleOption("user", "role"))
		s.Error(err)
	})
}

func (s *RoleSuite) TestDropRole() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		roleName := fmt.Sprintf("role_%s", s.randString(5))
		s.mock.EXPECT().DropRole(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *milvuspb.DropRoleRequest) (*commonpb.Status, error) {
			s.Equal(roleName, r.GetRoleName())
			return merr.Success(), nil
		}).Once()

		err := s.client.DropRole(ctx, NewDropRoleOption(roleName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().DropRole(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.DropRole(ctx, NewDropRoleOption("role"))
		s.Error(err)
	})
}

func (s *RoleSuite) TestDescribeRole() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		roleName := fmt.Sprintf("role_%s", s.randString(5))
		s.mock.EXPECT().SelectGrant(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *milvuspb.SelectGrantRequest) (*milvuspb.SelectGrantResponse, error) {
			s.Equal(roleName, r.GetEntity().GetRole().GetName())
			return &milvuspb.SelectGrantResponse{
				Entities: []*milvuspb.GrantEntity{
					{
						ObjectName: "*",
						Object: &milvuspb.ObjectEntity{
							Name: "collection",
						},
						Role:    &milvuspb.RoleEntity{Name: roleName},
						Grantor: &milvuspb.GrantorEntity{User: &milvuspb.UserEntity{Name: "admin"}, Privilege: &milvuspb.PrivilegeEntity{Name: "Insert"}},
					},
					{
						ObjectName: "*",
						Object: &milvuspb.ObjectEntity{
							Name: "collection",
						},
						Role:    &milvuspb.RoleEntity{Name: roleName},
						Grantor: &milvuspb.GrantorEntity{User: &milvuspb.UserEntity{Name: "admin"}, Privilege: &milvuspb.PrivilegeEntity{Name: "Query"}},
					},
				},
			}, nil
		}).Once()

		role, err := s.client.DescribeRole(ctx, NewDescribeRoleOption(roleName))
		s.NoError(err)
		s.Equal(roleName, role.RoleName)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().SelectGrant(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.DescribeRole(ctx, NewDescribeRoleOption("role"))
		s.Error(err)
	})
}

func (s *RoleSuite) TestGrantPrivilege() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		roleName := fmt.Sprintf("role_%s", s.randString(5))
		privilegeName := "Insert"
		collectionName := fmt.Sprintf("collection_%s", s.randString(6))

		s.mock.EXPECT().OperatePrivilege(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *milvuspb.OperatePrivilegeRequest) (*commonpb.Status, error) {
			s.Equal(roleName, r.GetEntity().GetRole().GetName())
			s.Equal("collection", r.GetEntity().GetObject().GetName())
			s.Equal(privilegeName, r.GetEntity().GetGrantor().GetPrivilege().GetName())
			s.Equal(collectionName, r.GetEntity().GetObjectName())
			s.Equal(milvuspb.OperatePrivilegeType_Grant, r.GetType())
			return merr.Success(), nil
		}).Once()

		err := s.client.GrantPrivilege(ctx, NewGrantPrivilegeOption(roleName, "collection", privilegeName, collectionName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().OperatePrivilege(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.GrantPrivilege(ctx, NewGrantPrivilegeOption("role", "collection", "privilege", "coll_1"))
		s.Error(err)
	})
}

func (s *RoleSuite) TestRevokePrivilege() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		roleName := fmt.Sprintf("role_%s", s.randString(5))
		privilegeName := "Insert"
		collectionName := fmt.Sprintf("collection_%s", s.randString(6))

		s.mock.EXPECT().OperatePrivilege(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *milvuspb.OperatePrivilegeRequest) (*commonpb.Status, error) {
			s.Equal(roleName, r.GetEntity().GetRole().GetName())
			s.Equal("collection", r.GetEntity().GetObject().GetName())
			s.Equal(privilegeName, r.GetEntity().GetGrantor().GetPrivilege().GetName())
			s.Equal(collectionName, r.GetEntity().GetObjectName())
			s.Equal(milvuspb.OperatePrivilegeType_Revoke, r.GetType())
			return merr.Success(), nil
		}).Once()

		err := s.client.RevokePrivilege(ctx, NewRevokePrivilegeOption(roleName, "collection", privilegeName, collectionName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().OperatePrivilege(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.RevokePrivilege(ctx, NewRevokePrivilegeOption("role", "collection", "privilege", "coll_1"))
		s.Error(err)
	})
}

func TestRoleRBAC(t *testing.T) {
	suite.Run(t, new(RoleSuite))
}

type PrivilegeGroupSuite struct {
	MockSuiteBase
}

func (s *PrivilegeGroupSuite) TestGrantV2() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	roleName := fmt.Sprintf("test_role_%s", s.randString(6))
	privilegeName := "Insert"
	dbName := fmt.Sprintf("test_db_%s", s.randString(6))
	collectionName := fmt.Sprintf("test_collection_%s", s.randString(6))

	s.Run("success", func() {
		s.mock.EXPECT().OperatePrivilegeV2(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *milvuspb.OperatePrivilegeV2Request) (*commonpb.Status, error) {
			s.Equal(roleName, r.GetRole().GetName())
			s.Equal(privilegeName, r.GetGrantor().GetPrivilege().GetName())
			s.Equal(dbName, r.GetDbName())
			s.Equal(collectionName, r.GetCollectionName())
			return merr.Success(), nil
		}).Once()

		err := s.client.GrantV2(ctx, NewGrantV2Option(roleName, privilegeName, dbName, collectionName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().OperatePrivilegeV2(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.GrantV2(ctx, NewGrantV2Option(roleName, privilegeName, dbName, collectionName))
		s.Error(err)
	})
}

func (s *PrivilegeGroupSuite) TestRevokeV2() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	roleName := fmt.Sprintf("test_role_%s", s.randString(6))
	privilegeName := "Insert"
	dbName := fmt.Sprintf("test_db_%s", s.randString(6))
	collectionName := fmt.Sprintf("test_collection_%s", s.randString(6))

	s.Run("success", func() {
		s.mock.EXPECT().OperatePrivilegeV2(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *milvuspb.OperatePrivilegeV2Request) (*commonpb.Status, error) {
			s.Equal(roleName, r.GetRole().GetName())
			s.Equal(privilegeName, r.GetGrantor().GetPrivilege().GetName())
			s.Equal(dbName, r.GetDbName())
			s.Equal(collectionName, r.GetCollectionName())
			return merr.Success(), nil
		}).Once()

		err := s.client.RevokeV2(ctx, NewRevokeV2Option(roleName, privilegeName, dbName, collectionName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().OperatePrivilegeV2(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.RevokeV2(ctx, NewRevokeV2Option(roleName, privilegeName, dbName, collectionName))
		s.Error(err)
	})
}

func (s *PrivilegeGroupSuite) TestCreatePrivilegeGroup() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	groupName := fmt.Sprintf("test_pg_%s", s.randString(6))

	s.Run("success", func() {
		s.mock.EXPECT().CreatePrivilegeGroup(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *milvuspb.CreatePrivilegeGroupRequest) (*commonpb.Status, error) {
			s.Equal(groupName, r.GetGroupName())
			return merr.Success(), nil
		}).Once()

		err := s.client.CreatePrivilegeGroup(ctx, NewCreatePrivilegeGroupOption(groupName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().CreatePrivilegeGroup(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.CreatePrivilegeGroup(ctx, NewCreatePrivilegeGroupOption(groupName))
		s.Error(err)
	})
}

func (s *PrivilegeGroupSuite) TestDropPrivilegeGroup() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	groupName := fmt.Sprintf("test_pg_%s", s.randString(6))

	s.Run("success", func() {
		s.mock.EXPECT().DropPrivilegeGroup(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *milvuspb.DropPrivilegeGroupRequest) (*commonpb.Status, error) {
			s.Equal(groupName, r.GetGroupName())
			return merr.Success(), nil
		}).Once()

		err := s.client.DropPrivilegeGroup(ctx, NewDropPrivilegeGroupOption(groupName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().DropPrivilegeGroup(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.DropPrivilegeGroup(ctx, NewDropPrivilegeGroupOption(groupName))
		s.Error(err)
	})
}

func (s *PrivilegeGroupSuite) TestListPrivilegeGroups() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		s.mock.EXPECT().ListPrivilegeGroups(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *milvuspb.ListPrivilegeGroupsRequest) (*milvuspb.ListPrivilegeGroupsResponse, error) {
			return &milvuspb.ListPrivilegeGroupsResponse{
				PrivilegeGroups: []*milvuspb.PrivilegeGroupInfo{
					{
						GroupName:  "pg1",
						Privileges: []*milvuspb.PrivilegeEntity{{Name: "Insert"}, {Name: "Query"}},
					},
					{
						GroupName:  "pg2",
						Privileges: []*milvuspb.PrivilegeEntity{{Name: "Delete"}, {Name: "Query"}},
					},
				},
			}, nil
		}).Once()

		pgs, err := s.client.ListPrivilegeGroups(ctx, NewListPrivilegeGroupsOption())
		s.NoError(err)
		s.Equal(2, len(pgs))
		s.Equal("pg1", pgs[0].GroupName)
		s.Equal([]string{"Insert", "Query"}, pgs[0].Privileges)
		s.Equal("pg2", pgs[1].GroupName)
		s.Equal([]string{"Delete", "Query"}, pgs[1].Privileges)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().ListPrivilegeGroups(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.ListPrivilegeGroups(ctx, NewListPrivilegeGroupsOption())
		s.Error(err)
	})
}

func (s *PrivilegeGroupSuite) TestOperatePrivilegeGroup() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	groupName := fmt.Sprintf("test_pg_%s", s.randString(6))
	privileges := []*milvuspb.PrivilegeEntity{{Name: "Insert"}, {Name: "Query"}}
	operateType := milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup

	s.Run("success", func() {
		s.mock.EXPECT().OperatePrivilegeGroup(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *milvuspb.OperatePrivilegeGroupRequest) (*commonpb.Status, error) {
			s.Equal(groupName, r.GetGroupName())
			return merr.Success(), nil
		}).Once()

		err := s.client.OperatePrivilegeGroup(ctx, NewOperatePrivilegeGroupOption(groupName, privileges, operateType))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().OperatePrivilegeGroup(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.OperatePrivilegeGroup(ctx, NewOperatePrivilegeGroupOption(groupName, privileges, operateType))
		s.Error(err)
	})
}

func TestPrivilegeGroup(t *testing.T) {
	suite.Run(t, new(PrivilegeGroupSuite))
}
