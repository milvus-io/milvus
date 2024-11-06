// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package rbac

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type PrivilegeGroupTestSuite struct {
	integration.MiniClusterSuite
}

func (s *PrivilegeGroupTestSuite) SetupSuite() {
	s.MiniClusterSuite.SetupSuite()
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.BalanceCheckInterval.Key, "1000")
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.GracefulStopTimeout.Key, "1")
	paramtable.Get().Save(paramtable.Get().CommonCfg.AuthorizationEnabled.Key, "true")
}

func (s *PrivilegeGroupTestSuite) TestBuiltinPrivilegeGroup() {
	ctx := GetContext(context.Background(), "root:123456")

	// Test empty RBAC content
	resp, err := s.Cluster.Proxy.BackupRBAC(ctx, &milvuspb.BackupRBACMetaRequest{})
	s.NoError(err)
	s.True(merr.Ok(resp.GetStatus()))
	s.Equal("", resp.GetRBACMeta().String())

	// Generate some RBAC content
	roleName := "test_role"
	createRoleResp, err := s.Cluster.Proxy.CreateRole(ctx, &milvuspb.CreateRoleRequest{
		Entity: &milvuspb.RoleEntity{Name: roleName},
	})
	s.NoError(err)
	s.True(merr.Ok(createRoleResp))

	s.grantPrivilege(ctx, roleName, "ReadOnly", commonpb.ObjectType_Collection.String(), util.AnyWord, util.AnyWord)
	s.grantPrivilege(ctx, roleName, "ReadWrite", commonpb.ObjectType_Collection.String(), util.AnyWord, util.AnyWord)
	s.grantPrivilege(ctx, roleName, "Admin", commonpb.ObjectType_Global.String(), util.AnyWord, util.AnyWord)

	s.validateGrants(ctx, roleName, commonpb.ObjectType_Global.String(), util.AnyWord, util.AnyWord, 1)
	s.validateGrants(ctx, roleName, commonpb.ObjectType_Collection.String(), util.AnyWord, util.AnyWord, 2)
}

func (s *PrivilegeGroupTestSuite) TestCustomPrivilegeGroup() {
	ctx := GetContext(context.Background(), "root:123456")
	groupName := "test_privilege_group"

	// Helper function to operate on privilege groups
	operatePrivilegeGroup := func(groupName string, operateType milvuspb.OperatePrivilegeGroupType, privileges []*milvuspb.PrivilegeEntity) {
		resp, err := s.Cluster.Proxy.OperatePrivilegeGroup(ctx, &milvuspb.OperatePrivilegeGroupRequest{
			GroupName:  groupName,
			Type:       operateType,
			Privileges: privileges,
		})
		s.NoError(err)
		s.True(merr.Ok(resp))
	}

	// Helper function to list privilege groups and return the target group and its privileges
	listAndValidatePrivilegeGroup := func(expectedGroupName string, expectedPrivilegeCount int) []*milvuspb.PrivilegeEntity {
		resp, err := s.Cluster.Proxy.ListPrivilegeGroups(ctx, &milvuspb.ListPrivilegeGroupsRequest{})
		s.NoError(err)
		privGroupInfo := resp.PrivilegeGroups[0]
		s.Equal(expectedGroupName, privGroupInfo.GroupName)
		s.Equal(expectedPrivilegeCount, len(privGroupInfo.Privileges))
		return privGroupInfo.Privileges
	}

	// Test creating a privilege group
	createResp, err := s.Cluster.Proxy.CreatePrivilegeGroup(ctx, &milvuspb.CreatePrivilegeGroupRequest{
		GroupName: groupName,
	})
	s.NoError(err)
	s.True(merr.Ok(createResp))

	// Validate the group was created
	listAndValidatePrivilegeGroup(groupName, 0)

	// Test adding privileges
	operatePrivilegeGroup(groupName, milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup, []*milvuspb.PrivilegeEntity{
		{Name: "CreateCollection"},
		{Name: "DescribeCollection"},
	})
	listAndValidatePrivilegeGroup(groupName, 2)

	// Test adding more privileges (one duplicate, one new)
	operatePrivilegeGroup(groupName, milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup, []*milvuspb.PrivilegeEntity{
		{Name: "DescribeCollection"},
		{Name: "DropCollection"},
	})
	listAndValidatePrivilegeGroup(groupName, 3)

	// Test removing privileges (including a non-existent one)
	operatePrivilegeGroup(groupName, milvuspb.OperatePrivilegeGroupType_RemovePrivilegesFromGroup, []*milvuspb.PrivilegeEntity{
		{Name: "DescribeCollection"},
		{Name: "DropCollection"},
		{Name: "RenameCollection"},
	})
	privileges := listAndValidatePrivilegeGroup(groupName, 1)
	s.Equal("CreateCollection", privileges[0].Name)

	// Test grant privilege group
	roleName := "test_role"
	createRoleResp, err := s.Cluster.Proxy.CreateRole(ctx, &milvuspb.CreateRoleRequest{
		Entity: &milvuspb.RoleEntity{Name: roleName},
	})
	s.NoError(err)
	s.True(merr.Ok(createRoleResp))

	// Grant privileges
	s.grantPrivilege(ctx, roleName, groupName, commonpb.ObjectType_Global.String(), util.AnyWord, util.AnyWord)
	s.validateGrants(ctx, roleName, commonpb.ObjectType_Global.String(), util.AnyWord, util.AnyWord, 1)

	// Drop the group during any role usage will cause error
	dropResp, _ := s.Cluster.Proxy.DropPrivilegeGroup(ctx, &milvuspb.DropPrivilegeGroupRequest{
		GroupName: groupName,
	})
	s.Error(merr.Error(dropResp))

	// Revoke privilege group
	revokeResp, err := s.Cluster.Proxy.OperatePrivilege(ctx, &milvuspb.OperatePrivilegeRequest{
		Type: milvuspb.OperatePrivilegeType_Revoke,
		Entity: &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: roleName},
			Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Global.String()},
			ObjectName: util.AnyWord,
			DbName:     util.AnyWord,
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: util.UserRoot},
				Privilege: &milvuspb.PrivilegeEntity{Name: groupName},
			},
		},
	})
	s.NoError(err)
	s.True(merr.Ok(revokeResp))

	// Drop the privilege group after revoking the privilege will succeed
	dropResp, err = s.Cluster.Proxy.DropPrivilegeGroup(ctx, &milvuspb.DropPrivilegeGroupRequest{
		GroupName: groupName,
	})
	s.NoError(err)
	s.True(merr.Ok(dropResp))

	// Validate the group was dropped
	resp, err := s.Cluster.Proxy.ListPrivilegeGroups(ctx, &milvuspb.ListPrivilegeGroupsRequest{})
	s.NoError(err)
	s.Equal(0, len(resp.PrivilegeGroups))

	// Drop the role
	dropRoleResp, err := s.Cluster.Proxy.DropRole(ctx, &milvuspb.DropRoleRequest{
		RoleName: roleName,
	})
	s.NoError(err)
	s.True(merr.Ok(dropRoleResp))
}

func (s *PrivilegeGroupTestSuite) grantPrivilege(ctx context.Context, roleName, privilegeName, objectType, objectName, dbName string) {
	resp, err := s.Cluster.Proxy.OperatePrivilege(ctx, &milvuspb.OperatePrivilegeRequest{
		Type: milvuspb.OperatePrivilegeType_Grant,
		Entity: &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: roleName},
			Object:     &milvuspb.ObjectEntity{Name: objectType},
			ObjectName: objectName,
			DbName:     dbName,
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: util.UserRoot},
				Privilege: &milvuspb.PrivilegeEntity{Name: privilegeName},
			},
		},
	})
	s.NoError(err)
	s.True(merr.Ok(resp))
}

func (s *PrivilegeGroupTestSuite) validateGrants(ctx context.Context, roleName, objectType, objectName, dbName string, expectedCount int) {
	resp, err := s.Cluster.Proxy.SelectGrant(ctx, &milvuspb.SelectGrantRequest{
		Entity: &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: roleName},
			Object:     &milvuspb.ObjectEntity{Name: objectType},
			ObjectName: objectName,
			DbName:     dbName,
		},
	})
	s.NoError(err)
	s.True(merr.Ok(resp.GetStatus()))
	s.Len(resp.GetEntities(), expectedCount)
}

func TestPrivilegeGroup(t *testing.T) {
	suite.Run(t, new(PrivilegeGroupTestSuite))
}
