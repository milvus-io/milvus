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
	"fmt"
	"strings"
	"testing"

	"github.com/samber/lo"
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
	backupResp, err := s.Cluster.Proxy.BackupRBAC(ctx, &milvuspb.BackupRBACMetaRequest{})
	s.NoError(err)
	s.True(merr.Ok(backupResp.GetStatus()))
	s.Equal("", backupResp.GetRBACMeta().String())

	// Generate some RBAC content
	roleName := "test_role"
	createRoleResp, err := s.Cluster.Proxy.CreateRole(ctx, &milvuspb.CreateRoleRequest{
		Entity: &milvuspb.RoleEntity{Name: roleName},
	})
	s.NoError(err)
	s.True(merr.Ok(createRoleResp))

	resp, _ := s.operatePrivilege(ctx, roleName, "ReadOnly", commonpb.ObjectType_Collection.String(), milvuspb.OperatePrivilegeType_Grant)
	s.True(merr.Ok(resp))
	resp, _ = s.operatePrivilege(ctx, roleName, "ReadWrite", commonpb.ObjectType_Collection.String(), milvuspb.OperatePrivilegeType_Grant)
	s.True(merr.Ok(resp))
	resp, _ = s.operatePrivilege(ctx, roleName, "Admin", commonpb.ObjectType_Global.String(), milvuspb.OperatePrivilegeType_Grant)
	s.True(merr.Ok(resp))

	for _, builtinGroup := range paramtable.Get().RbacConfig.GetDefaultPrivilegeGroupNames() {
		resp, _ = s.operatePrivilege(ctx, roleName, builtinGroup, commonpb.ObjectType_Global.String(), milvuspb.OperatePrivilegeType_Grant)
		s.False(merr.Ok(resp))
	}
}

func (s *PrivilegeGroupTestSuite) TestInvalidPrivilegeGroup() {
	ctx := GetContext(context.Background(), "root:123456")

	createResp, err := s.Cluster.Proxy.CreatePrivilegeGroup(ctx, &milvuspb.CreatePrivilegeGroupRequest{
		GroupName: "",
	})
	s.NoError(err)
	s.False(merr.Ok(createResp))

	// create group %$ will fail
	createResp, _ = s.Cluster.Proxy.CreatePrivilegeGroup(ctx, &milvuspb.CreatePrivilegeGroupRequest{
		GroupName: "%$",
	})
	s.False(merr.Ok(createResp))

	createResp, _ = s.Cluster.Proxy.CreatePrivilegeGroup(ctx, &milvuspb.CreatePrivilegeGroupRequest{
		GroupName: "a&",
	})
	s.False(merr.Ok(createResp))

	createResp, _ = s.Cluster.Proxy.CreatePrivilegeGroup(ctx, &milvuspb.CreatePrivilegeGroupRequest{
		GroupName: strings.Repeat("a", 300),
	})
	s.False(merr.Ok(createResp))

	// drop group %$ will fail
	dropResp, _ := s.Cluster.Proxy.DropPrivilegeGroup(ctx, &milvuspb.DropPrivilegeGroupRequest{
		GroupName: "%$",
	})
	s.False(merr.Ok(dropResp))

	dropResp, err = s.Cluster.Proxy.DropPrivilegeGroup(ctx, &milvuspb.DropPrivilegeGroupRequest{
		GroupName: "group1",
	})
	s.NoError(err)
	s.True(merr.Ok(dropResp))

	dropResp, err = s.Cluster.Proxy.DropPrivilegeGroup(ctx, &milvuspb.DropPrivilegeGroupRequest{
		GroupName: "",
	})
	s.NoError(err)
	s.False(merr.Ok(dropResp))

	operateResp, err := s.Cluster.Proxy.OperatePrivilegeGroup(ctx, &milvuspb.OperatePrivilegeGroupRequest{
		GroupName: "",
	})
	s.NoError(err)
	s.False(merr.Ok(operateResp))

	operateResp, err = s.Cluster.Proxy.OperatePrivilegeGroup(ctx, &milvuspb.OperatePrivilegeGroupRequest{
		GroupName:  "group1",
		Privileges: []*milvuspb.PrivilegeEntity{{Name: "123"}},
	})
	s.NoError(err)
	s.False(merr.Ok(operateResp))
}

func (s *PrivilegeGroupTestSuite) TestInvalidGrantV2() {
	ctx := GetContext(context.Background(), "root:123456")

	// invalid operate privilege type
	resp, _ := s.operatePrivilegeV2(ctx, "role", "Insert", util.AnyWord, util.AnyWord, milvuspb.OperatePrivilegeType(-1))
	s.False(merr.Ok(resp))

	// invlaid database name
	resp, _ = s.operatePrivilegeV2(ctx, "role", "Insert", "%$", util.AnyWord, milvuspb.OperatePrivilegeType_Grant)
	s.False(merr.Ok(resp))

	// invalid collection name
	resp, _ = s.operatePrivilegeV2(ctx, "role", "Insert", util.AnyWord, "%$", milvuspb.OperatePrivilegeType_Grant)
	s.False(merr.Ok(resp))
}

func (s *PrivilegeGroupTestSuite) TestGrantV2BuiltinPrivilegeGroup() {
	ctx := GetContext(context.Background(), "root:123456")

	roleName := "test_role"
	createRoleResp, err := s.Cluster.Proxy.CreateRole(ctx, &milvuspb.CreateRoleRequest{
		Entity: &milvuspb.RoleEntity{Name: roleName},
	})
	s.NoError(err)
	s.True(merr.Ok(createRoleResp))

	for _, builtinGroup := range paramtable.Get().RbacConfig.GetDefaultPrivilegeGroupNames() {
		resp, _ := s.operatePrivilegeV2(ctx, roleName, builtinGroup, util.AnyWord, util.AnyWord, milvuspb.OperatePrivilegeType_Grant)
		s.True(merr.Ok(resp))
	}

	resp, _ := s.operatePrivilegeV2(ctx, roleName, "ClusterAdmin", "db1", util.AnyWord, milvuspb.OperatePrivilegeType_Grant)
	s.False(merr.Ok(resp))
	resp, _ = s.operatePrivilegeV2(ctx, roleName, "ClusterAdmin", "db1", "col1", milvuspb.OperatePrivilegeType_Grant)
	s.False(merr.Ok(resp))
	resp, _ = s.operatePrivilegeV2(ctx, roleName, "ClusterAdmin", util.AnyWord, "col1", milvuspb.OperatePrivilegeType_Grant)
	s.False(merr.Ok(resp))
	resp, _ = s.operatePrivilegeV2(ctx, roleName, "DatabaseAdmin", "db1", util.AnyWord, milvuspb.OperatePrivilegeType_Grant)
	s.True(merr.Ok(resp))
	resp, _ = s.operatePrivilegeV2(ctx, roleName, "DatabaseAdmin", "db1", "col1", milvuspb.OperatePrivilegeType_Grant)
	s.False(merr.Ok(resp))
	resp, _ = s.operatePrivilegeV2(ctx, roleName, "DatabaseAdmin", util.AnyWord, "col1", milvuspb.OperatePrivilegeType_Grant)
	s.False(merr.Ok(resp))
	resp, _ = s.operatePrivilegeV2(ctx, roleName, "CollectionAdmin", "db1", util.AnyWord, milvuspb.OperatePrivilegeType_Grant)
	s.True(merr.Ok(resp))
	resp, _ = s.operatePrivilegeV2(ctx, roleName, "CollectionAdmin", "db1", "col1", milvuspb.OperatePrivilegeType_Grant)
	s.True(merr.Ok(resp))
	resp, _ = s.operatePrivilegeV2(ctx, roleName, "CollectionAdmin", util.AnyWord, "col1", milvuspb.OperatePrivilegeType_Grant)
	s.False(merr.Ok(resp))
}

func (s *PrivilegeGroupTestSuite) TestGrantV2CustomPrivilegeGroup() {
	ctx := GetContext(context.Background(), "root:123456")

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
	validatePrivilegeGroup := func(groupName string, privileges int) []*milvuspb.PrivilegeEntity {
		resp, err := s.Cluster.Proxy.ListPrivilegeGroups(ctx, &milvuspb.ListPrivilegeGroupsRequest{})
		s.NoError(err)
		for _, privGroup := range resp.PrivilegeGroups {
			if privGroup.GroupName == groupName {
				s.Equal(privileges, len(privGroup.Privileges))
				return privGroup.Privileges
			}
		}
		return nil
	}

	// create group1: query, search
	createResp, err := s.Cluster.Proxy.CreatePrivilegeGroup(ctx, &milvuspb.CreatePrivilegeGroupRequest{
		GroupName: "group1",
	})
	s.NoError(err)
	s.True(merr.Ok(createResp))
	validatePrivilegeGroup("group1", 0)
	operatePrivilegeGroup("group1", milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup, []*milvuspb.PrivilegeEntity{
		{Name: "Query"},
		{Name: "Search"},
	})
	validatePrivilegeGroup("group1", 2)

	// grant insert to role -> role: insert
	role := "role1"
	createRoleResp, err := s.Cluster.Proxy.CreateRole(ctx, &milvuspb.CreateRoleRequest{
		Entity: &milvuspb.RoleEntity{Name: role},
	})
	s.NoError(err)
	s.True(merr.Ok(createRoleResp))
	resp, _ := s.operatePrivilegeV2(ctx, role, "Insert", util.AnyWord, util.AnyWord, milvuspb.OperatePrivilegeType_Grant)
	s.True(merr.Ok(resp))
	selectResp, _ := s.validateGrants(ctx, role, commonpb.ObjectType_Collection.String(), util.AnyWord, util.AnyWord)
	s.Len(selectResp.GetEntities(), 1)

	// grant group1 to role -> role: insert, group1(query, search)
	resp, _ = s.operatePrivilegeV2(ctx, role, "group1", util.AnyWord, util.AnyWord, milvuspb.OperatePrivilegeType_Grant)
	s.True(merr.Ok(resp))
	selectResp, _ = s.validateGrants(ctx, role, commonpb.ObjectType_Global.String(), util.AnyWord, util.AnyWord)
	s.Len(selectResp.GetEntities(), 1)

	// create group2: query, delete
	createResp2, err := s.Cluster.Proxy.CreatePrivilegeGroup(ctx, &milvuspb.CreatePrivilegeGroupRequest{
		GroupName: "group2",
	})
	s.NoError(err)
	s.True(merr.Ok(createResp2))
	validatePrivilegeGroup("group2", 0)
	operatePrivilegeGroup("group2", milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup, []*milvuspb.PrivilegeEntity{
		{Name: "Query"},
		{Name: "Delete"},
	})
	validatePrivilegeGroup("group2", 2)

	// grant group2 to role -> role: insert, group1(query, search), group2(query, delete)
	resp, _ = s.operatePrivilegeV2(ctx, role, "group2", util.AnyWord, util.AnyWord, milvuspb.OperatePrivilegeType_Grant)
	s.True(merr.Ok(resp))
	selectResp, _ = s.validateGrants(ctx, role, commonpb.ObjectType_Global.String(), util.AnyWord, util.AnyWord)
	s.Len(selectResp.GetEntities(), 2)

	// add query, load to group1 -> group1: query, search, load -> role: insert, group1(query, search, load), group2(query, delete)
	operatePrivilegeGroup("group1", milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup, []*milvuspb.PrivilegeEntity{
		{Name: "Query"},
		{Name: "Load"},
	})
	validatePrivilegeGroup("group1", 3)
	selectResp, _ = s.validateGrants(ctx, role, commonpb.ObjectType_Global.String(), util.AnyWord, util.AnyWord)
	s.Len(selectResp.GetEntities(), 2)

	// add different object type privileges to group1 is not allowed
	resp, _ = s.Cluster.Proxy.OperatePrivilegeGroup(ctx, &milvuspb.OperatePrivilegeGroupRequest{
		GroupName:  "group1",
		Type:       milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup,
		Privileges: []*milvuspb.PrivilegeEntity{{Name: "CreateCollection"}},
	})
	s.Error(merr.Error(resp))

	// remove query from group1 -> group1: search, load -> role: insert, group1(search, load), group2(query, delete), role still have query privilege because of group2 granted.
	operatePrivilegeGroup("group1", milvuspb.OperatePrivilegeGroupType_RemovePrivilegesFromGroup, []*milvuspb.PrivilegeEntity{
		{Name: "Query"},
	})
	validatePrivilegeGroup("group1", 2)
	selectResp, _ = s.validateGrants(ctx, role, commonpb.ObjectType_Global.String(), util.AnyWord, util.AnyWord)
	s.Len(selectResp.GetEntities(), 2)

	// Drop the group during any role usage will cause error
	dropResp, _ := s.Cluster.Proxy.DropPrivilegeGroup(ctx, &milvuspb.DropPrivilegeGroupRequest{
		GroupName: "group1",
	})
	s.Error(merr.Error(dropResp))

	// Revoke privilege group and privileges
	resp, _ = s.operatePrivilegeV2(ctx, role, "group1", util.AnyWord, util.AnyWord, milvuspb.OperatePrivilegeType_Revoke)
	s.True(merr.Ok(resp))
	resp, _ = s.operatePrivilegeV2(ctx, role, "group2", util.AnyWord, util.AnyWord, milvuspb.OperatePrivilegeType_Revoke)
	s.True(merr.Ok(resp))
	resp, _ = s.operatePrivilegeV2(ctx, role, "Insert", util.AnyWord, util.AnyWord, milvuspb.OperatePrivilegeType_Revoke)
	s.True(merr.Ok(resp))

	// Drop the privilege group after revoking the privilege will succeed
	dropResp, err = s.Cluster.Proxy.DropPrivilegeGroup(ctx, &milvuspb.DropPrivilegeGroupRequest{
		GroupName: "group1",
	})
	s.NoError(err)
	s.True(merr.Ok(dropResp))

	dropResp, err = s.Cluster.Proxy.DropPrivilegeGroup(ctx, &milvuspb.DropPrivilegeGroupRequest{
		GroupName: "group2",
	})
	s.NoError(err)
	s.True(merr.Ok(dropResp))

	// Validate the group was dropped
	listResp, err := s.Cluster.Proxy.ListPrivilegeGroups(ctx, &milvuspb.ListPrivilegeGroupsRequest{})
	s.NoError(err)
	s.Equal(len(paramtable.Get().RbacConfig.GetDefaultPrivilegeGroupNames()), len(listResp.PrivilegeGroups))

	// validate edge cases
	resp, _ = s.operatePrivilegeV2(ctx, role, util.AnyWord, util.AnyWord, util.AnyWord, milvuspb.OperatePrivilegeType_Grant)
	s.True(merr.Ok(resp))

	resp, _ = s.operatePrivilegeV2(ctx, role, util.AnyWord, util.AnyWord, util.AnyWord, milvuspb.OperatePrivilegeType_Revoke)
	s.True(merr.Ok(resp))

	resp, _ = s.operatePrivilegeV2(ctx, role, "group3", util.AnyWord, util.AnyWord, milvuspb.OperatePrivilegeType_Revoke)
	s.False(merr.Ok(resp))

	resp, _ = s.operatePrivilegeV2(ctx, role, "%$", util.AnyWord, util.AnyWord, milvuspb.OperatePrivilegeType_Grant)
	s.False(merr.Ok(resp))

	// Drop the role
	dropRoleResp, err := s.Cluster.Proxy.DropRole(ctx, &milvuspb.DropRoleRequest{
		RoleName: role,
	})
	s.NoError(err)
	s.True(merr.Ok(dropRoleResp))
}

func (s *PrivilegeGroupTestSuite) TestVersionCrossed() {
	ctx := GetContext(context.Background(), "root:123456")

	role := "role1"
	createRoleResp, err := s.Cluster.Proxy.CreateRole(ctx, &milvuspb.CreateRoleRequest{
		Entity: &milvuspb.RoleEntity{Name: role},
	})
	s.NoError(err)
	s.True(merr.Ok(createRoleResp))
	resp, err := s.Cluster.Proxy.OperatePrivilege(ctx, &milvuspb.OperatePrivilegeRequest{
		Type: milvuspb.OperatePrivilegeType_Grant,
		Entity: &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: role},
			Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
			ObjectName: "collection1",
			DbName:     "",
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: util.UserRoot},
				Privilege: &milvuspb.PrivilegeEntity{Name: "Insert"},
			},
		},
	})
	s.NoError(err)
	s.True(merr.Ok(resp))
	selectResp, err := s.Cluster.Proxy.SelectGrant(ctx, &milvuspb.SelectGrantRequest{
		Entity: &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: role},
			Object:     nil,
			ObjectName: "",
			DbName:     "",
		},
	})
	s.NoError(err)
	s.True(merr.Ok(selectResp.GetStatus()))
	s.Len(selectResp.GetEntities(), 1)

	revoke, err := s.operatePrivilegeV2(ctx, role, "Insert", "default", "collection1", milvuspb.OperatePrivilegeType_Revoke)
	s.NoError(err)
	s.True(merr.Ok(revoke))

	selectResp, err = s.Cluster.Proxy.SelectGrant(ctx, &milvuspb.SelectGrantRequest{
		Entity: &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: role},
			Object:     nil,
			ObjectName: "",
			DbName:     "",
		},
	})
	s.NoError(err)
	s.True(merr.Ok(selectResp.GetStatus()))
	s.Len(selectResp.GetEntities(), 0)
}

func (s *PrivilegeGroupTestSuite) operatePrivilege(ctx context.Context, role, privilege, objectType string, operateType milvuspb.OperatePrivilegeType) (*commonpb.Status, error) {
	resp, err := s.Cluster.Proxy.OperatePrivilege(ctx, &milvuspb.OperatePrivilegeRequest{
		Type: operateType,
		Entity: &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: role},
			Object:     &milvuspb.ObjectEntity{Name: objectType},
			ObjectName: util.AnyWord,
			DbName:     util.AnyWord,
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: util.UserRoot},
				Privilege: &milvuspb.PrivilegeEntity{Name: privilege},
			},
		},
	})
	return resp, err
}

func (s *PrivilegeGroupTestSuite) operatePrivilegeV2(ctx context.Context, role, privilege, dbName, collectionName string, operateType milvuspb.OperatePrivilegeType) (*commonpb.Status, error) {
	resp, err := s.Cluster.Proxy.OperatePrivilegeV2(ctx, &milvuspb.OperatePrivilegeV2Request{
		Role: &milvuspb.RoleEntity{Name: role},
		Grantor: &milvuspb.GrantorEntity{
			User:      &milvuspb.UserEntity{Name: util.UserRoot},
			Privilege: &milvuspb.PrivilegeEntity{Name: privilege},
		},
		Type:           operateType,
		DbName:         dbName,
		CollectionName: collectionName,
	})
	return resp, err
}

func (s *PrivilegeGroupTestSuite) validateGrants(ctx context.Context, roleName, objectType, database, resource string) (*milvuspb.SelectGrantResponse, error) {
	resp, err := s.Cluster.Proxy.SelectGrant(ctx, &milvuspb.SelectGrantRequest{
		Entity: &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: roleName},
			Object:     &milvuspb.ObjectEntity{Name: objectType},
			ObjectName: util.AnyWord,
			DbName:     util.AnyWord,
		},
	})
	s.NoError(err)
	s.True(merr.Ok(resp.GetStatus()))
	return resp, err
}

func (s *PrivilegeGroupTestSuite) marshalGrants(selectResp *milvuspb.SelectGrantResponse) map[string]*milvuspb.GrantEntity {
	return lo.SliceToMap(selectResp.GetEntities(), func(e *milvuspb.GrantEntity) (string, *milvuspb.GrantEntity) {
		return fmt.Sprintf("%s-%s-%s-%s", e.Object.Name, e.Grantor.Privilege.Name, e.DbName, e.ObjectName), e
	})
}

func TestPrivilegeGroup(t *testing.T) {
	suite.Run(t, new(PrivilegeGroupTestSuite))
}
