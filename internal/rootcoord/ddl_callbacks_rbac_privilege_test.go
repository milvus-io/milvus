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

package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestDDLCallbacksRBACPrivilege(t *testing.T) {
	core := initStreamingSystemAndCore(t)

	// Create a new role.
	targetRoleName := "newRole"
	status, err := core.CreateRole(context.Background(), &milvuspb.CreateRoleRequest{
		Entity: &milvuspb.RoleEntity{
			Name: targetRoleName,
		},
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	targetUserName := "newUser"
	status, err = core.CreateCredential(context.Background(), &internalpb.CredentialInfo{
		Username:          targetUserName,
		EncryptedPassword: "123456",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	// Drop not existed privilege should return error.
	status, err = core.OperatePrivilege(context.Background(), &milvuspb.OperatePrivilegeRequest{
		Type: milvuspb.OperatePrivilegeType_Revoke,
		Entity: &milvuspb.GrantEntity{
			Role: &milvuspb.RoleEntity{
				Name: targetRoleName,
			},
			Grantor: &milvuspb.GrantorEntity{
				Privilege: &milvuspb.PrivilegeEntity{
					Name: "not existed",
				},
			},
		},
	})
	require.Error(t, merr.CheckRPCCall(status, err))

	entity := &milvuspb.GrantEntity{
		Role: &milvuspb.RoleEntity{
			Name: targetRoleName,
		},
		Object: &milvuspb.ObjectEntity{
			Name: "Global",
		},
		ObjectName: "*",
		Grantor: &milvuspb.GrantorEntity{
			Privilege: &milvuspb.PrivilegeEntity{
				Name: "DescribeCollection",
			},
			User: &milvuspb.UserEntity{
				Name: targetUserName,
			},
		},
	}

	// Grant and revoke with v2 version
	status, err = core.OperatePrivilege(context.Background(), &milvuspb.OperatePrivilegeRequest{
		Type:    milvuspb.OperatePrivilegeType_Grant,
		Entity:  entity,
		Version: "v2",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	selectGrantResp, err := core.SelectGrant(context.Background(), &milvuspb.SelectGrantRequest{
		Entity: entity,
	})
	require.NoError(t, merr.CheckRPCCall(selectGrantResp, err))
	require.Equal(t, 1, len(selectGrantResp.Entities))

	status, err = core.OperatePrivilege(context.Background(), &milvuspb.OperatePrivilegeRequest{
		Type:    milvuspb.OperatePrivilegeType_Revoke,
		Entity:  entity,
		Version: "v2",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	selectGrantResp, err = core.SelectGrant(context.Background(), &milvuspb.SelectGrantRequest{
		Entity: entity,
	})
	require.NoError(t, merr.CheckRPCCall(selectGrantResp, err))
	require.Equal(t, 0, len(selectGrantResp.Entities))

	// Grant and revoke with v1 version
	status, err = core.OperatePrivilege(context.Background(), &milvuspb.OperatePrivilegeRequest{
		Type:    milvuspb.OperatePrivilegeType_Grant,
		Entity:  entity,
		Version: "v1",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	selectGrantResp, err = core.SelectGrant(context.Background(), &milvuspb.SelectGrantRequest{
		Entity: entity,
	})
	require.NoError(t, merr.CheckRPCCall(selectGrantResp, err))
	require.Equal(t, 1, len(selectGrantResp.Entities))

	status, err = core.OperatePrivilege(context.Background(), &milvuspb.OperatePrivilegeRequest{
		Type:    milvuspb.OperatePrivilegeType_Revoke,
		Entity:  entity,
		Version: "v1",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	selectGrantResp, err = core.SelectGrant(context.Background(), &milvuspb.SelectGrantRequest{
		Entity: entity,
	})
	require.NoError(t, merr.CheckRPCCall(selectGrantResp, err))
	require.Equal(t, 0, len(selectGrantResp.Entities))

	// Grant and try drop role should return error
	status, err = core.OperatePrivilege(context.Background(), &milvuspb.OperatePrivilegeRequest{
		Type:    milvuspb.OperatePrivilegeType_Grant,
		Entity:  entity,
		Version: "v1",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	status, err = core.DropRole(context.Background(), &milvuspb.DropRoleRequest{
		RoleName: targetRoleName,
	})
	require.Error(t, merr.CheckRPCCall(status, err))
}

func TestDDLCallbacksRBACPrivilegeGroup(t *testing.T) {
	core := initStreamingSystemAndCore(t)

	groupName := "group1"
	status, err := core.CreatePrivilegeGroup(context.Background(), &milvuspb.CreatePrivilegeGroupRequest{
		GroupName: groupName,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	status, err = core.OperatePrivilegeGroup(context.Background(), &milvuspb.OperatePrivilegeGroupRequest{
		GroupName:  groupName,
		Type:       milvuspb.OperatePrivilegeGroupType_RemovePrivilegesFromGroup,
		Privileges: []*milvuspb.PrivilegeEntity{{Name: "Query"}},
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	status, err = core.OperatePrivilegeGroup(context.Background(), &milvuspb.OperatePrivilegeGroupRequest{
		GroupName:  groupName,
		Type:       milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup,
		Privileges: []*milvuspb.PrivilegeEntity{{Name: "Query"}},
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	status, err = core.OperatePrivilegeGroup(context.Background(), &milvuspb.OperatePrivilegeGroupRequest{
		GroupName:  groupName,
		Type:       milvuspb.OperatePrivilegeGroupType_RemovePrivilegesFromGroup,
		Privileges: []*milvuspb.PrivilegeEntity{{Name: "Query"}},
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	status, err = core.DropPrivilegeGroup(context.Background(), &milvuspb.DropPrivilegeGroupRequest{
		GroupName: groupName,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
}
