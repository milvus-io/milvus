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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestDDLCallbacksRBACRole(t *testing.T) {
	core := initStreamingSystemAndCore(t)

	// Test drop builtin role should return error
	roleDbAdmin := "db_admin"
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().RoleCfg.Enabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().RoleCfg.Roles.Key, `{"`+roleDbAdmin+`": {"privileges": [{"object_type": "Global", "object_name": "*", "privilege": "CreateCollection", "db_name": "*"}]}}`)
	err := core.initBuiltinRoles(context.Background())
	assert.Equal(t, nil, err)
	assert.True(t, util.IsBuiltinRole(roleDbAdmin))
	assert.False(t, util.IsBuiltinRole(util.RoleAdmin))
	resp, err := core.DropRole(context.Background(), &milvuspb.DropRoleRequest{RoleName: roleDbAdmin})
	assert.Equal(t, nil, err)
	assert.Equal(t, int32(1401), resp.Code) // merr.ErrPrivilegeNotPermitted

	// Create a new credential.
	testUserName := "user" + funcutil.RandomString(10)
	status, err := core.CreateCredential(context.Background(), &internalpb.CredentialInfo{
		Username:          testUserName,
		EncryptedPassword: "123456",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	testRoleName := "role" + funcutil.RandomString(10)

	// Drop a not existed role should return error.
	status, err = core.DropRole(context.Background(), &milvuspb.DropRoleRequest{
		RoleName: testRoleName,
	})
	require.Error(t, merr.CheckRPCCall(status, err))

	// Operate a not existed role should return error.
	status, err = core.OperateUserRole(context.Background(), &milvuspb.OperateUserRoleRequest{
		RoleName: testRoleName,
		Username: testUserName,
		Type:     milvuspb.OperateUserRoleType_AddUserToRole,
	})
	require.Error(t, merr.CheckRPCCall(status, err))

	// Create a new role.
	status, err = core.CreateRole(context.Background(), &milvuspb.CreateRoleRequest{
		Entity: &milvuspb.RoleEntity{
			Name: testRoleName,
		},
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	selectRoleResp, err := core.SelectRole(context.Background(), &milvuspb.SelectRoleRequest{
		Role: &milvuspb.RoleEntity{
			Name: testRoleName,
		},
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	assert.Equal(t, 1, len(selectRoleResp.Results))
	assert.Equal(t, testRoleName, selectRoleResp.Results[0].Role.GetName())

	// Add user to role.
	status, err = core.OperateUserRole(context.Background(), &milvuspb.OperateUserRoleRequest{
		RoleName: testRoleName,
		Username: testUserName,
		Type:     milvuspb.OperateUserRoleType_AddUserToRole,
	})
	assert.NoError(t, merr.CheckRPCCall(status, err))
	selectRoleResp, err = core.SelectRole(context.Background(), &milvuspb.SelectRoleRequest{
		Role: &milvuspb.RoleEntity{
			Name: testRoleName,
		},
		IncludeUserInfo: true,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	assert.Equal(t, 1, len(selectRoleResp.Results))
	assert.Equal(t, testRoleName, selectRoleResp.Results[0].Role.GetName())
	assert.Equal(t, 1, len(selectRoleResp.Results[0].Users))
	assert.Equal(t, testUserName, selectRoleResp.Results[0].Users[0].GetName())

	// Remove a user from role.
	status, err = core.OperateUserRole(context.Background(), &milvuspb.OperateUserRoleRequest{
		RoleName: testRoleName,
		Username: testUserName,
		Type:     milvuspb.OperateUserRoleType_RemoveUserFromRole,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	selectRoleResp, err = core.SelectRole(context.Background(), &milvuspb.SelectRoleRequest{
		Role: &milvuspb.RoleEntity{
			Name: testRoleName,
		},
		IncludeUserInfo: true,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	assert.Equal(t, 1, len(selectRoleResp.Results))
	assert.Equal(t, testRoleName, selectRoleResp.Results[0].Role.GetName())
	assert.Equal(t, 0, len(selectRoleResp.Results[0].Users))

	// Drop a role with force drop.
	status, err = core.DropRole(context.Background(), &milvuspb.DropRoleRequest{
		RoleName:  testRoleName,
		ForceDrop: true,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	selectRoleResp, err = core.SelectRole(context.Background(), &milvuspb.SelectRoleRequest{
		Role: &milvuspb.RoleEntity{
			Name: testRoleName,
		},
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	assert.Equal(t, 0, len(selectRoleResp.Results))
}
