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
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestDDLCallbacksRBACRestore(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	ctx := context.Background()

	rbacMeta := &milvuspb.RBACMeta{
		Users: []*milvuspb.UserInfo{
			{
				User:     "user1",
				Password: "passwd",
				Roles: []*milvuspb.RoleEntity{
					{
						Name: "role1",
					},
				},
			},
		},
		Roles: []*milvuspb.RoleEntity{
			{
				Name: "role1",
			},
		},

		Grants: []*milvuspb.GrantEntity{
			{
				Role:       &milvuspb.RoleEntity{Name: "role1"},
				Object:     &milvuspb.ObjectEntity{Name: "obj1"},
				ObjectName: "obj_name1",
				DbName:     util.DefaultDBName,
				Grantor: &milvuspb.GrantorEntity{
					User:      &milvuspb.UserEntity{Name: "user1"},
					Privilege: &milvuspb.PrivilegeEntity{Name: "Load"},
				},
			},
		},

		PrivilegeGroups: []*milvuspb.PrivilegeGroupInfo{
			{
				GroupName:  "custom_group",
				Privileges: []*milvuspb.PrivilegeEntity{{Name: "CreateCollection"}},
			},
		},
	}
	// test restore success
	status, err := core.RestoreRBAC(ctx, &milvuspb.RestoreRBACMetaRequest{
		RBACMeta: rbacMeta,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	status, err = core.RestoreRBAC(ctx, &milvuspb.RestoreRBACMetaRequest{
		RBACMeta: rbacMeta,
	})
	require.Error(t, merr.CheckRPCCall(status, err))

	// check user
	users, err := core.meta.ListCredentialUsernames(ctx)
	assert.NoError(t, err)
	assert.Len(t, users.Usernames, 1)
	assert.Equal(t, "user1", users.Usernames[0])
	// check grant
	userRoles, err := core.meta.ListUserRole(ctx, util.DefaultTenant)
	assert.NoError(t, err)
	assert.Len(t, userRoles, 1)
	assert.Equal(t, "user1/role1", userRoles[0])
	policies, err := core.meta.SelectGrant(ctx, util.DefaultTenant, rbacMeta.Grants[0])
	assert.NoError(t, err)
	assert.Len(t, policies, 1)
	assert.Equal(t, "obj_name1", policies[0].ObjectName)
	assert.Equal(t, "role1", policies[0].Role.Name)
	assert.Equal(t, "user1", policies[0].Grantor.User.Name)
	assert.Equal(t, "Load", policies[0].Grantor.Privilege.Name)
	// check privilege group
	privGroups, err := core.meta.ListPrivilegeGroups(ctx)
	assert.NoError(t, err)
	assert.Len(t, privGroups, 1)
	assert.Equal(t, "custom_group", privGroups[0].GroupName)
	assert.Equal(t, "CreateCollection", privGroups[0].Privileges[0].Name)

	rbacMeta2 := &milvuspb.RBACMeta{
		Users: []*milvuspb.UserInfo{
			{
				User:     "user2",
				Password: "passwd",
				Roles: []*milvuspb.RoleEntity{
					{
						Name: "role2",
					},
				},
			},
			{
				User:     "user1",
				Password: "passwd",
				Roles: []*milvuspb.RoleEntity{
					{
						Name: "role2",
					},
				},
			},
		},
		Roles: []*milvuspb.RoleEntity{
			{
				Name: "role2",
			},
		},

		Grants: []*milvuspb.GrantEntity{
			{
				Role:       &milvuspb.RoleEntity{Name: "role2"},
				Object:     &milvuspb.ObjectEntity{Name: "obj2"},
				ObjectName: "obj_name2",
				DbName:     util.DefaultDBName,
				Grantor: &milvuspb.GrantorEntity{
					User:      &milvuspb.UserEntity{Name: "user2"},
					Privilege: &milvuspb.PrivilegeEntity{Name: "Load"},
				},
			},
		},

		PrivilegeGroups: []*milvuspb.PrivilegeGroupInfo{
			{
				GroupName:  "custom_group2",
				Privileges: []*milvuspb.PrivilegeEntity{{Name: "DropCollection"}},
			},
		},
	}

	// test restore failed and roll back
	status, err = core.RestoreRBAC(ctx, &milvuspb.RestoreRBACMetaRequest{
		RBACMeta: rbacMeta2,
	})
	require.Error(t, merr.CheckRPCCall(status, err))
}
