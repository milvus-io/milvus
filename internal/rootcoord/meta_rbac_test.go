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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	coordmeta "github.com/milvus-io/milvus/pkg/v3/coordmeta/rootcoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func generateMetaTable(_ *testing.T) *MetaTable {
	kv, _ := kvfactory.GetEtcdAndPath()
	path := funcutil.RandomString(10)
	catalogKV := etcdkv.NewEtcdKV(kv, path)
	// RBAC operations talk to the catalog directly and need no reloaded cache,
	// so construct a catalog-only MetaTable (matching the upstream behaviour)
	// instead of NewMetaTable, whose reload() touches global channel stats.
	return coordmeta.NewMetaTableWithCatalog(rootcoord.NewCatalog(catalogKV))
}

func buildAlterUserMessage(credInfo *internalpb.CredentialInfo, timetick uint64) message.BroadcastResultAlterUserMessageV2 {
	msg := message.NewAlterUserMessageBuilderV2().
		WithHeader(&message.AlterUserMessageHeader{
			UserEntity: &milvuspb.UserEntity{
				Name: credInfo.Username,
			},
		}).
		WithBody(&message.AlterUserMessageBody{
			CredentialInfo: credInfo,
		}).
		WithBroadcast([]string{funcutil.GetControlChannel("by-dev-rootcoord-dml_1")}).
		MustBuildBroadcast()
	return message.BroadcastResultAlterUserMessageV2{
		Message: message.MustAsBroadcastAlterUserMessageV2(msg),
		Results: map[string]*message.AppendResult{
			funcutil.GetControlChannel("by-dev-rootcoord-dml_1"): {TimeTick: timetick},
		},
	}
}

func buildDropUserMessage(credInfo *internalpb.CredentialInfo, timetick uint64) message.BroadcastResultDropUserMessageV2 {
	msg := message.NewDropUserMessageBuilderV2().
		WithHeader(&message.DropUserMessageHeader{
			UserName: credInfo.Username,
		}).
		WithBody(&message.DropUserMessageBody{}).
		WithBroadcast([]string{funcutil.GetControlChannel("by-dev-rootcoord-dml_1")}).
		MustBuildBroadcast()
	return message.BroadcastResultDropUserMessageV2{
		Message: message.MustAsBroadcastDropUserMessageV2(msg),
		Results: map[string]*message.AppendResult{
			funcutil.GetControlChannel("by-dev-rootcoord-dml_1"): {TimeTick: timetick},
		},
	}
}

func TestRbacCredential(t *testing.T) {
	mt := generateMetaTable(t)

	username := "user" + funcutil.RandomString(10)
	credInfo := &internalpb.CredentialInfo{
		Username: username,
		Tenant:   util.DefaultTenant,
	}
	err := mt.CheckIfAddCredential(context.TODO(), credInfo)
	require.NoError(t, err)
	err = mt.AlterCredential(context.TODO(), buildAlterUserMessage(credInfo, 1))
	require.NoError(t, err)
	// idempotency
	err = mt.AlterCredential(context.TODO(), buildAlterUserMessage(credInfo, 1))
	require.NoError(t, err)

	tests := []struct {
		description string

		maxUser bool
		info    *internalpb.CredentialInfo
	}{
		{"Empty username", false, &internalpb.CredentialInfo{Username: ""}},
		{"exceed MaxUserNum", true, &internalpb.CredentialInfo{Username: "user3", Tenant: util.DefaultTenant}},
		{"user exist", false, &internalpb.CredentialInfo{Username: username, Tenant: util.DefaultTenant}},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			if test.maxUser {
				paramtable.Get().Save(Params.ProxyCfg.MaxUserNum.Key, "1")
			} else {
				paramtable.Get().Save(Params.ProxyCfg.MaxUserNum.Key, "3")
			}
			defer paramtable.Get().Reset(Params.ProxyCfg.MaxUserNum.Key)
			err := mt.CheckIfAddCredential(context.TODO(), test.info)
			assert.Error(t, err)
		})
	}

	// should be ignored if timetick is too low.
	err = mt.AlterCredential(context.TODO(), buildAlterUserMessage(credInfo, 0))
	require.NoError(t, err)
	newCred, err := mt.GetCredential(context.TODO(), credInfo.Username)
	require.NoError(t, err)
	assert.Equal(t, newCred.TimeTick, uint64(1))

	err = mt.AlterCredential(context.TODO(), buildAlterUserMessage(credInfo, 2))
	require.NoError(t, err)
	newCred, err = mt.GetCredential(context.TODO(), credInfo.Username)
	require.NoError(t, err)
	assert.Equal(t, newCred.TimeTick, uint64(2))

	// should be ignored if timetick is too low.
	err = mt.DeleteCredential(context.TODO(), buildDropUserMessage(credInfo, 2))
	require.NoError(t, err)
	newCred, err = mt.GetCredential(context.TODO(), credInfo.Username)
	require.NoError(t, err)
	assert.Equal(t, newCred.TimeTick, uint64(2))

	err = mt.DeleteCredential(context.TODO(), buildDropUserMessage(credInfo, 3))
	require.NoError(t, err)
	newCred, err = mt.GetCredential(context.TODO(), credInfo.Username)
	require.ErrorIs(t, err, merr.ErrIoKeyNotFound)
	require.Nil(t, newCred)

	err = mt.DeleteCredential(context.TODO(), buildDropUserMessage(credInfo, 0))
	require.NoError(t, err)
}

func TestRbacCredentialAlterCredentialMergesPartialUpdates(t *testing.T) {
	mt := generateMetaTable(t)

	ptr := func(s string) *string {
		return &s
	}

	username := "user" + funcutil.RandomString(10)
	err := mt.AlterCredential(context.TODO(), buildAlterUserMessage(&internalpb.CredentialInfo{
		Username:          username,
		EncryptedPassword: "old-password",
		Description:       ptr("initial description"),
	}, 1))
	require.NoError(t, err)

	err = mt.AlterCredential(context.TODO(), buildAlterUserMessage(&internalpb.CredentialInfo{
		Username:          username,
		EncryptedPassword: "new-password",
	}, 2))
	require.NoError(t, err)
	cred, err := mt.GetCredential(context.TODO(), username)
	require.NoError(t, err)
	assert.Equal(t, "new-password", cred.GetEncryptedPassword())
	assert.Equal(t, "initial description", cred.GetDescription())

	err = mt.AlterCredential(context.TODO(), buildAlterUserMessage(&internalpb.CredentialInfo{
		Username:    username,
		Description: ptr("updated description"),
	}, 3))
	require.NoError(t, err)
	cred, err = mt.GetCredential(context.TODO(), username)
	require.NoError(t, err)
	assert.Equal(t, "new-password", cred.GetEncryptedPassword())
	assert.Equal(t, "updated description", cred.GetDescription())

	err = mt.AlterCredential(context.TODO(), buildAlterUserMessage(&internalpb.CredentialInfo{
		Username:    username,
		Description: ptr(""),
	}, 4))
	require.NoError(t, err)
	cred, err = mt.GetCredential(context.TODO(), username)
	require.NoError(t, err)
	assert.Equal(t, "new-password", cred.GetEncryptedPassword())
	assert.Equal(t, "", cred.GetDescription())
}

func TestRbacCredentialRejectsInconsistentPasswordUpdate(t *testing.T) {
	mt := generateMetaTable(t)

	username := "user" + funcutil.RandomString(10)
	err := mt.AlterCredential(context.TODO(), buildAlterUserMessage(&internalpb.CredentialInfo{
		Username:          username,
		EncryptedPassword: "old-password",
	}, 1))
	require.NoError(t, err)

	err = mt.CheckIfUpdateCredential(context.TODO(), &internalpb.CredentialInfo{
		Username:          username,
		EncryptedPassword: "new-password",
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "must include both encrypted and sha256 password")

	err = mt.CheckIfUpdateCredential(context.TODO(), &internalpb.CredentialInfo{
		Username:       username,
		Sha256Password: "sha256",
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "must include both encrypted and sha256 password")

	description := "description-only update"
	err = mt.CheckIfUpdateCredential(context.TODO(), &internalpb.CredentialInfo{
		Username:    username,
		Description: &description,
	})
	require.NoError(t, err)

	err = mt.CheckIfUpdateCredential(context.TODO(), &internalpb.CredentialInfo{
		Username: username,
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "credential update must change password or description")
}

func TestRbacCreateRole(t *testing.T) {
	mt := generateMetaTable(t)

	paramtable.Get().Save(Params.ProxyCfg.MaxRoleNum.Key, "2")
	defer paramtable.Get().Reset(Params.ProxyCfg.MaxRoleNum.Key)
	err := mt.CheckIfCreateRole(context.TODO(), &milvuspb.CreateRoleRequest{Entity: &milvuspb.RoleEntity{Name: "role1"}})
	require.NoError(t, err)
	err = mt.CreateRole(context.TODO(), util.DefaultTenant, &milvuspb.RoleEntity{Name: "role1"})
	require.NoError(t, err)
	err = mt.CheckIfCreateRole(context.TODO(), &milvuspb.CreateRoleRequest{Entity: &milvuspb.RoleEntity{Name: "role2"}})
	require.NoError(t, err)
	err = mt.CreateRole(context.TODO(), util.DefaultTenant, &milvuspb.RoleEntity{Name: "role2"})
	require.NoError(t, err)

	tests := []struct {
		inEntity *milvuspb.RoleEntity

		description string
	}{
		{&milvuspb.RoleEntity{Name: ""}, "empty string"},
		{&milvuspb.RoleEntity{Name: "role3"}, "role number reached the limit"},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			err := mt.CheckIfCreateRole(context.TODO(), &milvuspb.CreateRoleRequest{Entity: test.inEntity})
			assert.Error(t, err)
		})
	}
	t.Run("role has existed", func(t *testing.T) {
		err := mt.CheckIfCreateRole(context.TODO(), &milvuspb.CreateRoleRequest{Entity: &milvuspb.RoleEntity{Name: "role1"}})
		assert.Error(t, err)
		assert.True(t, errors.Is(err, errRoleAlreadyExists))
	})

	{
		mockCata := mocks.NewRootCoordCatalog(t)
		mockCata.On("ListRole",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil, errors.New("error mock list role"))
		mockMt := coordmeta.NewMetaTableWithCatalog(mockCata)
		err := mockMt.CheckIfCreateRole(context.TODO(), &milvuspb.CreateRoleRequest{Entity: &milvuspb.RoleEntity{Name: "role1"}})
		assert.Error(t, err)
	}
}

func TestRbacAlterRoleDescription(t *testing.T) {
	mt := generateMetaTable(t)

	roleName := "role" + funcutil.RandomString(10)
	err := mt.CreateRole(context.TODO(), util.DefaultTenant, &milvuspb.RoleEntity{
		Name:        roleName,
		Description: "old description",
	})
	require.NoError(t, err)

	err = mt.AlterRole(context.TODO(), util.DefaultTenant, &milvuspb.RoleEntity{
		Name:        roleName,
		Description: "new description",
	})
	require.NoError(t, err)

	roles, err := mt.SelectRole(context.TODO(), util.DefaultTenant, &milvuspb.RoleEntity{Name: roleName}, false)
	require.NoError(t, err)
	require.Len(t, roles, 1)
	assert.Equal(t, "new description", roles[0].GetRole().GetDescription())

	err = mt.AlterRole(context.TODO(), util.DefaultTenant, &milvuspb.RoleEntity{
		Name:        "role_not_exist",
		Description: "ignored",
	})
	require.ErrorIs(t, err, errRoleNotExists)

	err = mt.AlterRole(context.TODO(), util.DefaultTenant, &milvuspb.RoleEntity{
		Name:        util.RoleAdmin,
		Description: "ignored",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)

	err = mt.CheckIfAlterRole(context.TODO(), &milvuspb.AlterRoleRequest{
		RoleName:    util.RolePublic,
		Description: "ignored",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
}

func TestRbacAlterRoleDescriptionErrors(t *testing.T) {
	ctx := context.TODO()

	t.Run("check empty role name", func(t *testing.T) {
		mockCata := mocks.NewRootCoordCatalog(t)
		mockMt := coordmeta.NewMetaTableWithCatalog(mockCata)

		err := mockMt.CheckIfAlterRole(ctx, &milvuspb.AlterRoleRequest{
			RoleName:    "",
			Description: "description",
		})
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("check list role error", func(t *testing.T) {
		targetErr := errors.New("mock list role error")
		roleName := "role_check_list_error"
		mockCata := mocks.NewRootCoordCatalog(t)
		mockCata.EXPECT().ListRole(
			mock.Anything,
			util.DefaultTenant,
			mock.MatchedBy(func(entity *milvuspb.RoleEntity) bool {
				return entity.GetName() == roleName
			}),
			false,
		).Return(nil, targetErr)
		mockMt := coordmeta.NewMetaTableWithCatalog(mockCata)

		err := mockMt.CheckIfAlterRole(ctx, &milvuspb.AlterRoleRequest{
			RoleName:    roleName,
			Description: "description",
		})
		assert.ErrorIs(t, err, targetErr)
	})

	t.Run("alter empty role name", func(t *testing.T) {
		mockCata := mocks.NewRootCoordCatalog(t)
		mockMt := coordmeta.NewMetaTableWithCatalog(mockCata)

		err := mockMt.AlterRole(ctx, util.DefaultTenant, &milvuspb.RoleEntity{
			Name:        "",
			Description: "description",
		})
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("alter list role error", func(t *testing.T) {
		targetErr := errors.New("mock list role error")
		roleName := "role_alter_list_error"
		mockCata := mocks.NewRootCoordCatalog(t)
		mockCata.EXPECT().ListRole(
			mock.Anything,
			util.DefaultTenant,
			mock.MatchedBy(func(entity *milvuspb.RoleEntity) bool {
				return entity.GetName() == roleName
			}),
			false,
		).Return(nil, targetErr)
		mockMt := coordmeta.NewMetaTableWithCatalog(mockCata)

		err := mockMt.AlterRole(ctx, util.DefaultTenant, &milvuspb.RoleEntity{
			Name:        roleName,
			Description: "description",
		})
		assert.ErrorIs(t, err, targetErr)
	})

	t.Run("alter catalog error", func(t *testing.T) {
		targetErr := errors.New("mock alter role error")
		roleName := "role_alter_catalog_error"
		mockCata := mocks.NewRootCoordCatalog(t)
		mockCata.EXPECT().ListRole(
			mock.Anything,
			util.DefaultTenant,
			mock.MatchedBy(func(entity *milvuspb.RoleEntity) bool {
				return entity.GetName() == roleName
			}),
			false,
		).Return([]*milvuspb.RoleResult{{Role: &milvuspb.RoleEntity{Name: roleName}}}, nil)
		mockCata.EXPECT().AlterRole(
			mock.Anything,
			util.DefaultTenant,
			mock.MatchedBy(func(entity *milvuspb.RoleEntity) bool {
				return entity.GetName() == roleName && entity.GetDescription() == "description"
			}),
		).Return(targetErr)
		mockMt := coordmeta.NewMetaTableWithCatalog(mockCata)

		err := mockMt.AlterRole(ctx, util.DefaultTenant, &milvuspb.RoleEntity{
			Name:        roleName,
			Description: "description",
		})
		assert.ErrorIs(t, err, targetErr)
	})
}

func TestRbacCreateRoleToleratesMalformedStoredRoleValue(t *testing.T) {
	ctx := context.TODO()
	mt := generateMetaTable(t)
	catalog := mt.Catalog().(*rootcoord.Catalog)

	require.NoError(t, catalog.CreateRole(ctx, util.DefaultTenant, &milvuspb.RoleEntity{Name: "existing_role"}))
	require.NoError(t, catalog.Txn.Save(ctx, rootcoord.RolePrefix+"/malformed_role", "{"))

	err := mt.CheckIfCreateRole(ctx, &milvuspb.CreateRoleRequest{
		Entity: &milvuspb.RoleEntity{Name: "new_role"},
	})
	require.NoError(t, err)
}

func TestRbacRoleDescriptionLengthLimit(t *testing.T) {
	mt := generateMetaTable(t)

	paramtable.Get().Save(Params.ProxyCfg.MaxRoleDescriptionLength.Key, "4")
	defer paramtable.Get().Reset(Params.ProxyCfg.MaxRoleDescriptionLength.Key)

	err := mt.CheckIfCreateRole(context.TODO(), &milvuspb.CreateRoleRequest{
		Entity: &milvuspb.RoleEntity{
			Name:        "role_desc_limit_create",
			Description: "12345",
		},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)

	err = mt.CreateRole(context.TODO(), util.DefaultTenant, &milvuspb.RoleEntity{Name: "role_desc_limit_alter"})
	require.NoError(t, err)
	err = mt.CheckIfAlterRole(context.TODO(), &milvuspb.AlterRoleRequest{
		RoleName:    "role_desc_limit_alter",
		Description: "12345",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)

	err = mt.CheckIfRBACRestorable(context.TODO(), &milvuspb.RestoreRBACMetaRequest{
		RBACMeta: &milvuspb.RBACMeta{
			Roles: []*milvuspb.RoleEntity{
				{
					Name:        "role_desc_limit_restore",
					Description: "12345",
				},
			},
		},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
}

func TestRbacRoleDescriptionApplyPathSkipsLengthLimit(t *testing.T) {
	t.Run("create role apply path", func(t *testing.T) {
		mt := generateMetaTable(t)
		roleName := "role_desc_apply_create"

		paramtable.Get().Save(Params.ProxyCfg.MaxRoleDescriptionLength.Key, "4")
		defer paramtable.Get().Reset(Params.ProxyCfg.MaxRoleDescriptionLength.Key)

		err := mt.CreateRole(context.TODO(), util.DefaultTenant, &milvuspb.RoleEntity{
			Name:        roleName,
			Description: "12345",
		})
		require.NoError(t, err)

		roles, err := mt.SelectRole(context.TODO(), util.DefaultTenant, &milvuspb.RoleEntity{Name: roleName}, false)
		require.NoError(t, err)
		require.Len(t, roles, 1)
		assert.Equal(t, "12345", roles[0].GetRole().GetDescription())
	})

	t.Run("alter role apply path", func(t *testing.T) {
		mt := generateMetaTable(t)
		roleName := "role_desc_apply_alter"

		err := mt.CreateRole(context.TODO(), util.DefaultTenant, &milvuspb.RoleEntity{Name: roleName})
		require.NoError(t, err)

		paramtable.Get().Save(Params.ProxyCfg.MaxRoleDescriptionLength.Key, "4")
		defer paramtable.Get().Reset(Params.ProxyCfg.MaxRoleDescriptionLength.Key)

		err = mt.AlterRole(context.TODO(), util.DefaultTenant, &milvuspb.RoleEntity{
			Name:        roleName,
			Description: "12345",
		})
		require.NoError(t, err)

		roles, err := mt.SelectRole(context.TODO(), util.DefaultTenant, &milvuspb.RoleEntity{Name: roleName}, false)
		require.NoError(t, err)
		require.Len(t, roles, 1)
		assert.Equal(t, "12345", roles[0].GetRole().GetDescription())
	})
}

func TestRbacDropRole(t *testing.T) {
	mt := generateMetaTable(t)

	// drop a exist role
	roleExist := "role" + funcutil.RandomString(10)
	err := mt.CreateRole(context.TODO(), util.DefaultTenant, &milvuspb.RoleEntity{Name: roleExist})
	require.NoError(t, err)
	err = mt.CheckIfDropRole(context.TODO(), &milvuspb.DropRoleRequest{RoleName: roleExist})
	require.NoError(t, err)
	err = mt.DropRole(context.TODO(), util.DefaultTenant, roleExist)
	require.NoError(t, err)
	// idempotency
	mt.DropRole(context.TODO(), util.DefaultTenant, roleExist)
	require.NoError(t, err)

	// drop a not exist role
	err = mt.CheckIfDropRole(context.TODO(), &milvuspb.DropRoleRequest{RoleName: "role_not_exist"})
	require.ErrorIs(t, err, errRoleNotExists)
}

func TestRbacOperateRole(t *testing.T) {
	mt := generateMetaTable(t)
	err := mt.CreateRole(context.TODO(), util.DefaultTenant, &milvuspb.RoleEntity{Name: "role1"})
	require.NoError(t, err)

	tests := []struct {
		description string

		user string
		role string

		oType milvuspb.OperateUserRoleType
	}{
		{"empty user", "", "role1", milvuspb.OperateUserRoleType_AddUserToRole},
		{"empty role", "user1", "", milvuspb.OperateUserRoleType_AddUserToRole},
		{"invalid type", "user1", "role1", milvuspb.OperateUserRoleType(100)},
		{"remove not exist pair", "user1", "role2", milvuspb.OperateUserRoleType_RemoveUserFromRole},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			err := mt.CheckIfOperateUserRole(context.TODO(), &milvuspb.OperateUserRoleRequest{
				Username: test.user,
				RoleName: test.role,
				Type:     test.oType,
			})
			assert.Error(t, err)
		})
	}
}

func TestRbacSelect(t *testing.T) {
	mt := generateMetaTable(t)
	roles := []string{"role1", "role2", "role3"}
	userRoles := map[string][]string{
		"user1": {"role1"},
		"user2": {"role1", "role2"},
		"user3": {"role1", "role3"},
		"user4": {},
	}

	for _, role := range roles {
		err := mt.CreateRole(context.TODO(), util.DefaultTenant, &milvuspb.RoleEntity{Name: role})
		require.NoError(t, err)
	}

	for user, rs := range userRoles {
		err := mt.Catalog().AlterCredential(context.TODO(), &model.Credential{
			Username: user,
			Tenant:   util.DefaultTenant,
		})

		require.NoError(t, err)
		for _, r := range rs {
			err := mt.OperateUserRole(
				context.TODO(),
				util.DefaultTenant,
				&milvuspb.UserEntity{Name: user},
				&milvuspb.RoleEntity{Name: r},
				milvuspb.OperateUserRoleType_AddUserToRole)
			require.NoError(t, err)
		}
	}

	tests := []struct {
		isValid     bool
		description string

		inEntity        *milvuspb.UserEntity
		includeRoleInfo bool

		expectedOutLength int
	}{
		{true, "no user entitiy, no role info", nil, false, 4},
		{true, "no user entitiy, with role info", nil, true, 4},
		{false, "not exist user", &milvuspb.UserEntity{Name: "not_exist"}, false, 0},
		{true, "user1, no role info", &milvuspb.UserEntity{Name: "user1"}, false, 1},
		{true, "user1, with role info", &milvuspb.UserEntity{Name: "user1"}, true, 1},
		{true, "user2, no role info", &milvuspb.UserEntity{Name: "user2"}, false, 1},
		{true, "user2, with role info", &milvuspb.UserEntity{Name: "user2"}, true, 1},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			res, err := mt.SelectUser(context.TODO(), util.DefaultTenant, test.inEntity, test.includeRoleInfo)

			if test.isValid {
				assert.NoError(t, err)
				assert.Equal(t, test.expectedOutLength, len(res))

				if test.includeRoleInfo {
					u := res[0].GetUser().GetName()
					roles, ok := userRoles[u]
					assert.True(t, ok)
					assert.Equal(t, len(roles), len(res[0].GetRoles()))
				}
			} else {
				assert.Error(t, err)
			}
		})
	}

	testRoles := []struct {
		isValid     bool
		description string

		inEntity        *milvuspb.RoleEntity
		includeUserInfo bool

		expectedOutLength int
	}{
		{true, "no role entitiy, no user info", nil, false, 3},
		{true, "no role entitiy, with user info", nil, true, 3},
		{false, "not exist role", &milvuspb.RoleEntity{Name: "not_exist"}, false, 0},
		{true, "role1, no user info", &milvuspb.RoleEntity{Name: "role1"}, false, 1},
		{true, "role1, with user info", &milvuspb.RoleEntity{Name: "role1"}, true, 1},
		{true, "role2, no user info", &milvuspb.RoleEntity{Name: "role2"}, false, 1},
		{true, "role2, with user info", &milvuspb.RoleEntity{Name: "role2"}, true, 1},
	}

	for _, test := range testRoles {
		t.Run(test.description, func(t *testing.T) {
			res, err := mt.SelectRole(context.TODO(), util.DefaultTenant, test.inEntity, test.includeUserInfo)

			if test.isValid {
				assert.NoError(t, err)
				assert.Equal(t, test.expectedOutLength, len(res))
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestRbacOperatePrivilege(t *testing.T) {
	mt := generateMetaTable(t)

	tests := []struct {
		description string

		entity *milvuspb.GrantEntity
		oType  milvuspb.OperatePrivilegeType
	}{
		{"empty objectName", &milvuspb.GrantEntity{ObjectName: ""}, milvuspb.OperatePrivilegeType_Grant},
		{"nil Object", &milvuspb.GrantEntity{
			Object:     nil,
			ObjectName: "obj_name",
		}, milvuspb.OperatePrivilegeType_Grant},
		{"empty Object name", &milvuspb.GrantEntity{
			Object:     &milvuspb.ObjectEntity{Name: ""},
			ObjectName: "obj_name",
		}, milvuspb.OperatePrivilegeType_Grant},
		{"nil Role", &milvuspb.GrantEntity{
			Role:       nil,
			Object:     &milvuspb.ObjectEntity{Name: "obj_name"},
			ObjectName: "obj_name",
		}, milvuspb.OperatePrivilegeType_Grant},
		{"empty Role name", &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: ""},
			Object:     &milvuspb.ObjectEntity{Name: "obj_name"},
			ObjectName: "obj_name",
		}, milvuspb.OperatePrivilegeType_Grant},
		{"nil grantor", &milvuspb.GrantEntity{
			Grantor:    nil,
			Role:       &milvuspb.RoleEntity{Name: "role_name"},
			Object:     &milvuspb.ObjectEntity{Name: "obj_name"},
			ObjectName: "obj_name",
		}, milvuspb.OperatePrivilegeType_Grant},
		{"nil grantor privilege", &milvuspb.GrantEntity{
			Grantor: &milvuspb.GrantorEntity{
				Privilege: nil,
			},
			Role:       &milvuspb.RoleEntity{Name: "role_name"},
			Object:     &milvuspb.ObjectEntity{Name: "obj_name"},
			ObjectName: "obj_name",
		}, milvuspb.OperatePrivilegeType_Grant},
		{"empty grantor privilege name", &milvuspb.GrantEntity{
			Grantor: &milvuspb.GrantorEntity{
				Privilege: &milvuspb.PrivilegeEntity{Name: ""},
			},
			Role:       &milvuspb.RoleEntity{Name: "role_name"},
			Object:     &milvuspb.ObjectEntity{Name: "obj_name"},
			ObjectName: "obj_name",
		}, milvuspb.OperatePrivilegeType_Grant},
		{"nil grantor user", &milvuspb.GrantEntity{
			Grantor: &milvuspb.GrantorEntity{
				User:      nil,
				Privilege: &milvuspb.PrivilegeEntity{Name: "privilege_name"},
			},
			Role:       &milvuspb.RoleEntity{Name: "role_name"},
			Object:     &milvuspb.ObjectEntity{Name: "obj_name"},
			ObjectName: "obj_name",
		}, milvuspb.OperatePrivilegeType_Grant},
		{"empty grantor user name", &milvuspb.GrantEntity{
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: ""},
				Privilege: &milvuspb.PrivilegeEntity{Name: "privilege_name"},
			},
			Role:       &milvuspb.RoleEntity{Name: "role_name"},
			Object:     &milvuspb.ObjectEntity{Name: "obj_name"},
			ObjectName: "obj_name",
		}, milvuspb.OperatePrivilegeType_Grant},
		{"invalid operateType", &milvuspb.GrantEntity{
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: "user_name"},
				Privilege: &milvuspb.PrivilegeEntity{Name: "privilege_name"},
			},
			Role:       &milvuspb.RoleEntity{Name: "role_name"},
			Object:     &milvuspb.ObjectEntity{Name: "obj_name"},
			ObjectName: "obj_name",
		}, milvuspb.OperatePrivilegeType(-1)},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			err := mt.OperatePrivilege(context.TODO(), util.DefaultTenant, test.entity, test.oType)
			assert.Error(t, err)
		})
	}

	validEntity := milvuspb.GrantEntity{
		Grantor: &milvuspb.GrantorEntity{
			User:      &milvuspb.UserEntity{Name: "user_name"},
			Privilege: &milvuspb.PrivilegeEntity{Name: "privilege_name"},
		},
		Role:       &milvuspb.RoleEntity{Name: "role_name"},
		Object:     &milvuspb.ObjectEntity{Name: "obj_name"},
		ObjectName: "obj_name",
	}

	err := mt.OperatePrivilege(context.TODO(), util.DefaultTenant, &validEntity, milvuspb.OperatePrivilegeType_Grant)
	assert.NoError(t, err)
}

func TestRbacSelectGrant(t *testing.T) {
	mt := generateMetaTable(t)

	tests := []struct {
		description string

		isValid bool
		entity  *milvuspb.GrantEntity
	}{
		{"nil Entity", false, nil},
		{"nil entity Role", false, &milvuspb.GrantEntity{
			Role: nil,
		}},
		{"empty entity Role name", false, &milvuspb.GrantEntity{
			Role: &milvuspb.RoleEntity{Name: ""},
		}},
		{"valid", true, &milvuspb.GrantEntity{
			Role: &milvuspb.RoleEntity{Name: "role"},
		}},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			entities, err := mt.SelectGrant(context.TODO(), util.DefaultTenant, test.entity)
			if test.isValid {
				assert.NoError(t, err)
				assert.Equal(t, 0, len(entities))
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestRbacDropGrant(t *testing.T) {
	mt := generateMetaTable(t)

	tests := []struct {
		description string

		isValid bool
		role    *milvuspb.RoleEntity
	}{
		{"nil role", false, nil},
		{"empty Role name", false, &milvuspb.RoleEntity{Name: ""}},
		{"valid", true, &milvuspb.RoleEntity{Name: "role"}},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			err := mt.DropGrant(context.TODO(), util.DefaultTenant, test.role)
			if test.isValid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestRbacListPolicy(t *testing.T) {
	mt := generateMetaTable(t)

	policies, err := mt.ListPolicy(context.TODO(), util.DefaultTenant)
	assert.NoError(t, err)
	assert.Empty(t, policies)

	userRoles, err := mt.ListUserRole(context.TODO(), util.DefaultTenant)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(userRoles))
}
