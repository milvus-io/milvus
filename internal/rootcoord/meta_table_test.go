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
	"math/rand"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	mocktso "github.com/milvus-io/milvus/internal/tso/mocks"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func generateMetaTable(_ *testing.T) *MetaTable {
	kv, _ := kvfactory.GetEtcdAndPath()
	path := funcutil.RandomString(10)
	catalogKV := etcdkv.NewEtcdKV(kv, path)
	return &MetaTable{catalog: rootcoord.NewCatalog(catalogKV)}
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
		mockMt := &MetaTable{catalog: mockCata}
		err := mockMt.CheckIfCreateRole(context.TODO(), &milvuspb.CreateRoleRequest{Entity: &milvuspb.RoleEntity{Name: "role1"}})
		assert.Error(t, err)
	}
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
		err := mt.catalog.AlterCredential(context.TODO(), &model.Credential{
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

	policies, err := mt.ListPolicy(context.TODO(), util.DefaultTenant, false)
	assert.NoError(t, err)
	assert.Empty(t, policies)

	userRoles, err := mt.ListUserRole(context.TODO(), util.DefaultTenant)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(userRoles))
}

func TestMetaTable_getCollectionByIDInternal(t *testing.T) {
	t.Run("failed to get from catalog", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("GetCollectionByID",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil, errors.New("error mock GetCollectionByID"))
		meta := &MetaTable{
			catalog: catalog,
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			collID2Meta: map[typeutil.UniqueID]*model.Collection{},
		}
		ctx := context.Background()
		_, err := meta.getCollectionByIDInternal(ctx, util.DefaultDBName, 100, 101, false)
		assert.Error(t, err)
	})

	t.Run("collection not available", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("GetCollectionByID",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(&model.Collection{State: pb.CollectionState_CollectionDropped}, nil)
		meta := &MetaTable{
			catalog: catalog,
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			collID2Meta: map[typeutil.UniqueID]*model.Collection{},
		}
		ctx := context.Background()
		_, err := meta.getCollectionByIDInternal(ctx, util.DefaultDBName, 100, 101, false)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrCollectionNotFound)
		coll, err := meta.getCollectionByIDInternal(ctx, util.DefaultDBName, 100, 101, true)
		assert.NoError(t, err)
		assert.False(t, coll.Available())
	})

	t.Run("normal case, filter unavailable partitions", func(t *testing.T) {
		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					State:      pb.CollectionState_CollectionCreated,
					CreateTime: 99,
					Partitions: []*model.Partition{
						{PartitionID: 11, PartitionName: Params.CommonCfg.DefaultPartitionName.GetValue(), State: pb.PartitionState_PartitionCreated},
						{PartitionID: 22, PartitionName: "dropped", State: pb.PartitionState_PartitionDropped},
					},
				},
			},
		}
		ctx := context.Background()
		coll, err := meta.getCollectionByIDInternal(ctx, "", 100, 101, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(coll.Partitions))
		assert.Equal(t, UniqueID(11), coll.Partitions[0].PartitionID)
		assert.Equal(t, Params.CommonCfg.DefaultPartitionName.GetValue(), coll.Partitions[0].PartitionName)
	})

	t.Run("get latest version", func(t *testing.T) {
		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					State:      pb.CollectionState_CollectionCreated,
					CreateTime: 99,
					Partitions: []*model.Partition{
						{PartitionID: 11, PartitionName: Params.CommonCfg.DefaultPartitionName.GetValue(), State: pb.PartitionState_PartitionCreated},
						{PartitionID: 22, PartitionName: "dropped", State: pb.PartitionState_PartitionDropped},
					},
				},
			},
		}
		ctx := context.Background()
		coll, err := meta.getCollectionByIDInternal(ctx, "", 100, typeutil.MaxTimestamp, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(coll.Partitions))
		assert.Equal(t, UniqueID(11), coll.Partitions[0].PartitionID)
		assert.Equal(t, Params.CommonCfg.DefaultPartitionName.GetValue(), coll.Partitions[0].PartitionName)
	})

	t.Run("UpdateTimestamp > ts triggers catalog fallback (time-travel correctness)", func(t *testing.T) {
		// Regression test for the bug fix in getCollectionByIDInternal:
		// cache invalidation was changed from CreateTime to UpdateTimestamp.
		// Scenario: collection created at T=50, schema altered at T=100.
		// A time-travel query at ts=80 (50 < 80 < 100) must NOT use the in-memory
		// cache (which holds the post-alteration schema) — it must fall back to catalog.
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("GetCollectionByID",
			mock.Anything,
			mock.Anything,
			uint64(80),
			int64(100),
		).Return(&model.Collection{
			State:           pb.CollectionState_CollectionCreated,
			CreateTime:      50,
			UpdateTimestamp: 100,
			Partitions: []*model.Partition{
				{PartitionID: 11, PartitionName: Params.CommonCfg.DefaultPartitionName.GetValue(), State: pb.PartitionState_PartitionCreated},
			},
		}, nil)

		meta := &MetaTable{
			catalog: catalog,
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					State:           pb.CollectionState_CollectionCreated,
					CreateTime:      50,
					UpdateTimestamp: 100, // schema was altered at T=100
					Partitions: []*model.Partition{
						{PartitionID: 11, PartitionName: Params.CommonCfg.DefaultPartitionName.GetValue(), State: pb.PartitionState_PartitionCreated},
					},
				},
			},
		}
		ctx := context.Background()

		// ts=80 is between CreateTime(50) and UpdateTimestamp(100):
		// UpdateTimestamp(100) > ts(80) → cache bypass → catalog must be called.
		coll, err := meta.getCollectionByIDInternal(ctx, util.DefaultDBName, 100, 80, false)
		assert.NoError(t, err)
		assert.NotNil(t, coll)
		catalog.AssertCalled(t, "GetCollectionByID", mock.Anything, mock.Anything, uint64(80), int64(100))
	})

	t.Run("UpdateTimestamp <= ts uses in-memory cache (no catalog call)", func(t *testing.T) {
		// ts=150 >= UpdateTimestamp(100) → the in-memory cache is fresh enough → no catalog call.
		catalog := mocks.NewRootCoordCatalog(t)
		// No expectations set — testify mock will fail if GetCollectionByID is called.

		meta := &MetaTable{
			catalog: catalog,
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					State:           pb.CollectionState_CollectionCreated,
					CreateTime:      50,
					UpdateTimestamp: 100,
					Partitions: []*model.Partition{
						{PartitionID: 11, PartitionName: Params.CommonCfg.DefaultPartitionName.GetValue(), State: pb.PartitionState_PartitionCreated},
					},
				},
			},
		}
		ctx := context.Background()

		coll, err := meta.getCollectionByIDInternal(ctx, "", 100, 150, false)
		assert.NoError(t, err)
		assert.NotNil(t, coll)
		catalog.AssertNotCalled(t, "GetCollectionByID")
	})
}

func TestMetaTable_GetCollectionByName(t *testing.T) {
	t.Run("db not found", func(t *testing.T) {
		meta := &MetaTable{
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					State:      pb.CollectionState_CollectionCreated,
					CreateTime: 99,
					Partitions: []*model.Partition{},
				},
			},
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
		}
		ctx := context.Background()
		_, err := meta.GetCollectionByName(ctx, "not_exist", "name", 101)
		assert.Error(t, err)
	})
	t.Run("get by alias", func(t *testing.T) {
		meta := &MetaTable{
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					State:      pb.CollectionState_CollectionCreated,
					CreateTime: 99,
					Partitions: []*model.Partition{
						{PartitionID: 11, PartitionName: Params.CommonCfg.DefaultPartitionName.GetValue(), State: pb.PartitionState_PartitionCreated},
						{PartitionID: 22, PartitionName: "dropped", State: pb.PartitionState_PartitionDropped},
					},
				},
			},
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
		}
		meta.aliases.insert(util.DefaultDBName, "alias", 100)
		ctx := context.Background()
		coll, err := meta.GetCollectionByName(ctx, "", "alias", 101)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(coll.Partitions))
		assert.Equal(t, UniqueID(11), coll.Partitions[0].PartitionID)
		assert.Equal(t, Params.CommonCfg.DefaultPartitionName.GetValue(), coll.Partitions[0].PartitionName)
	})

	t.Run("get by name", func(t *testing.T) {
		meta := &MetaTable{
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					State:      pb.CollectionState_CollectionCreated,
					CreateTime: 99,
					Partitions: []*model.Partition{
						{PartitionID: 11, PartitionName: Params.CommonCfg.DefaultPartitionName.GetValue(), State: pb.PartitionState_PartitionCreated},
						{PartitionID: 22, PartitionName: "dropped", State: pb.PartitionState_PartitionDropped},
					},
				},
			},
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
		}
		meta.names.insert(util.DefaultDBName, "name", 100)
		ctx := context.Background()
		coll, err := meta.GetCollectionByName(ctx, "", "name", 101)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(coll.Partitions))
		assert.Equal(t, UniqueID(11), coll.Partitions[0].PartitionID)
		assert.Equal(t, Params.CommonCfg.DefaultPartitionName.GetValue(), coll.Partitions[0].PartitionName)
	})

	t.Run("failed to get from catalog", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil, errors.New("error mock GetCollectionByName"))
		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			catalog: catalog,
		}
		ctx := context.Background()
		_, err := meta.GetCollectionByName(ctx, util.DefaultDBName, "name", 101)
		assert.Error(t, err)
	})

	t.Run("collection not available", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(&model.Collection{State: pb.CollectionState_CollectionDropped}, nil)
		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			catalog: catalog,
		}
		ctx := context.Background()
		_, err := meta.GetCollectionByName(ctx, util.DefaultDBName, "name", 101)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrCollectionNotFound)
	})

	t.Run("normal case, filter unavailable partitions", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(&model.Collection{
			State:      pb.CollectionState_CollectionCreated,
			CreateTime: 99,
			Partitions: []*model.Partition{
				{PartitionID: 11, PartitionName: Params.CommonCfg.DefaultPartitionName.GetValue(), State: pb.PartitionState_PartitionCreated},
				{PartitionID: 22, PartitionName: "dropped", State: pb.PartitionState_PartitionDropped},
			},
		}, nil)

		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			catalog: catalog,
		}
		ctx := context.Background()
		coll, err := meta.GetCollectionByName(ctx, util.DefaultDBName, "name", 101)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(coll.Partitions))
		assert.Equal(t, UniqueID(11), coll.Partitions[0].PartitionID)
		assert.Equal(t, Params.CommonCfg.DefaultPartitionName.GetValue(), coll.Partitions[0].PartitionName)
	})

	t.Run("get latest version", func(t *testing.T) {
		ctx := context.Background()
		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		_, err := meta.GetCollectionByName(ctx, "", "not_exist", typeutil.MaxTimestamp)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrCollectionNotFound)
	})
}

/*
func TestMetaTable_AlterCollection(t *testing.T) {
	t.Run("alter metastore fail", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterCollection",
			mock.Anything, // context.Context
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("error"))
		meta := &MetaTable{
			catalog:     catalog,
			collID2Meta: map[typeutil.UniqueID]*model.Collection{},
		}
		ctx := context.Background()
		err := meta.AlterCollection(ctx, nil, nil, 0, false, false)
		assert.Error(t, err)
	})

	t.Run("new name is already an alias", func(t *testing.T) {
		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				1: {
					CollectionID: 1,
					Name:         "old",
					DBID:         util.DefaultDBID,
					State:        pb.CollectionState_CollectionCreated,
				},
			},
		}
		meta.names.insert(util.DefaultDBName, "old", 1)
		meta.aliases.insert(util.DefaultDBName, "new", 1)

		err := meta.RenameCollection(context.TODO(), util.DefaultDBName, "old", util.DefaultDBName, "new", typeutil.MaxTimestamp)
		assert.Error(t, err)
	})

	t.Run("alter collection ok", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterCollection",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		meta := &MetaTable{
			catalog:     catalog,
			collID2Meta: map[typeutil.UniqueID]*model.Collection{},
		}
		ctx := context.Background()

		oldColl := &model.Collection{CollectionID: 1}
		newColl := &model.Collection{CollectionID: 1}
		err := meta.AlterCollection(ctx, oldColl, newColl, 0, false, false)
		assert.NoError(t, err)
		assert.Equal(t, meta.collID2Meta[1], newColl)
	})
}
*/

func TestMetaTable_DescribeAlias(t *testing.T) {
	t.Run("metatable describe alias ok", func(t *testing.T) {
		var collectionID int64 = 100
		collectionName := "test_metatable_describe_alias"
		aliasName := "a_alias"
		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				collectionID: {
					CollectionID: collectionID,
					Name:         collectionName,
				},
			},
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		meta.names.insert("", collectionName, collectionID)
		meta.aliases.insert("", aliasName, collectionID)

		ctx := context.Background()
		descCollectionName, err := meta.DescribeAlias(ctx, "", aliasName, 0)
		assert.NoError(t, err)
		assert.Equal(t, collectionName, descCollectionName)
	})

	t.Run("metatable describe not exist alias", func(t *testing.T) {
		var collectionID int64 = 100
		aliasName1 := "a_alias"
		aliasName2 := "a_alias2"
		meta := &MetaTable{
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		meta.aliases.insert("", aliasName1, collectionID)
		ctx := context.Background()
		descCollectionName, err := meta.DescribeAlias(ctx, "", aliasName2, 0)
		assert.Error(t, err)
		assert.Equal(t, "", descCollectionName)
	})

	t.Run("metatable describe not exist database", func(t *testing.T) {
		aliasName := "a_alias"
		meta := &MetaTable{
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		ctx := context.Background()
		descCollectionName, err := meta.DescribeAlias(ctx, "", aliasName, 0)
		assert.Error(t, err)
		assert.Equal(t, "", descCollectionName)
	})

	t.Run("metatable describe alias fail", func(t *testing.T) {
		var collectionID int64 = 100
		collectionName := "test_metatable_describe_alias"
		aliasName := "a_alias"
		meta := &MetaTable{
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		meta.names.insert("", collectionName, collectionID)
		meta.aliases.insert("", aliasName, collectionID)
		ctx := context.Background()
		_, err := meta.DescribeAlias(ctx, "", aliasName, 0)
		assert.Error(t, err)
	})

	t.Run("metatable describe alias dropped collection", func(t *testing.T) {
		var collectionID int64 = 100
		collectionName := "test_metatable_describe_alias"
		aliasName := "a_alias"
		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				collectionID: {
					CollectionID: collectionID,
					Name:         collectionName,
				},
			},
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		meta.names.insert("", collectionName, collectionID)
		meta.aliases.insert("", aliasName, collectionID)

		ctx := context.Background()
		meta.collID2Meta[collectionID] = &model.Collection{State: pb.CollectionState_CollectionDropped}
		alias, err := meta.DescribeAlias(ctx, "", aliasName, 0)
		assert.Equal(t, "", alias)
		assert.Error(t, err)
	})
}

func TestMetaTable_ListAliases(t *testing.T) {
	t.Run("metatable list alias ok", func(t *testing.T) {
		var collectionID1 int64 = 101
		collectionName1 := "test_metatable_list_alias1"
		aliasName1 := "a_alias"
		var collectionID2 int64 = 102
		collectionName2 := "test_metatable_list_alias2"
		aliasName2 := "a_alias2"
		var collectionID3 int64 = 103
		collectionName3 := "test_metatable_list_alias3"
		aliasName3 := "a_alias3"
		aliasName4 := "a_alias4"
		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				collectionID1: {
					CollectionID: collectionID1,
					Name:         collectionName1,
				},
				collectionID1: {
					CollectionID: collectionID2,
					Name:         collectionName2,
				},
			},
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		meta.names.insert("", collectionName1, collectionID1)
		meta.names.insert("", collectionName2, collectionID2)
		meta.names.insert("db2", collectionName3, collectionID3)

		meta.aliases.insert("", aliasName1, collectionID1)
		meta.aliases.insert("", aliasName2, collectionID2)
		meta.aliases.insert("db2", aliasName3, collectionID3)
		meta.aliases.insert("db2", aliasName4, collectionID3)

		meta.collID2Meta[collectionID1] = &model.Collection{State: pb.CollectionState_CollectionCreated}
		meta.collID2Meta[collectionID2] = &model.Collection{State: pb.CollectionState_CollectionCreated}
		meta.collID2Meta[collectionID3] = &model.Collection{State: pb.CollectionState_CollectionCreated}

		ctx := context.Background()
		aliases, err := meta.ListAliases(ctx, "", "", 0)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(aliases))

		aliases2, err := meta.ListAliases(ctx, "", collectionName1, 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(aliases2))

		aliases3, err := meta.ListAliases(ctx, "db2", "", 0)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(aliases3))

		aliases4, err := meta.ListAliases(ctx, "db2", collectionName3, 0)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(aliases4))
	})

	t.Run("metatable list alias in not exist database", func(t *testing.T) {
		aliasName := "a_alias"
		meta := &MetaTable{
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		ctx := context.Background()
		aliases, err := meta.ListAliases(ctx, "", aliasName, 0)
		assert.Error(t, err)
		assert.Equal(t, 0, len(aliases))
	})

	t.Run("metatable list alias error", func(t *testing.T) {
		var collectionID1 int64 = 101
		collectionName1 := "test_metatable_list_alias1"
		aliasName1 := "a_alias"
		var collectionID2 int64 = 102
		collectionName2 := "test_metatable_list_alias2"
		aliasName2 := "a_alias2"
		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				collectionID1: {
					CollectionID: collectionID1,
					Name:         collectionName1,
				},
				collectionID1: {
					CollectionID: collectionID2,
					Name:         collectionName2,
				},
			},
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		meta.aliases.insert("", aliasName1, collectionID1)
		meta.aliases.insert("", aliasName2, collectionID2)
		ctx := context.Background()
		_, err := meta.ListAliases(ctx, "", collectionName1, 0)
		assert.Error(t, err)
	})

	t.Run("metatable list alias Dropping collection", func(t *testing.T) {
		ctx := context.Background()

		var collectionID1 int64 = 101
		collectionName1 := "test_metatable_list_alias1"
		aliasName1 := "a_alias"
		var collectionID2 int64 = 102
		collectionName2 := "test_metatable_list_alias2"
		aliasName2 := "a_alias2"
		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				collectionID1: {
					CollectionID: collectionID1,
					Name:         collectionName1,
				},
				collectionID1: {
					CollectionID: collectionID2,
					Name:         collectionName2,
				},
			},
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		meta.names.insert("", collectionName1, collectionID1)
		meta.names.insert("", collectionName2, collectionID2)
		meta.aliases.insert("", aliasName1, collectionID1)
		meta.aliases.insert("", aliasName2, collectionID2)
		meta.collID2Meta[collectionID1] = &model.Collection{State: pb.CollectionState_CollectionCreated}
		meta.collID2Meta[collectionID2] = &model.Collection{State: pb.CollectionState_CollectionDropped}

		aliases, err := meta.ListAliases(ctx, "", "", 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(aliases))

		aliases2, err := meta.ListAliases(ctx, "", collectionName1, 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(aliases2))
	})
}

func Test_filterUnavailable(t *testing.T) {
	coll := &model.Collection{}
	nPartition := 10
	nAvailablePartition := 0
	for i := 0; i < nPartition; i++ {
		partition := &model.Partition{
			State: pb.PartitionState_PartitionDropping,
		}
		if rand.Int()%2 == 0 {
			partition.State = pb.PartitionState_PartitionCreated
			nAvailablePartition++
		}
		coll.Partitions = append(coll.Partitions, partition)
	}
	clone := filterUnavailablePartition(coll)
	assert.Equal(t, nAvailablePartition, len(clone.Partitions))
	for _, p := range clone.Partitions {
		assert.True(t, p.Available())
	}
}

func TestMetaTable_getLatestCollectionByIDInternal(t *testing.T) {
	t.Run("not exist", func(t *testing.T) {
		ctx := context.Background()
		mt := &MetaTable{collID2Meta: nil}
		_, err := mt.getLatestCollectionByIDInternal(ctx, 100, false)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrCollectionNotFound)
	})

	t.Run("nil case", func(t *testing.T) {
		ctx := context.Background()
		mt := &MetaTable{collID2Meta: map[typeutil.UniqueID]*model.Collection{
			100: nil,
		}}
		_, err := mt.getLatestCollectionByIDInternal(ctx, 100, false)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrCollectionNotFound)
	})

	t.Run("unavailable", func(t *testing.T) {
		ctx := context.Background()
		mt := &MetaTable{collID2Meta: map[typeutil.UniqueID]*model.Collection{
			100: {State: pb.CollectionState_CollectionDropping},
		}}
		_, err := mt.getLatestCollectionByIDInternal(ctx, 100, false)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrCollectionNotFound)
		coll, err := mt.getLatestCollectionByIDInternal(ctx, 100, true)
		assert.NoError(t, err)
		assert.False(t, coll.Available())
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()
		mt := &MetaTable{collID2Meta: map[typeutil.UniqueID]*model.Collection{
			100: {
				State: pb.CollectionState_CollectionCreated,
				Partitions: []*model.Partition{
					{State: pb.PartitionState_PartitionCreated},
					{State: pb.PartitionState_PartitionDropping},
				},
			},
		}}
		coll, err := mt.getLatestCollectionByIDInternal(ctx, 100, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(coll.Partitions))
	})
}

func TestMetaTable_RemoveCollection(t *testing.T) {
	t.Run("catalog error", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("DropCollection",
			mock.Anything, // context.Context
			mock.Anything, // model.Collection
			mock.AnythingOfType("uint64"),
		).Return(errors.New("error mock DropCollection"))

		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					CollectionID: 100,
					DBID:         int64(100),
				},
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			catalog: catalog,
		}

		ctx := context.Background()
		err := meta.RemoveCollection(ctx, 100, 9999)
		assert.Error(t, err)

		meta.collID2Meta[100].State = pb.CollectionState_CollectionDropping
		err = meta.RemoveCollection(ctx, 100, 9999)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("DropCollection",
			mock.Anything, // context.Context
			mock.Anything, // model.Collection
			mock.AnythingOfType("uint64"),
		).Return(nil)
		catalog.On("DeleteGrantByCollectionID",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		meta := &MetaTable{
			catalog: catalog,
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {Name: "collection", State: pb.CollectionState_CollectionDropping},
			},
		}
		channel.ResetStaticPChannelStatsManager()
		channel.RecoverPChannelStatsManager([]string{})
		meta.names.insert("", "collection", 100)
		meta.names.insert("", "alias1", 100)
		meta.names.insert("", "alias2", 100)
		ctx := context.Background()
		err := meta.RemoveCollection(ctx, 100, 9999)
		assert.NoError(t, err)
	})
}

func TestMetaTable_RemoveCollection_GrantDeleteBestEffort(t *testing.T) {
	// When DeleteGrantByCollectionID fails, RemoveCollection should still succeed (best-effort)
	catalog := mocks.NewRootCoordCatalog(t)
	catalog.On("DropCollection",
		mock.Anything,
		mock.Anything,
		mock.AnythingOfType("uint64"),
	).Return(nil)
	catalog.On("DeleteGrantByCollectionID",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(errors.New("grant delete failed"))

	meta := &MetaTable{
		catalog:            catalog,
		names:              newNameDb(),
		aliases:            newNameDb(),
		fileResourceRefCnt: make(map[int64]int),
		collID2Meta: map[typeutil.UniqueID]*model.Collection{
			100: {Name: "collection", State: pb.CollectionState_CollectionDropping},
		},
	}
	channel.ResetStaticPChannelStatsManager()
	channel.RecoverPChannelStatsManager([]string{})
	meta.names.insert("", "collection", 100)
	ctx := context.Background()
	err := meta.RemoveCollection(ctx, 100, 9999)
	assert.NoError(t, err)
}

func TestMetaTable_DropCollection_GrantCleanup(t *testing.T) {
	t.Run("grant cleanup on drop", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterCollection",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		catalog.On("DeleteGrantByCollectionID",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)

		meta := &MetaTable{
			catalog: catalog,
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {Name: "collection", DBID: 1, State: pb.CollectionState_CollectionCreated},
			},
			dbName2Meta: map[string]*model.Database{
				"testdb": {ID: 1, Name: "testdb"},
			},
			fileResourceRefCnt: make(map[int64]int),
		}
		channel.ResetStaticPChannelStatsManager()
		channel.RecoverPChannelStatsManager([]string{})
		meta.names.insert("testdb", "collection", 100)
		ctx := context.Background()
		err := meta.DropCollection(ctx, 100, 9999)
		assert.NoError(t, err)
		catalog.AssertCalled(t, "DeleteGrantByCollectionID", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("grant cleanup best-effort on drop", func(t *testing.T) {
		// When DeleteGrantByCollectionID fails, DropCollection should still succeed
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterCollection",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		catalog.On("DeleteGrantByCollectionID",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(errors.New("grant delete failed"))

		meta := &MetaTable{
			catalog: catalog,
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {Name: "collection", DBID: 1, State: pb.CollectionState_CollectionCreated},
			},
			dbName2Meta: map[string]*model.Database{
				"default": {ID: 1, Name: "default"},
			},
			fileResourceRefCnt: make(map[int64]int),
		}
		channel.ResetStaticPChannelStatsManager()
		channel.RecoverPChannelStatsManager([]string{})
		meta.names.insert("default", "collection", 100)
		ctx := context.Background()
		err := meta.DropCollection(ctx, 100, 9999)
		assert.NoError(t, err)
	})
}

func TestMetaTable_RemovePartition(t *testing.T) {
	t.Run("catalog error", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("DropPartition",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.AnythingOfType("uint64"),
		).Return(errors.New("error mock AlterPartition"))

		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					CollectionID: 100,
					DBID:         int64(100),
					Partitions: []*model.Partition{
						{PartitionID: 100, State: pb.PartitionState_PartitionCreated},
					},
				},
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			catalog: catalog,
		}

		ctx := context.Background()
		err := meta.RemovePartition(ctx, 100, 100, 9999)
		assert.Error(t, err)

		meta.collID2Meta[100].Partitions[0].State = pb.PartitionState_PartitionDropping
		err = meta.RemovePartition(ctx, 100, 100, 9999)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("DropPartition",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.AnythingOfType("uint64"),
		).Return(nil)
		meta := &MetaTable{
			catalog: catalog,
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					Name: "collection",
					Partitions: []*model.Partition{
						{PartitionID: 100, State: pb.PartitionState_PartitionDropping},
					},
				},
			},
		}
		channel.ResetStaticPChannelStatsManager()
		channel.RecoverPChannelStatsManager([]string{})
		meta.names.insert("", "collection", 100)
		meta.names.insert("", "alias1", 100)
		meta.names.insert("", "alias2", 100)
		ctx := context.Background()
		err := meta.RemovePartition(ctx, 100, 100, 9999)
		assert.NoError(t, err)
	})
}

func TestMetaTable_reload(t *testing.T) {
	createMetaTableFn := func(catalogOpts ...func(*mocks.RootCoordCatalog)) *MetaTable {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("ListDatabases",
			mock.Anything,
			mock.Anything,
		).Return(make([]*model.Database, 0), nil)
		catalog.On("CreateDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)

		tso := mocktso.NewAllocator(t)
		tso.On("GenerateTSO",
			mock.Anything,
		).Return(uint64(1), nil)

		for _, opt := range catalogOpts {
			opt(catalog)
		}
		return &MetaTable{
			names:        newNameDb(),
			aliases:      newNameDb(),
			catalog:      catalog,
			tsoAllocator: tso,
		}
	}

	t.Run("list db fail", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("ListDatabases",
			mock.Anything,
			mock.Anything,
		).Return(nil, errors.New("error mock ListDatabases"))

		meta := &MetaTable{catalog: catalog}
		err := meta.reload()
		assert.Error(t, err)
		assert.Empty(t, meta.collID2Meta)

		assert.Equal(t, 0, len(meta.names.listDB()))
		assert.Equal(t, 0, len(meta.aliases.listDB()))
	})

	t.Run("failed to list collections", func(t *testing.T) {
		meta := createMetaTableFn(func(catalog *mocks.RootCoordCatalog) {
			catalog.On("ListCollections",
				mock.Anything,
				mock.Anything,
				mock.Anything,
			).Return(nil, errors.New("error mock ListCollections"))
		})

		err := meta.reload()
		assert.Error(t, err)
		assert.Empty(t, meta.collID2Meta)
	})

	t.Run("failed to list aliases", func(t *testing.T) {
		meta := createMetaTableFn(
			func(catalog *mocks.RootCoordCatalog) {
				catalog.On("ListCollections",
					mock.Anything,
					mock.Anything,
					mock.Anything,
				).Return(
					[]*model.Collection{{CollectionID: 100, Name: "test"}},
					nil)
			},
			func(catalog *mocks.RootCoordCatalog) {
				catalog.On("ListAliases",
					mock.Anything,
					mock.Anything,
					mock.Anything,
				).Return(nil, errors.New("error mock ListAliases"))
			},
		)

		err := meta.reload()
		assert.Error(t, err)
	})

	t.Run("no collections and default db doesn't exists", func(t *testing.T) {
		meta := createMetaTableFn(
			func(catalog *mocks.RootCoordCatalog) {
				catalog.On("ListCollections",
					mock.Anything,
					mock.Anything,
					mock.Anything,
				).Return(make([]*model.Collection, 0), nil)
			},
			func(catalog *mocks.RootCoordCatalog) {
				catalog.On("ListAliases",
					mock.Anything,
					mock.Anything,
					mock.Anything,
				).Return(
					[]*model.Alias{},
					nil)
			},
			func(catalog *mocks.RootCoordCatalog) {
				catalog.On("ListFileResource",
					mock.Anything,
				).Return(nil, uint64(0), nil)
			},
		)
		channel.ResetStaticPChannelStatsManager()
		err := meta.reload()
		assert.NoError(t, err)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(meta.collID2Meta))

		assert.Equal(t, 1, len(meta.names.listDB()))
		assert.True(t, meta.names.exist("default"))
		assert.Equal(t, 1, len(meta.aliases.listDB()))
		assert.True(t, meta.aliases.exist("default"))
	})

	t.Run("no collections and default db already exists", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("ListDatabases",
			mock.Anything,
			mock.Anything,
		).Return([]*model.Database{model.NewDefaultDatabase(nil)}, nil)
		catalog.On("ListCollections",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(
			[]*model.Collection{{CollectionID: 100, Name: "test", State: pb.CollectionState_CollectionCreated}},
			nil)
		catalog.On("ListAliases",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(
			[]*model.Alias{{Name: "alias", CollectionID: 100}},
			nil)
		catalog.On("ListFileResource",
			mock.Anything,
		).Return(nil, uint64(0), nil)

		meta := &MetaTable{catalog: catalog}
		channel.ResetStaticPChannelStatsManager()
		err := meta.reload()
		assert.NoError(t, err)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(meta.collID2Meta))

		assert.Equal(t, 1, len(meta.names.listDB()))
		assert.True(t, meta.names.exist(util.DefaultDBName))
		assert.Equal(t, 1, len(meta.aliases.listDB()))
		assert.True(t, meta.aliases.exist(util.DefaultDBName))
	})

	t.Run("ok", func(t *testing.T) {
		meta := createMetaTableFn(
			func(catalog *mocks.RootCoordCatalog) {
				catalog.On("ListCollections",
					mock.Anything,
					mock.Anything,
					mock.Anything,
				).Return(
					[]*model.Collection{{CollectionID: 100, Name: "test", State: pb.CollectionState_CollectionCreated}},
					nil)
			},
			func(catalog *mocks.RootCoordCatalog) {
				catalog.On("ListAliases",
					mock.Anything,
					mock.Anything,
					mock.Anything,
				).Return(
					[]*model.Alias{{Name: "alias", CollectionID: 100}},
					nil)
			},
			func(catalog *mocks.RootCoordCatalog) {
				catalog.On("ListFileResource",
					mock.Anything,
				).Return(nil, uint64(0), nil)
			},
		)

		channel.ResetStaticPChannelStatsManager()
		err := meta.reload()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(meta.collID2Meta))

		assert.Equal(t, 1, len(meta.names.listDB()))
		assert.True(t, meta.names.exist(util.DefaultDBName))
		assert.Equal(t, 1, len(meta.aliases.listDB()))
		assert.True(t, meta.aliases.exist(util.DefaultDBName))

		colls, err := meta.names.listCollectionID(util.DefaultDBName)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(colls))
		assert.Equal(t, int64(100), colls[0])

		colls, err = meta.aliases.listCollectionID(util.DefaultDBName)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(colls))
		assert.Equal(t, int64(100), colls[0])
	})
}

func TestMetaTable_ListAllAvailCollections(t *testing.T) {
	meta := &MetaTable{
		dbName2Meta: map[string]*model.Database{
			util.DefaultDBName: {
				ID: util.DefaultDBID,
			},
			"db2": {
				ID: 11,
			},
			"db3": {
				ID: 2,
			},
			"db4": {
				ID: 1111,
			},
		},
		collID2Meta: map[typeutil.UniqueID]*model.Collection{
			111: {
				CollectionID: 111,
				DBID:         1111,
				State:        pb.CollectionState_CollectionDropped,
			},
			2: {
				CollectionID: 2,
				DBID:         11,
				State:        pb.CollectionState_CollectionCreated,
			},
			3: {
				CollectionID: 3,
				DBID:         11,
				State:        pb.CollectionState_CollectionCreated,
			},
			4: {
				CollectionID: 4,
				DBID:         2,
				State:        pb.CollectionState_CollectionCreated,
			},
			5: {
				CollectionID: 5,
				DBID:         util.NonDBID,
				State:        pb.CollectionState_CollectionCreated,
			},
		},
	}

	ret := meta.ListAllAvailCollections(context.TODO())
	assert.Equal(t, 4, len(ret))
	db0, ok := ret[util.DefaultDBID]
	assert.True(t, ok)
	assert.Equal(t, 1, len(db0))
	db1, ok := ret[11]
	assert.True(t, ok)
	assert.Equal(t, 2, len(db1))
	db2, ok2 := ret[2]
	assert.True(t, ok2)
	assert.Equal(t, 1, len(db2))
	db3, ok := ret[1111]
	assert.True(t, ok)
	assert.Equal(t, 0, len(db3))
}

func TestMetaTable_AddPartition(t *testing.T) {
	t.Run("collection not available", func(t *testing.T) {
		meta := &MetaTable{}
		err := meta.AddPartition(context.TODO(), &model.Partition{CollectionID: 100})
		assert.Error(t, err)
	})

	t.Run("add not-created partition", func(t *testing.T) {
		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					Name:         "test",
					CollectionID: 100,
				},
			},
		}
		err := meta.AddPartition(context.TODO(), &model.Partition{CollectionID: 100, State: pb.PartitionState_PartitionDropping})
		assert.Error(t, err)
	})

	t.Run("failed to create partition", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("CreatePartition",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("error mock CreatePartition"))
		meta := &MetaTable{
			catalog: catalog,
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {Name: "test", CollectionID: 100},
			},
		}
		err := meta.AddPartition(context.TODO(), &model.Partition{CollectionID: 100, State: pb.PartitionState_PartitionCreated})
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("CreatePartition",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		meta := &MetaTable{
			catalog: catalog,
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {Name: "test", CollectionID: 100},
			},
		}
		err := meta.AddPartition(context.TODO(), &model.Partition{CollectionID: 100, State: pb.PartitionState_PartitionCreated})
		assert.NoError(t, err)
	})
}

/*
func TestMetaTable_RenameCollection(t *testing.T) {
	t.Run("unsupported use a alias to rename collection", func(t *testing.T) {
		meta := &MetaTable{
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		meta.names.insert("", "alias", 1)
		err := meta.RenameCollection(context.TODO(), "", "alias", "", "new", typeutil.MaxTimestamp)
		assert.Error(t, err)
	})

	t.Run("target db doesn't exist", func(t *testing.T) {
		meta := &MetaTable{
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		err := meta.RenameCollection(context.TODO(), "", "non-exists", "non-exists", "new", typeutil.MaxTimestamp)
		assert.Error(t, err)
	})

	t.Run("collection name not exist", func(t *testing.T) {
		meta := &MetaTable{
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		err := meta.RenameCollection(context.TODO(), "", "non-exists", "", "new", typeutil.MaxTimestamp)
		assert.Error(t, err)
	})

	t.Run("collection id not exist", func(t *testing.T) {
		meta := &MetaTable{
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		meta.names.insert("", "old", 1)
		err := meta.RenameCollection(context.TODO(), "", "old", "", "new", typeutil.MaxTimestamp)
		assert.Error(t, err)
	})

	t.Run("new collection name already exist-1", func(t *testing.T) {
		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				util.DefaultDBID: {
					CollectionID: 1,
					Name:         "old",
					State:        pb.CollectionState_CollectionCreated,
				},
			},
		}
		meta.names.insert(util.DefaultDBName, "old", 1)
		err := meta.RenameCollection(context.TODO(), util.DefaultDBName, "old", util.DefaultDBName, "old", 1000)
		assert.Error(t, err)
	})

	t.Run("new collection name already exist-2", func(t *testing.T) {
		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				1: {
					CollectionID: 1,
					Name:         "old",
					State:        pb.CollectionState_CollectionCreated,
				},
				2: {
					CollectionID: 2,
					Name:         "new",
					State:        pb.CollectionState_CollectionCreated,
				},
			},
		}
		meta.names.insert(util.DefaultDBName, "old", 1)
		meta.names.insert(util.DefaultDBName, "new", 2)
		err := meta.RenameCollection(context.TODO(), util.DefaultDBName, "old", util.DefaultDBName, "new", 1000)
		assert.Error(t, err)
	})

	t.Run("alter collection fail", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterCollection",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("fail"))

		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			catalog: catalog,
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				1: {
					CollectionID: 1,
					Name:         "old",
					DBID:         util.DefaultDBID,
					State:        pb.CollectionState_CollectionCreated,
				},
			},
		}
		meta.names.insert(util.DefaultDBName, "old", 1)
		err := meta.RenameCollection(context.TODO(), util.DefaultDBName, "old", util.DefaultDBName, "new", typeutil.MaxTimestamp)
		assert.Error(t, err)
	})

	t.Run("new name is already an alias", func(t *testing.T) {
		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				1: {
					CollectionID: 1,
					Name:         "old",
					DBID:         util.DefaultDBID,
					State:        pb.CollectionState_CollectionCreated,
				},
			},
		}
		meta.names.insert(util.DefaultDBName, "old", 1)
		meta.aliases.insert(util.DefaultDBName, "new", 1)

		err := meta.RenameCollection(context.TODO(), util.DefaultDBName, "old", util.DefaultDBName, "new", typeutil.MaxTimestamp)
		assert.Error(t, err)
	})

	t.Run("alter collection ok", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterCollectionDB",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		catalog.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil, merr.WrapErrCollectionNotFound("error"))
		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			catalog: catalog,
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				1: {
					CollectionID: 1,
					Name:         "old",
				},
			},
		}
		meta.names.insert(util.DefaultDBName, "old", 1)
		err := meta.RenameCollection(context.TODO(), util.DefaultDBName, "old", util.DefaultDBName, "new", 1000)
		assert.NoError(t, err)

		id, ok := meta.names.get(util.DefaultDBName, "new")
		assert.True(t, ok)
		assert.Equal(t, int64(1), id)

		coll, ok := meta.collID2Meta[1]
		assert.True(t, ok)
		assert.Equal(t, "new", coll.Name)
	})

	t.Run("rename collection ok", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterCollection",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		catalog.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil, merr.WrapErrCollectionNotFound("error"))
		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
			catalog: catalog,
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				1: {
					CollectionID: 1,
					DBID:         1,
					Name:         "old",
				},
			},
		}
		meta.names.insert(util.DefaultDBName, "old", 1)
		err := meta.RenameCollection(context.TODO(), util.DefaultDBName, "old", util.DefaultDBName, "new", 1000)
		assert.NoError(t, err)

		id, ok := meta.names.get(util.DefaultDBName, "new")
		assert.True(t, ok)
		assert.Equal(t, int64(1), id)

		coll, ok := meta.collID2Meta[1]
		assert.True(t, ok)
		assert.Equal(t, "new", coll.Name)
	})

	t.Run("rename collection rename collName with db encryption on", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterCollection",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		catalog.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil, merr.WrapErrCollectionNotFound("error"))

		// Create database with encryption enabled
		encryptedDBProperties := []*commonpb.KeyValuePair{
			{Key: "cipher.enabled", Value: "true"},
		}
		encryptedDB := model.NewDatabase(1, util.DefaultDBName, pb.DatabaseState_DatabaseCreated, encryptedDBProperties)

		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: encryptedDB,
			},
			catalog: catalog,
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				1: {
					CollectionID: 1,
					DBID:         1,
					Name:         "old",
				},
			},
		}
		meta.names.insert(util.DefaultDBName, "old", 1)

		// Should succeed when renaming within the same encrypted database
		err := meta.RenameCollection(context.TODO(), util.DefaultDBName, "old", util.DefaultDBName, "new", 1000)
		assert.NoError(t, err)

		// Verify the collection name was updated
		id, ok := meta.names.get(util.DefaultDBName, "new")
		assert.True(t, ok)
		assert.Equal(t, int64(1), id)

		coll, ok := meta.collID2Meta[1]
		assert.True(t, ok)
		assert.Equal(t, "new", coll.Name)
	})
}
*/

func TestMetaTable_CreateDatabase(t *testing.T) {
	db := model.NewDatabase(1, "exist", pb.DatabaseState_DatabaseCreated, nil)
	t.Run("database already exist", func(t *testing.T) {
		meta := &MetaTable{
			names:       newNameDb(),
			aliases:     newNameDb(),
			dbName2Meta: make(map[string]*model.Database),
		}
		meta.names.insert("exist", "collection", 100)

		err := meta.CheckIfDatabaseCreatable(context.TODO(), &milvuspb.CreateDatabaseRequest{
			DbName: "exist",
		})
		assert.Error(t, err)
	})

	t.Run("database not persistent", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("CreateDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("error mock CreateDatabase"))
		meta := &MetaTable{
			names:   newNameDb(),
			aliases: newNameDb(),
			catalog: catalog,
		}
		err := meta.CreateDatabase(context.TODO(), db, 10000)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("CreateDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		meta := &MetaTable{
			dbName2Meta: make(map[string]*model.Database),
			names:       newNameDb(),
			aliases:     newNameDb(),
			catalog:     catalog,
		}
		err := meta.CheckIfDatabaseCreatable(context.TODO(), &milvuspb.CreateDatabaseRequest{
			DbName: "exist",
		})
		assert.NoError(t, err)
		err = meta.CreateDatabase(context.TODO(), db, 10000)
		assert.NoError(t, err)
		assert.True(t, meta.names.exist("exist"))
		assert.True(t, meta.aliases.exist("exist"))
		assert.True(t, meta.names.empty("exist"))
		assert.True(t, meta.aliases.empty("exist"))
	})
}

func TestCreateDefaultDb(t *testing.T) {
	hookutil.InitTestCipher()

	// Save original config and restore after test
	originalDefaultKey := paramtable.GetCipherParams().DefaultRootKey.GetValue()
	defer func() {
		paramtable.GetCipherParams().Save("cipherPlugin.kms.defaultKey", originalDefaultKey)
	}()

	t.Run("default db without encryption when defaultKey is empty", func(t *testing.T) {
		paramtable.GetCipherParams().Save("cipherPlugin.kms.defaultKey", "")

		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("CreateDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)

		tsoAllocator := mocktso.NewAllocator(t)
		tsoAllocator.On("GenerateTSO", mock.Anything).Return(uint64(100), nil)

		meta := &MetaTable{
			ctx:          context.Background(),
			dbName2Meta:  make(map[string]*model.Database),
			names:        newNameDb(),
			aliases:      newNameDb(),
			catalog:      catalog,
			tsoAllocator: tsoAllocator,
		}

		err := meta.createDefaultDb()
		assert.NoError(t, err)

		// Verify default database was created
		db, ok := meta.dbName2Meta[util.DefaultDBName]
		assert.True(t, ok)
		assert.Equal(t, util.DefaultDBName, db.Name)

		// Verify no encryption properties
		hasEncryption := hookutil.IsDBEncrypted(db.Properties)
		assert.False(t, hasEncryption, "default DB should not be encrypted when defaultKey is empty")
	})

	t.Run("default db with encryption when defaultKey is set", func(t *testing.T) {
		paramtable.GetCipherParams().Save("cipherPlugin.kms.defaultKey", "default-test-key")

		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("CreateDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)

		tsoAllocator := mocktso.NewAllocator(t)
		tsoAllocator.On("GenerateTSO", mock.Anything).Return(uint64(200), nil)

		meta := &MetaTable{
			ctx:          context.Background(),
			dbName2Meta:  make(map[string]*model.Database),
			names:        newNameDb(),
			aliases:      newNameDb(),
			catalog:      catalog,
			tsoAllocator: tsoAllocator,
		}

		err := meta.createDefaultDb()
		assert.NoError(t, err)

		// Verify default database was created
		db, ok := meta.dbName2Meta[util.DefaultDBName]
		assert.True(t, ok)
		assert.Equal(t, util.DefaultDBName, db.Name)

		// Verify encryption properties are present
		hasEzID := false
		hasRootKey := false
		for _, prop := range db.Properties {
			if prop.Key == common.EncryptionEzIDKey {
				hasEzID = true
				assert.Equal(t, prop.GetValue(), "199")
			}
			if prop.Key == common.EncryptionRootKeyKey && prop.Value == "default-test-key" {
				hasRootKey = true
			}
		}
		assert.True(t, hasRootKey, "default DB should have root key when encrypted")
		assert.True(t, hasEzID, "default DB should have ezID when encrypted")
	})

	t.Run("TSO allocation failure", func(t *testing.T) {
		tsoAllocator := mocktso.NewAllocator(t)
		tsoAllocator.On("GenerateTSO", mock.Anything).Return(uint64(0), errors.New("TSO allocation failed"))

		meta := &MetaTable{
			ctx:          context.Background(),
			dbName2Meta:  make(map[string]*model.Database),
			tsoAllocator: tsoAllocator,
		}

		err := meta.createDefaultDb()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "TSO allocation failed")
	})

	t.Run("catalog CreateDatabase failure", func(t *testing.T) {
		paramtable.GetCipherParams().Save("cipherPlugin.kms.defaultKey", "")

		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("CreateDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("catalog error"))

		tsoAllocator := mocktso.NewAllocator(t)
		tsoAllocator.On("GenerateTSO", mock.Anything).Return(uint64(300), nil)

		meta := &MetaTable{
			ctx:          context.Background(),
			dbName2Meta:  make(map[string]*model.Database),
			names:        newNameDb(),
			aliases:      newNameDb(),
			catalog:      catalog,
			tsoAllocator: tsoAllocator,
		}

		err := meta.createDefaultDb()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "catalog error")
	})
}

func TestAlterDatabase(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)

		db := model.NewDatabase(1, "db1", pb.DatabaseState_DatabaseCreated, nil)

		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				"db1": db,
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			catalog: catalog,
		}
		newDB := db.Clone()
		db.Properties = []*commonpb.KeyValuePair{
			{
				Key:   "key1",
				Value: "value1",
			},
		}
		err := meta.AlterDatabase(context.TODO(), newDB, typeutil.ZeroTimestamp)
		assert.NoError(t, err)
	})

	t.Run("access catalog failed", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		mockErr := errors.New("access catalog failed")
		catalog.On("AlterDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(mockErr)

		db := model.NewDatabase(1, "db1", pb.DatabaseState_DatabaseCreated, nil)

		meta := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				"db1": db,
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			catalog: catalog,
		}
		newDB := db.Clone()
		db.Properties = []*commonpb.KeyValuePair{
			{
				Key:   "key1",
				Value: "value1",
			},
		}
		err := meta.AlterDatabase(context.TODO(), newDB, typeutil.ZeroTimestamp)
		assert.ErrorIs(t, err, mockErr)
	})
}

func TestMetaTable_EmtpyDatabaseName(t *testing.T) {
	t.Run("getDatabaseByNameInternal with empty db", func(t *testing.T) {
		mt := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: {ID: 1},
			},
		}

		ret, err := mt.getDatabaseByNameInternal(context.TODO(), "", typeutil.MaxTimestamp)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), ret.ID)
	})

	t.Run("getCollectionByNameInternal with empty db", func(t *testing.T) {
		mt := &MetaTable{
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				1: {CollectionID: 1},
			},
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
			},
		}

		mt.aliases.insert(util.DefaultDBName, "aliases", 1)
		ret, err := mt.getCollectionByNameInternal(context.TODO(), "", "aliases", typeutil.MaxTimestamp)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), ret.CollectionID)
	})

	t.Run("listCollectionFromCache with empty db", func(t *testing.T) {
		mt := &MetaTable{
			names: newNameDb(),
			dbName2Meta: map[string]*model.Database{
				util.DefaultDBName: model.NewDefaultDatabase(nil),
				"db2":              model.NewDatabase(2, "db2", pb.DatabaseState_DatabaseCreated, nil),
			},
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				1: {
					CollectionID: 1,
					State:        pb.CollectionState_CollectionCreated,
				},
				2: {
					CollectionID: 2,
					State:        pb.CollectionState_CollectionDropping,
					DBID:         util.DefaultDBID,
				},
				3: {
					CollectionID: 3,
					State:        pb.CollectionState_CollectionCreated,
					DBID:         2,
				},
			},
		}

		ret, err := mt.listCollectionFromCache(context.TODO(), "none", false)
		assert.Error(t, err)
		assert.Nil(t, ret)

		ret, err = mt.listCollectionFromCache(context.TODO(), "", false)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(ret))
		assert.Contains(t, []int64{1, 2}, ret[0].CollectionID)
		assert.Contains(t, []int64{1, 2}, ret[1].CollectionID)

		ret, err = mt.listCollectionFromCache(context.TODO(), "db2", false)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(ret))
		assert.Equal(t, int64(3), ret[0].CollectionID)
	})

	t.Run("CreateAlias with empty db", func(t *testing.T) {
		mt := &MetaTable{
			names: newNameDb(),
		}

		mt.names.insert(util.DefaultDBName, "name", 1)
		err := mt.CheckIfAliasCreatable(context.TODO(), "", "name", "name")
		assert.Error(t, err)
	})
}

func TestMetaTable_DropDatabase(t *testing.T) {
	t.Run("can't drop default database", func(t *testing.T) {
		mt := &MetaTable{}
		err := mt.CheckIfDatabaseDroppable(context.TODO(), &milvuspb.DropDatabaseRequest{
			DbName: util.DefaultDBName,
		})
		assert.Error(t, err)
	})

	t.Run("database not exist", func(t *testing.T) {
		mt := &MetaTable{
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		err := mt.CheckIfDatabaseDroppable(context.TODO(), &milvuspb.DropDatabaseRequest{
			DbName: "not_exist",
		})
		assert.True(t, errors.Is(err, merr.ErrDatabaseNotFound))
	})

	t.Run("database not empty", func(t *testing.T) {
		mt := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				"not_empty": model.NewDatabase(1, "not_empty", pb.DatabaseState_DatabaseCreated, nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[int64]*model.Collection{
				10000000: {
					DBID:         1,
					CollectionID: 10000000,
					Name:         "collection",
				},
			},
		}
		mt.names.insert("not_empty", "collection", 10000000)
		err := mt.CheckIfDatabaseDroppable(context.TODO(), &milvuspb.DropDatabaseRequest{
			DbName: "not_empty",
		})
		assert.Error(t, err)
	})

	t.Run("not commit", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("DropDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(errors.New("error mock DropDatabase"))
		mt := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				"not_commit": model.NewDatabase(1, "not_commit", pb.DatabaseState_DatabaseCreated, nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			catalog: catalog,
		}
		mt.names.createDbIfNotExist("not_commit")
		mt.aliases.createDbIfNotExist("not_commit")
		err := mt.CheckIfDatabaseDroppable(context.TODO(), &milvuspb.DropDatabaseRequest{
			DbName: "not_commit",
		})
		assert.NoError(t, err)
		err = mt.DropDatabase(context.TODO(), "not_commit", 10000)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("DropDatabase",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		catalog.EXPECT().DeleteGrantByDatabaseID(
			mock.Anything,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil)
		mt := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				"not_commit": model.NewDatabase(1, "not_commit", pb.DatabaseState_DatabaseCreated, nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			catalog: catalog,
		}
		mt.names.createDbIfNotExist("not_commit")
		mt.aliases.createDbIfNotExist("not_commit")
		err := mt.CheckIfDatabaseDroppable(context.TODO(), &milvuspb.DropDatabaseRequest{
			DbName: "not_commit",
		})
		assert.NoError(t, err)
		err = mt.DropDatabase(context.TODO(), "not_commit", 10000)
		assert.NoError(t, err)
		assert.False(t, mt.names.exist("not_commit"))
		assert.False(t, mt.aliases.exist("not_commit"))
	})
}

func TestMetaTable_BackupRBAC(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	catalog.EXPECT().BackupRBAC(mock.Anything, mock.Anything).Return(&milvuspb.RBACMeta{}, nil)
	mt := &MetaTable{
		dbName2Meta: map[string]*model.Database{
			"not_commit": model.NewDatabase(1, "not_commit", pb.DatabaseState_DatabaseCreated, nil),
		},
		names:   newNameDb(),
		aliases: newNameDb(),
		catalog: catalog,
	}
	_, err := mt.BackupRBAC(context.TODO(), util.DefaultTenant)
	assert.NoError(t, err)

	catalog.ExpectedCalls = nil
	catalog.EXPECT().BackupRBAC(mock.Anything, mock.Anything).Return(nil, errors.New("error mock BackupRBAC"))
	_, err = mt.BackupRBAC(context.TODO(), util.DefaultTenant)
	assert.Error(t, err)
}

func TestMetaTable_RestoreRBAC(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	catalog.EXPECT().RestoreRBAC(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mt := &MetaTable{
		dbName2Meta: map[string]*model.Database{
			"not_commit": model.NewDatabase(1, "not_commit", pb.DatabaseState_DatabaseCreated, nil),
		},
		names:   newNameDb(),
		aliases: newNameDb(),
		catalog: catalog,
	}

	err := mt.RestoreRBAC(context.TODO(), util.DefaultTenant, &milvuspb.RBACMeta{})
	assert.NoError(t, err)

	catalog.ExpectedCalls = nil
	catalog.EXPECT().RestoreRBAC(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("error mock RestoreRBAC"))
	err = mt.RestoreRBAC(context.TODO(), util.DefaultTenant, &milvuspb.RBACMeta{})
	assert.Error(t, err)
}

// TestMetaTable_CheckIfRBACRestorable_Wildcard verifies that wildcard
// privilege "*" is accepted by CheckIfRBACRestorable rather than rejected
// as an undefined privilege. Mirrors the fix in PR #48978.
func TestMetaTable_CheckIfRBACRestorable_Wildcard(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	catalog.EXPECT().ListRole(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)
	catalog.EXPECT().ListPrivilegeGroups(mock.Anything).
		Return(nil, nil)
	catalog.EXPECT().ListUser(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)

	mt := &MetaTable{
		dbName2Meta: map[string]*model.Database{
			"not_commit": model.NewDatabase(1, "not_commit", pb.DatabaseState_DatabaseCreated, nil),
		},
		names:   newNameDb(),
		aliases: newNameDb(),
		catalog: catalog,
	}

	req := &milvuspb.RestoreRBACMetaRequest{
		RBACMeta: &milvuspb.RBACMeta{
			Roles: []*milvuspb.RoleEntity{{Name: "wildcard_role"}},
			Grants: []*milvuspb.GrantEntity{
				{
					Role:       &milvuspb.RoleEntity{Name: "wildcard_role"},
					Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Global.String()},
					ObjectName: util.AnyWord,
					DbName:     util.AnyWord,
					Grantor: &milvuspb.GrantorEntity{
						User:      &milvuspb.UserEntity{Name: util.UserRoot},
						Privilege: &milvuspb.PrivilegeEntity{Name: util.AnyWord},
					},
				},
			},
		},
	}
	assert.NoError(t, mt.CheckIfRBACRestorable(context.TODO(), req))
}

// TestMetaTable_RestoreRBAC_Wildcard verifies that RestoreRBAC preserves a
// wildcard privilege name "*" end-to-end, rather than routing it through
// PrivilegeGroupNameForMetastore and encoding it as "PrivilegeGroup*". This
// is the MetaTable-layer equivalent of upstream PR #48978's Catalog-layer
// fix — in our branch the grant loop was moved from Catalog.RestoreRBAC
// into MetaTable.RestoreRBAC, so the wildcard guard must live here.
func TestMetaTable_RestoreRBAC_Wildcard(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	catalog.EXPECT().RestoreRBAC(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	var observedPrivName string
	catalog.EXPECT().
		AlterGrant(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, entity *milvuspb.GrantEntity, _ milvuspb.OperatePrivilegeType, _, _ int64) error {
			observedPrivName = entity.GetGrantor().GetPrivilege().GetName()
			return nil
		}).Once()

	mt := &MetaTable{
		dbName2Meta: map[string]*model.Database{},
		names:       newNameDb(),
		aliases:     newNameDb(),
		catalog:     catalog,
	}

	rbacMeta := &milvuspb.RBACMeta{
		Roles: []*milvuspb.RoleEntity{{Name: "wildcard_role"}},
		Grants: []*milvuspb.GrantEntity{
			{
				Role:       &milvuspb.RoleEntity{Name: "wildcard_role"},
				Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Global.String()},
				ObjectName: util.AnyWord,
				DbName:     util.AnyWord,
				Grantor: &milvuspb.GrantorEntity{
					User:      &milvuspb.UserEntity{Name: util.UserRoot},
					Privilege: &milvuspb.PrivilegeEntity{Name: util.AnyWord},
				},
			},
		},
	}
	require.NoError(t, mt.RestoreRBAC(context.TODO(), util.DefaultTenant, rbacMeta))
	assert.Equal(t, util.AnyWord, observedPrivName,
		"wildcard privilege must be passed through to catalog as '*', not rewritten")
}

// TestMetaTable_RestoreRBAC_SkippedGrantsError verifies that RestoreRBAC
// reports an error when some grants reference collections that do not exist,
// rather than silently dropping them. The non-missing grants are still applied
// so the call is idempotent: the user can create the missing collections and
// retry RestoreRBAC.
func TestMetaTable_RestoreRBAC_SkippedGrantsError(t *testing.T) {
	t.Run("returns error listing skipped collections", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.EXPECT().RestoreRBAC(mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Only the existing collection's grant should reach AlterGrant
		var alteredNames []string
		catalog.EXPECT().AlterGrant(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, _ string, g *milvuspb.GrantEntity, _ milvuspb.OperatePrivilegeType, _, _ int64) error {
				alteredNames = append(alteredNames, g.ObjectName)
				return nil
			}).Maybe()

		mt := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				"default": model.NewDatabase(1, "default", pb.DatabaseState_DatabaseCreated, nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {CollectionID: 100, Name: "existing_col", DBID: 1},
			},
			catalog: catalog,
		}
		mt.names.insert("default", "existing_col", 100)

		grants := []*milvuspb.GrantEntity{
			{
				Role:       &milvuspb.RoleEntity{Name: "role1"},
				Object:     &milvuspb.ObjectEntity{Name: "Collection"},
				ObjectName: "existing_col",
				DbName:     "default",
				Grantor:    &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: "Insert"}},
			},
			{
				Role:       &milvuspb.RoleEntity{Name: "role1"},
				Object:     &milvuspb.ObjectEntity{Name: "Collection"},
				ObjectName: "missing_col",
				DbName:     "default",
				Grantor:    &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: "Insert"}},
			},
			{
				Role:       &milvuspb.RoleEntity{Name: "role1"},
				Object:     &milvuspb.ObjectEntity{Name: "Collection"},
				ObjectName: "another_missing_col",
				DbName:     "default",
				Grantor:    &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: "Search"}},
			},
		}

		err := mt.RestoreRBAC(context.TODO(), util.DefaultTenant, &milvuspb.RBACMeta{Grants: grants})
		require.Error(t, err, "RestoreRBAC must error when grants reference missing collections")
		assert.Contains(t, err.Error(), "missing_col", "error must name the missing collection")
		assert.Contains(t, err.Error(), "another_missing_col", "error must name all missing collections")

		// The error must be detectable as partial-skip so Core.RestoreRBAC
		// can unwrap it and return Success (not OperatePrivilegeFailure) to
		// the SDK. The sentinel must survive arbitrary levels of wrapping.
		assert.True(t, IsRestoreRBACPartialSkip(err),
			"partial-skip error must be detectable via IsRestoreRBACPartialSkip")
		assert.True(t, IsRestoreRBACPartialSkip(errors.Wrap(err, "outer context")),
			"partial-skip sentinel must survive additional errors.Wrap layers")

		// Existing collection's grant should still have been applied
		assert.Contains(t, alteredNames, "existing_col", "grants for existing collections must still be applied")
		assert.NotContains(t, alteredNames, "missing_col")
		assert.NotContains(t, alteredNames, "another_missing_col")
	})

	t.Run("IsRestoreRBACPartialSkip returns false for unrelated errors", func(t *testing.T) {
		assert.False(t, IsRestoreRBACPartialSkip(nil))
		assert.False(t, IsRestoreRBACPartialSkip(errors.New("some other error")))
		assert.False(t, IsRestoreRBACPartialSkip(errors.Wrap(errors.New("inner"), "outer")))
	})

	t.Run("no error when all grants resolve", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.EXPECT().RestoreRBAC(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().AlterGrant(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

		mt := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				"default": model.NewDatabase(1, "default", pb.DatabaseState_DatabaseCreated, nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {CollectionID: 100, Name: "col1", DBID: 1},
			},
			catalog: catalog,
		}
		mt.names.insert("default", "col1", 100)

		err := mt.RestoreRBAC(context.TODO(), util.DefaultTenant, &milvuspb.RBACMeta{
			Grants: []*milvuspb.GrantEntity{
				{
					Role:       &milvuspb.RoleEntity{Name: "role1"},
					Object:     &milvuspb.ObjectEntity{Name: "Collection"},
					ObjectName: "col1",
					DbName:     "default",
					Grantor:    &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: "Insert"}},
				},
			},
		})
		assert.NoError(t, err)
	})

	t.Run("wildcard collection grant is not affected by skip logic", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.EXPECT().RestoreRBAC(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().AlterGrant(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		mt := &MetaTable{
			dbName2Meta: map[string]*model.Database{
				"default": model.NewDatabase(1, "default", pb.DatabaseState_DatabaseCreated, nil),
			},
			names:   newNameDb(),
			aliases: newNameDb(),
			catalog: catalog,
		}

		// Wildcard grants don't require collection lookup
		err := mt.RestoreRBAC(context.TODO(), util.DefaultTenant, &milvuspb.RBACMeta{
			Grants: []*milvuspb.GrantEntity{
				{
					Role:       &milvuspb.RoleEntity{Name: "role1"},
					Object:     &milvuspb.ObjectEntity{Name: "Collection"},
					ObjectName: util.AnyWord,
					DbName:     "default",
					Grantor:    &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: "Insert"}},
				},
			},
		})
		assert.NoError(t, err)
	})
}

func TestMetaTable_RestoreRBAC_WildcardDbIDResolution(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	catalog.EXPECT().RestoreRBAC(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Track the dbID passed to AlterGrant for the wildcard grant
	var capturedDbID int64
	catalog.EXPECT().AlterGrant(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, _ *milvuspb.GrantEntity, _ milvuspb.OperatePrivilegeType, dbID int64, _ int64) error {
			capturedDbID = dbID
			return nil
		})

	mt := &MetaTable{
		dbName2Meta: map[string]*model.Database{
			"default": model.NewDatabase(1, "default", pb.DatabaseState_DatabaseCreated, nil),
		},
		names:   newNameDb(),
		aliases: newNameDb(),
		catalog: catalog,
	}

	// Restore a wildcard grant on "default" database
	err := mt.RestoreRBAC(context.TODO(), util.DefaultTenant, &milvuspb.RBACMeta{
		Grants: []*milvuspb.GrantEntity{
			{
				Role:       &milvuspb.RoleEntity{Name: "role1"},
				Object:     &milvuspb.ObjectEntity{Name: "Collection"},
				ObjectName: util.AnyWord,
				DbName:     "default",
				Grantor: &milvuspb.GrantorEntity{
					Privilege: &milvuspb.PrivilegeEntity{Name: "Insert"},
				},
			},
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(1), capturedDbID, "wildcard grant should resolve dbID")
}

func TestMetaTable_PrivilegeGroup(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	catalog.EXPECT().ListPrivilegeGroups(mock.Anything).Return([]*milvuspb.PrivilegeGroupInfo{
		{
			GroupName:  "pg1",
			Privileges: []*milvuspb.PrivilegeEntity{{Name: "CreateCollection"}, {Name: "DescribeCollection"}},
		},
	}, nil)
	catalog.EXPECT().ListRole(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	catalog.EXPECT().SavePrivilegeGroup(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().DropPrivilegeGroup(mock.Anything, mock.Anything).Return(nil)
	mt := &MetaTable{
		dbName2Meta: map[string]*model.Database{
			"not_commit": model.NewDatabase(1, "not_commit", pb.DatabaseState_DatabaseCreated, nil),
		},
		names:   newNameDb(),
		aliases: newNameDb(),
		catalog: catalog,
	}
	err := mt.CheckIfPrivilegeGroupCreatable(context.TODO(), &milvuspb.CreatePrivilegeGroupRequest{
		GroupName: "pg1",
	})
	assert.Error(t, err)
	err = mt.CheckIfPrivilegeGroupCreatable(context.TODO(), &milvuspb.CreatePrivilegeGroupRequest{
		GroupName: "",
	})
	assert.Error(t, err)
	err = mt.CheckIfPrivilegeGroupCreatable(context.TODO(), &milvuspb.CreatePrivilegeGroupRequest{
		GroupName: "Insert",
	})
	assert.Error(t, err)
	err = mt.CheckIfPrivilegeGroupCreatable(context.TODO(), &milvuspb.CreatePrivilegeGroupRequest{
		GroupName: "pg2",
	})
	assert.NoError(t, err)
	err = mt.CreatePrivilegeGroup(context.TODO(), "pg1")
	assert.NoError(t, err)
	// idempotency
	err = mt.CreatePrivilegeGroup(context.TODO(), "pg1")
	assert.NoError(t, err)

	err = mt.CheckIfPrivilegeGroupDropable(context.TODO(), &milvuspb.DropPrivilegeGroupRequest{
		GroupName: "",
	})
	assert.Error(t, err)
	err = mt.CheckIfPrivilegeGroupDropable(context.TODO(), &milvuspb.DropPrivilegeGroupRequest{
		GroupName: "pg1",
	})
	assert.NoError(t, err)
	err = mt.DropPrivilegeGroup(context.TODO(), "pg1")
	assert.NoError(t, err)
	err = mt.CheckIfPrivilegeGroupAlterable(context.TODO(), &milvuspb.OperatePrivilegeGroupRequest{
		GroupName:  "",
		Privileges: []*milvuspb.PrivilegeEntity{},
		Type:       milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup,
	})
	assert.Error(t, err)
	err = mt.CheckIfPrivilegeGroupAlterable(context.TODO(), &milvuspb.OperatePrivilegeGroupRequest{
		GroupName:  "ClusterReadOnly",
		Privileges: []*milvuspb.PrivilegeEntity{},
		Type:       milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup,
	})
	assert.Error(t, err)
	err = mt.CheckIfPrivilegeGroupAlterable(context.TODO(), &milvuspb.OperatePrivilegeGroupRequest{
		GroupName:  "pg3",
		Privileges: []*milvuspb.PrivilegeEntity{},
		Type:       milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup,
	})
	assert.Error(t, err)
	_, err = mt.GetPrivilegeGroupRoles(context.TODO(), "")
	assert.Error(t, err)
	_, err = mt.ListPrivilegeGroups(context.TODO())
	assert.NoError(t, err)
}

func TestMetaTable_TruncateCollection(t *testing.T) {
	channel.ResetStaticPChannelStatsManager()

	kv, _ := kvfactory.GetEtcdAndPath()
	path := funcutil.RandomString(10) + "/meta"
	catalogKV := etcdkv.NewEtcdKV(kv, path)
	catalog := rootcoord.NewCatalog(catalogKV)

	allocator := mocktso.NewAllocator(t)
	allocator.EXPECT().GenerateTSO(mock.Anything).Return(1000, nil)

	meta, err := NewMetaTable(context.Background(), catalog, allocator)
	require.NoError(t, err)

	err = meta.AddCollection(context.Background(), &model.Collection{
		CollectionID:         1,
		PhysicalChannelNames: []string{"pchannel1"},
		VirtualChannelNames:  []string{"vchannel1"},
		State:                pb.CollectionState_CollectionCreated,
		DBID:                 util.DefaultDBID,
		Properties:           common.NewKeyValuePairs(map[string]string{}),
		ShardInfos: map[string]*model.ShardInfo{
			"vchannel1": {
				VChannelName:         "vchannel1",
				PChannelName:         "pchannel1",
				LastTruncateTimeTick: 0,
			},
		},
	})
	require.NoError(t, err)

	// begin truncate collection
	err = meta.BeginTruncateCollection(context.Background(), 1)
	require.NoError(t, err)
	coll, err := meta.GetCollectionByID(context.Background(), util.DefaultDBName, 1, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	m := common.CloneKeyValuePairs(coll.Properties).ToMap()
	require.Equal(t, "1", m[common.CollectionOnTruncatingKey])
	require.Equal(t, uint64(0), coll.ShardInfos["vchannel1"].LastTruncateTimeTick)

	// reload the meta
	channel.ResetStaticPChannelStatsManager()
	meta, err = NewMetaTable(context.Background(), catalog, allocator)
	require.NoError(t, err)
	coll, err = meta.GetCollectionByID(context.Background(), util.DefaultDBName, 1, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	m = common.CloneKeyValuePairs(coll.Properties).ToMap()
	require.Equal(t, "1", m[common.CollectionOnTruncatingKey])
	require.Equal(t, uint64(0), coll.ShardInfos["vchannel1"].LastTruncateTimeTick)

	// remove the temp property
	b := message.NewTruncateCollectionMessageBuilderV2().
		WithHeader(&message.TruncateCollectionMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&message.TruncateCollectionMessageBody{}).
		WithBroadcast(coll.VirtualChannelNames, message.OptBuildBroadcastAckSyncUp()).
		MustBuildBroadcast()

	meta.TruncateCollection(context.Background(), message.BroadcastResultTruncateCollectionMessageV2{
		Message: message.MustAsBroadcastTruncateCollectionMessageV2(b),
		Results: map[string]*message.AppendResult{
			"vchannel1": {
				TimeTick: 1000,
			},
		},
	})

	require.NoError(t, err)
	coll, err = meta.GetCollectionByID(context.Background(), util.DefaultDBName, 1, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	m = common.CloneKeyValuePairs(coll.Properties).ToMap()
	_, ok := m[common.CollectionOnTruncatingKey]
	require.False(t, ok)
	require.Equal(t, uint64(1000), coll.ShardInfos["vchannel1"].LastTruncateTimeTick)

	// reload the meta again
	channel.ResetStaticPChannelStatsManager()
	meta, err = NewMetaTable(context.Background(), catalog, allocator)
	require.NoError(t, err)
	coll, err = meta.GetCollectionByID(context.Background(), util.DefaultDBName, 1, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	m = common.CloneKeyValuePairs(coll.Properties).ToMap()
	_, ok = m[common.CollectionOnTruncatingKey]
	require.False(t, ok)
	require.Equal(t, 1, len(coll.ShardInfos))
	require.Equal(t, uint64(1000), coll.ShardInfos["vchannel1"].LastTruncateTimeTick)
}

func TestConvertGrantsToNameBased(t *testing.T) {
	mt := &MetaTable{
		dbName2Meta: map[string]*model.Database{
			"mydb": model.NewDatabase(100, "mydb", pb.DatabaseState_DatabaseCreated, nil),
		},
		collID2Meta: map[typeutil.UniqueID]*model.Collection{
			200: {CollectionID: 200, Name: "mycoll"},
		},
	}

	t.Run("resolve ID-based dbName and objectName", func(t *testing.T) {
		entities := []*milvuspb.GrantEntity{
			{
				Role:       &milvuspb.RoleEntity{Name: "role1"},
				Object:     &milvuspb.ObjectEntity{Name: "Collection"},
				DbName:     funcutil.FormatDatabaseID(100),
				ObjectName: funcutil.FormatCollectionID(200),
				Grantor:    &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: "Insert"}},
			},
		}
		resolved := mt.convertGrantsToNameBased(context.TODO(), entities)
		assert.Equal(t, 1, len(resolved))
		assert.Equal(t, "mydb", resolved[0].DbName)
		assert.Equal(t, "mycoll", resolved[0].ObjectName)
	})

	t.Run("keep name-based entries unchanged", func(t *testing.T) {
		entities := []*milvuspb.GrantEntity{
			{
				Role:       &milvuspb.RoleEntity{Name: "role1"},
				Object:     &milvuspb.ObjectEntity{Name: "Collection"},
				DbName:     "mydb",
				ObjectName: "mycoll",
				Grantor:    &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: "Insert"}},
			},
		}
		resolved := mt.convertGrantsToNameBased(context.TODO(), entities)
		assert.Equal(t, 1, len(resolved))
		assert.Equal(t, "mydb", resolved[0].DbName)
		assert.Equal(t, "mycoll", resolved[0].ObjectName)
	})

	t.Run("deduplicate ID-based and name-based entries", func(t *testing.T) {
		entities := []*milvuspb.GrantEntity{
			{
				Role:       &milvuspb.RoleEntity{Name: "role1"},
				Object:     &milvuspb.ObjectEntity{Name: "Collection"},
				DbName:     "mydb",
				ObjectName: "mycoll",
				Grantor:    &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: "Insert"}},
			},
			{
				Role:       &milvuspb.RoleEntity{Name: "role1"},
				Object:     &milvuspb.ObjectEntity{Name: "Collection"},
				DbName:     funcutil.FormatDatabaseID(100),
				ObjectName: funcutil.FormatCollectionID(200),
				Grantor:    &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: "Insert"}},
			},
		}
		resolved := mt.convertGrantsToNameBased(context.TODO(), entities)
		assert.Equal(t, 1, len(resolved))
	})

	t.Run("unresolvable collection ID is filtered out (no colID:X leak to SDK)", func(t *testing.T) {
		entities := []*milvuspb.GrantEntity{
			{
				Role:       &milvuspb.RoleEntity{Name: "role1"},
				Object:     &milvuspb.ObjectEntity{Name: "Collection"},
				DbName:     funcutil.FormatDatabaseID(100),
				ObjectName: funcutil.FormatCollectionID(999), // 999 not in collID2Meta
				Grantor:    &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: "Insert"}},
			},
		}
		resolved := mt.convertGrantsToNameBased(context.TODO(), entities)
		assert.Empty(t, resolved, "unresolvable ID-based grants must not leak 'colID:X' strings to SDK")
	})

	t.Run("unresolvable database ID is filtered out (no dbID:X leak to SDK)", func(t *testing.T) {
		entities := []*milvuspb.GrantEntity{
			{
				Role:       &milvuspb.RoleEntity{Name: "role1"},
				Object:     &milvuspb.ObjectEntity{Name: "Collection"},
				DbName:     funcutil.FormatDatabaseID(999), // 999 not in dbName2Meta
				ObjectName: funcutil.FormatCollectionID(200),
				Grantor:    &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: "Insert"}},
			},
		}
		resolved := mt.convertGrantsToNameBased(context.TODO(), entities)
		assert.Empty(t, resolved, "unresolvable ID-based DB grants must not leak 'dbID:X' strings to SDK")
	})

	t.Run("resolvable grants alongside unresolvable ones: only resolvable are returned", func(t *testing.T) {
		entities := []*milvuspb.GrantEntity{
			{
				Role:       &milvuspb.RoleEntity{Name: "role1"},
				Object:     &milvuspb.ObjectEntity{Name: "Collection"},
				DbName:     funcutil.FormatDatabaseID(100),
				ObjectName: funcutil.FormatCollectionID(200),
				Grantor:    &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: "Insert"}},
			},
			{
				Role:       &milvuspb.RoleEntity{Name: "role1"},
				Object:     &milvuspb.ObjectEntity{Name: "Collection"},
				DbName:     funcutil.FormatDatabaseID(100),
				ObjectName: funcutil.FormatCollectionID(999), // dropped
				Grantor:    &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: "Search"}},
			},
		}
		resolved := mt.convertGrantsToNameBased(context.TODO(), entities)
		assert.Len(t, resolved, 1)
		assert.Equal(t, "mydb", resolved[0].DbName)
		assert.Equal(t, "mycoll", resolved[0].ObjectName)
		assert.Equal(t, "Insert", resolved[0].GetGrantor().GetPrivilege().GetName())
	})

	t.Run("nil grantor privilege", func(t *testing.T) {
		entities := []*milvuspb.GrantEntity{
			{
				Role:       &milvuspb.RoleEntity{Name: "role1"},
				Object:     &milvuspb.ObjectEntity{Name: "Global"},
				DbName:     "mydb",
				ObjectName: "*",
			},
		}
		resolved := mt.convertGrantsToNameBased(context.TODO(), entities)
		assert.Equal(t, 1, len(resolved))
	})
}

func TestLookupCollectionAndDBID(t *testing.T) {
	mt := &MetaTable{
		dbName2Meta: map[string]*model.Database{
			util.DefaultDBName: model.NewDatabase(1, util.DefaultDBName, pb.DatabaseState_DatabaseCreated, nil),
			"mydb":             model.NewDatabase(2, "mydb", pb.DatabaseState_DatabaseCreated, nil),
		},
		names:   newNameDb(),
		aliases: newNameDb(),
	}
	mt.names.insert(util.DefaultDBName, "coll1", 100)
	mt.aliases.insert(util.DefaultDBName, "alias1", 100)
	mt.names.insert("mydb", "coll2", 200)

	t.Run("resolve by collection name", func(t *testing.T) {
		dbID, collID := mt.LookupCollectionAndDBID(context.TODO(), util.DefaultDBName, "coll1")
		assert.Equal(t, int64(1), dbID)
		assert.Equal(t, int64(100), collID)
	})

	t.Run("resolve by alias", func(t *testing.T) {
		dbID, collID := mt.LookupCollectionAndDBID(context.TODO(), util.DefaultDBName, "alias1")
		assert.Equal(t, int64(1), dbID)
		assert.Equal(t, int64(100), collID)
	})

	t.Run("empty dbName defaults to default db", func(t *testing.T) {
		dbID, collID := mt.LookupCollectionAndDBID(context.TODO(), "", "coll1")
		assert.Equal(t, int64(1), dbID)
		assert.Equal(t, int64(100), collID)
	})

	t.Run("non-default database", func(t *testing.T) {
		dbID, collID := mt.LookupCollectionAndDBID(context.TODO(), "mydb", "coll2")
		assert.Equal(t, int64(2), dbID)
		assert.Equal(t, int64(200), collID)
	})

	t.Run("non-existent collection returns InvalidCollectionID", func(t *testing.T) {
		_, collID := mt.LookupCollectionAndDBID(context.TODO(), util.DefaultDBName, "no_such_coll")
		assert.Equal(t, InvalidCollectionID, collID)
	})

	t.Run("non-existent database returns InvalidCollectionID", func(t *testing.T) {
		_, collID := mt.LookupCollectionAndDBID(context.TODO(), "no_such_db", "coll1")
		assert.Equal(t, InvalidCollectionID, collID)
	})

	t.Run("wildcard collectionName returns dbID only", func(t *testing.T) {
		dbID, collID := mt.LookupCollectionAndDBID(context.TODO(), util.DefaultDBName, util.AnyWord)
		assert.Equal(t, int64(1), dbID)
		assert.Equal(t, InvalidCollectionID, collID)
	})

	t.Run("empty collectionName returns dbID only", func(t *testing.T) {
		dbID, collID := mt.LookupCollectionAndDBID(context.TODO(), util.DefaultDBName, "")
		assert.Equal(t, int64(1), dbID)
		assert.Equal(t, InvalidCollectionID, collID)
	})
}

func TestMigrateGrantsToEntityID(t *testing.T) {
	t.Run("catalog returns error", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.EXPECT().MigrateGrantsToEntityID(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("migrate error"))
		mt := &MetaTable{
			catalog: catalog,
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		err := mt.MigrateGrantsToEntityID(context.TODO())
		assert.Error(t, err)
	})

	t.Run("catalog succeeds", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.EXPECT().MigrateGrantsToEntityID(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mt := &MetaTable{
			catalog: catalog,
			names:   newNameDb(),
			aliases: newNameDb(),
		}
		err := mt.MigrateGrantsToEntityID(context.TODO())
		assert.NoError(t, err)
	})
}

func TestOperatePrivilege_CollectionIDResolution(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	mt := &MetaTable{
		dbName2Meta: map[string]*model.Database{
			util.DefaultDBName: model.NewDatabase(1, util.DefaultDBName, pb.DatabaseState_DatabaseCreated, nil),
		},
		names:   newNameDb(),
		aliases: newNameDb(),
		catalog: catalog,
	}
	mt.names.insert(util.DefaultDBName, "real_coll", 300)

	t.Run("grant on existing collection resolves ID", func(t *testing.T) {
		catalog.EXPECT().AlterGrant(mock.Anything, mock.Anything, mock.Anything, mock.Anything, int64(1), int64(300)).Return(nil).Once()
		entity := &milvuspb.GrantEntity{
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: "root"},
				Privilege: &milvuspb.PrivilegeEntity{Name: "Insert"},
			},
			Role:       &milvuspb.RoleEntity{Name: "role1"},
			Object:     &milvuspb.ObjectEntity{Name: "Collection"},
			ObjectName: "real_coll",
		}
		err := mt.OperatePrivilege(context.TODO(), util.DefaultTenant, entity, milvuspb.OperatePrivilegeType_Grant)
		assert.NoError(t, err)
	})

	t.Run("grant on non-existent collection returns error", func(t *testing.T) {
		entity := &milvuspb.GrantEntity{
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: "root"},
				Privilege: &milvuspb.PrivilegeEntity{Name: "Insert"},
			},
			Role:       &milvuspb.RoleEntity{Name: "role1"},
			Object:     &milvuspb.ObjectEntity{Name: "Collection"},
			ObjectName: "no_such_coll",
		}
		err := mt.OperatePrivilege(context.TODO(), util.DefaultTenant, entity, milvuspb.OperatePrivilegeType_Grant)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, merr.ErrCollectionNotFound))
	})

	t.Run("revoke on non-existent collection is skipped", func(t *testing.T) {
		entity := &milvuspb.GrantEntity{
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: "root"},
				Privilege: &milvuspb.PrivilegeEntity{Name: "Insert"},
			},
			Role:       &milvuspb.RoleEntity{Name: "role1"},
			Object:     &milvuspb.ObjectEntity{Name: "Collection"},
			ObjectName: "no_such_coll",
		}
		err := mt.OperatePrivilege(context.TODO(), util.DefaultTenant, entity, milvuspb.OperatePrivilegeType_Revoke)
		assert.Error(t, err)
		assert.True(t, common.IsIgnorableError(err), "revoke on dropped collection should return IgnorableError")
	})

	t.Run("AnyWord skips ID resolution", func(t *testing.T) {
		catalog.EXPECT().AlterGrant(mock.Anything, mock.Anything, mock.Anything, mock.Anything, int64(1), int64(0)).Return(nil).Once()
		entity := &milvuspb.GrantEntity{
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: "root"},
				Privilege: &milvuspb.PrivilegeEntity{Name: "Insert"},
			},
			Role:       &milvuspb.RoleEntity{Name: "role1"},
			Object:     &milvuspb.ObjectEntity{Name: "Collection"},
			ObjectName: util.AnyWord,
		}
		err := mt.OperatePrivilege(context.TODO(), util.DefaultTenant, entity, milvuspb.OperatePrivilegeType_Grant)
		assert.NoError(t, err)
	})
}
