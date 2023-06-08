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

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func generateMetaTable(t *testing.T) *MetaTable {
	return &MetaTable{catalog: &rootcoord.Catalog{Txn: memkv.NewMemoryKV()}}
}

func TestRbacAddCredential(t *testing.T) {
	mt := generateMetaTable(t)
	err := mt.AddCredential(&internalpb.CredentialInfo{
		Username: "user1",
		Tenant:   util.DefaultTenant,
	})
	require.NoError(t, err)

	tests := []struct {
		description string

		maxUser bool
		info    *internalpb.CredentialInfo
	}{
		{"Empty username", false, &internalpb.CredentialInfo{Username: ""}},
		{"exceed MaxUserNum", true, &internalpb.CredentialInfo{Username: "user3", Tenant: util.DefaultTenant}},
		{"user exist", false, &internalpb.CredentialInfo{Username: "user1", Tenant: util.DefaultTenant}},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			if test.maxUser {
				paramtable.Get().Save(Params.ProxyCfg.MaxUserNum.Key, "1")
			} else {
				paramtable.Get().Save(Params.ProxyCfg.MaxUserNum.Key, "3")
			}
			defer paramtable.Get().Reset(Params.ProxyCfg.MaxUserNum.Key)
			err := mt.AddCredential(test.info)
			assert.Error(t, err)
		})
	}
}

func TestRbacCreateRole(t *testing.T) {
	mt := generateMetaTable(t)

	paramtable.Get().Save(Params.ProxyCfg.MaxRoleNum.Key, "2")
	defer paramtable.Get().Reset(Params.ProxyCfg.MaxRoleNum.Key)
	err := mt.CreateRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: "role1"})
	require.NoError(t, err)
	err = mt.CreateRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: "role2"})
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
			err := mt.CreateRole(util.DefaultTenant, test.inEntity)
			assert.Error(t, err)
		})
	}
}

func TestRbacDropRole(t *testing.T) {
	mt := generateMetaTable(t)

	err := mt.CreateRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: "role1"})
	require.NoError(t, err)

	tests := []struct {
		roleName string

		description string
	}{
		{"role1", "drop role1"},
		{"role_not_exists", "drop not exist role"},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			err := mt.DropRole(util.DefaultTenant, test.roleName)
			assert.NoError(t, err)
		})
	}
}

func TestRbacOperateRole(t *testing.T) {
	mt := generateMetaTable(t)
	err := mt.CreateRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: "role1"})
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
			err := mt.OperateUserRole(util.DefaultTenant, &milvuspb.UserEntity{Name: test.user}, &milvuspb.RoleEntity{Name: test.role}, test.oType)
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
		err := mt.CreateRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: role})
		require.NoError(t, err)
	}

	for user, rs := range userRoles {
		err := mt.catalog.CreateCredential(context.TODO(), &model.Credential{
			Username: user,
			Tenant:   util.DefaultTenant,
		})

		require.NoError(t, err)
		for _, r := range rs {
			err := mt.OperateUserRole(
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
			res, err := mt.SelectUser(util.DefaultTenant, test.inEntity, test.includeRoleInfo)

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
			res, err := mt.SelectRole(util.DefaultTenant, test.inEntity, test.includeUserInfo)

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
			ObjectName: "obj_name"}, milvuspb.OperatePrivilegeType_Grant},
		{"empty Object name", &milvuspb.GrantEntity{
			Object:     &milvuspb.ObjectEntity{Name: ""},
			ObjectName: "obj_name"}, milvuspb.OperatePrivilegeType_Grant},
		{"nil Role", &milvuspb.GrantEntity{
			Role:       nil,
			Object:     &milvuspb.ObjectEntity{Name: "obj_name"},
			ObjectName: "obj_name"}, milvuspb.OperatePrivilegeType_Grant},
		{"empty Role name", &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: ""},
			Object:     &milvuspb.ObjectEntity{Name: "obj_name"},
			ObjectName: "obj_name"}, milvuspb.OperatePrivilegeType_Grant},
		{"nil grantor", &milvuspb.GrantEntity{
			Grantor:    nil,
			Role:       &milvuspb.RoleEntity{Name: "role_name"},
			Object:     &milvuspb.ObjectEntity{Name: "obj_name"},
			ObjectName: "obj_name"}, milvuspb.OperatePrivilegeType_Grant},
		{"nil grantor privilege", &milvuspb.GrantEntity{
			Grantor: &milvuspb.GrantorEntity{
				Privilege: nil,
			},
			Role:       &milvuspb.RoleEntity{Name: "role_name"},
			Object:     &milvuspb.ObjectEntity{Name: "obj_name"},
			ObjectName: "obj_name"}, milvuspb.OperatePrivilegeType_Grant},
		{"empty grantor privilege name", &milvuspb.GrantEntity{
			Grantor: &milvuspb.GrantorEntity{
				Privilege: &milvuspb.PrivilegeEntity{Name: ""}},
			Role:       &milvuspb.RoleEntity{Name: "role_name"},
			Object:     &milvuspb.ObjectEntity{Name: "obj_name"},
			ObjectName: "obj_name"}, milvuspb.OperatePrivilegeType_Grant},
		{"nil grantor user", &milvuspb.GrantEntity{
			Grantor: &milvuspb.GrantorEntity{
				User:      nil,
				Privilege: &milvuspb.PrivilegeEntity{Name: "privilege_name"}},
			Role:       &milvuspb.RoleEntity{Name: "role_name"},
			Object:     &milvuspb.ObjectEntity{Name: "obj_name"},
			ObjectName: "obj_name"}, milvuspb.OperatePrivilegeType_Grant},
		{"empty grantor user name", &milvuspb.GrantEntity{
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: ""},
				Privilege: &milvuspb.PrivilegeEntity{Name: "privilege_name"}},
			Role:       &milvuspb.RoleEntity{Name: "role_name"},
			Object:     &milvuspb.ObjectEntity{Name: "obj_name"},
			ObjectName: "obj_name"}, milvuspb.OperatePrivilegeType_Grant},
		{"invalid operateType", &milvuspb.GrantEntity{
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: "user_name"},
				Privilege: &milvuspb.PrivilegeEntity{Name: "privilege_name"}},
			Role:       &milvuspb.RoleEntity{Name: "role_name"},
			Object:     &milvuspb.ObjectEntity{Name: "obj_name"},
			ObjectName: "obj_name"}, milvuspb.OperatePrivilegeType(-1)},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			err := mt.OperatePrivilege(util.DefaultTenant, test.entity, test.oType)
			assert.Error(t, err)
		})
	}

	validEntity := milvuspb.GrantEntity{
		Grantor: &milvuspb.GrantorEntity{
			User:      &milvuspb.UserEntity{Name: "user_name"},
			Privilege: &milvuspb.PrivilegeEntity{Name: "privilege_name"}},
		Role:       &milvuspb.RoleEntity{Name: "role_name"},
		Object:     &milvuspb.ObjectEntity{Name: "obj_name"},
		ObjectName: "obj_name"}

	err := mt.OperatePrivilege(util.DefaultTenant, &validEntity, milvuspb.OperatePrivilegeType_Grant)
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
			Role: nil}},
		{"empty entity Role name", false, &milvuspb.GrantEntity{
			Role: &milvuspb.RoleEntity{Name: ""}}},
		{"valid", true, &milvuspb.GrantEntity{
			Role: &milvuspb.RoleEntity{Name: "role"}}},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			entities, err := mt.SelectGrant(util.DefaultTenant, test.entity)
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
			err := mt.DropGrant(util.DefaultTenant, test.role)
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

	policies, err := mt.ListPolicy(util.DefaultTenant)
	assert.NoError(t, err)
	assert.Empty(t, policies)

	userRoles, err := mt.ListUserRole(util.DefaultTenant)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(userRoles))
}

func TestMetaTable_getCollectionByIDInternal(t *testing.T) {
	t.Run("failed to get from catalog", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("GetCollectionByID",
			mock.Anything, // context.Context
			mock.AnythingOfType("int64"),
			mock.AnythingOfType("uint64"),
		).Return(nil, errors.New("error mock GetCollectionByID"))
		meta := &MetaTable{
			catalog:     catalog,
			collID2Meta: map[typeutil.UniqueID]*model.Collection{},
		}
		ctx := context.Background()
		_, err := meta.getCollectionByIDInternal(ctx, 100, 101, false)
		assert.Error(t, err)
	})

	t.Run("collection not available", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("GetCollectionByID",
			mock.Anything, // context.Context
			mock.AnythingOfType("int64"),
			mock.AnythingOfType("uint64"),
		).Return(&model.Collection{State: pb.CollectionState_CollectionDropped}, nil)
		meta := &MetaTable{
			catalog:     catalog,
			collID2Meta: map[typeutil.UniqueID]*model.Collection{},
		}
		ctx := context.Background()
		_, err := meta.getCollectionByIDInternal(ctx, 100, 101, false)
		assert.Error(t, err)
		assert.True(t, common.IsCollectionNotExistError(err))
		coll, err := meta.getCollectionByIDInternal(ctx, 100, 101, true)
		assert.NoError(t, err)
		assert.False(t, coll.Available())
	})

	t.Run("normal case, filter unavailable partitions", func(t *testing.T) {
		Params.Init()
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
		coll, err := meta.getCollectionByIDInternal(ctx, 100, 101, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(coll.Partitions))
		assert.Equal(t, UniqueID(11), coll.Partitions[0].PartitionID)
		assert.Equal(t, Params.CommonCfg.DefaultPartitionName.GetValue(), coll.Partitions[0].PartitionName)
	})

	t.Run("get latest version", func(t *testing.T) {
		Params.Init()
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
		coll, err := meta.getCollectionByIDInternal(ctx, 100, typeutil.MaxTimestamp, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(coll.Partitions))
		assert.Equal(t, UniqueID(11), coll.Partitions[0].PartitionID)
		assert.Equal(t, Params.CommonCfg.DefaultPartitionName.GetValue(), coll.Partitions[0].PartitionName)
	})
}

func TestMetaTable_GetCollectionByName(t *testing.T) {
	t.Run("get by alias", func(t *testing.T) {
		meta := &MetaTable{
			collAlias2ID: map[string]typeutil.UniqueID{
				"alias": 100,
			},
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
		coll, err := meta.GetCollectionByName(ctx, "alias", 101)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(coll.Partitions))
		assert.Equal(t, UniqueID(11), coll.Partitions[0].PartitionID)
		assert.Equal(t, Params.CommonCfg.DefaultPartitionName.GetValue(), coll.Partitions[0].PartitionName)
	})

	t.Run("get by name", func(t *testing.T) {
		meta := &MetaTable{
			collName2ID: map[string]typeutil.UniqueID{
				"name": 100,
			},
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
		coll, err := meta.GetCollectionByName(ctx, "name", 101)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(coll.Partitions))
		assert.Equal(t, UniqueID(11), coll.Partitions[0].PartitionID)
		assert.Equal(t, Params.CommonCfg.DefaultPartitionName.GetValue(), coll.Partitions[0].PartitionName)
	})

	t.Run("failed to get from catalog", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("GetCollectionByName",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("uint64"),
		).Return(nil, errors.New("error mock GetCollectionByName"))
		meta := &MetaTable{catalog: catalog}
		ctx := context.Background()
		_, err := meta.GetCollectionByName(ctx, "name", 101)
		assert.Error(t, err)
	})

	t.Run("collection not available", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("GetCollectionByName",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("uint64"),
		).Return(&model.Collection{State: pb.CollectionState_CollectionDropped}, nil)
		meta := &MetaTable{catalog: catalog}
		ctx := context.Background()
		_, err := meta.GetCollectionByName(ctx, "name", 101)
		assert.Error(t, err)
		assert.True(t, common.IsCollectionNotExistError(err))
	})

	t.Run("normal case, filter unavailable partitions", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("GetCollectionByName",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("uint64"),
		).Return(&model.Collection{
			State:      pb.CollectionState_CollectionCreated,
			CreateTime: 99,
			Partitions: []*model.Partition{
				{PartitionID: 11, PartitionName: Params.CommonCfg.DefaultPartitionName.GetValue(), State: pb.PartitionState_PartitionCreated},
				{PartitionID: 22, PartitionName: "dropped", State: pb.PartitionState_PartitionDropped},
			},
		}, nil)
		meta := &MetaTable{catalog: catalog}
		ctx := context.Background()
		coll, err := meta.GetCollectionByName(ctx, "name", 101)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(coll.Partitions))
		assert.Equal(t, UniqueID(11), coll.Partitions[0].PartitionID)
		assert.Equal(t, Params.CommonCfg.DefaultPartitionName.GetValue(), coll.Partitions[0].PartitionName)
	})

	t.Run("get latest version", func(t *testing.T) {
		ctx := context.Background()
		meta := &MetaTable{collName2ID: nil}
		_, err := meta.GetCollectionByName(ctx, "not_exist", typeutil.MaxTimestamp)
		assert.Error(t, err)
		assert.True(t, common.IsCollectionNotExistError(err))
	})
}

func TestMetaTable_AlterCollection(t *testing.T) {
	t.Run("alter metastore fail", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterCollection",
			mock.Anything, // context.Context
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
		err := meta.AlterCollection(ctx, nil, nil, 0)
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
		).Return(nil)
		meta := &MetaTable{
			catalog:     catalog,
			collID2Meta: map[typeutil.UniqueID]*model.Collection{},
		}
		ctx := context.Background()

		oldColl := &model.Collection{CollectionID: 1}
		newColl := &model.Collection{CollectionID: 1}
		err := meta.AlterCollection(ctx, oldColl, newColl, 0)
		assert.NoError(t, err)
		assert.Equal(t, meta.collID2Meta[1], newColl)
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
	clone := filterUnavailable(coll)
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
		assert.True(t, common.IsCollectionNotExistError(err))
	})

	t.Run("nil case", func(t *testing.T) {
		ctx := context.Background()
		mt := &MetaTable{collID2Meta: map[typeutil.UniqueID]*model.Collection{
			100: nil,
		}}
		_, err := mt.getLatestCollectionByIDInternal(ctx, 100, false)
		assert.Error(t, err)
		assert.True(t, common.IsCollectionNotExistError(err))
	})

	t.Run("unavailable", func(t *testing.T) {
		ctx := context.Background()
		mt := &MetaTable{collID2Meta: map[typeutil.UniqueID]*model.Collection{
			100: {State: pb.CollectionState_CollectionDropping},
		}}
		_, err := mt.getLatestCollectionByIDInternal(ctx, 100, false)
		assert.Error(t, err)
		assert.True(t, common.IsCollectionNotExistError(err))
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
		meta := &MetaTable{catalog: catalog}
		ctx := context.Background()
		err := meta.RemoveCollection(ctx, 100, 9999)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("DropCollection",
			mock.Anything, // context.Context
			mock.Anything, // model.Collection
			mock.AnythingOfType("uint64"),
		).Return(nil)
		meta := &MetaTable{
			catalog: catalog,
			collAlias2ID: map[string]typeutil.UniqueID{
				"alias1": 100,
				"alias2": 100,
			},
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {Name: "collection"},
			},
			collName2ID: map[string]typeutil.UniqueID{
				"collection": 100,
			},
		}
		ctx := context.Background()
		err := meta.RemoveCollection(ctx, 100, 9999)
		assert.NoError(t, err)
	})
}

func TestMetaTable_reload(t *testing.T) {
	t.Run("failed to list collections", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("ListCollections",
			mock.Anything, // context.Context
			mock.AnythingOfType("uint64"),
		).Return(nil, errors.New("error mock ListCollections"))
		meta := &MetaTable{catalog: catalog}
		err := meta.reload()
		assert.Error(t, err)
		assert.Empty(t, meta.collID2Meta)
		assert.Empty(t, meta.collName2ID)
		assert.Empty(t, meta.collAlias2ID)
	})

	t.Run("failed to list aliases", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("ListCollections",
			mock.Anything, // context.Context
			mock.AnythingOfType("uint64"),
		).Return(
			map[string]*model.Collection{"test": {CollectionID: 100, Name: "test"}},
			nil)
		catalog.On("ListAliases",
			mock.Anything, // context.Context
			mock.AnythingOfType("uint64"),
		).Return(nil, errors.New("error mock ListAliases"))
		meta := &MetaTable{catalog: catalog}
		err := meta.reload()
		assert.Error(t, err)
		assert.Empty(t, meta.collAlias2ID)
	})

	t.Run("normal case", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)

		catalog.On("ListCollections",
			mock.Anything, // context.Context
			mock.AnythingOfType("uint64"),
		).Return(
			map[string]*model.Collection{"test": {CollectionID: 100, Name: "test"}},
			nil)

		catalog.On("ListAliases",
			mock.Anything, // context.Context
			mock.AnythingOfType("uint64"),
		).Return(
			[]*model.Alias{{Name: "alias", CollectionID: 100}},
			nil)

		meta := &MetaTable{catalog: catalog}
		err := meta.reload()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(meta.collID2Meta))
		assert.Equal(t, 1, len(meta.collName2ID))
		assert.Equal(t, 1, len(meta.collAlias2ID))
	})
}

func TestMetaTable_ChangeCollectionState(t *testing.T) {
	t.Run("not exist", func(t *testing.T) {
		meta := &MetaTable{}
		err := meta.ChangeCollectionState(context.TODO(), 100, pb.CollectionState_CollectionCreated, 100)
		assert.NoError(t, err)
	})

	t.Run("failed to alter collection", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterCollection",
			mock.Anything, // context.Context
			mock.Anything, // *model.Collection
			mock.Anything, // *model.Collection
			mock.Anything, // metastore.AlterType
			mock.AnythingOfType("uint64"),
		).Return(errors.New("error mock AlterCollection"))
		meta := &MetaTable{
			catalog: catalog,
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {Name: "test", CollectionID: 100},
			},
		}
		err := meta.ChangeCollectionState(context.TODO(), 100, pb.CollectionState_CollectionCreated, 1000)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterCollection",
			mock.Anything, // context.Context
			mock.Anything, // *model.Collection
			mock.Anything, // *model.Collection
			mock.Anything, // metastore.AlterType
			mock.AnythingOfType("uint64"),
		).Return(nil)
		meta := &MetaTable{
			catalog: catalog,
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {Name: "test", CollectionID: 100},
			},
		}
		err := meta.ChangeCollectionState(context.TODO(), 100, pb.CollectionState_CollectionCreated, 1000)
		assert.NoError(t, err)
		err = meta.ChangeCollectionState(context.TODO(), 100, pb.CollectionState_CollectionDropping, 1000)
		assert.NoError(t, err)
	})
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
			mock.Anything, // context.Context
			mock.Anything, // *model.Partition
			mock.AnythingOfType("uint64"),
		).Return(errors.New("error mock CreatePartition"))
		meta := &MetaTable{
			catalog: catalog,
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {Name: "test", CollectionID: 100},
			},
		}
		err := meta.AddPartition(context.TODO(), &model.Partition{CollectionID: 100, State: pb.PartitionState_PartitionCreating})
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("CreatePartition",
			mock.Anything, // context.Context
			mock.Anything, // *model.Partition
			mock.AnythingOfType("uint64"),
		).Return(nil)
		meta := &MetaTable{
			catalog: catalog,
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {Name: "test", CollectionID: 100},
			},
		}
		err := meta.AddPartition(context.TODO(), &model.Partition{CollectionID: 100, State: pb.PartitionState_PartitionCreating})
		assert.NoError(t, err)
	})
}

func TestMetaTable_RenameCollection(t *testing.T) {
	t.Run("unsupported use a alias to rename collection", func(t *testing.T) {
		meta := &MetaTable{
			collAlias2ID: map[string]typeutil.UniqueID{
				"alias": 1,
			},
		}
		err := meta.RenameCollection(context.TODO(), "alias", "new", typeutil.MaxTimestamp)
		assert.Error(t, err)
	})

	t.Run("collection name not exist", func(t *testing.T) {
		meta := &MetaTable{}
		err := meta.RenameCollection(context.TODO(), "non-exists", "new", typeutil.MaxTimestamp)
		assert.Error(t, err)
	})

	t.Run("collection id not exist", func(t *testing.T) {
		meta := &MetaTable{
			collName2ID: map[string]typeutil.UniqueID{
				"old": 1,
			},
		}
		err := meta.RenameCollection(context.TODO(), "old", "new", typeutil.MaxTimestamp)
		assert.Error(t, err)
	})

	t.Run("new collection name already exist-1", func(t *testing.T) {
		meta := &MetaTable{
			collName2ID: map[string]typeutil.UniqueID{
				"old": 1,
				"new": 2,
			},
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				2: {
					CollectionID: 1,
					Name:         "old",
					State:        pb.CollectionState_CollectionCreated,
				},
			},
		}
		err := meta.RenameCollection(context.TODO(), "old", "new", 1000)
		assert.Error(t, err)
	})

	t.Run("new collection name already exist-2", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("GetCollectionByID",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil, errors.New("error mock GetCollectionByID"))
		meta := &MetaTable{
			catalog: catalog,
			collName2ID: map[string]typeutil.UniqueID{
				"old": 1,
				"new": 2,
			},
		}
		err := meta.RenameCollection(context.TODO(), "old", "new", 1000)
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
		).Return(errors.New("fail"))
		catalog.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil, common.NewCollectionNotExistError("error"))

		meta := &MetaTable{
			catalog: catalog,
			collName2ID: map[string]typeutil.UniqueID{
				"old": 1,
			},
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				1: {
					CollectionID: 1,
					Name:         "old",
				},
			},
		}
		err := meta.RenameCollection(context.TODO(), "old", "new", 1000)
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
		).Return(nil)
		catalog.On("GetCollectionByName",
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil, common.NewCollectionNotExistError("error"))
		meta := &MetaTable{
			catalog: catalog,
			collName2ID: map[string]typeutil.UniqueID{
				"old": 1,
			},
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				1: {
					CollectionID: 1,
					Name:         "old",
				},
			},
		}
		err := meta.RenameCollection(context.TODO(), "old", "new", 1000)
		assert.NoError(t, err)

		id, ok := meta.collName2ID["new"]
		assert.True(t, ok)
		assert.Equal(t, int64(1), id)

		coll, ok := meta.collID2Meta[1]
		assert.True(t, ok)
		assert.Equal(t, "new", coll.Name)
	})
}

func TestMetaTable_ChangePartitionState(t *testing.T) {
	t.Run("collection not exist", func(t *testing.T) {
		meta := &MetaTable{}
		err := meta.ChangePartitionState(context.TODO(), 100, 500, pb.PartitionState_PartitionDropping, 1000)
		assert.NoError(t, err)
	})

	t.Run("partition not exist", func(t *testing.T) {
		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {Name: "test", CollectionID: 100},
			},
		}
		err := meta.ChangePartitionState(context.TODO(), 100, 500, pb.PartitionState_PartitionDropping, 1000)
		assert.Error(t, err)
	})

	t.Run("failed to alter partition", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterPartition",
			mock.Anything, // context.Context
			mock.Anything, // *model.Partition
			mock.Anything, // *model.Partition
			mock.Anything, // metastore.AlterType
			mock.AnythingOfType("uint64"),
		).Return(errors.New("error mock AlterPartition"))
		meta := &MetaTable{
			catalog: catalog,
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					Name: "test", CollectionID: 100,
					Partitions: []*model.Partition{
						{CollectionID: 100, PartitionID: 500},
					},
				},
			},
		}
		err := meta.ChangePartitionState(context.TODO(), 100, 500, pb.PartitionState_PartitionDropping, 1000)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		catalog := mocks.NewRootCoordCatalog(t)
		catalog.On("AlterPartition",
			mock.Anything, // context.Context
			mock.Anything, // *model.Partition
			mock.Anything, // *model.Partition
			mock.Anything, // metastore.AlterType
			mock.AnythingOfType("uint64"),
		).Return(nil)
		meta := &MetaTable{
			catalog: catalog,
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					Name: "test", CollectionID: 100,
					Partitions: []*model.Partition{
						{CollectionID: 100, PartitionID: 500},
					},
				},
			},
		}
		err := meta.ChangePartitionState(context.TODO(), 100, 500, pb.PartitionState_PartitionCreated, 1000)
		assert.NoError(t, err)
		err = meta.ChangePartitionState(context.TODO(), 100, 500, pb.PartitionState_PartitionDropping, 1000)
		assert.NoError(t, err)
	})
}
