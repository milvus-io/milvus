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
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/metastore/model"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type mockTestKV struct {
	kv.SnapShotKV

	loadWithPrefix               func(key string, ts typeutil.Timestamp) ([]string, []string, error)
	save                         func(key, value string, ts typeutil.Timestamp) error
	multiSave                    func(kvs map[string]string, ts typeutil.Timestamp) error
	multiSaveAndRemoveWithPrefix func(saves map[string]string, removals []string, ts typeutil.Timestamp) error
}

func (m *mockTestKV) LoadWithPrefix(key string, ts typeutil.Timestamp) ([]string, []string, error) {
	return m.loadWithPrefix(key, ts)
}
func (m *mockTestKV) Load(key string, ts typeutil.Timestamp) (string, error) {
	return "", nil
}

func (m *mockTestKV) Save(key, value string, ts typeutil.Timestamp) error {
	return m.save(key, value, ts)
}

func (m *mockTestKV) MultiSave(kvs map[string]string, ts typeutil.Timestamp) error {
	return m.multiSave(kvs, ts)
}

func (m *mockTestKV) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string, ts typeutil.Timestamp) error {
	return m.multiSaveAndRemoveWithPrefix(saves, removals, ts)
}

type mockTestTxnKV struct {
	kv.TxnKV
	loadWithPrefix               func(key string) ([]string, []string, error)
	save                         func(key, value string) error
	multiSave                    func(kvs map[string]string) error
	multiSaveAndRemoveWithPrefix func(saves map[string]string, removals []string) error
	remove                       func(key string) error
	multiRemove                  func(keys []string) error
	load                         func(key string) (string, error)
	removeWithPrefix             func(key string) error
}

func (m *mockTestTxnKV) LoadWithPrefix(key string) ([]string, []string, error) {
	return m.loadWithPrefix(key)
}

func (m *mockTestTxnKV) Save(key, value string) error {
	return m.save(key, value)
}

func (m *mockTestTxnKV) MultiSave(kvs map[string]string) error {
	return m.multiSave(kvs)
}

func (m *mockTestTxnKV) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string) error {
	return m.multiSaveAndRemoveWithPrefix(saves, removals)
}

func (m *mockTestTxnKV) Remove(key string) error {
	return m.remove(key)
}

func (m *mockTestTxnKV) MultiRemove(keys []string) error {
	return m.multiRemove(keys)
}

func (m *mockTestTxnKV) Load(key string) (string, error) {
	return m.load(key)
}

func (m *mockTestTxnKV) RemoveWithPrefix(key string) error {
	return m.removeWithPrefix(key)
}

func generateMetaTable(t *testing.T) (*MetaTable, *mockTestKV, *mockTestTxnKV, func()) {
	rand.Seed(time.Now().UnixNano())
	Params.Init()

	mockSnapshotKV := &mockTestKV{
		loadWithPrefix: func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		},
	}
	mockTxnKV := &mockTestTxnKV{
		loadWithPrefix:               func(key string) ([]string, []string, error) { return nil, nil, nil },
		save:                         func(key, value string) error { return nil },
		multiSave:                    func(kvs map[string]string) error { return nil },
		multiSaveAndRemoveWithPrefix: func(kvs map[string]string, removal []string) error { return nil },
		remove:                       func(key string) error { return nil },
	}

	mockMt := &MetaTable{catalog: &rootcoord.Catalog{Txn: mockTxnKV, Snapshot: mockSnapshotKV}}
	return mockMt, mockSnapshotKV, mockTxnKV, func() {}
}

func TestRbacCreateRole(t *testing.T) {
	mt, _, mockTxnKV, closeCli := generateMetaTable(t)
	defer closeCli()
	var err error
	err = mt.CreateRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: ""})
	assert.NotNil(t, err)

	mockTxnKV.save = func(key, value string) error {
		return nil
	}
	mockTxnKV.load = func(key string) (string, error) {
		return "", common.NewKeyNotExistError(key)
	}
	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		return []string{}, []string{}, nil
	}
	err = mt.CreateRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: "role1"})
	assert.Nil(t, err)

	mockTxnKV.load = func(key string) (string, error) {
		return "", fmt.Errorf("load error")
	}
	err = mt.CreateRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: "role1"})
	assert.NotNil(t, err)

	mockTxnKV.load = func(key string) (string, error) {
		return "", nil
	}
	err = mt.CreateRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: "role1"})
	assert.Equal(t, true, common.IsIgnorableError(err))

	mockTxnKV.load = func(key string) (string, error) {
		return "", common.NewKeyNotExistError(key)
	}
	mockTxnKV.save = func(key, value string) error {
		return fmt.Errorf("save error")
	}
	err = mt.CreateRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: "role2"})
	assert.NotNil(t, err)

	mockTxnKV.save = func(key, value string) error {
		return nil
	}
	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		return []string{}, []string{}, fmt.Errorf("loadWithPrefix error")
	}
	err = mt.CreateRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: "role2"})
	assert.NotNil(t, err)

	Params.ProxyCfg.MaxRoleNum = 2
	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		return []string{key + "/a", key + "/b"}, []string{}, nil
	}
	err = mt.CreateRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: "role2"})
	assert.NotNil(t, err)
	Params.ProxyCfg.MaxRoleNum = 10

}

func TestRbacDropRole(t *testing.T) {
	mt, _, mockTxnKV, closeCli := generateMetaTable(t)
	defer closeCli()
	var err error

	mockTxnKV.remove = func(key string) error {
		return nil
	}

	err = mt.DropRole(util.DefaultTenant, "role1")
	assert.Nil(t, err)

	mockTxnKV.remove = func(key string) error {
		return fmt.Errorf("delete error")
	}

	err = mt.DropRole(util.DefaultTenant, "role2")
	assert.NotNil(t, err)
}

func TestRbacOperateRole(t *testing.T) {
	mt, _, mockTxnKV, closeCli := generateMetaTable(t)
	defer closeCli()
	var err error

	err = mt.OperateUserRole(util.DefaultTenant, &milvuspb.UserEntity{Name: " "}, &milvuspb.RoleEntity{Name: "role"}, milvuspb.OperateUserRoleType_AddUserToRole)
	assert.NotNil(t, err)

	err = mt.OperateUserRole(util.DefaultTenant, &milvuspb.UserEntity{Name: "user"}, &milvuspb.RoleEntity{Name: "  "}, milvuspb.OperateUserRoleType_AddUserToRole)
	assert.NotNil(t, err)

	mockTxnKV.save = func(key, value string) error {
		return nil
	}
	err = mt.OperateUserRole(util.DefaultTenant, &milvuspb.UserEntity{Name: "user"}, &milvuspb.RoleEntity{Name: "role"}, milvuspb.OperateUserRoleType(100))
	assert.NotNil(t, err)

	mockTxnKV.load = func(key string) (string, error) {
		return "", common.NewKeyNotExistError(key)
	}
	err = mt.OperateUserRole(util.DefaultTenant, &milvuspb.UserEntity{Name: "user"}, &milvuspb.RoleEntity{Name: "role"}, milvuspb.OperateUserRoleType_AddUserToRole)
	assert.Nil(t, err)

	mockTxnKV.save = func(key, value string) error {
		return fmt.Errorf("save error")
	}
	err = mt.OperateUserRole(util.DefaultTenant, &milvuspb.UserEntity{Name: "user"}, &milvuspb.RoleEntity{Name: "role"}, milvuspb.OperateUserRoleType_AddUserToRole)
	assert.NotNil(t, err)

	mockTxnKV.remove = func(key string) error {
		return nil
	}
	mockTxnKV.load = func(key string) (string, error) {
		return "", nil
	}
	err = mt.OperateUserRole(util.DefaultTenant, &milvuspb.UserEntity{Name: "user"}, &milvuspb.RoleEntity{Name: "role"}, milvuspb.OperateUserRoleType_RemoveUserFromRole)
	assert.Nil(t, err)

	mockTxnKV.remove = func(key string) error {
		return fmt.Errorf("remove error")
	}
	err = mt.OperateUserRole(util.DefaultTenant, &milvuspb.UserEntity{Name: "user"}, &milvuspb.RoleEntity{Name: "role"}, milvuspb.OperateUserRoleType_RemoveUserFromRole)
	assert.NotNil(t, err)
}

func TestRbacSelectRole(t *testing.T) {
	mt, _, mockTxnKV, closeCli := generateMetaTable(t)
	defer closeCli()
	var err error

	_, err = mt.SelectRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: ""}, false)
	assert.NotNil(t, err)

	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		return []string{}, []string{}, fmt.Errorf("load with prefix error")
	}
	_, err = mt.SelectRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: "role"}, true)
	assert.NotNil(t, err)

	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		return []string{key + "/key1", key + "/key2", key + "/a/err"}, []string{"value1", "value2", "values3"}, nil
	}
	mockTxnKV.load = func(key string) (string, error) {
		return "", nil
	}
	results, _ := mt.SelectRole(util.DefaultTenant, nil, false)
	assert.Equal(t, 2, len(results))
	results, _ = mt.SelectRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: "role"}, false)
	assert.Equal(t, 1, len(results))

	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		return []string{}, []string{}, fmt.Errorf("load with prefix error")
	}
	_, err = mt.SelectRole(util.DefaultTenant, nil, false)
	assert.NotNil(t, err)

	mockTxnKV.load = func(key string) (string, error) {
		return "", fmt.Errorf("load error")
	}
	_, err = mt.SelectRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: "role"}, false)
	assert.NotNil(t, err)

	roleName := "role1"
	mockTxnKV.load = func(key string) (string, error) {
		return "", nil
	}
	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		return []string{key + "/user1/" + roleName, key + "/user2/role2", key + "/user3/" + roleName, key + "/err"}, []string{"value1", "value2", "values3", "value4"}, nil
	}
	results, err = mt.SelectRole(util.DefaultTenant, &milvuspb.RoleEntity{Name: roleName}, true)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 2, len(results[0].Users))

	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		if key == rootcoord.RoleMappingPrefix {
			return []string{key + "/user1/role2", key + "/user2/role2", key + "/user1/role1", key + "/user2/role1"}, []string{"value1", "value2", "values3", "value4"}, nil
		} else if key == rootcoord.RolePrefix {
			return []string{key + "/role1", key + "/role2", key + "/role3"}, []string{"value1", "value2", "values3"}, nil
		} else {
			return []string{}, []string{}, fmt.Errorf("load with prefix error")
		}
	}
	results, err = mt.SelectRole(util.DefaultTenant, nil, true)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(results))
	for _, result := range results {
		if result.Role.Name == "role1" {
			assert.Equal(t, 2, len(result.Users))
		} else if result.Role.Name == "role2" {
			assert.Equal(t, 2, len(result.Users))
		}
	}
}

func TestRbacSelectUser(t *testing.T) {
	mt, _, mockTxnKV, closeCli := generateMetaTable(t)
	defer closeCli()
	var err error

	_, err = mt.SelectUser(util.DefaultTenant, &milvuspb.UserEntity{Name: ""}, false)
	assert.NotNil(t, err)

	credentialInfo := internalpb.CredentialInfo{
		EncryptedPassword: "password",
	}
	credentialInfoByte, _ := json.Marshal(credentialInfo)

	mockTxnKV.load = func(key string) (string, error) {
		return string(credentialInfoByte), nil
	}
	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		return []string{key + "/key1", key + "/key2"}, []string{string(credentialInfoByte), string(credentialInfoByte)}, nil
	}
	results, err := mt.SelectUser(util.DefaultTenant, &milvuspb.UserEntity{Name: "user"}, false)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	results, err = mt.SelectUser(util.DefaultTenant, nil, false)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(results))

	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		return []string{key + "/key1", key + "/key2", key + "/a/err"}, []string{"value1", "value2", "values3"}, nil
	}

	mockTxnKV.load = func(key string) (string, error) {
		return string(credentialInfoByte), nil
	}
	results, err = mt.SelectUser(util.DefaultTenant, &milvuspb.UserEntity{Name: "user"}, true)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 2, len(results[0].Roles))

	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		logger.Debug("simfg", zap.String("key", key))
		if strings.Contains(key, rootcoord.RoleMappingPrefix) {
			if strings.Contains(key, "user1") {
				return []string{key + "/role2", key + "/role1", key + "/role3"}, []string{"value1", "value4", "value2"}, nil
			} else if strings.Contains(key, "user2") {
				return []string{key + "/role2"}, []string{"value1"}, nil
			}
			return []string{}, []string{}, nil
		} else if key == rootcoord.CredentialPrefix {
			return []string{key + "/user1", key + "/user2", key + "/user3"}, []string{string(credentialInfoByte), string(credentialInfoByte), string(credentialInfoByte)}, nil
		} else {
			return []string{}, []string{}, fmt.Errorf("load with prefix error")
		}
	}
	results, err = mt.SelectUser(util.DefaultTenant, nil, true)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(results))
	for _, result := range results {
		if result.User.Name == "user1" {
			assert.Equal(t, 3, len(result.Roles))
		} else if result.User.Name == "user2" {
			assert.Equal(t, 1, len(result.Roles))
		}
	}

	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		return []string{}, []string{}, nil
	}
	_, err = mt.SelectUser(util.DefaultTenant, &milvuspb.UserEntity{Name: "user"}, true)
	assert.Nil(t, err)

	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		return []string{}, []string{}, fmt.Errorf("load with prefix error")
	}
	_, err = mt.SelectUser(util.DefaultTenant, &milvuspb.UserEntity{Name: "user"}, true)
	assert.NotNil(t, err)

	_, err = mt.SelectUser(util.DefaultTenant, nil, true)
	assert.NotNil(t, err)
}

func TestRbacOperatePrivilege(t *testing.T) {
	mt, _, mockTxnKV, closeCli := generateMetaTable(t)
	defer closeCli()
	var err error

	entity := &milvuspb.GrantEntity{}
	err = mt.OperatePrivilege(util.DefaultTenant, entity, milvuspb.OperatePrivilegeType_Grant)
	assert.NotNil(t, err)

	entity.ObjectName = "col1"
	err = mt.OperatePrivilege(util.DefaultTenant, entity, milvuspb.OperatePrivilegeType_Grant)
	assert.NotNil(t, err)

	entity.Object = &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()}
	err = mt.OperatePrivilege(util.DefaultTenant, entity, milvuspb.OperatePrivilegeType_Grant)
	assert.NotNil(t, err)

	entity.Role = &milvuspb.RoleEntity{Name: "admin"}
	err = mt.OperatePrivilege(util.DefaultTenant, entity, milvuspb.OperatePrivilegeType_Grant)
	assert.NotNil(t, err)

	entity.Grantor = &milvuspb.GrantorEntity{}
	err = mt.OperatePrivilege(util.DefaultTenant, entity, milvuspb.OperatePrivilegeType_Grant)
	assert.NotNil(t, err)

	entity.Grantor.Privilege = &milvuspb.PrivilegeEntity{Name: commonpb.ObjectPrivilege_PrivilegeLoad.String()}
	err = mt.OperatePrivilege(util.DefaultTenant, entity, milvuspb.OperatePrivilegeType_Grant)
	assert.NotNil(t, err)

	err = mt.OperatePrivilege(util.DefaultTenant, entity, 100)
	assert.NotNil(t, err)

	mockTxnKV.save = func(key, value string) error {
		return nil
	}
	mockTxnKV.load = func(key string) (string, error) {
		return "fail", fmt.Errorf("load error")
	}
	entity.Grantor.User = &milvuspb.UserEntity{Name: "user2"}
	err = mt.OperatePrivilege(util.DefaultTenant, entity, milvuspb.OperatePrivilegeType_Revoke)
	assert.NotNil(t, err)

	err = mt.OperatePrivilege(util.DefaultTenant, entity, milvuspb.OperatePrivilegeType_Grant)
	assert.NotNil(t, err)

	mockTxnKV.load = func(key string) (string, error) {
		return "", common.NewKeyNotExistError(key)
	}
	err = mt.OperatePrivilege(util.DefaultTenant, entity, milvuspb.OperatePrivilegeType_Revoke)
	assert.NotNil(t, err)
	assert.True(t, common.IsIgnorableError(err))

	mockTxnKV.load = func(key string) (string, error) {
		return "", common.NewKeyNotExistError(key)
	}
	err = mt.OperatePrivilege(util.DefaultTenant, entity, milvuspb.OperatePrivilegeType_Grant)
	assert.Nil(t, err)

	granteeKey := funcutil.HandleTenantForEtcdKey(rootcoord.GranteePrefix, util.DefaultTenant, fmt.Sprintf("%s/%s/%s", entity.Role.Name, entity.Object.Name, entity.ObjectName))
	granteeID := "123456"
	mockTxnKV.load = func(key string) (string, error) {
		if key == granteeKey {
			return granteeID, nil
		}
		return "", errors.New("test error")
	}
	err = mt.OperatePrivilege(util.DefaultTenant, entity, milvuspb.OperatePrivilegeType_Grant)
	assert.NotNil(t, err)

	mockTxnKV.load = func(key string) (string, error) {
		if key == granteeKey {
			return granteeID, nil
		}
		return "", common.NewKeyNotExistError(key)
	}
	err = mt.OperatePrivilege(util.DefaultTenant, entity, milvuspb.OperatePrivilegeType_Revoke)
	assert.NotNil(t, err)
	assert.True(t, common.IsIgnorableError(err))

	mockTxnKV.save = func(key, value string) error {
		return errors.New("test error")
	}
	err = mt.OperatePrivilege(util.DefaultTenant, entity, milvuspb.OperatePrivilegeType_Grant)
	assert.NotNil(t, err)

	mockTxnKV.save = func(key, value string) error {
		return nil
	}
	err = mt.OperatePrivilege(util.DefaultTenant, entity, milvuspb.OperatePrivilegeType_Grant)
	assert.Nil(t, err)

	mockTxnKV.load = func(key string) (string, error) {
		if key == granteeKey {
			return granteeID, nil
		}
		return "", nil
	}
	err = mt.OperatePrivilege(util.DefaultTenant, entity, milvuspb.OperatePrivilegeType_Grant)
	assert.NotNil(t, err)
	assert.True(t, common.IsIgnorableError(err))

	mockTxnKV.load = func(key string) (string, error) {
		if key == granteeKey {
			return granteeID, nil
		}
		return "", nil
	}
	mockTxnKV.remove = func(key string) error {
		return nil
	}
	err = mt.OperatePrivilege(util.DefaultTenant, entity, milvuspb.OperatePrivilegeType_Revoke)
	assert.Nil(t, err)

	mockTxnKV.remove = func(key string) error {
		return errors.New("test error")
	}
	err = mt.OperatePrivilege(util.DefaultTenant, entity, milvuspb.OperatePrivilegeType_Revoke)
	assert.NotNil(t, err)
}

func TestRbacSelectGrant(t *testing.T) {
	mt, _, mockTxnKV, closeCli := generateMetaTable(t)
	defer closeCli()
	var err error

	entity := &milvuspb.GrantEntity{}
	_, err = mt.SelectGrant(util.DefaultTenant, entity)
	assert.NotNil(t, err)

	entity.Role = &milvuspb.RoleEntity{Name: ""}
	_, err = mt.SelectGrant(util.DefaultTenant, entity)
	assert.NotNil(t, err)

	entity.Role = &milvuspb.RoleEntity{Name: "admin"}
	entity.ObjectName = "Collection"
	entity.Object = &milvuspb.ObjectEntity{Name: "col"}
	mockTxnKV.load = func(key string) (string, error) {
		return "", errors.New("test error")
	}
	_, err = mt.SelectGrant(util.DefaultTenant, entity)
	assert.NotNil(t, err)

	granteeID := "123456"
	mockTxnKV.load = func(key string) (string, error) {
		return granteeID, nil
	}
	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		return nil, nil, errors.New("test error")
	}
	_, err = mt.SelectGrant(util.DefaultTenant, entity)
	assert.NotNil(t, err)

	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		return []string{key + "/PrivilegeInsert", key + "/*", key + "/a/b"}, []string{"root", "root", "root"}, nil
	}
	grantEntities, err := mt.SelectGrant(util.DefaultTenant, entity)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(grantEntities))

	entity.Role = &milvuspb.RoleEntity{Name: "role1"}
	entity.ObjectName = ""
	entity.Object = nil
	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		return nil, nil, errors.New("test error")
	}
	_, err = mt.SelectGrant(util.DefaultTenant, entity)
	assert.NotNil(t, err)

	granteeID1 := "123456"
	granteeID2 := "147258"
	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		if key == funcutil.HandleTenantForEtcdKey(rootcoord.GranteePrefix, util.DefaultTenant, entity.Role.Name) {
			return []string{key + "/Collection/col1", key + "/Collection/col2", key + "/Collection/col1/x"}, []string{granteeID1, granteeID1, granteeID2}, nil
		}
		return nil, nil, errors.New("test error")
	}
	_, err = mt.SelectGrant(util.DefaultTenant, entity)
	assert.NotNil(t, err)

	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		if key == funcutil.HandleTenantForEtcdKey(rootcoord.GranteePrefix, util.DefaultTenant, entity.Role.Name) {
			return []string{key + "/Collection/col1", key + "/Collection/col2", key + "/Collection/col1/x"}, []string{granteeID1, granteeID1, granteeID2}, nil
		}
		return []string{key + "/PrivilegeInsert", key + "/*", key + "/a/b"}, []string{"root", "root", "root"}, nil
	}
	grantEntities, err = mt.SelectGrant(util.DefaultTenant, entity)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(grantEntities))
}

func TestRbacDropGrant(t *testing.T) {
	mt, _, mockTxnKV, closeCli := generateMetaTable(t)
	defer closeCli()
	var (
		roleName = "foo"
		entity   *milvuspb.RoleEntity
		err      error
	)

	err = mt.DropGrant(util.DefaultTenant, nil)
	assert.Error(t, err)

	entity = &milvuspb.RoleEntity{Name: ""}
	err = mt.DropGrant(util.DefaultTenant, entity)
	assert.Error(t, err)

	entity.Name = roleName
	mockTxnKV.removeWithPrefix = func(key string) error {
		return nil
	}
	err = mt.DropGrant(util.DefaultTenant, entity)
	assert.NoError(t, err)

	mockTxnKV.removeWithPrefix = func(key string) error {
		return errors.New("test error")
	}
	err = mt.DropGrant(util.DefaultTenant, entity)
	assert.Error(t, err)
}

func TestRbacListPolicy(t *testing.T) {
	mt, _, mockTxnKV, closeCli := generateMetaTable(t)
	defer closeCli()

	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		return []string{}, []string{}, fmt.Errorf("load with prefix err")
	}
	policies, err := mt.ListPolicy(util.DefaultTenant)
	assert.Error(t, err)
	assert.Equal(t, 0, len(policies))

	granteeID1 := "123456"
	granteeID2 := "147258"
	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		if key == funcutil.HandleTenantForEtcdKey(rootcoord.GranteePrefix, util.DefaultTenant, "") {
			return []string{key + "/alice/collection/col1", key + "/tom/collection/col2", key + "/tom/collection/a/col2"}, []string{granteeID1, granteeID2, granteeID2}, nil
		}
		return []string{}, []string{}, errors.New("test error")
	}
	_, err = mt.ListPolicy(util.DefaultTenant)
	assert.Error(t, err)

	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		if key == funcutil.HandleTenantForEtcdKey(rootcoord.GranteePrefix, util.DefaultTenant, "") {
			return []string{key + "/alice/collection/col1", key + "/tom/collection/col2", key + "/tom/collection/a/col2"}, []string{granteeID1, granteeID2, granteeID2}, nil
		}
		return []string{key + "/PrivilegeInsert", key + "/*", key + "/a/b"}, []string{"root", "root", "root"}, nil
	}
	policies, err = mt.ListPolicy(util.DefaultTenant)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(policies))
}

func TestRbacListUserRole(t *testing.T) {
	mt, _, mockTxnKV, closeCli := generateMetaTable(t)
	defer closeCli()
	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		return []string{}, []string{}, fmt.Errorf("load with prefix err")
	}
	userRoles, err := mt.ListUserRole(util.DefaultTenant)
	assert.NotNil(t, err)
	assert.Equal(t, 0, len(userRoles))

	mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
		return []string{key + "/user1/role2", key + "/user2/role2", key + "/user1/role1", key + "/user2/role1", key + "/user2/role1/a"}, []string{"value1", "value2", "values3", "value4", "value5"}, nil
	}
	userRoles, err = mt.ListUserRole(util.DefaultTenant)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(userRoles))
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
		_, err := meta.getCollectionByIDInternal(ctx, 100, 101)
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
		_, err := meta.getCollectionByIDInternal(ctx, 100, 101)
		assert.Error(t, err)
		assert.True(t, common.IsCollectionNotExistError(err))
	})

	t.Run("normal case, filter unavailable partitions", func(t *testing.T) {
		Params.InitOnce()
		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					State:      pb.CollectionState_CollectionCreated,
					CreateTime: 99,
					Partitions: []*model.Partition{
						{PartitionID: 11, PartitionName: Params.CommonCfg.DefaultPartitionName, State: pb.PartitionState_PartitionCreated},
						{PartitionID: 22, PartitionName: "dropped", State: pb.PartitionState_PartitionDropped},
					},
				},
			},
		}
		ctx := context.Background()
		coll, err := meta.getCollectionByIDInternal(ctx, 100, 101)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(coll.Partitions))
		assert.Equal(t, UniqueID(11), coll.Partitions[0].PartitionID)
		assert.Equal(t, Params.CommonCfg.DefaultPartitionName, coll.Partitions[0].PartitionName)
	})

	t.Run("get latest version", func(t *testing.T) {
		Params.InitOnce()
		meta := &MetaTable{
			collID2Meta: map[typeutil.UniqueID]*model.Collection{
				100: {
					State:      pb.CollectionState_CollectionCreated,
					CreateTime: 99,
					Partitions: []*model.Partition{
						{PartitionID: 11, PartitionName: Params.CommonCfg.DefaultPartitionName, State: pb.PartitionState_PartitionCreated},
						{PartitionID: 22, PartitionName: "dropped", State: pb.PartitionState_PartitionDropped},
					},
				},
			},
		}
		ctx := context.Background()
		coll, err := meta.getCollectionByIDInternal(ctx, 100, typeutil.MaxTimestamp)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(coll.Partitions))
		assert.Equal(t, UniqueID(11), coll.Partitions[0].PartitionID)
		assert.Equal(t, Params.CommonCfg.DefaultPartitionName, coll.Partitions[0].PartitionName)
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
						{PartitionID: 11, PartitionName: Params.CommonCfg.DefaultPartitionName, State: pb.PartitionState_PartitionCreated},
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
		assert.Equal(t, Params.CommonCfg.DefaultPartitionName, coll.Partitions[0].PartitionName)
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
						{PartitionID: 11, PartitionName: Params.CommonCfg.DefaultPartitionName, State: pb.PartitionState_PartitionCreated},
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
		assert.Equal(t, Params.CommonCfg.DefaultPartitionName, coll.Partitions[0].PartitionName)
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
				{PartitionID: 11, PartitionName: Params.CommonCfg.DefaultPartitionName, State: pb.PartitionState_PartitionCreated},
				{PartitionID: 22, PartitionName: "dropped", State: pb.PartitionState_PartitionDropped},
			},
		}, nil)
		meta := &MetaTable{catalog: catalog}
		ctx := context.Background()
		coll, err := meta.GetCollectionByName(ctx, "name", 101)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(coll.Partitions))
		assert.Equal(t, UniqueID(11), coll.Partitions[0].PartitionID)
		assert.Equal(t, Params.CommonCfg.DefaultPartitionName, coll.Partitions[0].PartitionName)
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
		_, err := mt.getLatestCollectionByIDInternal(ctx, 100)
		assert.Error(t, err)
		assert.True(t, common.IsCollectionNotExistError(err))
	})

	t.Run("nil case", func(t *testing.T) {
		ctx := context.Background()
		mt := &MetaTable{collID2Meta: map[typeutil.UniqueID]*model.Collection{
			100: nil,
		}}
		_, err := mt.getLatestCollectionByIDInternal(ctx, 100)
		assert.Error(t, err)
		assert.True(t, common.IsCollectionNotExistError(err))
	})

	t.Run("unavailable", func(t *testing.T) {
		ctx := context.Background()
		mt := &MetaTable{collID2Meta: map[typeutil.UniqueID]*model.Collection{
			100: {State: pb.CollectionState_CollectionDropping},
		}}
		_, err := mt.getLatestCollectionByIDInternal(ctx, 100)
		assert.Error(t, err)
		assert.True(t, common.IsCollectionNotExistError(err))
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
		coll, err := mt.getLatestCollectionByIDInternal(ctx, 100)
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
