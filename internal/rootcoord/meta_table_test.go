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
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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
	randVal := rand.Int()
	Params.Init()
	rootPath := fmt.Sprintf("/test/meta/%d", randVal)
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	require.Nil(t, err)

	skv, err := rootcoord.NewMetaSnapshot(etcdCli, rootPath, TimestampPrefix, 7)
	assert.Nil(t, err)
	assert.NotNil(t, skv)

	txnkv := etcdkv.NewEtcdKV(etcdCli, rootPath)
	_, err = NewMetaTable(context.TODO(), &rootcoord.Catalog{Txn: txnkv, Snapshot: skv})
	assert.Nil(t, err)
	mockSnapshotKV := &mockTestKV{
		SnapShotKV: skv,
		loadWithPrefix: func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return skv.LoadWithPrefix(key, ts)
		},
	}
	mockTxnKV := &mockTestTxnKV{
		TxnKV:          txnkv,
		loadWithPrefix: func(key string) ([]string, []string, error) { return txnkv.LoadWithPrefix(key) },
		save:           func(key, value string) error { return txnkv.Save(key, value) },
		multiSave:      func(kvs map[string]string) error { return txnkv.MultiSave(kvs) },
		multiSaveAndRemoveWithPrefix: func(kvs map[string]string, removal []string) error {
			return txnkv.MultiSaveAndRemoveWithPrefix(kvs, removal)
		},
		remove: func(key string) error { return txnkv.Remove(key) },
	}

	mockMt, err := NewMetaTable(context.TODO(), &rootcoord.Catalog{Txn: mockTxnKV, Snapshot: mockSnapshotKV})
	assert.Nil(t, err)
	return mockMt, mockSnapshotKV, mockTxnKV, func() {
		etcdCli.Close()
	}
}

func TestMetaTable(t *testing.T) {
	const (
		collName        = "testColl"
		collNameInvalid = "testColl_invalid"
		aliasName1      = "alias1"
		aliasName2      = "alias2"
		collID          = typeutil.UniqueID(1)
		collIDInvalid   = typeutil.UniqueID(2)
		partIDDefault   = typeutil.UniqueID(10)
		partID          = typeutil.UniqueID(20)
		partName        = "testPart"
		partIDInvalid   = typeutil.UniqueID(21)
		segID           = typeutil.UniqueID(100)
		segID2          = typeutil.UniqueID(101)
		fieldID         = typeutil.UniqueID(110)
		fieldID2        = typeutil.UniqueID(111)
		indexID         = typeutil.UniqueID(10000)
		indexID2        = typeutil.UniqueID(10001)
		buildID         = typeutil.UniqueID(201)
		indexName       = "testColl_index_110"
	)

	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()
	Params.Init()
	rootPath := fmt.Sprintf("/test/meta/%d", randVal)

	var vtso typeutil.Timestamp
	ftso := func() typeutil.Timestamp {
		vtso++
		return vtso
	}

	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	require.Nil(t, err)
	defer etcdCli.Close()

	skv, err := rootcoord.NewMetaSnapshot(etcdCli, rootPath, TimestampPrefix, 7)
	assert.Nil(t, err)
	assert.NotNil(t, skv)
	txnKV := etcdkv.NewEtcdKV(etcdCli, rootPath)
	mt, err := NewMetaTable(context.TODO(), &rootcoord.Catalog{Txn: txnKV, Snapshot: skv})
	assert.Nil(t, err)

	collInfo := &model.Collection{
		CollectionID: collID,
		Name:         collName,
		AutoID:       false,
		Fields: []*model.Field{
			{
				FieldID:      fieldID,
				Name:         "field110",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "field110-k1",
						Value: "field110-v1",
					},
					{
						Key:   "field110-k2",
						Value: "field110-v2",
					},
				},
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key:   "field110-i1",
						Value: "field110-v1",
					},
					{
						Key:   "field110-i2",
						Value: "field110-v2",
					},
				},
			},
		},
		CreateTime: 0,
		Partitions: []*model.Partition{
			{
				PartitionID:               partIDDefault,
				PartitionName:             Params.CommonCfg.DefaultPartitionName,
				PartitionCreatedTimestamp: 0,
			},
		},
		VirtualChannelNames: []string{
			fmt.Sprintf("dmChannel_%dv%d", collID, 0),
			fmt.Sprintf("dmChannel_%dv%d", collID, 1),
		},
		PhysicalChannelNames: []string{
			funcutil.ToPhysicalChannel(fmt.Sprintf("dmChannel_%dv%d", collID, 0)),
			funcutil.ToPhysicalChannel(fmt.Sprintf("dmChannel_%dv%d", collID, 1)),
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	t.Run("add collection", func(t *testing.T) {
		defer wg.Done()
		ts := ftso()

		err = mt.AddCollection(collInfo, ts, "")
		assert.Nil(t, err)
		assert.Equal(t, uint64(1), ts)

		collMeta, err := mt.GetCollectionByName(collName, ts)
		assert.Nil(t, err)
		assert.Equal(t, collMeta.CreateTime, ts)
		assert.Equal(t, collMeta.Partitions[0].PartitionCreatedTimestamp, ts)

		assert.Equal(t, partIDDefault, collMeta.Partitions[0].PartitionID)
		assert.Equal(t, 1, len(collMeta.Partitions))
		assert.True(t, mt.HasCollection(collInfo.CollectionID, 0))

		field, err := mt.GetFieldSchema(collName, "field110")
		assert.Nil(t, err)
		assert.Equal(t, collInfo.Fields[0].FieldID, field.FieldID)
	})

	wg.Add(1)
	t.Run("add alias", func(t *testing.T) {
		defer wg.Done()
		ts := ftso()
		exists := mt.IsAlias(aliasName1)
		assert.False(t, exists)
		err = mt.AddAlias(aliasName1, collName, ts)
		assert.Nil(t, err)
		aliases := mt.ListAliases(collID)
		assert.Equal(t, aliases, []string{aliasName1})
		exists = mt.IsAlias(aliasName1)
		assert.True(t, exists)
	})
	wg.Add(1)
	t.Run("alter alias", func(t *testing.T) {
		defer wg.Done()
		ts := ftso()
		err = mt.AlterAlias(aliasName1, collName, ts)
		assert.Nil(t, err)
		err = mt.AlterAlias(aliasName1, collNameInvalid, ts)
		assert.NotNil(t, err)
	})

	wg.Add(1)
	t.Run("delete alias", func(t *testing.T) {
		defer wg.Done()
		ts := ftso()
		err = mt.DropAlias(aliasName1, ts)
		assert.Nil(t, err)
	})

	wg.Add(1)
	t.Run("not load alias when load collection meta", func(t *testing.T) {
		defer wg.Done()
		ts := ftso()
		err = mt.AddAlias(aliasName1, collName, ts)
		assert.Nil(t, err)
		err = mt.reloadFromCatalog()
		assert.Nil(t, err)
		_, ok := mt.collName2ID[aliasName1]
		assert.False(t, ok)
	})

	wg.Add(1)
	t.Run("add partition", func(t *testing.T) {
		defer wg.Done()
		ts := ftso()
		err = mt.AddPartition(collID, partName, partID, ts, "")
		assert.Nil(t, err)
		//assert.Equal(t, ts, uint64(2))

		collMeta, ok := mt.collID2Meta[collID]
		assert.True(t, ok)
		assert.Equal(t, 2, len(collMeta.Partitions))
		assert.Equal(t, collMeta.Partitions[1].PartitionName, partName)
		assert.Equal(t, ts, collMeta.Partitions[1].PartitionCreatedTimestamp)
	})

	wg.Add(1)
	t.Run("drop partition", func(t *testing.T) {
		defer wg.Done()
		ts := ftso()
		id, err := mt.DeletePartition(collID, partName, ts, "")
		assert.Nil(t, err)
		assert.Equal(t, partID, id)
	})

	wg.Add(1)
	t.Run("drop collection", func(t *testing.T) {
		defer wg.Done()
		ts := ftso()
		err = mt.DeleteCollection(collIDInvalid, ts, "")
		assert.NotNil(t, err)
		ts2 := ftso()
		err = mt.AddAlias(aliasName2, collName, ts2)
		assert.Nil(t, err)
		err = mt.DeleteCollection(collID, ts, "")
		assert.Nil(t, err)
		ts3 := ftso()
		err = mt.DropAlias(aliasName2, ts3)
		assert.NotNil(t, err)
	})

	wg.Add(1)
	t.Run("delete credential", func(t *testing.T) {
		defer wg.Done()

		err = mt.DeleteCredential("")
		assert.Nil(t, err)

		err = mt.DeleteCredential("abcxyz")
		assert.Nil(t, err)
	})

	/////////////////////////// these tests should run at last, it only used to hit the error lines ////////////////////////
	txnkv := etcdkv.NewEtcdKV(etcdCli, rootPath)
	mockKV := &mockTestKV{
		loadWithPrefix: func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		},
	}
	mockTxnKV := &mockTestTxnKV{
		TxnKV:          txnkv,
		loadWithPrefix: func(key string) ([]string, []string, error) { return txnkv.LoadWithPrefix(key) },
		save:           func(key, value string) error { return txnkv.Save(key, value) },
		multiSave:      func(kvs map[string]string) error { return txnkv.MultiSave(kvs) },
		multiSaveAndRemoveWithPrefix: func(kvs map[string]string, removal []string) error {
			return txnkv.MultiSaveAndRemoveWithPrefix(kvs, removal)
		},
		remove: func(key string) error { return txnkv.Remove(key) },
	}

	mt, err = NewMetaTable(context.TODO(), &rootcoord.Catalog{Txn: mockTxnKV, Snapshot: mockKV})
	assert.Nil(t, err)

	wg.Add(1)
	t.Run("add collection failed", func(t *testing.T) {
		defer wg.Done()
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return fmt.Errorf("multi save error")
		}
		collInfo.Partitions = []*model.Partition{}
		assert.Error(t, mt.AddCollection(collInfo, 0, ""))
	})

	wg.Add(1)
	t.Run("delete collection failed", func(t *testing.T) {
		defer wg.Done()
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return nil
		}
		mockKV.multiSaveAndRemoveWithPrefix = func(save map[string]string, keys []string, ts typeutil.Timestamp) error {
			return fmt.Errorf("multi save and remove with prefix error")
		}
		ts := ftso()
		assert.Error(t, mt.DeleteCollection(collInfo.CollectionID, ts, ""))
	})

	wg.Add(1)
	t.Run("get collection failed", func(t *testing.T) {
		defer wg.Done()
		mockKV.save = func(key string, value string, ts typeutil.Timestamp) error {
			return nil
		}

		ts := ftso()
		collInfo.Partitions = []*model.Partition{}
		err = mt.AddCollection(collInfo, ts, "")
		assert.Nil(t, err)

		mt.collID2Meta = make(map[int64]model.Collection)
		_, err = mt.GetCollectionByName(collInfo.Name, 0)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("can't find collection %s with id %d", collInfo.Name, collInfo.CollectionID))

	})

	wg.Add(1)
	t.Run("add partition failed", func(t *testing.T) {
		defer wg.Done()
		mockKV.save = func(key string, value string, ts typeutil.Timestamp) error {
			return nil
		}
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		err := mt.reloadFromCatalog()
		assert.Nil(t, err)

		ts := ftso()
		collInfo.Partitions = []*model.Partition{}
		err = mt.AddCollection(collInfo, ts, "")
		assert.Nil(t, err)

		ts = ftso()
		err = mt.AddPartition(2, "no-part", 22, ts, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "can't find collection. id = 2")

		coll := mt.collID2Meta[collInfo.CollectionID]
		coll.Partitions = make([]*model.Partition, Params.RootCoordCfg.MaxPartitionNum)
		mt.collID2Meta[coll.CollectionID] = coll
		err = mt.AddPartition(coll.CollectionID, "no-part", 22, ts, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("maximum partition's number should be limit to %d", Params.RootCoordCfg.MaxPartitionNum))

		coll.Partitions = []*model.Partition{{PartitionID: partID, PartitionName: partName, PartitionCreatedTimestamp: ftso()}}
		mt.collID2Meta[coll.CollectionID] = coll

		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return fmt.Errorf("multi save error")
		}
		tmpSaveFunc := mockKV.save
		mockKV.save = func(key, value string, ts typeutil.Timestamp) error {
			return errors.New("mock")
		}
		assert.Error(t, mt.AddPartition(coll.CollectionID, "no-part", 22, ts, ""))
		mockKV.save = tmpSaveFunc
		//err = mt.AddPartition(coll.CollectionID, "no-part", 22, ts, nil)
		//assert.NotNil(t, err)
		//assert.EqualError(t, err, "multi save error")

		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return nil
		}

		collInfo.Partitions = []*model.Partition{}
		ts = ftso()
		err = mt.AddPartition(coll.CollectionID, partName, 22, ts, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("partition name = %s already exists", partName))
		err = mt.AddPartition(coll.CollectionID, "no-part", partID, ts, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("partition id = %d already exists", partID))
	})

	wg.Add(1)
	t.Run("has partition failed", func(t *testing.T) {
		defer wg.Done()
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return nil
		}
		err := mt.reloadFromCatalog()
		assert.Nil(t, err)

		collInfo.Partitions = []*model.Partition{}
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, "")
		assert.Nil(t, err)

		assert.False(t, mt.HasPartition(collInfo.CollectionID, "no-partName", 0))

		mt.collID2Meta = make(map[int64]model.Collection)
		assert.False(t, mt.HasPartition(collInfo.CollectionID, partName, 0))
	})

	wg.Add(1)
	t.Run("delete partition failed", func(t *testing.T) {
		defer wg.Done()
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return nil
		}
		err := mt.reloadFromCatalog()
		assert.Nil(t, err)

		collInfo.Partitions = []*model.Partition{{PartitionID: partID, PartitionName: partName, PartitionCreatedTimestamp: ftso()}}
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, "")
		assert.Nil(t, err)

		ts = ftso()
		_, err = mt.DeletePartition(collInfo.CollectionID, Params.CommonCfg.DefaultPartitionName, ts, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "default partition cannot be deleted")

		_, err = mt.DeletePartition(collInfo.CollectionID, "abc", ts, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "partition abc does not exist")

		mockKV.save = func(key, value string, ts typeutil.Timestamp) error { return errors.New("mocked error") }
		_, err = mt.DeletePartition(collInfo.CollectionID, partName, ts, "")
		assert.Error(t, err)

		mt.collID2Meta = make(map[int64]model.Collection)
		_, err = mt.DeletePartition(collInfo.CollectionID, "abc", ts, "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("can't find collection id = %d", collInfo.CollectionID))
	})

	wg.Add(1)
	t.Run("get field schema failed", func(t *testing.T) {
		defer wg.Done()
		mockKV.loadWithPrefix = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, nil
		}
		mockKV.multiSave = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return nil
		}
		mockKV.save = func(key string, value string, ts typeutil.Timestamp) error {
			return nil
		}
		err := mt.reloadFromCatalog()
		assert.Nil(t, err)

		collInfo.Partitions = []*model.Partition{}
		ts := ftso()
		err = mt.AddCollection(collInfo, ts, "")
		assert.Nil(t, err)

		mt.collID2Meta = make(map[int64]model.Collection)
		_, err = mt.getFieldSchemaInternal(collInfo.Name, collInfo.Fields[0].Name)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("collection %s not found", collInfo.Name))

		mt.collName2ID = make(map[string]int64)
		_, err = mt.getFieldSchemaInternal(collInfo.Name, collInfo.Fields[0].Name)
		assert.NotNil(t, err)
		assert.EqualError(t, err, fmt.Sprintf("collection %s not found", collInfo.Name))
	})

	wg.Add(1)
	t.Run("add credential failed", func(t *testing.T) {
		defer wg.Done()
		mockTxnKV.loadWithPrefix = func(key string) ([]string, []string, error) {
			return []string{}, []string{}, nil
		}
		mockTxnKV.load = func(key string) (string, error) {
			return "", errors.New("test error")
		}
		mockTxnKV.save = func(key, value string) error {
			return fmt.Errorf("save error")
		}
		err = mt.AddCredential(&internalpb.CredentialInfo{Username: "x", EncryptedPassword: "a\xc5z"})
		assert.Error(t, err)
	})

	wg.Add(1)
	t.Run("alter credential failed", func(t *testing.T) {
		defer wg.Done()
		mockTxnKV.save = func(key, value string) error {
			return fmt.Errorf("save error")
		}
		err = mt.AlterCredential(&internalpb.CredentialInfo{Username: "", EncryptedPassword: "az"})
		assert.Error(t, err)
	})

	wg.Add(1)
	t.Run("delete credential failed", func(t *testing.T) {
		defer wg.Done()
		mockTxnKV.remove = func(key string) error {
			return fmt.Errorf("delete error")
		}
		err := mt.DeleteCredential("")
		assert.Error(t, err)
	})

	wg.Wait()
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

func TestMetaWithTimestamp(t *testing.T) {
	const (
		collID1   = typeutil.UniqueID(1)
		collID2   = typeutil.UniqueID(2)
		collName1 = "t1"
		collName2 = "t2"
		partID1   = 11
		partID2   = 12
		partName1 = "p1"
		partName2 = "p2"
	)
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()
	Params.Init()
	rootPath := fmt.Sprintf("/test/meta/%d", randVal)

	var tsoStart typeutil.Timestamp = 100
	vtso := tsoStart
	ftso := func() typeutil.Timestamp {
		vtso++
		return vtso
	}
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	defer etcdCli.Close()

	skv, err := rootcoord.NewMetaSnapshot(etcdCli, rootPath, TimestampPrefix, 7)
	assert.Nil(t, err)
	assert.NotNil(t, skv)
	txnKV := etcdkv.NewEtcdKV(etcdCli, rootPath)
	mt, err := NewMetaTable(context.TODO(), &rootcoord.Catalog{Txn: txnKV, Snapshot: skv})
	assert.Nil(t, err)

	collInfo := &model.Collection{
		CollectionID: collID1,
		Name:         collName1,
	}

	collInfo.Partitions = []*model.Partition{{PartitionID: partID1, PartitionName: partName1, PartitionCreatedTimestamp: ftso()}}
	t1 := ftso()
	err = mt.AddCollection(collInfo, t1, "")
	assert.Nil(t, err)

	collInfo.CollectionID = collID2
	collInfo.Partitions = []*model.Partition{{PartitionID: partID2, PartitionName: partName2, PartitionCreatedTimestamp: ftso()}}
	collInfo.Name = collName2
	t2 := ftso()
	err = mt.AddCollection(collInfo, t2, "")
	assert.Nil(t, err)

	assert.True(t, mt.HasCollection(collID1, 0))
	assert.True(t, mt.HasCollection(collID2, 0))

	assert.True(t, mt.HasCollection(collID1, t2))
	assert.True(t, mt.HasCollection(collID2, t2))

	assert.True(t, mt.HasCollection(collID1, t1))
	assert.False(t, mt.HasCollection(collID2, t1))

	assert.False(t, mt.HasCollection(collID1, tsoStart))
	assert.False(t, mt.HasCollection(collID2, tsoStart))

	c1, err := mt.GetCollectionByID(collID1, 0)
	assert.Nil(t, err)
	c2, err := mt.GetCollectionByID(collID2, 0)
	assert.Nil(t, err)
	assert.Equal(t, collID1, c1.CollectionID)
	assert.Equal(t, collID2, c2.CollectionID)

	c1, err = mt.GetCollectionByID(collID1, t2)
	assert.Nil(t, err)
	c2, err = mt.GetCollectionByID(collID2, t2)
	assert.Nil(t, err)
	assert.Equal(t, collID1, c1.CollectionID)
	assert.Equal(t, collID2, c2.CollectionID)

	c1, err = mt.GetCollectionByID(collID1, t1)
	assert.Nil(t, err)
	c2, err = mt.GetCollectionByID(collID2, t1)
	assert.NotNil(t, err)
	assert.Equal(t, int64(1), c1.CollectionID)

	c1, err = mt.GetCollectionByID(collID1, tsoStart)
	assert.NotNil(t, err)
	c2, err = mt.GetCollectionByID(collID2, tsoStart)
	assert.NotNil(t, err)

	c1, err = mt.GetCollectionByName(collName1, 0)
	assert.Nil(t, err)
	c2, err = mt.GetCollectionByName(collName2, 0)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), c1.CollectionID)
	assert.Equal(t, int64(2), c2.CollectionID)

	c1, err = mt.GetCollectionByName(collName1, t2)
	assert.Nil(t, err)
	c2, err = mt.GetCollectionByName(collName2, t2)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), c1.CollectionID)
	assert.Equal(t, int64(2), c2.CollectionID)

	c1, err = mt.GetCollectionByName(collName1, t1)
	assert.Nil(t, err)
	c2, err = mt.GetCollectionByName(collName2, t1)
	assert.NotNil(t, err)
	assert.Equal(t, int64(1), c1.CollectionID)

	c1, err = mt.GetCollectionByName(collName1, tsoStart)
	assert.NotNil(t, err)
	c2, err = mt.GetCollectionByName(collName2, tsoStart)
	assert.NotNil(t, err)

	getKeys := func(m map[string]*model.Collection) []string {
		keys := make([]string, 0, len(m))
		for key := range m {
			keys = append(keys, key)
		}
		return keys
	}

	s1, err := mt.ListCollections(0)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(s1))
	assert.ElementsMatch(t, getKeys(s1), []string{collName1, collName2})

	s1, err = mt.ListCollections(t2)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(s1))
	assert.ElementsMatch(t, getKeys(s1), []string{collName1, collName2})

	s1, err = mt.ListCollections(t1)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(s1))
	assert.ElementsMatch(t, getKeys(s1), []string{collName1})

	s1, err = mt.ListCollections(tsoStart)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(s1))

	p1, err := mt.GetPartitionByName(collID1, partName1, 0)
	assert.Nil(t, err)
	p2, err := mt.GetPartitionByName(collID2, partName2, 0)
	assert.Nil(t, err)
	assert.Equal(t, int64(partID1), p1)
	assert.Equal(t, int64(partID2), p2)

	p1, err = mt.GetPartitionByName(collID1, partName1, t2)
	assert.Nil(t, err)
	p2, err = mt.GetPartitionByName(collID2, partName2, t2)
	assert.Nil(t, err)
	assert.Equal(t, int64(11), p1)
	assert.Equal(t, int64(12), p2)

	p1, err = mt.GetPartitionByName(1, partName1, t1)
	assert.Nil(t, err)
	_, err = mt.GetPartitionByName(2, partName2, t1)
	assert.NotNil(t, err)
	assert.Equal(t, int64(11), p1)

	_, err = mt.GetPartitionByName(1, partName1, tsoStart)
	assert.NotNil(t, err)
	_, err = mt.GetPartitionByName(2, partName2, tsoStart)
	assert.NotNil(t, err)

	var cID UniqueID
	cID, err = mt.GetCollectionIDByName(collName1)
	assert.NoError(t, err)
	assert.Equal(t, collID1, cID)

	_, err = mt.GetCollectionIDByName("badname")
	assert.Error(t, err)

	name, err := mt.GetCollectionNameByID(collID2)
	assert.Nil(t, err)
	assert.Equal(t, collName2, name)

	_, err = mt.GetCollectionNameByID(int64(999))
	assert.Error(t, err)
}

func TestFixIssue10540(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()
	Params.Init()
	rootPath := fmt.Sprintf("/test/meta/%d", randVal)

	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	defer etcdCli.Close()

	skv, err := rootcoord.NewMetaSnapshot(etcdCli, rootPath, TimestampPrefix, 7)
	assert.Nil(t, err)
	assert.NotNil(t, skv)
	txnKV := memkv.NewMemoryKV()

	_, err = NewMetaTable(context.TODO(), &rootcoord.Catalog{Txn: txnKV, Snapshot: skv})
	assert.Nil(t, err)
}

func TestMetaTable_unlockGetCollectionInfo(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		mt := &MetaTable{
			collName2ID: map[string]typeutil.UniqueID{"test": 100},
			collID2Meta: map[typeutil.UniqueID]model.Collection{
				100: {CollectionID: 100, Name: "test"},
			},
		}
		info, err := mt.getCollectionInfoInternal("test")
		assert.NoError(t, err)
		assert.Equal(t, UniqueID(100), info.CollectionID)
		assert.Equal(t, "test", info.Name)
	})

	t.Run("collection name not found", func(t *testing.T) {
		mt := &MetaTable{collName2ID: nil, collAlias2ID: nil}
		_, err := mt.getCollectionInfoInternal("test")
		assert.Error(t, err)
	})

	t.Run("name found, meta not found", func(t *testing.T) {
		mt := &MetaTable{
			collName2ID:  map[string]typeutil.UniqueID{"test": 100},
			collAlias2ID: nil,
			collID2Meta:  nil,
		}
		_, err := mt.getCollectionInfoInternal("test")
		assert.Error(t, err)
	})

	t.Run("alias found, meta not found", func(t *testing.T) {
		mt := &MetaTable{
			collName2ID:  nil,
			collAlias2ID: map[string]typeutil.UniqueID{"test": 100},
			collID2Meta:  nil,
		}
		_, err := mt.getCollectionInfoInternal("test")
		assert.Error(t, err)
	})
}

type MockedCatalog struct {
	mock.Mock
	metastore.RootCoordCatalog
	alterIndexParamsVerification  func(ctx context.Context, oldIndex *model.Index, newIndex *model.Index, alterType metastore.AlterType)
	createIndexParamsVerification func(ctx context.Context, col *model.Collection, index *model.Index)
	dropIndexParamsVerification   func(ctx context.Context, collectionInfo *model.Collection, dropIdxID typeutil.UniqueID)
}

func (mc *MockedCatalog) ListCollections(ctx context.Context, ts typeutil.Timestamp) (map[string]*model.Collection, error) {
	args := mc.Called()
	return args.Get(0).(map[string]*model.Collection), nil
}

func (mc *MockedCatalog) ListIndexes(ctx context.Context) ([]*model.Index, error) {
	args := mc.Called()
	return args.Get(0).([]*model.Index), nil
}

func (mc *MockedCatalog) ListAliases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Alias, error) {
	args := mc.Called()
	return args.Get(0).([]*model.Alias), nil
}

func (mc *MockedCatalog) AlterIndex(ctx context.Context, oldIndex *model.Index, newIndex *model.Index, alterType metastore.AlterType) error {
	if mc.alterIndexParamsVerification != nil {
		mc.alterIndexParamsVerification(ctx, oldIndex, newIndex, alterType)
	}
	args := mc.Called()
	err := args.Get(0)
	if err == nil {
		return nil
	}
	return err.(error)
}

func TestMetaTable_ReloadFromKV(t *testing.T) {
	mc := &MockedCatalog{}

	collectionName := "cn"
	collInfo := &model.Collection{
		CollectionID: 1,
		Name:         collectionName,
		AutoID:       false,
		Fields: []*model.Field{
			{
				FieldID:      1,
				Name:         "field110",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "field110-k1",
						Value: "field110-v1",
					},
				},
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key:   "field110-i1",
						Value: "field110-v1",
					},
				},
			},
		},
		Partitions: []*model.Partition{
			{
				PartitionID:               1,
				PartitionName:             Params.CommonCfg.DefaultPartitionName,
				PartitionCreatedTimestamp: 0,
			},
		},
		Aliases: []string{"a", "b"},
	}
	collections := map[string]*model.Collection{collectionName: collInfo}
	mc.On("ListCollections").Return(collections, nil)

	alias1 := *collInfo
	alias1.Name = collInfo.Aliases[0]

	alias2 := *collInfo
	alias2.Name = collInfo.Aliases[1]
	mc.On("ListAliases").Return([]*model.Alias{
		{
			CollectionID: collInfo.CollectionID,
			Name:         collInfo.Aliases[0],
		},
		{
			CollectionID: collInfo.CollectionID,
			Name:         collInfo.Aliases[1],
		},
	}, nil)

	mt := &MetaTable{}
	mt.catalog = mc
	mt.reloadFromCatalog()

	assert.True(t, len(mt.collID2Meta) == 1)
	assert.Equal(t, mt.collID2Meta[1], *collInfo)

	assert.True(t, len(mt.collName2ID) == 1)

	assert.True(t, len(mt.collAlias2ID) == 2)
	ret, ok := mt.collAlias2ID[collInfo.Aliases[0]]
	assert.True(t, ok)
	assert.Equal(t, int64(1), ret)
}
