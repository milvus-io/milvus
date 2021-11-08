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

package kv_test

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/paramtable"
)

var Params paramtable.GlobalParamTable

func TestMain(m *testing.M) {
	Params.Init()
	code := m.Run()
	os.Exit(code)
}

type mockTestKV struct {
	data map[string][]byte
}

func newMockTestKV() kv.ValueKV {
	return &mockTestKV{
		data: make(map[string][]byte),
	}
}

func (m *mockTestKV) Save(key string, value kv.Value) error {
	m.data[key] = value.Serialize()
	return nil
}

func (m *mockTestKV) Load(key string) (kv.Value, error) {
	d, ok := m.data[key]
	if !ok {
		return nil, errors.New("not found")
	}
	return kv.BytesValue(d), nil
}

func TestTKV(t *testing.T) {
	tkv := newMockTestKV()
	bv := kv.BytesValue([]byte{1, 2, 3})
	sv := kv.StringValue("test")
	tkv.Save("1", bv)
	tkv.Save("2", sv)

	v, err := tkv.Load("1")
	assert.NoError(t, err)

	assert.EqualValues(t, bv.Serialize(), v.Serialize())

	v, err = tkv.Load("2")
	assert.NoError(t, err)
	assert.EqualValues(t, sv.String(), v.String())
}

func TestBaseKVStandardBehaviour(t *testing.T) {
	t.Run("Test Load", func(t *testing.T) {
		etcdCli, err := etcd.GetEtcdClient(&Params.BaseParams)
		require.NoError(t, err)
		etcdRootPath := "/basekv/test/root/load"
		etcdKV := etcdkv.NewEtcdKV(etcdCli, etcdRootPath)

		err = etcdKV.RemoveWithPrefix("")
		require.NoError(t, err)

		defer etcdKV.Close()
		defer etcdKV.RemoveWithPrefix("")

		loadTests := []struct {
			isValid     bool
			baseKV      kv.BaseKV
			description string
		}{
			{true, memkv.NewMemoryKV(), "valid MemoryKV load"},
			{false, memkv.NewMemoryKV(), "false MemoryKV load"},
			// Fail for milvus-io/milvus#11313
			// {true, etcdKV, "valid EtcdKV load"},
			// {false, etcdKV, "false EtcdKV load"},
			// Missing embedded etcdkv
			// Missing minIO
			// Missing rocksdbkv
		}

		for _, loadTest := range loadTests {
			t.Run(loadTest.description, func(t *testing.T) {
				prepareData := map[string]string{
					"test1":   "value1",
					"test2":   "value2",
					"test1/a": "value_a",
					"test1/b": "value_b",
				}
				err := loadTest.baseKV.MultiSave(prepareData)
				require.NoError(t, err)

				if loadTest.isValid {

					validLoadTests := []struct {
						inKey     string
						expectedV string
					}{
						{"test1", "value1"},
						{"test2", "value2"},
						{"test1/a", "value_a"},
						{"test1/b", "value_b"},
					}

					for _, test := range validLoadTests {
						gotV, err := loadTest.baseKV.Load(test.inKey)
						assert.NoError(t, err)
						assert.Equal(t, test.expectedV, gotV)
					}

				} else {
					invalidLoadTests := []struct {
						inKey     string
						expectedV string
					}{
						{"t", ""},
						{"a", ""},
						{"b", ""},
						{"", ""},
					}
					for _, test := range invalidLoadTests {
						gotV, err := loadTest.baseKV.Load(test.inKey)
						assert.Error(t, err)
						assert.ErrorIs(t, kv.ErrorKeyInvalid, err)
						assert.Equal(t, test.expectedV, gotV)
					}
				}
			})
		}
	})
}
