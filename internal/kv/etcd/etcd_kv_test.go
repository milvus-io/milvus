// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package etcdkv_test

import (
	"os"
	"strings"
	"testing"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/clientv3"
)

var Params paramtable.BaseTable

func TestMain(m *testing.M) {
	Params.Init()
	code := m.Run()
	os.Exit(code)
}

func newEtcdClient() (*clientv3.Client, error) {
	endpoints, err := Params.Load("_EtcdEndpoints")
	if err != nil {
		panic(err)
	}
	etcdEndpoints := strings.Split(endpoints, ",")
	return clientv3.New(clientv3.Config{Endpoints: etcdEndpoints})
}

func TestEtcdKV_Load(t *testing.T) {
	cli, err := newEtcdClient()
	assert.Nil(t, err)
	rootPath := "/etcd/test/root"
	etcdKV := etcdkv.NewEtcdKV(cli, rootPath)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	err = etcdKV.Save("abc", "123")
	assert.Nil(t, err)
	err = etcdKV.Save("abcd", "1234")
	assert.Nil(t, err)

	val, err := etcdKV.Load("abc")
	assert.Nil(t, err)
	assert.Equal(t, val, "123")

	keys, vals, err := etcdKV.LoadWithPrefix("abc")
	assert.Nil(t, err)
	assert.Equal(t, len(keys), len(vals))
	assert.Equal(t, len(keys), 2)

	assert.Equal(t, keys[0], etcdKV.GetPath("abc"))
	assert.Equal(t, keys[1], etcdKV.GetPath("abcd"))
	assert.Equal(t, vals[0], "123")
	assert.Equal(t, vals[1], "1234")

	err = etcdKV.Save("key_1", "123")
	assert.Nil(t, err)
	err = etcdKV.Save("key_2", "456")
	assert.Nil(t, err)
	err = etcdKV.Save("key_3", "789")
	assert.Nil(t, err)

	keys = []string{"key_1", "key_100"}

	vals, err = etcdKV.MultiLoad(keys)
	assert.NotNil(t, err)
	assert.Equal(t, len(vals), len(keys))
	assert.Equal(t, vals[0], "123")
	assert.Empty(t, vals[1])

	keys = []string{"key_1", "key_2"}

	vals, err = etcdKV.MultiLoad(keys)
	assert.Nil(t, err)
	assert.Equal(t, len(vals), len(keys))
	assert.Equal(t, vals[0], "123")
	assert.Equal(t, vals[1], "456")
}

func TestEtcdKV_MultiSave(t *testing.T) {
	cli, err := newEtcdClient()
	assert.Nil(t, err)
	rootPath := "/etcd/test/root"
	etcdKV := etcdkv.NewEtcdKV(cli, rootPath)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	err = etcdKV.Save("key_1", "111")
	assert.Nil(t, err)

	kvs := map[string]string{
		"key_1": "123",
		"key_2": "456",
	}

	err = etcdKV.MultiSave(kvs)
	assert.Nil(t, err)

	val, err := etcdKV.Load("key_1")
	assert.Nil(t, err)
	assert.Equal(t, val, "123")
}

func TestEtcdKV_Remove(t *testing.T) {
	cli, err := newEtcdClient()
	assert.Nil(t, err)
	rootPath := "/etcd/test/root"
	etcdKV := etcdkv.NewEtcdKV(cli, rootPath)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	err = etcdKV.Save("key_1", "123")
	assert.Nil(t, err)
	err = etcdKV.Save("key_2", "456")
	assert.Nil(t, err)

	val, err := etcdKV.Load("key_1")
	assert.Nil(t, err)
	assert.Equal(t, val, "123")
	// delete "key_1"
	err = etcdKV.Remove("key_1")
	assert.Nil(t, err)
	val, err = etcdKV.Load("key_1")
	assert.Error(t, err)
	assert.Empty(t, val)

	val, err = etcdKV.Load("key_2")
	assert.Nil(t, err)
	assert.Equal(t, val, "456")

	keys, vals, err := etcdKV.LoadWithPrefix("key")
	assert.Nil(t, err)
	assert.Equal(t, len(keys), len(vals))
	assert.Equal(t, len(keys), 1)

	assert.Equal(t, keys[0], etcdKV.GetPath("key_2"))
	assert.Equal(t, vals[0], "456")

	// MultiRemove
	err = etcdKV.Save("key_1", "111")
	assert.Nil(t, err)

	kvs := map[string]string{
		"key_1": "123",
		"key_2": "456",
		"key_3": "789",
		"key_4": "012",
	}

	err = etcdKV.MultiSave(kvs)
	assert.Nil(t, err)
	val, err = etcdKV.Load("key_1")
	assert.Nil(t, err)
	assert.Equal(t, val, "123")
	val, err = etcdKV.Load("key_3")
	assert.Nil(t, err)
	assert.Equal(t, val, "789")

	keys = []string{"key_1", "key_2", "key_3"}
	err = etcdKV.MultiRemove(keys)
	assert.Nil(t, err)

	val, err = etcdKV.Load("key_1")
	assert.Error(t, err)
	assert.Empty(t, val)
}

func TestEtcdKV_MultiSaveAndRemove(t *testing.T) {
	cli, err := newEtcdClient()
	assert.Nil(t, err)
	rootPath := "/etcd/test/root"
	etcdKV := etcdkv.NewEtcdKV(cli, rootPath)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	err = etcdKV.Save("key_1", "123")
	assert.Nil(t, err)
	err = etcdKV.Save("key_2", "456")
	assert.Nil(t, err)
	err = etcdKV.Save("key_3", "789")
	assert.Nil(t, err)

	kvs := map[string]string{
		"key_1": "111",
		"key_2": "444",
	}

	keys := []string{"key_3"}

	err = etcdKV.MultiSaveAndRemove(kvs, keys)
	assert.Nil(t, err)
	val, err := etcdKV.Load("key_1")
	assert.Nil(t, err)
	assert.Equal(t, val, "111")
	val, err = etcdKV.Load("key_2")
	assert.Nil(t, err)
	assert.Equal(t, val, "444")
	val, err = etcdKV.Load("key_3")
	assert.Error(t, err)
	assert.Empty(t, val)
}

func TestEtcdKV_MultiRemoveWithPrefix(t *testing.T) {
	cli, err := newEtcdClient()
	assert.Nil(t, err)
	rootPath := "/etcd/test/root"
	etcdKV := etcdkv.NewEtcdKV(cli, rootPath)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	err = etcdKV.Save("x/abc/1", "1")
	assert.Nil(t, err)
	err = etcdKV.Save("x/abc/2", "2")
	assert.Nil(t, err)
	err = etcdKV.Save("x/def/1", "10")
	assert.Nil(t, err)
	err = etcdKV.Save("x/def/2", "20")
	assert.Nil(t, err)

	err = etcdKV.MultiRemoveWithPrefix([]string{"x/abc", "x/def", "not-exist"})
	assert.Nil(t, err)
	k, v, err := etcdKV.LoadWithPrefix("x")
	assert.Nil(t, err)
	assert.Zero(t, len(k))
	assert.Zero(t, len(v))
}

func TestEtcdKV_MultiSaveAndRemoveWithPrefix(t *testing.T) {
	cli, err := newEtcdClient()
	assert.Nil(t, err)
	rootPath := "/etcd/test/root"
	etcdKV := etcdkv.NewEtcdKV(cli, rootPath)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	err = etcdKV.Save("x/abc/1", "1")
	assert.Nil(t, err)
	err = etcdKV.Save("x/abc/2", "2")
	assert.Nil(t, err)
	err = etcdKV.Save("x/def/1", "10")
	assert.Nil(t, err)
	err = etcdKV.Save("x/def/2", "20")
	assert.Nil(t, err)

	err = etcdKV.MultiSaveAndRemoveWithPrefix(map[string]string{"y/k1": "v1", "y/k2": "v2"}, []string{"x/abc", "x/def", "not-exist"})
	assert.Nil(t, err)
	k, v, err := etcdKV.LoadWithPrefix("x")
	assert.Nil(t, err)
	assert.Zero(t, len(k))
	assert.Zero(t, len(v))

	k, v, err = etcdKV.LoadWithPrefix("y")
	assert.Nil(t, err)
	assert.Equal(t, len(k), len(v))
	assert.Equal(t, len(k), 2)
	assert.Equal(t, k[0], rootPath+"/y/k1")
	assert.Equal(t, k[1], rootPath+"/y/k2")
	assert.Equal(t, v[0], "v1")
	assert.Equal(t, v[1], "v2")
}

func TestEtcdKV_Watch(t *testing.T) {
	cli, err := newEtcdClient()
	assert.Nil(t, err)
	rootPath := "/etcd/test/root"
	etcdKV := etcdkv.NewEtcdKV(cli, rootPath)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	ch := etcdKV.Watch("x/abc/1")
	resp := <-ch

	assert.True(t, resp.Created)
}

func TestEtcdKV_WatchPrefix(t *testing.T) {
	cli, err := newEtcdClient()
	assert.Nil(t, err)
	rootPath := "/etcd/test/root"
	etcdKV := etcdkv.NewEtcdKV(cli, rootPath)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	ch := etcdKV.WatchWithPrefix("x")
	resp := <-ch

	assert.True(t, resp.Created)
}

func TestEtcdKV_CompareAndSwap(t *testing.T) {
	cli, err := newEtcdClient()
	assert.Nil(t, err)
	rootPath := "/etcd/test/root"
	etcdKV := etcdkv.NewEtcdKV(cli, rootPath)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	err = etcdKV.CompareVersionAndSwap("a/b/c", 0, "1")
	assert.Nil(t, err)
	value, err := etcdKV.Load("a/b/c")
	assert.Nil(t, err)
	assert.Equal(t, value, "1")
	err = etcdKV.CompareVersionAndSwap("a/b/c", 0, "1")
	assert.NotNil(t, err)
	err = etcdKV.CompareValueAndSwap("a/b/c", "1", "2")
	assert.Nil(t, err)
	err = etcdKV.CompareValueAndSwap("a/b/c", "1", "2")
	assert.NotNil(t, err)
}
