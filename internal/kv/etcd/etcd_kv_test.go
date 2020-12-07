package etcdkv_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
	"go.etcd.io/etcd/clientv3"
)

var Params paramtable.BaseTable

func TestMain(m *testing.M) {
	Params.Init()
	code := m.Run()
	os.Exit(code)
}

func TestEtcdKV_Load(t *testing.T) {

	etcdAddr, err := Params.Load("_EtcdAddress")
	if err != nil {
		panic(err)
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
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

	etcdAddr, err := Params.Load("_EtcdAddress")
	if err != nil {
		panic(err)
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
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

	etcdAddr, err := Params.Load("_EtcdAddress")
	if err != nil {
		panic(err)
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
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

	etcdAddr, err := Params.Load("_EtcdAddress")
	if err != nil {
		panic(err)
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
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
