package kv

import (
	"context"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	gparams "github.com/zilliztech/milvus-distributed/internal/util/paramtableutil"
	"go.etcd.io/etcd/clientv3"
)

func TestEtcdKV_Load(t *testing.T) {
	err := gparams.GParams.LoadYaml("config.yaml")
	if err != nil {
		panic(err)
	}
	etcdPort, err := gparams.GParams.Load("etcd.port")
	if err != nil {
		panic(err)
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:" + etcdPort}})
	assert.Nil(t, err)
	rootpath := "/etcd/test/root"
	kv := NewEtcdKV(cli, rootpath)
	ctx, cancel := context.WithTimeout(context.TODO(), requestTimeout)
	defer cancel()

	defer kv.Close()
	defer kv.client.Delete(ctx, rootpath, clientv3.WithPrefix())

	err = kv.Save("abc", "123")
	assert.Nil(t, err)
	err = kv.Save("abcd", "1234")
	assert.Nil(t, err)

	val, err := kv.Load("abc")
	assert.Nil(t, err)
	assert.Equal(t, val, "123")

	keys, vals, err := kv.LoadWithPrefix("abc")
	assert.Nil(t, err)
	assert.Equal(t, len(keys), len(vals))
	assert.Equal(t, len(keys), 2)

	assert.Equal(t, keys[0], path.Join(kv.rootPath, "abc"))
	assert.Equal(t, keys[1], path.Join(kv.rootPath, "abcd"))
	assert.Equal(t, vals[0], "123")
	assert.Equal(t, vals[1], "1234")

	err = kv.Save("key_1", "123")
	assert.Nil(t, err)
	err = kv.Save("key_2", "456")
	assert.Nil(t, err)
	err = kv.Save("key_3", "789")
	assert.Nil(t, err)

	keys = []string{"key_1", "key_100"}

	vals, err = kv.MultiLoad(keys)
	assert.NotNil(t, err)
	assert.Equal(t, len(vals), len(keys))
	assert.Equal(t, vals[0], "123")
	assert.Empty(t, vals[1])

	keys = []string{"key_1", "key_2"}

	vals, err = kv.MultiLoad(keys)
	assert.Nil(t, err)
	assert.Equal(t, len(vals), len(keys))
	assert.Equal(t, vals[0], "123")
	assert.Equal(t, vals[1], "456")
}

func TestEtcdKV_MultiSave(t *testing.T) {
	err := gparams.GParams.LoadYaml("config.yaml")
	if err != nil {
		panic(err)
	}
	etcdPort, err := gparams.GParams.Load("etcd.port")
	if err != nil {
		panic(err)
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:" + etcdPort}})
	assert.Nil(t, err)
	rootpath := "/etcd/test/root"
	kv := NewEtcdKV(cli, rootpath)
	ctx, cancel := context.WithTimeout(context.TODO(), requestTimeout)
	defer cancel()

	defer kv.Close()
	defer kv.client.Delete(ctx, rootpath, clientv3.WithPrefix())

	err = kv.Save("key_1", "111")
	assert.Nil(t, err)

	kvs := map[string]string{
		"key_1": "123",
		"key_2": "456",
	}

	err = kv.MultiSave(kvs)
	assert.Nil(t, err)

	val, err := kv.Load("key_1")
	assert.Nil(t, err)
	assert.Equal(t, val, "123")
}

func TestEtcdKV_Remove(t *testing.T) {
	err := gparams.GParams.LoadYaml("config.yaml")
	if err != nil {
		panic(err)
	}
	etcdPort, err := gparams.GParams.Load("etcd.port")
	if err != nil {
		panic(err)
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:" + etcdPort}})
	assert.Nil(t, err)
	rootpath := "/etcd/test/root"
	kv := NewEtcdKV(cli, rootpath)
	ctx, cancel := context.WithTimeout(context.TODO(), requestTimeout)
	defer cancel()

	defer kv.Close()
	defer kv.client.Delete(ctx, rootpath, clientv3.WithPrefix())

	err = kv.Save("key_1", "123")
	assert.Nil(t, err)
	err = kv.Save("key_2", "456")
	assert.Nil(t, err)

	val, err := kv.Load("key_1")
	assert.Nil(t, err)
	assert.Equal(t, val, "123")
	// delete "key_1"
	err = kv.Remove("key_1")
	assert.Nil(t, err)
	val, err = kv.Load("key_1")
	assert.Error(t, err)
	assert.Empty(t, val)

	val, err = kv.Load("key_2")
	assert.Nil(t, err)
	assert.Equal(t, val, "456")

	keys, vals, err := kv.LoadWithPrefix("key")
	assert.Nil(t, err)
	assert.Equal(t, len(keys), len(vals))
	assert.Equal(t, len(keys), 1)

	assert.Equal(t, keys[0], path.Join(kv.rootPath, "key_2"))
	assert.Equal(t, vals[0], "456")

	// MultiRemove
	err = kv.Save("key_1", "111")
	assert.Nil(t, err)

	kvs := map[string]string{
		"key_1": "123",
		"key_2": "456",
		"key_3": "789",
		"key_4": "012",
	}

	err = kv.MultiSave(kvs)
	assert.Nil(t, err)
	val, err = kv.Load("key_1")
	assert.Nil(t, err)
	assert.Equal(t, val, "123")
	val, err = kv.Load("key_3")
	assert.Nil(t, err)
	assert.Equal(t, val, "789")

	keys = []string{"key_1", "key_2", "key_3"}
	err = kv.MultiRemove(keys)
	assert.Nil(t, err)

	val, err = kv.Load("key_1")
	assert.Error(t, err)
	assert.Empty(t, val)
}

func TestEtcdKV_MultiSaveAndRemove(t *testing.T) {
	err := gparams.GParams.LoadYaml("config.yaml")
	if err != nil {
		panic(err)
	}
	etcdPort, err := gparams.GParams.Load("etcd.port")
	if err != nil {
		panic(err)
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:" + etcdPort}})
	assert.Nil(t, err)
	rootpath := "/etcd/test/root"
	kv := NewEtcdKV(cli, rootpath)
	ctx, cancel := context.WithTimeout(context.TODO(), requestTimeout)
	defer cancel()
	defer kv.Close()
	defer kv.client.Delete(ctx, rootpath, clientv3.WithPrefix())

	err = kv.Save("key_1", "123")
	assert.Nil(t, err)
	err = kv.Save("key_2", "456")
	assert.Nil(t, err)
	err = kv.Save("key_3", "789")
	assert.Nil(t, err)

	kvs := map[string]string{
		"key_1": "111",
		"key_2": "444",
	}

	keys := []string{"key_3"}

	err = kv.MultiSaveAndRemove(kvs, keys)
	assert.Nil(t, err)
	val, err := kv.Load("key_1")
	assert.Nil(t, err)
	assert.Equal(t, val, "111")
	val, err = kv.Load("key_2")
	assert.Nil(t, err)
	assert.Equal(t, val, "444")
	val, err = kv.Load("key_3")
	assert.Error(t, err)
	assert.Empty(t, val)
}
