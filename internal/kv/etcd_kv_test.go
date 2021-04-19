package kv

import (
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/clientv3"
)

func TestEtcdKV_Load(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:2379"}})
	assert.Nil(t, err)
	kv := NewEtcdKV(cli, "/etcd/test/root")
	defer kv.Close()

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
}
