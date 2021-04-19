package rocksdbkv_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	rocksdbkv "github.com/zilliztech/milvus-distributed/internal/kv/rocksdb"
)

func TestRocksdbKV(t *testing.T) {
	name := "/tmp/rocksdb"
	rocksdbKV, err := rocksdbkv.NewRocksdbKV(name)
	if err != nil {
		panic(err)
	}

	defer rocksdbKV.Close()
	// Need to call RemoveWithPrefix
	defer rocksdbKV.RemoveWithPrefix("")

	err = rocksdbKV.Save("abc", "123")
	assert.Nil(t, err)

	err = rocksdbKV.Save("abcd", "1234")
	assert.Nil(t, err)

	val, err := rocksdbKV.Load("abc")
	assert.Nil(t, err)
	assert.Equal(t, val, "123")

	keys, vals, err := rocksdbKV.LoadWithPrefix("abc")
	assert.Nil(t, err)
	assert.Equal(t, len(keys), len(vals))
	assert.Equal(t, len(keys), 2)

	assert.Equal(t, keys[0], "abc")
	assert.Equal(t, keys[1], "abcd")
	assert.Equal(t, vals[0], "123")
	assert.Equal(t, vals[1], "1234")

	err = rocksdbKV.Save("key_1", "123")
	assert.Nil(t, err)
	err = rocksdbKV.Save("key_2", "456")
	assert.Nil(t, err)
	err = rocksdbKV.Save("key_3", "789")
	assert.Nil(t, err)

	keys = []string{"key_1", "key_2"}
	vals, err = rocksdbKV.MultiLoad(keys)
	assert.Nil(t, err)
	assert.Equal(t, len(vals), len(keys))
	assert.Equal(t, vals[0], "123")
	assert.Equal(t, vals[1], "456")
}

func TestRocksdbKV_Prefix(t *testing.T) {
	name := "/tmp/rocksdb"
	rocksdbKV, err := rocksdbkv.NewRocksdbKV(name)
	if err != nil {
		panic(err)
	}

	defer rocksdbKV.Close()
	// Need to call RemoveWithPrefix
	defer rocksdbKV.RemoveWithPrefix("")

	err = rocksdbKV.Save("abcd", "123")
	assert.Nil(t, err)

	err = rocksdbKV.Save("abdd", "1234")
	assert.Nil(t, err)

	err = rocksdbKV.Save("abddqqq", "1234555")
	assert.Nil(t, err)

	keys, vals, err := rocksdbKV.LoadWithPrefix("abc")
	assert.Nil(t, err)
	assert.Equal(t, len(keys), 1)
	assert.Equal(t, len(vals), 1)
	//fmt.Println(keys)
	//fmt.Println(vals)

	err = rocksdbKV.RemoveWithPrefix("abc")
	assert.Nil(t, err)
	val, err := rocksdbKV.Load("abc")
	assert.Nil(t, err)
	assert.Equal(t, len(val), 0)
	val, err = rocksdbKV.Load("abdd")
	assert.Nil(t, err)
	assert.Equal(t, val, "1234")
	val, err = rocksdbKV.Load("abddqqq")
	assert.Nil(t, err)
	assert.Equal(t, val, "1234555")
}
