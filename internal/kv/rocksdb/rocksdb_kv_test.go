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

package rocksdbkv_test

import (
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	rocksdbkv "github.com/milvus-io/milvus/internal/kv/rocksdb"
)

func TestRocksdbKV(t *testing.T) {
	name := "/tmp/rocksdb"
	rocksdbKV, err := rocksdbkv.NewRocksdbKV(name)
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(name)
	defer rocksdbKV.Close()
	// Need to call RemoveWithPrefix
	defer rocksdbKV.RemoveWithPrefix("")

	err = rocksdbKV.Save("abc", "123")
	assert.NoError(t, err)

	err = rocksdbKV.SaveBytes("abcd", []byte("1234"))
	assert.NoError(t, err)

	val, err := rocksdbKV.Load("abc")
	assert.NoError(t, err)
	assert.Equal(t, val, "123")
	value, err := rocksdbKV.LoadBytes("abc")
	assert.NoError(t, err)
	assert.Equal(t, value, []byte("123"))

	_, err = rocksdbKV.Load("")
	assert.Error(t, err)
	_, err = rocksdbKV.LoadBytes("")
	assert.Error(t, err)

	keys, vals, err := rocksdbKV.LoadWithPrefix("abc")
	assert.NoError(t, err)
	assert.Equal(t, len(keys), len(vals))
	assert.Equal(t, len(keys), 2)

	assert.Equal(t, keys[0], "abc")
	assert.Equal(t, keys[1], "abcd")
	assert.Equal(t, vals[0], "123")
	assert.Equal(t, vals[1], "1234")

	var values [][]byte
	keys, values, err = rocksdbKV.LoadBytesWithPrefix("abc")
	assert.NoError(t, err)
	assert.Equal(t, len(keys), len(vals))
	assert.Equal(t, len(keys), 2)

	assert.Equal(t, keys[0], "abc")
	assert.Equal(t, keys[1], "abcd")
	assert.Equal(t, values[0], []byte("123"))
	assert.Equal(t, values[1], []byte("1234"))

	err = rocksdbKV.Save("key_1", "123")
	assert.NoError(t, err)
	err = rocksdbKV.Save("key_2", "456")
	assert.NoError(t, err)
	err = rocksdbKV.Save("key_3", "789")
	assert.NoError(t, err)

	keys = []string{"key_1", "key_2"}
	vals, err = rocksdbKV.MultiLoad(keys)
	assert.NoError(t, err)
	assert.Equal(t, len(vals), len(keys))
	assert.Equal(t, vals[0], "123")
	assert.Equal(t, vals[1], "456")

	values, err = rocksdbKV.MultiLoadBytes(keys)
	assert.NoError(t, err)
	assert.Equal(t, len(values), len(keys))
	assert.Equal(t, values[0], []byte("123"))
	assert.Equal(t, values[1], []byte("456"))

	err = rocksdbKV.MultiRemove(keys)
	assert.NoError(t, err)

	saves := map[string]string{
		"s_1": "111",
		"s_2": "222",
		"s_3": "333",
	}
	removals := []string{"key_3"}
	err = rocksdbKV.MultiSaveAndRemove(saves, removals)
	assert.NoError(t, err)

	err = rocksdbKV.DeleteRange("s_1", "s_3")
	assert.NoError(t, err)
}

func TestRocksdbKV_Prefix(t *testing.T) {
	name := "/tmp/rocksdb"
	rocksdbKV, err := rocksdbkv.NewRocksdbKV(name)
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(name)
	defer rocksdbKV.Close()
	// Need to call RemoveWithPrefix
	defer rocksdbKV.RemoveWithPrefix("")

	kvs := map[string]string{
		"abcd":    "123",
		"abdd":    "1234",
		"abddqqq": "1234555",
	}
	err = rocksdbKV.MultiSave(kvs)
	assert.NoError(t, err)

	keys, vals, err := rocksdbKV.LoadWithPrefix("abc")
	assert.NoError(t, err)

	assert.Equal(t, len(keys), 1)
	assert.Equal(t, len(vals), 1)
	assert.Equal(t, keys[0], "abcd")
	assert.Equal(t, vals[0], "123")

	bytesKvs := map[string][]byte{}
	for k, v := range kvs {
		rocksdbKV.Remove(k)
		bytesKvs[k] = []byte(v)
	}

	err = rocksdbKV.MultiSaveBytes(bytesKvs)
	assert.NoError(t, err)

	var values [][]byte
	keys, values, err = rocksdbKV.LoadBytesWithPrefix("abc")
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 1)
	assert.Equal(t, len(values), 1)
	assert.Equal(t, keys[0], "abcd")
	assert.Equal(t, values[0], []byte("123"))

	err = rocksdbKV.RemoveWithPrefix("abc")
	assert.NoError(t, err)
	val, err := rocksdbKV.Load("abc")
	assert.NoError(t, err)
	assert.Equal(t, len(val), 0)
	val, err = rocksdbKV.Load("abdd")
	assert.NoError(t, err)
	assert.Equal(t, val, "1234")
	val, err = rocksdbKV.Load("abddqqq")
	assert.NoError(t, err)
	assert.Equal(t, val, "1234555")

	// test remove ""
	err = rocksdbKV.RemoveWithPrefix("")
	assert.NoError(t, err)

	// test remove from an empty cf
	err = rocksdbKV.RemoveWithPrefix("")
	assert.NoError(t, err)

	val, err = rocksdbKV.Load("abddqqq")
	assert.NoError(t, err)
	assert.Equal(t, len(val), 0)

	// test we can still save after drop
	err = rocksdbKV.Save("abcd", "123")
	assert.NoError(t, err)

	val, err = rocksdbKV.Load("abcd")
	assert.NoError(t, err)
	assert.Equal(t, val, "123")

}

func TestRocksdbKV_Txn(t *testing.T) {
	name := "/tmp/rocksdb"
	rocksdbKV, err := rocksdbkv.NewRocksdbKV(name)
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(name)
	defer rocksdbKV.Close()
	// Need to call RemoveWithPrefix
	defer rocksdbKV.RemoveWithPrefix("")

	kvs := map[string]string{
		"abcd":    "123",
		"abdd":    "1234",
		"abddqqq": "1234555",
	}
	err = rocksdbKV.MultiSave(kvs)
	assert.NoError(t, err)

	keys, vals, err := rocksdbKV.LoadWithPrefix("")
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 3)
	assert.Equal(t, len(vals), 3)

	removePrefix := []string{"abc", "abd"}
	rocksdbKV.MultiRemoveWithPrefix(removePrefix)

	keys, vals, err = rocksdbKV.LoadWithPrefix("")
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(vals), 0)

	err = rocksdbKV.MultiSave(kvs)
	assert.NoError(t, err)
	keys, vals, err = rocksdbKV.LoadWithPrefix("")
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 3)
	assert.Equal(t, len(vals), 3)

	// test delete the whole table
	removePrefix = []string{"", "hello"}
	rocksdbKV.MultiRemoveWithPrefix(removePrefix)

	keys, vals, err = rocksdbKV.LoadWithPrefix("")
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(vals), 0)

	err = rocksdbKV.MultiSave(kvs)
	assert.NoError(t, err)
	keys, vals, err = rocksdbKV.LoadWithPrefix("")
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 3)
	assert.Equal(t, len(vals), 3)

	// test remove and save
	removePrefix = []string{"abc", "abd"}
	kvs2 := map[string]string{
		"abfad": "12345",
	}
	rocksdbKV.MultiSaveAndRemoveWithPrefix(kvs2, removePrefix)
	keys, vals, err = rocksdbKV.LoadWithPrefix("")
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 1)
	assert.Equal(t, len(vals), 1)
}

func TestRocksdbKV_Goroutines(t *testing.T) {
	name := "/tmp/rocksdb"
	rocksdbkv, err := rocksdbkv.NewRocksdbKV(name)
	assert.NoError(t, err)
	defer os.RemoveAll(name)
	defer rocksdbkv.Close()
	defer rocksdbkv.RemoveWithPrefix("")

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "key_" + strconv.Itoa(i)
			val := "val_" + strconv.Itoa(i)
			err := rocksdbkv.Save(key, val)
			assert.NoError(t, err)

			getVal, err := rocksdbkv.Load(key)
			assert.NoError(t, err)
			assert.Equal(t, getVal, val)
		}(i)
	}
	wg.Wait()
}

func TestRocksdbKV_DummyDB(t *testing.T) {
	name := "/tmp/rocksdb_dummy"
	rocksdbkv, err := rocksdbkv.NewRocksdbKV(name)
	assert.NoError(t, err)
	defer os.RemoveAll(name)
	defer rocksdbkv.Close()
	defer rocksdbkv.RemoveWithPrefix("")

	rocksdbkv.DB = nil
	_, err = rocksdbkv.Load("")
	assert.Error(t, err)
	_, _, err = rocksdbkv.LoadWithPrefix("")
	assert.Error(t, err)
	_, err = rocksdbkv.MultiLoad(nil)
	assert.Error(t, err)
	err = rocksdbkv.Save("", "")
	assert.Error(t, err)
	err = rocksdbkv.MultiSave(nil)
	assert.Error(t, err)
	err = rocksdbkv.RemoveWithPrefix("")
	assert.Error(t, err)
	err = rocksdbkv.Remove("")
	assert.Error(t, err)
	err = rocksdbkv.MultiRemove(nil)
	assert.Error(t, err)
	err = rocksdbkv.MultiSaveAndRemove(nil, nil)
	assert.Error(t, err)
	err = rocksdbkv.DeleteRange("", "")
	assert.Error(t, err)

	rocksdbkv.ReadOptions = nil
	_, err = rocksdbkv.Load("dummy")
	assert.Error(t, err)
}

func TestRocksdbKV_CornerCase(t *testing.T) {
	name := "/tmp/rocksdb_corner"
	rocksdbkv, err := rocksdbkv.NewRocksdbKV(name)
	assert.NoError(t, err)
	defer os.RemoveAll(name)
	defer rocksdbkv.Close()
	defer rocksdbkv.RemoveWithPrefix("")
	_, err = rocksdbkv.Load("")
	assert.Error(t, err)
	keys, values, err := rocksdbkv.LoadWithPrefix("")
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)
	err = rocksdbkv.Save("", "")
	assert.Error(t, err)
	err = rocksdbkv.Save("test", "")
	assert.Error(t, err)
	err = rocksdbkv.Remove("")
	assert.Error(t, err)
	err = rocksdbkv.DeleteRange("a", "a")
	assert.Error(t, err)
}

func TestHas(t *testing.T) {
	dir := t.TempDir()
	db, err := rocksdbkv.NewRocksdbKV(dir)
	assert.NoError(t, err)
	defer db.Close()
	defer db.RemoveWithPrefix("")

	has, err := db.Has("key1")
	assert.NoError(t, err)
	assert.False(t, has)
	err = db.Save("key1", "value1")
	assert.NoError(t, err)
	has, err = db.Has("key1")
	assert.NoError(t, err)
	assert.True(t, has)
	err = db.Remove("key1")
	assert.NoError(t, err)
	has, err = db.Has("key1")
	assert.NoError(t, err)
	assert.False(t, has)
}

func TestHasPrefix(t *testing.T) {
	dir := t.TempDir()
	db, err := rocksdbkv.NewRocksdbKV(dir)
	assert.NoError(t, err)
	defer db.Close()
	defer db.RemoveWithPrefix("")

	has, err := db.HasPrefix("key")
	assert.NoError(t, err)
	assert.False(t, has)

	err = db.Save("key1", "value1")
	assert.NoError(t, err)

	has, err = db.HasPrefix("key")
	assert.NoError(t, err)
	assert.True(t, has)

	err = db.Remove("key1")
	assert.NoError(t, err)

	has, err = db.HasPrefix("key")
	assert.NoError(t, err)
	assert.False(t, has)
}
