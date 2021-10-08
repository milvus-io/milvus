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

package rocksdbkv_test

import (
	"strconv"
	"sync"
	"testing"

	rocksdbkv "github.com/milvus-io/milvus/internal/kv/rocksdb"
	"github.com/stretchr/testify/assert"
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

func TestRocksdbKV_Goroutines(t *testing.T) {
	name := "/tmp/rocksdb"
	rocksdbkv, err := rocksdbkv.NewRocksdbKV(name)
	assert.Nil(t, err)
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
			assert.Nil(t, err)

			getVal, err := rocksdbkv.Load(key)
			assert.Nil(t, err)
			assert.Equal(t, getVal, val)
		}(i)
	}
	wg.Wait()
}

func TestRocksdbKV_Dummy(t *testing.T) {
	name := "/tmp/rocksdb_dummy"
	rocksdbkv, err := rocksdbkv.NewRocksdbKV(name)
	assert.Nil(t, err)
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
