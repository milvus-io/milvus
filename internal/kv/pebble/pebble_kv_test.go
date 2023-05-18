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

package pebblekv_test

import (
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	pebbleKV "github.com/milvus-io/milvus/internal/kv/pebble"
)

func TestPebbleKV(t *testing.T) {
	name := "/tmp/pebble"
	pebbleKV, err := pebbleKV.NewPebbleKV(name)
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(name)
	defer pebbleKV.Close()
	// Need to call RemoveWithPrefix
	defer pebbleKV.RemoveWithPrefix("")

	err = pebbleKV.Save("abc", "123")
	assert.Nil(t, err)
	err = pebbleKV.Save("abcd", "1234")
	assert.Nil(t, err)

	val, err := pebbleKV.Load("abc")
	assert.Nil(t, err)
	assert.Equal(t, val, "123")

	_, err = pebbleKV.Load("")
	assert.Error(t, err)

	keys, vals, err := pebbleKV.LoadWithPrefix("abc")
	assert.Nil(t, err)
	assert.Equal(t, len(keys), len(vals))
	assert.Equal(t, len(keys), 2)

	assert.Equal(t, keys[0], "abc")
	assert.Equal(t, keys[1], "abcd")
	assert.Equal(t, vals[0], "123")
	assert.Equal(t, vals[1], "1234")

	err = pebbleKV.Save("key_1", "123")
	assert.Nil(t, err)
	err = pebbleKV.Save("key_2", "456")
	assert.Nil(t, err)
	err = pebbleKV.Save("key_3", "789")
	assert.Nil(t, err)

	keys = []string{"key_1", "key_2"}
	vals, err = pebbleKV.MultiLoad(keys)
	assert.Nil(t, err)
	assert.Equal(t, len(vals), len(keys))
	assert.Equal(t, vals[0], "123")
	assert.Equal(t, vals[1], "456")

	err = pebbleKV.MultiRemove(keys)
	assert.NoError(t, err)

	saves := map[string]string{
		"s_1": "111",
		"s_2": "222",
		"s_3": "333",
	}
	removals := []string{"key_3"}
	err = pebbleKV.MultiSaveAndRemove(saves, removals)
	assert.NoError(t, err)

	err = pebbleKV.DeleteRange("s_1", "s_3")
	assert.NoError(t, err)
}

func TestPebbleKV_Prefix(t *testing.T) {
	name := "/tmp/pebble"
	pebbleKV, err := pebbleKV.NewPebbleKV(name)
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(name)
	defer pebbleKV.Close()
	// Need to call RemoveWithPrefix
	defer pebbleKV.RemoveWithPrefix("")

	kvs := map[string]string{
		"abcd":    "123",
		"abdd":    "1234",
		"abddqqq": "1234555",
	}
	err = pebbleKV.MultiSave(kvs)
	assert.Nil(t, err)

	keys, vals, err := pebbleKV.LoadWithPrefix("abc")
	assert.Nil(t, err)

	assert.Equal(t, len(keys), 1)
	assert.Equal(t, len(vals), 1)
	assert.Equal(t, keys[0], "abcd")
	assert.Equal(t, vals[0], "123")

	err = pebbleKV.RemoveWithPrefix("abc")
	assert.Nil(t, err)
	val, err := pebbleKV.Load("abc")
	assert.Nil(t, err)
	assert.Equal(t, len(val), 0)
	val, err = pebbleKV.Load("abdd")
	assert.Nil(t, err)
	assert.Equal(t, val, "1234")
	val, err = pebbleKV.Load("abddqqq")
	assert.Nil(t, err)
	assert.Equal(t, val, "1234555")

	// test remove ""
	err = pebbleKV.RemoveWithPrefix("")
	assert.Nil(t, err)

	// test remove from an empty cf
	err = pebbleKV.RemoveWithPrefix("")
	assert.Nil(t, err)

	val, err = pebbleKV.Load("abddqqq")
	assert.Nil(t, err)
	assert.Equal(t, len(val), 0)

	// test we can still save after drop
	err = pebbleKV.Save("abcd", "123")
	assert.Nil(t, err)

	val, err = pebbleKV.Load("abcd")
	assert.Nil(t, err)
	assert.Equal(t, val, "123")

}

func TestPebbleKV_Txn(t *testing.T) {
	name := "/tmp/pebble"
	pebbleKV, err := pebbleKV.NewPebbleKV(name)
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(name)
	defer pebbleKV.Close()
	// Need to call RemoveWithPrefix
	defer pebbleKV.RemoveWithPrefix("")

	kvs := map[string]string{
		"abcd":    "123",
		"abdd":    "1234",
		"abddqqq": "1234555",
	}
	err = pebbleKV.MultiSave(kvs)
	assert.NoError(t, err)

	keys, vals, err := pebbleKV.LoadWithPrefix("")
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 3)
	assert.Equal(t, len(vals), 3)

	removePrefix := []string{"abc", "abd"}
	pebbleKV.MultiRemoveWithPrefix(removePrefix)

	keys, vals, err = pebbleKV.LoadWithPrefix("")
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(vals), 0)

	err = pebbleKV.MultiSave(kvs)
	assert.NoError(t, err)
	keys, vals, err = pebbleKV.LoadWithPrefix("")
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 3)
	assert.Equal(t, len(vals), 3)

	// test delete the whole table
	removePrefix = []string{"", "hello"}
	pebbleKV.MultiRemoveWithPrefix(removePrefix)

	keys, vals, err = pebbleKV.LoadWithPrefix("")
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(vals), 0)

	err = pebbleKV.MultiSave(kvs)
	assert.NoError(t, err)
	keys, vals, err = pebbleKV.LoadWithPrefix("")
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 3)
	assert.Equal(t, len(vals), 3)

	// test remove and save
	removePrefix = []string{"abc", "abd"}
	kvs2 := map[string]string{
		"abfad": "12345",
	}
	pebbleKV.MultiSaveAndRemoveWithPrefix(kvs2, removePrefix)
	keys, vals, err = pebbleKV.LoadWithPrefix("")
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 1)
	assert.Equal(t, len(vals), 1)
}

func TestPebbleKV_Goroutines(t *testing.T) {
	name := "/tmp/pebble"
	pebbleKV, err := pebbleKV.NewPebbleKV(name)
	assert.Nil(t, err)
	defer os.RemoveAll(name)
	defer pebbleKV.Close()
	defer pebbleKV.RemoveWithPrefix("")

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "key_" + strconv.Itoa(i)
			val := "val_" + strconv.Itoa(i)
			err := pebbleKV.Save(key, val)
			assert.Nil(t, err)

			getVal, err := pebbleKV.Load(key)
			assert.Nil(t, err)
			assert.Equal(t, getVal, val)
		}(i)
	}
	wg.Wait()
}

func TestPebble_DummyDB(t *testing.T) {
	name := "/tmp/pebble_dummy"
	pebbleKV, err := pebbleKV.NewPebbleKV(name)
	assert.Nil(t, err)
	defer os.RemoveAll(name)
	defer pebbleKV.Close()
	defer pebbleKV.RemoveWithPrefix("")

	pebbleKV.DB = nil
	_, err = pebbleKV.Load("")
	assert.Error(t, err)
	_, _, err = pebbleKV.LoadWithPrefix("")
	assert.Error(t, err)
	_, err = pebbleKV.MultiLoad(nil)
	assert.Error(t, err)
	err = pebbleKV.Save("", "")
	assert.Error(t, err)
	err = pebbleKV.MultiSave(nil)
	assert.Error(t, err)
	err = pebbleKV.RemoveWithPrefix("")
	assert.Error(t, err)
	err = pebbleKV.Remove("")
	assert.Error(t, err)
	err = pebbleKV.MultiRemove(nil)
	assert.Error(t, err)
	err = pebbleKV.MultiSaveAndRemove(nil, nil)
	assert.Error(t, err)
	err = pebbleKV.DeleteRange("", "")
	assert.Error(t, err)

	pebbleKV.ReadOptions = nil
	_, err = pebbleKV.Load("dummy")
	assert.Error(t, err)
}

func TestPebbleKV_CornerCase(t *testing.T) {
	name := "/tmp/pebble_corner"
	pebbleKV, err := pebbleKV.NewPebbleKV(name)
	assert.Nil(t, err)
	defer os.RemoveAll(name)
	defer pebbleKV.Close()
	defer pebbleKV.RemoveWithPrefix("")
	_, err = pebbleKV.Load("")
	assert.Error(t, err)
	keys, values, err := pebbleKV.LoadWithPrefix("")
	assert.NoError(t, err)
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)
	err = pebbleKV.Save("", "")
	assert.Error(t, err)
	err = pebbleKV.Save("test", "")
	assert.Error(t, err)
	err = pebbleKV.Remove("")
	assert.Error(t, err)
	err = pebbleKV.DeleteRange("a", "a")
	assert.Error(t, err)
}

func TestHas(t *testing.T) {
	dir := t.TempDir()
	db, err := pebbleKV.NewPebbleKV(dir)
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
	db, err := pebbleKV.NewPebbleKV(dir)
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
