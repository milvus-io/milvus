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

package memkv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemoryKV_SaveAndLoadBytes(t *testing.T) {
	mem := NewMemoryKV()

	key := "key"
	value := []byte("value")
	err := mem.SaveBytes(key, value)
	assert.NoError(t, err)

	_value, err := mem.LoadBytes(key)
	assert.NoError(t, err)
	assert.Equal(t, value, _value)

	noKey := "no_key"
	_value, err = mem.LoadBytes(noKey)
	assert.Error(t, err)
	assert.Empty(t, _value)
}

func TestMemoryKV_LoadBytesRange(t *testing.T) {
	saveAndLoadBytesTests := []struct {
		key   string
		value []byte
	}{
		{"test1", []byte("value1")},
		{"test2", []byte("value2")},
		{"test1/a", []byte("value_a")},
		{"test1/b", []byte("value_b")},
	}

	mem := NewMemoryKV()
	for _, kv := range saveAndLoadBytesTests {
		err := mem.SaveBytes(kv.key, kv.value)
		assert.NoError(t, err)
	}

	keys, values, err := mem.LoadBytesRange("test1", "test1/b", 0)
	assert.Equal(t, len(keys), 2)
	assert.Equal(t, len(values), 2)
	assert.NoError(t, err)

	keys, values, err = mem.LoadBytesRange("test1", "test1/a", 2)
	assert.Equal(t, len(keys), 1)
	assert.Equal(t, len(values), 1)
	assert.NoError(t, err)
}

func TestMemoryKV_LoadBytesWithDefault(t *testing.T) {
	mem := NewMemoryKV()

	key := "key"
	value := []byte("value")
	err := mem.SaveBytes(key, value)
	assert.NoError(t, err)

	_default := []byte("default")
	_value := mem.LoadBytesWithDefault(key, _default)
	assert.Equal(t, value, _value)

	noKey := "no_key"
	_value = mem.LoadBytesWithDefault(noKey, _default)
	assert.Equal(t, _value, _default)
}

func TestMemoryKV_LoadBytesWithPrefix(t *testing.T) {
	saveAndLoadBytesTests := []struct {
		key   string
		value []byte
	}{
		{"test1", []byte("value1")},
		{"test2", []byte("value2")},
		{"test1/a", []byte("value_a")},
		{"test1/b", []byte("value_b")},
	}

	mem := NewMemoryKV()
	for _, kv := range saveAndLoadBytesTests {
		err := mem.SaveBytes(kv.key, kv.value)
		assert.NoError(t, err)
	}

	keys, values, err := mem.LoadBytesWithPrefix("test1")
	assert.Equal(t, len(keys), 3)
	assert.Equal(t, len(values), 3)
	assert.NoError(t, err)

	keys, values, err = mem.LoadBytesWithPrefix("test")
	assert.Equal(t, len(keys), 4)
	assert.Equal(t, len(values), 4)
	assert.NoError(t, err)

	keys, values, err = mem.LoadBytesWithPrefix("a")
	assert.Equal(t, len(keys), 0)
	assert.Equal(t, len(values), 0)
	assert.NoError(t, err)
}

func TestMemoryKV_MultiSaveBytes(t *testing.T) {
	saveAndLoadBytesTests := map[string][]byte{
		"test1":   []byte("value1"),
		"test2":   []byte("value2"),
		"test1/a": []byte("value_a"),
		"test1/b": []byte("value_b"),
	}

	var keys []string
	var values [][]byte
	for k, v := range saveAndLoadBytesTests {
		keys = append(keys, k)
		values = append(values, v)
	}

	mem := NewMemoryKV()
	err := mem.MultiSaveBytes(saveAndLoadBytesTests)
	assert.NoError(t, err)

	_values, err := mem.MultiLoadBytes(keys)
	assert.Equal(t, values, _values)
	assert.NoError(t, err)

	_values, err = mem.MultiLoadBytes([]string{})
	assert.Empty(t, _values)
	assert.NoError(t, err)
}

func TestMemoryKV_MultiSaveBytesAndRemove(t *testing.T) {
	saveAndLoadBytesTests := map[string][]byte{
		"test1":   []byte("value1"),
		"test2":   []byte("value2"),
		"test1/a": []byte("value_a"),
		"test1/b": []byte("value_b"),
	}

	var keys []string
	var values [][]byte
	for k, v := range saveAndLoadBytesTests {
		keys = append(keys, k)
		values = append(values, v)
	}

	mem := NewMemoryKV()
	err := mem.MultiSaveBytesAndRemove(saveAndLoadBytesTests, []string{keys[0]})
	assert.NoError(t, err)

	_value, err := mem.LoadBytes(keys[0])
	assert.Empty(t, _value)
	assert.Error(t, err)

	_values, err := mem.MultiLoadBytes(keys[1:])
	assert.Equal(t, values[1:], _values)
	assert.NoError(t, err)
}

func TestMemoryKV_MultiSaveBytesAndRemoveWithPrefix(t *testing.T) {
	saveAndLoadBytesTests := map[string][]byte{
		"test1":   []byte("value1"),
		"test2":   []byte("value2"),
		"test1/a": []byte("value_a"),
		"test1/b": []byte("value_b"),
	}

	var keys []string
	var values [][]byte
	for k, v := range saveAndLoadBytesTests {
		keys = append(keys, k)
		values = append(values, v)
	}

	mem := NewMemoryKV()
	err := mem.MultiSaveBytesAndRemoveWithPrefix(saveAndLoadBytesTests, []string{"test1"})
	assert.NoError(t, err)

	_keys, _values, err := mem.LoadBytesWithPrefix("test")
	assert.ElementsMatch(t, keys, _keys)
	assert.ElementsMatch(t, values, _values)
	assert.NoError(t, err)
}

func TestHas(t *testing.T) {
	kv := NewMemoryKV()

	has, err := kv.Has("key1")
	assert.NoError(t, err)
	assert.False(t, has)

	err = kv.Save("key1", "value1")
	assert.NoError(t, err)

	has, err = kv.Has("key1")
	assert.NoError(t, err)
	assert.True(t, has)

	err = kv.Remove("key1")
	assert.NoError(t, err)

	has, err = kv.Has("key1")
	assert.NoError(t, err)
	assert.False(t, has)
}

func TestHasPrefix(t *testing.T) {
	kv := NewMemoryKV()

	has, err := kv.HasPrefix("key")
	assert.NoError(t, err)
	assert.False(t, has)

	err = kv.Save("key1", "value1")
	assert.NoError(t, err)

	has, err = kv.HasPrefix("key")
	assert.NoError(t, err)
	assert.True(t, has)

	err = kv.Remove("key1")
	assert.NoError(t, err)

	has, err = kv.HasPrefix("key")
	assert.NoError(t, err)
	assert.False(t, has)
}
