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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/kv/predicates"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func TestMemoryKV_SaveAndLoadBytes(t *testing.T) {
	mem := NewMemoryKV()

	key := "key"
	value := []byte("value")
	err := mem.SaveBytes(context.TODO(), key, value)
	assert.NoError(t, err)

	_value, err := mem.LoadBytes(context.TODO(), key)
	assert.NoError(t, err)
	assert.Equal(t, value, _value)

	noKey := "no_key"
	_value, err = mem.LoadBytes(context.TODO(), noKey)
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
		err := mem.SaveBytes(context.TODO(), kv.key, kv.value)
		assert.NoError(t, err)
	}

	keys, values, err := mem.LoadBytesRange(context.TODO(), "test1", "test1/b", 0)
	assert.Equal(t, len(keys), 2)
	assert.Equal(t, len(values), 2)
	assert.NoError(t, err)

	keys, values, err = mem.LoadBytesRange(context.TODO(), "test1", "test1/a", 2)
	assert.Equal(t, len(keys), 1)
	assert.Equal(t, len(values), 1)
	assert.NoError(t, err)
}

func TestMemoryKV_LoadBytesWithDefault(t *testing.T) {
	mem := NewMemoryKV()

	key := "key"
	value := []byte("value")
	err := mem.SaveBytes(context.TODO(), key, value)
	assert.NoError(t, err)

	_default := []byte("default")
	_value := mem.LoadBytesWithDefault(context.TODO(), key, _default)
	assert.Equal(t, value, _value)

	noKey := "no_key"
	_value = mem.LoadBytesWithDefault(context.TODO(), noKey, _default)
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
		err := mem.SaveBytes(context.TODO(), kv.key, kv.value)
		assert.NoError(t, err)
	}

	keys, values, err := mem.LoadBytesWithPrefix(context.TODO(), "test1")
	assert.Equal(t, len(keys), 3)
	assert.Equal(t, len(values), 3)
	assert.NoError(t, err)

	keys, values, err = mem.LoadBytesWithPrefix(context.TODO(), "test")
	assert.Equal(t, len(keys), 4)
	assert.Equal(t, len(values), 4)
	assert.NoError(t, err)

	keys, values, err = mem.LoadBytesWithPrefix(context.TODO(), "a")
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
	err := mem.MultiSaveBytes(context.TODO(), saveAndLoadBytesTests)
	assert.NoError(t, err)

	_values, err := mem.MultiLoadBytes(context.TODO(), keys)
	assert.Equal(t, values, _values)
	assert.NoError(t, err)

	_values, err = mem.MultiLoadBytes(context.TODO(), []string{})
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
	err := mem.MultiSaveBytesAndRemove(context.TODO(), saveAndLoadBytesTests, []string{keys[0]})
	assert.NoError(t, err)

	_value, err := mem.LoadBytes(context.TODO(), keys[0])
	assert.Empty(t, _value)
	assert.Error(t, err)

	_values, err := mem.MultiLoadBytes(context.TODO(), keys[1:])
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
	err := mem.MultiSaveBytesAndRemoveWithPrefix(context.TODO(), saveAndLoadBytesTests, []string{"test1"})
	assert.NoError(t, err)

	_keys, _values, err := mem.LoadBytesWithPrefix(context.TODO(), "test")
	assert.ElementsMatch(t, keys, _keys)
	assert.ElementsMatch(t, values, _values)
	assert.NoError(t, err)
}

func TestHas(t *testing.T) {
	kv := NewMemoryKV()

	has, err := kv.Has(context.TODO(), "key1")
	assert.NoError(t, err)
	assert.False(t, has)

	err = kv.Save(context.TODO(), "key1", "value1")
	assert.NoError(t, err)

	has, err = kv.Has(context.TODO(), "key1")
	assert.NoError(t, err)
	assert.True(t, has)

	err = kv.Remove(context.TODO(), "key1")
	assert.NoError(t, err)

	has, err = kv.Has(context.TODO(), "key1")
	assert.NoError(t, err)
	assert.False(t, has)
}

func TestHasPrefix(t *testing.T) {
	kv := NewMemoryKV()

	has, err := kv.HasPrefix(context.TODO(), "key")
	assert.NoError(t, err)
	assert.False(t, has)

	err = kv.Save(context.TODO(), "key1", "value1")
	assert.NoError(t, err)

	has, err = kv.HasPrefix(context.TODO(), "key")
	assert.NoError(t, err)
	assert.True(t, has)

	err = kv.Remove(context.TODO(), "key1")
	assert.NoError(t, err)

	has, err = kv.HasPrefix(context.TODO(), "key")
	assert.NoError(t, err)
	assert.False(t, has)
}

func TestPredicates(t *testing.T) {
	kv := NewMemoryKV()

	// predicates not supported for mem kv for now
	err := kv.MultiSaveAndRemove(context.TODO(), map[string]string{}, []string{}, predicates.ValueEqual("a", "b"))
	assert.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)

	err = kv.MultiSaveAndRemoveWithPrefix(context.TODO(), map[string]string{}, []string{}, predicates.ValueEqual("a", "b"))
	assert.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
}
