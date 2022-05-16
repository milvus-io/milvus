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

package storage

import (
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalCM(t *testing.T) {
	t.Run("test load", func(t *testing.T) {
		testLoadRoot := "test_load"

		testCM := NewLocalChunkManager(RootPath(localPath))
		defer testCM.RemoveWithPrefix(testLoadRoot)

		prepareTests := []struct {
			key   string
			value []byte
		}{
			{"abc", []byte("123")},
			{"abcd", []byte("1234")},
			{"key_1", []byte("111")},
			{"key_2", []byte("222")},
			{"key_3", []byte("333")},
		}

		for _, test := range prepareTests {
			err := testCM.Write(path.Join(testLoadRoot, test.key), test.value)
			require.NoError(t, err)
		}

		loadTests := []struct {
			isvalid       bool
			loadKey       string
			expectedValue []byte

			description string
		}{
			{true, "abc", []byte("123"), "load valid key abc"},
			{true, "abcd", []byte("1234"), "load valid key abcd"},
			{true, "key_1", []byte("111"), "load valid key key_1"},
			{true, "key_2", []byte("222"), "load valid key key_2"},
			{true, "key_3", []byte("333"), "load valid key key_3"},
			{false, "key_not_exist", []byte(""), "load invalid key key_not_exist"},
			{false, "/", []byte(""), "load leading slash"},
		}

		for _, test := range loadTests {
			t.Run(test.description, func(t *testing.T) {
				if test.isvalid {
					got, err := testCM.Read(path.Join(testLoadRoot, test.loadKey))
					assert.NoError(t, err)
					assert.Equal(t, test.expectedValue, got)
				} else {
					if test.loadKey == "/" {
						got, err := testCM.Read(test.loadKey)
						assert.Error(t, err)
						assert.Empty(t, got)
						return
					}
					got, err := testCM.Read(path.Join(testLoadRoot, test.loadKey))
					assert.Error(t, err)
					assert.Empty(t, got)
				}
			})
		}

		loadWithPrefixTests := []struct {
			isvalid       bool
			prefix        string
			expectedValue [][]byte

			description string
		}{
			{true, "abc", [][]byte{[]byte("123"), []byte("1234")}, "load with valid prefix abc"},
			{true, "key_", [][]byte{[]byte("111"), []byte("222"), []byte("333")}, "load with valid prefix key_"},
			{true, "prefix", [][]byte{}, "load with valid but not exist prefix prefix"},
		}

		for _, test := range loadWithPrefixTests {
			t.Run(test.description, func(t *testing.T) {
				gotk, gotv, err := testCM.ReadWithPrefix(path.Join(testLoadRoot, test.prefix))
				assert.Nil(t, err)
				assert.Equal(t, len(test.expectedValue), len(gotk))
				assert.Equal(t, len(test.expectedValue), len(gotv))
				assert.ElementsMatch(t, test.expectedValue, gotv)
			})
		}

		multiLoadTests := []struct {
			isvalid   bool
			multiKeys []string

			expectedValue [][]byte
			description   string
		}{
			{false, []string{"key_1", "key_not_exist"}, [][]byte{[]byte("111"), []byte(nil)}, "multiload 1 exist 1 not"},
			{true, []string{"abc", "key_3"}, [][]byte{[]byte("123"), []byte("333")}, "multiload 2 exist"},
		}

		for _, test := range multiLoadTests {
			t.Run(test.description, func(t *testing.T) {
				for i := range test.multiKeys {
					test.multiKeys[i] = path.Join(testLoadRoot, test.multiKeys[i])
				}
				if test.isvalid {
					got, err := testCM.MultiRead(test.multiKeys)
					assert.Nil(t, err)
					assert.Equal(t, test.expectedValue, got)
				} else {
					got, err := testCM.MultiRead(test.multiKeys)
					assert.Error(t, err)
					assert.Equal(t, test.expectedValue, got)
				}
			})
		}
	})

	t.Run("test write", func(t *testing.T) {
		testMultiSaveRoot := "test_write"

		testCM := NewLocalChunkManager(RootPath(localPath))
		//defer testCM.RemoveWithPrefix(testMultiSaveRoot)

		err := testCM.Write(path.Join(testMultiSaveRoot, "key_1"), []byte("111"))
		assert.Nil(t, err)
		err = testCM.Write(path.Join(testMultiSaveRoot, "key_2"), []byte("222"))
		assert.Nil(t, err)

		val, err := testCM.Read(path.Join(testMultiSaveRoot, "key_1"))
		assert.Nil(t, err)
		assert.Equal(t, []byte("111"), val)

		val, err = testCM.Read(path.Join(testMultiSaveRoot, "key_2"))
		assert.Nil(t, err)
		assert.Equal(t, []byte("222"), val)

		err = testCM.Write(path.Join(testMultiSaveRoot, "key_1/key_1"), []byte("111"))
		assert.Error(t, err)

	})

	t.Run("test MultiSave", func(t *testing.T) {
		testMultiSaveRoot := "test_multisave"

		testCM := NewLocalChunkManager(RootPath(localPath))
		defer testCM.RemoveWithPrefix(testMultiSaveRoot)

		err := testCM.Write(path.Join(testMultiSaveRoot, "key_1"), []byte("111"))
		assert.Nil(t, err)

		kvs := map[string][]byte{
			path.Join(testMultiSaveRoot, "key_1"): []byte("123"),
			path.Join(testMultiSaveRoot, "key_2"): []byte("456"),
		}

		err = testCM.MultiWrite(kvs)
		assert.Nil(t, err)

		val, err := testCM.Read(path.Join(testMultiSaveRoot, "key_1"))
		assert.Nil(t, err)
		assert.Equal(t, []byte("123"), val)

		kvs = map[string][]byte{
			path.Join(testMultiSaveRoot, "key_1/key_1"): []byte("123"),
			path.Join(testMultiSaveRoot, "key_2/key_2"): []byte("456"),
		}

		err = testCM.MultiWrite(kvs)
		assert.Error(t, err)
	})

	t.Run("test Remove", func(t *testing.T) {
		testRemoveRoot := "test_remove"

		testCM := NewLocalChunkManager(RootPath(localPath))
		defer testCM.RemoveWithPrefix(testRemoveRoot)

		prepareTests := []struct {
			k string
			v []byte
		}{
			{"key_1", []byte("123")},
			{"key_2", []byte("456")},
			{"mkey_1", []byte("111")},
			{"mkey_2", []byte("222")},
			{"mkey_3", []byte("333")},
			{"key_prefix_1", []byte("111")},
			{"key_prefix_2", []byte("222")},
			{"key_prefix_3", []byte("333")},
		}

		for _, test := range prepareTests {
			k := path.Join(testRemoveRoot, test.k)
			err := testCM.Write(k, test.v)
			require.NoError(t, err)
		}

		removeTests := []struct {
			removeKey         string
			valueBeforeRemove []byte

			description string
		}{
			{"key_1", []byte("123"), "remove key_1"},
			{"key_2", []byte("456"), "remove key_2"},
		}

		for _, test := range removeTests {
			t.Run(test.description, func(t *testing.T) {
				k := path.Join(testRemoveRoot, test.removeKey)
				v, err := testCM.Read(k)
				require.NoError(t, err)
				require.Equal(t, test.valueBeforeRemove, v)

				err = testCM.Remove(k)
				assert.NoError(t, err)

				v, err = testCM.Read(k)
				require.Error(t, err)
				require.Empty(t, v)
			})
		}

		multiRemoveTest := []string{
			path.Join(testRemoveRoot, "mkey_1"),
			path.Join(testRemoveRoot, "mkey_2"),
			path.Join(testRemoveRoot, "mkey_3"),
		}

		lv, err := testCM.MultiRead(multiRemoveTest)
		require.Nil(t, err)
		require.ElementsMatch(t, [][]byte{[]byte("111"), []byte("222"), []byte("333")}, lv)

		err = testCM.MultiRemove(multiRemoveTest)
		assert.Nil(t, err)

		for _, k := range multiRemoveTest {
			v, err := testCM.Read(k)
			assert.Error(t, err)
			assert.Empty(t, v)
		}

		removeWithPrefixTest := []string{
			path.Join(testRemoveRoot, "key_prefix_1"),
			path.Join(testRemoveRoot, "key_prefix_2"),
			path.Join(testRemoveRoot, "key_prefix_3"),
		}
		removePrefix := path.Join(testRemoveRoot, "key_prefix")

		lv, err = testCM.MultiRead(removeWithPrefixTest)
		require.NoError(t, err)
		require.ElementsMatch(t, [][]byte{[]byte("111"), []byte("222"), []byte("333")}, lv)

		err = testCM.RemoveWithPrefix(removePrefix)
		assert.NoError(t, err)

		for _, k := range removeWithPrefixTest {
			v, err := testCM.Read(k)
			assert.Error(t, err)
			assert.Empty(t, v)
		}
	})

	t.Run("test ReadAt", func(t *testing.T) {
		testLoadPartialRoot := "read_at"

		testCM := NewLocalChunkManager(RootPath(localPath))
		defer testCM.RemoveWithPrefix(testLoadPartialRoot)

		key := path.Join(testLoadPartialRoot, "TestMinIOKV_LoadPartial_key")
		value := []byte("TestMinIOKV_LoadPartial_value")

		err := testCM.Write(key, value)
		assert.NoError(t, err)

		var off, length int64
		var partial []byte

		off, length = 1, 1
		partial, err = testCM.ReadAt(key, off, length)
		assert.NoError(t, err)
		assert.ElementsMatch(t, partial, value[off:off+length])

		off, length = 0, int64(len(value))
		partial, err = testCM.ReadAt(key, off, length)
		assert.NoError(t, err)
		assert.ElementsMatch(t, partial, value[off:off+length])

		// error case
		off, length = 5, -2
		_, err = testCM.ReadAt(key, off, length)
		assert.Error(t, err)

		off, length = -1, 2
		_, err = testCM.ReadAt(key, off, length)
		assert.Error(t, err)

		off, length = 1, -2
		_, err = testCM.ReadAt(key, off, length)
		assert.Error(t, err)

		err = testCM.Remove(key)
		assert.NoError(t, err)
		off, length = 1, 1
		_, err = testCM.ReadAt(key, off, length)
		assert.Error(t, err)
	})

	t.Run("test Size", func(t *testing.T) {
		testGetSizeRoot := "get_size"

		testCM := NewLocalChunkManager(RootPath(localPath))
		defer testCM.RemoveWithPrefix(testGetSizeRoot)

		key := path.Join(testGetSizeRoot, "TestMinIOKV_GetSize_key")
		value := []byte("TestMinIOKV_GetSize_value")

		err := testCM.Write(key, value)
		assert.NoError(t, err)

		size, err := testCM.Size(key)
		assert.NoError(t, err)
		assert.Equal(t, size, int64(len(value)))

		key2 := path.Join(testGetSizeRoot, "TestMemoryKV_GetSize_key2")

		size, err = testCM.Size(key2)
		assert.Error(t, err)
		assert.Equal(t, int64(0), size)
	})

	t.Run("test Path", func(t *testing.T) {
		testGetSizeRoot := "get_path"

		testCM := NewLocalChunkManager(RootPath(localPath))
		defer testCM.RemoveWithPrefix(testGetSizeRoot)

		key := path.Join(testGetSizeRoot, "TestMinIOKV_GetPath_key")
		value := []byte("TestMinIOKV_GetPath_value")

		err := testCM.Write(key, value)
		assert.NoError(t, err)

		p, err := testCM.Path(key)
		assert.NoError(t, err)
		assert.Equal(t, p, path.Join(localPath, key))

		key2 := path.Join(testGetSizeRoot, "TestMemoryKV_GetSize_key2")

		p, err = testCM.Path(key2)
		assert.Error(t, err)
		assert.Equal(t, p, "")
	})

	t.Run("test Prefix", func(t *testing.T) {
		testPrefix := "prefix"

		testCM := NewLocalChunkManager(RootPath(localPath))
		defer testCM.RemoveWithPrefix(testPrefix)

		pathB := path.Join("a", "b")

		key := path.Join(testPrefix, pathB)
		value := []byte("a")

		err := testCM.Write(key, value)
		assert.NoError(t, err)

		pathC := path.Join("a", "c")
		key = path.Join(testPrefix, pathC)
		err = testCM.Write(key, value)
		assert.NoError(t, err)

		pathPrefix := path.Join(testPrefix, "a")
		r, err := testCM.ListWithPrefix(pathPrefix)
		assert.NoError(t, err)
		assert.Equal(t, len(r), 2)

		testCM.RemoveWithPrefix(testPrefix)
		r, err = testCM.ListWithPrefix(pathPrefix)
		assert.NoError(t, err)
		assert.Equal(t, len(r), 0)
	})
}
