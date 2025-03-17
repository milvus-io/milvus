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
	"context"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
)

const (
	localPath = "/tmp/milvus_test/chunkmanager/"
)

func TestLocalCM(t *testing.T) {
	ctx := context.Background()

	t.Run("test RootPath", func(t *testing.T) {
		testCM := NewLocalChunkManager(objectstorage.RootPath(localPath))
		assert.Equal(t, localPath, testCM.RootPath())
	})

	t.Run("test load", func(t *testing.T) {
		testLoadRoot := "test_load"

		testCM := NewLocalChunkManager(objectstorage.RootPath(localPath))
		defer testCM.RemoveWithPrefix(ctx, testCM.RootPath())

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
			err := testCM.Write(ctx, path.Join(localPath, testLoadRoot, test.key), test.value)
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
					got, err := testCM.Read(ctx, path.Join(localPath, testLoadRoot, test.loadKey))
					assert.NoError(t, err)
					assert.Equal(t, test.expectedValue, got)
				} else {
					if test.loadKey == "/" {
						got, err := testCM.Read(ctx, path.Join(localPath, testLoadRoot, test.loadKey))
						assert.Error(t, err)
						assert.Empty(t, got)
						return
					}
					got, err := testCM.Read(ctx, path.Join(localPath, testLoadRoot, test.loadKey))
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
				gotk, gotv, err := readAllChunkWithPrefix(ctx, testCM, path.Join(localPath, testLoadRoot, test.prefix))
				assert.NoError(t, err)
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
					test.multiKeys[i] = path.Join(localPath, testLoadRoot, test.multiKeys[i])
				}
				if test.isvalid {
					got, err := testCM.MultiRead(ctx, test.multiKeys)
					assert.NoError(t, err)
					assert.Equal(t, test.expectedValue, got)
				} else {
					got, err := testCM.MultiRead(ctx, test.multiKeys)
					assert.Error(t, err)
					assert.Equal(t, test.expectedValue, got)
				}
			})
		}
	})

	t.Run("test write", func(t *testing.T) {
		testMultiSaveRoot := "test_write"

		testCM := NewLocalChunkManager(objectstorage.RootPath(localPath))
		defer testCM.RemoveWithPrefix(ctx, testCM.RootPath())

		key1 := path.Join(localPath, testMultiSaveRoot, "key_1")
		err := testCM.Write(ctx, key1, []byte("111"))
		assert.NoError(t, err)
		key2 := path.Join(localPath, testMultiSaveRoot, "key_2")
		err = testCM.Write(ctx, key2, []byte("222"))
		assert.NoError(t, err)

		val, err := testCM.Read(ctx, key1)
		assert.NoError(t, err)
		assert.Equal(t, []byte("111"), val)

		val, err = testCM.Read(ctx, key2)
		assert.NoError(t, err)
		assert.Equal(t, []byte("222"), val)

		// localPath/testMultiSaveRoot/key_1 is a file already exist, use its path as directory is not allowed
		key3 := path.Join(localPath, testMultiSaveRoot, "key_1/key_1")
		err = testCM.Write(ctx, key3, []byte("111"))
		assert.Error(t, err)
	})

	t.Run("test MultiSave", func(t *testing.T) {
		testMultiSaveRoot := "test_multisave"

		testCM := NewLocalChunkManager(objectstorage.RootPath(localPath))
		defer testCM.RemoveWithPrefix(ctx, testCM.RootPath())

		err := testCM.Write(ctx, path.Join(localPath, testMultiSaveRoot, "key_1"), []byte("111"))
		assert.NoError(t, err)

		kvs := map[string][]byte{
			path.Join(localPath, testMultiSaveRoot, "key_1"): []byte("123"),
			path.Join(localPath, testMultiSaveRoot, "key_2"): []byte("456"),
		}

		err = testCM.MultiWrite(ctx, kvs)
		assert.NoError(t, err)

		val, err := testCM.Read(ctx, path.Join(localPath, testMultiSaveRoot, "key_1"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("123"), val)

		kvs = map[string][]byte{
			path.Join(localPath, testMultiSaveRoot, "key_1/key_1"): []byte("123"),
			path.Join(localPath, testMultiSaveRoot, "key_2/key_2"): []byte("456"),
		}

		err = testCM.MultiWrite(ctx, kvs)
		assert.Error(t, err)
	})

	t.Run("test Remove", func(t *testing.T) {
		testRemoveRoot := "test_remove"

		testCM := NewLocalChunkManager(objectstorage.RootPath(localPath))

		// empty prefix is not allowed
		err := testCM.RemoveWithPrefix(ctx, "")
		assert.Error(t, err)

		// prefix ".", nothing deleted
		err = testCM.RemoveWithPrefix(ctx, ".")
		assert.NoError(t, err)

		defer testCM.RemoveWithPrefix(ctx, testCM.RootPath())

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
			k := path.Join(localPath, testRemoveRoot, test.k)
			err := testCM.Write(ctx, k, test.v)
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
				k := path.Join(localPath, testRemoveRoot, test.removeKey)
				v, err := testCM.Read(ctx, k)
				require.NoError(t, err)
				require.Equal(t, test.valueBeforeRemove, v)

				err = testCM.Remove(ctx, k)
				assert.NoError(t, err)

				v, err = testCM.Read(ctx, k)
				require.Error(t, err)
				require.Empty(t, v)
			})
		}

		multiRemoveTest := []string{
			path.Join(localPath, testRemoveRoot, "mkey_1"),
			path.Join(localPath, testRemoveRoot, "mkey_2"),
			path.Join(localPath, testRemoveRoot, "mkey_3"),
		}

		lv, err := testCM.MultiRead(ctx, multiRemoveTest)
		require.Nil(t, err)
		require.ElementsMatch(t, [][]byte{[]byte("111"), []byte("222"), []byte("333")}, lv)

		err = testCM.MultiRemove(ctx, multiRemoveTest)
		assert.NoError(t, err)

		for _, k := range multiRemoveTest {
			v, err := testCM.Read(ctx, k)
			assert.Error(t, err)
			assert.Empty(t, v)
		}

		removeWithPrefixTest := []string{
			path.Join(localPath, testRemoveRoot, "key_prefix_1"),
			path.Join(localPath, testRemoveRoot, "key_prefix_2"),
			path.Join(localPath, testRemoveRoot, "key_prefix_3"),
		}
		removePrefix := path.Join(localPath, testRemoveRoot, "key_prefix")

		lv, err = testCM.MultiRead(ctx, removeWithPrefixTest)
		require.NoError(t, err)
		require.ElementsMatch(t, [][]byte{[]byte("111"), []byte("222"), []byte("333")}, lv)

		err = testCM.RemoveWithPrefix(ctx, removePrefix)
		assert.NoError(t, err)

		for _, k := range removeWithPrefixTest {
			v, err := testCM.Read(ctx, k)
			assert.Error(t, err)
			assert.Empty(t, v)
		}
	})

	t.Run("test ReadAt", func(t *testing.T) {
		testLoadPartialRoot := "read_at"

		testCM := NewLocalChunkManager(objectstorage.RootPath(localPath))
		defer testCM.RemoveWithPrefix(ctx, testCM.RootPath())

		key := path.Join(localPath, testLoadPartialRoot, "TestMinIOKV_LoadPartial_key")
		value := []byte("TestMinIOKV_LoadPartial_value")

		err := testCM.Write(ctx, key, value)
		assert.NoError(t, err)

		var off, length int64
		var partial []byte

		off, length = 1, 1
		partial, err = testCM.ReadAt(ctx, key, off, length)
		assert.NoError(t, err)
		assert.ElementsMatch(t, partial, value[off:off+length])

		off, length = 0, int64(len(value))
		partial, err = testCM.ReadAt(ctx, key, off, length)
		assert.NoError(t, err)
		assert.ElementsMatch(t, partial, value[off:off+length])

		// error case
		off, length = 5, -2
		_, err = testCM.ReadAt(ctx, key, off, length)
		assert.Error(t, err)

		off, length = -1, 2
		_, err = testCM.ReadAt(ctx, key, off, length)
		assert.Error(t, err)

		off, length = 1, -2
		_, err = testCM.ReadAt(ctx, key, off, length)
		assert.Error(t, err)

		err = testCM.Remove(ctx, key)
		assert.NoError(t, err)
		off, length = 1, 1
		_, err = testCM.ReadAt(ctx, key, off, length)
		assert.Error(t, err)
	})

	t.Run("test Size", func(t *testing.T) {
		testGetSizeRoot := "get_size"

		testCM := NewLocalChunkManager(objectstorage.RootPath(localPath))
		defer testCM.RemoveWithPrefix(ctx, testCM.RootPath())

		key := path.Join(localPath, testGetSizeRoot, "TestMinIOKV_GetSize_key")
		value := []byte("TestMinIOKV_GetSize_value")

		err := testCM.Write(ctx, key, value)
		assert.NoError(t, err)

		size, err := testCM.Size(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, size, int64(len(value)))

		key2 := path.Join(localPath, testGetSizeRoot, "TestMemoryKV_GetSize_key2")

		size, err = testCM.Size(ctx, key2)
		assert.Error(t, err)
		assert.Equal(t, int64(0), size)
	})

	t.Run("test read", func(t *testing.T) {
		testGetSizeRoot := "get_path"

		testCM := NewLocalChunkManager(objectstorage.RootPath(localPath))
		defer testCM.RemoveWithPrefix(ctx, testCM.RootPath())

		key := path.Join(localPath, testGetSizeRoot, "TestMinIOKV_GetPath_key")
		value := []byte("TestMinIOKV_GetPath_value")

		reader, err := testCM.Reader(ctx, key)
		assert.Nil(t, reader)
		assert.Error(t, err)

		err = testCM.Write(ctx, key, value)
		assert.NoError(t, err)

		reader, err = testCM.Reader(ctx, key)
		assert.NoError(t, err)
		assert.NotNil(t, reader)
	})

	t.Run("test Path", func(t *testing.T) {
		testGetSizeRoot := "get_path"

		testCM := NewLocalChunkManager(objectstorage.RootPath(localPath))
		defer testCM.RemoveWithPrefix(ctx, testCM.RootPath())

		key := path.Join(localPath, testGetSizeRoot, "TestMinIOKV_GetPath_key")
		value := []byte("TestMinIOKV_GetPath_value")

		err := testCM.Write(ctx, key, value)
		assert.NoError(t, err)

		p, err := testCM.Path(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, p, key)

		key2 := path.Join(localPath, testGetSizeRoot, "TestMemoryKV_GetSize_key2")

		p, err = testCM.Path(ctx, key2)
		assert.Error(t, err)
		assert.Equal(t, p, "")
	})

	t.Run("test Prefix", func(t *testing.T) {
		testPrefix := "prefix"

		testCM := NewLocalChunkManager(objectstorage.RootPath(localPath))
		defer testCM.RemoveWithPrefix(ctx, testCM.RootPath())

		// write 2 files:
		// localPath/testPrefix/a/b
		// localPath/testPrefix/a/c
		value := []byte("a")
		pathB := path.Join("a", "b")
		key1 := path.Join(localPath, testPrefix, pathB)

		err := testCM.Write(ctx, key1, value)
		assert.NoError(t, err)

		pathC := path.Join("a", "c")
		key2 := path.Join(localPath, testPrefix, pathC)

		err = testCM.Write(ctx, key2, value)
		assert.NoError(t, err)

		// recursive find localPath/testPrefix/a*
		// return:
		//   localPath/testPrefix/a/b
		//   localPath/testPrefix/a/c
		pathPrefix := path.Join(localPath, testPrefix, "a")
		dirs, m, err := ListAllChunkWithPrefix(ctx, testCM, pathPrefix, true)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(dirs))
		assert.Equal(t, 2, len(m))
		assert.Contains(t, dirs, key1)
		assert.Contains(t, dirs, key2)

		// remove files of localPath/testPrefix
		err = testCM.RemoveWithPrefix(ctx, path.Join(localPath, testPrefix))
		assert.NoError(t, err)

		// no file returned
		dirs, m, err = ListAllChunkWithPrefix(ctx, testCM, pathPrefix, true)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(dirs))
		assert.Equal(t, 0, len(m))
	})

	t.Run("test ListWithPrefix", func(t *testing.T) {
		testPrefix := "prefix-ListWithPrefix"

		testCM := NewLocalChunkManager(objectstorage.RootPath(localPath))
		defer testCM.RemoveWithPrefix(ctx, testCM.RootPath())

		// write 4 files:
		//   localPath/testPrefix/abc/def
		//   localPath/testPrefix/abc/deg
		//   localPath/testPrefix/abd
		//   localPath/testPrefix/bcd
		key1 := path.Join(localPath, testPrefix, "abc", "def")
		value := []byte("a")
		err := testCM.Write(ctx, key1, value)
		assert.NoError(t, err)

		key2 := path.Join(localPath, testPrefix, "abc", "deg")
		err = testCM.Write(ctx, key2, value)
		assert.NoError(t, err)

		key3 := path.Join(localPath, testPrefix, "abd")
		err = testCM.Write(ctx, key3, value)
		assert.NoError(t, err)

		key4 := path.Join(localPath, testPrefix, "bcd")
		err = testCM.Write(ctx, key4, value)
		assert.NoError(t, err)

		// non-recursive find localPath/testPrefix/*
		// return:
		//   localPath/testPrefix/abc
		//   localPath/testPrefix/abd
		//   localPath/testPrefix/bcd
		testPrefix1 := path.Join(localPath, testPrefix)
		dirs, mods, err := ListAllChunkWithPrefix(ctx, testCM, testPrefix1+"/", false)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(dirs))
		assert.Equal(t, 3, len(mods))
		assert.Contains(t, dirs, filepath.Dir(key1))
		assert.Contains(t, dirs, key3)
		assert.Contains(t, dirs, key4)

		// recursive find localPath/testPrefix/*
		// return:
		//   localPath/testPrefix/abc/def
		//   localPath/testPrefix/abc/deg
		//   localPath/testPrefix/abd
		//   localPath/testPrefix/bcd
		dirs, mods, err = ListAllChunkWithPrefix(ctx, testCM, testPrefix1+"/", true)
		assert.NoError(t, err)
		assert.Equal(t, 4, len(dirs))
		assert.Equal(t, 4, len(mods))
		assert.Contains(t, dirs, key1)
		assert.Contains(t, dirs, key2)
		assert.Contains(t, dirs, key3)
		assert.Contains(t, dirs, key4)

		// non-recursive find localPath/testPrefix/a*
		// return:
		//   localPath/testPrefix/abc
		//   localPath/testPrefix/abd
		testPrefix2 := path.Join(localPath, testPrefix, "a")
		dirs, mods, err = ListAllChunkWithPrefix(ctx, testCM, testPrefix2, false)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(dirs))
		assert.Equal(t, 2, len(mods))
		assert.Contains(t, dirs, filepath.Dir(key1))
		assert.Contains(t, dirs, key3)

		// recursive find localPath/testPrefix/a*
		// return:
		//   localPath/testPrefix/abc/def
		//   localPath/testPrefix/abc/deg
		//   localPath/testPrefix/abd
		dirs, mods, err = ListAllChunkWithPrefix(ctx, testCM, testPrefix2, true)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(dirs))
		assert.Equal(t, 3, len(mods))
		assert.Contains(t, dirs, key1)
		assert.Contains(t, dirs, key2)
		assert.Contains(t, dirs, key3)

		// remove files of localPath/testPrefix/a*, one file left
		//   localPath/testPrefix/bcd
		err = testCM.RemoveWithPrefix(ctx, testPrefix2)
		assert.NoError(t, err)

		// non-recursive find localPath/testPrefix
		// return:
		//   localPath/testPrefix
		dirs, mods, err = ListAllChunkWithPrefix(ctx, testCM, testPrefix1, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(dirs))
		assert.Equal(t, 1, len(mods))
		assert.Contains(t, dirs, filepath.Dir(key4))

		// recursive find localPath/testPrefix
		// return:
		//   localPath/testPrefix/bcd
		dirs, mods, err = ListAllChunkWithPrefix(ctx, testCM, testPrefix1, true)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(dirs))
		assert.Equal(t, 1, len(mods))
		assert.Contains(t, dirs, key4)

		// non-recursive find localPath/testPrefix/a*
		// return:
		//   localPath/testPrefix/abc
		dirs, mods, err = ListAllChunkWithPrefix(ctx, testCM, testPrefix2, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(dirs))
		assert.Equal(t, 1, len(mods))
		assert.Contains(t, dirs, filepath.Dir(key1))

		// recursive find localPath/testPrefix/a*
		// no file returned
		dirs, mods, err = ListAllChunkWithPrefix(ctx, testCM, testPrefix2, true)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(dirs))
		assert.Equal(t, 0, len(mods))

		// remove the folder localPath/testPrefix
		// the file localPath/testPrefix/bcd is removed, but the folder testPrefix still exist
		err = testCM.RemoveWithPrefix(ctx, testPrefix1)
		assert.NoError(t, err)

		// recursive find localPath/testPrefix
		// no file returned
		dirs, mods, err = ListAllChunkWithPrefix(ctx, testCM, testPrefix1, true)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(dirs))
		assert.Equal(t, 0, len(mods))

		// recursive find localPath/testPrefix
		// return
		//   localPath/testPrefix
		dirs, mods, err = ListAllChunkWithPrefix(ctx, testCM, testPrefix1, false)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(dirs))
		assert.Equal(t, 1, len(mods))
		assert.Contains(t, dirs, filepath.Dir(key4))
	})
}

func readAllChunkWithPrefix(ctx context.Context, manager ChunkManager, prefix string) ([]string, [][]byte, error) {
	var paths []string
	var contents [][]byte
	if err := manager.WalkWithPrefix(ctx, prefix, true, func(object *ChunkObjectInfo) bool {
		paths = append(paths, object.FilePath)
		content, err := manager.Read(ctx, object.FilePath)
		if err != nil {
			return false
		}
		contents = append(contents, content)
		return true
	}); err != nil {
		return nil, nil, err
	}
	return paths, contents, nil
}
