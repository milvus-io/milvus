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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// TODO: NewRemoteChunkManager is deprecated. Rewrite this unittest.
func newMinioChunkManager(ctx context.Context, bucketName string, rootPath string) (ChunkManager, error) {
	return newRemoteChunkManager(ctx, "minio", bucketName, rootPath)
}

func newAzureChunkManager(ctx context.Context, bucketName string, rootPath string) (ChunkManager, error) {
	return newRemoteChunkManager(ctx, "azure", bucketName, rootPath)
}

func newRemoteChunkManager(ctx context.Context, cloudProvider string, bucketName string, rootPath string) (ChunkManager, error) {
	factory := NewChunkManagerFactory("remote",
		objectstorage.RootPath(rootPath),
		objectstorage.Address(Params.MinioCfg.Address.GetValue()),
		objectstorage.AccessKeyID(Params.MinioCfg.AccessKeyID.GetValue()),
		objectstorage.SecretAccessKeyID(Params.MinioCfg.SecretAccessKey.GetValue()),
		objectstorage.UseSSL(Params.MinioCfg.UseSSL.GetAsBool()),
		objectstorage.SslCACert(Params.MinioCfg.SslCACert.GetValue()),
		objectstorage.BucketName(bucketName),
		objectstorage.UseIAM(Params.MinioCfg.UseIAM.GetAsBool()),
		objectstorage.CloudProvider(cloudProvider),
		objectstorage.IAMEndpoint(Params.MinioCfg.IAMEndpoint.GetValue()),
		objectstorage.CreateBucket(true))
	return factory.NewPersistentStorageChunkManager(ctx)
}

func TestInitRemoteChunkManager(t *testing.T) {
	ctx := context.Background()
	client, err := NewRemoteChunkManager(ctx, &objectstorage.Config{
		BucketName:    Params.MinioCfg.BucketName.GetValue(),
		CreateBucket:  true,
		UseIAM:        false,
		CloudProvider: "azure",
	})
	assert.NoError(t, err)
	assert.NotNil(t, client)
}

func TestMinioChunkManager(t *testing.T) {
	testBucket := Params.MinioCfg.BucketName.GetValue()

	configRoot := Params.MinioCfg.RootPath.GetValue()

	testMinIOKVRoot := path.Join(configRoot, "milvus-minio-ut-root")

	t.Run("test load", func(t *testing.T) {
		testLoadRoot := path.Join(testMinIOKVRoot, "test_load")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testCM, err := newMinioChunkManager(ctx, testBucket, testLoadRoot)
		require.NoError(t, err)
		defer testCM.RemoveWithPrefix(ctx, testLoadRoot)

		assert.Equal(t, testLoadRoot, testCM.RootPath())

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
			err = testCM.Write(ctx, path.Join(testLoadRoot, test.key), test.value)
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
					got, err := testCM.Read(ctx, path.Join(testLoadRoot, test.loadKey))
					assert.NoError(t, err)
					assert.Equal(t, test.expectedValue, got)
				} else {
					if test.loadKey == "/" {
						got, err := testCM.Read(ctx, test.loadKey)
						assert.Error(t, err)
						assert.Empty(t, got)
						return
					}
					got, err := testCM.Read(ctx, path.Join(testLoadRoot, test.loadKey))
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
				gotk, gotv, err := readAllChunkWithPrefix(ctx, testCM, path.Join(testLoadRoot, test.prefix))
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
			{false, []string{"key_1", "key_not_exist"}, [][]byte{[]byte("111"), nil}, "multiload 1 exist 1 not"},
			{true, []string{"abc", "key_3"}, [][]byte{[]byte("123"), []byte("333")}, "multiload 2 exist"},
		}

		for _, test := range multiLoadTests {
			t.Run(test.description, func(t *testing.T) {
				for i := range test.multiKeys {
					test.multiKeys[i] = path.Join(testLoadRoot, test.multiKeys[i])
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

	t.Run("test MultiSave", func(t *testing.T) {
		testMultiSaveRoot := path.Join(testMinIOKVRoot, "test_multisave")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testCM, err := newMinioChunkManager(ctx, testBucket, testMultiSaveRoot)
		assert.NoError(t, err)
		defer testCM.RemoveWithPrefix(ctx, testMultiSaveRoot)

		err = testCM.Write(ctx, path.Join(testMultiSaveRoot, "key_1"), []byte("111"))
		assert.NoError(t, err)

		kvs := map[string][]byte{
			path.Join(testMultiSaveRoot, "key_1"): []byte("123"),
			path.Join(testMultiSaveRoot, "key_2"): []byte("456"),
		}

		err = testCM.MultiWrite(ctx, kvs)
		assert.NoError(t, err)

		val, err := testCM.Read(ctx, path.Join(testMultiSaveRoot, "key_1"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("123"), val)

		reader, err := testCM.Reader(ctx, path.Join(testMultiSaveRoot, "key_1"))
		assert.NoError(t, err)
		reader.Close()
	})

	t.Run("test Remove", func(t *testing.T) {
		testRemoveRoot := path.Join(testMinIOKVRoot, "test_remove")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testCM, err := newMinioChunkManager(ctx, testBucket, testRemoveRoot)
		assert.NoError(t, err)
		defer testCM.RemoveWithPrefix(ctx, testRemoveRoot)

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
			err = testCM.Write(ctx, k, test.v)
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
			path.Join(testRemoveRoot, "mkey_1"),
			path.Join(testRemoveRoot, "mkey_2"),
			path.Join(testRemoveRoot, "mkey_3"),
		}

		lv, err := testCM.MultiRead(ctx, multiRemoveTest)
		require.NoError(t, err)
		require.ElementsMatch(t, [][]byte{[]byte("111"), []byte("222"), []byte("333")}, lv)

		err = testCM.MultiRemove(ctx, multiRemoveTest)
		assert.NoError(t, err)

		for _, k := range multiRemoveTest {
			v, err := testCM.Read(ctx, k)
			assert.Error(t, err)
			assert.Empty(t, v)
		}

		removeWithPrefixTest := []string{
			path.Join(testRemoveRoot, "key_prefix_1"),
			path.Join(testRemoveRoot, "key_prefix_2"),
			path.Join(testRemoveRoot, "key_prefix_3"),
		}
		removePrefix := path.Join(testRemoveRoot, "key_prefix")

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
		testLoadPartialRoot := path.Join(testMinIOKVRoot, "load_partial")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testCM, err := newMinioChunkManager(ctx, testBucket, testLoadPartialRoot)
		require.NoError(t, err)
		defer testCM.RemoveWithPrefix(ctx, testLoadPartialRoot)

		key := path.Join(testLoadPartialRoot, "TestMinIOKV_LoadPartial_key")
		value := []byte("TestMinIOKV_LoadPartial_value")

		err = testCM.Write(ctx, key, value)
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
		testGetSizeRoot := path.Join(testMinIOKVRoot, "get_size")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testCM, err := newMinioChunkManager(ctx, testBucket, testGetSizeRoot)
		require.NoError(t, err)
		defer testCM.RemoveWithPrefix(ctx, testGetSizeRoot)

		key := path.Join(testGetSizeRoot, "TestMinIOKV_GetSize_key")
		value := []byte("TestMinIOKV_GetSize_value")

		err = testCM.Write(ctx, key, value)
		assert.NoError(t, err)

		size, err := testCM.Size(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, size, int64(len(value)))

		key2 := path.Join(testGetSizeRoot, "TestMemoryKV_GetSize_key2")

		size, err = testCM.Size(ctx, key2)
		assert.Error(t, err)
		assert.Equal(t, int64(0), size)
	})

	t.Run("test Path", func(t *testing.T) {
		testGetPathRoot := path.Join(testMinIOKVRoot, "get_path")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testCM, err := newMinioChunkManager(ctx, testBucket, testGetPathRoot)
		require.NoError(t, err)
		defer testCM.RemoveWithPrefix(ctx, testGetPathRoot)

		key := path.Join(testGetPathRoot, "TestMinIOKV_GetSize_key")
		value := []byte("TestMinIOKV_GetSize_value")

		err = testCM.Write(ctx, key, value)
		assert.NoError(t, err)

		p, err := testCM.Path(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, p, key)

		key2 := path.Join(testGetPathRoot, "TestMemoryKV_GetSize_key2")

		p, err = testCM.Path(ctx, key2)
		assert.Error(t, err)
		assert.Equal(t, p, "")
	})

	t.Run("test Mmap", func(t *testing.T) {
		testMmapRoot := path.Join(testMinIOKVRoot, "mmap")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testCM, err := newMinioChunkManager(ctx, testBucket, testMmapRoot)
		require.NoError(t, err)
		defer testCM.RemoveWithPrefix(ctx, testMmapRoot)

		key := path.Join(testMmapRoot, "TestMinIOKV_GetSize_key")
		value := []byte("TestMinIOKV_GetSize_value")

		err = testCM.Write(ctx, key, value)
		assert.NoError(t, err)

		r, err := testCM.Mmap(ctx, key)
		assert.Error(t, err)
		assert.Nil(t, r)
	})

	t.Run("test Prefix", func(t *testing.T) {
		testPrefix := path.Join(testMinIOKVRoot, "prefix")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testCM, err := newMinioChunkManager(ctx, testBucket, testPrefix)
		require.NoError(t, err)
		defer testCM.RemoveWithPrefix(ctx, testPrefix)

		pathB := path.Join("a", "b")

		key := path.Join(testPrefix, pathB)
		value := []byte("a")

		err = testCM.Write(ctx, key, value)
		assert.NoError(t, err)

		pathC := path.Join("a", "c")
		key = path.Join(testPrefix, pathC)
		err = testCM.Write(ctx, key, value)
		assert.NoError(t, err)

		pathPrefix := path.Join(testPrefix, "a")
		r, m, err := ListAllChunkWithPrefix(ctx, testCM, pathPrefix, true)
		assert.NoError(t, err)
		assert.Equal(t, len(r), 2)
		assert.Equal(t, len(m), 2)

		key = path.Join(testPrefix, "b", "b", "b")
		err = testCM.Write(ctx, key, value)
		assert.NoError(t, err)

		key = path.Join(testPrefix, "b", "a", "b")
		err = testCM.Write(ctx, key, value)
		assert.NoError(t, err)

		key = path.Join(testPrefix, "bc", "a", "b")
		err = testCM.Write(ctx, key, value)
		assert.NoError(t, err)
		dirs, mods, err := ListAllChunkWithPrefix(ctx, testCM, testPrefix+"/", true)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(dirs))
		assert.Equal(t, 5, len(mods))

		dirs, mods, err = ListAllChunkWithPrefix(ctx, testCM, path.Join(testPrefix, "b"), true)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(dirs))
		assert.Equal(t, 3, len(mods))

		testCM.RemoveWithPrefix(ctx, testPrefix)
		r, m, err = ListAllChunkWithPrefix(ctx, testCM, pathPrefix, true)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(r))
		assert.Equal(t, 0, len(m))

		// test wrong prefix
		b := make([]byte, 2048)
		pathWrong := path.Join(testPrefix, string(b))
		_, _, err = ListAllChunkWithPrefix(ctx, testCM, pathWrong, true)
		assert.Error(t, err)
	})

	t.Run("test NoSuchKey", func(t *testing.T) {
		testPrefix := path.Join(testMinIOKVRoot, "nokey")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testCM, err := newMinioChunkManager(ctx, testBucket, testPrefix)
		require.NoError(t, err)
		defer testCM.RemoveWithPrefix(ctx, testPrefix)

		key := "a"

		_, err = testCM.Read(ctx, key)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, merr.ErrIoKeyNotFound))

		file, err := testCM.Reader(ctx, key)
		assert.NoError(t, err) // todo
		file.Close()

		_, err = testCM.ReadAt(ctx, key, 100, 1)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, merr.ErrIoKeyNotFound))
	})
}

func TestAzureChunkManager(t *testing.T) {
	testBucket := Params.MinioCfg.BucketName.GetValue()

	configRoot := Params.MinioCfg.RootPath.GetValue()

	testMinIOKVRoot := path.Join(configRoot, "milvus-minio-ut-root")

	t.Run("test load", func(t *testing.T) {
		testLoadRoot := path.Join(testMinIOKVRoot, "test_load")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testCM, err := newAzureChunkManager(ctx, testBucket, testLoadRoot)
		require.NoError(t, err)
		defer testCM.RemoveWithPrefix(ctx, testLoadRoot)

		assert.Equal(t, testLoadRoot, testCM.RootPath())

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
			err = testCM.Write(ctx, path.Join(testLoadRoot, test.key), test.value)
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
					got, err := testCM.Read(ctx, path.Join(testLoadRoot, test.loadKey))
					assert.NoError(t, err)
					assert.Equal(t, test.expectedValue, got)
				} else {
					if test.loadKey == "/" {
						got, err := testCM.Read(ctx, test.loadKey)
						assert.Error(t, err)
						assert.Empty(t, got)
						return
					}
					got, err := testCM.Read(ctx, path.Join(testLoadRoot, test.loadKey))
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
				gotk, gotv, err := readAllChunkWithPrefix(ctx, testCM, path.Join(testLoadRoot, test.prefix))
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
			{false, []string{"key_1", "key_not_exist"}, [][]byte{[]byte("111"), nil}, "multiload 1 exist 1 not"},
			{true, []string{"abc", "key_3"}, [][]byte{[]byte("123"), []byte("333")}, "multiload 2 exist"},
		}

		for _, test := range multiLoadTests {
			t.Run(test.description, func(t *testing.T) {
				for i := range test.multiKeys {
					test.multiKeys[i] = path.Join(testLoadRoot, test.multiKeys[i])
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

	t.Run("test MultiSave", func(t *testing.T) {
		testMultiSaveRoot := path.Join(testMinIOKVRoot, "test_multisave")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testCM, err := newAzureChunkManager(ctx, testBucket, testMultiSaveRoot)
		assert.NoError(t, err)
		defer testCM.RemoveWithPrefix(ctx, testMultiSaveRoot)

		err = testCM.Write(ctx, path.Join(testMultiSaveRoot, "key_1"), []byte("111"))
		assert.NoError(t, err)

		kvs := map[string][]byte{
			path.Join(testMultiSaveRoot, "key_1"): []byte("123"),
			path.Join(testMultiSaveRoot, "key_2"): []byte("456"),
		}

		err = testCM.MultiWrite(ctx, kvs)
		assert.NoError(t, err)

		val, err := testCM.Read(ctx, path.Join(testMultiSaveRoot, "key_1"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("123"), val)

		reader, err := testCM.Reader(ctx, path.Join(testMultiSaveRoot, "key_1"))
		assert.NoError(t, err)
		reader.Close()
	})

	t.Run("test Remove", func(t *testing.T) {
		testRemoveRoot := path.Join(testMinIOKVRoot, "test_remove")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testCM, err := newAzureChunkManager(ctx, testBucket, testRemoveRoot)
		assert.NoError(t, err)
		defer testCM.RemoveWithPrefix(ctx, testRemoveRoot)

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
			err = testCM.Write(ctx, k, test.v)
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
			path.Join(testRemoveRoot, "mkey_1"),
			path.Join(testRemoveRoot, "mkey_2"),
			path.Join(testRemoveRoot, "mkey_3"),
		}

		lv, err := testCM.MultiRead(ctx, multiRemoveTest)
		require.NoError(t, err)
		require.ElementsMatch(t, [][]byte{[]byte("111"), []byte("222"), []byte("333")}, lv)

		err = testCM.MultiRemove(ctx, multiRemoveTest)
		assert.NoError(t, err)

		for _, k := range multiRemoveTest {
			v, err := testCM.Read(ctx, k)
			assert.Error(t, err)
			assert.Empty(t, v)
		}

		removeWithPrefixTest := []string{
			path.Join(testRemoveRoot, "key_prefix_1"),
			path.Join(testRemoveRoot, "key_prefix_2"),
			path.Join(testRemoveRoot, "key_prefix_3"),
		}
		removePrefix := path.Join(testRemoveRoot, "key_prefix")

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
		testLoadPartialRoot := path.Join(testMinIOKVRoot, "load_partial")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testCM, err := newAzureChunkManager(ctx, testBucket, testLoadPartialRoot)
		require.NoError(t, err)
		defer testCM.RemoveWithPrefix(ctx, testLoadPartialRoot)

		key := path.Join(testLoadPartialRoot, "TestMinIOKV_LoadPartial_key")
		value := []byte("TestMinIOKV_LoadPartial_value")

		err = testCM.Write(ctx, key, value)
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
		testGetSizeRoot := path.Join(testMinIOKVRoot, "get_size")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testCM, err := newAzureChunkManager(ctx, testBucket, testGetSizeRoot)
		require.NoError(t, err)
		defer testCM.RemoveWithPrefix(ctx, testGetSizeRoot)

		key := path.Join(testGetSizeRoot, "TestMinIOKV_GetSize_key")
		value := []byte("TestMinIOKV_GetSize_value")

		err = testCM.Write(ctx, key, value)
		assert.NoError(t, err)

		size, err := testCM.Size(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, size, int64(len(value)))

		key2 := path.Join(testGetSizeRoot, "TestMemoryKV_GetSize_key2")

		size, err = testCM.Size(ctx, key2)
		assert.Error(t, err)
		assert.Equal(t, int64(0), size)
	})

	t.Run("test Path", func(t *testing.T) {
		testGetPathRoot := path.Join(testMinIOKVRoot, "get_path")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testCM, err := newAzureChunkManager(ctx, testBucket, testGetPathRoot)
		require.NoError(t, err)
		defer testCM.RemoveWithPrefix(ctx, testGetPathRoot)

		key := path.Join(testGetPathRoot, "TestMinIOKV_GetSize_key")
		value := []byte("TestMinIOKV_GetSize_value")

		err = testCM.Write(ctx, key, value)
		assert.NoError(t, err)

		p, err := testCM.Path(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, p, key)

		key2 := path.Join(testGetPathRoot, "TestMemoryKV_GetSize_key2")

		p, err = testCM.Path(ctx, key2)
		assert.Error(t, err)
		assert.Equal(t, p, "")
	})

	t.Run("test Mmap", func(t *testing.T) {
		testMmapRoot := path.Join(testMinIOKVRoot, "mmap")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testCM, err := newAzureChunkManager(ctx, testBucket, testMmapRoot)
		require.NoError(t, err)
		defer testCM.RemoveWithPrefix(ctx, testMmapRoot)

		key := path.Join(testMmapRoot, "TestMinIOKV_GetSize_key")
		value := []byte("TestMinIOKV_GetSize_value")

		err = testCM.Write(ctx, key, value)
		assert.NoError(t, err)

		r, err := testCM.Mmap(ctx, key)
		assert.Error(t, err)
		assert.Nil(t, r)
	})

	t.Run("test Prefix", func(t *testing.T) {
		testPrefix := path.Join(testMinIOKVRoot, "prefix")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testCM, err := newAzureChunkManager(ctx, testBucket, testPrefix)
		require.NoError(t, err)
		defer testCM.RemoveWithPrefix(ctx, testPrefix)

		pathB := path.Join("a", "b")

		key := path.Join(testPrefix, pathB)
		value := []byte("a")

		err = testCM.Write(ctx, key, value)
		assert.NoError(t, err)

		pathC := path.Join("a", "c")
		key = path.Join(testPrefix, pathC)
		err = testCM.Write(ctx, key, value)
		assert.NoError(t, err)

		pathPrefix := path.Join(testPrefix, "a")
		r, m, err := ListAllChunkWithPrefix(ctx, testCM, pathPrefix, true)
		assert.NoError(t, err)
		assert.Equal(t, len(r), 2)
		assert.Equal(t, len(m), 2)

		key = path.Join(testPrefix, "b", "b", "b")
		err = testCM.Write(ctx, key, value)
		assert.NoError(t, err)

		key = path.Join(testPrefix, "b", "a", "b")
		err = testCM.Write(ctx, key, value)
		assert.NoError(t, err)

		key = path.Join(testPrefix, "bc", "a", "b")
		err = testCM.Write(ctx, key, value)
		assert.NoError(t, err)
		dirs, mods, err := ListAllChunkWithPrefix(ctx, testCM, testPrefix+"/", true)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(dirs))
		assert.Equal(t, 5, len(mods))

		dirs, mods, err = ListAllChunkWithPrefix(ctx, testCM, path.Join(testPrefix, "b"), true)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(dirs))
		assert.Equal(t, 3, len(mods))

		testCM.RemoveWithPrefix(ctx, testPrefix)
		r, m, err = ListAllChunkWithPrefix(ctx, testCM, pathPrefix, true)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(r))
		assert.Equal(t, 0, len(m))

		// test wrong prefix
		b := make([]byte, 2048)
		pathWrong := path.Join(testPrefix, string(b))
		_, _, err = ListAllChunkWithPrefix(ctx, testCM, pathWrong, true)
		assert.Error(t, err)
	})

	t.Run("test NoSuchKey", func(t *testing.T) {
		testPrefix := path.Join(testMinIOKVRoot, "nokey")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testCM, err := newAzureChunkManager(ctx, testBucket, testPrefix)
		require.NoError(t, err)
		defer testCM.RemoveWithPrefix(ctx, testPrefix)

		key := "a"

		_, err = testCM.Read(ctx, key)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, merr.ErrIoKeyNotFound))

		_, err = testCM.Reader(ctx, key)
		// lazy error for real read
		assert.NoError(t, err)

		_, err = testCM.ReadAt(ctx, key, 100, 1)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, merr.ErrIoKeyNotFound))
	})
}
