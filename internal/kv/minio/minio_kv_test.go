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

package miniokv_test

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"testing"

	miniokv "github.com/milvus-io/milvus/internal/kv/minio"
	"github.com/milvus-io/milvus/internal/util/paramtable"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var Params paramtable.BaseTable

func newMinIOKVClient(ctx context.Context, bucketName string) (*miniokv.MinIOKV, error) {
	endPoint, _ := Params.Load("_MinioAddress")
	accessKeyID, _ := Params.Load("minio.accessKeyID")
	secretAccessKey, _ := Params.Load("minio.secretAccessKey")
	useSSLStr, _ := Params.Load("minio.useSSL")
	useSSL, _ := strconv.ParseBool(useSSLStr)
	option := &miniokv.Option{
		Address:           endPoint,
		AccessKeyID:       accessKeyID,
		SecretAccessKeyID: secretAccessKey,
		UseSSL:            useSSL,
		BucketName:        bucketName,
		CreateBucket:      true,
	}
	client, err := miniokv.NewMinIOKV(ctx, option)
	return client, err
}

func TestMinIOKV(t *testing.T) {
	Params.Init()
	testBucket, err := Params.Load("minio.bucketName")
	require.NoError(t, err)

	configRoot, err := Params.Load("minio.rootPath")
	require.NoError(t, err)

	testMinIOKVRoot := path.Join(configRoot, "milvus-minio-ut-root")

	t.Run("test load", func(t *testing.T) {
		testLoadRoot := path.Join(testMinIOKVRoot, "test_load")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testKV, err := newMinIOKVClient(ctx, testBucket)
		require.NoError(t, err)
		defer testKV.RemoveWithPrefix(testLoadRoot)

		prepareTests := []struct {
			key   string
			value string
		}{
			{"abc", "123"},
			{"abcd", "1234"},
			{"key_1", "111"},
			{"key_2", "222"},
			{"key_3", "333"},
		}

		for _, test := range prepareTests {
			err = testKV.Save(path.Join(testLoadRoot, test.key), test.value)
			require.NoError(t, err)
		}

		loadTests := []struct {
			isvalid       bool
			loadKey       string
			expectedValue string

			description string
		}{
			{true, "abc", "123", "load valid key abc"},
			{true, "abcd", "1234", "load valid key abcd"},
			{true, "key_1", "111", "load valid key key_1"},
			{true, "key_2", "222", "load valid key key_2"},
			{true, "key_3", "333", "load valid key key_3"},
			{false, "key_not_exist", "", "load invalid key key_not_exist"},
			{false, "/", "", "load leading slash"},
		}

		for _, test := range loadTests {
			t.Run(test.description, func(t *testing.T) {
				if test.isvalid {
					got, err := testKV.Load(path.Join(testLoadRoot, test.loadKey))
					assert.NoError(t, err)
					assert.Equal(t, test.expectedValue, got)
				} else {
					if test.loadKey == "/" {
						got, err := testKV.Load(test.loadKey)
						assert.Error(t, err)
						assert.Empty(t, got)
						return
					}
					got, err := testKV.Load(path.Join(testLoadRoot, test.loadKey))
					assert.Error(t, err)
					assert.Empty(t, got)
				}
			})
		}

		loadWithPrefixTests := []struct {
			isvalid       bool
			prefix        string
			expectedValue []string

			description string
		}{
			{true, "abc", []string{"123", "1234"}, "load with valid prefix abc"},
			{true, "key_", []string{"111", "222", "333"}, "load with valid prefix key_"},
			{true, "prefix", []string{}, "load with valid but not exist prefix prefix"},
		}

		for _, test := range loadWithPrefixTests {
			t.Run(test.description, func(t *testing.T) {
				gotk, gotv, err := testKV.LoadWithPrefix(path.Join(testLoadRoot, test.prefix))
				assert.NoError(t, err)
				assert.Equal(t, len(test.expectedValue), len(gotk))
				assert.Equal(t, len(test.expectedValue), len(gotv))
				assert.ElementsMatch(t, test.expectedValue, gotv)
			})
		}

		multiLoadTests := []struct {
			isvalid   bool
			multiKeys []string

			expectedValue []string
			description   string
		}{
			{false, []string{"key_1", "key_not_exist"}, []string{"111", ""}, "multiload 1 exist 1 not"},
			{true, []string{"abc", "key_3"}, []string{"123", "333"}, "multiload 2 exist"},
		}

		for _, test := range multiLoadTests {
			t.Run(test.description, func(t *testing.T) {
				for i := range test.multiKeys {
					test.multiKeys[i] = path.Join(testLoadRoot, test.multiKeys[i])
				}
				if test.isvalid {
					got, err := testKV.MultiLoad(test.multiKeys)
					assert.NoError(t, err)
					assert.Equal(t, test.expectedValue, got)
				} else {
					got, err := testKV.MultiLoad(test.multiKeys)
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

		testKV, err := newMinIOKVClient(ctx, testBucket)
		assert.Nil(t, err)
		defer testKV.RemoveWithPrefix(testMultiSaveRoot)

		err = testKV.Save(path.Join(testMultiSaveRoot, "key_1"), "111")
		assert.Nil(t, err)

		kvs := map[string]string{
			path.Join(testMultiSaveRoot, "key_1"): "123",
			path.Join(testMultiSaveRoot, "key_2"): "456",
		}

		err = testKV.MultiSave(kvs)
		assert.Nil(t, err)

		val, err := testKV.Load(path.Join(testMultiSaveRoot, "key_1"))
		assert.Nil(t, err)
		assert.Equal(t, "123", val)
	})

	t.Run("test Remove", func(t *testing.T) {
		testRemoveRoot := path.Join(testMinIOKVRoot, "test_remove")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testKV, err := newMinIOKVClient(ctx, testBucket)
		assert.Nil(t, err)
		defer testKV.RemoveWithPrefix(testRemoveRoot)

		prepareTests := []struct {
			k string
			v string
		}{
			{"key_1", "123"},
			{"key_2", "456"},
			{"mkey_1", "111"},
			{"mkey_2", "222"},
			{"mkey_3", "333"},
		}

		for _, test := range prepareTests {
			k := path.Join(testRemoveRoot, test.k)
			err = testKV.Save(k, test.v)
			require.NoError(t, err)
		}

		removeTests := []struct {
			removeKey         string
			valueBeforeRemove string

			description string
		}{
			{"key_1", "123", "remove key_1"},
			{"key_2", "456", "remove key_2"},
		}

		for _, test := range removeTests {
			t.Run(test.description, func(t *testing.T) {
				k := path.Join(testRemoveRoot, test.removeKey)
				v, err := testKV.Load(k)
				require.NoError(t, err)
				require.Equal(t, test.valueBeforeRemove, v)

				err = testKV.Remove(k)
				assert.NoError(t, err)

				v, err = testKV.Load(k)
				require.Error(t, err)
				require.Empty(t, v)
			})
		}

		multiRemoveTest := []string{
			path.Join(testRemoveRoot, "mkey_1"),
			path.Join(testRemoveRoot, "mkey_2"),
			path.Join(testRemoveRoot, "mkey_3"),
		}

		lv, err := testKV.MultiLoad(multiRemoveTest)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"111", "222", "333"}, lv)

		err = testKV.MultiRemove(multiRemoveTest)
		assert.NoError(t, err)

		for _, k := range multiRemoveTest {
			v, err := testKV.Load(k)
			assert.Error(t, err)
			assert.Empty(t, v)
		}
	})

	t.Run("test LoadPartial", func(t *testing.T) {
		testLoadPartialRoot := path.Join(testMinIOKVRoot, "load_partial")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testKV, err := newMinIOKVClient(ctx, testBucket)
		require.NoError(t, err)
		defer testKV.RemoveWithPrefix(testLoadPartialRoot)

		key := path.Join(testLoadPartialRoot, "TestMinIOKV_LoadPartial_key")
		value := "TestMinIOKV_LoadPartial_value"

		err = testKV.Save(key, value)
		assert.NoError(t, err)

		var start, end int64
		var partial []byte

		start, end = 1, 2
		partial, err = testKV.LoadPartial(key, start, end)
		assert.NoError(t, err)
		assert.ElementsMatch(t, partial, []byte(value[start:end]))

		start, end = 0, int64(len(value))
		partial, err = testKV.LoadPartial(key, start, end)
		assert.NoError(t, err)
		assert.ElementsMatch(t, partial, []byte(value[start:end]))

		// error case
		start, end = 5, 3
		_, err = testKV.LoadPartial(key, start, end)
		assert.Error(t, err)

		start, end = 1, 1
		_, err = testKV.LoadPartial(key, start, end)
		assert.Error(t, err)

		start, end = -1, 1
		_, err = testKV.LoadPartial(key, start, end)
		assert.Error(t, err)

		start, end = 1, -1
		_, err = testKV.LoadPartial(key, start, end)
		assert.Error(t, err)

		err = testKV.Remove(key)
		assert.NoError(t, err)
		start, end = 1, 2
		_, err = testKV.LoadPartial(key, start, end)
		assert.Error(t, err)
	})

	t.Run("test FGetObject", func(t *testing.T) {
		testPath := "/tmp/milvus/data"
		testFGetObjectRoot := path.Join(testMinIOKVRoot, "fget_object")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testKV, err := newMinIOKVClient(ctx, testBucket)
		require.Nil(t, err)
		defer testKV.RemoveWithPrefix(testFGetObjectRoot)

		name1 := path.Join(testFGetObjectRoot, "31280791048324/4325023534/53443534/key_1")
		value1 := "123"
		err = testKV.Save(name1, value1)
		assert.Nil(t, err)

		name2 := path.Join(testFGetObjectRoot, "312895849354/31205934503459/18948129301/key_2")
		value2 := "333"
		err = testKV.Save(name2, value2)
		assert.Nil(t, err)

		err = testKV.FGetObject(name1, testPath)
		assert.Nil(t, err)

		err = testKV.FGetObject(name2, testPath)
		assert.Nil(t, err)

		err = testKV.FGetObject("fail", testPath)
		assert.NotNil(t, err)

		file1, err := os.Open(testPath + name1)
		assert.Nil(t, err)
		content1, err := ioutil.ReadAll(file1)
		assert.Nil(t, err)
		assert.Equal(t, value1, string(content1))
		defer file1.Close()
		defer os.Remove(testPath + name1)

		file2, err := os.Open(testPath + name2)
		assert.Nil(t, err)
		content2, err := ioutil.ReadAll(file2)
		assert.Nil(t, err)
		assert.Equal(t, value2, string(content2))
		defer file1.Close()
		defer os.Remove(testPath + name2)
	})

	t.Run("test FGetObjects", func(t *testing.T) {
		testPath := "/tmp/milvus/data"
		testFGetObjectsRoot := path.Join(testMinIOKVRoot, "fget_objects")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		bucketName := "fantastic-tech-test"
		testKV, err := newMinIOKVClient(ctx, bucketName)
		require.NoError(t, err)
		defer testKV.RemoveWithPrefix(testFGetObjectsRoot)

		name1 := path.Join(testFGetObjectsRoot, "31280791048324/4325023534/53443534/key_1")
		value1 := "123"
		err = testKV.Save(name1, value1)
		assert.Nil(t, err)

		name2 := path.Join(testFGetObjectsRoot, "312895849354/31205934503459/18948129301/key_2")
		value2 := "333"
		err = testKV.Save(name2, value2)
		assert.Nil(t, err)

		err = testKV.FGetObjects([]string{name1, name2}, testPath)
		assert.Nil(t, err)

		err = testKV.FGetObjects([]string{"fail1", "fail2"}, testPath)
		assert.NotNil(t, err)

		file1, err := os.Open(testPath + name1)
		assert.Nil(t, err)
		content1, err := ioutil.ReadAll(file1)
		assert.Nil(t, err)
		assert.Equal(t, value1, string(content1))
		defer file1.Close()
		defer os.Remove(testPath + name1)

		file2, err := os.Open(testPath + name2)
		assert.Nil(t, err)
		content2, err := ioutil.ReadAll(file2)
		assert.Nil(t, err)
		assert.Equal(t, value2, string(content2))
		defer file1.Close()
		defer os.Remove(testPath + name2)
	})

	t.Run("test GetSize", func(t *testing.T) {
		testGetSizeRoot := path.Join(testMinIOKVRoot, "get_size")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testKV, err := newMinIOKVClient(ctx, testBucket)
		require.NoError(t, err)
		defer testKV.RemoveWithPrefix(testGetSizeRoot)

		key := path.Join(testGetSizeRoot, "TestMinIOKV_GetSize_key")
		value := "TestMinIOKV_GetSize_value"

		err = testKV.Save(key, value)
		assert.NoError(t, err)

		size, err := testKV.GetSize(key)
		assert.NoError(t, err)
		assert.Equal(t, size, int64(len(value)))

		key2 := path.Join(testGetSizeRoot, "TestMemoryKV_GetSize_key2")

		size, err = testKV.GetSize(key2)
		assert.Error(t, err)
		assert.Equal(t, int64(0), size)
	})
}
