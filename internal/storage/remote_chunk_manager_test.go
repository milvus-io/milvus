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
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

func TestRemoteChunkManagerImplementsCrossBucketCopier(t *testing.T) {
	var _ CrossBucketCopier = (*RemoteChunkManager)(nil)
}

type objectStorageTarget struct {
	ObjectStorage
}

type bulkObjectStorageTarget struct {
	ObjectStorage
	results []RemoveResult
	calls   int
}

type individualObjectStorageTarget struct {
	ObjectStorage
	remove func(context.Context, string, string) error
}

type prefixBatchObjectStorageTarget struct {
	ObjectStorage
	objects       map[string][]string
	walkErrors    map[string]error
	removeErrors  map[string]error
	omitResults   map[string]struct{}
	removeBatches [][]string
}

func (s *individualObjectStorageTarget) RemoveObject(ctx context.Context, bucketName, objectName string) error {
	return s.remove(ctx, bucketName, objectName)
}

func (s *bulkObjectStorageTarget) RemoveObjects(_ context.Context, _ string, objectNames []string) []RemoveResult {
	s.calls++
	results := make([]RemoveResult, len(objectNames))
	copy(results, s.results)
	return results
}

func (s *prefixBatchObjectStorageTarget) WalkWithObjects(
	_ context.Context,
	_ string,
	prefix string,
	_ bool,
	walkFunc ChunkObjectWalkFunc,
) error {
	for _, object := range s.objects[prefix] {
		if !walkFunc(&ChunkObjectInfo{FilePath: object}) {
			break
		}
	}
	return s.walkErrors[prefix]
}

func (s *prefixBatchObjectStorageTarget) RemoveObjects(
	_ context.Context,
	_ string,
	objectNames []string,
) []RemoveResult {
	s.removeBatches = append(s.removeBatches, append([]string(nil), objectNames...))
	results := make([]RemoveResult, 0, len(objectNames))
	for _, objectName := range objectNames {
		if _, omitted := s.omitResults[objectName]; omitted {
			continue
		}
		results = append(results, RemoveResult{Path: objectName, Err: s.removeErrors[objectName]})
	}
	return results
}

func TestRemoteChunkManagerMultiRemoveWithResult(t *testing.T) {
	removeErr := merr.WrapErrIoTooManyRequests("b", errors.New("throttled"))
	client := &bulkObjectStorageTarget{results: []RemoveResult{
		{Path: "a"},
		{Path: "b", Err: removeErr},
		{Path: "c", Err: merr.WrapErrIoKeyNotFound("c")},
	}}
	mcm := &RemoteChunkManager{client: client, bucketName: "bucket"}

	results := mcm.MultiRemoveWithResult(context.Background(), []string{"a", "b", "c"})
	require.Len(t, results, 3)
	assert.NoError(t, results[0].Err)
	assert.ErrorIs(t, results[1].Err, merr.ErrIoTooManyRequests)
	assert.NoError(t, results[2].Err)
	assert.Equal(t, 1, client.calls)
}

func TestRemoteChunkManagerMultiRemoveWithResultFallbackIsBounded(t *testing.T) {
	params := paramtable.Get()
	oldConcurrency := params.DataCoordCfg.GCRemoveConcurrent.GetValue()
	require.NoError(t, params.Save(params.DataCoordCfg.GCRemoveConcurrent.Key, "2"))
	t.Cleanup(func() {
		require.NoError(t, params.Save(params.DataCoordCfg.GCRemoveConcurrent.Key, oldConcurrency))
	})

	started := make(chan string, 4)
	release := make(chan struct{})
	client := &individualObjectStorageTarget{remove: func(_ context.Context, bucketName, objectName string) error {
		assert.Equal(t, "bucket", bucketName)
		started <- objectName
		<-release
		if objectName == "b" {
			return merr.WrapErrIoTooManyRequests(objectName, errors.New("throttled"))
		}
		return nil
	}}
	mcm := &RemoteChunkManager{client: client, bucketName: "bucket"}

	done := make(chan []RemoveResult, 1)
	go func() {
		done <- mcm.MultiRemoveWithResult(context.Background(), []string{"a", "b", "c", "d"})
	}()

	assert.NotEmpty(t, <-started)
	assert.NotEmpty(t, <-started)
	select {
	case objectName := <-started:
		t.Fatalf("fallback exceeded configured concurrency: third object %s started before release", strconv.Quote(objectName))
	case <-time.After(20 * time.Millisecond):
	}
	close(release)

	results := <-done
	require.Len(t, results, 4)
	assert.Equal(t, []string{"a", "b", "c", "d"}, []string{results[0].Path, results[1].Path, results[2].Path, results[3].Path})
	assert.NoError(t, results[0].Err)
	assert.ErrorIs(t, results[1].Err, merr.ErrIoTooManyRequests)
	assert.NoError(t, results[2].Err)
	assert.NoError(t, results[3].Err)
}

func TestRemoteChunkManagerMultiRemoveWithPrefix(t *testing.T) {
	params := paramtable.Get()
	oldBatchSize := params.DataCoordCfg.GCIndexFileBatchSize.GetValue()
	oldConcurrency := params.DataCoordCfg.GCRemoveConcurrent.GetValue()
	require.NoError(t, params.Save(params.DataCoordCfg.GCIndexFileBatchSize.Key, "3"))
	require.NoError(t, params.Save(params.DataCoordCfg.GCRemoveConcurrent.Key, "2"))
	t.Cleanup(func() {
		require.NoError(t, params.Save(params.DataCoordCfg.GCIndexFileBatchSize.Key, oldBatchSize))
		require.NoError(t, params.Save(params.DataCoordCfg.GCRemoveConcurrent.Key, oldConcurrency))
	})

	t.Run("coalesces exact keys across prefixes", func(t *testing.T) {
		client := &prefixBatchObjectStorageTarget{objects: map[string][]string{
			"prefix-a": {"a/1"},
			"prefix-b": {"b/1", "b/2"},
			"prefix-c": {"c/1"},
		}}
		mcm := &RemoteChunkManager{client: client, bucketName: "bucket"}

		results := mcm.MultiRemoveWithPrefix(context.Background(), []string{"prefix-a", "prefix-b", "prefix-c"})

		require.Len(t, results, 3)
		assert.Equal(t, RemovePrefixResult{Prefix: "prefix-a", Listed: 1, Removed: 1}, results[0])
		assert.Equal(t, RemovePrefixResult{Prefix: "prefix-b", Listed: 2, Removed: 2}, results[1])
		assert.Equal(t, RemovePrefixResult{Prefix: "prefix-c", Listed: 1, Removed: 1}, results[2])
		require.Len(t, client.removeBatches, 2)
		assert.Len(t, client.removeBatches[0], 3)
		assert.Len(t, client.removeBatches[1], 1)
	})

	t.Run("fails only prefixes with incomplete work", func(t *testing.T) {
		listErr := merr.WrapErrIoTooManyRequests("prefix-b", errors.New("list throttled"))
		deleteErr := merr.WrapErrIoPermissionDenied("c/1", errors.New("delete denied"))
		client := &prefixBatchObjectStorageTarget{
			objects: map[string][]string{
				"prefix-a": {"a/1"},
				"prefix-b": {"b/1"},
				"prefix-c": {"c/1"},
			},
			walkErrors:   map[string]error{"prefix-b": listErr},
			removeErrors: map[string]error{"c/1": deleteErr},
		}
		mcm := &RemoteChunkManager{client: client, bucketName: "bucket"}

		results := mcm.MultiRemoveWithPrefix(context.Background(), []string{"prefix-a", "prefix-b", "prefix-c"})

		require.Len(t, results, 3)
		assert.NoError(t, results[0].Err)
		assert.ErrorIs(t, results[1].Err, merr.ErrIoTooManyRequests)
		assert.ErrorIs(t, results[2].Err, merr.ErrIoPermissionDenied)
		assert.Equal(t, 1, results[0].Removed)
		assert.Equal(t, 1, results[1].Removed, "partial deletion remains observable while list failure keeps metadata")
		assert.Zero(t, results[2].Removed)
	})

	t.Run("missing delete result fails the owning prefix closed", func(t *testing.T) {
		client := &prefixBatchObjectStorageTarget{
			objects:     map[string][]string{"prefix-a": {"a/1"}, "prefix-b": {"b/1"}},
			omitResults: map[string]struct{}{"b/1": {}},
		}
		mcm := &RemoteChunkManager{client: client, bucketName: "bucket"}

		results := mcm.MultiRemoveWithPrefix(context.Background(), []string{"prefix-a", "prefix-b"})

		require.Len(t, results, 2)
		assert.NoError(t, results[0].Err)
		assert.ErrorIs(t, results[1].Err, merr.ErrIoFailed)
	})

	t.Run("empty prefix is rejected", func(t *testing.T) {
		client := &prefixBatchObjectStorageTarget{}
		mcm := &RemoteChunkManager{client: client, bucketName: "bucket"}

		results := mcm.MultiRemoveWithPrefix(context.Background(), []string{""})

		require.Len(t, results, 1)
		assert.ErrorIs(t, results[0].Err, merr.ErrIoFailed)
		assert.Empty(t, client.removeBatches)
	})
}

func TestRemoteChunkManagerCopyCrossBucket(t *testing.T) {
	ctx := context.Background()
	copyErr := errors.New("copy failed")

	t.Run("copy object across buckets successfully", func(t *testing.T) {
		client := &objectStorageTarget{}
		called := false
		mockCopy := mockey.Mock((*objectStorageTarget).CopyObjectCrossBucket).To(
			func(_ context.Context, srcBucket, srcObject, dstBucket, dstObject string) error {
				called = true
				assert.Equal(t, "src-bucket", srcBucket)
				assert.Equal(t, "src-object", srcObject)
				assert.Equal(t, "dst-bucket", dstBucket)
				assert.Equal(t, "dst-object", dstObject)
				return nil
			},
		).Build()
		defer mockCopy.UnPatch()
		mcm := &RemoteChunkManager{client: client}

		err := mcm.CopyCrossBucket(ctx, "src-bucket", "src-object", "dst-bucket", "dst-object")
		require.NoError(t, err)
		assert.True(t, called)
	})

	t.Run("return copy error", func(t *testing.T) {
		client := &objectStorageTarget{}
		mockCopy := mockey.Mock((*objectStorageTarget).CopyObjectCrossBucket).Return(copyErr).Build()
		defer mockCopy.UnPatch()
		mcm := &RemoteChunkManager{client: client}

		err := mcm.CopyCrossBucket(ctx, "src-bucket", "src-object", "dst-bucket", "dst-object")
		require.ErrorIs(t, err, copyErr)
	})
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

	t.Run("test Copy", func(t *testing.T) {
		testCopyRoot := path.Join(testMinIOKVRoot, "test_copy")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testCM, err := newMinioChunkManager(ctx, testBucket, testCopyRoot)
		require.NoError(t, err)
		defer testCM.RemoveWithPrefix(ctx, testCopyRoot)

		// Test successful copy
		t.Run("copy file successfully", func(t *testing.T) {
			srcKey := path.Join(testCopyRoot, "src", "file1")
			dstKey := path.Join(testCopyRoot, "dst", "file1")
			value := []byte("test data for copy")

			// Write source file
			err := testCM.Write(ctx, srcKey, value)
			require.NoError(t, err)

			// Copy file
			err = testCM.Copy(ctx, srcKey, dstKey)
			assert.NoError(t, err)

			// Verify destination file exists and has correct content
			dstData, err := testCM.Read(ctx, dstKey)
			assert.NoError(t, err)
			assert.Equal(t, value, dstData)

			// Verify source file still exists
			srcData, err := testCM.Read(ctx, srcKey)
			assert.NoError(t, err)
			assert.Equal(t, value, srcData)
		})

		// Test copy with non-existent source
		t.Run("copy non-existent source file", func(t *testing.T) {
			srcKey := path.Join(testCopyRoot, "not_exist", "file")
			dstKey := path.Join(testCopyRoot, "dst", "file")

			err := testCM.Copy(ctx, srcKey, dstKey)
			assert.Error(t, err)
		})

		// Test copy overwrite existing file
		t.Run("copy and overwrite existing file", func(t *testing.T) {
			srcKey := path.Join(testCopyRoot, "src3", "file3")
			dstKey := path.Join(testCopyRoot, "dst3", "file3")
			srcValue := []byte("new content")
			oldValue := []byte("old content")

			// Create destination with old content
			err := testCM.Write(ctx, dstKey, oldValue)
			require.NoError(t, err)

			// Create source with new content
			err = testCM.Write(ctx, srcKey, srcValue)
			require.NoError(t, err)

			// Copy (should overwrite)
			err = testCM.Copy(ctx, srcKey, dstKey)
			assert.NoError(t, err)

			// Verify destination has new content
			dstData, err := testCM.Read(ctx, dstKey)
			assert.NoError(t, err)
			assert.Equal(t, srcValue, dstData)
		})

		// Test copy large file
		t.Run("copy large file", func(t *testing.T) {
			srcKey := path.Join(testCopyRoot, "src4", "large_file")
			dstKey := path.Join(testCopyRoot, "dst4", "large_file")

			// Create 5MB file
			largeData := make([]byte, 5*1024*1024)
			for i := range largeData {
				largeData[i] = byte(i % 256)
			}

			err := testCM.Write(ctx, srcKey, largeData)
			require.NoError(t, err)

			// Copy large file
			err = testCM.Copy(ctx, srcKey, dstKey)
			assert.NoError(t, err)

			// Verify content
			dstData, err := testCM.Read(ctx, dstKey)
			assert.NoError(t, err)
			assert.Equal(t, largeData, dstData)
		})

		// Test copy empty file
		t.Run("copy empty file", func(t *testing.T) {
			srcKey := path.Join(testCopyRoot, "src5", "empty_file")
			dstKey := path.Join(testCopyRoot, "dst5", "empty_file")
			emptyData := []byte{}

			// Write empty file
			err := testCM.Write(ctx, srcKey, emptyData)
			require.NoError(t, err)

			// Copy empty file
			err = testCM.Copy(ctx, srcKey, dstKey)
			assert.NoError(t, err)

			// Verify destination exists and has size 0
			size, err := testCM.Size(ctx, dstKey)
			assert.NoError(t, err)
			assert.Equal(t, int64(0), size)
		})

		// Test copy with nested path
		t.Run("copy file with nested path", func(t *testing.T) {
			srcKey := path.Join(testCopyRoot, "src6", "file6")
			dstKey := path.Join(testCopyRoot, "dst6", "nested", "deep", "path", "file6")
			value := []byte("test data for nested path copy")

			// Write source file
			err := testCM.Write(ctx, srcKey, value)
			require.NoError(t, err)

			// Copy to nested path
			err = testCM.Copy(ctx, srcKey, dstKey)
			assert.NoError(t, err)

			// Verify destination file exists and has correct content
			dstData, err := testCM.Read(ctx, dstKey)
			assert.NoError(t, err)
			assert.Equal(t, value, dstData)
		})
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
		require.NoError(t, err)
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
		require.NoError(t, err)
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

	t.Run("test Copy", func(t *testing.T) {
		testCopyRoot := path.Join(testMinIOKVRoot, "test_copy_azure")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testCM, err := newAzureChunkManager(ctx, testBucket, testCopyRoot)
		require.NoError(t, err)
		defer testCM.RemoveWithPrefix(ctx, testCopyRoot)

		// Test successful copy
		t.Run("copy file successfully", func(t *testing.T) {
			srcKey := path.Join(testCopyRoot, "src", "file1")
			dstKey := path.Join(testCopyRoot, "dst", "file1")
			value := []byte("test data for azure copy")

			// Write source file
			err := testCM.Write(ctx, srcKey, value)
			require.NoError(t, err)

			// Copy file
			err = testCM.Copy(ctx, srcKey, dstKey)
			assert.NoError(t, err)

			// Verify destination file exists and has correct content
			dstData, err := testCM.Read(ctx, dstKey)
			assert.NoError(t, err)
			assert.Equal(t, value, dstData)

			// Verify source file still exists
			srcData, err := testCM.Read(ctx, srcKey)
			assert.NoError(t, err)
			assert.Equal(t, value, srcData)
		})

		// Test copy with non-existent source
		t.Run("copy non-existent source file", func(t *testing.T) {
			srcKey := path.Join(testCopyRoot, "not_exist", "file")
			dstKey := path.Join(testCopyRoot, "dst", "file")

			err := testCM.Copy(ctx, srcKey, dstKey)
			assert.Error(t, err)
		})

		// Test copy overwrite existing file
		t.Run("copy and overwrite existing file", func(t *testing.T) {
			srcKey := path.Join(testCopyRoot, "src3", "file3")
			dstKey := path.Join(testCopyRoot, "dst3", "file3")
			srcValue := []byte("new azure content")
			oldValue := []byte("old azure content")

			// Create destination with old content
			err := testCM.Write(ctx, dstKey, oldValue)
			require.NoError(t, err)

			// Create source with new content
			err = testCM.Write(ctx, srcKey, srcValue)
			require.NoError(t, err)

			// Copy (should overwrite)
			err = testCM.Copy(ctx, srcKey, dstKey)
			assert.NoError(t, err)

			// Verify destination has new content
			dstData, err := testCM.Read(ctx, dstKey)
			assert.NoError(t, err)
			assert.Equal(t, srcValue, dstData)
		})

		// Test copy large file
		t.Run("copy large file", func(t *testing.T) {
			srcKey := path.Join(testCopyRoot, "src4", "large_file")
			dstKey := path.Join(testCopyRoot, "dst4", "large_file")

			// Create 5MB file
			largeData := make([]byte, 5*1024*1024)
			for i := range largeData {
				largeData[i] = byte(i % 256)
			}

			err := testCM.Write(ctx, srcKey, largeData)
			require.NoError(t, err)

			// Copy large file
			err = testCM.Copy(ctx, srcKey, dstKey)
			assert.NoError(t, err)

			// Verify content
			dstData, err := testCM.Read(ctx, dstKey)
			assert.NoError(t, err)
			assert.Equal(t, largeData, dstData)
		})

		// Test copy empty file
		t.Run("copy empty file", func(t *testing.T) {
			srcKey := path.Join(testCopyRoot, "src5", "empty_file")
			dstKey := path.Join(testCopyRoot, "dst5", "empty_file")
			emptyData := []byte{}

			// Write empty file
			err := testCM.Write(ctx, srcKey, emptyData)
			require.NoError(t, err)

			// Copy empty file
			err = testCM.Copy(ctx, srcKey, dstKey)
			assert.NoError(t, err)

			// Verify destination exists and has size 0
			size, err := testCM.Size(ctx, dstKey)
			assert.NoError(t, err)
			assert.Equal(t, int64(0), size)
		})

		// Test copy with nested path
		t.Run("copy file with nested path", func(t *testing.T) {
			srcKey := path.Join(testCopyRoot, "src6", "file6")
			dstKey := path.Join(testCopyRoot, "dst6", "nested", "deep", "path", "file6")
			value := []byte("test data for nested path copy")

			// Write source file
			err := testCM.Write(ctx, srcKey, value)
			require.NoError(t, err)

			// Copy to nested path
			err = testCM.Copy(ctx, srcKey, dstKey)
			assert.NoError(t, err)

			// Verify destination file exists and has correct content
			dstData, err := testCM.Read(ctx, dstKey)
			assert.NoError(t, err)
			assert.Equal(t, value, dstData)
		})
	})
}

func TestRemoteChunkManagerCanceledMetrics(t *testing.T) {
	mcm := &RemoteChunkManager{}

	t.Run("get", func(t *testing.T) {
		cancelCounter := metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataGetLabel, metrics.CancelLabel)
		failCounter := metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataGetLabel, metrics.FailLabel)
		cancelBefore := testutil.ToFloat64(cancelCounter)
		failBefore := testutil.ToFloat64(failCounter)

		mcm.client = &canceledObjectStorage{getErr: ToMilvusIoError("test/path", context.Canceled)}
		_, err := mcm.getObject(context.Background(), "test-bucket", "test/path", 0, 0)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, cancelBefore+1, testutil.ToFloat64(cancelCounter))
		assert.Equal(t, failBefore, testutil.ToFloat64(failCounter))
	})

	t.Run("put", func(t *testing.T) {
		cancelCounter := metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataPutLabel, metrics.CancelLabel)
		failCounter := metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataPutLabel, metrics.FailLabel)
		cancelBefore := testutil.ToFloat64(cancelCounter)
		failBefore := testutil.ToFloat64(failCounter)

		mcm.client = &canceledObjectStorage{putErr: ToMilvusIoError("test/path", context.Canceled)}
		err := mcm.putObject(context.Background(), "test-bucket", "test/path", nil, 0)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, cancelBefore+1, testutil.ToFloat64(cancelCounter))
		assert.Equal(t, failBefore, testutil.ToFloat64(failCounter))
	})
}

type canceledObjectStorage struct {
	getErr error
	putErr error
}

func (s *canceledObjectStorage) GetObject(context.Context, string, string, int64, int64) (FileReader, error) {
	return nil, s.getErr
}

func (s *canceledObjectStorage) PutObject(context.Context, string, string, io.Reader, int64) error {
	return s.putErr
}

func (*canceledObjectStorage) StatObject(context.Context, string, string) (int64, error) {
	return 0, nil
}

func (*canceledObjectStorage) WalkWithObjects(context.Context, string, string, bool, ChunkObjectWalkFunc) error {
	return nil
}

func (*canceledObjectStorage) RemoveObject(context.Context, string, string) error {
	return nil
}

func (*canceledObjectStorage) CopyObjectCrossBucket(context.Context, string, string, string, string) error {
	return nil
}

func TestToMilvusIoError(t *testing.T) {
	fileName := "test_file"

	t.Run("nil error", func(t *testing.T) {
		err := ToMilvusIoError(fileName, nil)
		assert.NoError(t, err)
	})

	t.Run("io.ErrUnexpectedEOF", func(t *testing.T) {
		err := ToMilvusIoError(fileName, io.ErrUnexpectedEOF)
		assert.ErrorIs(t, err, merr.ErrIoUnexpectEOF)
	})

	t.Run("syscall.ECONNRESET", func(t *testing.T) {
		err := ToMilvusIoError(fileName, syscall.ECONNRESET)
		assert.ErrorIs(t, err, merr.ErrIoTooManyRequests)
	})

	t.Run("generic error", func(t *testing.T) {
		err := ToMilvusIoError(fileName, errors.New("some error"))
		assert.ErrorIs(t, err, merr.ErrIoFailed)
	})

	for _, test := range []struct {
		name string
		err  error
		code int32
	}{
		{name: "context canceled", err: context.Canceled, code: merr.CanceledCode},
		{name: "context deadline exceeded", err: context.DeadlineExceeded, code: merr.TimeoutCode},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ToMilvusIoError(fileName, test.err)
			assert.ErrorIs(t, err, test.err)
			assert.Equal(t, test.code, merr.Code(err))
		})

		t.Run("wrapped "+test.name, func(t *testing.T) {
			cause := merr.Wrap(test.err, "object storage request failed")
			err := ToMilvusIoError(fileName, cause)
			assert.ErrorIs(t, err, test.err)
			assert.Equal(t, test.code, merr.Code(err))
		})
	}

	t.Run("minio NoSuchKey", func(t *testing.T) {
		minioErr := minio.ErrorResponse{Code: "NoSuchKey"}
		err := ToMilvusIoError(fileName, minioErr)
		assert.ErrorIs(t, err, merr.ErrIoKeyNotFound)
	})

	t.Run("minio SlowDown", func(t *testing.T) {
		minioErr := minio.ErrorResponse{Code: "SlowDown"}
		err := ToMilvusIoError(fileName, minioErr)
		assert.ErrorIs(t, err, merr.ErrIoTooManyRequests)
	})

	t.Run("minio TooManyRequestsException", func(t *testing.T) {
		minioErr := minio.ErrorResponse{Code: "TooManyRequestsException"}
		err := ToMilvusIoError(fileName, minioErr)
		assert.ErrorIs(t, err, merr.ErrIoTooManyRequests)
	})

	t.Run("minio other error", func(t *testing.T) {
		minioErr := minio.ErrorResponse{Code: "AccessDenied"}
		err := ToMilvusIoError(fileName, minioErr)
		assert.ErrorIs(t, err, merr.ErrIoPermissionDenied)
	})

	t.Run("azure BlobNotFound", func(t *testing.T) {
		azureErr := &azcore.ResponseError{ErrorCode: string(bloberror.BlobNotFound)}
		err := ToMilvusIoError(fileName, azureErr)
		assert.ErrorIs(t, err, merr.ErrIoKeyNotFound)
	})

	t.Run("azure ServerBusy", func(t *testing.T) {
		azureErr := &azcore.ResponseError{ErrorCode: string(bloberror.ServerBusy)}
		err := ToMilvusIoError(fileName, azureErr)
		assert.ErrorIs(t, err, merr.ErrIoTooManyRequests)
	})

	t.Run("azure other error", func(t *testing.T) {
		azureErr := &azcore.ResponseError{ErrorCode: "SomeOtherError"}
		err := ToMilvusIoError(fileName, azureErr)
		assert.ErrorIs(t, err, merr.ErrIoFailed)
	})

	t.Run("googleapi NotFound", func(t *testing.T) {
		googleErr := &googleapi.Error{Code: http.StatusNotFound}
		err := ToMilvusIoError(fileName, googleErr)
		assert.ErrorIs(t, err, merr.ErrIoKeyNotFound)
	})

	t.Run("googleapi TooManyRequests", func(t *testing.T) {
		googleErr := &googleapi.Error{Code: http.StatusTooManyRequests}
		err := ToMilvusIoError(fileName, googleErr)
		assert.ErrorIs(t, err, merr.ErrIoTooManyRequests)
	})

	t.Run("googleapi permission denied", func(t *testing.T) {
		googleErr := &googleapi.Error{Code: http.StatusForbidden}
		err := ToMilvusIoError(fileName, googleErr)
		assert.ErrorIs(t, err, merr.ErrIoPermissionDenied)
	})

	// Test cases for passing merr.ErrIo* errors directly
	// These should be returned as-is without re-wrapping
	t.Run("direct merr.ErrIoKeyNotFound", func(t *testing.T) {
		err := ToMilvusIoError(fileName, merr.ErrIoKeyNotFound)
		assert.ErrorIs(t, err, merr.ErrIoKeyNotFound)
		// assert.Same() removed: merr errors are struct values, not pointers
		// ErrorIs() is sufficient to verify no unwanted wrapping occurred
	})

	t.Run("direct merr.ErrIoPermissionDenied", func(t *testing.T) {
		err := ToMilvusIoError(fileName, merr.ErrIoPermissionDenied)
		assert.ErrorIs(t, err, merr.ErrIoPermissionDenied)
	})

	t.Run("direct merr.ErrIoBucketNotFound", func(t *testing.T) {
		err := ToMilvusIoError(fileName, merr.ErrIoBucketNotFound)
		assert.ErrorIs(t, err, merr.ErrIoBucketNotFound)
	})

	t.Run("direct merr.ErrIoInvalidArgument", func(t *testing.T) {
		err := ToMilvusIoError(fileName, merr.ErrIoInvalidArgument)
		assert.ErrorIs(t, err, merr.ErrIoInvalidArgument)
	})

	t.Run("wrapped merr.ErrIoKeyNotFound", func(t *testing.T) {
		wrappedErr := fmt.Errorf("failed to read: %w", merr.ErrIoKeyNotFound)
		err := ToMilvusIoError(fileName, wrappedErr)
		assert.ErrorIs(t, err, merr.ErrIoKeyNotFound)
		// Verify the error is returned without additional wrapping
		assert.Equal(t, wrappedErr, err, "should return wrapped error as-is")
	})
}

func tlsVersionName(v uint16) string {
	switch v {
	case tls.VersionTLS10:
		return "TLS 1.0"
	case tls.VersionTLS11:
		return "TLS 1.1"
	case tls.VersionTLS12:
		return "TLS 1.2"
	case tls.VersionTLS13:
		return "TLS 1.3"
	default:
		return "unknown"
	}
}

// TestRemoteChunkManagerTLSVersion tests TLS version configuration via NewRemoteChunkManager.
// Works for any cloud provider. Auth: ACCESS_KEY+SECRET_KEY, or USE_IAM=true.
//
// ACCESS_KEY+SECRET_KEY require:
//   - ADDRESS, BUCKET_NAME, CLOUD_PROVIDER, ACCESS_KEY, SECRET_KEY.
//
// USE_IAM require:
//   - ADDRESS, BUCKET_NAME, CLOUD_PROVIDER, USE_IAM=true.
//
// CLOUD_PROVIDER: aws, gcp (S3 compatibility mode), gcpnative, or azure.
func TestRemoteChunkManagerTLSVersion(t *testing.T) {
	address := os.Getenv("ADDRESS")
	accessKey := os.Getenv("ACCESS_KEY")
	secretKey := os.Getenv("SECRET_KEY")
	bucketName := os.Getenv("BUCKET_NAME")
	cloudProvider := os.Getenv("CLOUD_PROVIDER")
	useIAM := os.Getenv("USE_IAM") == "true"

	if bucketName == "" || cloudProvider == "" {
		t.Skip("Skipping: set BUCKET_NAME, CLOUD_PROVIDER env vars to run this test")
	}
	hasAKSK := accessKey != "" && secretKey != ""
	if !hasAKSK && !useIAM {
		t.Skip("Skipping: set ACCESS_KEY+SECRET_KEY or USE_IAM=true to run this test")
	}

	// Determine the TLS host for probing
	tlsHost := address
	if cloudProvider == "azure" {
		tlsHost = accessKey + ".blob." + address
	}
	if cloudProvider == "gcpnative" && tlsHost == "" {
		tlsHost = "storage.googleapis.com"
	}

	newConfig := func(tlsMinVersion string) *objectstorage.Config {
		return &objectstorage.Config{
			Address:           address,
			AccessKeyID:       accessKey,
			SecretAccessKeyID: secretKey,
			BucketName:        bucketName,
			UseSSL:            true,
			SslTLSMinVersion:  tlsMinVersion,
			CloudProvider:     cloudProvider,
			UseIAM:            useIAM,
			CreateBucket:      true,
		}
	}

	ctx := context.Background()

	t.Run("check_server_tls_support", func(t *testing.T) {
		if tlsHost == "" {
			t.Skip("Skipping: ADDRESS not set, cannot probe TLS")
		}
		for _, ver := range []struct {
			name string
			ver  uint16
		}{
			{"TLS 1.2", tls.VersionTLS12},
			{"TLS 1.3", tls.VersionTLS13},
		} {
			conn, err := tls.Dial("tcp", tlsHost+":443", &tls.Config{
				MinVersion: ver.ver,
				MaxVersion: ver.ver,
			})
			if err != nil {
				t.Logf("%s -> %s: NOT supported (%v)", tlsHost, ver.name, err)
			} else {
				state := conn.ConnectionState()
				t.Logf("%s -> %s: supported (negotiated: %s)", tlsHost, ver.name, tlsVersionName(state.Version))
				conn.Close()
			}
		}
	})

	t.Run("tls12", func(t *testing.T) {
		cm, err := NewRemoteChunkManager(ctx, newConfig("1.2"))
		require.NoError(t, err, "NewRemoteChunkManager with TLS 1.2 should succeed")
		require.NotNil(t, cm)

		// Write and read back to verify the connection works end-to-end
		key := path.Join("tls-test", "tls12-test-key")
		value := []byte("tls12-test-value")
		err = cm.Write(ctx, key, value)
		require.NoError(t, err, "Write should succeed over TLS 1.2")

		got, err := cm.Read(ctx, key)
		require.NoError(t, err, "Read should succeed over TLS 1.2")
		assert.Equal(t, value, got)

		_ = cm.Remove(ctx, key)
		t.Logf("NewRemoteChunkManager(SslTLSMinVersion=1.2, CloudProvider=%s): Write/Read OK", cloudProvider)
	})

	t.Run("tls13", func(t *testing.T) {
		if tlsHost != "" {
			conn, err := tls.Dial("tcp", tlsHost+":443", &tls.Config{
				MinVersion: tls.VersionTLS13,
			})
			if err != nil {
				t.Skipf("Skipping: %s does not support TLS 1.3 (%v)", tlsHost, err)
			}
			conn.Close()
		}

		cm, err := NewRemoteChunkManager(ctx, newConfig("1.3"))
		require.NoError(t, err, "NewRemoteChunkManager with TLS 1.3 should succeed")
		require.NotNil(t, cm)

		key := path.Join("tls-test", "tls13-test-key")
		value := []byte("tls13-test-value")
		err = cm.Write(ctx, key, value)
		require.NoError(t, err, "Write should succeed over TLS 1.3")

		got, err := cm.Read(ctx, key)
		require.NoError(t, err, "Read should succeed over TLS 1.3")
		assert.Equal(t, value, got)

		_ = cm.Remove(ctx, key)
		t.Logf("NewRemoteChunkManager(SslTLSMinVersion=1.3, CloudProvider=%s): Write/Read OK", cloudProvider)
	})

	t.Run("no_tls_version_set", func(t *testing.T) {
		cm, err := NewRemoteChunkManager(ctx, newConfig(""))
		require.NoError(t, err, "NewRemoteChunkManager without TLS version should succeed")
		require.NotNil(t, cm)
		t.Logf("NewRemoteChunkManager(SslTLSMinVersion=<empty>, CloudProvider=%s): OK (default)", cloudProvider)
	})
}
