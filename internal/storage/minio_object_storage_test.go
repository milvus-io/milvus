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
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
)

func TestMinioObjectStorage(t *testing.T) {
	ctx := context.Background()
	config := objectstorage.Config{
		Address:           Params.MinioCfg.Address.GetValue(),
		AccessKeyID:       Params.MinioCfg.AccessKeyID.GetValue(),
		SecretAccessKeyID: Params.MinioCfg.SecretAccessKey.GetValue(),
		RootPath:          Params.MinioCfg.RootPath.GetValue(),

		BucketName:    Params.MinioCfg.BucketName.GetValue(),
		CreateBucket:  true,
		UseIAM:        false,
		CloudProvider: "minio",
	}

	t.Run("test initialize", func(t *testing.T) {
		var err error
		bucketName := config.BucketName
		config.BucketName = ""
		_, err = newMinioObjectStorageWithConfig(ctx, &config)
		assert.Error(t, err)
		config.BucketName = bucketName
		_, err = newMinioObjectStorageWithConfig(ctx, &config)
		assert.Equal(t, err, nil)
	})

	t.Run("test load", func(t *testing.T) {
		testCM, err := newMinioObjectStorageWithConfig(ctx, &config)
		assert.Equal(t, err, nil)
		defer testCM.RemoveBucket(ctx, config.BucketName)

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
			err := testCM.PutObject(ctx, config.BucketName, test.key, bytes.NewReader(test.value), int64(len(test.value)))
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
					got, err := testCM.GetObject(ctx, config.BucketName, test.loadKey, 0, 1024)
					assert.NoError(t, err)
					contentData, err := io.ReadAll(got)
					assert.NoError(t, err)
					assert.Equal(t, len(contentData), len(test.expectedValue))
					assert.Equal(t, test.expectedValue, contentData)
					statSize, err := testCM.StatObject(ctx, config.BucketName, test.loadKey)
					assert.NoError(t, err)
					assert.Equal(t, statSize, int64(len(contentData)))
					_, err = testCM.GetObject(ctx, config.BucketName, test.loadKey, 1, 1023)
					assert.NoError(t, err)
				} else {
					got, err := testCM.GetObject(ctx, config.BucketName, test.loadKey, 0, 1024)
					assert.NoError(t, err)
					_, err = io.ReadAll(got)
					errResponse := minio.ToErrorResponse(err)
					if test.loadKey == "/" {
						assert.Equal(t, errResponse.Code, "XMinioInvalidObjectName")
					} else {
						assert.Equal(t, errResponse.Code, "NoSuchKey")
					}
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
				gotk, _, err := listAllObjectsWithPrefixAtBucket(ctx, testCM, config.BucketName, test.prefix, false)
				assert.NoError(t, err)
				assert.Equal(t, len(test.expectedValue), len(gotk))
				for _, key := range gotk {
					err := testCM.RemoveObject(ctx, config.BucketName, key)
					assert.NoError(t, err)
				}
			})
		}
	})

	t.Run("test list", func(t *testing.T) {
		testCM, err := newMinioObjectStorageWithConfig(ctx, &config)
		assert.Equal(t, err, nil)
		defer testCM.RemoveBucketWithOptions(ctx, config.BucketName, minio.RemoveBucketOptions{
			ForceDelete: true,
		})

		prepareTests := []struct {
			valid bool
			key   string
			value []byte
		}{
			// {true, "abc/", []byte("123")}, will write as abc
			{true, "abc/d", []byte("1234")},
			{true, "abc/d/e", []byte("12345")},
			{true, "abc/e/d", []byte("12354")},
			{true, "key_/1/1", []byte("111")},
			{true, "key_/1/2", []byte("222")},
			{true, "key_/1/2/3", []byte("333")},
			{true, "key_/2/3", []byte("333")},
		}

		for _, test := range prepareTests {
			t.Run(test.key, func(t *testing.T) {
				err := testCM.PutObject(ctx, config.BucketName, test.key, bytes.NewReader(test.value), int64(len(test.value)))
				require.Equal(t, test.valid, err == nil, err)
			})
		}

		insertWithPrefixTests := []struct {
			recursive     bool
			prefix        string
			expectedValue []string
		}{
			{true, "abc/", []string{"abc/d", "abc/e/d"}},
			{true, "key_/", []string{"key_/1/1", "key_/1/2", "key_/2/3"}},
			{false, "abc/", []string{"abc/d", "abc/e/"}},
			{false, "key_/", []string{"key_/1/", "key_/2/"}},
		}

		for _, test := range insertWithPrefixTests {
			t.Run(fmt.Sprintf("prefix: %s, recursive: %t", test.prefix, test.recursive), func(t *testing.T) {
				gotk, _, err := listAllObjectsWithPrefixAtBucket(ctx, testCM, config.BucketName, test.prefix, test.recursive)
				assert.NoError(t, err)
				assert.Equal(t, len(test.expectedValue), len(gotk))
				for _, key := range gotk {
					assert.Contains(t, test.expectedValue, key)
				}
			})
		}
	})

	t.Run("test useIAM", func(t *testing.T) {
		var err error
		config.UseIAM = true
		_, err = newMinioObjectStorageWithConfig(ctx, &config)
		assert.Error(t, err)
		config.UseIAM = false
	})

	t.Run("test ssl", func(t *testing.T) {
		var err error
		config.UseSSL = true
		config.SslCACert = "/tmp/dummy.crt"
		_, err = newMinioObjectStorageWithConfig(ctx, &config)
		assert.Error(t, err)
		config.UseSSL = false
	})

	t.Run("test cloud provider", func(t *testing.T) {
		var err error
		cloudProvider := config.CloudProvider
		config.CloudProvider = "aliyun"
		config.UseIAM = true
		_, err = newMinioObjectStorageWithConfig(ctx, &config)
		assert.Error(t, err)
		config.UseIAM = false
		_, err = newMinioObjectStorageWithConfig(ctx, &config)
		assert.Error(t, err)
		config.CloudProvider = "gcp"
		_, err = newMinioObjectStorageWithConfig(ctx, &config)
		assert.NoError(t, err)
		config.CloudProvider = cloudProvider
	})
}

// listAllObjectsWithPrefixAtBucket is a helper function to list all objects with same @prefix at bucket by using `ListWithPrefix`.
func listAllObjectsWithPrefixAtBucket(ctx context.Context, objectStorage ObjectStorage, bucket string, prefix string, recursive bool) ([]string, []time.Time, error) {
	var dirs []string
	var mods []time.Time
	if err := objectStorage.WalkWithObjects(ctx, bucket, prefix, recursive, func(chunkObjectInfo *ChunkObjectInfo) bool {
		dirs = append(dirs, chunkObjectInfo.FilePath)
		mods = append(mods, chunkObjectInfo.ModifyTime)
		return true
	}); err != nil {
		return nil, nil, err
	}
	return dirs, mods, nil
}
