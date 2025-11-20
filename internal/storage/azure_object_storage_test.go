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
	"os"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
)

func TestAzureObjectStorage(t *testing.T) {
	ctx := context.Background()
	bucketName := Params.MinioCfg.BucketName.GetValue()
	config := objectstorage.Config{
		BucketName:    bucketName,
		CreateBucket:  true,
		UseIAM:        false,
		CloudProvider: "azure",
	}

	t.Run("test initialize", func(t *testing.T) {
		var err error
		config.BucketName = ""
		_, err = newAzureObjectStorageWithConfig(ctx, &config)
		assert.Error(t, err)
		config.BucketName = bucketName
		_, err = newAzureObjectStorageWithConfig(ctx, &config)
		assert.Equal(t, err, nil)
	})

	t.Run("test load", func(t *testing.T) {
		testCM, err := newAzureObjectStorageWithConfig(ctx, &config)
		assert.Equal(t, err, nil)
		defer testCM.DeleteContainer(ctx, config.BucketName, &azblob.DeleteContainerOptions{})

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
					assert.NotEmpty(t, got)
					_, err = io.ReadAll(got)
					assert.Error(t, err)
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
		testCM, err := newAzureObjectStorageWithConfig(ctx, &config)
		assert.Equal(t, err, nil)
		defer testCM.DeleteContainer(ctx, config.BucketName, &azblob.DeleteContainerOptions{})

		prepareTests := []struct {
			valid bool
			key   string
			value []byte
		}{
			{false, "abc/", []byte("123")},
			{true, "abc/d", []byte("1234")},
			{false, "abc/d/e", []byte("12345")},
			{true, "abc/e/d", []byte("12354")},
			{true, "key_/1/1", []byte("111")},
			{true, "key_/1/2", []byte("222")},
			{false, "key_/1/2/3", []byte("333")},
			{true, "key_/2/3", []byte("333")},
		}

		for _, test := range prepareTests {
			err := testCM.PutObject(ctx, config.BucketName, test.key, bytes.NewReader(test.value), int64(len(test.value)))
			require.Nil(t, err)
			if !test.valid {
				err := testCM.RemoveObject(ctx, config.BucketName, test.key)
				require.Nil(t, err)
			}
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
		_, err = newAzureObjectStorageWithConfig(ctx, &config)
		assert.Error(t, err)
		os.Setenv("AZURE_CLIENT_ID", "00000000-0000-0000-0000-00000000000")
		os.Setenv("AZURE_TENANT_ID", "00000000-0000-0000-0000-00000000000")
		os.Setenv("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/tokens/azure-identity-token")
		_, err = newAzureObjectStorageWithConfig(ctx, &config)
		assert.Error(t, err)
		config.UseIAM = false
	})

	t.Run("test key secret", func(t *testing.T) {
		var err error
		connectionString := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
		os.Setenv("AZURE_STORAGE_CONNECTION_STRING", "")
		config.AccessKeyID = "devstoreaccount1"
		config.SecretAccessKeyID = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
		config.Address = "core.windows.net"
		_, err = newAzureObjectStorageWithConfig(ctx, &config)
		assert.Error(t, err)
		os.Setenv("AZURE_STORAGE_CONNECTION_STRING", connectionString)
	})

	t.Run("test CopyObject", func(t *testing.T) {
		testCM, err := newAzureObjectStorageWithConfig(ctx, &config)
		assert.NoError(t, err)
		defer testCM.DeleteContainer(ctx, config.BucketName, &azblob.DeleteContainerOptions{})

		// Test successful copy
		t.Run("copy object successfully", func(t *testing.T) {
			srcKey := "copy_test/src/file1"
			dstKey := "copy_test/dst/file1"
			value := []byte("test data for copy")

			// Put source object
			err := testCM.PutObject(ctx, config.BucketName, srcKey, bytes.NewReader(value), int64(len(value)))
			require.NoError(t, err)

			// Copy object
			err = testCM.CopyObject(ctx, config.BucketName, srcKey, dstKey)
			assert.NoError(t, err)

			// Verify destination object exists and has correct content
			dstReader, err := testCM.GetObject(ctx, config.BucketName, dstKey, 0, 1024)
			assert.NoError(t, err)
			dstData, err := io.ReadAll(dstReader)
			assert.NoError(t, err)
			assert.Equal(t, value, dstData)

			// Verify source object still exists
			srcReader, err := testCM.GetObject(ctx, config.BucketName, srcKey, 0, 1024)
			assert.NoError(t, err)
			srcData, err := io.ReadAll(srcReader)
			assert.NoError(t, err)
			assert.Equal(t, value, srcData)

			// Clean up
			err = testCM.RemoveObject(ctx, config.BucketName, srcKey)
			assert.NoError(t, err)
			err = testCM.RemoveObject(ctx, config.BucketName, dstKey)
			assert.NoError(t, err)
		})

		// Test copy non-existent source
		t.Run("copy non-existent source object", func(t *testing.T) {
			srcKey := "copy_test/not_exist/file"
			dstKey := "copy_test/dst/file"

			err := testCM.CopyObject(ctx, config.BucketName, srcKey, dstKey)
			assert.Error(t, err)
		})

		// Test copy overwrite existing object
		t.Run("copy and overwrite existing object", func(t *testing.T) {
			srcKey := "copy_test/src3/file3"
			dstKey := "copy_test/dst3/file3"
			srcValue := []byte("new content")
			oldValue := []byte("old content")

			// Put destination with old content
			err := testCM.PutObject(ctx, config.BucketName, dstKey, bytes.NewReader(oldValue), int64(len(oldValue)))
			require.NoError(t, err)

			// Put source with new content
			err = testCM.PutObject(ctx, config.BucketName, srcKey, bytes.NewReader(srcValue), int64(len(srcValue)))
			require.NoError(t, err)

			// Copy (should overwrite)
			err = testCM.CopyObject(ctx, config.BucketName, srcKey, dstKey)
			assert.NoError(t, err)

			// Verify destination has new content
			dstReader, err := testCM.GetObject(ctx, config.BucketName, dstKey, 0, 1024)
			assert.NoError(t, err)
			dstData, err := io.ReadAll(dstReader)
			assert.NoError(t, err)
			assert.Equal(t, srcValue, dstData)

			// Clean up
			err = testCM.RemoveObject(ctx, config.BucketName, srcKey)
			assert.NoError(t, err)
			err = testCM.RemoveObject(ctx, config.BucketName, dstKey)
			assert.NoError(t, err)
		})

		// Test copy large object
		t.Run("copy large object", func(t *testing.T) {
			srcKey := "copy_test/src4/large_file"
			dstKey := "copy_test/dst4/large_file"

			// Create 5MB data
			largeData := make([]byte, 5*1024*1024)
			for i := range largeData {
				largeData[i] = byte(i % 256)
			}

			err := testCM.PutObject(ctx, config.BucketName, srcKey, bytes.NewReader(largeData), int64(len(largeData)))
			require.NoError(t, err)

			// Copy large object
			err = testCM.CopyObject(ctx, config.BucketName, srcKey, dstKey)
			assert.NoError(t, err)

			// Verify content
			dstReader, err := testCM.GetObject(ctx, config.BucketName, dstKey, 0, int64(len(largeData)))
			assert.NoError(t, err)
			dstData, err := io.ReadAll(dstReader)
			assert.NoError(t, err)
			assert.Equal(t, largeData, dstData)

			// Clean up
			err = testCM.RemoveObject(ctx, config.BucketName, srcKey)
			assert.NoError(t, err)
			err = testCM.RemoveObject(ctx, config.BucketName, dstKey)
			assert.NoError(t, err)
		})

		// Test copy empty object
		t.Run("copy empty object", func(t *testing.T) {
			srcKey := "copy_test/src5/empty_file"
			dstKey := "copy_test/dst5/empty_file"
			emptyData := []byte{}

			// Put empty object
			err := testCM.PutObject(ctx, config.BucketName, srcKey, bytes.NewReader(emptyData), 0)
			require.NoError(t, err)

			// Copy empty object
			err = testCM.CopyObject(ctx, config.BucketName, srcKey, dstKey)
			assert.NoError(t, err)

			// Verify destination exists and has size 0
			size, err := testCM.StatObject(ctx, config.BucketName, dstKey)
			assert.NoError(t, err)
			assert.Equal(t, int64(0), size)

			// Clean up
			err = testCM.RemoveObject(ctx, config.BucketName, srcKey)
			assert.NoError(t, err)
			err = testCM.RemoveObject(ctx, config.BucketName, dstKey)
			assert.NoError(t, err)
		})

		// Test copy with nested path
		t.Run("copy object with nested path", func(t *testing.T) {
			srcKey := "copy_test/src6/file6"
			dstKey := "copy_test/dst6/nested/deep/path/file6"
			value := []byte("test data for nested path copy")

			// Put source object
			err := testCM.PutObject(ctx, config.BucketName, srcKey, bytes.NewReader(value), int64(len(value)))
			require.NoError(t, err)

			// Copy to nested path
			err = testCM.CopyObject(ctx, config.BucketName, srcKey, dstKey)
			assert.NoError(t, err)

			// Verify destination exists and has correct content
			dstReader, err := testCM.GetObject(ctx, config.BucketName, dstKey, 0, 1024)
			assert.NoError(t, err)
			dstData, err := io.ReadAll(dstReader)
			assert.NoError(t, err)
			assert.Equal(t, value, dstData)

			// Clean up
			err = testCM.RemoveObject(ctx, config.BucketName, srcKey)
			assert.NoError(t, err)
			err = testCM.RemoveObject(ctx, config.BucketName, dstKey)
			assert.NoError(t, err)
		})
	})
}

func TestReadFile(t *testing.T) {
	ctx := context.Background()
	bucketName := Params.MinioCfg.BucketName.GetValue()
	c := &objectstorage.Config{
		BucketName:    bucketName,
		CreateBucket:  true,
		UseIAM:        false,
		CloudProvider: "azure",
	}
	rcm, err := NewRemoteChunkManager(ctx, c)

	t.Run("Read", func(t *testing.T) {
		filePath := "test-Read"
		data := []byte("Test data for Read.")

		err = rcm.Write(ctx, filePath, data)
		assert.NoError(t, err)
		defer rcm.Remove(ctx, filePath)

		reader, err := rcm.Reader(ctx, filePath)
		assert.NoError(t, err)

		buffer := make([]byte, 4)
		n, err := reader.Read(buffer)
		assert.NoError(t, err)
		assert.Equal(t, 4, n)
		assert.Equal(t, "Test", string(buffer))

		buffer = make([]byte, 6)
		n, err = reader.Read(buffer)
		assert.NoError(t, err)
		assert.Equal(t, 6, n)
		assert.Equal(t, " data ", string(buffer))

		buffer = make([]byte, 40)
		n, err = reader.Read(buffer)
		assert.Error(t, err)
		assert.Equal(t, 9, n)
		assert.Equal(t, "for Read.", string(buffer[:9]))
	})

	t.Run("ReadAt", func(t *testing.T) {
		filePath := "test-ReadAt"
		data := []byte("Test data for ReadAt.")

		err = rcm.Write(ctx, filePath, data)
		assert.NoError(t, err)
		defer rcm.Remove(ctx, filePath)

		reader, err := rcm.Reader(ctx, filePath)
		assert.NoError(t, err)

		buffer := make([]byte, 4)
		n, err := reader.ReadAt(buffer, 5)
		assert.NoError(t, err)
		assert.Equal(t, 4, n)
		assert.Equal(t, "data", string(buffer))

		buffer = make([]byte, 4)
		n, err = reader.Read(buffer)
		assert.NoError(t, err)
		assert.Equal(t, 4, n)
		assert.Equal(t, "Test", string(buffer))

		buffer = make([]byte, 4)
		n, err = reader.ReadAt(buffer, 20)
		assert.Error(t, err)
		assert.Equal(t, 1, n)
		assert.Equal(t, ".", string(buffer[:1]))

		buffer = make([]byte, 4)
		n, err = reader.ReadAt(buffer, 25)
		assert.Error(t, err)
		assert.Equal(t, 0, n)
	})

	t.Run("Seek start", func(t *testing.T) {
		filePath := "test-SeekStart"
		data := []byte("Test data for Seek start.")

		err = rcm.Write(ctx, filePath, data)
		assert.NoError(t, err)
		defer rcm.Remove(ctx, filePath)

		reader, err := rcm.Reader(ctx, filePath)
		assert.NoError(t, err)

		offset, err := reader.Seek(10, io.SeekStart)
		assert.NoError(t, err)
		assert.Equal(t, int64(10), offset)

		buffer := make([]byte, 4)
		n, err := reader.Read(buffer)
		assert.NoError(t, err)
		assert.Equal(t, 4, n)
		assert.Equal(t, "for ", string(buffer))

		offset, err = reader.Seek(40, io.SeekStart)
		assert.NoError(t, err)
		assert.Equal(t, int64(40), offset)

		buffer = make([]byte, 4)
		n, err = reader.Read(buffer)
		assert.Error(t, err)
		assert.Equal(t, 0, n)
	})

	t.Run("Seek current", func(t *testing.T) {
		filePath := "test-SeekStart"
		data := []byte("Test data for Seek current.")

		err = rcm.Write(ctx, filePath, data)
		assert.NoError(t, err)
		defer rcm.Remove(ctx, filePath)

		reader, err := rcm.Reader(ctx, filePath)
		assert.NoError(t, err)

		buffer := make([]byte, 4)
		n, err := reader.Read(buffer)
		assert.NoError(t, err)
		assert.Equal(t, 4, n)
		assert.Equal(t, "Test", string(buffer))

		offset, err := reader.Seek(10, io.SeekCurrent)
		assert.NoError(t, err)
		assert.Equal(t, int64(14), offset)

		buffer = make([]byte, 4)
		n, err = reader.Read(buffer)
		assert.NoError(t, err)
		assert.Equal(t, 4, n)
		assert.Equal(t, "Seek", string(buffer))

		offset, err = reader.Seek(40, io.SeekCurrent)
		assert.NoError(t, err)
		assert.Equal(t, int64(58), offset)

		buffer = make([]byte, 4)
		n, err = reader.Read(buffer)
		assert.Error(t, err)
		assert.Equal(t, 0, n)
	})

	t.Run("Seek end", func(t *testing.T) {
		filePath := "test-SeekEnd"
		data := []byte("Test data for Seek end.")

		err = rcm.Write(ctx, filePath, data)
		assert.NoError(t, err)
		defer rcm.Remove(ctx, filePath)

		reader, err := rcm.Reader(ctx, filePath)
		assert.NoError(t, err)

		buffer := make([]byte, 4)
		n, err := reader.Read(buffer)
		assert.NoError(t, err)
		assert.Equal(t, 4, n)
		assert.Equal(t, "Test", string(buffer))

		offset, err := reader.Seek(10, io.SeekEnd)
		assert.NoError(t, err)
		assert.Equal(t, int64(33), offset)

		buffer = make([]byte, 4)
		n, err = reader.Read(buffer)
		assert.Error(t, err)
		assert.Equal(t, 0, n)

		offset, err = reader.Seek(10, 3)
		assert.Error(t, err)
		assert.Equal(t, int64(0), offset)
	})

	t.Run("Close", func(t *testing.T) {
		filePath := "test-Close"
		data := []byte("Test data for Close.")

		err = rcm.Write(ctx, filePath, data)
		assert.NoError(t, err)
		defer rcm.Remove(ctx, filePath)

		reader, err := rcm.Reader(ctx, filePath)
		assert.NoError(t, err)

		err = reader.Close()
		assert.NoError(t, err)
	})
}
