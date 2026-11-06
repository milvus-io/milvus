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
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestMinioObjectStoragePutObjectOptions(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.MinioCfg.DisableAWSChunkedEncoding.Key, "false")
	t.Cleanup(func() {
		params.Reset(params.MinioCfg.DisableAWSChunkedEncoding.Key)
	})

	storage := &MinioObjectStorage{}
	opts := storage.putObjectOptions()
	assert.False(t, opts.DisableContentSha256)
	assert.False(t, opts.SendContentMd5)

	params.Save(params.MinioCfg.DisableAWSChunkedEncoding.Key, "true")
	opts = storage.putObjectOptions()
	assert.True(t, opts.DisableContentSha256)
	assert.False(t, opts.SendContentMd5)
}

func TestMinioObjectStorage(t *testing.T) {
	ctx := context.Background()
	// This test deletes its bucket, so it must not share one with other test packages.
	config := objectstorage.Config{
		Address:           Params.MinioCfg.Address.GetValue(),
		AccessKeyID:       Params.MinioCfg.AccessKeyID.GetValue(),
		SecretAccessKeyID: Params.MinioCfg.SecretAccessKey.GetValue(),
		RootPath:          Params.MinioCfg.RootPath.GetValue(),

		BucketName:    fmt.Sprintf("test-minio-object-storage-%d", time.Now().UnixNano()),
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
		// newMinioObjectStorageWithConfig validates IAM credentials by calling
		// BucketExists against the configured endpoint. With invalid IAM
		// credentials on a host where the endpoint is unreachable, the dial
		// blocks long enough that retry.Do (CheckBucketRetryAttempts=20) drags
		// the whole package out to the 10min testing.M timeout. Bound each
		// call with a short context so it fails fast regardless of
		// environment; the test only asserts an error is returned. Mirrors
		// the Azure fix in PR #49814.
		var err error
		config.UseIAM = true
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err = newMinioObjectStorageWithConfig(cctx, &config)
		cancel()
		assert.Error(t, err)
		config.UseIAM = false
	})

	t.Run("test ssl", func(t *testing.T) {
		// Same endpoint-unreachable hang as the "test useIAM" subtest above:
		// UseSSL=true with a dummy CA cert against a non-TLS minio endpoint
		// keeps retry.Do dialing until the testing.M timeout. Bound it.
		var err error
		config.UseSSL = true
		config.SslCACert = "/tmp/dummy.crt"
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err = newMinioObjectStorageWithConfig(cctx, &config)
		cancel()
		assert.Error(t, err)
		config.UseSSL = false
	})

	t.Run("test cloud provider", func(t *testing.T) {
		// Same endpoint-unreachable hang as the "test useIAM" subtest above.
		var err error
		cloudProvider := config.CloudProvider
		config.CloudProvider = "aliyun"
		config.UseIAM = true
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err = newMinioObjectStorageWithConfig(cctx, &config)
		cancel()
		assert.Error(t, err)
		config.UseIAM = false
		cctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		_, err = newMinioObjectStorageWithConfig(cctx, &config)
		cancel()
		assert.Error(t, err)
		config.CloudProvider = "gcp"
		_, err = newMinioObjectStorageWithConfig(ctx, &config)
		assert.NoError(t, err)
		config.CloudProvider = cloudProvider
	})

	t.Run("test CopyObject", func(t *testing.T) {
		testCM, err := newMinioObjectStorageWithConfig(ctx, &config)
		assert.NoError(t, err)
		defer testCM.RemoveBucketWithOptions(ctx, config.BucketName, minio.RemoveBucketOptions{
			ForceDelete: true,
		})

		// Test successful copy
		t.Run("copy object successfully", func(t *testing.T) {
			srcKey := "copy_test/src/file1"
			dstKey := "copy_test/dst/file1"
			value := []byte("test data for copy")

			// Put source object
			err := testCM.PutObject(ctx, config.BucketName, srcKey, bytes.NewReader(value), int64(len(value)))
			require.NoError(t, err)

			// Copy object
			err = testCM.CopyObjectCrossBucket(ctx, config.BucketName, srcKey, config.BucketName, dstKey)
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

			err := testCM.CopyObjectCrossBucket(ctx, config.BucketName, srcKey, config.BucketName, dstKey)
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
			err = testCM.CopyObjectCrossBucket(ctx, config.BucketName, srcKey, config.BucketName, dstKey)
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
			err = testCM.CopyObjectCrossBucket(ctx, config.BucketName, srcKey, config.BucketName, dstKey)
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
			err = testCM.CopyObjectCrossBucket(ctx, config.BucketName, srcKey, config.BucketName, dstKey)
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
			err = testCM.CopyObjectCrossBucket(ctx, config.BucketName, srcKey, config.BucketName, dstKey)
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

func TestMinioObjectStorageWalkWithObjectsMapsErrors(t *testing.T) {
	for _, test := range []struct {
		name       string
		statusCode int
		code       string
		expected   error
	}{
		{name: "access denied", statusCode: http.StatusForbidden, code: "AccessDenied", expected: merr.ErrIoPermissionDenied},
		{name: "bucket not found", statusCode: http.StatusNotFound, code: "NoSuchBucket", expected: merr.ErrIoBucketNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(test.statusCode)
				fmt.Fprintf(w, "<Error><Code>%s</Code><Message>test error</Message></Error>", test.code)
			}))
			defer server.Close()

			client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{
				Creds:  credentials.NewStaticV4("access-key", "secret-key", ""),
				Secure: false,
			})
			require.NoError(t, err)

			storage := &MinioObjectStorage{Client: client}
			err = storage.WalkWithObjects(context.Background(), "test-bucket", "test-prefix", true, func(*ChunkObjectInfo) bool {
				return true
			})
			assert.ErrorIs(t, err, test.expected)
		})
	}

	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		client, err := minio.New("127.0.0.1:1", &minio.Options{
			Creds:  credentials.NewStaticV4("access-key", "secret-key", ""),
			Secure: false,
		})
		require.NoError(t, err)

		storage := &MinioObjectStorage{Client: client}
		err = storage.WalkWithObjects(ctx, "test-bucket", "test-prefix", true, func(*ChunkObjectInfo) bool {
			return true
		})
		assert.ErrorIs(t, err, context.Canceled)
	})
}

func TestMapObjectStorageError_MinIO_NewErrors(t *testing.T) {
	tests := []struct {
		name          string
		inputError    error
		expectedError error
	}{
		// Permanent errors
		{
			name:          "AccessDenied",
			inputError:    minio.ErrorResponse{Code: "AccessDenied"},
			expectedError: merr.ErrIoPermissionDenied,
		},
		{
			name:          "InvalidAccessKeyId",
			inputError:    minio.ErrorResponse{Code: "InvalidAccessKeyId"},
			expectedError: merr.ErrIoPermissionDenied,
		},
		{
			name:          "SignatureDoesNotMatch",
			inputError:    minio.ErrorResponse{Code: "SignatureDoesNotMatch"},
			expectedError: merr.ErrIoPermissionDenied,
		},
		{
			name:          "NoSuchBucket",
			inputError:    minio.ErrorResponse{Code: "NoSuchBucket"},
			expectedError: merr.ErrIoBucketNotFound,
		},
		{
			name:          "InvalidToken",
			inputError:    minio.ErrorResponse{Code: "InvalidToken"},
			expectedError: merr.ErrIoInvalidCredentials,
		},
		{
			name:          "ExpiredToken",
			inputError:    minio.ErrorResponse{Code: "ExpiredToken"},
			expectedError: merr.ErrIoInvalidCredentials,
		},
		// Client validation errors
		{
			name:          "InvalidArgument",
			inputError:    minio.ErrorResponse{Code: "InvalidArgument"},
			expectedError: merr.ErrIoInvalidArgument,
		},
		{
			name:          "InvalidRequest",
			inputError:    minio.ErrorResponse{Code: "InvalidRequest"},
			expectedError: merr.ErrIoInvalidArgument,
		},
		{
			name:          "InvalidRange",
			inputError:    minio.ErrorResponse{Code: "InvalidRange"},
			expectedError: merr.ErrIoInvalidRange,
		},
		{
			name:          "EntityTooLarge",
			inputError:    minio.ErrorResponse{Code: "EntityTooLarge"},
			expectedError: merr.ErrIoEntityTooLarge,
		},
		{
			name:          "MaxMessageLengthExceeded",
			inputError:    minio.ErrorResponse{Code: "MaxMessageLengthExceeded"},
			expectedError: merr.ErrIoEntityTooLarge,
		},
		// Aliyun OSS specific error codes
		{
			name:          "AliyunSecurityTokenExpired",
			inputError:    minio.ErrorResponse{Code: "SecurityTokenExpired"},
			expectedError: merr.ErrIoInvalidCredentials,
		},
		{
			name:          "AliyunInvalidAccessKeyIdInactive",
			inputError:    minio.ErrorResponse{Code: "InvalidAccessKeyId.Inactive"},
			expectedError: merr.ErrIoPermissionDenied,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := objectstorage.ToMilvusIoError("test/path", tt.inputError)
			assert.True(t, errors.Is(result, tt.expectedError),
				"expected %v, got %v", tt.expectedError, result)
		})
	}
}

// A store that answers DeleteObject with HTTP 200 has deleted the object; minio-go
// still reports the response as an error. Only that case is treated as success:
// a 404 keeps surfacing as ErrIoKeyNotFound for the callers that handle it.
func TestIsDeleteSucceededWithOK(t *testing.T) {
	assert.False(t, isDeleteSucceededWithOK(nil))
	assert.True(t, isDeleteSucceededWithOK(minio.ErrorResponse{StatusCode: http.StatusOK}))

	notFound := minio.ErrorResponse{StatusCode: http.StatusNotFound, Code: "NoSuchKey", Message: "The specified key does not exist."}
	assert.False(t, isDeleteSucceededWithOK(notFound))
	assert.True(t, errors.Is(objectstorage.ToMilvusIoError("k", notFound), merr.ErrIoKeyNotFound))

	assert.False(t, isDeleteSucceededWithOK(errors.New("dial tcp: connection refused")))
}

func TestMinioObjectStorageMultipartConfiguration(t *testing.T) {
	params := paramtable.Get()
	threshold, part := &params.MinioCfg.MultipartCopyThreshold, &params.MinioCfg.MultipartCopyPartSize
	oldThreshold, oldPart := threshold.GetValue(), part.GetValue()
	require.NoError(t, params.Save(threshold.Key, threshold.DefaultValue))
	t.Cleanup(func() { params.Save(threshold.Key, oldThreshold); params.Save(part.Key, oldPart) })
	var mu sync.Mutex
	var size int64
	var nextPart string
	var ranges map[int]string
	var single, completed bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/xml")
		q := r.URL.Query()
		switch {
		case r.Method == http.MethodHead:
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			w.Header().Set("ETag", `"source"`)
			w.Header().Set("Last-Modified", "Wed, 23 Sep 2026 00:00:00 GMT")
			if nextPart != "" {
				assert.NoError(t, params.Save(part.Key, nextPart))
			}
		case r.Method == http.MethodPost && q.Has("uploads"):
			fmt.Fprint(w, `<InitiateMultipartUploadResult><UploadId>upload</UploadId></InitiateMultipartUploadResult>`)
		case r.Method == http.MethodPut && q.Get("uploadId") == "upload":
			n, err := strconv.Atoi(q.Get("partNumber"))
			assert.NoError(t, err)
			ranges[n] = r.Header.Get("X-Amz-Copy-Source-Range")
			fmt.Fprintf(w, `<CopyPartResult><ETag>"part-%d"</ETag></CopyPartResult>`, n)
		case r.Method == http.MethodPost && q.Get("uploadId") == "upload":
			completed = true
			fmt.Fprint(w, `<CompleteMultipartUploadResult><Bucket>target-bucket</Bucket><Key>target</Key><ETag>"complete"</ETag></CompleteMultipartUploadResult>`)
		case r.Method == http.MethodPut:
			single = true
			fmt.Fprint(w, `<CopyObjectResult><ETag>"whole"</ETag></CopyObjectResult>`)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{
		Creds: credentials.NewStaticV4("access", "secret", ""), Region: "us-east-1", BucketLookup: minio.BucketLookupPath,
	})
	require.NoError(t, err)
	store := &MinioObjectStorage{Client: client, cloudProvider: objectstorage.CloudProviderAWS}
	for _, tc := range []struct {
		name, configured, next string
		size, wantPart         int64
		multipart              bool
	}{
		{"below restored threshold", "10485760", "", 500 * 1024 * 1024, 0, false},
		{"at restored threshold", "10485760", "", 1024 * 1024 * 1024, 10 * 1024 * 1024, true},
		{"freeze part size", "134217728", "33554432", 1024*1024*1024 + 3, 128 * 1024 * 1024, true},
		{"next copy sees update", "", "", 1024*1024*1024 + 3, 32 * 1024 * 1024, true},
		{"invalid size falls back", "1", "", 1024*1024*1024 + 3, 10 * 1024 * 1024, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.configured != "" {
				require.NoError(t, params.Save(part.Key, tc.configured))
			}
			mu.Lock()
			size, nextPart = tc.size, tc.next
			ranges = make(map[int]string)
			single, completed = false, false
			mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			copied, err := store.CopyObjectCrossBucketAndGetSize(ctx, "source-bucket", "source", "target-bucket", "target")
			require.NoError(t, err)
			require.Equal(t, tc.size, copied)
			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, !tc.multipart, single)
			require.Equal(t, tc.multipart, completed)
			if !tc.multipart {
				require.Empty(t, ranges)
				return
			}
			require.Len(t, ranges, int((tc.size+tc.wantPart-1)/tc.wantPart))
			for n := 1; n <= len(ranges); n++ {
				start := int64(n-1) * tc.wantPart
				require.Equal(t, fmt.Sprintf("bytes=%d-%d", start, min(start+tc.wantPart, tc.size)-1), ranges[n])
			}
		})
	}
	_, err = (&MinioObjectStorage{}).CopyObjectCrossBucketAndGetSize(context.Background(), "s", "s", "d", "d")
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}
