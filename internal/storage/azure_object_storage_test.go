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
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestWaitAzureCopyComplete(t *testing.T) {
	t.Run("wait until success", func(t *testing.T) {
		statuses := []blob.CopyStatusType{blob.CopyStatusTypePending, blob.CopyStatusTypeSuccess}
		client, calls := mockAzureCopyStatuses(t, statuses, "source", "copy-id", "")

		err := waitAzureCopyComplete(context.Background(), client, "dst", "source", "copy-id")
		require.NoError(t, err)
		require.Equal(t, 2, *calls)
	})

	t.Run("failed status returns error", func(t *testing.T) {
		statuses := []blob.CopyStatusType{blob.CopyStatusTypeFailed}
		client, _ := mockAzureCopyStatuses(t, statuses, "source", "copy-id", "copy failed")

		err := waitAzureCopyComplete(context.Background(), client, "dst", "source", "copy-id")
		require.Error(t, err)
	})

	t.Run("aborted status returns error", func(t *testing.T) {
		statuses := []blob.CopyStatusType{blob.CopyStatusTypeAborted}
		client, _ := mockAzureCopyStatuses(t, statuses, "source", "copy-id", "")

		err := waitAzureCopyComplete(context.Background(), client, "dst", "source", "copy-id")
		require.Error(t, err)
	})

	t.Run("context canceled returns context error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := waitAzureCopyComplete(ctx, new(blockblob.Client), "dst", "source", "copy-id")
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("pending copy respects caller deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cancel()
		statuses := []blob.CopyStatusType{blob.CopyStatusTypePending}
		client, _ := mockAzureCopyStatuses(t, statuses, "source", "copy-id", "")

		err := waitAzureCopyComplete(ctx, client, "dst", "source", "copy-id")
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("rejects mismatched source", func(t *testing.T) {
		statuses := []blob.CopyStatusType{blob.CopyStatusTypePending}
		client, _ := mockAzureCopyStatuses(t, statuses, "other-source", "copy-id", "")

		err := waitAzureCopyComplete(context.Background(), client, "dst", "source", "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "copy source mismatch")
	})

	t.Run("mismatch error never carries the source SAS", func(t *testing.T) {
		statuses := []blob.CopyStatusType{blob.CopyStatusTypePending}
		actual := "https://other-account.blob.core.windows.net/src-container/srcobj?sv=2024-08-04&sig=other"
		client, _ := mockAzureCopyStatuses(t, statuses, actual, "copy-id", "")

		err := waitAzureCopyComplete(context.Background(), client, "dst",
			"https://src-account.blob.core.windows.net/src-container/srcobj?sv=2024-08-04&sig=abc", "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "https://src-account.blob.core.windows.net/src-container/srcobj")
		assert.NotContains(t, err.Error(), "sig=abc")
		assert.NotContains(t, err.Error(), "sig=other")
	})

	t.Run("rejects replaced copy ID", func(t *testing.T) {
		statuses := []blob.CopyStatusType{blob.CopyStatusTypePending}
		client, _ := mockAzureCopyStatuses(t, statuses, "source", "other-copy-id", "")

		err := waitAzureCopyComplete(context.Background(), client, "dst", "source", "copy-id")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "copy ID mismatch")
	})

	t.Run("permanent poll error fails immediately", func(t *testing.T) {
		calls := 0
		patch := mockey.Mock((*blockblob.Client).GetProperties).To(
			func(_ *blockblob.Client, _ context.Context, _ *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
				calls++
				return blob.GetPropertiesResponse{}, &azcore.ResponseError{ErrorCode: string(bloberror.AuthorizationFailure)}
			}).Build()
		t.Cleanup(func() { patch.UnPatch() })

		err := waitAzureCopyComplete(context.Background(), new(blockblob.Client), "dst", "source", "copy-id")
		require.ErrorIs(t, err, merr.ErrIoPermissionDenied)
		assert.Equal(t, 1, calls)
	})
}

func TestStartOrResumeAzureCopySourceIdentityIgnoresQuery(t *testing.T) {
	// The service echoes the copy source without a guarantee about the query
	// string or percent-encoding; a SAS-bearing, escaped expected source must
	// verify against its bare, unescaped form.
	status := blob.CopyStatusTypeSuccess
	copyID := "copy-id"
	bareSource := "https://src-account.blob.core.windows.net/src-container/src/obj"
	client, calls := mockAzureCopyStatuses(t, []blob.CopyStatusType{status}, bareSource, copyID, "")

	err := waitAzureCopyComplete(context.Background(), client, "dst",
		"https://src-account.blob.core.windows.net/src-container/src%2Fobj?sv=2024-08-04&sig=abc", copyID)
	require.NoError(t, err)
	require.Equal(t, 1, *calls)
}

func TestAzureObjectStorageCopyObjectCrossBucketSourceSAS(t *testing.T) {
	newStorage := func(t *testing.T, sourceEndpoint, sourceSAS string, sourceUseSSL bool) *AzureObjectStorage {
		t.Helper()
		// #nosec G101 -- "ZmFrZS1rZXk=" is base64 for "fake-key", an offline
		// fixture credential; all requests are mocked, nothing is ever signed.
		cfg := &objectstorage.Config{
			Address:                     "core.windows.net",
			BucketName:                  "dst-container",
			AccessKeyID:                 "dst-account",
			SecretAccessKeyID:           "ZmFrZS1rZXk=",
			CloudProvider:               objectstorage.CloudProviderAzure,
			UseSSL:                      true,
			SkipBucketCheck:             true,
			IgnoreAzureConnectionString: true,
			AzureSourceEndpoint:         sourceEndpoint,
			AzureSourceUseSSL:           sourceUseSSL,
			AzureSourceSAS:              sourceSAS,
		}
		storage, err := newAzureObjectStorageWithConfig(context.Background(), cfg)
		require.NoError(t, err)
		return storage
	}

	mockCopy := func(t *testing.T, source *string, echoSourceWithQuery bool) {
		t.Helper()
		copyID := "copy-id"
		status := blob.CopyStatusTypeSuccess
		startPatch := mockey.Mock((*blockblob.Client).StartCopyFromURL).To(
			func(_ *blockblob.Client, _ context.Context, url string, _ *blob.StartCopyFromURLOptions) (blob.StartCopyFromURLResponse, error) {
				*source = url
				return blob.StartCopyFromURLResponse{CopyID: &copyID}, nil
			}).Build()
		t.Cleanup(func() { startPatch.UnPatch() })
		echoedSource := ""
		pollPatch := mockey.Mock((*blockblob.Client).GetProperties).To(
			func(_ *blockblob.Client, _ context.Context, _ *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
				echoedSource = *source
				if !echoSourceWithQuery {
					if i := strings.IndexByte(echoedSource, '?'); i >= 0 {
						echoedSource = echoedSource[:i]
					}
				}
				return blob.GetPropertiesResponse{
					CopyID:     &copyID,
					CopySource: &echoedSource,
					CopyStatus: &status,
				}, nil
			}).Build()
		t.Cleanup(func() { pollPatch.UnPatch() })
	}

	t.Run("cross-account source URL addresses the source account and carries the SAS", func(t *testing.T) {
		storage := newStorage(t, "src-account.blob.core.windows.net", "sv=2024-08-04&sig=abc", true)
		var captured string
		mockCopy(t, &captured, false)

		err := storage.CopyObjectCrossBucket(context.Background(), "src-container", "srcobj", "dst-container", "dstobj")
		require.NoError(t, err)
		assert.Equal(t, "https://src-account.blob.core.windows.net/src-container/srcobj?sv=2024-08-04&sig=abc", captured)
	})

	t.Run("source URL scheme follows the source config, not the destination", func(t *testing.T) {
		// The destination config uses SSL; the source account does not, so the
		// source URL must be http even though the client config has UseSSL=true.
		storage := newStorage(t, "src-account.blob.core.windows.net", "sv=2024-08-04&sig=abc", false)
		var captured string
		mockCopy(t, &captured, false)

		err := storage.CopyObjectCrossBucket(context.Background(), "src-container", "srcobj", "dst-container", "dstobj")
		require.NoError(t, err)
		assert.Equal(t, "http://src-account.blob.core.windows.net/src-container/srcobj?sv=2024-08-04&sig=abc", captured)
	})

	t.Run("same-account source URL stays on the client service", func(t *testing.T) {
		storage := newStorage(t, "", "", true)
		var captured string
		mockCopy(t, &captured, true)

		err := storage.CopyObjectCrossBucket(context.Background(), "dst-container", "srcobj", "dst-container", "dstobj")
		require.NoError(t, err)
		assert.Equal(t, "https://dst-account.blob.core.windows.net/dst-container/srcobj", captured)
	})

	t.Run("invalid source endpoint host is rejected at construction", func(t *testing.T) {
		// #nosec G101 -- "ZmFrZS1rZXk=" is base64 for "fake-key", an offline
		// fixture credential; all requests are mocked, nothing is ever signed.
		cfg := &objectstorage.Config{
			Address:                     "core.windows.net",
			BucketName:                  "dst-container",
			AccessKeyID:                 "dst-account",
			SecretAccessKeyID:           "ZmFrZS1rZXk=",
			CloudProvider:               objectstorage.CloudProviderAzure,
			UseSSL:                      true,
			SkipBucketCheck:             true,
			IgnoreAzureConnectionString: true,
			AzureSourceEndpoint:         "not a host",
			AzureSourceSAS:              "sv=2024-08-04&sig=abc",
		}
		_, err := newAzureObjectStorageWithConfig(context.Background(), cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must be a bare service host")
	})
}

func TestStartOrResumeAzureCopy(t *testing.T) {
	t.Run("poll retry does not restart copy", func(t *testing.T) {
		startCalls := 0
		copyID := "copy-id"
		startPatch := mockey.Mock((*blockblob.Client).StartCopyFromURL).To(
			func(_ *blockblob.Client, _ context.Context, _ string, _ *blob.StartCopyFromURLOptions) (blob.StartCopyFromURLResponse, error) {
				startCalls++
				return blob.StartCopyFromURLResponse{CopyID: &copyID}, nil
			}).Build()
		t.Cleanup(func() { startPatch.UnPatch() })

		pollCalls := 0
		source := "source"
		status := blob.CopyStatusTypeSuccess
		getPropertiesPatch := mockey.Mock((*blockblob.Client).GetProperties).To(
			func(_ *blockblob.Client, _ context.Context, _ *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
				pollCalls++
				if pollCalls == 1 {
					return blob.GetPropertiesResponse{}, &azcore.ResponseError{ErrorCode: string(bloberror.ServerBusy)}
				}
				return blob.GetPropertiesResponse{
					CopyID:     &copyID,
					CopySource: &source,
					CopyStatus: &status,
				}, nil
			}).Build()
		t.Cleanup(func() { getPropertiesPatch.UnPatch() })

		err := startOrResumeAzureCopy(context.Background(), new(blockblob.Client), "source", "dst")
		require.NoError(t, err)
		assert.Equal(t, 1, startCalls)
		assert.Equal(t, 2, pollCalls)
	})

	t.Run("resumes pending copy", func(t *testing.T) {
		startCalls := 0
		startPatch := mockey.Mock((*blockblob.Client).StartCopyFromURL).To(
			func(_ *blockblob.Client, _ context.Context, _ string, _ *blob.StartCopyFromURLOptions) (blob.StartCopyFromURLResponse, error) {
				startCalls++
				return blob.StartCopyFromURLResponse{}, &azcore.ResponseError{ErrorCode: string(bloberror.PendingCopyOperation)}
			}).Build()
		t.Cleanup(func() { startPatch.UnPatch() })
		statuses := []blob.CopyStatusType{blob.CopyStatusTypePending, blob.CopyStatusTypeSuccess}
		client, _ := mockAzureCopyStatuses(t, statuses, "source", "existing-copy-id", "")

		err := startOrResumeAzureCopy(context.Background(), client, "source", "dst")
		require.NoError(t, err)
		assert.Equal(t, 1, startCalls)
	})
}

func mockAzureCopyStatuses(
	t *testing.T,
	statuses []blob.CopyStatusType,
	source string,
	copyID string,
	description string,
) (*blockblob.Client, *int) {
	t.Helper()
	calls := 0
	patch := mockey.Mock((*blockblob.Client).GetProperties).To(
		func(_ *blockblob.Client, _ context.Context, _ *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
			idx := calls
			calls++
			if len(statuses) == 0 {
				return blob.GetPropertiesResponse{}, nil
			}
			if idx >= len(statuses) {
				idx = len(statuses) - 1
			}
			status := statuses[idx]
			return blob.GetPropertiesResponse{
				CopyID:                &copyID,
				CopySource:            &source,
				CopyStatus:            &status,
				CopyStatusDescription: &description,
			}, nil
		}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	return new(blockblob.Client), &calls
}

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
		// newAzureObjectStorageWithConfig probes the Azure managed-identity
		// IMDS endpoint (link-local 169.254.169.254). On hosts without an
		// IMDS responder (bare-metal dev machines) the SDK blocks on the
		// TCP connect until the 10min testing.M timeout. Bound each call
		// with a short context so it fail-fast regardless of environment;
		// the test only asserts that an error is returned.
		var err error
		config.UseIAM = true

		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err = newAzureObjectStorageWithConfig(cctx, &config)
		cancel()
		assert.Error(t, err)

		os.Setenv("AZURE_CLIENT_ID", "00000000-0000-0000-0000-00000000000")
		os.Setenv("AZURE_TENANT_ID", "00000000-0000-0000-0000-00000000000")
		os.Setenv("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/tokens/azure-identity-token")

		cctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		_, err = newAzureObjectStorageWithConfig(cctx, &config)
		cancel()
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

func TestMapObjectStorageError_Azure_NewErrors(t *testing.T) {
	tests := []struct {
		name          string
		inputError    error
		expectedError error
	}{
		{
			name:          "AuthenticationFailed",
			inputError:    &azcore.ResponseError{ErrorCode: "AuthenticationFailed"},
			expectedError: merr.ErrIoPermissionDenied,
		},
		{
			name:          "AuthorizationFailure",
			inputError:    &azcore.ResponseError{ErrorCode: "AuthorizationFailure"},
			expectedError: merr.ErrIoPermissionDenied,
		},
		{
			name:          "ContainerNotFound",
			inputError:    &azcore.ResponseError{ErrorCode: "ContainerNotFound"},
			expectedError: merr.ErrIoBucketNotFound,
		},
		{
			name:          "InvalidParameterValue",
			inputError:    &azcore.ResponseError{ErrorCode: "InvalidParameterValue"},
			expectedError: merr.ErrIoInvalidArgument,
		},
		{
			name:          "InvalidRange",
			inputError:    &azcore.ResponseError{ErrorCode: "InvalidRange"},
			expectedError: merr.ErrIoInvalidRange,
		},
		{
			name:          "RequestBodyTooLarge",
			inputError:    &azcore.ResponseError{ErrorCode: "RequestBodyTooLarge"},
			expectedError: merr.ErrIoEntityTooLarge,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapObjectStorageError("test/path", tt.inputError)
			assert.True(t, errors.Is(result, tt.expectedError),
				"expected %v, got %v", tt.expectedError, result)
		})
	}
}
