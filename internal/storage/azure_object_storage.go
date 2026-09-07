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
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type AzureObjectStorage struct {
	*service.Client

	// sourceService addresses the copy source account when it differs from the
	// client's own account. Cross-account source reads cannot be authorized by
	// the client credential, so source URLs built from it carry sourceSAS.
	sourceService *service.Client
	sourceSAS     string
}

const (
	azureCopyPollInterval = 200 * time.Millisecond
	// Caller deadlines are honored. This default only applies when the caller
	// passes a context without a deadline, so pending Azure copies cannot hang forever.
	azureCopyDefaultTimeout = 24 * time.Hour
)

func newAzureObjectStorageWithConfig(ctx context.Context, c *objectstorage.Config) (*AzureObjectStorage, error) {
	client, err := objectstorage.NewAzureObjectStorageClient(ctx, c)
	if err != nil {
		return nil, err
	}
	storage := &AzureObjectStorage{Client: client}
	if c.AzureSourceSAS == "" {
		return storage, nil
	}
	sourceURL, err := azureServiceURL(c.AzureSourceEndpoint, c.AzureSourceUseSSL)
	if err != nil {
		return nil, err
	}
	// NoCredential keeps this client anonymous: the source account is
	// authorized by the SAS token carried on each source URL, not by any
	// credential pipeline of its own.
	sourceService, err := service.NewClientWithNoCredential(sourceURL, nil)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidErr(err, "invalid azure copy source endpoint")
	}
	storage.sourceService = sourceService
	storage.sourceSAS = strings.TrimPrefix(c.AzureSourceSAS, "?")
	return storage, nil
}

func azureServiceURL(host string, useSSL bool) (string, error) {
	host = strings.ToLower(strings.TrimSpace(strings.TrimSuffix(host, ".")))
	if host == "" {
		return "", merr.WrapErrParameterInvalidMsg("azure copy source endpoint is empty")
	}
	scheme := "https"
	if !useSSL {
		scheme = "http"
	}
	// Round-trip through url.Parse so anything that is not a bare host — a
	// path, query, fragment, userinfo, or an invalid character — is rejected
	// here rather than producing a malformed copy source URL.
	u, err := url.Parse(scheme + "://" + host + "/")
	if err != nil || u.Host != host || u.Path != "/" || u.RawQuery != "" || u.Fragment != "" {
		return "", merr.WrapErrParameterInvalidMsg("azure copy source endpoint %q must be a bare service host", host)
	}
	return u.String(), nil
}

// BlobReader is implemented because Azure's stream body does not have ReadAt and Seek interfaces.
// BlobReader is not concurrency safe.
type BlobReader struct {
	client          *blockblob.Client
	position        int64
	body            io.ReadCloser
	contentLength   int64
	needResetStream bool
}

func NewBlobReader(client *blockblob.Client, offset int64) (*BlobReader, error) {
	return &BlobReader{client: client, position: offset, needResetStream: true}, nil
}

func (b *BlobReader) Read(p []byte) (n int, err error) {
	ctx := context.TODO()

	if b.needResetStream {
		opts := &azblob.DownloadStreamOptions{
			Range: blob.HTTPRange{
				Offset: b.position,
			},
		}
		object, err := b.client.DownloadStream(ctx, opts)
		if err != nil {
			return 0, err
		}
		b.body = object.Body
		b.contentLength = *object.ContentLength
	}

	n, err = b.body.Read(p)
	if err != nil {
		return n, err
	}
	b.position += int64(n)
	b.needResetStream = false
	return n, nil
}

func (b *BlobReader) Close() error {
	if b.body != nil {
		return b.body.Close()
	}
	return nil
}

func (b *BlobReader) ReadAt(p []byte, off int64) (n int, err error) {
	httpRange := blob.HTTPRange{
		Offset: off,
		Count:  int64(len(p)),
	}
	object, err := b.client.DownloadStream(context.Background(), &blob.DownloadStreamOptions{
		Range: httpRange,
	})
	if err != nil {
		return 0, err
	}
	defer object.Body.Close()
	return io.ReadFull(object.Body, p)
}

func (b *BlobReader) Seek(offset int64, whence int) (int64, error) {
	props, err := b.client.GetProperties(context.Background(), &blob.GetPropertiesOptions{})
	if err != nil {
		return 0, err
	}
	size := *props.ContentLength
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = b.position + offset
	case io.SeekEnd:
		newOffset = size + offset
	default:
		return 0, merr.WrapErrIoFailedReason("invalid whence")
	}

	b.position = newOffset
	b.needResetStream = true
	return newOffset, nil
}

func (b *BlobReader) Size() (int64, error) {
	return b.contentLength, nil
}

func (AzureObjectStorage *AzureObjectStorage) GetObject(ctx context.Context, bucketName, objectName string, offset int64, size int64) (FileReader, error) {
	return NewBlobReader(AzureObjectStorage.Client.NewContainerClient(bucketName).NewBlockBlobClient(objectName), offset)
}

func (AzureObjectStorage *AzureObjectStorage) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) error {
	_, err := AzureObjectStorage.Client.NewContainerClient(bucketName).NewBlockBlobClient(objectName).UploadStream(ctx, reader, &azblob.UploadStreamOptions{})
	return mapObjectStorageError(objectName, err)
}

func (AzureObjectStorage *AzureObjectStorage) StatObject(ctx context.Context, bucketName, objectName string) (int64, error) {
	info, err := AzureObjectStorage.Client.NewContainerClient(bucketName).NewBlockBlobClient(objectName).GetProperties(ctx, &blob.GetPropertiesOptions{})
	if err != nil {
		return 0, mapObjectStorageError(objectName, err)
	}
	return *info.ContentLength, nil
}

func (AzureObjectStorage *AzureObjectStorage) WalkWithObjects(ctx context.Context, bucketName string, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc) error {
	if recursive {
		pager := AzureObjectStorage.Client.NewContainerClient(bucketName).NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{
			Prefix: &prefix,
		})
		for pager.More() {
			pageResp, err := pager.NextPage(ctx)
			if err != nil {
				return mapObjectStorageError(prefix, err)
			}
			for _, blob := range pageResp.Segment.BlobItems {
				if !walkFunc(&ChunkObjectInfo{FilePath: *blob.Name, ModifyTime: *blob.Properties.LastModified}) {
					return nil
				}
			}
		}
	} else {
		pager := AzureObjectStorage.Client.NewContainerClient(bucketName).NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{
			Prefix: &prefix,
		})
		for pager.More() {
			pageResp, err := pager.NextPage(ctx)
			if err != nil {
				return mapObjectStorageError(prefix, err)
			}

			for _, blob := range pageResp.Segment.BlobItems {
				if !walkFunc(&ChunkObjectInfo{FilePath: *blob.Name, ModifyTime: *blob.Properties.LastModified}) {
					return nil
				}
			}
			for _, blob := range pageResp.Segment.BlobPrefixes {
				if !walkFunc(&ChunkObjectInfo{FilePath: *blob.Name, ModifyTime: time.Now()}) {
					return nil
				}
			}
		}
	}
	return nil
}

func (AzureObjectStorage *AzureObjectStorage) RemoveObject(ctx context.Context, bucketName, objectName string) error {
	_, err := AzureObjectStorage.Client.NewContainerClient(bucketName).NewBlockBlobClient(objectName).Delete(ctx, &blob.DeleteOptions{})
	return mapObjectStorageError(objectName, err)
}

func (AzureObjectStorage *AzureObjectStorage) CopyObjectCrossBucket(ctx context.Context, srcContainer, srcObjectName, dstContainer, dstObjectName string) error {
	srcURL := AzureObjectStorage.sourceCopyURL(srcContainer, srcObjectName)
	dstBlobClient := AzureObjectStorage.NewContainerClient(dstContainer).NewBlockBlobClient(dstObjectName)
	return startOrResumeAzureCopy(ctx, dstBlobClient, srcURL, dstObjectName)
}

// sourceCopyURL builds the read-side URL of a server-side copy. Same-account
// copies reuse the client's own service URL; cross-account copies address the
// source account directly and append the SAS that authorizes the read.
func (AzureObjectStorage *AzureObjectStorage) sourceCopyURL(srcContainer, srcObjectName string) string {
	if AzureObjectStorage.sourceService == nil {
		return AzureObjectStorage.NewContainerClient(srcContainer).NewBlockBlobClient(srcObjectName).URL()
	}
	return AzureObjectStorage.sourceService.NewContainerClient(srcContainer).NewBlockBlobClient(srcObjectName).URL() +
		"?" + AzureObjectStorage.sourceSAS
}

// startOrResumeAzureCopy starts one asynchronous Azure copy. If an SDK retry
// observes the copy already in progress, polling resumes that operation rather
// than issuing another non-idempotent start request.
func startOrResumeAzureCopy(ctx context.Context, dstBlobClient *blockblob.Client, srcURL, dstObjectName string) error {
	response, err := dstBlobClient.StartCopyFromURL(ctx, srcURL, &blob.StartCopyFromURLOptions{})
	if err != nil {
		if !bloberror.HasCode(err, bloberror.PendingCopyOperation) {
			return mapObjectStorageError(dstObjectName, err)
		}
	}

	copyID := ""
	if response.CopyID != nil {
		copyID = *response.CopyID
	}
	return waitAzureCopyComplete(ctx, dstBlobClient, dstObjectName, srcURL, copyID)
}

func waitAzureCopyComplete(
	ctx context.Context,
	dstBlobClient *blockblob.Client,
	dstObjectName string,
	expectedSource string,
	expectedCopyID string,
) error {
	if _, ok := ctx.Deadline(); !ok {
		timeoutCtx, cancel := context.WithTimeout(ctx, azureCopyDefaultTimeout)
		defer cancel()
		ctx = timeoutCtx
	}

	ticker := time.NewTicker(azureCopyPollInterval)
	defer ticker.Stop()
	copyID := expectedCopyID

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		props, err := dstBlobClient.GetProperties(ctx, &blob.GetPropertiesOptions{})
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			mappedErr := mapObjectStorageError(dstObjectName, err)
			if merr.IsNonRetryableErr(mappedErr) {
				return mappedErr
			}
			// GetProperties is idempotent, so transient poll failures can be
			// retried without replaying StartCopyFromURL.
			if err := waitAzureCopyPoll(ctx, ticker); err != nil {
				return err
			}
			continue
		}
		if expectedSource != "" {
			// The service echoes the copy source without a guarantee about the
			// query string, so compare the URL identity only: a SAS-bearing
			// source must still verify against its credential-free form.
			expectedIdentity := azureCopySourceIdentity(expectedSource)
			actualIdentity := ""
			if props.CopySource != nil {
				actualIdentity = azureCopySourceIdentity(*props.CopySource)
			}
			if actualIdentity != expectedIdentity {
				// Report credential-free identities only: the raw URLs may carry
				// the source SAS in their query strings, and this error bubbles
				// into snapshot job failure reasons and logs.
				return merr.WrapErrIoFailedMsg(
					"azure copy source mismatch for %s: expected %s, actual %s",
					dstObjectName, expectedIdentity, actualIdentity)
			}
		}
		if copyID == "" && props.CopyID != nil {
			copyID = *props.CopyID
		} else if copyID != "" && (props.CopyID == nil || *props.CopyID != copyID) {
			actualCopyID := ""
			if props.CopyID != nil {
				actualCopyID = *props.CopyID
			}
			return merr.WrapErrIoFailedMsg(
				"azure copy ID mismatch for %s: expected %s, actual %s",
				dstObjectName, copyID, actualCopyID)
		}
		if props.CopyStatus == nil {
			return merr.WrapErrIoFailedReason(fmt.Sprintf("azure copy status for %s is empty", dstObjectName))
		}

		switch *props.CopyStatus {
		case blob.CopyStatusTypeSuccess:
			return nil
		case blob.CopyStatusTypeFailed, blob.CopyStatusTypeAborted:
			statusDescription := ""
			if props.CopyStatusDescription != nil {
				statusDescription = *props.CopyStatusDescription
			}
			return merr.WrapErrIoFailedReason(
				fmt.Sprintf("azure copy for %s finished with status %s: %s", dstObjectName, *props.CopyStatus, statusDescription))
		case blob.CopyStatusTypePending:
			if err := waitAzureCopyPoll(ctx, ticker); err != nil {
				return err
			}
		default:
			return merr.WrapErrIoFailedReason(
				fmt.Sprintf("azure copy for %s returned unknown status %s", dstObjectName, *props.CopyStatus))
		}
	}
}

func waitAzureCopyPoll(ctx context.Context, ticker *time.Ticker) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ticker.C:
		return nil
	}
}

func azureCopySourceIdentity(sourceURL string) string {
	if i := strings.Index(sourceURL, "?"); i >= 0 {
		sourceURL = sourceURL[:i]
	}
	// Compare the unescaped form: the service may echo an equivalent source
	// URL with different percent-encoding than the request carried.
	if unescaped, err := url.PathUnescape(sourceURL); err == nil {
		return unescaped
	}
	return sourceURL
}
