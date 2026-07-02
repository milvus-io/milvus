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
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type AzureObjectStorage struct {
	*service.Client
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
	return &AzureObjectStorage{Client: client}, nil
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
				return err
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
				return err
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

func (AzureObjectStorage *AzureObjectStorage) CopyObject(ctx context.Context, bucketName, srcObjectName, dstObjectName string) error {
	return AzureObjectStorage.CopyObjectCrossBucket(ctx, bucketName, srcObjectName, bucketName, dstObjectName)
}

func (AzureObjectStorage *AzureObjectStorage) CopyObjectCrossBucket(ctx context.Context, srcContainer, srcObjectName, dstContainer, dstObjectName string) error {
	srcURL := AzureObjectStorage.NewContainerClient(srcContainer).NewBlockBlobClient(srcObjectName).URL()
	dstBlobClient := AzureObjectStorage.NewContainerClient(dstContainer).NewBlockBlobClient(dstObjectName)

	_, err := dstBlobClient.StartCopyFromURL(ctx, srcURL, &blob.StartCopyFromURLOptions{})
	if err != nil {
		return mapObjectStorageError(dstObjectName, err)
	}
	return waitAzureCopyComplete(ctx, dstBlobClient, dstObjectName)
}

type azureCopyStatusGetter interface {
	GetProperties(ctx context.Context, options *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error)
}

func waitAzureCopyComplete(ctx context.Context, dstBlobClient azureCopyStatusGetter, dstObjectName string) error {
	if _, ok := ctx.Deadline(); !ok {
		timeoutCtx, cancel := context.WithTimeout(ctx, azureCopyDefaultTimeout)
		defer cancel()
		ctx = timeoutCtx
	}

	ticker := time.NewTicker(azureCopyPollInterval)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		props, err := dstBlobClient.GetProperties(ctx, &blob.GetPropertiesOptions{})
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return mapObjectStorageError(dstObjectName, err)
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
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
		default:
			return merr.WrapErrIoFailedReason(
				fmt.Sprintf("azure copy for %s returned unknown status %s", dstObjectName, *props.CopyStatus))
		}
	}
}
