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
	"io"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"

	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type AzureObjectStorage struct {
	*service.Client
}

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

func (AzureObjectStorage *AzureObjectStorage) GetObject(ctx context.Context, bucketName, objectName string, offset int64, size int64) (FileReader, error) {
	return NewBlobReader(AzureObjectStorage.Client.NewContainerClient(bucketName).NewBlockBlobClient(objectName), offset)
}

func (AzureObjectStorage *AzureObjectStorage) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) error {
	_, err := AzureObjectStorage.Client.NewContainerClient(bucketName).NewBlockBlobClient(objectName).UploadStream(ctx, reader, &azblob.UploadStreamOptions{})
	return checkObjectStorageError(objectName, err)
}

func (AzureObjectStorage *AzureObjectStorage) StatObject(ctx context.Context, bucketName, objectName string) (int64, error) {
	info, err := AzureObjectStorage.Client.NewContainerClient(bucketName).NewBlockBlobClient(objectName).GetProperties(ctx, &blob.GetPropertiesOptions{})
	if err != nil {
		return 0, checkObjectStorageError(objectName, err)
	}
	return *info.ContentLength, nil
}

func (AzureObjectStorage *AzureObjectStorage) WalkWithObjects(ctx context.Context, bucketName string, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc) error {
	if recursive {
		pager := AzureObjectStorage.Client.NewContainerClient(bucketName).NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{
			Prefix: &prefix,
		})
		if pager.More() {
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
		if pager.More() {
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
	return checkObjectStorageError(objectName, err)
}
