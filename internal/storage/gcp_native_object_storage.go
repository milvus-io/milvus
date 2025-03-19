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

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"

	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type GcpNativeObjectStorage struct {
	client *storage.Client
}

func newGcpNativeObjectStorageWithConfig(ctx context.Context, c *objectstorage.Config) (*GcpNativeObjectStorage, error) {
	client, err := objectstorage.NewGcpObjectStorageClient(ctx, c)
	if err != nil {
		return nil, err
	}
	return &GcpNativeObjectStorage{client: client}, nil
}

func (gcs *GcpNativeObjectStorage) GetObject(ctx context.Context, bucketName, objectName string,
	offset int64, size int64,
) (FileReader, error) {
	bucket := gcs.client.Bucket(bucketName)
	_, err := bucket.Attrs(ctx)
	if err != nil {
		return nil, checkObjectStorageError(objectName, err)
	}

	obj := bucket.Object(objectName)
	_, err = obj.Attrs(ctx)
	if err != nil {
		return nil, checkObjectStorageError(objectName, err)
	}
	var reader *storage.Reader
	if offset == 0 && size == 0 {
		reader, err = obj.NewReader(ctx)
	} else {
		reader, err = obj.NewRangeReader(ctx, offset, size)
	}

	if err != nil {
		return nil, checkObjectStorageError(objectName, err)
	}
	return &GcsReader{reader: reader, obj: obj}, nil
}

func (gcs *GcpNativeObjectStorage) PutObject(ctx context.Context, bucketName, objectName string,
	reader io.Reader, objectSize int64,
) error {
	obj := gcs.client.Bucket(bucketName).Object(objectName)
	writer := obj.NewWriter(ctx)
	_, err := io.Copy(writer, reader)
	if err != nil {
		return checkObjectStorageError(objectName, err)
	}
	err = writer.Close()
	if err != nil {
		return checkObjectStorageError(objectName, err)
	}
	return nil
}

func (gcs *GcpNativeObjectStorage) StatObject(ctx context.Context, bucketName,
	objectName string,
) (int64, error) {
	obj := gcs.client.Bucket(bucketName).Object(objectName)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return 0, checkObjectStorageError(objectName, err)
	}
	return attrs.Size, nil
}

func (gcs *GcpNativeObjectStorage) WalkWithObjects(ctx context.Context,
	bucketName string, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc,
) error {
	query := &storage.Query{
		Prefix: prefix,
	}
	if !recursive {
		query.Delimiter = "/"
	}

	it := gcs.client.Bucket(bucketName).Objects(ctx, query)
	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return checkObjectStorageError(prefix, err)
		}
		if objAttrs.Prefix != "" {
			if !walkFunc(&ChunkObjectInfo{FilePath: objAttrs.Prefix, ModifyTime: objAttrs.Updated}) {
				return nil
			}
		} else if objAttrs.Name != "" {
			if !walkFunc(&ChunkObjectInfo{FilePath: objAttrs.Name, ModifyTime: objAttrs.Updated}) {
				return nil
			}
		}
	}
	return nil
}

func (gcs *GcpNativeObjectStorage) RemoveObject(ctx context.Context, bucketName, prefix string) error {
	bucket := gcs.client.Bucket(bucketName)
	query := &storage.Query{Prefix: prefix}
	it := bucket.Objects(ctx, query)

	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return checkObjectStorageError(prefix, err)
		}

		obj := bucket.Object(objAttrs.Name)
		if err := obj.Delete(ctx); err != nil {
			return checkObjectStorageError(objAttrs.Name, err)
		}
	}

	return nil
}

func (gcs *GcpNativeObjectStorage) DeleteBucket(ctx context.Context, bucketName string) error {
	bucket := gcs.client.Bucket(bucketName)

	err := gcs.RemoveObject(ctx, bucketName, "")
	if err != nil {
		return err
	}

	err = bucket.Delete(ctx)
	if err != nil {
		return err
	}
	return nil
}

type GcsReader struct {
	reader   *storage.Reader
	obj      *storage.ObjectHandle
	position int64
}

func (gcsReader *GcsReader) Read(p []byte) (n int, err error) {
	n, err = gcsReader.reader.Read(p)
	if err != nil {
		return n, err
	}
	gcsReader.position = gcsReader.position + int64(n)
	return n, nil
}

func (gcsReader *GcsReader) Close() error {
	return gcsReader.reader.Close()
}

func (gcsReader *GcsReader) ReadAt(p []byte, off int64) (n int, err error) {
	reader, err := gcsReader.obj.NewRangeReader(context.Background(), off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	defer reader.Close()
	return io.ReadFull(reader, p)
}

func (gcsReader *GcsReader) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64

	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = gcsReader.position + offset
	case io.SeekEnd:
		objectAttrs, err := gcsReader.obj.Attrs(context.Background())
		if err != nil {
			return 0, err
		}
		newOffset = objectAttrs.Size + offset
	default:
		return 0, merr.WrapErrIoFailedReason("invalid whence")
	}

	if newOffset < 0 {
		return 0, merr.WrapErrIoFailedReason("negative offset")
	}

	// Reset the underlying reader to the new offset
	newReader, err := gcsReader.obj.NewRangeReader(context.Background(), newOffset, -1)
	if err != nil {
		if gErr, ok := err.(*googleapi.Error); ok {
			if gErr.Code == 416 {
				newReader, _ = gcsReader.obj.NewRangeReader(context.Background(), 0, 0)
			}
		} else {
			return 0, err
		}
	}

	if gcsReader.reader != nil {
		if err := gcsReader.reader.Close(); err != nil {
			return 0, err
		}
	}

	// Update the reader and the current position
	gcsReader.reader = newReader
	gcsReader.position = newOffset
	return newOffset, nil
}
