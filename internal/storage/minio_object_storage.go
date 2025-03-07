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

	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var _ ObjectStorage = (*MinioObjectStorage)(nil)

type MinioObjectStorage struct {
	*minio.Client
}

func newMinioObjectStorageWithConfig(ctx context.Context, c *objectstorage.Config) (*MinioObjectStorage, error) {
	minIOClient, err := objectstorage.NewMinioClient(ctx, c)
	if err != nil {
		return nil, err
	}
	return &MinioObjectStorage{minIOClient}, nil
}

func (minioObjectStorage *MinioObjectStorage) GetObject(ctx context.Context, bucketName, objectName string, offset int64, size int64) (FileReader, error) {
	opts := minio.GetObjectOptions{}
	if offset > 0 {
		err := opts.SetRange(offset, offset+size-1)
		if err != nil {
			log.Warn("failed to set range", zap.String("bucket", bucketName), zap.String("path", objectName), zap.Error(err))
			return nil, checkObjectStorageError(objectName, err)
		}
	}
	object, err := minioObjectStorage.Client.GetObject(ctx, bucketName, objectName, opts)
	if err != nil {
		return nil, checkObjectStorageError(objectName, err)
	}
	return object, nil
}

func (minioObjectStorage *MinioObjectStorage) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) error {
	_, err := minioObjectStorage.Client.PutObject(ctx, bucketName, objectName, reader, objectSize, minio.PutObjectOptions{})
	return checkObjectStorageError(objectName, err)
}

func (minioObjectStorage *MinioObjectStorage) StatObject(ctx context.Context, bucketName, objectName string) (int64, error) {
	info, err := minioObjectStorage.Client.StatObject(ctx, bucketName, objectName, minio.StatObjectOptions{})
	return info.Size, checkObjectStorageError(objectName, err)
}

func (minioObjectStorage *MinioObjectStorage) WalkWithObjects(ctx context.Context, bucketName string, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc) (err error) {
	// if minio has lots of objects under the provided path
	// recursive = true may timeout during the recursive browsing the objects.
	// See also: https://github.com/milvus-io/milvus/issues/19095
	// So we can change the `ListObjectsMaxKeys` to limit the max keys by batch to avoid timeout.
	in := minioObjectStorage.Client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: recursive,
		MaxKeys:   paramtable.Get().MinioCfg.ListObjectsMaxKeys.GetAsInt(),
	})

	for object := range in {
		if object.Err != nil {
			return object.Err
		}
		if !walkFunc(&ChunkObjectInfo{FilePath: object.Key, ModifyTime: object.LastModified}) {
			return nil
		}
	}
	return nil
}

func (minioObjectStorage *MinioObjectStorage) RemoveObject(ctx context.Context, bucketName, objectName string) error {
	err := minioObjectStorage.Client.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
	return checkObjectStorageError(objectName, err)
}
