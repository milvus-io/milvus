// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific
// language governing permissions and limitations under the License.

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

var _ ObjectStorage = (*RustFSObjectStorage)(nil)

// RustFSObjectStorage is the RustFS implementation of ObjectStorage interface.
// RustFS is a high-performance S3-compatible object storage system built with Rust.
// It uses the minio-go SDK for S3 API compatibility.
type RustFSObjectStorage struct {
	*minio.Client
	bucketName string
}

// RustFSObjectReader wraps minio.Object for RustFS object reading.
type RustFSObjectReader struct {
	*minio.Object
}

// Size returns the size of the object.
func (rr *RustFSObjectReader) Size() (int64, error) {
	stat, err := rr.Stat()
	if err != nil {
		return -1, err
	}
	return stat.Size, nil
}

// newRustFSObjectStorageWithConfig creates a new RustFS object storage client.
// It uses the minio-go SDK to connect to RustFS via its S3-compatible API.
func newRustFSObjectStorageWithConfig(ctx context.Context, c *objectstorage.Config) (*RustFSObjectStorage, error) {
	// Create minio client which will connect to RustFS using S3 protocol
	minioClient, err := objectstorage.NewMinioClient(ctx, c)
	if err != nil {
		return nil, err
	}

	rustfs := &RustFSObjectStorage{
		Client:     minioClient,
		bucketName: c.BucketName,
	}

	log.Info("RustFS object storage initialized",
		zap.String("address", c.Address),
		zap.String("bucket", c.BucketName),
		zap.String("rootPath", c.RootPath))

	return rustfs, nil
}

// GetObject retrieves an object from RustFS with optional offset and size.
func (rs *RustFSObjectStorage) GetObject(ctx context.Context, bucketName, objectName string, offset int64, size int64) (FileReader, error) {
	opts := minio.GetObjectOptions{}
	if offset > 0 {
		err := opts.SetRange(offset, offset+size-1)
		if err != nil {
			log.Warn("failed to set range for RustFS",
				zap.String("bucket", bucketName),
				zap.String("path", objectName),
				zap.Error(err))
			return nil, mapObjectStorageError(objectName, err)
		}
	}

	object, err := rs.Client.GetObject(ctx, bucketName, objectName, opts)
	if err != nil {
		return nil, mapObjectStorageError(objectName, err)
	}

	return &RustFSObjectReader{Object: object}, nil
}

// PutObject stores an object in RustFS.
func (rs *RustFSObjectStorage) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) error {
	// RustFS-specific optimizations can be added here in the future
	_, err := rs.Client.PutObject(ctx, bucketName, objectName, reader, objectSize, minio.PutObjectOptions{})
	return mapObjectStorageError(objectName, err)
}

// StatObject returns the size of an object in RustFS.
func (rs *RustFSObjectStorage) StatObject(ctx context.Context, bucketName, objectName string) (int64, error) {
	info, err := rs.Client.StatObject(ctx, bucketName, objectName, minio.StatObjectOptions{})
	return info.Size, mapObjectStorageError(objectName, err)
}

// WalkWithObjects lists objects with a given prefix in RustFS.
func (rs *RustFSObjectStorage) WalkWithObjects(ctx context.Context, bucketName string, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc) error {
	in := rs.Client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
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

// RemoveObject deletes an object from RustFS.
func (rs *RustFSObjectStorage) RemoveObject(ctx context.Context, bucketName, objectName string) error {
	err := rs.Client.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
	return mapObjectStorageError(objectName, err)
}

// CopyObject copies an object within RustFS.
func (rs *RustFSObjectStorage) CopyObject(ctx context.Context, bucketName, srcObjectName, dstObjectName string) error {
	srcOpts := minio.CopySrcOptions{
		Bucket: bucketName,
		Object: srcObjectName,
	}
	dstOpts := minio.CopyDestOptions{
		Bucket: bucketName,
		Object: dstObjectName,
	}
	_, err := rs.Client.CopyObject(ctx, dstOpts, srcOpts)
	return mapObjectStorageError(srcObjectName, err)
}
