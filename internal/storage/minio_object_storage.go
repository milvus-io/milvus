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
	"net/http"
	"strings"

	"github.com/minio/minio-go/v7"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var _ ObjectStorage = (*MinioObjectStorage)(nil)

// minio-go caps a single CopyObject request at 5 GiB, regardless of the configured threshold.
const minioSingleCopyObjectMaxSize = 5 * 1024 * 1024 * 1024

type MinioObjectStorage struct {
	*minio.Client

	cloudProvider string
}

type ObjectReader struct {
	*minio.Object
}

func (or *ObjectReader) Size() (int64, error) {
	stat, err := or.Stat()
	if err != nil {
		return -1, err
	}
	return stat.Size, nil
}

func newMinioObjectStorageWithConfig(ctx context.Context, c *objectstorage.Config) (*MinioObjectStorage, error) {
	resolvedConfig := *c
	resolvedConfig.CloudProvider = objectstorage.ResolveCloudProvider(c)
	minIOClient, err := objectstorage.NewMinioClient(ctx, &resolvedConfig)
	if err != nil {
		return nil, err
	}
	return &MinioObjectStorage{Client: minIOClient, cloudProvider: resolvedConfig.CloudProvider}, nil
}

func (minioObjectStorage *MinioObjectStorage) GetObject(ctx context.Context, bucketName, objectName string, offset int64, size int64) (FileReader, error) {
	opts := minio.GetObjectOptions{}
	if offset > 0 {
		end := int64(0)
		if size > 0 {
			end = offset + size - 1
		}
		err := opts.SetRange(offset, end)
		if err != nil {
			mlog.Warn(ctx, "failed to set range", mlog.String("bucket", bucketName), mlog.String("path", objectName), mlog.Err(err))
			return nil, mapObjectStorageError(objectName, err)
		}
	}
	object, err := minioObjectStorage.Client.GetObject(ctx, bucketName, objectName, opts)
	if err != nil {
		return nil, mapObjectStorageError(objectName, err)
	}
	return &ObjectReader{
		Object: object,
	}, nil
}

func (minioObjectStorage *MinioObjectStorage) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) error {
	_, err := minioObjectStorage.Client.PutObject(ctx, bucketName, objectName, reader, objectSize, minioObjectStorage.putObjectOptions())
	return mapObjectStorageError(objectName, err)
}

func (minioObjectStorage *MinioObjectStorage) putObjectOptions() minio.PutObjectOptions {
	if !paramtable.Get().MinioCfg.DisableAWSChunkedEncoding.GetAsBool() {
		return minio.PutObjectOptions{}
	}
	return minio.PutObjectOptions{
		DisableContentSha256: true,
	}
}

func (minioObjectStorage *MinioObjectStorage) StatObject(ctx context.Context, bucketName, objectName string) (int64, error) {
	info, err := minioObjectStorage.Client.StatObject(ctx, bucketName, objectName, minio.StatObjectOptions{})
	return info.Size, mapObjectStorageError(objectName, err)
}

func (minioObjectStorage *MinioObjectStorage) WalkWithObjects(ctx context.Context, bucketName string, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc) (err error) {
	// if minio has lots of objects under the provided path
	// recursive = true may timeout during the recursive browsing the objects.
	// See also: https://github.com/milvus-io/milvus/issues/19095
	// So we can change the `ListObjectsMaxKeys` to limit the max keys by batch to avoid timeout.
	in := minioObjectStorage.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: recursive,
		MaxKeys:   paramtable.Get().MinioCfg.ListObjectsMaxKeys.GetAsInt(),
	})

	for object := range in {
		if object.Err != nil {
			return mapObjectStorageError(prefix, object.Err)
		}
		if !walkFunc(&ChunkObjectInfo{FilePath: object.Key, ModifyTime: object.LastModified}) {
			return nil
		}
	}
	return nil
}

func (minioObjectStorage *MinioObjectStorage) RemoveObject(ctx context.Context, bucketName, objectName string) error {
	err := minioObjectStorage.Client.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
	if isDeleteSucceededWithOK(err) {
		return nil
	}
	return mapObjectStorageError(objectName, err)
}

// isDeleteSucceededWithOK reports whether a RemoveObject error only says that the
// store answered DeleteObject with HTTP 200. The S3 API specifies 204 for a
// successful delete and minio-go reports every other status as an error, but
// some S3-compatible stores answer 200 and have deleted the object. A
// conforming store never answers 200 here, so this changes nothing for them.
func isDeleteSucceededWithOK(err error) bool {
	if err == nil {
		return false
	}
	return minio.ToErrorResponse(err).StatusCode == http.StatusOK
}

func (minioObjectStorage *MinioObjectStorage) CopyObjectCrossBucket(ctx context.Context, srcBucket, srcObjectName, dstBucket, dstObjectName string) error {
	_, err := minioObjectStorage.CopyObjectCrossBucketAndGetSize(ctx, srcBucket, srcObjectName, dstBucket, dstObjectName)
	return err
}

// CopyObjectCrossBucketAndGetSize discovers the immutable source size at copy time.
func (minioObjectStorage *MinioObjectStorage) CopyObjectCrossBucketAndGetSize(ctx context.Context, srcBucket, srcObjectName, dstBucket, dstObjectName string) (int64, error) {
	threshold := paramtable.Get().MinioCfg.MultipartCopyThreshold.GetAsInt64()
	partSize := paramtable.Get().MinioCfg.MultipartCopyPartSize.GetAsInt64()
	srcInfo, err := minioObjectStorage.Client.StatObject(ctx, srcBucket, srcObjectName, minio.StatObjectOptions{})
	if err != nil {
		return 0, mapObjectStorageError(srcObjectName, err)
	}
	if srcInfo.Size < 0 {
		return 0, merr.WrapErrDataIntegrityMsg("source object size cannot be negative")
	}
	srcOpts := minio.CopySrcOptions{Bucket: srcBucket, Object: srcObjectName}
	dstOpts := minio.CopyDestOptions{Bucket: dstBucket, Object: dstObjectName}
	// GCS's XML API has no multipart copy: x-amz-copy-source-range (emitted by
	// UploadPartCopy) has no x-goog-* equivalent. Its whole-object copy has no
	// 5GiB cap though, so GCP always takes the single-copy path.
	multipart := srcInfo.Size >= max(threshold, minioCopyPartSize) || srcInfo.Size > minioSingleCopyObjectMaxSize
	if !multipart || minioObjectStorage.cloudProvider == objectstorage.CloudProviderGCP {
		// minio-go v7.0.73 Client.CopyObject returns the ETag from the HTTP
		// header, but S3 puts it in the XML body. Core.CopyObject reads the
		// body correctly, so use it without upgrading the shared SDK.
		core := minio.Core{Client: minioObjectStorage.Client}
		var result minio.ObjectInfo
		result, err = core.CopyObject(ctx, srcBucket, srcObjectName, dstBucket, dstObjectName, nil, srcOpts, minio.PutObjectOptions{})
		// Both SDK paths can decode HTTP 200 <Error> as an empty result.
		// Require the body ETag before reporting success or counting bytes;
		// an empty ETag does not confirm a copy and remains a retriable I/O error.
		if err == nil && strings.TrimSpace(result.ETag) == "" {
			return 0, merr.WrapErrIoFailedMsg("single-object copy returned an empty ETag")
		}
	} else {
		err = minioObjectStorage.copyObjectMultipart(ctx, dstOpts, srcOpts, srcInfo, partSize)
	}
	if err != nil {
		return 0, mapObjectStorageError(srcObjectName, err)
	}
	return srcInfo.Size, nil
}
