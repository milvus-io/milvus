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
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/storage/aliyun"
	"github.com/milvus-io/milvus/internal/storage/gcp"
	"github.com/milvus-io/milvus/internal/storage/tencent"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
)

var CheckBucketRetryAttempts uint = 20

var _ ObjectStorage = (*MinioObjectStorage)(nil)

type MinioObjectStorage struct {
	*minio.Client
}

func newMinioClient(ctx context.Context, c *config) (*minio.Client, error) {
	var creds *credentials.Credentials
	newMinioFn := minio.New
	bucketLookupType := minio.BucketLookupAuto

	if c.useVirtualHost {
		bucketLookupType = minio.BucketLookupDNS
	}

	matchedDefault := false
	switch c.cloudProvider {
	case CloudProviderAliyun:
		// auto doesn't work for aliyun, so we set to dns deliberately
		bucketLookupType = minio.BucketLookupDNS
		if c.useIAM {
			newMinioFn = aliyun.NewMinioClient
		} else {
			creds = credentials.NewStaticV4(c.accessKeyID, c.secretAccessKeyID, "")
		}
	case CloudProviderGCP:
		newMinioFn = gcp.NewMinioClient
		if !c.useIAM {
			creds = credentials.NewStaticV2(c.accessKeyID, c.secretAccessKeyID, "")
		}
	case CloudProviderTencent:
		bucketLookupType = minio.BucketLookupDNS
		newMinioFn = tencent.NewMinioClient
		if !c.useIAM {
			creds = credentials.NewStaticV4(c.accessKeyID, c.secretAccessKeyID, "")
		}

	default: // aws, minio
		matchedDefault = true
	}

	// Compatibility logic. If the cloud provider is not specified in the request,
	// it shall be inferred based on the service address.
	if matchedDefault {
		matchedDefault = false
		switch {
		case strings.Contains(c.address, gcp.GcsDefaultAddress):
			newMinioFn = gcp.NewMinioClient
			if !c.useIAM {
				creds = credentials.NewStaticV2(c.accessKeyID, c.secretAccessKeyID, "")
			}
		case strings.Contains(c.address, aliyun.OSSAddressFeatureString):
			// auto doesn't work for aliyun, so we set to dns deliberately
			bucketLookupType = minio.BucketLookupDNS
			if c.useIAM {
				newMinioFn = aliyun.NewMinioClient
			} else {
				creds = credentials.NewStaticV4(c.accessKeyID, c.secretAccessKeyID, "")
			}
		default:
			matchedDefault = true
		}
	}

	if matchedDefault {
		// aws, minio
		if c.useIAM {
			creds = credentials.NewIAM("")
		} else {
			creds = credentials.NewStaticV4(c.accessKeyID, c.secretAccessKeyID, "")
		}
	}

	// We must set the cert path by os environment variable "SSL_CERT_FILE",
	// because the minio.DefaultTransport() need this path to read the file content,
	// we shouldn't read this file by ourself.
	if c.useSSL && len(c.sslCACert) > 0 {
		err := os.Setenv("SSL_CERT_FILE", c.sslCACert)
		if err != nil {
			return nil, err
		}
	}

	minioOpts := &minio.Options{
		BucketLookup: bucketLookupType,
		Creds:        creds,
		Secure:       c.useSSL,
		Region:       c.region,
	}
	minIOClient, err := newMinioFn(c.address, minioOpts)
	// options nil or invalid formatted endpoint, don't need to retry
	if err != nil {
		return nil, err
	}
	var bucketExists bool
	// check valid in first query
	checkBucketFn := func() error {
		bucketExists, err = minIOClient.BucketExists(ctx, c.bucketName)
		if err != nil {
			log.Warn("failed to check blob bucket exist", zap.String("bucket", c.bucketName), zap.Error(err))
			return err
		}
		if !bucketExists {
			if c.createBucket {
				log.Info("blob bucket not exist, create bucket.", zap.String("bucket name", c.bucketName))
				err := minIOClient.MakeBucket(ctx, c.bucketName, minio.MakeBucketOptions{})
				if err != nil {
					log.Warn("failed to create blob bucket", zap.String("bucket", c.bucketName), zap.Error(err))
					return err
				}
			} else {
				return fmt.Errorf("bucket %s not Existed", c.bucketName)
			}
		}
		return nil
	}
	err = retry.Do(ctx, checkBucketFn, retry.Attempts(CheckBucketRetryAttempts))
	if err != nil {
		return nil, err
	}
	return minIOClient, nil
}

func newMinioObjectStorageWithConfig(ctx context.Context, c *config) (*MinioObjectStorage, error) {
	minIOClient, err := newMinioClient(ctx, c)
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
