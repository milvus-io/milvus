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
	"container/list"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/storage/aliyun"
	"github.com/milvus-io/milvus/internal/storage/gcp"
	"github.com/milvus-io/milvus/internal/storage/tencent"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

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
			creds = credentials.NewStaticV4(c.accessKeyID, c.secretAccessKeyID, "IQoJb3JpZ2luX2VjEBIaCXVzLWVhc3QtMSJGMEQCIB0FdalRHrGIk6jB0icOweZ1SZlnsww8eoX4Ze/Jl8LsAiBrsuKYK75AJPQywHswG4i7iTqcGSr5WzeNidEUoIWb2iqpAwi7//////////8BEAQaDDI0MDg1MjU4ODU3OCIMFQEJeJLiLC1eFzlAKv0C7AmeOhB/lICGRyjOn6GUfpKTYyteIwxCc0WhlfK1+C6w8HLrlKDMMZmy8B+XIUyD85gxfWRDkGQ0MXh7GBxwZcAk0wLIfNJIWnOC4x143VkfDUsY1Z+Miwc8NjrXxr8XshGx1AEqN567SUsdFWWMtbYYiKk+luAEVR7F4p5XBtCTvPKRK74RUqeW/AdiUkbo+vB/c7qedph12M1B76I592DMlpywpncc3vXA4WPSwfhNgf5i9EuKmAOFmMIaEVk8cMMzGuHXvAWDuBVdeJLEQ1HEAaRZ3B6BjbG6ivjlKCkaxL1Ksddgp1LLIQMF06TGrW8NbWq+9Dpqjw+Ia1w/1PusegxG8fClBrhMFwVCkgMGgbE2MtNKOQW1KpjUhY3MzXnRexAK/S2rVYiDrVPE2rL2luux0rUWtja5vDryjHQyGNSzxw6SY+x4I0PoRJh1MOJCZFDvN7I1keohgf0WrEQbkvej9UbcIWO1ZDvSd/jUL0Ql5xfD9jm/D00qMNKf6rMGOt8BgKxEXpuUAc0l2VpfcyGWsrABoKQtlAcM1731i23K4sd26tkLrIkwkUuDpzTXSyrs13eHZNNuZcpKRbetfnclXWnbebX/iZSsKWuC73B+yCvFXSA17MdYVVh7cANkDpBx8MaokkccVlyL4QTsaCWWTsdZ7wNnsJ59hg5fneCgzRuPzTjgtxOEG9hlrs3o1BvF8toIzmk0pjESF8Dwg8hZZvrZI1KrcgaleaOuqZX28/+dFMAd7PYHk0MEVkIqB46Hev5Hmp5FA8cEh7nWyLkL98jb9o4HokS6BR9oMcUGMA==")
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
				log.Info("blob bucket not exist, create bucket.", zap.Any("bucket name", c.bucketName))
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
			return nil, err
		}
	}
	object, err := minioObjectStorage.Client.GetObject(ctx, bucketName, objectName, opts)
	if err != nil {
		return nil, err
	}
	return object, nil
}

func (minioObjectStorage *MinioObjectStorage) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) error {
	_, err := minioObjectStorage.Client.PutObject(ctx, bucketName, objectName, reader, objectSize, minio.PutObjectOptions{})
	return err
}

func (minioObjectStorage *MinioObjectStorage) StatObject(ctx context.Context, bucketName, objectName string) (int64, error) {
	info, err := minioObjectStorage.Client.StatObject(ctx, bucketName, objectName, minio.StatObjectOptions{})
	return info.Size, err
}

func (minioObjectStorage *MinioObjectStorage) ListObjects(ctx context.Context, bucketName string, prefix string, recursive bool) ([]string, []time.Time, error) {
	var objectsKeys []string
	var modTimes []time.Time
	tasks := list.New()
	tasks.PushBack(prefix)
	for tasks.Len() > 0 {
		e := tasks.Front()
		pre := e.Value.(string)
		tasks.Remove(e)

		res := minioObjectStorage.Client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
			Prefix:    pre,
			Recursive: false,
		})

		objects := map[string]time.Time{}
		for object := range res {
			if object.Err != nil {
				log.Warn("failed to list with prefix", zap.String("bucket", bucketName), zap.String("prefix", prefix), zap.Error(object.Err))
				return []string{}, []time.Time{}, object.Err
			}
			objects[object.Key] = object.LastModified
		}
		for object, lastModified := range objects {
			// with tailing "/", object is a "directory"
			if strings.HasSuffix(object, "/") && recursive {
				// enqueue when recursive is true
				if object != pre {
					tasks.PushBack(object)
				}
				continue
			}
			objectsKeys = append(objectsKeys, object)
			modTimes = append(modTimes, lastModified)
		}
	}
	return objectsKeys, modTimes, nil
}

func (minioObjectStorage *MinioObjectStorage) RemoveObject(ctx context.Context, bucketName, objectName string) error {
	return minioObjectStorage.Client.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
}
