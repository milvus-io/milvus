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
	"container/list"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/storage/aliyun"
	"github.com/milvus-io/milvus/internal/storage/gcp"
	"github.com/milvus-io/milvus/internal/util/merr"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
	"golang.org/x/exp/mmap"
)

var (
	ErrNoSuchKey = errors.New("NoSuchKey")
)

const (
	CloudProviderGCP    = "gcp"
	CloudProviderAWS    = "aws"
	CloudProviderAliyun = "aliyun"
)

func WrapErrNoSuchKey(key string) error {
	return fmt.Errorf("%w(key=%s)", ErrNoSuchKey, key)
}

var CheckBucketRetryAttempts uint = 20

// MinioChunkManager is responsible for read and write data stored in minio.
type MinioChunkManager struct {
	*minio.Client

	//	ctx        context.Context
	bucketName string
	rootPath   string
}

var _ ChunkManager = (*MinioChunkManager)(nil)

// NewMinioChunkManager create a new local manager object.
// Deprecated: Do not call this directly! Use factory.NewPersistentStorageChunkManager instead.
func NewMinioChunkManager(ctx context.Context, opts ...Option) (*MinioChunkManager, error) {
	c := newDefaultConfig()
	for _, opt := range opts {
		opt(c)
	}

	return newMinioChunkManagerWithConfig(ctx, c)
}

func newMinioChunkManagerWithConfig(ctx context.Context, c *config) (*MinioChunkManager, error) {
	var creds *credentials.Credentials
	var newMinioFn = minio.New
	var bucketLookupType = minio.BucketLookupAuto

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
	default: // aws, minio
		if c.useIAM {
			creds = credentials.NewIAM("")
		} else {
			creds = credentials.NewStaticV4(c.accessKeyID, c.secretAccessKeyID, "")
		}
	}
	minioOpts := &minio.Options{
		BucketLookup: bucketLookupType,
		Creds:        creds,
		Secure:       c.useSSL,
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

	mcm := &MinioChunkManager{
		Client:     minIOClient,
		bucketName: c.bucketName,
	}
	mcm.rootPath = mcm.normalizeRootPath(c.rootPath)
	log.Info("minio chunk manager init success.", zap.String("bucketname", c.bucketName), zap.String("root", mcm.RootPath()))
	return mcm, nil
}

// normalizeRootPath
func (mcm *MinioChunkManager) normalizeRootPath(rootPath string) string {
	// no leading "/"
	return strings.TrimLeft(rootPath, "/")
}

// SetVar set the variable value of mcm
func (mcm *MinioChunkManager) SetVar(bucketName string, rootPath string) {
	log.Info("minio chunkmanager ", zap.String("bucketName", bucketName), zap.String("rootpath", rootPath))
	mcm.bucketName = bucketName
	mcm.rootPath = rootPath
}

// RootPath returns minio root path.
func (mcm *MinioChunkManager) RootPath() string {
	return mcm.rootPath
}

// Path returns the path of minio data if exists.
func (mcm *MinioChunkManager) Path(ctx context.Context, filePath string) (string, error) {
	exist, err := mcm.Exist(ctx, filePath)
	if err != nil {
		return "", err
	}
	if !exist {
		return "", errors.New("minio file manage cannot be found with filePath:" + filePath)
	}
	return filePath, nil
}

// Reader returns the path of minio data if exists.
func (mcm *MinioChunkManager) Reader(ctx context.Context, filePath string) (FileReader, error) {
	reader, err := mcm.Client.GetObject(ctx, mcm.bucketName, filePath, minio.GetObjectOptions{})
	if err != nil {
		log.Warn("failed to get object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	return reader, nil
}

func (mcm *MinioChunkManager) Size(ctx context.Context, filePath string) (int64, error) {
	objectInfo, err := mcm.Client.StatObject(ctx, mcm.bucketName, filePath, minio.StatObjectOptions{})
	if err != nil {
		log.Warn("failed to stat object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return 0, err
	}

	return objectInfo.Size, nil
}

// Write writes the data to minio storage.
func (mcm *MinioChunkManager) Write(ctx context.Context, filePath string, content []byte) error {
	_, err := mcm.Client.PutObject(ctx, mcm.bucketName, filePath, bytes.NewReader(content), int64(len(content)), minio.PutObjectOptions{})

	if err != nil {
		log.Warn("failed to put object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return err
	}

	return nil
}

// MultiWrite saves multiple objects, the path is the key of @kvs.
// The object value is the value of @kvs.
func (mcm *MinioChunkManager) MultiWrite(ctx context.Context, kvs map[string][]byte) error {
	var el error
	for key, value := range kvs {
		err := mcm.Write(ctx, key, value)
		if err != nil {
			el = merr.Combine(el, errors.Wrapf(err, "failed to write %s", key))
		}
	}
	return el
}

// Exist checks whether chunk is saved to minio storage.
func (mcm *MinioChunkManager) Exist(ctx context.Context, filePath string) (bool, error) {
	_, err := mcm.Client.StatObject(ctx, mcm.bucketName, filePath, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return false, nil
		}
		log.Warn("failed to stat object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return false, err
	}
	return true, nil
}

// Read reads the minio storage data if exists.
func (mcm *MinioChunkManager) Read(ctx context.Context, filePath string) ([]byte, error) {
	object, err := mcm.Client.GetObject(ctx, mcm.bucketName, filePath, minio.GetObjectOptions{})
	if err != nil {
		log.Warn("failed to get object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	defer object.Close()

	// Prefetch object data
	var empty []byte
	_, err = object.Read(empty)
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return nil, WrapErrNoSuchKey(filePath)
		}
		log.Warn("failed to read object", zap.String("path", filePath), zap.Error(err))
		return nil, err
	}

	objectInfo, err := object.Stat()
	if err != nil {
		log.Warn("failed to stat object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return nil, WrapErrNoSuchKey(filePath)
		}
		return nil, err
	}

	data, err := Read(object, objectInfo.Size)
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return nil, WrapErrNoSuchKey(filePath)
		}
		log.Warn("failed to read object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	return data, nil
}

func (mcm *MinioChunkManager) MultiRead(ctx context.Context, keys []string) ([][]byte, error) {
	var el error
	var objectsValues [][]byte
	for _, key := range keys {
		objectValue, err := mcm.Read(ctx, key)
		if err != nil {
			el = merr.Combine(el, errors.Wrapf(err, "failed to read %s", key))
		}
		objectsValues = append(objectsValues, objectValue)
	}

	return objectsValues, el
}

func (mcm *MinioChunkManager) ReadWithPrefix(ctx context.Context, prefix string) ([]string, [][]byte, error) {
	objectsKeys, _, err := mcm.ListWithPrefix(ctx, prefix, true)
	if err != nil {
		return nil, nil, err
	}
	objectsValues, err := mcm.MultiRead(ctx, objectsKeys)
	if err != nil {
		return nil, nil, err
	}

	return objectsKeys, objectsValues, nil
}

func (mcm *MinioChunkManager) Mmap(ctx context.Context, filePath string) (*mmap.ReaderAt, error) {
	return nil, errors.New("this method has not been implemented")
}

// ReadAt reads specific position data of minio storage if exists.
func (mcm *MinioChunkManager) ReadAt(ctx context.Context, filePath string, off int64, length int64) ([]byte, error) {
	if off < 0 || length < 0 {
		return nil, io.EOF
	}

	opts := minio.GetObjectOptions{}
	err := opts.SetRange(off, off+length-1)
	if err != nil {
		log.Warn("failed to set range", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return nil, err
	}

	object, err := mcm.Client.GetObject(ctx, mcm.bucketName, filePath, opts)
	if err != nil {
		log.Warn("failed to get object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	defer object.Close()

	data, err := Read(object, length)
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return nil, WrapErrNoSuchKey(filePath)
		}
		log.Warn("failed to read object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	return data, nil
}

// Remove deletes an object with @key.
func (mcm *MinioChunkManager) Remove(ctx context.Context, filePath string) error {
	err := mcm.Client.RemoveObject(ctx, mcm.bucketName, filePath, minio.RemoveObjectOptions{})
	if err != nil {
		log.Warn("failed to remove object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return err
	}
	return nil
}

// MultiRemove deletes a objects with @keys.
func (mcm *MinioChunkManager) MultiRemove(ctx context.Context, keys []string) error {
	var el error
	for _, key := range keys {
		err := mcm.Remove(ctx, key)
		if err != nil {
			el = merr.Combine(el, errors.Wrapf(err, "failed to remove %s", key))
		}
	}
	return el
}

// RemoveWithPrefix removes all objects with the same prefix @prefix from minio.
func (mcm *MinioChunkManager) RemoveWithPrefix(ctx context.Context, prefix string) error {
	objects := mcm.Client.ListObjects(ctx, mcm.bucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})
	for rErr := range mcm.Client.RemoveObjects(ctx, mcm.bucketName, objects, minio.RemoveObjectsOptions{GovernanceBypass: false}) {
		if rErr.Err != nil {
			log.Warn("failed to remove objects", zap.String("bucket", mcm.bucketName), zap.String("prefix", prefix), zap.Error(rErr.Err))
			return rErr.Err
		}
	}
	return nil
}

// ListWithPrefix returns objects with provided prefix.
// by default, if `recursive`=false, list object with return object with path under save level
// say minio has followinng objects: [a, ab, a/b, ab/c]
// calling `ListWithPrefix` with `prefix` = a && `recursive` = false will only returns [a, ab]
// If caller needs all objects without level limitation, `recursive` shall be true.
func (mcm *MinioChunkManager) ListWithPrefix(ctx context.Context, prefix string, recursive bool) ([]string, []time.Time, error) {

	// cannot use ListObjects(ctx, bucketName, Opt{Prefix:prefix, Recursive:true})
	// if minio has lots of objects under the provided path
	// recursive = true may timeout during the recursive browsing the objects.
	// See also: https://github.com/milvus-io/milvus/issues/19095

	var objectsKeys []string
	var modTimes []time.Time

	tasks := list.New()
	tasks.PushBack(prefix)
	for tasks.Len() > 0 {
		e := tasks.Front()
		pre := e.Value.(string)
		tasks.Remove(e)

		// TODO add concurrent call if performance matters
		// only return current level per call
		objects := mcm.Client.ListObjects(ctx, mcm.bucketName, minio.ListObjectsOptions{Prefix: pre, Recursive: false})

		for object := range objects {
			if object.Err != nil {
				log.Warn("failed to list with prefix", zap.String("bucket", mcm.bucketName), zap.String("prefix", prefix), zap.Error(object.Err))
				return nil, nil, object.Err
			}

			// with tailing "/", object is a "directory"
			if strings.HasSuffix(object.Key, "/") && recursive {
				// enqueue when recursive is true
				if object.Key != pre {
					tasks.PushBack(object.Key)
				}
				continue
			}
			objectsKeys = append(objectsKeys, object.Key)
			modTimes = append(modTimes, object.LastModified)
		}
	}

	return objectsKeys, modTimes, nil
}

// Learn from file.ReadFile
func Read(r io.Reader, size int64) ([]byte, error) {
	data := make([]byte, 0, size)
	for {
		n, err := r.Read(data[len(data):cap(data)])
		data = data[:len(data)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return data, err
		}
		if len(data) == cap(data) {
			return data, nil
		}
	}
}
