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
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/errorutil"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
	"golang.org/x/exp/mmap"
)

// MinioChunkManager is responsible for read and write data stored in minio.
type MinioChunkManager struct {
	*minio.Client

	ctx        context.Context
	bucketName string
}

var _ ChunkManager = (*MinioChunkManager)(nil)

// NewMinioChunkManager create a new local manager object.
// Deprecated: Do not call this directly! Use factory.NewVectorStorageChunkManager instead.
func NewMinioChunkManager(ctx context.Context, opts ...Option) (*MinioChunkManager, error) {
	c := newDefaultConfig()
	for _, opt := range opts {
		opt(c)
	}

	return newMinioChunkManagerWithConfig(ctx, c)
}

func newMinioChunkManagerWithConfig(ctx context.Context, c *config) (*MinioChunkManager, error) {
	var creds *credentials.Credentials
	if c.useIAM {
		creds = credentials.NewIAM(c.iamEndpoint)
	} else {
		creds = credentials.NewStaticV4(c.accessKeyID, c.secretAccessKeyID, "")
	}
	minIOClient, err := minio.New(c.address, &minio.Options{
		Creds:  creds,
		Secure: c.useSSL,
	})
	// options nil or invalid formatted endpoint, don't need to retry
	if err != nil {
		return nil, err
	}
	var bucketExists bool
	// check valid in first query
	checkBucketFn := func() error {
		bucketExists, err = minIOClient.BucketExists(ctx, c.bucketName)
		if err != nil {
			errResponse := minio.ToErrorResponse(err)
			log.Warn("failed to check blob bucket exist", zap.String("bucket", c.bucketName), zap.String("requestID", errResponse.RequestID), zap.Error(err))
			return err
		}
		if !bucketExists {
			if c.createBucket {
				log.Info("blob bucket not exist, create bucket.", zap.Any("bucket name", c.bucketName))
				err := minIOClient.MakeBucket(ctx, c.bucketName, minio.MakeBucketOptions{})
				if err != nil {
					errResponse := minio.ToErrorResponse(err)
					log.Warn("failed to create blob bucket", zap.String("bucket", c.bucketName), zap.String("requestID", errResponse.RequestID), zap.Error(err))
					return err
				}
			} else {
				return fmt.Errorf("bucket %s not Existed", c.bucketName)
			}
		}
		return nil
	}
	err = retry.Do(ctx, checkBucketFn, retry.Attempts(20))
	if err != nil {
		return nil, err
	}

	mcm := &MinioChunkManager{
		ctx:        ctx,
		Client:     minIOClient,
		bucketName: c.bucketName,
	}
	log.Info("minio chunk manager init success.", zap.String("bucketname", c.bucketName), zap.String("root", c.rootPath))
	return mcm, nil
}

// SetVar set the variable value of mcm
func (mcm *MinioChunkManager) SetVar(ctx context.Context, bucketName string) {
	mcm.ctx = ctx
	mcm.bucketName = bucketName
}

// Path returns the path of minio data if exists.
func (mcm *MinioChunkManager) Path(filePath string) (string, error) {
	exist, err := mcm.Exist(filePath)
	if err != nil {
		return "", err
	}
	if !exist {
		return "", errors.New("minio file manage cannot be found with filePath:" + filePath)
	}
	return filePath, nil
}

// Reader returns the path of minio data if exists.
func (mcm *MinioChunkManager) Reader(filePath string) (FileReader, error) {
	reader, err := mcm.Client.GetObject(mcm.ctx, mcm.bucketName, filePath, minio.GetObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		log.Warn("failed to get object", zap.String("path", filePath), zap.String("requestID", errResponse.RequestID), zap.Error(err))
		return nil, err
	}
	return reader, nil
}

func (mcm *MinioChunkManager) Size(filePath string) (int64, error) {
	objectInfo, err := mcm.Client.StatObject(mcm.ctx, mcm.bucketName, filePath, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		log.Warn("failed to stat object", zap.String("path", filePath), zap.String("requestID", errResponse.RequestID), zap.Error(err))
		return 0, err
	}

	return objectInfo.Size, nil
}

// Write writes the data to minio storage.
func (mcm *MinioChunkManager) Write(filePath string, content []byte) error {
	_, err := mcm.Client.PutObject(mcm.ctx, mcm.bucketName, filePath, bytes.NewReader(content), int64(len(content)), minio.PutObjectOptions{})

	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		log.Warn("failed to put object", zap.String("path", filePath), zap.String("requestID", errResponse.RequestID), zap.Error(err))
		return err
	}

	return nil
}

// MultiWrite saves multiple objects, the path is the key of @kvs.
// The object value is the value of @kvs.
func (mcm *MinioChunkManager) MultiWrite(kvs map[string][]byte) error {
	var el errorutil.ErrorList
	for key, value := range kvs {
		err := mcm.Write(key, value)
		if err != nil {
			el = append(el, err)
		}
	}
	if len(el) == 0 {
		return nil
	}
	return el
}

// Exist checks whether chunk is saved to minio storage.
func (mcm *MinioChunkManager) Exist(filePath string) (bool, error) {
	_, err := mcm.Client.StatObject(mcm.ctx, mcm.bucketName, filePath, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return false, nil
		}
		log.Warn("failed to stat object", zap.String("path", filePath), zap.String("requestID", errResponse.RequestID), zap.Error(err))
		return false, err
	}
	return true, nil
}

// Read reads the minio storage data if exists.
func (mcm *MinioChunkManager) Read(filePath string) ([]byte, error) {
	object, err := mcm.Client.GetObject(mcm.ctx, mcm.bucketName, filePath, minio.GetObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		log.Warn("failed to get object", zap.String("path", filePath), zap.String("requestID", errResponse.RequestID), zap.Error(err))
		return nil, err
	}
	defer object.Close()

	data, err := ioutil.ReadAll(object)
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return nil, errors.New("NoSuchKey")
		}
		log.Warn("failed to read object", zap.String("path", filePath), zap.String("requestID", errResponse.RequestID), zap.Error(err))
		return nil, err
	}
	return data, nil
}

func (mcm *MinioChunkManager) MultiRead(keys []string) ([][]byte, error) {
	var el errorutil.ErrorList
	var objectsValues [][]byte
	for _, key := range keys {
		objectValue, err := mcm.Read(key)
		if err != nil {
			el = append(el, err)
		}
		objectsValues = append(objectsValues, objectValue)
	}

	if len(el) == 0 {
		return objectsValues, nil
	}
	return objectsValues, el
}

func (mcm *MinioChunkManager) ReadWithPrefix(prefix string) ([]string, [][]byte, error) {
	objectsKeys, _, err := mcm.ListWithPrefix(prefix, true)
	if err != nil {
		return nil, nil, err
	}
	objectsValues, err := mcm.MultiRead(objectsKeys)
	if err != nil {
		return nil, nil, err
	}

	return objectsKeys, objectsValues, nil
}

func (mcm *MinioChunkManager) Mmap(filePath string) (*mmap.ReaderAt, error) {
	return nil, errors.New("this method has not been implemented")
}

// ReadAt reads specific position data of minio storage if exists.
func (mcm *MinioChunkManager) ReadAt(filePath string, off int64, length int64) ([]byte, error) {
	if off < 0 || length < 0 {
		return nil, io.EOF
	}

	opts := minio.GetObjectOptions{}
	err := opts.SetRange(off, off+length-1)
	if err != nil {
		log.Warn("failed to set range", zap.String("path", filePath), zap.Error(err))
		return nil, err
	}

	object, err := mcm.Client.GetObject(mcm.ctx, mcm.bucketName, filePath, opts)
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		log.Warn("failed to get object", zap.String("path", filePath), zap.String("requestID", errResponse.RequestID), zap.Error(err))
		return nil, err
	}
	defer object.Close()
	data, err := ioutil.ReadAll(object)
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		log.Warn("failed to read object", zap.String("path", filePath), zap.String("requestID", errResponse.RequestID), zap.Error(err))
		return nil, err
	}
	return data, nil
}

// Remove deletes an object with @key.
func (mcm *MinioChunkManager) Remove(filePath string) error {
	err := mcm.Client.RemoveObject(mcm.ctx, mcm.bucketName, filePath, minio.RemoveObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		log.Warn("failed to remove object", zap.String("path", filePath), zap.String("requestID", errResponse.RequestID), zap.Error(err))
		return err
	}
	return nil
}

// MultiRemove deletes a objects with @keys.
func (mcm *MinioChunkManager) MultiRemove(keys []string) error {
	var el errorutil.ErrorList
	for _, key := range keys {
		err := mcm.Remove(key)
		if err != nil {
			el = append(el, err)
		}
	}
	if len(el) == 0 {
		return nil
	}
	return el
}

// RemoveWithPrefix removes all objects with the same prefix @prefix from minio.
func (mcm *MinioChunkManager) RemoveWithPrefix(prefix string) error {
	objects := mcm.Client.ListObjects(mcm.ctx, mcm.bucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})
	for rErr := range mcm.Client.RemoveObjects(mcm.ctx, mcm.bucketName, objects, minio.RemoveObjectsOptions{GovernanceBypass: false}) {
		if rErr.Err != nil {
			errResponse := minio.ToErrorResponse(rErr.Err)
			log.Warn("failed to remove objects", zap.String("prefix", prefix), zap.String("requestID", errResponse.RequestID), zap.Error(rErr.Err))
			return rErr.Err
		}
	}
	return nil
}

func (mcm *MinioChunkManager) ListWithPrefix(prefix string, recursive bool) ([]string, []time.Time, error) {
	objects := mcm.Client.ListObjects(mcm.ctx, mcm.bucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: recursive})
	var objectsKeys []string
	var modTimes []time.Time

	for object := range objects {
		if object.Err != nil {
			errResponse := minio.ToErrorResponse(object.Err)
			log.Warn("failed to list with prefix", zap.String("prefix", prefix), zap.String("requestID", errResponse.RequestID), zap.Error(object.Err))
			return nil, nil, object.Err
		}
		objectsKeys = append(objectsKeys, object.Key)
		modTimes = append(modTimes, object.LastModified)
	}
	return objectsKeys, modTimes, nil
}
