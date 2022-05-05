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

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
	"golang.org/x/exp/mmap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/errorutil"
	"github.com/milvus-io/milvus/internal/util/retry"
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
	minIOClient, err := minio.New(c.address, &minio.Options{
		Creds:  credentials.NewStaticV4(c.accessKeyID, c.secretAccessKeyID, ""),
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
			return err
		}
		if !bucketExists {
			log.Debug("minio chunk manager new minio client", zap.Any("Check bucket", "bucket not exist"))
			if c.createBucket {
				log.Debug("minio chunk manager create minio bucket.", zap.Any("bucket name", c.bucketName))
				return minIOClient.MakeBucket(ctx, c.bucketName, minio.MakeBucketOptions{})
			}
			return fmt.Errorf("bucket %s not Existed", c.bucketName)
		}
		return nil
	}
	err = retry.Do(ctx, checkBucketFn, retry.Attempts(100))
	if err != nil {
		return nil, err
	}

	mcm := &MinioChunkManager{
		ctx:        ctx,
		Client:     minIOClient,
		bucketName: c.bucketName,
	}
	log.Debug("minio chunk manager new minio client success.")

	return mcm, nil
}

// Path returns the path of minio data if exists.
func (mcm *MinioChunkManager) Path(filePath string) (string, error) {
	if !mcm.Exist(filePath) {
		return "", errors.New("minio file manage cannot be found with filePath:" + filePath)
	}
	return filePath, nil
}

// Reader returns the path of minio data if exists.
func (mcm *MinioChunkManager) Reader(filePath string) (FileReader, error) {
	if !mcm.Exist(filePath) {
		return nil, errors.New("minio file manage cannot be found with filePath:" + filePath)
	}
	return mcm.Client.GetObject(mcm.ctx, mcm.bucketName, filePath, minio.GetObjectOptions{})
}

func (mcm *MinioChunkManager) Size(filePath string) (int64, error) {
	objectInfo, err := mcm.Client.StatObject(mcm.ctx, mcm.bucketName, filePath, minio.StatObjectOptions{})
	if err != nil {
		return 0, err
	}

	return objectInfo.Size, nil
}

// Write writes the data to minio storage.
func (mcm *MinioChunkManager) Write(filePath string, content []byte) error {
	_, err := mcm.Client.PutObject(mcm.ctx, mcm.bucketName, filePath, bytes.NewReader(content), int64(len(content)), minio.PutObjectOptions{})

	if err != nil {
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
func (mcm *MinioChunkManager) Exist(filePath string) bool {
	_, err := mcm.Client.StatObject(mcm.ctx, mcm.bucketName, filePath, minio.StatObjectOptions{})
	return err == nil
}

// Read reads the minio storage data if exists.
func (mcm *MinioChunkManager) Read(filePath string) ([]byte, error) {
	object, err := mcm.Client.GetObject(mcm.ctx, mcm.bucketName, filePath, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer object.Close()

	return ioutil.ReadAll(object)
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
	objectsKeys, err := mcm.ListWithPrefix(prefix)
	if err != nil {
		return nil, nil, err
	}
	objectsValues, err := mcm.MultiRead(objectsKeys)
	if err != nil {
		log.Error(fmt.Sprintf("MinIO load with prefix error. path = %s", prefix), zap.Error(err))
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
		return nil, err
	}

	object, err := mcm.Client.GetObject(mcm.ctx, mcm.bucketName, filePath, opts)
	if err != nil {
		return nil, err
	}
	defer object.Close()
	return ioutil.ReadAll(object)
}

// Remove deletes an object with @key.
func (mcm *MinioChunkManager) Remove(key string) error {
	err := mcm.Client.RemoveObject(mcm.ctx, mcm.bucketName, key, minio.RemoveObjectOptions{})
	return err
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
	objectsCh := make(chan minio.ObjectInfo)

	go func() {
		defer close(objectsCh)

		for object := range mcm.Client.ListObjects(mcm.ctx, mcm.bucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			objectsCh <- object
		}
	}()

	for rErr := range mcm.Client.RemoveObjects(mcm.ctx, mcm.bucketName, objectsCh, minio.RemoveObjectsOptions{GovernanceBypass: true}) {
		if rErr.Err != nil {
			return rErr.Err
		}
	}
	return nil
}

func (mcm *MinioChunkManager) ListWithPrefix(prefix string) ([]string, error) {
	objects := mcm.Client.ListObjects(mcm.ctx, mcm.bucketName, minio.ListObjectsOptions{Prefix: prefix})

	var objectsKeys []string

	for object := range objects {
		objectsKeys = append(objectsKeys, object.Key)
	}
	return objectsKeys, nil
}
