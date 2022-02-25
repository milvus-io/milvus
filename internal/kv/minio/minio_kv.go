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

package miniokv

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"sync"

	"io"
	"strings"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

var _ kv.DataKV = (*MinIOKV)(nil)

// MinIOKV implements DataKV interface and relies on underling MinIO service.
// MinIOKV object contains a client which can be used to access the MinIO service.
type MinIOKV struct {
	ctx         context.Context
	minioClient *minio.Client
	bucketName  string
}

// Option option when creates MinIOKV.
type Option struct {
	Address           string
	AccessKeyID       string
	BucketName        string
	SecretAccessKeyID string
	UseSSL            bool
	CreateBucket      bool // when bucket not existed, create it
}

// NewMinIOKV creates MinIOKV to save and load object to MinIOKV.
func NewMinIOKV(ctx context.Context, option *Option) (*MinIOKV, error) {
	var minIOClient *minio.Client
	var err error
	minIOClient, err = minio.New(option.Address, &minio.Options{
		Creds:  credentials.NewStaticV4(option.AccessKeyID, option.SecretAccessKeyID, ""),
		Secure: option.UseSSL,
	})
	// options nil or invalid formatted endpoint, don't need to retry
	if err != nil {
		return nil, err
	}
	var bucketExists bool
	// check valid in first query
	checkBucketFn := func() error {
		bucketExists, err = minIOClient.BucketExists(ctx, option.BucketName)
		if err != nil {
			return err
		}
		if !bucketExists {
			log.Debug("MinioKV NewMinioKV", zap.Any("Check bucket", "bucket not exist"))
			if option.CreateBucket {
				log.Debug("MinioKV NewMinioKV create bucket.")
				return minIOClient.MakeBucket(ctx, option.BucketName, minio.MakeBucketOptions{})
			}
			return fmt.Errorf("bucket %s not Existed", option.BucketName)
		}
		return nil
	}
	err = retry.Do(ctx, checkBucketFn, retry.Attempts(300))
	if err != nil {
		return nil, err
	}

	kv := &MinIOKV{
		ctx:         ctx,
		minioClient: minIOClient,
		bucketName:  option.BucketName,
	}
	log.Debug("MinioKV new MinioKV success.")

	return kv, nil
}

// Exist checks whether a key exists in MinIO.
func (kv *MinIOKV) Exist(key string) bool {
	_, err := kv.minioClient.StatObject(kv.ctx, kv.bucketName, key, minio.StatObjectOptions{})
	return err == nil
}

// LoadWithPrefix loads objects with the same prefix @key from minio .
func (kv *MinIOKV) LoadWithPrefix(key string) ([]string, []string, error) {
	objects := kv.minioClient.ListObjects(kv.ctx, kv.bucketName, minio.ListObjectsOptions{Prefix: key})

	var objectsKeys []string
	var objectsValues []string

	for object := range objects {
		objectsKeys = append(objectsKeys, object.Key)
	}
	objectsValues, err := kv.MultiLoad(objectsKeys)
	if err != nil {
		log.Error(fmt.Sprintf("MinIO load with prefix error. path = %s", key), zap.Error(err))
		return nil, nil, err
	}

	return objectsKeys, objectsValues, nil
}

func (kv *MinIOKV) LoadBytesWithPrefix(key string) ([]string, [][]byte, error) {
	objects := kv.minioClient.ListObjects(kv.ctx, kv.bucketName, minio.ListObjectsOptions{Prefix: key})

	var (
		objectsKeys   = make([]string, 0, len(objects))
		objectsValues [][]byte
	)

	for object := range objects {
		objectsKeys = append(objectsKeys, object.Key)
	}
	objectsValues, err := kv.MultiLoadBytes(objectsKeys)
	if err != nil {
		log.Error(fmt.Sprintf("MinIO load with prefix error. path = %s", key), zap.Error(err))
		return nil, nil, err
	}

	return objectsKeys, objectsValues, nil
}

// Load loads an object with @key.
func (kv *MinIOKV) Load(key string) (string, error) {
	object, err := kv.minioClient.GetObject(kv.ctx, kv.bucketName, key, minio.GetObjectOptions{})
	if err != nil {
		return "", err
	}
	if object != nil {
		defer object.Close()
	}

	info, err := object.Stat()
	if err != nil {
		return "", err
	}

	buf := new(strings.Builder)
	buf.Grow(int(info.Size))
	_, err = io.Copy(buf, object)
	if err != nil && err != io.EOF {
		return "", err
	}

	return buf.String(), nil
}

// Load loads an object with @key.
func (kv *MinIOKV) LoadBytes(key string) ([]byte, error) {
	object, err := kv.minioClient.GetObject(kv.ctx, kv.bucketName, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	if object != nil {
		defer object.Close()
	}

	info, err := object.Stat()
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(make([]byte, 0, info.Size))
	_, err = io.Copy(buf, object)
	if err != nil && err != io.EOF {
		return nil, err
	}

	return buf.Bytes(), nil
}

// FGetObject downloads file from minio to local storage system.
func (kv *MinIOKV) FGetObject(key, localPath string) error {
	return kv.minioClient.FGetObject(kv.ctx, kv.bucketName, key, localPath+key, minio.GetObjectOptions{})
}

// FGetObjects downloads files from minio to local storage system.
// For parallell downloads file, n goroutines will be started to download n keys.
func (kv *MinIOKV) FGetObjects(keys []string, localPath string) error {
	var wg sync.WaitGroup
	el := make(errorList, len(keys))
	for i, key := range keys {
		wg.Add(1)
		go func(i int, key string) {
			err := kv.minioClient.FGetObject(kv.ctx, kv.bucketName, key, localPath+key, minio.GetObjectOptions{})
			if err != nil {
				el[i] = err
			}
			wg.Done()
		}(i, key)
	}
	wg.Wait()
	for _, err := range el {
		if err != nil {
			return el
		}
	}
	return nil
}

// MultiLoad loads objects with multi @keys.
func (kv *MinIOKV) MultiLoad(keys []string) ([]string, error) {
	var objectsValues []string
	for _, key := range keys {
		objectValue, err := kv.Load(key)
		if err != nil {
			return nil, err
		}
		objectsValues = append(objectsValues, objectValue)
	}

	return objectsValues, nil
}

func (kv *MinIOKV) MultiLoadBytes(keys []string) ([][]byte, error) {
	objectsValues := make([][]byte, 0, len(keys))

	for _, key := range keys {
		objectValue, err := kv.LoadBytes(key)
		if err != nil {
			return nil, err
		}
		objectsValues = append(objectsValues, objectValue)
	}

	return objectsValues, nil
}

// Save object with @key to Minio. Object value is @value.
func (kv *MinIOKV) Save(key, value string) error {
	reader := strings.NewReader(value)
	_, err := kv.minioClient.PutObject(kv.ctx, kv.bucketName, key, reader, int64(len(value)), minio.PutObjectOptions{})

	return err
}

func (kv *MinIOKV) SaveBytes(key string, value []byte) error {
	reader := bytes.NewReader(value)
	_, err := kv.minioClient.PutObject(kv.ctx, kv.bucketName, key, reader, int64(len(value)), minio.PutObjectOptions{})

	return err
}

// MultiSave saves multiple objects, the path is the key of @kvs.
// The object value is the value of @kvs.
func (kv *MinIOKV) MultiSave(kvs map[string]string) error {
	for key, value := range kvs {
		err := kv.Save(key, value)
		if err != nil {
			return err
		}
	}

	return nil
}

func (kv *MinIOKV) MultiSaveBytes(kvs map[string][]byte) error {
	for key, value := range kvs {
		err := kv.SaveBytes(key, value)
		if err != nil {
			return err
		}
	}

	return nil
}

// RemoveWithPrefix removes all objects with the same prefix @prefix from minio.
func (kv *MinIOKV) RemoveWithPrefix(prefix string) error {
	objectsCh := make(chan minio.ObjectInfo)

	go func() {
		defer close(objectsCh)

		for object := range kv.minioClient.ListObjects(kv.ctx, kv.bucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			objectsCh <- object
		}
	}()

	for rErr := range kv.minioClient.RemoveObjects(kv.ctx, kv.bucketName, objectsCh, minio.RemoveObjectsOptions{GovernanceBypass: true}) {
		if rErr.Err != nil {
			return rErr.Err
		}
	}
	return nil
}

// Remove deletes an object with @key.
func (kv *MinIOKV) Remove(key string) error {
	return kv.minioClient.RemoveObject(kv.ctx, kv.bucketName, key, minio.RemoveObjectOptions{})
}

// MultiRemove deletes an objects with @keys.
func (kv *MinIOKV) MultiRemove(keys []string) error {
	for _, key := range keys {
		err := kv.Remove(key)
		if err != nil {
			return err
		}
	}

	return nil
}

// LoadPartial loads partial data ranged in [start, end) with @key.
func (kv *MinIOKV) LoadPartial(key string, start, end int64) ([]byte, error) {
	switch {
	case start < 0 || end < 0:
		return nil, fmt.Errorf("invalid range specified: start=%d end=%d",
			start, end)
	case start >= end:
		return nil, fmt.Errorf("invalid range specified: start=%d end=%d",
			start, end)
	}

	opts := minio.GetObjectOptions{}
	err := opts.SetRange(start, end-1)
	if err != nil {
		return nil, err
	}

	object, err := kv.minioClient.GetObject(kv.ctx, kv.bucketName, key, opts)
	if err != nil {
		return nil, err
	}
	defer object.Close()

	return ioutil.ReadAll(object)
}

// GetSize obtains the data size of the object with @key.
func (kv *MinIOKV) GetSize(key string) (int64, error) {
	objectInfo, err := kv.minioClient.StatObject(kv.ctx, kv.bucketName, key, minio.StatObjectOptions{})
	if err != nil {
		return 0, err
	}

	return objectInfo.Size, nil
}

// Close close the MinIOKV.
func (kv *MinIOKV) Close() {

}

type errorList []error

func (el errorList) Error() string {
	var builder strings.Builder
	builder.WriteString("All downloads results:\n")
	for index, err := range el {
		builder.WriteString(fmt.Sprintf("downloads #%d:%s\n", index+1, err.Error()))
	}
	return builder.String()
}
