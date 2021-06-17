// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package miniokv

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"time"

	"io"
	"strings"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/performance"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

type MinIOKV struct {
	ctx         context.Context
	minioClient *minio.Client
	bucketName  string
}

type Option struct {
	Address           string
	AccessKeyID       string
	BucketName        string
	SecretAccessKeyID string
	UseSSL            bool
	CreateBucket      bool // when bucket not existed, create it
}

func NewMinIOKV(ctx context.Context, option *Option) (*MinIOKV, error) {
	var minIOClient *minio.Client
	var err error
	minIOClient, err = minio.New(option.Address, &minio.Options{
		Creds:  credentials.NewStaticV4(option.AccessKeyID, option.SecretAccessKeyID, ""),
		Secure: option.UseSSL,
	})
	// options nil or invalid formatted endpoint, don't need retry
	if err != nil {
		return nil, err
	}
	var bucketExists bool
	// check valid in first query
	checkBucketFn := func() error {
		bucketExists, err = minIOClient.BucketExists(ctx, option.BucketName)
		return err
	}
	err = retry.Retry(100000, time.Millisecond*200, checkBucketFn)
	if err != nil {
		return nil, err
	}
	// connection shall be valid here, no need to retry
	if option.CreateBucket {
		if !bucketExists {
			err = minIOClient.MakeBucket(ctx, option.BucketName, minio.MakeBucketOptions{})
			if err != nil {
				return nil, err
			}
		}
	} else {
		if !bucketExists {
			return nil, fmt.Errorf("bucket %s not Existed", option.BucketName)
		}
	}

	kv := &MinIOKV{
		ctx:         ctx,
		minioClient: minIOClient,
		bucketName:  option.BucketName,
	}
	//go kv.performanceTest(false, 16<<20)

	return kv, nil
}

func (kv *MinIOKV) LoadWithPrefix(key string) ([]string, []string, error) {
	objects := kv.minioClient.ListObjects(kv.ctx, kv.bucketName, minio.ListObjectsOptions{Prefix: key})

	var objectsKeys []string
	var objectsValues []string

	for object := range objects {
		objectsKeys = append(objectsKeys, object.Key)
	}
	objectsValues, err := kv.MultiLoad(objectsKeys)
	if err != nil {
		log.Debug("MinIO", zap.String("cannot load value with prefix:%s", key))
	}

	return objectsKeys, objectsValues, nil
}

func (kv *MinIOKV) Load(key string) (string, error) {
	object, err := kv.minioClient.GetObject(kv.ctx, kv.bucketName, key, minio.GetObjectOptions{})
	if err != nil {
		return "", err
	}

	buf := new(strings.Builder)
	_, err = io.Copy(buf, object)
	if err != nil && err != io.EOF {
		return "", err
	}
	return buf.String(), nil
}

func (kv *MinIOKV) MultiLoad(keys []string) ([]string, error) {
	var resultErr error
	var objectsValues []string
	for _, key := range keys {
		objectValue, err := kv.Load(key)
		if err != nil {
			if resultErr == nil {
				resultErr = err
			}
		}
		objectsValues = append(objectsValues, objectValue)
	}

	return objectsValues, resultErr
}

func (kv *MinIOKV) Save(key, value string) error {
	reader := strings.NewReader(value)
	_, err := kv.minioClient.PutObject(kv.ctx, kv.bucketName, key, reader, int64(len(value)), minio.PutObjectOptions{})

	if err != nil {
		return err
	}

	return err
}

func (kv *MinIOKV) MultiSave(kvs map[string]string) error {
	var resultErr error
	for key, value := range kvs {
		err := kv.Save(key, value)
		if err != nil {
			if resultErr == nil {
				resultErr = err
			}
		}
	}
	return resultErr
}

func (kv *MinIOKV) RemoveWithPrefix(prefix string) error {
	objectsCh := make(chan minio.ObjectInfo)

	go func() {
		defer close(objectsCh)

		for object := range kv.minioClient.ListObjects(kv.ctx, kv.bucketName, minio.ListObjectsOptions{Prefix: prefix}) {
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

func (kv *MinIOKV) Remove(key string) error {
	err := kv.minioClient.RemoveObject(kv.ctx, kv.bucketName, string(key), minio.RemoveObjectOptions{})
	return err
}

func (kv *MinIOKV) MultiRemove(keys []string) error {
	var resultErr error
	for _, key := range keys {
		err := kv.Remove(key)
		if err != nil {
			if resultErr == nil {
				resultErr = err
			}
		}
	}
	return resultErr
}

func (kv *MinIOKV) Close() {

}

type Case struct {
	Name      string
	BlockSize int     // unit: byte
	Speed     float64 // unit: MB/s
}

type Test struct {
	Name  string
	Cases []Case
}

func (kv *MinIOKV) performanceTest(toFile bool, totalBytes int) {
	r := rand.Int()
	results := Test{Name: "MinIO performance"}
	for i := 0; i < 10; i += 2 {
		data := performance.GenerateData(2*1024, float64(9-i))
		startT := time.Now()
		for j := 0; j < totalBytes/(len(data)); j++ {
			kv.Save(fmt.Sprintf("performance-rand%d-test-%d-%d", r, i, j), data)
		}
		tc := time.Since(startT)
		results.Cases = append(results.Cases, Case{Name: "write", BlockSize: len(data), Speed: 16.0 / tc.Seconds()})

		startT = time.Now()
		for j := 0; j < totalBytes/(len(data)); j++ {
			kv.Load(fmt.Sprintf("performance-rand%d-test-%d-%d", r, i, j))
		}
		tc = time.Since(startT)
		results.Cases = append(results.Cases, Case{Name: "read", BlockSize: len(data), Speed: 16.0 / tc.Seconds()})
	}
	kv.RemoveWithPrefix(fmt.Sprintf("performance-rand%d", r))
	mb, err := json.Marshal(results)
	if err != nil {
		return
	}
	log.Debug(string(mb))
	if toFile {
		err = ioutil.WriteFile(fmt.Sprintf("./%d", r), mb, 0644)
		if err != nil {
			return
		}
	}
}
