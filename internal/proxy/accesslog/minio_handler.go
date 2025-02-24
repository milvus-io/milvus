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

package accesslog

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
)

type config struct {
	address           string
	bucketName        string
	accessKeyID       string
	secretAccessKeyID string
	useSSL            bool
	sslCACert         string
	createBucket      bool
	useIAM            bool
	iamEndpoint       string
}

// minIO client for upload access log
// TODO file retention on minio
type (
	RetentionFunc func(object minio.ObjectInfo) bool
	task          struct {
		objectName string
		filePath   string
	}
)

type minioHandler struct {
	bucketName string
	rootPath   string

	retentionPolicy RetentionFunc
	client          *minio.Client

	taskCh    chan task
	closeCh   chan struct{}
	closeWg   sync.WaitGroup
	closeOnce sync.Once
}

func NewMinioHandler(ctx context.Context, cfg *paramtable.MinioConfig, rootPath string, queueLen int) (*minioHandler, error) {
	if !strings.HasSuffix(rootPath, "/") {
		rootPath = rootPath + "/"
	}

	handlerCfg := config{
		address:           cfg.Address.GetValue(),
		bucketName:        cfg.BucketName.GetValue(),
		accessKeyID:       cfg.AccessKeyID.GetValue(),
		secretAccessKeyID: cfg.SecretAccessKey.GetValue(),
		useSSL:            cfg.UseSSL.GetAsBool(),
		sslCACert:         cfg.SslCACert.GetValue(),
		createBucket:      true,
		useIAM:            cfg.UseIAM.GetAsBool(),
		iamEndpoint:       cfg.IAMEndpoint.GetValue(),
	}

	client, err := newMinioClient(ctx, handlerCfg)
	if err != nil {
		return nil, err
	}

	handler := &minioHandler{
		bucketName: handlerCfg.bucketName,
		rootPath:   rootPath,
		client:     client,
	}
	handler.start(queueLen)
	return handler, nil
}

func newMinioClient(ctx context.Context, cfg config) (*minio.Client, error) {
	var creds *credentials.Credentials
	if cfg.useIAM {
		creds = credentials.NewIAM(cfg.iamEndpoint)
	} else {
		creds = credentials.NewStaticV4(cfg.accessKeyID, cfg.secretAccessKeyID, "")
	}

	// We must set the cert path by os environment variable "SSL_CERT_FILE",
	// because the minio.DefaultTransport() need this path to read the file content,
	// we shouldn't read this file by ourself.
	if cfg.useSSL && len(cfg.sslCACert) > 0 {
		err := os.Setenv("SSL_CERT_FILE", cfg.sslCACert)
		if err != nil {
			return nil, err
		}
	}

	minioClient, err := minio.New(cfg.address, &minio.Options{
		Creds:  creds,
		Secure: cfg.useSSL,
	})
	// options nil or invalid formatted endpoint, don't need to retry
	if err != nil {
		return nil, err
	}

	var bucketExists bool
	// check valid in first query
	checkBucketFn := func() error {
		bucketExists, err = minioClient.BucketExists(ctx, cfg.bucketName)
		if err != nil {
			log.Warn("failed to check blob bucket exist", zap.String("bucket", cfg.bucketName), zap.Error(err))
			return err
		}
		if !bucketExists {
			if cfg.createBucket {
				log.Info("blob bucket not exist, create bucket.", zap.String("bucket name", cfg.bucketName))
				err := minioClient.MakeBucket(ctx, cfg.bucketName, minio.MakeBucketOptions{})
				if err != nil {
					log.Warn("failed to create blob bucket", zap.String("bucket", cfg.bucketName), zap.Error(err))
					return err
				}
			} else {
				return fmt.Errorf("bucket %s not Existed", cfg.bucketName)
			}
		}
		return nil
	}
	err = retry.Do(ctx, checkBucketFn, retry.Attempts(CheckBucketRetryAttempts))
	if err != nil {
		return nil, err
	}
	return minioClient, nil
}

func (c *minioHandler) scheduler() {
	defer c.closeWg.Done()
	for {
		select {
		case task := <-c.taskCh:
			log.Info("Update access log file to minIO",
				zap.String("object name", task.objectName),
				zap.String("file path", task.filePath))
			c.update(task.objectName, task.filePath)
			c.Retention()
		case <-c.closeCh:
			log.Warn("close minio logger handler")
			return
		}
	}
}

func (c *minioHandler) start(queueLen int) error {
	c.closeWg = sync.WaitGroup{}
	c.closeCh = make(chan struct{})
	c.taskCh = make(chan task, queueLen)

	c.closeWg.Add(1)
	go c.scheduler()
	return nil
}

func (c *minioHandler) Update(objectName string, filePath string) {
	c.taskCh <- task{
		objectName: objectName,
		filePath:   filePath,
	}
	taskNum := len(c.taskCh)
	if taskNum >= cap(c.taskCh)/2 {
		log.Warn("Minio Access log file handler was busy", zap.Int("task num", taskNum))
	}
}

// update log file to minio
func (c *minioHandler) update(objectName string, filePath string) error {
	path := join(c.rootPath, filePath)
	_, err := c.client.FPutObject(context.Background(), c.bucketName, path, objectName, minio.PutObjectOptions{})
	return err
}

func (c *minioHandler) Retention() error {
	if c.retentionPolicy == nil {
		return nil
	}

	objects := c.client.ListObjects(context.Background(), c.bucketName, minio.ListObjectsOptions{Prefix: c.rootPath, Recursive: false})
	removeObjects := make(chan minio.ObjectInfo)
	go func() {
		defer close(removeObjects)
		for object := range objects {
			if c.retentionPolicy(object) {
				removeObjects <- object
			}
		}
	}()

	for rErr := range c.client.RemoveObjects(context.Background(), c.bucketName, removeObjects, minio.RemoveObjectsOptions{GovernanceBypass: false}) {
		if rErr.Err != nil {
			log.Warn("failed to remove retention objects", zap.Error(rErr.Err))
			return rErr.Err
		}
	}
	return nil
}

func (c *minioHandler) removeWithPrefix(prefix string) error {
	objects := c.client.ListObjects(context.Background(), c.bucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})
	for rErr := range c.client.RemoveObjects(context.Background(), c.bucketName, objects, minio.RemoveObjectsOptions{GovernanceBypass: false}) {
		if rErr.Err != nil {
			log.Warn("failed to remove objects", zap.String("prefix", prefix), zap.Error(rErr.Err))
			return rErr.Err
		}
	}
	return nil
}

func (c *minioHandler) listAll() ([]string, error) {
	var objectsKeys []string

	objects := c.client.ListObjects(context.Background(), c.bucketName, minio.ListObjectsOptions{Prefix: c.rootPath, Recursive: false})

	for object := range objects {
		if object.Err != nil {
			log.Warn("failed to list with rootpath", zap.String("rootpath", c.rootPath), zap.Error(object.Err))
			return nil, object.Err
		}
		// with tailing "/", object is a "directory"
		if strings.HasSuffix(object.Key, "/") {
			continue
		}
		objectsKeys = append(objectsKeys, object.Key)
	}
	return objectsKeys, nil
}

func (c *minioHandler) Clean() error {
	err := c.removeWithPrefix(c.rootPath)
	return err
}

func (c *minioHandler) Close() error {
	c.closeOnce.Do(func() {
		close(c.closeCh)
		c.closeWg.Wait()
	})
	return nil
}

func getTimeRetentionFunc(retentionTime int, prefix, ext string) RetentionFunc {
	if retentionTime == 0 {
		return nil
	}

	return func(object minio.ObjectInfo) bool {
		name := path.Base(object.Key)
		fileTime, err := timeFromName(name, prefix, ext)
		if err != nil {
			return false
		}

		nowWallTime, _ := time.Parse(timeNameFormat, time.Now().Format(timeNameFormat))
		intervalTime := nowWallTime.Sub(fileTime)
		return intervalTime > (time.Duration(retentionTime) * time.Hour)
	}
}
