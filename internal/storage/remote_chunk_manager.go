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
	"io"
	"net/http"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/cockroachdb/errors"
	minio "github.com/minio/minio-go/v7"
	"go.uber.org/zap"
	"golang.org/x/exp/mmap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
)

const (
	CloudProviderGCP       = "gcp"
	CloudProviderGCPNative = "gcpnative"
	CloudProviderAWS       = "aws"
	CloudProviderAliyun    = "aliyun"
	CloudProviderAzure     = "azure"
	CloudProviderTencent   = "tencent"
)

// ChunkObjectWalkFunc is the callback function for walking objects.
// If return false, WalkWithObjects will stop.
// Otherwise, WalkWithObjects will continue until reach the last object.
type ChunkObjectWalkFunc func(chunkObjectInfo *ChunkObjectInfo) bool

type ObjectStorage interface {
	GetObject(ctx context.Context, bucketName, objectName string, offset int64, size int64) (FileReader, error)
	PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) error
	StatObject(ctx context.Context, bucketName, objectName string) (int64, error)
	// WalkWithPrefix walks all objects with prefix @prefix, and call walker for each object.
	// WalkWithPrefix will stop if following conditions met:
	// 1. cb return false or reach the last object, WalkWithPrefix will stop and return nil.
	// 2. underlying walking failed or context canceled, WalkWithPrefix will stop and return a error.
	WalkWithObjects(ctx context.Context, bucketName string, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc) error
	RemoveObject(ctx context.Context, bucketName, objectName string) error
}

// RemoteChunkManager is responsible for read and write data stored in minio.
type RemoteChunkManager struct {
	client ObjectStorage

	//	ctx        context.Context
	bucketName string
	rootPath   string
}

var _ ChunkManager = (*RemoteChunkManager)(nil)

func NewRemoteChunkManager(ctx context.Context, c *config) (*RemoteChunkManager, error) {
	var client ObjectStorage
	var err error
	if c.cloudProvider == CloudProviderAzure {
		client, err = newAzureObjectStorageWithConfig(ctx, c)
	} else if c.cloudProvider == CloudProviderGCPNative {
		client, err = newGcpNativeObjectStorageWithConfig(ctx, c)
	} else {
		client, err = newMinioObjectStorageWithConfig(ctx, c)
	}
	if err != nil {
		return nil, err
	}
	mcm := &RemoteChunkManager{
		client:     client,
		bucketName: c.bucketName,
		rootPath:   strings.TrimLeft(c.rootPath, "/"),
	}
	log.Info("remote chunk manager init success.", zap.String("remote", c.cloudProvider), zap.String("bucketname", c.bucketName), zap.String("root", mcm.RootPath()))
	return mcm, nil
}

// NewRemoteChunkManagerForTesting is used for testing.
func NewRemoteChunkManagerForTesting(c *minio.Client, bucket string, rootPath string) *RemoteChunkManager {
	mcm := &RemoteChunkManager{
		client:     &MinioObjectStorage{c},
		bucketName: bucket,
		rootPath:   rootPath,
	}
	return mcm
}

// RootPath returns minio root path.
func (mcm *RemoteChunkManager) RootPath() string {
	return mcm.rootPath
}

// UnderlyingObjectStorage returns the underlying object storage.
func (mcm *RemoteChunkManager) UnderlyingObjectStorage() ObjectStorage {
	return mcm.client
}

// Path returns the path of minio data if exists.
func (mcm *RemoteChunkManager) Path(ctx context.Context, filePath string) (string, error) {
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
func (mcm *RemoteChunkManager) Reader(ctx context.Context, filePath string) (FileReader, error) {
	reader, err := mcm.getObject(ctx, mcm.bucketName, filePath, int64(0), int64(0))
	if err != nil {
		log.Warn("failed to get object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	return reader, nil
}

func (mcm *RemoteChunkManager) Size(ctx context.Context, filePath string) (int64, error) {
	objectInfo, err := mcm.getObjectSize(ctx, mcm.bucketName, filePath)
	if err != nil {
		log.Warn("failed to stat object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return 0, err
	}

	return objectInfo, nil
}

// Write writes the data to minio storage.
func (mcm *RemoteChunkManager) Write(ctx context.Context, filePath string, content []byte) error {
	err := mcm.putObject(ctx, mcm.bucketName, filePath, bytes.NewReader(content), int64(len(content)))
	if err != nil {
		log.Warn("failed to put object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return err
	}

	metrics.PersistentDataKvSize.WithLabelValues(metrics.DataPutLabel).Observe(float64(len(content)))
	return nil
}

// MultiWrite saves multiple objects, the path is the key of @kvs.
// The object value is the value of @kvs.
func (mcm *RemoteChunkManager) MultiWrite(ctx context.Context, kvs map[string][]byte) error {
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
func (mcm *RemoteChunkManager) Exist(ctx context.Context, filePath string) (bool, error) {
	_, err := mcm.getObjectSize(ctx, mcm.bucketName, filePath)
	if err != nil {
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			return false, nil
		}
		log.Warn("failed to stat object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return false, err
	}
	return true, nil
}

// Read reads the minio storage data if exists.
func (mcm *RemoteChunkManager) Read(ctx context.Context, filePath string) ([]byte, error) {
	var data []byte
	err := retry.Do(ctx, func() error {
		object, err := mcm.getObject(ctx, mcm.bucketName, filePath, int64(0), int64(0))
		if err != nil {
			log.Warn("failed to get object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
			return err
		}
		defer object.Close()

		// Prefetch object data
		var empty []byte
		_, err = object.Read(empty)
		err = checkObjectStorageError(filePath, err)
		if err != nil {
			log.Warn("failed to read object", zap.String("path", filePath), zap.Error(err))
			return err
		}
		size, err := mcm.getObjectSize(ctx, mcm.bucketName, filePath)
		if err != nil {
			log.Warn("failed to stat object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
			return err
		}
		data, err = read(object, size)
		err = checkObjectStorageError(filePath, err)
		if err != nil {
			log.Warn("failed to read object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
			return err
		}
		metrics.PersistentDataKvSize.WithLabelValues(metrics.DataGetLabel).Observe(float64(size))
		return nil
	}, retry.Attempts(3), retry.RetryErr(merr.IsRetryableErr))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (mcm *RemoteChunkManager) MultiRead(ctx context.Context, keys []string) ([][]byte, error) {
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

func (mcm *RemoteChunkManager) Mmap(ctx context.Context, filePath string) (*mmap.ReaderAt, error) {
	return nil, errors.New("this method has not been implemented")
}

// ReadAt reads specific position data of minio storage if exists.
func (mcm *RemoteChunkManager) ReadAt(ctx context.Context, filePath string, off int64, length int64) ([]byte, error) {
	if off < 0 || length < 0 {
		return nil, io.EOF
	}

	object, err := mcm.getObject(ctx, mcm.bucketName, filePath, off, length)
	if err != nil {
		log.Warn("failed to get object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	defer object.Close()

	data, err := read(object, length)
	err = checkObjectStorageError(filePath, err)
	if err != nil {
		log.Warn("failed to read object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	metrics.PersistentDataKvSize.WithLabelValues(metrics.DataGetLabel).Observe(float64(length))
	return data, nil
}

// Remove deletes an object with @key.
func (mcm *RemoteChunkManager) Remove(ctx context.Context, filePath string) error {
	err := mcm.removeObject(ctx, mcm.bucketName, filePath)
	if err != nil {
		log.Warn("failed to remove object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return err
	}
	return nil
}

// MultiRemove deletes a objects with @keys.
func (mcm *RemoteChunkManager) MultiRemove(ctx context.Context, keys []string) error {
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
func (mcm *RemoteChunkManager) RemoveWithPrefix(ctx context.Context, prefix string) error {
	// removeObject in parallel.
	runningGroup, _ := errgroup.WithContext(ctx)
	runningGroup.SetLimit(10)
	err := mcm.WalkWithPrefix(ctx, prefix, true, func(object *ChunkObjectInfo) bool {
		key := object.FilePath
		runningGroup.Go(func() error {
			err := mcm.removeObject(ctx, mcm.bucketName, key)
			if err != nil {
				log.Warn("failed to remove object", zap.String("path", key), zap.Error(err))
			}
			return err
		})
		return true
	})
	// wait all goroutines done.
	if err := runningGroup.Wait(); err != nil {
		return err
	}
	// return the iteration error
	return err
}

func (mcm *RemoteChunkManager) WalkWithPrefix(ctx context.Context, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc) (err error) {
	metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataWalkLabel, metrics.TotalLabel).Inc()
	logger := log.With(zap.String("prefix", prefix), zap.Bool("recursive", recursive))

	logger.Info("start walk through objects")
	if err := mcm.client.WalkWithObjects(ctx, mcm.bucketName, prefix, recursive, walkFunc); err != nil {
		metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataWalkLabel, metrics.FailLabel).Inc()
		logger.Warn("failed to walk through objects", zap.Error(err))
		return err
	}
	metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataWalkLabel, metrics.SuccessLabel).Inc()
	logger.Info("finish walk through objects")
	return nil
}

func (mcm *RemoteChunkManager) getObject(ctx context.Context, bucketName, objectName string,
	offset int64, size int64,
) (FileReader, error) {
	reader, err := mcm.client.GetObject(ctx, bucketName, objectName, offset, size)
	metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataGetLabel, metrics.TotalLabel).Inc()
	if err == nil && reader != nil {
		metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataGetLabel, metrics.SuccessLabel).Inc()
	} else {
		if errors.Is(err, context.Canceled) {
			metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataGetLabel, metrics.CancelLabel).Inc()
		} else {
			metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataGetLabel, metrics.FailLabel).Inc()
		}
	}

	return reader, err
}

func (mcm *RemoteChunkManager) putObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) error {
	start := timerecord.NewTimeRecorder("putObject")

	err := mcm.client.PutObject(ctx, bucketName, objectName, reader, objectSize)
	metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataPutLabel, metrics.TotalLabel).Inc()
	if err == nil {
		metrics.PersistentDataRequestLatency.WithLabelValues(metrics.DataPutLabel).
			Observe(float64(start.ElapseSpan().Milliseconds()))
		metrics.PersistentDataOpCounter.WithLabelValues(metrics.MetaPutLabel, metrics.SuccessLabel).Inc()
	} else {
		if errors.Is(err, context.Canceled) {
			metrics.PersistentDataOpCounter.WithLabelValues(metrics.MetaPutLabel, metrics.CancelLabel).Inc()
		} else {
			metrics.PersistentDataOpCounter.WithLabelValues(metrics.MetaPutLabel, metrics.FailLabel).Inc()
		}
	}

	return err
}

func (mcm *RemoteChunkManager) getObjectSize(ctx context.Context, bucketName, objectName string) (int64, error) {
	start := timerecord.NewTimeRecorder("getObjectSize")

	info, err := mcm.client.StatObject(ctx, bucketName, objectName)
	metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataStatLabel, metrics.TotalLabel).Inc()
	if err == nil {
		metrics.PersistentDataRequestLatency.WithLabelValues(metrics.DataStatLabel).
			Observe(float64(start.ElapseSpan().Milliseconds()))
		metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataStatLabel, metrics.SuccessLabel).Inc()
	} else {
		if errors.Is(err, context.Canceled) {
			metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataStatLabel, metrics.CancelLabel).Inc()
		} else {
			metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataStatLabel, metrics.FailLabel).Inc()
		}
	}

	return info, err
}

func (mcm *RemoteChunkManager) removeObject(ctx context.Context, bucketName, objectName string) error {
	start := timerecord.NewTimeRecorder("removeObject")

	err := mcm.client.RemoveObject(ctx, bucketName, objectName)
	metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataRemoveLabel, metrics.TotalLabel).Inc()
	if err == nil {
		metrics.PersistentDataRequestLatency.WithLabelValues(metrics.DataRemoveLabel).
			Observe(float64(start.ElapseSpan().Milliseconds()))
		metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataRemoveLabel, metrics.SuccessLabel).Inc()
	} else {
		if errors.Is(err, context.Canceled) {
			metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataRemoveLabel, metrics.CancelLabel).Inc()
		} else {
			metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataRemoveLabel, metrics.FailLabel).Inc()
		}
	}

	return err
}

func checkObjectStorageError(fileName string, err error) error {
	if err == nil {
		return nil
	}

	switch err := err.(type) {
	case *azcore.ResponseError:
		if err.ErrorCode == string(bloberror.BlobNotFound) {
			return merr.WrapErrIoKeyNotFound(fileName, err.Error())
		}
		return merr.WrapErrIoFailed(fileName, err)
	case minio.ErrorResponse:
		if err.Code == "NoSuchKey" {
			return merr.WrapErrIoKeyNotFound(fileName, err.Error())
		}
		return merr.WrapErrIoFailed(fileName, err)
	case *googleapi.Error:
		if err.Code == http.StatusNotFound {
			return merr.WrapErrIoKeyNotFound(fileName, err.Error())
		}
		return merr.WrapErrIoFailed(fileName, err)
	}
	if err == io.ErrUnexpectedEOF {
		return merr.WrapErrIoUnexpectEOF(fileName, err)
	}
	return merr.WrapErrIoFailed(fileName, err)
}

// Learn from file.ReadFile
func read(r io.Reader, size int64) ([]byte, error) {
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
