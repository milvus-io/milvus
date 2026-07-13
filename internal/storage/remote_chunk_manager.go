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
	"syscall"

	cstorage "cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"golang.org/x/exp/mmap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"

	"github.com/milvus-io/milvus/internal/storageprofile"
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
	CopyObject(ctx context.Context, bucketName, srcObjectName, dstObjectName string) error
}

// RemoteChunkManager is responsible for read and write data stored in mminio.
type RemoteChunkManager struct {
	client ObjectStorage

	//	ctx        context.Context
	bucketName string
	rootPath   string

	readRetryAttempts uint
	backendKind       storageprofile.BackendKind
}

var _ ChunkManager = (*RemoteChunkManager)(nil)

func NewRemoteChunkManager(ctx context.Context, c *objectstorage.Config) (*RemoteChunkManager, error) {
	var client ObjectStorage
	var err error
	switch c.CloudProvider {
	case objectstorage.CloudProviderAzure:
		client, err = newAzureObjectStorageWithConfig(ctx, c)
	case objectstorage.CloudProviderGCPNative:
		client, err = newGcpNativeObjectStorageWithConfig(ctx, c)
	default:
		client, err = newMinioObjectStorageWithConfig(ctx, c)
	}
	if err != nil {
		return nil, err
	}
	mcm := &RemoteChunkManager{
		client:            client,
		bucketName:        c.BucketName,
		rootPath:          strings.TrimLeft(c.RootPath, "/"),
		readRetryAttempts: c.ReadRetryAttempts,
		backendKind:       backendKindFromCloudProvider(c.CloudProvider),
	}
	mlog.Info(ctx, "remote chunk manager init success.", mlog.String("remote", c.CloudProvider), mlog.String("bucketname", c.BucketName), mlog.String("root", mcm.RootPath()))
	return mcm, nil
}

// NewRemoteChunkManagerForTesting is used for testing.
func NewRemoteChunkManagerForTesting(c *minio.Client, bucket string, rootPath string) *RemoteChunkManager {
	mcm := &RemoteChunkManager{
		client:            &MinioObjectStorage{c},
		bucketName:        bucket,
		rootPath:          rootPath,
		readRetryAttempts: 10,
		backendKind:       storageprofile.BackendKindS3Compatible,
	}
	return mcm
}

// RootPath returns minio root path.
func (mcm *RemoteChunkManager) RootPath() string {
	return mcm.rootPath
}

// BucketName returns the bucket name this chunk manager is configured with.
func (mcm *RemoteChunkManager) BucketName() string {
	return mcm.bucketName
}

// UnderlyingObjectStorage returns the underlying object storage.
func (mcm *RemoteChunkManager) UnderlyingObjectStorage() ObjectStorage {
	return mcm.client
}

// Path returns the path of minio data if exists.
func (mcm *RemoteChunkManager) Path(ctx context.Context, filePath string) (string, error) {
	ctx = storageprofile.WithBackendKind(ctx, mcm.backendKind)
	operation := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{Operation: storageprofile.StorageOperationStat, AccessLayer: storageprofile.AccessLayerMilvus})
	exist, err := mcm.Exist(storageprofile.WithSuppressed(ctx), filePath)
	if err != nil {
		operation.Finish(storageprofile.OperationResult{Err: err})
		return "", err
	}
	if !exist {
		err := merr.WrapErrServiceInternalMsg("minio file manage cannot be found with filePath:" + filePath)
		operation.Finish(storageprofile.OperationResult{Err: err, Category: storageprofile.ErrorCategoryNotFound})
		return "", err
	}
	operation.Finish(storageprofile.OperationResult{})
	return filePath, nil
}

// Reader returns the path of minio data if exists.
func (mcm *RemoteChunkManager) Reader(ctx context.Context, filePath string) (FileReader, error) {
	ctx = storageprofile.WithBackendKind(ctx, mcm.backendKind)
	operation := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{Operation: storageprofile.StorageOperationRead, AccessLayer: storageprofile.AccessLayerMilvus, StreamingTTFBObservable: true})
	reader, err := mcm.getObject(ctx, mcm.bucketName, filePath, int64(0), int64(0))
	if err != nil {
		mlog.Warn(ctx, "failed to get object", mlog.String("bucket", mcm.bucketName), mlog.String("path", filePath), mlog.Err(err))
		operation.Finish(storageprofile.OperationResult{Err: err})
		return nil, err
	}
	return newInstrumentedFileReader(reader, operation, func(err error) storageprofile.ErrorCategory {
		return storageprofile.ClassifyError(mapObjectStorageError(filePath, err))
	}), nil
}

func (mcm *RemoteChunkManager) ReaderAtOffset(ctx context.Context, filePath string, offset int64) (FileReader, error) {
	ctx = storageprofile.WithBackendKind(ctx, mcm.backendKind)
	operation := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{Operation: storageprofile.StorageOperationRangeRead, AccessLayer: storageprofile.AccessLayerMilvus, StreamingTTFBObservable: true})
	if offset < 0 {
		operation.Finish(storageprofile.OperationResult{Err: io.EOF, Category: storageprofile.ErrorCategoryInvalidRange})
		return nil, io.EOF
	}

	reader, err := mcm.getObject(ctx, mcm.bucketName, filePath, offset, int64(0))
	if err != nil {
		mlog.Warn(ctx, "failed to get object", mlog.String("bucket", mcm.bucketName), mlog.String("path", filePath), mlog.Int64("offset", offset), mlog.Err(err))
		operation.Finish(storageprofile.OperationResult{Err: err})
		return nil, err
	}
	return newInstrumentedFileReader(reader, operation, func(err error) storageprofile.ErrorCategory {
		return storageprofile.ClassifyError(mapObjectStorageError(filePath, err))
	}), nil
}

func (mcm *RemoteChunkManager) Size(ctx context.Context, filePath string) (int64, error) {
	ctx = storageprofile.WithBackendKind(ctx, mcm.backendKind)
	operation := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{Operation: storageprofile.StorageOperationStat, AccessLayer: storageprofile.AccessLayerMilvus})
	var objectInfo int64
	var err error
	var attempts uint64
	var retryReason storageprofile.ErrorCategory
	err = retry.Handle(ctx, func() (bool, error) {
		attempts++
		objectInfo, err = mcm.getObjectSize(ctx, mcm.bucketName, filePath)
		if err == nil {
			return false, nil
		}
		mlog.Warn(ctx, "failed to get object size", mlog.String("bucket", mcm.bucketName), mlog.String("path", filePath), mlog.Err(err))
		err = mapObjectStorageError(filePath, err)
		if merr.IsRetryableErr(err) {
			retryReason = storageprofile.ClassifyError(err)
			return true, err
		}
		return false, err
	}, retry.Attempts(mcm.readRetryAttempts))
	result := storageprofile.OperationResult{Err: err, RetryReason: retryReason}
	if attempts > 1 {
		result.RetryCount = attempts - 1
	}
	operation.Finish(result)
	return objectInfo, err
}

// Write writes the data to minio storage.
func (mcm *RemoteChunkManager) Write(ctx context.Context, filePath string, content []byte) error {
	ctx = storageprofile.WithBackendKind(ctx, mcm.backendKind)
	operation := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{
		Operation: storageprofile.StorageOperationWrite, AccessLayer: storageprofile.AccessLayerMilvus,
		BytesRequested: uint64(len(content)), RequestedBytesKnown: true,
	})
	err := mcm.putObject(ctx, mcm.bucketName, filePath, bytes.NewReader(content), int64(len(content)))
	if err != nil {
		mlog.Warn(ctx, "failed to put object", mlog.String("bucket", mcm.bucketName), mlog.String("path", filePath), mlog.Err(err))
		operation.Finish(storageprofile.OperationResult{Err: err, SizeKnown: true})
		return err
	}

	operation.AddCompletedBytes(uint64(len(content)))
	operation.Finish(storageprofile.OperationResult{SizeKnown: true})
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
			el = merr.Combine(el, merr.Wrapf(err, "failed to write %s", key))
		}
	}
	return el
}

// Exist checks whether chunk is saved to minio storage.
func (mcm *RemoteChunkManager) Exist(ctx context.Context, filePath string) (bool, error) {
	ctx = storageprofile.WithBackendKind(ctx, mcm.backendKind)
	operation := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{Operation: storageprofile.StorageOperationStat, AccessLayer: storageprofile.AccessLayerMilvus})
	_, err := mcm.getObjectSize(ctx, mcm.bucketName, filePath)
	if err != nil {
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			operation.Finish(storageprofile.OperationResult{Err: err})
			return false, nil
		}
		mlog.Warn(ctx, "failed to stat object", mlog.String("bucket", mcm.bucketName), mlog.String("path", filePath), mlog.Err(err))
		operation.Finish(storageprofile.OperationResult{Err: err})
		return false, err
	}
	operation.Finish(storageprofile.OperationResult{})
	return true, nil
}

// Read reads the minio storage data if exists.
func (mcm *RemoteChunkManager) Read(ctx context.Context, filePath string) ([]byte, error) {
	ctx = storageprofile.WithBackendKind(ctx, mcm.backendKind)
	operation := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{Operation: storageprofile.StorageOperationRead, AccessLayer: storageprofile.AccessLayerMilvus})
	var data []byte
	var attempts uint64
	var retryReason storageprofile.ErrorCategory
	err := retry.Do(ctx, func() error {
		attempts++
		object, err := mcm.getObject(ctx, mcm.bucketName, filePath, int64(0), int64(0))
		if err != nil {
			if merr.IsRetryableErr(err) {
				retryReason = storageprofile.ClassifyError(err)
			}
			mlog.Warn(ctx, "failed to get object", mlog.String("bucket", mcm.bucketName), mlog.String("path", filePath), mlog.Err(err))
			return err
		}
		defer object.Close()

		// Prefetch object data
		var empty []byte
		_, err = object.Read(empty)
		err = mapObjectStorageError(filePath, err)
		if err != nil {
			if merr.IsRetryableErr(err) {
				retryReason = storageprofile.ClassifyError(err)
			}
			mlog.Warn(ctx, "failed to read object", mlog.String("path", filePath), mlog.Err(err))
			return err
		}
		size, err := object.Size()
		if err != nil {
			if merr.IsRetryableErr(err) {
				retryReason = storageprofile.ClassifyError(err)
			}
			mlog.Warn(ctx, "failed to stat object", mlog.String("bucket", mcm.bucketName), mlog.String("path", filePath), mlog.Err(err))
			return err
		}
		data, err = read(object, size)
		err = mapObjectStorageError(filePath, err)
		if err != nil {
			mlog.Warn(ctx, "failed to read object", mlog.String("bucket", mcm.bucketName), mlog.String("path", filePath), mlog.Err(err))
			return err
		}
		metrics.PersistentDataKvSize.WithLabelValues(metrics.DataGetLabel).Observe(float64(size))
		return nil
	}, retry.Attempts(mcm.readRetryAttempts), retry.RetryErr(merr.IsRetryableErr))
	if err != nil {
		result := storageprofile.OperationResult{Err: err, RetryReason: retryReason}
		if attempts > 1 {
			result.RetryCount = attempts - 1
		}
		operation.Finish(result)
		return nil, err
	}

	operation.AddCompletedBytes(uint64(len(data)))
	result := storageprofile.OperationResult{SizeKnown: true, RetryReason: retryReason}
	if attempts > 1 {
		result.RetryCount = attempts - 1
	}
	operation.Finish(result)
	return data, nil
}

func (mcm *RemoteChunkManager) MultiRead(ctx context.Context, keys []string) ([][]byte, error) {
	var el error
	var objectsValues [][]byte
	for _, key := range keys {
		objectValue, err := mcm.Read(ctx, key)
		if err != nil {
			el = merr.Combine(el, merr.Wrapf(err, "failed to read %s", key))
		}
		objectsValues = append(objectsValues, objectValue)
	}

	return objectsValues, el
}

func (mcm *RemoteChunkManager) Mmap(ctx context.Context, filePath string) (*mmap.ReaderAt, error) {
	return nil, merr.WrapErrServiceInternalMsg("this method has not been implemented")
}

// ReadAt reads specific position data of minio storage if exists.
func (mcm *RemoteChunkManager) ReadAt(ctx context.Context, filePath string, off int64, length int64) ([]byte, error) {
	ctx = storageprofile.WithBackendKind(ctx, mcm.backendKind)
	operation := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{
		Operation: storageprofile.StorageOperationRangeRead, AccessLayer: storageprofile.AccessLayerMilvus,
		BytesRequested: uint64(max(length, 0)), RequestedBytesKnown: length >= 0,
	})
	if off < 0 || length < 0 {
		operation.Finish(storageprofile.OperationResult{Err: io.EOF, Category: storageprofile.ErrorCategoryInvalidRange, SizeKnown: length >= 0})
		return nil, io.EOF
	}

	object, err := mcm.getObject(ctx, mcm.bucketName, filePath, off, length)
	if err != nil {
		mlog.Warn(ctx, "failed to get object", mlog.String("bucket", mcm.bucketName), mlog.String("path", filePath), mlog.Err(err))
		operation.Finish(storageprofile.OperationResult{Err: err, SizeKnown: true})
		return nil, err
	}
	defer object.Close()

	data, err := read(object, length)
	err = mapObjectStorageError(filePath, err)
	if err != nil {
		mlog.Warn(ctx, "failed to read object", mlog.String("bucket", mcm.bucketName), mlog.String("path", filePath), mlog.Err(err))
		operation.Finish(storageprofile.OperationResult{Err: err, SizeKnown: true})
		return nil, err
	}
	operation.AddCompletedBytes(uint64(len(data)))
	if int64(len(data)) < length {
		operation.Finish(storageprofile.OperationResult{
			Err:       io.ErrUnexpectedEOF,
			Category:  storageprofile.ErrorCategoryUnexpectedEOF,
			SizeKnown: true,
		})
		return data, nil
	}
	operation.Finish(storageprofile.OperationResult{SizeKnown: true})
	metrics.PersistentDataKvSize.WithLabelValues(metrics.DataGetLabel).Observe(float64(length))
	return data, nil
}

// Remove deletes an object with @key.
func (mcm *RemoteChunkManager) Remove(ctx context.Context, filePath string) error {
	ctx = storageprofile.WithBackendKind(ctx, mcm.backendKind)
	operation := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{Operation: storageprofile.StorageOperationDelete, AccessLayer: storageprofile.AccessLayerMilvus})
	err := mcm.removeObject(ctx, mcm.bucketName, filePath)
	if err != nil {
		mlog.Warn(ctx, "failed to remove object", mlog.String("bucket", mcm.bucketName), mlog.String("path", filePath), mlog.Err(err))
		operation.Finish(storageprofile.OperationResult{Err: err})
		return err
	}
	operation.Finish(storageprofile.OperationResult{})
	return nil
}

// MultiRemove deletes a objects with @keys.
func (mcm *RemoteChunkManager) MultiRemove(ctx context.Context, keys []string) error {
	var el error
	for _, key := range keys {
		err := mcm.Remove(ctx, key)
		if err != nil {
			el = merr.Combine(el, merr.Wrapf(err, "failed to remove %s", key))
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
			err := mcm.Remove(ctx, key)
			if err != nil {
				mlog.Warn(ctx, "failed to remove object", mlog.String("path", key), mlog.Err(err))
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
	ctx = storageprofile.WithBackendKind(ctx, mcm.backendKind)
	operation := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{Operation: storageprofile.StorageOperationList, AccessLayer: storageprofile.AccessLayerMilvus})
	start := timerecord.NewTimeRecorder("WalkWithPrefix")
	metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataWalkLabel, metrics.TotalLabel).Inc()
	logger := mlog.With(mlog.String("prefix", prefix), mlog.Bool("recursive", recursive))

	logger.Info(ctx, "start walk through objects")
	if err := mcm.client.WalkWithObjects(ctx, mcm.bucketName, prefix, recursive, walkFunc); err != nil {
		metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataWalkLabel, metrics.FailLabel).Inc()
		logger.Warn(ctx, "failed to walk through objects", mlog.Err(err))
		operation.Finish(storageprofile.OperationResult{
			Err:      err,
			Category: storageprofile.ClassifyError(mapObjectStorageError(prefix, err)),
		})
		return err
	}
	metrics.PersistentDataRequestLatency.WithLabelValues(metrics.DataWalkLabel).
		Observe(float64(start.ElapseSpan().Milliseconds()))
	metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataWalkLabel, metrics.SuccessLabel).Inc()
	logger.Info(ctx, "finish walk through objects")
	operation.Finish(storageprofile.OperationResult{})
	return nil
}

func (mcm *RemoteChunkManager) getObject(ctx context.Context, bucketName, objectName string,
	offset int64, size int64,
) (FileReader, error) {
	start := timerecord.NewTimeRecorder("getObject")
	reader, err := mcm.client.GetObject(ctx, bucketName, objectName, offset, size)
	metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataGetLabel, metrics.TotalLabel).Inc()
	if err == nil && reader != nil {
		metrics.PersistentDataRequestLatency.WithLabelValues(metrics.DataGetLabel).
			Observe(float64(start.ElapseSpan().Milliseconds()))
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

func ToMilvusIoError(fileName string, err error) error {
	return mapObjectStorageError(fileName, err)
}

func (mcm *RemoteChunkManager) Copy(ctx context.Context, srcFilePath string, dstFilePath string) error {
	ctx = storageprofile.WithBackendKind(ctx, mcm.backendKind)
	operation := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{Operation: storageprofile.StorageOperationCopy, AccessLayer: storageprofile.AccessLayerMilvus})
	err := mcm.copyObject(ctx, mcm.bucketName, srcFilePath, dstFilePath)
	if err != nil {
		mlog.Warn(ctx, "failed to copy object", mlog.String("bucket", mcm.bucketName), mlog.String("src", srcFilePath), mlog.String("dst", dstFilePath), mlog.Err(err))
		operation.Finish(storageprofile.OperationResult{Err: err})
		return err
	}
	operation.Finish(storageprofile.OperationResult{})
	return nil
}

func backendKindFromCloudProvider(provider string) storageprofile.BackendKind {
	switch provider {
	case objectstorage.CloudProviderAzure:
		return storageprofile.BackendKindAzure
	case objectstorage.CloudProviderGCP, objectstorage.CloudProviderGCPNative:
		return storageprofile.BackendKindGCP
	default:
		return storageprofile.BackendKindS3Compatible
	}
}

func (mcm *RemoteChunkManager) copyObject(ctx context.Context, bucketName, srcObjectName, dstObjectName string) error {
	start := timerecord.NewTimeRecorder("copyObject")

	err := mcm.client.CopyObject(ctx, bucketName, srcObjectName, dstObjectName)
	metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataPutLabel, metrics.TotalLabel).Inc()
	if err == nil {
		metrics.PersistentDataRequestLatency.WithLabelValues(metrics.DataPutLabel).
			Observe(float64(start.ElapseSpan().Milliseconds()))
		metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataPutLabel, metrics.SuccessLabel).Inc()
	} else {
		if errors.Is(err, context.Canceled) {
			metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataPutLabel, metrics.CancelLabel).Inc()
		} else {
			metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataPutLabel, metrics.FailLabel).Inc()
		}
	}

	return err
}

// Error code constants for cloud provider error mapping.
const (
	// Azure Blob Storage error codes
	azureBlobNotFound      = "BlobNotFound"
	azureServerBusy        = "ServerBusy"
	azureAuthFailed        = "AuthenticationFailed"
	azureAuthFailure       = "AuthorizationFailure"
	azureContainerNotFound = "ContainerNotFound"
	azureInvalidParam      = "InvalidParameterValue"
	azureInvalidRange      = "InvalidRange"
	azureRequestBodyTooLg  = "RequestBodyTooLarge"

	// S3-compatible error codes (AWS S3, MinIO, Aliyun OSS, Tencent COS, etc.)
	minioNoSuchKey      = "NoSuchKey"
	minioSlowDown       = "SlowDown"
	minioTooMany        = "TooManyRequestsException"
	minioAccessDenied   = "AccessDenied"
	minioInvalidKeyId   = "InvalidAccessKeyId"
	minioSigMismatch    = "SignatureDoesNotMatch"
	minioNoSuchBucket   = "NoSuchBucket"
	minioInvalidToken   = "InvalidToken"
	minioExpiredToken   = "ExpiredToken"
	minioInvalidArg     = "InvalidArgument"
	minioInvalidRequest = "InvalidRequest"
	minioInvalidRange   = "InvalidRange"
	minioEntityTooLarge = "EntityTooLarge"
	minioMaxMessage     = "MaxMessageLengthExceeded"
	// Aliyun OSS specific error codes (S3-compatible, via MinIO client)
	ossSecurityTokenExpired = "SecurityTokenExpired"
	ossInvalidAccessKeyId   = "InvalidAccessKeyId.Inactive"
)

func mapObjectStorageError(fileName string, err error) error {
	if err == nil {
		return nil
	}

	// If error is already a Milvus error, return it as-is to avoid double-wrapping
	if merr.IsMilvusError(err) {
		return err
	}

	switch err := err.(type) {
	case *azcore.ResponseError:
		switch err.ErrorCode {
		case azureBlobNotFound:
			return merr.WrapErrIoKeyNotFound(fileName, err.Error())
		case azureServerBusy:
			return merr.WrapErrIoTooManyRequests(fileName, err)
		case azureAuthFailed:
			return storageprofile.WithErrorCategory(
				merr.WrapErrIoPermissionDenied(fileName, err),
				storageprofile.ErrorCategoryInvalidCredentials,
			)
		case azureAuthFailure:
			return merr.WrapErrIoPermissionDenied(fileName, err)
		case azureContainerNotFound:
			return merr.WrapErrIoBucketNotFound(fileName, err)
		case azureInvalidParam:
			return merr.WrapErrIoInvalidArgument(fileName, err)
		case azureInvalidRange:
			return merr.WrapErrIoInvalidRange(fileName, err)
		case azureRequestBodyTooLg:
			return merr.WrapErrIoEntityTooLarge(fileName, err)
		default:
			return merr.WrapErrIoFailed(fileName, err)
		}
	case minio.ErrorResponse:
		switch err.Code {
		case minioNoSuchKey:
			return merr.WrapErrIoKeyNotFound(fileName, err.Error())
		case minioSlowDown, minioTooMany:
			return merr.WrapErrIoTooManyRequests(fileName, err)
		case minioAccessDenied:
			return merr.WrapErrIoPermissionDenied(fileName, err)
		case minioInvalidKeyId, minioSigMismatch, ossInvalidAccessKeyId:
			return storageprofile.WithErrorCategory(
				merr.WrapErrIoPermissionDenied(fileName, err),
				storageprofile.ErrorCategoryInvalidCredentials,
			)
		case minioNoSuchBucket:
			return merr.WrapErrIoBucketNotFound(fileName, err)
		case minioInvalidToken, minioExpiredToken, ossSecurityTokenExpired:
			return merr.WrapErrIoInvalidCredentials(fileName, err)
		case minioInvalidArg, minioInvalidRequest:
			return merr.WrapErrIoInvalidArgument(fileName, err)
		case minioInvalidRange:
			return merr.WrapErrIoInvalidRange(fileName, err)
		case minioEntityTooLarge, minioMaxMessage:
			return merr.WrapErrIoEntityTooLarge(fileName, err)
		default:
			return merr.WrapErrIoFailed(fileName, err)
		}
	case *googleapi.Error:
		switch err.Code {
		case http.StatusNotFound:
			return merr.WrapErrIoKeyNotFound(fileName, err.Error())
		case http.StatusTooManyRequests:
			return merr.WrapErrIoTooManyRequests(fileName, err)
		case http.StatusForbidden:
			return merr.WrapErrIoPermissionDenied(fileName, err)
		case http.StatusUnauthorized:
			return merr.WrapErrIoInvalidCredentials(fileName, err)
		case http.StatusBadRequest:
			return merr.WrapErrIoInvalidArgument(fileName, err)
		case http.StatusRequestedRangeNotSatisfiable:
			return merr.WrapErrIoInvalidRange(fileName, err)
		case http.StatusRequestEntityTooLarge:
			return merr.WrapErrIoEntityTooLarge(fileName, err)
		default:
			return merr.WrapErrIoFailed(fileName, err)
		}
	}

	// GCP cloud.google.com/go/storage sentinel errors (not *googleapi.Error)
	if errors.Is(err, cstorage.ErrObjectNotExist) {
		return merr.WrapErrIoKeyNotFound(fileName, err.Error())
	}
	if errors.Is(err, cstorage.ErrBucketNotExist) {
		return merr.WrapErrIoBucketNotFound(fileName, err)
	}

	// syscall.ECONNRESET is typically triggered by rate limiting
	if errors.Is(err, syscall.ECONNRESET) {
		return merr.WrapErrIoTooManyRequests(fileName, err)
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
