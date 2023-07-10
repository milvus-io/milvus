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
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	minio "github.com/minio/minio-go/v7"
	"go.uber.org/zap"
	"golang.org/x/exp/mmap"
	"golang.org/x/sync/errgroup"
)

type fileInfo struct {
	Size         int64
	LastModified time.Time
	Key          string
	Err          error
}

type ObjectStorage interface {
	CreateBucket(ctx context.Context) error
	Stat(ctx context.Context, name string) (*fileInfo, error) // check blob exist or get size
	Get(ctx context.Context, name string, off, length int64) (FileReader, error)
	Write(ctx context.Context, name string, data []byte) error //
	Delete(ctx context.Context, name string) error
	ListWithPrefix(ctx context.Context, preFix string, recursive bool) (iterator, error)
}

var (
	ErrNoSuchKey             = errors.New("NoSuchKey")
)

const (
	CloudProviderGCP    = "gcp"
	CloudProviderAWS    = "aws"
	CloudProviderAliyun = "aliyun"
	CloudProviderAzure  = "azure"
)

func WrapErrNoSuchKey(key string) error {
	return fmt.Errorf("%w(key=%s)", ErrNoSuchKey, key)
}

var CheckBucketRetryAttempts uint = 20

// MinioChunkManager is responsible for read and write data stored in minio.
type MinioChunkManager struct {
	ObjectStorage
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
	Object, err := newMinioObjectStorage(c)
	if err != nil {
		return nil, err
	}

	// check valid in first query
	checkBucketFn := func() error {
		return Object.CreateBucket(ctx)
	}
	err = retry.Do(ctx, checkBucketFn, retry.Attempts(CheckBucketRetryAttempts))
	if err != nil {
		return nil, err
	}

	mcm := &MinioChunkManager{
		ObjectStorage: Object,
		bucketName:    c.bucketName,
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
	objectInfo, err := mcm.ObjectStorage.Stat(ctx, filePath)
	if objectInfo.Err != nil {
		if errors.Is(objectInfo.Err, ErrNoSuchKey) {
			return nil, WrapErrNoSuchKey(filePath)
		}
		log.Warn("failed to stat object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return nil, err
	}

	return mcm.getMinioObject(ctx, filePath, 0, objectInfo.Size)
}

func (mcm *MinioChunkManager) Size(ctx context.Context, filePath string) (int64, error) {
	//objectInfo, err := mcm.statMinioObject(ctx, mcm.bucketName, filePath, minio.StatObjectOptions{})
	info, err := mcm.Stat(ctx, filePath)
	if err != nil {
		log.Warn("failed to stat object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return 0, err
	}

	return info.Size, nil
}

// Write writes the data to minio storage.
func (mcm *MinioChunkManager) Write(ctx context.Context, filePath string, content []byte) error {
	err := mcm.putMinioObject(ctx, filePath, content)

	if err != nil {
		log.Warn("failed to put object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return err
	}

	metrics.PersistentDataKvSize.WithLabelValues(metrics.DataPutLabel).Observe(float64(len(content)))
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
	_, err := mcm.statMinioObject(ctx, filePath)
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
	objectInfo, err := mcm.ObjectStorage.Stat(ctx, filePath)
	if err != nil {
		if errors.Is(err, ErrNoSuchKey) {
			return nil, WrapErrNoSuchKey(filePath)
		}
		log.Warn("failed to stat object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return nil, err
	}

	object, err := mcm.getMinioObject(ctx, filePath, 0, objectInfo.Size)
	if err != nil {
		if errors.Is(objectInfo.Err, ErrNoSuchKey) {
			return nil, WrapErrNoSuchKey(filePath)
		}
		log.Warn("failed to read object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	data, err := Read(object, objectInfo.Size)
	metrics.PersistentDataKvSize.WithLabelValues(metrics.DataGetLabel).Observe(float64(objectInfo.Size))
	if err != nil {
		if errors.Is(objectInfo.Err, ErrNoSuchKey) {
			return nil, WrapErrNoSuchKey(filePath)
		}
		log.Warn("failed to read object", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
		return nil, err
	}
	return data, err
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

	//opts := minio.GetObjectOptions{}
	//err := opts.SetRange(off, off+length-1)
	//if err != nil {
	//	log.Warn("failed to set range", zap.String("bucket", mcm.bucketName), zap.String("path", filePath), zap.Error(err))
	//	return nil, err
	//}

	object, err := mcm.getMinioObject(ctx, filePath, off, off+length-1)
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
	metrics.PersistentDataKvSize.WithLabelValues(metrics.DataGetLabel).Observe(float64(length))
	return data, nil
}

// Remove deletes an object with @key.
func (mcm *MinioChunkManager) Remove(ctx context.Context, filePath string) error {
	err := mcm.removeMinioObject(ctx, filePath)
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
	objects, err := mcm.listMinioObjects(ctx, prefix, true)
	if err != nil {
		return err
	}
	i := 0
	maxGoroutine := 10
	removeKeys := make([]string, 0)
	for objects.HasNext(nil) {
		object := objects.Next(nil)
		if object.Err != nil {
			return object.Err
		}
		removeKeys = append(removeKeys, object.Key)
	}
	for i < len(removeKeys) {
		runningGroup, groupCtx := errgroup.WithContext(ctx)
		for j := 0; j < maxGoroutine && i < len(removeKeys); j++ {
			key := removeKeys[i]
			runningGroup.Go(func() error {
				err := mcm.removeMinioObject(groupCtx, key)
				if err != nil {
					log.Warn("failed to remove object", zap.String("path", key), zap.Error(err))
					return err
				}
				return nil
			})
			i++
		}
		if err := runningGroup.Wait(); err != nil {
			return err
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
		objects, err := mcm.listMinioObjects(ctx, pre, recursive)
		if err != nil {
			return nil, nil, err
		}
		for objects.HasNext(ctx) {
			object := objects.Next(ctx)
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

func (mcm *MinioChunkManager) getMinioObject(ctx context.Context, objectName string, offset int64, length int64) (io.ReadCloser, error) {
	start := timerecord.NewTimeRecorder("getMinioObject")

	reader, err := mcm.Get(ctx, objectName, offset, length)

	updateMetrics(metrics.DataGetLabel, err, start)
	return reader, err
}

func (mcm *MinioChunkManager) putMinioObject(ctx context.Context, objectName string, content []byte) error {
	start := timerecord.NewTimeRecorder("putMinioObject")

	//info, err := mcm.Client.PutObject(ctx, bucketName, objectName, reader, objectSize, opts)
	err := mcm.ObjectStorage.Write(ctx, objectName, content)
	metrics.PersistentDataOpCounter.WithLabelValues(metrics.DataPutLabel, metrics.TotalLabel).Inc()
	updateMetrics(metrics.DataPutLabel, err, start)
	return err
}

func (mcm *MinioChunkManager) statMinioObject(ctx context.Context, objectName string) (*fileInfo, error) {
	start := timerecord.NewTimeRecorder("statMinioObject")

	info, err := mcm.ObjectStorage.Stat(ctx, objectName)
	updateMetrics(metrics.DataStatLabel, err, start)

	return info, err
}

type iterator interface {
	HasNext(ctx context.Context) bool
	Next(ctx context.Context) fileInfo
}

func (mcm *MinioChunkManager) listMinioObjects(ctx context.Context, prefix string, recurive bool) (iterator, error) {
	start := timerecord.NewTimeRecorder("listMinioObjects")

	res, err := mcm.ObjectStorage.ListWithPrefix(ctx, prefix, recurive)
	updateMetrics(metrics.DataListLabel, err, start)
	return res, err
}

func (mcm *MinioChunkManager) removeMinioObject(ctx context.Context, objectName string) error {
	start := timerecord.NewTimeRecorder("removeMinioObject")

	err := mcm.ObjectStorage.Delete(ctx, objectName)
	updateMetrics(metrics.DataRemoveLabel, nil, start)
	return err
}

func updateMetrics(label string, err error, start *timerecord.TimeRecorder) {
	metrics.PersistentDataOpCounter.WithLabelValues(label, metrics.TotalLabel).Inc()
	if err == nil {
		metrics.PersistentDataRequestLatency.WithLabelValues(label).Observe(float64(start.ElapseSpan().Milliseconds()))
		metrics.PersistentDataOpCounter.WithLabelValues(label, metrics.SuccessLabel).Inc()
	} else {
		metrics.PersistentDataOpCounter.WithLabelValues(label, metrics.FailLabel).Inc()
	}
}
