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
	"context"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
	"golang.org/x/exp/mmap"

	"github.com/milvus-io/milvus/internal/storageprofile"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// LocalChunkManager is responsible for read and write local file.
type LocalChunkManager struct {
	localPath string
}

var _ ChunkManager = (*LocalChunkManager)(nil)

// NewLocalChunkManager create a new local manager object.
func NewLocalChunkManager(opts ...objectstorage.Option) *LocalChunkManager {
	c := objectstorage.NewDefaultConfig()
	for _, opt := range opts {
		opt(c)
	}
	return &LocalChunkManager{
		localPath: c.RootPath,
	}
}

// RootPath returns lcm root path.
func (lcm *LocalChunkManager) RootPath() string {
	return lcm.localPath
}

// Path returns the path of local data if exists.
func (lcm *LocalChunkManager) Path(ctx context.Context, filePath string) (string, error) {
	ctx = storageprofile.WithBackendKind(ctx, storageprofile.BackendKindLocal)
	operation := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{Operation: storageprofile.StorageOperationStat, AccessLayer: storageprofile.AccessLayerMilvus})
	exist, err := lcm.Exist(storageprofile.WithSuppressed(ctx), filePath)
	if err != nil {
		operation.Finish(storageprofile.OperationResult{Err: err})
		return "", err
	}

	if !exist {
		err := merr.WrapErrIoKeyNotFound(filePath)
		operation.Finish(storageprofile.OperationResult{Err: err})
		return "", err
	}

	operation.Finish(storageprofile.OperationResult{})
	return filePath, nil
}

func (lcm *LocalChunkManager) Reader(ctx context.Context, filePath string) (FileReader, error) {
	ctx = storageprofile.WithBackendKind(ctx, storageprofile.BackendKindLocal)
	operation := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{Operation: storageprofile.StorageOperationRead, AccessLayer: storageprofile.AccessLayerMilvus, StreamingTTFBObservable: true})
	file, err := Open(filePath)
	if err != nil {
		operation.Finish(storageprofile.OperationResult{Err: err})
		return nil, err
	}
	return newInstrumentedFileReader(&LocalReader{
		File: file,
	}, operation), nil
}

func (lcm *LocalChunkManager) ReaderAtOffset(ctx context.Context, filePath string, offset int64) (FileReader, error) {
	ctx = storageprofile.WithBackendKind(ctx, storageprofile.BackendKindLocal)
	operation := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{Operation: storageprofile.StorageOperationRangeRead, AccessLayer: storageprofile.AccessLayerMilvus, StreamingTTFBObservable: true})
	if offset < 0 {
		operation.Finish(storageprofile.OperationResult{Err: io.EOF, Category: storageprofile.ErrorCategoryInvalidRange})
		return nil, io.EOF
	}
	reader, err := lcm.Reader(storageprofile.WithSuppressed(ctx), filePath)
	if err != nil {
		operation.Finish(storageprofile.OperationResult{Err: err})
		return nil, err
	}
	if offset > 0 {
		if _, err = reader.Seek(offset, io.SeekStart); err != nil {
			_ = reader.Close()
			operation.Finish(storageprofile.OperationResult{Err: err})
			return nil, err
		}
	}
	return newInstrumentedFileReader(reader, operation), nil
}

// Write writes the data to local storage.
func (lcm *LocalChunkManager) Write(ctx context.Context, filePath string, content []byte) error {
	ctx = storageprofile.WithBackendKind(ctx, storageprofile.BackendKindLocal)
	operation := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{
		Operation: storageprofile.StorageOperationWrite, AccessLayer: storageprofile.AccessLayerMilvus,
		BytesRequested: uint64(len(content)), RequestedBytesKnown: true,
	})
	dir := path.Dir(filePath)
	exist, err := lcm.Exist(storageprofile.WithSuppressed(ctx), dir)
	if err != nil {
		operation.Finish(storageprofile.OperationResult{Err: err, SizeKnown: true})
		return err
	}
	if !exist {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			operation.Finish(storageprofile.OperationResult{Err: err, SizeKnown: true})
			return merr.WrapErrIoFailed(filePath, err)
		}
	}
	err = WriteFile(filePath, content, os.ModePerm)
	if err == nil {
		operation.AddCompletedBytes(uint64(len(content)))
	}
	operation.Finish(storageprofile.OperationResult{Err: err, SizeKnown: true})
	return err
}

// MultiWrite writes the data to local storage.
func (lcm *LocalChunkManager) MultiWrite(ctx context.Context, contents map[string][]byte) error {
	var el error
	for filePath, content := range contents {
		err := lcm.Write(ctx, filePath, content)
		if err != nil {
			el = merr.Combine(el, merr.Wrapf(err, "write %s failed", filePath))
		}
	}
	return el
}

// Exist checks whether chunk is saved to local storage.
func (lcm *LocalChunkManager) Exist(ctx context.Context, filePath string) (bool, error) {
	ctx = storageprofile.WithBackendKind(ctx, storageprofile.BackendKindLocal)
	operation := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{Operation: storageprofile.StorageOperationStat, AccessLayer: storageprofile.AccessLayerMilvus})
	_, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			operation.Finish(storageprofile.OperationResult{Err: merr.WrapErrIoKeyNotFound(filePath)})
			return false, nil
		}
		err = merr.WrapErrIoFailed(filePath, err)
		operation.Finish(storageprofile.OperationResult{Err: err})
		return false, err
	}
	operation.Finish(storageprofile.OperationResult{})
	return true, nil
}

// Read reads the local storage data if exists.
func (lcm *LocalChunkManager) Read(ctx context.Context, filePath string) ([]byte, error) {
	ctx = storageprofile.WithBackendKind(ctx, storageprofile.BackendKindLocal)
	operation := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{Operation: storageprofile.StorageOperationRead, AccessLayer: storageprofile.AccessLayerMilvus})
	content, err := ReadFile(filePath)
	if err == nil {
		operation.AddCompletedBytes(uint64(len(content)))
	}
	operation.Finish(storageprofile.OperationResult{Err: err, SizeKnown: true})
	return content, err
}

// MultiRead reads the local storage data if exists.
func (lcm *LocalChunkManager) MultiRead(ctx context.Context, filePaths []string) ([][]byte, error) {
	results := make([][]byte, len(filePaths))
	var el error
	for i, filePath := range filePaths {
		content, err := lcm.Read(ctx, filePath)
		if err != nil {
			el = merr.Combine(el, merr.Wrapf(err, "failed to read %s", filePath))
		}
		results[i] = content
	}
	return results, el
}

func (lcm *LocalChunkManager) WalkWithPrefix(ctx context.Context, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc) (err error) {
	ctx = storageprofile.WithBackendKind(ctx, storageprofile.BackendKindLocal)
	operation := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{Operation: storageprofile.StorageOperationList, AccessLayer: storageprofile.AccessLayerMilvus})
	defer func() { operation.Finish(storageprofile.OperationResult{Err: err}) }()
	logger := mlog.With(mlog.String("prefix", prefix), mlog.Bool("recursive", recursive))
	logger.Info(ctx, "start walk through objects")
	defer func() {
		if err != nil {
			logger.Warn(ctx, "failed to walk through objects", mlog.Err(err))
			return
		}
		logger.Info(ctx, "finish walk through objects")
	}()

	if recursive {
		dir := filepath.Dir(prefix)
		return filepath.Walk(dir, func(filePath string, f os.FileInfo, err error) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err != nil {
				return err
			}

			if strings.HasPrefix(filePath, prefix) && !f.IsDir() {
				if !walkFunc(&ChunkObjectInfo{FilePath: filePath, ModifyTime: f.ModTime()}) {
					return nil
				}
			}
			return nil
		})
	}

	globPaths, err := filepath.Glob(prefix + "*")
	if err != nil {
		return err
	}
	for _, filePath := range globPaths {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		f, err := os.Stat(filePath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return merr.WrapErrIoKeyNotFound(filePath)
			}
			return merr.WrapErrIoFailed(filePath, err)
		}
		if !walkFunc(&ChunkObjectInfo{FilePath: filePath, ModifyTime: f.ModTime()}) {
			return nil
		}
	}
	return nil
}

// ReadAt reads specific position data of local storage if exists.
func (lcm *LocalChunkManager) ReadAt(ctx context.Context, filePath string, off int64, length int64) ([]byte, error) {
	ctx = storageprofile.WithBackendKind(ctx, storageprofile.BackendKindLocal)
	operation := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{
		Operation: storageprofile.StorageOperationRangeRead, AccessLayer: storageprofile.AccessLayerMilvus,
		BytesRequested: uint64(max(length, 0)), RequestedBytesKnown: length >= 0,
	})
	if off < 0 || length < 0 {
		operation.Finish(storageprofile.OperationResult{Err: io.EOF, Category: storageprofile.ErrorCategoryInvalidRange, SizeKnown: length >= 0})
		return nil, io.EOF
	}

	file, err := Open(path.Clean(filePath))
	if err != nil {
		operation.Finish(storageprofile.OperationResult{Err: err, SizeKnown: true})
		return nil, err
	}
	defer file.Close()

	res := make([]byte, length)
	n, err := file.ReadAt(res, off)
	if n > 0 {
		operation.AddCompletedBytes(uint64(n))
	}
	if err != nil {
		category := storageprofile.ErrorCategoryIOFailed
		if errors.Is(err, io.EOF) {
			category = storageprofile.ErrorCategoryUnexpectedEOF
		}
		err = merr.WrapErrIoFailed(filePath, err)
		operation.Finish(storageprofile.OperationResult{Err: err, Category: category, SizeKnown: true})
		return nil, err
	}
	operation.Finish(storageprofile.OperationResult{SizeKnown: true})
	return res, nil
}

func (lcm *LocalChunkManager) Mmap(ctx context.Context, filePath string) (*mmap.ReaderAt, error) {
	reader, err := mmap.Open(path.Clean(filePath))
	if errors.Is(err, os.ErrNotExist) {
		return nil, merr.WrapErrIoKeyNotFound(filePath, err.Error())
	}

	return reader, merr.WrapErrIoFailed(filePath, err)
}

func (lcm *LocalChunkManager) Size(ctx context.Context, filePath string) (int64, error) {
	ctx = storageprofile.WithBackendKind(ctx, storageprofile.BackendKindLocal)
	operation := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{Operation: storageprofile.StorageOperationStat, AccessLayer: storageprofile.AccessLayerMilvus})
	fi, err := os.Stat(filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = merr.WrapErrIoKeyNotFound(filePath, err.Error())
			operation.Finish(storageprofile.OperationResult{Err: err})
			return 0, err
		}
		err = merr.WrapErrIoFailed(filePath, err)
		operation.Finish(storageprofile.OperationResult{Err: err})
		return 0, err
	}
	// get the size
	size := fi.Size()
	operation.Finish(storageprofile.OperationResult{})
	return size, nil
}

func (lcm *LocalChunkManager) Remove(ctx context.Context, filePath string) error {
	ctx = storageprofile.WithBackendKind(ctx, storageprofile.BackendKindLocal)
	operation := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{Operation: storageprofile.StorageOperationDelete, AccessLayer: storageprofile.AccessLayerMilvus})
	err := os.RemoveAll(filePath)
	err = merr.WrapErrIoFailed(filePath, err)
	operation.Finish(storageprofile.OperationResult{Err: err})
	return err
}

func (lcm *LocalChunkManager) MultiRemove(ctx context.Context, filePaths []string) error {
	errors := make([]error, 0, len(filePaths))
	for _, filePath := range filePaths {
		err := lcm.Remove(ctx, filePath)
		errors = append(errors, err)
	}
	return merr.Combine(errors...)
}

func (lcm *LocalChunkManager) RemoveWithPrefix(ctx context.Context, prefix string) error {
	// If the prefix is empty string, the ListWithPrefix() will return all files under current process work folder,
	// MultiRemove() will delete all these files. This is a danger behavior, empty prefix is not allowed.
	if len(prefix) == 0 {
		errMsg := "empty prefix is not allowed for ChunkManager remove operation"
		mlog.Warn(ctx, errMsg)
		return merr.WrapErrStorageMsg("%s", errMsg)
	}
	var removeErr error
	if err := lcm.WalkWithPrefix(ctx, prefix, true, func(chunkInfo *ChunkObjectInfo) bool {
		err := lcm.MultiRemove(ctx, []string{chunkInfo.FilePath})
		if err != nil {
			removeErr = err
		}
		return true
	}); err != nil {
		return err
	}
	return removeErr
}

func (lcm *LocalChunkManager) Copy(ctx context.Context, srcFilePath string, dstFilePath string) error {
	ctx = storageprofile.WithBackendKind(ctx, storageprofile.BackendKindLocal)
	operation := storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{Operation: storageprofile.StorageOperationCopy, AccessLayer: storageprofile.AccessLayerMilvus})
	// Read source file
	srcFile, err := Open(srcFilePath)
	if err != nil {
		err = merr.WrapErrIoFailed(srcFilePath, err)
		operation.Finish(storageprofile.OperationResult{Err: err})
		return err
	}
	defer srcFile.Close()

	// Create destination directory if not exists
	dstDir := path.Dir(dstFilePath)
	exist, err := lcm.Exist(storageprofile.WithSuppressed(ctx), dstDir)
	if err != nil {
		operation.Finish(storageprofile.OperationResult{Err: err})
		return err
	}
	if !exist {
		err := os.MkdirAll(dstDir, os.ModePerm)
		if err != nil {
			err = merr.WrapErrIoFailed(dstFilePath, err)
			operation.Finish(storageprofile.OperationResult{Err: err})
			return err
		}
	}

	// Create destination file
	dstFile, err := os.Create(dstFilePath)
	if err != nil {
		err = merr.WrapErrIoFailed(dstFilePath, err)
		operation.Finish(storageprofile.OperationResult{Err: err})
		return err
	}
	defer dstFile.Close()

	// Copy content
	written, err := io.Copy(dstFile, srcFile)
	if written > 0 {
		operation.AddCompletedBytes(uint64(written))
	}
	if err != nil {
		err = merr.WrapErrIoFailed(dstFilePath, err)
		operation.Finish(storageprofile.OperationResult{Err: err, SizeKnown: true})
		return err
	}

	operation.Finish(storageprofile.OperationResult{SizeKnown: true})
	return nil
}

type LocalReader struct {
	*os.File
}

func (lr *LocalReader) Size() (int64, error) {
	stat, err := lr.Stat()
	if err != nil {
		return -1, nil
	}
	return stat.Size(), nil
}
