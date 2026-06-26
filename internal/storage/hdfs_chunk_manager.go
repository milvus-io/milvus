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
	"strings"

	"github.com/colinmarc/hdfs/v2"
	"golang.org/x/exp/mmap"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// HDFSChunkManager implements ChunkManager backed by HDFS.
type HDFSChunkManager struct {
	client   *hdfs.Client
	rootPath string
}

var _ ChunkManager = (*HDFSChunkManager)(nil)

// NewHDFSChunkManager creates a new HDFSChunkManager connected to the given NameNode address.
func NewHDFSChunkManager(address, user, rootPath string) (*HDFSChunkManager, error) {
	client, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: []string{address},
		User:      user,
	})
	if err != nil {
		return nil, merr.WrapErrIoFailed("hdfs_connect", err)
	}
	return &HDFSChunkManager{
		client:   client,
		rootPath: strings.TrimRight(rootPath, "/"),
	}, nil
}

func (h *HDFSChunkManager) RootPath() string {
	return h.rootPath
}

func (h *HDFSChunkManager) Path(ctx context.Context, filePath string) (string, error) {
	abs := h.absPath(filePath)
	_, err := h.client.Stat(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return "", merr.WrapErrIoKeyNotFound(filePath)
		}
		return "", merr.WrapErrIoFailed(filePath, err)
	}
	return abs, nil
}

func (h *HDFSChunkManager) Size(ctx context.Context, filePath string) (int64, error) {
	info, err := h.client.Stat(h.absPath(filePath))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, merr.WrapErrIoKeyNotFound(filePath)
		}
		return 0, merr.WrapErrIoFailed(filePath, err)
	}
	return info.Size(), nil
}

func (h *HDFSChunkManager) Write(ctx context.Context, filePath string, content []byte) error {
	abs := h.absPath(filePath)
	if err := h.client.MkdirAll(hdfsParentDir(abs), 0o755); err != nil {
		return merr.WrapErrIoFailed(filePath, err)
	}
	// Remove existing file so Create doesn't fail on an already-existing path.
	_ = h.client.Remove(abs)
	fw, err := h.client.Create(abs)
	if err != nil {
		return merr.WrapErrIoFailed(filePath, err)
	}
	defer fw.Close()
	if _, err = fw.Write(content); err != nil {
		return merr.WrapErrIoFailed(filePath, err)
	}
	return fw.Flush()
}

func (h *HDFSChunkManager) MultiWrite(ctx context.Context, contents map[string][]byte) error {
	var el error
	for filePath, data := range contents {
		if err := h.Write(ctx, filePath, data); err != nil {
			el = merr.Combine(el, merr.Wrapf(err, "write %s failed", filePath))
		}
	}
	return el
}

func (h *HDFSChunkManager) Exist(ctx context.Context, filePath string) (bool, error) {
	_, err := h.client.Stat(h.absPath(filePath))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, merr.WrapErrIoFailed(filePath, err)
	}
	return true, nil
}

func (h *HDFSChunkManager) Read(ctx context.Context, filePath string) ([]byte, error) {
	f, err := h.client.Open(h.absPath(filePath))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, merr.WrapErrIoKeyNotFound(filePath)
		}
		return nil, merr.WrapErrIoFailed(filePath, err)
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, merr.WrapErrIoFailed(filePath, err)
	}
	return data, nil
}

func (h *HDFSChunkManager) Reader(ctx context.Context, filePath string) (FileReader, error) {
	f, err := h.client.Open(h.absPath(filePath))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, merr.WrapErrIoKeyNotFound(filePath)
		}
		return nil, merr.WrapErrIoFailed(filePath, err)
	}
	return &hdfsReader{f}, nil
}

func (h *HDFSChunkManager) MultiRead(ctx context.Context, filePaths []string) ([][]byte, error) {
	results := make([][]byte, len(filePaths))
	var el error
	for i, p := range filePaths {
		data, err := h.Read(ctx, p)
		if err != nil {
			el = merr.Combine(el, merr.Wrapf(err, "failed to read %s", p))
		}
		results[i] = data
	}
	return results, el
}

func (h *HDFSChunkManager) WalkWithPrefix(ctx context.Context, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc) (err error) {
	abs := h.absPath(prefix)
	logger := mlog.With(mlog.String("prefix", abs), mlog.Bool("recursive", recursive))
	logger.Info(ctx, "start walk through HDFS objects")
	defer func() {
		if err != nil {
			logger.Warn(ctx, "failed to walk through HDFS objects", mlog.Err(err))
			return
		}
		logger.Info(ctx, "finish walk through HDFS objects")
	}()

	// Derive the directory that contains objects matching the prefix.
	dir := hdfsParentDir(abs)
	infos, readErr := h.client.ReadDir(dir)
	if readErr != nil {
		if os.IsNotExist(readErr) {
			return nil
		}
		return merr.WrapErrIoFailed(dir, readErr)
	}

	for _, info := range infos {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fullPath := dir + "/" + info.Name()
		if !strings.HasPrefix(fullPath, abs) {
			continue
		}
		if info.IsDir() {
			if !recursive {
				continue
			}
			// Recurse into the sub-directory.
			if err = h.WalkWithPrefix(ctx, fullPath+"/", recursive, walkFunc); err != nil {
				return err
			}
			continue
		}
		if !walkFunc(&ChunkObjectInfo{FilePath: fullPath, ModifyTime: info.ModTime()}) {
			return nil
		}
	}
	return nil
}

// Mmap is not supported over HDFS.
func (h *HDFSChunkManager) Mmap(ctx context.Context, filePath string) (*mmap.ReaderAt, error) {
	return nil, merr.WrapErrServiceInternalMsg("Mmap is not supported on HDFS")
}

func (h *HDFSChunkManager) ReadAt(ctx context.Context, filePath string, off int64, length int64) ([]byte, error) {
	if off < 0 || length < 0 {
		return nil, io.EOF
	}
	f, err := h.client.Open(h.absPath(filePath))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, merr.WrapErrIoKeyNotFound(filePath)
		}
		return nil, merr.WrapErrIoFailed(filePath, err)
	}
	defer f.Close()
	buf := make([]byte, length)
	if _, err = f.ReadAt(buf, off); err != nil && err != io.EOF {
		return nil, merr.WrapErrIoFailed(filePath, err)
	}
	return buf, nil
}

func (h *HDFSChunkManager) Remove(ctx context.Context, filePath string) error {
	err := h.client.Remove(h.absPath(filePath))
	if err != nil && !os.IsNotExist(err) {
		return merr.WrapErrIoFailed(filePath, err)
	}
	return nil
}

func (h *HDFSChunkManager) MultiRemove(ctx context.Context, filePaths []string) error {
	var el error
	for _, p := range filePaths {
		if err := h.Remove(ctx, p); err != nil {
			el = merr.Combine(el, err)
		}
	}
	return el
}

func (h *HDFSChunkManager) RemoveWithPrefix(ctx context.Context, prefix string) error {
	if len(prefix) == 0 {
		errMsg := "empty prefix is not allowed for ChunkManager remove operation"
		mlog.Warn(ctx, errMsg)
		return merr.WrapErrStorageMsg("%s", errMsg)
	}
	var removeErr error
	if err := h.WalkWithPrefix(ctx, prefix, true, func(info *ChunkObjectInfo) bool {
		if err := h.Remove(ctx, info.FilePath); err != nil {
			removeErr = err
		}
		return true
	}); err != nil {
		return err
	}
	return removeErr
}

func (h *HDFSChunkManager) Copy(ctx context.Context, srcFilePath, dstFilePath string) error {
	data, err := h.Read(ctx, srcFilePath)
	if err != nil {
		return err
	}
	return h.Write(ctx, dstFilePath, data)
}

// absPath prepends rootPath when the given path is not already absolute.
func (h *HDFSChunkManager) absPath(p string) string {
	if strings.HasPrefix(p, "/") {
		return p
	}
	return h.rootPath + "/" + p
}

// hdfsParentDir returns the parent directory of an HDFS path.
func hdfsParentDir(path string) string {
	path = strings.TrimRight(path, "/")
	idx := strings.LastIndex(path, "/")
	if idx <= 0 {
		return "/"
	}
	return path[:idx]
}

// hdfsReader wraps *hdfs.FileReader to satisfy the FileReader interface.
type hdfsReader struct {
	*hdfs.FileReader
}

func (r *hdfsReader) Size() (int64, error) {
	return r.FileReader.Stat().Size(), nil
}
