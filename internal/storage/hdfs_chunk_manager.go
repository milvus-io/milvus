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
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/colinmarc/hdfs/v2"
	"golang.org/x/exp/mmap"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// errWalkStopped is returned by walkHelper when walkFunc signals early stop
// (returns false). WalkWithPrefix converts it to nil before returning.
var errWalkStopped = errors.New("walk stopped by caller")

// HDFSChunkManager implements ChunkManager backed by HDFS.
//
// Path contract: callers already include RootPath() in every key they pass,
// exactly as they do with LocalChunkManager and RemoteChunkManager.
// HDFSChunkManager uses filePath as-is and does NOT re-prepend rootPath on
// every call — rootPath is stored only for RootPath() to return.
type HDFSChunkManager struct {
	client   *hdfs.Client
	rootPath string
}

var _ ChunkManager = (*HDFSChunkManager)(nil)

// NewHDFSChunkManager dials the HDFS NameNode and verifies connectivity.
// Construction honours ctx — if the NameNode is unreachable and ctx expires,
// the call returns an error instead of blocking indefinitely.
func NewHDFSChunkManager(ctx context.Context, address, user, rootPath string) (*HDFSChunkManager, error) {
	client, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: []string{address},
		User:      user,
	})
	if err != nil {
		return nil, merr.WrapErrIoFailed("hdfs_connect", err)
	}

	// Force a NameNode RPC to fail fast when the cluster is unreachable.
	done := make(chan error, 1)
	go func() { _, e := client.Stat("/"); done <- e }()
	select {
	case <-ctx.Done():
		return nil, merr.WrapErrIoFailed("hdfs_connect", ctx.Err())
	case err = <-done:
		if err != nil {
			return nil, merr.WrapErrIoFailed("hdfs_connect", err)
		}
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
	_, err := h.client.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", merr.WrapErrIoKeyNotFound(filePath)
		}
		return "", merr.WrapErrIoFailed(filePath, err)
	}
	return filePath, nil
}

func (h *HDFSChunkManager) Size(ctx context.Context, filePath string) (int64, error) {
	info, err := h.client.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, merr.WrapErrIoKeyNotFound(filePath)
		}
		return 0, merr.WrapErrIoFailed(filePath, err)
	}
	return info.Size(), nil
}

// Write writes content to filePath atomically via a temp file + Rename so
// readers never observe a partial write or a missing key.
func (h *HDFSChunkManager) Write(ctx context.Context, filePath string, content []byte) error {
	if err := h.client.MkdirAll(path.Dir(filePath), 0o755); err != nil {
		return merr.WrapErrIoFailed(filePath, err)
	}

	tmp := filePath + ".tmp." + tmpSuffix()
	fw, err := h.client.Create(tmp)
	if err != nil {
		return merr.WrapErrIoFailed(filePath, err)
	}
	if _, err = fw.Write(content); err != nil {
		fw.Close()
		_ = h.client.Remove(tmp)
		return merr.WrapErrIoFailed(filePath, err)
	}
	if err = fw.Flush(); err != nil {
		fw.Close()
		_ = h.client.Remove(tmp)
		return merr.WrapErrIoFailed(filePath, err)
	}
	if err = fw.Close(); err != nil {
		_ = h.client.Remove(tmp)
		return merr.WrapErrIoFailed(filePath, err)
	}

	// HDFS Rename requires the destination to not exist.
	if rmErr := h.client.Remove(filePath); rmErr != nil && !os.IsNotExist(rmErr) {
		_ = h.client.Remove(tmp)
		return merr.WrapErrIoFailed(filePath, rmErr)
	}
	if err = h.client.Rename(tmp, filePath); err != nil {
		_ = h.client.Remove(tmp)
		return merr.WrapErrIoFailed(filePath, err)
	}
	return nil
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
	_, err := h.client.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, merr.WrapErrIoFailed(filePath, err)
	}
	return true, nil
}

func (h *HDFSChunkManager) Read(ctx context.Context, filePath string) ([]byte, error) {
	f, err := h.client.Open(filePath)
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
	f, err := h.client.Open(filePath)
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

// WalkWithPrefix lists all objects whose path starts with prefix, calling
// walkFunc for each one. When recursive is false, directory entries (common
// prefixes) are yielded — matching MinIO/S3 backend semantics that callers
// such as datacoord/import_util.go depend on. Walking stops when walkFunc
// returns false or ctx is cancelled.
func (h *HDFSChunkManager) WalkWithPrefix(ctx context.Context, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc) (err error) {
	logger := mlog.With(mlog.String("prefix", prefix), mlog.Bool("recursive", recursive))
	logger.Info(ctx, "start walk through HDFS objects")
	defer func() {
		if err != nil {
			logger.Warn(ctx, "failed to walk through HDFS objects", mlog.Err(err))
			return
		}
		logger.Info(ctx, "finish walk through HDFS objects")
	}()

	walkErr := h.walkHelper(ctx, prefix, recursive, walkFunc)
	if walkErr != nil && walkErr != errWalkStopped {
		return walkErr
	}
	return nil
}

// walkHelper is the recursive core. It returns errWalkStopped when walkFunc
// signals early termination; the sentinel propagates through all recursion
// levels and is stripped by WalkWithPrefix before returning to the caller.
func (h *HDFSChunkManager) walkHelper(ctx context.Context, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc) error {
	dir := strings.TrimRight(prefix, "/")

	infos, err := h.client.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		// prefix is a partial file-name, not a directory — list parent + filter.
		return h.walkParentWithFilter(ctx, prefix, recursive, walkFunc)
	}

	return h.walkEntries(ctx, dir, infos, recursive, walkFunc)
}

// walkParentWithFilter handles the case where prefix is a partial name
// (e.g. "/milvus/logs/seg_4" matching seg_40, seg_41).
func (h *HDFSChunkManager) walkParentWithFilter(ctx context.Context, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc) error {
	dir := strings.TrimRight(prefix, "/")
	parentDir := path.Dir(dir)
	basePfx := path.Base(dir)

	infos, err := h.client.ReadDir(parentDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return merr.WrapErrIoFailed(prefix, err)
	}

	var filtered []os.FileInfo
	for _, info := range infos {
		if strings.HasPrefix(info.Name(), basePfx) {
			filtered = append(filtered, info)
		}
	}
	return h.walkEntries(ctx, parentDir, filtered, recursive, walkFunc)
}

// walkEntries iterates a ReadDir result, recursing into sub-directories when
// recursive is true, or emitting them as common-prefix entries when false.
func (h *HDFSChunkManager) walkEntries(ctx context.Context, dir string, infos []os.FileInfo, recursive bool, walkFunc ChunkObjectWalkFunc) error {
	for _, info := range infos {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fullPath := path.Join(dir, info.Name())
		if info.IsDir() {
			if recursive {
				if err := h.walkHelper(ctx, fullPath, recursive, walkFunc); err != nil {
					return err // propagates errWalkStopped upward
				}
			} else {
				// Emit directory as a common-prefix entry (matches S3/minio semantics).
				if !walkFunc(&ChunkObjectInfo{FilePath: fullPath, ModifyTime: info.ModTime()}) {
					return errWalkStopped
				}
			}
			continue
		}
		if !walkFunc(&ChunkObjectInfo{FilePath: fullPath, ModifyTime: info.ModTime()}) {
			return errWalkStopped
		}
	}
	return nil
}

// Mmap is not supported over HDFS.
func (h *HDFSChunkManager) Mmap(ctx context.Context, filePath string) (*mmap.ReaderAt, error) {
	return nil, merr.WrapErrServiceInternalMsg("Mmap is not supported on HDFS")
}

// ReadAt reads length bytes from filePath starting at off.
// The returned slice may be shorter than length when the range extends past EOF.
func (h *HDFSChunkManager) ReadAt(ctx context.Context, filePath string, off int64, length int64) ([]byte, error) {
	if off < 0 || length < 0 {
		return nil, io.EOF
	}
	f, err := h.client.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, merr.WrapErrIoKeyNotFound(filePath)
		}
		return nil, merr.WrapErrIoFailed(filePath, err)
	}
	defer f.Close()

	buf := make([]byte, length)
	n, err := f.ReadAt(buf, off)
	if err != nil && err != io.EOF {
		return nil, merr.WrapErrIoFailed(filePath, err)
	}
	return buf[:n], nil
}

func (h *HDFSChunkManager) Remove(ctx context.Context, filePath string) error {
	err := h.client.Remove(filePath)
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

// Copy streams srcFilePath to dstFilePath via an HDFS reader/writer pipeline
// so memory usage is O(buffer), not O(file size).
func (h *HDFSChunkManager) Copy(ctx context.Context, srcFilePath, dstFilePath string) error {
	src, err := h.client.Open(srcFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return merr.WrapErrIoKeyNotFound(srcFilePath)
		}
		return merr.WrapErrIoFailed(srcFilePath, err)
	}
	defer src.Close()

	if err := h.client.MkdirAll(path.Dir(dstFilePath), 0o755); err != nil {
		return merr.WrapErrIoFailed(dstFilePath, err)
	}

	tmp := dstFilePath + ".tmp." + tmpSuffix()
	fw, err := h.client.Create(tmp)
	if err != nil {
		return merr.WrapErrIoFailed(dstFilePath, err)
	}
	if _, err = io.Copy(fw, src); err != nil {
		fw.Close()
		_ = h.client.Remove(tmp)
		return merr.WrapErrIoFailed(dstFilePath, err)
	}
	if err = fw.Flush(); err != nil {
		fw.Close()
		_ = h.client.Remove(tmp)
		return merr.WrapErrIoFailed(dstFilePath, err)
	}
	if err = fw.Close(); err != nil {
		_ = h.client.Remove(tmp)
		return merr.WrapErrIoFailed(dstFilePath, err)
	}
	if rmErr := h.client.Remove(dstFilePath); rmErr != nil && !os.IsNotExist(rmErr) {
		_ = h.client.Remove(tmp)
		return merr.WrapErrIoFailed(dstFilePath, rmErr)
	}
	if err = h.client.Rename(tmp, dstFilePath); err != nil {
		_ = h.client.Remove(tmp)
		return merr.WrapErrIoFailed(dstFilePath, err)
	}
	return nil
}

// tmpSuffix returns a short unique suffix for temporary file names.
func tmpSuffix() string {
	return fmt.Sprintf("%016x", time.Now().UnixNano())
}

// hdfsReader wraps *hdfs.FileReader to satisfy the FileReader interface,
// which requires a Size() method that *hdfs.FileReader does not expose directly.
type hdfsReader struct {
	*hdfs.FileReader
}

func (r *hdfsReader) Size() (int64, error) {
	return r.FileReader.Stat().Size(), nil
}
