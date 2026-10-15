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

// Package pkindexstore moves primary key index tables between the local disk
// of a streaming node and object storage.
package pkindexstore

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"

	"github.com/milvus-io/milvus/internal/pkindex/index"
	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// The error codes of a missing object that a remote reader returns from its
// first Read. The MinIO and Azure readers send the request lazily and do not
// map these errors to merr.
const (
	minioNoSuchKey    = "NoSuchKey"
	azureBlobNotFound = "BlobNotFound"
)

var _ index.TableStore = (*Store)(nil)

// Store implements index.TableStore on a storage.ChunkManager.
type Store struct {
	cm storage.ChunkManager
}

// New returns a Store that uses cm.
func New(cm storage.ChunkManager) *Store {
	return &Store{cm: cm}
}

// UploadFile writes the local file at path to key. On a remote chunk manager
// it streams the file, so the file is not held in memory.
//
// A failure of the local file returns an error marked pkindex.ErrIO. A failure
// of a remote chunk manager or the object storage returns an error marked
// pkindex.ErrUnavailable, so the caller can retry it. A failure of a local chunk
// manager is a local disk failure and returns an error marked pkindex.ErrIO.
func (s *Store) UploadFile(ctx context.Context, key, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return pkindex.MarkIO(err, "open %s", path)
	}
	defer f.Close()

	if remote, ok := s.cm.(*storage.RemoteChunkManager); ok {
		st, err := f.Stat()
		if err != nil {
			return pkindex.MarkIO(err, "stat %s", path)
		}
		// The object storage reads the body from a local file. A read error
		// of that file is wrapped in the PutObject error, so it is marked as
		// pkindex.ErrUnavailable too. A retry reads the file again.
		if err := remote.UnderlyingObjectStorage().PutObject(ctx, remote.BucketName(), key, f, st.Size()); err != nil {
			return markUnavailable(err, "upload %s", key)
		}
		return nil
	}

	data, err := io.ReadAll(f)
	if err != nil {
		return pkindex.MarkIO(err, "read %s", path)
	}
	if err := s.cm.Write(ctx, key, data); err != nil {
		if _, local := s.cm.(*storage.LocalChunkManager); local {
			return pkindex.MarkIO(err, "write %s", key)
		}
		return markUnavailable(err, "write %s", key)
	}
	return nil
}

// DownloadFile writes the object at key to the local file at path through a
// temporary file and a rename. It syncs the file before the rename and the
// directory after the rename, so the file survives a crash.
//
// A missing object returns an error marked pkindex.ErrCorrupted. Any other
// failure of the chunk manager or the object storage returns an error marked
// pkindex.ErrUnavailable. A failure of the local disk returns an error marked
// pkindex.ErrIO.
func (s *Store) DownloadFile(ctx context.Context, key, path string) error {
	r, err := s.cm.Reader(ctx, key)
	if err != nil {
		return markRead(err, "open %s", key)
	}
	defer r.Close()

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".download-*")
	if err != nil {
		return pkindex.MarkIO(err, "create temp file for %s", path)
	}
	defer os.Remove(tmp.Name()) // no effect after the rename

	w := &recordingWriter{w: tmp}
	if _, err := io.Copy(w, r); err != nil {
		tmp.Close()
		if w.err != nil {
			return pkindex.MarkIO(err, "write %s", tmp.Name())
		}
		return markRead(err, "download %s", key)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return pkindex.MarkIO(err, "sync %s", tmp.Name())
	}
	if err := tmp.Close(); err != nil {
		return pkindex.MarkIO(err, "close %s", tmp.Name())
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return pkindex.MarkIO(err, "rename to %s", path)
	}
	return syncDir(dir)
}

// recordingWriter records the error of w, so that the caller of io.Copy can
// tell a write error from a read error.
type recordingWriter struct {
	w   io.Writer
	err error
}

func (rw *recordingWriter) Write(p []byte) (int, error) {
	n, err := rw.w.Write(p)
	if err != nil {
		rw.err = err
	}
	return n, err
}

// syncDir syncs the directory at dir, so a rename in it survives a crash.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return pkindex.MarkIO(err, "open dir %s", dir)
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return pkindex.MarkIO(err, "sync dir %s", dir)
	}
	if err := d.Close(); err != nil {
		return pkindex.MarkIO(err, "close dir %s", dir)
	}
	return nil
}

// markRead marks a missing object as pkindex.ErrCorrupted and any other read
// failure of the chunk manager as pkindex.ErrUnavailable.
func markRead(err error, format string, args ...any) error {
	if isNotFound(err) {
		return errors.Mark(errors.Wrapf(err, format, args...), pkindex.ErrCorrupted)
	}
	return markUnavailable(err, format, args...)
}

// markUnavailable adds context to a failure of the chunk manager or the object
// storage and marks it as pkindex.ErrUnavailable.
func markUnavailable(err error, format string, args ...any) error {
	return errors.Mark(errors.Wrapf(err, format, args...), pkindex.ErrUnavailable)
}

// isNotFound reports whether err says that the object does not exist. The
// local and GCS readers return merr.ErrIoKeyNotFound from Reader. The MinIO
// and Azure readers return their raw error from the first Read.
func isNotFound(err error) bool {
	if errors.Is(err, merr.ErrIoKeyNotFound) {
		return true
	}
	var minioErr minio.ErrorResponse
	if errors.As(err, &minioErr) && minioErr.Code == minioNoSuchKey {
		return true
	}
	var azureErr *azcore.ResponseError
	if errors.As(err, &azureErr) && azureErr.ErrorCode == azureBlobNotFound {
		return true
	}
	return false
}
