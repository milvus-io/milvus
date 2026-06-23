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

package common

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// RetryableReader is a wrapper around a FileReader that retries reads on errors.
type RetryableReader interface {
	storage.FileReader
	// Read is explicitly declared to help type checkers resolve the method
	Read(p []byte) (n int, err error)
}

type (
	ReopenReaderFunc func(context.Context, string, int64) (storage.FileReader, error)
	ReaderSizeFunc   func(context.Context, string) (int64, error)
)

type offsetReader interface {
	ReaderAtOffset(context.Context, string, int64) (storage.FileReader, error)
}

// NewChunkManagerReopenReaderFunc creates a reopen function that resumes reading at the given offset.
func NewChunkManagerReopenReaderFunc(cm storage.ChunkManager) ReopenReaderFunc {
	return func(ctx context.Context, path string, offset int64) (storage.FileReader, error) {
		if reader, ok := cm.(offsetReader); ok {
			return reader.ReaderAtOffset(ctx, path, offset)
		}
		reader, err := cm.Reader(ctx, path)
		if err != nil {
			return nil, err
		}
		if offset > 0 {
			if _, err = reader.Seek(offset, io.SeekStart); err != nil {
				_ = reader.Close()
				return nil, err
			}
		}
		return reader, nil
	}
}

// retryableReader is the implementation of RetryableReader.
type retryableReader struct {
	storage.FileReader
	ctx           context.Context
	path          string
	retryAttempts uint
	reopen        ReopenReaderFunc
	sizeFunc      ReaderSizeFunc
	offset        int64
	size          int64
}

// NewRetryableReader creates a new RetryableReader.
func NewRetryableReader(ctx context.Context, path string, reader storage.FileReader) RetryableReader {
	return newRetryableReader(ctx, path, reader, nil, nil)
}

func NewRetryableReaderWithReopen(ctx context.Context, path string, reader storage.FileReader, reopen ReopenReaderFunc, sizeFunc ReaderSizeFunc) RetryableReader {
	return newRetryableReader(ctx, path, reader, reopen, sizeFunc)
}

func newRetryableReader(ctx context.Context, path string, reader storage.FileReader, reopen ReopenReaderFunc, sizeFunc ReaderSizeFunc) RetryableReader {
	size := int64(-1)
	if reader != nil {
		var err error
		size, err = reader.Size()
		if err != nil {
			size = -1
		}
	}
	return &retryableReader{
		FileReader:    reader,
		ctx:           ctx,
		path:          path,
		retryAttempts: paramtable.Get().CommonCfg.StorageReadRetryAttempts.GetAsUint(),
		reopen:        reopen,
		sizeFunc:      sizeFunc,
		size:          size,
	}
}

func (r *retryableReader) objectSize() (int64, bool) {
	if r.size > 0 {
		return r.size, true
	}
	if r.sizeFunc == nil {
		return r.size, r.size >= 0
	}
	size, err := r.sizeFunc(r.ctx, r.path)
	if err != nil {
		log.Ctx(r.ctx).Warn("retryable reader failed to get object size",
			zap.String("path", r.path),
			zap.Error(err),
		)
		return 0, false
	}
	r.size = size
	r.sizeFunc = nil
	return size, true
}

func (r *retryableReader) reopenAtOffset() error {
	if r.reopen == nil {
		return nil
	}
	if r.FileReader != nil {
		_ = r.Close()
		r.FileReader = nil
	}
	reader, err := r.reopen(r.ctx, r.path, r.offset)
	if err != nil {
		return storage.ToMilvusIoError(r.path, err)
	}
	r.FileReader = reader
	return nil
}

// Read reads from the underlying FileReader and retries on errors.
func (r *retryableReader) Read(p []byte) (int, error) {
	var n int
	var err error
	err = retry.Handle(r.ctx, func() (bool, error) {
		if r.FileReader == nil {
			if reopenErr := r.reopenAtOffset(); reopenErr != nil {
				return !merr.IsNonRetryableErr(reopenErr), reopenErr
			}
			if r.FileReader == nil {
				err = storage.ToMilvusIoError(r.path, io.ErrClosedPipe)
				return false, err
			}
		}
		n, err = r.FileReader.Read(p)
		if n > 0 {
			r.offset += int64(n)
			// Preserve io.Reader semantics: return buffered bytes first.
			// Any accompanying error is handled by a following Read, which may reopen.
			return false, nil
		}
		if err == nil {
			return false, nil
		}
		if errors.Is(err, io.EOF) {
			if size, ok := r.objectSize(); ok && r.offset < size && r.reopen != nil {
				err = storage.ToMilvusIoError(r.path, io.ErrUnexpectedEOF)
				log.Ctx(r.ctx).Warn("retryable reader got premature EOF",
					zap.String("path", r.path),
					zap.Int64("offset", r.offset),
					zap.Int64("size", size),
				)
				if reopenErr := r.reopenAtOffset(); reopenErr != nil {
					return !merr.IsNonRetryableErr(reopenErr), reopenErr
				}
				return true, err
			}
			return false, err
		}
		// Context canceled or deadline exceeded - don't retry
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return false, err
		}
		log.Ctx(r.ctx).Warn("retryable reader read failed",
			zap.String("path", r.path),
			zap.Error(err),
		)
		err = storage.ToMilvusIoError(r.path, err)
		// Denylist check - don't retry permanent/validation errors
		if merr.IsNonRetryableErr(err) {
			return false, err
		}
		if reopenErr := r.reopenAtOffset(); reopenErr != nil {
			return !merr.IsNonRetryableErr(reopenErr), reopenErr
		}
		// Retry everything else (network errors, timeouts, 500s, etc.)
		return true, err
	}, retry.Attempts(r.retryAttempts))
	return n, err
}
