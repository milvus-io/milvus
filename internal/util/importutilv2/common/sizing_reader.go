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

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// SizingReaderAt is a minimal io.Reader / io.ReaderAt / io.Seeker over a
// ChunkManager, used only to read the small header/footer a sizing pass needs.
// Each ReadAt is an independent ranged GET wrapped in retry, so a brief
// object-store fault is absorbed at the fetch layer. retry.Handle gives up after a
// bounded number of attempts though, so a persistent outage does surface to the
// decoder as a typed IO error — see WrapDecodeErr, which is why a decoder failure
// may not be assumed to be a file-format problem. Unlike RetryableReader it retries the
// ReadAt/Seek path the parquet footer relies on, and it holds no open handle
// (nothing to close, no reopen-at-offset logic).
type SizingReaderAt struct {
	ctx  context.Context
	cm   storage.ChunkManager
	path string
	size int64
	off  int64
}

func NewSizingReaderAt(ctx context.Context, cm storage.ChunkManager, path string) (*SizingReaderAt, error) {
	size, err := cm.Size(ctx, path) // Size retries internally.
	if err != nil {
		return nil, storage.ToMilvusIoError(path, err)
	}
	return &SizingReaderAt{ctx: ctx, cm: cm, path: path, size: size}, nil
}

// FileSize returns the object size observed when the reader was created, so a
// caller inspecting a trailer does not pay a second Size call for it.
func (r *SizingReaderAt) FileSize() int64 {
	return r.size
}

func (r *SizingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, merr.WrapErrParameterInvalidMsg("negative read offset %d", off)
	}
	if len(p) == 0 || off >= r.size {
		if off >= r.size {
			return 0, io.EOF
		}
		return 0, nil
	}
	var data []byte
	if err := retry.Handle(r.ctx, func() (bool, error) {
		b, e := r.cm.ReadAt(r.ctx, r.path, off, int64(len(p)))
		if e != nil {
			ioErr := storage.ToMilvusIoError(r.path, e)
			return !merr.IsNonRetryableErr(ioErr), ioErr
		}
		data = b
		return false, nil
	}); err != nil {
		return 0, err
	}
	n := copy(p, data)
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r *SizingReaderAt) Read(p []byte) (int, error) {
	n, err := r.ReadAt(p, r.off)
	r.off += int64(n)
	return n, err
}

func (r *SizingReaderAt) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.off + offset
	case io.SeekEnd:
		abs = r.size + offset
	default:
		return 0, merr.WrapErrParameterInvalidMsg("invalid seek whence %d", whence)
	}
	if abs < 0 {
		return 0, merr.WrapErrParameterInvalidMsg("negative seek position %d", abs)
	}
	r.off = abs
	return abs, nil
}
