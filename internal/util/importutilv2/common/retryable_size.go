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

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
)

// GetFilesSizeWithRetry sums the size of each file with denylist retry, mirroring
// WalkWithPrefixRetry and the binlog reader's multiReadWithRetry. It is used by every
// import reader (binlog / numpy / parquet / csv / json) to estimate total file size
// during pre-import.
//
// Each cm.Size is a per-file StatObject (an object-store HEAD). RemoteChunkManager.Size()
// retries internally only on an allowlist (merr.IsRetryableErr), which excludes
// ErrIoFailed; a transient transport error such as "net/http: timeout awaiting
// response headers" maps to ErrIoFailed and therefore fails the whole pre-import on
// the first attempt while the read/list/walk paths retry it. This wrapper closes that
// gap.
func GetFilesSizeWithRetry(ctx context.Context, cm storage.ChunkManager, paths []string) (int64, error) {
	retryAttempts := paramtable.Get().CommonCfg.StorageReadRetryAttempts.GetAsUint()
	return getFilesSizeWithRetry(ctx, cm, paths, retryAttempts)
}

// getFilesSizeWithRetry is the testable core: retry is per-file, so a transient failure
// on one file does not re-stat the files already done.
func getFilesSizeWithRetry(ctx context.Context, cm storage.ChunkManager,
	paths []string, retryAttempts uint,
) (int64, error) {
	// Defensive: retry.Attempts(0) means unbounded retry. The exported entrypoint reads
	// the StorageReadRetryAttempts config, whose formatter already clamps <1 to the
	// default, but guard direct callers too so a 0 can never spin forever here.
	if retryAttempts < 1 {
		retryAttempts = 10
	}
	var totalSize int64
	for _, filePath := range paths {
		var size int64
		err := retry.Do(ctx, func() error {
			s, e := cm.Size(ctx, filePath)
			if e == nil {
				size = s
				return nil
			}
			// Defensive fast-fail on a bare context error returned directly by the
			// ChunkManager (e.g. a mock, or a future non-Remote impl). On the
			// RemoteChunkManager path a mid-flight cancellation is mapped to ErrIoFailed
			// (mapObjectStorageError has no context case) and loses its context identity,
			// so this branch does not fire there -- retry.Do's own ctx.Done handling
			// terminates that case instead.
			if errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded) {
				return retry.Unrecoverable(e)
			}
			log.Ctx(ctx).Warn("get file size failed, checking retryability",
				zap.String("path", filePath),
				zap.Error(e),
			)
			// Map raw storage error to Milvus IO error for consistent classification.
			e = storage.ToMilvusIoError(filePath, e)
			// RemoteChunkManager.Size() already retries retryable codes
			// (ErrIoUnexpectEOF / ErrIoTooManyRequests) internally on its own allowlist.
			// This outer layer only fills the gap that inner retry leaves: a transient
			// error it does not cover -- ErrIoFailed, e.g. "net/http: timeout awaiting
			// response headers" -- that is not a permanent/validation error. Retrying
			// inner-retryable or permanent codes here would stack the two layers (up to
			// retryAttempts^2 HEAD calls under a SlowDown storm), so bail out on both.
			//
			// Note ErrIoFailed is a catch-all: known-permanent errors are classified to
			// denylist codes (KeyNotFound / PermissionDenied / BucketNotFound / ...) and
			// fail fast on both the Remote and the Local paths, so only genuinely-unknown
			// errors are retried here -- the same trade-off the existing read/walk denylist
			// retries make.
			if merr.IsNonRetryableErr(e) || merr.IsRetryableErr(e) {
				return retry.Unrecoverable(e)
			}
			return e
		}, retry.Attempts(retryAttempts))
		if err != nil {
			return 0, err
		}
		totalSize += size
	}
	return totalSize, nil
}
