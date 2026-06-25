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
// gap: it retries every transient error and bails out only on permanent/validation
// ones (permission denied, bucket/key not found, invalid argument, ...).
func GetFilesSizeWithRetry(ctx context.Context, cm storage.ChunkManager, paths []string) (int64, error) {
	retryAttempts := paramtable.Get().CommonCfg.StorageReadRetryAttempts.GetAsUint()
	return getFilesSizeWithRetry(ctx, cm, paths, retryAttempts)
}

// getFilesSizeWithRetry is the testable core: retry is per-file, so a transient failure
// on one file does not re-stat the files already done.
func getFilesSizeWithRetry(ctx context.Context, cm storage.ChunkManager,
	paths []string, retryAttempts uint,
) (int64, error) {
	var totalSize int64
	for _, filePath := range paths {
		var size int64
		err := retry.Do(ctx, func() error {
			s, e := cm.Size(ctx, filePath)
			if e == nil {
				size = s
				return nil
			}
			log.Ctx(ctx).Warn("get file size failed, checking retryability",
				zap.String("path", filePath),
				zap.Error(e),
			)
			// Map raw storage error to Milvus IO error for consistent classification
			e = storage.ToMilvusIoError(filePath, e)
			// Denylist: don't retry permanent/validation errors
			if merr.IsNonRetryableErr(e) {
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
