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
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// WalkWithPrefixRetry wraps WalkWithPrefix with denylist retry.
// resetFunc is called before each attempt to reset caller-side accumulators,
// preventing duplicates from partial walks on retry.
func WalkWithPrefixRetry(ctx context.Context, cm storage.ChunkManager,
	prefix string, recursive bool, retryAttempts uint,
	resetFunc func(),
	walkFunc storage.ChunkObjectWalkFunc,
) error {
	return retry.Do(ctx, func() error {
		resetFunc()
		err := cm.WalkWithPrefix(ctx, prefix, recursive, walkFunc)
		if err == nil {
			return nil
		}
		log.Ctx(ctx).Warn("WalkWithPrefix failed, checking retryability",
			zap.String("prefix", prefix),
			zap.Error(err),
		)
		// Map raw storage error to Milvus IO error for consistent classification
		err = storage.ToMilvusIoError(prefix, err)
		// Denylist: don't retry permanent/validation errors
		if merr.IsNonRetryableErr(err) {
			return retry.Unrecoverable(err)
		}
		return err
	}, retry.Attempts(retryAttempts))
}
