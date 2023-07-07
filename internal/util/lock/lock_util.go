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

package lock

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"go.uber.org/zap"
)

func TryLock(ctx context.Context, source string, mutex *sync.RWMutex,
	interval time.Duration, retryCount uint, printDuration bool) bool {
	log := logutil.Logger(ctx).With(zap.String("source", source))
	beforeTryLock := time.Now()
	err := retry.Do(ctx, func() error {
		success := mutex.TryLock()
		if !success {
			log.Debug("failed to try lock")
			return errors.New("failed to try lock, retry")
		}
		return nil
	}, retry.Attempts(retryCount), retry.Sleep(interval))
	if printDuration {
		log.Debug("try lock", zap.Duration("time cost", time.Since(beforeTryLock)),
			zap.Bool("is_success", err == nil))
	}
	return err == nil
}
