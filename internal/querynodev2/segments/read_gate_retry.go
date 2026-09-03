// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package segments

import (
	"context"
	"math/rand/v2"
	"time"

	segcoreutil "github.com/milvus-io/milvus/internal/util/segcore"
)

const (
	segmentReadGateRetryInitialBackoff = 5 * time.Millisecond
	segmentReadGateRetryMaxBackoff     = 100 * time.Millisecond
)

type segmentReadGateRetryWait func(context.Context, int) error

func waitSegmentReadGateRetry(ctx context.Context, retryCount int) error {
	shift := min(max(retryCount-1, 0), 5)
	backoff := segmentReadGateRetryInitialBackoff << shift
	backoff = min(backoff, segmentReadGateRetryMaxBackoff)
	half := backoff / 2
	delay := half + time.Duration(rand.Int64N(int64(half)+1))

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func retrySegmentReadGate[T any](
	ctx context.Context,
	segmentType SegmentType,
	operation func() (T, error),
	waitRetry segmentReadGateRetryWait,
) (T, error) {
	retryCount := 0
	for {
		result, err := operation()
		if err == nil || segmentType != SegmentTypeSealed ||
			!segcoreutil.IsSegmentReadGateBusy(err) {
			return result, err
		}

		retryCount++
		if err := waitRetry(ctx, retryCount); err != nil {
			var zero T
			return zero, err
		}
	}
}
