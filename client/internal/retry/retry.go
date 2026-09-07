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

package retry

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
)

const (
	maxAttempts = 10
	initialWait = 200 * time.Millisecond
	maxWait     = 3 * time.Second
)

// Handle retries fn while it explicitly returns shouldRetry=true. It keeps the
// behavior used by the SDK's schema-cache refresh path without depending on
// the server-side retry and logging packages.
func Handle(ctx context.Context, fn func() (shouldRetry bool, err error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var lastErr error
	wait := initialWait
	for range maxAttempts {
		shouldRetry, err := fn()
		if err == nil {
			return nil
		}
		if !shouldRetry {
			if (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) && lastErr != nil {
				return lastErr
			}
			return err
		}

		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < wait {
			return err
		}
		lastErr = err

		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return lastErr
		}

		wait *= 2
		if wait > maxWait {
			wait = maxWait
		}
	}
	return lastErr
}
