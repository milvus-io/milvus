// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package retry

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/errutil"
	"go.uber.org/zap"
)

// Do will run function with retry mechanism.
// fn is the func to run.
// Option can control the retry times and timeout.
func Do(ctx context.Context, fn func() error, opts ...Option) error {
	log := log.Ctx(ctx)

	c := newDefaultConfig()

	for _, opt := range opts {
		opt(c)
	}

	var el error

	if c.attempts < 1 {
		c.attempts = 1
	}

	for i := uint(0); i < c.attempts; i++ {
		if err := fn(); err != nil {
			if i%10 == 0 {
				log.Error("retry func failed", zap.Uint("retry time", i), zap.Error(err))
			}

			err = errors.Wrapf(err, "attempt #%d", i)
			el = errutil.Combine(el, err)

			if !IsRecoverable(err) {
				return el
			}

			select {
			case <-time.After(c.sleep):
			case <-ctx.Done():
				el = errutil.Combine(el, errors.Wrapf(ctx.Err(), "context done during sleep after run#%d", i))
				return el
			}

			c.sleep *= 2
			if c.sleep > c.maxSleepTime {
				c.sleep = c.maxSleepTime
			}
		} else {
			return nil
		}
	}
	return el
}

// errUnrecoverable is error instance for unrecoverable.
var errUnrecoverable = errors.New("unrecoverable error")

// Unrecoverable method wrap an error to unrecoverableError. This will make retry
// quick return.
func Unrecoverable(err error) error {
	return errutil.Combine(err, errUnrecoverable)
}

// IsRecoverable is used to judge whether the error is wrapped by unrecoverableError.
func IsRecoverable(err error) bool {
	return !errors.Is(err, errUnrecoverable)
}

func DoGrpc(ctx context.Context, times uint, rpcFunc func() (any, error)) (any, error) {
	innerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		result     any
		innerError error
	)

	err := Do(innerCtx, func() error {
		result, innerError = rpcFunc()
		if innerError != nil {
			result = nil
			cancel()
			return innerError
		}

		var errorCode commonpb.ErrorCode
		var reason string
		switch res := result.(type) {
		case *commonpb.Status:
			errorCode = res.GetErrorCode()
			reason = res.GetReason()
		case interface{ GetStatus() *commonpb.Status }:
			errorCode = res.GetStatus().GetErrorCode()
			reason = res.GetStatus().GetReason()
		default:
			cancel()
			return innerError
		}

		if errorCode == commonpb.ErrorCode_Success {
			return nil
		}
		innerError = errors.New(reason)
		if errorCode != commonpb.ErrorCode_NotReadyServe {
			cancel()
		}
		return innerError
	}, Attempts(times), Sleep(50*time.Millisecond), MaxSleepTime(time.Second))
	if result != nil {
		return result, nil
	}
	return result, err
}
