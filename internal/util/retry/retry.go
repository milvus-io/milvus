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
	"errors"
	"fmt"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/errorutil"
	"go.uber.org/zap"
)

// Do will run function with retry mechanism.
// fn is the func to run.
// Option can control the retry times and timeout.
func Do(ctx context.Context, fn func() error, opts ...Option) error {
	c := newDefaultConfig()

	for _, opt := range opts {
		opt(c)
	}

	if c.attempts < 1 {
		c.attempts = 1
	}

	if c.totalTimeOut != 0 {
		var el error
		resultChan := make(chan bool)
		go func() {
			el = innerdo(ctx, c, fn)
			resultChan <- true
		}()

		select {
		case <-resultChan:
			log.Debug("retry func success")
		case <-time.After(c.totalTimeOut):
			return fmt.Errorf("total timed out")
		}
		return el
	}
	return innerdo(ctx, c, fn)
}

func fnWithTimeout(fn func() error, d time.Duration) error {
	if d != 0 {
		resultChan := make(chan bool)
		var err1 error
		go func() {
			err1 = fn()
			resultChan <- true
		}()

		select {
		case <-resultChan:
			log.Debug("retry func success")
		case <-time.After(d):
			return fmt.Errorf("func timed out")
		}
		return err1
	}
	return fn()
}

func innerdo(ctx context.Context, c *config, fn func() error) error {
	var el errorutil.ErrorList

	for i := uint(0); i < c.attempts; i++ {
		if err := fnWithTimeout(fn, c.fnTimeOut); err != nil {
			if i%10 == 0 {
				log.Warn("retry func failed", zap.Uint("retry time", i), zap.Error(err))
			}

			el = append(el, err)

			if ok := IsUnRecoverable(err); ok {
				return el
			}

			select {
			case <-time.After(c.sleep):
			case <-ctx.Done():
				el = append(el, ctx.Err())
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

type unrecoverableError struct {
	error
}

// Unrecoverable method wrap an error to unrecoverableError. This will make retry
// quick return.
func Unrecoverable(err error) error {
	return unrecoverableError{err}
}

// IsUnRecoverable is used to judge whether the error is wrapped by unrecoverableError.
func IsUnRecoverable(err error) bool {
	_, isUnrecoverable := err.(unrecoverableError)
	return isUnrecoverable
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
