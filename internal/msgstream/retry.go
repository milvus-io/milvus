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

package msgstream

import (
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

// Reference: https://blog.cyeam.com/golang/2018/08/27/retry

func Retry(attempts int, sleep time.Duration, fn func() error) error {
	if err := fn(); err != nil {
		if s, ok := err.(InterruptError); ok {
			return s.error
		}

		if attempts--; attempts > 0 {
			log.Debug("retry func error", zap.Int("attempts", attempts), zap.Duration("sleep", sleep), zap.Error(err))
			time.Sleep(sleep)
			return Retry(attempts, 2*sleep, fn)
		}
		return err
	}
	return nil
}

type InterruptError struct {
	error
}

func NoRetryError(err error) InterruptError {
	return InterruptError{err}
}
