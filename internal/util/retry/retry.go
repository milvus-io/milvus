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
	"fmt"
	"strings"
	"time"
)

func Do(ctx context.Context, fn func() error, opts ...Option) error {

	c := newDefaultConfig()

	for _, opt := range opts {
		opt(c)
	}
	el := make(ErrorList, 0)

	for i := uint(0); i < c.attempts; i++ {
		if err := fn(); err != nil {
			if ok := IsUncoverable(err); ok {
				return err
			}
			el = append(el, err)

			select {
			case <-time.After(c.sleep):
			case <-ctx.Done():
				return ctx.Err()
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

// ErrorList for print error log
type ErrorList []error

// TODO shouldn't print all retries, might be too much
// Error method return an string representation of retry error list.
func (el ErrorList) Error() string {
	var builder strings.Builder
	builder.WriteString("All attempts results:\n")
	for index, err := range el {
		// if early termination happens
		if err == nil {
			break
		}
		builder.WriteString(fmt.Sprintf("attempt #%d:%s\n", index+1, err.Error()))
	}
	return builder.String()
}

type unrecoverableError struct {
	error
}

// Unrecoverable method wrap an error to unrecoverableError. This will make retry
// quick return.
func Unrecoverable(err error) error {
	return unrecoverableError{err}
}

func IsUncoverable(err error) bool {
	_, isUnrecoverable := err.(unrecoverableError)
	return isUnrecoverable
}
