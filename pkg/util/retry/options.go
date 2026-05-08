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
)

// maxAttemptsKey is the context key for storing max retry attempts.
type maxAttemptsKey struct{}

// WithMaxAttemptsContext stores the max retry attempts in the context.
func WithMaxAttemptsContext(ctx context.Context, attempts uint) context.Context {
	return context.WithValue(ctx, maxAttemptsKey{}, attempts)
}

// MaxAttemptsFromContext reads the max retry attempts from the context.
// Returns (0, false) if not set.
func MaxAttemptsFromContext(ctx context.Context) (uint, bool) {
	raw := ctx.Value(maxAttemptsKey{})
	if raw == nil {
		return 0, false
	}
	return raw.(uint), true
}

// MaxAttemptsFromContextOrDefault reads the max retry attempts from the context,
// returning defaultVal if no value is set.
func MaxAttemptsFromContextOrDefault(ctx context.Context, defaultVal uint) uint {
	if v, ok := MaxAttemptsFromContext(ctx); ok {
		return v
	}
	return defaultVal
}

type config struct {
	attempts     uint
	sleep        time.Duration
	maxSleepTime time.Duration
	isRetryErr   func(err error) bool
}

func newDefaultConfig() *config {
	return &config{
		attempts:     uint(10),
		sleep:        200 * time.Millisecond,
		maxSleepTime: 3 * time.Second,
	}
}

// Option is used to config the retry function.
type Option func(*config)

func AttemptAlways() Option {
	return func(c *config) {
		c.attempts = 0
	}
}

// Attempts is used to config the max retry times.
func Attempts(attempts uint) Option {
	return func(c *config) {
		c.attempts = attempts
	}
}

// Sleep is used to config the initial interval time of each execution.
func Sleep(sleep time.Duration) Option {
	return func(c *config) {
		c.sleep = sleep
		// ensure max retry interval is always larger than retry interval
		if c.sleep*2 > c.maxSleepTime {
			c.maxSleepTime = 2 * c.sleep
		}
	}
}

// MaxSleepTime is used to config the max interval time of each execution.
func MaxSleepTime(maxSleepTime time.Duration) Option {
	return func(c *config) {
		// ensure max retry interval is always larger than retry interval
		if c.sleep*2 > maxSleepTime {
			c.maxSleepTime = 2 * c.sleep
		} else {
			c.maxSleepTime = maxSleepTime
		}
	}
}

func RetryErr(isRetryErr func(err error) bool) Option {
	return func(c *config) {
		c.isRetryErr = isRetryErr
	}
}
