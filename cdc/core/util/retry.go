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

package util

import (
	"context"
	"time"

	"go.uber.org/zap"
)

type config struct {
	attempts     uint
	sleep        time.Duration
	maxSleepTime time.Duration
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

// Do will run function with retry mechanism.
// fn is the func to run.
// Option can control the retry times and timeout.
func Do(ctx context.Context, fn func() error, opts ...Option) error {

	c := newDefaultConfig()

	for _, opt := range opts {
		opt(c)
	}
	var el ErrorList

	for i := uint(0); i < c.attempts; i++ {
		if err := fn(); err != nil {
			if i%10 == 0 {
				Log.Debug("retry func failed", zap.Uint("retry time", i), zap.Error(err))
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
