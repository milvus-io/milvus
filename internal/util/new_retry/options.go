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

import "time"

type Config struct {
	attempts     uint
	sleep        time.Duration
	maxSleepTime time.Duration
}

func NewDefaultConfig() *Config {
	return &Config{
		attempts:     uint(10),
		sleep:        200 * time.Millisecond,
		maxSleepTime: 1 * time.Second,
	}
}

type Option func(*Config)

func Attempts(attempts uint) Option {
	return func(c *Config) {
		c.attempts = attempts
	}
}

func Sleep(sleep time.Duration) Option {
	return func(c *Config) {
		c.sleep = sleep
	}
}

func MaxSleepTime(maxSleepTime time.Duration) Option {
	return func(c *Config) {
		c.maxSleepTime = maxSleepTime
	}
}
