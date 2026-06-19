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

package ratelimit

import "time"

var (
	defaultRecoveryConfig = RecoveryConfig{
		IncreaseInterval:    5 * time.Second,
		Incremental:         1 * 1024 * 1024,
		HWM:                 64 * 1024 * 1024,
		LWM:                 4 * 1024 * 1024,
		NormalDelayInterval: 60 * time.Second,
	}
	defaultSlowdownConfig = SlowdownConfig{
		HWM:                 32 * 1024 * 1024,
		LWM:                 2 * 1024 * 1024,
		DecreaseInterval:    10 * time.Second,
		DecreaseRatio:       0.5,
		RejectDelayInterval: 0 * time.Second,
	}
)

type SlowdownConfig struct {
	HWM                 int64
	LWM                 int64
	DecreaseInterval    time.Duration
	DecreaseRatio       float64
	RejectDelayInterval time.Duration
	FirstSlowdownDelay  time.Duration
}

type RecoveryConfig struct {
	HWM                 int64
	LWM                 int64
	IncreaseInterval    time.Duration
	Incremental         int64
	NormalDelayInterval time.Duration
}

type AdaptiveRateLimitControllerConfigFetcher interface {
	FetchRecoveryConfig() RecoveryConfig
	FetchSlowdownConfig() SlowdownConfig
	Close()
}

type DefaultAdaptiveRateLimitControllerConfigFetcher struct{}

func (f DefaultAdaptiveRateLimitControllerConfigFetcher) FetchRecoveryConfig() RecoveryConfig {
	return defaultRecoveryConfig
}

func (f DefaultAdaptiveRateLimitControllerConfigFetcher) FetchSlowdownConfig() SlowdownConfig {
	return defaultSlowdownConfig
}

func (f DefaultAdaptiveRateLimitControllerConfigFetcher) Close() {}
