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

package nodescheduler

import (
	"math"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type taskOptions struct {
	backoff typeutil.BackoffConfig
}

// Option configures one submitted task. Options may be reused across submissions.
type Option func(*taskOptions)

// WithBackoff configures ErrDelay retries without affecting explicit DelayUntil
// deadlines. InitialInterval must be positive, MaxInterval must be at least
// InitialInterval, and Multiplier must be finite and at least one.
// Invalid parameters panic, as with an invalid scheduler concurrency.
func WithBackoff(config typeutil.BackoffConfig) Option {
	if config.InitialInterval <= 0 || config.MaxInterval < config.InitialInterval ||
		config.Multiplier < 1 || math.IsNaN(config.Multiplier) || math.IsInf(config.Multiplier, 0) {
		panic("invalid node scheduler backoff configuration")
	}
	return func(options *taskOptions) { options.backoff = config }
}

func newTaskBackoff(opts ...Option) *backoff.ExponentialBackOff {
	options := taskOptions{backoff: typeutil.BackoffConfig{
		InitialInterval: 10 * time.Millisecond,
		Multiplier:      2,
		MaxInterval:     5 * time.Second,
	}}
	for _, opt := range opts {
		opt(&options)
	}
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = options.backoff.InitialInterval
	b.Multiplier = options.backoff.Multiplier
	b.MaxInterval = options.backoff.MaxInterval
	b.RandomizationFactor = 0
	b.MaxElapsedTime = 0 // Retry until completion or cancellation, without a time limit.
	b.Reset()
	return b
}
