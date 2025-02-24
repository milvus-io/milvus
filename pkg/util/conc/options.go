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

package conc

import (
	"time"

	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

type poolOption struct {
	// pre-allocs workers
	preAlloc bool
	// block or not when pool is full
	nonBlocking bool
	// duration to cleanup worker goroutine
	expiryDuration time.Duration
	// disable purge worker
	disablePurge bool
	// whether conceal panic when job has panic
	concealPanic bool
	// panicHandler when task panics
	panicHandler func(any)

	// preHandler function executed before actual method executed
	preHandler func()
}

func (opt *poolOption) antsOptions() []ants.Option {
	var result []ants.Option
	result = append(result, ants.WithPreAlloc(opt.preAlloc))
	result = append(result, ants.WithNonblocking(opt.nonBlocking))
	result = append(result, ants.WithDisablePurge(opt.disablePurge))
	// ants recovers panic by default
	// however the error is not returned
	result = append(result, ants.WithPanicHandler(func(v any) {
		log.Error("Conc pool panicked", zap.Any("panic", v))
		if !opt.concealPanic {
			panic(v)
		}
	}))
	if opt.panicHandler != nil {
		result = append(result, ants.WithPanicHandler(opt.panicHandler))
	}
	if opt.expiryDuration > 0 {
		result = append(result, ants.WithExpiryDuration(opt.expiryDuration))
	}

	return result
}

// PoolOption options function to setup pool.
type PoolOption func(opt *poolOption)

func defaultPoolOption() *poolOption {
	return &poolOption{
		preAlloc:       false,
		nonBlocking:    false,
		expiryDuration: 0,
		disablePurge:   false,
		concealPanic:   false,
	}
}

func WithPreAlloc(v bool) PoolOption {
	return func(opt *poolOption) {
		opt.preAlloc = v
	}
}

func WithNonBlocking(v bool) PoolOption {
	return func(opt *poolOption) {
		opt.nonBlocking = v
	}
}

func WithDisablePurge(v bool) PoolOption {
	return func(opt *poolOption) {
		opt.disablePurge = v
	}
}

func WithExpiryDuration(d time.Duration) PoolOption {
	return func(opt *poolOption) {
		opt.expiryDuration = d
	}
}

func WithConcealPanic(v bool) PoolOption {
	return func(opt *poolOption) {
		opt.concealPanic = v
	}
}

func WithPreHandler(fn func()) PoolOption {
	return func(opt *poolOption) {
		opt.preHandler = fn
	}
}
