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

package querynode

import (
	"math"
	"runtime"
	"sync"

	"github.com/milvus-io/milvus/internal/util/concurrency"
	"go.uber.org/atomic"
)

var (
	// Use separate pool for search/query
	// and other operations (insert/delete/statistics/etc.)
	// since in concurrency.rrent situation, there operation may block each other in high payload

	sqp     atomic.Pointer[concurrency.Pool]
	sqOnce  sync.Once
	dp      atomic.Pointer[concurrency.Pool]
	dynOnce sync.Once
)

// initSQPool initialize
func initSQPool() {
	sqOnce.Do(func() {
		size := float64(Params.QueryNodeCfg.MaxReadConcurrency) * Params.QueryNodeCfg.CGOPoolSizeRatio
		pool, _ := concurrency.NewPool(
			int(math.Ceil(size)),
			concurrency.WithPreAlloc(true),
			concurrency.WithDisablePurge(true),
		)
		concurrency.WarmupPool(pool, runtime.LockOSThread)

		sqp.Store(pool)
	})
}

func initDynamicPool() {
	dynOnce.Do(func() {
		pool, _ := concurrency.NewPool(
			runtime.GOMAXPROCS(0),
			concurrency.WithPreAlloc(false),
			concurrency.WithDisablePurge(false),
			concurrency.WithPreHandler(runtime.LockOSThread), // lock os thread for cgo thread disposal
		)

		dp.Store(pool)
	})
}

// GetSQPool returns the singleton pool instance for search/query operations.
func GetSQPool() *concurrency.Pool {
	initSQPool()
	return sqp.Load()
}

// GetDynamicPool returns the singleton pool for dynamic cgo operations.
func GetDynamicPool() *concurrency.Pool {
	initDynamicPool()
	return dp.Load()
}
