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

package segments

import (
	"math"
	"runtime"
	"sync"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var (
	// Use separate pool for search/query
	// and other operations (insert/delete/statistics/etc.)
	// since in concurrent situation, there operation may block each other in high payload

	sqp     atomic.Pointer[conc.Pool[any]]
	sqOnce  sync.Once
	dp      atomic.Pointer[conc.Pool[any]]
	dynOnce sync.Once
)

// initSQPool initialize
func initSQPool() {
	sqOnce.Do(func() {
		pt := paramtable.Get()
		pool := conc.NewPool[any](
			int(math.Ceil(pt.QueryNodeCfg.MaxReadConcurrency.GetAsFloat()*pt.QueryNodeCfg.CGOPoolSizeRatio.GetAsFloat())),
			conc.WithPreAlloc(true),
			conc.WithDisablePurge(true),
		)
		conc.WarmupPool(pool, runtime.LockOSThread)

		sqp.Store(pool)
	})
}

func initDynamicPool() {
	dynOnce.Do(func() {
		pool := conc.NewPool[any](
			hardware.GetCPUNum(),
			conc.WithPreAlloc(false),
			conc.WithDisablePurge(false),
			conc.WithPreHandler(runtime.LockOSThread), // lock os thread for cgo thread disposal
		)

		dp.Store(pool)
	})
}

// GetSQPool returns the singleton pool instance for search/query operations.
func GetSQPool() *conc.Pool[any] {
	initSQPool()
	return sqp.Load()
}

// GetDynamicPool returns the singleton pool for dynamic cgo operations.
func GetDynamicPool() *conc.Pool[any] {
	initDynamicPool()
	return dp.Load()
}
