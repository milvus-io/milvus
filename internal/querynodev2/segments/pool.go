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

	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"go.uber.org/atomic"
)

var (
	p        atomic.Pointer[conc.Pool[any]]
	initOnce sync.Once
)

// InitPool initialize
func InitPool() {
	initOnce.Do(func() {
		pt := paramtable.Get()
		pool := conc.NewPool[any](
			int(math.Ceil(pt.QueryNodeCfg.MaxReadConcurrency.GetAsFloat()*pt.QueryNodeCfg.CGOPoolSizeRatio.GetAsFloat())),
			conc.WithPreAlloc(true),
			conc.WithDisablePurge(true),
		)
		conc.WarmupPool(pool, runtime.LockOSThread)

		p.Store(pool)
	})
}

// GetPool returns the singleton pool instance.
func GetPool() *conc.Pool[any] {
	InitPool()
	return p.Load()
}
