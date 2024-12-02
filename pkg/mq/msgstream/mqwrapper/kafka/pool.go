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

package kafka

import (
	"runtime"
	"sync"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/hardware"
)

var (
	kafkaCPool atomic.Pointer[conc.Pool[any]]
	initOnce   sync.Once
)

func initPool() {
	initOnce.Do(func() {
		pool := conc.NewPool[any](
			hardware.GetCPUNum(),
			conc.WithPreAlloc(false),
			conc.WithDisablePurge(false),
			conc.WithPreHandler(runtime.LockOSThread), // lock os thread for cgo thread disposal
		)

		kafkaCPool.Store(pool)
		log.Info("init dynamicPool done", zap.Int("size", hardware.GetCPUNum()))
	})
}

// GetSQPool returns the singleton pool instance for search/query operations.
func getPool() *conc.Pool[any] {
	initPool()
	return kafkaCPool.Load()
}
