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

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include <stdint.h>
#include "common/init_c.h"
#include "segcore/segcore_init_c.h"
*/
import "C"

import (
	"context"
	"math"
	"runtime"
	"sync"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var (
	// Use separate pool for search/query
	// and other operations (insert/delete/statistics/etc.)
	// since in concurrent situation, there operation may block each other in high payload

	sqp        atomic.Pointer[conc.Pool[any]]
	sqOnce     sync.Once
	dp         atomic.Pointer[conc.Pool[any]]
	dynOnce    sync.Once
	loadPool   atomic.Pointer[conc.Pool[any]]
	loadOnce   sync.Once
	warmupPool atomic.Pointer[conc.Pool[any]]
	warmupOnce sync.Once

	deletePool     atomic.Pointer[conc.Pool[struct{}]]
	deletePoolOnce sync.Once

	bfPool      atomic.Pointer[conc.Pool[any]]
	bfApplyOnce sync.Once

	// intentionally leaked CGO tag names
	cgoTagSQ      = C.CString("CGO_SQ")
	cgoTagLoad    = C.CString("CGO_LOAD")
	cgoTagDynamic = C.CString("CGO_DYN")
	cgoTagWarmup  = C.CString("CGO_WARMUP")
)

// initSQPool initialize
func initSQPool() {
	sqOnce.Do(func() {
		pt := paramtable.Get()
		initPoolSize := int(math.Ceil(pt.QueryNodeCfg.MaxReadConcurrency.GetAsFloat() * pt.QueryNodeCfg.CGOPoolSizeRatio.GetAsFloat()))
		pool := conc.NewPool[any](
			initPoolSize,
			conc.WithPreAlloc(false), // pre alloc must be false to resize pool dynamically, use warmup to alloc worker here
			conc.WithDisablePurge(true),
		)
		conc.WarmupPool(pool, func() {
			runtime.LockOSThread()
			C.SetThreadName(cgoTagSQ)
		})
		sqp.Store(pool)

		pt.Watch(pt.QueryNodeCfg.MaxReadConcurrency.Key, config.NewHandler("qn.sqpool.maxconc", ResizeSQPool))
		pt.Watch(pt.QueryNodeCfg.CGOPoolSizeRatio.Key, config.NewHandler("qn.sqpool.cgopoolratio", ResizeSQPool))
		log.Info("init SQPool done", zap.Int("size", initPoolSize))
	})
}

func initDynamicPool() {
	dynOnce.Do(func() {
		size := hardware.GetCPUNum()
		pool := conc.NewPool[any](
			size,
			conc.WithPreAlloc(false),
			conc.WithDisablePurge(false),
			conc.WithPreHandler(func() {
				runtime.LockOSThread()
				C.SetThreadName(cgoTagDynamic)
			}), // lock os thread for cgo thread disposal
		)

		dp.Store(pool)
		log.Info("init dynamicPool done", zap.Int("size", size))
	})
}

func initLoadPool() {
	loadOnce.Do(func() {
		pt := paramtable.Get()
		poolSize := hardware.GetCPUNum() * pt.CommonCfg.MiddlePriorityThreadCoreCoefficient.GetAsInt()
		pool := conc.NewPool[any](
			poolSize,
			conc.WithPreAlloc(false),
			conc.WithDisablePurge(false),
			conc.WithPreHandler(func() {
				runtime.LockOSThread()
				C.SetThreadName(cgoTagLoad)
			}), // lock os thread for cgo thread disposal
		)

		loadPool.Store(pool)

		pt.Watch(pt.CommonCfg.MiddlePriorityThreadCoreCoefficient.Key, config.NewHandler("qn.loadpool.middlepriority", ResizeLoadPool))
		log.Info("init loadPool done", zap.Int("size", poolSize))
	})
}

func initWarmupPool() {
	warmupOnce.Do(func() {
		pt := paramtable.Get()
		poolSize := hardware.GetCPUNum() * pt.CommonCfg.LowPriorityThreadCoreCoefficient.GetAsInt()
		pool := conc.NewPool[any](
			poolSize,
			conc.WithPreAlloc(false),
			conc.WithDisablePurge(false),
			conc.WithPreHandler(func() {
				runtime.LockOSThread()
				C.SetThreadName(cgoTagWarmup)
			}), // lock os thread for cgo thread disposal
			conc.WithNonBlocking(false),
		)

		warmupPool.Store(pool)
		pt.Watch(pt.CommonCfg.LowPriorityThreadCoreCoefficient.Key, config.NewHandler("qn.warmpool.lowpriority", ResizeWarmupPool))
	})
}

func initBFApplyPool() {
	bfApplyOnce.Do(func() {
		pt := paramtable.Get()
		poolSize := hardware.GetCPUNum() * pt.QueryNodeCfg.BloomFilterApplyParallelFactor.GetAsInt()
		pool := conc.NewPool[any](
			poolSize,
		)

		bfPool.Store(pool)
		pt.Watch(pt.QueryNodeCfg.BloomFilterApplyParallelFactor.Key, config.NewHandler("qn.bfapply.parallel", ResizeBFApplyPool))
	})
}

func initDeletePool() {
	deletePoolOnce.Do(func() {
		pool := conc.NewPool[struct{}](runtime.GOMAXPROCS(0))
		deletePool.Store(pool)
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

func GetLoadPool() *conc.Pool[any] {
	initLoadPool()
	return loadPool.Load()
}

func GetWarmupPool() *conc.Pool[any] {
	initWarmupPool()
	return warmupPool.Load()
}

func GetBFApplyPool() *conc.Pool[any] {
	initBFApplyPool()
	return bfPool.Load()
}

func GetDeletePool() *conc.Pool[struct{}] {
	initDeletePool()
	return deletePool.Load()
}

func ResizeSQPool(evt *config.Event) {
	if evt.HasUpdated {
		pt := paramtable.Get()
		newSize := int(math.Ceil(pt.QueryNodeCfg.MaxReadConcurrency.GetAsFloat() * pt.QueryNodeCfg.CGOPoolSizeRatio.GetAsFloat()))
		pool := GetSQPool()
		resizePool(pool, newSize, "SQPool")
		conc.WarmupPool(pool, runtime.LockOSThread)
	}
}

func ResizeLoadPool(evt *config.Event) {
	if evt.HasUpdated {
		pt := paramtable.Get()
		newSize := hardware.GetCPUNum() * pt.CommonCfg.MiddlePriorityThreadCoreCoefficient.GetAsInt()
		resizePool(GetLoadPool(), newSize, "LoadPool")
	}
}

func ResizeWarmupPool(evt *config.Event) {
	if evt.HasUpdated {
		pt := paramtable.Get()
		newSize := hardware.GetCPUNum() * pt.CommonCfg.LowPriorityThreadCoreCoefficient.GetAsInt()
		resizePool(GetWarmupPool(), newSize, "WarmupPool")
	}
}

func ResizeBFApplyPool(evt *config.Event) {
	if evt.HasUpdated {
		pt := paramtable.Get()
		newSize := hardware.GetCPUNum() * pt.QueryNodeCfg.BloomFilterApplyParallelFactor.GetAsInt()
		resizePool(GetBFApplyPool(), newSize, "BFApplyPool")
	}
}

func resizePool(pool *conc.Pool[any], newSize int, tag string) {
	log := log.Ctx(context.Background()).
		With(
			zap.String("poolTag", tag),
			zap.Int("newSize", newSize),
		)

	if newSize <= 0 {
		log.Warn("cannot set pool size to non-positive value")
		return
	}

	err := pool.Resize(newSize)
	if err != nil {
		log.Warn("failed to resize pool", zap.Error(err))
		return
	}
	log.Info("pool resize successfully")
}
