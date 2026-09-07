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
	"github.com/milvus-io/milvus/pkg/v2/metrics"
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

	// mutatePool serves the online write CGO path (segment Insert/Delete).
	// It is isolated from the load/management work on DynamicPool so that a
	// burst of segment loading (e.g. delta-log replay after compaction) cannot
	// starve online insert/delete, which would otherwise stall tSafe advancement.
	mutatePool     atomic.Pointer[conc.Pool[any]]
	mutatePoolOnce sync.Once

	deletePool     atomic.Pointer[conc.Pool[struct{}]]
	deletePoolOnce sync.Once

	bfPool      atomic.Pointer[conc.Pool[any]]
	bfApplyOnce sync.Once

	bm25LoadPool atomic.Pointer[conc.Pool[any]]
	bm25PoolOnce sync.Once

	// intentionally leaked CGO tag names
	cgoTagSQ      = C.CString("CGO_SQ")
	cgoTagLoad    = C.CString("CGO_LOAD")
	cgoTagDynamic = C.CString("CGO_DYN")
	cgoTagMutate  = C.CString("CGO_MUTATE")
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

func dynamicPoolSize() int {
	size := hardware.GetCPUNum() * paramtable.Get().QueryNodeCfg.DynamicPoolSizeFactor.GetAsInt()
	if size < 1 {
		size = hardware.GetCPUNum()
	}
	return size
}

func initDynamicPool() {
	dynOnce.Do(func() {
		pt := paramtable.Get()
		size := dynamicPoolSize()
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
		pt.Watch(pt.QueryNodeCfg.DynamicPoolSizeFactor.Key, config.NewHandler("qn.dynamicpool.sizefactor", ResizeDynamicPool))
		log.Info("init dynamicPool done", zap.Int("size", size))
	})
}

func mutatePoolSize() int {
	size := hardware.GetCPUNum() * paramtable.Get().QueryNodeCfg.MutatePoolSizeFactor.GetAsInt()
	if size < 1 {
		size = hardware.GetCPUNum()
	}
	return size
}

// initMutatePool initializes the dedicated pool for the online-write CGO path
// (segment Insert/Delete), isolated from segment loading on DynamicPool/LoadPool
// so a post-compaction delta-replay burst cannot starve online writes and freeze
// tSafe advancement (#49435).
func initMutatePool() {
	mutatePoolOnce.Do(func() {
		pt := paramtable.Get()
		size := mutatePoolSize()
		pool := conc.NewPool[any](
			size,
			conc.WithPreAlloc(false),
			conc.WithDisablePurge(false),
			conc.WithPreHandler(func() {
				runtime.LockOSThread()
				C.SetThreadName(cgoTagMutate)
			}), // lock os thread for cgo thread disposal
		)

		mutatePool.Store(pool)
		pt.Watch(pt.QueryNodeCfg.MutatePoolSizeFactor.Key, config.NewHandler("qn.mutatepool.sizefactor", ResizeMutatePool))
		log.Info("init mutatePool done", zap.Int("size", size))
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

func initBM25LoadPool() {
	bm25PoolOnce.Do(func() {
		pt := paramtable.Get()
		poolSize := float64(hardware.GetCPUNum()) * pt.CommonCfg.BM25LoadThreadCoreCoefficient.GetAsFloat()
		pool := conc.NewPool[any](int(poolSize))
		bm25LoadPool.Store(pool)

		pt.Watch(pt.CommonCfg.BM25LoadThreadCoreCoefficient.Key, config.NewHandler("qn.bm25loadpool.bm25loadthreadcorecoefficient", ResizeBM25LoadPool))
		log.Info("init BM25LoadPool done", zap.Int("size", int(poolSize)))
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

func deletePoolSize() int {
	size := hardware.GetCPUNum() * paramtable.Get().QueryNodeCfg.DeletePoolSizeFactor.GetAsInt()
	if size < 1 {
		size = hardware.GetCPUNum()
	}
	return size
}

func initDeletePool() {
	deletePoolOnce.Do(func() {
		pt := paramtable.Get()
		size := deletePoolSize()
		pool := conc.NewPool[struct{}](size)
		deletePool.Store(pool)
		pt.Watch(pt.QueryNodeCfg.DeletePoolSizeFactor.Key, config.NewHandler("qn.deletepool.sizefactor", ResizeDeletePool))
		log.Info("init deletePool done", zap.Int("size", size))
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

// GetMutatePool returns the singleton pool for online write cgo operations
// (segment Insert/Delete).
func GetMutatePool() *conc.Pool[any] {
	initMutatePool()
	return mutatePool.Load()
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

func GetBM25LoadPool() *conc.Pool[any] {
	initBM25LoadPool()
	return bm25LoadPool.Load()
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

func ResizeBM25LoadPool(evt *config.Event) {
	if evt.HasUpdated {
		pt := paramtable.Get()
		newSize := float64(hardware.GetCPUNum()) * pt.CommonCfg.BM25LoadThreadCoreCoefficient.GetAsFloat()
		resizePool(GetBM25LoadPool(), int(newSize), "BM25LoadPool")
	}
}

func ResizeMutatePool(evt *config.Event) {
	if evt.HasUpdated {
		resizePool(GetMutatePool(), mutatePoolSize(), "MutatePool")
	}
}

func ResizeDynamicPool(evt *config.Event) {
	if evt.HasUpdated {
		resizePool(GetDynamicPool(), dynamicPoolSize(), "DynamicPool")
	}
}

func ResizeDeletePool(evt *config.Event) {
	if evt.HasUpdated {
		resizePool(GetDeletePool(), deletePoolSize(), "DeletePool")
	}
}

// CollectPoolStats returns current stats for all goroutine pools.
// Called by PoolMetricsCollector at Prometheus scrape time (pull model).
func CollectPoolStats() []metrics.PoolStats {
	type poolRef struct {
		name string
		pool interface {
			Cap() int
			Running() int
			Waiting() int
		}
	}

	refs := []poolRef{
		{"SQPool", GetSQPool()},
		{"DynamicPool", GetDynamicPool()},
		{"MutatePool", GetMutatePool()},
		{"LoadPool", GetLoadPool()},
		{"WarmupPool", GetWarmupPool()},
		{"BFApplyPool", GetBFApplyPool()},
		{"BM25LoadPool", GetBM25LoadPool()},
		{"DeletePool", GetDeletePool()},
	}

	stats := make([]metrics.PoolStats, 0, len(refs))
	for _, r := range refs {
		stats = append(stats, metrics.PoolStats{
			Name:    r.name,
			Cap:     r.pool.Cap(),
			Running: r.pool.Running(),
			Waiting: r.pool.Waiting(),
		})
	}
	return stats
}

func resizePool[T any](pool *conc.Pool[T], newSize int, tag string) {
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
