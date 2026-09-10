package io

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var (
	ioPool         *conc.Pool[any]
	ioPoolInitOnce sync.Once
)

var (
	statsPool         *conc.Pool[any]
	statsPoolInitOnce sync.Once
)

var (
	bfApplyPool         atomic.Pointer[conc.Pool[any]]
	bfApplyPoolInitOnce sync.Once
)

// ioDefaultPoolFloor preserves the historical default pool size (16) as a lower
// bound for the auto-scaled capacity, so small nodes are not throttled below the
// pre-existing default after switching to CPU-relative sizing.
const ioDefaultPoolFloor = 16

// ioPoolCapacity resolves the size of the datanode object-storage IO pool.
// When dataNode.dataSync.ioConcurrency is unset (<=0), it scales with the node
// as max(16, CPU*2): the pool is object-storage IO bound and its goroutines mostly
// block on network, so the concurrency can safely exceed the CPU count, while the
// floor keeps small nodes at the old default. An explicit configuration is honored
// as-is (no more hard-coded 32 cap).
func ioPoolCapacity() int {
	capacity := paramtable.Get().DataNodeCfg.IOConcurrency.GetAsInt()
	if capacity <= 0 {
		capacity = hardware.GetCPUNum() * 2
		if capacity < ioDefaultPoolFloor {
			capacity = ioDefaultPoolFloor
		}
	}
	return capacity
}

func initIOPool() {
	// error only happens with negative expiry duration or with negative pre-alloc size.
	ioPool = conc.NewPool[any](ioPoolCapacity())
}

func GetOrCreateIOPool() *conc.Pool[any] {
	ioPoolInitOnce.Do(initIOPool)
	return ioPool
}

func initStatsPool() {
	poolSize := paramtable.Get().DataNodeCfg.ChannelWorkPoolSize.GetAsInt()
	if poolSize <= 0 {
		poolSize = hardware.GetCPUNum()
	}
	statsPool = conc.NewPool[any](poolSize, conc.WithPreAlloc(false), conc.WithNonBlocking(false))
}

func GetOrCreateStatsPool() *conc.Pool[any] {
	statsPoolInitOnce.Do(initStatsPool)
	return statsPool
}

func initMultiReadPool() {
	capacity := paramtable.Get().DataNodeCfg.FileReadConcurrency.GetAsInt()
	if capacity > hardware.GetCPUNum() {
		capacity = hardware.GetCPUNum()
	}
	// error only happens with negative expiry duration or with negative pre-alloc size.
	ioPool = conc.NewPool[any](capacity)
}

func getMultiReadPool() *conc.Pool[any] {
	ioPoolInitOnce.Do(initMultiReadPool)
	return ioPool
}

func resizePool(pool *conc.Pool[any], newSize int, tag string) {
	log := mlog.With(
		mlog.String("poolTag", tag),
		mlog.Int("newSize", newSize),
	)

	if newSize <= 0 {
		log.Warn(context.TODO(), "cannot set pool size to non-positive value")
		return
	}

	err := pool.Resize(newSize)
	if err != nil {
		log.Warn(context.TODO(), "failed to resize pool", mlog.Err(err))
		return
	}
	log.Info(context.TODO(), "pool resize successfully")
}

func ResizeBFApplyPool(evt *config.Event) {
	if evt.HasUpdated {
		pt := paramtable.Get()
		newSize := hardware.GetCPUNum() * pt.QueryNodeCfg.BloomFilterApplyParallelFactor.GetAsInt()
		resizePool(GetBFApplyPool(), newSize, "BFApplyPool")
	}
}

func initBFApplyPool() {
	bfApplyPoolInitOnce.Do(func() {
		pt := paramtable.Get()
		poolSize := hardware.GetCPUNum() * pt.QueryNodeCfg.BloomFilterApplyParallelFactor.GetAsInt()
		mlog.Info(context.TODO(), "init BFApplyPool", mlog.Int("poolSize", poolSize))
		pool := conc.NewPool[any](
			poolSize,
		)

		bfApplyPool.Store(pool)
		pt.Watch(pt.QueryNodeCfg.BloomFilterApplyParallelFactor.Key, config.NewHandler("dn.bfapply.parallel", ResizeBFApplyPool))
	})
}

func GetBFApplyPool() *conc.Pool[any] {
	initBFApplyPool()
	return bfApplyPool.Load()
}
