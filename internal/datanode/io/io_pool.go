package io

import (
	"context"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var (
	bfApplyPool         atomic.Pointer[conc.Pool[any]]
	bfApplyPoolInitOnce sync.Once
)

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
		log.Info("init BFApplyPool", zap.Int("poolSize", poolSize))
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
