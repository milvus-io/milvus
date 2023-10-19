package datanode

import (
	"runtime"
	"sync"

	"github.com/milvus-io/milvus/pkg/util/conc"
)

var (
	ioPool         *conc.Pool[any]
	ioPoolInitOnce sync.Once
)

var (
	statsPool         *conc.Pool[any]
	statsPoolInitOnce sync.Once
)

func initIOPool() {
	capacity := Params.DataNodeCfg.IOConcurrency.GetAsInt()
	if capacity > 32 {
		capacity = 32
	}
	// error only happens with negative expiry duration or with negative pre-alloc size.
	ioPool = conc.NewPool[any](capacity)
}

func getOrCreateIOPool() *conc.Pool[any] {
	ioPoolInitOnce.Do(initIOPool)
	return ioPool
}

func initStatsPool() {
	poolSize := Params.DataNodeCfg.ChannelWorkPoolSize.GetAsInt()
	if poolSize <= 0 {
		poolSize = runtime.GOMAXPROCS(0)
	}
	statsPool = conc.NewPool[any](poolSize, conc.WithPreAlloc(false), conc.WithNonBlocking(false))
}

func getOrCreateStatsPool() *conc.Pool[any] {
	statsPoolInitOnce.Do(initStatsPool)
	return statsPool
}

func initMultiReadPool() {
	capacity := Params.DataNodeCfg.FileReadConcurrency.GetAsInt()
	if capacity > runtime.GOMAXPROCS(0) {
		capacity = runtime.GOMAXPROCS(0)
	}
	// error only happens with negative expiry duration or with negative pre-alloc size.
	ioPool = conc.NewPool[any](capacity)
}

func getMultiReadPool() *conc.Pool[any] {
	ioPoolInitOnce.Do(initMultiReadPool)
	return ioPool
}
