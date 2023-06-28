package datanode

import (
	"runtime"
	"sync"

	"github.com/milvus-io/milvus/internal/util/concurrency"
	"github.com/panjf2000/ants/v2"
)

var ioPool *concurrency.Pool
var ioPoolInitOnce sync.Once

var statsPool *concurrency.Pool
var statsPoolInitOnce sync.Once

func initIOPool() {
	capacity := Params.DataNodeCfg.IOConcurrency
	if capacity > 32 {
		capacity = 32
	}
	// error only happens with negative expiry duration or with negative pre-alloc size.
	ioPool, _ = concurrency.NewPool(capacity)
}

func getOrCreateIOPool() *concurrency.Pool {
	ioPoolInitOnce.Do(initIOPool)
	return ioPool
}

func initStatsPool() {
	pool, err := concurrency.NewPool(runtime.GOMAXPROCS(0), ants.WithPreAlloc(false), ants.WithNonblocking(false))
	if err != nil {
		// shall no happen here
		panic(err)
	}
	statsPool = pool
}

func getOrCreateStatsPool() *concurrency.Pool {
	statsPoolInitOnce.Do(initStatsPool)
	return statsPool
}
