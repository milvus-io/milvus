package datanode

import (
	"runtime"
	"sync"

	"github.com/milvus-io/milvus/internal/util/concurrency"
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
	pool, err := concurrency.NewPool(runtime.GOMAXPROCS(0), concurrency.WithPreAlloc(false), concurrency.WithNonBlocking(false))

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
