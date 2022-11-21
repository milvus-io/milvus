package datanode

import (
	"sync"

	"github.com/milvus-io/milvus/internal/util/concurrency"
)

var ioPool *concurrency.Pool
var ioPoolInitOnce sync.Once

func initIOPool() {
	capacity := Params.DataNodeCfg.IOConcurrency.GetAsInt()
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
