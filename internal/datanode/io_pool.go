package datanode

import (
	"sync"

	"github.com/milvus-io/milvus/internal/util/conc"
)

var ioPool *conc.Pool
var ioPoolInitOnce sync.Once

func initIOPool() {
	capacity := Params.DataNodeCfg.IOConcurrency.GetAsInt()
	if capacity > 32 {
		capacity = 32
	}
	// error only happens with negative expiry duration or with negative pre-alloc size.
	ioPool = conc.NewPool(capacity)
}

func getOrCreateIOPool() *conc.Pool {
	ioPoolInitOnce.Do(initIOPool)
	return ioPool
}
