package datanode

import (
	"runtime"
	"sync"

	"github.com/milvus-io/milvus/pkg/util/conc"
)

var ioPool *conc.Pool[any]
var ioPoolInitOnce sync.Once

var statsPool *conc.Pool[any]
var statsPoolInitOnce sync.Once

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
	statsPool = conc.NewPool[any](runtime.GOMAXPROCS(0), conc.WithPreAlloc(false), conc.WithNonBlocking(false))
}

func getOrCreateStatsPool() *conc.Pool[any] {
	statsPoolInitOnce.Do(initStatsPool)
	return statsPool
}
