package datanode

import (
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/concurrency"
	"go.uber.org/zap"
)

var ioPool *concurrency.Pool
var ioPoolInitOnce sync.Once

var readerPool *concurrency.Pool
var readerPoolInitOnce sync.Once

func initIOPool() {
	capacity := Params.DataNodeCfg.IOConcurrency
	if capacity > 32 {
		capacity = 32
	}
	var err error
	// error only happens with negative expiry duration or with negative pre-alloc size.
	ioPool, err = concurrency.NewPool(capacity)
	if err != nil {
		log.Error("failed to init io pool", zap.Error(err))
		panic(err)
	}
}

func getOrCreateIOPool() *concurrency.Pool {
	ioPoolInitOnce.Do(initIOPool)
	return ioPool
}

func initReaderPool() {
	capacity := Params.DataNodeCfg.FileReaderConcurrency
	if capacity > 256 {
		capacity = 256
	}
	var err error
	// error only happens with negative expiry duration or with negative pre-alloc size.
	readerPool, err = concurrency.NewPool(capacity)
	if err != nil {
		log.Error("failed to init io pool", zap.Error(err))
		panic(err)
	}
}

func getOrCreateReaderPool() *concurrency.Pool {
	readerPoolInitOnce.Do(initReaderPool)
	return readerPool
}
