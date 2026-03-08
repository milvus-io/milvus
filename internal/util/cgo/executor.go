package cgo

/*
#cgo pkg-config: milvus_core

#include "futures/future_c.h"
*/
import "C"

import (
	"math"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// initExecutor initialize underlying cgo thread pool.
func initExecutor() {
	pt := paramtable.Get()
	initPoolSize := int(math.Ceil(pt.QueryNodeCfg.MaxReadConcurrency.GetAsFloat() * pt.QueryNodeCfg.CGOPoolSizeRatio.GetAsFloat()))
	C.executor_set_thread_num(C.int(initPoolSize))

	resetThreadNum := func(evt *config.Event) {
		if evt.HasUpdated {
			pt := paramtable.Get()
			newSize := int(math.Ceil(pt.QueryNodeCfg.MaxReadConcurrency.GetAsFloat() * pt.QueryNodeCfg.CGOPoolSizeRatio.GetAsFloat()))
			log.Info("reset cgo thread num", zap.Int("thread_num", newSize))
			C.executor_set_thread_num(C.int(newSize))
		}
	}
	pt.Watch(pt.QueryNodeCfg.MaxReadConcurrency.Key, config.NewHandler("cgo."+pt.QueryNodeCfg.MaxReadConcurrency.Key, resetThreadNum))
	pt.Watch(pt.QueryNodeCfg.CGOPoolSizeRatio.Key, config.NewHandler("cgo."+pt.QueryNodeCfg.CGOPoolSizeRatio.Key, resetThreadNum))
}
