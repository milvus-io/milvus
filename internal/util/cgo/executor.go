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
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// initExecutor initialize underlying cgo thread pools.
func initExecutor() {
	pt := paramtable.Get()

	// Initialize search executor pool.
	searchPoolSize := int(math.Ceil(pt.QueryNodeCfg.MaxReadConcurrency.GetAsFloat() * pt.QueryNodeCfg.CGOPoolSizeRatio.GetAsFloat()))
	C.executor_set_search_thread_num(C.int(searchPoolSize))

	resetSearchThreadNum := func(evt *config.Event) {
		if evt.HasUpdated {
			pt := paramtable.Get()
			newSize := int(math.Ceil(pt.QueryNodeCfg.MaxReadConcurrency.GetAsFloat() * pt.QueryNodeCfg.CGOPoolSizeRatio.GetAsFloat()))
			log.Info("reset cgo search thread num", zap.Int("thread_num", newSize))
			C.executor_set_search_thread_num(C.int(newSize))
		}
	}
	pt.Watch(pt.QueryNodeCfg.MaxReadConcurrency.Key, config.NewHandler("cgo.search."+pt.QueryNodeCfg.MaxReadConcurrency.Key, resetSearchThreadNum))
	pt.Watch(pt.QueryNodeCfg.CGOPoolSizeRatio.Key, config.NewHandler("cgo.search."+pt.QueryNodeCfg.CGOPoolSizeRatio.Key, resetSearchThreadNum))

	// Initialize load executor pool.
	loadPoolSize := hardware.GetCPUNum() * pt.CommonCfg.MiddlePriorityThreadCoreCoefficient.GetAsInt()
	C.executor_set_load_thread_num(C.int(loadPoolSize))

	resetLoadThreadNum := func(evt *config.Event) {
		if evt.HasUpdated {
			pt := paramtable.Get()
			newSize := hardware.GetCPUNum() * pt.CommonCfg.MiddlePriorityThreadCoreCoefficient.GetAsInt()
			log.Info("reset cgo load thread num", zap.Int("thread_num", newSize))
			C.executor_set_load_thread_num(C.int(newSize))
		}
	}
	pt.Watch(pt.CommonCfg.MiddlePriorityThreadCoreCoefficient.Key, config.NewHandler("cgo.load."+pt.CommonCfg.MiddlePriorityThreadCoreCoefficient.Key, resetLoadThreadNum))
}
