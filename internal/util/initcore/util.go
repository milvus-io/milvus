// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package initcore

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include "common/init_c.h"
*/
import "C"

import (
	"strings"
	"unsafe"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func UpdateLogLevel(level string) error {
	// always use lower case
	level = strings.ToLower(level)
	cvalue := C.CString(level)
	C.SetLogLevel(cvalue)
	C.free(unsafe.Pointer(cvalue))
	return nil
}

func UpdateIndexSliceSize(size int) {
	C.SetIndexSliceSize(C.int64_t(size))
}

func UpdateStreamBudgetRatio(ratio float64) {
	C.SetStreamBudgetRatio(C.double(ratio))
}

func UpdateHighPriorityThreadCoreCoefficient(coefficient float64) {
	C.SetHighPriorityThreadCoreCoefficient(C.float(coefficient))
}

func UpdateMiddlePriorityThreadCoreCoefficient(coefficient float64) {
	C.SetMiddlePriorityThreadCoreCoefficient(C.float(coefficient))
}

func UpdateLowPriorityThreadCoreCoefficient(coefficient float64) {
	C.SetLowPriorityThreadCoreCoefficient(C.float(coefficient))
}

func UpdateThreadPoolMaxThreadsSize(size int) {
	C.SetThreadPoolMaxThreadsSize(C.int(size))
}

func UpdateDefaultExprEvalBatchSize(size int) {
	C.SetDefaultExprEvalBatchSize(C.int64_t(size))
}

func UpdateDefaultDeleteDumpBatchSize(size int) {
	C.SetDefaultDeleteDumpBatchSize(C.int64_t(size))
}

func UpdateDefaultOptimizeExprEnable(enable bool) {
	C.SetDefaultOptimizeExprEnable(C.bool(enable))
}

func UpdateExprResCacheEnable(enable bool) {
	C.SetExprResCacheEnable(C.bool(enable))
}

func UpdateExprResCacheCapacityBytes(capacity int) {
	C.SetExprResCacheCapacityBytes(C.int64_t(capacity))
}

func UpdateArrowIOThreadPoolCapacity(threads int) {
	C.SetArrowIOThreadPoolCapacity(C.int(threads))
}

// ResolveArrowIOThreadPoolCapacity returns the effective arrow IO thread pool
// size: coefficient × CPU cores, clamped by MaxCapacity when > 0. Returns 0
// when the coefficient is unset, which signals the C++ side to keep arrow's
// built-in default (8).
func ResolveArrowIOThreadPoolCapacity() int {
	cfg := &paramtable.Get().CommonCfg
	coef := cfg.ArrowIOThreadPoolCoefficient.GetAsFloat()
	if coef <= 0 {
		return 0
	}
	threads := int(coef * float64(hardware.GetCPUNum()))
	if threads < 1 {
		threads = 1
	}
	if maxCap := cfg.ArrowIOThreadPoolMaxCapacity.GetAsInt(); maxCap > 0 && threads > maxCap {
		threads = maxCap
	}
	return threads
}

// RegisterArrowIOThreadPoolWatchers wires hot-reload of arrow IO pool capacity
// to paramtable updates on the two coefficient/maxCapacity keys. `source` is
// included in the log entry so log lines from different components (e.g.
// "querynode" vs "datanode" in standalone, where both register the same keys)
// remain distinguishable.
func RegisterArrowIOThreadPoolWatchers(pt *paramtable.ComponentParam, source string) {
	handler := func(key string) func(*config.Event) {
		return func(evt *config.Event) {
			if !evt.HasUpdated {
				return
			}
			newThreads := ResolveArrowIOThreadPoolCapacity()
			UpdateArrowIOThreadPoolCapacity(newThreads)
			log.Info("arrow io thread pool capacity updated",
				zap.String("source", source),
				zap.String("trigger", key),
				zap.Int("threads", newThreads))
		}
	}
	pt.Watch(pt.CommonCfg.ArrowIOThreadPoolCoefficient.Key,
		config.NewHandler(pt.CommonCfg.ArrowIOThreadPoolCoefficient.Key,
			handler(pt.CommonCfg.ArrowIOThreadPoolCoefficient.Key)))
	pt.Watch(pt.CommonCfg.ArrowIOThreadPoolMaxCapacity.Key,
		config.NewHandler(pt.CommonCfg.ArrowIOThreadPoolMaxCapacity.Key,
			handler(pt.CommonCfg.ArrowIOThreadPoolMaxCapacity.Key)))
}

// RegisterArrowReaderConfigWatchers wires hot-reload of arrow parquet reader
// range-coalescing limits to paramtable updates on the two hole/range size
// keys. `source` is included in the log entry for the same reason as in
// RegisterArrowIOThreadPoolWatchers.
func RegisterArrowReaderConfigWatchers(pt *paramtable.ComponentParam, source string) {
	handler := func(evt *config.Event) {
		if !evt.HasUpdated {
			return
		}
		if err := InitArrowReaderConfig(pt); err != nil {
			log.Warn("failed to reconfigure arrow reader params",
				zap.String("source", source), zap.Error(err))
			return
		}
		log.Info("arrow reader params reconfigured",
			zap.String("source", source),
			zap.Int64("holeSizeLimitBytes", pt.CommonCfg.ArrowReaderHoleSizeLimitBytes.GetAsInt64()),
			zap.Int64("rangeSizeLimitBytes", pt.CommonCfg.ArrowReaderRangeSizeLimitBytes.GetAsInt64()))
	}
	pt.Watch(pt.CommonCfg.ArrowReaderHoleSizeLimitBytes.Key,
		config.NewHandler(pt.CommonCfg.ArrowReaderHoleSizeLimitBytes.Key, handler))
	pt.Watch(pt.CommonCfg.ArrowReaderRangeSizeLimitBytes.Key,
		config.NewHandler(pt.CommonCfg.ArrowReaderRangeSizeLimitBytes.Key, handler))
}

func UpdateDefaultGrowingJSONKeyStatsEnable(enable bool) {
	C.SetDefaultGrowingJSONKeyStatsEnable(C.bool(enable))
}

func UpdateDefaultConfigParamTypeCheck(enable bool) {
	C.SetDefaultConfigParamTypeCheck(C.bool(enable))
}

func UpdateEnableLatestDeleteSnapshotOptimization(enable bool) {
	C.SetEnableLatestDeleteSnapshotOptimization(C.bool(enable))
}
