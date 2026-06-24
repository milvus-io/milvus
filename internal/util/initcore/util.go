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
	"context"
	"strings"
	"unsafe"

	"github.com/milvus-io/milvus/internal/util/pathutil"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

func UpdateDefaultVortexScanPushdownEnable(enable bool) {
	C.SetDefaultVortexScanPushdownEnable(C.bool(enable))
}

func UpdateDefaultOptimizeExprEnable(enable bool) {
	C.SetDefaultOptimizeExprEnable(C.bool(enable))
}

func UpdateDefaultJSONKeyStatsEnable(enable bool) {
	C.SetDefaultJSONKeyStatsEnable(C.bool(enable))
}

func UpdateExprResCacheEnable(enable bool) {
	C.SetExprResCacheEnable(C.bool(enable))
}

func UpdateExprResCacheConfig() {
	params := paramtable.Get()
	diskPath := pathutil.GetPath(pathutil.ExprCachePath, paramtable.GetNodeID())
	cMode := C.CString(params.QueryNodeCfg.ExprResCacheMode.GetValue())
	cDiskPath := C.CString(diskPath)
	defer C.free(unsafe.Pointer(cMode))
	defer C.free(unsafe.Pointer(cDiskPath))

	C.SetExprResCacheConfig(cMode, cDiskPath,
		C.int64_t(params.QueryNodeCfg.ExprResCacheMemMaxBytes.GetAsInt64()),
		C.bool(params.QueryNodeCfg.ExprResCacheMemCompressionEnabled.GetAsBool()),
		C.int32_t(params.QueryNodeCfg.ExprResCacheAdmissionThreshold.GetAsInt32()),
		C.int64_t(params.QueryNodeCfg.ExprResCacheMinEvalDurationUs.GetAsInt64()),
		C.int64_t(params.QueryNodeCfg.ExprResCacheDiskMaxBytes.GetAsInt64()),
		C.int64_t(params.QueryNodeCfg.ExprResCacheDiskMaxFileSizeBytes.GetAsInt64()),
		C.int64_t(params.QueryNodeCfg.ExprResCacheMinEvalDurationUs.GetAsInt64()))
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
			mlog.Info(context.TODO(), "arrow io thread pool capacity updated",
				mlog.String("source", source),
				mlog.String("trigger", key),
				mlog.Int("threads", newThreads))
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
			mlog.Warn(context.TODO(), "failed to reconfigure arrow reader params",
				mlog.String("source", source), mlog.Err(err))
			return
		}
		mlog.Info(context.TODO(), "arrow reader params reconfigured",
			mlog.String("source", source),
			mlog.Int64("holeSizeLimitBytes", pt.CommonCfg.ArrowReaderHoleSizeLimitBytes.GetAsInt64()),
			mlog.Int64("rangeSizeLimitBytes", pt.CommonCfg.ArrowReaderRangeSizeLimitBytes.GetAsInt64()))
	}
	pt.Watch(pt.CommonCfg.ArrowReaderHoleSizeLimitBytes.Key,
		config.NewHandler(pt.CommonCfg.ArrowReaderHoleSizeLimitBytes.Key, handler))
	pt.Watch(pt.CommonCfg.ArrowReaderRangeSizeLimitBytes.Key,
		config.NewHandler(pt.CommonCfg.ArrowReaderRangeSizeLimitBytes.Key, handler))
}

func UpdateStorageV2CellTargetSizeBytes(bytes int64) {
	C.SetStorageV2CellTargetSizeBytes(C.int64_t(bytes))
}

func UpdateDefaultGrowingJSONKeyStatsEnable(enable bool) {
	C.SetDefaultGrowingJSONKeyStatsEnable(C.bool(enable))
}

func UpdateDefaultConfigParamTypeCheck(enable bool) {
	C.SetDefaultConfigParamTypeCheck(C.bool(enable))
}

func UpdateDefaultEnableParquetStatsSkipIndex(enable bool) {
	C.SetDefaultEnableParquetStatsSkipIndex(C.bool(enable))
}

func UpdateEnableLatestDeleteSnapshotOptimization(enable bool) {
	C.SetEnableLatestDeleteSnapshotOptimization(C.bool(enable))
}
