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
