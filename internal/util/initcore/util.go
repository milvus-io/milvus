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

func UpdateDefaultGrowingJSONKeyStatsEnable(enable bool) {
	C.SetDefaultGrowingJSONKeyStatsEnable(C.bool(enable))
}

func UpdateDefaultConfigParamTypeCheck(enable bool) {
	C.SetDefaultConfigParamTypeCheck(C.bool(enable))
}
