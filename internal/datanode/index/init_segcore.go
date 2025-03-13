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

package index

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include <stdint.h>
#include "common/init_c.h"
#include "segcore/segcore_init_c.h"
#include "indexbuilder/init_c.h"
*/
import "C"
import (
	"path"
	"path/filepath"
	"unsafe"

	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func InitSegcore() {
	cGlogConf := C.CString(path.Join(paramtable.GetBaseTable().GetConfigDir(), paramtable.DefaultGlogConf))
	C.IndexBuilderInit(cGlogConf)
	C.free(unsafe.Pointer(cGlogConf))

	// override index builder SIMD type
	cSimdType := C.CString(paramtable.Get().CommonCfg.SimdType.GetValue())
	C.IndexBuilderSetSimdType(cSimdType)
	C.free(unsafe.Pointer(cSimdType))

	// override segcore index slice size
	cIndexSliceSize := C.int64_t(paramtable.Get().CommonCfg.IndexSliceSize.GetAsInt64())
	C.InitIndexSliceSize(cIndexSliceSize)

	// set up thread pool for different priorities
	cHighPriorityThreadCoreCoefficient := C.int64_t(paramtable.Get().CommonCfg.HighPriorityThreadCoreCoefficient.GetAsInt64())
	C.InitHighPriorityThreadCoreCoefficient(cHighPriorityThreadCoreCoefficient)
	cMiddlePriorityThreadCoreCoefficient := C.int64_t(paramtable.Get().CommonCfg.MiddlePriorityThreadCoreCoefficient.GetAsInt64())
	C.InitMiddlePriorityThreadCoreCoefficient(cMiddlePriorityThreadCoreCoefficient)
	cLowPriorityThreadCoreCoefficient := C.int64_t(paramtable.Get().CommonCfg.LowPriorityThreadCoreCoefficient.GetAsInt64())
	C.InitLowPriorityThreadCoreCoefficient(cLowPriorityThreadCoreCoefficient)

	cCPUNum := C.int(hardware.GetCPUNum())
	C.InitCpuNum(cCPUNum)

	cKnowhereThreadPoolSize := C.uint32_t(hardware.GetCPUNum() * paramtable.DefaultKnowhereThreadPoolNumRatioInBuild)
	if paramtable.GetRole() == typeutil.StandaloneRole {
		threadPoolSize := int(float64(hardware.GetCPUNum()) * paramtable.Get().CommonCfg.BuildIndexThreadPoolRatio.GetAsFloat())
		if threadPoolSize < 1 {
			threadPoolSize = 1
		}
		cKnowhereThreadPoolSize = C.uint32_t(threadPoolSize)
	}
	C.SegcoreSetKnowhereBuildThreadPoolNum(cKnowhereThreadPoolSize)

	localDataRootPath := filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.IndexNodeRole)
	initcore.InitLocalChunkManager(localDataRootPath)
	cGpuMemoryPoolInitSize := C.uint32_t(paramtable.Get().GpuConfig.InitSize.GetAsUint32())
	cGpuMemoryPoolMaxSize := C.uint32_t(paramtable.Get().GpuConfig.MaxSize.GetAsUint32())
	C.SegcoreSetKnowhereGpuMemoryPoolSize(cGpuMemoryPoolInitSize, cGpuMemoryPoolMaxSize)
}

func CloseSegcore() {
	initcore.CleanGlogManager()
}
