// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
#include "common/init_c.h"
#include "segcore/segcore_init_c.h"
#include "storage/storage_c.h"
#include "segcore/arrow_fs_c.h"
#include "common/type_c.h"
#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
#include "exec/expression/function/init_c.h"
*/
import "C"

import (
	"context"
	"path"
	"sync"
	"unsafe"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/pathutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var initQueryNodeOnce sync.Once

// InitQueryNode initializes query node once.
func InitQueryNode(ctx context.Context) error {
	var err error
	initQueryNodeOnce.Do(func() {
		err = doInitQueryNodeOnce(ctx)
	})
	return err
}

// doInitQueryNodeOnce initializes query node once.
func doInitQueryNodeOnce(ctx context.Context) error {
	nodeID := paramtable.GetNodeID()

	cGlogConf := C.CString(path.Join(paramtable.GetBaseTable().GetConfigDir(), paramtable.DefaultGlogConf))
	C.SegcoreInit(cGlogConf)
	C.free(unsafe.Pointer(cGlogConf))

	// update log level based on current setup
	UpdateLogLevel(paramtable.Get().LogCfg.Level.GetValue())

	// override segcore chunk size
	cChunkRows := C.int64_t(paramtable.Get().QueryNodeCfg.ChunkRows.GetAsInt64())
	C.SegcoreSetChunkRows(cChunkRows)

	cKnowhereThreadPoolSize := C.uint32_t(paramtable.Get().QueryNodeCfg.KnowhereThreadPoolSize.GetAsUint32())
	C.SegcoreSetKnowhereSearchThreadPoolNum(cKnowhereThreadPoolSize)

	cKnowhereFetchThreadPoolSize := C.uint32_t(paramtable.Get().QueryNodeCfg.KnowhereFetchThreadPoolSize.GetAsUint32())
	C.SegcoreSetKnowhereFetchThreadPoolNum(cKnowhereFetchThreadPoolSize)

	// override segcore SIMD type
	cSimdType := C.CString(paramtable.Get().CommonCfg.SimdType.GetValue())
	C.SegcoreSetSimdType(cSimdType)
	C.free(unsafe.Pointer(cSimdType))

	enableKnowhereScoreConsistency := paramtable.Get().QueryNodeCfg.KnowhereScoreConsistency.GetAsBool()
	if enableKnowhereScoreConsistency {
		C.SegcoreEnableKnowhereScoreConsistency()
	}

	// override segcore index slice size
	cIndexSliceSize := C.int64_t(paramtable.Get().CommonCfg.IndexSliceSize.GetAsInt64())
	C.SetIndexSliceSize(cIndexSliceSize)

	// set up thread pool for different priorities
	cHighPriorityThreadCoreCoefficient := C.float(paramtable.Get().CommonCfg.HighPriorityThreadCoreCoefficient.GetAsFloat())
	C.SetHighPriorityThreadCoreCoefficient(cHighPriorityThreadCoreCoefficient)
	cMiddlePriorityThreadCoreCoefficient := C.float(paramtable.Get().CommonCfg.MiddlePriorityThreadCoreCoefficient.GetAsFloat())
	C.SetMiddlePriorityThreadCoreCoefficient(cMiddlePriorityThreadCoreCoefficient)
	cLowPriorityThreadCoreCoefficient := C.float(paramtable.Get().CommonCfg.LowPriorityThreadCoreCoefficient.GetAsFloat())
	C.SetLowPriorityThreadCoreCoefficient(cLowPriorityThreadCoreCoefficient)

	cCPUNum := C.int(hardware.GetCPUNum())
	C.InitCpuNum(cCPUNum)

	knowhereBuildPoolSize := uint32(float32(paramtable.Get().QueryNodeCfg.InterimIndexBuildParallelRate.GetAsFloat()) * float32(hardware.GetCPUNum()))
	if knowhereBuildPoolSize < uint32(1) {
		knowhereBuildPoolSize = uint32(1)
	}
	log.Ctx(ctx).Info("set up knowhere build pool size", zap.Uint32("pool_size", knowhereBuildPoolSize))
	cKnowhereBuildPoolSize := C.uint32_t(knowhereBuildPoolSize)
	C.SegcoreSetKnowhereBuildThreadPoolNum(cKnowhereBuildPoolSize)

	cExprBatchSize := C.int64_t(paramtable.Get().QueryNodeCfg.ExprEvalBatchSize.GetAsInt64())
	C.SetDefaultExprEvalBatchSize(cExprBatchSize)

	cDeleteDumpBatchSize := C.int64_t(paramtable.Get().QueryNodeCfg.DeleteDumpBatchSize.GetAsInt64())
	C.SetDefaultDeleteDumpBatchSize(cDeleteDumpBatchSize)

	cOptimizeExprEnabled := C.bool(paramtable.Get().CommonCfg.EnabledOptimizeExpr.GetAsBool())
	C.SetDefaultOptimizeExprEnable(cOptimizeExprEnabled)

	cGrowingJSONKeyStatsEnabled := C.bool(paramtable.Get().CommonCfg.EnabledGrowingSegmentJSONKeyStats.GetAsBool())
	C.SetDefaultGrowingJSONKeyStatsEnable(cGrowingJSONKeyStatsEnabled)

	if paramtable.GetRole() != typeutil.StreamingNodeRole {
		cGpuMemoryPoolInitSize := C.uint32_t(paramtable.Get().GpuConfig.InitSize.GetAsUint32())
		cGpuMemoryPoolMaxSize := C.uint32_t(paramtable.Get().GpuConfig.MaxSize.GetAsUint32())
		C.SegcoreSetKnowhereGpuMemoryPoolSize(cGpuMemoryPoolInitSize, cGpuMemoryPoolMaxSize)
	}

	cEnableConfigParamTypeCheck := C.bool(paramtable.Get().CommonCfg.EnableConfigParamTypeCheck.GetAsBool())
	C.SetDefaultConfigParamTypeCheck(cEnableConfigParamTypeCheck)

	cExprResCacheEnabled := C.bool(paramtable.Get().QueryNodeCfg.ExprResCacheEnabled.GetAsBool())
	C.SetExprResCacheEnable(cExprResCacheEnabled)

	cExprResCacheCapacityBytes := C.int64_t(paramtable.Get().QueryNodeCfg.ExprResCacheCapacityBytes.GetAsInt64())
	C.SetExprResCacheCapacityBytes(cExprResCacheCapacityBytes)

	cEnableParquetStatsSkipIndex := C.bool(paramtable.Get().CommonCfg.EnableNamespace.GetAsBool())
	C.SetDefaultEnableParquetStatsSkipIndex(cEnableParquetStatsSkipIndex)

	localDataRootPath := pathutil.GetPath(pathutil.LocalChunkPath, nodeID)

	InitLocalChunkManager(localDataRootPath)

	err := InitRemoteChunkManager(paramtable.Get())
	if err != nil {
		return err
	}

	err = InitDiskFileWriterConfig(paramtable.Get())
	if err != nil {
		return err
	}

	if paramtable.Get().CommonCfg.EnableStorageV2.GetAsBool() {
		err = InitStorageV2FileSystem(paramtable.Get())
		if err != nil {
			return err
		}
	}

	err = InitMmapManager(paramtable.Get(), nodeID)
	if err != nil {
		return err
	}

	err = InitTieredStorage(paramtable.Get())
	if err != nil {
		return err
	}

	err = InitInterminIndexConfig(paramtable.Get())
	if err != nil {
		return err
	}

	err = InitGeometryCache(paramtable.Get())
	if err != nil {
		return err
	}

	InitTraceConfig(paramtable.Get())
	C.InitExecExpressionFunctionFactory()

	// init paramtable change callback for core related config
	SetupCoreConfigChangelCallback()
	return InitPluginLoader()
}
