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
#include "common/init_c.h"
#include "segcore/segcore_init_c.h"
#include "storage/storage_c.h"
#include "segcore/arrow_fs_c.h"
*/
import "C"

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/pathutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func InitLocalChunkManager(path string) {
	CLocalRootPath := C.CString(path)
	defer C.free(unsafe.Pointer(CLocalRootPath))
	C.InitLocalChunkManagerSingleton(CLocalRootPath)
}

func InitTraceConfig(params *paramtable.ComponentParam) {
	sampleFraction := C.float(params.TraceCfg.SampleFraction.GetAsFloat())
	nodeID := C.int(paramtable.GetNodeID())
	exporter := C.CString(params.TraceCfg.Exporter.GetValue())
	jaegerURL := C.CString(params.TraceCfg.JaegerURL.GetValue())
	otlpMethod := C.CString(params.TraceCfg.OtlpMethod.GetValue())
	endpoint := C.CString(params.TraceCfg.OtlpEndpoint.GetValue())
	otlpSecure := params.TraceCfg.OtlpSecure.GetAsBool()
	otlpHeaders := C.CString(serializeHeaders(params.TraceCfg.OtlpHeaders.GetValue()))
	defer C.free(unsafe.Pointer(exporter))
	defer C.free(unsafe.Pointer(jaegerURL))
	defer C.free(unsafe.Pointer(endpoint))
	defer C.free(unsafe.Pointer(otlpMethod))
	defer C.free(unsafe.Pointer(otlpHeaders))

	config := C.CTraceConfig{
		exporter:       exporter,
		sampleFraction: sampleFraction,
		jaegerURL:      jaegerURL,
		otlpEndpoint:   endpoint,
		otlpMethod:     otlpMethod,
		otlpHeaders:    otlpHeaders,
		oltpSecure:     (C.bool)(otlpSecure),
		nodeID:         nodeID,
	}
	// oltp grpc may hangs forever, add timeout logic at go side
	timeout := params.TraceCfg.InitTimeoutSeconds.GetAsDuration(time.Second)
	callWithTimeout(func() {
		C.InitTrace(&config)
	}, func() {
		panic("init segcore tracing timeout, See issue #33483")
	}, timeout)
}

func ResetTraceConfig(params *paramtable.ComponentParam) {
	sampleFraction := C.float(params.TraceCfg.SampleFraction.GetAsFloat())
	nodeID := C.int(paramtable.GetNodeID())
	exporter := C.CString(params.TraceCfg.Exporter.GetValue())
	jaegerURL := C.CString(params.TraceCfg.JaegerURL.GetValue())
	endpoint := C.CString(params.TraceCfg.OtlpEndpoint.GetValue())
	otlpMethod := C.CString(params.TraceCfg.OtlpMethod.GetValue())
	otlpSecure := params.TraceCfg.OtlpSecure.GetAsBool()
	otlpHeaders := C.CString(serializeHeaders(params.TraceCfg.OtlpHeaders.GetValue()))
	defer C.free(unsafe.Pointer(exporter))
	defer C.free(unsafe.Pointer(jaegerURL))
	defer C.free(unsafe.Pointer(endpoint))
	defer C.free(unsafe.Pointer(otlpMethod))
	defer C.free(unsafe.Pointer(otlpHeaders))

	config := C.CTraceConfig{
		exporter:       exporter,
		sampleFraction: sampleFraction,
		jaegerURL:      jaegerURL,
		otlpEndpoint:   endpoint,
		otlpMethod:     otlpMethod,
		otlpHeaders:    otlpHeaders,
		oltpSecure:     (C.bool)(otlpSecure),
		nodeID:         nodeID,
	}

	// oltp grpc may hangs forever, add timeout logic at go side
	timeout := params.TraceCfg.InitTimeoutSeconds.GetAsDuration(time.Second)
	callWithTimeout(func() {
		C.SetTrace(&config)
	}, func() {
		panic("set segcore tracing timeout, See issue #33483")
	}, timeout)
}

func callWithTimeout(fn func(), timeoutHandler func(), timeout time.Duration) {
	if timeout > 0 {
		ch := make(chan struct{})
		go func() {
			defer close(ch)
			fn()
		}()
		select {
		case <-ch:
		case <-time.After(timeout):
			timeoutHandler()
		}
	} else {
		fn()
	}
}

func InitStorageV2FileSystem(params *paramtable.ComponentParam) error {
	if params.CommonCfg.StorageType.GetValue() == "local" {
		return InitLocalArrowFileSystem(params.LocalStorageCfg.Path.GetValue())
	}
	return InitRemoteArrowFileSystem(params)
}

func InitLocalArrowFileSystem(path string) error {
	CLocalRootPath := C.CString(path)
	defer C.free(unsafe.Pointer(CLocalRootPath))
	status := C.InitLocalArrowFileSystemSingleton(CLocalRootPath)
	return HandleCStatus(&status, "InitLocalArrowFileSystemSingleton failed")
}

func InitRemoteArrowFileSystem(params *paramtable.ComponentParam) error {
	cAddress := C.CString(params.MinioCfg.Address.GetValue())
	cBucketName := C.CString(params.MinioCfg.BucketName.GetValue())
	cAccessKey := C.CString(params.MinioCfg.AccessKeyID.GetValue())
	cAccessValue := C.CString(params.MinioCfg.SecretAccessKey.GetValue())
	cRootPath := C.CString(params.MinioCfg.RootPath.GetValue())
	cStorageType := C.CString(params.CommonCfg.StorageType.GetValue())
	cIamEndPoint := C.CString(params.MinioCfg.IAMEndpoint.GetValue())
	cCloudProvider := C.CString(params.MinioCfg.CloudProvider.GetValue())
	cLogLevel := C.CString(params.MinioCfg.LogLevel.GetValue())
	cRegion := C.CString(params.MinioCfg.Region.GetValue())
	cSslCACert := C.CString(params.MinioCfg.SslCACert.GetValue())
	cGcpCredentialJSON := C.CString(params.MinioCfg.GcpCredentialJSON.GetValue())
	defer C.free(unsafe.Pointer(cAddress))
	defer C.free(unsafe.Pointer(cBucketName))
	defer C.free(unsafe.Pointer(cAccessKey))
	defer C.free(unsafe.Pointer(cAccessValue))
	defer C.free(unsafe.Pointer(cRootPath))
	defer C.free(unsafe.Pointer(cStorageType))
	defer C.free(unsafe.Pointer(cIamEndPoint))
	defer C.free(unsafe.Pointer(cLogLevel))
	defer C.free(unsafe.Pointer(cRegion))
	defer C.free(unsafe.Pointer(cCloudProvider))
	defer C.free(unsafe.Pointer(cSslCACert))
	defer C.free(unsafe.Pointer(cGcpCredentialJSON))
	storageConfig := C.CStorageConfig{
		address:                cAddress,
		bucket_name:            cBucketName,
		access_key_id:          cAccessKey,
		access_key_value:       cAccessValue,
		root_path:              cRootPath,
		storage_type:           cStorageType,
		iam_endpoint:           cIamEndPoint,
		cloud_provider:         cCloudProvider,
		useSSL:                 C.bool(params.MinioCfg.UseSSL.GetAsBool()),
		sslCACert:              cSslCACert,
		useIAM:                 C.bool(params.MinioCfg.UseIAM.GetAsBool()),
		log_level:              cLogLevel,
		region:                 cRegion,
		useVirtualHost:         C.bool(params.MinioCfg.UseVirtualHost.GetAsBool()),
		requestTimeoutMs:       C.int64_t(params.MinioCfg.RequestTimeoutMs.GetAsInt64()),
		gcp_credential_json:    cGcpCredentialJSON,
		use_custom_part_upload: true,
		max_connections:        C.uint32_t(params.MinioCfg.MaxConnections.GetAsInt()),
	}

	status := C.InitRemoteArrowFileSystemSingleton(storageConfig)
	return HandleCStatus(&status, "InitRemoteArrowFileSystemSingleton failed")
}

func InitRemoteChunkManager(params *paramtable.ComponentParam) error {
	cAddress := C.CString(params.MinioCfg.Address.GetValue())
	cBucketName := C.CString(params.MinioCfg.BucketName.GetValue())
	cAccessKey := C.CString(params.MinioCfg.AccessKeyID.GetValue())
	cAccessValue := C.CString(params.MinioCfg.SecretAccessKey.GetValue())
	cRootPath := C.CString(params.MinioCfg.RootPath.GetValue())
	cStorageType := C.CString(params.CommonCfg.StorageType.GetValue())
	cIamEndPoint := C.CString(params.MinioCfg.IAMEndpoint.GetValue())
	cCloudProvider := C.CString(params.MinioCfg.CloudProvider.GetValue())
	cLogLevel := C.CString(params.MinioCfg.LogLevel.GetValue())
	cRegion := C.CString(params.MinioCfg.Region.GetValue())
	cSslCACert := C.CString(params.MinioCfg.SslCACert.GetValue())
	cGcpCredentialJSON := C.CString(params.MinioCfg.GcpCredentialJSON.GetValue())
	defer C.free(unsafe.Pointer(cAddress))
	defer C.free(unsafe.Pointer(cBucketName))
	defer C.free(unsafe.Pointer(cAccessKey))
	defer C.free(unsafe.Pointer(cAccessValue))
	defer C.free(unsafe.Pointer(cRootPath))
	defer C.free(unsafe.Pointer(cStorageType))
	defer C.free(unsafe.Pointer(cIamEndPoint))
	defer C.free(unsafe.Pointer(cLogLevel))
	defer C.free(unsafe.Pointer(cRegion))
	defer C.free(unsafe.Pointer(cCloudProvider))
	defer C.free(unsafe.Pointer(cSslCACert))
	defer C.free(unsafe.Pointer(cGcpCredentialJSON))
	storageConfig := C.CStorageConfig{
		address:             cAddress,
		bucket_name:         cBucketName,
		access_key_id:       cAccessKey,
		access_key_value:    cAccessValue,
		root_path:           cRootPath,
		storage_type:        cStorageType,
		iam_endpoint:        cIamEndPoint,
		cloud_provider:      cCloudProvider,
		useSSL:              C.bool(params.MinioCfg.UseSSL.GetAsBool()),
		sslCACert:           cSslCACert,
		useIAM:              C.bool(params.MinioCfg.UseIAM.GetAsBool()),
		log_level:           cLogLevel,
		region:              cRegion,
		useVirtualHost:      C.bool(params.MinioCfg.UseVirtualHost.GetAsBool()),
		requestTimeoutMs:    C.int64_t(params.MinioCfg.RequestTimeoutMs.GetAsInt64()),
		gcp_credential_json: cGcpCredentialJSON,
		max_connections:     C.uint32_t(params.MinioCfg.MaxConnections.GetAsInt()),
	}

	status := C.InitRemoteChunkManagerSingleton(storageConfig)
	return HandleCStatus(&status, "InitRemoteChunkManagerSingleton failed")
}

func InitMmapManager(params *paramtable.ComponentParam, nodeID int64) error {
	growingMMapDir := pathutil.GetPath(pathutil.GrowingMMapPath, nodeID)
	cGrowingMMapDir := C.CString(growingMMapDir)
	cCacheReadAheadPolicy := C.CString(params.QueryNodeCfg.ReadAheadPolicy.GetValue())
	defer C.free(unsafe.Pointer(cGrowingMMapDir))
	defer C.free(unsafe.Pointer(cCacheReadAheadPolicy))
	diskCapacity := params.QueryNodeCfg.DiskCapacityLimit.GetAsUint64()
	diskLimit := uint64(float64(params.QueryNodeCfg.MaxMmapDiskPercentageForMmapManager.GetAsUint64()*diskCapacity) * 0.01)
	mmapFileSize := params.QueryNodeCfg.FixedFileSizeForMmapManager.GetAsFloat() * 1024 * 1024
	mmapConfig := C.CMmapConfig{
		cache_read_ahead_policy:  cCacheReadAheadPolicy,
		mmap_path:                cGrowingMMapDir,
		disk_limit:               C.uint64_t(diskLimit),
		fix_file_size:            C.uint64_t(mmapFileSize),
		growing_enable_mmap:      C.bool(params.QueryNodeCfg.GrowingMmapEnabled.GetAsBool()),
		scalar_index_enable_mmap: C.bool(params.QueryNodeCfg.MmapScalarIndex.GetAsBool()),
		scalar_field_enable_mmap: C.bool(params.QueryNodeCfg.MmapScalarField.GetAsBool()),
		vector_index_enable_mmap: C.bool(params.QueryNodeCfg.MmapVectorIndex.GetAsBool()),
		vector_field_enable_mmap: C.bool(params.QueryNodeCfg.MmapVectorField.GetAsBool()),
	}
	status := C.InitMmapManager(mmapConfig)
	return HandleCStatus(&status, "InitMmapManager failed")
}

func ConvertCacheWarmupPolicy(policy string) (C.CacheWarmupPolicy, error) {
	switch policy {
	case "sync":
		return C.CacheWarmupPolicy_Sync, nil
	case "disable":
		return C.CacheWarmupPolicy_Disable, nil
	default:
		return C.CacheWarmupPolicy_Disable, fmt.Errorf("invalid Tiered Storage cache warmup policy: %s", policy)
	}
}

func InitTieredStorage(params *paramtable.ComponentParam) error {
	// init tiered storage
	scalarFieldCacheWarmupPolicy, err := ConvertCacheWarmupPolicy(params.QueryNodeCfg.TieredWarmupScalarField.GetValue())
	if err != nil {
		return err
	}
	vectorFieldCacheWarmupPolicy, err := ConvertCacheWarmupPolicy(params.QueryNodeCfg.TieredWarmupVectorField.GetValue())
	if err != nil {
		return err
	}
	deprecatedCacheWarmupPolicy := params.QueryNodeCfg.ChunkCacheWarmingUp.GetValue()
	if deprecatedCacheWarmupPolicy == "sync" {
		log.Warn("queryNode.cache.warmup is being deprecated, use queryNode.segcore.tieredStorage.warmup.vectorField instead.")
		log.Warn("for now, if queryNode.cache.warmup is set to sync, it will override queryNode.segcore.tieredStorage.warmup.vectorField to sync.")
		log.Warn("otherwise, queryNode.cache.warmup will be ignored")
		vectorFieldCacheWarmupPolicy = C.CacheWarmupPolicy_Sync
	} else if deprecatedCacheWarmupPolicy == "async" {
		log.Warn("queryNode.cache.warmup is being deprecated and ignored, use queryNode.segcore.tieredStorage.warmup.vectorField instead.")
	}
	scalarIndexCacheWarmupPolicy, err := ConvertCacheWarmupPolicy(params.QueryNodeCfg.TieredWarmupScalarIndex.GetValue())
	if err != nil {
		return err
	}
	vectorIndexCacheWarmupPolicy, err := ConvertCacheWarmupPolicy(params.QueryNodeCfg.TieredWarmupVectorIndex.GetValue())
	if err != nil {
		return err
	}
	osMemBytes := hardware.GetMemoryCount()
	osDiskBytes := params.QueryNodeCfg.DiskCapacityLimit.GetAsInt64()

	memoryLowWatermarkRatio := params.QueryNodeCfg.TieredMemoryLowWatermarkRatio.GetAsFloat()
	memoryHighWatermarkRatio := params.QueryNodeCfg.TieredMemoryHighWatermarkRatio.GetAsFloat()
	memoryMaxRatio := params.QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat()
	diskLowWatermarkRatio := params.QueryNodeCfg.TieredDiskLowWatermarkRatio.GetAsFloat()
	diskHighWatermarkRatio := params.QueryNodeCfg.TieredDiskHighWatermarkRatio.GetAsFloat()
	diskMaxRatio := params.QueryNodeCfg.MaxDiskUsagePercentage.GetAsFloat()

	if memoryLowWatermarkRatio > memoryHighWatermarkRatio {
		return errors.New("memoryLowWatermarkRatio should not be greater than memoryHighWatermarkRatio")
	}
	if memoryHighWatermarkRatio > memoryMaxRatio {
		return errors.New("memoryHighWatermarkRatio should not be greater than memoryMaxRatio")
	}
	if memoryMaxRatio >= 1 {
		return errors.New("memoryMaxRatio should not be greater than 1")
	}

	if diskLowWatermarkRatio > diskHighWatermarkRatio {
		return errors.New("diskLowWatermarkRatio should not be greater than diskHighWatermarkRatio")
	}
	if diskHighWatermarkRatio > diskMaxRatio {
		return errors.New("diskHighWatermarkRatio should not be greater than diskMaxRatio")
	}
	if diskMaxRatio >= 1 {
		return errors.New("diskMaxRatio should not be greater than 1")
	}

	memoryLowWatermarkBytes := C.int64_t(memoryLowWatermarkRatio * float64(osMemBytes))
	memoryHighWatermarkBytes := C.int64_t(memoryHighWatermarkRatio * float64(osMemBytes))
	memoryMaxBytes := C.int64_t(memoryMaxRatio * float64(osMemBytes))

	diskLowWatermarkBytes := C.int64_t(diskLowWatermarkRatio * float64(osDiskBytes))
	diskHighWatermarkBytes := C.int64_t(diskHighWatermarkRatio * float64(osDiskBytes))
	diskMaxBytes := C.int64_t(diskMaxRatio * float64(osDiskBytes))

	storageUsageTrackingEnabled := C.bool(params.QueryNodeCfg.StorageUsageTrackingEnabled.GetAsBool())
	evictionEnabled := C.bool(params.QueryNodeCfg.TieredEvictionEnabled.GetAsBool())
	cacheTouchWindowMs := C.int64_t(params.QueryNodeCfg.TieredCacheTouchWindowMs.GetAsInt64())
	backgroundEvictionEnabled := C.bool(params.QueryNodeCfg.TieredBackgroundEvictionEnabled.GetAsBool())
	evictionIntervalMs := C.int64_t(params.QueryNodeCfg.TieredEvictionIntervalMs.GetAsInt64())
	cacheCellUnaccessedSurvivalTime := C.int64_t(params.QueryNodeCfg.CacheCellUnaccessedSurvivalTime.GetAsInt64())
	loadingResourceFactor := C.float(params.QueryNodeCfg.TieredLoadingResourceFactor.GetAsFloat())
	overloadedMemoryThresholdPercentage := C.float(memoryMaxRatio)
	maxDiskUsagePercentage := C.float(diskMaxRatio)
	diskPath := C.CString(params.LocalStorageCfg.Path.GetValue())
	defer C.free(unsafe.Pointer(diskPath))

	C.ConfigureTieredStorage(scalarFieldCacheWarmupPolicy,
		vectorFieldCacheWarmupPolicy,
		scalarIndexCacheWarmupPolicy,
		vectorIndexCacheWarmupPolicy,
		memoryLowWatermarkBytes, memoryHighWatermarkBytes, memoryMaxBytes,
		diskLowWatermarkBytes, diskHighWatermarkBytes, diskMaxBytes,
		storageUsageTrackingEnabled,
		evictionEnabled, cacheTouchWindowMs,
		backgroundEvictionEnabled, evictionIntervalMs, cacheCellUnaccessedSurvivalTime,
		overloadedMemoryThresholdPercentage, loadingResourceFactor, maxDiskUsagePercentage, diskPath)

	tieredEvictableMemoryCacheRatio := params.QueryNodeCfg.TieredEvictableMemoryCacheRatio.GetAsFloat()
	tieredEvictableDiskCacheRatio := params.QueryNodeCfg.TieredEvictableDiskCacheRatio.GetAsFloat()

	log.Info("tiered storage eviction cache ratio configured",
		zap.Float64("tieredEvictableMemoryCacheRatio", tieredEvictableMemoryCacheRatio),
		zap.Float64("tieredEvictableDiskCacheRatio", tieredEvictableDiskCacheRatio),
	)

	return nil
}

func InitDiskFileWriterConfig(params *paramtable.ComponentParam) error {
	mode := params.CommonCfg.DiskWriteMode.GetValue()
	bufferSize := params.CommonCfg.DiskWriteBufferSizeKb.GetAsUint64()
	numThreads := params.CommonCfg.DiskWriteNumThreads.GetAsInt()
	refillPeriodUs := params.CommonCfg.DiskWriteRateLimiterRefillPeriodUs.GetAsInt64()
	maxBurstKBps := params.CommonCfg.DiskWriteRateLimiterMaxBurstKBps.GetAsInt64()
	avgKBps := params.CommonCfg.DiskWriteRateLimiterAvgKBps.GetAsInt64()
	highPriorityRatio := params.CommonCfg.DiskWriteRateLimiterHighPriorityRatio.GetAsInt()
	middlePriorityRatio := params.CommonCfg.DiskWriteRateLimiterMiddlePriorityRatio.GetAsInt()
	lowPriorityRatio := params.CommonCfg.DiskWriteRateLimiterLowPriorityRatio.GetAsInt()
	cMode := C.CString(mode)
	cBufferSize := C.uint64_t(bufferSize)
	cNumThreads := C.int(numThreads)
	defer C.free(unsafe.Pointer(cMode))
	diskWriteRateLimiterConfig := C.CDiskWriteRateLimiterConfig{
		refill_period_us:      C.int64_t(refillPeriodUs),
		avg_bps:               C.int64_t(avgKBps * 1024),
		max_burst_bps:         C.int64_t(maxBurstKBps * 1024),
		high_priority_ratio:   C.int32_t(highPriorityRatio),
		middle_priority_ratio: C.int32_t(middlePriorityRatio),
		low_priority_ratio:    C.int32_t(lowPriorityRatio),
	}
	diskWriteConfig := C.CDiskWriteConfig{
		mode:                cMode,
		buffer_size_kb:      cBufferSize,
		nr_threads:          cNumThreads,
		rate_limiter_config: diskWriteRateLimiterConfig,
	}
	status := C.InitDiskFileWriterConfig(diskWriteConfig)
	return HandleCStatus(&status, "InitDiskFileWriterConfig failed")
}

var coreParamCallbackInitOnce sync.Once

func SetupCoreConfigChangelCallback() {
	coreParamCallbackInitOnce.Do(func() {
		paramtable.Get().CommonCfg.IndexSliceSize.RegisterCallback(func(ctx context.Context, key, oldValue, newValue string) error {
			size, err := strconv.Atoi(newValue)
			if err != nil {
				return err
			}
			UpdateIndexSliceSize(size)
			return nil
		})

		paramtable.Get().QueryNodeCfg.KnowhereThreadPoolSize.RegisterCallback(func(ctx context.Context, key, oldValue, newValue string) error {
			factor, err := strconv.ParseFloat(newValue, 64)
			if err != nil {
				return err
			}
			if factor <= 0 || !paramtable.Get().QueryNodeCfg.EnableDisk.GetAsBool() {
				factor = 1
			} else if factor > 32 {
				factor = 32
			}
			knowhereThreadPoolSize := uint32(float64(hardware.GetCPUNum()) * factor)
			log.Info("UpdateKnowhereThreadPoolSize", zap.Uint32("knowhereThreadPoolSize", knowhereThreadPoolSize))
			C.SegcoreSetKnowhereSearchThreadPoolNum(C.uint32_t(knowhereThreadPoolSize))
			return nil
		})

		paramtable.Get().QueryNodeCfg.KnowhereFetchThreadPoolSize.RegisterCallback(func(ctx context.Context, key, oldValue, newValue string) error {
			factor, err := strconv.ParseFloat(newValue, 64)
			if err != nil {
				return err
			}
			if factor <= 0 {
				factor = 1
			} else if factor > 32 {
				factor = 32
			}
			knowhereFetchThreadPoolSize := uint32(float64(hardware.GetCPUNum()) * factor)
			log.Info("UpdateKnowhereFetchThreadPoolSize", zap.Uint32("knowhereFetchThreadPoolSize", knowhereFetchThreadPoolSize))
			C.SegcoreSetKnowhereFetchThreadPoolNum(C.uint32_t(knowhereFetchThreadPoolSize))
			return nil
		})

		paramtable.Get().CommonCfg.HighPriorityThreadCoreCoefficient.RegisterCallback(func(ctx context.Context, key, oldValue, newValue string) error {
			coefficient, err := strconv.ParseFloat(newValue, 64)
			if err != nil {
				return err
			}
			UpdateHighPriorityThreadCoreCoefficient(coefficient)
			return nil
		})

		paramtable.Get().CommonCfg.MiddlePriorityThreadCoreCoefficient.RegisterCallback(func(ctx context.Context, key, oldValue, newValue string) error {
			coefficient, err := strconv.ParseFloat(newValue, 64)
			if err != nil {
				return err
			}
			UpdateMiddlePriorityThreadCoreCoefficient(coefficient)
			return nil
		})

		paramtable.Get().CommonCfg.LowPriorityThreadCoreCoefficient.RegisterCallback(func(ctx context.Context, key, oldValue, newValue string) error {
			coefficient, err := strconv.ParseFloat(newValue, 64)
			if err != nil {
				return err
			}
			UpdateLowPriorityThreadCoreCoefficient(coefficient)
			return nil
		})

		paramtable.Get().CommonCfg.EnabledOptimizeExpr.RegisterCallback(func(ctx context.Context, key, oldValue, newValue string) error {
			enable, err := strconv.ParseBool(newValue)
			if err != nil {
				return err
			}
			UpdateDefaultOptimizeExprEnable(enable)
			return nil
		})

		paramtable.Get().CommonCfg.EnabledGrowingSegmentJSONKeyStats.RegisterCallback(func(ctx context.Context, key, oldValue, newValue string) error {
			enable, err := strconv.ParseBool(newValue)
			if err != nil {
				return err
			}
			UpdateDefaultGrowingJSONKeyStatsEnable(enable)
			return nil
		})

		paramtable.Get().CommonCfg.EnableConfigParamTypeCheck.RegisterCallback(func(ctx context.Context, key, oldValue, newValue string) error {
			enable, err := strconv.ParseBool(newValue)
			if err != nil {
				return err
			}
			UpdateDefaultConfigParamTypeCheck(enable)
			return nil
		})

		paramtable.Get().LogCfg.Level.RegisterCallback(func(ctx context.Context, key, oldValue, newValue string) error {
			return UpdateLogLevel(newValue)
		})

		paramtable.Get().QueryNodeCfg.ExprEvalBatchSize.RegisterCallback(func(ctx context.Context, key, oldValue, newValue string) error {
			size, err := strconv.Atoi(newValue)
			if err != nil {
				return err
			}
			UpdateDefaultExprEvalBatchSize(size)
			return nil
		})

		paramtable.Get().QueryNodeCfg.DeleteDumpBatchSize.RegisterCallback(func(ctx context.Context, key, oldValue, newValue string) error {
			size, err := strconv.Atoi(newValue)
			if err != nil {
				return err
			}
			UpdateDefaultDeleteDumpBatchSize(size)
			return nil
		})

		paramtable.Get().QueryNodeCfg.ExprResCacheEnabled.RegisterCallback(func(ctx context.Context, key, oldValue, newValue string) error {
			enable, err := strconv.ParseBool(newValue)
			if err != nil {
				return err
			}
			UpdateExprResCacheEnable(enable)
			return nil
		})

		paramtable.Get().QueryNodeCfg.ExprResCacheCapacityBytes.RegisterCallback(func(ctx context.Context, key, oldValue, newValue string) error {
			capacity, err := strconv.Atoi(newValue)
			if err != nil {
				return err
			}
			UpdateExprResCacheCapacityBytes(capacity)
			return nil
		})
	})
}

func InitInterminIndexConfig(params *paramtable.ComponentParam) error {
	enableInterminIndex := C.bool(params.QueryNodeCfg.EnableInterminSegmentIndex.GetAsBool())
	C.SegcoreSetEnableInterminSegmentIndex(enableInterminIndex)

	nlist := C.int64_t(params.QueryNodeCfg.InterimIndexNlist.GetAsInt64())
	C.SegcoreSetNlist(nlist)

	nprobe := C.int64_t(params.QueryNodeCfg.InterimIndexNProbe.GetAsInt64())
	C.SegcoreSetNprobe(nprobe)

	subDim := C.int64_t(params.QueryNodeCfg.InterimIndexSubDim.GetAsInt64())
	C.SegcoreSetSubDim(subDim)

	refineRatio := C.float(params.QueryNodeCfg.InterimIndexRefineRatio.GetAsFloat())
	C.SegcoreSetRefineRatio(refineRatio)

	indexBuildRatio := C.float(params.QueryNodeCfg.InterimIndexBuildRatio.GetAsFloat())
	C.SegcoreSetIndexBuildRatio(indexBuildRatio)

	denseVecIndexType := C.CString(params.QueryNodeCfg.DenseVectorInterminIndexType.GetValue())
	defer C.free(unsafe.Pointer(denseVecIndexType))
	status := C.SegcoreSetDenseVectorInterminIndexType(denseVecIndexType)
	statErr := HandleCStatus(&status, "InitInterminIndexConfig failed")
	if statErr != nil {
		return statErr
	}

	refineWithQuantFlag := C.bool(params.QueryNodeCfg.InterimIndexRefineWithQuant.GetAsBool())
	C.SegcoreSetDenseVectorInterminIndexRefineWithQuantFlag(refineWithQuantFlag)

	denseVecIndexRefineQuantType := C.CString(params.QueryNodeCfg.InterimIndexRefineQuantType.GetValue())
	defer C.free(unsafe.Pointer(denseVecIndexRefineQuantType))
	status = C.SegcoreSetDenseVectorInterminIndexRefineQuantType(denseVecIndexRefineQuantType)
	return HandleCStatus(&status, "InitInterminIndexConfig failed")
}

func InitGeometryCache(params *paramtable.ComponentParam) error {
	enableGeometryCache := C.bool(params.QueryNodeCfg.EnableGeometryCache.GetAsBool())
	C.SegcoreSetEnableGeometryCache(enableGeometryCache)
	return nil
}

func CleanRemoteChunkManager() {
	C.CleanRemoteChunkManagerSingleton()
}

func CleanGlogManager() {
	C.SegcoreCloseGlog()
}

// HandleCStatus deals with the error returned from CGO
func HandleCStatus(status *C.CStatus, extraInfo string) error {
	if status.error_code == 0 {
		return nil
	}
	errorCode := status.error_code
	errorName, ok := commonpb.ErrorCode_name[int32(errorCode)]
	if !ok {
		errorName = "UnknownError"
	}
	errorMsg := C.GoString(status.error_msg)
	defer C.free(unsafe.Pointer(status.error_msg))

	finalMsg := fmt.Sprintf("[%s] %s", errorName, errorMsg)
	logMsg := fmt.Sprintf("%s, C Runtime Exception: %s\n", extraInfo, finalMsg)
	log.Warn(logMsg)
	return errors.New(finalMsg)
}

func serializeHeaders(headerstr string) string {
	if len(headerstr) == 0 {
		return ""
	}
	decodeheaders, err := base64.StdEncoding.DecodeString(headerstr)
	if err != nil {
		return headerstr
	}
	return string(decodeheaders)
}

func InitPluginLoader() error {
	if hookutil.IsClusterEncyptionEnabled() {
		cSoPath := C.CString(paramtable.GetCipherParams().SoPathCpp.GetValue())
		log.Info("Init PluginLoader", zap.String("soPath", paramtable.GetCipherParams().SoPathCpp.GetValue()))
		defer C.free(unsafe.Pointer(cSoPath))
		status := C.InitPluginLoader(cSoPath)
		return HandleCStatus(&status, "InitPluginLoader failed")
	}
	return nil
}

func CleanPluginLoader() {
	C.CleanPluginLoader()
}
