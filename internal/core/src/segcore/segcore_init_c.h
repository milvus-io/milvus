// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <stdbool.h>
#include <stdint.h>
#include "common/type_c.h"

#include "common/type_c.h"

#ifdef __cplusplus
extern "C" {
#endif

void
SegcoreInit(const char*);

void
SegcoreSetChunkRows(const int64_t);

void
SegcoreSetEnableInterminSegmentIndex(const bool);

void
SegcoreSetNlist(const int64_t);

void
SegcoreSetNprobe(const int64_t);

CStatus
SegcoreSetDenseVectorInterminIndexType(const char*);

void
SegcoreSetSubDim(const int64_t);

void
SegcoreSetRefineRatio(const float);

void
SegcoreSetIndexBuildRatio(const float);

void
SegcoreInterminDenseIndexType(const char*);

CStatus
SegcoreSetDenseVectorInterminIndexRefineQuantType(const char*);

void
SegcoreSetDenseVectorInterminIndexRefineWithQuantFlag(const bool);

// return value must be freed by the caller
char*
SegcoreSetSimdType(const char*);

void
SegcoreEnableKnowhereScoreConsistency();

void
SegcoreSetKnowhereBuildThreadPoolNum(const uint32_t num_threads);

void
SegcoreSetKnowhereSearchThreadPoolNum(const uint32_t num_threads);

void
SegcoreSetKnowhereFetchThreadPoolNum(const uint32_t num_threads);

void
SegcoreSetKnowhereGpuMemoryPoolSize(const uint32_t init_size,
                                    const uint32_t max_size);

void
SegcoreCloseGlog();

int32_t
GetCurrentIndexVersion();

int32_t
GetMinimalIndexVersion();

int32_t
GetMaximumIndexVersion();

void
SetThreadName(const char*);

void
ConfigureTieredStorage(
    // Cache warmup policies
    const CacheWarmupPolicy scalarFieldCacheWarmupPolicy,
    const CacheWarmupPolicy vectorFieldCacheWarmupPolicy,
    const CacheWarmupPolicy scalarIndexCacheWarmupPolicy,
    const CacheWarmupPolicy vectorIndexCacheWarmupPolicy,
    // watermarks
    const int64_t memory_low_watermark_bytes,
    const int64_t memory_high_watermark_bytes,
    const int64_t memory_max_bytes,
    const int64_t disk_low_watermark_bytes,
    const int64_t disk_high_watermark_bytes,
    const int64_t disk_max_bytes,
    // storage usage tracking enabled
    const bool storage_usage_tracking_enabled,
    // eviction enabled
    const bool eviction_enabled,
    // eviction configs
    const int64_t cache_touch_window_ms,
    const bool background_eviction_enabled,
    const int64_t eviction_interval_ms,
    const int64_t cache_cell_unaccessed_survival_time,
    const float overloaded_memory_threshold_percentage,
    const float loading_resource_factor,
    const float max_disk_usage_percentage,
    const char* disk_path);

#ifdef __cplusplus
}
#endif
