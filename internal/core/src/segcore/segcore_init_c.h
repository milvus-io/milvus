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

#ifdef __cplusplus
}
#endif
