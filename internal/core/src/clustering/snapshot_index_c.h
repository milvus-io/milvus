// Copyright (C) 2019-2026 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once
#include <stdint.h>
#include <stddef.h>
#include "common/type_c.h"
#include "common/protobuf_utils_c.h"
#include "segcore/load_index_c.h"
#ifdef __cplusplus
extern "C" {
#endif
CStatus
BuildSnapshotIndex(ProtoLayoutInterface result,
                   const uint8_t* build_info,
                   uint64_t build_size,
                   const uint8_t* centroids,
                   uint64_t centroid_size,
                   const char* prefix);
CStatus
SearchSnapshotIndex(CLoadIndexInfo index,
                    const uint8_t* queries,
                    uint64_t query_bytes,
                    int64_t nq,
                    int64_t topk,
                    const char* search_params,
                    int64_t* labels,
                    float* distances);
#ifdef __cplusplus
}
#endif
