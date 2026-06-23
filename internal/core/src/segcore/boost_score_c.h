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

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdint.h>

#include "common/common_type_c.h"
#include "common/type_c.h"
#include "futures/future_c_types.h"
#include "segcore/plan_c.h"

struct ArrowArray;
struct ArrowSchema;

CStatus
ComputeScorerScoresOnOffsetChunks(CSegmentInterface segment,
                                  CSearchPlan plan,
                                  const void* serialized_score_function,
                                  int64_t serialized_score_function_size,
                                  struct ArrowArray* offset_chunks,
                                  struct ArrowSchema* offset_schemas,
                                  int64_t num_chunks,
                                  uint64_t timestamp,
                                  uint64_t collection_ttl,
                                  int32_t consistency_level,
                                  uint64_t entity_ttl_physical_time_us,
                                  float* const* output_score_chunks,
                                  bool* const* output_has_score_chunks);

CFuture*
AsyncComputeScorerScoresOnOffsetChunks(CSegmentInterface segment,
                                       CSearchPlan plan,
                                       const void* serialized_score_function,
                                       int64_t serialized_score_function_size,
                                       struct ArrowArray* offset_chunks,
                                       struct ArrowSchema* offset_schemas,
                                       int64_t num_chunks,
                                       uint64_t timestamp,
                                       uint64_t collection_ttl,
                                       int32_t consistency_level,
                                       uint64_t entity_ttl_physical_time_us,
                                       float* const* output_score_chunks,
                                       bool* const* output_has_score_chunks);

#ifdef __cplusplus
}
#endif
