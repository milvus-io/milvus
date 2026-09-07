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

#include <stdbool.h>
#include <stdint.h>

#include "common/type_c.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void* CXGBoostModel;

typedef struct CXGBoostLoadModelResult {
    CStatus status;
    CXGBoostModel model;
    int32_t num_features;
} CXGBoostLoadModelResult;

struct ArrowArray;
struct ArrowSchema;

typedef struct CXGBoostPredictRequest {
    CXGBoostModel model;
    const struct ArrowArray* feature_arrays;
    const struct ArrowSchema* feature_schemas;
    int32_t num_features;
    bool output_default;
    float* output;
} CXGBoostPredictRequest;

CXGBoostLoadModelResult
LoadXGBoostUBJModel(const char* path);

CStatus
PredictXGBoost(CXGBoostPredictRequest request);

CStatus
DeleteXGBoostModel(CXGBoostModel model);

#ifdef __cplusplus
}
#endif
