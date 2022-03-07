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

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "common/type_c.h"
#include "common/vector_index_c.h"
#include "indexbuilder/type_c.h"

// TODO: how could we pass map between go and c++ more efficiently?
// Solution: using Protobuf instead of JSON, this way significantly increase programming efficiency

CStatus
CreateIndex(const char* serialized_type_params, const char* serialized_index_params, CIndex* res_index);

void
DeleteIndex(CIndex index);

CStatus
BuildFloatVecIndexWithoutIds(CIndex index, int64_t float_value_num, const float* vectors);

CStatus
BuildBinaryVecIndexWithoutIds(CIndex index, int64_t data_size, const uint8_t* vectors);

CStatus
SerializeToSlicedBuffer(CIndex index, CBinary* c_binary);

CStatus
SerializeToBinarySet(CIndex index, CBinarySet* c_binary_set);

int64_t
GetCBinarySize(CBinary c_binary);

// Note: the memory of data is allocated outside
void
GetCBinaryData(CBinary c_binary, void* data);

void
DeleteCBinary(CBinary c_binary);

CStatus
LoadFromSlicedBuffer(CIndex index, const char* serialized_sliced_blob_buffer, int32_t size);

CStatus
LoadFromBinarySet(CIndex index, CBinarySet c_binary_set);

CStatus
QueryOnFloatVecIndex(CIndex index, int64_t float_value_num, const float* vectors, CIndexQueryResult* res);

CStatus
QueryOnFloatVecIndexWithParam(CIndex index,
                              int64_t float_value_num,
                              const float* vectors,
                              const char* serialized_search_params,
                              CIndexQueryResult* res);

CStatus
QueryOnBinaryVecIndex(CIndex index, int64_t data_size, const uint8_t* vectors, CIndexQueryResult* res);

CStatus
QueryOnBinaryVecIndexWithParam(CIndex index,
                               int64_t data_size,
                               const uint8_t* vectors,
                               const char* serialized_search_params,
                               CIndexQueryResult* res);

CStatus
CreateQueryResult(CIndexQueryResult* res);

int64_t
NqOfQueryResult(CIndexQueryResult res);

int64_t
TopkOfQueryResult(CIndexQueryResult res);

void
GetIdsOfQueryResult(CIndexQueryResult res, int64_t* ids);

void
GetDistancesOfQueryResult(CIndexQueryResult res, float* distances);

CStatus
DeleteIndexQueryResult(CIndexQueryResult res);

void
DeleteByteArray(const char* array);

#ifdef __cplusplus
};
#endif
