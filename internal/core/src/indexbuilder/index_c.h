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

CStatus
CreateIndex(enum CDataType dtype,
            const char* serialized_type_params,
            const char* serialized_index_params,
            CIndex* res_index);

CStatus
DeleteIndex(CIndex index);

CStatus
BuildFloatVecIndex(CIndex index, int64_t float_value_num, const float* vectors);

CStatus
BuildBinaryVecIndex(CIndex index, int64_t data_size, const uint8_t* vectors);

// field_data:
//  1, serialized proto::schema::BoolArray, if type is bool;
//  2, serialized proto::schema::StringArray, if type is string;
//  3, raw pointer, if type is of fundamental except bool type;
// TODO: optimize here if necessary.
CStatus
BuildScalarIndex(CIndex c_index, int64_t size, const void* field_data);

CStatus
SerializeIndexToBinarySet(CIndex index, CBinarySet* c_binary_set);

CStatus
LoadIndexFromBinarySet(CIndex index, CBinarySet c_binary_set);

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

#ifdef __cplusplus
};
#endif
