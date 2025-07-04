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
#include "common/protobuf_utils_c.h"
#include "common/binary_set_c.h"
#include "indexbuilder/type_c.h"

CStatus
CreateIndex(CIndex* res_index,
            const uint8_t* serialized_build_index_info,
            const uint64_t len);

CStatus
DeleteIndex(CIndex index);

CStatus
BuildJsonKeyIndex(ProtoLayoutInterface c_binary_set,
                  const uint8_t* serialized_build_index_info,
                  const uint64_t len);

CStatus
BuildTextIndex(ProtoLayoutInterface c_binary_set,
               const uint8_t* serialized_build_index_info,
               const uint64_t len);

CStatus
CleanLocalData(CIndex index);

CStatus
SerializeIndexAndUpLoad(CIndex index, ProtoLayoutInterface result);

// =========== Followings are used only in test ==========
CStatus
CreateIndexV0(enum CDataType dtype,
              const char* serialized_type_params,
              const char* serialized_index_params,
              CIndex* res_index);

CStatus
BuildFloatVecIndex(CIndex index, int64_t float_value_num, const float* vectors);

CStatus
BuildBinaryVecIndex(CIndex index, int64_t data_size, const uint8_t* vectors);

CStatus
BuildFloat16VecIndex(CIndex index, int64_t data_size, const uint8_t* vectors);

CStatus
BuildBFloat16VecIndex(CIndex index, int64_t data_size, const uint8_t* vectors);

CStatus
BuildSparseFloatVecIndex(CIndex index,
                         int64_t row_num,
                         int64_t dim,
                         const uint8_t* vectors);

CStatus
BuildInt8VecIndex(CIndex index, int64_t data_size, const int8_t* vectors);

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

#ifdef __cplusplus
};
#endif
