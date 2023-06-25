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
#include "common/binary_set_c.h"
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
CleanLocalData(CIndex index);

CStatus
NewBuildIndexInfo(CBuildIndexInfo* c_build_index_info,
                  CStorageConfig c_storage_config);

void
DeleteBuildIndexInfo(CBuildIndexInfo c_build_index_info);

CStatus
AppendBuildIndexParam(CBuildIndexInfo c_build_index_info,
                      const uint8_t* serialized_type_params,
                      const uint64_t len);

CStatus
AppendBuildTypeParam(CBuildIndexInfo c_build_index_info,
                     const uint8_t* serialized_type_params,
                     const uint64_t len);

CStatus
AppendFieldMetaInfo(CBuildIndexInfo c_build_index_info,
                    int64_t collection_id,
                    int64_t partition_id,
                    int64_t segment_id,
                    int64_t field_id,
                    enum CDataType field_type);

CStatus
AppendIndexMetaInfo(CBuildIndexInfo c_build_index_info,
                    int64_t index_id,
                    int64_t build_id,
                    int64_t version);

CStatus
AppendInsertFilePath(CBuildIndexInfo c_build_index_info, const char* file_path);

CStatus
CreateIndexV2(CIndex* res_index, CBuildIndexInfo c_build_index_info);

CStatus
SerializeIndexAndUpLoad(CIndex index, CBinarySet* c_binary_set);

#ifdef __cplusplus
};
#endif
