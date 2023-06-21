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

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#include "common/binary_set_c.h"
#include "common/type_c.h"
#include "segcore/collection_c.h"

typedef void* CLoadIndexInfo;

CStatus
NewLoadIndexInfo(CLoadIndexInfo* c_load_index_info);

void
DeleteLoadIndexInfo(CLoadIndexInfo c_load_index_info);

CStatus
AppendIndexParam(CLoadIndexInfo c_load_index_info,
                 const char* index_key,
                 const char* index_value);

CStatus
AppendFieldInfo(CLoadIndexInfo c_load_index_info,
                int64_t collection_id,
                int64_t partition_id,
                int64_t segment_id,
                int64_t field_id,
                enum CDataType field_type);

CStatus
AppendIndexInfo(CLoadIndexInfo c_load_index_info,
                int64_t index_id,
                int64_t build_id,
                int64_t version);

CStatus
AppendIndex(CLoadIndexInfo c_load_index_info, CBinarySet c_binary_set);

CStatus
AppendIndexFilePath(CLoadIndexInfo c_load_index_info, const char* file_path);

CStatus
AppendIndexV2(CLoadIndexInfo c_load_index_info);

CStatus
CleanLoadedIndex(CLoadIndexInfo c_load_index_info);

#ifdef __cplusplus
}
#endif
