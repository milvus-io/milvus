
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

#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>

#include "common/type_c.h"

typedef void* CLoadFieldDataInfo;

CStatus
NewLoadFieldDataInfo(CLoadFieldDataInfo* c_load_field_data_info,
                     int64_t storage_version);

void
DeleteLoadFieldDataInfo(CLoadFieldDataInfo c_load_field_data_info);

CStatus
AppendLoadFieldInfo(CLoadFieldDataInfo c_load_field_data_info,
                    int64_t field_id,
                    int64_t row_count);

CStatus
SetLoadFieldInfoChildFields(CLoadFieldDataInfo c_load_field_data_info,
                            int64_t field_id,
                            const int64_t* child_field_ids,
                            const int64_t child_field_num);

CStatus
AppendLoadFieldDataPath(CLoadFieldDataInfo c_load_field_data_info,
                        int64_t field_id,
                        int64_t entries_num,
                        int64_t memory_size,
                        const char* file_path);

void
AppendWarmupPolicy(CLoadFieldDataInfo c_load_field_data_info,
                   CacheWarmupPolicy warmup_policy);

void
SetStorageVersion(CLoadFieldDataInfo c_load_field_data_info,
                  int64_t storage_version);

void
EnableMmap(CLoadFieldDataInfo c_load_field_data_info,
           int64_t field_id,
           bool enabled);

void
SetLoadPriority(CLoadFieldDataInfo c_load_field_data_info, int32_t priority);

#ifdef __cplusplus
}
#endif
