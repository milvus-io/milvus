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

#include <map>
#include <string>

#include "knowhere/index/vector_index/VecIndex.h"

struct LoadIndexInfo {
    int64_t field_id;
    std::map<std::string, std::string> index_params;
    milvus::knowhere::VecIndexPtr index;
};

// NOTE: field_id can be system field
// NOTE: Refer to common/SystemProperty.cpp for details
struct LoadFieldDataInfo {
    int64_t field_id;
    const void* blob = nullptr;
    int64_t row_count = -1;
};

struct LoadDeletedRecordInfo {
    const void* timestamps = nullptr;
    const void* primary_keys = nullptr;
    int64_t row_count = -1;
};
