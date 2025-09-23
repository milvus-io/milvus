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
#include <vector>

#include "Types.h"

// NOTE: field_id can be system field
// NOTE: Refer to common/SystemProperty.cpp for details
struct FieldBinlogInfo {
    int64_t field_id;
    int64_t row_count = -1;
    std::vector<int64_t> entries_nums;
    // estimated memory size for each binlog file, in bytes, used by caching layer
    std::vector<int64_t> memory_sizes;
    bool enable_mmap{false};
    std::vector<std::string> insert_files;
    std::vector<int64_t> child_field_ids;
};

struct LoadFieldDataInfo {
    std::map<int64_t, FieldBinlogInfo> field_infos;
    int64_t storage_version = 0;
    milvus::proto::common::LoadPriority load_priority =
        milvus::proto::common::LoadPriority::HIGH;
    CacheWarmupPolicy warmup_policy =
        CacheWarmupPolicy::CacheWarmupPolicy_Disable;
    std::vector<int64_t> child_field_ids;
};

struct LoadDeletedRecordInfo {
    const void* timestamps = nullptr;
    const milvus::IdArray* primary_keys = nullptr;
    int64_t row_count = -1;
};
