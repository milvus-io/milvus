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
#include <string>
#include <map>

#include "knowhere/index/vector_index/VecIndex.h"

struct LoadIndexInfo {
    std::string field_name;
    int64_t field_id;
    std::map<std::string, std::string> index_params;
    milvus::knowhere::VecIndexPtr index;
};

struct LoadFieldDataInfo {
    int64_t field_id;
    void* blob;
    int64_t row_count;
};
