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

#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "common/Types.h"
#include "common/type_c.h"
#include "index/Index.h"
#include "storage/Types.h"

namespace milvus::segcore {

// see also internal/querynode/load_index_info.go
struct LoadIndexInfo {
    int64_t collection_id;
    int64_t partition_id;
    int64_t segment_id;
    int64_t field_id;
    DataType field_type;
    int64_t index_id;
    int64_t index_build_id;
    int64_t index_version;
    std::map<std::string, std::string> index_params;
    std::vector<std::string> index_files;
    index::IndexBasePtr index;
};

}  // namespace milvus::segcore
