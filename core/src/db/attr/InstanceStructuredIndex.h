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

#include <src/db/meta/Meta.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/meta/MetaTypes.h"
#include "knowhere/index/structured_index/StructuredIndexSort.h"
#include "utils/Status.h"

namespace milvus {
namespace Attr {

class InstanceStructuredIndex {
 public:
    static Status
    CreateStructuredIndex(const std::string& collection_id, const engine::meta::MetaPtr meta_ptr);

    static Status
    GenStructuredIndex(const std::string& collection_id, const std::vector<std::string>& field_names,
                       const std::unordered_map<std::string, engine::meta::hybrid::DataType>& attr_types,
                       const std::unordered_map<std::string, std::vector<uint8_t>>& attr_datas,
                       std::unordered_map<std::string, int64_t>& attr_sizes,
                       std::unordered_map<std::string, knowhere::IndexPtr>& attr_indexes);

    static Status
    SerializeStructuredIndex(const engine::meta::SegmentSchema& segment_schema,
                             const std::unordered_map<std::string, knowhere::IndexPtr>& attr_indexes,
                             const std::unordered_map<std::string, int64_t>& attr_sizes,
                             const std::unordered_map<std::string, engine::meta::hybrid::DataType>& attr_types);
};

}  // namespace Attr
}  // namespace milvus
