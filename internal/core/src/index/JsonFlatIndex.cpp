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

#include "index/JsonFlatIndex.h"

#include <simdjson.h>
#include <cstddef>
#include <string>
#include <vector>

#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/JsonUtils.h"
#include "pb/schema.pb.h"
#include "simdjson/dom/element.h"
#include "simdjson/error.h"

namespace milvus::index {

void
JsonFlatIndex::build_index_for_json(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas) {
    int64_t offset = 0;
    auto tokens = parse_json_pointer(nested_path_);
    constexpr size_t kBatchRows = 4096;
    constexpr size_t kBatchBytes = 8 * 1024 * 1024;
    std::vector<std::string> values;
    std::vector<uintptr_t> row_offsets{0};
    std::vector<int64_t> doc_ids;
    size_t value_bytes = 0;
    row_offsets.reserve(kBatchRows + 1);
    doc_ids.reserve(kBatchRows);

    auto flush = [&]() {
        if (doc_ids.empty()) {
            return;
        }
        wrapper_->add_json_rows(values, row_offsets, doc_ids);
        values.clear();
        row_offsets.assign(1, 0);
        doc_ids.clear();
        value_bytes = 0;
    };

    for (const auto& data : field_datas) {
        auto n = data->get_num_rows();
        for (int i = 0; i < n; i++) {
            auto value_count_before = values.size();
            if (schema_.nullable() && !data->is_valid(i)) {
                null_offset_.push_back(offset);
            } else {
                auto json = static_cast<const Json*>(data->RawValue(i));
                auto exists = path_exists(json->dom_doc(), tokens);
                if (exists && json->exist(nested_path_)) {
                    if (nested_path_.empty()) {
                        values.emplace_back(json->data());
                    } else {
                        auto res = json->doc().at_pointer(nested_path_);
                        if (res.error() == simdjson::SUCCESS) {
                            auto serialized =
                                simdjson::to_json_string(res.value());
                            if (serialized.error() == simdjson::SUCCESS) {
                                values.emplace_back(serialized.value());
                            }
                        }
                    }
                }
            }
            if (values.size() > value_count_before) {
                value_bytes += values.back().size();
            }
            row_offsets.push_back(values.size());
            doc_ids.push_back(offset++);
            if (doc_ids.size() >= kBatchRows || value_bytes >= kBatchBytes) {
                flush();
            }
        }
    }
    flush();
}
}  // namespace milvus::index
