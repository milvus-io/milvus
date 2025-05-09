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
#include "common/Types.h"
#include "index/InvertedIndexUtil.h"
#include "log/Log.h"
#include "simdjson/builtin.h"
#include "simdjson/padded_string.h"
#include "common/JsonUtils.h"
namespace milvus::index {

void
JsonFlatIndex::build_index_for_json(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas) {
    int64_t offset = 0;
    auto tokens = parse_json_pointer(nested_path_);
    for (const auto& data : field_datas) {
        auto n = data->get_num_rows();
        for (int i = 0; i < n; i++) {
            if (schema_.nullable() && !data->is_valid(i)) {
                null_offset_.push_back(offset);
                wrapper_->add_json_array_data(nullptr, 0, offset++);
                continue;
            }
            auto json = static_cast<const Json*>(data->RawValue(i));
            auto exists = path_exists(json->dom_doc(), tokens);
            if (!exists || !json->exist(nested_path_)) {
                null_offset_.push_back(offset);
                wrapper_->add_json_array_data(nullptr, 0, offset++);
                continue;
            }

            if (nested_path_ == "") {
                wrapper_->add_json_data(json, 1, offset++);
            } else {
                auto doc = json->doc();
                auto res = doc.at_pointer(nested_path_);
                auto err = res.error();
                if (err != simdjson::SUCCESS) {
                    wrapper_->add_json_array_data(nullptr, 0, offset++);
                } else {
                    auto str = simdjson::to_json_string(res.value()).value();
                    Json subpath_json = Json(simdjson::padded_string(str));
                    wrapper_->add_json_data(&subpath_json, 1, offset++);
                }
            }
        }
    }
}
}  // namespace milvus::index
