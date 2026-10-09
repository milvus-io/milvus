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
#include "common/FastMem.h"

#include <simdjson.h>
#include <string.h>

#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/JsonUtils.h"
#include "pb/schema.pb.h"
#include "simdjson/dom/element.h"
#include "simdjson/error.h"
#include "simdjson/padded_string.h"

namespace milvus::index {
namespace {

bool
ContainsNegativeZero(simdjson::dom::element value) {
    if (value.is_double()) {
        const auto number = value.get_double().value();
        return number == 0.0 && std::signbit(number);
    }
    if (value.is_array()) {
        auto array = value.get_array().value();
        for (auto element : array) {
            if (ContainsNegativeZero(element)) {
                return true;
            }
        }
    } else if (value.is_object()) {
        auto object = value.get_object().value();
        for (auto field : object) {
            if (ContainsNegativeZero(field.value)) {
                return true;
            }
        }
    }
    return false;
}

}  // namespace

void
JsonFlatIndex::build_index_for_json(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas) {
    int64_t offset = 0;
    auto tokens = parse_json_pointer(nested_path_);
    // Scratch buffer for nested JSON serialization - reused across iterations
    // to avoid repeated heap allocations
    simdjson::padded_string scratch_buffer(256);
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
                wrapper_->add_json_array_data(nullptr, 0, offset++);
                continue;
            }

            if (!supports_nan_total_order_) {
                auto indexed_value = json->dom_doc().at_pointer(nested_path_);
                if (!indexed_value.error() &&
                    ContainsNegativeZero(indexed_value.value())) {
                    ThrowInfo(Unsupported,
                              "JSON flat negative zero requires scalar index "
                              "version 6 for canonical floating-point keys");
                }
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
                    auto str_result = simdjson::to_json_string(res.value());
                    if (str_result.error() != simdjson::SUCCESS) {
                        wrapper_->add_json_array_data(nullptr, 0, offset++);
                        continue;
                    }
                    std::string_view str = str_result.value();
                    // Resize scratch buffer if needed (with some growth factor)
                    // Need space for str.size() + 1 for null terminator
                    if (scratch_buffer.size() < str.size() + 1) {
                        scratch_buffer =
                            simdjson::padded_string((str.size() + 1) * 2);
                    }
                    milvus::fastmem::FastMemcpy(
                        scratch_buffer.data(), str.data(), str.size());
                    // Add null terminator - required for C string FFI to Rust
                    scratch_buffer.data()[str.size()] = '\0';
                    // Create Json referencing scratch buffer (non-owning)
                    Json subpath_json(scratch_buffer.data(), str.size());
                    wrapper_->add_json_data(&subpath_json, 1, offset++);
                }
            }
        }
    }
}
}  // namespace milvus::index
