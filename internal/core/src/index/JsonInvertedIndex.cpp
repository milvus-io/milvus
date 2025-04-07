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

#include "index/JsonInvertedIndex.h"
#include <string>
#include <string_view>
#include <type_traits>
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/Types.h"
#include "log/Log.h"
#include "simdjson/error.h"

namespace milvus::index {

// Parse a JSON Pointer into unescaped path segments
std::vector<std::string>
parse_json_pointer(const std::string& pointer) {
    std::vector<std::string> tokens;
    if (pointer.empty())
        return tokens;  // Root path (entire document)
    if (pointer[0] != '/') {
        throw std::invalid_argument(
            "Invalid JSON Pointer: must start with '/'");
    }
    size_t start = 1;
    while (start < pointer.size()) {
        size_t end = pointer.find('/', start);
        if (end == std::string::npos)
            end = pointer.size();
        std::string token = pointer.substr(start, end - start);
        // Replace ~1 with / and ~0 with ~
        size_t pos = 0;
        while ((pos = token.find("~1", pos)) != std::string::npos) {
            token.replace(pos, 2, "/");
            pos += 1;  // Avoid infinite loops on overlapping replacements
        }
        pos = 0;
        while ((pos = token.find("~0", pos)) != std::string::npos) {
            token.replace(pos, 2, "~");
            pos += 1;
        }
        tokens.push_back(token);
        start = end + 1;
    }
    return tokens;
}

// Check if a JSON Pointer path exists
bool
path_exists(const simdjson::dom::element& root,
            const std::vector<std::string>& tokens) {
    simdjson::dom::element current = root;
    for (const auto& token : tokens) {
        if (current.type() == simdjson::dom::element_type::OBJECT) {
            auto obj = current.get_object();
            if (obj.error())
                return false;
            auto next = obj.value().at_key(token);
            if (next.error())
                return false;
            current = next.value();
        } else if (current.type() == simdjson::dom::element_type::ARRAY) {
            if (token == "-")
                return false;  // "-" is invalid for existence checks
            char* endptr;
            long index = strtol(token.c_str(), &endptr, 10);
            if (*endptr != '\0' || index < 0)
                return false;  // Not a valid index
            auto arr = current.get_array();
            if (arr.error())
                return false;
            if (static_cast<size_t>(index) >= arr.value().size())
                return false;
            auto next = arr.value().at(index);
            if (next.error())
                return false;
            current = next.value();
        } else {
            return false;  // Path cannot be resolved
        }
    }
    return true;
}
template <typename T>
void
JsonInvertedIndex<T>::build_index_for_json(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas) {
    using GetType =
        std::conditional_t<std::is_same_v<std::string, T>, std::string_view, T>;
    int64_t offset = 0;
    LOG_INFO("Start to build json inverted index for field: {}", nested_path_);

    auto tokens = parse_json_pointer(nested_path_);

    for (const auto& data : field_datas) {
        auto n = data->get_num_rows();
        for (int64_t i = 0; i < n; i++) {
            auto json_column = static_cast<const Json*>(data->RawValue(i));
            if (this->schema_.nullable() && !data->is_valid(i)) {
                {
                    folly::SharedMutex::WriteHolder lock(this->mutex_);
                    this->null_offset_.push_back(offset);
                }
                this->wrapper_->template add_array_data<T>(
                    nullptr, 0, offset++);
                continue;
            }

            auto exists = path_exists(json_column->dom_doc(), tokens);
            if (!exists ||
                nested_path_ != "" &&
                    json_column->doc().at_pointer(nested_path_).is_null()) {
                error_recorder_.Record(
                    *json_column, nested_path_, simdjson::NO_SUCH_FIELD);
                this->null_offset_.push_back(offset);
                this->wrapper_->template add_array_data<T>(
                    nullptr, 0, offset++);
                continue;
            }
            value_result<GetType> res = json_column->at<GetType>(nested_path_);
            auto err = res.error();
            if (err != simdjson::SUCCESS) {
                error_recorder_.Record(*json_column, nested_path_, err);
                this->wrapper_->template add_array_data<T>(
                    nullptr, 0, offset++);
                continue;
            }
            if constexpr (std::is_same_v<GetType, std::string_view>) {
                auto value = std::string(res.value());
                this->wrapper_->template add_data(&value, 1, offset++);
            } else {
                auto value = res.value();
                this->wrapper_->template add_data(&value, 1, offset++);
            }
        }
    }

    error_recorder_.PrintErrStats();
}

template class JsonInvertedIndex<bool>;
template class JsonInvertedIndex<int64_t>;
template class JsonInvertedIndex<double>;
template class JsonInvertedIndex<std::string>;

}  // namespace milvus::index