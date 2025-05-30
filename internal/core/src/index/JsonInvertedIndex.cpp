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
#include "folly/FBVector.h"
#include "log/Log.h"
#include "common/JsonUtils.h"
#include "simdjson/error.h"

namespace milvus::index {

template <typename T>
void
JsonInvertedIndex<T>::build_index_for_json(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas) {
    int64_t offset = 0;
    LOG_INFO("Start to build json inverted index for field: {}", nested_path_);
    using SIMDJSON_T =
        std::conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;

    auto tokens = parse_json_pointer(nested_path_);

    bool is_array = cast_type_.data_type() == JsonCastType::DataType::ARRAY;

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
            if (!exists || !json_column->exist(nested_path_)) {
                error_recorder_.Record(
                    *json_column, nested_path_, simdjson::NO_SUCH_FIELD);
                this->null_offset_.push_back(offset);
                this->wrapper_->template add_array_data<T>(
                    nullptr, 0, offset++);
                continue;
            }
            folly::fbvector<T> values;
            if (is_array) {
                auto doc = json_column->dom_doc();
                auto array_res = doc.at_pointer(nested_path_).get_array();
                if (array_res.error() != simdjson::SUCCESS) {
                    error_recorder_.Record(
                        *json_column, nested_path_, array_res.error());
                } else {
                    auto array_values = array_res.value();
                    for (auto value : array_values) {
                        auto val = value.template get<SIMDJSON_T>();

                        if (val.error() == simdjson::SUCCESS) {
                            values.push_back(static_cast<T>(val.value()));
                        }
                    }
                }
            } else {
                if (cast_function_.match<T>()) {
                    auto res = JsonCastFunction::CastJsonValue<T>(
                        cast_function_, *json_column, nested_path_);
                    if (res.has_value()) {
                        values.push_back(res.value());
                    }
                } else {
                    value_result<SIMDJSON_T> res =
                        json_column->at<SIMDJSON_T>(nested_path_);
                    if (res.error() != simdjson::SUCCESS) {
                        error_recorder_.Record(
                            *json_column, nested_path_, res.error());
                    } else {
                        values.push_back(static_cast<T>(res.value()));
                    }
                }
            }
            this->wrapper_->template add_array_data<T>(
                values.data(), values.size(), offset++);
        }
    }

    error_recorder_.PrintErrStats();
}

template class JsonInvertedIndex<bool>;
template class JsonInvertedIndex<int64_t>;
template class JsonInvertedIndex<double>;
template class JsonInvertedIndex<std::string>;

}  // namespace milvus::index