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

#include <simdjson.h>
#include <string>
#include <string_view>
#include <type_traits>

#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/JsonCastType.h"
#include "common/JsonUtils.h"
#include "folly/FBVector.h"
#include "index/JsonIndexBuilder.h"
#include "pb/schema.pb.h"
#include "simdjson/dom/array.h"
#include "simdjson/dom/element.h"
#include "simdjson/error.h"

namespace milvus::index {

template <typename T>
void
ProcessJsonFieldData(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    JsonCastFunction cast_function,
    JsonDataAdder<T> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder) {
    using SIMDJSON_T =
        std::conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;

    auto tokens = parse_json_pointer(nested_path);
    int64_t row_id = 0;
    folly::fbvector<T> values;

    for (const auto& data : field_datas) {
        auto n = data->get_num_rows();
        for (int64_t i = 0; i < n; i++) {
            auto json_column = static_cast<const Json*>(data->RawValue(i));
            if (schema.nullable() && !data->is_valid(i)) {
                non_exist_adder(row_id);
                null_adder(row_id);
                data_adder(nullptr, 0, row_id++);
                continue;
            }

            auto exists = path_exists(json_column->dom_doc(), tokens);
            if (!exists || !json_column->exist(nested_path)) {
                error_recorder(
                    *json_column, nested_path, simdjson::NO_SUCH_FIELD);
                non_exist_adder(row_id);
                data_adder(nullptr, 0, row_id++);
                continue;
            }

            values.clear();
            if (cast_function.match<T>()) {
                auto res = JsonCastFunction::CastJsonValue<T>(
                    cast_function, *json_column, nested_path);
                if (res.has_value()) {
                    values.push_back(res.value());
                }
            } else {
                value_result<SIMDJSON_T> res =
                    json_column->at<SIMDJSON_T>(nested_path);
                if (res.error() != simdjson::SUCCESS) {
                    error_recorder(*json_column, nested_path, res.error());
                } else {
                    values.push_back(static_cast<T>(res.value()));
                }
            }
            data_adder(values.data(), values.size(), row_id++);
        }
    }
}

template <typename T>
void
ProcessJsonFieldArrayData(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    JsonDataAdder<T> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder) {
    using SIMDJSON_T =
        std::conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;

    auto tokens = parse_json_pointer(nested_path);
    int64_t row_id = 0;
    // Running count of elements emitted so far across all rows. Passed to
    // data_adder as the starting doc-id of the current row's element slice
    // (size 0 on null / missing-path rows leaves elem_start unchanged).
    int64_t elem_start = 0;
    folly::fbvector<T> values;

    for (const auto& data : field_datas) {
        auto n = data->get_num_rows();
        for (int64_t i = 0; i < n; i++) {
            auto json_column = static_cast<const Json*>(data->RawValue(i));
            if (schema.nullable() && !data->is_valid(i)) {
                non_exist_adder(row_id);
                null_adder(row_id);
                data_adder(nullptr, 0, elem_start);
                ++row_id;
                continue;
            }

            auto exists = path_exists(json_column->dom_doc(), tokens);
            if (!exists || !json_column->exist(nested_path)) {
                error_recorder(
                    *json_column, nested_path, simdjson::NO_SUCH_FIELD);
                non_exist_adder(row_id);
                data_adder(nullptr, 0, elem_start);
                ++row_id;
                continue;
            }

            values.clear();
            auto doc = json_column->dom_doc();
            auto array_res = doc.at_pointer(nested_path).get_array();
            if (array_res.error() != simdjson::SUCCESS) {
                error_recorder(*json_column, nested_path, array_res.error());
            } else {
                for (auto value : array_res.value()) {
                    auto val = value.template get<SIMDJSON_T>();
                    if (val.error() == simdjson::SUCCESS) {
                        values.push_back(static_cast<T>(val.value()));
                    }
                }
            }
            data_adder(values.data(), values.size(), elem_start);
            elem_start += static_cast<int64_t>(values.size());
            ++row_id;
        }
    }
}

template void
ProcessJsonFieldData<bool>(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    JsonCastFunction cast_function,
    JsonDataAdder<bool> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder);

template void
ProcessJsonFieldData<int64_t>(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    JsonCastFunction cast_function,
    JsonDataAdder<int64_t> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder);

template void
ProcessJsonFieldData<double>(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    JsonCastFunction cast_function,
    JsonDataAdder<double> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder);

template void
ProcessJsonFieldData<std::string>(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    JsonCastFunction cast_function,
    JsonDataAdder<std::string> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder);

template void
ProcessJsonFieldArrayData<bool>(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    JsonDataAdder<bool> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder);

template void
ProcessJsonFieldArrayData<int64_t>(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    JsonDataAdder<int64_t> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder);

template void
ProcessJsonFieldArrayData<double>(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    JsonDataAdder<double> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder);

template void
ProcessJsonFieldArrayData<std::string>(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    JsonDataAdder<std::string> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder);

}  // namespace milvus::index
