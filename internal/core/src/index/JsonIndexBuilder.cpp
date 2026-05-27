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

#include "common/FieldData.h"
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
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonDataAdder<T> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder) {
    int64_t offset = 0;
    using SIMDJSON_T =
        std::conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;

    auto tokens = parse_json_pointer(nested_path);

    bool is_array = cast_type.data_type() == JsonCastType::DataType::ARRAY;

    folly::fbvector<T> values;
    for (const auto& data : field_datas) {
        auto n = data->get_num_rows();
        for (int64_t i = 0; i < n; i++) {
            auto json_column = static_cast<const Json*>(data->RawValue(i));
            if (schema.nullable() && !data->is_valid(i)) {
                non_exist_adder(offset);
                null_adder(offset);
                data_adder(nullptr, 0, offset++);
                continue;
            }

            auto exists = path_exists(json_column->dom_doc(), tokens);
            if (!exists || !json_column->exist(nested_path)) {
                error_recorder(
                    *json_column, nested_path, simdjson::NO_SUCH_FIELD);
                non_exist_adder(offset);
                data_adder(nullptr, 0, offset++);
                continue;
            }

            values.clear();
            if (is_array) {
                auto doc = json_column->dom_doc();
                auto array_res = doc.at_pointer(nested_path).get_array();
                if (array_res.error() != simdjson::SUCCESS) {
                    error_recorder(
                        *json_column, nested_path, array_res.error());
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
            }

            data_adder(values.data(), values.size(), offset++);
        }
    }
}

template void
ProcessJsonFieldData<bool>(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
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
    const JsonCastType& cast_type,
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
    const JsonCastType& cast_type,
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
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonDataAdder<std::string> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder);

namespace {

template <typename T>
DataType
GetMilvusDataType() {
    if constexpr (std::is_same_v<T, bool>) {
        return DataType::BOOL;
    } else if constexpr (std::is_same_v<T, int64_t>) {
        return DataType::INT64;
    } else if constexpr (std::is_same_v<T, double>) {
        return DataType::DOUBLE;
    } else if constexpr (std::is_same_v<T, std::string>) {
        return DataType::VARCHAR;
    } else {
        static_assert(sizeof(T) == 0, "unsupported type for JSON conversion");
    }
}

}  // namespace

template <typename T>
JsonToTypedResult
ConvertJsonToTypedFieldData(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function) {
    int64_t total_rows = 0;
    for (const auto& data : json_field_datas) {
        total_rows += data->get_num_rows();
    }

    auto data_type = GetMilvusDataType<T>();
    auto field_data = std::make_shared<FieldData<T>>(
        data_type, /*nullable=*/true, total_rows);

    // Use FixedVector to avoid std::vector<bool> specialization
    FixedVector<T> values(total_rows);
    std::vector<uint8_t> valid_data((total_rows + 7) / 8, 0);
    std::vector<size_t> non_exist_offsets;

    ProcessJsonFieldData<T>(
        json_field_datas,
        schema,
        nested_path,
        cast_type,
        cast_function,
        [&values, &valid_data](const T* data, int64_t size, int64_t offset) {
            if (size > 0) {
                values[offset] = data[0];
                valid_data[offset / 8] |= (1 << (offset % 8));
            }
        },
        [](int64_t) {},
        // non_exist_adder: track offsets where the path truly doesn't exist
        [&non_exist_offsets](int64_t offset) {
            non_exist_offsets.push_back(offset);
        },
        [](const Json&, const std::string&, simdjson::error_code) {});

    const void* data_ptr = values.data();
    FieldDataBase* base_ptr = field_data.get();
    base_ptr->FillFieldData(
        data_ptr, valid_data.data(), (ssize_t)total_rows, (ssize_t)0);

    return JsonToTypedResult{
        .field_data = field_data,
        .non_exist_offsets = std::move(non_exist_offsets),
    };
}

template JsonToTypedResult
ConvertJsonToTypedFieldData<bool>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function);

// NOTE: int64_t instantiation is kept solely for ExprJsonIndexTest.cpp which
// uses JsonIndexTestFixture parameterized over <bool, int64_t, double,
// std::string>. JSON cast_type does NOT support INT64 — that test uses
// DOUBLE cast with int64 query values, and static_casts the resulting index
// to JsonInvertedIndex<int64_t>* which is UB in principle but harmless in
// practice. Remove this instantiation when that test is cleaned up.
template JsonToTypedResult
ConvertJsonToTypedFieldData<int64_t>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function);

template JsonToTypedResult
ConvertJsonToTypedFieldData<double>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function);

template JsonToTypedResult
ConvertJsonToTypedFieldData<std::string>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function);

namespace json {

bool
IsDataTypeSupported(JsonCastType cast_type, DataType data_type, bool is_array) {
    bool cast_type_is_array =
        cast_type.data_type() == JsonCastType::DataType::ARRAY;
    auto type = cast_type.ToMilvusDataType();
    return is_array == cast_type_is_array &&
           (type == data_type ||
            (data_type == DataType::INT64 && type == DataType::DOUBLE));
}

}  // namespace json

}  // namespace milvus::index