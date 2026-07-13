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

#include "common/Array.h"
#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/JsonCastType.h"
#include "common/JsonUtils.h"
#include "common/Types.h"
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

namespace {

// Establish the ScalarField oneof data_case for element type T without adding
// any element. This fixes Array's element_type_ even for a 0-element (empty)
// array, so get_element_type() is correct (the string nested builder asserts on
// it). Calling the mutable accessor sets the oneof case; Array(ScalarField)
// then reads element_type_ from data_case(): bool_data -> BOOL, double_data ->
// DOUBLE, string_data -> STRING.
template <typename T>
void
SetArrayElementCase(ScalarFieldProto& row_proto) {
    if constexpr (std::is_same_v<T, bool>) {
        (void)row_proto.mutable_bool_data();
    } else if constexpr (std::is_same_v<T, double>) {
        (void)row_proto.mutable_double_data();
    } else if constexpr (std::is_same_v<T, std::string>) {
        (void)row_proto.mutable_string_data();
    } else {
        static_assert(sizeof(T) == 0,
                      "unsupported element type for JSON array conversion");
    }
}

// Append one parsed element into the per-row ScalarField proto. The proto data
// container picked here decides Array's element_type_ (see Array(ScalarField)):
// bool_data -> BOOL, double_data -> DOUBLE, string_data -> STRING.
template <typename T>
void
AppendArrayElement(ScalarFieldProto& row_proto, const T& value) {
    if constexpr (std::is_same_v<T, bool>) {
        row_proto.mutable_bool_data()->add_data(value);
    } else if constexpr (std::is_same_v<T, double>) {
        row_proto.mutable_double_data()->add_data(value);
    } else if constexpr (std::is_same_v<T, std::string>) {
        row_proto.mutable_string_data()->add_data(value);
    } else {
        static_assert(sizeof(T) == 0,
                      "unsupported element type for JSON array conversion");
    }
}

}  // namespace

template <typename T>
FieldDataPtr
ConvertJsonToArrayFieldData(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type) {
    AssertInfo(cast_type.data_type() == JsonCastType::DataType::ARRAY,
               "ConvertJsonToArrayFieldData requires an ARRAY cast type, got {}",
               cast_type);

    using SIMDJSON_T =
        std::conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;

    int64_t total_rows = 0;
    for (const auto& data : json_field_datas) {
        total_rows += data->get_num_rows();
    }

    // Dense, nullable ARRAY FieldData: one Array slot per logical row, mirroring
    // the real ARRAY FillFieldData path (FieldData.cpp DataType::ARRAY). NULL /
    // missing-path / non-array rows keep an empty Array with valid-bit 0 and so
    // contribute zero elements to the element-level nested build.
    auto field_data = std::make_shared<FieldData<Array>>(
        DataType::ARRAY, /*nullable=*/true, total_rows);

    FixedVector<Array> values(total_rows);
    std::vector<uint8_t> valid_data((total_rows + 7) / 8, 0);

    auto tokens = parse_json_pointer(nested_path);

    int64_t offset = 0;
    for (const auto& data : json_field_datas) {
        auto n = data->get_num_rows();
        for (int64_t i = 0; i < n; i++, offset++) {
            if (schema.nullable() && !data->is_valid(i)) {
                // null row -> empty invalid Array, no elements
                continue;
            }

            auto json_column = static_cast<const Json*>(data->RawValue(i));
            auto exists = path_exists(json_column->dom_doc(), tokens);
            if (!exists || !json_column->exist(nested_path)) {
                // missing path -> empty invalid Array, no elements
                continue;
            }

            auto array_res =
                json_column->dom_doc().at_pointer(nested_path).get_array();
            if (array_res.error() != simdjson::SUCCESS) {
                // value at path is not an array -> empty invalid Array
                continue;
            }

            // Fix the element type up-front so an empty (0-element) array still
            // reports the correct get_element_type() (the string nested builder
            // asserts on it) instead of an unknown default.
            ScalarFieldProto row_proto;
            SetArrayElementCase<T>(row_proto);
            for (auto value : array_res.value()) {
                auto val = value.template get<SIMDJSON_T>();
                if (val.error() == simdjson::SUCCESS) {
                    AppendArrayElement<T>(row_proto,
                                          static_cast<T>(val.value()));
                }
            }

            // Even an empty array is a present value: mark the row valid so it
            // is not treated as a null row (it just contributes zero elements).
            values[offset] = Array(row_proto);
            valid_data[offset / 8] |= (1 << (offset % 8));
        }
    }

    field_data->FillFieldData(values.data(),
                              valid_data.data(),
                              static_cast<ssize_t>(total_rows),
                              static_cast<ssize_t>(0));
    return field_data;
}

template FieldDataPtr
ConvertJsonToArrayFieldData<bool>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type);

template FieldDataPtr
ConvertJsonToArrayFieldData<double>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type);

template FieldDataPtr
ConvertJsonToArrayFieldData<std::string>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type);

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