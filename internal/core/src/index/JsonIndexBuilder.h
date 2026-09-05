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
#include <stdint.h>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/JsonCastFunction.h"
#include "common/JsonCastType.h"
#include "pb/schema.pb.h"
#include "simdjson/error.h"

namespace milvus::index {

namespace json {
// Returns true if the JSON cast_type is compatible with the raw JSON value's
// data_type (e.g., DOUBLE cast can accept INT64 JSON values). Array cast
// types require is_array=true.
bool
IsDataTypeSupported(JsonCastType cast_type, DataType data_type, bool is_array);
}  // namespace json

template <typename T>
using JsonDataAdder =
    std::function<void(const T* data, int64_t size, int64_t offset)>;

using JsonErrorRecorder = std::function<void(const Json& json,
                                             const std::string& nested_path,
                                             simdjson::error_code error)>;

using JsonNullAdder = std::function<void(int64_t offset)>;
using JsonNonExistAdder = std::function<void(int64_t offset)>;

enum class JsonPathPresenceSemantics {
    // Scalar index engine < v6: null, empty containers, and containers whose
    // descendants are all absent are encoded as not existing.
    LEGACY_RECURSIVE_NON_EMPTY,
    // Scalar index engine v6+: presence is a property of the target itself;
    // every non-null target exists regardless of its descendants.
    NON_NULL_TARGET,
};

// Result of converting JSON field data to typed FieldData.
struct JsonToTypedResult {
    // Typed field data with nullable semantics. Rows where the path doesn't
    // exist or the cast fails are marked as invalid.
    FieldDataPtr field_data;

    // Offsets of rows where Milvus JSON EXISTS is false under the selected
    // build semantics. Missing/null and unrepresentable-number targets are
    // always absent; empty containers differ between legacy and v6 semantics.
    // This is a SUBSET of invalid typed rows: ordinary type/cast mismatch
    // remains a physically present JSON value and is not included.
    std::vector<size_t> non_exist_offsets;
};

// Convert JSON field data into typed FieldData by extracting values at
// the given nested_path.
template <typename T>
JsonToTypedResult
ConvertJsonToTypedFieldData(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonPathPresenceSemantics presence_semantics =
        JsonPathPresenceSemantics::NON_NULL_TARGET);

extern template JsonToTypedResult
ConvertJsonToTypedFieldData<bool>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonPathPresenceSemantics presence_semantics);

extern template JsonToTypedResult
ConvertJsonToTypedFieldData<int8_t>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonPathPresenceSemantics presence_semantics);

extern template JsonToTypedResult
ConvertJsonToTypedFieldData<int16_t>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonPathPresenceSemantics presence_semantics);

extern template JsonToTypedResult
ConvertJsonToTypedFieldData<int32_t>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonPathPresenceSemantics presence_semantics);

extern template JsonToTypedResult
ConvertJsonToTypedFieldData<int64_t>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonPathPresenceSemantics presence_semantics);

extern template JsonToTypedResult
ConvertJsonToTypedFieldData<double>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonPathPresenceSemantics presence_semantics);

extern template JsonToTypedResult
ConvertJsonToTypedFieldData<std::string>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonPathPresenceSemantics presence_semantics);

// A helper function for processing json data for building inverted index
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
    JsonErrorRecorder error_recorder,
    JsonPathPresenceSemantics presence_semantics =
        JsonPathPresenceSemantics::NON_NULL_TARGET);

extern template void
ProcessJsonFieldData<bool>(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonDataAdder<bool> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder,
    JsonPathPresenceSemantics presence_semantics);

extern template void
ProcessJsonFieldData<int8_t>(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonDataAdder<int8_t> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder,
    JsonPathPresenceSemantics presence_semantics);

extern template void
ProcessJsonFieldData<int16_t>(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonDataAdder<int16_t> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder,
    JsonPathPresenceSemantics presence_semantics);

extern template void
ProcessJsonFieldData<int32_t>(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonDataAdder<int32_t> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder,
    JsonPathPresenceSemantics presence_semantics);

extern template void
ProcessJsonFieldData<int64_t>(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonDataAdder<int64_t> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder,
    JsonPathPresenceSemantics presence_semantics);

extern template void
ProcessJsonFieldData<double>(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonDataAdder<double> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder,
    JsonPathPresenceSemantics presence_semantics);

extern template void
ProcessJsonFieldData<std::string>(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonDataAdder<std::string> data_adder,
    JsonNullAdder null_adder,
    JsonNonExistAdder non_exist_adder,
    JsonErrorRecorder error_recorder,
    JsonPathPresenceSemantics presence_semantics);

}  // namespace milvus::index
