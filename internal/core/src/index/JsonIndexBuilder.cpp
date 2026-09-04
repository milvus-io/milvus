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
#include <cmath>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/JsonCastType.h"
#include "common/JsonUtils.h"
#include "folly/FBVector.h"
#include "index/JsonIndexBuilder.h"
#include "pb/schema.pb.h"
#include "simdjson/error.h"

namespace milvus::index {

namespace {

enum class JsonPathLookupStatus {
    MISSING,
    FOUND,
};

struct JsonPathInspection {
    JsonPathLookupStatus lookup_status;
    bool semantically_exists;
};

bool
IsLegacyJsonValuePresent(simdjson::ondemand::value value) {
    auto type = value.type();
    if (type.error() != simdjson::SUCCESS) {
        if (IsUnrepresentableJsonNumberError(type.error())) {
            return false;
        }
        AssertInfo(false,
                   "failed to inspect legacy JSON value: {}",
                   simdjson::error_message(type.error()));
    }

    switch (type.value()) {
        case simdjson::ondemand::json_type::null:
            return false;
        case simdjson::ondemand::json_type::object: {
            auto object = value.get_object();
            AssertInfo(object.error() == simdjson::SUCCESS,
                       "failed to inspect legacy JSON object: {}",
                       simdjson::error_message(object.error()));
            for (auto field : object) {
                if (IsLegacyJsonValuePresent(field.value())) {
                    return true;
                }
            }
            return false;
        }
        case simdjson::ondemand::json_type::array: {
            auto array = value.get_array();
            AssertInfo(array.error() == simdjson::SUCCESS,
                       "failed to inspect legacy JSON array: {}",
                       simdjson::error_message(array.error()));
            for (auto element : array) {
                if (IsLegacyJsonValuePresent(std::move(element))) {
                    return true;
                }
            }
            return false;
        }
        case simdjson::ondemand::json_type::number: {
            auto error = value.get_number().error();
            if (error != simdjson::SUCCESS &&
                !IsUnrepresentableJsonNumberError(error)) {
                AssertInfo(false,
                           "failed to inspect legacy JSON number: {}",
                           simdjson::error_message(error));
            }
            return error == simdjson::SUCCESS;
        }
        default:
            return true;
    }
}

bool
IsLegacyJsonDocumentPresent(simdjson::ondemand::document document) {
    auto type = document.type();
    if (type.error() != simdjson::SUCCESS) {
        if (IsUnrepresentableJsonNumberError(type.error())) {
            return false;
        }
        AssertInfo(false,
                   "failed to inspect legacy JSON document: {}",
                   simdjson::error_message(type.error()));
    }

    switch (type.value()) {
        case simdjson::ondemand::json_type::null:
            return false;
        case simdjson::ondemand::json_type::object: {
            auto object = document.get_object();
            AssertInfo(object.error() == simdjson::SUCCESS,
                       "failed to inspect legacy JSON object document: {}",
                       simdjson::error_message(object.error()));
            for (auto field : object) {
                if (IsLegacyJsonValuePresent(field.value())) {
                    return true;
                }
            }
            return false;
        }
        case simdjson::ondemand::json_type::array: {
            auto array = document.get_array();
            AssertInfo(array.error() == simdjson::SUCCESS,
                       "failed to inspect legacy JSON array document: {}",
                       simdjson::error_message(array.error()));
            for (auto element : array) {
                if (IsLegacyJsonValuePresent(std::move(element))) {
                    return true;
                }
            }
            return false;
        }
        case simdjson::ondemand::json_type::number: {
            auto error = document.get_number().error();
            if (error != simdjson::SUCCESS &&
                !IsUnrepresentableJsonNumberError(error)) {
                AssertInfo(false,
                           "failed to inspect legacy JSON document number: {}",
                           simdjson::error_message(error));
            }
            return error == simdjson::SUCCESS;
        }
        default:
            return true;
    }
}

bool
HasLegacyJsonPathPresence(const Json& json, const std::string& nested_path) {
    auto doc = json.doc();
    if (nested_path.empty()) {
        return doc.error() == simdjson::SUCCESS &&
               IsLegacyJsonDocumentPresent(std::move(doc));
    }
    auto value = doc.at_pointer(nested_path);
    return value.error() == simdjson::SUCCESS &&
           IsLegacyJsonValuePresent(value.value());
}

// Probe path presence without materializing a DOM value. Invalid JSON numbers
// follow Milvus' configured EXISTS=false contract: the typed value is invalid
// and EXISTS is false, matching Json::exist and JSON stats. Containers are
// present regardless of their contents under v6 semantics; legacy semantics
// recursively requires a non-empty descendant.
JsonPathInspection
InspectJsonPathForTypedIndex(const Json& json,
                             const std::string& nested_path,
                             JsonPathPresenceSemantics presence_semantics) {
    auto type = json.type(nested_path);
    auto error = type.error();
    if (error == simdjson::SUCCESS) {
        // Physical access and semantic EXISTS are deliberately separate. A
        // present value may still be invalid for the configured typed index.
        auto semantically_exists =
            presence_semantics == JsonPathPresenceSemantics::NON_NULL_TARGET
                ? json.exist(nested_path)
                : HasLegacyJsonPathPresence(json, nested_path);
        return {JsonPathLookupStatus::FOUND, semantically_exists};
    }

    // A path through a scalar, an absent object key, or an out-of-range array
    // element is not present.
    if (error == simdjson::NO_SUCH_FIELD ||
        error == simdjson::INDEX_OUT_OF_BOUNDS ||
        error == simdjson::INCORRECT_TYPE ||
        error == simdjson::SCALAR_DOCUMENT_AS_VALUE ||
        error == simdjson::INVALID_JSON_POINTER) {
        return {JsonPathLookupStatus::MISSING, false};
    }
    if (IsUnrepresentableJsonNumberError(error)) {
        // TODO: If Milvus later distinguishes a present-but-invalid JSON value
        // from null/missing, update Json::exist, typed path index presence,
        // JSON stats BSON/typed validity, and all predicate validity paths
        // together.
        return {JsonPathLookupStatus::FOUND, false};
    }

    // Preserve the previous all-or-nothing behavior for malformed JSON and
    // invalid index paths. Treating an unclassifiable row as either present or
    // absent would publish a semantically incomplete index.
    AssertInfo(false,
               "failed to inspect JSON path {}: {}",
               nested_path,
               simdjson::error_message(error));
    return {JsonPathLookupStatus::MISSING, false};  // unreachable
}

// Strictly cast a JSON number to the integer type T. A cast succeeds only
// when the number is an integral value within T's representable range.
// Integral doubles (e.g. 2.0) that round-trip exactly to the same integer are
// accepted; fractional doubles, out-of-range integers, and uint64 values above
// T's maximum fail the cast and return nullopt.
template <typename T>
std::optional<T>
StrictCastJsonNumberToInteger(simdjson::ondemand::number number) {
    static_assert(std::is_integral_v<T> && !std::is_same_v<T, bool>);
    if (number.is_int64()) {
        int64_t v = number.get_int64();
        if (v < static_cast<int64_t>(std::numeric_limits<T>::lowest()) ||
            v > static_cast<int64_t>(std::numeric_limits<T>::max())) {
            return std::nullopt;
        }
        return static_cast<T>(v);
    }
    if (number.is_uint64()) {
        uint64_t v = number.get_uint64();
        if (v > static_cast<uint64_t>(std::numeric_limits<T>::max())) {
            return std::nullopt;
        }
        return static_cast<T>(v);
    }
    double v = number.get_double();
    if (!std::isfinite(v) || std::trunc(v) != v) {
        return std::nullopt;
    }
    constexpr double kLower =
        static_cast<double>(std::numeric_limits<T>::lowest());
    constexpr double kUpper =
        static_cast<double>(std::numeric_limits<T>::max());
    if (v < kLower || v > kUpper) {
        return std::nullopt;
    }
    if constexpr (std::is_same_v<T, int64_t>) {
        // 2^63 is the first integer outside the int64 range and is exactly
        // representable; reject it before the narrowing cast.
        if (v >= 0x1p63) {
            return std::nullopt;
        }
    }
    T casted = static_cast<T>(v);
    if (static_cast<double>(casted) != v) {
        return std::nullopt;
    }
    return casted;
}

}  // namespace

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
    JsonPathPresenceSemantics presence_semantics) {
    int64_t offset = 0;
    using SIMDJSON_T =
        std::conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;

    // Preserve the previous up-front validation of the configured pointer, but
    // do not materialize each row as a DOM document merely to test presence.
    (void)parse_json_pointer(nested_path);

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

            auto path_inspection = InspectJsonPathForTypedIndex(
                *json_column, nested_path, presence_semantics);
            if (path_inspection.lookup_status ==
                JsonPathLookupStatus::MISSING) {
                error_recorder(
                    *json_column, nested_path, simdjson::NO_SUCH_FIELD);
                non_exist_adder(offset);
                if (is_array) {
                    null_adder(offset);
                }
                data_adder(nullptr, 0, offset++);
                continue;
            }
            if (!path_inspection.semantically_exists) {
                non_exist_adder(offset);
            }
            values.clear();
            if (is_array) {
                auto doc = json_column->doc();
                auto array_res = doc.at_pointer(nested_path).get_array();
                if (array_res.error() != simdjson::SUCCESS) {
                    error_recorder(
                        *json_column, nested_path, array_res.error());
                    // The path still exists, but it is not a valid ARRAY_*
                    // operand. Keep EXISTS separate and make typed predicates
                    // UNKNOWN through the persisted row-null offsets.
                    null_adder(offset);
                } else {
                    auto array_values = array_res.value();
                    for (auto value : array_values) {
                        if constexpr (std::is_same_v<T, double>) {
                            auto val = value.get_number();
                            if (val.error() == simdjson::SUCCESS) {
                                values.push_back(val.value().as_double());
                            }
                        } else {
                            auto val = value.template get<SIMDJSON_T>();
                            if (val.error() == simdjson::SUCCESS) {
                                values.push_back(static_cast<T>(val.value()));
                            }
                        }
                    }
                }
            } else {
                if constexpr (std::is_same_v<T, double>) {
                    if (cast_function.match<T>()) {
                        auto res = JsonCastFunction::CastJsonValue<T>(
                            cast_function, *json_column, nested_path);
                        if (res.has_value()) {
                            values.push_back(res.value());
                        }
                    } else {
                        auto res = json_column->at_numeric(nested_path);
                        if (res.error() != simdjson::SUCCESS) {
                            error_recorder(
                                *json_column, nested_path, res.error());
                        } else {
                            values.push_back(res.value().as_double());
                        }
                    }
                } else if constexpr (std::is_integral_v<T> &&
                                     !std::is_same_v<T, bool>) {
                    // Strict integer cast: only integral, in-range values are
                    // indexed. Fractional / out-of-range numbers leave the row
                    // null (UNKNOWN) rather than being silently truncated.
                    auto res = json_column->at_numeric(nested_path);
                    if (res.error() != simdjson::SUCCESS) {
                        error_recorder(*json_column, nested_path, res.error());
                    } else if (auto casted = StrictCastJsonNumberToInteger<T>(
                                   res.value());
                               casted.has_value()) {
                        values.push_back(*casted);
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
    JsonErrorRecorder error_recorder,
    JsonPathPresenceSemantics presence_semantics);

template void
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

template void
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

template void
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
    JsonErrorRecorder error_recorder,
    JsonPathPresenceSemantics presence_semantics);

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
    JsonErrorRecorder error_recorder,
    JsonPathPresenceSemantics presence_semantics);

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
    JsonErrorRecorder error_recorder,
    JsonPathPresenceSemantics presence_semantics);

namespace {

template <typename T>
DataType
GetMilvusDataType() {
    if constexpr (std::is_same_v<T, bool>) {
        return DataType::BOOL;
    } else if constexpr (std::is_same_v<T, int8_t>) {
        return DataType::INT8;
    } else if constexpr (std::is_same_v<T, int16_t>) {
        return DataType::INT16;
    } else if constexpr (std::is_same_v<T, int32_t>) {
        return DataType::INT32;
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
    JsonCastFunction cast_function,
    JsonPathPresenceSemantics presence_semantics) {
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
        // non_exist_adder: track offsets where semantic JSON EXISTS is false
        [&non_exist_offsets](int64_t offset) {
            non_exist_offsets.push_back(offset);
        },
        [](const Json&, const std::string&, simdjson::error_code) {},
        presence_semantics);

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
    JsonCastFunction cast_function,
    JsonPathPresenceSemantics presence_semantics);

template JsonToTypedResult
ConvertJsonToTypedFieldData<int8_t>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonPathPresenceSemantics presence_semantics);

template JsonToTypedResult
ConvertJsonToTypedFieldData<int16_t>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonPathPresenceSemantics presence_semantics);

template JsonToTypedResult
ConvertJsonToTypedFieldData<int32_t>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonPathPresenceSemantics presence_semantics);

template JsonToTypedResult
ConvertJsonToTypedFieldData<int64_t>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonPathPresenceSemantics presence_semantics);

template JsonToTypedResult
ConvertJsonToTypedFieldData<double>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonPathPresenceSemantics presence_semantics);

template JsonToTypedResult
ConvertJsonToTypedFieldData<std::string>(
    const std::vector<std::shared_ptr<FieldDataBase>>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function,
    JsonPathPresenceSemantics presence_semantics);

namespace json {

bool
IsDataTypeSupported(JsonCastType cast_type, DataType data_type, bool is_array) {
    bool cast_type_is_array =
        cast_type.data_type() == JsonCastType::DataType::ARRAY;
    auto type = cast_type.ToMilvusDataType();
    return is_array == cast_type_is_array &&
           (type == data_type ||
            (data_type == DataType::INT64 && type == DataType::DOUBLE) ||
            (data_type == DataType::INT64 && type != DataType::BOOL &&
             IsIntegerDataType(type)));
}

}  // namespace json

}  // namespace milvus::index
