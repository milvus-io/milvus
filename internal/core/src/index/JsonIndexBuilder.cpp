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

#include <string>
#include <string_view>
#include "common/JsonUtils.h"
#include "index/JsonIndexBuilder.h"
#include "simdjson/error.h"

namespace milvus::index {

namespace {

enum class JsonPathPresence {
    Missing,
    Present,
    PresentButInvalid,
};

struct JsonPathPresenceResult {
    JsonPathPresence presence;
    simdjson::error_code error;
};

// Probe path presence without materializing a DOM value. In particular,
// ondemand::value::type() can identify an out-of-range JSON number as a number
// without converting it to double; the conversion error is handled later as an
// invalid indexed value while the path remains present for EXISTS.
JsonPathPresenceResult
GetJsonPathPresence(const Json& json, const std::string& nested_path) {
    auto type = json.type(nested_path);
    auto error = type.error();
    if (error == simdjson::SUCCESS) {
        return {json.exist(nested_path) ? JsonPathPresence::Present
                                        : JsonPathPresence::Missing,
                simdjson::SUCCESS};
    }

    // A path through a scalar, an absent object key, or an out-of-range array
    // element is not present. Numeric parse/conversion failures still describe
    // a present value; keep it out of the typed value index without marking the
    // path as non-existent.
    if (error == simdjson::NO_SUCH_FIELD ||
        error == simdjson::INDEX_OUT_OF_BOUNDS ||
        error == simdjson::INCORRECT_TYPE ||
        error == simdjson::SCALAR_DOCUMENT_AS_VALUE ||
        error == simdjson::INVALID_JSON_POINTER) {
        return {JsonPathPresence::Missing, error};
    }
    if (error == simdjson::NUMBER_ERROR || error == simdjson::BIGINT_ERROR ||
        error == simdjson::NUMBER_OUT_OF_RANGE) {
        return {JsonPathPresence::PresentButInvalid, error};
    }

    // Preserve the previous all-or-nothing behavior for malformed JSON and
    // invalid index paths. Treating an unclassifiable row as either present or
    // absent would publish a semantically incomplete index.
    AssertInfo(false,
               "failed to inspect JSON path {}: {}",
               nested_path,
               simdjson::error_message(error));
    return {JsonPathPresence::PresentButInvalid, error};  // unreachable
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
    JsonErrorRecorder error_recorder) {
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

            auto presence = GetJsonPathPresence(*json_column, nested_path);
            if (presence.presence == JsonPathPresence::Missing) {
                error_recorder(
                    *json_column, nested_path, simdjson::NO_SUCH_FIELD);
                non_exist_adder(offset);
                data_adder(nullptr, 0, offset++);
                continue;
            }
            if (presence.presence == JsonPathPresence::PresentButInvalid) {
                error_recorder(*json_column, nested_path, presence.error);
                data_adder(nullptr, 0, offset++);
                continue;
            }

            values.clear();
            if (is_array) {
                auto doc = json_column->doc();
                auto array_res = doc.at_pointer(nested_path).get_array();
                if (array_res.error() != simdjson::SUCCESS) {
                    error_recorder(
                        *json_column, nested_path, array_res.error());
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
                if (cast_function.match<T>()) {
                    auto res = JsonCastFunction::CastJsonValue<T>(
                        cast_function, *json_column, nested_path);
                    if (res.has_value()) {
                        values.push_back(res.value());
                    }
                } else if constexpr (std::is_same_v<T, double>) {
                    auto res = json_column->at_numeric(nested_path);
                    if (res.error() != simdjson::SUCCESS) {
                        error_recorder(*json_column, nested_path, res.error());
                    } else {
                        values.push_back(res.value().as_double());
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

}  // namespace milvus::index
