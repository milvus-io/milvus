// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <string>
#include <boost/filesystem.hpp>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>

#include "index/InvertedIndexTantivy.h"
#include "common/jsmn.h"
#include "arrow/api.h"
#include "common/EasyAssert.h"
#include <simdjson.h>
#include <cstring>

namespace milvus::index {

constexpr int64_t DEFAULT_BATCH_SIZE = 8192 * 10;
constexpr int64_t DEFAULT_BUFFER_SIZE = 16 * 1024 * 1024;
constexpr int64_t DEFAULT_PART_UPLOAD_SIZE = 10 * 1024 * 1024;

enum class JSONType {
    UNKNOWN,
    NONE,
    BOOL,
    INT8,
    INT16,
    INT32,
    INT64,
    FLOAT,
    DOUBLE,
    STRING,
    STRING_ESCAPE,
    ARRAY,
    OBJECT
};

inline bool
JsonStringHasEscape(std::string_view s) {
    // Any JSON escape must start with a backslash
    return std::memchr(s.data(), '\\', s.size()) != nullptr;
}

// Unescape a JSON-escaped string slice (without surrounding quotes)
// Returns a decoded UTF-8 std::string or throws on error
inline std::string
UnescapeJsonString(const std::string& escaped) {
    if (!JsonStringHasEscape(escaped)) {
        return escaped;
    }
    try {
        simdjson::dom::parser parser;
        std::string quoted;
        quoted.resize(escaped.size() + 2);
        quoted[0] = '"';
        std::memcpy(&quoted[1], escaped.data(), escaped.size());
        quoted[quoted.size() - 1] = '"';
        simdjson::dom::element elem = parser.parse(quoted);
        if (elem.type() != simdjson::dom::element_type::STRING) {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "input is not a JSON string: {}",
                      escaped);
        }
        return std::string(std::string_view(elem.get_string()));
    } catch (const simdjson::simdjson_error& e) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Failed to unescape json string (simdjson): {}, {}",
                  escaped,
                  e.what());
    } catch (const std::exception& e) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Failed to unescape json string: {}, {}",
                  escaped,
                  e.what());
    }
    return {};
}

inline std::string
ToString(JSONType type) {
    switch (type) {
        case JSONType::NONE:
            return "NONE";
        case JSONType::BOOL:
            return "BOOL";
        case JSONType::INT8:
            return "INT8";
        case JSONType::INT16:
            return "INT16";
        case JSONType::INT32:
            return "INT32";
        case JSONType::INT64:
            return "INT64";
        case JSONType::FLOAT:
            return "FLOAT";
        case JSONType::DOUBLE:
            return "DOUBLE";
        case JSONType::STRING:
            return "STRING";
        case JSONType::STRING_ESCAPE:
            return "STRING_ESCAPE";
        case JSONType::ARRAY:
            return "ARRAY";
        case JSONType::OBJECT:
            return "OBJECT";
        default:
            return "UNKNOWN";
    }
}

inline milvus::DataType
GetPrimitiveDataType(JSONType type) {
    switch (type) {
        case JSONType::NONE:
            return milvus::DataType::NONE;
        case JSONType::BOOL:
            return milvus::DataType::BOOL;
        case JSONType::INT8:
            return milvus::DataType::INT8;
        case JSONType::INT16:
            return milvus::DataType::INT16;
        case JSONType::INT32:
            return milvus::DataType::INT32;
        case JSONType::INT64:
            return milvus::DataType::INT64;
        case JSONType::FLOAT:
            return milvus::DataType::FLOAT;
        case JSONType::DOUBLE:
            return milvus::DataType::DOUBLE;
        case JSONType::STRING:
            return milvus::DataType::STRING;
        // for array type(bson format), we use string type instead of real binary type
        case JSONType::ARRAY:
            return milvus::DataType::STRING;
        default:
            return milvus::DataType::NONE;
    }
}

inline JSONType
GetPrimitiveJsonType(std::shared_ptr<arrow::DataType> type) {
    if (type->id() == arrow::Type::BOOL) {
        return JSONType::BOOL;
    } else if (type->id() == arrow::Type::INT8) {
        return JSONType::INT8;
    } else if (type->id() == arrow::Type::INT16) {
        return JSONType::INT16;
    } else if (type->id() == arrow::Type::INT32) {
        return JSONType::INT32;
    } else if (type->id() == arrow::Type::INT64) {
        return JSONType::INT64;
    } else if (type->id() == arrow::Type::FLOAT) {
        return JSONType::FLOAT;
    } else if (type->id() == arrow::Type::DOUBLE) {
        return JSONType::DOUBLE;
    } else if (type->id() == arrow::Type::STRING) {
        return JSONType::STRING;
    } else {
        return JSONType::UNKNOWN;
    }
}

inline bool
IsPrimitiveJsonType(JSONType type) {
    return type == JSONType::NONE || type == JSONType::BOOL ||
           type == JSONType::INT8 || type == JSONType::INT16 ||
           type == JSONType::INT32 || type == JSONType::INT64 ||
           type == JSONType::FLOAT || type == JSONType::DOUBLE ||
           type == JSONType::STRING || type == JSONType::STRING_ESCAPE;
}

inline bool
IsIntegerJsonType(JSONType type) {
    return type == JSONType::INT8 || type == JSONType::INT16 ||
           type == JSONType::INT32 || type == JSONType::INT64;
}

inline bool
IsFloatJsonType(JSONType type) {
    return type == JSONType::FLOAT || type == JSONType::DOUBLE;
}

inline bool
IsStringJsonType(JSONType type) {
    return type == JSONType::STRING || type == JSONType::STRING_ESCAPE;
}

inline bool
IsComplexJsonType(JSONType type) {
    return type == JSONType::ARRAY || type == JSONType::OBJECT;
}

inline bool
IsNullJsonType(JSONType type) {
    return type == JSONType::NONE;
}

inline bool
IsShreddingJsonType(JSONType type) {
    return IsPrimitiveJsonType(type) || type == JSONType::ARRAY;
}

enum class JsonKeyLayoutType {
    UNKNOWN = 0,
    TYPED = 1,
    TYPED_NOT_ALL = 2,
    DYNAMIC = 3,
    DYNAMIC_ONLY = 4,
    SHARED = 5,
};

inline std::string
ToString(JsonKeyLayoutType type) {
    switch (type) {
        case JsonKeyLayoutType::TYPED:
            return "TYPED";
        case JsonKeyLayoutType::TYPED_NOT_ALL:
            return "TYPED_NOT_ALL";
        case JsonKeyLayoutType::DYNAMIC:
            return "DYNAMIC";
        case JsonKeyLayoutType::DYNAMIC_ONLY:
            return "DYNAMIC_ONLY";
        case JsonKeyLayoutType::SHARED:
            return "SHARED";
        default:
            return "UNKNOWN";
    }
}

inline JsonKeyLayoutType
JsonKeyLayoutTypeFromString(const std::string& str) {
    if (str == "TYPED") {
        return JsonKeyLayoutType::TYPED;
    } else if (str == "TYPED_NOT_ALL") {
        return JsonKeyLayoutType::TYPED_NOT_ALL;
    } else if (str == "DYNAMIC") {
        return JsonKeyLayoutType::DYNAMIC;
    } else if (str == "DYNAMIC_ONLY") {
        return JsonKeyLayoutType::DYNAMIC_ONLY;
    } else if (str == "SHARED") {
        return JsonKeyLayoutType::SHARED;
    }
    return JsonKeyLayoutType::UNKNOWN;
}

inline bool
EndWith(std::string_view str, std::string_view suffix) {
    return str.size() >= suffix.size() &&
           str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
}

inline JSONType
GetJsonTypeFromKeyName(const std::string& key_name) {
    if (EndWith(key_name, "_INT64")) {
        return JSONType::INT64;
    } else if (EndWith(key_name, "_DOUBLE")) {
        return JSONType::DOUBLE;
    } else if (EndWith(key_name, "_STRING")) {
        return JSONType::STRING;
    } else if (EndWith(key_name, "_BOOL")) {
        return JSONType::BOOL;
    } else if (EndWith(key_name, "_NULL")) {
        return JSONType::NONE;
    } else if (EndWith(key_name, "_ARRAY")) {
        return JSONType::ARRAY;
    } else if (EndWith(key_name, "_OBJECT")) {
        return JSONType::OBJECT;
    } else {
        return JSONType::UNKNOWN;
    }
}

inline std::string
GetKeyFromColumnName(const std::string& column_name) {
    if (EndWith(column_name, "_INT64")) {
        return column_name.substr(0, column_name.size() - 6);
    } else if (EndWith(column_name, "_DOUBLE")) {
        return column_name.substr(0, column_name.size() - 7);
    } else if (EndWith(column_name, "_STRING")) {
        return column_name.substr(0, column_name.size() - 7);
    } else if (EndWith(column_name, "_BOOL")) {
        return column_name.substr(0, column_name.size() - 5);
    } else if (EndWith(column_name, "_NULL")) {
        return column_name.substr(0, column_name.size() - 5);
    } else if (EndWith(column_name, "_ARRAY")) {
        return column_name.substr(0, column_name.size() - 6);
    } else if (EndWith(column_name, "_OBJECT")) {
        return column_name.substr(0, column_name.size() - 7);
    } else {
        return column_name;
    }
}

// construct json pointer with nested path vector
inline std::string
JsonPointer(std::vector<std::string> nested_path) {
    if (nested_path.empty()) {
        return "";
    }

    std::for_each(nested_path.begin(), nested_path.end(), [](std::string& key) {
        boost::replace_all(key, "~", "~0");
        boost::replace_all(key, "/", "~1");
    });

    auto pointer = "/" + boost::algorithm::join(nested_path, "/");
    return pointer;
}

inline std::string
AppendJsonPointer(std::string pointer, std::string key) {
    boost::replace_all(key, "~", "~0");
    boost::replace_all(key, "/", "~1");
    return pointer + "/" + key;
}

// parse json pointer to nested path vector
inline std::vector<std::string>
ParseJsonPointerPath(const std::string& pointer) {
    if (pointer.empty()) {
        return {};
    }
    if (pointer[0] != '/') {
        ThrowInfo(ErrorCode::PathInvalid,
                  "Invalid JSON pointer: must start with '/'");
    }

    std::vector<std::string> tokens;
    boost::split(tokens, pointer.substr(1), boost::is_any_of("/"));

    for (auto& token : tokens) {
        if (token.find('~') != std::string::npos) {
            boost::replace_all(token, "~1", "/");
            boost::replace_all(token, "~0", "~");
        }
    }

    return tokens;
}

struct JsonKey {
    std::string key_;
    JSONType type_;

    JsonKey(const std::string& key, JSONType type) : key_(key), type_(type) {
    }

    JsonKey() {
    }

    bool
    operator<(const JsonKey& other) const {
        return std::tie(key_, type_) < std::tie(other.key_, other.type_);
    }

    bool
    operator==(const JsonKey& other) const {
        return key_ == other.key_ && type_ == other.type_;
    }

    bool
    operator!=(const JsonKey& other) const {
        return !(*this == other);
    }

    std::string
    ToString() const {
        return key_ + ":" + milvus::index::ToString(type_);
    }

    std::string
    ToColumnName() const {
        return key_ + "_" + milvus::index::ToString(type_);
    }
};

struct KeyStatsInfo {
    int32_t hit_row_num_ = 0;
    // TODO: add min/max value for stats info
    uint8_t min_value_[8] = {0};
    uint8_t max_value_[8] = {0};

    std::string
    ToString() const {
        return "row_num: " + std::to_string(hit_row_num_);
    }
};
struct PathWriter {
    JsonKeyLayoutType type_;
    JsonKey key_;
};

std::shared_ptr<arrow::ArrayBuilder>
CreateSharedArrowBuilder();

std::shared_ptr<arrow::ArrayBuilder>
CreateArrowBuilder(JSONType type);

std::shared_ptr<arrow::Field>
CreateSharedArrowField(const std::string& field_name, int64_t field_id);

std::shared_ptr<arrow::Field>
CreateArrowField(const JsonKey& key, const JsonKeyLayoutType& key_type);

std::pair<std::vector<std::shared_ptr<arrow::ArrayBuilder>>,
          std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>>
CreateArrowBuilders(std::map<JsonKey, JsonKeyLayoutType> column_map);

std::shared_ptr<arrow::Schema>
CreateArrowSchema(std::map<JsonKey, JsonKeyLayoutType> column_map);

std::vector<std::pair<std::string, std::string>>
CreateParquetKVMetadata(std::map<JsonKey, JsonKeyLayoutType> column_map);

inline size_t
GetArrowArrayMemorySize(const std::shared_ptr<arrow::Array>& array) {
    if (!array || !array->data()) {
        return 0;
    }
    size_t total_size = 0;
    for (const auto& buffer : array->data()->buffers) {
        if (buffer) {
            total_size += buffer->size();
        }
    }
    return total_size;
}

inline std::string
CreateColumnGroupParquetPath(const std::string& path_prefix,
                             size_t column_group_id,
                             size_t file_log_id) {
    return path_prefix + "/" + std::to_string(column_group_id) + "/" +
           std::to_string(file_log_id);
}

// parse <column_group_id, file_id> from parquet path
inline std::pair<size_t, size_t>
ParseParquetPath(const std::string& path) {
    const auto last_slash = path.find_last_of('/');
    const auto second_last_slash = path.find_last_of('/', last_slash - 1);
    const size_t column_group_id = std::stoll(
        path.substr(second_last_slash + 1, last_slash - second_last_slash - 1));
    const size_t file_id = std::stoll(path.substr(last_slash + 1));
    return {column_group_id, file_id};
}

// sort parquet paths by column_group_id and file_id
// return vector of <column_group_id, vector<file_id>>
inline std::vector<std::pair<int64_t, std::vector<int64_t>>>
SortByParquetPath(const std::vector<std::string>& paths) {
    // preprocess stage: parse all path info
    std::vector<std::pair<int64_t, int64_t>> parsed_info;
    parsed_info.reserve(paths.size());
    for (const auto& path : paths) {
        parsed_info.emplace_back(ParseParquetPath(path));
    }

    // sort stage: directly compare parsed values
    std::sort(parsed_info.begin(),
              parsed_info.end(),
              [](const auto& a, const auto& b) {
                  return std::tie(a.first, a.second) <
                         std::tie(b.first, b.second);
              });

    // group stage: single traversal to complete grouping
    std::vector<std::pair<int64_t, std::vector<int64_t>>> result;
    for (const auto& [col, file] : parsed_info) {
        if (result.empty() || result.back().first != col) {
            result.emplace_back(col, std::vector<int64_t>());
        }
        result.back().second.push_back(file);
    }

    return result;
}

}  // namespace milvus::index

template <>
struct fmt::formatter<milvus::index::JSONType> : fmt::formatter<std::string> {
    template <typename FormatContext>
    auto
    format(const milvus::index::JSONType& jt, FormatContext& ctx) {
        switch (jt) {
            case milvus::index::JSONType::UNKNOWN:
                return fmt::format_to(ctx.out(), "UNKNOWN");
            case milvus::index::JSONType::NONE:
                return fmt::format_to(ctx.out(), "NULL");
            case milvus::index::JSONType::BOOL:
                return fmt::format_to(ctx.out(), "BOOL");
            case milvus::index::JSONType::INT8:
                return fmt::format_to(ctx.out(), "INT8");
            case milvus::index::JSONType::INT16:
                return fmt::format_to(ctx.out(), "INT16");
            case milvus::index::JSONType::INT32:
                return fmt::format_to(ctx.out(), "INT32");
            case milvus::index::JSONType::INT64:
                return fmt::format_to(ctx.out(), "INT64");
            case milvus::index::JSONType::FLOAT:
                return fmt::format_to(ctx.out(), "FLOAT");
            case milvus::index::JSONType::DOUBLE:
                return fmt::format_to(ctx.out(), "DOUBLE");
            case milvus::index::JSONType::STRING:
                return fmt::format_to(ctx.out(), "STRING");
            case milvus::index::JSONType::STRING_ESCAPE:
                return fmt::format_to(ctx.out(), "STRING_ESCAPE");
            case milvus::index::JSONType::ARRAY:
                return fmt::format_to(ctx.out(), "ARRAY");
            case milvus::index::JSONType::OBJECT:
                return fmt::format_to(ctx.out(), "OBJECT");
            default:
                return fmt::format_to(ctx.out(), "UNKNOWN");
        }
    }
};

template <>
struct fmt::formatter<milvus::index::JsonKeyLayoutType>
    : fmt::formatter<std::string> {
    auto
    format(milvus::index::JsonKeyLayoutType type, fmt::format_context& ctx) {
        std::string name;
        switch (type) {
            case milvus::index::JsonKeyLayoutType::TYPED:
                name = "TYPED";
                break;
            case milvus::index::JsonKeyLayoutType::DYNAMIC:
                name = "DYNAMIC";
                break;
            case milvus::index::JsonKeyLayoutType::SHARED:
                name = "SHARED";
                break;
            case milvus::index::JsonKeyLayoutType::TYPED_NOT_ALL:
                name = "TYPED_NOT_ALL";
                break;
            case milvus::index::JsonKeyLayoutType::DYNAMIC_ONLY:
                name = "DYNAMIC_ONLY";
                break;
            default:
                name = "UNKNOWN";
                break;
        }
        return fmt::formatter<std::string>::format(name, ctx);
    }
};
