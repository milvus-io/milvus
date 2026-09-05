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

#include <bson/bson.h>
#include <simdjson.h>
#include "common/FastMem.h"
#include <string.h>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <map>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/Utils.h"
#include "common/bson_shim.h"
#include "glog/logging.h"
#include "index/json_stats/bson_builder.h"
#include "log/Log.h"
#include "simdjson/dom/array.h"
#include "simdjson/dom/element.h"
#include "simdjson/dom/object.h"
#include "simdjson/dom/parser.h"
#include "simdjson/error.h"

namespace milvus::index {

namespace {

void
AppendNodeToDom(DomNode& root,
                const std::vector<std::string>& keys,
                DomNode value_node) {
    if (keys.empty()) {
        // BSON documents cannot directly hold a scalar root. Use an empty
        // field name as the physical wrapper; BuildKeyStatsForRow remaps its
        // offset from "/" back to the logical root pointer "". A real JSON
        // object member named "" is still kept as "/" because that row does
        // not contain a root scalar.
        root.document_children[""] = std::move(value_node);
        return;
    }

    DomNode* current = &root;
    for (size_t i = 0; i < keys.size(); ++i) {
        const std::string& key = keys[i];
        if (i == keys.size() - 1) {
            current->document_children[key] = std::move(value_node);
        } else {
            auto& child = current->document_children[key];
            if (child.type != DomNode::Type::DOCUMENT) {
                child = DomNode(DomNode::Type::DOCUMENT);
            }
            current = &child;
        }
    }
}

double
ParseJsonDouble(const std::string& value) {
    simdjson::padded_string padded(value.data(), value.size());
    simdjson::ondemand::parser parser;
    auto document = parser.iterate(padded);
    AssertInfo(document.error() == simdjson::SUCCESS,
               "invalid json number {}: {}",
               value,
               simdjson::error_message(document.error()));
    auto number = document.get_number();
    AssertInfo(number.error() == simdjson::SUCCESS,
               "invalid json number {}: {}",
               value,
               simdjson::error_message(number.error()));
    return number.value().as_double();
}

void
AppendJsonValueToBson(simdjson::ondemand::value value,
                      bson_t* out,
                      const char* key,
                      int key_len,
                      UnrepresentableJsonNumberPolicy number_policy) {
    auto type = value.type();
    if (type.error() != simdjson::SUCCESS &&
        number_policy == UnrepresentableJsonNumberPolicy::NORMALIZE_TO_NULL &&
        IsUnrepresentableJsonNumberError(type.error())) {
        bson_append_null(out, key, key_len);
        return;
    }
    AssertInfo(type.error() == simdjson::SUCCESS,
               "failed to read json array element type: {}",
               simdjson::error_message(type.error()));

    switch (type.value()) {
        case simdjson::ondemand::json_type::string: {
            auto string = value.get_string();
            AssertInfo(string.error() == simdjson::SUCCESS,
                       "failed to read json string: {}",
                       simdjson::error_message(string.error()));
            auto sv = string.value();
            bson_append_utf8(
                out, key, key_len, sv.data(), static_cast<int>(sv.size()));
            break;
        }
        case simdjson::ondemand::json_type::number: {
            auto number_result = value.get_number();
            if (number_result.error() != simdjson::SUCCESS) {
                AssertInfo(
                    number_policy ==
                        UnrepresentableJsonNumberPolicy::NORMALIZE_TO_NULL,
                    "V3 JSON stats cannot encode invalid array number: {}",
                    simdjson::error_message(number_result.error()));
                // Keep the array shape and valid siblings, but normalize the
                // unrepresentable element to JSON null in V4.
                bson_append_null(out, key, key_len);
                break;
            }
            const auto& number = number_result.value();
            if (number.is_int64()) {
                bson_append_int64(out, key, key_len, number.get_int64());
            } else {
                bson_append_double(out, key, key_len, number.as_double());
            }
            break;
        }
        case simdjson::ondemand::json_type::boolean: {
            auto boolean = value.get_bool();
            AssertInfo(boolean.error() == simdjson::SUCCESS,
                       "failed to read json boolean: {}",
                       simdjson::error_message(boolean.error()));
            bson_append_bool(out, key, key_len, boolean.value());
            break;
        }
        case simdjson::ondemand::json_type::null:
            bson_append_null(out, key, key_len);
            break;
        case simdjson::ondemand::json_type::object: {
            auto object = value.get_object();
            AssertInfo(object.error() == simdjson::SUCCESS,
                       "failed to read nested json object: {}",
                       simdjson::error_message(object.error()));
            bson_t child;
            bson_append_document_begin(out, key, key_len, &child);
            for (auto field : object.value()) {
                auto field_key = field.unescaped_key();
                AssertInfo(field_key.error() == simdjson::SUCCESS,
                           "failed to read nested json object key: {}",
                           simdjson::error_message(field_key.error()));
                auto child_value = field.value();
                AssertInfo(child_value.error() == simdjson::SUCCESS,
                           "failed to read nested json object value: {}",
                           simdjson::error_message(child_value.error()));
                AppendJsonValueToBson(
                    child_value.value(),
                    &child,
                    field_key.value().data(),
                    static_cast<int>(field_key.value().size()),
                    number_policy);
            }
            bson_append_document_end(out, &child);
            break;
        }
        case simdjson::ondemand::json_type::array: {
            auto array = value.get_array();
            AssertInfo(array.error() == simdjson::SUCCESS,
                       "failed to read nested json array: {}",
                       simdjson::error_message(array.error()));
            bson_t child;
            bson_append_array_begin(out, key, key_len, &child);
            uint32_t i = 0;
            char buf[16];
            const char* idx_key = nullptr;
            for (auto child_value : array.value()) {
                AssertInfo(child_value.error() == simdjson::SUCCESS,
                           "failed to read nested json array value: {}",
                           simdjson::error_message(child_value.error()));
                size_t klen =
                    bson_uint32_to_string(i, &idx_key, buf, sizeof(buf));
                AppendJsonValueToBson(child_value.value(),
                                      &child,
                                      idx_key,
                                      static_cast<int>(klen),
                                      number_policy);
                i++;
            }
            bson_append_array_end(out, &child);
            break;
        }
        default:
            ThrowInfo(ErrorCode::UnexpectedError,
                      "unsupported json array element type");
    }
}

}  // namespace

std::vector<uint8_t>
BuildBsonArrayBytesFromJsonString(
    const std::string& json_array,
    UnrepresentableJsonNumberPolicy number_policy) {
    simdjson::padded_string padded(json_array.data(), json_array.size());
    simdjson::ondemand::parser parser;
    auto document = parser.iterate(padded);
    AssertInfo(document.error() == simdjson::SUCCESS,
               "failed to parse json array: {}",
               simdjson::error_message(document.error()));
    auto root = document.get_array();
    AssertInfo(root.error() == simdjson::SUCCESS,
               "input is not a json array: {}",
               simdjson::error_message(root.error()));

    BsonDocument arr;
    uint32_t i = 0;
    char buf[16];
    const char* idx_key = nullptr;
    for (auto element : root.value()) {
        AssertInfo(element.error() == simdjson::SUCCESS,
                   "failed to read json array element: {}",
                   simdjson::error_message(element.error()));
        size_t klen = bson_uint32_to_string(i, &idx_key, buf, sizeof(buf));
        AppendJsonValueToBson(element.value(),
                              arr.get(),
                              idx_key,
                              static_cast<int>(klen),
                              number_policy);
        i++;
    }
    return std::vector<uint8_t>(arr.data(), arr.data() + arr.length());
}

void
BsonBuilder::AppendToDom(DomNode& root,
                         const std::vector<std::string>& keys,
                         const std::string& value,
                         const JSONType& type) {
    LOG_TRACE("append to dom: {} with value {} and type {}",
              Join(keys, "."),
              value,
              ToString(type));
    AppendNodeToDom(root, keys, CreateValueNode(value, type));
}

void
BsonBuilder::AppendDoubleToDom(DomNode& root,
                               const std::vector<std::string>& keys,
                               double value) {
    AppendNodeToDom(root, keys, CreateDoubleValueNode(value));
}

void
BsonBuilder::AppendArrayToDom(DomNode& root,
                              const std::vector<std::string>& keys,
                              std::vector<uint8_t> array_bytes) {
    AppendNodeToDom(root, keys, CreateArrayValueNode(std::move(array_bytes)));
}

DomNode
BsonBuilder::CreateValueNode(const std::string& value, JSONType type) {
    switch (type) {
        case JSONType::NONE: {
            DomScalar s;
            s.type = JSONType::NONE;
            return DomNode(std::move(s));
        }
        case JSONType::BOOL: {
            DomScalar s;
            s.type = JSONType::BOOL;
            s.b = (value == "true" || value == "1");
            return DomNode(std::move(s));
        }
        case JSONType::INT32: {
            DomScalar s;
            s.type = JSONType::INT32;
            s.i32 = std::stoi(value);
            return DomNode(std::move(s));
        }
        case JSONType::INT64: {
            DomScalar s;
            s.type = JSONType::INT64;
            s.i64 = std::stoll(value);
            return DomNode(std::move(s));
        }
        case JSONType::DOUBLE: {
            return CreateDoubleValueNode(ParseJsonDouble(value));
        }
        case JSONType::STRING: {
            DomScalar s;
            s.type = JSONType::STRING;
            s.str = value;
            return DomNode(std::move(s));
        }
        case JSONType::ARRAY: {
            try {
                DomScalar s;
                s.type = JSONType::ARRAY;
                s.arr_bytes = BuildBsonArrayBytesFromJsonString(
                    value, UnrepresentableJsonNumberPolicy::NORMALIZE_TO_NULL);
                return DomNode(std::move(s));
            } catch (const simdjson::simdjson_error& e) {
                ThrowInfo(
                    ErrorCode::UnexpectedError,
                    "Failed to build bson array (simdjson) from string: {}, {}",
                    value,
                    e.what());
            } catch (const std::exception& e) {
                ThrowInfo(
                    ErrorCode::UnexpectedError,
                    "Failed to build bson array (generic) from string: {}, {}",
                    value,
                    e.what());
            }
        }
        case JSONType::OBJECT: {
            AssertInfo(value == "{}",
                       "object value should be empty but got {}",
                       value);
            // return an empty json object as a document node
            return DomNode(DomNode::Type::DOCUMENT);
        }
        default:
            ThrowInfo(ErrorCode::Unsupported, "Unsupported JSON type {}", type);
    }
}

DomNode
BsonBuilder::CreateDoubleValueNode(double value) {
    DomScalar scalar;
    scalar.type = JSONType::DOUBLE;
    scalar.d = value;
    return DomNode(std::move(scalar));
}

DomNode
BsonBuilder::CreateArrayValueNode(std::vector<uint8_t> array_bytes) {
    DomScalar scalar;
    scalar.type = JSONType::ARRAY;
    scalar.arr_bytes = std::move(array_bytes);
    return DomNode(std::move(scalar));
}

void
BsonBuilder::ConvertDomToBson(const DomNode& node, bson_t* builder) {
    for (const auto& [key, child] : node.document_children) {
        const char* k = key.c_str();
        const int klen = static_cast<int>(key.size());
        switch (child.type) {
            case DomNode::Type::VALUE: {
                const DomScalar& s = child.value.value();
                switch (s.type) {
                    case JSONType::NONE:
                        bson_append_null(builder, k, klen);
                        break;
                    case JSONType::BOOL:
                        bson_append_bool(builder, k, klen, s.b);
                        break;
                    case JSONType::INT32:
                        bson_append_int32(builder, k, klen, s.i32);
                        break;
                    case JSONType::INT64:
                        bson_append_int64(builder, k, klen, s.i64);
                        break;
                    case JSONType::DOUBLE:
                        bson_append_double(builder, k, klen, s.d);
                        break;
                    case JSONType::STRING:
                        bson_append_utf8(builder,
                                         k,
                                         klen,
                                         s.str.data(),
                                         static_cast<int>(s.str.size()));
                        break;
                    case JSONType::ARRAY: {
                        bson_t arr;
                        if (bson_init_static(
                                &arr, s.arr_bytes.data(), s.arr_bytes.size())) {
                            bson_append_array(builder, k, klen, &arr);
                        }
                        break;
                    }
                    default:
                        ThrowInfo(ErrorCode::Unsupported,
                                  "Unsupported scalar JSON type {}",
                                  static_cast<int>(s.type));
                }
                break;
            }
            case DomNode::Type::DOCUMENT: {
                bson_t child_doc;
                bson_append_document_begin(builder, k, klen, &child_doc);
                ConvertDomToBson(child, &child_doc);
                bson_append_document_end(builder, &child_doc);
                break;
            }
            default: {
                ThrowInfo(ErrorCode::Unsupported,
                          "Unsupported DOM node type {}",
                          static_cast<int>(child.type));
            }
        }
    }
}

void
BsonBuilder::ExtractOffsetsRecursive(
    const uint8_t* root_base_ptr,
    const uint8_t* current_base_ptr,
    const std::string& current_path,
    std::vector<std::pair<std::string, size_t>>& result) {
    uint32_t length;
    milvus::fastmem::FastMemcpy(&length, current_base_ptr, 4);

    const uint8_t* end_ptr = current_base_ptr + length - 1;
    AssertInfo(*(end_ptr) == 0x00, "miss bson document terminator");

    const uint8_t* ptr = current_base_ptr + 4;

    while (ptr <= end_ptr && *ptr != 0x00) {
        // record key offset
        size_t key_offset = ptr - root_base_ptr;

        // read key type
        auto type = static_cast<milvus::bson::type>(*ptr++);

        // read key
        auto key_name = reinterpret_cast<const char*>(ptr);
        ptr += strlen(key_name) + 1;

        // construct key path
        std::string key_path = AppendJsonPointer(current_path, key_name);

        // do not record key offset pair for null value
        // because null value is not a valid key
        if (type != milvus::bson::type::k_null) {
            result.emplace_back(key_path, key_offset);
        }

        // handle value
        switch (type) {
            case milvus::bson::type::k_document: {
                ExtractOffsetsRecursive(root_base_ptr, ptr, key_path, result);
                // skip sub doc
                uint32_t child_len;
                milvus::fastmem::FastMemcpy(&child_len, ptr, 4);
                ptr += child_len;
                break;
            }
            case milvus::bson::type::k_array: {
                // not parse array
                // skip sub doc
                uint32_t child_len;
                milvus::fastmem::FastMemcpy(&child_len, ptr, 4);
                ptr += child_len;
                break;
            }
            case milvus::bson::type::k_string: {
                uint32_t str_len;
                milvus::fastmem::FastMemcpy(&str_len, ptr, 4);
                ptr += 4 + str_len;
                break;
            }
            case milvus::bson::type::k_int32: {
                ptr += 4;
                break;
            }
            case milvus::bson::type::k_int64: {
                ptr += 8;
                break;
            }
            case milvus::bson::type::k_double: {
                ptr += 8;
                break;
            }
            case milvus::bson::type::k_bool: {
                ptr += 1;
                break;
            }
            case milvus::bson::type::k_null: {
                break;
            }
            default: {
                ThrowInfo(ErrorCode::Unsupported,
                          "Unsupported BSON type {}",
                          static_cast<int>(type));
            }
        }
    }
}

}  // namespace milvus::index
