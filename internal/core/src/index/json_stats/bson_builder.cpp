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

#include <cstdlib>
#include <iostream>
#include <vector>
#include <string>
#include <map>

#include "index/json_stats/bson_builder.h"
#include "common/EasyAssert.h"

namespace milvus::index {

namespace {

using bsoncxx::builder::basic::kvp;

void
AppendJsonValueToBsonArray(simdjson::ondemand::value value,
                           bsoncxx::builder::basic::array& out);

void
AppendNodeToDom(DomNode& root,
                const std::vector<std::string>& keys,
                DomNode value_node) {
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
                      std::string_view key,
                      bsoncxx::builder::basic::document& out) {
    auto type = value.type();
    AssertInfo(type.error() == simdjson::SUCCESS,
               "failed to read json array element type: {}",
               simdjson::error_message(type.error()));

    switch (type.value()) {
        case simdjson::ondemand::json_type::string: {
            auto string = value.get_string();
            AssertInfo(string.error() == simdjson::SUCCESS,
                       "failed to read json string: {}",
                       simdjson::error_message(string.error()));
            out.append(kvp(std::string(key), std::string(string.value())));
            break;
        }
        case simdjson::ondemand::json_type::number: {
            auto number_result = value.get_number();
            if (number_result.error() != simdjson::SUCCESS) {
                out.append(
                    kvp(std::string(key), bsoncxx::types::b_undefined{}));
                break;
            }
            const auto& number = number_result.value();
            if (number.is_int64()) {
                out.append(kvp(std::string(key), number.get_int64()));
            } else {
                out.append(kvp(std::string(key), number.as_double()));
            }
            break;
        }
        case simdjson::ondemand::json_type::boolean: {
            auto boolean = value.get_bool();
            AssertInfo(boolean.error() == simdjson::SUCCESS,
                       "failed to read json boolean: {}",
                       simdjson::error_message(boolean.error()));
            out.append(kvp(std::string(key), boolean.value()));
            break;
        }
        case simdjson::ondemand::json_type::null:
            out.append(kvp(std::string(key), bsoncxx::types::b_null{}));
            break;
        case simdjson::ondemand::json_type::object: {
            auto object = value.get_object();
            AssertInfo(object.error() == simdjson::SUCCESS,
                       "failed to read nested json object: {}",
                       simdjson::error_message(object.error()));
            bsoncxx::builder::basic::document child;
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
                    child_value.value(), field_key.value(), child);
            }
            out.append(kvp(std::string(key), child.extract()));
            break;
        }
        case simdjson::ondemand::json_type::array: {
            auto array = value.get_array();
            AssertInfo(array.error() == simdjson::SUCCESS,
                       "failed to read nested json array: {}",
                       simdjson::error_message(array.error()));
            bsoncxx::builder::basic::array child;
            for (auto child_value : array.value()) {
                AssertInfo(child_value.error() == simdjson::SUCCESS,
                           "failed to read nested json array value: {}",
                           simdjson::error_message(child_value.error()));
                AppendJsonValueToBsonArray(child_value.value(), child);
            }
            out.append(kvp(std::string(key), child.extract()));
            break;
        }
        default:
            ThrowInfo(ErrorCode::UnexpectedError,
                      "unsupported json value type");
    }
}

void
AppendJsonValueToBsonArray(simdjson::ondemand::value value,
                           bsoncxx::builder::basic::array& out) {
    auto type = value.type();
    AssertInfo(type.error() == simdjson::SUCCESS,
               "failed to read json array element type: {}",
               simdjson::error_message(type.error()));

    switch (type.value()) {
        case simdjson::ondemand::json_type::string: {
            auto string = value.get_string();
            AssertInfo(string.error() == simdjson::SUCCESS,
                       "failed to read json string: {}",
                       simdjson::error_message(string.error()));
            out.append(std::string(string.value()));
            break;
        }
        case simdjson::ondemand::json_type::number: {
            auto number = value.get_number();
            if (number.error() != simdjson::SUCCESS) {
                out.append(bsoncxx::types::b_undefined{});
                break;
            }
            if (number.value().is_int64()) {
                out.append(number.value().get_int64());
            } else {
                out.append(number.value().as_double());
            }
            break;
        }
        case simdjson::ondemand::json_type::boolean: {
            auto boolean = value.get_bool();
            AssertInfo(boolean.error() == simdjson::SUCCESS,
                       "failed to read json boolean: {}",
                       simdjson::error_message(boolean.error()));
            out.append(boolean.value());
            break;
        }
        case simdjson::ondemand::json_type::null:
            out.append(bsoncxx::types::b_null{});
            break;
        case simdjson::ondemand::json_type::object: {
            auto object = value.get_object();
            AssertInfo(object.error() == simdjson::SUCCESS,
                       "failed to read nested json object: {}",
                       simdjson::error_message(object.error()));
            bsoncxx::builder::basic::document child;
            for (auto field : object.value()) {
                auto field_key = field.unescaped_key();
                auto child_value = field.value();
                AssertInfo(field_key.error() == simdjson::SUCCESS &&
                               child_value.error() == simdjson::SUCCESS,
                           "failed to read nested json object");
                AppendJsonValueToBson(
                    child_value.value(), field_key.value(), child);
            }
            out.append(child.extract());
            break;
        }
        case simdjson::ondemand::json_type::array: {
            auto array = value.get_array();
            AssertInfo(array.error() == simdjson::SUCCESS,
                       "failed to read nested json array: {}",
                       simdjson::error_message(array.error()));
            bsoncxx::builder::basic::array child;
            for (auto child_value : array.value()) {
                AssertInfo(child_value.error() == simdjson::SUCCESS,
                           "failed to read nested json array value: {}",
                           simdjson::error_message(child_value.error()));
                AppendJsonValueToBsonArray(child_value.value(), child);
            }
            out.append(child.extract());
            break;
        }
        default:
            ThrowInfo(ErrorCode::UnexpectedError,
                      "unsupported json array element type");
    }
}
}  // namespace

// Parse a JSON array string with simdjson and build an owning BSON array value
bsoncxx::array::value
BuildBsonArrayFromJsonString(const std::string& json_array) {
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

    bsoncxx::builder::basic::array out;
    for (auto element : root.value()) {
        AssertInfo(element.error() == simdjson::SUCCESS,
                   "failed to read json array element: {}",
                   simdjson::error_message(element.error()));
        AppendJsonValueToBsonArray(element.value(), out);
    }
    return out.extract();
}

std::vector<uint8_t>
BuildBsonArrayBytesFromJsonString(const std::string& json_array) {
    auto arr_value = BuildBsonArrayFromJsonString(json_array);
    auto view = arr_value.view();
    return std::vector<uint8_t>(view.data(), view.data() + view.length());
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
BsonBuilder::AppendUndefinedToDom(DomNode& root,
                                  const std::vector<std::string>& keys) {
    AppendNodeToDom(root, keys, CreateUndefinedValueNode());
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
            return DomNode(bsoncxx::types::b_null{});
        }
        case JSONType::BOOL: {
            bool b = (value == "true" || value == "1");
            return DomNode(bsoncxx::types::b_bool{b});
        }
        case JSONType::INT32: {
            int32_t i = std::stoi(value);
            return DomNode(bsoncxx::types::b_int32{i});
        }
        case JSONType::INT64: {
            int64_t l = std::stoll(value);
            return DomNode(bsoncxx::types::b_int64{l});
        }
        case JSONType::DOUBLE: {
            return CreateDoubleValueNode(ParseJsonDouble(value));
        }
        case JSONType::STRING: {
            return DomNode(bsoncxx::types::b_string{value});
        }
        case JSONType::ARRAY: {
            try {
                auto arr_value = BuildBsonArrayFromJsonString(value);
                return DomNode(bsoncxx::types::b_array{arr_value.view()});
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
    return DomNode(bsoncxx::types::b_double{value});
}

DomNode
BsonBuilder::CreateUndefinedValueNode() {
    return DomNode(bsoncxx::types::b_undefined{});
}

DomNode
BsonBuilder::CreateArrayValueNode(std::vector<uint8_t> array_bytes) {
    bsoncxx::array::view view(array_bytes.data(), array_bytes.size());
    return DomNode(bsoncxx::types::b_array{view});
}

void
BsonBuilder::ConvertDomToBson(const DomNode& node,
                              bsoncxx::builder::basic::document& builder) {
    for (const auto& [key, child] : node.document_children) {
        switch (child.type) {
            case DomNode::Type::VALUE: {
                builder.append(bsoncxx::builder::basic::kvp(
                    key, child.bson_value.value()));
                break;
            }
            case DomNode::Type::DOCUMENT: {
                bsoncxx::builder::basic::document sub_doc;
                ConvertDomToBson(child, sub_doc);
                builder.append(bsoncxx::builder::basic::kvp(key, sub_doc));
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
    memcpy(&length, current_base_ptr, 4);

    const uint8_t* end_ptr = current_base_ptr + length - 1;
    AssertInfo(*(end_ptr) == 0x00, "miss bson document terminator");

    const uint8_t* ptr = current_base_ptr + 4;

    while (ptr <= end_ptr && *ptr != 0x00) {
        // record key offset
        size_t key_offset = ptr - root_base_ptr;

        // read key type
        auto type = static_cast<bsoncxx::type>(*ptr++);

        // read key
        auto key_name = reinterpret_cast<const char*>(ptr);
        ptr += strlen(key_name) + 1;

        // construct key path
        std::string key_path = AppendJsonPointer(current_path, key_name);

        // do not record key offset pair for null value
        // because null value is not a valid key
        if (type != bsoncxx::type::k_null) {
            result.emplace_back(key_path, key_offset);
        }

        // handle value
        switch (type) {
            case bsoncxx::type::k_document: {
                ExtractOffsetsRecursive(root_base_ptr, ptr, key_path, result);
                // skip sub doc
                uint32_t child_len;
                memcpy(&child_len, ptr, 4);
                ptr += child_len;
                break;
            }
            case bsoncxx::type::k_array: {
                // not parse array
                // skip sub doc
                uint32_t child_len;
                memcpy(&child_len, ptr, 4);
                ptr += child_len;
                break;
            }
            case bsoncxx::type::k_string: {
                uint32_t str_len;
                memcpy(&str_len, ptr, 4);
                ptr += 4 + str_len;
                break;
            }
            case bsoncxx::type::k_int32: {
                ptr += 4;
                break;
            }
            case bsoncxx::type::k_int64: {
                ptr += 8;
                break;
            }
            case bsoncxx::type::k_double: {
                ptr += 8;
                break;
            }
            case bsoncxx::type::k_bool: {
                ptr += 1;
                break;
            }
            case bsoncxx::type::k_null: {
                break;
            }
            case bsoncxx::type::k_undefined: {
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
